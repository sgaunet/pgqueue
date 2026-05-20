# pgqueue Quick Start Guide

## Installation

```bash
go get github.com/sgaunet/pgqueue
```

## Prerequisites

- PostgreSQL 18+
- Go 1.25+
- Docker (for running tests)

## Basic Usage

### 1. Point-to-Point Channel

```go
package main

import (
    "context"
    "database/sql"
    "log"
    "time"

    _ "github.com/jackc/pgx/v5/stdlib"  // or _ "github.com/lib/pq"
    "github.com/sgaunet/pgqueue/pkg/pgqueue"
)

func main() {
    ctx := context.Background()

    // 1. Connect to PostgreSQL
    db, err := sql.Open("pgx", "postgres://user:pass@localhost:5432/mydb?sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // 2. Initialize base schema (one-time setup, idempotent)
    if err := pgqueue.InitSchema(ctx, db); err != nil {
        log.Fatal(err)
    }

    // 3. Initialize pgqueue
    pq, err := pgqueue.Init(ctx, pgqueue.Config{
        DB:                db,
        MaxMessageSize:   1024,      // 1KB default
        DefaultMaxRetries: 3,         // Retry 3 times before DLQ
    })
    if err != nil {
        log.Fatal(err)
    }

    // 4. Create a channel
    err = pq.CreateChannel(ctx, "orders", pgqueue.ChannelOptions{
        MaxMessageSize: 2048, // Override default
    })
    if err != nil {
        log.Fatal(err)
    }

    // 5. Publish messages
    msgID, err := pq.Publish(ctx, "orders", []byte(`{"order_id": "123"}`))
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Published message: %s", msgID)

    // 6. Consume messages
    msg, err := pq.ConsumeFromChannel(ctx, "orders", 30*time.Second)
    if err != nil {
        log.Fatal(err)
    }
    if msg != nil {
        log.Printf("Processing message: %s", string(msg.Payload))

        // Process your message...

        // 7. Acknowledge when done
        if err := pq.AckChannel(ctx, "orders", msg.ID); err != nil {
            log.Fatal(err)
        }
        log.Println("Message acknowledged")
    }
}
```

### 2. Pub/Sub Topic (Fan-Out)

```go
package main

import (
    "context"
    "database/sql"
    "log"
    "time"

    _ "github.com/jackc/pgx/v5/stdlib"
    "github.com/sgaunet/pgqueue/pkg/pgqueue"
)

func main() {
    ctx := context.Background()

    // Connect and initialize base schema
    db, err := sql.Open("pgx", "postgres://user:pass@localhost:5432/mydb")
    if err != nil {
        log.Fatal(err)
    }
    if err := pgqueue.InitSchema(ctx, db); err != nil {
        log.Fatal(err)
    }
    pq, err := pgqueue.Init(ctx, pgqueue.Config{DB: db})
    if err != nil {
        log.Fatal(err)
    }

    // Create a topic
    if err := pq.CreateTopic(ctx, "events", pgqueue.TopicOptions{}); err != nil {
        log.Fatal(err)
    }

    // Register subscribers
    pq.Subscribe(ctx, "events", "email-service")
    pq.Subscribe(ctx, "events", "analytics-service")
    pq.Subscribe(ctx, "events", "notification-service")

    // Publish a message (all subscribers will receive it)
    msgID, _ := pq.Publish(ctx, "events", []byte(`{"event": "user_signup"}`))
    log.Printf("Published event: %s", msgID)

    // Each service consumes independently
    consumeForService := func(serviceName string) {
        msg, _ := pq.ConsumeFromTopic(ctx, "events", serviceName, 30*time.Second)
        if msg != nil {
            log.Printf("%s received: %s", serviceName, string(msg.Payload))
            pq.AckTopic(ctx, "events", serviceName, msg.ID)
        }
    }

    consumeForService("email-service")
    consumeForService("analytics-service")
    consumeForService("notification-service")
}
```

## Schema Initialization

The library handles schema initialization automatically via `pgqueue.InitSchema()`:

```go
// Initialize base schema (one-time setup, idempotent)
if err := pgqueue.InitSchema(ctx, db); err != nil {
    log.Fatal(err)
}
```

This creates three base tables:
- `pgqueue_metadata` - Tracks all queues and topics
- `pgqueue_subscribers` - Tracks pub/sub subscriptions
- `pgqueue_replay_log` - Audit log for message replay operations

The function is idempotent (safe to call multiple times) and uses `CREATE TABLE IF NOT EXISTS`.

**Note**: Per-queue tables (`pgqueue_msg_*`, `pgqueue_dlq_*`, etc.) are created automatically when you call `CreateChannel()` or `CreateTopic()`.

## Running Tests

Unit tests live in the root module; the Docker-backed integration suite lives in
the nested `internal/integration` module.

```bash
# Unit tests (root module, no Docker)
go test ./...

# Ensure Docker is running for the integration suite
docker ps

# Run the integration suite
cd internal/integration && go test ./... -v

# Run a specific integration test
cd internal/integration && go test . -run TestPubSubFanout -v
```

## Common Patterns

### Error Handling (Nack)

```go
msg, _ := pq.ConsumeFromChannel(ctx, "orders", 30*time.Second)
if msg != nil {
    if err := processOrder(msg.Payload); err != nil {
        // Negative acknowledge - will retry or move to DLQ
        pq.NackChannel(ctx, "orders", msg.ID, err.Error())
    } else {
        pq.AckChannel(ctx, "orders", msg.ID)
    }
}
```

### Worker Loop

```go
func worker(ctx context.Context, pq *pgqueue.PGQueue, channelName string) {
    for {
        select {
        case <-ctx.Done():
            return
        default:
            msg, err := pq.ConsumeFromChannel(ctx, channelName, 30*time.Second)
            if err != nil {
                log.Printf("Error consuming: %v", err)
                time.Sleep(time.Second)
                continue
            }
            if msg == nil {
                // No messages, wait and retry
                time.Sleep(100 * time.Millisecond)
                continue
            }

            // Process message
            if err := processMessage(msg.Payload); err != nil {
                pq.NackChannel(ctx, channelName, msg.ID, err.Error())
            } else {
                pq.AckChannel(ctx, channelName, msg.ID)
            }
        }
    }
}

// Run multiple workers
func main() {
    // ... init code ...

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    for i := 0; i < 5; i++ {
        go worker(ctx, pq, "orders")
    }

    // Wait for shutdown signal
    <-ctx.Done()
}
```

### Deduplication

```go
// Use PublishWithID for deduplication.
// messageID must be a valid UUID — derive one deterministically from your
// business key, or generate a fresh UUIDv7.
messageID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("order-12345"))

// This will fail if the same ID is published twice.
_, err := pq.PublishWithID(ctx, "orders", messageID, payload, nil)
if err != nil {
    log.Printf("Duplicate message: %v", err)
}
```

## Configuration Options

### ChannelOptions

```go
type ChannelOptions struct {
    MaxMessageSize int           // Max message size (0 = use default)
    TTL            time.Duration // Message expiration (0 = no expiration)
    MaxRetries     int           // Max retry attempts (0 = use default)
}
```

### TopicOptions

```go
type TopicOptions struct {
    MaxMessageSize int           // Max message size (0 = use default)
    TTL            time.Duration // Message TTL (0 = no expiration)
    MaxRetries     int           // Max retry attempts per subscriber
}
```

## Architecture

- **Table-per-queue**: Each channel/topic gets dedicated tables for isolation
- **UUIDv7**: Time-ordered message IDs for natural ordering
- **FOR UPDATE SKIP LOCKED**: Efficient non-blocking consumption
- **Transactional**: All operations are ACID-compliant
- **Direct SQL**: Parameterized queries via database/sql

## Next Steps

- Check [examples/](examples/) for more examples

## Support

- Issues: https://github.com/sgaunet/pgqueue/issues
- Documentation: https://pkg.go.dev/github.com/sgaunet/pgqueue

## License

MIT
