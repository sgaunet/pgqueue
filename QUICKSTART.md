# pgqueue Quick Start Guide

## Installation

```bash
go get github.com/sgaunet/pgqueue
```

## Prerequisites

- PostgreSQL 18+
- Go 1.25+
- Docker (only for running the integration test suite)

## Basic Usage

### 1. Point-to-Point Channel

```go
package main

import (
    "context"
    "database/sql"
    "errors"
    "log"

    _ "github.com/jackc/pgx/v5/stdlib" // or _ "github.com/lib/pq"
    "github.com/sgaunet/pgqueue"
)

func main() {
    ctx := context.Background()

    // 1. Connect to PostgreSQL.
    db, err := sql.Open("pgx", "postgres://user:pass@localhost:5432/mydb?sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // 2. Install / migrate the schema (one-time, idempotent).
    if err := pgqueue.InitSchema(ctx, db); err != nil {
        log.Fatal(err)
    }

    // 3. Construct a Queue with functional options.
    q, err := pgqueue.New(ctx, db,
        pgqueue.WithDefaultMaxRetries(3), // retry 3 times before the DLQ
    )
    if err != nil {
        log.Fatal(err)
    }
    defer q.Close() // stops background consume loops, the GC, and the listener

    // 4. Create a channel. Per-queue overrides are functional QueueOptions.
    if err := q.CreateChannel(ctx, "orders",
        pgqueue.WithQueueMaxMessageSize(2048),
    ); err != nil {
        log.Fatal(err)
    }

    // 5. Publish a message.
    msgID, err := q.PublishChannel(ctx, "orders", []byte(`{"order_id":"123"}`))
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("published message: %s", msgID)

    // 6. Consume a message (single-shot). ErrQueueEmpty means nothing is ready.
    msg, err := q.ReceiveChannel(ctx, "orders")
    switch {
    case errors.Is(err, pgqueue.ErrQueueEmpty):
        log.Println("queue empty")
        return
    case err != nil:
        log.Fatal(err)
    }
    log.Printf("processing: %s", msg.Payload)

    // 7. Acknowledge using the receipt carried by the message.
    if err := q.Ack(ctx, msg.Receipt()); err != nil {
        log.Fatal(err)
    }
    log.Println("message acknowledged")
}
```

### 2. Pub/Sub Topic (Fan-Out)

```go
package main

import (
    "context"
    "database/sql"
    "errors"
    "log"

    _ "github.com/jackc/pgx/v5/stdlib"
    "github.com/sgaunet/pgqueue"
)

func main() {
    ctx := context.Background()

    db, err := sql.Open("pgx", "postgres://user:pass@localhost:5432/mydb?sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    if err := pgqueue.InitSchema(ctx, db); err != nil {
        log.Fatal(err)
    }
    q, err := pgqueue.New(ctx, db)
    if err != nil {
        log.Fatal(err)
    }
    defer q.Close()

    // Create a topic and register subscribers.
    if err := q.CreateTopic(ctx, "events"); err != nil {
        log.Fatal(err)
    }
    services := []string{"email-service", "analytics-service", "notification-service"}
    for _, sub := range services {
        if err := q.Subscribe(ctx, "events", sub); err != nil {
            log.Fatal(err)
        }
    }

    // Publish once; every subscriber registered at publish time gets a copy.
    msgID, err := q.PublishTopic(ctx, "events", []byte(`{"event":"user_signup"}`))
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("published event: %s", msgID)

    // Each subscriber consumes independently.
    for _, sub := range services {
        msg, err := q.ReceiveTopic(ctx, "events", sub)
        switch {
        case errors.Is(err, pgqueue.ErrQueueEmpty):
            continue
        case err != nil:
            log.Fatal(err)
        }
        log.Printf("%s received: %s", sub, msg.Payload)
        if err := q.Ack(ctx, msg.Receipt()); err != nil {
            log.Fatal(err)
        }
    }
}
```

> A subscriber receives only messages published **after** it subscribed:
> subscription rows are created at publish time for the subscribers that exist
> then.

## Schema Initialization

The library handles schema initialization itself via `pgqueue.InitSchema()`:

```go
// Install / migrate the base schema (one-time setup, idempotent).
if err := pgqueue.InitSchema(ctx, db); err != nil {
    log.Fatal(err)
}
```

This creates four base tables:
- `pgqueue_metadata` - Tracks all queues and topics
- `pgqueue_subscribers` - Tracks pub/sub subscriptions
- `pgqueue_replay_log` - Audit log for message replay operations
- `pgqueue_schema_version` - Tracks applied schema migrations

The function is idempotent (safe to call multiple times) and safe to run
concurrently from multiple processes. It is also the **upgrade path**: when a
newer release of pgqueue changes the schema, `InitSchema()` transparently
applies the pending migrations — no external migration tool needed. Call
`q.GetSchemaVersion(ctx)` to inspect the applied version.

**Note**: Per-queue tables (`pgqueue_msg_*`, `pgqueue_dlq_*`, etc.) are created
automatically when you call `CreateChannel()` or `CreateTopic()`.

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

### Handler-based worker pool (recommended)

`ConsumeChannel` owns the loop — it fetches messages, calls your handler, and
auto-acks (handler returns `nil`) or auto-nacks with backoff retry (handler
returns an error). `WithConcurrency` runs parallel workers.

```go
err := q.ConsumeChannel(ctx, "orders",
    func(ctx context.Context, msg *pgqueue.Message) error {
        return processOrder(ctx, msg.Payload) // nil -> ack; err -> nack + retry
    },
    pgqueue.WithConcurrency(8),
)
// ConsumeChannel blocks until ctx is cancelled, then drains its workers.
if err != nil {
    log.Fatal(err)
}
```

`ConsumeTopic(ctx, "events", "email-service", handler, ...)` is the pub/sub
equivalent.

### Manual ack/nack

With the single-shot `ReceiveChannel` (or the `ChannelMessages` range-over-func
iterator) you acknowledge explicitly. A nack retries the message, or moves it to
the DLQ once `max_retries` is exhausted.

```go
msg, err := q.ReceiveChannel(ctx, "orders")
switch {
case errors.Is(err, pgqueue.ErrQueueEmpty):
    // nothing available right now
case err != nil:
    log.Fatal(err)
default:
    if err := processOrder(msg.Payload); err != nil {
        _ = q.Nack(ctx, msg.Receipt(), err.Error()) // retry or DLQ
    } else {
        _ = q.Ack(ctx, msg.Receipt())
    }
}
```

`Ack`/`Nack` return `ErrClaimExpired` if the visibility timeout lapsed and the
message was already redelivered to another consumer — discard your result in
that case.

### Deduplication

Publish with an explicit message ID; a second publish of the same ID returns
`ErrDuplicateMessageID`.

```go
import "github.com/google/uuid"

// Derive a stable ID from your business key, or generate a fresh UUIDv7.
id := uuid.NewSHA1(uuid.NameSpaceURL, []byte("order-12345"))

_, err := q.PublishChannel(ctx, "orders", payload, pgqueue.WithMessageID(id))
if errors.Is(err, pgqueue.ErrDuplicateMessageID) {
    log.Println("already published")
}
```

### Garbage collection (storage retention)

Message redelivery on a crashed consumer and DLQ promotion of retry-exhausted
messages happen automatically. Reclaiming **storage** (completed messages,
expired DLQ entries, acked subscription rows) does not — you must run a
`GarbageCollector`.

```go
gc := pgqueue.NewGarbageCollector(q, pgqueue.GarbageCollectorConfig{
    Interval: 5 * time.Minute,
    DefaultPolicy: pgqueue.RetentionPolicy{
        CompletedMessageTTL: 24 * time.Hour,
        MaxPendingAge:       7 * 24 * time.Hour,
        DLQRetention:        30 * 24 * time.Hour,
    },
})
gc.Start(ctx) // background loop; q.Close() stops it
```

> **`RetentionPolicy` fields default to `0`, which means "keep forever".** A
> `GarbageCollector` with an empty policy reclaims nothing — set positive
> durations to enable purging.

## Configuration Options

Configuration is supplied through functional options, not config structs.

### Queue-wide options — passed to `pgqueue.New`

| Option | Purpose |
|--------|---------|
| `WithMaxMessageSize(bytes)` | Max payload size (default 256 KiB) |
| `WithDefaultMaxRetries(n)` | Delivery attempts before the DLQ (default 3; `0` = DLQ on first failure) |
| `WithDefaultTTL(d)` | Default message TTL (`0` = never expire) |
| `WithMaxQueues(n)` | Cap the total number of queues (`0` = unlimited) |
| `WithSchema(name)` | PostgreSQL schema for all pgqueue tables (default `public`) |
| `WithBackoffPolicy(p)` | Retry backoff policy (decorrelated jitter) |
| `WithListener(l)` | Enable `LISTEN/NOTIFY` push delivery |
| `WithTracer(t)` / `WithMetrics(m)` | Observability hooks |
| `WithLogger(l)` | Structured logging (`nil` = silent, the default) |

### Per-queue options — passed to `CreateChannel` / `CreateTopic`

| Option | Purpose |
|--------|---------|
| `WithQueueMaxMessageSize(bytes)` | Override the max payload size for this queue |
| `WithQueueTTL(d)` | Override the message TTL for this queue (`0` = never expire) |
| `WithQueueMaxRetries(n)` | Override max retries (`0` = DLQ on first failure) |

> `WithSchema` must be passed to **both** `InitSchema` and `New`, with the same
> value.

## Architecture

- **Table-per-queue**: Each channel/topic gets dedicated tables for isolation
- **UUIDv7**: Time-ordered message IDs for natural ordering
- **FOR UPDATE SKIP LOCKED**: Efficient non-blocking consumption
- **Transactional**: All operations are ACID-compliant
- **Direct SQL**: Parameterized queries via database/sql

## Next Steps

- Check [examples/](examples/) for complete, runnable programs

## Support

- Issues: https://github.com/sgaunet/pgqueue/issues
- Documentation: https://pkg.go.dev/github.com/sgaunet/pgqueue

## License

MIT
