# pgqueue

[![CI](https://github.com/sgaunet/pgqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/sgaunet/pgqueue/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/sgaunet/pgqueue)](https://goreportcard.com/report/github.com/sgaunet/pgqueue)
[![GoDoc](https://godoc.org/github.com/sgaunet/pgqueue?status.svg)](https://godoc.org/github.com/sgaunet/pgqueue/pkg/pgqueue)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A PostgreSQL-based message queue library for Go with exactly-once delivery guarantees.

## Features

- **Two Messaging Patterns**:
  - **Pub/Sub**: Fan-out messaging where each subscriber receives all messages
  - **Channels**: Point-to-point queuing where each message is delivered to a single consumer

- **Exactly-Once Delivery**: Guaranteed via deduplication, visibility timeout, and transactional acknowledgments
- **Message Ordering**: Per-topic/channel ordering using UUIDv7
- **Dead Letter Queue**: Failed messages automatically moved after max retries
- **Message Replay**: Replay messages from specific timestamps or DLQ
- **Garbage Collection**: Automatic cleanup with configurable retention policies
- **Statistics & Monitoring**: Track message counts, processing times, and queue depth

## Requirements

- PostgreSQL 18+
- Go 1.21+

## Installation

```bash
go get github.com/sgaunet/pgqueue
```

## Quick Start

### With pgx driver (recommended)

```go
package main

import (
    "context"
    "database/sql"
    "log"

    _ "github.com/jackc/pgx/v5/stdlib"
    "github.com/sgaunet/pgqueue/pkg/pgqueue"
)

func main() {
    ctx := context.Background()

    // Open database connection
    db, err := sql.Open("pgx", "postgres://user:pass@localhost/dbname?sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Initialize base schema (one-time setup, idempotent)
    if err := pgqueue.InitSchema(ctx, db); err != nil {
        log.Fatal(err)
    }

    // Initialize pgqueue
    pq, err := pgqueue.Init(ctx, pgqueue.Config{
        DB:                db,
        MaxMessageSize:   1024,
        DefaultMaxRetries: 3,
    })
    if err != nil {
        log.Fatal(err)
    }

    // Create a channel
    err = pq.CreateChannel(ctx, "orders", pgqueue.ChannelOptions{})
    if err != nil {
        log.Fatal(err)
    }

    // Publish a message
    msgID, err := pq.Publish(ctx, "orders", []byte("order-123"))
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Published message: %s", msgID)
}
```

### With lib/pq driver

```go
package main

import (
    "context"
    "database/sql"
    "log"

    _ "github.com/lib/pq"
    "github.com/sgaunet/pgqueue/pkg/pgqueue"
)

func main() {
    ctx := context.Background()

    // Open database connection
    db, err := sql.Open("postgres", "postgres://user:pass@localhost/dbname?sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Initialize base schema (one-time setup, idempotent)
    if err := pgqueue.InitSchema(ctx, db); err != nil {
        log.Fatal(err)
    }

    // Initialize pgqueue (same API as pgx)
    pq, err := pgqueue.Init(ctx, pgqueue.Config{
        DB: db,
    })
    if err != nil {
        log.Fatal(err)
    }

    // Use the library...
}
```

## Architecture

- **Table-per-queue**: Each channel/topic has dedicated tables for isolation and performance
- **UUIDv7 for ordering**: Time-ordered identifiers ensure message ordering
- **Direct SQL**: Parameterized queries via database/sql for type safety
- **Self-migrating schema**: `InitSchema()` creates and version-migrates the schema in-process — no external migration tools required

## Examples

Complete working examples demonstrating both messaging patterns:

- **[Channel Example](examples/channel/)** - Point-to-point order processing with retry logic
- **[Pub/Sub Example](examples/pubsub/)** - Fan-out user events to multiple subscribers

Both examples include:
- Setup instructions and prerequisites
- pgx and lib/pq driver alternatives
- Error handling and acknowledgment patterns
- Statistics and monitoring

## Connection Pooling

pgqueue uses `*sql.DB` which manages its own connection pool. Configure it for your workload:

```go
db, err := sql.Open("pgx", connString)
if err != nil {
    log.Fatal(err)
}

// Recommended settings
db.SetMaxOpenConns(25)                  // Max concurrent connections
db.SetMaxIdleConns(5)                   // Idle connections to keep
db.SetConnMaxLifetime(5 * time.Minute)  // Recycle connections periodically
```

### Sizing Guidelines

| Setting | Formula | Notes |
|---------|---------|-------|
| `MaxOpenConns` | workers x 2-3 | Account for GC, stats, and admin queries |
| `MaxIdleConns` | 25% of MaxOpenConns | Avoid reconnection overhead |
| `ConnMaxLifetime` | 5-15 minutes | Helps with connection rebalancing behind load balancers |

For high-throughput workloads:

```go
db.SetMaxOpenConns(100)
db.SetMaxIdleConns(20)
db.SetConnMaxLifetime(10 * time.Minute)
```

### Monitoring Pool Health

```go
stats := db.Stats()
log.Printf("Open: %d, InUse: %d, Idle: %d, WaitCount: %d",
    stats.OpenConnections, stats.InUse, stats.Idle, stats.WaitCount)
```

If `WaitCount` is consistently growing, increase `MaxOpenConns`. If `Idle` stays near `MaxIdleConns`, you can raise it to reduce reconnection churn.

## Development

### Running Tests

```bash
# Integration tests require Docker for testcontainers
go test ./... -v
```

## License

MIT

## Contributing

Contributions welcome! Please open an issue or PR.
