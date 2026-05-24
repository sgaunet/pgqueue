# pgqueue

[![CI](https://github.com/sgaunet/pgqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/sgaunet/pgqueue/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/sgaunet/pgqueue)](https://goreportcard.com/report/github.com/sgaunet/pgqueue)
[![GoDoc](https://godoc.org/github.com/sgaunet/pgqueue?status.svg)](https://godoc.org/github.com/sgaunet/pgqueue)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A PostgreSQL-based message queue library for Go with at-least-once delivery.
Driver-agnostic — works with `pgx` **or** `lib/pq`, and forces neither on you.

## Features

- **Two Messaging Patterns**:
  - **Pub/Sub**: Fan-out messaging where each subscriber receives all messages
  - **Channels**: Point-to-point queuing where each message is delivered to a single consumer

- **At-Least-Once Delivery**: Visibility timeouts redeliver messages whose consumer crashed before acknowledging, so message handlers should be idempotent. Publishing with an explicit message ID deduplicates enqueues.
- **Three Consume APIs**: a handler-based loop (`ConsumeChannel`), a range-over-func iterator (`ChannelMessages`), and single-shot `ReceiveChannel` — pick the ergonomics you want.
- **Push Delivery**: optional `LISTEN/NOTIFY` listener wakes idle consumers in milliseconds, with a bounded safety-net poll as a fallback.
- **Automatic Retry Backoff**: nacked messages are retried with decorrelated-jitter exponential backoff; override per nack with `WithRetryDelay`.
- **Message Ordering**: per-topic/channel ordering using UUIDv7.
- **Dead Letter Queue**: failed messages moved after max retries; inspect with keyset-paginated `ListDLQMessages` / `DLQStats`.
- **Message Replay**: replay from a timestamp or the DLQ — transactional, deterministic, and memory-bounded for large backlogs.
- **Opt-in Observability**: zero-dependency `Tracer` / `MetricsRecorder` hooks; ready-made OpenTelemetry and Prometheus adapters ship as optional sub-modules.
- **Testable Without a Database**: the `fake` sub-package is an in-memory implementation of the published interfaces for unit tests with no Docker.
- **Garbage Collection**: opt-in background `GarbageCollector` reclaims old rows under configurable retention policies (see [Garbage collection](#garbage-collection)).
- **Statistics & Monitoring**: track message counts, processing times, and queue depth.

## Requirements

- PostgreSQL 18+
- Go 1.25+

Core depends only on the Go standard library and `github.com/google/uuid` — no
PostgreSQL driver is a mandatory dependency.

## Installation

```bash
go get github.com/sgaunet/pgqueue
```

## Quick Start

The constructor takes a `*sql.DB` and functional options. The driver is your
choice — `sql.Open("pgx", …)` or `sql.Open("postgres", …)` with `lib/pq`.

```go
package main

import (
    "context"
    "database/sql"
    "log"

    _ "github.com/jackc/pgx/v5/stdlib" // or _ "github.com/lib/pq"
    "github.com/sgaunet/pgqueue"
)

func main() {
    ctx := context.Background()

    db, err := sql.Open("pgx", "postgres://user:pass@localhost/dbname?sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Install / migrate the schema (one-time, idempotent).
    if err := pgqueue.InitSchema(ctx, db); err != nil {
        log.Fatal(err)
    }

    // Construct a Queue with functional options.
    q, err := pgqueue.New(ctx, db,
        pgqueue.WithMaxMessageSize(1<<20), // 1 MiB
        pgqueue.WithDefaultMaxRetries(3),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer q.Close() // stops the GC and the LISTEN/NOTIFY listener

    // Create a channel and publish.
    if err := q.CreateChannel(ctx, "orders"); err != nil {
        log.Fatal(err)
    }
    id, err := q.PublishChannel(ctx, "orders", []byte("order-123"))
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("published message: %s", id)
}
```

### Consuming — handler API (recommended)

`ConsumeChannel` owns the loop: it fetches messages, calls your handler, and
auto-acks on `nil` / auto-nacks (with backoff retry) on an error.

```go
err := q.ConsumeChannel(ctx, "orders", func(ctx context.Context, m *pgqueue.Message) error {
    return handleOrder(ctx, m.Payload) // nil -> ack; err -> nack + retry
}, pgqueue.WithConcurrency(8))
// returns cleanly when ctx is cancelled
```

Two lower-level styles are also available: the `ChannelMessages` range-over-func
iterator (manual ack/nack) and single-shot `ReceiveChannel` (returns
`ErrQueueEmpty` when nothing is available).

### Push delivery, observability, and testing

```go
// Push delivery: wake idle consumers in milliseconds via LISTEN/NOTIFY.
import "github.com/sgaunet/pgqueue/pglisten"
l, _ := pglisten.New(ctx, connString)
q, _ := pgqueue.New(ctx, db, pgqueue.WithListener(l))

// Observability: opt-in, zero core dependencies.
import "github.com/sgaunet/pgqueue/otelpgqueue"
q, _ = pgqueue.New(ctx, db,
    pgqueue.WithTracer(otelpgqueue.NewTracer(tracerProvider)),
    pgqueue.WithMetrics(otelpgqueue.NewMetrics(meterProvider)))

// Prometheus adapter: NewMetrics returns an error so a registration
// failure is surfaced rather than silently dropped.
import "github.com/sgaunet/pgqueue/prompgqueue"
m, err := prompgqueue.NewMetrics(prometheus.DefaultRegisterer)
if err != nil { /* handle */ }
q, _ = pgqueue.New(ctx, db, pgqueue.WithMetrics(m))

// Unit-test your code with no database via the in-memory fake.
import "github.com/sgaunet/pgqueue/fake"
q := fake.New()
```

> **Metric cardinality:** both observability adapters set a `queue` label/attribute
> to the queue or topic name. Keep the set of queue names bounded — a per-tenant or
> otherwise unbounded name set causes unbounded metric cardinality.

## Garbage collection

Garbage collection is **opt-in**. Delivery correctness does not depend on it —
a crashed consumer's message is still redelivered after the visibility timeout,
and a retry-exhausted message is still promoted to the DLQ, both on the consume
path. But *storage reclamation* (purging completed messages, expired DLQ
entries, acked subscription rows, orphaned topic messages) only happens if you
run a `GarbageCollector`:

```go
gc := pgqueue.NewGarbageCollector(q, pgqueue.GarbageCollectorConfig{
    Interval: 5 * time.Minute,
    DefaultPolicy: pgqueue.RetentionPolicy{
        CompletedMessageTTL: 24 * time.Hour,      // delete completed messages after 24h
        MaxPendingAge:       7 * 24 * time.Hour,  // drop deliveries pending longer than 7d
        DLQRetention:        30 * 24 * time.Hour, // delete DLQ entries after 30d
    },
    // Per-queue overrides: Policies map[string]RetentionPolicy{"orders": {...}}
})
gc.Start(ctx) // runs in the background until ctx is cancelled or q.Close()
```

> **Retention defaults.** `NewGarbageCollector` substitutes default retention
> (`CompletedMessageTTL` 24h, `DLQRetention` 30d) when `DefaultPolicy` is left
> empty, so a `GarbageCollector` created without a policy still bounds table
> growth. `MaxPendingAge` stays unbounded by default — pending messages are
> live, unprocessed data. A `DefaultPolicy` that sets even one field, and every
> per-queue `Policies` entry, is used as-is (there, a `0` field disables that
> cleanup). To run a `GarbageCollector` that keeps everything forever, set the
> fields explicitly to `pgqueue.KeepForever`.

`q.Close()` stops every `GarbageCollector` created for the queue (it
back-registers on construction), so you do not have to track them for shutdown.

## Architecture

- **Table-per-queue**: Each channel/topic has dedicated tables for isolation and performance. This design targets **tens to low hundreds of queues per database**; it is not suited for per-tenant/per-user queues at multi-tenant scale. See [ADR-002](ADR.md#adr-002-table-per-queue-architecture) for the ceiling and the linear-scaling operations (`GarbageCollector.Collect`, `ListChannels`/`ListTopics`, `GetUnhealthySubscribers`).
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
