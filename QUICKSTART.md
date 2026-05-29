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

### At-least-once delivery: make handlers idempotent

pgqueue guarantees **at-least-once** delivery, not exactly-once. The same
message can be handed to a consumer more than once, so **handlers must be
idempotent** — processing a message twice must produce the same end state as
processing it once.

A message is redelivered when:

- **The visibility timeout expires.** Consuming a channel message sets
  `status='processing'` and a `visibility_timeout` (default 30s). If the
  message is not acked before that deadline — a slow handler, a long GC pause,
  a lost database connection — the `GarbageCollector` resets it to `pending`
  and another consumer can pick it up. A late `Ack`/`Nack` then returns
  `ErrClaimExpired`.
- **The consumer crashes mid-handler.** A process that dies after fetching a
  message but before acking never commits the ack, so the message is reclaimed
  the same way once its timeout lapses.
- **A handler returns an error (nack).** The message is retried — with backoff —
  until `max_retries` is exhausted, after which it moves to the DLQ.

The handler may also have run partway and produced side effects (an email sent,
a row written) before the redelivery, so "exactly-once side effects" is your
responsibility, not the queue's. The standard technique is to key your work on
the immutable message ID (`msg.ID`) and make the write idempotent — for
example an UPSERT that no-ops on a duplicate:

```go
err := q.ConsumeChannel(ctx, "orders",
    func(ctx context.Context, msg *pgqueue.Message) error {
        // Idempotent side effect: a second delivery of the same msg.ID is a
        // no-op because the primary key already exists. ON CONFLICT DO NOTHING
        // makes the INSERT safe to replay.
        _, err := appDB.ExecContext(ctx,
            `INSERT INTO processed_orders (message_id, order_id, processed_at)
             VALUES ($1, $2, now())
             ON CONFLICT (message_id) DO NOTHING`,
            msg.ID, orderIDFrom(msg.Payload),
        )
        return err // nil -> ack; err -> nack + retry
    },
    pgqueue.WithConcurrency(8),
)
```

For work that cannot be expressed as a single idempotent statement (calling a
non-idempotent external API, say), record the message ID in a "seen" table
inside the same transaction as your side effect, and skip messages whose ID is
already present.

> **Testing the redelivery contract.** The in-memory `fake.Queue`
> (`github.com/sgaunet/pgqueue/fake`) deliberately does **not** model
> visibility-timeout reclamation: a claimed message stays claimed until it is
> acked or nacked, so it cannot exercise timeout-driven redelivery. Tests that
> must verify idempotency under redelivery should run against a real Queue on
> PostgreSQL — see the `internal/integration` suite for the redelivery tests.

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

> **An empty policy gets default retention.** `NewGarbageCollector` replaces an
> empty `DefaultPolicy` with default retention (`CompletedMessageTTL` 24h,
> `DLQRetention` 30d; `MaxPendingAge` stays unbounded — pending messages are
> live data), so a `GarbageCollector` created without a policy still bounds
> table growth. A `DefaultPolicy` with any field set, and every per-queue
> `Policies` entry, is used verbatim. Use `pgqueue.KeepForever` to keep a
> field's rows forever.

## Delivery model: polling vs push

By default, consume loops **poll**. An idle `ConsumeChannel` / `ReceiveChannel`
worker issues one query every poll interval (default 30s) to check for ready
work, even when the queue is empty. The aggregate query load is therefore
roughly:

```
empty-poll QPS ≈ (consumers × queues consumed) ÷ poll interval
```

That is fine for a handful of queues and consumers, but the cost scales with
**consumers × queues × poll frequency** — many consumers each watching many
queues at a short interval can keep the database busy with empty polls and add
up to a full poll interval of latency before a freshly published message is
picked up.

For high fan-in or low-latency workloads, register the optional
`LISTEN/NOTIFY` push adapter. A `NOTIFY` fired inside each publishing
transaction wakes the relevant idle consumer in milliseconds, and the poll
drops to a bounded **safety net** rather than the primary delivery path:

```go
import "github.com/sgaunet/pgqueue/pglisten"

l, err := pglisten.New(ctx, connString) // its own dedicated connection
if err != nil {
    log.Fatal(err)
}
q, err := pgqueue.New(ctx, db, pgqueue.WithListener(l))
```

You can also widen the poll interval with `WithPollInterval` (longer interval =
fewer empty polls, higher worst-case latency without a listener).

## Connection pool sizing

pgqueue runs all of its queries on the `*sql.DB` you pass to `New`, so size that
pool to the connections pgqueue actually holds concurrently:

- **Consume loops** — each `ConsumeChannel` / `ConsumeTopic` worker briefly
  holds **one** connection while it fetches-and-claims a message (a short
  `BEGIN … FOR UPDATE SKIP LOCKED … COMMIT`). A loop started with
  `WithConcurrency(n)` can hold up to `n` at once. An *idle* (polling) worker
  holds none between polls. Sum the concurrency across every consume loop.
- **The garbage collector** — a `Collect` pass fans queues out to a bounded
  worker pool, so it can hold up to `GarbageCollectorConfig.MaxWorkers`
  connections (default **10**) during a pass, and none between passes.
- **Publish / ack / admin calls** — each `Publish*`, `Ack`/`Nack`,
  `Create*`/`Delete*`, stats, and replay call uses one connection for its
  duration. Budget for your peak concurrent publishers/callers.
- **The optional `pglisten` listener** does **not** draw from this pool — it
  opens its own dedicated connection. (Don't forget it still counts against the
  server's `max_connections`.)

A practical starting formula:

```go
// Σ(consume-loop concurrency) + GC MaxWorkers + peak concurrent publishers + headroom
db.SetMaxOpenConns(maxOpen)
db.SetMaxIdleConns(maxOpen / 4) // keep ~25% warm to avoid reconnection churn
```

Concrete example — two consume loops at `WithConcurrency(8)` each, a GC at the
default `MaxWorkers` (10), and up to ~10 concurrent publishers:

```
8 + 8 + 10 + 10 = 36 → round up for headroom
db.SetMaxOpenConns(48)
db.SetMaxIdleConns(12)
```

Watch `db.Stats().WaitCount`: if it grows steadily, callers are blocking on the
pool — raise `MaxOpenConns` (and `max_connections` / PgBouncer accordingly).

## Configuration Options

Configuration is supplied through functional options, not config structs.

### Queue-wide options — passed to `pgqueue.New`

| Option | Purpose |
|--------|---------|
| `WithMaxMessageSize(bytes)` | Max payload size (default 256 KiB) |
| `WithDefaultMaxRetries(n)` | Delivery attempts before the DLQ (default 3; `0` = DLQ on first failure) |
| `WithDefaultTTL(d)` | Default message TTL (`0` = never expire) |
| `WithMaxQueues(n)` | Cap the total number of queues (`0` = unlimited). The table-per-queue design targets tens to low hundreds of queues per database — see [ADR-002](ADR.md#adr-002-table-per-queue-architecture). |
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
