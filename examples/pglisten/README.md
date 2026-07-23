# Push Delivery Example (pglisten adapter)

This example demonstrates push-based delivery using the optional `pglisten`
adapter, which wires PostgreSQL `LISTEN`/`NOTIFY` into pgqueue.

## What it does

- Creates a `pglisten.Listener` (a pgx-backed `pgqueue.Listener`) and registers
  it with `pgqueue.New(ctx, db, pgqueue.WithListener(listener))`.
- Starts a consumer that blocks waiting for work.
- Publishes one message and measures how quickly the consumer wakes.

With the listener registered, a `NOTIFY` emitted inside the publishing
transaction wakes the blocked consumer almost immediately, instead of waiting up
to one safety-net poll interval.

## Key concepts

**Optional adapter, isolated dependency**: `pglisten` is a separate Go module, so
its `pgx` dependency stays out of the core pgqueue dependency graph. Only programs
that want push delivery import it.

**Push is an optimization, not a correctness requirement**: the safety-net poll
still runs, so delivery is guaranteed even if a `NOTIFY` is missed (for example
during a listener reconnect). Push simply lowers latency.

**Lifecycle**: `WithListener` hands the listener's lifecycle to the Queue —
`pq.Close()` closes the listener and its dedicated connection too.

## Prerequisites

- Docker and Docker Compose (easiest option)
- OR PostgreSQL 18+ running locally

## Quick start with Docker Compose

```bash
cd examples/pglisten

docker-compose up -d          # start PostgreSQL
sleep 3                        # wait for it to be ready
go run main.go                 # run the example
docker-compose down            # stop PostgreSQL when done
```

## Running with local PostgreSQL

```bash
createdb pgqueue_example
cd examples/pglisten
go run main.go
```

The connection string is hardcoded in `main.go` as the `connString` constant
(`postgres://postgres:postgres@localhost:5432/pgqueue_example?sslmode=disable`)
and is used for both the `database/sql` handle and the `pglisten` listener's
dedicated connection; edit it if your PostgreSQL differs.
