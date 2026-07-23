# Channel Example

This example demonstrates point-to-point messaging using pgqueue channels.

## What it does

- Initializes the base schema (pgqueue_metadata, pgqueue_subscribers, pgqueue_replay_log,
  pgqueue_schema_version tables)
- Creates an "orders" channel for processing customer orders
- Publishes 5 example orders to the channel
- Consumes them with the handler-based `ConsumeChannel` loop, using 4 concurrent
  workers (`WithConcurrency(4)`) bounded to a 5-second window
- Demonstrates automatic acknowledgment: `ConsumeChannel` acks when the handler
  returns nil and nacks (feeding retry/backoff) when it returns an error
- Shows queue statistics

## Key Concepts

**Schema Initialization**: The `InitSchema()` function creates base tables required by pgqueue. It's idempotent (safe to call multiple times) and should be called once before creating any queues or topics.

**Point-to-Point**: Each message is delivered to exactly one consumer. If multiple consumers are running, they compete for messages (load balancing).

**Visibility Timeout**: When a message is consumed, it becomes invisible to other consumers for 30 seconds. If the consumer crashes before acknowledging, the message is redelivered once the timeout expires (at-least-once delivery), so handlers should be idempotent.

**Retry Logic**: Failed messages are automatically retried up to `MaxRetries` times before moving to the Dead Letter Queue (DLQ).

## Prerequisites

- Docker and Docker Compose (easiest option)
- OR PostgreSQL 18+ running locally

## Quick Start with Docker Compose

The easiest way to run this example:

```bash
cd examples/channel

# Start PostgreSQL in the background
docker-compose up -d

# Wait a few seconds for PostgreSQL to be ready
sleep 3

# Run the example
go run main.go

# Stop PostgreSQL when done
docker-compose down
```

## Running with Local PostgreSQL

If you have PostgreSQL running locally:

1. Create the database:
   ```bash
   createdb pgqueue_example
   ```

2. Run the example:
   ```bash
   cd examples/channel
   go run main.go
   ```

The connection string is hardcoded in `main.go`
(`postgres://postgres:postgres@localhost:5432/pgqueue_example?sslmode=disable`);
edit it if your PostgreSQL uses a different host, port, or credentials.

## Expected Output

All 5 orders are published first, then the consume loop drains them. Because the
loop runs 4 concurrent workers, the `Completed` lines may appear in any order.
The run takes about 6 seconds: ~1s publishing plus the fixed 5-second consume
window. A typical run looks like:

```
Publishing orders...
Published: order-001: 2x Widget A (ID: 019a3c...)
Published: order-002: 1x Widget B (ID: 019a3c...)
Published: order-003: 5x Widget C (ID: 019a3c...)
Published: order-004: 3x Widget A (ID: 019a3c...)
Published: order-005: 1x Widget D (ID: 019a3c...)

Processing orders...
Completed: order-001: 2x Widget A
Completed: order-002: 1x Widget B
Completed: order-003: 5x Widget C
Completed: order-004: 3x Widget A
Completed: order-005: 1x Widget D

Queue Statistics:
  Pending: 0
  Processing: 0
  Completed: 5
  DLQ: 0

Example completed successfully!
```

## Using lib/pq Driver

To use the lib/pq driver instead of pgx:

1. Uncomment the lib/pq import in `main.go`:
   ```go
   _ "github.com/lib/pq"
   ```

2. Change the connection string:
   ```go
   db, err := sql.Open("postgres", "postgres://postgres:postgres@localhost:5432/pgqueue_example?sslmode=disable")
   ```

3. Add lib/pq to dependencies:
   ```bash
   go get github.com/lib/pq
   ```
