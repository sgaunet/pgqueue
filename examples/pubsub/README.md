# Pub/Sub Example

This example demonstrates fan-out messaging using pgqueue topics and subscriptions.

## What it does

- Initializes the base schema (pgqueue_metadata, pgqueue_subscribers, pgqueue_replay_log,
  pgqueue_schema_version tables)
- Creates a "user-events" topic for broadcasting user activity
- Registers 3 subscribers: email-service, analytics-service, notification-service
- Starts one handler-based `ConsumeTopic` loop per subscriber, bounded to a
  6-second window, then publishes 5 user events to the topic
- Each subscriber processes all events independently (fan-out pattern)
- Shows per-subscriber lag (pending, processing, acknowledged counts)

## Key Concepts

**Schema Initialization**: The `InitSchema()` function creates base tables required by pgqueue. It's idempotent (safe to call multiple times) and should be called once before creating any queues or topics.

**Fan-Out**: Each published message is delivered to ALL subscribers. Each subscriber maintains its own processing state.

**Independent Processing**: Subscribers process messages at their own pace. One slow subscriber doesn't block others.

**Per-Subscriber Acknowledgment**: Each subscriber must acknowledge messages independently. A message is only marked complete when all subscribers have processed it.

**Subscriber Lag**: Track how far behind each subscriber is (pending vs. processed messages).

## Prerequisites

- Docker and Docker Compose (easiest option)
- OR PostgreSQL 18+ running locally

## Quick Start with Docker Compose

The easiest way to run this example:

```bash
cd examples/pubsub

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
   cd examples/pubsub
   go run main.go
   ```

The connection string is hardcoded in `main.go`
(`postgres://postgres:postgres@localhost:5432/pgqueue_example?sslmode=disable`);
edit it if your PostgreSQL uses a different host, port, or credentials.

## Expected Output

The three consume loops start before publishing begins, so the `completed` lines
interleave with the `Published` lines. Line order across subscribers is
nondeterministic. A typical run looks like:

```
Registering subscribers...
Registered: email-service
Registered: analytics-service
Registered: notification-service
[email-service] starting...
[analytics-service] starting...
[notification-service] starting...

Publishing user events...
Published: user.registered: user_id=1001, email=alice@example.com (ID: 019a3c...)
[email-service] completed: user.registered: user_id=1001, email=alice@example.com
[analytics-service] completed: user.registered: user_id=1001, email=alice@example.com
[notification-service] completed: user.registered: user_id=1001, email=alice@example.com
Published: user.registered: user_id=1002, email=bob@example.com (ID: 019a3c...)
...

[email-service] shutting down
[analytics-service] shutting down
[notification-service] shutting down

Subscriber Statistics:
  email-service:
    Pending: 0
    Processing: 0
    Acknowledged: 5
  analytics-service:
    Pending: 0
    Processing: 0
    Acknowledged: 5
  notification-service:
    Pending: 0
    Processing: 0
    Acknowledged: 5

Example completed successfully!
Note: each subscriber processed every event independently (fan-out).
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
