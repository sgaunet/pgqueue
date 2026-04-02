# Pub/Sub Example

This example demonstrates fan-out messaging using pgqueue topics and subscriptions.

## What it does

- Initializes the base schema (pgqueue_metadata, pgqueue_subscribers, pgqueue_replay_log tables)
- Creates a "user-events" topic for broadcasting user activity
- Registers 3 subscribers: email-service, analytics-service, notification-service
- Publishes 5 user events to the topic
- Each subscriber processes all events independently (fan-out pattern)
- Shows per-subscriber statistics (lag, processed count)

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
   go mod tidy
   go run main.go
   ```

## Expected Output

```
Registering subscribers...
Registered: email-service
Registered: analytics-service
Registered: notification-service

Publishing user events...
Published: user.registered: user_id=1001, email=alice@example.com (ID: 019a3c...)
...

[email-service] Starting...
[analytics-service] Starting...
[notification-service] Starting...

[email-service] Processing: user.registered: user_id=1001, email=alice@example.com
[analytics-service] Processing: user.registered: user_id=1001, email=alice@example.com
[notification-service] Processing: user.registered: user_id=1001, email=alice@example.com

[email-service] Completed: user.registered: user_id=1001, email=alice@example.com
[analytics-service] Completed: user.registered: user_id=1001, email=alice@example.com
...

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
Note: Each subscriber processes the same events independently (fan-out pattern)
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
