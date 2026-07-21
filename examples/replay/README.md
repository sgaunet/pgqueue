# Replay & Dead-Letter Queue Example

This example demonstrates pgqueue's dead-letter queue (DLQ) and replay APIs.

## What it does

- Creates a "payments" channel with `WithQueueMaxRetries(0)`, so a failed
  message is dead-lettered after its single attempt.
- Publishes 3 payments and consumes them with a handler that always fails,
  driving every message into the DLQ.
- Inspects the DLQ with `ListDLQMessages` (payload, failure reason, retry count).
- Previews a replay with `ReplayDLQ` + `ReplayOptions{DryRun: true}` — this
  mutates nothing and reports how many rows *would* be replayed vs skipped.
- Performs the real `ReplayDLQ`, moving the messages back onto the channel.
- Consumes again with a handler that now succeeds, and prints final stats.

## Key concepts

**Dead-letter queue**: When a message exhausts its retry budget
(`retry_count + 1 > MaxRetries`), pgqueue moves it to the per-queue DLQ instead
of redelivering it forever. The payload, failure reason, and retry count are
preserved for inspection.

**Replay is execute-by-default**: `ReplayDLQ`, `ReplayFrom`, and `ReplayMessage`
perform the replay immediately. Set `ReplayOptions.DryRun = true` to preview the
outcome without mutating anything — always preview first when operating on
production data. See `DLQ_OPERATIONS.md` in the repository root for an
inspect-and-replay runbook.

## Quick start with Docker Compose

```bash
cd examples/replay

docker-compose up -d          # start PostgreSQL
sleep 3                        # wait for it to be ready
go run main.go                 # run the example
docker-compose down            # stop PostgreSQL when done
```

## Running with local PostgreSQL

```bash
createdb pgqueue_example
cd examples/replay
go run main.go
```
