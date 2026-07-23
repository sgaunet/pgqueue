# otelpgqueue

OpenTelemetry tracing and metrics adapters for [pgqueue](../README.md).

It implements the `pgqueue.Tracer` and `pgqueue.MetricsRecorder` hook
interfaces. It ships as a separate Go module so the OpenTelemetry dependency
stays out of the core pgqueue dependency graph — consumers who do not want it
never pull it in.

## Requirements

- PostgreSQL 18+
- Go 1.25+

## Install

```bash
go get github.com/sgaunet/pgqueue/otelpgqueue
```

## Wiring

Neither constructor returns an error — an instrument that fails to build is
simply never recorded. Pass `WithLogger` to have those failures logged at WARN.

```go
import (
    "github.com/sgaunet/pgqueue"
    "github.com/sgaunet/pgqueue/otelpgqueue"
)

q, err := pgqueue.New(ctx, db,
    pgqueue.WithTracer(otelpgqueue.NewTracer(tracerProvider)),
    pgqueue.WithMetrics(otelpgqueue.NewMetrics(meterProvider,
        otelpgqueue.WithLogger(logger))),
)
```

A `nil` provider installs a no-op tracer/meter; it does **not** fall back to the
OpenTelemetry global provider. To use the global provider, pass
`otel.GetTracerProvider()` / `otel.GetMeterProvider()` explicitly.

Instrumentation scope name: `github.com/sgaunet/pgqueue`.

## Instruments

Every instrument carries a `queue` attribute (the queue or topic name).

| Name | Type | Unit | Extra attributes |
| --- | --- | --- | --- |
| `pgqueue.publish.messages` | Int64Counter | | |
| `pgqueue.handle.duration` | Float64Histogram | s | |
| `pgqueue.delivery.latency` | Float64Histogram | s | |
| `pgqueue.ack.total` | Int64Counter | | `ack` (bool) |
| `pgqueue.ack.after_expired` | Int64Counter | | |
| `pgqueue.queue.depth` | Int64Gauge | | |
| `pgqueue.dlq.size` | Int64Gauge | | |
| `pgqueue.metadata.parse_errors` | Int64Counter | | |
| `pgqueue.gc.runs` | Int64Counter | | `result` (`ok`/`error`) |
| `pgqueue.gc.duration` | Float64Histogram | s | |
| `pgqueue.gc.reclaimed` | Int64Counter | | |
| `pgqueue.gc.purged` | Int64Counter | | |
| `pgqueue.missed_notifications` | Int64Counter | | |

Notes:

- `handle.duration` is handler time only — it excludes queue wait, the receive
  round-trip, and the ack round-trip. `delivery.latency` covers publish to
  handler start, so it includes the wait; it is always measured from the
  original publish, so redeliveries report cumulative time.
- The two gauges are recorded **only when the application calls
  `Queue.Stats`**. Nothing samples them in the background.
- `gc.reclaimed` and `gc.purged` are added only when the pass moved at least
  one row; `gc.duration` carries no `result` attribute.
- `missed_notifications` counts dropped `LISTEN/NOTIFY` wakes; delivery is
  still covered by the safety-net poll.

## Spans

Span names come from the core module; this adapter records them. Errors set the
span status via `RecordError` + `codes.Error`.

| Span | Attributes |
| --- | --- |
| `pgqueue.publish` | `queue` |
| `pgqueue.publish_batch` | `queue` |
| `pgqueue.consume` | `queue`, `message_id` |
| `pgqueue.ack` | `queue`, `message_id` |
| `pgqueue.nack` | `queue`, `message_id` |
| `pgqueue.extend` | `queue`, `message_id` |
| `pgqueue.replay` | `queue`, `replay_type` (`timestamp`/`message_id`/`dlq`) |

## Backend divergence: ack outcome

The ack outcome is labelled differently by the two shipped metric adapters, so
a query does not port between them unchanged:

- **otelpgqueue** — boolean attribute `ack=true` / `ack=false` on
  `pgqueue.ack.total`.
- **prompgqueue** — string label `result="ack"` / `result="nack"` on
  `pgqueue_ack_total`.

Everything else matches; only this one attribute differs.

## Cardinality

`queue` is on every metric, and each distinct value is a separate metric
stream. Keep queue names drawn from a small, fixed set — a per-tenant name
grows the stream count without limit. `message_id` appears on spans only (they
are sampled, not aggregated), never on a metric.
