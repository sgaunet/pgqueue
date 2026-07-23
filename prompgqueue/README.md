# prompgqueue

Prometheus metrics adapter for [pgqueue](../README.md).

It implements the `pgqueue.MetricsRecorder` hook interface. It ships as a
separate Go module so the `prometheus/client_golang` dependency stays out of
the core pgqueue dependency graph — consumers who do not want it never pull it
in.

## Requirements

- PostgreSQL 18+
- Go 1.25+

## Install

```bash
go get github.com/sgaunet/pgqueue/prompgqueue
```

## Wiring

`NewMetrics` returns an error, so a registration failure is surfaced instead of
leaving a collector that never scrapes. A `nil` registerer falls back to
`prometheus.DefaultRegisterer`.

```go
import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/sgaunet/pgqueue"
    "github.com/sgaunet/pgqueue/prompgqueue"
)

m, err := prompgqueue.NewMetrics(prometheus.DefaultRegisterer)
if err != nil {
    return err
}

q, err := pgqueue.New(ctx, db, pgqueue.WithMetrics(m))
```

Calling `NewMetrics` twice against the same registerer is safe: a collector
already registered with an identical descriptor is reused. A descriptor
collision against a *different* collector type is an error — give pgqueue its
own `prometheus.Registerer` in that case.

Latency histogram buckets default to `DefaultLatencyBuckets`
(1ms → 60s); override with `prompgqueue.WithLatencyBuckets([]float64{...})`,
which must be sorted strictly ascending.

## Collectors

Every collector carries a `queue` label (the queue or topic name).

| Name | Type | Extra labels |
| --- | --- | --- |
| `pgqueue_publish_messages_total` | CounterVec | |
| `pgqueue_handle_duration_seconds` | HistogramVec | |
| `pgqueue_delivery_latency_seconds` | HistogramVec | |
| `pgqueue_ack_total` | CounterVec | `result` (`ack`/`nack`) |
| `pgqueue_ack_after_expired_total` | CounterVec | |
| `pgqueue_queue_depth` | GaugeVec | |
| `pgqueue_dlq_size` | GaugeVec | |
| `pgqueue_metadata_parse_errors_total` | CounterVec | |
| `pgqueue_gc_runs_total` | CounterVec | `result` (`ok`/`error`) |
| `pgqueue_gc_duration_seconds` | HistogramVec | |
| `pgqueue_gc_reclaimed_total` | CounterVec | |
| `pgqueue_gc_purged_total` | CounterVec | |
| `pgqueue_missed_notifications_total` | CounterVec | |

Notes:

- `pgqueue_handle_duration_seconds` is handler time only — it excludes queue
  wait, the receive round-trip, and the ack round-trip.
  `pgqueue_delivery_latency_seconds` covers publish to handler start, so it
  includes the wait; it is always measured from the original publish, so
  redeliveries report cumulative time. Both use `DefaultLatencyBuckets`.
- `pgqueue_gc_duration_seconds` has its own fixed buckets (1ms → 30s) and is
  not affected by `WithLatencyBuckets`; it carries no `result` label.
- The two gauges are set **only when the application calls `Queue.Stats`**.
  Nothing samples them in the background — schedule a periodic `Stats` call if
  you want them on a dashboard.
- `pgqueue_gc_reclaimed_total` and `pgqueue_gc_purged_total` are incremented
  only when the pass moved at least one row.
- `pgqueue_missed_notifications_total` counts dropped `LISTEN/NOTIFY` wakes;
  delivery is still covered by the safety-net poll.
- These are label vectors, so a series only appears after its queue records its
  first observation.

## Spans

This adapter records metrics only. Tracing spans (`pgqueue.publish`,
`pgqueue.publish_batch`, `pgqueue.consume`, `pgqueue.ack`, `pgqueue.nack`,
`pgqueue.extend`, `pgqueue.replay`) go through `pgqueue.Tracer` — see the
[`otelpgqueue`](../otelpgqueue) module. The two are independent and can be
registered together on one `pgqueue.New` call.

## Backend divergence: ack outcome

The ack outcome is labelled differently by the two shipped metric adapters, so
a query does not port between them unchanged:

- **prompgqueue** — string label `result="ack"` / `result="nack"` on
  `pgqueue_ack_total`.
- **otelpgqueue** — boolean attribute `ack=true` / `ack=false` on
  `pgqueue.ack.total`.

Everything else matches; only this one label differs.

## Cardinality

`queue` is on every metric, and Prometheus creates one time series per distinct
label value. Keep queue names drawn from a small, fixed set — a per-tenant name
grows the series count without limit. No metric carries the message ID, for the
same reason.
