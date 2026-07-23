// Package prompgqueue provides a Prometheus metrics adapter for pgqueue.
//
// It implements the pgqueue.MetricsRecorder hook interface so metrics can be
// enabled with pgqueue.WithMetrics. It is a separate Go module, so the
// Prometheus client dependency never enters the core pgqueue dependency graph
// (FR-019):
//
//	m, err := prompgqueue.NewMetrics(prometheus.DefaultRegisterer)
//	if err != nil { ... }
//	q, err := pgqueue.New(ctx, db, pgqueue.WithMetrics(m))
//
// # Collectors
//
// NewMetrics registers the following collectors. Names are stable API: they
// are what dashboards and alerts key on, so they change only with a major
// version. Every collector carries a "queue" label holding the pgqueue queue
// or topic name; the collectors that carry more are called out below.
//
//   - pgqueue_publish_messages_total — CounterVec. Messages published (Publish
//     adds 1, PublishBatch adds the batch size). Labels: queue.
//   - pgqueue_handle_duration_seconds — HistogramVec, seconds. Handler-only
//     execution latency: it excludes queue wait, the receive round-trip, and
//     the ack round-trip. Buckets: DefaultLatencyBuckets, overridable with
//     WithLatencyBuckets. Labels: queue.
//   - pgqueue_delivery_latency_seconds — HistogramVec, seconds. The
//     publish-to-delivery interval: message creation to handler start, so it
//     includes the queue wait the handle histogram excludes. Always measured
//     from the original publish time, so redeliveries report cumulative time.
//     Buckets:
//     DefaultLatencyBuckets, overridable with WithLatencyBuckets.
//     Labels: queue.
//   - pgqueue_ack_total — CounterVec. Acknowledgement outcomes.
//     Labels: queue, result ("ack" or "nack"). See "Backend divergence" below.
//   - pgqueue_ack_after_expired_total — CounterVec. Receipts ack'd or nack'd
//     after their claim expired; those messages will be redelivered.
//     Labels: queue.
//   - pgqueue_queue_depth — GaugeVec. Consumable pending-message count. Set
//     only when the application calls Queue.Stats — nothing samples it in the
//     background, so a dashboard needs a periodic Stats call. Labels: queue.
//   - pgqueue_dlq_size — GaugeVec. Dead-letter queue size. Set on the same
//     Queue.Stats call as the depth gauge. Labels: queue.
//   - pgqueue_metadata_parse_errors_total — CounterVec. Messages whose JSON
//     metadata column could not be parsed; metadata is dropped and delivery
//     continues. Labels: queue.
//   - pgqueue_gc_runs_total — CounterVec. Garbage-collector passes.
//     Labels: queue, result ("ok" or "error").
//   - pgqueue_gc_duration_seconds — HistogramVec, seconds. Wall-clock duration
//     of one per-queue GC pass. Fixed buckets 0.001s–30s (not affected by
//     WithLatencyBuckets). Labels: queue (no result label).
//   - pgqueue_gc_reclaimed_total — CounterVec. Timed-out messages reset to
//     pending by the GC. Incremented only when the pass reclaimed at least one
//     row. Labels: queue.
//   - pgqueue_gc_purged_total — CounterVec. Rows deleted by the retention
//     policy. Incremented only when the pass purged at least one row.
//     Labels: queue.
//   - pgqueue_missed_notifications_total — CounterVec. LISTEN confirmations
//     that failed, meaning notifications were dropped until LISTEN was
//     re-confirmed; the safety-net poll still delivers. Labels: queue.
//
// Because these are label vectors, a series only appears once its queue has
// recorded its first observation; a queue that has never published shows no
// pgqueue_publish_messages_total sample at all.
//
// # Spans
//
// This adapter records metrics only. Tracing spans (pgqueue.publish,
// pgqueue.publish_batch, pgqueue.consume, pgqueue.ack, pgqueue.nack,
// pgqueue.extend, pgqueue.replay) are emitted through pgqueue.Tracer; see the
// otelpgqueue module for an implementation. The two are independent — a
// pgqueue.New call can register this metrics adapter and an OpenTelemetry
// tracer together.
//
// # Backend divergence: RecordAck
//
// The ack outcome is labelled differently by the two shipped adapters, so a
// query written against one does not port to the other unchanged:
//
//   - prompgqueue (this package) sets a string label on pgqueue_ack_total:
//     result="ack" or result="nack".
//   - otelpgqueue sets a boolean attribute on pgqueue.ack.total:
//     ack=true for an ack, ack=false for a nack.
//
// Everything else (metric semantics, the "queue" label, the gc "result"
// label) matches; only this one label differs.
//
// # Metric label cardinality
//
// Every metric exposed by this adapter carries a "queue" label set to the
// pgqueue queue or topic name. Prometheus creates a separate time series per
// distinct label value, so the set of queue names must stay bounded: a
// per-tenant or otherwise dynamically generated queue name will grow the time
// series count without limit and can overwhelm Prometheus. Keep queue names
// drawn from a small, fixed set (R-24). No metric carries the message ID, for
// the same reason.
//
// # MetricsRecorder context parameter
//
// pgqueue.MetricsRecorder methods take a context.Context first parameter,
// threaded through from the triggering operation. Prometheus's client_golang
// collectors are not context-aware (Inc/Add/Observe/Set take no context), so
// this adapter accepts and ignores it on every method.
package prompgqueue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sgaunet/pgqueue"
)

// queueLabel is the Prometheus label carrying the queue name.
const queueLabel = "queue"

// DefaultLatencyBuckets are the recommended histogram bucket boundaries for
// pgqueue's latency metrics (pgqueue_handle_duration_seconds and
// pgqueue_delivery_latency_seconds). Queue latency spans a much wider range
// than typical HTTP latency: sub-millisecond fast paths, the common 10ms–1s
// band, and multi-minute outliers during DLQ replay or large-fan-out delivery.
// These buckets cover that range while keeping cardinality reasonable.
//
// Operators who need different resolution (for example, a high-throughput
// queue where most jobs complete in under 10ms) can supply custom buckets
// via WithLatencyBuckets.
var DefaultLatencyBuckets = []float64{
	0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
}

// options holds optional configuration for NewMetrics.
type options struct {
	latencyBuckets []float64
}

// Option configures a Metrics adapter at construction time.
type Option func(*options)

// WithLatencyBuckets overrides the histogram bucket boundaries used for both
// latency metrics (pgqueue_handle_duration_seconds and
// pgqueue_delivery_latency_seconds). The supplied slice must be sorted in
// strictly ascending order; prometheus.NewHistogram will panic if it is not.
// When nil or empty the adapter falls back to DefaultLatencyBuckets. Example:
//
//	m, err := prompgqueue.NewMetrics(reg,
//	    prompgqueue.WithLatencyBuckets([]float64{0.001, 0.01, 0.1, 1, 10, 60}),
//	)
func WithLatencyBuckets(buckets []float64) Option {
	return func(o *options) {
		if len(buckets) > 0 {
			o.latencyBuckets = buckets
		}
	}
}

// Metrics is a Prometheus-backed pgqueue.MetricsRecorder.
type Metrics struct {
	publishes           *prometheus.CounterVec
	handleDur           *prometheus.HistogramVec
	deliveryLatency     *prometheus.HistogramVec
	acks                *prometheus.CounterVec
	ackAfterExpired     *prometheus.CounterVec
	queueDepth          *prometheus.GaugeVec
	dlqSize             *prometheus.GaugeVec
	metadataParseErrors *prometheus.CounterVec
	gcRuns              *prometheus.CounterVec
	gcDuration          *prometheus.HistogramVec
	gcReclaimed         *prometheus.CounterVec
	gcPurged            *prometheus.CounterVec
	missedNotifications *prometheus.CounterVec
}

// compile-time check.
var _ pgqueue.MetricsRecorder = (*Metrics)(nil)

// NewMetrics builds the pgqueue Prometheus collectors and registers them with
// reg. A nil registerer falls back to prometheus.DefaultRegisterer.
//
// A collector already registered with an identical descriptor (for example on
// a second NewMetrics call against the same registerer) is reused and is not
// an error. Any other registration failure is returned: the previous behavior
// of silently returning an unregistered collector meant the affected metric
// never scraped (R-17).
//
//nolint:cyclop,funlen // flat list of collector registrations; both counts track the number of metrics, not logic.
func NewMetrics(reg prometheus.Registerer, opts ...Option) (*Metrics, error) {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	cfg := &options{latencyBuckets: DefaultLatencyBuckets}
	for _, o := range opts {
		o(cfg)
	}
	m := &Metrics{}
	var err error

	if m.publishes, err = registerCollector(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pgqueue_publish_messages_total",
		Help: "Messages published to pgqueue queues.",
	}, []string{queueLabel})); err != nil {
		return nil, fmt.Errorf("prompgqueue: register publish counter: %w", err)
	}
	if m.handleDur, err = registerCollector(reg, prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "pgqueue_handle_duration_seconds",
		Help: "Handler-only execution latency in seconds (excludes queue wait, fetch, and ack); " +
			"see DefaultLatencyBuckets for the bucket layout.",
		Buckets: cfg.latencyBuckets,
	}, []string{queueLabel})); err != nil {
		return nil, fmt.Errorf("prompgqueue: register handle histogram: %w", err)
	}
	if m.deliveryLatency, err = registerCollector(reg, prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "pgqueue_delivery_latency_seconds",
		Help: "Publish-to-delivery latency in seconds: time from message creation to handler start; " +
			"see DefaultLatencyBuckets for the bucket layout.",
		Buckets: cfg.latencyBuckets,
	}, []string{queueLabel})); err != nil {
		return nil, fmt.Errorf("prompgqueue: register delivery-latency histogram: %w", err)
	}
	if m.acks, err = registerCollector(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pgqueue_ack_total",
		Help: "Acknowledgement outcomes by queue and result.",
	}, []string{queueLabel, "result"})); err != nil {
		return nil, fmt.Errorf("prompgqueue: register ack counter: %w", err)
	}
	if m.ackAfterExpired, err = registerCollector(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pgqueue_ack_after_expired_total",
		Help: "Receipts ack'd/nack'd after their claim expired or no longer matched; messages will redeliver.",
	}, []string{queueLabel})); err != nil {
		return nil, fmt.Errorf("prompgqueue: register ack-after-expired counter: %w", err)
	}
	if m.queueDepth, err = registerCollector(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgqueue_queue_depth",
		Help: "Pending message count.",
	}, []string{queueLabel})); err != nil {
		return nil, fmt.Errorf("prompgqueue: register queue-depth gauge: %w", err)
	}
	if m.dlqSize, err = registerCollector(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgqueue_dlq_size",
		Help: "Dead-letter queue size.",
	}, []string{queueLabel})); err != nil {
		return nil, fmt.Errorf("prompgqueue: register dlq-size gauge: %w", err)
	}
	if m.metadataParseErrors, err = registerCollector(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pgqueue_metadata_parse_errors_total",
		Help: "Messages whose JSON metadata column could not be parsed; metadata is dropped, delivery continues.",
	}, []string{queueLabel})); err != nil {
		return nil, fmt.Errorf("prompgqueue: register metadata-parse-error counter: %w", err)
	}
	if m.gcRuns, err = registerCollector(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pgqueue_gc_runs_total",
		Help: "Garbage-collector passes by queue and outcome (ok/error).",
	}, []string{queueLabel, "result"})); err != nil {
		return nil, fmt.Errorf("prompgqueue: register gc-runs counter: %w", err)
	}
	if m.gcDuration, err = registerCollector(reg, prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pgqueue_gc_duration_seconds",
		Help:    "Wall-clock duration of a per-queue garbage-collector pass.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30},
	}, []string{queueLabel})); err != nil {
		return nil, fmt.Errorf("prompgqueue: register gc-duration histogram: %w", err)
	}
	if m.gcReclaimed, err = registerCollector(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pgqueue_gc_reclaimed_total",
		Help: "Timed-out messages reset to pending (re-deliverable) by the garbage collector.",
	}, []string{queueLabel})); err != nil {
		return nil, fmt.Errorf("prompgqueue: register gc-reclaimed counter: %w", err)
	}
	if m.gcPurged, err = registerCollector(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pgqueue_gc_purged_total",
		Help: "Messages deleted by the retention policy during a garbage-collector pass.",
	}, []string{queueLabel})); err != nil {
		return nil, fmt.Errorf("prompgqueue: register gc-purged counter: %w", err)
	}
	if m.missedNotifications, err = registerCollector(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pgqueue_missed_notifications_total",
		Help: "LISTEN/NOTIFY confirmations that failed; notifications on this channel were dropped until re-confirmed.",
	}, []string{queueLabel})); err != nil {
		return nil, fmt.Errorf("prompgqueue: register missed-notifications counter: %w", err)
	}
	return m, nil
}

// registerCollector registers c. It returns the already-registered collector of
// the same type when one with an identical descriptor exists — so NewMetrics is
// safe to call more than once against the same registerer — and surfaces any
// other registration error to the caller.
//
// A descriptor collision against a *different* Go collector type (for example
// a caller pre-registered a Counter where pgqueue ships a CounterVec) is also
// surfaced as an error: silently returning the unregistered instance, as the
// previous behavior did, meant writes went to a dangling object while the
// already-registered metric stayed frozen at its initial values (issue #92).
// Callers in that situation must either drop the conflicting registration or
// hand pgqueue its own dedicated prometheus.Registerer.
//
// T is a concrete collector type at every call site; the generic constraint is
// only how the helper stays type-safe across them.
//
//nolint:ireturn // T is concrete at every call site; the constraint is for type safety.
func registerCollector[T prometheus.Collector](reg prometheus.Registerer, c T) (T, error) {
	err := reg.Register(c)
	if err == nil {
		return c, nil
	}
	var are prometheus.AlreadyRegisteredError
	if errors.As(err, &are) {
		if existing, ok := are.ExistingCollector.(T); ok {
			return existing, nil
		}
		return c, fmt.Errorf(
			"metric already registered as %T but pgqueue ships %T; "+
				"use a dedicated prometheus.Registerer for pgqueue: %w",
			are.ExistingCollector, c, err,
		)
	}
	return c, fmt.Errorf("register collector: %w", err)
}

// RecordPublish counts messages published to a queue. ctx is accepted to
// satisfy pgqueue.MetricsRecorder (see the package doc's ctx-parameter note)
// and ignored: Prometheus collectors are not context-aware.
func (m *Metrics) RecordPublish(_ context.Context, queue string, count int) {
	m.publishes.WithLabelValues(queue).Add(float64(count))
}

// RecordHandle observes one message's handler-only execution latency. ctx is
// accepted and ignored; see RecordPublish.
func (m *Metrics) RecordHandle(_ context.Context, queue string, latency time.Duration) {
	m.handleDur.WithLabelValues(queue).Observe(latency.Seconds())
}

// RecordDeliveryLatency observes one message's publish-to-delivery latency.
// ctx is accepted and ignored; see RecordPublish.
func (m *Metrics) RecordDeliveryLatency(_ context.Context, queue string, latency time.Duration) {
	m.deliveryLatency.WithLabelValues(queue).Observe(latency.Seconds())
}

// RecordAck counts an acknowledgement outcome (ack or nack) for a queue. ctx
// is accepted and ignored; see RecordPublish.
func (m *Metrics) RecordAck(_ context.Context, queue string, ok bool) {
	result := "nack"
	if ok {
		result = "ack"
	}
	m.acks.WithLabelValues(queue, result).Inc()
}

// RecordAckAfterExpired counts n receipts whose claims expired at ack/nack time
// — those messages will be redelivered. ctx is accepted and ignored; see
// RecordPublish.
func (m *Metrics) RecordAckAfterExpired(_ context.Context, queue string, n int) {
	m.ackAfterExpired.WithLabelValues(queue).Add(float64(n))
}

// ObserveQueueDepth records the current pending-message count for a queue.
// ctx is accepted and ignored; see RecordPublish.
func (m *Metrics) ObserveQueueDepth(_ context.Context, queue string, depth int64) {
	m.queueDepth.WithLabelValues(queue).Set(float64(depth))
}

// ObserveDLQSize records the current dead-letter queue size for a queue. ctx
// is accepted and ignored; see RecordPublish.
func (m *Metrics) ObserveDLQSize(_ context.Context, queue string, size int64) {
	m.dlqSize.WithLabelValues(queue).Set(float64(size))
}

// RecordMetadataParseError counts one corrupt-metadata event for queue. ctx is
// accepted and ignored; see RecordPublish.
func (m *Metrics) RecordMetadataParseError(_ context.Context, queue string) {
	m.metadataParseErrors.WithLabelValues(queue).Inc()
}

// RecordGCRun records the outcome of a single per-queue GC pass. It
// increments the gc-runs counter (labelled "ok" or "error"), observes the
// duration histogram, and adds reclaimed and purged rows to their respective
// counters. ctx is accepted and ignored; see RecordPublish.
func (m *Metrics) RecordGCRun(
	_ context.Context, queue string, duration time.Duration, reclaimed, purged int64, err error,
) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	m.gcRuns.WithLabelValues(queue, result).Inc()
	m.gcDuration.WithLabelValues(queue).Observe(duration.Seconds())
	if reclaimed > 0 {
		m.gcReclaimed.WithLabelValues(queue).Add(float64(reclaimed))
	}
	if purged > 0 {
		m.gcPurged.WithLabelValues(queue).Add(float64(purged))
	}
}

// RecordMissedNotification counts one LISTEN confirmation failure for queue,
// indicating that notifications on this channel were dropped until LISTEN was
// re-confirmed. ctx is accepted and ignored; see RecordPublish.
func (m *Metrics) RecordMissedNotification(_ context.Context, queue string) {
	m.missedNotifications.WithLabelValues(queue).Inc()
}
