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
// # Metric label cardinality
//
// Every metric exposed by this adapter carries a "queue" label set to the
// pgqueue queue or topic name. Prometheus creates a separate time series per
// distinct label value, so the set of queue names must stay bounded: a
// per-tenant or otherwise dynamically generated queue name will grow the time
// series count without limit and can overwhelm Prometheus. Keep queue names
// drawn from a small, fixed set (R-24).
package prompgqueue

import (
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sgaunet/pgqueue"
)

// queueLabel is the Prometheus label carrying the queue name.
const queueLabel = "queue"

// Metrics is a Prometheus-backed pgqueue.MetricsRecorder.
type Metrics struct {
	publishes        *prometheus.CounterVec
	consumeDur       *prometheus.HistogramVec
	acks             *prometheus.CounterVec
	ackAfterExpired  *prometheus.CounterVec
	queueDepth       *prometheus.GaugeVec
	dlqSize          *prometheus.GaugeVec
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
func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	m := &Metrics{}
	var err error

	if m.publishes, err = registerCollector(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pgqueue_publish_messages_total",
		Help: "Messages published to pgqueue queues.",
	}, []string{queueLabel})); err != nil {
		return nil, fmt.Errorf("prompgqueue: register publish counter: %w", err)
	}
	if m.consumeDur, err = registerCollector(reg, prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pgqueue_consume_duration_seconds",
		Help:    "Message processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{queueLabel})); err != nil {
		return nil, fmt.Errorf("prompgqueue: register consume histogram: %w", err)
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
	return m, nil
}

// registerCollector registers c. It returns the already-registered collector of
// the same type when one with an identical descriptor exists — so NewMetrics is
// safe to call more than once against the same registerer — and surfaces any
// other registration error to the caller.
//
//nolint:ireturn // T is a concrete collector type at every call site; the
// generic constraint is only how the helper stays type-safe across them.
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
		// Already registered, but under a different Go type — reuse our
		// instance; the descriptors match so scraping still works.
		return c, nil
	}
	return c, fmt.Errorf("register collector: %w", err)
}

// RecordPublish counts messages published to a queue.
func (m *Metrics) RecordPublish(queue string, count int) {
	m.publishes.WithLabelValues(queue).Add(float64(count))
}

// RecordConsume observes one message's processing latency.
func (m *Metrics) RecordConsume(queue string, latency time.Duration) {
	m.consumeDur.WithLabelValues(queue).Observe(latency.Seconds())
}

// RecordAck counts an acknowledgement outcome (ack or nack) for a queue.
func (m *Metrics) RecordAck(queue string, ok bool) {
	result := "nack"
	if ok {
		result = "ack"
	}
	m.acks.WithLabelValues(queue, result).Inc()
}

// RecordAckAfterExpired counts one receipt whose claim no longer matched at
// ack/nack time — the message will be redelivered.
func (m *Metrics) RecordAckAfterExpired(queue string) {
	m.ackAfterExpired.WithLabelValues(queue).Inc()
}

// ObserveQueueDepth records the current pending-message count for a queue.
func (m *Metrics) ObserveQueueDepth(queue string, depth int64) {
	m.queueDepth.WithLabelValues(queue).Set(float64(depth))
}

// ObserveDLQSize records the current dead-letter queue size for a queue.
func (m *Metrics) ObserveDLQSize(queue string, size int64) {
	m.dlqSize.WithLabelValues(queue).Set(float64(size))
}
