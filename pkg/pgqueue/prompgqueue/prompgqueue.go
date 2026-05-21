// Package prompgqueue provides a Prometheus metrics adapter for pgqueue.
//
// It implements the pgqueue.MetricsRecorder hook interface so metrics can be
// enabled with pgqueue.WithMetrics. It is a separate Go module, so the
// Prometheus client dependency never enters the core pgqueue dependency graph
// (FR-019):
//
//	m := prompgqueue.NewMetrics(prometheus.DefaultRegisterer)
//	q, err := pgqueue.New(ctx, db, pgqueue.WithMetrics(m))
package prompgqueue

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sgaunet/pgqueue/pkg/pgqueue"
)

// queueLabel is the Prometheus label carrying the queue name.
const queueLabel = "queue"

// Metrics is a Prometheus-backed pgqueue.MetricsRecorder.
type Metrics struct {
	publishes  *prometheus.CounterVec
	consumeDur *prometheus.HistogramVec
	acks       *prometheus.CounterVec
	queueDepth *prometheus.GaugeVec
	dlqSize    *prometheus.GaugeVec
}

// compile-time check.
var _ pgqueue.MetricsRecorder = (*Metrics)(nil)

// NewMetrics builds the pgqueue Prometheus collectors and registers them with
// reg. A nil registerer falls back to prometheus.DefaultRegisterer. Collectors
// already registered (for example on a second call) are reused.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	return &Metrics{
		publishes: registerCollector(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgqueue_publish_messages_total",
			Help: "Messages published to pgqueue queues.",
		}, []string{queueLabel})),
		consumeDur: registerCollector(reg, prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "pgqueue_consume_duration_seconds",
			Help:    "Message processing latency in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{queueLabel})),
		acks: registerCollector(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgqueue_ack_total",
			Help: "Acknowledgement outcomes by queue and result.",
		}, []string{queueLabel, "result"})),
		queueDepth: registerCollector(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgqueue_queue_depth",
			Help: "Pending message count.",
		}, []string{queueLabel})),
		dlqSize: registerCollector(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgqueue_dlq_size",
			Help: "Dead-letter queue size.",
		}, []string{queueLabel})),
	}
}

// registerCollector registers c, returning the already-registered collector of
// the same type when one with an identical descriptor exists — so NewMetrics is
// safe to call more than once against the same registerer.
//
//nolint:ireturn // T is a concrete collector type at every call site; the
// generic constraint is only how the helper stays type-safe across them.
func registerCollector[T prometheus.Collector](reg prometheus.Registerer, c T) T {
	err := reg.Register(c)
	if err == nil {
		return c
	}
	var are prometheus.AlreadyRegisteredError
	if errors.As(err, &are) {
		if existing, ok := are.ExistingCollector.(T); ok {
			return existing
		}
	}
	return c
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

// ObserveQueueDepth records the current pending-message count for a queue.
func (m *Metrics) ObserveQueueDepth(queue string, depth int64) {
	m.queueDepth.WithLabelValues(queue).Set(float64(depth))
}

// ObserveDLQSize records the current dead-letter queue size for a queue.
func (m *Metrics) ObserveDLQSize(queue string, size int64) {
	m.dlqSize.WithLabelValues(queue).Set(float64(size))
}
