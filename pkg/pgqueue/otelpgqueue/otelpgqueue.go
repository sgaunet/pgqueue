// Package otelpgqueue provides OpenTelemetry tracing and metrics adapters for
// pgqueue.
//
// It implements the pgqueue.Tracer and pgqueue.MetricsRecorder hook interfaces
// so observability can be enabled with pgqueue.WithTracer and
// pgqueue.WithMetrics. It is a separate Go module, so the OpenTelemetry
// dependency never enters the core pgqueue dependency graph (FR-019):
//
//	q, err := pgqueue.New(ctx, db,
//	    pgqueue.WithTracer(otelpgqueue.NewTracer(tracerProvider)),
//	    pgqueue.WithMetrics(otelpgqueue.NewMetrics(meterProvider)),
//	)
//
// # Metric label cardinality
//
// Every metric records a "queue" attribute set to the pgqueue queue or topic
// name, and every span carries it too. Each distinct attribute value is a
// separate metric stream, so the set of queue names must stay bounded: a
// per-tenant or otherwise dynamically generated queue name grows the stream
// count without limit. Keep queue names drawn from a small, fixed set (R-24).
//
// # Deferred: MetricsRecorder context parameter
//
// Threading a context.Context through the pgqueue.MetricsRecorder interface
// methods (so metrics could carry exemplar/trace correlation) was evaluated and
// deferred — it is an interface-breaking change for every implementer and is
// not adopted here (R-23b). This adapter records metrics with
// context.Background until that interface change is approved separately.
package otelpgqueue

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/sgaunet/pgqueue/pkg/pgqueue"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// instrumentationName identifies this library to OpenTelemetry.
const instrumentationName = "github.com/sgaunet/pgqueue"

// Option configures a Metrics adapter at construction time.
type Option func(*Metrics)

// WithLogger attaches a structured logger. When set, OpenTelemetry
// instrument-creation failures are logged at WARN instead of being silently
// discarded (R-23). When nil (the default) the adapter is silent.
func WithLogger(logger *slog.Logger) Option {
	return func(m *Metrics) { m.logger = logger }
}

// Tracer adapts an OpenTelemetry tracer to the pgqueue.Tracer hook interface.
type Tracer struct {
	tracer trace.Tracer
}

// compile-time check.
var _ pgqueue.Tracer = (*Tracer)(nil)

// NewTracer builds a pgqueue.Tracer backed by tp. A nil provider falls back to
// the global OpenTelemetry tracer provider.
func NewTracer(tp trace.TracerProvider) *Tracer {
	if tp == nil {
		return &Tracer{tracer: tracenoop.NewTracerProvider().Tracer(instrumentationName)}
	}
	return &Tracer{tracer: tp.Tracer(instrumentationName)}
}

// StartSpan begins an OpenTelemetry span and wraps it as a pgqueue.Span.
//
//nolint:ireturn // pgqueue.Span is the hook interface this adapter implements;
// the span is intentionally returned for the caller (pgqueue) to End.
func (t *Tracer) StartSpan(
	ctx context.Context,
	name string,
	attrs ...pgqueue.Attr,
) (context.Context, pgqueue.Span) {
	ctx, span := t.tracer.Start(ctx, name, trace.WithAttributes(toKeyValues(attrs)...))
	// A misbehaving TracerProvider can return a nil span; substitute a no-op
	// span so the otelSpan wrapper cannot panic on End/SetError (R-23).
	if span == nil {
		_, span = tracenoop.NewTracerProvider().
			Tracer(instrumentationName).Start(ctx, name)
	}
	return ctx, &otelSpan{span: span}
}

// otelSpan adapts an OpenTelemetry span to pgqueue.Span.
type otelSpan struct {
	span trace.Span
}

func (s *otelSpan) End() { s.span.End() }

func (s *otelSpan) SetError(err error) {
	if err == nil {
		return
	}
	s.span.RecordError(err)
	s.span.SetStatus(codes.Error, err.Error())
}

func (s *otelSpan) SetAttr(attrs ...pgqueue.Attr) {
	s.span.SetAttributes(toKeyValues(attrs)...)
}

// toKeyValues converts pgqueue attributes to OpenTelemetry key/value pairs.
func toKeyValues(attrs []pgqueue.Attr) []attribute.KeyValue {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]attribute.KeyValue, 0, len(attrs))
	for _, a := range attrs {
		switch v := a.Value.(type) {
		case string:
			out = append(out, attribute.String(a.Key, v))
		case int64:
			out = append(out, attribute.Int64(a.Key, v))
		case int:
			out = append(out, attribute.Int(a.Key, v))
		case bool:
			out = append(out, attribute.Bool(a.Key, v))
		case float64:
			out = append(out, attribute.Float64(a.Key, v))
		default:
			// Fall back to a string rendering for unsupported value types.
			out = append(out, attribute.String(a.Key, "unsupported"))
		}
	}
	return out
}

// Metrics adapts OpenTelemetry instruments to the pgqueue.MetricsRecorder hook.
type Metrics struct {
	logger     *slog.Logger
	publishes  metric.Int64Counter
	consumeDur metric.Float64Histogram
	acks       metric.Int64Counter
	queueDepth metric.Int64Gauge
	dlqSize    metric.Int64Gauge
}

// compile-time check.
var _ pgqueue.MetricsRecorder = (*Metrics)(nil)

// NewMetrics builds a pgqueue.MetricsRecorder backed by mp. A nil provider
// falls back to a no-op meter.
//
// Instrument-creation errors are non-fatal — the affected metric is simply not
// recorded — but they are no longer silently discarded: pass WithLogger to have
// them logged once at WARN (R-23).
func NewMetrics(mp metric.MeterProvider, opts ...Option) *Metrics {
	if mp == nil {
		mp = metricnoop.NewMeterProvider()
	}
	m := &Metrics{}
	for _, o := range opts {
		o(m)
	}
	meter := mp.Meter(instrumentationName)

	var errs []error
	var err error
	if m.publishes, err = meter.Int64Counter("pgqueue.publish.messages",
		metric.WithDescription("Messages published to pgqueue queues")); err != nil {
		errs = append(errs, err)
	}
	if m.consumeDur, err = meter.Float64Histogram("pgqueue.consume.duration",
		metric.WithDescription("Message processing latency"),
		metric.WithUnit("s")); err != nil {
		errs = append(errs, err)
	}
	if m.acks, err = meter.Int64Counter("pgqueue.ack.total",
		metric.WithDescription("Acknowledgement outcomes (ack and nack)")); err != nil {
		errs = append(errs, err)
	}
	if m.queueDepth, err = meter.Int64Gauge("pgqueue.queue.depth",
		metric.WithDescription("Pending message count")); err != nil {
		errs = append(errs, err)
	}
	if m.dlqSize, err = meter.Int64Gauge("pgqueue.dlq.size",
		metric.WithDescription("Dead-letter queue size")); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 && m.logger != nil {
		m.logger.Warn("otelpgqueue: some metric instruments failed to create",
			"error", errors.Join(errs...))
	}
	return m
}

// RecordPublish counts messages published to a queue.
func (m *Metrics) RecordPublish(queue string, count int) {
	if m.publishes != nil {
		m.publishes.Add(context.Background(), int64(count), queueAttr(queue))
	}
}

// RecordConsume records one message's processing latency.
func (m *Metrics) RecordConsume(queue string, latency time.Duration) {
	if m.consumeDur != nil {
		m.consumeDur.Record(context.Background(), latency.Seconds(), queueAttr(queue))
	}
}

// RecordAck counts an acknowledgement outcome (ack or nack) for a queue.
func (m *Metrics) RecordAck(queue string, ok bool) {
	if m.acks != nil {
		m.acks.Add(context.Background(), 1,
			metric.WithAttributes(
				attribute.String("queue", queue),
				attribute.Bool("ack", ok),
			))
	}
}

// ObserveQueueDepth records the current pending-message count for a queue.
func (m *Metrics) ObserveQueueDepth(queue string, depth int64) {
	if m.queueDepth != nil {
		m.queueDepth.Record(context.Background(), depth, queueAttr(queue))
	}
}

// ObserveDLQSize records the current dead-letter queue size for a queue.
func (m *Metrics) ObserveDLQSize(queue string, size int64) {
	if m.dlqSize != nil {
		m.dlqSize.Record(context.Background(), size, queueAttr(queue))
	}
}

// queueAttr is the common single-attribute option keyed by queue name.
//
//nolint:ireturn // metric.MeasurementOption is the option type the OTel metric
// API requires; returning it is the only possible shape.
func queueAttr(queue string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("queue", queue))
}
