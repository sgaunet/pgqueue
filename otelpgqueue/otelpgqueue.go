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
// # Instrumentation scope
//
// Every instrument and span is emitted under the instrumentation scope name
// "github.com/sgaunet/pgqueue". Use it to filter pgqueue telemetry apart from
// the rest of the application's.
//
// # Instruments
//
// NewMetrics creates the following instruments. Names are stable API: they are
// what dashboards and alerts key on, so they change only with a major version.
// Every instrument carries a "queue" attribute holding the pgqueue queue or
// topic name; the instruments that carry more are called out below.
//
//   - pgqueue.publish.messages — Int64Counter. Messages published (Publish
//     adds 1, PublishBatch adds the batch size). Attributes: queue.
//   - pgqueue.handle.duration — Float64Histogram, unit "s". Handler-only
//     execution latency: it excludes queue wait, the receive round-trip, and
//     the ack round-trip. Attributes: queue.
//   - pgqueue.delivery.latency — Float64Histogram, unit "s". The
//     publish-to-delivery interval: message creation to handler start, so it
//     includes the queue wait handle.duration excludes. Always measured from
//     the original publish time, so redeliveries report cumulative time.
//     Attributes: queue.
//   - pgqueue.ack.total — Int64Counter. Acknowledgement outcomes.
//     Attributes: queue, ack (bool — true for an ack, false for a nack).
//     See "Backend divergence" below.
//   - pgqueue.ack.after_expired — Int64Counter. Receipts ack'd or nack'd after
//     their claim expired; those messages will be redelivered. Attributes:
//     queue.
//   - pgqueue.queue.depth — Int64Gauge. Consumable pending-message count.
//     Recorded only when the application calls Queue.Stats — nothing samples
//     it in the background, so a dashboard needs a periodic Stats call.
//     Attributes: queue.
//   - pgqueue.dlq.size — Int64Gauge. Dead-letter queue size. Recorded on the
//     same Queue.Stats call as queue.depth. Attributes: queue.
//   - pgqueue.metadata.parse_errors — Int64Counter. Messages whose JSON
//     metadata column could not be parsed; metadata is dropped and delivery
//     continues. Attributes: queue.
//   - pgqueue.gc.runs — Int64Counter. Garbage-collector passes.
//     Attributes: queue, result ("ok" or "error").
//   - pgqueue.gc.duration — Float64Histogram, unit "s". Wall-clock duration of
//     one per-queue GC pass. Attributes: queue (no result attribute).
//   - pgqueue.gc.reclaimed — Int64Counter. Timed-out messages reset to pending
//     by the GC. Added only when the pass reclaimed at least one row.
//     Attributes: queue.
//   - pgqueue.gc.purged — Int64Counter. Rows deleted by the retention policy.
//     Added only when the pass purged at least one row. Attributes: queue.
//   - pgqueue.missed_notifications — Int64Counter. LISTEN confirmations that
//     failed, meaning notifications were dropped until LISTEN was
//     re-confirmed; the safety-net poll still delivers. Attributes: queue.
//
// Instrument creation is best-effort: an instrument that fails to build is
// simply never recorded. Pass WithLogger to have those failures logged.
//
// # Spans
//
// Span names are chosen by the core pgqueue module and recorded through
// Tracer.StartSpan. A span's error status is set from the operation's error
// (via Span.SetError, which calls RecordError and sets codes.Error).
//
//   - pgqueue.publish — Publish. Attributes: queue.
//   - pgqueue.publish_batch — PublishBatch. Attributes: queue.
//   - pgqueue.consume — one handler invocation. Attributes: queue,
//     message_id.
//   - pgqueue.ack — Attributes: queue, message_id.
//   - pgqueue.nack — Attributes: queue, message_id.
//   - pgqueue.extend — visibility-timeout extension. Attributes: queue,
//     message_id.
//   - pgqueue.replay — ReplayFrom, ReplayMessage, and ReplayDLQ. Attributes:
//     queue, replay_type ("timestamp", "message_id", or "dlq").
//
// # Backend divergence: RecordAck
//
// The ack outcome is labelled differently by the two shipped adapters, so a
// query written against one does not port to the other unchanged:
//
//   - otelpgqueue (this package) sets a boolean attribute:
//     ack=true for an ack, ack=false for a nack.
//   - prompgqueue sets a string label on pgqueue_ack_total:
//     result="ack" or result="nack".
//
// Everything else (metric semantics, the "queue" attribute, the gc "result"
// attribute) matches; only this one attribute differs.
//
// # Metric label cardinality
//
// Every metric records a "queue" attribute set to the pgqueue queue or topic
// name, and every span carries it too. Each distinct attribute value is a
// separate metric stream, so the set of queue names must stay bounded: a
// per-tenant or otherwise dynamically generated queue name grows the stream
// count without limit. Keep queue names drawn from a small, fixed set (R-24).
//
// The message_id span attribute is high-cardinality by nature. That is fine
// for spans (which are sampled and not aggregated into time series) but it is
// why no metric carries it.
//
// # MetricsRecorder context parameter (R-23b)
//
// pgqueue.MetricsRecorder methods take a context.Context first parameter,
// threaded through from the triggering operation (publish, consume, GC pass,
// …). This adapter forwards that real ctx to every OpenTelemetry instrument
// call instead of context.Background(), so a meter provider that supports
// exemplars can correlate a metric observation with the in-flight trace.
// Both pgqueue.Tracer and pgqueue.MetricsRecorder carry ctx as of their first
// released version, so no interface-breaking change is pending here.
package otelpgqueue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"time"

	"github.com/sgaunet/pgqueue"
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

// TracerOption configures a Tracer adapter at construction time.
type TracerOption func(*Tracer)

// WithTracerLogger attaches a structured logger to the Tracer. When set, any
// pgqueue.Attr whose value falls through the type switch to fmt.Sprintf is
// logged at WARN so the unexpected type is not silently coerced (issue #93).
// When nil (the default) the Tracer is silent.
func WithTracerLogger(logger *slog.Logger) TracerOption {
	return func(t *Tracer) { t.logger = logger }
}

// Tracer adapts an OpenTelemetry tracer to the pgqueue.Tracer hook interface.
type Tracer struct {
	tracer trace.Tracer
	logger *slog.Logger
}

// compile-time check.
var _ pgqueue.Tracer = (*Tracer)(nil)

// NewTracer builds a pgqueue.Tracer backed by tp. A nil provider installs a
// no-op tracer (it does NOT fall back to the OpenTelemetry global provider); to
// wire the global provider, pass otel.GetTracerProvider() explicitly.
func NewTracer(tp trace.TracerProvider, opts ...TracerOption) *Tracer {
	t := &Tracer{}
	if tp == nil {
		t.tracer = tracenoop.NewTracerProvider().Tracer(instrumentationName)
	} else {
		t.tracer = tp.Tracer(instrumentationName)
	}
	for _, o := range opts {
		o(t)
	}
	return t
}

// StartSpan begins an OpenTelemetry span and wraps it as a pgqueue.Span.
// Returning pgqueue.Span is intentional — the caller (pgqueue core) ends it.
//
//nolint:ireturn // pgqueue.Span is the hook interface this adapter implements.
func (t *Tracer) StartSpan(
	ctx context.Context,
	name string,
	attrs ...pgqueue.Attr,
) (context.Context, pgqueue.Span) {
	ctx, span := t.tracer.Start(ctx, name, trace.WithAttributes(toKeyValues(attrs, t.logger)...))
	// A misbehaving TracerProvider can return a nil span; substitute a no-op
	// span so the otelSpan wrapper cannot panic on End/SetError (R-23).
	if span == nil {
		_, span = tracenoop.NewTracerProvider().
			Tracer(instrumentationName).Start(ctx, name)
	}
	return ctx, &otelSpan{span: span, logger: t.logger}
}

// otelSpan adapts an OpenTelemetry span to pgqueue.Span.
type otelSpan struct {
	span   trace.Span
	logger *slog.Logger
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
	s.span.SetAttributes(toKeyValues(attrs, s.logger)...)
}

// toKeyValues converts pgqueue attributes to OpenTelemetry key/value pairs.
// The set of natively recognised types is documented on pgqueue.Attr; any
// other value type falls back to fmt.Sprintf("%v", v) and, when logger is
// non-nil, is logged once at WARN so the coercion is not silent (issue #93).
func toKeyValues(attrs []pgqueue.Attr, logger *slog.Logger) []attribute.KeyValue {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]attribute.KeyValue, 0, len(attrs))
	for _, a := range attrs {
		out = append(out, toKeyValue(a, logger))
	}
	return out
}

// toKeyValue converts a single pgqueue.Attr to an attribute.KeyValue. It is a
// flat type-switch dispatch: the complexity is inherent to the number of
// supported scalar types and carries no nested branching beyond the two uint
// overflow checks, so it is suppressed rather than fragmented into artificial
// per-type-family helpers that would only obscure the dispatch.
//
//nolint:cyclop,funlen // exhaustive type-switch dispatch; width is case count, not nesting
func toKeyValue(a pgqueue.Attr, logger *slog.Logger) attribute.KeyValue {
	switch v := a.Value.(type) {
	case string:
		return attribute.String(a.Key, v)
	case bool:
		return attribute.Bool(a.Key, v)
	case int:
		return attribute.Int(a.Key, v)
	case int8:
		return attribute.Int64(a.Key, int64(v))
	case int16:
		return attribute.Int64(a.Key, int64(v))
	case int32:
		return attribute.Int64(a.Key, int64(v))
	case int64:
		return attribute.Int64(a.Key, v)
	case uint8:
		return attribute.Int64(a.Key, int64(v))
	case uint16:
		return attribute.Int64(a.Key, int64(v))
	case uint32:
		return attribute.Int64(a.Key, int64(v))
	case uint:
		// uint can be 64-bit on this platform; values past MaxInt64 cannot fit
		// in an OTel Int64 attribute, so render them as a decimal string to
		// keep the high bit instead of silently truncating.
		if uint64(v) > math.MaxInt64 {
			return attribute.String(a.Key, strconv.FormatUint(uint64(v), 10))
		}
		return attribute.Int64(a.Key, int64(v))
	case uint64:
		if v > math.MaxInt64 {
			return attribute.String(a.Key, strconv.FormatUint(v, 10))
		}
		return attribute.Int64(a.Key, int64(v))
	case float32:
		return attribute.Float64(a.Key, float64(v))
	case float64:
		return attribute.Float64(a.Key, v)
	case time.Duration:
		return attribute.Int64(a.Key, v.Nanoseconds())
	case time.Time:
		return attribute.String(a.Key, v.Format(time.RFC3339Nano))
	case error:
		return attribute.String(a.Key, v.Error())
	case fmt.Stringer:
		return attribute.String(a.Key, v.String())
	default:
		if logger != nil {
			logger.Warn("otelpgqueue: unsupported attribute value type, coerced via fmt.Sprintf",
				"key", a.Key, "type", fmt.Sprintf("%T", a.Value))
		}
		return attribute.String(a.Key, fmt.Sprintf("%v", a.Value))
	}
}

// Metrics adapts OpenTelemetry instruments to the pgqueue.MetricsRecorder hook.
type Metrics struct {
	logger              *slog.Logger
	publishes           metric.Int64Counter
	handleDur           metric.Float64Histogram
	deliveryLatency     metric.Float64Histogram
	acks                metric.Int64Counter
	ackAfterExpired     metric.Int64Counter
	queueDepth          metric.Int64Gauge
	dlqSize             metric.Int64Gauge
	metadataParseErrors metric.Int64Counter
	gcRuns              metric.Int64Counter
	gcDuration          metric.Float64Histogram
	gcReclaimed         metric.Int64Counter
	gcPurged            metric.Int64Counter
	missedNotifications metric.Int64Counter
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
	errs := m.buildInstruments(meter)
	if len(errs) > 0 && m.logger != nil {
		m.logger.Warn("otelpgqueue: some metric instruments failed to create",
			"error", errors.Join(errs...))
	}
	return m
}

// RecordPublish counts messages published to a queue.
func (m *Metrics) RecordPublish(ctx context.Context, queue string, count int) {
	if m.publishes != nil {
		m.publishes.Add(ctx, int64(count), queueAttr(queue))
	}
}

// RecordHandle records one message's handler-only execution latency.
func (m *Metrics) RecordHandle(ctx context.Context, queue string, latency time.Duration) {
	if m.handleDur != nil {
		m.handleDur.Record(ctx, latency.Seconds(), queueAttr(queue))
	}
}

// RecordDeliveryLatency records one message's publish-to-delivery latency.
func (m *Metrics) RecordDeliveryLatency(ctx context.Context, queue string, latency time.Duration) {
	if m.deliveryLatency != nil {
		m.deliveryLatency.Record(ctx, latency.Seconds(), queueAttr(queue))
	}
}

// RecordAck counts an acknowledgement outcome (ack or nack) for a queue.
func (m *Metrics) RecordAck(ctx context.Context, queue string, ok bool) {
	if m.acks != nil {
		m.acks.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("queue", queue),
				attribute.Bool("ack", ok),
			))
	}
}

// RecordAckAfterExpired counts n receipts whose claims expired at ack/nack time
// — those messages will be redelivered.
func (m *Metrics) RecordAckAfterExpired(ctx context.Context, queue string, n int) {
	if m.ackAfterExpired != nil {
		m.ackAfterExpired.Add(ctx, int64(n), queueAttr(queue))
	}
}

// ObserveQueueDepth records the current pending-message count for a queue.
func (m *Metrics) ObserveQueueDepth(ctx context.Context, queue string, depth int64) {
	if m.queueDepth != nil {
		m.queueDepth.Record(ctx, depth, queueAttr(queue))
	}
}

// ObserveDLQSize records the current dead-letter queue size for a queue.
func (m *Metrics) ObserveDLQSize(ctx context.Context, queue string, size int64) {
	if m.dlqSize != nil {
		m.dlqSize.Record(ctx, size, queueAttr(queue))
	}
}

// RecordMetadataParseError counts one corrupt-metadata event for queue.
func (m *Metrics) RecordMetadataParseError(ctx context.Context, queue string) {
	if m.metadataParseErrors != nil {
		m.metadataParseErrors.Add(ctx, 1, queueAttr(queue))
	}
}

// RecordGCRun records the outcome of a single per-queue GC pass. It
// increments the gc-runs counter (with an "ok"/"error" result attribute),
// observes the duration histogram, and adds reclaimed and purged row counts
// to their respective counters.
func (m *Metrics) RecordGCRun(
	ctx context.Context, queue string, duration time.Duration, reclaimed, purged int64, err error,
) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	attrs := metric.WithAttributes(
		attribute.String("queue", queue),
		attribute.String("result", result),
	)
	if m.gcRuns != nil {
		m.gcRuns.Add(ctx, 1, attrs)
	}
	if m.gcDuration != nil {
		m.gcDuration.Record(ctx, duration.Seconds(), queueAttr(queue))
	}
	if m.gcReclaimed != nil && reclaimed > 0 {
		m.gcReclaimed.Add(ctx, reclaimed, queueAttr(queue))
	}
	if m.gcPurged != nil && purged > 0 {
		m.gcPurged.Add(ctx, purged, queueAttr(queue))
	}
}

// RecordMissedNotification counts one LISTEN confirmation failure for queue,
// indicating that notifications on this channel were dropped until LISTEN was
// re-confirmed.
func (m *Metrics) RecordMissedNotification(ctx context.Context, queue string) {
	if m.missedNotifications != nil {
		m.missedNotifications.Add(ctx, 1, queueAttr(queue))
	}
}

// buildInstruments creates every metric instrument on m, returning the errors
// from any individual instrument that failed. Split out of NewMetrics to keep
// each function's cyclomatic complexity in check (a single new instrument
// otherwise pushes NewMetrics past the lint threshold). The cyclomatic count
// and length both track the number of instruments, not branching logic.
//
//nolint:cyclop,funlen // Linear list of instrument constructions.
func (m *Metrics) buildInstruments(meter metric.Meter) []error {
	var errs []error
	var err error
	if m.publishes, err = meter.Int64Counter("pgqueue.publish.messages",
		metric.WithDescription("Messages published to pgqueue queues")); err != nil {
		errs = append(errs, err)
	}
	if m.handleDur, err = meter.Float64Histogram("pgqueue.handle.duration",
		metric.WithDescription("Handler-only execution latency (excludes queue wait, fetch, and ack)"),
		metric.WithUnit("s")); err != nil {
		errs = append(errs, err)
	}
	if m.deliveryLatency, err = meter.Float64Histogram("pgqueue.delivery.latency",
		metric.WithDescription("Publish-to-delivery latency: time from message creation to handler start"),
		metric.WithUnit("s")); err != nil {
		errs = append(errs, err)
	}
	if m.acks, err = meter.Int64Counter("pgqueue.ack.total",
		metric.WithDescription("Acknowledgement outcomes (ack and nack)")); err != nil {
		errs = append(errs, err)
	}
	if m.ackAfterExpired, err = meter.Int64Counter("pgqueue.ack.after_expired",
		metric.WithDescription(
			"Receipts ack'd/nack'd after their claim expired or no longer matched; messages will redeliver",
		)); err != nil {
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
	if m.metadataParseErrors, err = meter.Int64Counter("pgqueue.metadata.parse_errors",
		metric.WithDescription(
			"Messages whose JSON metadata column could not be parsed; metadata is dropped, delivery continues",
		)); err != nil {
		errs = append(errs, err)
	}
	if m.gcRuns, err = meter.Int64Counter("pgqueue.gc.runs",
		metric.WithDescription("Garbage-collector passes by queue and outcome")); err != nil {
		errs = append(errs, err)
	}
	if m.gcDuration, err = meter.Float64Histogram("pgqueue.gc.duration",
		metric.WithDescription("Wall-clock duration of a per-queue garbage-collector pass"),
		metric.WithUnit("s")); err != nil {
		errs = append(errs, err)
	}
	if m.gcReclaimed, err = meter.Int64Counter("pgqueue.gc.reclaimed",
		metric.WithDescription("Timed-out messages reset to pending by the garbage collector")); err != nil {
		errs = append(errs, err)
	}
	if m.gcPurged, err = meter.Int64Counter("pgqueue.gc.purged",
		metric.WithDescription("Messages deleted by the retention policy during a GC pass")); err != nil {
		errs = append(errs, err)
	}
	if m.missedNotifications, err = meter.Int64Counter("pgqueue.missed_notifications",
		metric.WithDescription(
			"LISTEN/NOTIFY confirmations that failed; notifications dropped until re-confirmed",
		)); err != nil {
		errs = append(errs, err)
	}
	return errs
}

// queueAttr is the common single-attribute option keyed by queue name.
// metric.MeasurementOption is the option type the OTel metric API requires;
// returning it is the only possible shape.
//
//nolint:ireturn // Returning the OTel-mandated interface type.
func queueAttr(queue string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("queue", queue))
}
