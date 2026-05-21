package pgqueue

import (
	"context"
	"time"
)

// Attr is a single key/value attribute attached to a tracing span or a metric
// observation. It is intentionally a plain struct so the observability hook
// interfaces carry no third-party dependency.
type Attr struct {
	Key   string
	Value any
}

// StringAttr builds a string-valued Attr.
func StringAttr(key, value string) Attr {
	return Attr{Key: key, Value: value}
}

// Int64Attr builds an int64-valued Attr.
func Int64Attr(key string, value int64) Attr {
	return Attr{Key: key, Value: value}
}

// Span represents an in-progress tracing span started by a Tracer. The caller
// (pgqueue) ends the span when the traced operation completes.
type Span interface {
	// End marks the span as finished.
	End()
	// SetError records that the traced operation failed with err.
	SetError(err error)
	// SetAttr attaches additional attributes to the span.
	SetAttr(attrs ...Attr)
}

// Tracer is the hook interface pgqueue uses to emit tracing spans. It is
// deliberately dependency-free: adapters for concrete tracing systems (for
// example OpenTelemetry) live in optional sub-packages such as otelpgqueue.
// Register an implementation with the WithTracer option.
type Tracer interface {
	// StartSpan begins a span named name and returns a context carrying it
	// together with the Span handle used to finish it.
	StartSpan(ctx context.Context, name string, attrs ...Attr) (context.Context, Span)
}

// MetricsRecorder is the hook interface pgqueue uses to emit metrics. Like
// Tracer it carries no third-party dependency; adapters live in optional
// sub-packages such as otelpgqueue and prompgqueue. Register an implementation
// with the WithMetrics option.
type MetricsRecorder interface {
	// RecordPublish reports that count messages were published to queue.
	RecordPublish(queue string, count int)
	// RecordConsume reports the end-to-end processing latency of one message.
	RecordConsume(queue string, latency time.Duration)
	// RecordAck reports an acknowledgement outcome; ok is false for a nack.
	RecordAck(queue string, ok bool)
	// ObserveQueueDepth reports the current number of pending messages.
	ObserveQueueDepth(queue string, depth int64)
	// ObserveDLQSize reports the current dead-letter queue size.
	ObserveDLQSize(queue string, size int64)
}

// noopSpan is the Span returned when no Tracer is registered, so instrumented
// code can call span methods unconditionally without a nil check.
type noopSpan struct{}

func (noopSpan) End()             {}
func (noopSpan) SetError(error)   {}
func (noopSpan) SetAttr(...Attr)  {}

// startSpan begins a tracing span when a Tracer is registered (WithTracer);
// otherwise it returns ctx unchanged and a no-op span. The returned span must
// always be ended by the caller.
//
//nolint:ireturn // Span is the public observability hook interface; returning
// it (a real span or the no-op) is the intended polymorphism.
func (pq *Queue) startSpan(
	ctx context.Context,
	name string,
	attrs ...Attr,
) (context.Context, Span) {
	if pq.cfg.tracer == nil {
		return ctx, noopSpan{}
	}
	ctx, span := pq.cfg.tracer.StartSpan(ctx, name, attrs...)
	// A third-party Tracer may return a nil Span; substitute the no-op so the
	// caller can End/SetError it unconditionally without a panic (R-23).
	if span == nil {
		return ctx, noopSpan{}
	}
	return ctx, span
}

// recordPublish reports a publish to the registered MetricsRecorder, if any.
func (pq *Queue) recordPublish(queue string, count int) {
	if pq.cfg.metrics != nil {
		pq.cfg.metrics.RecordPublish(queue, count)
	}
}

// recordConsume reports one message's processing latency, if metrics are on.
func (pq *Queue) recordConsume(queue string, latency time.Duration) {
	if pq.cfg.metrics != nil {
		pq.cfg.metrics.RecordConsume(queue, latency)
	}
}

// recordAck reports an acknowledgement outcome (ok=false for a nack), if on.
func (pq *Queue) recordAck(queue string, ok bool) {
	if pq.cfg.metrics != nil {
		pq.cfg.metrics.RecordAck(queue, ok)
	}
}

// observeQueueDepth reports the current pending depth, if metrics are on.
func (pq *Queue) observeQueueDepth(queue string, depth int64) {
	if pq.cfg.metrics != nil {
		pq.cfg.metrics.ObserveQueueDepth(queue, depth)
	}
}

// observeDLQSize reports the current DLQ size, if metrics are on.
func (pq *Queue) observeDLQSize(queue string, size int64) {
	if pq.cfg.metrics != nil {
		pq.cfg.metrics.ObserveDLQSize(queue, size)
	}
}

// endSpan finishes a span, recording err on it when non-nil. It is a small
// convenience for the common deferred-cleanup pattern.
func endSpan(span Span, err error) {
	if err != nil {
		span.SetError(err)
	}
	span.End()
}
