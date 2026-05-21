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
