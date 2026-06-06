package pgqueue

import (
	"context"
	"runtime/debug"
	"time"
)

// Attr is a single key/value attribute attached to a tracing span or a metric
// observation. It is intentionally a plain struct so the observability hook
// interfaces carry no third-party dependency.
//
// Supported value types — what every bundled adapter (otelpgqueue, prompgqueue)
// is expected to handle natively:
//
//   - string, bool
//   - int, int8, int16, int32, int64 (always rendered as int64)
//   - uint8, uint16, uint32 (rendered as int64; uint and uint64 are rendered as
//     a decimal string to avoid losing the high bit on values > math.MaxInt64)
//   - float32, float64 (always rendered as float64)
//   - time.Duration (rendered as an int64 nanosecond count)
//   - time.Time (rendered as a RFC 3339 nanosecond string)
//   - error (rendered via Error())
//   - fmt.Stringer (rendered via String()) — the last-resort typed escape hatch
//     for custom types
//
// Anything else is rendered with fmt.Sprintf("%v", v); adapters configured with
// a logger (see otelpgqueue.WithTracerLogger) additionally emit a one-line
// warning so the unexpected type is not silently coerced.
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
//
// Implementations must not panic. pgqueue invokes every Tracer method (and the
// returned Span's methods) behind a recover, so a panicking adapter is logged
// and swallowed rather than crashing a consumer goroutine; but a hook that
// panics still loses its span/observation for that call.
type Tracer interface {
	// StartSpan begins a span named name and returns a context carrying it
	// together with the Span handle used to finish it.
	StartSpan(ctx context.Context, name string, attrs ...Attr) (context.Context, Span)
}

// MetricsRecorder is the hook interface pgqueue uses to emit metrics. Like
// Tracer it carries no third-party dependency; adapters live in optional
// sub-packages such as otelpgqueue and prompgqueue. Register an implementation
// with the WithMetrics option.
//
// Implementations must not panic. pgqueue invokes every MetricsRecorder method
// behind a recover, so a panicking adapter is logged and swallowed rather than
// crashing a consumer goroutine; but a hook that panics still loses its
// observation for that call.
type MetricsRecorder interface {
	// RecordPublish reports that count messages were published to queue.
	RecordPublish(queue string, count int)
	// RecordHandle reports the handler-only execution latency of one message:
	// the time the registered Handler spent processing it. This deliberately
	// EXCLUDES queue wait, the receive (SELECT ... FOR UPDATE) round-trip, and
	// the ack round-trip. For the publish-to-delivery interval, see
	// RecordDeliveryLatency.
	RecordHandle(queue string, latency time.Duration)
	// RecordDeliveryLatency reports the publish-to-delivery latency of one
	// message: the interval from when its row was created on publish to when
	// the handler began executing. It captures the queue wait plus fetch
	// latency that RecordHandle excludes, so wiring both lets operators
	// separate time-waiting-in-queue from time-in-handler. The value is clamped
	// to zero when consumer/database clock skew would otherwise make it
	// negative.
	//
	// Latency is always measured from the original publish time, so a redelivery
	// (after a nack or a visibility-timeout reclaim) reports the cumulative
	// time since publish, not since the redelivery. The histogram therefore
	// mixes fresh deliveries with redeliveries: a long tail can reflect
	// redelivery or DLQ replay rather than queue backlog.
	RecordDeliveryLatency(queue string, latency time.Duration)
	// RecordAck reports an acknowledgement outcome; ok is false for a nack.
	RecordAck(queue string, ok bool)
	// RecordAckAfterExpired reports n receipts whose claim was no longer valid
	// at ack/nack time because it expired and the message was reassigned to
	// another consumer — so those n messages will be redelivered. Only genuine
	// claim expirations are counted (not receipts for an already-acked or
	// purged message, which do not redeliver). The batch ack/nack helpers call
	// this once per batch with the expired count; operators wire it to detect
	// at-least-twice delivery driven by handlers outrunning the visibility
	// timeout. Implementations should add n to the counter in one call.
	RecordAckAfterExpired(queue string, n int)
	// ObserveQueueDepth reports the current number of pending messages.
	ObserveQueueDepth(queue string, depth int64)
	// ObserveDLQSize reports the current dead-letter queue size.
	ObserveDLQSize(queue string, size int64)
	// RecordMetadataParseError reports that the JSON metadata column for a
	// message in queue could not be parsed. The message is delivered with no
	// metadata rather than being dropped; operators wire this counter to detect
	// sustained corruption.
	RecordMetadataParseError(queue string)
	// RecordGCRun reports the outcome of a single per-queue garbage-collection
	// pass: how long it took, how many expired/timed-out messages were
	// reclaimed (reset to pending or dead-lettered), how many rows were purged
	// by the retention policy, and whether the pass encountered an error.
	// duration is the wall-clock time of the collectQueue call; reclaimed is
	// the count of timed-out messages reset to pending or moved to the DLQ;
	// purged is the total rows deleted by the retention policy; err is non-nil
	// when collectQueue returned an error (the GC logs and continues).
	RecordGCRun(queue string, duration time.Duration, reclaimed, purged int64, err error)
	// RecordMissedNotification reports that the LISTEN/NOTIFY channel for
	// queue lost at least one notification — typically during a reconnect.
	// The safety-net poll recovers correctness; this counter lets operators
	// quantify how often push delivery degrades to polling.
	RecordMissedNotification(queue string)
}

// safeHook runs fn — a call into a user-supplied Tracer/MetricsRecorder (or a
// Span returned by one) — behind a recover so a panicking third-party adapter
// is logged and swallowed instead of propagating out of a consumer goroutine
// and crashing the process (matching the notify.go pump precedent, #69). name
// identifies the hook in the log line. Behaviour is identical to calling fn
// directly when the hook does not panic.
func (pq *Queue) safeHook(name string, fn func()) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		pq.logError("recovered panic in observability hook",
			"hook", name,
			"panic", r,
			"stack", string(debug.Stack()),
		)
	}()
	fn()
}

// safeStartSpan invokes the registered Tracer.StartSpan behind a recover so a
// panicking third-party tracer cannot crash the consumer goroutine. On a
// recovered panic it returns the original ctx and a nil span (the caller
// substitutes the no-op span). The tracer call is wrapped in safeHook, reusing
// the established recover-and-log precedent.
//
//nolint:ireturn // Span is the public observability hook interface.
func (pq *Queue) safeStartSpan(
	ctx context.Context,
	name string,
	attrs ...Attr,
) (context.Context, Span) {
	// spanCtx starts as the parent ctx so a recovered panic (the closure body
	// never completing) leaves it unchanged, matching the documented fallback.
	spanCtx := ctx
	var span Span
	pq.safeHook("Tracer.StartSpan", func() {
		// fatcontext: deriving a context from the tracer is the whole point here,
		// and the result is propagated out via the captured locals — not stored.
		spanCtx, span = pq.cfg.tracer.StartSpan(ctx, name, attrs...) //nolint:fatcontext
	})
	return spanCtx, span
}

// noopSpan is the Span returned when no Tracer is registered, so instrumented
// code can call span methods unconditionally without a nil check.
type noopSpan struct{}

func (noopSpan) End()            {}
func (noopSpan) SetError(error)  {}
func (noopSpan) SetAttr(...Attr) {}

// startSpan begins a tracing span when a Tracer is registered (WithTracer);
// otherwise it returns ctx unchanged and a no-op span. The returned span must
// always be ended by the caller. Returning Span (real or no-op) is the
// intended polymorphism.
//
//nolint:ireturn // Span is the public observability hook interface.
func (pq *Queue) startSpan(
	ctx context.Context,
	name string,
	attrs ...Attr,
) (context.Context, Span) {
	if pq.cfg.tracer == nil {
		return ctx, noopSpan{}
	}
	// A panicking Tracer.StartSpan must not crash the consumer goroutine that is
	// starting the span. safeStartSpan recovers, leaves ctx unchanged, and hands
	// back a nil span so we can substitute the no-op below.
	spanCtx, span := pq.safeStartSpan(ctx, name, attrs...)
	// A third-party Tracer may return (or, on a recovered panic, leave) a nil
	// Span; substitute the no-op so the caller can End/SetError it
	// unconditionally without a panic (R-23).
	if span == nil {
		return ctx, noopSpan{}
	}
	return spanCtx, span
}

// recordPublish reports a publish to the registered MetricsRecorder, if any.
func (pq *Queue) recordPublish(queue string, count int) {
	if pq.cfg.metrics != nil {
		pq.safeHook("MetricsRecorder.RecordPublish", func() {
			pq.cfg.metrics.RecordPublish(queue, count)
		})
	}
}

// recordHandle reports one message's handler-only execution latency, if
// metrics are on.
func (pq *Queue) recordHandle(queue string, latency time.Duration) {
	if pq.cfg.metrics != nil {
		pq.safeHook("MetricsRecorder.RecordHandle", func() {
			pq.cfg.metrics.RecordHandle(queue, latency)
		})
	}
}

// recordDeliveryLatency reports publish-to-delivery latency, if metrics are on:
// the wall-clock interval from createdAt (the message row's creation time, set
// by the database on publish) to handlerStart (when this process began running
// the handler). createdAt is database time while handlerStart is this process's
// clock, so a consumer whose clock lags the database can yield a negative
// interval; such values are clamped to zero. A zero createdAt is skipped.
func (pq *Queue) recordDeliveryLatency(queue string, createdAt, handlerStart time.Time) {
	if pq.cfg.metrics == nil || createdAt.IsZero() {
		return
	}
	latency := max(handlerStart.Sub(createdAt), 0)
	pq.safeHook("MetricsRecorder.RecordDeliveryLatency", func() {
		pq.cfg.metrics.RecordDeliveryLatency(queue, latency)
	})
}

// recordAck reports an acknowledgement outcome (ok=false for a nack), if on.
func (pq *Queue) recordAck(queue string, ok bool) {
	if pq.cfg.metrics != nil {
		pq.safeHook("MetricsRecorder.RecordAck", func() {
			pq.cfg.metrics.RecordAck(queue, ok)
		})
	}
}

// recordAckAfterExpired reports n receipts whose claims had genuinely expired at
// ack/nack time (and whose messages will therefore redeliver), if metrics are
// on. The batch helpers pass the expired count for the whole batch in one call.
func (pq *Queue) recordAckAfterExpired(queue string, n int) {
	if pq.cfg.metrics == nil || n <= 0 {
		return
	}
	pq.safeHook("MetricsRecorder.RecordAckAfterExpired", func() {
		pq.cfg.metrics.RecordAckAfterExpired(queue, n)
	})
}

// observeQueueDepth reports the current pending depth, if metrics are on.
func (pq *Queue) observeQueueDepth(queue string, depth int64) {
	if pq.cfg.metrics != nil {
		pq.safeHook("MetricsRecorder.ObserveQueueDepth", func() {
			pq.cfg.metrics.ObserveQueueDepth(queue, depth)
		})
	}
}

// observeDLQSize reports the current DLQ size, if metrics are on.
func (pq *Queue) observeDLQSize(queue string, size int64) {
	if pq.cfg.metrics != nil {
		pq.safeHook("MetricsRecorder.ObserveDLQSize", func() {
			pq.cfg.metrics.ObserveDLQSize(queue, size)
		})
	}
}

// recordMetadataParseError reports one corrupt-metadata event for queue, if on.
func (pq *Queue) recordMetadataParseError(queue string) {
	if pq.cfg.metrics != nil {
		pq.safeHook("MetricsRecorder.RecordMetadataParseError", func() {
			pq.cfg.metrics.RecordMetadataParseError(queue)
		})
	}
}

// recordGCRun reports the outcome of a single per-queue GC pass, if metrics on.
func (pq *Queue) recordGCRun(queue string, duration time.Duration, reclaimed, purged int64, err error) {
	if pq.cfg.metrics != nil {
		pq.safeHook("MetricsRecorder.RecordGCRun", func() {
			pq.cfg.metrics.RecordGCRun(queue, duration, reclaimed, purged, err)
		})
	}
}

// recordMissedNotification reports one lost LISTEN/NOTIFY notification, if on.
func (pq *Queue) recordMissedNotification(queue string) {
	if pq.cfg.metrics != nil {
		pq.safeHook("MetricsRecorder.RecordMissedNotification", func() {
			pq.cfg.metrics.RecordMissedNotification(queue)
		})
	}
}

// endSpan finishes a span, recording err on it when non-nil. It is a small
// convenience for the common deferred-cleanup pattern. span may be a user
// Tracer's Span, so both calls run behind a recover (see safeHook): a panicking
// SetError/End is logged and swallowed instead of crashing the caller's
// goroutine. A nil span is tolerated (startSpan never hands one out, but guard
// defensively so a manual caller cannot panic here).
func (pq *Queue) endSpan(span Span, err error) {
	if span == nil {
		return
	}
	pq.safeHook("Span.End", func() {
		if err != nil {
			span.SetError(err)
		}
		span.End()
	})
}
