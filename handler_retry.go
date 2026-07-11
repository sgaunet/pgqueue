package pgqueue

// handler_retry.go defines RetryAfter, the mechanism by which a Handler (see
// ConsumeChannel/ConsumeTopic) can request a specific redelivery delay for a
// message it failed to process — e.g. to honor a rate limiter's Retry-After —
// without widening the Handler signature. Handler must stay func(context.Context,
// *Message) error forever (it is part of the frozen v1 surface), so this is
// implemented as a distinguishable error returned in place of a plain one, not
// as an extra parameter or return value.

import (
	"fmt"
	"time"
)

// retryAfterError is the unexported error type constructed by RetryAfter.
// dispatchToHandler type-asserts for it (via errors.As) after a Handler
// returns a non-nil error, and — only when found — routes the automatic nack
// through WithRetryDelay(delay) instead of letting the queue's BackoffPolicy
// compute the redelivery delay.
type retryAfterError struct {
	delay time.Duration
	cause error
}

// RetryAfter lets a Handler (used with ConsumeChannel/ConsumeTopic) pin the
// redelivery delay for the message it just failed to process, overriding the
// queue's BackoffPolicy for that one delivery. Return it in place of a plain
// error:
//
//	func handle(ctx context.Context, msg *pgqueue.Message) error {
//		err := callRateLimitedAPI(ctx, msg)
//		var rl *rateLimitError
//		if errors.As(err, &rl) {
//			// Honor the API's Retry-After instead of the queue's own backoff.
//			return pgqueue.RetryAfter(rl.RetryAfter, err)
//		}
//		return err
//	}
//
// This is the handler-based equivalent of calling the low-level Nack with
// WithRetryDelay(d): it exists because the recommended handler API
// (ConsumeChannel/ConsumeTopic) auto-nacks with no options on a returned
// error, which otherwise makes WithRetryDelay unreachable without dropping
// out of the handler loop down to the low-level Receive/Nack API.
//
// Delay validation. delay is subject to exactly the same rules as
// WithRetryDelay, because it is funneled through that same option internally:
// only a strictly positive delay overrides the backoff; a non-positive delay
// (0 or negative) is silently ignored and the queue's BackoffPolicy computes
// the redelivery delay instead, as if a plain error had been returned. Any
// positive delay is honored verbatim up to the same internal maximum (24h)
// that WithRetryDelay is clamped to — a delay beyond that is clamped down to
// it rather than deferring the message effectively forever.
//
// Cause preservation. cause is wrapped, not discarded: errors.Is(err, cause)
// and errors.As(err, ...) still succeed against the error RetryAfter returns,
// and the composed Error() string embeds cause's message, so logs and the
// dead-letter queue's failure_reason still show the real failure rather than
// an opaque "retry after" wrapper. cause should not be nil; a nil cause is
// handled without panicking but carries no diagnostic information.
//
// Interaction with max-retries. RetryAfter changes only when the next
// delivery attempt is scheduled — it does not change whether the message is
// retried or dead-lettered. A message returned via RetryAfter still counts as
// one nack toward the channel/topic's max-retries: once its retry count
// exceeds the configured maximum it is moved to the dead-letter queue on that
// nack exactly as it would be for a plain returned error, regardless of the
// delay requested.
func RetryAfter(delay time.Duration, cause error) error {
	return &retryAfterError{delay: delay, cause: cause}
}

// Error composes a message that embeds both the requested delay and the
// underlying cause, so the string recorded as a nack's failure reason (see
// Handler) stays informative rather than collapsing to an opaque wrapper.
func (e *retryAfterError) Error() string {
	if e.cause == nil {
		return fmt.Sprintf("retry after %s", e.delay)
	}
	return fmt.Sprintf("retry after %s: %s", e.delay, e.cause.Error())
}

// Unwrap exposes cause so errors.Is/errors.As can match against it through a
// RetryAfter-wrapped error.
func (e *retryAfterError) Unwrap() error {
	return e.cause
}
