package pgqueue

import (
	"math/rand/v2"
	"time"
)

// Default backoff parameters applied by DefaultBackoffPolicy.
const (
	defaultBackoffBase       = 1 * time.Second
	defaultBackoffMax        = 5 * time.Minute
	defaultBackoffMultiplier = 3.0
)

// BackoffPolicy controls how long a nacked message waits before it becomes
// eligible for redelivery. pgqueue uses decorrelated jitter: each retry waits a
// random duration between BaseDelay and the previous delay scaled by
// Multiplier, capped at MaxDelay. The jitter spreads retries out and avoids
// redelivery storms when many messages fail at once.
type BackoffPolicy struct {
	// BaseDelay is the minimum wait before the first retry. A zero or negative
	// value is replaced by the 1s default in normalized(). Setting it below ~1ms
	// in production is discouraged: sub-millisecond backoff makes a batch of
	// simultaneously-failing messages redeliver in a tight loop that spikes
	// database load, so pick a value that meaningfully spreads retries out. (The
	// integration suite deliberately uses sub-ms values for fast redelivery; that
	// is a test-only trade-off, L5.)
	BaseDelay time.Duration
	// MaxDelay caps the wait for any single retry.
	MaxDelay time.Duration
	// Multiplier scales the upper jitter bound between successive retries; it
	// must be >= 1.
	Multiplier float64
}

// DefaultBackoffPolicy is applied to queues that do not configure their own
// policy: base 1s, max 5m, multiplier 3.
func DefaultBackoffPolicy() BackoffPolicy {
	return BackoffPolicy{
		BaseDelay:  defaultBackoffBase,
		MaxDelay:   defaultBackoffMax,
		Multiplier: defaultBackoffMultiplier,
	}
}

// Delay returns the backoff duration before the next retry using decorrelated
// jitter. prev is the delay used by the previous attempt; pass 0 for the first
// retry. The result is always within [BaseDelay, MaxDelay].
func (p BackoffPolicy) Delay(prev time.Duration) time.Duration {
	pn := p.normalized()
	if prev < pn.BaseDelay {
		prev = pn.BaseDelay
	}

	low := float64(pn.BaseDelay)
	high := float64(prev) * pn.Multiplier
	// When prev*Multiplier hasn't grown past BaseDelay yet, hold the floor so
	// high-low is never negative and the jitter term is always non-negative.
	if high < low {
		high = low
	}
	//nolint:gosec // G404: backoff jitter is not security-sensitive.
	d := time.Duration(low + rand.Float64()*(high-low))

	switch {
	case d > pn.MaxDelay:
		return pn.MaxDelay
	case d < pn.BaseDelay:
		return pn.BaseDelay
	default:
		return d
	}
}

// maxBackoffSteps caps how many times computeRetryDelay advances the backoff
// policy. The decorrelated-jitter series saturates at MaxDelay within a handful
// of steps, so iterating further is wasted work — and attempt comes from the
// unbounded retry_count column, where a corrupted large value would otherwise
// hang the call (R-13).
const maxBackoffSteps = 64

// maxRetryDelay caps an explicit WithRetryDelay override. A day is far beyond
// any sane redelivery backoff, so a larger value almost always means a unit
// mix-up (e.g. nanoseconds where seconds were intended) that would otherwise
// defer a message effectively forever; the override is clamped rather than
// rejected because Nack is already a failure path (L9).
const maxRetryDelay = 24 * time.Hour

// computeRetryDelay resolves how long a nacked message must wait before it
// becomes eligible for redelivery. A positive override (from WithRetryDelay)
// wins outright, clamped to maxRetryDelay; otherwise the queue's BackoffPolicy
// is advanced attempt times to produce the decorrelated-jitter delay for this
// retry (FR-023).
//
// The iteration count is capped at maxBackoffSteps so the call runs in O(cap)
// time regardless of attempt; the delay has already saturated at MaxDelay by
// then.
func (pq *Queue) computeRetryDelay(attempt int, override time.Duration) time.Duration {
	if override > 0 {
		if override > maxRetryDelay {
			return maxRetryDelay
		}
		return override
	}
	steps := min(attempt, maxBackoffSteps)
	d := time.Duration(0)
	for range steps {
		d = pq.cfg.backoffPolicy.Delay(d)
	}
	return d
}

// normalized returns a copy of p with any zero or invalid field replaced by the
// corresponding default, so a partially-specified policy is still usable.
func (p BackoffPolicy) normalized() BackoffPolicy {
	d := DefaultBackoffPolicy()
	if p.BaseDelay <= 0 {
		p.BaseDelay = d.BaseDelay
	}
	if p.MaxDelay < p.BaseDelay {
		p.MaxDelay = d.MaxDelay
	}
	if p.MaxDelay < p.BaseDelay {
		p.MaxDelay = p.BaseDelay
	}
	if p.Multiplier < 1 {
		p.Multiplier = d.Multiplier
	}
	return p
}
