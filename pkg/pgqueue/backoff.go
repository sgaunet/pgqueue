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
	// BaseDelay is the minimum wait before the first retry.
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
