package pgqueue

import (
	"testing"
	"time"
)

// TestComputeRetryDelayBounded is the R-13 regression test: computeRetryDelay
// must run in O(cap) time and stay within the policy bounds even when the
// attempt count is absurdly large (a corrupted retry_count column value).
func TestComputeRetryDelayBounded(t *testing.T) {
	policy := DefaultBackoffPolicy()
	pq := &Queue{cfg: queueConfig{backoffPolicy: policy}}

	start := time.Now()
	got := pq.computeRetryDelay(1<<30, 0)
	elapsed := time.Since(start)

	if elapsed > time.Second {
		t.Fatalf("computeRetryDelay(1<<30) took %v; expected O(cap) bounded time", elapsed)
	}
	if got < policy.BaseDelay || got > policy.MaxDelay {
		t.Fatalf("delay %v outside policy bounds [%v, %v]", got, policy.BaseDelay, policy.MaxDelay)
	}
}

// TestComputeRetryDelaySaturates checks that a very large attempt count yields
// the same bounded result as exactly maxBackoffSteps attempts — the geometric
// series has fully saturated, so further iterations cannot change the bound.
func TestComputeRetryDelaySaturates(t *testing.T) {
	policy := BackoffPolicy{BaseDelay: time.Second, MaxDelay: time.Minute, Multiplier: 3}
	pq := &Queue{cfg: queueConfig{backoffPolicy: policy}}

	for _, attempt := range []int{maxBackoffSteps, maxBackoffSteps + 1, 1 << 20} {
		got := pq.computeRetryDelay(attempt, 0)
		if got < policy.BaseDelay || got > policy.MaxDelay {
			t.Errorf("attempt=%d: delay %v outside [%v, %v]",
				attempt, got, policy.BaseDelay, policy.MaxDelay)
		}
	}
}

// TestComputeRetryDelayOverride confirms a positive override still wins
// outright and is returned unchanged.
func TestComputeRetryDelayOverride(t *testing.T) {
	pq := &Queue{cfg: queueConfig{backoffPolicy: DefaultBackoffPolicy()}}
	override := 7 * time.Second
	if got := pq.computeRetryDelay(1<<30, override); got != override {
		t.Errorf("override delay = %v, want %v", got, override)
	}
}

// TestBackoffDelayNoUnderflow is the #85 regression: when prev*Multiplier is
// still below BaseDelay the jitter must not produce a negative intermediate and
// the result must still be within [BaseDelay, MaxDelay].
func TestBackoffDelayNoUnderflow(t *testing.T) {
	// Multiplier=1.0 means high==low on the first call (prev==0 → clamped to
	// BaseDelay), so high-low==0 and the random term must be exactly 0.
	p := BackoffPolicy{
		BaseDelay:  500 * time.Millisecond,
		MaxDelay:   5 * time.Minute,
		Multiplier: 1.0,
	}
	for i := range 1000 {
		got := p.Delay(0)
		if got < p.BaseDelay || got > p.MaxDelay {
			t.Fatalf("iteration %d: Delay(0) = %v, outside [%v, %v]", i, got, p.BaseDelay, p.MaxDelay)
		}
	}

	// Tiny prev still below BaseDelay after multiplication must stay in bounds.
	tiny := 1 * time.Nanosecond
	for i := range 1000 {
		got := p.Delay(tiny)
		if got < p.BaseDelay || got > p.MaxDelay {
			t.Fatalf("iteration %d: Delay(tiny) = %v, outside [%v, %v]", i, got, p.BaseDelay, p.MaxDelay)
		}
	}
}
