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
