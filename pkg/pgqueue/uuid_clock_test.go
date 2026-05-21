package pgqueue

import (
	"testing"
)

// resetUUIDv7State clears the process-global generator state so a test starts
// from a known baseline.
func resetUUIDv7State() {
	uuidV7State.mu.Lock()
	uuidV7State.lastMS = 0
	uuidV7State.counter = 0
	uuidV7State.mu.Unlock()
}

// TestUUIDv7ClockRegression is the R-20 regression test: after a forward then
// backward wall-clock move the generator keeps timestamps monotonic by pinning
// ahead, but once real time catches up past the pinned millisecond, freshly
// minted UUIDs embed the real timestamp again.
func TestUUIDv7ClockRegression(t *testing.T) {
	resetUUIDv7State()
	const base uint64 = 1_700_000_000_000 // arbitrary fixed Unix ms

	// Normal forward step.
	u1, err := newUUIDv7At(base + 1000)
	if err != nil {
		t.Fatalf("newUUIDv7At forward: %v", err)
	}
	if ts := uint64(ExtractTimestamp(u1).UnixMilli()); ts != base+1000 {
		t.Fatalf("forward step: embedded ts = %d, want %d", ts, base+1000)
	}

	// Clock steps backwards: monotonicity is preserved by pinning at the
	// previous (higher) millisecond, so the embedded timestamp does not regress.
	u2, err := newUUIDv7At(base + 500)
	if err != nil {
		t.Fatalf("newUUIDv7At backward: %v", err)
	}
	if ts := uint64(ExtractTimestamp(u2).UnixMilli()); ts < base+1000 {
		t.Fatalf("backward step: embedded ts = %d regressed below pinned %d", ts, base+1000)
	}

	// Real time catches up past the pinned millisecond: the generator must
	// resync to the real timestamp rather than stay pinned ahead.
	u3, err := newUUIDv7At(base + 5000)
	if err != nil {
		t.Fatalf("newUUIDv7At catch-up: %v", err)
	}
	if ts := uint64(ExtractTimestamp(u3).UnixMilli()); ts != base+5000 {
		t.Errorf("after catch-up: embedded ts = %d, want resync to %d", ts, base+5000)
	}
}

// TestUUIDv7MonotonicWithinMillisecond confirms the counter keeps ordering
// strict for UUIDs minted within the same millisecond.
func TestUUIDv7MonotonicWithinMillisecond(t *testing.T) {
	resetUUIDv7State()
	const ms uint64 = 1_700_000_000_000

	prev, err := newUUIDv7At(ms)
	if err != nil {
		t.Fatalf("newUUIDv7At: %v", err)
	}
	for i := range 50 {
		cur, err := newUUIDv7At(ms)
		if err != nil {
			t.Fatalf("newUUIDv7At[%d]: %v", i, err)
		}
		if string(cur[:]) <= string(prev[:]) {
			t.Fatalf("UUID %d not strictly increasing within the same millisecond", i)
		}
		prev = cur
	}
}
