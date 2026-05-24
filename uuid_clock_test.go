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

// fixedClock returns a clock function that always reports ms.
func fixedClock(ms uint64) func() uint64 {
	return func() uint64 { return ms }
}

// TestUUIDv7ClockRegression is the R-20 regression test: after a forward then
// backward wall-clock move the generator keeps timestamps monotonic by pinning
// ahead, but once real time catches up past the pinned millisecond, freshly
// minted UUIDs embed the real timestamp again.
func TestUUIDv7ClockRegression(t *testing.T) {
	resetUUIDv7State()
	const base uint64 = 1_700_000_000_000 // arbitrary fixed Unix ms

	// Normal forward step.
	u1, err := newUUIDv7At(fixedClock(base + 1000))
	if err != nil {
		t.Fatalf("newUUIDv7At forward: %v", err)
	}
	if ts := uint64(ExtractTimestamp(u1).UnixMilli()); ts != base+1000 {
		t.Fatalf("forward step: embedded ts = %d, want %d", ts, base+1000)
	}

	// Clock steps backwards: monotonicity is preserved by pinning at the
	// previous (higher) millisecond, so the embedded timestamp does not regress.
	u2, err := newUUIDv7At(fixedClock(base + 500))
	if err != nil {
		t.Fatalf("newUUIDv7At backward: %v", err)
	}
	if ts := uint64(ExtractTimestamp(u2).UnixMilli()); ts < base+1000 {
		t.Fatalf("backward step: embedded ts = %d regressed below pinned %d", ts, base+1000)
	}

	// Real time catches up past the pinned millisecond: the generator must
	// resync to the real timestamp rather than stay pinned ahead.
	u3, err := newUUIDv7At(fixedClock(base + 5000))
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
	clock := fixedClock(ms)

	prev, err := newUUIDv7At(clock)
	if err != nil {
		t.Fatalf("newUUIDv7At: %v", err)
	}
	for i := range 50 {
		cur, err := newUUIDv7At(clock)
		if err != nil {
			t.Fatalf("newUUIDv7At[%d]: %v", i, err)
		}
		if string(cur[:]) <= string(prev[:]) {
			t.Fatalf("UUID %d not strictly increasing within the same millisecond", i)
		}
		prev = cur
	}
}

// TestUUIDv7CounterOverflowRefreshesClock guards issue #58: when the 12-bit
// per-millisecond counter wraps, the generator must re-read the wall clock
// and resync to it instead of unconditionally borrowing the next millisecond.
// Without this guard, sustained generation above ~4M UUIDs/sec lets lastMS
// drift forward forever and ExtractTimestamp returns future timestamps.
func TestUUIDv7CounterOverflowRefreshesClock(t *testing.T) {
	resetUUIDv7State()
	const base uint64 = 1_700_000_000_000

	// Pre-arm: pin at base with the counter one tick below overflow so the
	// very next call lands in the overflow branch.
	uuidV7State.mu.Lock()
	uuidV7State.lastMS = base
	uuidV7State.counter = uuidV7CounterMask
	uuidV7State.mu.Unlock()

	// Two-stage clock: first read (top of newUUIDv7At) reports base so we
	// stay in the default branch; second read (inside the overflow branch)
	// reports a time that has moved on. The fix must resync to it.
	var calls int
	clock := func() uint64 {
		calls++
		if calls == 1 {
			return base
		}
		return base + 50
	}

	u, err := newUUIDv7At(clock)
	if err != nil {
		t.Fatalf("newUUIDv7At: %v", err)
	}
	if calls != 2 {
		t.Fatalf("overflow path should read clock twice, got %d reads", calls)
	}
	if ts := uint64(ExtractTimestamp(u).UnixMilli()); ts != base+50 {
		t.Errorf("overflow resync: embedded ts = %d, want resync to %d", ts, base+50)
	}
}

// TestUUIDv7CounterOverflowFallsBackToBorrow confirms that if the wall clock
// has not advanced at counter-overflow time, the generator still borrows the
// next millisecond — preserving strict monotonicity (R-20) when the issue #58
// resync isn't applicable.
func TestUUIDv7CounterOverflowFallsBackToBorrow(t *testing.T) {
	resetUUIDv7State()
	const base uint64 = 1_700_000_000_000

	uuidV7State.mu.Lock()
	uuidV7State.lastMS = base
	uuidV7State.counter = uuidV7CounterMask
	uuidV7State.mu.Unlock()

	u, err := newUUIDv7At(fixedClock(base))
	if err != nil {
		t.Fatalf("newUUIDv7At: %v", err)
	}
	if ts := uint64(ExtractTimestamp(u).UnixMilli()); ts != base+1 {
		t.Errorf("overflow borrow: embedded ts = %d, want %d", ts, base+1)
	}
}
