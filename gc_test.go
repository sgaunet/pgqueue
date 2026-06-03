package pgqueue

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// NewGarbageCollector substitutes default retention for an all-zero
// DefaultPolicy so a GarbageCollector created without a policy still bounds
// table growth (issue #47). These tests pin that defaulting and its escape
// hatches; they need no database — the constructor only reads/writes config.

func TestNewGarbageCollectorDefaultsEmptyPolicy(t *testing.T) {
	gc := NewGarbageCollector(&Queue{}, GarbageCollectorConfig{})

	if gc.config.DefaultPolicy != defaultRetentionPolicy {
		t.Errorf("empty DefaultPolicy: got %+v, want default %+v",
			gc.config.DefaultPolicy, defaultRetentionPolicy)
	}
	// Pending messages are live data: the default must never auto-purge them.
	if defaultRetentionPolicy.MaxPendingAge != 0 {
		t.Errorf("default MaxPendingAge = %v, want 0 (pending messages kept)",
			defaultRetentionPolicy.MaxPendingAge)
	}
}

func TestNewGarbageCollectorKeepsConfiguredPolicy(t *testing.T) {
	want := RetentionPolicy{
		CompletedMessageTTL: time.Hour,
		MaxPendingAge:       2 * time.Hour,
		DLQRetention:        3 * time.Hour,
	}
	gc := NewGarbageCollector(&Queue{}, GarbageCollectorConfig{DefaultPolicy: want})

	if gc.config.DefaultPolicy != want {
		t.Errorf("configured DefaultPolicy: got %+v, want %+v", gc.config.DefaultPolicy, want)
	}
}

// TestNewGarbageCollectorKeepForeverNotOverridden proves a policy that opts a
// field into KeepForever is not mistaken for "unconfigured": it makes the
// struct non-zero, so the constructor leaves it — and the other zero fields
// keep their "forever" meaning rather than picking up defaults.
func TestNewGarbageCollectorKeepForeverNotOverridden(t *testing.T) {
	policy := RetentionPolicy{CompletedMessageTTL: KeepForever}
	gc := NewGarbageCollector(&Queue{}, GarbageCollectorConfig{DefaultPolicy: policy})

	if gc.config.DefaultPolicy != policy {
		t.Errorf("KeepForever policy was overridden: got %+v, want %+v",
			gc.config.DefaultPolicy, policy)
	}
}

// TestNewGarbageCollectorNormalizesNonPositiveInterval pins the fix for the
// negative-interval crash: a non-positive Interval (zero, or a negative from a
// caller-side misconfiguration) must be normalized to defaultGCInterval. A
// negative value used to pass through unchanged and reach time.NewTicker in the
// background GC goroutine, which panics on a <= 0 duration and crashed the
// whole process.
func TestNewGarbageCollectorNormalizesNonPositiveInterval(t *testing.T) {
	for _, in := range []time.Duration{0, -time.Second, -time.Hour} {
		gc := NewGarbageCollector(&Queue{}, GarbageCollectorConfig{Interval: in})
		if gc.config.Interval != defaultGCInterval {
			t.Errorf("Interval %v: got %v, want default %v",
				in, gc.config.Interval, defaultGCInterval)
		}
	}
	// A positive interval must still be honored verbatim.
	gc := NewGarbageCollector(&Queue{}, GarbageCollectorConfig{Interval: 90 * time.Second})
	if gc.config.Interval != 90*time.Second {
		t.Errorf("positive Interval: got %v, want %v", gc.config.Interval, 90*time.Second)
	}
}

// TestAppendCappedErrCapsAtThreshold is the issue #62 regression: a sustained
// database outage used to grow the per-queue error slice to len(allQueues)
// every tick, producing megabyte-sized joined-error logs. The cap stops
// appending the normal "queue X: ..." entry at maxAccumulatedCollectErrs and
// emits exactly one truncation sentinel; further calls are no-ops.
func TestAppendCappedErrCapsAtThreshold(t *testing.T) {
	t.Parallel()
	var errs []error
	for i := range maxAccumulatedCollectErrs + 5 {
		errs = appendCappedErr(errs, fmt.Sprintf("q%d", i), errors.New("boom"))
	}
	if got, want := len(errs), maxAccumulatedCollectErrs+1; got != want {
		t.Fatalf("len(errs) = %d, want %d (cap + 1 truncation sentinel)", got, want)
	}
	// First N entries are the per-queue wrapping; the last is the sentinel.
	if !strings.HasPrefix(errs[0].Error(), "queue q0:") {
		t.Errorf("first error wrapping unexpected: %q", errs[0])
	}
	last := errs[len(errs)-1].Error()
	if !strings.Contains(last, "truncated") {
		t.Errorf("last error should be the truncation sentinel, got %q", last)
	}
	// Adding more must not grow the slice — the truncation marker is sticky.
	errs = appendCappedErr(errs, "another", errors.New("late"))
	if got, want := len(errs), maxAccumulatedCollectErrs+1; got != want {
		t.Errorf("after-cap call grew slice to %d, want %d", got, want)
	}
}

// TestIsPoolDead pins which sentinels short-circuit the GC dispatch loop.
func TestIsPoolDead(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"ErrConnDone", sql.ErrConnDone, true},
		{"ErrConnDone wrapped", fmt.Errorf("op: %w", sql.ErrConnDone), true},
		{"ErrBadConn", driver.ErrBadConn, true},
		{"ErrBadConn wrapped", fmt.Errorf("op: %w", driver.ErrBadConn), true},
		{"unrelated", errors.New("something else"), false},
	}
	for _, tc := range cases {
		if got := isPoolDead(tc.err); got != tc.want {
			t.Errorf("isPoolDead(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}
