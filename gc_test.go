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

// mustNewGC constructs a GarbageCollector for a config expected to be valid,
// failing the test on the (now returned) validation error.
func mustNewGC(t *testing.T, config GarbageCollectorConfig) *GarbageCollector {
	t.Helper()
	gc, err := NewGarbageCollector(&Queue{}, config)
	if err != nil {
		t.Fatalf("NewGarbageCollector(%+v): unexpected error %v", config, err)
	}
	return gc
}

func TestNewGarbageCollectorDefaultsEmptyPolicy(t *testing.T) {
	gc := mustNewGC(t, GarbageCollectorConfig{})

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
	gc := mustNewGC(t, GarbageCollectorConfig{DefaultPolicy: want})

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
	gc := mustNewGC(t, GarbageCollectorConfig{DefaultPolicy: policy})

	if gc.config.DefaultPolicy != policy {
		t.Errorf("KeepForever policy was overridden: got %+v, want %+v",
			gc.config.DefaultPolicy, policy)
	}
}

// TestNewGarbageCollectorInterval pins the interval handling: a zero Interval is
// the "unset" sentinel and normalizes to defaultGCInterval (so an empty config
// still works — issue #47), a positive interval is honored verbatim, and a
// negative interval is now a rejected configuration error rather than being
// silently normalized. A negative value used to pass through and reach
// time.NewTicker in the background GC goroutine, which panics on a <= 0 duration.
func TestNewGarbageCollectorInterval(t *testing.T) {
	// Zero -> default.
	gc, err := NewGarbageCollector(&Queue{}, GarbageCollectorConfig{Interval: 0})
	if err != nil {
		t.Fatalf("zero Interval: unexpected error %v", err)
	}
	if gc.config.Interval != defaultGCInterval {
		t.Errorf("zero Interval: got %v, want default %v", gc.config.Interval, defaultGCInterval)
	}

	// Positive -> verbatim.
	gc, err = NewGarbageCollector(&Queue{}, GarbageCollectorConfig{Interval: 90 * time.Second})
	if err != nil {
		t.Fatalf("positive Interval: unexpected error %v", err)
	}
	if gc.config.Interval != 90*time.Second {
		t.Errorf("positive Interval: got %v, want %v", gc.config.Interval, 90*time.Second)
	}

	// Negative -> error.
	for _, in := range []time.Duration{-time.Second, -time.Hour} {
		if _, err := NewGarbageCollector(&Queue{}, GarbageCollectorConfig{Interval: in}); err == nil {
			t.Errorf("Interval %v: expected an error, got nil", in)
		} else if !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("Interval %v: error = %v, want ErrInvalidConfig", in, err)
		}
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
