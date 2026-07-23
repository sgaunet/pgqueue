package pgqueue

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
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

// TestGCRecoverPanicWrapsAndLogs verifies the GC background-goroutine panic
// guard (the safety fix for the unrecovered-panic-crashes-the-process gap): a
// nil recover() value yields no error and logs nothing, and a real panic value
// is logged at ERROR and returned as an error wrapping errGCPanic, without
// re-panicking. This is the unit half of the "a panic in run/collectQueue must
// not crash the host process" contract; the wiring into run and the per-queue
// workers is covered by build + review.
func TestGCRecoverPanicWrapsAndLogs(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError}))
	gc := &GarbageCollector{pq: &Queue{logger: logger}}

	if err := gc.recoverPanic(nil, "run"); err != nil {
		t.Errorf("recoverPanic(nil) = %v, want nil", err)
	}
	if got := buf.Len(); got != 0 {
		t.Errorf("recoverPanic(nil) logged %d bytes, want 0", got)
	}

	err := gc.recoverPanic("boom", "collectQueue[orders]")
	if err == nil {
		t.Fatal("recoverPanic(non-nil) = nil, want error")
	}
	if !errors.Is(err, errGCPanic) {
		t.Errorf("recoverPanic error %v does not wrap errGCPanic", err)
	}
	out := buf.String()
	if !strings.Contains(out, "level=ERROR") ||
		!strings.Contains(out, "recovered panic in garbage collector") {
		t.Errorf("panic was not logged at ERROR with the expected message: %q", out)
	}
	if !strings.Contains(out, "collectQueue[orders]") {
		t.Errorf("log did not include the where context: %q", out)
	}
}

// TestGCRecoverPanicNilLoggerDoesNotPanic ensures the guard is safe when the
// Queue has no logger configured (the common default): it still returns the
// wrapped error and does not itself panic.
func TestGCRecoverPanicNilLoggerDoesNotPanic(t *testing.T) {
	gc := &GarbageCollector{pq: &Queue{}}
	if err := gc.recoverPanic("boom", "run"); err == nil || !errors.Is(err, errGCPanic) {
		t.Errorf("recoverPanic with nil logger = %v, want error wrapping errGCPanic", err)
	}
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
