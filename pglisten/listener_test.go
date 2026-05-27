package pglisten

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// TestReconnectBackoffGrowsAndCaps is part of the R-07 regression set: the
// reconnect backoff window grows exponentially per attempt and is capped at
// MaxDelay; the jittered delay always lands within [0, window).
func TestReconnectBackoffGrowsAndCaps(t *testing.T) {
	l := &Listener{reconnectPolicy: ReconnectPolicy{
		BaseDelay:  1 * time.Second,
		MaxDelay:   30 * time.Second,
		Multiplier: 2,
	}.normalized()}

	cases := map[int]time.Duration{
		0:  1 * time.Second,  // window = Base
		1:  2 * time.Second,  // window = Base*2
		2:  4 * time.Second,  // window = Base*4
		3:  8 * time.Second,  // window = Base*8
		20: 30 * time.Second, // saturated at MaxDelay
	}
	for attempt, window := range cases {
		for i := range 300 {
			d := l.reconnectBackoff(attempt)
			if d < 0 || d >= window {
				t.Fatalf("attempt %d sample %d: delay %v outside [0, %v)", attempt, i, d, window)
			}
		}
	}
}

// TestReconnectBackoffJitter is part of the R-07 regression set: full jitter
// makes the delay non-deterministic, so repeated calls do not all coincide.
func TestReconnectBackoffJitter(t *testing.T) {
	l := &Listener{reconnectPolicy: ReconnectPolicy{}.normalized()}
	first := l.reconnectBackoff(5)
	allSame := true
	for range 100 {
		if l.reconnectBackoff(5) != first {
			allSame = false
			break
		}
	}
	if allSame {
		t.Error("reconnect backoff produced identical delays; jitter is not applied")
	}
}

// TestReconnectPolicyNormalized confirms a zero/partial policy is completed
// per-field with sane defaults.
func TestReconnectPolicyNormalized(t *testing.T) {
	got := ReconnectPolicy{}.normalized()
	if got.BaseDelay != defaultReconnectBaseDelay {
		t.Errorf("BaseDelay = %v, want default %v", got.BaseDelay, defaultReconnectBaseDelay)
	}
	if got.MaxDelay != defaultReconnectMaxDelay {
		t.Errorf("MaxDelay = %v, want default %v", got.MaxDelay, defaultReconnectMaxDelay)
	}
	if got.Multiplier != defaultReconnectMultiplier {
		t.Errorf("Multiplier = %v, want default %v", got.Multiplier, defaultReconnectMultiplier)
	}

	// A multiplier below 1 is invalid and must fall back to the default.
	if got := (ReconnectPolicy{Multiplier: 0.5}).normalized(); got.Multiplier < 1 {
		t.Errorf("Multiplier %v not corrected to >= 1", got.Multiplier)
	}
}

// TestKeepaliveIntervalNormalized confirms a zero or negative interval is
// replaced by the default; a positive value is preserved verbatim (#49).
func TestKeepaliveIntervalNormalized(t *testing.T) {
	if got := normalizeKeepaliveInterval(0); got != defaultKeepaliveInterval {
		t.Errorf("zero: got %v, want default %v", got, defaultKeepaliveInterval)
	}
	if got := normalizeKeepaliveInterval(-5 * time.Second); got != defaultKeepaliveInterval {
		t.Errorf("negative: got %v, want default %v", got, defaultKeepaliveInterval)
	}
	if got := normalizeKeepaliveInterval(2 * time.Second); got != 2*time.Second {
		t.Errorf("custom: got %v, want %v", got, 2*time.Second)
	}
}

// TestWithKeepaliveIntervalOption checks the option wires through to the
// Listener field and normalizes a zero value to the default (#49).
func TestWithKeepaliveIntervalOption(t *testing.T) {
	l := &Listener{}
	WithKeepaliveInterval(2 * time.Second)(l)
	if l.keepaliveInterval != 2*time.Second {
		t.Errorf("keepaliveInterval = %v, want %v", l.keepaliveInterval, 2*time.Second)
	}
	l2 := &Listener{}
	WithKeepaliveInterval(0)(l2)
	if l2.keepaliveInterval != defaultKeepaliveInterval {
		t.Errorf("zero via option: got %v, want default %v", l2.keepaliveInterval, defaultKeepaliveInterval)
	}
}

// TestKeepaliveProbeLogsAtWarnOnFailure asserts the keepalive-probe failure
// log goes out at WARN level via the shared logWarn helper (#49). We cannot
// drive a real Ping failure here without a DB; this mirrors the shape check
// done in TestReconnectLogsAtWarn.
func TestKeepaliveProbeLogsAtWarnOnFailure(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	l := &Listener{logger: logger}

	l.logWarn("pglisten: keepalive probe failed; reconnecting",
		"interval", 30*time.Second, "error", errors.New("boom"))

	out := buf.String()
	if !strings.Contains(out, "level=WARN") {
		t.Errorf("keepalive probe log not at WARN level: %q", out)
	}
	if !strings.Contains(out, "keepalive probe failed") {
		t.Errorf("keepalive probe log missing expected message: %q", out)
	}
}

// newListenerForUnlistenTest builds a Listener wired enough for the Unlisten
// state-machine tests: a non-nil channels map, a done channel so requestInterrupt
// is safe, and pre-populated entries.
func newListenerForUnlistenTest(channels ...string) *Listener {
	l := &Listener{
		channels: make(map[string]bool, len(channels)),
		done:     make(chan struct{}),
	}
	for _, ch := range channels {
		l.channels[ch] = true
	}
	return l
}

// TestUnlistenRemovesChannelFromReconnectSet verifies that Unlisten removes the
// channel from the LISTEN set (so reconnect does not re-issue it) and queues an
// UNLISTEN for the run loop to drain (#52).
func TestUnlistenRemovesChannelFromReconnectSet(t *testing.T) {
	l := newListenerForUnlistenTest("a", "b")

	if err := l.Unlisten(context.Background(), "a"); err != nil {
		t.Fatalf("Unlisten(a) returned error: %v", err)
	}

	l.mu.Lock()
	_, stillListed := l.channels["a"]
	bStillListed := l.channels["b"]
	unpending := append([]string(nil), l.unpending...)
	l.mu.Unlock()

	if stillListed {
		t.Error(`channels["a"] still present after Unlisten`)
	}
	if !bStillListed {
		t.Error(`channels["b"] dropped by Unlisten("a")`)
	}
	if len(unpending) != 1 || unpending[0] != "a" {
		t.Errorf("unpending = %v, want [a]", unpending)
	}
}

// TestUnlistenIsIdempotent verifies Unlisten on a channel that was never
// registered (or already unlistened) is a no-op and returns nil (#52).
func TestUnlistenIsIdempotent(t *testing.T) {
	l := newListenerForUnlistenTest("a")

	// First call removes "a"; second is a no-op.
	if err := l.Unlisten(context.Background(), "a"); err != nil {
		t.Fatalf("Unlisten(a) first call: %v", err)
	}
	if err := l.Unlisten(context.Background(), "a"); err != nil {
		t.Fatalf("Unlisten(a) second call: %v", err)
	}
	// Unknown channel — also a no-op.
	if err := l.Unlisten(context.Background(), "never"); err != nil {
		t.Fatalf("Unlisten(never) returned error: %v", err)
	}

	l.mu.Lock()
	unpendingLen := len(l.unpending)
	l.mu.Unlock()
	if unpendingLen != 1 {
		t.Errorf("unpending length = %d, want 1 (only the first Unlisten queues work)", unpendingLen)
	}
}

// TestUnlistenAfterCloseReturnsErrListenerClosed mirrors Listen's closed-state
// behavior so callers see a consistent shutdown signal (#52).
func TestUnlistenAfterCloseReturnsErrListenerClosed(t *testing.T) {
	l := newListenerForUnlistenTest("a")
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()

	if err := l.Unlisten(context.Background(), "a"); !errors.Is(err, errListenerClosed) {
		t.Errorf("Unlisten after close: got %v, want errListenerClosed", err)
	}
}

// TestQuoteListenIdent is the issue #70 regression: pglisten is an exported
// package, so a third-party caller may pass a channel name containing a
// double-quote — and PostgreSQL's LISTEN does not accept parameter binding,
// forcing the name to be interpolated. The identifier must be wrapped in
// double quotes with any embedded quote doubled per the SQL grammar.
func TestQuoteListenIdent(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in, want string
	}{
		{"orders", `"orders"`},
		{"with-dash", `"with-dash"`},
		{"upper Case", `"upper Case"`},
		{`bad"name`, `"bad""name"`},                  // single embedded quote
		{`a"b"c`, `"a""b""c"`},                       // multiple quotes
		{`"; DROP TABLE x; --`, `"""; DROP TABLE x; --"`}, // injection attempt
		{"", `""`},
	}
	for _, tc := range cases {
		if got := quoteListenIdent(tc.in); got != tc.want {
			t.Errorf("quoteListenIdent(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestReconnectLogsAtWarn is part of the R-07 regression set: a reconnect
// attempt is logged at WARN level (the reconnect loop logs via logWarn).
func TestReconnectLogsAtWarn(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	l := &Listener{logger: logger}

	l.logWarn("pglisten: reconnect attempt failed; retrying", "attempt", 1)

	out := buf.String()
	if !strings.Contains(out, "level=WARN") {
		t.Errorf("reconnect log not emitted at WARN level: %q", out)
	}
	if !strings.Contains(out, "reconnect") {
		t.Errorf("reconnect log missing expected message: %q", out)
	}
}
