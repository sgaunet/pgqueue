package pglisten

import (
	"bytes"
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
