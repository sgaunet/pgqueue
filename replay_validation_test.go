package pgqueue_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sgaunet/pgqueue"
)

// TestReplayOptionsPerformedByValidation is the issue #72 regression: the
// validation that runs before any database call must reject PerformedBy
// values that are too long or carry any control character (NUL, CR, LF, tab,
// ESC, and other C0/C1 controls), so the audit log in pgqueue_replay_log cannot
// be bloated, made unreadable, or used to inject terminal escape sequences by a
// misconfigured caller.
//
// Only the rejection path is exercised here — the test drives validation
// through ReplayMessage on a zero-value Queue, so any accepted value would
// next dereference the nil database handle. The existing integration tests
// (PerformedBy: "test-user", etc.) cover the accepted path end to end.
func TestReplayOptionsPerformedByValidation(t *testing.T) {
	t.Parallel()

	pq := &pgqueue.Queue{}

	cases := []struct {
		name      string
		performed string
	}{
		{"over_limit", strings.Repeat("a", pgqueue.MaxPerformedByLen+1)},
		{"NUL", "alice\x00root"},
		{"CR", "alice\rinjected"},
		{"LF", "alice\ninjected"},
		{"tab", "alice\tinjected"},
		{"ESC", "alice\x1b[31minjected"},
		{"vertical_tab", "alice\x0binjected"},
		{"DEL", "alice\x7finjected"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := pq.ReplayMessage(t.Context(), "queue", pgqueue.QueueTypeChannel,
				uuid.Nil,
				pgqueue.ReplayOptions{PerformedBy: tc.performed},
			)
			if !errors.Is(err, pgqueue.ErrInvalidPerformedBy) {
				t.Fatalf("want ErrInvalidPerformedBy, got %v", err)
			}
		})
	}
}

// TestReplayOptionsNegativeLimitRejected (#75) is a companion to the
// PerformedBy validation test above: validateReplayOpts runs before any
// database call, so a negative Limit is rejected on a zero-value Queue too.
// It pins the documented bound on ReplayOptions.Limit — must be >= 0 — added
// alongside the ReplayAll named constant.
func TestReplayOptionsNegativeLimitRejected(t *testing.T) {
	t.Parallel()

	pq := &pgqueue.Queue{}

	err := pq.ReplayMessage(t.Context(), "queue", pgqueue.QueueTypeChannel,
		uuid.Nil,
		pgqueue.ReplayOptions{Limit: -1},
	)
	if !errors.Is(err, pgqueue.ErrInvalidConfig) {
		t.Fatalf("want ErrInvalidConfig for Limit=-1, got %v", err)
	}
}

// TestReplayAllIsZeroValue (#75) pins ReplayAll as the documented "no cap"
// sentinel: it must equal 0, the zero value of ReplayOptions.Limit, so a
// caller who never sets Limit already gets ReplayAll semantics.
func TestReplayAllIsZeroValue(t *testing.T) {
	t.Parallel()

	if pgqueue.ReplayAll != 0 {
		t.Errorf("ReplayAll = %d, want 0", pgqueue.ReplayAll)
	}

	var opts pgqueue.ReplayOptions
	if opts.Limit != pgqueue.ReplayAll {
		t.Errorf("zero-value ReplayOptions.Limit = %d, want ReplayAll (%d)", opts.Limit, pgqueue.ReplayAll)
	}
}

// TestDefaultReplayHistoryLimitValue (#115) pins the exported constant that
// replaced ReplayHistory's bare "100" magic literal.
func TestDefaultReplayHistoryLimitValue(t *testing.T) {
	t.Parallel()

	if pgqueue.DefaultReplayHistoryLimit != 100 {
		t.Errorf("DefaultReplayHistoryLimit = %d, want 100", pgqueue.DefaultReplayHistoryLimit)
	}
}
