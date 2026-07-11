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
