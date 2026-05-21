package pgqueue

import (
	"errors"
	"fmt"
	"testing"
)

// fakeSQLErr is a stand-in for a driver error exposing SQLSTATE via SQLState(),
// matching how pgx's *pgconn.PgError behaves.
type fakeSQLErr struct{ state string }

func (e fakeSQLErr) Error() string    { return "sql error " + e.state }
func (e fakeSQLErr) SQLState() string { return e.state }

func TestIsTransientError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"serialization failure via SQLState", fakeSQLErr{state: "40001"}, true},
		{"deadlock via SQLState", fakeSQLErr{state: "40P01"}, true},
		{"unique violation is not transient", fakeSQLErr{state: "23505"}, false},
		{"wrapped serialization failure", fmt.Errorf("query: %w", fakeSQLErr{state: "40001"}), true},
		{"connection reset via text", errors.New("read tcp: connection reset by peer"), true},
		{"bad connection via text", errors.New("driver: bad connection"), true},
		{"plain error is not transient", errors.New("something else went wrong"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTransientError(tt.err); got != tt.want {
				t.Errorf("isTransientError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestTransientBackoffEscalatesAndCaps(t *testing.T) {
	prev := transientBackoff(1)
	if prev <= 0 {
		t.Fatalf("first backoff must be positive, got %v", prev)
	}
	for attempt := 2; attempt <= 10; attempt++ {
		d := transientBackoff(attempt)
		if d < prev {
			t.Errorf("backoff decreased at attempt %d: %v < %v", attempt, d, prev)
		}
		if d > transientBackoffCap {
			t.Errorf("backoff exceeded cap at attempt %d: %v > %v", attempt, d, transientBackoffCap)
		}
		prev = d
	}
}
