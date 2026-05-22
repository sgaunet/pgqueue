package pgqueue_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/sgaunet/pgqueue"
)

// TestReplayNotFoundWrapsMessageNotFound is the R-21 regression test:
// ErrReplayMessageNotFound must wrap ErrMessageNotFound so a caller matching
// the general sentinel also catches the replay-specific one.
func TestReplayNotFoundWrapsMessageNotFound(t *testing.T) {
	if !errors.Is(pgqueue.ErrReplayMessageNotFound, pgqueue.ErrMessageNotFound) {
		t.Error("errors.Is(ErrReplayMessageNotFound, ErrMessageNotFound) should be true")
	}

	// The relationship must survive additional wrapping, as replay call sites do.
	wrapped := fmt.Errorf("some-message-id: %w", pgqueue.ErrReplayMessageNotFound)
	if !errors.Is(wrapped, pgqueue.ErrMessageNotFound) {
		t.Error("a wrapped ErrReplayMessageNotFound should still match ErrMessageNotFound")
	}
	if !errors.Is(wrapped, pgqueue.ErrReplayMessageNotFound) {
		t.Error("a wrapped ErrReplayMessageNotFound should still match ErrReplayMessageNotFound")
	}
}
