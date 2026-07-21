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

// TestChannelAndTopicNotFoundWrapQueueNotFound pins the symmetric not-found
// taxonomy: ErrChannelNotFound and ErrTopicNotFound both wrap ErrQueueNotFound
// (so errors.Is(err, ErrQueueNotFound) matches either), stay distinct from each
// other, and keep matching through the "%s: %w" wrapping their call sites apply.
func TestChannelAndTopicNotFoundWrapQueueNotFound(t *testing.T) {
	if !errors.Is(pgqueue.ErrChannelNotFound, pgqueue.ErrQueueNotFound) {
		t.Error("errors.Is(ErrChannelNotFound, ErrQueueNotFound) should be true")
	}
	if !errors.Is(pgqueue.ErrTopicNotFound, pgqueue.ErrQueueNotFound) {
		t.Error("errors.Is(ErrTopicNotFound, ErrQueueNotFound) should be true")
	}
	if errors.Is(pgqueue.ErrChannelNotFound, pgqueue.ErrTopicNotFound) ||
		errors.Is(pgqueue.ErrTopicNotFound, pgqueue.ErrChannelNotFound) {
		t.Error("ErrChannelNotFound and ErrTopicNotFound must be distinct sentinels")
	}
	wrapped := fmt.Errorf("orders: %w", pgqueue.ErrChannelNotFound)
	if !errors.Is(wrapped, pgqueue.ErrQueueNotFound) {
		t.Error("a wrapped ErrChannelNotFound should still match ErrQueueNotFound")
	}
	if !errors.Is(wrapped, pgqueue.ErrChannelNotFound) {
		t.Error("a wrapped ErrChannelNotFound should still match ErrChannelNotFound")
	}
}

// TestClaimClassificationSentinelsAreDistinct is a regression test for issue
// #129: ErrMessageNotFound, ErrMessageAlreadyAcked, and ErrClaimExpired are
// the three outcomes of classifyClaimState (queries.go) and must never match
// one another via errors.Is, or a caller branching on one would silently also
// catch the others.
func TestClaimClassificationSentinelsAreDistinct(t *testing.T) {
	sentinels := map[string]error{
		"ErrMessageNotFound":     pgqueue.ErrMessageNotFound,
		"ErrMessageAlreadyAcked": pgqueue.ErrMessageAlreadyAcked,
		"ErrClaimExpired":        pgqueue.ErrClaimExpired,
	}
	for aName, a := range sentinels {
		for bName, b := range sentinels {
			if aName == bName {
				continue
			}
			if errors.Is(a, b) {
				t.Errorf("errors.Is(%s, %s) should be false: the sentinels must stay distinct", aName, bName)
			}
		}
	}
}

// TestReceiptMissingQueueTypeIdentity pins ErrReceiptMissingQueueType's
// identity and confirms it stays distinct from the claim-classification
// sentinels (issue #129: its doc now covers any invalid QueueType, not only
// an unset one, but the sentinel value itself is unchanged).
func TestReceiptMissingQueueTypeIdentity(t *testing.T) {
	if !errors.Is(pgqueue.ErrReceiptMissingQueueType, pgqueue.ErrReceiptMissingQueueType) {
		t.Error("errors.Is(ErrReceiptMissingQueueType, ErrReceiptMissingQueueType) should be true")
	}
	wrapped := fmt.Errorf("some-context: %w", pgqueue.ErrReceiptMissingQueueType)
	if !errors.Is(wrapped, pgqueue.ErrReceiptMissingQueueType) {
		t.Error("a wrapped ErrReceiptMissingQueueType should still match ErrReceiptMissingQueueType")
	}
	if errors.Is(pgqueue.ErrReceiptMissingQueueType, pgqueue.ErrMessageNotFound) ||
		errors.Is(pgqueue.ErrReceiptMissingQueueType, pgqueue.ErrMessageAlreadyAcked) ||
		errors.Is(pgqueue.ErrReceiptMissingQueueType, pgqueue.ErrClaimExpired) {
		t.Error("ErrReceiptMissingQueueType must not match any claim-classification sentinel")
	}
}
