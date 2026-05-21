package pgqueue_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/sgaunet/pgqueue/pkg/pgqueue"
)

// TestSetReceiptHonorsEmptyQueueName is the R-22 regression test: a receipt
// bound via SetReceipt must be returned by Receipt() unchanged, even when its
// QueueName is empty — a legitimate case for an in-memory test double.
func TestSetReceiptHonorsEmptyQueueName(t *testing.T) {
	msg := &pgqueue.Message{ID: uuid.New(), ClaimID: uuid.New()}
	r := pgqueue.Receipt{
		MessageID: msg.ID,
		ClaimID:   msg.ClaimID,
		QueueType: pgqueue.QueueTypeChannel,
		QueueName: "", // intentionally empty
	}

	pgqueue.SetReceipt(msg, r)

	got := msg.Receipt()
	if got != r {
		t.Errorf("Receipt() = %+v, want %+v: a set receipt must be honored verbatim", got, r)
	}
	if got.QueueType != pgqueue.QueueTypeChannel {
		t.Errorf("QueueType lost: got %q, want %q", got.QueueType, pgqueue.QueueTypeChannel)
	}
}

// TestReceiptFallbackWhenUnset confirms a Message with no bound receipt still
// returns the bare MessageID/ClaimID fallback.
func TestReceiptFallbackWhenUnset(t *testing.T) {
	msg := &pgqueue.Message{ID: uuid.New(), ClaimID: uuid.New()}
	got := msg.Receipt()
	want := pgqueue.Receipt{MessageID: msg.ID, ClaimID: msg.ClaimID}
	if got != want {
		t.Errorf("Receipt() fallback = %+v, want %+v", got, want)
	}
}
