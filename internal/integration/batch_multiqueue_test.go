package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sgaunet/pgqueue"
)

// TestMultiQueueBatchAtomicity verifies the multi-queue AckBatch contract fixed
// by M1/FR-025: when one per-queue group fails operationally (here, a channel
// that was deleted after its message was claimed), the groups that already
// committed are still reported in BatchResult alongside a joined error — the
// result is not silently zeroed.
//
// AckBatch groups receipts by queue and each group commits in its own
// transaction, so the surviving group's ack must survive the sibling group's
// failure.
func TestMultiQueueBatchAtomicity(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	if err := pq.CreateChannel(ctx, "mq-ok"); err != nil {
		t.Fatalf("create mq-ok: %v", err)
	}
	if err := pq.CreateChannel(ctx, "mq-gone"); err != nil {
		t.Fatalf("create mq-gone: %v", err)
	}

	// A live claim on the surviving channel.
	if _, err := pq.Publish(ctx, "mq-ok", []byte("keep")); err != nil {
		t.Fatalf("publish mq-ok: %v", err)
	}
	okMsg, err := pq.ReceiveChannel(ctx, "mq-ok", pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("receive mq-ok: %v", err)
	}

	// A live claim on a channel we then delete, so its group fails at ack time.
	if _, err := pq.Publish(ctx, "mq-gone", []byte("doomed")); err != nil {
		t.Fatalf("publish mq-gone: %v", err)
	}
	goneMsg, err := pq.ReceiveChannel(ctx, "mq-gone", pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("receive mq-gone: %v", err)
	}
	if err := pq.DeleteChannel(ctx, "mq-gone"); err != nil {
		t.Fatalf("delete mq-gone: %v", err)
	}

	res, err := pq.AckBatch(ctx, []pgqueue.Receipt{okMsg.Receipt(), goneMsg.Receipt()})

	// The deleted group surfaces a joined ErrQueueNotFound...
	if !errors.Is(err, pgqueue.ErrQueueNotFound) {
		t.Fatalf("expected joined ErrQueueNotFound from the deleted group, got %v", err)
	}
	// ...while the committed group's ack is still reported, not zeroed (M1).
	if len(res.Succeeded) != 1 {
		t.Fatalf("expected 1 committed ack preserved, got %d (BatchResult was zeroed?)", len(res.Succeeded))
	}
	if res.Succeeded[0].MessageID != okMsg.ID {
		t.Fatalf("expected committed ack for %s, got %s", okMsg.ID, res.Succeeded[0].MessageID)
	}

	// And the surviving message really is acked: it is not redelivered.
	if _, err := pq.ReceiveChannel(ctx, "mq-ok"); !errors.Is(err, pgqueue.ErrQueueEmpty) {
		t.Fatalf("mq-ok message should be acked (queue empty), got %v", err)
	}
}
