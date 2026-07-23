package pgqueue

import (
	"context"
	"errors"
	"testing"
)

// The queue-agnostic AckBatch/NackBatch group receipts by queue and dispatch
// each group. A receipt that carries no queue binding (QueueType unset — e.g.
// one built by hand, or obtained from the legacy ConsumeFrom* APIs) used to hit
// the grouping switch's `default: continue` and be silently dropped: AckBatch
// returned nil as if the message had been acknowledged, when in fact nothing
// happened and the message will be redelivered after its visibility timeout.
//
// The single-message Ack/Nack reject such a receipt with
// ErrReceiptMissingQueueType; these tests pin the batch variants to the same
// contract.

// TestAckBatchRejectsReceiptMissingQueueType proves AckBatch no longer silently
// drops a receipt with an unset QueueType.
func TestAckBatchRejectsReceiptMissingQueueType(t *testing.T) {
	pq := &Queue{}

	res, err := pq.AckBatch(context.Background(), []Receipt{{}})

	if !errors.Is(err, ErrReceiptMissingQueueType) {
		t.Fatalf("AckBatch with an unbound receipt: err = %v, want ErrReceiptMissingQueueType", err)
	}
	if res.Succeeded != nil || res.Failed != nil {
		t.Fatalf("AckBatch returned a non-zero result alongside an operational error: %+v", res)
	}
}

// TestNackBatchRejectsReceiptMissingQueueType proves NackBatch no longer
// silently drops a receipt with an unset QueueType.
func TestNackBatchRejectsReceiptMissingQueueType(t *testing.T) {
	pq := &Queue{}

	res, err := pq.NackBatch(context.Background(), []Receipt{{}}, "reason")

	if !errors.Is(err, ErrReceiptMissingQueueType) {
		t.Fatalf("NackBatch with an unbound receipt: err = %v, want ErrReceiptMissingQueueType", err)
	}
	if res.Succeeded != nil || res.Failed != nil {
		t.Fatalf("NackBatch returned a non-zero result alongside an operational error: %+v", res)
	}
}

// TestAckBatchEmptyStillSucceeds is a guard: an empty batch is a no-op and must
// not be turned into an error by the new validation.
func TestAckBatchEmptyStillSucceeds(t *testing.T) {
	pq := &Queue{}
	res, err := pq.AckBatch(context.Background(), nil)
	if err != nil {
		t.Fatalf("AckBatch(nil) = %v, want nil", err)
	}
	if len(res.Succeeded) != 0 || len(res.Failed) != 0 {
		t.Fatalf("AckBatch(nil) result = %+v, want empty", res)
	}
	res, err = pq.NackBatch(context.Background(), nil, "reason")
	if err != nil {
		t.Fatalf("NackBatch(nil) = %v, want nil", err)
	}
	if len(res.Succeeded) != 0 || len(res.Failed) != 0 {
		t.Fatalf("NackBatch(nil) result = %+v, want empty", res)
	}
}
