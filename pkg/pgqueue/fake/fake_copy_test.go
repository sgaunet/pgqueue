package fake_test

import (
	"context"
	"testing"

	"github.com/sgaunet/pgqueue/pkg/pgqueue/fake"
)

// TestClaimReturnsCopyForRedelivery is part of the R-16 regression set: a
// consumer mutating a delivered message's payload must not corrupt the stored
// copy, so a later redelivery still sees the original bytes.
func TestClaimReturnsCopyForRedelivery(t *testing.T) {
	ctx := context.Background()
	q := fake.New()
	if err := q.CreateChannel(ctx, "orders"); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := q.PublishChannel(ctx, "orders", []byte("original")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msg1, err := q.ReceiveChannel(ctx, "orders")
	if err != nil {
		t.Fatalf("first receive: %v", err)
	}
	// Mutate the delivered payload, then nack so the message is redelivered.
	msg1.Payload[0] = 'X'
	if err := q.Nack(ctx, msg1.Receipt(), "retry"); err != nil {
		t.Fatalf("nack: %v", err)
	}

	msg2, err := q.ReceiveChannel(ctx, "orders")
	if err != nil {
		t.Fatalf("redelivery receive: %v", err)
	}
	if string(msg2.Payload) != "original" {
		t.Errorf("redelivered payload = %q, want %q: a mutation leaked into the store",
			msg2.Payload, "original")
	}
}

// TestPublishTopicFanOutCopies is part of the R-16 regression set: each
// subscriber must receive an independent copy of a published payload, so one
// subscriber mutating its message cannot corrupt a fan-out sibling.
func TestPublishTopicFanOutCopies(t *testing.T) {
	ctx := context.Background()
	q := fake.New()
	if err := q.CreateTopic(ctx, "events"); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := q.Subscribe(ctx, "events", "sub-a"); err != nil {
		t.Fatalf("subscribe a: %v", err)
	}
	if err := q.Subscribe(ctx, "events", "sub-b"); err != nil {
		t.Fatalf("subscribe b: %v", err)
	}
	if _, err := q.PublishTopic(ctx, "events", []byte("shared")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msgA, err := q.ReceiveTopic(ctx, "events", "sub-a")
	if err != nil {
		t.Fatalf("receive sub-a: %v", err)
	}
	msgA.Payload[0] = 'X' // subscriber A corrupts its own copy

	msgB, err := q.ReceiveTopic(ctx, "events", "sub-b")
	if err != nil {
		t.Fatalf("receive sub-b: %v", err)
	}
	if string(msgB.Payload) != "shared" {
		t.Errorf("sub-b payload = %q, want %q: a sibling's mutation leaked", msgB.Payload, "shared")
	}
}
