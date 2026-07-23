package fake_test

import (
	"context"
	"errors"
	"testing"

	"github.com/sgaunet/pgqueue"
	"github.com/sgaunet/pgqueue/fake"
)

// TestFakeChannelPublishDedup verifies the fake honors WithMessageID for
// publish-side dedup: a second publish of the same ID while the first is still
// live is rejected with ErrDuplicateMessageID, mirroring the real Queue's
// ON CONFLICT (id) DO NOTHING behavior. Before the fix the option slice was
// discarded and the duplicate was silently accepted with a fresh ID.
func TestFakeChannelPublishDedup(t *testing.T) {
	ctx := context.Background()
	q := fake.New()
	if err := q.CreateChannel(ctx, "orders"); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	id, err := pgqueue.NewUUIDv7()
	if err != nil {
		t.Fatalf("new uuid: %v", err)
	}

	got, err := q.Publish(ctx, "orders", []byte("a"), pgqueue.WithMessageID(id))
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if got != id {
		t.Fatalf("publish returned %v, want the supplied id %v", got, id)
	}

	if _, err := q.Publish(ctx, "orders", []byte("b"), pgqueue.WithMessageID(id)); !errors.Is(
		err, pgqueue.ErrDuplicateMessageID,
	) {
		t.Fatalf("duplicate publish: got %v, want ErrDuplicateMessageID", err)
	}
}

// TestFakeTopicPublishDedup verifies WithMessageID dedup on the fan-out path: a
// duplicate ID is rejected before any subscriber copy is appended, so exactly
// one message is delivered.
func TestFakeTopicPublishDedup(t *testing.T) {
	ctx := context.Background()
	q := fake.New()
	if err := q.CreateTopic(ctx, "events"); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := q.Subscribe(ctx, "events", "sub-1"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	id, err := pgqueue.NewUUIDv7()
	if err != nil {
		t.Fatalf("new uuid: %v", err)
	}

	if _, err := q.Publish(ctx, "events", []byte("a"), pgqueue.WithMessageID(id)); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if _, err := q.Publish(ctx, "events", []byte("b"), pgqueue.WithMessageID(id)); !errors.Is(
		err, pgqueue.ErrDuplicateMessageID,
	) {
		t.Fatalf("duplicate publish: got %v, want ErrDuplicateMessageID", err)
	}

	if _, err := q.ReceiveTopic(ctx, "events", "sub-1"); err != nil {
		t.Fatalf("receive first: %v", err)
	}
	if _, err := q.ReceiveTopic(ctx, "events", "sub-1"); !errors.Is(err, pgqueue.ErrQueueEmpty) {
		t.Fatalf("expected ErrQueueEmpty after a single delivery, got %v", err)
	}
}

// TestFakeMetadataRoundTripAndIsolation verifies WithMessageMetadata is
// propagated to the consumed message (not silently dropped) and that the
// delivered map is an independent copy: mutating the caller's map after publish,
// or the delivered map after receive, must not corrupt a later redelivery.
func TestFakeMetadataRoundTripAndIsolation(t *testing.T) {
	ctx := context.Background()
	q := fake.New(fake.WithMaxRetries(3))
	if err := q.CreateChannel(ctx, "orders"); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	meta := map[string]any{"trace": "abc"}
	if _, err := q.Publish(ctx, "orders", []byte("p"), pgqueue.WithMessageMetadata(meta)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// Mutating the caller's map after publish must not change stored metadata.
	meta["trace"] = "caller-mutated"

	msg, err := q.ReceiveChannel(ctx, "orders")
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if got := msg.Metadata["trace"]; got != "abc" {
		t.Fatalf("metadata not round-tripped or not isolated from caller: got %v, want abc", got)
	}

	// Mutate the delivered map, then nack to force a redelivery and confirm the
	// stored copy was untouched.
	msg.Metadata["trace"] = "consumer-mutated"
	if err := q.Nack(ctx, msg.Receipt(), "retry"); err != nil {
		t.Fatalf("nack: %v", err)
	}
	msg2, err := q.ReceiveChannel(ctx, "orders")
	if err != nil {
		t.Fatalf("receive redelivery: %v", err)
	}
	if got := msg2.Metadata["trace"]; got != "abc" {
		t.Fatalf("redelivered metadata corrupted by consumer mutation: got %v, want abc", got)
	}
}
