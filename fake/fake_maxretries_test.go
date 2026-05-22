package fake_test

import (
	"context"
	"errors"
	"testing"

	"github.com/sgaunet/pgqueue"
	"github.com/sgaunet/pgqueue/fake"
)

// TestFakeWithMaxRetriesZero proves that fake.WithMaxRetries(0) is honored:
// a message nacked once is dead-lettered immediately, mirroring the core
// library's WithDefaultMaxRetries(0). The earlier implementation ignored any
// non-positive argument, so WithMaxRetries(0) silently left the limit at the
// default of 3 — and code under test could not exercise the "no retries" path
// against the in-memory double.
func TestFakeWithMaxRetriesZero(t *testing.T) {
	ctx := context.Background()
	q := fake.New(fake.WithMaxRetries(0))
	if err := q.CreateChannel(ctx, "jobs"); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := q.PublishChannel(ctx, "jobs", []byte("poison")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// With a zero retry limit the first nack must dead-letter the message.
	msg, err := q.ReceiveChannel(ctx, "jobs")
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if err := q.Nack(ctx, msg.Receipt(), "failed"); err != nil {
		t.Fatalf("nack: %v", err)
	}

	if _, err := q.ReceiveChannel(ctx, "jobs"); !errors.Is(err, pgqueue.ErrQueueEmpty) {
		t.Fatalf("message was redelivered; expected immediate DLQ with maxRetries=0, got %v", err)
	}
	if dlq := q.ChannelDLQ("jobs"); len(dlq) != 1 || string(dlq[0]) != "poison" {
		t.Fatalf("expected poison message in DLQ after one nack, got %v", dlq)
	}
}
