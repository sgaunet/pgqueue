package fake_test

import (
	"context"
	"errors"
	"testing"

	"github.com/sgaunet/pgqueue/pkg/pgqueue"
	"github.com/sgaunet/pgqueue/pkg/pgqueue/fake"
)

// TestFakeSatisfiesPublishedInterfaces is a compile-time and runtime check that
// *fake.Queue implements every published pgqueue interface (FR-020/FR-021).
func TestFakeSatisfiesPublishedInterfaces(t *testing.T) {
	var (
		_ pgqueue.Publisher       = fake.New()
		_ pgqueue.ChannelConsumer = fake.New()
		_ pgqueue.TopicConsumer   = fake.New()
	)
}

// TestFakeChannelPublishConsumeAck covers the basic channel happy path.
func TestFakeChannelPublishConsumeAck(t *testing.T) {
	ctx := context.Background()
	q := fake.New()
	if err := q.CreateChannel(ctx, "orders"); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// An empty channel reports ErrQueueEmpty, never (nil, nil).
	if _, err := q.ReceiveChannel(ctx, "orders"); !errors.Is(err, pgqueue.ErrQueueEmpty) {
		t.Fatalf("expected ErrQueueEmpty, got %v", err)
	}

	if _, err := q.PublishChannel(ctx, "orders", []byte("payload")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	msg, err := q.ReceiveChannel(ctx, "orders")
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if string(msg.Payload) != "payload" {
		t.Fatalf("unexpected payload %q", msg.Payload)
	}
	if err := q.Ack(ctx, msg.Receipt()); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if _, err := q.ReceiveChannel(ctx, "orders"); !errors.Is(err, pgqueue.ErrQueueEmpty) {
		t.Fatalf("expected ErrQueueEmpty after ack, got %v", err)
	}
}

// TestFakeNackRetryThenDLQ verifies that a repeatedly nacked message is retried
// up to the limit and then promoted to the DLQ at exactly max-retries.
func TestFakeNackRetryThenDLQ(t *testing.T) {
	ctx := context.Background()
	q := fake.New(fake.WithMaxRetries(2))
	if err := q.CreateChannel(ctx, "jobs"); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := q.PublishChannel(ctx, "jobs", []byte("poison")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Deliveries 1 and 2 are retried; delivery 3 (retryCount 2, 2+1>2) -> DLQ.
	for attempt := 1; attempt <= 3; attempt++ {
		msg, err := q.ReceiveChannel(ctx, "jobs")
		if err != nil {
			t.Fatalf("attempt %d receive: %v", attempt, err)
		}
		if err := q.Nack(ctx, msg.Receipt(), "still failing"); err != nil {
			t.Fatalf("attempt %d nack: %v", attempt, err)
		}
	}

	if _, err := q.ReceiveChannel(ctx, "jobs"); !errors.Is(err, pgqueue.ErrQueueEmpty) {
		t.Fatalf("expected queue empty after DLQ promotion, got %v", err)
	}
	dlq := q.ChannelDLQ("jobs")
	if len(dlq) != 1 || string(dlq[0]) != "poison" {
		t.Fatalf("expected poison message in DLQ, got %v", dlq)
	}
}

// TestFakeStaleClaimResolvesToClaimExpired verifies the fencing-token semantics:
// a receipt from a delivery superseded by a nack-retry is rejected.
func TestFakeStaleClaimResolvesToClaimExpired(t *testing.T) {
	ctx := context.Background()
	q := fake.New()
	if err := q.CreateChannel(ctx, "c"); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := q.PublishChannel(ctx, "c", []byte("m")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	first, err := q.ReceiveChannel(ctx, "c")
	if err != nil {
		t.Fatalf("first receive: %v", err)
	}
	if err := q.Nack(ctx, first.Receipt(), "retry"); err != nil {
		t.Fatalf("nack: %v", err)
	}
	// The message is redelivered under a fresh claim.
	if _, err := q.ReceiveChannel(ctx, "c"); err != nil {
		t.Fatalf("second receive: %v", err)
	}
	// The first receipt is now stale.
	if err := q.Ack(ctx, first.Receipt()); !errors.Is(err, pgqueue.ErrClaimExpired) {
		t.Fatalf("expected ErrClaimExpired for stale receipt, got %v", err)
	}
}

// TestFakeTopicFanOut verifies that every subscriber receives each topic message.
func TestFakeTopicFanOut(t *testing.T) {
	ctx := context.Background()
	q := fake.New()
	if err := q.CreateTopic(ctx, "events"); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for _, sub := range []string{"a", "b"} {
		if err := q.Subscribe(ctx, "events", sub); err != nil {
			t.Fatalf("subscribe %s: %v", sub, err)
		}
	}
	if _, err := q.PublishTopic(ctx, "events", []byte("evt")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	for _, sub := range []string{"a", "b"} {
		msg, err := q.ReceiveTopic(ctx, "events", sub)
		if err != nil {
			t.Fatalf("receive %s: %v", sub, err)
		}
		if string(msg.Payload) != "evt" {
			t.Fatalf("subscriber %s got %q", sub, msg.Payload)
		}
		if err := q.Ack(ctx, msg.Receipt()); err != nil {
			t.Fatalf("ack %s: %v", sub, err)
		}
	}
}

// TestFakePauseBlocksConsumption verifies pause/resume semantics.
func TestFakePauseBlocksConsumption(t *testing.T) {
	ctx := context.Background()
	q := fake.New()
	if err := q.CreateChannel(ctx, "p"); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := q.PublishChannel(ctx, "p", []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := q.PauseChannel(ctx, "p"); err != nil {
		t.Fatalf("pause: %v", err)
	}
	if _, err := q.ReceiveChannel(ctx, "p"); !errors.Is(err, pgqueue.ErrQueuePaused) {
		t.Fatalf("expected ErrQueuePaused, got %v", err)
	}
	if err := q.ResumeChannel(ctx, "p"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if _, err := q.ReceiveChannel(ctx, "p"); err != nil {
		t.Fatalf("receive after resume: %v", err)
	}
}

// TestFakeHandlerConsume covers the handler-based ConsumeChannel loop.
func TestFakeHandlerConsume(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q := fake.New()
	if err := q.CreateChannel(ctx, "h"); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	const total = 3
	for i := range total {
		if _, err := q.PublishChannel(ctx, "h", []byte{byte('0' + i)}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	done := make(chan struct{})
	consumed := 0
	go func() {
		_ = q.ConsumeChannel(ctx, "h", func(_ context.Context, _ *pgqueue.Message) error {
			consumed++
			if consumed == total {
				close(done)
			}
			return nil
		})
	}()

	<-done
	cancel()
	if consumed != total {
		t.Fatalf("expected %d consumed, got %d", total, consumed)
	}
}
