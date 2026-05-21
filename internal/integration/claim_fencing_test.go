package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sgaunet/pgqueue/pkg/pgqueue"
)

// TestClaimFencing verifies a consumer whose visibility timeout lapsed cannot
// ack or nack a message that was redelivered to another consumer.
func TestClaimFencing(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("channel_stale_ack_rejected", func(t *testing.T) {
		if err := pq.CreateChannel(ctx, "fence-chan"); err != nil {
			t.Fatalf("create channel: %v", err)
		}
		if _, err := pq.Publish(ctx, "fence-chan", []byte("payload")); err != nil {
			t.Fatalf("publish: %v", err)
		}
		msgA, err := pq.ConsumeFromChannel(ctx, "fence-chan", 50*time.Millisecond)
		if err != nil || msgA == nil {
			t.Fatalf("consumer A consume: msg=%v err=%v", msgA, err)
		}
		time.Sleep(100 * time.Millisecond) // let A's visibility timeout lapse
		msgB, err := pq.ConsumeFromChannel(ctx, "fence-chan", 30*time.Second)
		if err != nil || msgB == nil {
			t.Fatalf("consumer B reclaim: msg=%v err=%v", msgB, err)
		}
		if msgA.ClaimID == msgB.ClaimID {
			t.Fatal("expected B to receive a fresh claim token")
		}
		if err := pq.AckChannel(ctx, "fence-chan", msgA.Receipt()); !errors.Is(err, pgqueue.ErrClaimExpired) {
			t.Errorf("stale AckChannel: got %v, want ErrClaimExpired", err)
		}
		if err := pq.AckChannel(ctx, "fence-chan", msgB.Receipt()); err != nil {
			t.Errorf("current AckChannel: %v", err)
		}
	})

	t.Run("channel_stale_nack_rejected", func(t *testing.T) {
		if err := pq.CreateChannel(ctx, "fence-chan-n"); err != nil {
			t.Fatalf("create channel: %v", err)
		}
		if _, err := pq.Publish(ctx, "fence-chan-n", []byte("payload")); err != nil {
			t.Fatalf("publish: %v", err)
		}
		msgA, err := pq.ConsumeFromChannel(ctx, "fence-chan-n", 50*time.Millisecond)
		if err != nil || msgA == nil {
			t.Fatalf("consumer A consume: msg=%v err=%v", msgA, err)
		}
		time.Sleep(100 * time.Millisecond)
		msgB, err := pq.ConsumeFromChannel(ctx, "fence-chan-n", 30*time.Second)
		if err != nil || msgB == nil {
			t.Fatalf("consumer B reclaim: msg=%v err=%v", msgB, err)
		}
		if err := pq.NackChannel(ctx, "fence-chan-n", msgA.Receipt(), "stale"); !errors.Is(err, pgqueue.ErrClaimExpired) {
			t.Errorf("stale NackChannel: got %v, want ErrClaimExpired", err)
		}
		if err := pq.AckChannel(ctx, "fence-chan-n", msgB.Receipt()); err != nil {
			t.Errorf("current AckChannel: %v", err)
		}
	})

	t.Run("topic_stale_ack_rejected", func(t *testing.T) {
		if err := pq.CreateTopic(ctx, "fence-topic"); err != nil {
			t.Fatalf("create topic: %v", err)
		}
		if err := pq.Subscribe(ctx, "fence-topic", "sub-1"); err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		if _, err := pq.Publish(ctx, "fence-topic", []byte("payload")); err != nil {
			t.Fatalf("publish: %v", err)
		}
		msgA, err := pq.ConsumeFromTopic(ctx, "fence-topic", "sub-1", 50*time.Millisecond)
		if err != nil || msgA == nil {
			t.Fatalf("consumer A consume: msg=%v err=%v", msgA, err)
		}
		time.Sleep(100 * time.Millisecond)
		msgB, err := pq.ConsumeFromTopic(ctx, "fence-topic", "sub-1", 30*time.Second)
		if err != nil || msgB == nil {
			t.Fatalf("consumer B reclaim: msg=%v err=%v", msgB, err)
		}
		if msgA.ClaimID == msgB.ClaimID {
			t.Fatal("expected B to receive a fresh claim token")
		}
		if err := pq.AckTopic(ctx, "fence-topic", "sub-1", msgA.Receipt()); !errors.Is(err, pgqueue.ErrClaimExpired) {
			t.Errorf("stale AckTopic: got %v, want ErrClaimExpired", err)
		}
		if err := pq.AckTopic(ctx, "fence-topic", "sub-1", msgB.Receipt()); err != nil {
			t.Errorf("current AckTopic: %v", err)
		}
	})
}
