package pgqueue_test

import (
	"context"
	"testing"
	"time"

	"github.com/sgaunet/pgqueue/pkg/pgqueue"
)

func TestReplayFrom(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel
	err := pq.CreateChannel(ctx, "replay-test", pgqueue.ChannelOptions{})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Record start time
	startTime := time.Now()
	time.Sleep(10 * time.Millisecond)

	// Publish and process some messages
	for i := 0; i < 5; i++ {
		if _, err := pq.Publish(ctx, "replay-test", []byte("test message")); err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}
	}

	// Consume and ack all messages
	for i := 0; i < 5; i++ {
		msg, err := pq.ConsumeFromChannel(ctx, "replay-test", 30*time.Second)
		if err != nil {
			t.Fatalf("failed to consume message: %v", err)
		}
		if err := pq.AckChannel(ctx, "replay-test", msg.ID); err != nil {
			t.Fatalf("failed to ack message: %v", err)
		}
	}

	// Verify all completed
	stats, err := pq.GetStats(ctx, "replay-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if stats.CompletedCount != 5 {
		t.Errorf("expected 5 completed messages, got %d", stats.CompletedCount)
	}

	// Dry-run replay
	count, err := pq.ReplayFrom(ctx, "replay-test", pgqueue.QueueTypeChannel, startTime, pgqueue.ReplayOptions{
		DryRun: true,
	})
	if err != nil {
		t.Fatalf("dry-run replay failed: %v", err)
	}
	if count != 5 {
		t.Errorf("expected dry-run to report 5 messages, got %d", count)
	}

	// Replay without confirmation should fail
	_, err = pq.ReplayFrom(ctx, "replay-test", pgqueue.QueueTypeChannel, startTime, pgqueue.ReplayOptions{})
	if err == nil {
		t.Error("expected error when replaying without confirmation")
	}

	// Replay with confirmation
	count, err = pq.ReplayFrom(ctx, "replay-test", pgqueue.QueueTypeChannel, startTime, pgqueue.ReplayOptions{
		Confirm:     true,
		PerformedBy: "test-user",
	})
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if count != 5 {
		t.Errorf("expected to replay 5 messages, got %d", count)
	}

	// Verify messages are pending again
	stats, err = pq.GetStats(ctx, "replay-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if stats.PendingCount != 5 {
		t.Errorf("expected 5 pending messages after replay, got %d", stats.PendingCount)
	}
	if stats.CompletedCount != 0 {
		t.Errorf("expected 0 completed messages after replay, got %d", stats.CompletedCount)
	}

	// Verify replay history
	history, err := pq.GetReplayHistory(ctx, "replay-test", pgqueue.QueueTypeChannel, 10)
	if err != nil {
		t.Fatalf("failed to get replay history: %v", err)
	}
	if len(history) != 1 {
		t.Errorf("expected 1 replay history entry, got %d", len(history))
	}
}

func TestReplayFromWithLimit(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "replay-limit", pgqueue.ChannelOptions{})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	startTime := time.Now()
	time.Sleep(10 * time.Millisecond)

	// Publish and complete 5 messages
	for i := 0; i < 5; i++ {
		if _, err := pq.Publish(ctx, "replay-limit", []byte("msg")); err != nil {
			t.Fatalf("failed to publish: %v", err)
		}
	}
	for i := 0; i < 5; i++ {
		msg, err := pq.ConsumeFromChannel(ctx, "replay-limit", 30*time.Second)
		if err != nil {
			t.Fatalf("failed to consume: %v", err)
		}
		if err := pq.AckChannel(ctx, "replay-limit", msg.ID); err != nil {
			t.Fatalf("failed to ack: %v", err)
		}
	}

	// Replay with limit=2 — only 2 of the 5 completed messages should be replayed
	count, err := pq.ReplayFrom(ctx, "replay-limit", pgqueue.QueueTypeChannel, startTime, pgqueue.ReplayOptions{
		Confirm: true,
		Limit:   2,
	})
	if err != nil {
		t.Fatalf("replay with limit failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 replayed messages, got %d", count)
	}

	stats, err := pq.GetStats(ctx, "replay-limit", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if stats.PendingCount != 2 {
		t.Errorf("expected 2 pending messages, got %d", stats.PendingCount)
	}
	if stats.CompletedCount != 3 {
		t.Errorf("expected 3 completed messages, got %d", stats.CompletedCount)
	}
}

func TestReplayMessage(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel
	err := pq.CreateChannel(ctx, "replay-msg-test", pgqueue.ChannelOptions{})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish a message
	if _, err := pq.Publish(ctx, "replay-msg-test", []byte("test message")); err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	// Consume and ack
	msg, err := pq.ConsumeFromChannel(ctx, "replay-msg-test", 30*time.Second)
	if err != nil {
		t.Fatalf("failed to consume message: %v", err)
	}
	if err := pq.AckChannel(ctx, "replay-msg-test", msg.ID); err != nil {
		t.Fatalf("failed to ack message: %v", err)
	}

	// Verify completed
	stats, err := pq.GetStats(ctx, "replay-msg-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if stats.CompletedCount != 1 {
		t.Errorf("expected 1 completed message, got %d", stats.CompletedCount)
	}

	// Replay the specific message
	err = pq.ReplayMessage(ctx, "replay-msg-test", pgqueue.QueueTypeChannel, msg.ID, pgqueue.ReplayOptions{
		Confirm:     true,
		PerformedBy: "test-user",
	})
	if err != nil {
		t.Fatalf("replay message failed: %v", err)
	}

	// Verify message is pending again
	stats, err = pq.GetStats(ctx, "replay-msg-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if stats.PendingCount != 1 {
		t.Errorf("expected 1 pending message after replay, got %d", stats.PendingCount)
	}
}

func TestReplayDLQ(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel with max retries
	err := pq.CreateChannel(ctx, "replay-dlq-test", pgqueue.ChannelOptions{
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish a message
	if _, err := pq.Publish(ctx, "replay-dlq-test", []byte("test message")); err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	// Consume and nack twice to send to DLQ
	for i := 0; i < 2; i++ {
		msg, err := pq.ConsumeFromChannel(ctx, "replay-dlq-test", 30*time.Second)
		if err != nil {
			t.Fatalf("failed to consume message on attempt %d: %v", i+1, err)
		}
		if msg == nil {
			t.Fatalf("consume returned nil message on attempt %d", i+1)
		}
		if err := pq.NackChannel(ctx, "replay-dlq-test", msg.ID, "test failure"); err != nil {
			t.Fatalf("failed to nack message on attempt %d: %v", i+1, err)
		}
	}

	// Verify message in DLQ
	dlqStats, err := pq.GetDLQStats(ctx, "replay-dlq-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get DLQ stats: %v", err)
	}
	if dlqStats.TotalCount != 1 {
		t.Errorf("expected 1 message in DLQ, got %d", dlqStats.TotalCount)
	}

	// Dry-run DLQ replay
	count, err := pq.ReplayDLQ(ctx, "replay-dlq-test", pgqueue.QueueTypeChannel, pgqueue.ReplayOptions{
		DryRun: true,
	})
	if err != nil {
		t.Fatalf("dry-run DLQ replay failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected dry-run to report 1 message, got %d", count)
	}

	// Replay from DLQ
	count, err = pq.ReplayDLQ(ctx, "replay-dlq-test", pgqueue.QueueTypeChannel, pgqueue.ReplayOptions{
		Confirm:     true,
		PerformedBy: "test-user",
	})
	if err != nil {
		t.Fatalf("DLQ replay failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected to replay 1 message from DLQ, got %d", count)
	}

	// Verify message is back in main queue
	depth, err := pq.GetQueueDepth(ctx, "replay-dlq-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get queue depth: %v", err)
	}
	if depth != 1 {
		t.Errorf("expected 1 pending message after DLQ replay, got %d", depth)
	}

	// Verify DLQ is empty
	dlqStats, err = pq.GetDLQStats(ctx, "replay-dlq-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get DLQ stats: %v", err)
	}
	if dlqStats.TotalCount != 0 {
		t.Errorf("expected 0 messages in DLQ after replay, got %d", dlqStats.TotalCount)
	}
}

func TestReplayDLQPubSub(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create topic with max 1 retry so messages go to DLQ after 2 nacks
	err := pq.CreateTopic(ctx, "replay-dlq-pubsub", pgqueue.TopicOptions{
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	err = pq.Subscribe(ctx, "replay-dlq-pubsub", "sub1")
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	if _, err := pq.Publish(ctx, "replay-dlq-pubsub", []byte("replay-me")); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// Nack until message goes to DLQ
	for i := 0; i < 2; i++ {
		msg, err := pq.ConsumeFromTopic(ctx, "replay-dlq-pubsub", "sub1", 30*time.Second)
		if err != nil {
			t.Fatalf("consume %d failed: %v", i, err)
		}
		if msg == nil {
			t.Fatalf("consume %d returned nil", i)
		}
		err = pq.NackTopic(ctx, "replay-dlq-pubsub", "sub1", msg.ID, "fail")
		if err != nil {
			t.Fatalf("nack %d failed: %v", i, err)
		}
	}

	// Verify message in DLQ
	dlqStats, err := pq.GetDLQStats(ctx, "replay-dlq-pubsub", pgqueue.QueueTypePubSub)
	if err != nil {
		t.Fatalf("failed to get DLQ stats: %v", err)
	}
	if dlqStats.TotalCount != 1 {
		t.Errorf("expected 1 DLQ message, got %d", dlqStats.TotalCount)
	}

	// Replay from DLQ
	count, err := pq.ReplayDLQ(ctx, "replay-dlq-pubsub", pgqueue.QueueTypePubSub, pgqueue.ReplayOptions{
		Confirm: true,
	})
	if err != nil {
		t.Fatalf("replay DLQ failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 replayed, got %d", count)
	}

	// The replayed message must be consumable by the subscriber
	msg, err := pq.ConsumeFromTopic(ctx, "replay-dlq-pubsub", "sub1", 30*time.Second)
	if err != nil {
		t.Fatalf("consume after replay failed: %v", err)
	}
	if msg == nil {
		t.Fatal("expected message after DLQ replay, got nil")
	}
	if string(msg.Payload) != "replay-me" {
		t.Errorf("expected payload 'replay-me', got '%s'", msg.Payload)
	}
}
