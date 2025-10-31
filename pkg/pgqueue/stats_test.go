package pgqueue

import (
	"context"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestGetStats(t *testing.T) {
	pq, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel
	err := pq.CreateChannel(ctx, "stats-test", ChannelOptions{})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish messages
	for i := 0; i < 10; i++ {
		if _, err := pq.Publish(ctx, "stats-test", []byte("test message")); err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}
	}

	// Get initial stats
	stats, err := pq.GetStats(ctx, "stats-test", QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}

	if stats.PendingCount != 10 {
		t.Errorf("expected 10 pending messages, got %d", stats.PendingCount)
	}

	// Consume and ack 5 messages
	for i := 0; i < 5; i++ {
		msg, err := pq.ConsumeFromChannel(ctx, "stats-test", 30*time.Second)
		if err != nil {
			t.Fatalf("failed to consume message: %v", err)
		}
		if err := pq.AckChannel(ctx, "stats-test", msg.ID); err != nil {
			t.Fatalf("failed to ack message: %v", err)
		}
	}

	// Get updated stats
	stats, err = pq.GetStats(ctx, "stats-test", QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}

	if stats.PendingCount != 5 {
		t.Errorf("expected 5 pending messages, got %d", stats.PendingCount)
	}

	if stats.CompletedCount != 5 {
		t.Errorf("expected 5 completed messages, got %d", stats.CompletedCount)
	}

	if stats.AvgProcessingTime == nil {
		t.Error("expected avg processing time to be set")
	}

	if stats.OldestPendingAge == nil {
		t.Error("expected oldest pending age to be set")
	}
}

func TestGetQueueDepth(t *testing.T) {
	pq, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel
	err := pq.CreateChannel(ctx, "depth-test", ChannelOptions{})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Initial depth should be 0
	depth, err := pq.GetQueueDepth(ctx, "depth-test", QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get queue depth: %v", err)
	}
	if depth != 0 {
		t.Errorf("expected depth 0, got %d", depth)
	}

	// Publish messages
	for i := 0; i < 20; i++ {
		if _, err := pq.Publish(ctx, "depth-test", []byte("test message")); err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}
	}

	// Depth should be 20
	depth, err = pq.GetQueueDepth(ctx, "depth-test", QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get queue depth: %v", err)
	}
	if depth != 20 {
		t.Errorf("expected depth 20, got %d", depth)
	}

	// Consume 10 messages
	for i := 0; i < 10; i++ {
		_, err := pq.ConsumeFromChannel(ctx, "depth-test", 30*time.Second)
		if err != nil {
			t.Fatalf("failed to consume message: %v", err)
		}
	}

	// Depth should still be 10 (consumed but not acked are processing, not pending)
	depth, err = pq.GetQueueDepth(ctx, "depth-test", QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get queue depth: %v", err)
	}
	if depth != 10 {
		t.Errorf("expected depth 10 after consuming, got %d", depth)
	}
}

func TestGetSubscriberLag(t *testing.T) {
	pq, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test topic
	err := pq.CreateTopic(ctx, "lag-test", TopicOptions{})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Subscribe
	if err := pq.Subscribe(ctx, "lag-test", "subscriber-1"); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	// Publish messages
	for i := 0; i < 10; i++ {
		if _, err := pq.Publish(ctx, "lag-test", []byte("test message")); err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}
	}

	// Get subscriber lag
	lag, err := pq.GetSubscriberLag(ctx, "lag-test", "subscriber-1")
	if err != nil {
		t.Fatalf("failed to get subscriber lag: %v", err)
	}

	if lag.PendingCount != 10 {
		t.Errorf("expected 10 pending messages, got %d", lag.PendingCount)
	}

	// Consume and ack 5 messages
	for i := 0; i < 5; i++ {
		msg, err := pq.ConsumeFromTopic(ctx, "lag-test", "subscriber-1", 30*time.Second)
		if err != nil {
			t.Fatalf("failed to consume message: %v", err)
		}
		if err := pq.AckTopic(ctx, "lag-test", "subscriber-1", msg.ID); err != nil {
			t.Fatalf("failed to ack message: %v", err)
		}
	}

	// Get updated lag
	lag, err = pq.GetSubscriberLag(ctx, "lag-test", "subscriber-1")
	if err != nil {
		t.Fatalf("failed to get subscriber lag: %v", err)
	}

	if lag.PendingCount != 5 {
		t.Errorf("expected 5 pending messages after processing, got %d", lag.PendingCount)
	}

	if lag.AckedCount != 5 {
		t.Errorf("expected 5 acked messages, got %d", lag.AckedCount)
	}
}

func TestGetDLQStats(t *testing.T) {
	pq, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel with max retries
	err := pq.CreateChannel(ctx, "dlq-stats-test", ChannelOptions{
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Initial DLQ stats should be empty
	dlqStats, err := pq.GetDLQStats(ctx, "dlq-stats-test", QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get DLQ stats: %v", err)
	}
	if dlqStats.TotalCount != 0 {
		t.Errorf("expected 0 DLQ messages initially, got %d", dlqStats.TotalCount)
	}

	// Publish and fail messages
	for i := 0; i < 5; i++ {
		if _, err := pq.Publish(ctx, "dlq-stats-test", []byte("test message")); err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}

		// Consume and nack twice to send to DLQ
		for j := 0; j < 2; j++ {
			msg, err := pq.ConsumeFromChannel(ctx, "dlq-stats-test", 30*time.Second)
			if err != nil {
				t.Fatalf("failed to consume message: %v", err)
			}
			if err := pq.NackChannel(ctx, "dlq-stats-test", msg.ID, "test failure"); err != nil {
				t.Fatalf("failed to nack message: %v", err)
			}
		}
	}

	// Get DLQ stats
	dlqStats, err = pq.GetDLQStats(ctx, "dlq-stats-test", QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get DLQ stats: %v", err)
	}

	if dlqStats.TotalCount != 5 {
		t.Errorf("expected 5 DLQ messages, got %d", dlqStats.TotalCount)
	}

	if dlqStats.OldestMovedAt == nil {
		t.Error("expected oldest moved at to be set")
	}

	if dlqStats.NewestMovedAt == nil {
		t.Error("expected newest moved at to be set")
	}

	if dlqStats.AvgRetryCount == 0 {
		t.Error("expected avg retry count to be > 0")
	}
}

func TestPubSubStats(t *testing.T) {
	pq, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a topic
	err := pq.CreateTopic(ctx, "pubsub-stats-test", TopicOptions{})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Subscribe with 2 subscribers
	if err := pq.Subscribe(ctx, "pubsub-stats-test", "sub-1"); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	if err := pq.Subscribe(ctx, "pubsub-stats-test", "sub-2"); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	// Publish messages
	for i := 0; i < 5; i++ {
		if _, err := pq.Publish(ctx, "pubsub-stats-test", []byte("test message")); err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}
	}

	// Get stats (should show subscriptions)
	stats, err := pq.GetStats(ctx, "pubsub-stats-test", QueueTypePubSub)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}

	// 5 messages * 2 subscribers = 10 subscription records
	if stats.PendingCount != 10 {
		t.Errorf("expected 10 pending subscriptions (5 messages * 2 subscribers), got %d", stats.PendingCount)
	}
}
