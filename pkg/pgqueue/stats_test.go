package pgqueue_test

import (
	"context"
	"testing"
	"time"

	"github.com/sgaunet/pgqueue/pkg/pgqueue"
)

func TestGetStats(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel
	err := pq.CreateChannel(ctx, "stats-test", pgqueue.ChannelOptions{})
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
	stats, err := pq.GetStats(ctx, "stats-test", pgqueue.QueueTypeChannel)
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
	stats, err = pq.GetStats(ctx, "stats-test", pgqueue.QueueTypeChannel)
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
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel
	err := pq.CreateChannel(ctx, "depth-test", pgqueue.ChannelOptions{})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Initial depth should be 0
	depth, err := pq.GetQueueDepth(ctx, "depth-test", pgqueue.QueueTypeChannel)
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
	depth, err = pq.GetQueueDepth(ctx, "depth-test", pgqueue.QueueTypeChannel)
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
	depth, err = pq.GetQueueDepth(ctx, "depth-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get queue depth: %v", err)
	}
	if depth != 10 {
		t.Errorf("expected depth 10 after consuming, got %d", depth)
	}
}

func TestGetSubscriberLag(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test topic
	err := pq.CreateTopic(ctx, "lag-test", pgqueue.TopicOptions{})
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
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel with max retries
	err := pq.CreateChannel(ctx, "dlq-stats-test", pgqueue.ChannelOptions{
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Initial DLQ stats should be empty
	dlqStats, err := pq.GetDLQStats(ctx, "dlq-stats-test", pgqueue.QueueTypeChannel)
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
	dlqStats, err = pq.GetDLQStats(ctx, "dlq-stats-test", pgqueue.QueueTypeChannel)
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

func TestGetSubscriberHealth(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateTopic(ctx, "health-test", pgqueue.TopicOptions{})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	if err := pq.Subscribe(ctx, "health-test", "sub-healthy"); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	if err := pq.Subscribe(ctx, "health-test", "sub-lagging"); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	// Publish messages
	for i := 0; i < 5; i++ {
		if _, err := pq.Publish(ctx, "health-test", []byte("test message")); err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}
	}

	// sub-healthy consumes and acks all messages
	for i := 0; i < 5; i++ {
		msg, err := pq.ConsumeFromTopic(ctx, "health-test", "sub-healthy", 30*time.Second)
		if err != nil {
			t.Fatalf("failed to consume message: %v", err)
		}
		if err := pq.AckTopic(ctx, "health-test", "sub-healthy", msg.ID); err != nil {
			t.Fatalf("failed to ack message: %v", err)
		}
	}

	// sub-lagging does nothing — all messages stay pending

	// Check healthy subscriber
	health, err := pq.GetSubscriberHealth(ctx, "health-test", "sub-healthy")
	if err != nil {
		t.Fatalf("failed to get subscriber health: %v", err)
	}
	if health.PendingMessages != 0 {
		t.Errorf("expected 0 pending for healthy sub, got %d", health.PendingMessages)
	}
	if health.StuckMessages != 0 {
		t.Errorf("expected 0 stuck for healthy sub, got %d", health.StuckMessages)
	}
	if health.LastActivity == nil {
		t.Error("expected last activity to be set for healthy sub")
	}

	// Check lagging subscriber
	health, err = pq.GetSubscriberHealth(ctx, "health-test", "sub-lagging")
	if err != nil {
		t.Fatalf("failed to get subscriber health: %v", err)
	}
	if health.PendingMessages != 5 {
		t.Errorf("expected 5 pending for lagging sub, got %d", health.PendingMessages)
	}
	if health.StuckMessages != 0 {
		t.Errorf("expected 0 stuck (nothing consumed), got %d", health.StuckMessages)
	}
	if health.OldestPending == nil {
		t.Error("expected oldest pending to be set for lagging sub")
	}
	if health.LastActivity != nil {
		t.Error("expected no last activity for lagging sub")
	}
}

func TestGetSubscriberHealthStuckMessages(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateTopic(ctx, "stuck-test", pgqueue.TopicOptions{})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	if err := pq.Subscribe(ctx, "stuck-test", "sub-stuck"); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := pq.Publish(ctx, "stuck-test", []byte("test message")); err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}
	}

	// Consume messages with a short visibility timeout
	for i := 0; i < 3; i++ {
		_, err := pq.ConsumeFromTopic(ctx, "stuck-test", "sub-stuck", 1*time.Millisecond)
		if err != nil {
			t.Fatalf("failed to consume message: %v", err)
		}
	}

	// Backdate visibility timeouts so they appear expired
	_, err = db.ExecContext(ctx,
		"UPDATE pgqueue_sub_stuck_test SET visibility_timeout = NOW() - INTERVAL '1 hour' WHERE status = 'processing'")
	if err != nil {
		t.Fatalf("failed to backdate visibility timeouts: %v", err)
	}

	health, err := pq.GetSubscriberHealth(ctx, "stuck-test", "sub-stuck")
	if err != nil {
		t.Fatalf("failed to get subscriber health: %v", err)
	}

	if health.StuckMessages != 3 {
		t.Errorf("expected 3 stuck messages, got %d", health.StuckMessages)
	}
}

func TestGetUnhealthySubscribers(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create two topics
	err := pq.CreateTopic(ctx, "unhealthy-a", pgqueue.TopicOptions{})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	err = pq.CreateTopic(ctx, "unhealthy-b", pgqueue.TopicOptions{})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Subscribe
	if err := pq.Subscribe(ctx, "unhealthy-a", "sub-ok"); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	if err := pq.Subscribe(ctx, "unhealthy-a", "sub-stuck"); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	if err := pq.Subscribe(ctx, "unhealthy-b", "sub-lagging"); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	// Publish to both topics
	for i := 0; i < 3; i++ {
		if _, err := pq.Publish(ctx, "unhealthy-a", []byte("msg")); err != nil {
			t.Fatalf("failed to publish: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := pq.Publish(ctx, "unhealthy-b", []byte("msg")); err != nil {
			t.Fatalf("failed to publish: %v", err)
		}
	}

	// sub-ok acks all its messages on topic-a
	for i := 0; i < 3; i++ {
		msg, err := pq.ConsumeFromTopic(ctx, "unhealthy-a", "sub-ok", 30*time.Second)
		if err != nil {
			t.Fatalf("failed to consume: %v", err)
		}
		if err := pq.AckTopic(ctx, "unhealthy-a", "sub-ok", msg.ID); err != nil {
			t.Fatalf("failed to ack: %v", err)
		}
	}

	// sub-stuck consumes on topic-a but doesn't ack — force expired visibility
	for i := 0; i < 3; i++ {
		_, err := pq.ConsumeFromTopic(ctx, "unhealthy-a", "sub-stuck", 1*time.Millisecond)
		if err != nil {
			t.Fatalf("failed to consume: %v", err)
		}
	}
	_, err = db.ExecContext(ctx,
		"UPDATE pgqueue_sub_unhealthy_a SET visibility_timeout = NOW() - INTERVAL '1 hour' WHERE subscriber_id = 'sub-stuck' AND status = 'processing'")
	if err != nil {
		t.Fatalf("failed to backdate: %v", err)
	}

	// sub-lagging on topic-b: backdate pending messages to make them old
	_, err = db.ExecContext(ctx,
		"UPDATE pgqueue_sub_unhealthy_b SET created_at = NOW() - INTERVAL '1 hour'")
	if err != nil {
		t.Fatalf("failed to backdate: %v", err)
	}

	// With a 30-minute threshold, sub-stuck (stuck msgs) and sub-lagging (old pending) should be unhealthy
	unhealthy, err := pq.GetUnhealthySubscribers(ctx, 30*time.Minute)
	if err != nil {
		t.Fatalf("failed to get unhealthy subscribers: %v", err)
	}

	if len(unhealthy) != 2 {
		t.Fatalf("expected 2 unhealthy subscribers, got %d", len(unhealthy))
	}

	// Verify we find both expected subscribers
	found := map[string]bool{}
	for _, h := range unhealthy {
		found[h.TopicName+"/"+h.SubscriberID] = true
	}

	if !found["unhealthy-a/sub-stuck"] {
		t.Error("expected sub-stuck on unhealthy-a to be unhealthy")
	}
	if !found["unhealthy-b/sub-lagging"] {
		t.Error("expected sub-lagging on unhealthy-b to be unhealthy")
	}
}

func TestGetUnhealthySubscribersNoTopics(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	unhealthy, err := pq.GetUnhealthySubscribers(ctx, 5*time.Minute)
	if err != nil {
		t.Fatalf("failed to get unhealthy subscribers: %v", err)
	}

	if len(unhealthy) != 0 {
		t.Errorf("expected 0 unhealthy subscribers, got %d", len(unhealthy))
	}
}

func TestPubSubStats(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a topic
	err := pq.CreateTopic(ctx, "pubsub-stats-test", pgqueue.TopicOptions{})
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
	stats, err := pq.GetStats(ctx, "pubsub-stats-test", pgqueue.QueueTypePubSub)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}

	// 5 messages * 2 subscribers = 10 subscription records
	if stats.PendingCount != 10 {
		t.Errorf("expected 10 pending subscriptions (5 messages * 2 subscribers), got %d", stats.PendingCount)
	}
}
