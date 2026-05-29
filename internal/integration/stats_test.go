package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sgaunet/pgqueue"
)

func TestStats(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel
	err := pq.CreateChannel(ctx, "stats-test")
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
	stats, err := pq.Stats(ctx, "stats-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}

	if stats.PendingCount != 10 {
		t.Errorf("expected 10 pending messages, got %d", stats.PendingCount)
	}

	// Consume and ack 5 messages
	for i := 0; i < 5; i++ {
		msg, err := pq.ReceiveChannel(ctx, "stats-test", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("failed to consume message: %v", err)
		}
		if err := pq.Ack(ctx, msg.Receipt()); err != nil {
			t.Fatalf("failed to ack message: %v", err)
		}
	}

	// Get updated stats
	stats, err = pq.Stats(ctx, "stats-test", pgqueue.QueueTypeChannel)
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

func TestQueueDepth(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel
	err := pq.CreateChannel(ctx, "depth-test")
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Initial depth should be 0
	depth, err := pq.QueueDepth(ctx, "depth-test", pgqueue.QueueTypeChannel)
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
	depth, err = pq.QueueDepth(ctx, "depth-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get queue depth: %v", err)
	}
	if depth != 20 {
		t.Errorf("expected depth 20, got %d", depth)
	}

	// Consume 10 messages
	for i := 0; i < 10; i++ {
		_, err := pq.ReceiveChannel(ctx, "depth-test", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("failed to consume message: %v", err)
		}
	}

	// Depth should still be 10 (consumed but not acked are processing, not pending)
	depth, err = pq.QueueDepth(ctx, "depth-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get queue depth: %v", err)
	}
	if depth != 10 {
		t.Errorf("expected depth 10 after consuming, got %d", depth)
	}
}

func TestSubscriberLag(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test topic
	err := pq.CreateTopic(ctx, "lag-test")
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
	lag, err := pq.SubscriberLag(ctx, "lag-test", "subscriber-1")
	if err != nil {
		t.Fatalf("failed to get subscriber lag: %v", err)
	}

	if lag.PendingCount != 10 {
		t.Errorf("expected 10 pending messages, got %d", lag.PendingCount)
	}

	// Consume and ack 5 messages
	for i := 0; i < 5; i++ {
		msg, err := pq.ReceiveTopic(ctx, "lag-test", "subscriber-1", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("failed to consume message: %v", err)
		}
		if err := pq.Ack(ctx, msg.Receipt()); err != nil {
			t.Fatalf("failed to ack message: %v", err)
		}
	}

	// Get updated lag
	lag, err = pq.SubscriberLag(ctx, "lag-test", "subscriber-1")
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

func TestDLQStats(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel with max retries
	err := pq.CreateChannel(ctx, "dlq-stats-test", pgqueue.WithQueueMaxRetries(1))
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Initial DLQ stats should be empty
	dlqStats, err := pq.DLQStats(ctx, "dlq-stats-test", pgqueue.QueueTypeChannel)
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
			msg, err := pq.ReceiveChannel(ctx, "dlq-stats-test", pgqueue.WithVisibilityTimeout(30*time.Second))
			if err != nil {
				t.Fatalf("failed to consume message: %v", err)
			}
			if err := pq.Nack(ctx, msg.Receipt(), "test failure"); err != nil {
				t.Fatalf("failed to nack message: %v", err)
			}
		}
	}

	// Get DLQ stats
	dlqStats, err = pq.DLQStats(ctx, "dlq-stats-test", pgqueue.QueueTypeChannel)
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

func TestSubscriberHealth(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateTopic(ctx, "health-test")
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
		msg, err := pq.ReceiveTopic(ctx, "health-test", "sub-healthy", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("failed to consume message: %v", err)
		}
		if err := pq.Ack(ctx, msg.Receipt()); err != nil {
			t.Fatalf("failed to ack message: %v", err)
		}
	}

	// sub-lagging does nothing — all messages stay pending

	// Check healthy subscriber
	health, err := pq.SubscriberHealth(ctx, "health-test", "sub-healthy")
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
	health, err = pq.SubscriberHealth(ctx, "health-test", "sub-lagging")
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

func TestSubscriberHealthStuckMessages(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateTopic(ctx, "stuck-test")
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

	// Consume the messages with a long visibility timeout so all 3 are claimed
	// as distinct messages; the backdating step below makes them appear stuck.
	// (A short timeout would let each consume reclaim the previous, now-expired
	// message instead of a fresh one.)
	for i := 0; i < 3; i++ {
		_, err := pq.ReceiveTopic(ctx, "stuck-test", "sub-stuck", pgqueue.WithVisibilityTimeout(time.Minute))
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

	health, err := pq.SubscriberHealth(ctx, "stuck-test", "sub-stuck")
	if err != nil {
		t.Fatalf("failed to get subscriber health: %v", err)
	}

	if health.StuckMessages != 3 {
		t.Errorf("expected 3 stuck messages, got %d", health.StuckMessages)
	}
}

func TestUnhealthySubscribers(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create two topics
	err := pq.CreateTopic(ctx, "unhealthy-a")
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	err = pq.CreateTopic(ctx, "unhealthy-b")
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
		msg, err := pq.ReceiveTopic(ctx, "unhealthy-a", "sub-ok", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("failed to consume: %v", err)
		}
		if err := pq.Ack(ctx, msg.Receipt()); err != nil {
			t.Fatalf("failed to ack: %v", err)
		}
	}

	// sub-stuck consumes on topic-a but doesn't ack — force expired visibility
	for i := 0; i < 3; i++ {
		_, err := pq.ReceiveTopic(ctx, "unhealthy-a", "sub-stuck", pgqueue.WithVisibilityTimeout(1*time.Millisecond))
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
	unhealthy, err := pq.UnhealthySubscribers(ctx, 30*time.Minute)
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

func TestUnhealthySubscribersNoTopics(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	unhealthy, err := pq.UnhealthySubscribers(ctx, 5*time.Minute)
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
	err := pq.CreateTopic(ctx, "pubsub-stats-test")
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
	stats, err := pq.Stats(ctx, "pubsub-stats-test", pgqueue.QueueTypePubSub)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}

	// 5 messages * 2 subscribers = 10 subscription records
	if stats.PendingCount != 10 {
		t.Errorf("expected 10 pending subscriptions (5 messages * 2 subscribers), got %d", stats.PendingCount)
	}
}

// TestStatsAPIRejectsAfterClose (R-18) verifies that every public stats method
// returns ErrQueueClosed once the Queue has been closed.
func TestStatsAPIRejectsAfterClose(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a channel and a topic so the methods have real queues to target;
	// they must still reject because the handle is closed.
	if err := pq.CreateChannel(ctx, "closed-stats-ch"); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}
	if err := pq.CreateTopic(ctx, "closed-stats-topic"); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	if err := pq.Subscribe(ctx, "closed-stats-topic", "sub1"); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	if err := pq.Close(); err != nil {
		t.Fatalf("Close() failed: %v", err)
	}

	checks := []struct {
		name string
		call func() error
	}{
		{"Stats", func() error {
			_, err := pq.Stats(ctx, "closed-stats-ch", pgqueue.QueueTypeChannel)
			return err
		}},
		{"QueueDepth", func() error {
			_, err := pq.QueueDepth(ctx, "closed-stats-ch", pgqueue.QueueTypeChannel)
			return err
		}},
		{"SubscriberLag", func() error {
			_, err := pq.SubscriberLag(ctx, "closed-stats-topic", "sub1")
			return err
		}},
		{"DLQStats", func() error {
			_, err := pq.DLQStats(ctx, "closed-stats-ch", pgqueue.QueueTypeChannel)
			return err
		}},
		{"SubscriberHealth", func() error {
			_, err := pq.SubscriberHealth(ctx, "closed-stats-topic", "sub1")
			return err
		}},
		{"UnhealthySubscribers", func() error {
			_, err := pq.UnhealthySubscribers(ctx, 5*time.Minute)
			return err
		}},
	}

	for _, c := range checks {
		if err := c.call(); !errors.Is(err, pgqueue.ErrQueueClosed) {
			t.Errorf("%s after Close: got %v, want ErrQueueClosed", c.name, err)
		}
	}
}

// TestStatsAgesNeverNegative (R-19) verifies that age values reported by the
// stats API are never negative — the age must be computed in SQL (NOW() -
// created_at) so it cannot go negative due to host/DB clock skew.
func TestStatsAgesNeverNegative(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	const channelName = "age-test-ch"
	if err := pq.CreateChannel(ctx, channelName); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish a handful of messages; the freshly published rows have the most
	// clock-skew-sensitive ages.
	for range 10 {
		if _, err := pq.Publish(ctx, channelName, []byte("msg")); err != nil {
			t.Fatalf("failed to publish: %v", err)
		}
	}

	stats, err := pq.Stats(ctx, channelName, pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if stats.OldestPendingAge != nil && *stats.OldestPendingAge < 0 {
		t.Errorf("OldestPendingAge is negative: %v", *stats.OldestPendingAge)
	}

	// Subscriber-side ages too.
	const topic = "age-test-topic"
	if err := pq.CreateTopic(ctx, topic); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	if err := pq.Subscribe(ctx, topic, "sub-age"); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	for range 5 {
		if _, err := pq.Publish(ctx, topic, []byte("msg")); err != nil {
			t.Fatalf("failed to publish to topic: %v", err)
		}
	}

	lag, err := pq.SubscriberLag(ctx, topic, "sub-age")
	if err != nil {
		t.Fatalf("failed to get subscriber lag: %v", err)
	}
	if lag.OldestPendingAge != nil && *lag.OldestPendingAge < 0 {
		t.Errorf("subscriber OldestPendingAge is negative: %v", *lag.OldestPendingAge)
	}

	// SubscriberHealth.OldestPending is a wall-clock timestamp; its age relative
	// to now must not be in the future (which would imply a negative age).
	health, err := pq.SubscriberHealth(ctx, topic, "sub-age")
	if err != nil {
		t.Fatalf("failed to get subscriber health: %v", err)
	}
	if health.OldestPending != nil && time.Since(*health.OldestPending) < 0 {
		t.Errorf("SubscriberHealth.OldestPending is in the future: %v", *health.OldestPending)
	}
}
