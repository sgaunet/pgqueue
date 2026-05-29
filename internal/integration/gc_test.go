package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sgaunet/pgqueue"
)

func TestGarbageCollector(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel
	err := pq.CreateChannel(ctx, "gc-test", pgqueue.WithQueueMaxRetries(3))
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish some messages
	for i := 0; i < 5; i++ {
		if _, err := pq.Publish(ctx, "gc-test", []byte("test message")); err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}
	}

	// Consume and ack some messages
	for i := 0; i < 3; i++ {
		msg, err := pq.ReceiveChannel(ctx, "gc-test", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("failed to consume message: %v", err)
		}
		if err := pq.Ack(ctx, msg.Receipt()); err != nil {
			t.Fatalf("failed to ack message: %v", err)
		}
	}

	// Wait a bit for processed_at to be set
	time.Sleep(100 * time.Millisecond)

	// Run garbage collector to purge completed messages
	gcConfig := pgqueue.GarbageCollectorConfig{
		DefaultPolicy: pgqueue.RetentionPolicy{
			CompletedMessageTTL: 1 * time.Millisecond, // Very short TTL for testing
		},
	}
	gc := pgqueue.NewGarbageCollector(pq, gcConfig)

	// Wait for TTL to expire
	time.Sleep(10 * time.Millisecond)

	if err := gc.Collect(ctx); err != nil {
		t.Fatalf("garbage collection failed: %v", err)
	}

	// Check stats
	stats, err := pq.GetStats(ctx, "gc-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}

	// Should have 0 completed messages after GC, and 2 pending
	if stats.CompletedCount != 0 {
		t.Errorf("expected 0 completed messages after GC, got %d", stats.CompletedCount)
	}

	if stats.PendingCount != 2 {
		t.Errorf("expected 2 pending messages, got %d", stats.PendingCount)
	}
}

func TestGarbageCollectorVisibilityTimeout(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel
	err := pq.CreateChannel(ctx, "gc-timeout-test", pgqueue.WithQueueMaxRetries(3))
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish a message
	if _, err := pq.Publish(ctx, "gc-timeout-test", []byte("test message")); err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	// Consume but don't ack (simulating stuck processing)
	msg, err := pq.ReceiveChannel(ctx, "gc-timeout-test", pgqueue.WithVisibilityTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatalf("failed to consume message: %v", err)
	}

	// Verify message is in processing state
	stats, err := pq.GetStats(ctx, "gc-timeout-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if stats.ProcessingCount != 1 {
		t.Errorf("expected 1 processing message, got %d", stats.ProcessingCount)
	}

	// Wait for visibility timeout to expire
	time.Sleep(150 * time.Millisecond)

	// Run GC to reset timed-out messages
	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{})
	if err := gc.Collect(ctx); err != nil {
		t.Fatalf("garbage collection failed: %v", err)
	}

	// Check that message is back to pending
	stats, err = pq.GetStats(ctx, "gc-timeout-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}

	if stats.PendingCount != 1 {
		t.Errorf("expected 1 pending message after timeout reset, got %d", stats.PendingCount)
	}

	if stats.ProcessingCount != 0 {
		t.Errorf("expected 0 processing messages after timeout reset, got %d", stats.ProcessingCount)
	}

	// Verify we can consume the message again
	msg2, err := pq.ReceiveChannel(ctx, "gc-timeout-test", pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("failed to re-consume message: %v", err)
	}

	if msg2.ID != msg.ID {
		t.Errorf("expected same message ID after reset, got different ID")
	}
}

func TestGarbageCollectorKeepForeverPreservesMessages(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel
	err := pq.CreateChannel(ctx, "gc-keep-forever-test", pgqueue.WithQueueMaxRetries(3))
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish and acknowledge a message
	_, err = pq.Publish(ctx, "gc-keep-forever-test", []byte("test message"))
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	msg, err := pq.ReceiveChannel(ctx, "gc-keep-forever-test", pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("failed to consume message: %v", err)
	}

	if err := pq.Ack(ctx, msg.Receipt()); err != nil {
		t.Fatalf("failed to ack message: %v", err)
	}

	// Wait for processed_at to be set
	time.Sleep(100 * time.Millisecond)

	// Verify message is completed
	stats, err := pq.GetStats(ctx, "gc-keep-forever-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if stats.CompletedCount != 1 {
		t.Errorf("expected 1 completed message, got %d", stats.CompletedCount)
	}

	// Run garbage collector with an explicit KeepForever policy. A bare
	// RetentionPolicy{} would be treated as unconfigured and replaced with
	// default retention, so KeepForever is how a GC opts out of all cleanup.
	gcConfig := pgqueue.GarbageCollectorConfig{
		DefaultPolicy: pgqueue.RetentionPolicy{
			CompletedMessageTTL: pgqueue.KeepForever,
		},
	}
	gc := pgqueue.NewGarbageCollector(pq, gcConfig)

	// Run collection multiple times
	for i := 0; i < 3; i++ {
		if err := gc.Collect(ctx); err != nil {
			t.Fatalf("garbage collection failed: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Verify completed message is still present
	stats, err = pq.GetStats(ctx, "gc-keep-forever-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}

	if stats.CompletedCount != 1 {
		t.Errorf("expected 1 completed message to be preserved with KeepForever, got %d", stats.CompletedCount)
	}
}

// TestGarbageCollectorDefaultPolicyPurgesCompleted is the regression test for
// issue #47: a GarbageCollector created with an empty config must still reclaim
// completed messages, rather than letting the table grow without bound.
func TestGarbageCollectorDefaultPolicyPurgesCompleted(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	if err := pq.CreateChannel(ctx, "gc-default-policy"); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}
	if _, err := pq.Publish(ctx, "gc-default-policy", []byte("done")); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}
	msg, err := pq.ReceiveChannel(ctx, "gc-default-policy", pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}
	if err := pq.Ack(ctx, msg.Receipt()); err != nil {
		t.Fatalf("failed to ack: %v", err)
	}

	// Backdate processed_at past the 24h default CompletedMessageTTL.
	if _, err := db.ExecContext(ctx,
		"UPDATE pgqueue_msg_gc_default_policy SET processed_at = NOW() - INTERVAL '25 hours'"); err != nil {
		t.Fatalf("failed to backdate completed message: %v", err)
	}

	// An empty config: NewGarbageCollector substitutes the default policy.
	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{})
	if err := gc.Collect(ctx); err != nil {
		t.Fatalf("garbage collection failed: %v", err)
	}

	stats, err := pq.GetStats(ctx, "gc-default-policy", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if stats.CompletedCount != 0 {
		t.Errorf("expected completed message purged by default policy, got %d", stats.CompletedCount)
	}
}

func TestGarbageCollectorParallel(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create multiple channels to test parallel GC
	const numQueues = 5
	for i := 0; i < numQueues; i++ {
		name := "gc-parallel-" + string(rune('a'+i))
		err := pq.CreateChannel(ctx, name, pgqueue.WithQueueMaxRetries(3))
		if err != nil {
			t.Fatalf("failed to create channel %s: %v", name, err)
		}

		// Publish messages and ack some
		for j := 0; j < 3; j++ {
			if _, err := pq.Publish(ctx, name, []byte("test message")); err != nil {
				t.Fatalf("failed to publish message: %v", err)
			}
		}

		msg, err := pq.ReceiveChannel(ctx, name, pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("failed to consume message: %v", err)
		}
		if err := pq.Ack(ctx, msg.Receipt()); err != nil {
			t.Fatalf("failed to ack message: %v", err)
		}
	}

	// Wait for processed_at to be set
	time.Sleep(100 * time.Millisecond)

	// Run parallel GC with MaxWorkers=3 (less than numQueues to test semaphore)
	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{
		DefaultPolicy: pgqueue.RetentionPolicy{
			CompletedMessageTTL: 1 * time.Millisecond,
		},
		MaxWorkers: 3,
	})

	time.Sleep(10 * time.Millisecond)

	if err := gc.Collect(ctx); err != nil {
		t.Fatalf("parallel garbage collection failed: %v", err)
	}

	// Verify all queues were processed
	for i := 0; i < numQueues; i++ {
		name := "gc-parallel-" + string(rune('a'+i))
		stats, err := pq.GetStats(ctx, name, pgqueue.QueueTypeChannel)
		if err != nil {
			t.Fatalf("failed to get stats for %s: %v", name, err)
		}

		if stats.CompletedCount != 0 {
			t.Errorf("queue %s: expected 0 completed messages after GC, got %d", name, stats.CompletedCount)
		}
		if stats.PendingCount != 2 {
			t.Errorf("queue %s: expected 2 pending messages, got %d", name, stats.PendingCount)
		}
	}
}

func TestGarbageCollectorPubSub(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test topic
	err := pq.CreateTopic(ctx, "gc-pubsub-test")
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Subscribe two subscribers
	if err := pq.Subscribe(ctx, "gc-pubsub-test", "sub-1"); err != nil {
		t.Fatalf("failed to subscribe sub-1: %v", err)
	}
	if err := pq.Subscribe(ctx, "gc-pubsub-test", "sub-2"); err != nil {
		t.Fatalf("failed to subscribe sub-2: %v", err)
	}

	// Publish 5 messages
	for i := 0; i < 5; i++ {
		if _, err := pq.Publish(ctx, "gc-pubsub-test", []byte("test message")); err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}
	}

	// Ack all messages for both subscribers (fully consumed)
	for _, sub := range []string{"sub-1", "sub-2"} {
		for i := 0; i < 5; i++ {
			msg, err := pq.ReceiveTopic(ctx, "gc-pubsub-test", sub, pgqueue.WithVisibilityTimeout(30*time.Second))
			if err != nil {
				t.Fatalf("failed to consume message for %s: %v", sub, err)
			}
			if err := pq.Ack(ctx, msg.Receipt()); err != nil {
				t.Fatalf("failed to ack message for %s: %v", sub, err)
			}
		}
	}

	// Verify messages exist in the message table before GC
	var msgCount int
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM pgqueue_msg_gc_pubsub_test").Scan(&msgCount)
	if err != nil {
		t.Fatalf("failed to count messages: %v", err)
	}
	if msgCount != 5 {
		t.Fatalf("expected 5 messages before GC, got %d", msgCount)
	}

	// Wait for TTL to expire then run GC
	time.Sleep(10 * time.Millisecond)

	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{
		DefaultPolicy: pgqueue.RetentionPolicy{
			CompletedMessageTTL: 1 * time.Millisecond,
		},
	})

	if err := gc.Collect(ctx); err != nil {
		t.Fatalf("garbage collection failed: %v", err)
	}

	// Verify messages were purged from the message table
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM pgqueue_msg_gc_pubsub_test").Scan(&msgCount)
	if err != nil {
		t.Fatalf("failed to count messages after GC: %v", err)
	}
	if msgCount != 0 {
		t.Errorf("expected 0 messages after GC, got %d", msgCount)
	}
}

func TestGarbageCollectorPubSubPartialAck(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test topic
	err := pq.CreateTopic(ctx, "gc-pubsub-partial")
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Subscribe two subscribers
	if err := pq.Subscribe(ctx, "gc-pubsub-partial", "sub-1"); err != nil {
		t.Fatalf("failed to subscribe sub-1: %v", err)
	}
	if err := pq.Subscribe(ctx, "gc-pubsub-partial", "sub-2"); err != nil {
		t.Fatalf("failed to subscribe sub-2: %v", err)
	}

	// Publish 3 messages
	for i := 0; i < 3; i++ {
		if _, err := pq.Publish(ctx, "gc-pubsub-partial", []byte("test message")); err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}
	}

	// Only sub-1 acks all messages; sub-2 does NOT consume
	for i := 0; i < 3; i++ {
		msg, err := pq.ReceiveTopic(ctx, "gc-pubsub-partial", "sub-1", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("failed to consume message for sub-1: %v", err)
		}
		if err := pq.Ack(ctx, msg.Receipt()); err != nil {
			t.Fatalf("failed to ack message for sub-1: %v", err)
		}
	}

	// Wait for TTL to expire then run GC
	time.Sleep(10 * time.Millisecond)

	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{
		DefaultPolicy: pgqueue.RetentionPolicy{
			CompletedMessageTTL: 1 * time.Millisecond,
		},
	})

	if err := gc.Collect(ctx); err != nil {
		t.Fatalf("garbage collection failed: %v", err)
	}

	// Messages should NOT be purged because sub-2 still has pending subscriptions
	var msgCount int
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM pgqueue_msg_gc_pubsub_partial").Scan(&msgCount)
	if err != nil {
		t.Fatalf("failed to count messages after GC: %v", err)
	}
	if msgCount != 3 {
		t.Errorf("expected 3 messages preserved (sub-2 not acked), got %d", msgCount)
	}
}

func TestPurgeQueue(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel
	err := pq.CreateChannel(ctx, "purge-test")
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish some messages
	for i := 0; i < 10; i++ {
		if _, err := pq.Publish(ctx, "purge-test", []byte("test message")); err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}
	}

	// Verify messages exist
	depth, err := pq.GetQueueDepth(ctx, "purge-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get queue depth: %v", err)
	}
	if depth != 10 {
		t.Errorf("expected 10 messages, got %d", depth)
	}

	// Purge the queue.
	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{})
	err = gc.PurgeQueue(ctx, "purge-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to purge queue: %v", err)
	}

	// Verify all messages are gone
	depth, err = pq.GetQueueDepth(ctx, "purge-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get queue depth: %v", err)
	}
	if depth != 0 {
		t.Errorf("expected 0 messages after purge, got %d", depth)
	}
}

func TestGarbageCollectorDoubleStop(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{
		Interval: 100 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	gc.Start(ctx)

	// Let it run briefly
	time.Sleep(50 * time.Millisecond)
	cancel()

	// First Stop should work
	gc.Stop()
	// Second Stop must not panic
	gc.Stop()
}

func TestGarbageCollectorStopWithoutStart(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{})

	// Stop without Start must not block or panic
	done := make(chan struct{})
	go func() {
		gc.Stop()
		close(done)
	}()

	select {
	case <-done:
		// OK
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() blocked without Start() being called")
	}
}

func TestGarbageCollectorPubSubVisibilityTimeout(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateTopic(ctx, "gc-pubsub-vt")
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	if err := pq.Subscribe(ctx, "gc-pubsub-vt", "sub-vt"); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	if _, err := pq.Publish(ctx, "gc-pubsub-vt", []byte("timeout-msg")); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// Consume with minimum visibility timeout
	msg, err := pq.ReceiveTopic(ctx, "gc-pubsub-vt", "sub-vt", pgqueue.WithVisibilityTimeout(1*time.Millisecond))
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}
	if msg == nil {
		t.Fatal("expected message, got nil")
	}

	// Wait for visibility timeout to expire
	time.Sleep(50 * time.Millisecond)

	// GC should reset timed-out subscription back to pending
	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{})
	if err := gc.Collect(ctx); err != nil {
		t.Fatalf("garbage collection failed: %v", err)
	}

	// Verify message is consumable again via subscriber lag
	lag, err := pq.GetSubscriberLag(ctx, "gc-pubsub-vt", "sub-vt")
	if err != nil {
		t.Fatalf("failed to get subscriber lag: %v", err)
	}
	if lag.PendingCount != 1 {
		t.Errorf("expected 1 pending after timeout reset, got %d", lag.PendingCount)
	}

	// Re-consume should return the same message
	msg2, err := pq.ReceiveTopic(ctx, "gc-pubsub-vt", "sub-vt", pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("failed to re-consume: %v", err)
	}
	if msg2 == nil {
		t.Fatal("expected message after timeout reset, got nil")
	}
	if msg2.ID != msg.ID {
		t.Errorf("expected same message ID after reset, got different")
	}
}

// TestExhaustedTimedOutSubscriptionsPromotedByGC verifies the GC backstop for
// pub/sub: a subscription that times out in 'processing' state after exhausting
// its retries is promoted to the DLQ by promoteExhaustedTopicSubscriptions,
// rather than being reset back to 'pending' indefinitely by
// resetTimedOutSubscriptions. This covers a subscriber that has gone idle and
// so never reaches the message via the consume path.
func TestExhaustedTimedOutSubscriptionsPromotedByGC(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	const topicName = "gc-exhausted-sub"
	const subID = "sub-exhaust"
	// maxRetries=1: the subscription tolerates one redelivery; a second
	// timed-out reclaim exhausts it.
	if err := pq.CreateTopic(ctx, topicName, pgqueue.WithQueueMaxRetries(1)); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	if err := pq.Subscribe(ctx, topicName, subID); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	if _, err := pq.Publish(ctx, topicName, []byte("exhaust-me")); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{})

	// Claim with a 1ms visibility timeout and never ack; a GC pass then resets
	// the timed-out subscription to pending, counting the timeout (retry_count
	// 0 -> 1).
	if _, err := pq.ReceiveTopic(ctx, topicName, subID, pgqueue.WithVisibilityTimeout(1*time.Millisecond)); err != nil {
		t.Fatalf("first consume failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := gc.Collect(ctx); err != nil {
		t.Fatalf("first GC collect failed: %v", err)
	}

	// Claim again and again never ack: the subscription is now at retry_count 1
	// and times out in 'processing' state — exhausted, since a further reclaim
	// would breach maxRetries=1.
	if _, err := pq.ReceiveTopic(ctx, topicName, subID, pgqueue.WithVisibilityTimeout(1*time.Millisecond)); err != nil {
		t.Fatalf("second consume failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	// The GC backstop must promote the exhausted timed-out subscription to the
	// DLQ rather than resetTimedOutSubscriptions resetting it to pending.
	if err := gc.Collect(ctx); err != nil {
		t.Fatalf("second GC collect failed: %v", err)
	}

	dlq, err := pq.GetDLQStats(ctx, topicName, pgqueue.QueueTypePubSub)
	if err != nil {
		t.Fatalf("failed to get DLQ stats: %v", err)
	}
	if dlq.TotalCount != 1 {
		t.Errorf("expected the exhausted subscription promoted to the DLQ by GC, got TotalCount=%d", dlq.TotalCount)
	}

	// The live subscription row must be gone (moved to the DLQ).
	lag, err := pq.GetSubscriberLag(ctx, topicName, subID)
	if err != nil {
		t.Fatalf("failed to get subscriber lag: %v", err)
	}
	if lag.PendingCount != 0 || lag.ProcessingCount != 0 {
		t.Errorf("expected no live subscription after DLQ promotion, got pending=%d processing=%d",
			lag.PendingCount, lag.ProcessingCount)
	}
}

func TestGarbageCollectorMaxPendingAge(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("channel", func(t *testing.T) {
		err := pq.CreateChannel(ctx, "gc-maxage-ch")
		if err != nil {
			t.Fatalf("failed to create channel: %v", err)
		}

		// Publish 3 messages that will be backdated
		for i := range 3 {
			if _, err := pq.Publish(ctx, "gc-maxage-ch", fmt.Appendf(nil, "old-%d", i)); err != nil {
				t.Fatalf("failed to publish: %v", err)
			}
		}

		// Backdate created_at to 2 hours ago
		_, err = db.ExecContext(ctx,
			"UPDATE pgqueue_msg_gc_maxage_ch SET created_at = NOW() - INTERVAL '2 hours'")
		if err != nil {
			t.Fatalf("failed to backdate messages: %v", err)
		}

		// Publish 2 recent messages
		for i := range 2 {
			if _, err := pq.Publish(ctx, "gc-maxage-ch", fmt.Appendf(nil, "new-%d", i)); err != nil {
				t.Fatalf("failed to publish: %v", err)
			}
		}

		gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{
			DefaultPolicy: pgqueue.RetentionPolicy{
				MaxPendingAge: 1 * time.Hour,
			},
		})
		if err := gc.Collect(ctx); err != nil {
			t.Fatalf("garbage collection failed: %v", err)
		}

		depth, err := pq.GetQueueDepth(ctx, "gc-maxage-ch", pgqueue.QueueTypeChannel)
		if err != nil {
			t.Fatalf("failed to get queue depth: %v", err)
		}
		if depth != 2 {
			t.Errorf("expected 2 messages after MaxPendingAge purge, got %d", depth)
		}
	})

	t.Run("pubsub", func(t *testing.T) {
		err := pq.CreateTopic(ctx, "gc-maxage-ps")
		if err != nil {
			t.Fatalf("failed to create topic: %v", err)
		}
		if err := pq.Subscribe(ctx, "gc-maxage-ps", "sub-pa"); err != nil {
			t.Fatalf("failed to subscribe: %v", err)
		}

		for i := range 3 {
			if _, err := pq.Publish(ctx, "gc-maxage-ps", fmt.Appendf(nil, "old-%d", i)); err != nil {
				t.Fatalf("failed to publish: %v", err)
			}
		}

		_, err = db.ExecContext(ctx,
			"UPDATE pgqueue_msg_gc_maxage_ps SET created_at = NOW() - INTERVAL '2 hours'")
		if err != nil {
			t.Fatalf("failed to backdate messages: %v", err)
		}

		for i := range 2 {
			if _, err := pq.Publish(ctx, "gc-maxage-ps", fmt.Appendf(nil, "new-%d", i)); err != nil {
				t.Fatalf("failed to publish: %v", err)
			}
		}

		gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{
			DefaultPolicy: pgqueue.RetentionPolicy{
				MaxPendingAge: 1 * time.Hour,
			},
		})
		if err := gc.Collect(ctx); err != nil {
			t.Fatalf("garbage collection failed: %v", err)
		}

		var msgCount int
		err = db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM pgqueue_msg_gc_maxage_ps").Scan(&msgCount)
		if err != nil {
			t.Fatalf("failed to count messages: %v", err)
		}
		if msgCount != 2 {
			t.Errorf("expected 2 messages after MaxPendingAge purge, got %d", msgCount)
		}
	})
}

func TestGarbageCollectorDLQRetention(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "gc-dlq-ret", pgqueue.WithQueueMaxRetries(1))
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish 3 messages and move them all to DLQ
	for i := range 3 {
		if _, err := pq.Publish(ctx, "gc-dlq-ret", fmt.Appendf(nil, "dlq-msg-%d", i)); err != nil {
			t.Fatalf("failed to publish: %v", err)
		}
	}
	for range 3 {
		// First consume + nack: retry
		msg, err := pq.ReceiveChannel(ctx, "gc-dlq-ret", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("failed to consume: %v", err)
		}
		if err := pq.Nack(ctx, msg.Receipt(), "fail"); err != nil {
			t.Fatalf("first nack failed: %v", err)
		}
		// Second consume + nack: exceeds max retries -> DLQ
		msg, err = pq.ReceiveChannel(ctx, "gc-dlq-ret", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("failed to consume for DLQ: %v", err)
		}
		if err := pq.Nack(ctx, msg.Receipt(), "fail again"); err != nil {
			t.Fatalf("second nack failed: %v", err)
		}
	}

	dlqStats, err := pq.GetDLQStats(ctx, "gc-dlq-ret", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get DLQ stats: %v", err)
	}
	if dlqStats.TotalCount != 3 {
		t.Fatalf("expected 3 DLQ messages, got %d", dlqStats.TotalCount)
	}

	// Backdate DLQ moved_at
	_, err = db.ExecContext(ctx,
		"UPDATE pgqueue_dlq_gc_dlq_ret SET moved_at = NOW() - INTERVAL '2 hours'")
	if err != nil {
		t.Fatalf("failed to backdate DLQ: %v", err)
	}

	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{
		DefaultPolicy: pgqueue.RetentionPolicy{
			DLQRetention: 1 * time.Hour,
		},
	})
	if err := gc.Collect(ctx); err != nil {
		t.Fatalf("garbage collection failed: %v", err)
	}

	dlqStats, err = pq.GetDLQStats(ctx, "gc-dlq-ret", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get DLQ stats after GC: %v", err)
	}
	if dlqStats.TotalCount != 0 {
		t.Errorf("expected 0 DLQ messages after retention purge, got %d", dlqStats.TotalCount)
	}
}

func TestGarbageCollectorPerQueuePolicy(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create two channels
	for _, name := range []string{"gc-policy-a", "gc-policy-b"} {
		err := pq.CreateChannel(ctx, name)
		if err != nil {
			t.Fatalf("failed to create channel %s: %v", name, err)
		}

		// Publish and ack 3 messages each
		for i := range 3 {
			if _, err := pq.Publish(ctx, name, fmt.Appendf(nil, "msg-%d", i)); err != nil {
				t.Fatalf("failed to publish to %s: %v", name, err)
			}
		}
		for range 3 {
			msg, err := pq.ReceiveChannel(ctx, name, pgqueue.WithVisibilityTimeout(30*time.Second))
			if err != nil {
				t.Fatalf("failed to consume from %s: %v", name, err)
			}
			if err := pq.Ack(ctx, msg.Receipt()); err != nil {
				t.Fatalf("failed to ack in %s: %v", name, err)
			}
		}
	}

	// Backdate processed_at for both queues
	for _, table := range []string{"gc_policy_a", "gc_policy_b"} {
		_, err := db.ExecContext(ctx,
			fmt.Sprintf("UPDATE pgqueue_msg_%s SET processed_at = NOW() - INTERVAL '2 hours' WHERE status = '%s'", table, pgqueue.MessageStatusCompleted))
		if err != nil {
			t.Fatalf("failed to backdate %s: %v", table, err)
		}
	}

	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{
		DefaultPolicy: pgqueue.RetentionPolicy{
			CompletedMessageTTL: 24 * time.Hour, // Retains all
		},
		Policies: map[string]pgqueue.RetentionPolicy{
			"gc-policy-a": {CompletedMessageTTL: 1 * time.Hour}, // Purges old
		},
	})
	if err := gc.Collect(ctx); err != nil {
		t.Fatalf("garbage collection failed: %v", err)
	}

	// gc-policy-a should have 0 completed (override policy purged them)
	statsA, err := pq.GetStats(ctx, "gc-policy-a", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats for gc-policy-a: %v", err)
	}
	if statsA.CompletedCount != 0 {
		t.Errorf("gc-policy-a: expected 0 completed after GC, got %d", statsA.CompletedCount)
	}

	// gc-policy-b should still have 3 completed (default 24h policy retains them)
	statsB, err := pq.GetStats(ctx, "gc-policy-b", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats for gc-policy-b: %v", err)
	}
	if statsB.CompletedCount != 3 {
		t.Errorf("gc-policy-b: expected 3 completed retained, got %d", statsB.CompletedCount)
	}
}

// TestT019_GCReclaimCountsVisibilityTimeoutOnce verifies that a visibility
// timeout reclaimed by the garbage collector is counted as exactly one delivery
// attempt: the GC reset increments retry_count by one (so the timeout counts
// toward max_retries), and the subsequent pending-state consume does not add a
// second increment for the same timeout.
//
// Sequence: consume → timeout → GC reset → consume again.
func TestT019_GCReclaimCountsVisibilityTimeoutOnce(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	const queueName = "t019-gc-retry-count"
	// maxRetries=5 gives plenty of headroom so we stay far from DLQ.
	if err := pq.CreateChannel(ctx, queueName, pgqueue.WithQueueMaxRetries(5)); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	if _, err := pq.Publish(ctx, queueName, []byte("msg")); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// Consume with a very short visibility timeout so it expires quickly.
	if _, err := pq.ReceiveChannel(ctx, queueName, pgqueue.WithVisibilityTimeout(1*time.Millisecond)); err != nil {
		t.Fatalf("failed to consume: %v", err)
	}

	// Wait for the visibility timeout to expire.
	time.Sleep(50 * time.Millisecond)

	// Run GC: it resets the timed-out message to pending and counts the timeout
	// as one delivery attempt (retry_count 0 -> 1).
	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{})
	if err := gc.Collect(ctx); err != nil {
		t.Fatalf("GC collect failed: %v", err)
	}

	// The GC reset must have counted the timeout exactly once.
	var dbRetryCountAfterGC int
	if err := db.QueryRowContext(ctx,
		"SELECT retry_count FROM pgqueue_msg_t019_gc_retry_count LIMIT 1",
	).Scan(&dbRetryCountAfterGC); err != nil {
		t.Fatalf("failed to query retry_count after GC: %v", err)
	}
	if dbRetryCountAfterGC != 1 {
		t.Errorf("GC must count the timeout once: got retry_count=%d after GC, want 1", dbRetryCountAfterGC)
	}

	// Re-consume: the message is pending, so the consume path must not add a
	// second increment for the same timeout (no double-counting).
	msg2, err := pq.ReceiveChannel(ctx, queueName, pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("failed to re-consume: %v", err)
	}
	if msg2 == nil {
		t.Fatal("expected message after GC reset, got nil")
	}
	if msg2.RetryCount != 1 {
		t.Errorf("timeout double-counted: expected retry_count=1, got %d", msg2.RetryCount)
	}
}

// TestExhaustedTimedOutMessagesPromotedByConsume verifies that the channel
// consume path moves timed-out messages that have exhausted their retries to
// the dead-letter queue inline — with no GarbageCollector pass. This keeps an
// exhausted-by-timeout message from being stranded in 'processing' forever
// when no GC is running, matching how the pub/sub consume path handles
// exhausted subscriptions.
func TestExhaustedTimedOutMessagesPromotedByConsume(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// maxRetries=1 so a message is exhausted after a couple of timed-out claims.
	const channelName = "consume-exhausted-promote"
	if err := pq.CreateChannel(ctx, channelName, pgqueue.WithQueueMaxRetries(1)); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	const numMessages = 5
	for range numMessages {
		if _, err := pq.Publish(ctx, channelName, []byte("exhaust-me")); err != nil {
			t.Fatalf("failed to publish: %v", err)
		}
	}

	// Consume every message with a 1ms visibility timeout and never ack, then
	// wait so the claims expire. Repeated enough times to exhaust the retry
	// budget. No GarbageCollector is ever created or started.
	for attempt := 0; attempt < 4; attempt++ {
		for {
			_, err := pq.ReceiveChannel(ctx, channelName, pgqueue.WithVisibilityTimeout(1*time.Millisecond))
			if errors.Is(err, pgqueue.ErrQueueEmpty) {
				break
			}
			if err != nil {
				t.Fatalf("attempt %d consume: %v", attempt, err)
			}
			// Never ack: let the 1ms visibility timeout lapse.
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Once exhausted, a consume call must report the queue empty: an exhausted
	// timed-out message is dead-lettered, not delivered. This final call also
	// drains any message still awaiting its last reclaim.
	if _, err := pq.ReceiveChannel(ctx, channelName); !errors.Is(err, pgqueue.ErrQueueEmpty) {
		t.Fatalf("expected ErrQueueEmpty after exhaustion, got %v", err)
	}

	// The consume path itself must have moved every exhausted message to the
	// DLQ — verified with no GarbageCollector run.
	dlq, err := pq.GetDLQStats(ctx, channelName, pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get DLQ stats: %v", err)
	}
	if dlq.TotalCount != numMessages {
		t.Errorf("expected %d messages dead-lettered by the consume path, got %d",
			numMessages, dlq.TotalCount)
	}
}

// TestGCCountsVisibilityTimeoutTowardDLQ verifies that visibility timeouts the
// GC reclaims count toward max_retries: a message that is repeatedly claimed
// and never acked, with a GC pass between each timeout, reaches the DLQ once it
// has exhausted its retry budget. Without the GC counting timeouts, retry_count
// would stay at 0 and the message would be redelivered forever.
func TestGCCountsVisibilityTimeoutTowardDLQ(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	const channelName = "gc-timeout-to-dlq"
	if err := pq.CreateChannel(ctx, channelName, pgqueue.WithQueueMaxRetries(2)); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := pq.Publish(ctx, channelName, []byte("never-acked")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{})

	// maxRetries=2 means the message tolerates retry_count 0 and 1; a third
	// timed-out reclaim (retry_count would reach 2) promotes it to the DLQ.
	for attempt := range 3 {
		msg, err := pq.ReceiveChannel(ctx, channelName, pgqueue.WithVisibilityTimeout(1*time.Millisecond))
		if err != nil {
			t.Fatalf("attempt %d consume: %v", attempt, err)
		}
		if msg == nil {
			t.Fatalf("attempt %d: message not redelivered (lost before reaching DLQ)", attempt)
		}
		// Never ack: let the visibility timeout lapse, then GC reclaims it.
		time.Sleep(30 * time.Millisecond)
		if err := gc.Collect(ctx); err != nil {
			t.Fatalf("attempt %d GC collect: %v", attempt, err)
		}
	}

	dlq, err := pq.GetDLQStats(ctx, channelName, pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("DLQ stats: %v", err)
	}
	if dlq.TotalCount != 1 {
		t.Errorf("expected the exhausted message in the DLQ, got TotalCount=%d", dlq.TotalCount)
	}
	if _, err := pq.ReceiveChannel(ctx, channelName); !errors.Is(err, pgqueue.ErrQueueEmpty) {
		t.Errorf("expected channel empty after DLQ promotion, got err=%v", err)
	}
}

// TestGCKeepsDLQReferencedPubSubMessage verifies that purgeCompletedMessages
// does not delete a pub/sub message that is still referenced by a DLQ row
// (FR-027). One subscriber acks the message and another is dead-lettered; the
// completed-message GC pass must keep the message row so the DLQ entry stays
// replayable.
func TestGCKeepsDLQReferencedPubSubMessage(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	const topicName = "gc-dlq-ref-topic"
	// MaxRetries=0 so the failing subscriber is dead-lettered on its first nack.
	if err := pq.CreateTopic(ctx, topicName, pgqueue.WithQueueMaxRetries(0)); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := pq.Subscribe(ctx, topicName, "sub-ok"); err != nil {
		t.Fatalf("subscribe sub-ok: %v", err)
	}
	if err := pq.Subscribe(ctx, topicName, "sub-fail"); err != nil {
		t.Fatalf("subscribe sub-fail: %v", err)
	}
	if _, err := pq.Publish(ctx, topicName, []byte("fan-out")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// sub-ok acks the message.
	okMsg, err := pq.ReceiveTopic(ctx, topicName, "sub-ok", pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil || okMsg == nil {
		t.Fatalf("sub-ok consume: msg=%v err=%v", okMsg, err)
	}
	if err := pq.Ack(ctx, okMsg.Receipt()); err != nil {
		t.Fatalf("sub-ok ack: %v", err)
	}

	// sub-fail nacks the message; with MaxRetries=0 it goes straight to the DLQ.
	failMsg, err := pq.ReceiveTopic(ctx, topicName, "sub-fail", pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil || failMsg == nil {
		t.Fatalf("sub-fail consume: msg=%v err=%v", failMsg, err)
	}
	if err := pq.Nack(ctx, failMsg.Receipt(), "boom"); err != nil {
		t.Fatalf("sub-fail nack: %v", err)
	}

	// Run the completed-message GC pass: every remaining subscription is acked,
	// so without the FR-027 guard the message row would be purged here.
	time.Sleep(20 * time.Millisecond)
	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{
		DefaultPolicy: pgqueue.RetentionPolicy{CompletedMessageTTL: 1 * time.Millisecond},
	})
	if err := gc.Collect(ctx); err != nil {
		t.Fatalf("GC collect: %v", err)
	}

	// The DLQ entry must still be replayable: replay re-creates sub-fail's
	// subscription row, which foreign-keys the message row that survived GC.
	res, err := pq.ReplayDLQ(ctx, topicName, pgqueue.QueueTypePubSub,
		pgqueue.ReplayOptions{})
	if err != nil {
		t.Fatalf("replay DLQ: %v", err)
	}
	if res.Replayed != 1 {
		t.Errorf("expected 1 DLQ message replayed (message row kept), got Replayed=%d Skipped=%d",
			res.Replayed, res.Skipped)
	}
}

// TestGCPubSubDLQRetentionExceedsCompletedTTL is the regression test for the C9
// audit finding (issue #140): for pub/sub topics a GC policy with
// CompletedMessageTTL < DLQRetention must be safe. The completed-message purge
// never removes a message while a DLQ entry references it, so the DLQ entry is
// always reaped first and stays replayable even though the message is long past
// its CompletedMessageTTL.
func TestGCPubSubDLQRetentionExceedsCompletedTTL(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	const topicName = "gc-c9-ttl-topic"
	const msgTable = "pgqueue_msg_gc_c9_ttl_topic"
	// MaxRetries=0 so the failing subscriber is dead-lettered on its first nack.
	if err := pq.CreateTopic(ctx, topicName, pgqueue.WithQueueMaxRetries(0)); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := pq.Subscribe(ctx, topicName, "sub-ok"); err != nil {
		t.Fatalf("subscribe sub-ok: %v", err)
	}
	if err := pq.Subscribe(ctx, topicName, "sub-fail"); err != nil {
		t.Fatalf("subscribe sub-fail: %v", err)
	}
	if _, err := pq.Publish(ctx, topicName, []byte("fan-out")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// sub-ok acks; sub-fail nacks straight to the DLQ. The message row is now
	// "completed" (no non-acked subscription rows remain) but a DLQ entry
	// references it.
	okMsg, err := pq.ReceiveTopic(ctx, topicName, "sub-ok", pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil || okMsg == nil {
		t.Fatalf("sub-ok consume: msg=%v err=%v", okMsg, err)
	}
	if err := pq.Ack(ctx, okMsg.Receipt()); err != nil {
		t.Fatalf("sub-ok ack: %v", err)
	}
	failMsg, err := pq.ReceiveTopic(ctx, topicName, "sub-fail", pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil || failMsg == nil {
		t.Fatalf("sub-fail consume: msg=%v err=%v", failMsg, err)
	}
	if err := pq.Nack(ctx, failMsg.Receipt(), "boom"); err != nil {
		t.Fatalf("sub-fail nack: %v", err)
	}

	// Backdate the message well past CompletedMessageTTL so the completed-message
	// purge would delete it if the DLQ guard were absent.
	if _, err := db.ExecContext(ctx,
		fmt.Sprintf("UPDATE %s SET created_at = NOW() - INTERVAL '2 hours'", msgTable)); err != nil {
		t.Fatalf("backdate message: %v", err)
	}

	// CompletedMessageTTL (1ms) is far shorter than DLQRetention (1h): the exact
	// configuration C9 warned about. The DLQ entry is fresh, so it survives this
	// pass; the message must survive too because the DLQ entry pins it.
	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{
		DefaultPolicy: pgqueue.RetentionPolicy{
			CompletedMessageTTL: 1 * time.Millisecond,
			DLQRetention:        1 * time.Hour,
		},
	})
	if err := gc.Collect(ctx); err != nil {
		t.Fatalf("GC collect: %v", err)
	}

	var msgCount int
	if err := db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s", msgTable)).Scan(&msgCount); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if msgCount != 1 {
		t.Fatalf("expected message row to survive GC (pinned by DLQ entry), got %d rows", msgCount)
	}

	dlqStats, err := pq.GetDLQStats(ctx, topicName, pgqueue.QueueTypePubSub)
	if err != nil {
		t.Fatalf("DLQ stats: %v", err)
	}
	if dlqStats.TotalCount != 1 {
		t.Fatalf("expected DLQ entry to survive (within DLQRetention), got %d", dlqStats.TotalCount)
	}

	// The surviving DLQ entry is still replayable.
	res, err := pq.ReplayDLQ(ctx, topicName, pgqueue.QueueTypePubSub, pgqueue.ReplayOptions{})
	if err != nil {
		t.Fatalf("replay DLQ: %v", err)
	}
	if res.Replayed != 1 {
		t.Errorf("expected 1 DLQ message replayed, got Replayed=%d Skipped=%d",
			res.Replayed, res.Skipped)
	}
}

// TestGarbageCollectorInertAfterClose verifies that a GarbageCollector created
// after the owning Queue is closed does not start a background loop: Start is a
// no-op and the create/start/stop sequence completes without panicking.
func TestGarbageCollectorInertAfterClose(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	if err := pq.Close(); err != nil {
		t.Fatalf("close queue: %v", err)
	}

	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{})
	gc.Start(ctx) // must be a no-op on a closed Queue

	done := make(chan struct{})
	go func() {
		gc.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("GC Stop hung: Start spawned a loop on a closed Queue")
	}
}

// TestGarbageCollectorPaginatesRetentionPurges is the regression test for
// issue #50: the four GC retention purges (purgeCompletedMessages,
// purgeOldPendingMessages, purgeDLQMessages, reclaimOrphanTopicMessages) must
// drain a backlog larger than one page rather than stopping after one
// unbounded DELETE.
//
// Each sub-test seeds 1100 rows — strictly more than retentionPurgePageSize
// (1000) — so a single-page implementation would leave a remainder. Rows are
// seeded directly via SQL because the GC behaviour under test is pure DELETE:
// going through the public publish/consume/ack API for 1100 messages × 6
// sub-tests would add several seconds without exercising anything new.
func TestGarbageCollectorPaginatesRetentionPurges(t *testing.T) {
	const seedCount = 1100

	pq, db, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("completed/channel", func(t *testing.T) {
		const name = "gc-page-completed-ch"
		if err := pq.CreateChannel(ctx, name); err != nil {
			t.Fatalf("create channel: %v", err)
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO pgqueue_msg_gc_page_completed_ch (id, payload, status, processed_at)
			SELECT uuidv7(), 'p'::bytea, 'completed', NOW() - INTERVAL '25 hours'
			FROM generate_series(1, %d)
		`, seedCount)); err != nil {
			t.Fatalf("seed completed channel rows: %v", err)
		}

		gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{
			DefaultPolicy: pgqueue.RetentionPolicy{CompletedMessageTTL: time.Hour},
		})
		if err := gc.Collect(ctx); err != nil {
			t.Fatalf("collect: %v", err)
		}

		var remaining int
		if err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM pgqueue_msg_gc_page_completed_ch").Scan(&remaining); err != nil {
			t.Fatalf("count: %v", err)
		}
		if remaining != 0 {
			t.Errorf("expected all %d completed rows purged across pages, got %d remaining",
				seedCount, remaining)
		}
	})

	t.Run("completed/pubsub", func(t *testing.T) {
		const name = "gc-page-completed-ps"
		if err := pq.CreateTopic(ctx, name); err != nil {
			t.Fatalf("create topic: %v", err)
		}
		// Seed messages and matching acked subscriptions. The purge predicate
		// requires (no sub != acked) AND (no DLQ ref), so every sub row must be
		// 'acked' for the message to be eligible.
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO pgqueue_msg_gc_page_completed_ps (id, payload, created_at)
			SELECT uuidv7(), 'p'::bytea, NOW() - INTERVAL '25 hours'
			FROM generate_series(1, %d)
		`, seedCount)); err != nil {
			t.Fatalf("seed pubsub messages: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO pgqueue_sub_gc_page_completed_ps (message_id, subscriber_id, status)
			SELECT id, 'sub-1', 'acked' FROM pgqueue_msg_gc_page_completed_ps
		`); err != nil {
			t.Fatalf("seed pubsub subscriptions: %v", err)
		}

		gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{
			DefaultPolicy: pgqueue.RetentionPolicy{CompletedMessageTTL: time.Hour},
		})
		if err := gc.Collect(ctx); err != nil {
			t.Fatalf("collect: %v", err)
		}

		var remaining int
		if err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM pgqueue_msg_gc_page_completed_ps").Scan(&remaining); err != nil {
			t.Fatalf("count: %v", err)
		}
		if remaining != 0 {
			t.Errorf("expected all %d pubsub messages purged across pages, got %d remaining",
				seedCount, remaining)
		}
	})

	t.Run("pending/channel", func(t *testing.T) {
		const name = "gc-page-pending-ch"
		if err := pq.CreateChannel(ctx, name); err != nil {
			t.Fatalf("create channel: %v", err)
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO pgqueue_msg_gc_page_pending_ch (id, payload, created_at)
			SELECT uuidv7(), 'p'::bytea, NOW() - INTERVAL '2 hours'
			FROM generate_series(1, %d)
		`, seedCount)); err != nil {
			t.Fatalf("seed pending channel rows: %v", err)
		}

		gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{
			DefaultPolicy: pgqueue.RetentionPolicy{MaxPendingAge: time.Hour},
		})
		if err := gc.Collect(ctx); err != nil {
			t.Fatalf("collect: %v", err)
		}

		var remaining int
		if err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM pgqueue_msg_gc_page_pending_ch").Scan(&remaining); err != nil {
			t.Fatalf("count: %v", err)
		}
		if remaining != 0 {
			t.Errorf("expected all %d pending channel rows purged across pages, got %d remaining",
				seedCount, remaining)
		}
	})

	t.Run("pending/pubsub", func(t *testing.T) {
		const name = "gc-page-pending-ps"
		if err := pq.CreateTopic(ctx, name); err != nil {
			t.Fatalf("create topic: %v", err)
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO pgqueue_msg_gc_page_pending_ps (id, payload, created_at)
			SELECT uuidv7(), 'p'::bytea, NOW() - INTERVAL '2 hours'
			FROM generate_series(1, %d)
		`, seedCount)); err != nil {
			t.Fatalf("seed pubsub messages: %v", err)
		}
		// Two subscribers per message: sub-1 stale-pending (target of the
		// purge), sub-2 already acked. The acked sub row keeps the message off
		// the orphan-reclaim list within the same Collect() pass, so we can
		// distinctly assert that the *pending* purge only touches its own
		// rows — what the public doc on purgeOldPendingMessages promises.
		if _, err := db.ExecContext(ctx, `
			INSERT INTO pgqueue_sub_gc_page_pending_ps (message_id, subscriber_id, status)
			SELECT id, 'sub-1', 'pending' FROM pgqueue_msg_gc_page_pending_ps
		`); err != nil {
			t.Fatalf("seed pending subs: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO pgqueue_sub_gc_page_pending_ps (message_id, subscriber_id, status)
			SELECT id, 'sub-2', 'acked' FROM pgqueue_msg_gc_page_pending_ps
		`); err != nil {
			t.Fatalf("seed acked subs: %v", err)
		}

		gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{
			DefaultPolicy: pgqueue.RetentionPolicy{MaxPendingAge: time.Hour},
		})
		if err := gc.Collect(ctx); err != nil {
			t.Fatalf("collect: %v", err)
		}

		var pendingSubs, ackedSubs, msgRemaining int
		if err := db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM pgqueue_sub_gc_page_pending_ps WHERE status = 'pending'`,
		).Scan(&pendingSubs); err != nil {
			t.Fatalf("count pending subs: %v", err)
		}
		if err := db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM pgqueue_sub_gc_page_pending_ps WHERE status = 'acked'`,
		).Scan(&ackedSubs); err != nil {
			t.Fatalf("count acked subs: %v", err)
		}
		if err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM pgqueue_msg_gc_page_pending_ps").Scan(&msgRemaining); err != nil {
			t.Fatalf("count messages: %v", err)
		}
		if pendingSubs != 0 {
			t.Errorf("expected all %d pending pubsub subs purged across pages, got %d remaining",
				seedCount, pendingSubs)
		}
		if ackedSubs != seedCount {
			t.Errorf("expected %d acked subs untouched by pending purge, got %d",
				seedCount, ackedSubs)
		}
		if msgRemaining != seedCount {
			t.Errorf("expected message rows preserved by pending purge, got %d (want %d)",
				msgRemaining, seedCount)
		}
	})

	t.Run("dlq", func(t *testing.T) {
		const name = "gc-page-dlq"
		if err := pq.CreateChannel(ctx, name); err != nil {
			t.Fatalf("create channel: %v", err)
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO pgqueue_dlq_gc_page_dlq
				(original_message_id, payload, failure_reason, retry_count, moved_at)
			SELECT uuidv7(), 'p'::bytea, 'test', 0, NOW() - INTERVAL '2 hours'
			FROM generate_series(1, %d)
		`, seedCount)); err != nil {
			t.Fatalf("seed DLQ rows: %v", err)
		}

		gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{
			DefaultPolicy: pgqueue.RetentionPolicy{DLQRetention: time.Hour},
		})
		if err := gc.Collect(ctx); err != nil {
			t.Fatalf("collect: %v", err)
		}

		var remaining int
		if err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM pgqueue_dlq_gc_page_dlq").Scan(&remaining); err != nil {
			t.Fatalf("count: %v", err)
		}
		if remaining != 0 {
			t.Errorf("expected all %d DLQ rows purged across pages, got %d remaining",
				seedCount, remaining)
		}
	})

	t.Run("orphan", func(t *testing.T) {
		const name = "gc-page-orphan"
		if err := pq.CreateTopic(ctx, name); err != nil {
			t.Fatalf("create topic: %v", err)
		}
		// No subscribers → no sub rows → every inserted message is an orphan.
		// created_at is left fresh so purgeCompletedMessages (24h default TTL)
		// does not also delete them; reclaimOrphanTopicMessages alone should
		// drain the backlog.
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO pgqueue_msg_gc_page_orphan (id, payload)
			SELECT uuidv7(), 'p'::bytea
			FROM generate_series(1, %d)
		`, seedCount)); err != nil {
			t.Fatalf("seed orphan messages: %v", err)
		}

		// Default policy is fine: orphan reclaim runs regardless of policy.
		gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{})
		if err := gc.Collect(ctx); err != nil {
			t.Fatalf("collect: %v", err)
		}

		var remaining int
		if err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM pgqueue_msg_gc_page_orphan").Scan(&remaining); err != nil {
			t.Fatalf("count: %v", err)
		}
		if remaining != 0 {
			t.Errorf("expected all %d orphan messages reclaimed across pages, got %d remaining",
				seedCount, remaining)
		}
	})
}
