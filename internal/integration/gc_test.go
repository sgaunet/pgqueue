package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sgaunet/pgqueue/pkg/pgqueue"
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
		msg, err := pq.ConsumeFromChannel(ctx, "gc-test", 30*time.Second)
		if err != nil {
			t.Fatalf("failed to consume message: %v", err)
		}
		if err := pq.AckChannel(ctx, "gc-test", msg.Receipt()); err != nil {
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
	msg, err := pq.ConsumeFromChannel(ctx, "gc-timeout-test", 100*time.Millisecond)
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
	msg2, err := pq.ConsumeFromChannel(ctx, "gc-timeout-test", 30*time.Second)
	if err != nil {
		t.Fatalf("failed to re-consume message: %v", err)
	}

	if msg2.ID != msg.ID {
		t.Errorf("expected same message ID after reset, got different ID")
	}
}

func TestGarbageCollectorZeroTTLPreservesMessages(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel
	err := pq.CreateChannel(ctx, "gc-zero-ttl-test", pgqueue.WithQueueMaxRetries(3))
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish and acknowledge a message
	_, err = pq.Publish(ctx, "gc-zero-ttl-test", []byte("test message"))
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	msg, err := pq.ConsumeFromChannel(ctx, "gc-zero-ttl-test", 30*time.Second)
	if err != nil {
		t.Fatalf("failed to consume message: %v", err)
	}

	if err := pq.AckChannel(ctx, "gc-zero-ttl-test", msg.Receipt()); err != nil {
		t.Fatalf("failed to ack message: %v", err)
	}

	// Wait for processed_at to be set
	time.Sleep(100 * time.Millisecond)

	// Verify message is completed
	stats, err := pq.GetStats(ctx, "gc-zero-ttl-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if stats.CompletedCount != 1 {
		t.Errorf("expected 1 completed message, got %d", stats.CompletedCount)
	}

	// Run garbage collector with TTL=0 (never expire)
	gcConfig := pgqueue.GarbageCollectorConfig{
		DefaultPolicy: pgqueue.RetentionPolicy{
			CompletedMessageTTL: 0, // Never expire
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
	stats, err = pq.GetStats(ctx, "gc-zero-ttl-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}

	if stats.CompletedCount != 1 {
		t.Errorf("expected 1 completed message to be preserved with TTL=0, got %d", stats.CompletedCount)
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

		msg, err := pq.ConsumeFromChannel(ctx, name, 30*time.Second)
		if err != nil {
			t.Fatalf("failed to consume message: %v", err)
		}
		if err := pq.AckChannel(ctx, name, msg.Receipt()); err != nil {
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
			msg, err := pq.ConsumeFromTopic(ctx, "gc-pubsub-test", sub, 30*time.Second)
			if err != nil {
				t.Fatalf("failed to consume message for %s: %v", sub, err)
			}
			if err := pq.AckTopic(ctx, "gc-pubsub-test", sub, msg.Receipt()); err != nil {
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
		msg, err := pq.ConsumeFromTopic(ctx, "gc-pubsub-partial", "sub-1", 30*time.Second)
		if err != nil {
			t.Fatalf("failed to consume message for sub-1: %v", err)
		}
		if err := pq.AckTopic(ctx, "gc-pubsub-partial", "sub-1", msg.Receipt()); err != nil {
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

	// Purge without confirmation should fail
	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{})
	err = gc.PurgeQueue(ctx, "purge-test", pgqueue.QueueTypeChannel, false)
	if err == nil {
		t.Error("expected error when purging without confirmation")
	}

	// Purge with confirmation
	err = gc.PurgeQueue(ctx, "purge-test", pgqueue.QueueTypeChannel, true)
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
	msg, err := pq.ConsumeFromTopic(ctx, "gc-pubsub-vt", "sub-vt", 1*time.Millisecond)
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
	msg2, err := pq.ConsumeFromTopic(ctx, "gc-pubsub-vt", "sub-vt", 30*time.Second)
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
		msg, err := pq.ConsumeFromChannel(ctx, "gc-dlq-ret", 30*time.Second)
		if err != nil {
			t.Fatalf("failed to consume: %v", err)
		}
		if err := pq.NackChannel(ctx, "gc-dlq-ret", msg.Receipt(), "fail"); err != nil {
			t.Fatalf("first nack failed: %v", err)
		}
		// Second consume + nack: exceeds max retries -> DLQ
		msg, err = pq.ConsumeFromChannel(ctx, "gc-dlq-ret", 30*time.Second)
		if err != nil {
			t.Fatalf("failed to consume for DLQ: %v", err)
		}
		if err := pq.NackChannel(ctx, "gc-dlq-ret", msg.Receipt(), "fail again"); err != nil {
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
			msg, err := pq.ConsumeFromChannel(ctx, name, 30*time.Second)
			if err != nil {
				t.Fatalf("failed to consume from %s: %v", name, err)
			}
			if err := pq.AckChannel(ctx, name, msg.Receipt()); err != nil {
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
