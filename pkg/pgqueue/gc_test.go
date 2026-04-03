package pgqueue_test

import (
	"context"
	"testing"
	"time"

	"github.com/sgaunet/pgqueue/pkg/pgqueue"
)

func TestGarbageCollector(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel
	err := pq.CreateChannel(ctx, "gc-test", pgqueue.ChannelOptions{
		MaxRetries: 3,
	})
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
		if err := pq.AckChannel(ctx, "gc-test", msg.ID); err != nil {
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
	err := pq.CreateChannel(ctx, "gc-timeout-test", pgqueue.ChannelOptions{
		MaxRetries: 3,
	})
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
	err := pq.CreateChannel(ctx, "gc-zero-ttl-test", pgqueue.ChannelOptions{
		MaxRetries: 3,
	})
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

	if err := pq.AckChannel(ctx, "gc-zero-ttl-test", msg.ID); err != nil {
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
		err := pq.CreateChannel(ctx, name, pgqueue.ChannelOptions{
			MaxRetries: 3,
		})
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
		if err := pq.AckChannel(ctx, name, msg.ID); err != nil {
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

func TestPurgeQueue(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel
	err := pq.CreateChannel(ctx, "purge-test", pgqueue.ChannelOptions{})
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
