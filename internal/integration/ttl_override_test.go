package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sgaunet/pgqueue"
)

// setupTTLTestDB mirrors setupTestDB (pgqueue_test.go) but additionally sets a
// queue-wide default TTL via WithDefaultTTL, which setupTestDB does not expose.
// It is needed to reproduce bug #4 (WithQueueTTL(0) must override a non-zero
// default TTL) and its regression guard (an unset per-queue TTL must still
// inherit that default), so this builds its own Queue on top of the shared
// setupTestContainer helper instead of duplicating container setup.
func setupTTLTestDB(t *testing.T, defaultTTL time.Duration) (*pgqueue.Queue, func()) {
	t.Helper()
	ctx := context.Background()

	db, containerCleanup := setupTestContainer(t)

	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("failed to initialize schema: %v", err)
	}

	pq, err := pgqueue.New(ctx, db, pgqueue.WithDefaultTTL(defaultTTL))
	if err != nil {
		t.Fatalf("failed to init pgqueue: %v", err)
	}

	cleanup := func() {
		_ = pq.Close()
		containerCleanup()
	}

	return pq, cleanup
}

// TestChannelQueueTTLZeroOverridesDefault is the bug #4 regression test:
// WithQueueTTL(0) on a channel must mean "no expiry" even when the queue-wide
// WithDefaultTTL is non-zero, per WithQueueTTL's documented "Zero means no
// expiry" contract. Before the fix, getQueueTTL treated an explicit per-queue
// TTL of 0 as "not configured" and silently fell through to the non-zero
// default, so the message would be filtered out of ReceiveChannel and
// QueueDepth once that default elapsed, with no error.
func TestChannelQueueTTLZeroOverridesDefault(t *testing.T) {
	pq, cleanup := setupTTLTestDB(t, 1) // default TTL: 1ns, elapses almost immediately
	defer cleanup()
	ctx := context.Background()

	if err := pq.CreateChannel(ctx, "ttl-zero-override",
		pgqueue.WithQueueTTL(0)); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	if _, err := pq.Publish(ctx, "ttl-zero-override", []byte("never-expires")); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// intentional: proves the 1ns default TTL window has elapsed, so a queue
	// that inherited it (instead of honoring its own WithQueueTTL(0)) would
	// filter the message out on consume and out of the depth count.
	time.Sleep(10 * time.Millisecond) // intentional: let the 1ns default TTL expire

	depth, err := pq.QueueDepth(ctx, "ttl-zero-override", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("QueueDepth: %v", err)
	}
	if depth != 1 {
		t.Errorf("QueueDepth = %d, want 1: WithQueueTTL(0) must keep the message in the "+
			"consumable count past the queue-wide default TTL", depth)
	}

	msg, err := pq.ReceiveChannel(ctx, "ttl-zero-override",
		pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("WithQueueTTL(0) should keep the message consumable past the queue-wide "+
			"default TTL, got err=%v", err)
	}
	if msg == nil {
		t.Fatal("expected a message, got nil")
	}
	if string(msg.Payload) != "never-expires" {
		t.Errorf("payload = %q, want %q", msg.Payload, "never-expires")
	}
}

// TestTopicQueueTTLZeroOverridesDefault is the pub/sub counterpart of
// TestChannelQueueTTLZeroOverridesDefault: pubsub.go's ReceiveTopic reads the
// TTL through the same getQueueTTL call as channel.go, so the same
// WithQueueTTL(0)-overrides-the-default contract must hold for topics.
func TestTopicQueueTTLZeroOverridesDefault(t *testing.T) {
	pq, cleanup := setupTTLTestDB(t, 1) // default TTL: 1ns, elapses almost immediately
	defer cleanup()
	ctx := context.Background()

	if err := pq.CreateTopic(ctx, "ttl-zero-override-topic",
		pgqueue.WithQueueTTL(0)); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	if err := pq.Subscribe(ctx, "ttl-zero-override-topic", "sub-1"); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	if _, err := pq.Publish(ctx, "ttl-zero-override-topic", []byte("never-expires")); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// intentional: let the 1ns default TTL expire.
	time.Sleep(10 * time.Millisecond)

	msg, err := pq.ReceiveTopic(ctx, "ttl-zero-override-topic", "sub-1",
		pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("WithQueueTTL(0) should keep the message consumable past the queue-wide "+
			"default TTL, got err=%v", err)
	}
	if msg == nil {
		t.Fatal("expected a message, got nil")
	}
	if string(msg.Payload) != "never-expires" {
		t.Errorf("payload = %q, want %q", msg.Payload, "never-expires")
	}
}

// TestChannelQueueTTLUnsetInheritsDefault is the non-regression counterpart of
// TestChannelQueueTTLZeroOverridesDefault: a channel created without
// WithQueueTTL at all must still inherit and enforce the queue-wide
// WithDefaultTTL, exactly as before the fix. It guards against a fix that
// makes getQueueTTL return "no expiry" unconditionally instead of correctly
// telling an explicit WithQueueTTL(0) apart from no override.
func TestChannelQueueTTLUnsetInheritsDefault(t *testing.T) {
	pq, cleanup := setupTTLTestDB(t, 1) // default TTL: 1ns, elapses almost immediately
	defer cleanup()
	ctx := context.Background()

	if err := pq.CreateChannel(ctx, "ttl-unset-inherits"); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	if _, err := pq.Publish(ctx, "ttl-unset-inherits", []byte("expires-with-default")); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// intentional: let the 1ns default TTL expire.
	time.Sleep(10 * time.Millisecond)

	depth, err := pq.QueueDepth(ctx, "ttl-unset-inherits", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("QueueDepth: %v", err)
	}
	if depth != 0 {
		t.Errorf("QueueDepth = %d, want 0: a channel without WithQueueTTL must still inherit "+
			"and enforce the queue-wide default TTL", depth)
	}

	msg, err := pq.ReceiveChannel(ctx, "ttl-unset-inherits",
		pgqueue.WithVisibilityTimeout(30*time.Second))
	if !errors.Is(err, pgqueue.ErrQueueEmpty) {
		t.Fatalf("expected ErrQueueEmpty once the inherited default TTL elapsed, got err=%v msg=%v",
			err, msg)
	}
}
