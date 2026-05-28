package integration_test

import (
	"context"
	"fmt"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sgaunet/pgqueue"
)

// TestZeroSubscriberTopicMessageReclaimed verifies that a message published to
// a topic with no subscribers — and therefore with no subscription rows and no
// possible delivery — is reclaimed (deleted) by the garbage collector (FR-027),
// while a message that still has live subscriptions is left untouched.
func TestZeroSubscriberTopicMessageReclaimed(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	// No dash: the physical table name is used directly in raw SQL below.
	const topicName = "zerosubtopic"
	if err := pq.CreateTopic(ctx, topicName); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Publish with zero subscribers: this message can never be delivered.
	if _, err := pq.Publish(ctx, topicName, []byte("orphan")); err != nil {
		t.Fatalf("publish orphan: %v", err)
	}

	// Now register a subscriber and publish a second message — that one has a
	// live subscription and must survive the GC pass.
	if err := pq.Subscribe(ctx, topicName, "live-sub"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if _, err := pq.Publish(ctx, topicName, []byte("delivered")); err != nil {
		t.Fatalf("publish delivered: %v", err)
	}

	countMessages := func() int {
		t.Helper()
		var n int
		if err := db.QueryRowContext(ctx,
			fmt.Sprintf("SELECT COUNT(*) FROM pgqueue_msg_%s", topicName)).Scan(&n); err != nil {
			t.Fatalf("count messages: %v", err)
		}
		return n
	}

	if got := countMessages(); got != 2 {
		t.Fatalf("expected 2 messages before GC, got %d", got)
	}

	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{})
	if err := gc.Collect(ctx); err != nil {
		t.Fatalf("GC Collect: %v", err)
	}

	// The orphan must be reclaimed; the message with a live subscription stays.
	if got := countMessages(); got != 1 {
		t.Fatalf("expected 1 message after GC reclaim, got %d", got)
	}

	// The surviving message must still be consumable by its subscriber.
	msg, err := pq.ReceiveTopic(ctx, topicName, "live-sub")
	if err != nil {
		t.Fatalf("receive surviving message: %v", err)
	}
	if string(msg.Payload) != "delivered" {
		t.Fatalf("surviving message payload = %q, want %q", msg.Payload, "delivered")
	}
}
