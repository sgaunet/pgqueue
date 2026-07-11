package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sgaunet/pgqueue"
)

// TestGCPreservesInFlightSubscriptionOnUnsubscribe is the regression for the
// silent pub/sub message loss on unsubscribe-while-processing. Unsubscribe is
// soft precisely so an in-flight message can still be drained, but the GC's
// purgeInactiveSubscriptions used to delete every row of an inactive subscriber
// unconditionally — including a row under a live claim. The consumer's later
// Ack then matched nothing and the message was lost (no DLQ, no retry).
//
// The fix preserves rows that are still being processed within a live claim
// window, so the Ack below must succeed even after a GC pass.
func TestGCPreservesInFlightSubscriptionOnUnsubscribe(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	const topicName = "unsub-inflight"
	const subID = "worker-1"
	if err := pq.CreateTopic(ctx, topicName); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := pq.Subscribe(ctx, topicName, subID); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if _, err := pq.Publish(ctx, topicName, []byte("in-flight")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Claim the message with a long visibility timeout so it stays in-flight
	// (status='processing', claim live) for the whole test.
	msg, err := pq.ReceiveTopic(ctx, topicName, subID,
		pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("receive topic: %v", err)
	}
	if msg == nil {
		t.Fatal("receive topic: nil message")
	}

	// Soft-unsubscribe while the message is still being processed.
	if err := pq.Unsubscribe(ctx, topicName, subID); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}

	// A GC pass must not delete the live in-flight subscription row.
	gc, err := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{})
	if err != nil {
		t.Fatalf("NewGarbageCollector: %v", err)
	}
	if err := gc.Collect(ctx); err != nil {
		t.Fatalf("gc collect: %v", err)
	}

	// The consumer finishes its work: the Ack must still succeed.
	if err := pq.Ack(ctx, msg.Receipt()); err != nil {
		t.Fatalf("ack after unsubscribe+GC: in-flight message was lost: %v", err)
	}
}

// TestReplayDLQRestoresChannelRetryBudget is the regression for the silent
// retry-budget loss on DLQ replay. A channel message reinstated from the DLQ
// must come back with the queue's configured max_retries, not the message-table
// column default of 0 — which channelMaxRetries reads as "no retries". With the
// budget lost, a replayed message re-dead-letters on its first failed delivery
// instead of getting its retries.
func TestReplayDLQRestoresChannelRetryBudget(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	const channelName = "replay-retry-budget"
	const maxRetries = 3
	if err := pq.CreateChannel(ctx, channelName,
		pgqueue.WithQueueMaxRetries(maxRetries)); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := pq.Publish(ctx, channelName, []byte("retry-me")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// driveToDLQ nacks the single in-flight message (negligible retry delay)
	// until it lands in the DLQ, returning how many nacks it took.
	driveToDLQ := func() int {
		nacks := 0
		for {
			dlq, err := pq.DLQStats(ctx, channelName, pgqueue.QueueTypeChannel)
			if err != nil {
				t.Fatalf("DLQ stats: %v", err)
			}
			if dlq.TotalCount == 1 {
				return nacks
			}
			msg, err := pq.ReceiveChannel(ctx, channelName)
			if err != nil {
				t.Fatalf("receive (after %d nacks): %v", nacks, err)
			}
			if err := pq.Nack(ctx, msg.Receipt(), "boom", pgqueue.WithRetryDelay(1)); err != nil {
				t.Fatalf("nack: %v", err)
			}
			nacks++
		}
	}

	// A fresh message with max-retries 3 dead-letters on the 4th nack
	// (retry_count+1 > maxRetry).
	if got := driveToDLQ(); got != maxRetries+1 {
		t.Fatalf("fresh message: dead-lettered after %d nacks, want %d", got, maxRetries+1)
	}

	res, err := pq.ReplayDLQ(ctx, channelName, pgqueue.QueueTypeChannel, pgqueue.ReplayOptions{})
	if err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}
	if res.Replayed != 1 {
		t.Fatalf("ReplayDLQ replayed %d, want 1", res.Replayed)
	}

	// The reinstated message must carry the configured cap again.
	msg, err := pq.ReceiveChannel(ctx, channelName)
	if err != nil {
		t.Fatalf("receive after replay: %v", err)
	}
	if msg.MaxRetries != maxRetries {
		t.Errorf("replayed message MaxRetries = %d, want %d (retry budget lost on replay)",
			msg.MaxRetries, maxRetries)
	}
	if err := pq.Nack(ctx, msg.Receipt(), "boom", pgqueue.WithRetryDelay(1)); err != nil {
		t.Fatalf("nack after replay: %v", err)
	}

	// A single post-replay nack must NOT dead-letter it...
	dlq, err := pq.DLQStats(ctx, channelName, pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("DLQ stats after replay nack: %v", err)
	}
	if dlq.TotalCount != 0 {
		t.Fatalf("replayed message dead-lettered after a single nack (DLQ=%d): retry budget not restored",
			dlq.TotalCount)
	}

	// ...and it should again take the full remaining budget to reach the DLQ
	// (one nack already spent above, so maxRetries more).
	if got := driveToDLQ(); got != maxRetries {
		t.Errorf("replayed message: dead-lettered after %d further nacks, want %d", got, maxRetries)
	}
}

// TestCreateQueueRejectsNegativeMaxRetries is the regression for the unvalidated
// negative max-retries cap. A negative value used to pass through to topics
// (which have no max_retries CHECK constraint), silently dead-lettering every
// message on its first failure. Both CreateChannel and CreateTopic must now
// reject it with ErrInvalidConfig.
func TestCreateQueueRejectsNegativeMaxRetries(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	if err := pq.CreateChannel(ctx, "neg-retry-ch",
		pgqueue.WithQueueMaxRetries(-1)); !errors.Is(err, pgqueue.ErrInvalidConfig) {
		t.Errorf("CreateChannel(-1): err = %v, want ErrInvalidConfig", err)
	}
	if err := pq.CreateTopic(ctx, "neg-retry-tp",
		pgqueue.WithQueueMaxRetries(-1)); !errors.Is(err, pgqueue.ErrInvalidConfig) {
		t.Errorf("CreateTopic(-1): err = %v, want ErrInvalidConfig", err)
	}
}

// TestMetadataTableNameCheckConstraint verifies the v8 hardening migration: a
// CHECK on pgqueue_metadata.table_name rejects any value outside the sanitized
// [a-z0-9_]+ charset, so an identifier that could break out of the DDL/DML
// interpolation paths can never be stored, even by direct SQL.
func TestMetadataTableNameCheckConstraint(t *testing.T) {
	_, db, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	bad := []string{"Bad-Name", "drop table x", `evil"; DROP`, "UPPER", "has.dot"}
	for _, name := range bad {
		_, err := db.ExecContext(ctx,
			`INSERT INTO pgqueue_metadata (queue_type, queue_name, table_name, config)
			 VALUES ('channel', $1, $1, '{}')`, name)
		if err == nil {
			t.Errorf("expected CHECK to reject table_name %q, but insert succeeded", name)
		}
	}

	// A sanitized name must still be accepted.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO pgqueue_metadata (queue_type, queue_name, table_name, config)
		 VALUES ('channel', 'ok_name', 'ok_name', '{}')`); err != nil {
		t.Errorf("valid sanitized table_name was rejected: %v", err)
	}
}
