package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sgaunet/pgqueue/pkg/pgqueue"
)

// publishOne publishes a single payload to a channel, failing the test on error.
func publishOne(t *testing.T, pq *pgqueue.Queue, channel string, payload []byte) {
	t.Helper()
	if _, err := pq.PublishChannel(context.Background(), channel, payload); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

// TestListDLQMessagesKeysetPagination verifies that ListDLQMessages walks the
// dead-letter queue with keyset pagination, returning every message exactly
// once across pages and never skipping or repeating one.
func TestListDLQMessagesKeysetPagination(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	const channelName = "dlq-pagination"
	if err := pq.CreateChannel(ctx, channelName,
		pgqueue.WithQueueMaxRetries(1)); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// Publish 25 messages, then nack everything available (with a negligible
	// retry delay) until the queue is empty: with max-retries 1 each message
	// takes two nacks to reach the DLQ, so the loop drains all 25 into it.
	const total = 25
	for i := range total {
		publishOne(t, pq, channelName, fmt.Appendf(nil, "msg-%02d", i))
	}
	for {
		msg, err := pq.ReceiveChannel(ctx, channelName)
		if errors.Is(err, pgqueue.ErrQueueEmpty) {
			break
		}
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		if err := pq.Nack(ctx, msg.Receipt(), "fail", pgqueue.WithRetryDelay(1)); err != nil {
			t.Fatalf("nack: %v", err)
		}
	}

	// Page through the DLQ 10 at a time and assert each id appears once.
	seen := make(map[uuid.UUID]int)
	page := pgqueue.DLQPage{Limit: 10}
	pages := 0
	for {
		msgs, next, err := pq.ListDLQMessages(ctx, channelName, pgqueue.QueueTypeChannel, page)
		if err != nil {
			t.Fatalf("ListDLQMessages: %v", err)
		}
		if len(msgs) == 0 {
			break
		}
		pages++
		for _, m := range msgs {
			seen[m.ID]++
		}
		if len(msgs) < page.Limit {
			break
		}
		page = next
	}

	if len(seen) != total {
		t.Fatalf("expected %d distinct DLQ messages, got %d", total, len(seen))
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("DLQ message %s returned %d times across pages", id, count)
		}
	}
	if pages < 3 {
		t.Fatalf("expected pagination across >=3 pages, got %d", pages)
	}

	// DLQStats should agree on the total.
	stats, err := pq.DLQStats(ctx, channelName, pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("DLQStats: %v", err)
	}
	if stats.TotalCount != total {
		t.Fatalf("DLQStats.TotalCount = %d, want %d", stats.TotalCount, total)
	}
}

// TestReplayDLQLargeBacklogPaged verifies that replaying a large DLQ backlog
// processes it in bounded keyset pages (FR-025) and reinstates every message.
func TestReplayDLQLargeBacklogPaged(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large-backlog replay in -short mode")
	}
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	// No dash: the physical table name is used directly in raw SQL below.
	const channelName = "dlqbigbacklog"
	if err := pq.CreateChannel(ctx, channelName); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// Insert 10,000 DLQ rows directly: driving them through nack would be far
	// too slow, and the replay path is what is under test here.
	const backlog = 10000
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO pgqueue_dlq_%s (original_message_id, payload, failure_reason, retry_count)
		SELECT uuidv7(), convert_to('p' || g, 'UTF8'), 'seeded', 3
		FROM generate_series(1, %d) g
	`, channelName, backlog)); err != nil {
		t.Fatalf("seed DLQ backlog: %v", err)
	}

	res, err := pq.ReplayDLQ(ctx, channelName, pgqueue.QueueTypeChannel,
		pgqueue.ReplayOptions{Confirm: true})
	if err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}
	if res.Replayed != backlog {
		t.Fatalf("replayed %d messages, want %d", res.Replayed, backlog)
	}

	// The DLQ must now be empty and the message table must hold the backlog.
	var dlqLeft, msgCount int
	if err := db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM pgqueue_dlq_%s", channelName)).Scan(&dlqLeft); err != nil {
		t.Fatalf("count DLQ: %v", err)
	}
	if dlqLeft != 0 {
		t.Fatalf("expected empty DLQ after replay, got %d", dlqLeft)
	}
	if err := db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM pgqueue_msg_%s", channelName)).Scan(&msgCount); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if msgCount != backlog {
		t.Fatalf("expected %d reinstated messages, got %d", backlog, msgCount)
	}
}
