package integration_test

import (
	"context"
	"testing"

	"github.com/sgaunet/pgqueue"
)

// TestPagedReset verifies M3/FR-024: the visibility-timeout reset reclaims a
// backlog larger than a single page. resetTimedOutMessages paginates at
// retentionPurgePageSize (1000) rows per statement, so a backlog of 2500
// timed-out messages must be fully reset to pending across pages — a buggy loop
// that stopped after the first page would leave ~1500 stuck in 'processing'.
func TestPagedReset(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	const timedOut = 2500 // > 2 * retentionPurgePageSize(1000)
	if err := pq.CreateChannel(ctx, "paged"); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	seedChannelMessages(t, db, "paged", channelSeed{TimedOut: timedOut})

	// The seed leaves max_retries at the channel column default (0), which the
	// GC's exhausted-message pass would treat as retry-exhausted and divert to
	// the DLQ before the reset runs. Give the rows retry headroom so they flow
	// through the paged reset this test exercises rather than being promoted.
	msgTable := "pgqueue_msg_" + queueTableName(t, db, "paged")
	//nolint:gosec // G201: table name derived from a sanitized queue name in a test
	if _, err := db.ExecContext(ctx, "UPDATE "+msgTable+" SET max_retries = 100"); err != nil {
		t.Fatalf("bump max_retries: %v", err)
	}

	gc, err := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{})
	if err != nil {
		t.Fatalf("new gc: %v", err)
	}
	if err := gc.Collect(ctx); err != nil {
		t.Fatalf("gc collect: %v", err)
	}

	var pending, processing int
	//nolint:gosec // G201: table name derived from a sanitized queue name in a test
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM "+msgTable+" WHERE status='pending'").Scan(&pending); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	//nolint:gosec // G201: table name derived from a sanitized queue name in a test
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM "+msgTable+" WHERE status='processing'").Scan(&processing); err != nil {
		t.Fatalf("count processing: %v", err)
	}

	if pending != timedOut {
		t.Fatalf("expected all %d timed-out messages reset to pending across pages, got %d", timedOut, pending)
	}
	if processing != 0 {
		t.Fatalf("expected 0 messages still in processing after paged reset, got %d", processing)
	}
}
