package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
)

// This file provides large-table seeding helpers for the performance
// plan-regression tests (US1). They insert rows directly with server-side
// generate_series + uuidv7() rather than through the publish API, so a table can
// be brought to hundreds of thousands of rows in one statement — fast enough to
// run in the integration suite while still exercising the planner against a
// realistic history depth.

// channelSeed describes how many channel message rows to create in each
// lifecycle state. The categories are deliberately explicit (rather than a
// single "completed" count) so a test can shape selectivity — e.g. many
// not-yet-purgeable completed rows plus a few old ones — to exercise a specific
// query plan.
type channelSeed struct {
	Pending         int // status=pending, available_at<=NOW() (immediately consumable)
	PendingFuture   int // status=pending, available_at in the future (backoff-deferred)
	TimedOut        int // status=processing, visibility_timeout in the past (reclaimable)
	InFlight        int // status=processing, visibility_timeout in the future (held)
	CompletedRecent int // status=completed, processed_at=NOW() (inside any retention TTL)
	CompletedOld    int // status=completed, processed_at 30 days ago (past any short TTL)
	DLQ             int // dead-letter rows
}

// queueTableName resolves the sanitized per-queue table suffix recorded in
// pgqueue_metadata for a created queue, so seeding targets the real physical
// tables regardless of dash/underscore sanitization.
func queueTableName(t *testing.T, db *sql.DB, queueName string) string {
	t.Helper()
	var tableName string
	err := db.QueryRowContext(context.Background(),
		`SELECT table_name FROM pgqueue_metadata WHERE queue_name = $1`,
		queueName,
	).Scan(&tableName)
	if err != nil {
		t.Fatalf("queueTableName(%q): %v", queueName, err)
	}
	return tableName
}

// seedChannelMessages bulk-inserts rows into a channel's message and DLQ tables
// according to seed. The channel must already exist (CreateChannel). It runs
// ANALYZE afterward so the planner has fresh statistics for EXPLAIN assertions.
func seedChannelMessages(t *testing.T, db *sql.DB, channelName string, seed channelSeed) {
	t.Helper()
	ctx := context.Background()
	table := queueTableName(t, db, channelName)
	msgTable := "pgqueue_msg_" + table
	dlqTable := "pgqueue_dlq_" + table

	// Each category is a single INSERT ... SELECT over generate_series. id is an
	// app-supplied uuidv7() (the channel msg table has no id DEFAULT), which also
	// gives roughly chronological ORDER BY id.
	insert := func(label, stmt string, count int) {
		if count <= 0 {
			return
		}
		if _, err := db.ExecContext(ctx, stmt, count); err != nil {
			t.Fatalf("seedChannelMessages %s (%d rows): %v", label, count, err)
		}
	}

	insert("pending", fmt.Sprintf(`
		INSERT INTO %s (id, payload, created_at, status, available_at)
		SELECT uuidv7(), '\x00'::bytea, NOW() - (g * interval '1 second'), 'pending', NOW()
		FROM generate_series(1, $1) g`, msgTable), seed.Pending)

	insert("pending_future", fmt.Sprintf(`
		INSERT INTO %s (id, payload, created_at, status, available_at)
		SELECT uuidv7(), '\x00'::bytea, NOW() - (g * interval '1 second'), 'pending', NOW() + interval '1 hour'
		FROM generate_series(1, $1) g`, msgTable), seed.PendingFuture)

	insert("timed_out", fmt.Sprintf(`
		INSERT INTO %s (id, payload, created_at, status, visibility_timeout, claim_id)
		SELECT uuidv7(), '\x00'::bytea, NOW() - (g * interval '1 second'), 'processing',
		       NOW() - interval '1 minute', uuidv7()
		FROM generate_series(1, $1) g`, msgTable), seed.TimedOut)

	insert("in_flight", fmt.Sprintf(`
		INSERT INTO %s (id, payload, created_at, status, visibility_timeout, claim_id)
		SELECT uuidv7(), '\x00'::bytea, NOW() - (g * interval '1 second'), 'processing',
		       NOW() + interval '1 hour', uuidv7()
		FROM generate_series(1, $1) g`, msgTable), seed.InFlight)

	insert("completed_recent", fmt.Sprintf(`
		INSERT INTO %s (id, payload, created_at, status, processed_at)
		SELECT uuidv7(), '\x00'::bytea, NOW() - (g * interval '1 second'), 'completed', NOW()
		FROM generate_series(1, $1) g`, msgTable), seed.CompletedRecent)

	insert("completed_old", fmt.Sprintf(`
		INSERT INTO %s (id, payload, created_at, status, processed_at)
		SELECT uuidv7(), '\x00'::bytea, NOW() - interval '30 days', 'completed', NOW() - interval '30 days'
		FROM generate_series(1, $1) g`, msgTable), seed.CompletedOld)

	insert("dlq", fmt.Sprintf(`
		INSERT INTO %s (original_message_id, payload, failure_reason, retry_count, moved_at)
		SELECT uuidv7(), '\x00'::bytea, 'seed', 3, NOW() - (g * interval '1 second')
		FROM generate_series(1, $1) g`, dlqTable), seed.DLQ)

	analyzeTables(t, db, msgTable, dlqTable)
}

// topicSeed describes how many per-subscriber subscription rows to create in
// each lifecycle state for a single subscriber. Every subscription row is backed
// by its own message row (the FK requires it).
type topicSeed struct {
	Pending  int // status=pending, available now (consumable)
	TimedOut int // status=processing, visibility_timeout in the past (reclaimable)
	Acked    int // status=acked (delivered history the consume path must skip)
}

// seedTopicMessages bulk-inserts message rows and matching per-subscriber
// subscription rows into a topic's tables. The topic and subscriber must already
// exist. Each category inserts messages and their subscription rows atomically
// via a CTE so the FK is always satisfied.
func seedTopicMessages(
	t *testing.T, db *sql.DB, topicName, subscriberID string, seed topicSeed,
) {
	t.Helper()
	ctx := context.Background()
	table := queueTableName(t, db, topicName)
	msgTable := "pgqueue_msg_" + table
	subTable := "pgqueue_sub_" + table

	seedCat := func(label, subCols, subVals string, count int) {
		if count <= 0 {
			return
		}
		//nolint:gosec // test helper: table names are library-sanitized, subCols/subVals are in-tree literals
		stmt := fmt.Sprintf(`
			WITH ins AS (
				INSERT INTO %s (id, payload, created_at)
				SELECT uuidv7(), '\x00'::bytea, NOW() - (g * interval '1 second')
				FROM generate_series(1, $2) g
				RETURNING id
			)
			INSERT INTO %s (message_id, subscriber_id, status%s)
			SELECT id, $1, %s FROM ins`,
			msgTable, subTable, subCols, subVals)
		if _, err := db.ExecContext(ctx, stmt, subscriberID, count); err != nil {
			t.Fatalf("seedTopicMessages %s (%d rows): %v", label, count, err)
		}
	}

	seedCat("pending", ", available_at", "'pending', NOW()", seed.Pending)
	seedCat("timed_out", ", visibility_timeout, claim_id",
		"'processing', NOW() - interval '1 minute', uuidv7()", seed.TimedOut)
	seedCat("acked", ", acked_at", "'acked', NOW()", seed.Acked)

	analyzeTables(t, db, msgTable, subTable)
}

// analyzeTables runs ANALYZE on each table so EXPLAIN reflects the seeded row
// distribution rather than stale (empty-table) statistics.
func analyzeTables(t *testing.T, db *sql.DB, tables ...string) {
	t.Helper()
	ctx := context.Background()
	for _, tbl := range tables {
		if _, err := db.ExecContext(ctx, "ANALYZE "+tbl); err != nil {
			t.Fatalf("ANALYZE %s: %v", tbl, err)
		}
	}
}
