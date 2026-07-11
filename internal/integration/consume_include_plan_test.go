package integration_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// consumableNullIndexName and consumableTimeoutIndexName name the per-queue
// channel-message indexes createChannelIndexes emits, matching the naming
// already used throughout the plan-regression suite (consume_plan_test.go,
// consume_scale_test.go).
func consumableNullIndexName(table string) string {
	return "idx_pgqueue_msg_" + table + "_consumable_null"
}

func consumableTimeoutIndexName(table string) string {
	return "idx_pgqueue_msg_" + table + "_consumable_timeout"
}

// filterDropOf sums "Rows Removed by Filter" across every node in an ANALYZE
// plan, for diagnostic logging only (see TestConsumePlanRetryStormStillUsesIndex
// for why this is logged rather than asserted).
func filterDropOf(qp queryPlan) float64 {
	var total float64
	for _, n := range qp.nodes {
		total += n.RowsRemovedByFilter
	}
	return total
}

// planHasNodeType reports whether any node in the plan has the given
// PostgreSQL EXPLAIN "Node Type" (e.g. "Index Only Scan").
func planHasNodeType(qp queryPlan, nodeType string) bool {
	for _, n := range qp.nodes {
		if n.NodeType == nodeType {
			return true
		}
	}
	return false
}

// TestConsumableIndexIncludeColumnsPresent confirms PostgreSQL 18 accepts
// INCLUDE on a partial btree index for every shape pgqueue emits -- the
// single-key-column channel message indexes and the composite-key pub/sub
// subscription indexes (#11) -- and that the resulting catalog definition
// actually carries the INCLUDE clause. CREATE INDEX IF NOT EXISTS silently
// no-ops against a pre-existing index of the same name, so this reads the
// definition back with pg_get_indexdef rather than only checking that the DDL
// ran without error.
func TestConsumableIndexIncludeColumnsPresent(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	const (
		channel = "incddlchan"
		topic   = "incddltop"
	)
	if err := pq.CreateChannel(ctx, channel); err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	if err := pq.CreateTopic(ctx, topic); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	chanTable := queueTableName(t, db, channel)
	topicTable := queueTableName(t, db, topic)

	cases := []struct {
		index   string
		include string
	}{
		{consumableNullIndexName(chanTable), "INCLUDE (available_at)"},
		{consumableTimeoutIndexName(chanTable), "INCLUDE (visibility_timeout)"},
		{"idx_pgqueue_sub_" + topicTable + "_consumable_null", "INCLUDE (available_at)"},
		{"idx_pgqueue_sub_" + topicTable + "_consumable_timeout", "INCLUDE (visibility_timeout)"},
	}
	for _, c := range cases {
		def := indexDef(t, db, c.index)
		if !strings.Contains(def, c.include) {
			t.Errorf("index %s definition = %q, want it to contain %q", c.index, def, c.include)
		}
	}
}

// TestConsumePlanRetryStormStillUsesIndex is the direct regression guard for
// the #11 DDL change: adding INCLUDE (available_at) / INCLUDE
// (visibility_timeout) to the consumable partial indexes must not change which
// index the planner selects, and a channel must still correctly deliver a
// ready message and reclaim a timed-out one when each sits behind a large
// storm of not-yet-actionable rows -- the shape a crash-looping consumer with
// a configured BackoffPolicy produces: a retried message keeps its original
// (low) id but has its available_at pushed into the future, so it sits ahead
// of newer, genuinely-ready messages in the id-ordered scan; an in-flight
// message under a live claim sits ahead of an older message whose claim
// actually timed out.
//
// It deliberately does not assert "Rows Removed by Filter" is small: that
// filter drop is expected to scale with the storm size today (logged, not
// asserted -- see TestConsumableIndexIncludeGroundwork for why), because both
// probes carry FOR UPDATE SKIP LOCKED. This test only guards that the index is
// still selected, the plan never degrades to a sequential scan, and delivery
// stays correct.
func TestConsumePlanRetryStormStillUsesIndex(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	const channel = "retrystorm"
	if err := pq.CreateChannel(ctx, channel); err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	table := queueTableName(t, db, channel)
	msgTable := "pgqueue_msg_" + table

	// Two storms, seeded first so their ids sort before the rows added below:
	// 20000 backoff-sleeping pending rows (probe A candidates) and 5000 held
	// in-flight processing rows not yet timed out (probe B candidates), both
	// behind a deep completed/DLQ history for index selectivity.
	seedChannelMessages(t, db, channel, channelSeed{
		PendingFuture: 20000,
		InFlight:      5000,
		CompletedOld:  50000,
		DLQ:           5000,
	})

	// The row each probe must still find, added after the storms so their ids
	// sort last -- the worst case for an id-ordered scan. The timed-out row is
	// seeded directly, rather than via seedChannelMessages's TimedOut category,
	// so it carries a positive max_retries: the table default max_retries=0
	// would make retryCount+1 > maxRetry true on the very first reclaim
	// (0+1 > 0), moving it straight to the DLQ instead of being redelivered.
	if _, err := pq.Publish(ctx, channel, []byte("ready")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	//nolint:gosec // G201: msgTable is a library-sanitized table name, not user input
	seedTimedOut := fmt.Sprintf(`
		INSERT INTO %s (id, payload, status, visibility_timeout, claim_id, max_retries)
		VALUES (uuidv7(), '\x00'::bytea, 'processing', NOW() - interval '1 minute', uuidv7(), 3)`,
		msgTable)
	if _, err := db.ExecContext(ctx, seedTimedOut); err != nil {
		t.Fatalf("seed timed-out row: %v", err)
	}
	analyzeTables(t, db, msgTable)

	pendingPlan := explainAnalyzePlan(t, db, channelPendingProbeSQL(msgTable))
	assertPlanUsesIndex(t, pendingPlan, consumableNullIndexName(table))
	assertNoSeqScan(t, pendingPlan, msgTable)

	reclaimPlan := explainAnalyzePlan(t, db, channelReclaimProbeSQL(msgTable))
	assertPlanUsesIndex(t, reclaimPlan, consumableTimeoutIndexName(table))
	assertNoSeqScan(t, reclaimPlan, msgTable)

	t.Logf("pending probe: rows removed by filter=%.0f, shared blocks=%.0f",
		filterDropOf(pendingPlan), pendingPlan.maxSharedBlocks())
	t.Logf("reclaim probe: rows removed by filter=%.0f, shared blocks=%.0f",
		filterDropOf(reclaimPlan), reclaimPlan.maxSharedBlocks())

	// Behavioral proof: both the ready message and the reclaimed timed-out
	// message are still delivered correctly despite their respective storms
	// (pending-first tie-break: probe A wins the first ReceiveChannel call).
	msg, err := pq.ReceiveChannel(ctx, channel)
	if err != nil {
		t.Fatalf("ReceiveChannel (pending): %v", err)
	}
	if msg == nil || string(msg.Payload) != "ready" {
		t.Fatalf("ReceiveChannel (pending) = %v, want the ready message", msg)
	}

	reclaimed, err := pq.ReceiveChannel(ctx, channel)
	if err != nil {
		t.Fatalf("ReceiveChannel (reclaim): %v", err)
	}
	if reclaimed == nil {
		t.Fatal("ReceiveChannel (reclaim) = nil, want the reclaimed timed-out message")
	}
}

// TestConsumableIndexIncludeGroundwork pins down *why* the retry-storm probes
// above still pay a heap fetch for every not-yet-ready candidate despite the
// INCLUDE (available_at) index (#11): PostgreSQL's planner never produces an
// Index Only Scan under a row-locking clause (FOR UPDATE / FOR UPDATE SKIP
// LOCKED) on the locked relation -- taking the lock, and SKIP LOCKED's
// already-locked check, both require the heap tuple, which is exactly what a
// plain Index Only Scan uses the visibility map to skip. The two-probe
// consume path (channelConsumeQueries in channel.go) and the GC's bulk
// reclaim (resetTimedOutMessages / resetTimedOutSubscriptions in gc.go) both
// require FOR UPDATE ... SKIP LOCKED for correctness -- it is what lets two
// consumers, or a consumer and the GC, race for the same row without
// double-claiming it -- so neither can benefit from the INCLUDE columns as
// written today.
//
// This test proves both halves of that claim against the real index
// PostgreSQL 18 builds for a channel, using a query that selects only id (the
// sole column consumableNull's key + INCLUDE actually cover) so any plan-shape
// difference is attributable to the locking clause alone, never to needing
// payload/metadata/etc.
//
// If a future PostgreSQL release changes this and the "want an Index Only
// Scan" branch below starts failing on the *locked* query, the #11 finding
// becomes fixable purely at the DDL level and the limitation notes in
// pgqueue.go's createChannelIndexes/createPubSubIndexes should be revisited.
func TestConsumableIndexIncludeGroundwork(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	const channel = "includemech"
	if err := pq.CreateChannel(ctx, channel); err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}

	// A storm of backoff-sleeping pending rows behind a deep non-pending
	// history, so the partial index is selective enough to be chosen over a
	// sequential scan (an unselective partial index -- e.g. every row pending
	// -- is cheaper to seq scan, which would defeat this comparison).
	seedChannelMessages(t, db, channel, channelSeed{
		PendingFuture: 5000,
		CompletedOld:  20000,
	})

	table := queueTableName(t, db, channel)
	msgTable := "pgqueue_msg_" + table
	consumableNull := consumableNullIndexName(table)

	// VACUUM sets the visibility map bit an Index Only Scan relies on to skip
	// the heap. Production relies on the aggressive per-table autovacuum
	// (highChurnStorageParams) for the same effect; this VACUUM mirrors that
	// steady state rather than a freshly bulk-loaded, not-yet-vacuumed table.
	if _, err := db.ExecContext(ctx, "VACUUM (ANALYZE) "+msgTable); err != nil {
		t.Fatalf("VACUUM: %v", err)
	}

	pendingProbe := fmt.Sprintf(`
		SELECT id FROM %s
		WHERE status = 'pending' AND available_at <= NOW()
		ORDER BY id
		LIMIT 1`, msgTable)
	pendingProbeLocked := pendingProbe + "\n\t\tFOR UPDATE SKIP LOCKED"

	withoutLock := explainAnalyzePlan(t, db, pendingProbe)
	assertPlanUsesIndex(t, withoutLock, consumableNull)
	if !planHasNodeType(withoutLock, "Index Only Scan") {
		t.Fatalf("without a row-locking clause, want an Index Only Scan; node types: %v",
			withoutLock.nodeTypes())
	}

	withLock := explainAnalyzePlan(t, db, pendingProbeLocked)
	assertPlanUsesIndex(t, withLock, consumableNull)
	if planHasNodeType(withLock, "Index Only Scan") {
		t.Fatalf("FOR UPDATE SKIP LOCKED unexpectedly produced an Index Only Scan "+
			"(PostgreSQL is expected to never do this: locking requires the heap "+
			"tuple); node types: %v -- if this now passes, the #11 finding may be "+
			"fixable purely via INCLUDE after all",
			withLock.nodeTypes())
	}

	// The locked scan pays for every candidate the unlocked scan skipped via
	// the visibility map: it must touch materially more shared buffers for the
	// identical predicate over the identical storm.
	if withLock.maxSharedBlocks() <= withoutLock.maxSharedBlocks() {
		t.Fatalf("FOR UPDATE SKIP LOCKED scan touched %.0f shared blocks, want more "+
			"than the unlocked Index Only Scan's %.0f (it should pay a heap fetch per "+
			"candidate the unlocked scan did not)",
			withLock.maxSharedBlocks(), withoutLock.maxSharedBlocks())
	}
}
