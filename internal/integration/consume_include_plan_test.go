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
// plan, for diagnostic logging alongside the assertNoLargeFilterDrop assertions
// in TestConsumePlanRetryStormStillUsesIndex.
func filterDropOf(qp queryPlan) float64 {
	var total float64
	for _, n := range qp.nodes {
		total += n.RowsRemovedByFilter
	}
	return total
}

// TestConsumableIndexKeyColumns confirms the consume partial indexes lead with
// the eligibility timestamp as their leading key (#11) -- the shape that turns
// available_at <= NOW() / visibility_timeout < NOW() into a btree range boundary
// rather than a Filter -- for every shape pgqueue emits: the channel message
// indexes keyed (available_at, id) / (visibility_timeout, id) and the pub/sub
// subscription indexes keyed (subscriber_id, available_at, message_id) /
// (subscriber_id, visibility_timeout, message_id). CREATE INDEX IF NOT EXISTS
// silently no-ops against a pre-existing index of the same name, so this reads
// the definition back with pg_get_indexdef. It also asserts the former INCLUDE
// clause is gone -- an INCLUDE column may not also be a key column, and the range
// key, not INCLUDE, is what fixes the sleeping-row scan.
func TestConsumableIndexKeyColumns(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	const (
		channel = "keycolchan"
		topic   = "keycoltop"
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
		index string
		key   string
	}{
		{consumableNullIndexName(chanTable), "(available_at, id)"},
		{consumableTimeoutIndexName(chanTable), "(visibility_timeout, id)"},
		{"idx_pgqueue_sub_" + topicTable + "_consumable_null", "(subscriber_id, available_at, message_id)"},
		{"idx_pgqueue_sub_" + topicTable + "_consumable_timeout", "(subscriber_id, visibility_timeout, message_id)"},
	}
	for _, c := range cases {
		def := indexDef(t, db, c.index)
		if !strings.Contains(def, c.key) {
			t.Errorf("index %s definition = %q, want it to contain key %q", c.index, def, c.key)
		}
		if strings.Contains(def, "INCLUDE") {
			t.Errorf("index %s definition = %q, want no INCLUDE clause "+
				"(key columns cannot be INCLUDEd)", c.index, def)
		}
	}
}

// TestConsumePlanRetryStormStillUsesIndex is the direct acceptance test for the
// #11 fix: a channel must deliver a ready message and reclaim a timed-out one
// cheaply even when each sits behind a large storm of not-yet-actionable rows --
// the shape a crash-looping consumer with a configured BackoffPolicy produces. A
// retried message keeps its original (low) id but has its available_at pushed
// into the future, and an in-flight message under a live claim has a future
// visibility_timeout; with the eligibility timestamp as the leading index key,
// both storms sort past the probe's range boundary and are never scanned.
//
// Beyond "the index is still selected and never a seq scan", this asserts the
// probes drop essentially no rows to a Filter (assertNoLargeFilterDrop): the old
// (id)-keyed shape walked and heap-fetched every sleeping/in-flight row ahead of
// the target, which is exactly the O(storm) cost #11 removes. If a future change
// regresses the index key back to (id), the filter-drop assertions fail here.
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

	// The core #11 guarantee: neither probe walks its 20000 / 5000 ineligible
	// rows. The leading range key stops the scan at the eligibility boundary, so
	// almost nothing is read only to be discarded by a Filter.
	assertNoLargeFilterDrop(t, pendingPlan, 100)
	assertNoLargeFilterDrop(t, reclaimPlan, 100)

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
