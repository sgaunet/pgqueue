package integration_test

import (
	"context"
	"fmt"
	"testing"
)

// channelPendingProbeSQL is the "probe A" statement the two-probe channel
// consume path issues for the next immediately-consumable pending message. The
// plan-regression tests assert this statement is served by the pending partial
// index and never scans historical (completed/processing) rows.
func channelPendingProbeSQL(msgTable string) string {
	return fmt.Sprintf(`
		SELECT id, payload, created_at, status, retry_count, max_retries,
		       metadata, processed_at, error_message
		FROM %s
		WHERE status = 'pending' AND available_at <= NOW()
		ORDER BY id
		LIMIT 1
		FOR UPDATE SKIP LOCKED`, msgTable)
}

// channelReclaimProbeSQL is the "probe B" statement issued only when probe A
// finds nothing: it reclaims a timed-out 'processing' message.
func channelReclaimProbeSQL(msgTable string) string {
	return fmt.Sprintf(`
		SELECT id, payload, created_at, status, retry_count, max_retries,
		       metadata, processed_at, error_message
		FROM %s
		WHERE status = 'processing' AND visibility_timeout < NOW()
		ORDER BY id
		LIMIT 1
		FOR UPDATE SKIP LOCKED`, msgTable)
}

// TestConsumePlanChannelPending is the C1 regression guard: with a large
// completed/DLQ history behind a small pending set, the pending consume probe
// must be served by the pending partial index — never a sequential scan and
// never a primary-key scan that reads and discards historical rows.
func TestConsumePlanChannelPending(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	const channel = "consumeplan"
	if err := pq.CreateChannel(ctx, channel); err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}

	// A few thousand consumable pending rows sitting behind a deep completed
	// history: the plan must touch only the pending rows.
	seedChannelMessages(t, db, channel, channelSeed{
		Pending:         2000,
		CompletedOld:    50000,
		CompletedRecent: 20000,
		DLQ:             5000,
	})

	table := queueTableName(t, db, channel)
	msgTable := "pgqueue_msg_" + table
	consumableNull := "idx_pgqueue_msg_" + table + "_consumable_null"
	consumableTimeout := "idx_pgqueue_msg_" + table + "_consumable_timeout"

	plan := explainAnalyzePlan(t, db, channelPendingProbeSQL(msgTable))
	assertPlanUsesIndex(t, plan, consumableNull)
	assertNoSeqScan(t, plan, msgTable)
	// The pending set is 2000; a plan that filters out history rows would drop
	// far more than that. Allow generous slack for a few concurrently-locked rows.
	assertNoLargeFilterDrop(t, plan, 100)

	// The reclaim probe (B) must likewise be index-served, not a seq scan.
	reclaimPlan := explainAnalyzePlan(t, db, channelReclaimProbeSQL(msgTable))
	assertPlanUsesIndex(t, reclaimPlan, consumableTimeout)
	assertNoSeqScan(t, reclaimPlan, msgTable)
}

// TestConsumeChannelDeliversDespiteHistory is a behavioral companion to the plan
// assertions: consuming from a channel buried under history still returns the
// pending message (proving the two-probe restructure did not change delivery).
func TestConsumeChannelDeliversDespiteHistory(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	const channel = "consumehist"
	if err := pq.CreateChannel(ctx, channel); err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	if _, err := pq.Publish(ctx, channel, []byte("live")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	seedChannelMessages(t, db, channel, channelSeed{CompletedOld: 20000, DLQ: 2000})

	msg, err := pq.ReceiveChannel(ctx, channel)
	if err != nil {
		t.Fatalf("ReceiveChannel: %v", err)
	}
	if msg == nil {
		t.Fatal("expected the live pending message, got nil")
	}
	if string(msg.Payload) != "live" {
		t.Fatalf("payload = %q, want %q", msg.Payload, "live")
	}
}
