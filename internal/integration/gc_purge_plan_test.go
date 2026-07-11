package integration_test

import (
	"context"
	"fmt"
	"testing"
)

// TestGCCompletedPurgePlan is the C2 regression guard: the default GC
// completed-message purge (channel branch) selects the rows to delete by
// status='completed' AND processed_at < cutoff. With a deep completed history of
// which only a small fraction is past the retention cutoff, that selection must
// be served by the dedicated completed partial index — not a sequential scan of
// the whole message table.
func TestGCCompletedPurgePlan(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	const channel = "gcpurgeplan"
	if err := pq.CreateChannel(ctx, channel); err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}

	// Mostly-recent completed rows (inside the retention window) with a small
	// past-cutoff tail: this is the selectivity that makes an index worthwhile
	// and a seq scan wasteful.
	seedChannelMessages(t, db, channel, channelSeed{
		Pending:         1000,
		CompletedRecent: 50000,
		CompletedOld:    1000,
	})

	table := queueTableName(t, db, channel)
	msgTable := "pgqueue_msg_" + table
	completedIdx := "idx_pgqueue_msg_" + table + "_completed"

	// The inner selection of purgeCompletedMessages (channel branch). One day of
	// retention leaves only the CompletedOld tail eligible.
	const oneDaySeconds = 86400
	purgeSelect := fmt.Sprintf(`
		SELECT id FROM %s
		WHERE status = 'completed'
		  AND processed_at < NOW() - make_interval(secs => $1)
		ORDER BY id
		LIMIT 1000
		FOR UPDATE SKIP LOCKED`, msgTable)

	plan := explainPlan(t, db, purgeSelect, oneDaySeconds)
	assertPlanUsesIndex(t, plan, completedIdx)
	assertNoSeqScan(t, plan, msgTable)
}
