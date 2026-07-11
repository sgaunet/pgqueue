package integration_test

import (
	"context"
	"testing"
)

// TestConsumeHistoryIndependence is the SC-001 guard: consume cost must be a
// function of the consumable (pending) set, not of history depth. Two channels
// with the same pending set but a 100x difference in completed history must
// produce the same consume plan shape and touch a bounded, history-independent
// number of buffers.
func TestConsumeHistoryIndependence(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	const (
		shallow = "scaleshallow"
		deep    = "scaledeep"
	)
	for _, ch := range []string{shallow, deep} {
		if err := pq.CreateChannel(ctx, ch); err != nil {
			t.Fatalf("CreateChannel(%s): %v", ch, err)
		}
	}

	// Identical pending set; 100x difference in completed history depth.
	seedChannelMessages(t, db, shallow, channelSeed{Pending: 1000, CompletedOld: 5000})
	seedChannelMessages(t, db, deep, channelSeed{Pending: 1000, CompletedOld: 500000})

	shallowTable := queueTableName(t, db, shallow)
	deepTable := queueTableName(t, db, deep)

	shallowPlan := explainAnalyzePlan(t, db, channelPendingProbeSQL("pgqueue_msg_"+shallowTable))
	deepPlan := explainAnalyzePlan(t, db, channelPendingProbeSQL("pgqueue_msg_"+deepTable))

	assertPlanUsesIndex(t, shallowPlan, "idx_pgqueue_msg_"+shallowTable+"_consumable_null")
	assertPlanUsesIndex(t, deepPlan, "idx_pgqueue_msg_"+deepTable+"_consumable_null")

	// Same structural plan shape regardless of history depth (index names differ
	// per table, so compare only node types).
	if got, want := deepPlan.nodeTypes(), shallowPlan.nodeTypes(); !sameNodeTypes(got, want) {
		t.Fatalf("consume plan shape scaled with history depth:\nshallow=%v\ndeep=%v", want, got)
	}

	// The pending probe reads a partial index over the pending set plus one heap
	// row; a 100x-larger completed history must not inflate the blocks touched.
	const blockBudget = 300
	if b := deepPlan.maxSharedBlocks(); b > blockBudget {
		t.Fatalf("deep-history consume touched %.0f shared blocks (> %d): cost scaled with history",
			b, blockBudget)
	}
	if deep, shallowB := deepPlan.maxSharedBlocks(), shallowPlan.maxSharedBlocks(); deep > shallowB*4+50 {
		t.Fatalf("deep-history consume touched %.0f blocks vs shallow %.0f: cost scaled with history",
			deep, shallowB)
	}
}

func sameNodeTypes(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
