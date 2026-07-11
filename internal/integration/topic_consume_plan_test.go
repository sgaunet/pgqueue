package integration_test

import (
	"context"
	"fmt"
	"testing"
)

// topicPendingProbeSQL is the "probe A" statement the two-probe topic consume
// path issues for a subscriber's next immediately-consumable pending
// subscription. It must be served by the subscriber's pending partial index
// without scanning that subscriber's delivered (acked) history.
func topicPendingProbeSQL(subTable, msgTable string) string {
	return fmt.Sprintf(`
		SELECT s.id, s.message_id, m.payload, m.created_at,
		       s.status, s.retry_count, m.metadata, s.error_message
		FROM %s s
		JOIN %s m ON s.message_id = m.id
		WHERE s.subscriber_id = $1
		  AND s.status = 'pending' AND s.available_at <= NOW()
		ORDER BY m.id
		LIMIT 1
		FOR UPDATE OF s SKIP LOCKED`, subTable, msgTable)
}

// TestTopicConsumePlan is the pub/sub half of the C1 regression guard: a
// subscriber consuming from a topic with a deep acked history must have its
// pending subscription found via the subscriber's pending partial index, never a
// sequential scan of the subscription table.
func TestTopicConsumePlan(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	const (
		topic      = "topicplan"
		subscriber = "sub-a"
	)
	if err := pq.CreateTopic(ctx, topic); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if err := pq.Subscribe(ctx, topic, subscriber); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// A small pending set behind a deep delivered (acked) history for the same
	// subscriber: the plan must touch only the pending rows.
	seedTopicMessages(t, db, topic, subscriber, topicSeed{
		Pending: 200,
		Acked:   50000,
	})

	table := queueTableName(t, db, topic)
	subTable := "pgqueue_sub_" + table
	msgTable := "pgqueue_msg_" + table
	consumableNull := "idx_pgqueue_sub_" + table + "_consumable_null"

	plan := explainAnalyzePlan(t, db, topicPendingProbeSQL(subTable, msgTable), subscriber)
	assertPlanUsesIndex(t, plan, consumableNull)
	assertNoSeqScan(t, plan, subTable)
	assertNoLargeFilterDrop(t, plan, 100)
}
