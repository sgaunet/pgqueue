package integration_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sgaunet/pgqueue"
)

func TestReplayFrom(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel
	err := pq.CreateChannel(ctx, "replay-test")
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Record start time
	// A generous backward margin: ReplayFrom matches created_at (DB clock)
	// against this value, so it must not land ahead of the container clock.
	startTime := time.Now().Add(-time.Hour)

	// Publish and process some messages
	for i := 0; i < 5; i++ {
		if _, err := pq.Publish(ctx, "replay-test", []byte("test message")); err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}
	}

	// Consume and ack all messages
	for i := 0; i < 5; i++ {
		msg, err := pq.ReceiveChannel(ctx, "replay-test", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("failed to consume message: %v", err)
		}
		if err := pq.Ack(ctx, msg.Receipt()); err != nil {
			t.Fatalf("failed to ack message: %v", err)
		}
	}

	// Verify all completed
	stats, err := pq.Stats(ctx, "replay-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if stats.CompletedCount != 5 {
		t.Errorf("expected 5 completed messages, got %d", stats.CompletedCount)
	}

	// Dry-run replay
	count, err := pq.ReplayFrom(ctx, "replay-test", pgqueue.QueueTypeChannel, startTime, pgqueue.ReplayOptions{
		DryRun: true,
	})
	if err != nil {
		t.Fatalf("dry-run replay failed: %v", err)
	}
	if count != 5 {
		t.Errorf("expected dry-run to report 5 messages, got %d", count)
	}

	// Replay for real.
	count, err = pq.ReplayFrom(ctx, "replay-test", pgqueue.QueueTypeChannel, startTime, pgqueue.ReplayOptions{
		PerformedBy: "test-user",
	})
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if count != 5 {
		t.Errorf("expected to replay 5 messages, got %d", count)
	}

	// Verify messages are pending again
	stats, err = pq.Stats(ctx, "replay-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if stats.PendingCount != 5 {
		t.Errorf("expected 5 pending messages after replay, got %d", stats.PendingCount)
	}
	if stats.CompletedCount != 0 {
		t.Errorf("expected 0 completed messages after replay, got %d", stats.CompletedCount)
	}

	// Verify replay history
	history, err := pq.ReplayHistory(ctx, "replay-test", pgqueue.QueueTypeChannel, 10)
	if err != nil {
		t.Fatalf("failed to get replay history: %v", err)
	}
	if len(history) != 1 {
		t.Errorf("expected 1 replay history entry, got %d", len(history))
	}
}

func TestReplayFromWithLimit(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "replay-limit")
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// A generous backward margin: ReplayFrom matches created_at (DB clock)
	// against this value, so it must not land ahead of the container clock.
	startTime := time.Now().Add(-time.Hour)

	// Publish and complete 5 messages
	for i := 0; i < 5; i++ {
		if _, err := pq.Publish(ctx, "replay-limit", []byte("msg")); err != nil {
			t.Fatalf("failed to publish: %v", err)
		}
	}
	for i := 0; i < 5; i++ {
		msg, err := pq.ReceiveChannel(ctx, "replay-limit", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("failed to consume: %v", err)
		}
		if err := pq.Ack(ctx, msg.Receipt()); err != nil {
			t.Fatalf("failed to ack: %v", err)
		}
	}

	// Replay with limit=2 — only 2 of the 5 completed messages should be replayed
	count, err := pq.ReplayFrom(ctx, "replay-limit", pgqueue.QueueTypeChannel, startTime, pgqueue.ReplayOptions{
		Limit:   2,
	})
	if err != nil {
		t.Fatalf("replay with limit failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 replayed messages, got %d", count)
	}

	stats, err := pq.Stats(ctx, "replay-limit", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if stats.PendingCount != 2 {
		t.Errorf("expected 2 pending messages, got %d", stats.PendingCount)
	}
	if stats.CompletedCount != 3 {
		t.Errorf("expected 3 completed messages, got %d", stats.CompletedCount)
	}
}

func TestReplayMessage(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel
	err := pq.CreateChannel(ctx, "replay-msg-test")
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish a message
	if _, err := pq.Publish(ctx, "replay-msg-test", []byte("test message")); err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	// Consume and ack
	msg, err := pq.ReceiveChannel(ctx, "replay-msg-test", pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("failed to consume message: %v", err)
	}
	if err := pq.Ack(ctx, msg.Receipt()); err != nil {
		t.Fatalf("failed to ack message: %v", err)
	}

	// Verify completed
	stats, err := pq.Stats(ctx, "replay-msg-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if stats.CompletedCount != 1 {
		t.Errorf("expected 1 completed message, got %d", stats.CompletedCount)
	}

	// Replay the specific message
	err = pq.ReplayMessage(ctx, "replay-msg-test", pgqueue.QueueTypeChannel, msg.ID, pgqueue.ReplayOptions{
		PerformedBy: "test-user",
	})
	if err != nil {
		t.Fatalf("replay message failed: %v", err)
	}

	// Verify message is pending again
	stats, err = pq.Stats(ctx, "replay-msg-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if stats.PendingCount != 1 {
		t.Errorf("expected 1 pending message after replay, got %d", stats.PendingCount)
	}
}

func TestReplayDLQ(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel with max retries
	err := pq.CreateChannel(ctx, "replay-dlq-test", pgqueue.WithQueueMaxRetries(1))
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish a message
	if _, err := pq.Publish(ctx, "replay-dlq-test", []byte("test message")); err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	// Consume and nack twice to send to DLQ
	for i := 0; i < 2; i++ {
		msg, err := pq.ReceiveChannel(ctx, "replay-dlq-test", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("failed to consume message on attempt %d: %v", i+1, err)
		}
		if msg == nil {
			t.Fatalf("consume returned nil message on attempt %d", i+1)
		}
		if err := pq.Nack(ctx, msg.Receipt(), "test failure"); err != nil {
			t.Fatalf("failed to nack message on attempt %d: %v", i+1, err)
		}
	}

	// Verify message in DLQ
	dlqStats, err := pq.DLQStats(ctx, "replay-dlq-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get DLQ stats: %v", err)
	}
	if dlqStats.TotalCount != 1 {
		t.Errorf("expected 1 message in DLQ, got %d", dlqStats.TotalCount)
	}

	// Dry-run DLQ replay
	res, err := pq.ReplayDLQ(ctx, "replay-dlq-test", pgqueue.QueueTypeChannel, pgqueue.ReplayOptions{
		DryRun: true,
	})
	if err != nil {
		t.Fatalf("dry-run DLQ replay failed: %v", err)
	}
	if res.Replayed != 1 {
		t.Errorf("expected dry-run to report 1 message, got %d", res.Replayed)
	}

	// Replay from DLQ
	res, err = pq.ReplayDLQ(ctx, "replay-dlq-test", pgqueue.QueueTypeChannel, pgqueue.ReplayOptions{
		PerformedBy: "test-user",
	})
	if err != nil {
		t.Fatalf("DLQ replay failed: %v", err)
	}
	if res.Replayed != 1 {
		t.Errorf("expected to replay 1 message from DLQ, got %d", res.Replayed)
	}

	// Verify message is back in main queue
	depth, err := pq.QueueDepth(ctx, "replay-dlq-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get queue depth: %v", err)
	}
	if depth != 1 {
		t.Errorf("expected 1 pending message after DLQ replay, got %d", depth)
	}

	// Verify DLQ is empty
	dlqStats, err = pq.DLQStats(ctx, "replay-dlq-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get DLQ stats: %v", err)
	}
	if dlqStats.TotalCount != 0 {
		t.Errorf("expected 0 messages in DLQ after replay, got %d", dlqStats.TotalCount)
	}
}

func TestReplayDLQPubSub(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create topic with max 1 retry so messages go to DLQ after 2 nacks
	err := pq.CreateTopic(ctx, "replay-dlq-pubsub", pgqueue.WithQueueMaxRetries(1))
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	err = pq.Subscribe(ctx, "replay-dlq-pubsub", "sub1")
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	if _, err := pq.Publish(ctx, "replay-dlq-pubsub", []byte("replay-me")); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// Nack until message goes to DLQ
	for i := 0; i < 2; i++ {
		msg, err := pq.ReceiveTopic(ctx, "replay-dlq-pubsub", "sub1", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("consume %d failed: %v", i, err)
		}
		if msg == nil {
			t.Fatalf("consume %d returned nil", i)
		}
		err = pq.Nack(ctx, msg.Receipt(), "fail")
		if err != nil {
			t.Fatalf("nack %d failed: %v", i, err)
		}
	}

	// Verify message in DLQ
	dlqStats, err := pq.DLQStats(ctx, "replay-dlq-pubsub", pgqueue.QueueTypePubSub)
	if err != nil {
		t.Fatalf("failed to get DLQ stats: %v", err)
	}
	if dlqStats.TotalCount != 1 {
		t.Errorf("expected 1 DLQ message, got %d", dlqStats.TotalCount)
	}

	// Replay from DLQ
	res, err := pq.ReplayDLQ(ctx, "replay-dlq-pubsub", pgqueue.QueueTypePubSub, pgqueue.ReplayOptions{
	})
	if err != nil {
		t.Fatalf("replay DLQ failed: %v", err)
	}
	if res.Replayed != 1 {
		t.Errorf("expected 1 replayed, got %d", res.Replayed)
	}

	// The replayed message must be consumable by the subscriber
	msg, err := pq.ReceiveTopic(ctx, "replay-dlq-pubsub", "sub1", pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("consume after replay failed: %v", err)
	}
	if msg == nil {
		t.Fatal("expected message after DLQ replay, got nil")
	}
	if string(msg.Payload) != "replay-me" {
		t.Errorf("expected payload 'replay-me', got '%s'", msg.Payload)
	}
}

// TestT021_ConcurrentReplayDLQNoLossNoDuplication verifies that when two goroutines
// call ReplayDLQ concurrently, every DLQ message is reinstated exactly once (none
// lost, none duplicated). FOR UPDATE SKIP LOCKED is the mechanism under test.
func TestT021_ConcurrentReplayDLQNoLossNoDuplication(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	const queueName = "t021-concurrent-dlq"
	const numMessages = 10

	if err := pq.CreateChannel(ctx, queueName, pgqueue.WithQueueMaxRetries(1)); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish and move all messages to DLQ (2 nacks each: retry then DLQ)
	for i := range numMessages {
		payload := []byte{'m', 's', 'g', byte('0' + i)}
		if _, err := pq.Publish(ctx, queueName, payload); err != nil {
			t.Fatalf("publish %d failed: %v", i, err)
		}
	}
	for range numMessages {
		msg, err := pq.ReceiveChannel(ctx, queueName, pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil || msg == nil {
			t.Fatalf("consume for first nack failed: %v", err)
		}
		if err := pq.Nack(ctx, msg.Receipt(), "fail1"); err != nil {
			t.Fatalf("first nack failed: %v", err)
		}
	}
	for range numMessages {
		msg, err := pq.ReceiveChannel(ctx, queueName, pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil || msg == nil {
			t.Fatalf("consume for second nack failed: %v", err)
		}
		if err := pq.Nack(ctx, msg.Receipt(), "fail2"); err != nil {
			t.Fatalf("second nack (DLQ) failed: %v", err)
		}
	}

	// Verify all in DLQ
	dlqStats, err := pq.DLQStats(ctx, queueName, pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get DLQ stats: %v", err)
	}
	if dlqStats.TotalCount != numMessages {
		t.Fatalf("expected %d DLQ messages before replay, got %d", numMessages, dlqStats.TotalCount)
	}

	// Two concurrent ReplayDLQ calls
	var wg sync.WaitGroup
	wg.Add(2)
	counts := make([]int64, 2)
	errs := make([]error, 2)
	for i := range 2 {
		go func(idx int) {
			defer wg.Done()
			res, err := pq.ReplayDLQ(ctx, queueName, pgqueue.QueueTypeChannel, pgqueue.ReplayOptions{
			})
			counts[idx], errs[idx] = res.Replayed, err
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Errorf("ReplayDLQ[%d] error: %v", i, e)
		}
	}

	totalReplayed := counts[0] + counts[1]
	if totalReplayed != numMessages {
		t.Errorf("total replayed=%d, want %d (goroutine 0=%d, goroutine 1=%d)",
			totalReplayed, numMessages, counts[0], counts[1])
	}

	// DLQ must be empty now
	dlqStats, err = pq.DLQStats(ctx, queueName, pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get DLQ stats after replay: %v", err)
	}
	if dlqStats.TotalCount != 0 {
		t.Errorf("DLQ not empty after concurrent replay: %d remaining", dlqStats.TotalCount)
	}

	// Pending count must be exactly numMessages (no duplicates in main queue)
	var pendingCount int
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM pgqueue_msg_t021_concurrent_dlq WHERE status = 'pending'",
	).Scan(&pendingCount)
	if err != nil {
		t.Fatalf("failed to count pending messages: %v", err)
	}
	if pendingCount != numMessages {
		t.Errorf("pending count=%d after replay, want %d (duplication detected)", pendingCount, numMessages)
	}
}

// TestT023_NegativeReplayLimitRejected verifies that ReplayFrom/ReplayDLQ reject
// a negative Limit with ErrInvalidConfig, and that the replay audit log entry is
// written atomically in the same transaction as the replay data change.
func TestT023_NegativeReplayLimitRejected(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	const queueName = "t023-neg-limit"
	if err := pq.CreateChannel(ctx, queueName, pgqueue.WithQueueMaxRetries(3)); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}
	if _, err := pq.Publish(ctx, queueName, []byte("payload")); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}
	msg, err := pq.ReceiveChannel(ctx, queueName, pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil || msg == nil {
		t.Fatalf("consume failed: %v", err)
	}
	if err := pq.Ack(ctx, msg.Receipt()); err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	// Negative Limit must be rejected with ErrInvalidConfig
	since := time.Now().Add(-time.Hour)
	_, err = pq.ReplayFrom(ctx, queueName, pgqueue.QueueTypeChannel, since, pgqueue.ReplayOptions{
		Limit:   -1,
	})
	if !errors.Is(err, pgqueue.ErrInvalidConfig) {
		t.Errorf("ReplayFrom with Limit=-1 should return ErrInvalidConfig, got: %v", err)
	}

	_, err = pq.ReplayDLQ(ctx, queueName, pgqueue.QueueTypeChannel, pgqueue.ReplayOptions{
		Limit:   -5,
	})
	if !errors.Is(err, pgqueue.ErrInvalidConfig) {
		t.Errorf("ReplayDLQ with Limit=-5 should return ErrInvalidConfig, got: %v", err)
	}

	// Verify the replay log entry for a SUCCESSFUL replay is written atomically:
	// run a successful ReplayFrom and check the log was written.
	count, err := pq.ReplayFrom(ctx, queueName, pgqueue.QueueTypeChannel, since, pgqueue.ReplayOptions{
		PerformedBy: "test-user",
	})
	if err != nil {
		t.Fatalf("successful ReplayFrom failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 replayed message, got %d", count)
	}

	// The replay log must have exactly one entry
	var logCount int
	err = db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pgqueue_replay_log WHERE queue_name = $1`,
		queueName,
	).Scan(&logCount)
	if err != nil {
		t.Fatalf("failed to query replay log: %v", err)
	}
	if logCount != 1 {
		t.Errorf("expected 1 replay log entry, got %d", logCount)
	}
}

// TestReplayFromPubSubFiltersOnMessagePublishTime (R-03) verifies that
// ReplayFrom for a pub/sub topic selects messages by the message-table
// created_at, NOT by when a subscriber subscribed or consumed. A subscriber
// that joins long after publication must still see exactly the messages whose
// publish time is at or after the replay cutoff.
func TestReplayFromPubSubFiltersOnMessagePublishTime(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	const topic = "replay-pubsub-time"
	if err := pq.CreateTopic(ctx, topic); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	if err := pq.Subscribe(ctx, topic, "sub1"); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	// Publish 6 messages, then consume+ack all of them so they are no longer
	// pending for the subscriber.
	const total = 6
	for i := range total {
		if _, err := pq.Publish(ctx, topic, []byte{byte('A' + i)}); err != nil {
			t.Fatalf("publish %d failed: %v", i, err)
		}
	}
	for range total {
		msg, err := pq.ReceiveTopic(ctx, topic, "sub1", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil || msg == nil {
			t.Fatalf("consume failed: %v", err)
		}
		if err := pq.Ack(ctx, msg.Receipt()); err != nil {
			t.Fatalf("ack failed: %v", err)
		}
	}

	// Backdate the first 4 messages to an hour ago; the last 2 keep "now".
	// The replay cutoff sits between them, so exactly 2 must be reinstated.
	if _, err := db.ExecContext(ctx,
		`UPDATE pgqueue_msg_replay_pubsub_time
		    SET created_at = NOW() - INTERVAL '1 hour'
		  WHERE id IN (SELECT id FROM pgqueue_msg_replay_pubsub_time
		               ORDER BY id LIMIT 4)`); err != nil {
		t.Fatalf("failed to backdate messages: %v", err)
	}

	cutoff := time.Now().Add(-1 * time.Minute)

	// Dry-run: the count must reflect message-table publish time only.
	count, err := pq.ReplayFrom(ctx, topic, pgqueue.QueueTypePubSub, cutoff, pgqueue.ReplayOptions{
		DryRun: true,
	})
	if err != nil {
		t.Fatalf("dry-run replay failed: %v", err)
	}
	if count != 2 {
		t.Errorf("dry-run: expected 2 messages at/after cutoff, got %d", count)
	}

	count, err = pq.ReplayFrom(ctx, topic, pgqueue.QueueTypePubSub, cutoff, pgqueue.ReplayOptions{
		PerformedBy: "test-user",
	})
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 replayed messages, got %d", count)
	}

	// The subscriber must now have exactly 2 pending messages again.
	lag, err := pq.SubscriberLag(ctx, topic, "sub1")
	if err != nil {
		t.Fatalf("failed to get subscriber lag: %v", err)
	}
	if lag.PendingCount != 2 {
		t.Errorf("expected 2 pending messages after replay, got %d", lag.PendingCount)
	}
}

// TestReplayFromLargeBacklogNoLossNoDuplication (R-02) seeds a backlog well
// above the internal per-page bound (100 rows) and replays it with no explicit
// Limit. The paged, one-transaction-per-page loop must reinstate every message
// exactly once: the final pending count equals the backlog, with none skipped
// or replayed twice.
func TestReplayFromLargeBacklogNoLossNoDuplication(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	const channelName = "replay-large-backlog"
	if err := pq.CreateChannel(ctx, channelName); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// A generous backward margin: ReplayFrom matches created_at (DB clock)
	// against this value, so it must not land ahead of the container clock.
	startTime := time.Now().Add(-time.Hour)

	// Seed >=10,000 messages directly into the message table in the completed
	// state so ReplayFrom has a large backlog to reinstate. Going through the
	// public publish/consume/ack path 10k times would make the test extremely
	// slow, so we INSERT the rows directly with uuidv7() ids.
	const backlog = 10_000
	if _, err := db.ExecContext(ctx, `
		INSERT INTO pgqueue_msg_replay_large_backlog
			(id, payload, status, retry_count, max_retries, created_at, processed_at)
		SELECT uuidv7(), '\x00'::bytea, 'completed', 0, 3, NOW(), NOW()
		FROM generate_series(1, $1)`, backlog); err != nil {
		t.Fatalf("failed to seed backlog: %v", err)
	}

	// Confirm the seed worked.
	var completed int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM pgqueue_msg_replay_large_backlog WHERE status = 'completed'",
	).Scan(&completed); err != nil {
		t.Fatalf("failed to count seeded messages: %v", err)
	}
	if completed != backlog {
		t.Fatalf("expected %d seeded completed messages, got %d", backlog, completed)
	}

	// Replay with NO explicit Limit: the keyset-paged loop must reinstate them all.
	count, err := pq.ReplayFrom(ctx, channelName, pgqueue.QueueTypeChannel, startTime, pgqueue.ReplayOptions{
		PerformedBy: "test-user",
	})
	if err != nil {
		t.Fatalf("replay of large backlog failed: %v", err)
	}
	if count != backlog {
		t.Errorf("expected %d replayed messages, got %d", backlog, count)
	}

	// Every message must now be pending exactly once: no loss, no duplication.
	var pending, stillCompleted int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM pgqueue_msg_replay_large_backlog WHERE status = 'pending'",
	).Scan(&pending); err != nil {
		t.Fatalf("failed to count pending messages: %v", err)
	}
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM pgqueue_msg_replay_large_backlog WHERE status = 'completed'",
	).Scan(&stillCompleted); err != nil {
		t.Fatalf("failed to count remaining completed messages: %v", err)
	}
	if pending != backlog {
		t.Errorf("expected %d pending messages after replay, got %d", backlog, pending)
	}
	if stillCompleted != 0 {
		t.Errorf("expected 0 completed messages after replay, got %d (some not replayed)", stillCompleted)
	}

	// The total row count is unchanged: replay never duplicates rows.
	var totalRows int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM pgqueue_msg_replay_large_backlog",
	).Scan(&totalRows); err != nil {
		t.Fatalf("failed to count total rows: %v", err)
	}
	if totalRows != backlog {
		t.Errorf("expected %d total rows (no duplication), got %d", backlog, totalRows)
	}
}

// TestReplayDLQAllUnreplayableReturnsPromptly (R-04) builds a DLQ in which every
// entry is un-replayable (the original channel message id is still live in the
// message table, so reinstating it would violate the primary key). ReplayDLQ
// with a Limit must return promptly reporting Replayed == 0 and Skipped > 0,
// rather than scanning forever or failing outright.
func TestReplayDLQAllUnreplayableReturnsPromptly(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	const channelName = "replay-dlq-unreplayable"
	if err := pq.CreateChannel(ctx, channelName, pgqueue.WithQueueMaxRetries(1)); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish and push 5 messages to the DLQ via repeated nacks.
	const numMessages = 5
	for range numMessages {
		if _, err := pq.Publish(ctx, channelName, []byte("payload")); err != nil {
			t.Fatalf("publish failed: %v", err)
		}
	}
	for range numMessages {
		msg, err := pq.ReceiveChannel(ctx, channelName, pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil || msg == nil {
			t.Fatalf("consume for first nack failed: %v", err)
		}
		if err := pq.Nack(ctx, msg.Receipt(), "fail1"); err != nil {
			t.Fatalf("first nack failed: %v", err)
		}
	}
	for range numMessages {
		msg, err := pq.ReceiveChannel(ctx, channelName, pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil || msg == nil {
			t.Fatalf("consume for second nack failed: %v", err)
		}
		if err := pq.Nack(ctx, msg.Receipt(), "fail2"); err != nil {
			t.Fatalf("second nack (DLQ) failed: %v", err)
		}
	}

	dlqStats, err := pq.DLQStats(ctx, channelName, pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get DLQ stats: %v", err)
	}
	if dlqStats.TotalCount != numMessages {
		t.Fatalf("expected %d DLQ messages, got %d", numMessages, dlqStats.TotalCount)
	}

	// Re-insert each DLQ message id back into the live message table so that a
	// replay of any DLQ row would collide with an existing live row: every DLQ
	// entry is now un-replayable.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO pgqueue_msg_replay_dlq_unreplayable
			(id, payload, status, retry_count, max_retries, created_at)
		SELECT original_message_id, '\x00'::bytea, 'pending', 0, 1, NOW()
		FROM pgqueue_dlq_replay_dlq_unreplayable`); err != nil {
		t.Fatalf("failed to re-insert live message ids: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	res, err := pq.ReplayDLQ(ctx, channelName, pgqueue.QueueTypeChannel, pgqueue.ReplayOptions{
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("ReplayDLQ of un-replayable backlog failed: %v", err)
	}
	if time.Now().After(deadline) {
		t.Error("ReplayDLQ did not return promptly for an un-replayable DLQ")
	}
	if res.Replayed != 0 {
		t.Errorf("expected 0 replayed, got %d", res.Replayed)
	}
	if res.Skipped == 0 {
		t.Errorf("expected Skipped > 0 for an all-un-replayable DLQ, got %d", res.Skipped)
	}
}

// TestReplayDLQDryRunCountsOnlyReplayable verifies the dry-run accuracy fix: for
// a DLQ mixing replayable and un-replayable rows, DryRun reports exactly what a
// real run would replay/skip (not a raw COUNT of all rows), and mutates nothing
// — the subsequent real run still finds and replays the same rows.
func TestReplayDLQDryRunCountsOnlyReplayable(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	const channelName = "replay-dlq-dryrun-mixed"
	if err := pq.CreateChannel(ctx, channelName, pgqueue.WithQueueMaxRetries(1)); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// Push 4 messages to the DLQ via two nacks each.
	const total = 4
	for range total {
		if _, err := pq.Publish(ctx, channelName, []byte("payload")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	for pass := range 2 {
		for range total {
			msg, err := pq.ReceiveChannel(ctx, channelName, pgqueue.WithVisibilityTimeout(30*time.Second))
			if err != nil || msg == nil {
				t.Fatalf("consume (pass %d): %v", pass, err)
			}
			if err := pq.Nack(ctx, msg.Receipt(), "fail"); err != nil {
				t.Fatalf("nack (pass %d): %v", pass, err)
			}
		}
	}

	// Make exactly 2 of the 4 DLQ rows un-replayable by re-inserting their
	// original_message_id into the live message table, so a replay would collide
	// with an existing live row and skip them.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO pgqueue_msg_replay_dlq_dryrun_mixed
			(id, payload, status, retry_count, max_retries, created_at)
		SELECT original_message_id, '\x00'::bytea, 'pending', 0, 1, NOW()
		FROM pgqueue_dlq_replay_dlq_dryrun_mixed
		ORDER BY id LIMIT 2`); err != nil {
		t.Fatalf("seed un-replayable rows: %v", err)
	}

	// Dry run: must report the 2 replayable rows, skip the 2 un-replayable ones,
	// and NOT report all 4 (the old raw-COUNT behavior).
	dry, err := pq.ReplayDLQ(ctx, channelName, pgqueue.QueueTypeChannel, pgqueue.ReplayOptions{DryRun: true})
	if err != nil {
		t.Fatalf("dry-run replay: %v", err)
	}
	if dry.Replayed != 2 || dry.Skipped != 2 {
		t.Errorf("dry-run = {Replayed:%d Skipped:%d}, want {2 2}", dry.Replayed, dry.Skipped)
	}

	// The dry run must not have mutated the DLQ — all 4 rows remain.
	dlqStats, err := pq.DLQStats(ctx, channelName, pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("DLQ stats after dry-run: %v", err)
	}
	if dlqStats.TotalCount != total {
		t.Errorf("DLQ count after dry-run = %d, want %d (dry-run must not delete)", dlqStats.TotalCount, total)
	}

	// Real run: replays the same 2 the dry run predicted.
	actual, err := pq.ReplayDLQ(ctx, channelName, pgqueue.QueueTypeChannel, pgqueue.ReplayOptions{PerformedBy: "test"})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if actual.Replayed != dry.Replayed || actual.Skipped != dry.Skipped {
		t.Errorf("real = {Replayed:%d Skipped:%d}, want it to match dry-run {Replayed:%d Skipped:%d}",
			actual.Replayed, actual.Skipped, dry.Replayed, dry.Skipped)
	}
}

// TestReplayDLQPerPageAuditLog (R-11) seeds a channel DLQ with a backlog that
// spans more than one replay page (the internal page size is 100), runs
// ReplayDLQ, and verifies that the audit row is written per-page inside each
// page's transaction rather than once at the end. There must be more than one
// pgqueue_replay_log row, and the sum of their message counts must equal the
// total ReplayDLQResult.Replayed.
func TestReplayDLQPerPageAuditLog(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	const channelName = "replay-dlq-perpage"
	if err := pq.CreateChannel(ctx, channelName, pgqueue.WithQueueMaxRetries(1)); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Seed >100 messages directly into the DLQ table so the replay backlog
	// spans more than one page. Going through publish/consume/nack 250 times
	// would make the test extremely slow, so we INSERT the rows directly.
	// Each DLQ row carries a fresh original_message_id that does not collide
	// with any live message, so every row is replayable.
	const backlog = 250
	if _, err := db.ExecContext(ctx, `
		INSERT INTO pgqueue_dlq_replay_dlq_perpage
			(id, original_message_id, payload, failure_reason, retry_count, moved_at)
		SELECT uuidv7(), uuidv7(), '\x00'::bytea, 'seeded failure', 1, NOW()
		FROM generate_series(1, $1)`, backlog); err != nil {
		t.Fatalf("failed to seed DLQ backlog: %v", err)
	}

	dlqStats, err := pq.DLQStats(ctx, channelName, pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get DLQ stats: %v", err)
	}
	if dlqStats.TotalCount != backlog {
		t.Fatalf("expected %d seeded DLQ messages, got %d", backlog, dlqStats.TotalCount)
	}

	// Replay the whole DLQ with no explicit Limit: the keyset-paged loop must
	// reinstate every message.
	res, err := pq.ReplayDLQ(ctx, channelName, pgqueue.QueueTypeChannel, pgqueue.ReplayOptions{
		PerformedBy: "test-user",
	})
	if err != nil {
		t.Fatalf("DLQ replay failed: %v", err)
	}
	if res.Replayed != backlog {
		t.Errorf("expected %d replayed messages, got %d", backlog, res.Replayed)
	}

	// The DLQ must be empty after the replay.
	dlqStats, err = pq.DLQStats(ctx, channelName, pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get DLQ stats after replay: %v", err)
	}
	if dlqStats.TotalCount != 0 {
		t.Errorf("expected 0 DLQ messages after replay, got %d", dlqStats.TotalCount)
	}

	// The audit log must hold one row per replayed page — more than one row,
	// since the backlog (250) spans 3 pages of 100. The page size is an
	// internal constant, so we assert ">1" rather than an exact count.
	history, err := pq.ReplayHistory(ctx, channelName, pgqueue.QueueTypeChannel, 100)
	if err != nil {
		t.Fatalf("failed to get replay history: %v", err)
	}
	if len(history) <= 1 {
		t.Errorf("expected more than 1 audit-log row (per-page logging), got %d", len(history))
	}

	// The sum of the per-page audit rows' message counts must equal the total
	// number of messages replayed.
	var totalLogged int64
	for _, entry := range history {
		if entry.MessageCount <= 0 {
			t.Errorf("audit-log row has non-positive message count %d", entry.MessageCount)
		}
		totalLogged += entry.MessageCount
	}
	if totalLogged != res.Replayed {
		t.Errorf("sum of audit-log message counts = %d, want %d (= ReplayDLQResult.Replayed)",
			totalLogged, res.Replayed)
	}

	// Cross-check directly against the replay log table: same row count and
	// same message-count sum.
	var rowCount int
	var dbSum int64
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(SUM(message_count), 0)
		   FROM pgqueue_replay_log
		  WHERE queue_name = $1 AND replay_type = 'dlq'`,
		channelName,
	).Scan(&rowCount, &dbSum); err != nil {
		t.Fatalf("failed to query replay log: %v", err)
	}
	if rowCount != len(history) {
		t.Errorf("replay log row count = %d, ReplayHistory returned %d", rowCount, len(history))
	}
	if dbSum != res.Replayed {
		t.Errorf("replay log message_count sum = %d, want %d", dbSum, res.Replayed)
	}
}

// TestReplayDLQLegacyNullSubscriberCount verifies that ReplayDLQ reports an
// accurate count for a legacy DLQ row whose subscriber_id is NULL. Such a row
// fans out to every active subscriber, but it is still one replayed DLQ entry:
// ReplayDLQ must count it once (Replayed=1, Skipped=0), not once per
// subscription record it produces — which previously yielded a negative Skipped.
func TestReplayDLQLegacyNullSubscriberCount(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	const topicName = "legacy-dlq-topic"
	if err := pq.CreateTopic(ctx, topicName); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for _, sub := range []string{"sub-a", "sub-b"} {
		if err := pq.Subscribe(ctx, topicName, sub); err != nil {
			t.Fatalf("subscribe %s: %v", sub, err)
		}
	}

	// Publish a message so the message table has a row for the DLQ entry to
	// reference (replay re-creates subscription rows that foreign-key it).
	msgID, err := pq.Publish(ctx, topicName, []byte("legacy"))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Insert a legacy DLQ row directly with subscriber_id left NULL, as rows
	// written before the subscriber_id column existed would be.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO pgqueue_dlq_legacy_dlq_topic
		   (original_message_id, payload, failure_reason, retry_count)
		 VALUES ($1, $2, $3, $4)`,
		msgID, []byte("legacy"), "legacy failure", 0,
	); err != nil {
		t.Fatalf("insert legacy DLQ row: %v", err)
	}

	res, err := pq.ReplayDLQ(ctx, topicName, pgqueue.QueueTypePubSub,
		pgqueue.ReplayOptions{})
	if err != nil {
		t.Fatalf("replay DLQ: %v", err)
	}
	if res.Replayed != 1 {
		t.Errorf("expected Replayed=1 for one legacy DLQ row, got %d", res.Replayed)
	}
	if res.Skipped != 0 {
		t.Errorf("expected Skipped=0, got %d (negative means the replayed count was inflated)", res.Skipped)
	}
}
