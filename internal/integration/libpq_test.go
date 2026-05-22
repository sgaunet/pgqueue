package integration_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/lib/pq" // registers the "postgres" driver
	"github.com/sgaunet/pgqueue"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// This file pins driver portability (issue #46). The batch ack/nack and replay
// paths pass uuid[]/float8[] parameters as PostgreSQL array literals so they
// marshal on every database/sql driver. Every test here opens the database with
// the lib/pq driver ("postgres"); a regression would surface as a marshaling
// error ("unsupported type []string") from the operation under test.

// setupLibPQQueue starts a PostgreSQL container and returns a Queue whose
// *sql.DB is opened with the lib/pq driver rather than pgx.
func setupLibPQQueue(t *testing.T) (*pgqueue.Queue, func()) {
	t.Helper()
	ctx := context.Background()

	container, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithDatabase(testDBName),
		postgres.WithUsername(testUser),
		postgres.WithPassword(testPass),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(testWaitLogOccurrence).
				WithStartupTimeout(testStartupTimeout)))
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("failed to open lib/pq connection: %v", err)
	}

	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("failed to initialize schema: %v", err)
	}

	pq, err := pgqueue.New(ctx, db,
		pgqueue.WithMaxMessageSize(testMaxMessageSize),
		pgqueue.WithDefaultMaxRetries(testDefaultMaxRetries),
		// Negligible backoff so a nacked message is re-consumable immediately.
		pgqueue.WithBackoffPolicy(pgqueue.BackoffPolicy{
			BaseDelay:  time.Nanosecond,
			MaxDelay:   time.Nanosecond,
			Multiplier: 1,
		}),
	)
	if err != nil {
		t.Fatalf("failed to init pgqueue: %v", err)
	}

	cleanup := func() {
		_ = pq.Close()
		_ = db.Close()
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}

	return pq, cleanup
}

const libPQBatchSize = 3

func libPQMessages(prefix string) []pgqueue.PublishMessage {
	msgs := make([]pgqueue.PublishMessage, libPQBatchSize)
	for i := range msgs {
		msgs[i] = pgqueue.PublishMessage{Payload: []byte(prefix)}
	}
	return msgs
}

// TestLibPQDriver exercises every batch and replay path that passes an array
// parameter, under the lib/pq driver, within a single container.
func TestLibPQDriver(t *testing.T) {
	pq, cleanup := setupLibPQQueue(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("AckChannelBatch", func(t *testing.T) {
		if err := pq.CreateChannel(ctx, "libpq-ack-chan"); err != nil {
			t.Fatalf("CreateChannel failed: %v", err)
		}
		if _, err := pq.PublishBatch(ctx, "libpq-ack-chan", libPQMessages("ack")); err != nil {
			t.Fatalf("PublishBatch failed: %v", err)
		}

		receipts := make([]pgqueue.Receipt, libPQBatchSize)
		for i := range receipts {
			msg, err := pq.ConsumeFromChannel(ctx, "libpq-ack-chan", 30*time.Second)
			if err != nil {
				t.Fatalf("ConsumeFromChannel failed: %v", err)
			}
			receipts[i] = msg.Receipt()
		}

		if err := pq.AckChannelBatch(ctx, "libpq-ack-chan", receipts); err != nil {
			t.Fatalf("AckChannelBatch failed under lib/pq: %v", err)
		}

		stats, err := pq.GetStats(ctx, "libpq-ack-chan", pgqueue.QueueTypeChannel)
		if err != nil {
			t.Fatalf("GetStats failed: %v", err)
		}
		if stats.CompletedCount != libPQBatchSize {
			t.Errorf("expected %d completed, got %d", libPQBatchSize, stats.CompletedCount)
		}
	})

	// NackChannelBatch exercises fetchBatchMessageStates (unnest uuid[],uuid[]),
	// batchRetryMessages (unnest uuid[],float8[]) and batchMoveToDLQ (ANY uuid[]).
	t.Run("NackChannelBatch", func(t *testing.T) {
		if err := pq.CreateChannel(ctx, "libpq-nack-chan", pgqueue.WithQueueMaxRetries(1)); err != nil {
			t.Fatalf("CreateChannel failed: %v", err)
		}
		if _, err := pq.PublishBatch(ctx, "libpq-nack-chan", libPQMessages("nack")); err != nil {
			t.Fatalf("PublishBatch failed: %v", err)
		}

		consume := func() []pgqueue.Receipt {
			receipts := make([]pgqueue.Receipt, libPQBatchSize)
			for i := range receipts {
				msg, err := pq.ConsumeFromChannel(ctx, "libpq-nack-chan", 30*time.Second)
				if err != nil {
					t.Fatalf("ConsumeFromChannel failed: %v", err)
				}
				receipts[i] = msg.Receipt()
			}
			return receipts
		}

		// First nack: retry path (batchRetryMessages).
		if err := pq.NackChannelBatch(ctx, "libpq-nack-chan", consume(), "retry"); err != nil {
			t.Fatalf("NackChannelBatch retry failed under lib/pq: %v", err)
		}
		stats, err := pq.GetStats(ctx, "libpq-nack-chan", pgqueue.QueueTypeChannel)
		if err != nil {
			t.Fatalf("GetStats failed: %v", err)
		}
		if stats.PendingCount != libPQBatchSize {
			t.Errorf("expected %d pending after retry nack, got %d", libPQBatchSize, stats.PendingCount)
		}

		// Second nack: retry count exceeds max, DLQ path (batchMoveToDLQ).
		if err := pq.NackChannelBatch(ctx, "libpq-nack-chan", consume(), "dlq"); err != nil {
			t.Fatalf("NackChannelBatch DLQ failed under lib/pq: %v", err)
		}
		stats, err = pq.GetStats(ctx, "libpq-nack-chan", pgqueue.QueueTypeChannel)
		if err != nil {
			t.Fatalf("GetStats failed: %v", err)
		}
		if stats.DLQCount != libPQBatchSize {
			t.Errorf("expected %d in DLQ, got %d", libPQBatchSize, stats.DLQCount)
		}
	})

	t.Run("AckTopicBatch", func(t *testing.T) {
		if err := pq.CreateTopic(ctx, "libpq-ack-topic"); err != nil {
			t.Fatalf("CreateTopic failed: %v", err)
		}
		if err := pq.Subscribe(ctx, "libpq-ack-topic", "sub"); err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}
		if _, err := pq.PublishBatch(ctx, "libpq-ack-topic", libPQMessages("ack")); err != nil {
			t.Fatalf("PublishBatch failed: %v", err)
		}

		receipts := make([]pgqueue.Receipt, libPQBatchSize)
		for i := range receipts {
			msg, err := pq.ConsumeFromTopic(ctx, "libpq-ack-topic", "sub", 30*time.Second)
			if err != nil {
				t.Fatalf("ConsumeFromTopic failed: %v", err)
			}
			receipts[i] = msg.Receipt()
		}

		if err := pq.AckTopicBatch(ctx, "libpq-ack-topic", "sub", receipts); err != nil {
			t.Fatalf("AckTopicBatch failed under lib/pq: %v", err)
		}
	})

	// NackTopicBatch exercises fetchBatchSubStates, batchRetryPubSubMessages and
	// the pub/sub batchMoveToDLQ; ReplayDLQ then exercises reinsertDLQPubSub.
	t.Run("NackTopicBatchAndReplayDLQ", func(t *testing.T) {
		if err := pq.CreateTopic(ctx, "libpq-nack-topic", pgqueue.WithQueueMaxRetries(1)); err != nil {
			t.Fatalf("CreateTopic failed: %v", err)
		}
		if err := pq.Subscribe(ctx, "libpq-nack-topic", "sub"); err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}
		if _, err := pq.PublishBatch(ctx, "libpq-nack-topic", libPQMessages("nack")); err != nil {
			t.Fatalf("PublishBatch failed: %v", err)
		}

		consume := func() []pgqueue.Receipt {
			receipts := make([]pgqueue.Receipt, libPQBatchSize)
			for i := range receipts {
				msg, err := pq.ConsumeFromTopic(ctx, "libpq-nack-topic", "sub", 30*time.Second)
				if err != nil {
					t.Fatalf("ConsumeFromTopic failed: %v", err)
				}
				receipts[i] = msg.Receipt()
			}
			return receipts
		}

		// First nack: retry path (batchRetryPubSubMessages).
		if err := pq.NackTopicBatch(ctx, "libpq-nack-topic", "sub", consume(), "retry"); err != nil {
			t.Fatalf("NackTopicBatch retry failed under lib/pq: %v", err)
		}
		// Second nack: DLQ path (batchMoveToDLQ, pub/sub).
		if err := pq.NackTopicBatch(ctx, "libpq-nack-topic", "sub", consume(), "dlq"); err != nil {
			t.Fatalf("NackTopicBatch DLQ failed under lib/pq: %v", err)
		}

		dlqStats, err := pq.GetDLQStats(ctx, "libpq-nack-topic", pgqueue.QueueTypePubSub)
		if err != nil {
			t.Fatalf("GetDLQStats failed: %v", err)
		}
		if dlqStats.TotalCount != libPQBatchSize {
			t.Fatalf("expected %d in topic DLQ, got %d", libPQBatchSize, dlqStats.TotalCount)
		}

		res, err := pq.ReplayDLQ(ctx, "libpq-nack-topic", pgqueue.QueueTypePubSub, pgqueue.ReplayOptions{
			Confirm:     true,
			PerformedBy: "test",
		})
		if err != nil {
			t.Fatalf("ReplayDLQ (pub/sub) failed under lib/pq: %v", err)
		}
		if res.Replayed != libPQBatchSize {
			t.Errorf("expected %d replayed from topic DLQ, got %d", libPQBatchSize, res.Replayed)
		}
	})

	// ReplayFrom exercises applyReplayFrom (UPDATE ... WHERE id = ANY(uuid[])).
	t.Run("ReplayFrom", func(t *testing.T) {
		if err := pq.CreateChannel(ctx, "libpq-replay"); err != nil {
			t.Fatalf("CreateChannel failed: %v", err)
		}
		startTime := time.Now().Add(-time.Hour)
		for range libPQBatchSize {
			if _, err := pq.Publish(ctx, "libpq-replay", []byte("replay")); err != nil {
				t.Fatalf("Publish failed: %v", err)
			}
			msg, err := pq.ConsumeFromChannel(ctx, "libpq-replay", 30*time.Second)
			if err != nil {
				t.Fatalf("ConsumeFromChannel failed: %v", err)
			}
			if err := pq.AckChannel(ctx, "libpq-replay", msg.Receipt()); err != nil {
				t.Fatalf("AckChannel failed: %v", err)
			}
		}

		count, err := pq.ReplayFrom(ctx, "libpq-replay", pgqueue.QueueTypeChannel, startTime, pgqueue.ReplayOptions{
			Confirm:     true,
			PerformedBy: "test",
		})
		if err != nil {
			t.Fatalf("ReplayFrom failed under lib/pq: %v", err)
		}
		if count != libPQBatchSize {
			t.Errorf("expected %d replayed, got %d", libPQBatchSize, count)
		}
	})

	// ReplayDLQ on a channel exercises reinsertDLQChannel and
	// filterExistingMessages (both WHERE id = ANY(uuid[])).
	t.Run("ReplayDLQChannel", func(t *testing.T) {
		if err := pq.CreateChannel(ctx, "libpq-replay-dlq", pgqueue.WithQueueMaxRetries(1)); err != nil {
			t.Fatalf("CreateChannel failed: %v", err)
		}
		if _, err := pq.Publish(ctx, "libpq-replay-dlq", []byte("dlq")); err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
		for range 2 {
			msg, err := pq.ConsumeFromChannel(ctx, "libpq-replay-dlq", 30*time.Second)
			if err != nil {
				t.Fatalf("ConsumeFromChannel failed: %v", err)
			}
			if err := pq.NackChannel(ctx, "libpq-replay-dlq", msg.Receipt(), "fail"); err != nil {
				t.Fatalf("NackChannel failed: %v", err)
			}
		}

		res, err := pq.ReplayDLQ(ctx, "libpq-replay-dlq", pgqueue.QueueTypeChannel, pgqueue.ReplayOptions{
			Confirm:     true,
			PerformedBy: "test",
		})
		if err != nil {
			t.Fatalf("ReplayDLQ (channel) failed under lib/pq: %v", err)
		}
		if res.Replayed != 1 {
			t.Errorf("expected 1 replayed from DLQ, got %d", res.Replayed)
		}
	})
}
