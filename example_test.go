package pgqueue_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/sgaunet/pgqueue"
)

// ExampleNew shows the standard initialization sequence: open a database
// handle, install the schema, then construct a Queue with functional options.
func ExampleNew() {
	ctx := context.Background()
	db, err := sql.Open("pgx", "postgres://localhost/app?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if err := pgqueue.InitSchema(ctx, db); err != nil {
		log.Fatal(err)
	}

	q, err := pgqueue.New(ctx, db,
		pgqueue.WithMaxMessageSize(1<<20), // 1 MiB
		pgqueue.WithDefaultMaxRetries(5),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = q.Close() }()
}

// ExampleQueue_Publish publishes a message; the queue type is resolved from the
// queue's metadata, so the same call serves channels and topics.
func ExampleQueue_Publish() {
	var (
		ctx context.Context
		q   *pgqueue.Queue
	)
	if err := q.CreateChannel(ctx, "orders"); err != nil {
		log.Fatal(err)
	}
	id, err := q.Publish(ctx, "orders", []byte(`{"order":123}`))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("published", id)
}

// ExampleQueue_ConsumeChannel consumes with the handler-based API: pgqueue owns
// the loop and acknowledges automatically based on the handler result.
func ExampleQueue_ConsumeChannel() {
	var (
		ctx context.Context
		q   *pgqueue.Queue
	)
	err := q.ConsumeChannel(ctx, "orders",
		func(_ context.Context, m *pgqueue.Message) error {
			// Returning nil auto-acks; returning an error auto-nacks with backoff.
			return process(m.Payload)
		},
		pgqueue.WithConcurrency(8),
	)
	if err != nil {
		log.Fatal(err)
	}
}

// ExampleQueue_ReceiveChannel shows the single-shot consume API and the
// ErrQueueEmpty signal that replaces the old (nil, nil) sentinel.
func ExampleQueue_ReceiveChannel() {
	var (
		ctx context.Context
		q   *pgqueue.Queue
	)
	msg, err := q.ReceiveChannel(ctx, "orders")
	switch {
	case errors.Is(err, pgqueue.ErrQueueEmpty):
		// nothing available right now
	case err != nil:
		log.Fatal(err)
	default:
		if err := process(msg.Payload); err != nil {
			_ = q.Nack(ctx, msg.Receipt(), err.Error())
		} else {
			_ = q.Ack(ctx, msg.Receipt())
		}
	}
}

// ExampleQueue_ChannelMessages consumes with the range-over-func iterator,
// acknowledging each message explicitly.
func ExampleQueue_ChannelMessages() {
	var (
		ctx context.Context
		q   *pgqueue.Queue
	)
	for msg, err := range q.ChannelMessages(ctx, "orders") {
		if err != nil {
			log.Print(err)
			break
		}
		if err := process(msg.Payload); err != nil {
			_ = q.Nack(ctx, msg.Receipt(), err.Error())
			continue
		}
		_ = q.Ack(ctx, msg.Receipt())
	}
}

// ExampleQueue_ListDLQMessages walks the dead-letter queue with keyset
// pagination.
func ExampleQueue_ListDLQMessages() {
	var (
		ctx context.Context
		q   *pgqueue.Queue
	)
	page := pgqueue.DLQPage{Limit: 100}
	for {
		msgs, next, err := q.ListDLQMessages(ctx, "orders", pgqueue.QueueTypeChannel, page)
		if err != nil {
			log.Fatal(err)
		}
		for _, m := range msgs {
			fmt.Println(m.ID, m.FailureReason)
		}
		if len(msgs) < page.Limit {
			break
		}
		page = next
	}
}

// ExampleQueue_ReplayDLQ replays every message from the dead-letter queue back
// onto the main queue. Pass ReplayOptions{DryRun: true} first if you want to
// preview the count without re-injecting. ReplayDLQ returns a ReplayDLQResult
// distinguishing replayed from skipped (un-replayable) rows.
func ExampleQueue_ReplayDLQ() {
	var (
		ctx context.Context
		q   *pgqueue.Queue
	)
	res, err := q.ReplayDLQ(ctx, "orders", pgqueue.QueueTypeChannel,
		pgqueue.ReplayOptions{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("replayed %d messages, skipped %d\n", res.Replayed, res.Skipped)
}

// ExampleNewGarbageCollector shows how to enable storage retention. The
// GarbageCollector is opt-in: message redelivery and DLQ promotion work without
// it, but old completed messages, expired DLQ entries, and acked subscription
// rows are only reclaimed once one is running. RetentionPolicy fields default
// to 0 ("keep forever"), so positive durations must be set to enable purging.
func ExampleNewGarbageCollector() {
	var (
		ctx context.Context
		q   *pgqueue.Queue
	)
	gc, err := pgqueue.NewGarbageCollector(q, pgqueue.GarbageCollectorConfig{
		Interval: 5 * time.Minute,
		DefaultPolicy: pgqueue.RetentionPolicy{
			CompletedMessageTTL: 24 * time.Hour,
			MaxPendingAge:       7 * 24 * time.Hour,
			DLQRetention:        30 * 24 * time.Hour,
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	gc.Start(ctx) // background loop; q.Close() stops it
}

// process is a placeholder for the caller's message-handling logic.
func process(payload []byte) error {
	_ = payload
	return nil
}
