// Package main demonstrates pgqueue's dead-letter queue and replay APIs: a
// message that keeps failing lands in the DLQ, is inspected with
// ListDLQMessages, previewed with a ReplayDLQ dry run (which mutates nothing),
// and finally replayed back onto the live channel where a fixed consumer
// processes it to completion.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	// _ "github.com/lib/pq" // Alternative: use lib/pq driver ("postgres").
	"github.com/sgaunet/pgqueue"
)

const (
	// A zero retry budget dead-letters a message right after its single failed
	// attempt, so the example reaches the DLQ deterministically and fast.
	channelMaxRetries = 0
	consumeWindow     = 3 * time.Second
	pollInterval      = 200 * time.Millisecond
	dlqPageLimit      = 50
)

// errPoison is the failure the first consumer always returns, so every message
// exhausts its retries and is dead-lettered.
var errPoison = errors.New("simulated processing failure")

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	// The driver is the caller's choice — pgqueue depends only on database/sql.
	db, err := sql.Open(
		"pgx",
		"postgres://postgres:postgres@localhost:5432/pgqueue_example?sslmode=disable")
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := pgqueue.InitSchema(ctx, db); err != nil {
		return fmt.Errorf("failed to initialize schema: %w", err)
	}

	pq, err := pgqueue.New(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to initialize pgqueue: %w", err)
	}
	defer func() { _ = pq.Close() }()

	const channelName = "payments"
	if err := pq.CreateChannel(ctx, channelName,
		pgqueue.WithQueueMaxRetries(channelMaxRetries)); err != nil {
		log.Printf("channel might already exist: %v", err)
	}

	publishPayments(ctx, pq, channelName)
	failEveryMessage(ctx, pq, channelName) // drives them all into the DLQ
	inspectDLQ(ctx, pq, channelName)
	previewReplay(ctx, pq, channelName) // dry run: mutates nothing
	doReplay(ctx, pq, channelName)      // real replay back onto the channel
	processSuccessfully(ctx, pq, channelName)
	printStats(ctx, pq, channelName)

	fmt.Println("\nExample completed successfully!")
	return nil
}

func publishPayments(ctx context.Context, pq *pgqueue.Queue, channelName string) {
	fmt.Println("Publishing payments...")
	for _, p := range []string{"pay-001", "pay-002", "pay-003"} {
		if _, err := pq.Publish(ctx, channelName, []byte(p)); err != nil {
			log.Printf("failed to publish %s: %v", p, err)
			continue
		}
		fmt.Printf("Published: %s\n", p)
	}
}

// failEveryMessage consumes with a handler that always errors. With
// WithQueueMaxRetries(0) each message is attempted once, then dead-lettered.
func failEveryMessage(ctx context.Context, pq *pgqueue.Queue, channelName string) {
	fmt.Println("\nProcessing (handler fails every message -> DLQ)...")
	consumeCtx, cancel := context.WithTimeout(ctx, consumeWindow)
	defer cancel()

	handler := func(_ context.Context, msg *pgqueue.Message) error {
		fmt.Printf("Failed: %s\n", string(msg.Payload))
		return errPoison
	}
	if err := pq.ConsumeChannel(consumeCtx, channelName, handler,
		pgqueue.WithPollInterval(pollInterval)); err != nil {
		log.Printf("consume loop error: %v", err)
	}
}

func inspectDLQ(ctx context.Context, pq *pgqueue.Queue, channelName string) {
	fmt.Println("\nDead-letter queue contents:")
	msgs, _, err := pq.ListDLQMessages(ctx, channelName, pgqueue.QueueTypeChannel,
		pgqueue.DLQPage{Limit: dlqPageLimit})
	if err != nil {
		log.Printf("failed to list DLQ: %v", err)
		return
	}
	for _, m := range msgs {
		fmt.Printf("  %s (reason: %q, retries: %d)\n",
			string(m.Payload), m.FailureReason, m.RetryCount)
	}
}

func previewReplay(ctx context.Context, pq *pgqueue.Queue, channelName string) {
	fmt.Println("\nReplay dry run (no mutation):")
	res, err := pq.ReplayDLQ(ctx, channelName, pgqueue.QueueTypeChannel,
		pgqueue.ReplayOptions{DryRun: true})
	if err != nil {
		log.Printf("dry-run replay failed: %v", err)
		return
	}
	fmt.Printf("  would replay %d, skip %d\n", res.Replayed, res.Skipped)
}

func doReplay(ctx context.Context, pq *pgqueue.Queue, channelName string) {
	fmt.Println("\nReplaying from the DLQ back onto the channel:")
	res, err := pq.ReplayDLQ(ctx, channelName, pgqueue.QueueTypeChannel,
		pgqueue.ReplayOptions{PerformedBy: "replay-example"})
	if err != nil {
		log.Printf("replay failed: %v", err)
		return
	}
	fmt.Printf("  replayed %d, skipped %d\n", res.Replayed, res.Skipped)
}

// processSuccessfully drains the replayed messages with a handler that now
// succeeds, so they leave the queue for good.
func processSuccessfully(ctx context.Context, pq *pgqueue.Queue, channelName string) {
	fmt.Println("\nProcessing replayed payments (handler now succeeds)...")
	consumeCtx, cancel := context.WithTimeout(ctx, consumeWindow)
	defer cancel()

	handler := func(_ context.Context, msg *pgqueue.Message) error {
		fmt.Printf("Completed: %s\n", string(msg.Payload))
		return nil
	}
	if err := pq.ConsumeChannel(consumeCtx, channelName, handler,
		pgqueue.WithPollInterval(pollInterval)); err != nil {
		log.Printf("consume loop error: %v", err)
	}
}

func printStats(ctx context.Context, pq *pgqueue.Queue, channelName string) {
	stats, err := pq.Stats(ctx, channelName, pgqueue.QueueTypeChannel)
	if err != nil {
		log.Printf("failed to get stats: %v", err)
		return
	}
	fmt.Printf("\nQueue Statistics:\n")
	fmt.Printf("  Pending: %d\n", stats.PendingCount)
	fmt.Printf("  Completed: %d\n", stats.CompletedCount)
	fmt.Printf("  DLQ: %d\n", stats.DLQCount)
}
