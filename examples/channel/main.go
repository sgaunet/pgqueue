// Package main demonstrates pgqueue channel (point-to-point) messaging using
// the handler-based consume API: pgqueue owns the loop and ack/nack lifecycle.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	// _ "github.com/lib/pq" // Alternative: use lib/pq driver ("postgres").
	"github.com/sgaunet/pgqueue"
)

const (
	maxMessageSize    = 1024 * 1024 // 1 MiB.
	defaultMaxRetries = 3
	channelMaxRetries = 3
	publishDelay      = 200 * time.Millisecond
	consumeWindow     = 5 * time.Second
	workerConcurrency = 4
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	// Open a database connection. The driver is the caller's choice — pgqueue
	// depends only on database/sql, not on any specific driver.
	db, err := sql.Open(
		"pgx",
		"postgres://postgres:postgres@localhost:5432/pgqueue_example?sslmode=disable")
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer func() { _ = db.Close() }()

	// Initialize the base schema (one-time setup, idempotent).
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		return fmt.Errorf("failed to initialize schema: %w", err)
	}

	pq, err := pgqueue.New(ctx, db,
		pgqueue.WithMaxMessageSize(maxMessageSize),
		pgqueue.WithDefaultMaxRetries(defaultMaxRetries),
	)
	if err != nil {
		return fmt.Errorf("failed to initialize pgqueue: %w", err)
	}
	defer func() { _ = pq.Close() }()

	channelName := "orders"
	if err := pq.CreateChannel(ctx, channelName,
		pgqueue.WithQueueMaxRetries(channelMaxRetries)); err != nil {
		log.Printf("channel might already exist: %v", err)
	}

	publishOrders(ctx, pq, channelName)
	consumeOrders(ctx, pq, channelName)
	printStats(ctx, pq, channelName)

	fmt.Println("\nExample completed successfully!")
	return nil
}

func publishOrders(ctx context.Context, pq *pgqueue.Queue, channelName string) {
	orders := []string{
		"order-001: 2x Widget A",
		"order-002: 1x Widget B",
		"order-003: 5x Widget C",
		"order-004: 3x Widget A",
		"order-005: 1x Widget D",
	}

	fmt.Println("Publishing orders...")
	for _, order := range orders {
		msgID, err := pq.Publish(ctx, channelName, []byte(order))
		if err != nil {
			log.Printf("failed to publish order: %v", err)
			continue
		}
		fmt.Printf("Published: %s (ID: %s)\n", order, msgID)
		time.Sleep(publishDelay)
	}
}

// consumeOrders runs the handler-based consume loop. ConsumeChannel owns the
// loop: it fetches each message, calls the handler, then auto-acks on a nil
// return or auto-nacks (with retry/backoff) on an error. It returns when the
// context is cancelled.
func consumeOrders(ctx context.Context, pq *pgqueue.Queue, channelName string) {
	fmt.Println("\nProcessing orders...")

	consumeCtx, cancel := context.WithTimeout(ctx, consumeWindow)
	defer cancel()

	handler := func(_ context.Context, msg *pgqueue.Message) error {
		fmt.Printf("Completed: %s\n", string(msg.Payload))
		return nil
	}

	if err := pq.ConsumeChannel(consumeCtx, channelName, handler,
		pgqueue.WithConcurrency(workerConcurrency)); err != nil {
		log.Printf("consume loop error: %v", err)
	}
}

func printStats(ctx context.Context, pq *pgqueue.Queue, channelName string) {
	stats, err := pq.GetStats(ctx, channelName, pgqueue.QueueTypeChannel)
	if err != nil {
		log.Printf("failed to get stats: %v", err)
		return
	}
	fmt.Printf("\nQueue Statistics:\n")
	fmt.Printf("  Pending: %d\n", stats.PendingCount)
	fmt.Printf("  Processing: %d\n", stats.ProcessingCount)
	fmt.Printf("  Completed: %d\n", stats.CompletedCount)
	fmt.Printf("  DLQ: %d\n", stats.DLQCount)
}
