// Package main demonstrates pgqueue channel (point-to-point) messaging pattern.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	// _ "github.com/lib/pq" // Alternative: use lib/pq driver.
	"github.com/sgaunet/pgqueue/pkg/pgqueue"
)

const (
	maxMessageSize    = 1024 * 1024 // 1MB.
	defaultMaxRetries = 3
	channelMaxRetries = 3
	publishDelay      = 500 * time.Millisecond
	processingWait    = 5 * time.Second
	visibilityTimeout = 30 * time.Second
	pollInterval      = 1 * time.Second
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	// Open database connection with pgx driver
	db, err := sql.Open(
		"pgx",
		"postgres://postgres:postgres@localhost:5432/pgqueue_example?sslmode=disable")
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer func() { _ = db.Close() }()

	// Initialize base schema (one-time setup, idempotent)
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		return fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Initialize pgqueue
	pq, err := pgqueue.New(ctx, db,
		pgqueue.WithMaxMessageSize(maxMessageSize),
		pgqueue.WithDefaultMaxRetries(defaultMaxRetries),
	)
	if err != nil {
		return fmt.Errorf("failed to initialize pgqueue: %w", err)
	}

	// Create a channel for order processing
	channelName := "orders"
	err = pq.CreateChannel(ctx, channelName, pgqueue.WithQueueMaxRetries(channelMaxRetries))
	if err != nil {
		log.Printf("channel might already exist: %v", err)
	}

	// Start a goroutine to consume and process orders
	go consumeOrders(ctx, pq, channelName)

	publishOrders(ctx, pq, channelName)

	// Wait for processing to complete
	fmt.Println("\nWaiting for orders to be processed...")
	time.Sleep(processingWait)

	printStats(ctx, pq, channelName)
	fmt.Println("\nExample completed successfully!")

	return nil
}

func publishOrders(
	ctx context.Context,
	pq *pgqueue.Queue,
	channelName string) {
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

func printStats(
	ctx context.Context,
	pq *pgqueue.Queue,
	channelName string) {
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

func consumeOrders(
	ctx context.Context,
	pq *pgqueue.Queue,
	channelName string) {
	fmt.Println("Starting order processor...")

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Consume next order with visibility timeout
		msg, err := pq.ConsumeFromChannel(ctx, channelName, visibilityTimeout)
		if err != nil {
			log.Printf("error consuming message: %v", err)
			time.Sleep(pollInterval)
			continue
		}

		// No messages available
		if msg == nil {
			time.Sleep(pollInterval)
			continue
		}

		// Process the order
		fmt.Printf("Processing: %s\n", string(msg.Payload))
		time.Sleep(pollInterval) // Simulate processing time

		// Simulate occasional failures
		if msg.RetryCount > 0 && msg.RetryCount%2 == 0 {
			err = pq.NackChannel(
				ctx, channelName, msg.Receipt(), "simulated processing error")
			if err != nil {
				log.Printf("error nacking message: %v", err)
			}
			fmt.Printf(
				"Failed to process (retry %d/%d): %s\n",
				msg.RetryCount+1, msg.MaxRetries, string(msg.Payload))
			continue
		}

		// Acknowledge successful processing
		err = pq.AckChannel(ctx, channelName, msg.Receipt())
		if err != nil {
			log.Printf("error acking message: %v", err)
			continue
		}

		fmt.Printf("Completed: %s\n", string(msg.Payload))
	}
}
