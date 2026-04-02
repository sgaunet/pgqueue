package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	// _ "github.com/lib/pq" // Alternative: use lib/pq driver
	"github.com/sgaunet/pgqueue/pkg/pgqueue"
)

func main() {
	ctx := context.Background()

	// Open database connection with pgx driver
	db, err := sql.Open("pgx", "postgres://postgres:postgres@localhost:5432/pgqueue_example?sslmode=disable")
	// Alternative with lib/pq driver:
	// db, err := sql.Open("postgres", "postgres://postgres:postgres@localhost:5432/pgqueue_example?sslmode=disable")
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Initialize base schema (one-time setup, idempotent)
	// This creates the pgqueue_metadata, pgqueue_subscribers, and pgqueue_replay_log tables
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		log.Fatalf("failed to initialize schema: %v", err)
	}

	// Initialize pgqueue
	pq, err := pgqueue.Init(ctx, pgqueue.Config{
		DB:                db,
		MaxMessageSize:    1024 * 1024, // 1MB
		DefaultMaxRetries: 3,
	})
	if err != nil {
		log.Fatalf("failed to initialize pgqueue: %v", err)
	}

	// Create a channel for order processing
	channelName := "orders"
	err = pq.CreateChannel(ctx, channelName, pgqueue.ChannelOptions{
		MaxRetries: 3,
	})
	if err != nil {
		log.Printf("channel might already exist: %v", err)
	}

	// Start a goroutine to consume and process orders
	go consumeOrders(ctx, pq, channelName)

	// Publish some example orders
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
		time.Sleep(500 * time.Millisecond)
	}

	// Wait for processing to complete
	fmt.Println("\nWaiting for orders to be processed...")
	time.Sleep(5 * time.Second)

	// Get queue statistics
	stats, err := pq.GetStats(ctx, channelName, pgqueue.QueueTypeChannel)
	if err != nil {
		log.Printf("failed to get stats: %v", err)
	} else {
		fmt.Printf("\nQueue Statistics:\n")
		fmt.Printf("  Pending: %d\n", stats.PendingCount)
		fmt.Printf("  Processing: %d\n", stats.ProcessingCount)
		fmt.Printf("  Completed: %d\n", stats.CompletedCount)
		fmt.Printf("  Failed (DLQ): %d\n", stats.FailedCount)
	}

	fmt.Println("\nExample completed successfully!")
}

func consumeOrders(ctx context.Context, pq *pgqueue.PGQueue, channelName string) {
	fmt.Println("Starting order processor...")

	for {
		// Consume next order with 30 second visibility timeout
		msg, err := pq.ConsumeFromChannel(ctx, channelName, 30*time.Second)
		if err != nil {
			log.Printf("error consuming message: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		// No messages available
		if msg == nil {
			time.Sleep(1 * time.Second)
			continue
		}

		// Process the order
		fmt.Printf("Processing: %s\n", string(msg.Payload))
		time.Sleep(1 * time.Second) // Simulate processing time

		// Simulate occasional failures (10% failure rate for demonstration)
		// In production, this would be based on actual processing results
		if msg.RetryCount > 0 && msg.RetryCount%2 == 0 {
			// Nack the message (will retry or move to DLQ after max retries)
			err = pq.NackChannel(ctx, channelName, msg.ID, "simulated processing error")
			if err != nil {
				log.Printf("error nacking message: %v", err)
			}
			fmt.Printf("Failed to process (retry %d/%d): %s\n", msg.RetryCount+1, msg.MaxRetries, string(msg.Payload))
			continue
		}

		// Acknowledge successful processing
		err = pq.AckChannel(ctx, channelName, msg.ID)
		if err != nil {
			log.Printf("error acking message: %v", err)
			continue
		}

		fmt.Printf("Completed: %s\n", string(msg.Payload))
	}
}
