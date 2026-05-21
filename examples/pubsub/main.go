// Package main demonstrates pgqueue pub/sub (fan-out) messaging pattern.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	// _ "github.com/lib/pq" // Alternative: use lib/pq driver.
	"github.com/sgaunet/pgqueue/pkg/pgqueue"
)

const (
	maxMessageSize     = 1024 * 1024 // 1MB.
	defaultMaxRetries  = 3
	publishDelay       = 500 * time.Millisecond
	processingWait     = 8 * time.Second
	consumerTimeout    = 10 * time.Second
	visibilityTimeout  = 30 * time.Second
	noMessageDelay     = 500 * time.Millisecond
	baseProcessingTime = 100
	pollInterval       = 1 * time.Second
	consumerStartDelay = 1 * time.Second
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

	// Create a topic for user events
	topicName := "user-events"
	err = pq.CreateTopic(ctx, topicName)
	if err != nil {
		log.Printf("topic might already exist: %v", err)
	}

	subscribers := []string{
		"email-service",
		"analytics-service",
		"notification-service",
	}

	registerSubscribers(ctx, pq, topicName, subscribers)

	// Start goroutines for each subscriber
	var wg sync.WaitGroup
	for _, subscriberID := range subscribers {
		wg.Add(1)
		go consumeEvents(ctx, pq, topicName, subscriberID, &wg)
	}

	// Give consumers time to start
	time.Sleep(consumerStartDelay)

	publishEvents(ctx, pq, topicName)

	// Wait for processing to complete
	fmt.Println("\nWaiting for events to be processed by all subscribers...")
	time.Sleep(processingWait)

	printSubscriberStats(ctx, pq, topicName, subscribers)

	fmt.Println("\nExample completed successfully!")
	fmt.Println(
		"Note: Each subscriber processes the same events independently (fan-out pattern)")

	return nil
}

func registerSubscribers(
	ctx context.Context,
	pq *pgqueue.Queue,
	topicName string,
	subscribers []string) {
	fmt.Println("Registering subscribers...")
	for _, subscriberID := range subscribers {
		err := pq.Subscribe(ctx, topicName, subscriberID)
		if err != nil {
			log.Printf("failed to subscribe %s: %v", subscriberID, err)
		} else {
			fmt.Printf("Registered: %s\n", subscriberID)
		}
	}
}

func publishEvents(
	ctx context.Context,
	pq *pgqueue.Queue,
	topicName string) {
	events := []string{
		"user.registered: user_id=1001, email=alice@example.com",
		"user.registered: user_id=1002, email=bob@example.com",
		"user.login: user_id=1001, ip=192.168.1.100",
		"user.profile_updated: user_id=1002, field=avatar",
		"user.registered: user_id=1003, email=carol@example.com",
	}

	fmt.Println("\nPublishing user events...")
	for _, event := range events {
		msgID, err := pq.Publish(ctx, topicName, []byte(event))
		if err != nil {
			log.Printf("failed to publish event: %v", err)
			continue
		}
		fmt.Printf("Published: %s (ID: %s)\n", event, msgID)
		time.Sleep(publishDelay)
	}
}

func printSubscriberStats(
	ctx context.Context,
	pq *pgqueue.Queue,
	topicName string,
	subscribers []string) {
	fmt.Println("\nSubscriber Statistics:")
	for _, subscriberID := range subscribers {
		lag, err := pq.GetSubscriberLag(ctx, topicName, subscriberID)
		if err != nil {
			log.Printf("failed to get lag for %s: %v", subscriberID, err)
			continue
		}
		fmt.Printf("  %s:\n", subscriberID)
		fmt.Printf("    Pending: %d\n", lag.PendingCount)
		fmt.Printf("    Processing: %d\n", lag.ProcessingCount)
		fmt.Printf("    Acknowledged: %d\n", lag.AckedCount)
		if lag.OldestPendingAge != nil {
			fmt.Printf("    Oldest Pending Age: %v\n", *lag.OldestPendingAge)
		}
	}
}

func consumeEvents(
	ctx context.Context,
	pq *pgqueue.Queue,
	topicName, subscriberID string,
	wg *sync.WaitGroup) {
	defer wg.Done()

	fmt.Printf("[%s] Starting...\n", subscriberID)

	// Process for a limited time (for example purposes)
	timeout := time.After(consumerTimeout)

	for {
		select {
		case <-timeout:
			fmt.Printf("[%s] Shutting down\n", subscriberID)
			return
		default:
			// Consume next event for this subscriber
			msg, err := pq.ConsumeFromTopic(
				ctx, topicName, subscriberID, visibilityTimeout)
			if err != nil {
				log.Printf("[%s] error consuming: %v", subscriberID, err)
				time.Sleep(pollInterval)
				continue
			}

			// No messages available
			if msg == nil {
				time.Sleep(noMessageDelay)
				continue
			}

			// Process the event (each subscriber handles it differently)
			processTime := time.Duration(
				baseProcessingTime+subscriberID[0]%3*baseProcessingTime) * time.Millisecond
			fmt.Printf(
				"[%s] Processing: %s\n", subscriberID, string(msg.Payload))
			time.Sleep(processTime) // Simulate varying processing times

			// Acknowledge successful processing
			err = pq.AckTopic(ctx, topicName, subscriberID, msg.Receipt())
			if err != nil {
				log.Printf("[%s] error acking: %v", subscriberID, err)
				continue
			}

			fmt.Printf(
				"[%s] Completed: %s\n", subscriberID, string(msg.Payload))
		}
	}
}
