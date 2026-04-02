package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
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

	// Create a topic for user events
	topicName := "user-events"
	err = pq.CreateTopic(ctx, topicName, pgqueue.TopicOptions{})
	if err != nil {
		log.Printf("topic might already exist: %v", err)
	}

	// Define subscribers (different services that need to process user events)
	subscribers := []string{
		"email-service",      // Sends welcome emails
		"analytics-service",  // Tracks user behavior
		"notification-service", // Sends push notifications
	}

	// Register all subscribers
	fmt.Println("Registering subscribers...")
	for _, subscriberID := range subscribers {
		err = pq.Subscribe(ctx, topicName, subscriberID)
		if err != nil {
			log.Printf("failed to subscribe %s: %v", subscriberID, err)
		} else {
			fmt.Printf("Registered: %s\n", subscriberID)
		}
	}

	// Start goroutines for each subscriber
	var wg sync.WaitGroup
	for _, subscriberID := range subscribers {
		wg.Add(1)
		go consumeEvents(ctx, pq, topicName, subscriberID, &wg)
	}

	// Give consumers time to start
	time.Sleep(1 * time.Second)

	// Publish some user events
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
		time.Sleep(500 * time.Millisecond)
	}

	// Wait for processing to complete
	fmt.Println("\nWaiting for events to be processed by all subscribers...")
	time.Sleep(8 * time.Second)

	// Get statistics for each subscriber
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

	fmt.Println("\nExample completed successfully!")
	fmt.Println("Note: Each subscriber processes the same events independently (fan-out pattern)")
}

func consumeEvents(ctx context.Context, pq *pgqueue.PGQueue, topicName, subscriberID string, wg *sync.WaitGroup) {
	defer wg.Done()
	
	fmt.Printf("[%s] Starting...\n", subscriberID)
	
	// Process for a limited time (for example purposes)
	timeout := time.After(10 * time.Second)

	for {
		select {
		case <-timeout:
			fmt.Printf("[%s] Shutting down\n", subscriberID)
			return
		default:
			// Consume next event for this subscriber
			msg, err := pq.ConsumeFromTopic(ctx, topicName, subscriberID, 30*time.Second)
			if err != nil {
				log.Printf("[%s] error consuming: %v", subscriberID, err)
				time.Sleep(1 * time.Second)
				continue
			}

			// No messages available
			if msg == nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			// Process the event (each subscriber handles it differently)
			processTime := time.Duration(100+subscriberID[0]%3*100) * time.Millisecond
			fmt.Printf("[%s] Processing: %s\n", subscriberID, string(msg.Payload))
			time.Sleep(processTime) // Simulate varying processing times

			// Acknowledge successful processing
			err = pq.AckTopic(ctx, topicName, subscriberID, msg.ID)
			if err != nil {
				log.Printf("[%s] error acking: %v", subscriberID, err)
				continue
			}

			fmt.Printf("[%s] Completed: %s\n", subscriberID, string(msg.Payload))
		}
	}
}
