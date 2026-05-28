// Package main demonstrates pgqueue pub/sub (fan-out) messaging using the
// handler-based consume API: each subscriber runs its own ConsumeTopic loop and
// receives every published event independently.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	// _ "github.com/lib/pq" // Alternative: use lib/pq driver ("postgres").
	"github.com/sgaunet/pgqueue"
)

const (
	maxMessageSize     = 1024 * 1024 // 1 MiB.
	defaultMaxRetries  = 3
	publishDelay       = 200 * time.Millisecond
	consumeWindow      = 6 * time.Second
	consumerStartDelay = 500 * time.Millisecond
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	// Open a database connection. pgqueue depends only on database/sql; the
	// driver is the caller's choice.
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

	pq, err := pgqueue.New(ctx, db,
		pgqueue.WithMaxMessageSize(maxMessageSize),
		pgqueue.WithDefaultMaxRetries(defaultMaxRetries),
	)
	if err != nil {
		return fmt.Errorf("failed to initialize pgqueue: %w", err)
	}
	defer func() { _ = pq.Close() }()

	topicName := "user-events"
	if err := pq.CreateTopic(ctx, topicName); err != nil {
		log.Printf("topic might already exist: %v", err)
	}

	subscribers := []string{
		"email-service",
		"analytics-service",
		"notification-service",
	}
	registerSubscribers(ctx, pq, topicName, subscribers)

	// Each subscriber runs its own handler-based consume loop, bounded to a
	// fixed window for the example.
	consumeCtx, cancel := context.WithTimeout(ctx, consumeWindow)
	defer cancel()

	var wg sync.WaitGroup
	for _, subscriberID := range subscribers {
		wg.Add(1)
		go consumeEvents(consumeCtx, pq, topicName, subscriberID, &wg)
	}

	time.Sleep(consumerStartDelay) // give consumers time to attach
	publishEvents(ctx, pq, topicName)

	wg.Wait()
	printSubscriberStats(ctx, pq, topicName, subscribers)

	fmt.Println("\nExample completed successfully!")
	fmt.Println("Note: each subscriber processed every event independently (fan-out).")
	return nil
}

func registerSubscribers(
	ctx context.Context,
	pq *pgqueue.Queue,
	topicName string,
	subscribers []string,
) {
	fmt.Println("Registering subscribers...")
	for _, subscriberID := range subscribers {
		if err := pq.Subscribe(ctx, topicName, subscriberID); err != nil {
			log.Printf("failed to subscribe %s: %v", subscriberID, err)
			continue
		}
		fmt.Printf("Registered: %s\n", subscriberID)
	}
}

func publishEvents(ctx context.Context, pq *pgqueue.Queue, topicName string) {
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

// consumeEvents runs one subscriber's handler-based consume loop. ConsumeTopic
// owns the loop and the ack/nack lifecycle; it returns when consumeCtx expires.
func consumeEvents(
	ctx context.Context,
	pq *pgqueue.Queue,
	topicName, subscriberID string,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	fmt.Printf("[%s] starting...\n", subscriberID)

	handler := func(_ context.Context, msg *pgqueue.Message) error {
		fmt.Printf("[%s] completed: %s\n", subscriberID, string(msg.Payload))
		return nil
	}

	if err := pq.ConsumeTopic(ctx, topicName, subscriberID, handler); err != nil {
		log.Printf("[%s] consume loop error: %v", subscriberID, err)
	}
	fmt.Printf("[%s] shutting down\n", subscriberID)
}

func printSubscriberStats(
	ctx context.Context,
	pq *pgqueue.Queue,
	topicName string,
	subscribers []string,
) {
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
	}
}
