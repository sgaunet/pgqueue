// Package main demonstrates push-based delivery: the optional pglisten adapter
// wires PostgreSQL LISTEN/NOTIFY into pgqueue so a blocked consumer wakes the
// instant a message is published, instead of waiting for the safety-net poll.
//
// pglisten is a separate Go module, so only programs that want push delivery
// pull in its pgx dependency.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sgaunet/pgqueue"
	"github.com/sgaunet/pgqueue/pglisten"
)

const (
	connString    = "postgres://postgres:postgres@localhost:5432/pgqueue_example?sslmode=disable"
	startupGrace  = 500 * time.Millisecond
	deliveryLimit = 5 * time.Second
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	db, err := sql.Open("pgx", connString)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := pgqueue.InitSchema(ctx, db); err != nil {
		return fmt.Errorf("failed to initialize schema: %w", err)
	}

	// pglisten opens its own dedicated pgx connection for LISTEN/NOTIFY.
	// Registering it with WithListener hands ownership to the Queue: pq.Close
	// closes the listener too.
	listener, err := pglisten.New(ctx, connString)
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}

	pq, err := pgqueue.New(ctx, db, pgqueue.WithListener(listener))
	if err != nil {
		return fmt.Errorf("failed to initialize pgqueue: %w", err)
	}
	defer func() { _ = pq.Close() }()

	const channelName = "notifications"
	if err := pq.CreateChannel(ctx, channelName); err != nil {
		log.Printf("channel might already exist: %v", err)
	}

	// Start a consumer that blocks waiting for work. With the listener wired in,
	// a NOTIFY emitted inside the publishing transaction wakes it immediately,
	// rather than on the next safety-net poll tick.
	consumeCtx, cancel := context.WithCancel(ctx)
	received := make(chan time.Time, 1)
	var wg sync.WaitGroup
	wg.Go(func() {
		handler := func(_ context.Context, msg *pgqueue.Message) error {
			received <- time.Now()
			fmt.Printf("Received (push): %s\n", string(msg.Payload))
			return nil
		}
		if err := pq.ConsumeChannel(consumeCtx, channelName, handler); err != nil {
			log.Printf("consume loop error: %v", err)
		}
	})

	// Give the consumer a moment to start blocking on the listener.
	time.Sleep(startupGrace)

	fmt.Println("Publishing one message; the consumer should wake almost instantly...")
	publishedAt := time.Now()
	if _, err := pq.Publish(ctx, channelName, []byte("hello via LISTEN/NOTIFY")); err != nil {
		cancel()
		wg.Wait()
		return fmt.Errorf("failed to publish: %w", err)
	}

	select {
	case at := <-received:
		fmt.Printf("Push latency: %s\n", at.Sub(publishedAt).Round(time.Millisecond))
	case <-time.After(deliveryLimit):
		fmt.Println("Timed out waiting for push delivery")
	}

	cancel()
	wg.Wait()

	fmt.Println("\nExample completed successfully!")
	return nil
}
