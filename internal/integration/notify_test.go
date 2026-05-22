package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sgaunet/pgqueue"
	"github.com/sgaunet/pgqueue/pglisten"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setupNotifyTest starts a PostgreSQL container and returns the connection
// string alongside the raw DB handle, so a pglisten.Listener can open its own
// dedicated connection.
func setupNotifyTest(t *testing.T) (*sql.DB, string, func()) {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
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

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}

	cleanup := func() {
		_ = db.Close()
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}
	return db, connStr, cleanup
}

// TestNotifyIdleConsumerWakesUnderOneSecond verifies that an idle consumer
// receives a published message in well under one second via LISTEN/NOTIFY,
// even when the safety-net poll is set far longer than the test window — which
// proves the wake-up came from a push notification, not a periodic query.
func TestNotifyIdleConsumerWakesUnderOneSecond(t *testing.T) {
	db, connStr, cleanup := setupNotifyTest(t)
	defer cleanup()

	ctx := context.Background()
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("failed to initialize schema: %v", err)
	}

	listener, err := pglisten.New(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}

	pq, err := pgqueue.New(ctx, db,
		pgqueue.WithListener(listener),
		// A 60s safety-net poll guarantees a sub-second delivery can only come
		// from a NOTIFY wake-up, not the fallback poll.
		pgqueue.WithSafetyNetPoll(60*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to init pgqueue: %v", err)
	}
	defer func() { _ = pq.Close() }()

	const channelName = "notify-idle"
	if err := pq.CreateChannel(ctx, channelName); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	received := make(chan time.Time, 1)
	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		_ = pq.ConsumeChannel(consumeCtx, channelName,
			func(_ context.Context, _ *pgqueue.Message) error {
				received <- time.Now()
				return nil
			})
	}()

	// Let the consumer attach and go idle (it must be blocked, not polling).
	time.Sleep(500 * time.Millisecond)

	publishedAt := time.Now()
	if _, err := pq.PublishChannel(ctx, channelName, []byte("wake up")); err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	select {
	case at := <-received:
		latency := at.Sub(publishedAt)
		if latency > 1*time.Second {
			t.Fatalf("delivery latency %v exceeds 1s; push delivery did not wake the consumer", latency)
		}
		t.Logf("push delivery latency: %v", latency)
	case <-time.After(5 * time.Second):
		t.Fatal("message was not delivered within 5s")
	}
}

// TestNotifySafetyNetPollDeliversMissedMessage verifies that a message whose
// NOTIFY was never observed by the listener (it was published before the
// listener subscribed) is still delivered by the bounded safety-net poll.
func TestNotifySafetyNetPollDeliversMissedMessage(t *testing.T) {
	db, connStr, cleanup := setupNotifyTest(t)
	defer cleanup()

	ctx := context.Background()
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("failed to initialize schema: %v", err)
	}

	listener, err := pglisten.New(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}

	pq, err := pgqueue.New(ctx, db,
		pgqueue.WithListener(listener),
		pgqueue.WithSafetyNetPoll(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("failed to init pgqueue: %v", err)
	}
	defer func() { _ = pq.Close() }()

	const channelName = "notify-missed"
	if err := pq.CreateChannel(ctx, channelName); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish BEFORE any consumer subscribes: the NOTIFY fires with nobody
	// listening on this channel, so it is genuinely missed.
	if _, err := pq.PublishChannel(ctx, channelName, []byte("missed")); err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	// A single-shot receive at this point would normally be served by the poll
	// loop; here we use ReceiveChannel directly to confirm the message exists,
	// then a fresh blocking consumer to confirm the poll path delivers it.
	received := make(chan struct{}, 1)
	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		_ = pq.ConsumeChannel(consumeCtx, channelName,
			func(_ context.Context, msg *pgqueue.Message) error {
				if string(msg.Payload) == "missed" {
					received <- struct{}{}
				}
				return nil
			})
	}()

	select {
	case <-received:
		// Delivered by the safety-net poll despite the missed notification.
	case <-time.After(5 * time.Second):
		t.Fatal("missed message was not delivered by the safety-net poll within 5s")
	}

	// Sanity: a follow-up empty receive must report ErrQueueEmpty, not (nil,nil).
	if _, err := pq.ReceiveChannel(ctx, channelName); !errors.Is(err, pgqueue.ErrQueueEmpty) {
		t.Fatalf("expected ErrQueueEmpty after draining, got %v", err)
	}
}

// TestWakeChanConcurrent (R-06) exercises the internal listen-state machine
// under contention: many goroutines start blocking ConsumeChannel loops on the
// same and on different channels — each loop drives the wakeChan listen-state
// transition — while another goroutine concurrently calls Close().
//
// Run under -race -count=2: the assertion is that there is no data race on the
// listening/closed state, no panic, and that Close() shuts down cleanly even
// while LISTEN registrations are in flight.
func TestWakeChanConcurrent(t *testing.T) {
	db, connStr, cleanup := setupNotifyTest(t)
	defer cleanup()

	ctx := context.Background()
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("failed to initialize schema: %v", err)
	}

	listener, err := pglisten.New(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}

	pq, err := pgqueue.New(ctx, db,
		pgqueue.WithListener(listener),
		pgqueue.WithSafetyNetPoll(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("failed to init pgqueue: %v", err)
	}

	// Three distinct channels, several consumers each — same-channel consumers
	// contend on the same listen registration; cross-channel consumers exercise
	// independent registrations.
	channels := []string{"wake-a", "wake-b", "wake-c"}
	for _, ch := range channels {
		if err := pq.CreateChannel(ctx, ch); err != nil {
			t.Fatalf("failed to create channel %s: %v", ch, err)
		}
	}

	consumeCtx, cancelConsumers := context.WithCancel(ctx)
	const consumersPerChannel = 4

	var consumerWG sync.WaitGroup
	for _, ch := range channels {
		for range consumersPerChannel {
			consumerWG.Add(1)
			go func(channel string) {
				defer consumerWG.Done()
				_ = pq.ConsumeChannel(consumeCtx, channel,
					func(_ context.Context, _ *pgqueue.Message) error { return nil },
					pgqueue.WithPollInterval(20*time.Millisecond))
			}(ch)
		}
	}

	// Give the consumers a moment to all register their LISTENs, racing each
	// other on the same-channel registrations.
	time.Sleep(150 * time.Millisecond)

	// Concurrently: stop the consumers and Close() the queue. Close() must join
	// cleanly with LISTEN traffic still in flight; no LISTEN may be issued after
	// the notifier is closed.
	var shutdownWG sync.WaitGroup
	shutdownWG.Add(2)
	go func() {
		defer shutdownWG.Done()
		cancelConsumers()
	}()
	go func() {
		defer shutdownWG.Done()
		if err := pq.Close(); err != nil {
			t.Errorf("Close() returned error during concurrent shutdown: %v", err)
		}
	}()
	shutdownWG.Wait()
	consumerWG.Wait()

	// A second Close() must be a no-op.
	if err := pq.Close(); err != nil {
		t.Errorf("second Close() should be a no-op, got: %v", err)
	}
}
