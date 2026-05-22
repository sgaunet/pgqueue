package integration_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sgaunet/pgqueue"
)

// TestGracefulShutdownAcksInFlightMessage is the regression test for the bug
// where Queue.Close marked the Queue closed *before* joining the handler-based
// consume loops, so the automatic ack for a handler that finished during the
// shutdown window was rejected by the closed-state gate in the public Ack.
// The message was left stuck in 'processing' and needlessly redelivered after
// its visibility timeout.
//
// The test drives a handler that is still running when Close is called, lets
// it finish only after Close has marked the Queue closed, and asserts the
// message ends up 'completed' (acked) rather than 'processing'.
func TestGracefulShutdownAcksInFlightMessage(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	const channelName = "shutdown-ack"
	if err := pq.CreateChannel(ctx, channelName); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}
	if _, err := pq.PublishChannel(ctx, channelName, []byte("inflight")); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	var startedOnce sync.Once
	started := make(chan struct{})
	proceed := make(chan struct{})

	handler := func(_ context.Context, _ *pgqueue.Message) error {
		startedOnce.Do(func() { close(started) })
		<-proceed // block until the test has called Close
		return nil // success -> auto-ack
	}

	consumeCtx, cancelConsume := context.WithCancel(ctx)
	defer cancelConsume()
	loopDone := make(chan error, 1)
	go func() {
		loopDone <- pq.ConsumeChannel(consumeCtx, channelName, handler,
			pgqueue.WithConcurrency(1),
			// A long visibility timeout: if the auto-ack were dropped the
			// message would sit in 'processing' for the whole test instead of
			// being redelivered, so the final status check stays unambiguous.
			pgqueue.WithVisibilityTimeout(5*time.Minute),
			pgqueue.WithPollInterval(50*time.Millisecond))
	}()

	// Wait until the handler is actually running.
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}

	// Call Close while the handler is still in-flight. Close marks the Queue
	// closed immediately, then blocks joining the worker loop.
	closeDone := make(chan error, 1)
	go func() { closeDone <- pq.Close() }()

	// Give Close time to mark the Queue closed (it does so before it blocks on
	// the worker WaitGroup). The handler's auto-ack therefore runs against an
	// already-closed Queue — exactly the window the bug lived in.
	time.Sleep(150 * time.Millisecond)

	// Let the handler finish; its auto-ack must still go through.
	close(proceed)

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return within 5s")
	}
	select {
	case err := <-loopDone:
		if err != nil {
			t.Fatalf("ConsumeChannel returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ConsumeChannel loop did not return")
	}

	// The in-flight message must be 'completed' (acked), not stuck 'processing'.
	var status string
	if err := db.QueryRowContext(ctx,
		`SELECT status FROM pgqueue_msg_shutdown_ack`).Scan(&status); err != nil {
		t.Fatalf("failed to read message status: %v", err)
	}
	if status != string(pgqueue.MessageStatusCompleted) {
		t.Fatalf("in-flight message status = %q, want %q "+
			"(auto-ack was dropped during graceful shutdown)",
			status, pgqueue.MessageStatusCompleted)
	}
}

// TestPubSubMaxPendingAgePreservesOtherSubscribers is the regression test for
// the bug where a RetentionPolicy.MaxPendingAge purge on a pub/sub topic
// DELETEd the shared message row whenever any one subscriber had a stale
// pending delivery — cascading to every other subscriber's subscription row,
// including deliveries still being processed.
//
// The fix purges only the stale 'pending' subscription rows and leaves the
// message row (and every non-pending subscription) intact. The test has one
// subscriber mid-processing while another's delivery goes stale, runs the GC,
// and asserts the mid-processing subscriber can still ack and the message row
// survives.
func TestPubSubMaxPendingAgePreservesOtherSubscribers(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	const topicName = "pending-age-isolation"
	if err := pq.CreateTopic(ctx, topicName); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	if err := pq.Subscribe(ctx, topicName, "slow"); err != nil {
		t.Fatalf("failed to subscribe slow: %v", err)
	}
	if err := pq.Subscribe(ctx, topicName, "fast"); err != nil {
		t.Fatalf("failed to subscribe fast: %v", err)
	}

	if _, err := pq.PublishTopic(ctx, topicName, []byte("shared")); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// "fast" consumes the message and is left mid-processing (a long visibility
	// timeout keeps its subscription row in 'processing'); it has NOT acked.
	// "slow" never consumes, so its subscription row stays 'pending'.
	fastMsg, err := pq.ReceiveTopic(ctx, topicName, "fast",
		pgqueue.WithVisibilityTimeout(5*time.Minute))
	if err != nil {
		t.Fatalf("fast subscriber failed to receive: %v", err)
	}

	// Let the message age past the (tiny) MaxPendingAge cutoff.
	time.Sleep(30 * time.Millisecond)

	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{
		DefaultPolicy: pgqueue.RetentionPolicy{
			MaxPendingAge: time.Millisecond,
		},
	})
	if err := gc.Collect(ctx); err != nil {
		t.Fatalf("GC Collect failed: %v", err)
	}

	// The fast subscriber's in-flight delivery survived the purge and can still
	// be acked. Under the old behaviour the whole message row was deleted and
	// cascaded to this 'processing' row, so the ack would fail.
	if err := pq.Ack(ctx, fastMsg.Receipt()); err != nil {
		t.Fatalf("fast subscriber could not ack after GC; its in-flight "+
			"delivery was destroyed by the MaxPendingAge purge: %v", err)
	}

	// The shared message row still exists.
	var msgCount int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pgqueue_msg_pending_age_isolation`).Scan(&msgCount); err != nil {
		t.Fatalf("failed to count messages: %v", err)
	}
	if msgCount != 1 {
		t.Fatalf("shared message row count = %d, want 1 "+
			"(message row destroyed by MaxPendingAge purge)", msgCount)
	}

	// The slow subscriber's stale pending delivery was the only thing reclaimed.
	var slowPending int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pgqueue_sub_pending_age_isolation
		 WHERE subscriber_id = $1 AND status = 'pending'`,
		"slow").Scan(&slowPending); err != nil {
		t.Fatalf("failed to count slow subscriber rows: %v", err)
	}
	if slowPending != 0 {
		t.Fatalf("slow subscriber pending rows = %d, want 0 "+
			"(stale delivery was not reclaimed)", slowPending)
	}
}
