package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sgaunet/pgqueue/pkg/pgqueue"
)

// TestNackBackoffDelaysRedelivery verifies that a nacked message is not
// redelivered until its automatic backoff delay has elapsed, and that
// WithRetryDelay overrides that delay per nack (FR-023).
func TestNackBackoffDelaysRedelivery(t *testing.T) {
	db, containerCleanup := setupTestContainer(t)
	defer containerCleanup()

	ctx := context.Background()
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// A fast, deterministic backoff so the test does not wait minutes: every
	// retry waits between 800ms and 1s.
	pq, err := pgqueue.New(ctx, db, pgqueue.WithBackoffPolicy(pgqueue.BackoffPolicy{
		BaseDelay:  800 * time.Millisecond,
		MaxDelay:   1 * time.Second,
		Multiplier: 1,
	}))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() { _ = pq.Close() }()

	const channelName = "backoff-channel"
	if err := pq.CreateChannel(ctx, channelName); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// --- automatic backoff ---
	if _, err := pq.PublishChannel(ctx, channelName, []byte("retry-me")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	msg, err := pq.ReceiveChannel(ctx, channelName)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if err := pq.Nack(ctx, msg.Receipt(), "transient failure"); err != nil {
		t.Fatalf("nack: %v", err)
	}

	// Immediately after the nack the message must NOT be redelivered.
	if _, err := pq.ReceiveChannel(ctx, channelName); !errors.Is(err, pgqueue.ErrQueueEmpty) {
		t.Fatalf("message redelivered before backoff elapsed: err=%v", err)
	}

	// After the backoff window it must become available again.
	time.Sleep(1300 * time.Millisecond)
	if _, err := pq.ReceiveChannel(ctx, channelName); err != nil {
		t.Fatalf("message not redelivered after backoff: %v", err)
	}

	// --- WithRetryDelay override ---
	if _, err := pq.PublishChannel(ctx, channelName, []byte("override-me")); err != nil {
		t.Fatalf("publish override: %v", err)
	}
	om, err := pq.ReceiveChannel(ctx, channelName)
	if err != nil {
		t.Fatalf("receive override: %v", err)
	}
	// A long explicit delay must hold the message back well past the default.
	if err := pq.Nack(ctx, om.Receipt(), "rate limited",
		pgqueue.WithRetryDelay(10*time.Second)); err != nil {
		t.Fatalf("nack with retry delay: %v", err)
	}
	time.Sleep(1300 * time.Millisecond) // longer than the default backoff
	if _, err := pq.ReceiveChannel(ctx, channelName); !errors.Is(err, pgqueue.ErrQueueEmpty) {
		t.Fatalf("WithRetryDelay override was ignored: err=%v", err)
	}
}

// TestTimeoutReclaimAppliesBackoff (R-05) verifies that a message reclaimed
// after its visibility timeout lapsed — i.e. its consumer crashed without
// acking — is not redelivered immediately: the configured retry backoff is
// applied to the timeout-reclaim path just as it is to an explicit Nack, and
// retry_count increments exactly once.
//
// Run under -race: the crashed-consumer helper claims the message; no sibling
// goroutine acks it.
func TestTimeoutReclaimAppliesBackoff(t *testing.T) {
	db, containerCleanup := setupTestContainer(t)
	defer containerCleanup()

	ctx := context.Background()
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// A real (non-negligible) backoff so the test can observe the delay window.
	pq, err := pgqueue.New(ctx, db, pgqueue.WithBackoffPolicy(pgqueue.BackoffPolicy{
		BaseDelay:  2 * time.Second,
		MaxDelay:   2 * time.Second,
		Multiplier: 1,
	}))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() { _ = pq.Close() }()

	const channelName = "reclaim-backoff"
	if err := pq.CreateChannel(ctx, channelName); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	if _, err := pq.PublishChannel(ctx, channelName, []byte("crash-me")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// A "crashed" consumer claims the message with a short visibility timeout
	// and never acks it.
	claimed := crashedConsumerClaim(t, pq, channelName, crashedConsumerTimeout)

	// Wait for the visibility timeout to lapse so the message is reclaim-eligible.
	time.Sleep(crashedConsumerTimeout + 100*time.Millisecond)

	// Immediately after the timeout lapses, the backoff must still hold the
	// message back: it is not yet redelivered.
	if _, err := pq.ReceiveChannel(ctx, channelName); !errors.Is(err, pgqueue.ErrQueueEmpty) {
		t.Fatalf("message redelivered before backoff elapsed: err=%v", err)
	}

	// After the backoff window the message becomes available again.
	time.Sleep(2500 * time.Millisecond)
	redelivered, err := pq.ReceiveChannel(ctx, channelName, pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("message not redelivered after backoff: %v", err)
	}
	if redelivered == nil {
		t.Fatal("expected redelivered message, got nil")
	}
	if redelivered.ID != claimed.ID {
		t.Errorf("redelivered a different message: got %s, want %s", redelivered.ID, claimed.ID)
	}

	// retry_count must have incremented exactly once for the single reclaim.
	if redelivered.RetryCount != 1 {
		t.Errorf("expected retry_count == 1 after one timeout-reclaim, got %d", redelivered.RetryCount)
	}
}
