package integration_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sgaunet/pgqueue"
)

// TestConsumeChannelHandlerRetryAfter verifies that a Handler used with
// ConsumeChannel can pin the redelivery delay for one message by returning
// pgqueue.RetryAfter(d, cause) instead of a plain error: the message must be
// redelivered after approximately d, not the channel's configured
// BackoffPolicy — and, once the channel's max-retries is exceeded, the
// message must still land in the DLQ exactly as it would for a plain
// returned error, proving RetryAfter counts as a nack toward max-retries.
func TestConsumeChannelHandlerRetryAfter(t *testing.T) {
	db, containerCleanup := setupTestContainer(t)
	defer containerCleanup()

	ctx := context.Background()
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// The queue's own BackoffPolicy is set far larger than retryAfterDelay
	// below so redelivery timing can distinguish "RetryAfter's delay was
	// honored" from "the queue's default backoff was used instead" (which
	// would indicate dispatchToHandler failed to detect the *retryAfterError
	// and fell back to the plain-error path).
	pq, err := pgqueue.New(ctx, db, pgqueue.WithBackoffPolicy(pgqueue.BackoffPolicy{
		BaseDelay:  3 * time.Second,
		MaxDelay:   3 * time.Second,
		Multiplier: 1,
	}))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() { _ = pq.Close() }()

	const (
		channelName     = "handler-retry-after"
		maxRetries      = 2 // total deliveries before DLQ = maxRetries+1 = 3
		retryAfterDelay = 400 * time.Millisecond
	)
	if err := pq.CreateChannel(ctx, channelName,
		pgqueue.WithQueueMaxRetries(maxRetries)); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	if _, err := pq.Publish(ctx, channelName, []byte("rate-limited")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	cause := errors.New("simulated rate limit: retry after backoff")

	var (
		mu         sync.Mutex
		deliveries []time.Time
		allSeen    = make(chan struct{})
	)
	var closeOnce sync.Once

	handler := func(_ context.Context, _ *pgqueue.Message) error {
		mu.Lock()
		deliveries = append(deliveries, time.Now())
		n := len(deliveries)
		mu.Unlock()

		if n > maxRetries+1 {
			// The (maxRetries+1)-th nack must have moved the message to the
			// DLQ; a further delivery means it was wrongly redelivered
			// instead.
			t.Errorf("handler invoked a %dth time; expected DLQ after %d deliveries", n, maxRetries+1)
			return cause
		}
		if n == maxRetries+1 {
			closeOnce.Do(func() { close(allSeen) })
		}
		return pgqueue.RetryAfter(retryAfterDelay, cause)
	}

	consumeCtx, cancel := context.WithCancel(ctx)
	loopDone := make(chan error, 1)
	go func() {
		loopDone <- pq.ConsumeChannel(consumeCtx, channelName, handler,
			pgqueue.WithPollInterval(50*time.Millisecond))
	}()

	waitClosed(t, allSeen, "message was not delivered maxRetries+1 times")

	// The final nack (retryCount+1 > maxRetries) moves the message to the
	// DLQ; give it a moment to land.
	eventually(t, 2*time.Second, 50*time.Millisecond, func() bool {
		msgs, _, err := pq.ListDLQMessages(ctx, channelName, pgqueue.QueueTypeChannel,
			pgqueue.DLQPage{Limit: 10})
		return err == nil && len(msgs) == 1
	}, "message did not land in the DLQ after exceeding max_retries")

	cancel()
	select {
	case err := <-loopDone:
		if err != nil {
			t.Fatalf("ConsumeChannel returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ConsumeChannel did not return within 3s of context cancellation")
	}

	// Verify the DLQ entry: it must exist exactly once, carry the composed
	// RetryAfter failure reason (proving the wrapped cause survived into the
	// recorded reason), and record all maxRetries+1 delivery attempts.
	msgs, _, err := pq.ListDLQMessages(ctx, channelName, pgqueue.QueueTypeChannel,
		pgqueue.DLQPage{Limit: 10})
	if err != nil {
		t.Fatalf("list DLQ messages: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected exactly 1 DLQ message, got %d", len(msgs))
	}
	if msgs[0].RetryCount != maxRetries+1 {
		t.Errorf("expected DLQ retry_count == %d, got %d", maxRetries+1, msgs[0].RetryCount)
	}
	if !strings.Contains(msgs[0].FailureReason, cause.Error()) {
		t.Errorf("DLQ failure_reason %q does not contain the wrapped cause %q",
			msgs[0].FailureReason, cause.Error())
	}

	// Verify redelivery timing: each gap between successive deliveries must
	// be close to retryAfterDelay (400ms) and well below the queue's
	// configured 3s BackoffPolicy — proving RetryAfter's delay was actually
	// applied rather than ignored (near-zero gap) or falling back to the
	// queue's own backoff (~3s gap).
	mu.Lock()
	defer mu.Unlock()
	if len(deliveries) < maxRetries+1 {
		t.Fatalf("expected %d deliveries, got %d", maxRetries+1, len(deliveries))
	}
	for i := 1; i < len(deliveries); i++ {
		gap := deliveries[i].Sub(deliveries[i-1])
		if gap < retryAfterDelay/2 {
			t.Errorf("delivery %d gap %v too short; RetryAfter's delay (%v) does not appear to have been honored",
				i, gap, retryAfterDelay)
		}
		if gap >= 2*time.Second {
			t.Errorf("delivery %d gap %v too close to the queue's 3s BackoffPolicy; RetryAfter's delay was not honored",
				i, gap)
		}
	}
}
