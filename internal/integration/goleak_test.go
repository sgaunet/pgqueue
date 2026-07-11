package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/sgaunet/pgqueue"
	"go.uber.org/goleak"
)

// TestNoGoroutineLeaks exercises the publish → handler-consume → GC → Close
// lifecycle and asserts pgqueue leaks no goroutines across it (M10/FR-027,
// SC-010).
//
// goleak.IgnoreCurrent() is captured right after the queue is built, so the
// testcontainers and database/sql+pgx background goroutines already running are
// ignored — the check targets goroutines pgqueue itself spawns (consume workers,
// the GC loop, the notifier) and must join on Close. Defers run LIFO, so the
// order is: pq.Close() (joins pgqueue's goroutines) → VerifyNone (asserts none
// survived) → container teardown (which would otherwise add transient goroutines
// the check must not see).
func TestNoGoroutineLeaks(t *testing.T) {
	ctx := context.Background()

	db, containerCleanup := setupTestContainer(t)
	defer containerCleanup() // registered first → runs LAST, after the leak check

	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	pq, err := pgqueue.New(ctx, db,
		pgqueue.WithMaxMessageSize(testMaxMessageSize),
		pgqueue.WithDefaultMaxRetries(testDefaultMaxRetries),
	)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	// Snapshot the goroutine set now (container + driver already up, so ignored),
	// then assert no NEW goroutine survives Close. Registered before the Close
	// defer so it runs AFTER Close but before container teardown.
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	defer func() { _ = pq.Close() }()

	if err := pq.CreateChannel(ctx, "leak"); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	for i := range 20 {
		if _, perr := pq.Publish(ctx, "leak", []byte{byte('a' + i)}); perr != nil {
			t.Fatalf("publish: %v", perr)
		}
	}

	// A concurrent handler loop (4 workers) and a fast-ticking GC, both of which
	// Close/Stop must join cleanly.
	consumeCtx, stopConsume := context.WithCancel(ctx)
	consumeErr := make(chan error, 1)
	go func() {
		consumeErr <- pq.ConsumeChannel(consumeCtx, "leak",
			func(context.Context, *pgqueue.Message) error { return nil },
			pgqueue.WithConcurrency(4))
	}()

	gc, err := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{
		Interval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new gc: %v", err)
	}
	gc.Start(ctx)

	// Wait for the workers to drain the queue and the GC to tick a few times.
	eventually(t, 15*time.Second, 20*time.Millisecond, func() bool {
		stats, serr := pq.Stats(ctx, "leak", pgqueue.QueueTypeChannel)
		return serr == nil && stats.PendingCount == 0
	}, "messages were not all consumed")

	// Wind the loop and GC down explicitly; the deferred pq.Close() is the backstop.
	gc.Stop()
	stopConsume()
	if cerr := <-consumeErr; cerr != nil {
		t.Fatalf("consume returned error: %v", cerr)
	}
}
