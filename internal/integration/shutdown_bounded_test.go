package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sgaunet/pgqueue"
)

// TestWithoutCancelBounded exercises the grace-bounded, cancellation-detached
// cleanup paths (M11/FR-026, SC-011). DeleteChannel runs its post-commit cache
// and LISTEN invalidation on a context detached from the caller's cancellation
// but bounded by the cleanup grace period, so it must complete promptly and
// leave the queue actually gone — never hang. InitSchema's advisory-unlock and
// search_path-reset sites use the same bounded-detached context and are
// exercised by every setupTestDB.
//
// True wedged-connection injection is out of scope for the testcontainers
// harness; this guards the happy path against a regression that makes a detached
// cleanup block indefinitely.
func TestWithoutCancelBounded(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	if err := pq.CreateChannel(ctx, "wc"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := pq.Publish(ctx, "wc", []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// DeleteChannel must return within the grace budget (far under this ceiling),
	// not hang on its detached cleanup.
	done := make(chan error, 1)
	go func() { done <- pq.DeleteChannel(ctx, "wc") }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("DeleteChannel: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("DeleteChannel did not return within 15s: a detached cleanup may be hanging (M11)")
	}

	// The detached cache invalidation ran: the queue is gone, so a further publish
	// resolves to ErrQueueNotFound rather than hitting a stale cache entry.
	if _, err := pq.Publish(ctx, "wc", []byte("y")); !errors.Is(err, pgqueue.ErrQueueNotFound) {
		t.Fatalf("after delete, publish should be ErrQueueNotFound, got %v", err)
	}
}
