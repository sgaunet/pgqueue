package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/sgaunet/pgqueue"
)

// TestCreateQueue_MaxQueuesLimit verifies that Config.MaxQueues caps the total
// number of queues (channels and topics combined) that can be created.
func TestCreateQueue_MaxQueuesLimit(t *testing.T) {
	_, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	const maxQueues = 3

	pq, err := pgqueue.New(ctx, db,
		pgqueue.WithMaxMessageSize(testMaxMessageSize),
		pgqueue.WithDefaultMaxRetries(testDefaultMaxRetries),
		pgqueue.WithMaxQueues(maxQueues),
	)
	if err != nil {
		t.Fatalf("failed to init pgqueue: %v", err)
	}

	// Fill the quota: two channels and one topic = 3 queues.
	for i := range 2 {
		name := fmt.Sprintf("max-ch-%d", i)
		if err := pq.CreateChannel(ctx, name); err != nil {
			t.Fatalf("CreateChannel(%s) failed within quota: %v", name, err)
		}
	}
	if err := pq.CreateTopic(ctx, "max-topic-0"); err != nil {
		t.Fatalf("CreateTopic failed within quota: %v", err)
	}

	// The next creation must be rejected.
	err = pq.CreateChannel(ctx, "max-ch-overflow")
	if !errors.Is(err, pgqueue.ErrMaxQueuesReached) {
		t.Fatalf("expected ErrMaxQueuesReached, got %v", err)
	}

	// A topic over the limit is rejected too.
	err = pq.CreateTopic(ctx, "max-topic-overflow")
	if !errors.Is(err, pgqueue.ErrMaxQueuesReached) {
		t.Fatalf("expected ErrMaxQueuesReached for topic, got %v", err)
	}

	// Freeing a slot allows creation again.
	if err := pq.DeleteChannel(ctx, "max-ch-0", true); err != nil {
		t.Fatalf("DeleteChannel failed: %v", err)
	}
	if err := pq.CreateChannel(ctx, "max-ch-after-delete"); err != nil {
		t.Fatalf("CreateChannel after freeing a slot failed: %v", err)
	}
}

// TestCreateQueue_MaxQueuesUnlimited verifies that the default (MaxQueues = 0)
// imposes no limit.
func TestCreateQueue_MaxQueuesUnlimited(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	for i := range 10 {
		name := fmt.Sprintf("unlimited-ch-%d", i)
		if err := pq.CreateChannel(ctx, name); err != nil {
			t.Fatalf("CreateChannel(%s) failed with unlimited config: %v", name, err)
		}
	}
}
