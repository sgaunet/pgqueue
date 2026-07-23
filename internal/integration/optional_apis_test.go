package integration_test

import (
	"context"
	"errors"
	"testing"

	"github.com/sgaunet/pgqueue"
)

// TestListSubscribers exercises the new Queue.ListSubscribers admin API: it
// returns active subscriber IDs in registration order, excludes unsubscribed
// ones, distinguishes an empty topic from a missing one, and reports
// ErrTopicNotFound for a topic that does not exist.
func TestListSubscribers(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := pq.ListSubscribers(ctx, "no-such-topic"); !errors.Is(err, pgqueue.ErrTopicNotFound) {
		t.Fatalf("ListSubscribers on missing topic: got %v, want ErrTopicNotFound", err)
	}

	if err := pq.CreateTopic(ctx, "list-subs"); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	subs, err := pq.ListSubscribers(ctx, "list-subs")
	if err != nil {
		t.Fatalf("ListSubscribers (empty): %v", err)
	}
	if len(subs) != 0 {
		t.Errorf("expected 0 subscribers on a fresh topic, got %v", subs)
	}

	// Names are alphabetical in registration order, so the expected result holds
	// whether the ORDER BY breaks a same-timestamp tie by created_at or by
	// subscriber_id.
	for _, id := range []string{"alpha", "bravo", "charlie"} {
		if err := pq.Subscribe(ctx, "list-subs", id); err != nil {
			t.Fatalf("Subscribe(%s): %v", id, err)
		}
	}

	subs, err = pq.ListSubscribers(ctx, "list-subs")
	if err != nil {
		t.Fatalf("ListSubscribers: %v", err)
	}
	if want := []string{"alpha", "bravo", "charlie"}; !equalStrings(subs, want) {
		t.Fatalf("ListSubscribers = %v, want %v", subs, want)
	}

	if err := pq.Unsubscribe(ctx, "list-subs", "bravo"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	subs, err = pq.ListSubscribers(ctx, "list-subs")
	if err != nil {
		t.Fatalf("ListSubscribers after unsubscribe: %v", err)
	}
	if want := []string{"alpha", "charlie"}; !equalStrings(subs, want) {
		t.Errorf("after unsubscribe ListSubscribers = %v, want %v", subs, want)
	}
}

// TestQueuePurgeQueue exercises the new Queue.PurgeQueue forwarder: it empties a
// queue's messages, leaves the queue and its tables in place (re-publish works),
// and reports ErrQueueNotFound for a missing queue.
func TestQueuePurgeQueue(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	if err := pq.PurgeQueue(ctx, "no-such", pgqueue.QueueTypeChannel); !errors.Is(err, pgqueue.ErrQueueNotFound) {
		t.Fatalf("PurgeQueue on missing queue: got %v, want ErrQueueNotFound", err)
	}

	if err := pq.CreateChannel(ctx, "purge-me"); err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	for range 5 {
		if _, err := pq.Publish(ctx, "purge-me", []byte("x")); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	if err := pq.PurgeQueue(ctx, "purge-me", pgqueue.QueueTypeChannel); err != nil {
		t.Fatalf("PurgeQueue: %v", err)
	}

	stats, err := pq.Stats(ctx, "purge-me", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.PendingCount != 0 {
		t.Errorf("after purge PendingCount = %d, want 0", stats.PendingCount)
	}

	// The queue and its tables survive the purge — re-publishing succeeds.
	if _, err := pq.Publish(ctx, "purge-me", []byte("y")); err != nil {
		t.Errorf("Publish after purge (queue should still exist): %v", err)
	}
}

// TestReceiveChannelMissingIsErrChannelNotFound proves the channel not-found
// path returns the new ErrChannelNotFound sentinel while still matching the
// general ErrQueueNotFound.
func TestReceiveChannelMissingIsErrChannelNotFound(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	_, err := pq.ReceiveChannel(ctx, "ghost")
	if !errors.Is(err, pgqueue.ErrChannelNotFound) {
		t.Errorf("ReceiveChannel on missing channel: got %v, want ErrChannelNotFound", err)
	}
	if !errors.Is(err, pgqueue.ErrQueueNotFound) {
		t.Errorf("ReceiveChannel on missing channel should also match ErrQueueNotFound: %v", err)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
