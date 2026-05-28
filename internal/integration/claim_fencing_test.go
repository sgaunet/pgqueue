package integration_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sgaunet/pgqueue"
)

// TestClaimFencing verifies a consumer whose visibility timeout lapsed cannot
// ack or nack a message that was redelivered to another consumer.
func TestClaimFencing(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("channel_stale_ack_rejected", func(t *testing.T) {
		if err := pq.CreateChannel(ctx, "fence-chan"); err != nil {
			t.Fatalf("create channel: %v", err)
		}
		if _, err := pq.Publish(ctx, "fence-chan", []byte("payload")); err != nil {
			t.Fatalf("publish: %v", err)
		}
		msgA, err := pq.ReceiveChannel(ctx, "fence-chan", pgqueue.WithVisibilityTimeout(50*time.Millisecond))
		if err != nil || msgA == nil {
			t.Fatalf("consumer A consume: msg=%v err=%v", msgA, err)
		}
		time.Sleep(100 * time.Millisecond) // let A's visibility timeout lapse
		msgB, err := pq.ReceiveChannel(ctx, "fence-chan", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil || msgB == nil {
			t.Fatalf("consumer B reclaim: msg=%v err=%v", msgB, err)
		}
		if msgA.ClaimID == msgB.ClaimID {
			t.Fatal("expected B to receive a fresh claim token")
		}
		if err := pq.Ack(ctx, msgA.Receipt()); !errors.Is(err, pgqueue.ErrClaimExpired) {
			t.Errorf("stale Ack: got %v, want ErrClaimExpired", err)
		}
		if err := pq.Ack(ctx, msgB.Receipt()); err != nil {
			t.Errorf("current Ack: %v", err)
		}
	})

	t.Run("channel_stale_nack_rejected", func(t *testing.T) {
		if err := pq.CreateChannel(ctx, "fence-chan-n"); err != nil {
			t.Fatalf("create channel: %v", err)
		}
		if _, err := pq.Publish(ctx, "fence-chan-n", []byte("payload")); err != nil {
			t.Fatalf("publish: %v", err)
		}
		msgA, err := pq.ReceiveChannel(ctx, "fence-chan-n", pgqueue.WithVisibilityTimeout(50*time.Millisecond))
		if err != nil || msgA == nil {
			t.Fatalf("consumer A consume: msg=%v err=%v", msgA, err)
		}
		time.Sleep(100 * time.Millisecond)
		msgB, err := pq.ReceiveChannel(ctx, "fence-chan-n", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil || msgB == nil {
			t.Fatalf("consumer B reclaim: msg=%v err=%v", msgB, err)
		}
		if err := pq.Nack(ctx, msgA.Receipt(), "stale"); !errors.Is(err, pgqueue.ErrClaimExpired) {
			t.Errorf("stale Nack: got %v, want ErrClaimExpired", err)
		}
		if err := pq.Ack(ctx, msgB.Receipt()); err != nil {
			t.Errorf("current Ack: %v", err)
		}
	})

	t.Run("topic_stale_ack_rejected", func(t *testing.T) {
		if err := pq.CreateTopic(ctx, "fence-topic"); err != nil {
			t.Fatalf("create topic: %v", err)
		}
		if err := pq.Subscribe(ctx, "fence-topic", "sub-1"); err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		if _, err := pq.Publish(ctx, "fence-topic", []byte("payload")); err != nil {
			t.Fatalf("publish: %v", err)
		}
		msgA, err := pq.ReceiveTopic(ctx, "fence-topic", "sub-1", pgqueue.WithVisibilityTimeout(50*time.Millisecond))
		if err != nil || msgA == nil {
			t.Fatalf("consumer A consume: msg=%v err=%v", msgA, err)
		}
		time.Sleep(100 * time.Millisecond)
		msgB, err := pq.ReceiveTopic(ctx, "fence-topic", "sub-1", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil || msgB == nil {
			t.Fatalf("consumer B reclaim: msg=%v err=%v", msgB, err)
		}
		if msgA.ClaimID == msgB.ClaimID {
			t.Fatal("expected B to receive a fresh claim token")
		}
		if err := pq.Ack(ctx, msgA.Receipt()); !errors.Is(err, pgqueue.ErrClaimExpired) {
			t.Errorf("stale Ack: got %v, want ErrClaimExpired", err)
		}
		if err := pq.Ack(ctx, msgB.Receipt()); err != nil {
			t.Errorf("current Ack: %v", err)
		}
	})
}

// TestAckMissClassificationStable (R-09) verifies that acking an already-acked
// channel message while a concurrent reclaim races is classified consistently:
// the ack-miss error does not flap between error types across repeated runs.
// The AckChannel UPDATE and the classification SELECT must observe a single
// consistent snapshot (one transaction), so the same sentinel is returned every
// time.
//
// Run under -race.
func TestAckMissClassificationStable(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	const channelName = "ack-miss-classify"
	if err := pq.CreateChannel(ctx, channelName); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// Each iteration: publish, consume, ack (success), then concurrently fire a
	// second (stale) ack while a reclaim attempt races. Record the classified
	// error so we can assert it never flaps.
	const runs = 20
	classifications := make(map[string]int)

	for i := range runs {
		if _, err := pq.Publish(ctx, channelName, []byte("payload")); err != nil {
			t.Fatalf("run %d: publish: %v", i, err)
		}
		msg, err := pq.ReceiveChannel(ctx, channelName, pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil || msg == nil {
			t.Fatalf("run %d: consume: msg=%v err=%v", i, msg, err)
		}
		// First ack succeeds.
		if err := pq.Ack(ctx, msg.Receipt()); err != nil {
			t.Fatalf("run %d: first ack: %v", i, err)
		}

		// Now race a stale re-ack against a reclaim attempt on the (already
		// completed) message.
		var (
			wg       sync.WaitGroup
			staleErr error
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			staleErr = pq.Ack(ctx, msg.Receipt())
		}()
		go func() {
			defer wg.Done()
			// A reclaim attempt: there is nothing pending, so this just races
			// the ack-miss classification path.
			_, _ = pq.ReceiveChannel(ctx, channelName, pgqueue.WithVisibilityTimeout(30*time.Second))
		}()
		wg.Wait()

		if staleErr == nil {
			t.Fatalf("run %d: stale re-ack unexpectedly succeeded", i)
		}
		// The stale ack must resolve to a known, stable sentinel.
		switch {
		case errors.Is(staleErr, pgqueue.ErrMessageAlreadyAcked):
			classifications["already-acked"]++
		case errors.Is(staleErr, pgqueue.ErrClaimExpired):
			classifications["claim-expired"]++
		case errors.Is(staleErr, pgqueue.ErrMessageNotFound):
			classifications["not-found"]++
		default:
			t.Fatalf("run %d: stale ack returned an unclassified error: %v", i, staleErr)
		}
	}

	// The classification must be stable: every run resolved to the same bucket.
	if len(classifications) != 1 {
		t.Errorf("ack-miss classification flapped across runs: %v", classifications)
	}
}

// TestAckTopicMissClassificationStable is the pub/sub counterpart of
// TestAckMissClassificationStable. AckTopic runs its UPDATE and the classifying
// SELECT in one transaction (R-09), so a concurrent reclaim cannot flip the
// classified error type between the two statements.
func TestAckTopicMissClassificationStable(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	const topicName = "ack-miss-classify-topic"
	const subID = "sub-1"
	if err := pq.CreateTopic(ctx, topicName); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := pq.Subscribe(ctx, topicName, subID); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	const runs = 20
	classifications := make(map[string]int)

	for i := range runs {
		if _, err := pq.Publish(ctx, topicName, []byte("payload")); err != nil {
			t.Fatalf("run %d: publish: %v", i, err)
		}
		msg, err := pq.ReceiveTopic(ctx, topicName, subID, pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil || msg == nil {
			t.Fatalf("run %d: consume: msg=%v err=%v", i, msg, err)
		}
		// First ack succeeds.
		if err := pq.Ack(ctx, msg.Receipt()); err != nil {
			t.Fatalf("run %d: first ack: %v", i, err)
		}

		// Race a stale re-ack against a reclaim attempt on the acked message.
		var (
			wg       sync.WaitGroup
			staleErr error
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			staleErr = pq.Ack(ctx, msg.Receipt())
		}()
		go func() {
			defer wg.Done()
			_, _ = pq.ReceiveTopic(ctx, topicName, subID, pgqueue.WithVisibilityTimeout(30*time.Second))
		}()
		wg.Wait()

		if staleErr == nil {
			t.Fatalf("run %d: stale re-ack unexpectedly succeeded", i)
		}
		switch {
		case errors.Is(staleErr, pgqueue.ErrMessageAlreadyAcked):
			classifications["already-acked"]++
		case errors.Is(staleErr, pgqueue.ErrClaimExpired):
			classifications["claim-expired"]++
		case errors.Is(staleErr, pgqueue.ErrMessageNotFound):
			classifications["not-found"]++
		default:
			t.Fatalf("run %d: stale ack returned an unclassified error: %v", i, staleErr)
		}
	}

	if len(classifications) != 1 {
		t.Errorf("ack-miss classification flapped across runs: %v", classifications)
	}
}
