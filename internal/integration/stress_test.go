package integration_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sgaunet/pgqueue"
)

// TestT024_StressWorkloadZeroLossZeroDuplication runs a concurrent publish/consume/
// ack/nack/GC/replay workload and asserts that every published message is eventually
// consumed+acked exactly once (zero lost, zero duplicated).
//
// Strategy:
//  1. Publish N messages, recording their UUIDs.
//  2. Run M consumer goroutines that consume, occasionally nack, and ack.
//  3. Run a GC goroutine that resets timed-out messages periodically.
//  4. Once all N messages are acked, collect the set of acked IDs and assert
//     it equals the set of published IDs (no loss, no dup).
func TestT024_StressWorkloadZeroLossZeroDuplication(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const (
		queueName   = "t024-stress"
		numMessages = 50
		numConsumers = 5
		maxRetries  = 5
	)

	if err := pq.CreateChannel(ctx, queueName, pgqueue.WithQueueMaxRetries(maxRetries)); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish all messages and record their IDs
	publishedIDs := make(map[uuid.UUID]struct{}, numMessages)
	var publishedMu sync.Mutex

	for i := range numMessages {
		payload := []byte{byte(i)}
		id, err := pq.Publish(ctx, queueName, payload)
		if err != nil {
			t.Fatalf("publish %d failed: %v", i, err)
		}
		publishedMu.Lock()
		publishedIDs[id] = struct{}{}
		publishedMu.Unlock()
	}

	// Track acked messages
	ackedIDs := make(map[uuid.UUID]struct{}, numMessages)
	var ackedMu sync.Mutex
	var ackedCount atomic.Int64

	// Start GC goroutine
	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{
		Interval: 50 * time.Millisecond,
	})
	gc.Start(ctx)
	defer gc.Stop()

	// Start consumer goroutines
	var consumerWg sync.WaitGroup
	for range numConsumers {
		consumerWg.Add(1)
		go func() {
			defer consumerWg.Done()
			for {
				if ackedCount.Load() >= numMessages {
					return
				}
				if ctx.Err() != nil {
					return
				}

				msg, err := pq.ReceiveChannel(ctx, queueName, pgqueue.WithVisibilityTimeout(200*time.Millisecond))
				if err != nil || msg == nil {
					// intentional: back-off between empty-queue polls in the stress
					// consumer to avoid saturating the DB connection pool.
					time.Sleep(10 * time.Millisecond)
					continue
				}

				// Occasionally nack to test retry path (every 5th message by retry count)
				if msg.RetryCount == 0 && msg.ID[15]%5 == 0 {
					_ = pq.Nack(ctx, msg.Receipt(), "stress-nack")
					continue
				}

				if err := pq.Ack(ctx, msg.Receipt()); err != nil {
					// Stale claim: another consumer got it first, skip
					continue
				}

				ackedMu.Lock()
				if _, dup := ackedIDs[msg.ID]; dup {
					ackedMu.Unlock()
					t.Errorf("DUPLICATION: message %s acked twice", msg.ID)
					return
				}
				ackedIDs[msg.ID] = struct{}{}
				ackedMu.Unlock()
				ackedCount.Add(1)
			}
		}()
	}

	// Wait for all messages to be acked or context to expire
	deadline := time.NewTimer(90 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline.C:
			t.Fatalf("timed out: only %d/%d messages acked", ackedCount.Load(), numMessages)
		case <-ctx.Done():
			t.Fatalf("context cancelled: only %d/%d messages acked", ackedCount.Load(), numMessages)
		case <-ticker.C:
			if ackedCount.Load() >= numMessages {
				goto done
			}
		}
	}
done:
	consumerWg.Wait()

	// Verify: every published ID was acked exactly once
	ackedMu.Lock()
	defer ackedMu.Unlock()

	publishedMu.Lock()
	defer publishedMu.Unlock()

	for id := range publishedIDs {
		if _, ok := ackedIDs[id]; !ok {
			t.Errorf("LOSS: published message %s was never acked", id)
		}
	}
	for id := range ackedIDs {
		if _, ok := publishedIDs[id]; !ok {
			t.Errorf("PHANTOM: acked message %s was never published", id)
		}
	}

	if len(ackedIDs) != numMessages {
		t.Errorf("acked %d messages, published %d", len(ackedIDs), numMessages)
	}
}
