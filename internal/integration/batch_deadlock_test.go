package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sgaunet/pgqueue"
)

// TestBatchNoDeadlock stresses concurrent batch ack/nack against a shared queue
// and asserts no operation ever surfaces an unhandled deadlock (M2/FR-023,
// SC-009). Overlapping batch operations lock rows in a canonical order
// (receiptsToIDClaimLiterals sorts its unnest arrays; the FOR UPDATE state
// fetches ORDER BY id) and any residual deadlock/serialization failure is
// retried by withBatchRetry, so a 40P01 must never escape to the caller.
//
// Phase A drains the queue with concurrent AckBatch; phase B runs a concurrent
// round of NackBatch. Both share the same batch-locking machinery, so a
// regression in either the ordering or the retry would fail one of them.
func TestBatchNoDeadlock(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	const (
		workers   = 8
		batchSize = 12
		nAck      = 300
		nNack     = 96
	)
	if err := pq.CreateChannel(ctx, "deadlock"); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	publish := func(n int, tag string) {
		msgs := make([]pgqueue.PublishMessage, n)
		for i := range msgs {
			msgs[i] = pgqueue.PublishMessage{Payload: []byte(fmt.Sprintf("%s-%d", tag, i))}
		}
		if _, err := pq.PublishBatch(ctx, "deadlock", msgs); err != nil {
			t.Fatalf("publish batch: %v", err)
		}
	}

	// claimUpTo claims at most batchSize currently-available messages.
	claimUpTo := func() ([]pgqueue.Receipt, error) {
		var receipts []pgqueue.Receipt
		for len(receipts) < batchSize {
			msg, err := pq.ReceiveChannel(ctx, "deadlock", pgqueue.WithVisibilityTimeout(30*time.Second))
			if errors.Is(err, pgqueue.ErrQueueEmpty) {
				break
			}
			if err != nil {
				return receipts, err
			}
			receipts = append(receipts, msg.Receipt())
		}
		return receipts, nil
	}

	var (
		mu       sync.Mutex
		firstErr error
	)
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}

	// ---- Phase A: concurrent AckBatch drain ----
	publish(nAck, "ack")
	var acked atomic.Int64
	deadline := time.Now().Add(60 * time.Second)
	var wgA sync.WaitGroup
	for range workers {
		wgA.Add(1)
		go func() {
			defer wgA.Done()
			for acked.Load() < nAck && time.Now().Before(deadline) {
				receipts, err := claimUpTo()
				if err != nil {
					fail(fmt.Errorf("phase A receive: %w", err))
					return
				}
				if len(receipts) == 0 {
					time.Sleep(time.Millisecond)
					continue
				}
				res, err := pq.AckBatch(ctx, receipts)
				if err != nil {
					fail(fmt.Errorf("phase A AckBatch (possible unretried deadlock): %w", err))
					return
				}
				acked.Add(int64(len(res.Succeeded)))
			}
		}()
	}
	wgA.Wait()
	if firstErr != nil {
		t.Fatalf("%v", firstErr)
	}
	if got := acked.Load(); got != nAck {
		t.Fatalf("phase A: expected %d acked under concurrency, got %d", nAck, got)
	}

	// ---- Phase B: concurrent NackBatch round ----
	// A long retry delay defers each nacked message far past the test window, so
	// every message is claimed and nacked exactly once (no re-queue churn) and the
	// nacked count is deterministic.
	publish(nNack, "nack")
	var nacked atomic.Int64
	deadline = time.Now().Add(60 * time.Second)
	var wgB sync.WaitGroup
	for range workers {
		wgB.Add(1)
		go func() {
			defer wgB.Done()
			for nacked.Load() < nNack && time.Now().Before(deadline) {
				receipts, err := claimUpTo()
				if err != nil {
					fail(fmt.Errorf("phase B receive: %w", err))
					return
				}
				if len(receipts) == 0 {
					time.Sleep(time.Millisecond)
					continue
				}
				res, err := pq.NackBatch(ctx, receipts, "stress",
					pgqueue.WithRetryDelay(time.Hour))
				if err != nil {
					fail(fmt.Errorf("phase B NackBatch (possible unretried deadlock): %w", err))
					return
				}
				nacked.Add(int64(len(res.Succeeded)))
			}
		}()
	}
	wgB.Wait()
	if firstErr != nil {
		t.Fatalf("%v", firstErr)
	}
	if got := nacked.Load(); got != nNack {
		t.Fatalf("phase B: expected %d nacked under concurrency, got %d", nNack, got)
	}
}
