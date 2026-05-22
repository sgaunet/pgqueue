package integration_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sgaunet/pgqueue"
)

// TestConsumeChannelHandlerAutoAck verifies the handler-based ConsumeChannel
// auto-acks when the handler returns nil and auto-nacks (retrying) when it
// returns an error, and that context cancellation stops the loop promptly with
// no leaked goroutines.
func TestConsumeChannelHandlerAutoAck(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	const channelName = "consume-autoack"
	if err := pq.CreateChannel(ctx, channelName); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// okMsg always succeeds; failMsg fails on its first delivery then succeeds.
	if _, err := pq.PublishChannel(ctx, channelName, []byte("ok")); err != nil {
		t.Fatalf("failed to publish ok message: %v", err)
	}
	if _, err := pq.PublishChannel(ctx, channelName, []byte("fail-once")); err != nil {
		t.Fatalf("failed to publish fail message: %v", err)
	}

	goroutinesBefore := runtime.NumGoroutine()

	var (
		mu         sync.Mutex
		seen       = map[string]int{}
		okDone     = make(chan struct{})
		failRetried = make(chan struct{})
	)
	closeOnce := func(ch chan struct{}) func() {
		var once sync.Once
		return func() { once.Do(func() { close(ch) }) }
	}
	signalOK := closeOnce(okDone)
	signalRetry := closeOnce(failRetried)

	handler := func(_ context.Context, msg *pgqueue.Message) error {
		body := string(msg.Payload)
		mu.Lock()
		seen[body]++
		count := seen[body]
		mu.Unlock()

		switch body {
		case "ok":
			signalOK()
			return nil
		case "fail-once":
			if count == 1 {
				return errors.New("simulated failure")
			}
			signalRetry()
			return nil
		}
		return nil
	}

	consumeCtx, cancel := context.WithCancel(ctx)
	loopDone := make(chan error, 1)
	go func() {
		loopDone <- pq.ConsumeChannel(consumeCtx, channelName, handler,
			pgqueue.WithConcurrency(2),
			pgqueue.WithPollInterval(50*time.Millisecond))
	}()

	// Wait for both the success path and the retry-after-nack path.
	waitClosed(t, okDone, "ok message was never handled")
	waitClosed(t, failRetried, "fail-once message was never retried after nack")

	// Cancelling the context must stop the loop promptly.
	cancel()
	select {
	case err := <-loopDone:
		if err != nil {
			t.Fatalf("ConsumeChannel returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ConsumeChannel did not return within 3s of context cancellation")
	}

	// The fail-once message must have been delivered at least twice (nack -> retry).
	mu.Lock()
	failCount := seen["fail-once"]
	mu.Unlock()
	if failCount < 2 {
		t.Fatalf("expected fail-once to be delivered >=2 times, got %d", failCount)
	}

	// Allow lingering goroutines to wind down, then assert no leak.
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	goroutinesAfter := runtime.NumGoroutine()
	if goroutinesAfter > goroutinesBefore+2 {
		t.Fatalf("goroutine leak: before=%d after=%d", goroutinesBefore, goroutinesAfter)
	}
}

// TestChannelMessagesIteratorAndReceive verifies the ChannelMessages iterator
// delivers published messages and that single-shot ReceiveChannel returns
// ErrQueueEmpty when nothing is available.
func TestChannelMessagesIteratorAndReceive(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	const channelName = "consume-iter"
	if err := pq.CreateChannel(ctx, channelName); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Single-shot on an empty channel must signal ErrQueueEmpty, not (nil, nil).
	if _, err := pq.ReceiveChannel(ctx, channelName); !errors.Is(err, pgqueue.ErrQueueEmpty) {
		t.Fatalf("expected ErrQueueEmpty on empty channel, got %v", err)
	}

	const total = 5
	for i := range total {
		if _, err := pq.PublishChannel(ctx, channelName, []byte{byte('A' + i)}); err != nil {
			t.Fatalf("failed to publish message %d: %v", i, err)
		}
	}

	iterCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var consumed atomic.Int64
	for msg, err := range pq.ChannelMessages(iterCtx, channelName,
		pgqueue.WithPollInterval(50 * time.Millisecond)) {
		if err != nil {
			t.Fatalf("iterator yielded error: %v", err)
		}
		if err := pq.Ack(iterCtx, msg.Receipt()); err != nil {
			t.Fatalf("failed to ack message: %v", err)
		}
		if consumed.Add(1) == total {
			cancel() // stop the iterator once every message is consumed
		}
	}

	if got := consumed.Load(); got != total {
		t.Fatalf("expected %d messages from iterator, got %d", total, got)
	}
}

// waitClosed fails the test if ch is not closed within a short deadline.
func waitClosed(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal(msg)
	}
}

// TestConsumeChannelHandlerPanicRecovered (R-01) verifies that a panic inside a
// handler invocation does NOT crash the process: the panicking message is
// auto-nacked (its retry_count is incremented and it is redelivered), every
// other message is acked, and sibling concurrency workers keep running.
//
// Run under -race; the panic is raised concurrently with sibling workers.
func TestConsumeChannelHandlerPanicRecovered(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	const channelName = "consume-panic"
	if err := pq.CreateChannel(ctx, channelName); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// One payload ("boom") panics on its first delivery; the rest always succeed.
	const okCount = 6
	for i := range okCount {
		if _, err := pq.PublishChannel(ctx, channelName, []byte{byte('a' + i)}); err != nil {
			t.Fatalf("failed to publish ok message %d: %v", i, err)
		}
	}
	if _, err := pq.PublishChannel(ctx, channelName, []byte("boom")); err != nil {
		t.Fatalf("failed to publish panic message: %v", err)
	}

	var (
		mu      sync.Mutex
		seen    = map[string]int{}
		okDone  = make(chan struct{})
		boom2nd = make(chan struct{})
	)
	var okOnce, boomOnce sync.Once

	handler := func(_ context.Context, msg *pgqueue.Message) error {
		body := string(msg.Payload)
		mu.Lock()
		seen[body]++
		count := seen[body]
		okSeen := len(seen)
		mu.Unlock()

		if body == "boom" {
			if count == 1 {
				// First delivery panics with an error value; the loop must
				// recover, nack it, and survive.
				panic(panicPayload)
			}
			boomOnce.Do(func() { close(boom2nd) })
			return nil
		}
		// Every non-boom payload counted at least once means siblings progressed.
		if okSeen >= okCount {
			okOnce.Do(func() { close(okDone) })
		}
		return nil
	}

	consumeCtx, cancel := context.WithCancel(ctx)
	loopDone := make(chan error, 1)
	go func() {
		loopDone <- pq.ConsumeChannel(consumeCtx, channelName, handler,
			pgqueue.WithConcurrency(3),
			pgqueue.WithPollInterval(50*time.Millisecond))
	}()

	// The process must survive the panic: all non-boom messages get handled and
	// the panicking message is redelivered and succeeds on retry.
	waitClosed(t, okDone, "sibling workers did not handle all non-panic messages")
	waitClosed(t, boom2nd, "panicking message was never redelivered after recovery")

	cancel()
	select {
	case err := <-loopDone:
		if err != nil {
			t.Fatalf("ConsumeChannel returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ConsumeChannel did not return within 3s of context cancellation")
	}

	// The panicking message must have been nacked (retry_count incremented), then
	// completed on its retry.
	mu.Lock()
	boomDeliveries := seen["boom"]
	mu.Unlock()
	if boomDeliveries < 2 {
		t.Fatalf("panic message delivered %d times, want >=2 (recover -> nack -> retry)", boomDeliveries)
	}

	var completed int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM pgqueue_msg_consume_panic WHERE status = $1",
		string(pgqueue.MessageStatusCompleted),
	).Scan(&completed); err != nil {
		t.Fatalf("failed to count completed messages: %v", err)
	}
	if completed != okCount+1 {
		t.Errorf("expected %d completed messages, got %d", okCount+1, completed)
	}
}
