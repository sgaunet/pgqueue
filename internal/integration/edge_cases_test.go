package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sgaunet/pgqueue"
)

// TestPublish_PoolExhaustion verifies that under extreme connection pool pressure
// (2 open connections, 50 goroutines × 10 messages), the library either succeeds
// or returns identifiable errors — never panics or corrupts data.
func TestPublish_PoolExhaustion(t *testing.T) {
	_, db, cleanup := setupTestDB(t)
	defer cleanup()

	// Constrain the connection pool to 2 connections
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)

	ctx := context.Background()

	// Re-initialize Queue with the constrained pool
	pq, err := pgqueue.New(ctx, db,
		pgqueue.WithMaxMessageSize(testMaxMessageSize),
		pgqueue.WithDefaultMaxRetries(testDefaultMaxRetries),
	)
	if err != nil {
		t.Fatalf("failed to re-init pgqueue: %v", err)
	}

	err = pq.CreateChannel(ctx, "pool-exhaust")
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	const numGoroutines = 50
	const messagesPerGoroutine = 10

	var wg sync.WaitGroup
	var successCount atomic.Int64
	var errCount atomic.Int64

	wg.Add(numGoroutines)
	for i := range numGoroutines {
		go func() {
			defer wg.Done()
			for range messagesPerGoroutine {
				_, pubErr := pq.Publish(ctx, "pool-exhaust",
					fmt.Appendf(nil, "pool-test-msg-%d", i))
				if pubErr != nil {
					errCount.Add(1)
				} else {
					successCount.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	t.Logf("successes: %d, errors: %d", successCount.Load(), errCount.Load())

	// Verify no data corruption: queue depth must match success count
	depth, err := pq.GetQueueDepth(ctx, "pool-exhaust", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get queue depth: %v", err)
	}

	if depth != successCount.Load() {
		t.Errorf("data integrity violation: depth=%d but successCount=%d",
			depth, successCount.Load())
	}

	if successCount.Load() == 0 {
		t.Error("expected at least some messages to succeed")
	}
}

// TestPublish_LargePayload verifies that a 10MB payload can be published
// and consumed with bit-perfect integrity.
func TestPublish_LargePayload(t *testing.T) {
	_, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	const largeSize = 10 * 1024 * 1024 // 10MB

	// Re-initialize with large max message size
	pq, err := pgqueue.New(ctx, db,
		pgqueue.WithMaxMessageSize(largeSize),
		pgqueue.WithDefaultMaxRetries(testDefaultMaxRetries),
	)
	if err != nil {
		t.Fatalf("failed to re-init pgqueue: %v", err)
	}

	err = pq.CreateChannel(ctx, "large-payload", pgqueue.WithQueueMaxMessageSize(largeSize))
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	payload := bytes.Repeat([]byte("X"), largeSize)

	msgID, err := pq.Publish(ctx, "large-payload", payload)
	if err != nil {
		t.Fatalf("failed to publish large payload: %v", err)
	}
	t.Logf("published 10MB message: %s", msgID)

	msg, err := pq.ConsumeFromChannel(ctx, "large-payload", 30*time.Second)
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}
	if msg == nil {
		t.Fatal("expected message, got nil")
	}

	if !bytes.Equal(msg.Payload, payload) {
		t.Errorf("payload mismatch: expected %d bytes, got %d bytes",
			len(payload), len(msg.Payload))
	}
}

// TestPubSub_SubscriberChurn verifies correct behavior when subscribers are
// added and removed between message publications.
func TestPubSub_SubscriberChurn(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateTopic(ctx, "churn-topic")
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Phase 1: Subscribe 3 subscribers
	for _, sub := range []string{"sub-a", "sub-b", "sub-c"} {
		if err := pq.Subscribe(ctx, "churn-topic", sub); err != nil {
			t.Fatalf("failed to subscribe %s: %v", sub, err)
		}
	}

	const messagesPerPhase = 5

	// Publish phase-1 messages
	for i := range messagesPerPhase {
		_, err := pq.Publish(ctx, "churn-topic",
			fmt.Appendf(nil, "phase1-msg-%d", i))
		if err != nil {
			t.Fatalf("failed to publish phase-1 message %d: %v", i, err)
		}
	}

	// Churn: unsubscribe sub-c, subscribe sub-d
	if err := pq.Unsubscribe(ctx, "churn-topic", "sub-c"); err != nil {
		t.Fatalf("failed to unsubscribe sub-c: %v", err)
	}
	if err := pq.Subscribe(ctx, "churn-topic", "sub-d"); err != nil {
		t.Fatalf("failed to subscribe sub-d: %v", err)
	}

	// Publish phase-2 messages
	for i := range messagesPerPhase {
		_, err := pq.Publish(ctx, "churn-topic",
			fmt.Appendf(nil, "phase2-msg-%d", i))
		if err != nil {
			t.Fatalf("failed to publish phase-2 message %d: %v", i, err)
		}
	}

	// Helper: consume all available messages for a subscriber
	consumeAll := func(t *testing.T, subscriberID string) []string {
		t.Helper()
		var payloads []string
		for {
			msg, err := pq.ConsumeFromTopic(ctx, "churn-topic", subscriberID, 30*time.Second)
			if err != nil {
				t.Fatalf("subscriber %s: consume error: %v", subscriberID, err)
			}
			if msg == nil {
				break
			}
			payloads = append(payloads, string(msg.Payload))
			if err := pq.AckTopic(ctx, "churn-topic", subscriberID, msg.Receipt()); err != nil {
				t.Fatalf("subscriber %s: ack error: %v", subscriberID, err)
			}
		}
		return payloads
	}

	// sub-a: subscribed for both phases → all 10 messages
	subAPayloads := consumeAll(t, "sub-a")
	if len(subAPayloads) != 2*messagesPerPhase {
		t.Errorf("sub-a: expected %d messages, got %d", 2*messagesPerPhase, len(subAPayloads))
	}

	// sub-b: subscribed for both phases → all 10 messages
	subBPayloads := consumeAll(t, "sub-b")
	if len(subBPayloads) != 2*messagesPerPhase {
		t.Errorf("sub-b: expected %d messages, got %d", 2*messagesPerPhase, len(subBPayloads))
	}

	// sub-c: subscribed only for phase 1 → 5 messages
	subCPayloads := consumeAll(t, "sub-c")
	if len(subCPayloads) != messagesPerPhase {
		t.Errorf("sub-c: expected %d messages, got %d", messagesPerPhase, len(subCPayloads))
	}
	for _, p := range subCPayloads {
		if !strings.HasPrefix(p, "phase1-") {
			t.Errorf("sub-c received unexpected message: %s", p)
		}
	}

	// sub-d: subscribed only for phase 2 → 5 messages
	subDPayloads := consumeAll(t, "sub-d")
	if len(subDPayloads) != messagesPerPhase {
		t.Errorf("sub-d: expected %d messages, got %d", messagesPerPhase, len(subDPayloads))
	}
	for _, p := range subDPayloads {
		if !strings.HasPrefix(p, "phase2-") {
			t.Errorf("sub-d received unexpected message: %s", p)
		}
	}
}

// TestPublish_InvalidMetadata verifies that PublishWithID returns a descriptive
// error when metadata contains values that json.Marshal cannot handle.
func TestPublish_InvalidMetadata(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "meta-fail")
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	t.Run("channel_value", func(t *testing.T) {
		msgID, _ := pgqueue.NewUUIDv7()
		_, err := pq.PublishWithID(ctx, "meta-fail", msgID, []byte("test"),
			map[string]any{"bad": make(chan int)})
		if err == nil {
			t.Fatal("expected error for channel metadata, got nil")
		}
		if !strings.Contains(err.Error(), "failed to marshal metadata") {
			t.Errorf("expected 'failed to marshal metadata' in error, got: %v", err)
		}
	})

	t.Run("func_value", func(t *testing.T) {
		msgID, _ := pgqueue.NewUUIDv7()
		_, err := pq.PublishWithID(ctx, "meta-fail", msgID, []byte("test"),
			map[string]any{"bad": func() {}})
		if err == nil {
			t.Fatal("expected error for func metadata, got nil")
		}
		if !strings.Contains(err.Error(), "failed to marshal metadata") {
			t.Errorf("expected 'failed to marshal metadata' in error, got: %v", err)
		}
	})
}

// TestPublish_ContextTimeout verifies that operations respect context deadlines
// and return context.DeadlineExceeded.
func TestPublish_ContextTimeout(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "ctx-timeout")
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	t.Run("publish_with_timeout", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(ctx, 1*time.Millisecond)
		defer cancel()
		time.Sleep(5 * time.Millisecond) // Ensure context is expired

		_, err := pq.Publish(timeoutCtx, "ctx-timeout", []byte("should-fail"))
		if err == nil {
			t.Fatal("expected error with expired context, got nil")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded, got: %v", err)
		}
	})

	t.Run("consume_with_timeout", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(ctx, 1*time.Millisecond)
		defer cancel()
		time.Sleep(5 * time.Millisecond) // Ensure context is expired

		_, err := pq.ConsumeFromChannel(timeoutCtx, "ctx-timeout", 30*time.Second)
		if err == nil {
			t.Fatal("expected error with expired context, got nil")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded, got: %v", err)
		}
	})
}

// TestPublishWithID_ConcurrentDuplicates verifies that when 20 goroutines all
// try to PublishWithID with the same messageID simultaneously, exactly 1 succeeds
// and all others get ErrDuplicateMessageID.
func TestPublishWithID_ConcurrentDuplicates(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "dedup-concurrent")
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	messageID, err := pgqueue.NewUUIDv7()
	if err != nil {
		t.Fatalf("failed to generate UUID: %v", err)
	}

	const numGoroutines = 20
	var wg sync.WaitGroup
	var successCount atomic.Int64
	var dupCount atomic.Int64
	var otherErrCount atomic.Int64

	start := make(chan struct{}) // Synchronize all goroutines to start together

	wg.Add(numGoroutines)
	for range numGoroutines {
		go func() {
			defer wg.Done()
			<-start
			_, pubErr := pq.PublishWithID(ctx, "dedup-concurrent", messageID,
				[]byte("dedup-payload"), nil)
			if pubErr == nil {
				successCount.Add(1)
			} else if errors.Is(pubErr, pgqueue.ErrDuplicateMessageID) {
				dupCount.Add(1)
			} else {
				otherErrCount.Add(1)
				t.Errorf("unexpected error: %v", pubErr)
			}
		}()
	}

	close(start) // Release all goroutines simultaneously
	wg.Wait()

	if successCount.Load() != 1 {
		t.Errorf("expected exactly 1 success, got %d", successCount.Load())
	}
	if dupCount.Load() != int64(numGoroutines-1) {
		t.Errorf("expected %d duplicates, got %d", numGoroutines-1, dupCount.Load())
	}
	if otherErrCount.Load() != 0 {
		t.Errorf("expected 0 other errors, got %d", otherErrCount.Load())
	}

	// Verify exactly 1 message in the queue
	depth, err := pq.GetQueueDepth(ctx, "dedup-concurrent", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get queue depth: %v", err)
	}
	if depth != 1 {
		t.Errorf("expected queue depth 1, got %d", depth)
	}
}

// TestBinaryAndEmptyPayload verifies that BYTEA storage handles arbitrary binary
// data and empty payloads with bit-perfect integrity.
func TestBinaryAndEmptyPayload(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("binary_null_bytes", func(t *testing.T) {
		err := pq.CreateChannel(ctx, "binary-null")
		if err != nil {
			t.Fatalf("failed to create channel: %v", err)
		}

		payload := []byte{0x00, 0x01, 0x02, 0xFF, 0x00, 0xFE}
		if _, err := pq.Publish(ctx, "binary-null", payload); err != nil {
			t.Fatalf("failed to publish: %v", err)
		}

		msg, err := pq.ConsumeFromChannel(ctx, "binary-null", 30*time.Second)
		if err != nil {
			t.Fatalf("failed to consume: %v", err)
		}
		if msg == nil {
			t.Fatal("expected message, got nil")
		}
		if !bytes.Equal(msg.Payload, payload) {
			t.Errorf("payload mismatch: expected %x, got %x", payload, msg.Payload)
		}
	})

	t.Run("non_utf8", func(t *testing.T) {
		err := pq.CreateChannel(ctx, "binary-nonutf8")
		if err != nil {
			t.Fatalf("failed to create channel: %v", err)
		}

		payload := []byte{0x80, 0x81, 0xFE, 0xFF, 0xC0, 0xC1}
		if _, err := pq.Publish(ctx, "binary-nonutf8", payload); err != nil {
			t.Fatalf("failed to publish: %v", err)
		}

		msg, err := pq.ConsumeFromChannel(ctx, "binary-nonutf8", 30*time.Second)
		if err != nil {
			t.Fatalf("failed to consume: %v", err)
		}
		if msg == nil {
			t.Fatal("expected message, got nil")
		}
		if !bytes.Equal(msg.Payload, payload) {
			t.Errorf("payload mismatch: expected %x, got %x", payload, msg.Payload)
		}
	})

	t.Run("empty_payload", func(t *testing.T) {
		err := pq.CreateChannel(ctx, "binary-empty")
		if err != nil {
			t.Fatalf("failed to create channel: %v", err)
		}

		payload := []byte{}
		if _, err := pq.Publish(ctx, "binary-empty", payload); err != nil {
			t.Fatalf("failed to publish empty payload: %v", err)
		}

		msg, err := pq.ConsumeFromChannel(ctx, "binary-empty", 30*time.Second)
		if err != nil {
			t.Fatalf("failed to consume: %v", err)
		}
		if msg == nil {
			t.Fatal("expected message, got nil")
		}
		if msg.Payload == nil {
			t.Error("expected empty []byte, got nil")
		}
		if len(msg.Payload) != 0 {
			t.Errorf("expected empty payload, got %d bytes", len(msg.Payload))
		}
	})

	t.Run("mixed_binary_text", func(t *testing.T) {
		err := pq.CreateChannel(ctx, "binary-mixed")
		if err != nil {
			t.Fatalf("failed to create channel: %v", err)
		}

		payload := append([]byte("hello\x00world"), 0xFF, 0x00)
		if _, err := pq.Publish(ctx, "binary-mixed", payload); err != nil {
			t.Fatalf("failed to publish: %v", err)
		}

		msg, err := pq.ConsumeFromChannel(ctx, "binary-mixed", 30*time.Second)
		if err != nil {
			t.Fatalf("failed to consume: %v", err)
		}
		if msg == nil {
			t.Fatal("expected message, got nil")
		}
		if !bytes.Equal(msg.Payload, payload) {
			t.Errorf("payload mismatch: expected %x, got %x", payload, msg.Payload)
		}
	})
}

// TestT022_NackErrorMsgUTF8TruncationBoundary verifies that a nack error message
// containing multi-byte UTF-8 characters that straddle the 1024-byte truncation
// boundary is stored as valid UTF-8 text (not a broken sequence).
func TestT022_NackErrorMsgUTF8TruncationBoundary(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	const queueName = "t022-utf8-trunc"
	if err := pq.CreateChannel(ctx, queueName, pgqueue.WithQueueMaxRetries(5)); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	if _, err := pq.Publish(ctx, queueName, []byte("payload")); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	msg, err := pq.ConsumeFromChannel(ctx, queueName, 30*time.Second)
	if err != nil || msg == nil {
		t.Fatalf("consume failed: %v", err)
	}

	// Build a 1024+ byte string where the 1024th byte is in the MIDDLE of a 3-byte
	// UTF-8 rune (U+4E16, "世" = 0xE4 0xB8 0x96). If truncated on byte boundary
	// the result would end with 0xE4 (incomplete sequence); rune-aware truncation
	// must stop at 1023 bytes to avoid splitting the rune.
	// Fill 1022 bytes with ASCII 'a', then append "世界" (6 bytes), totalling 1028.
	errorMsg := strings.Repeat("a", 1022) + "世界"

	if err := pq.NackChannel(ctx, queueName, msg.Receipt(), errorMsg); err != nil {
		t.Fatalf("nack failed: %v", err)
	}

	// Read the stored error_message from the database
	var stored string
	err = db.QueryRowContext(ctx,
		"SELECT error_message FROM pgqueue_msg_t022_utf8_trunc WHERE id = $1",
		msg.ID,
	).Scan(&stored)
	if err != nil {
		t.Fatalf("failed to read stored error_message: %v", err)
	}

	// Must be valid UTF-8 (no split multi-byte sequences)
	if strings.ToValidUTF8(stored, "�") != stored {
		t.Errorf("stored error_message is not valid UTF-8: %q", stored)
	}

	// Must be <= 1024 bytes
	if len(stored) > 1024 {
		t.Errorf("stored error_message too long: %d bytes, want <= 1024", len(stored))
	}
}
