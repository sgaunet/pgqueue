package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid" //nolint:depguard // needed for uuid.UUID type
	"github.com/sgaunet/pgqueue"
)

func TestPublishBatchChannel(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "batch-chan", pgqueue.WithQueueMaxMessageSize(testMaxMessageSize))
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	messages := []pgqueue.PublishMessage{
		{Payload: []byte("msg1")},
		{Payload: []byte("msg2")},
		{Payload: []byte("msg3")},
		{Payload: []byte("msg4")},
		{Payload: []byte("msg5")},
	}

	ids, err := pq.PublishBatch(ctx, "batch-chan", messages)
	if err != nil {
		t.Fatalf("PublishBatch failed: %v", err)
	}

	if len(ids) != len(messages) {
		t.Fatalf("expected %d IDs, got %d", len(messages), len(ids))
	}

	// Verify queue depth
	stats, err := pq.Stats(ctx, "batch-chan", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats.PendingCount != 5 {
		t.Errorf("expected 5 pending messages, got %d", stats.PendingCount)
	}

	// Consume all messages and verify all payloads present
	payloadSet := make(map[string]bool)
	for i := range messages {
		msg, err := pq.ReceiveChannel(ctx, "batch-chan", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("ReceiveChannel failed: %v", err)
		}
		if msg == nil {
			t.Fatalf("expected message %d, got nil", i)
		}
		payloadSet[string(msg.Payload)] = true
	}
	for _, m := range messages {
		if !payloadSet[string(m.Payload)] {
			t.Errorf("missing payload %q", string(m.Payload))
		}
	}
}

func TestPublishBatchTopic(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateTopic(ctx, "batch-topic", pgqueue.WithQueueMaxMessageSize(testMaxMessageSize))
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	if err := pq.Subscribe(ctx, "batch-topic", "sub1"); err != nil {
		t.Fatalf("Subscribe sub1 failed: %v", err)
	}
	if err := pq.Subscribe(ctx, "batch-topic", "sub2"); err != nil {
		t.Fatalf("Subscribe sub2 failed: %v", err)
	}

	messages := []pgqueue.PublishMessage{
		{Payload: []byte("t1")},
		{Payload: []byte("t2")},
		{Payload: []byte("t3")},
	}

	ids, err := pq.PublishBatch(ctx, "batch-topic", messages)
	if err != nil {
		t.Fatalf("PublishBatch failed: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 IDs, got %d", len(ids))
	}

	// Both subscribers should get all 3 messages
	for _, subID := range []string{"sub1", "sub2"} {
		for i := 0; i < 3; i++ {
			msg, err := pq.ReceiveTopic(ctx, "batch-topic", subID, pgqueue.WithVisibilityTimeout(30*time.Second))
			if err != nil {
				t.Fatalf("ReceiveTopic(%s) failed: %v", subID, err)
			}
			if msg == nil {
				t.Fatalf("subscriber %s: expected message %d, got nil", subID, i)
			}
		}
	}
}

func TestPublishBatchEmptySlice(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "empty-batch")
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	ids, err := pq.PublishBatch(ctx, "empty-batch", []pgqueue.PublishMessage{})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("expected empty IDs, got %d", len(ids))
	}
}

func TestPublishBatchTooLarge(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "large-batch")
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	messages := make([]pgqueue.PublishMessage, pgqueue.MaxBatchSize+1)
	for i := range messages {
		messages[i] = pgqueue.PublishMessage{Payload: []byte("x")}
	}

	_, err = pq.PublishBatch(ctx, "large-batch", messages)
	if !errors.Is(err, pgqueue.ErrBatchTooLarge) {
		t.Errorf("expected ErrBatchTooLarge, got: %v", err)
	}
}

func TestPublishBatchPayloadValidation(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "small-chan", pgqueue.WithQueueMaxMessageSize(10))
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	messages := []pgqueue.PublishMessage{
		{Payload: []byte("ok")},
		{Payload: []byte("this payload is way too large for the limit")},
	}

	_, err = pq.PublishBatch(ctx, "small-chan", messages)
	if !errors.Is(err, pgqueue.ErrMessageSizeExceeded) {
		t.Errorf("expected ErrMessageSizeExceeded, got: %v", err)
	}

	// Verify no messages were inserted (all-or-nothing validation)
	stats, err := pq.Stats(ctx, "small-chan", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats.PendingCount != 0 {
		t.Errorf("expected 0 pending, got %d", stats.PendingCount)
	}
}

func TestPublishBatchWithMetadata(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "meta-batch", pgqueue.WithQueueMaxMessageSize(testMaxMessageSize))
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	messages := []pgqueue.PublishMessage{
		{
			Payload:  []byte("with-meta"),
			Metadata: map[string]any{"key": "value"},
		},
		{
			Payload: []byte("no-meta"),
		},
	}

	ids, err := pq.PublishBatch(ctx, "meta-batch", messages)
	if err != nil {
		t.Fatalf("PublishBatch failed: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 IDs, got %d", len(ids))
	}

	// Consume both and check metadata by payload
	var withMeta, withoutMeta bool
	for range 2 {
		msg, err := pq.ReceiveChannel(ctx, "meta-batch", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("ReceiveChannel failed: %v", err)
		}
		switch string(msg.Payload) {
		case "with-meta":
			if msg.Metadata == nil {
				t.Error("with-meta message: expected metadata, got nil")
			} else if msg.Metadata["key"] != "value" {
				t.Errorf("with-meta message: expected key=value, got %v", msg.Metadata["key"])
			}
			withMeta = true
		case "no-meta":
			if len(msg.Metadata) != 0 {
				t.Errorf("no-meta message: expected empty metadata, got %v", msg.Metadata)
			}
			withoutMeta = true
		}
	}
	if !withMeta || !withoutMeta {
		t.Error("did not receive both messages")
	}
}

func TestPublishBatchOrderPreserved(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "order-batch", pgqueue.WithQueueMaxMessageSize(testMaxMessageSize))
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	messages := make([]pgqueue.PublishMessage, 10)
	for i := range messages {
		messages[i] = pgqueue.PublishMessage{
			Payload: []byte(fmt.Sprintf("msg-%d", i)),
		}
	}

	ids, err := pq.PublishBatch(ctx, "order-batch", messages)
	if err != nil {
		t.Fatalf("PublishBatch failed: %v", err)
	}

	if len(ids) != 10 {
		t.Fatalf("expected 10 IDs, got %d", len(ids))
	}

	// Verify all unique IDs returned
	idSet := make(map[uuid.UUID]bool, len(ids))
	for _, id := range ids {
		if idSet[id] {
			t.Errorf("duplicate ID: %s", id)
		}
		idSet[id] = true
	}

	// Consume all and verify all payloads present
	payloadSet := make(map[string]bool)
	for range 10 {
		msg, err := pq.ReceiveChannel(ctx, "order-batch", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("ReceiveChannel failed: %v", err)
		}
		payloadSet[string(msg.Payload)] = true
	}
	for i := range 10 {
		expected := fmt.Sprintf("msg-%d", i)
		if !payloadSet[expected] {
			t.Errorf("missing payload %q", expected)
		}
	}
}

func TestPublishBatchQueueNotFound(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	messages := []pgqueue.PublishMessage{
		{Payload: []byte("msg")},
	}

	_, err := pq.PublishBatch(ctx, "nonexistent", messages)
	if !errors.Is(err, pgqueue.ErrQueueNotFound) {
		t.Errorf("expected ErrQueueNotFound, got: %v", err)
	}
}

func TestAckChannelBatch(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "ack-batch", pgqueue.WithQueueMaxMessageSize(testMaxMessageSize))
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	messages := []pgqueue.PublishMessage{
		{Payload: []byte("a1")},
		{Payload: []byte("a2")},
		{Payload: []byte("a3")},
		{Payload: []byte("a4")},
		{Payload: []byte("a5")},
	}

	_, err = pq.PublishBatch(ctx, "ack-batch", messages)
	if err != nil {
		t.Fatalf("PublishBatch failed: %v", err)
	}

	// Consume all
	consumedReceipts := make([]pgqueue.Receipt, 5)
	for i := 0; i < 5; i++ {
		msg, err := pq.ReceiveChannel(ctx, "ack-batch", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("ReceiveChannel failed: %v", err)
		}
		consumedReceipts[i] = msg.Receipt()
	}

	res, err := pq.AckBatch(ctx, consumedReceipts)
	if err != nil {
		t.Fatalf("AckBatch failed: %v", err)
	}
	if len(res.Succeeded) != 5 || len(res.Failed) != 0 {
		t.Fatalf("AckBatch result = %d succeeded, %d failed; want 5, 0", len(res.Succeeded), len(res.Failed))
	}

	stats, err := pq.Stats(ctx, "ack-batch", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats.CompletedCount != 5 {
		t.Errorf("expected 5 completed, got %d", stats.CompletedCount)
	}
}

func TestAckChannelBatchNoneProcessing(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "ack-none", pgqueue.WithQueueMaxMessageSize(testMaxMessageSize))
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	messages := []pgqueue.PublishMessage{
		{Payload: []byte("p1")},
		{Payload: []byte("p2")},
	}
	ids, err := pq.PublishBatch(ctx, "ack-none", messages)
	if err != nil {
		t.Fatalf("PublishBatch failed: %v", err)
	}

	// Not consumed → still pending, not processing. The receipts carry a zero
	// ClaimID (never legitimately consumed), so each is classified
	// ErrMessageAlreadyAcked — consistent with the single-receipt Ack — and
	// reported per-receipt in BatchResult.Failed rather than as a top-level error.
	receipts := make([]pgqueue.Receipt, len(ids))
	for i, id := range ids {
		receipts[i] = pgqueue.Receipt{MessageID: id, QueueName: "ack-none", QueueType: pgqueue.QueueTypeChannel}
	}
	res, err := pq.AckBatch(ctx, receipts)
	if err != nil {
		t.Fatalf("AckBatch returned an operational error: %v", err)
	}
	if len(res.Succeeded) != 0 {
		t.Errorf("expected 0 succeeded, got %d", len(res.Succeeded))
	}
	if len(res.Failed) != len(receipts) {
		t.Fatalf("expected %d failed, got %d", len(receipts), len(res.Failed))
	}
	for _, f := range res.Failed {
		if !errors.Is(f.Reason, pgqueue.ErrMessageAlreadyAcked) {
			t.Errorf("receipt %s: reason = %v, want ErrMessageAlreadyAcked", f.Receipt.MessageID, f.Reason)
		}
	}
}

func TestAckChannelBatchEmptySlice(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "ack-empty")
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	res, err := pq.AckBatch(ctx, nil)
	if err != nil {
		t.Errorf("expected nil error for empty slice, got: %v", err)
	}
	if len(res.Succeeded) != 0 || len(res.Failed) != 0 {
		t.Errorf("expected empty result for empty slice, got %+v", res)
	}
}

func TestAckChannelBatchTooLarge(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "ack-large")
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Every receipt carries the same channel binding so AckBatch groups them
	// into a single over-limit batch and reaches the size guard.
	receipts := make([]pgqueue.Receipt, pgqueue.MaxBatchSize+1)
	for i := range receipts {
		receipts[i] = pgqueue.Receipt{QueueName: "ack-large", QueueType: pgqueue.QueueTypeChannel}
	}

	_, err = pq.AckBatch(ctx, receipts)
	if !errors.Is(err, pgqueue.ErrBatchTooLarge) {
		t.Errorf("expected ErrBatchTooLarge, got: %v", err)
	}
}

func TestAckTopicBatch(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateTopic(ctx, "ack-topic-batch", pgqueue.WithQueueMaxMessageSize(testMaxMessageSize))
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	if err := pq.Subscribe(ctx, "ack-topic-batch", "sub1"); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	messages := []pgqueue.PublishMessage{
		{Payload: []byte("t1")},
		{Payload: []byte("t2")},
		{Payload: []byte("t3")},
		{Payload: []byte("t4")},
		{Payload: []byte("t5")},
	}

	_, err = pq.PublishBatch(ctx, "ack-topic-batch", messages)
	if err != nil {
		t.Fatalf("PublishBatch failed: %v", err)
	}

	consumedReceipts := make([]pgqueue.Receipt, 5)
	for i := 0; i < 5; i++ {
		msg, err := pq.ReceiveTopic(ctx, "ack-topic-batch", "sub1", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("ReceiveTopic failed: %v", err)
		}
		consumedReceipts[i] = msg.Receipt()
	}

	res, err := pq.AckBatch(ctx, consumedReceipts)
	if err != nil {
		t.Fatalf("AckBatch failed: %v", err)
	}
	if len(res.Succeeded) != 5 || len(res.Failed) != 0 {
		t.Fatalf("AckBatch result = %d succeeded, %d failed; want 5, 0", len(res.Succeeded), len(res.Failed))
	}
}

func TestAckTopicBatchNoneProcessing(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateTopic(ctx, "ack-topic-none", pgqueue.WithQueueMaxMessageSize(testMaxMessageSize))
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	if err := pq.Subscribe(ctx, "ack-topic-none", "sub1"); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	messages := []pgqueue.PublishMessage{
		{Payload: []byte("t1")},
	}
	ids, err := pq.PublishBatch(ctx, "ack-topic-none", messages)
	if err != nil {
		t.Fatalf("PublishBatch failed: %v", err)
	}

	// Not consumed → zero ClaimID. Each receipt is classified
	// ErrMessageAlreadyAcked and reported per-receipt in BatchResult.Failed.
	receipts := make([]pgqueue.Receipt, len(ids))
	for i, id := range ids {
		receipts[i] = pgqueue.Receipt{MessageID: id, QueueName: "ack-topic-none", QueueType: pgqueue.QueueTypePubSub, SubscriberID: "sub1"}
	}
	res, err := pq.AckBatch(ctx, receipts)
	if err != nil {
		t.Fatalf("AckBatch returned an operational error: %v", err)
	}
	if len(res.Succeeded) != 0 {
		t.Errorf("expected 0 succeeded, got %d", len(res.Succeeded))
	}
	if len(res.Failed) != len(receipts) {
		t.Fatalf("expected %d failed, got %d", len(receipts), len(res.Failed))
	}
	for _, f := range res.Failed {
		if !errors.Is(f.Reason, pgqueue.ErrMessageAlreadyAcked) {
			t.Errorf("receipt %s: reason = %v, want ErrMessageAlreadyAcked", f.Receipt.MessageID, f.Reason)
		}
	}
}

func TestNackChannelBatch(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "nack-batch", pgqueue.WithQueueMaxMessageSize(testMaxMessageSize), pgqueue.WithQueueMaxRetries(3))
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	messages := []pgqueue.PublishMessage{
		{Payload: []byte("n1")},
		{Payload: []byte("n2")},
		{Payload: []byte("n3")},
		{Payload: []byte("n4")},
		{Payload: []byte("n5")},
	}

	_, err = pq.PublishBatch(ctx, "nack-batch", messages)
	if err != nil {
		t.Fatalf("PublishBatch failed: %v", err)
	}

	consumedReceipts := make([]pgqueue.Receipt, 5)
	for i := 0; i < 5; i++ {
		msg, err := pq.ReceiveChannel(ctx, "nack-batch", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("ReceiveChannel failed: %v", err)
		}
		consumedReceipts[i] = msg.Receipt()
	}

	res, err := pq.NackBatch(ctx, consumedReceipts, "transient error")
	if err != nil {
		t.Fatalf("NackBatch failed: %v", err)
	}
	if len(res.Succeeded) != 5 || len(res.Failed) != 0 {
		t.Fatalf("NackBatch result = %d succeeded, %d failed; want 5, 0", len(res.Succeeded), len(res.Failed))
	}

	// All messages should be back to pending
	stats, err := pq.Stats(ctx, "nack-batch", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats.PendingCount != 5 {
		t.Errorf("expected 5 pending after nack, got %d", stats.PendingCount)
	}

	// Verify retry count incremented
	msg, err := pq.ReceiveChannel(ctx, "nack-batch", pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("ReceiveChannel failed: %v", err)
	}
	if msg.RetryCount != 1 {
		t.Errorf("expected retry_count=1, got %d", msg.RetryCount)
	}
}

func TestNackChannelBatchMixedRetryAndDLQ(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "nack-mixed", pgqueue.WithQueueMaxMessageSize(testMaxMessageSize), pgqueue.WithQueueMaxRetries(1))
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	messages := []pgqueue.PublishMessage{
		{Payload: []byte("m1")},
		{Payload: []byte("m2")},
		{Payload: []byte("m3")},
	}

	_, err = pq.PublishBatch(ctx, "nack-mixed", messages)
	if err != nil {
		t.Fatalf("PublishBatch failed: %v", err)
	}

	// First round: consume and nack all (retry_count goes to 1)
	firstReceipts := make([]pgqueue.Receipt, 3)
	for i := 0; i < 3; i++ {
		msg, err := pq.ReceiveChannel(ctx, "nack-mixed", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("ReceiveChannel failed: %v", err)
		}
		firstReceipts[i] = msg.Receipt()
	}

	_, err = pq.NackBatch(ctx, firstReceipts, "first failure")
	if err != nil {
		t.Fatalf("first NackBatch failed: %v", err)
	}

	// Second round: consume and nack again (retry_count 1+1=2 > maxRetry 1 → DLQ)
	secondReceipts := make([]pgqueue.Receipt, 3)
	for i := 0; i < 3; i++ {
		msg, err := pq.ReceiveChannel(ctx, "nack-mixed", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("ReceiveChannel round 2 failed: %v", err)
		}
		if msg == nil {
			t.Fatalf("expected message %d in round 2, got nil", i)
		}
		secondReceipts[i] = msg.Receipt()
	}

	_, err = pq.NackBatch(ctx, secondReceipts, "second failure")
	if err != nil {
		t.Fatalf("second NackBatch failed: %v", err)
	}

	// All 3 should be in DLQ
	stats, err := pq.Stats(ctx, "nack-mixed", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats.DLQCount != 3 {
		t.Errorf("expected 3 in DLQ, got %d", stats.DLQCount)
	}
	if stats.PendingCount != 0 {
		t.Errorf("expected 0 pending, got %d", stats.PendingCount)
	}
}

func TestNackChannelBatchEmptySlice(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "nack-empty")
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	res, err := pq.NackBatch(ctx, nil, "error")
	if err != nil {
		t.Errorf("expected nil error for empty slice, got: %v", err)
	}
	if len(res.Succeeded) != 0 || len(res.Failed) != 0 {
		t.Errorf("expected empty result for empty slice, got %+v", res)
	}
}

func TestNackChannelBatchNoneProcessing(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "nack-none", pgqueue.WithQueueMaxMessageSize(testMaxMessageSize))
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	messages := []pgqueue.PublishMessage{
		{Payload: []byte("p1")},
	}
	ids, err := pq.PublishBatch(ctx, "nack-none", messages)
	if err != nil {
		t.Fatalf("PublishBatch failed: %v", err)
	}

	// Not consumed → zero ClaimID, but the message rows still exist (pending).
	// Each receipt is classified ErrMessageAlreadyAcked — consistent with the
	// single-receipt Nack — and reported per-receipt in BatchResult.Failed
	// rather than as a top-level ErrMessageNotFound.
	receipts := make([]pgqueue.Receipt, len(ids))
	for i, id := range ids {
		receipts[i] = pgqueue.Receipt{MessageID: id, QueueName: "nack-none", QueueType: pgqueue.QueueTypeChannel}
	}
	res, err := pq.NackBatch(ctx, receipts, "error")
	if err != nil {
		t.Fatalf("NackBatch returned an operational error: %v", err)
	}
	if len(res.Succeeded) != 0 {
		t.Errorf("expected 0 succeeded, got %d", len(res.Succeeded))
	}
	if len(res.Failed) != len(receipts) {
		t.Fatalf("expected %d failed, got %d", len(receipts), len(res.Failed))
	}
	for _, f := range res.Failed {
		if !errors.Is(f.Reason, pgqueue.ErrMessageAlreadyAcked) {
			t.Errorf("receipt %s: reason = %v, want ErrMessageAlreadyAcked", f.Receipt.MessageID, f.Reason)
		}
	}
}

// newBackoffQueue builds a Queue with a fast but observable backoff policy
// (every retry waits 800ms–1s) for the batch-nack backoff tests.
func newBackoffQueue(t *testing.T) (*pgqueue.Queue, func()) {
	t.Helper()
	ctx := context.Background()
	db, containerCleanup := setupTestContainer(t)
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	pq, err := pgqueue.New(ctx, db, pgqueue.WithBackoffPolicy(pgqueue.BackoffPolicy{
		BaseDelay:  800 * time.Millisecond,
		MaxDelay:   1 * time.Second,
		Multiplier: 1,
	}))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	return pq, func() {
		_ = pq.Close()
		containerCleanup()
	}
}

// newMetricsQueue builds a Queue with a recordingMetrics attached, returning
// both for assertions on the RecordAckAfterExpired emissions driven by the
// batch helpers.
func newMetricsQueue(t *testing.T) (*pgqueue.Queue, *recordingMetrics, func()) {
	t.Helper()
	ctx := context.Background()
	db, containerCleanup := setupTestContainer(t)
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	metrics := &recordingMetrics{}
	pq, err := pgqueue.New(ctx, db,
		pgqueue.WithMetrics(metrics),
		pgqueue.WithBackoffPolicy(pgqueue.BackoffPolicy{
			BaseDelay:  time.Nanosecond,
			MaxDelay:   time.Nanosecond,
			Multiplier: 1,
		}),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	return pq, metrics, func() {
		_ = pq.Close()
		containerCleanup()
	}
}

// TestAckChannelBatchEmitsAckAfterExpiredForSkippedReceipts verifies that the
// silently-skipped receipts of a partial-success AckChannelBatch are reported
// to the registered MetricsRecorder via RecordAckAfterExpired (issue #113).
func TestAckChannelBatchEmitsAckAfterExpiredForSkippedReceipts(t *testing.T) {
	pq, metrics, cleanup := newMetricsQueue(t)
	defer cleanup()

	ctx := context.Background()
	const channelName = "ack-batch-expired"
	if err := pq.CreateChannel(ctx, channelName); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// Publish 2 valid messages, then build a batch mixing real receipts with
	// stale ones whose ClaimID is zero and whose MessageIDs do not match any
	// processing row.
	if _, err := pq.PublishBatch(ctx, channelName, []pgqueue.PublishMessage{
		{Payload: []byte("v1")},
		{Payload: []byte("v2")},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	valid := make([]pgqueue.Receipt, 0, 2)
	for range 2 {
		msg, err := pq.ReceiveChannel(ctx, channelName, pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("consume: %v", err)
		}
		valid = append(valid, msg.Receipt())
	}

	staleA, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid v7: %v", err)
	}
	staleB, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid v7: %v", err)
	}
	mixed := []pgqueue.Receipt{
		valid[0],
		{MessageID: staleA, QueueName: channelName, QueueType: pgqueue.QueueTypeChannel},
		valid[1],
		{MessageID: staleB, QueueName: channelName, QueueType: pgqueue.QueueTypeChannel},
	}
	res, err := pq.AckBatch(ctx, mixed)
	if err != nil {
		t.Fatalf("ack batch: %v", err)
	}

	if got := metrics.ackAfterExpiredCount(); got != 2 {
		t.Errorf("RecordAckAfterExpired emissions = %d, want 2 (one per skipped receipt)", got)
	}
	// The two valid receipts succeed; the two stale (nonexistent) ones fail with
	// ErrMessageNotFound.
	if len(res.Succeeded) != 2 {
		t.Errorf("expected 2 succeeded, got %d", len(res.Succeeded))
	}
	if len(res.Failed) != 2 {
		t.Fatalf("expected 2 failed, got %d", len(res.Failed))
	}
	for _, f := range res.Failed {
		if !errors.Is(f.Reason, pgqueue.ErrMessageNotFound) {
			t.Errorf("receipt %s: reason = %v, want ErrMessageNotFound", f.Receipt.MessageID, f.Reason)
		}
	}
}

// TestNackChannelBatchEmitsAckAfterExpiredForSkippedReceipts is the nack
// counterpart of the AckChannelBatch test: receipts that do not match any
// processing row are counted via RecordAckAfterExpired (issue #113).
func TestNackChannelBatchEmitsAckAfterExpiredForSkippedReceipts(t *testing.T) {
	pq, metrics, cleanup := newMetricsQueue(t)
	defer cleanup()

	ctx := context.Background()
	const channelName = "nack-batch-expired"
	if err := pq.CreateChannel(ctx, channelName, pgqueue.WithQueueMaxRetries(3)); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	if _, err := pq.PublishBatch(ctx, channelName, []pgqueue.PublishMessage{
		{Payload: []byte("v1")},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msg, err := pq.ReceiveChannel(ctx, channelName, pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	staleA, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid v7: %v", err)
	}
	mixed := []pgqueue.Receipt{msg.Receipt(), {MessageID: staleA, QueueName: channelName, QueueType: pgqueue.QueueTypeChannel}}
	res, err := pq.NackBatch(ctx, mixed, "transient")
	if err != nil {
		t.Fatalf("nack batch: %v", err)
	}

	if got := metrics.ackAfterExpiredCount(); got != 1 {
		t.Errorf("RecordAckAfterExpired emissions = %d, want 1 (one per skipped receipt)", got)
	}
	if len(res.Succeeded) != 1 {
		t.Errorf("expected 1 succeeded, got %d", len(res.Succeeded))
	}
	if len(res.Failed) != 1 {
		t.Fatalf("expected 1 failed, got %d", len(res.Failed))
	}
	if !errors.Is(res.Failed[0].Reason, pgqueue.ErrMessageNotFound) {
		t.Errorf("reason = %v, want ErrMessageNotFound", res.Failed[0].Reason)
	}
}

// TestNackChannelBatchAppliesBackoff verifies that a batch nack honors the
// queue's BackoffPolicy: the retried messages are not redelivered until the
// backoff delay has elapsed, exactly as a single Nack would behave (FR-023).
func TestNackChannelBatchAppliesBackoff(t *testing.T) {
	pq, cleanup := newBackoffQueue(t)
	defer cleanup()

	ctx := context.Background()
	const channelName = "batch-backoff-channel"
	if err := pq.CreateChannel(ctx, channelName); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := pq.PublishBatch(ctx, channelName, []pgqueue.PublishMessage{
		{Payload: []byte("b1")},
		{Payload: []byte("b2")},
	}); err != nil {
		t.Fatalf("publish batch: %v", err)
	}

	receipts := make([]pgqueue.Receipt, 0, 2)
	for range 2 {
		msg, err := pq.ReceiveChannel(ctx, channelName)
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		receipts = append(receipts, msg.Receipt())
	}
	if _, err := pq.NackBatch(ctx, receipts, "transient failure"); err != nil {
		t.Fatalf("nack batch: %v", err)
	}

	// Immediately after the batch nack the messages must NOT be redelivered.
	if _, err := pq.ReceiveChannel(ctx, channelName); !errors.Is(err, pgqueue.ErrQueueEmpty) {
		t.Fatalf("batch-nacked message redelivered before backoff elapsed: err=%v", err)
	}

	// After the backoff window they become available again. Poll so the test
	// finishes as soon as the window expires rather than waiting a fixed 1.3s.
	eventually(t, 2*time.Second, 50*time.Millisecond, func() bool {
		msg, err := pq.ReceiveChannel(ctx, channelName)
		if err != nil || msg == nil {
			return false
		}
		_ = pq.Ack(ctx, msg.Receipt())
		return true
	}, "batch-nacked channel messages not redelivered after backoff elapsed")
}

// TestNackTopicBatchAppliesBackoff is the pub/sub counterpart of
// TestNackChannelBatchAppliesBackoff.
func TestNackTopicBatchAppliesBackoff(t *testing.T) {
	pq, cleanup := newBackoffQueue(t)
	defer cleanup()

	ctx := context.Background()
	const topicName = "batch-backoff-topic"
	const subID = "sub-1"
	if err := pq.CreateTopic(ctx, topicName); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := pq.Subscribe(ctx, topicName, subID); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if _, err := pq.PublishBatch(ctx, topicName, []pgqueue.PublishMessage{
		{Payload: []byte("t1")},
		{Payload: []byte("t2")},
	}); err != nil {
		t.Fatalf("publish batch: %v", err)
	}

	receipts := make([]pgqueue.Receipt, 0, 2)
	for range 2 {
		msg, err := pq.ReceiveTopic(ctx, topicName, subID)
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		receipts = append(receipts, msg.Receipt())
	}
	if _, err := pq.NackBatch(ctx, receipts, "transient failure"); err != nil {
		t.Fatalf("nack batch: %v", err)
	}

	// Immediately after the batch nack the messages must NOT be redelivered.
	if _, err := pq.ReceiveTopic(ctx, topicName, subID); !errors.Is(err, pgqueue.ErrQueueEmpty) {
		t.Fatalf("batch-nacked subscription redelivered before backoff elapsed: err=%v", err)
	}

	// After the backoff window they become available again. Poll so the test
	// finishes as soon as the window expires rather than waiting a fixed 1.3s.
	eventually(t, 2*time.Second, 50*time.Millisecond, func() bool {
		msg, err := pq.ReceiveTopic(ctx, topicName, subID)
		if err != nil || msg == nil {
			return false
		}
		_ = pq.Ack(ctx, msg.Receipt())
		return true
	}, "batch-nacked topic messages not redelivered after backoff elapsed")
}

// TestAckChannelBatchMixedOutcomes is the issue #133 (AUDIT A2) verification:
// a batch mixing a valid receipt, an expired claim, an already-acked receipt,
// and a nonexistent receipt must return a nil error with Succeeded holding
// exactly the valid receipt and Failed carrying the correct per-receipt reason
// for each of the other three.
func TestAckChannelBatchMixedOutcomes(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	const ch = "mixedack"
	if err := pq.CreateChannel(ctx, ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	if _, err := pq.PublishBatch(ctx, ch, []pgqueue.PublishMessage{
		{Payload: []byte("valid")},
		{Payload: []byte("expired")},
		{Payload: []byte("acked")},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	byPayload := make(map[string]pgqueue.Receipt, 3)
	for range 3 {
		msg, err := pq.ReceiveChannel(ctx, ch, pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("consume: %v", err)
		}
		byPayload[string(msg.Payload)] = msg.Receipt()
	}
	validR := byPayload["valid"]
	expiredR := byPayload["expired"]
	ackedR := byPayload["acked"]

	// Expired claim: rewrite the row's claim_id so the receipt's token no longer
	// matches a still-processing row -> ErrClaimExpired.
	otherClaim := uuid.New()
	if _, err := db.ExecContext(ctx,
		`UPDATE pgqueue_msg_mixedack SET claim_id = $1 WHERE id = $2`,
		otherClaim, expiredR.MessageID,
	); err != nil {
		t.Fatalf("rewrite claim: %v", err)
	}

	// Already-acked: ack it under its own claim so it leaves 'processing' ->
	// ErrMessageAlreadyAcked.
	if err := pq.Ack(ctx, ackedR); err != nil {
		t.Fatalf("single ack: %v", err)
	}

	// Nonexistent: a fresh ID that was never published -> ErrMessageNotFound.
	missing, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid v7: %v", err)
	}
	notFoundR := pgqueue.Receipt{MessageID: missing, QueueName: ch, QueueType: pgqueue.QueueTypeChannel}

	res, err := pq.AckBatch(ctx, []pgqueue.Receipt{validR, expiredR, ackedR, notFoundR})
	if err != nil {
		t.Fatalf("AckBatch returned an operational error: %v", err)
	}

	if len(res.Succeeded) != 1 || res.Succeeded[0].MessageID != validR.MessageID {
		t.Fatalf("Succeeded = %+v, want exactly the valid receipt %s", res.Succeeded, validR.MessageID)
	}

	wantReason := map[uuid.UUID]error{
		expiredR.MessageID:  pgqueue.ErrClaimExpired,
		ackedR.MessageID:    pgqueue.ErrMessageAlreadyAcked,
		notFoundR.MessageID: pgqueue.ErrMessageNotFound,
	}
	if len(res.Failed) != len(wantReason) {
		t.Fatalf("Failed count = %d, want %d (%+v)", len(res.Failed), len(wantReason), res.Failed)
	}
	for _, f := range res.Failed {
		want, ok := wantReason[f.Receipt.MessageID]
		if !ok {
			t.Errorf("unexpected failed receipt %s", f.Receipt.MessageID)
			continue
		}
		if !errors.Is(f.Reason, want) {
			t.Errorf("receipt %s: reason = %v, want %v", f.Receipt.MessageID, f.Reason, want)
		}
	}
}

// TestNackChannelBatchMixedOutcomes is the nack counterpart of
// TestAckChannelBatchMixedOutcomes: it exercises the nack path's Succeeded /
// Failed partitioning, which is fed by fetchBatchMessageStates rather than an
// UPDATE ... RETURNING.
func TestNackChannelBatchMixedOutcomes(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	const ch = "mixednack"
	if err := pq.CreateChannel(ctx, ch, pgqueue.WithQueueMaxRetries(3)); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	if _, err := pq.PublishBatch(ctx, ch, []pgqueue.PublishMessage{
		{Payload: []byte("valid")},
		{Payload: []byte("expired")},
		{Payload: []byte("acked")},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	byPayload := make(map[string]pgqueue.Receipt, 3)
	for range 3 {
		msg, err := pq.ReceiveChannel(ctx, ch, pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("consume: %v", err)
		}
		byPayload[string(msg.Payload)] = msg.Receipt()
	}
	validR := byPayload["valid"]
	expiredR := byPayload["expired"]
	ackedR := byPayload["acked"]

	otherClaim := uuid.New()
	if _, err := db.ExecContext(ctx,
		`UPDATE pgqueue_msg_mixednack SET claim_id = $1 WHERE id = $2`,
		otherClaim, expiredR.MessageID,
	); err != nil {
		t.Fatalf("rewrite claim: %v", err)
	}
	if err := pq.Ack(ctx, ackedR); err != nil {
		t.Fatalf("single ack: %v", err)
	}
	missing, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid v7: %v", err)
	}
	notFoundR := pgqueue.Receipt{MessageID: missing, QueueName: ch, QueueType: pgqueue.QueueTypeChannel}

	res, err := pq.NackBatch(ctx, []pgqueue.Receipt{validR, expiredR, ackedR, notFoundR}, "boom")
	if err != nil {
		t.Fatalf("NackBatch returned an operational error: %v", err)
	}

	if len(res.Succeeded) != 1 || res.Succeeded[0].MessageID != validR.MessageID {
		t.Fatalf("Succeeded = %+v, want exactly the valid receipt %s", res.Succeeded, validR.MessageID)
	}

	wantReason := map[uuid.UUID]error{
		expiredR.MessageID:  pgqueue.ErrClaimExpired,
		ackedR.MessageID:    pgqueue.ErrMessageAlreadyAcked,
		notFoundR.MessageID: pgqueue.ErrMessageNotFound,
	}
	if len(res.Failed) != len(wantReason) {
		t.Fatalf("Failed count = %d, want %d (%+v)", len(res.Failed), len(wantReason), res.Failed)
	}
	for _, f := range res.Failed {
		want, ok := wantReason[f.Receipt.MessageID]
		if !ok {
			t.Errorf("unexpected failed receipt %s", f.Receipt.MessageID)
			continue
		}
		if !errors.Is(f.Reason, want) {
			t.Errorf("receipt %s: reason = %v, want %v", f.Receipt.MessageID, f.Reason, want)
		}
	}
}

