package pgqueue_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sgaunet/pgqueue/pkg/pgqueue"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestPublishAfterConnectionLoss tests behavior when connection is lost during publish
func TestPublishAfterConnectionLoss(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel
	err := pq.CreateChannel(ctx, "error-test", pgqueue.ChannelOptions{})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish a message successfully
	_, err = pq.Publish(ctx, "error-test", []byte("test message"))
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	// Close the database connection directly (caller owns the DB)
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	// Try to publish after connection is closed - should fail
	_, err = pq.Publish(ctx, "error-test", []byte("test message 2"))
	if err == nil {
		t.Error("expected error when publishing after connection closed, got nil")
	}
}

// TestConsumeFromNonExistentQueue tests consuming from a queue that doesn't exist
func TestConsumeFromNonExistentQueue(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Try to consume from non-existent channel
	_, err := pq.ConsumeFromChannel(ctx, "non-existent", 30*time.Second)
	if err == nil {
		t.Error("expected error when consuming from non-existent channel, got nil")
	}

	// Try to consume from non-existent topic
	_, err = pq.ConsumeFromTopic(ctx, "non-existent", "subscriber-1", 30*time.Second)
	if err == nil {
		t.Error("expected error when consuming from non-existent topic, got nil")
	}
}

// TestAckNonExistentMessage tests acknowledging a message that doesn't exist
func TestAckNonExistentMessage(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel
	err := pq.CreateChannel(ctx, "ack-error-test", pgqueue.ChannelOptions{})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Try to ack non-existent message
	fakeID, _ := pgqueue.NewUUIDv7()
	err = pq.AckChannel(ctx, "ack-error-test", fakeID)
	if err == nil {
		t.Error("expected error when acking non-existent message, got nil")
	}
}

// TestDuplicateQueueCreation tests creating a queue that already exists
func TestDuplicateQueueCreation(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a channel
	err := pq.CreateChannel(ctx, "duplicate-test", pgqueue.ChannelOptions{})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Try to create the same channel again
	err = pq.CreateChannel(ctx, "duplicate-test", pgqueue.ChannelOptions{})
	if err == nil {
		t.Error("expected error when creating duplicate channel, got nil")
	}

	// Create a topic
	err = pq.CreateTopic(ctx, "duplicate-topic", pgqueue.TopicOptions{})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Try to create the same topic again
	err = pq.CreateTopic(ctx, "duplicate-topic", pgqueue.TopicOptions{})
	if err == nil {
		t.Error("expected error when creating duplicate topic, got nil")
	}
}

func TestTableNameCollision(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a channel with a hyphenated name
	err := pq.CreateChannel(ctx, "my-queue", pgqueue.ChannelOptions{})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Creating a channel whose sanitized table name collides should fail
	err = pq.CreateChannel(ctx, "my_queue", pgqueue.ChannelOptions{})
	if !errors.Is(err, pgqueue.ErrQueueAlreadyExists) {
		t.Errorf("expected ErrQueueAlreadyExists, got %v", err)
	}

	// Also test cross-type collision: topic with colliding table name
	err = pq.CreateTopic(ctx, "my_queue", pgqueue.TopicOptions{})
	if !errors.Is(err, pgqueue.ErrQueueAlreadyExists) {
		t.Errorf("expected cross-type ErrQueueAlreadyExists, got %v", err)
	}
}

// TestInvalidQueueNames tests creating queues with invalid names
func TestInvalidQueueNames(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	invalidNames := []string{
		"test queue",  // space
		"test@queue",  // special char
		"test/queue",  // slash
		"test.queue",  // dot
		"test;queue",  // semicolon
		"",            // empty
		"test\nqueue", // newline
	}

	for _, name := range invalidNames {
		err := pq.CreateChannel(ctx, name, pgqueue.ChannelOptions{})
		if err == nil {
			t.Errorf("expected error for invalid channel name %q, got nil", name)
		}

		err = pq.CreateTopic(ctx, name, pgqueue.TopicOptions{})
		if err == nil {
			t.Errorf("expected error for invalid topic name %q, got nil", name)
		}
	}
}

// TestMessageSizeExceedsLimit tests publishing messages that exceed size limit
func TestMessageSizeExceedsLimit(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a channel with small message size limit
	err := pq.CreateChannel(ctx, "size-test", pgqueue.ChannelOptions{
		MaxMessageSize: 100, // 100 bytes limit
	})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Try to publish a message larger than the limit
	largeMessage := make([]byte, 200)
	for i := range largeMessage {
		largeMessage[i] = 'A'
	}

	_, err = pq.Publish(ctx, "size-test", largeMessage)
	if err == nil {
		t.Error("expected error when publishing message exceeding size limit, got nil")
	}
}

// TestPublishWithDuplicateMessageID tests deduplication logic
func TestPublishWithDuplicateMessageID(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel
	err := pq.CreateChannel(ctx, "dedup-test", pgqueue.ChannelOptions{})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish a message with a specific ID
	messageID, _ := pgqueue.NewUUIDv7()
	_, err = pq.PublishWithID(ctx, "dedup-test", messageID, []byte("first message"), nil)
	if err != nil {
		t.Fatalf("failed to publish first message: %v", err)
	}

	// Try to publish with the same message ID (should be deduplicated)
	_, err = pq.PublishWithID(ctx, "dedup-test", messageID, []byte("duplicate message"), nil)
	if err == nil {
		t.Error("expected error when publishing duplicate message ID, got nil")
	}

	// Verify only one message exists
	depth, err := pq.GetQueueDepth(ctx, "dedup-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get queue depth: %v", err)
	}
	if depth != 1 {
		t.Errorf("expected queue depth of 1 after deduplication, got %d", depth)
	}
}

// TestConsumeWithExpiredContext tests consuming with an already-expired context
func TestConsumeWithExpiredContext(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create and publish to a channel
	err := pq.CreateChannel(ctx, "context-test", pgqueue.ChannelOptions{})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	_, err = pq.Publish(ctx, "context-test", []byte("test message"))
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	// Create an already-expired context
	expiredCtx, cancel := context.WithDeadline(ctx, time.Now().Add(-1*time.Second))
	defer cancel()

	// Try to consume with expired context
	_, err = pq.ConsumeFromChannel(expiredCtx, "context-test", 30*time.Second)
	if err == nil {
		t.Error("expected error when consuming with expired context, got nil")
	}
}

// TestReplayWithoutConfirmation tests replay operations without confirmation
func TestReplayWithoutConfirmation(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a channel and publish a message
	err := pq.CreateChannel(ctx, "replay-confirm-test", pgqueue.ChannelOptions{})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	_, err = pq.Publish(ctx, "replay-confirm-test", []byte("test message"))
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	// Consume and ack the message
	msg, err := pq.ConsumeFromChannel(ctx, "replay-confirm-test", 30*time.Second)
	if err != nil {
		t.Fatalf("failed to consume message: %v", err)
	}
	if err := pq.AckChannel(ctx, "replay-confirm-test", msg.ID); err != nil {
		t.Fatalf("failed to ack message: %v", err)
	}

	// Try to replay without confirmation (should fail)
	_, err = pq.ReplayFrom(ctx, "replay-confirm-test", pgqueue.QueueTypeChannel, time.Now().Add(-1*time.Hour), pgqueue.ReplayOptions{
		Confirm: false,
		DryRun:  false,
	})
	if err == nil {
		t.Error("expected error when replaying without confirmation, got nil")
	}

	// Try to replay message without confirmation (should fail)
	err = pq.ReplayMessage(ctx, "replay-confirm-test", pgqueue.QueueTypeChannel, msg.ID, pgqueue.ReplayOptions{
		Confirm: false,
		DryRun:  false,
	})
	if err == nil {
		t.Error("expected error when replaying message without confirmation, got nil")
	}

	// Try to replay DLQ without confirmation (should fail)
	_, err = pq.ReplayDLQ(ctx, "replay-confirm-test", pgqueue.QueueTypeChannel, pgqueue.ReplayOptions{
		Confirm: false,
		DryRun:  false,
	})
	if err == nil {
		t.Error("expected error when replaying DLQ without confirmation, got nil")
	}
}

// TestNackExceedsMaxRetries tests that messages go to DLQ after max retries
func TestNackExceedsMaxRetries(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a channel with max retries = 2
	err := pq.CreateChannel(ctx, "max-retry-test", pgqueue.ChannelOptions{
		MaxRetries: 2,
	})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish a message
	msgID, err := pq.Publish(ctx, "max-retry-test", []byte("test message"))
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	// Nack the message 3 times (should move to DLQ after 3rd nack)
	for i := 0; i < 3; i++ {
		msg, err := pq.ConsumeFromChannel(ctx, "max-retry-test", 30*time.Second)
		if err != nil {
			t.Fatalf("failed to consume message on attempt %d: %v", i+1, err)
		}
		if msg == nil {
			t.Fatalf("consume returned nil on attempt %d", i+1)
		}
		if err := pq.NackChannel(ctx, "max-retry-test", msg.ID, "test failure"); err != nil {
			t.Fatalf("failed to nack message on attempt %d: %v", i+1, err)
		}
	}

	// Message should now be in DLQ
	dlqStats, err := pq.GetDLQStats(ctx, "max-retry-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get DLQ stats: %v", err)
	}

	if dlqStats.TotalCount != 1 {
		t.Errorf("expected 1 message in DLQ after max retries, got %d", dlqStats.TotalCount)
	}

	// Queue should be empty
	depth, err := pq.GetQueueDepth(ctx, "max-retry-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get queue depth: %v", err)
	}
	if depth != 0 {
		t.Errorf("expected queue depth 0 after message moved to DLQ, got %d", depth)
	}

	// Verify the message ID matches
	_ = msgID // Used for deduplication test earlier
}

// TestInvalidSubscriberID tests that subscriber operations reject invalid subscriber IDs.
func TestInvalidSubscriberID(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateTopic(ctx, "sub-id-test", pgqueue.TopicOptions{})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	invalidIDs := []string{
		"",                  // empty
		"sub with spaces",   // space
		"sub@id",            // special char
		"sub/id",            // slash
		"sub.id",            // dot
		"sub\x00id",         // null byte
		string(make([]byte, 129)), // too long (129 chars, filled with null bytes)
	}

	// Also test a 129-char alphanumeric string
	longID := make([]byte, 129)
	for i := range longID {
		longID[i] = 'a'
	}
	invalidIDs[len(invalidIDs)-1] = string(longID)

	for _, id := range invalidIDs {
		if err := pq.Subscribe(ctx, "sub-id-test", id); err == nil {
			t.Errorf("Subscribe: expected error for invalid subscriber ID %q, got nil", id)
		}
		if err := pq.Unsubscribe(ctx, "sub-id-test", id); err == nil {
			t.Errorf("Unsubscribe: expected error for invalid subscriber ID %q, got nil", id)
		}
		if _, err := pq.ConsumeFromTopic(ctx, "sub-id-test", id, 30*time.Second); err == nil {
			t.Errorf("ConsumeFromTopic: expected error for invalid subscriber ID %q, got nil", id)
		}
	}

	// Verify valid IDs still work
	validIDs := []string{
		"subscriber-1",
		"sub_123",
		"A",
		string(make([]byte, 128)), // exactly 128 chars
	}
	// Fill the 128-char ID with valid characters
	valid128 := make([]byte, 128)
	for i := range valid128 {
		valid128[i] = 'b'
	}
	validIDs[len(validIDs)-1] = string(valid128)

	for _, id := range validIDs {
		if err := pq.Subscribe(ctx, "sub-id-test", id); err != nil {
			t.Errorf("Subscribe: unexpected error for valid subscriber ID %q: %v", id, err)
		}
	}
}

// TestSubscribeToNonExistentTopic tests subscribing to a topic that doesn't exist
func TestSubscribeToNonExistentTopic(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Try to subscribe to non-existent topic
	err := pq.Subscribe(ctx, "non-existent-topic", "subscriber-1")
	if err == nil {
		t.Error("expected error when subscribing to non-existent topic, got nil")
	}
}

// TestGetStatsForNonExistentQueue tests getting stats for a queue that doesn't exist
func TestGetStatsForNonExistentQueue(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Try to get stats for non-existent channel
	_, err := pq.GetStats(ctx, "non-existent", pgqueue.QueueTypeChannel)
	if err == nil {
		t.Error("expected error when getting stats for non-existent channel, got nil")
	}

	// Try to get stats for non-existent topic
	_, err = pq.GetStats(ctx, "non-existent", pgqueue.QueueTypePubSub)
	if err == nil {
		t.Error("expected error when getting stats for non-existent topic, got nil")
	}
}

// TestPurgeQueueWithoutConfirmation tests purging without confirmation
func TestPurgeQueueWithoutConfirmation(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a channel
	err := pq.CreateChannel(ctx, "purge-confirm-test", pgqueue.ChannelOptions{})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish some messages
	for i := 0; i < 5; i++ {
		_, err := pq.Publish(ctx, "purge-confirm-test", []byte("test message"))
		if err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}
	}

	// Create garbage collector
	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{})

	// Try to purge without confirmation (should fail)
	err = gc.PurgeQueue(ctx, "purge-confirm-test", pgqueue.QueueTypeChannel, false)
	if err == nil {
		t.Error("expected error when purging without confirmation, got nil")
	}

	// Verify messages still exist
	depth, err := pq.GetQueueDepth(ctx, "purge-confirm-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get queue depth: %v", err)
	}
	if depth != 5 {
		t.Errorf("expected 5 messages after failed purge, got %d", depth)
	}
}

// TestInitWithNilDatabase tests initialization with nil database
func TestInitWithNilDatabase(t *testing.T) {
	ctx := context.Background()

	// Try to initialize with nil database
	_, err := pgqueue.Init(ctx, pgqueue.Config{
		DB: nil,
	})
	if err == nil {
		t.Error("expected error when initializing with nil database, got nil")
	}
}

// TestInitWithUnsupportedPGVersion tests that Init rejects PostgreSQL < 18.
func TestInitWithUnsupportedPGVersion(t *testing.T) {
	ctx := context.Background()

	// Start a PostgreSQL 16 container
	postgresContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase(testDBName),
		postgres.WithUsername(testUser),
		postgres.WithPassword(testPass),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(testWaitLogOccurrence).
				WithStartupTimeout(testStartupTimeout)),
	)
	if err != nil {
		t.Fatalf("failed to start postgres 16 container: %v", err)
	}
	defer func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}()

	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer func() { _ = db.Close() }()

	_, err = pgqueue.Init(ctx, pgqueue.Config{
		DB:                db,
		MaxMessageSize:    testMaxMessageSize,
		DefaultMaxRetries: testDefaultMaxRetries,
	})
	if err == nil {
		t.Fatal("expected error for PostgreSQL 16, got nil")
	}
	if !errors.Is(err, pgqueue.ErrUnsupportedPGVersion) {
		t.Errorf("expected ErrUnsupportedPGVersion, got: %v", err)
	}
}

// TestConcurrentPublish tests concurrent publishing to the same queue
func TestConcurrentPublish(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test channel
	err := pq.CreateChannel(ctx, "concurrent-test", pgqueue.ChannelOptions{})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish concurrently from multiple goroutines
	numGoroutines := 10
	messagesPerGoroutine := 5
	errChan := make(chan error, numGoroutines*messagesPerGoroutine)
	doneChan := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < messagesPerGoroutine; j++ {
				_, err := pq.Publish(ctx, "concurrent-test", []byte("concurrent message"))
				if err != nil {
					errChan <- err
				}
			}
			doneChan <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-doneChan
	}
	close(errChan)

	// Check for errors
	for err := range errChan {
		t.Errorf("concurrent publish error: %v", err)
	}

	// Verify all messages were published
	depth, err := pq.GetQueueDepth(ctx, "concurrent-test", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to get queue depth: %v", err)
	}

	expectedCount := int64(numGoroutines * messagesPerGoroutine)
	if depth != expectedCount {
		t.Errorf("expected %d messages after concurrent publish, got %d", expectedCount, depth)
	}
}
