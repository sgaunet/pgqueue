package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sgaunet/pgqueue"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	testDBName            = "testdb"
	testUser              = "testuser"
	testPass              = "testpass"
	testWaitLogOccurrence = 2
	testStartupTimeout    = 5 * time.Second
	testMaxMessageSize    = 1024 * 1024 // 1MB
	testDefaultMaxRetries = 3
)

// setupTestContainer starts a PostgreSQL 18 container and returns a raw DB handle.
// This is the single source of truth for test container configuration.
func setupTestContainer(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	ctx := context.Background()

	postgresContainer, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithDatabase(testDBName),
		postgres.WithUsername(testUser),
		postgres.WithPassword(testPass),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(testWaitLogOccurrence).
				WithStartupTimeout(testStartupTimeout)))
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}

	cleanup := func() {
		_ = db.Close()
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}

	return db, cleanup
}

// setupTestDB creates a PostgreSQL container and returns a PGQueue instance and raw DB handle.
func setupTestDB(t *testing.T) (*pgqueue.Queue, *sql.DB, func()) {
	t.Helper()
	ctx := context.Background()

	db, containerCleanup := setupTestContainer(t)

	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("failed to initialize schema: %v", err)
	}

	pq, err := pgqueue.New(ctx, db,
		pgqueue.WithMaxMessageSize(testMaxMessageSize),
		pgqueue.WithDefaultMaxRetries(testDefaultMaxRetries),
		// A negligible retry backoff so tests that nack and immediately
		// re-consume see the message right away. Tests that exercise the
		// backoff timing itself (backoff_test.go) build their own Queue.
		pgqueue.WithBackoffPolicy(pgqueue.BackoffPolicy{
			BaseDelay:  time.Nanosecond,
			MaxDelay:   time.Nanosecond,
			Multiplier: 1,
		}),
	)
	if err != nil {
		t.Fatalf("failed to init pgqueue: %v", err)
	}

	cleanup := func() {
		_ = pq.Close()
		containerCleanup()
	}

	return pq, db, cleanup
}

func TestInit(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	if pq == nil {
		t.Fatal("expected non-nil pgqueue instance")
	}
}

func TestCreateChannel(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a channel
	err := pq.CreateChannel(ctx, "test-channel", pgqueue.WithQueueMaxMessageSize(2048))
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Verify we can list the channel
	channels, err := pq.ListChannels(ctx)
	if err != nil {
		t.Fatalf("failed to list channels: %v", err)
	}

	if len(channels) != 1 {
		t.Fatalf("expected 1 channel, got %d", len(channels))
	}

	if channels[0] != "test-channel" {
		t.Errorf("expected channel name 'test-channel', got '%s'", channels[0])
	}

	// Try creating duplicate channel (should fail)
	err = pq.CreateChannel(ctx, "test-channel")
	if err == nil {
		t.Fatal("expected error when creating duplicate channel")
	}
}

func TestPublishAndConsumeChannel(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a channel
	err := pq.CreateChannel(ctx, "orders")
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish a message
	payload := []byte("order-123")
	msgID, err := pq.Publish(ctx, "orders", payload)
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	t.Logf("Published message: %s", msgID)

	// Consume the message
	msg, err := pq.ReceiveChannel(ctx, "orders", pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("failed to consume message: %v", err)
	}

	if msg == nil {
		t.Fatal("expected message, got nil")
	}

	if string(msg.Payload) != string(payload) {
		t.Errorf("expected payload '%s', got '%s'", payload, msg.Payload)
	}

	// Acknowledge the message
	err = pq.Ack(ctx, msg.Receipt())
	if err != nil {
		t.Fatalf("failed to acknowledge message: %v", err)
	}

	// Try to consume again (should be empty)
	_, err = pq.ReceiveChannel(ctx, "orders", pgqueue.WithVisibilityTimeout(30*time.Second))
	if !errors.Is(err, pgqueue.ErrQueueEmpty) {
		t.Errorf("expected ErrQueueEmpty, got: %v", err)
	}
}

func TestCreateTopic(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a topic
	err := pq.CreateTopic(ctx, "notifications")
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// List topics
	topics, err := pq.ListTopics(ctx)
	if err != nil {
		t.Fatalf("failed to list topics: %v", err)
	}

	if len(topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(topics))
	}

	if topics[0] != "notifications" {
		t.Errorf("expected topic name 'notifications', got '%s'", topics[0])
	}
}

func TestPubSubFanout(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a topic
	err := pq.CreateTopic(ctx, "events")
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Register three subscribers
	subscribers := []string{"service-a", "service-b", "service-c"}
	for _, sub := range subscribers {
		err = pq.Subscribe(ctx, "events", sub)
		if err != nil {
			t.Fatalf("failed to subscribe %s: %v", sub, err)
		}
	}

	// Publish a message
	payload := []byte("important-event")
	msgID, err := pq.Publish(ctx, "events", payload)
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	t.Logf("Published message: %s", msgID)

	// Each subscriber should receive the message
	for _, sub := range subscribers {
		msg, err := pq.ReceiveTopic(ctx, "events", sub, pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("failed to consume for %s: %v", sub, err)
		}

		if msg == nil {
			t.Fatalf("subscriber %s: expected message, got nil", sub)
		}

		if string(msg.Payload) != string(payload) {
			t.Errorf("subscriber %s: expected payload '%s', got '%s'", sub, payload, msg.Payload)
		}

		// Acknowledge the message
		err = pq.Ack(ctx, msg.Receipt())
		if err != nil {
			t.Fatalf("failed to ack for %s: %v", sub, err)
		}

		t.Logf("Subscriber %s received and acked message", sub)
	}
}

func TestMessageOrdering(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("channel_ordering", func(t *testing.T) {
		// Create a channel
		err := pq.CreateChannel(ctx, "ordered-channel")
		if err != nil {
			t.Fatalf("failed to create channel: %v", err)
		}

		// Publish multiple messages
		messageCount := 10
		for i := 0; i < messageCount; i++ {
			payload := []byte(fmt.Sprintf("message-%d", i))
			_, err := pq.Publish(ctx, "ordered-channel", payload)
			if err != nil {
				t.Fatalf("failed to publish message %d: %v", i, err)
			}
			// intentional: UUIDv7 embeds a millisecond-resolution timestamp; without a
			// 1ms gap between publishes two UUIDs can share the same timestamp prefix
			// and their ordering is random, breaking the ORDER BY id assertion below.
			time.Sleep(time.Millisecond)
		}

		// Consume messages and verify order
		for i := 0; i < messageCount; i++ {
			msg, err := pq.ReceiveChannel(ctx, "ordered-channel", pgqueue.WithVisibilityTimeout(30*time.Second))
			if err != nil {
				t.Fatalf("failed to consume message %d: %v", i, err)
			}

			if msg == nil {
				t.Fatalf("expected message %d, got nil", i)
			}

			expected := fmt.Sprintf("message-%d", i)
			if string(msg.Payload) != expected {
				t.Errorf("message %d: expected '%s', got '%s'", i, expected, msg.Payload)
			}

			// Ack the message
			err = pq.Ack(ctx, msg.Receipt())
			if err != nil {
				t.Fatalf("failed to ack message %d: %v", i, err)
			}
		}
	})

	t.Run("topic_ordering", func(t *testing.T) {
		// Create a topic
		err := pq.CreateTopic(ctx, "ordered-topic")
		if err != nil {
			t.Fatalf("failed to create topic: %v", err)
		}

		// Subscribe to topic
		subscriberID := "ordering-subscriber"
		err = pq.Subscribe(ctx, "ordered-topic", subscriberID)
		if err != nil {
			t.Fatalf("failed to subscribe: %v", err)
		}

		// Publish multiple messages
		messageCount := 10
		for i := 0; i < messageCount; i++ {
			payload := []byte(fmt.Sprintf("topic-message-%d", i))
			_, err := pq.Publish(ctx, "ordered-topic", payload)
			if err != nil {
				t.Fatalf("failed to publish message %d: %v", i, err)
			}
			// intentional: UUIDv7 millisecond-resolution ordering; see channel_ordering
			time.Sleep(time.Millisecond)
		}

		// Consume messages and verify order
		for i := 0; i < messageCount; i++ {
			msg, err := pq.ReceiveTopic(ctx, "ordered-topic", subscriberID, pgqueue.WithVisibilityTimeout(30*time.Second))
			if err != nil {
				t.Fatalf("failed to consume message %d: %v", i, err)
			}

			if msg == nil {
				t.Fatalf("expected message %d, got nil", i)
			}

			expected := fmt.Sprintf("topic-message-%d", i)
			if string(msg.Payload) != expected {
				t.Errorf("message %d: expected '%s', got '%s'", i, expected, msg.Payload)
			}

			// Ack the message
			err = pq.Ack(ctx, msg.Receipt())
			if err != nil {
				t.Fatalf("failed to ack message %d: %v", i, err)
			}
		}
	})
}

func TestDeleteChannel(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a channel and publish a message
	err := pq.CreateChannel(ctx, "delete-me")
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	_, err = pq.Publish(ctx, "delete-me", []byte(`{"test": true}`))
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	// Delete the channel
	err = pq.DeleteChannel(ctx, "delete-me")
	if err != nil {
		t.Fatalf("failed to delete channel: %v", err)
	}

	// Verify channel no longer appears in list
	channels, err := pq.ListChannels(ctx)
	if err != nil {
		t.Fatalf("failed to list channels: %v", err)
	}

	if len(channels) != 0 {
		t.Fatalf("expected 0 channels after delete, got %d", len(channels))
	}

	// Verify tables are dropped
	var tableCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = 'public'
		AND table_name IN ('pgqueue_msg_delete_me', 'pgqueue_dlq_delete_me')
	`).Scan(&tableCount)
	if err != nil {
		t.Fatalf("failed to check tables: %v", err)
	}

	if tableCount != 0 {
		t.Fatalf("expected 0 queue tables after delete, got %d", tableCount)
	}

	// Verify we can recreate the same channel
	err = pq.CreateChannel(ctx, "delete-me")
	if err != nil {
		t.Fatalf("failed to recreate channel after delete: %v", err)
	}
}

func TestDeleteTopic(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a topic with a subscriber
	err := pq.CreateTopic(ctx, "delete-topic")
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	err = pq.Subscribe(ctx, "delete-topic", "sub1")
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	_, err = pq.Publish(ctx, "delete-topic", []byte(`{"event": "test"}`))
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	// Delete the topic
	err = pq.DeleteTopic(ctx, "delete-topic")
	if err != nil {
		t.Fatalf("failed to delete topic: %v", err)
	}

	// Verify topic no longer appears in list
	topics, err := pq.ListTopics(ctx)
	if err != nil {
		t.Fatalf("failed to list topics: %v", err)
	}

	if len(topics) != 0 {
		t.Fatalf("expected 0 topics after delete, got %d", len(topics))
	}

	// Verify all tables are dropped (msg, sub, dlq)
	var tableCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = 'public'
		AND table_name IN ('pgqueue_msg_delete_topic', 'pgqueue_sub_delete_topic', 'pgqueue_dlq_delete_topic')
	`).Scan(&tableCount)
	if err != nil {
		t.Fatalf("failed to check tables: %v", err)
	}

	if tableCount != 0 {
		t.Fatalf("expected 0 queue tables after delete, got %d", tableCount)
	}

	// Verify subscriber registrations are cleaned up
	var subCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM pgqueue_subscribers WHERE topic_name = 'delete-topic'
	`).Scan(&subCount)
	if err != nil {
		t.Fatalf("failed to check subscribers: %v", err)
	}

	if subCount != 0 {
		t.Fatalf("expected 0 subscribers after delete, got %d", subCount)
	}
}

func TestDeleteNonExistentQueue(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.DeleteChannel(ctx, "ghost-queue")
	if !errors.Is(err, pgqueue.ErrQueueNotFound) {
		t.Fatalf("expected ErrQueueNotFound, got: %v", err)
	}

	err = pq.DeleteTopic(ctx, "ghost-topic")
	if !errors.Is(err, pgqueue.ErrQueueNotFound) {
		t.Fatalf("expected ErrQueueNotFound, got: %v", err)
	}
}

func TestChannelTTLEnforcedOnConsume(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a channel with a very short TTL
	err := pq.CreateChannel(ctx, "ttl-test", pgqueue.WithQueueTTL(1))
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish a message
	_, err = pq.Publish(ctx, "ttl-test", []byte("expires-fast"))
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	// intentional: proves the 1-second TTL has elapsed so the message is
	// filtered out on consume; we cannot observe TTL expiry from outside before
	// attempting the consume that exercises the filter.
	time.Sleep(10 * time.Millisecond) // intentional: let TTL=1s expire

	// Consume should return ErrQueueEmpty (message expired, filtered out)
	msg, err := pq.ReceiveChannel(ctx, "ttl-test", pgqueue.WithVisibilityTimeout(30*time.Second))
	if !errors.Is(err, pgqueue.ErrQueueEmpty) {
		t.Fatalf("expected ErrQueueEmpty after TTL expiration, got err=%v msg=%v", err, msg)
	}

	// Verify the message still exists in the table (not deleted, just filtered)
	var count int
	err = db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM pgqueue_msg_ttl_test WHERE status = '%s'", pgqueue.MessageStatusPending)).Scan(&count)
	if err != nil {
		t.Fatalf("failed to count messages: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 pending message in table, got %d", count)
	}
}

func TestChannelNoTTLDeliversAllMessages(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a channel without TTL (TTL=0 means no expiration)
	err := pq.CreateChannel(ctx, "no-ttl")
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	_, err = pq.Publish(ctx, "no-ttl", []byte("lives-forever"))
	if err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// Should still be consumable
	msg, err := pq.ReceiveChannel(ctx, "no-ttl", pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}
	if msg == nil {
		t.Error("expected message with TTL=0, got nil")
	}
}

func TestTopicTTLEnforcedOnConsume(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a topic with a very short TTL
	err := pq.CreateTopic(ctx, "ttl-topic", pgqueue.WithQueueTTL(1))
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Subscribe
	err = pq.Subscribe(ctx, "ttl-topic", "sub-1")
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	// Publish
	_, err = pq.Publish(ctx, "ttl-topic", []byte("expires-fast"))
	if err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// intentional: proves the 1-second TTL has elapsed before the consume that
	// exercises the filter.
	time.Sleep(10 * time.Millisecond) // intentional: let TTL=1s expire

	// Consume should return ErrQueueEmpty (message expired, filtered out)
	msg, err := pq.ReceiveTopic(ctx, "ttl-topic", "sub-1", pgqueue.WithVisibilityTimeout(30*time.Second))
	if !errors.Is(err, pgqueue.ErrQueueEmpty) {
		t.Fatalf("expected ErrQueueEmpty after TTL expiration, got err=%v msg=%v", err, msg)
	}
}

func TestPauseResumeChannel(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "pause-ch")
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish a message
	_, err = pq.Publish(ctx, "pause-ch", []byte("hello"))
	if err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// Pause the queue
	err = pq.PauseChannel(ctx, "pause-ch")
	if err != nil {
		t.Fatalf("failed to pause queue: %v", err)
	}

	// Consume should fail with ErrQueuePaused
	_, err = pq.ReceiveChannel(ctx, "pause-ch", pgqueue.WithVisibilityTimeout(30*time.Second))
	if !errors.Is(err, pgqueue.ErrQueuePaused) {
		t.Fatalf("expected ErrQueuePaused, got: %v", err)
	}

	// Publishing should still work while paused
	_, err = pq.Publish(ctx, "pause-ch", []byte("world"))
	if err != nil {
		t.Fatalf("publishing should work while paused: %v", err)
	}

	// Resume the queue
	err = pq.ResumeChannel(ctx, "pause-ch")
	if err != nil {
		t.Fatalf("failed to resume queue: %v", err)
	}

	// Consume should work now
	msg, err := pq.ReceiveChannel(ctx, "pause-ch", pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("failed to consume after resume: %v", err)
	}
	if msg == nil {
		t.Fatal("expected message after resume, got nil")
	}
}

func TestPauseResumeTopic(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateTopic(ctx, "pause-topic")
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	err = pq.Subscribe(ctx, "pause-topic", "sub-1")
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	_, err = pq.Publish(ctx, "pause-topic", []byte("hello"))
	if err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// Pause
	err = pq.PauseTopic(ctx, "pause-topic")
	if err != nil {
		t.Fatalf("failed to pause: %v", err)
	}

	// Consume should fail
	_, err = pq.ReceiveTopic(ctx, "pause-topic", "sub-1", pgqueue.WithVisibilityTimeout(30*time.Second))
	if !errors.Is(err, pgqueue.ErrQueuePaused) {
		t.Fatalf("expected ErrQueuePaused, got: %v", err)
	}

	// Resume
	err = pq.ResumeTopic(ctx, "pause-topic")
	if err != nil {
		t.Fatalf("failed to resume: %v", err)
	}

	// Consume should work
	msg, err := pq.ReceiveTopic(ctx, "pause-topic", "sub-1", pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("failed to consume after resume: %v", err)
	}
	if msg == nil {
		t.Fatal("expected message after resume, got nil")
	}
}

func TestIsQueuePaused(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "pause-check")
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Initially not paused
	paused, err := pq.IsQueuePaused(ctx, "pause-check", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to check paused state: %v", err)
	}
	if paused {
		t.Error("expected queue to not be paused initially")
	}

	// Pause
	err = pq.PauseChannel(ctx, "pause-check")
	if err != nil {
		t.Fatalf("failed to pause: %v", err)
	}

	paused, err = pq.IsQueuePaused(ctx, "pause-check", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to check paused state: %v", err)
	}
	if !paused {
		t.Error("expected queue to be paused")
	}

	// Resume
	err = pq.ResumeChannel(ctx, "pause-check")
	if err != nil {
		t.Fatalf("failed to resume: %v", err)
	}

	paused, err = pq.IsQueuePaused(ctx, "pause-check", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("failed to check paused state: %v", err)
	}
	if paused {
		t.Error("expected queue to not be paused after resume")
	}
}

func TestResubscribePreservesCreatedAt(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateTopic(ctx, "resub-test")
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Subscribe
	if err := pq.Subscribe(ctx, "resub-test", "sub-1"); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	// Record original created_at
	var originalCreatedAt time.Time
	err = db.QueryRowContext(ctx,
		"SELECT created_at FROM pgqueue_subscribers WHERE topic_name = $1 AND subscriber_id = $2",
		"resub-test", "sub-1").Scan(&originalCreatedAt)
	if err != nil {
		t.Fatalf("failed to get original created_at: %v", err)
	}

	// Unsubscribe
	if err := pq.Unsubscribe(ctx, "resub-test", "sub-1"); err != nil {
		t.Fatalf("failed to unsubscribe: %v", err)
	}

	// intentional: ensures the created_at timestamp would differ if the DB were
	// reset on re-subscribe; cannot be observed any other way.
	time.Sleep(10 * time.Millisecond) // intentional: let time advance so a clock-reset would be detectable

	// Re-subscribe
	if err := pq.Subscribe(ctx, "resub-test", "sub-1"); err != nil {
		t.Fatalf("failed to re-subscribe: %v", err)
	}

	// Verify created_at was preserved
	var newCreatedAt time.Time
	err = db.QueryRowContext(ctx,
		"SELECT created_at FROM pgqueue_subscribers WHERE topic_name = $1 AND subscriber_id = $2",
		"resub-test", "sub-1").Scan(&newCreatedAt)
	if err != nil {
		t.Fatalf("failed to get new created_at: %v", err)
	}

	if !originalCreatedAt.Equal(newCreatedAt) {
		t.Errorf("created_at was reset on re-subscribe: original=%v, new=%v",
			originalCreatedAt, newCreatedAt)
	}

	// Verify subscriber is active again
	var active bool
	err = db.QueryRowContext(ctx,
		"SELECT active FROM pgqueue_subscribers WHERE topic_name = $1 AND subscriber_id = $2",
		"resub-test", "sub-1").Scan(&active)
	if err != nil {
		t.Fatalf("failed to get active state: %v", err)
	}
	if !active {
		t.Error("expected subscriber to be active after re-subscribe")
	}
}

func TestPauseNonExistentQueue(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.PauseChannel(ctx, "ghost-queue")
	if !errors.Is(err, pgqueue.ErrQueueNotFound) {
		t.Fatalf("expected ErrQueueNotFound, got: %v", err)
	}
}

func TestNackTopicMovesToDLQ(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create topic with max 1 retry
	err := pq.CreateTopic(ctx, "nack-dlq-topic", pgqueue.WithQueueMaxRetries(1))
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	err = pq.Subscribe(ctx, "nack-dlq-topic", "sub1")
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	if _, err := pq.Publish(ctx, "nack-dlq-topic", []byte("poison")); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// First nack: retry (retryCount 0 + 1 = 1, not > maxRetry 1)
	msg, err := pq.ReceiveTopic(ctx, "nack-dlq-topic", "sub1", pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}
	err = pq.Nack(ctx, msg.Receipt(), "transient error")
	if err != nil {
		t.Fatalf("first nack failed: %v", err)
	}

	// Second nack: should move to DLQ (retryCount 1 + 1 = 2, > maxRetry 1)
	msg, err = pq.ReceiveTopic(ctx, "nack-dlq-topic", "sub1", pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("failed to consume after retry: %v", err)
	}
	err = pq.Nack(ctx, msg.Receipt(), "permanent error")
	if err != nil {
		t.Fatalf("second nack failed: %v", err)
	}

	// Message should no longer be consumable
	_, err = pq.ReceiveTopic(ctx, "nack-dlq-topic", "sub1", pgqueue.WithVisibilityTimeout(30*time.Second))
	if !errors.Is(err, pgqueue.ErrQueueEmpty) {
		t.Errorf("expected ErrQueueEmpty after DLQ move, got: %v", err)
	}

	// Verify DLQ has the message
	dlqStats, err := pq.DLQStats(ctx, "nack-dlq-topic", pgqueue.QueueTypePubSub)
	if err != nil {
		t.Fatalf("failed to get DLQ stats: %v", err)
	}
	if dlqStats.TotalCount != 1 {
		t.Errorf("expected 1 message in DLQ, got %d", dlqStats.TotalCount)
	}
}

func TestConcurrentConsumeExactlyOnce(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "concurrent-consume")
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	const messageCount = 50
	const workerCount = 10

	// Publish messages with distinct payloads
	for i := range messageCount {
		_, err := pq.Publish(ctx, "concurrent-consume", fmt.Appendf(nil, "msg-%d", i))
		if err != nil {
			t.Fatalf("failed to publish message %d: %v", i, err)
		}
	}

	// Spawn concurrent consumers
	var mu sync.Mutex
	consumed := make(map[uuid.UUID]int)
	var wg sync.WaitGroup

	wg.Add(workerCount)
	for range workerCount {
		go func() {
			defer wg.Done()
			for {
				msg, err := pq.ReceiveChannel(ctx, "concurrent-consume", pgqueue.WithVisibilityTimeout(30*time.Second))
				if errors.Is(err, pgqueue.ErrQueueEmpty) {
					return // Queue empty
				}
				if err != nil {
					t.Errorf("consume error: %v", err)
					return
				}
				if err := pq.Ack(ctx, msg.Receipt()); err != nil {
					t.Errorf("ack error: %v", err)
					return
				}
				mu.Lock()
				consumed[msg.ID]++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Verify exactly-once: all messages consumed, no duplicates
	if len(consumed) != messageCount {
		t.Errorf("expected %d unique messages, got %d", messageCount, len(consumed))
	}
	for id, count := range consumed {
		if count != 1 {
			t.Errorf("message %s consumed %d times (expected 1)", id, count)
		}
	}
}

func TestUnsubscribe(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("unsubscribe_stops_new_deliveries", func(t *testing.T) {
		err := pq.CreateTopic(ctx, "unsub-test")
		if err != nil {
			t.Fatalf("failed to create topic: %v", err)
		}

		if err := pq.Subscribe(ctx, "unsub-test", "sub-active"); err != nil {
			t.Fatalf("failed to subscribe sub-active: %v", err)
		}
		if err := pq.Subscribe(ctx, "unsub-test", "sub-leaving"); err != nil {
			t.Fatalf("failed to subscribe sub-leaving: %v", err)
		}

		// Publish before unsubscribe — both get subscription records
		if _, err := pq.Publish(ctx, "unsub-test", []byte("before-unsub")); err != nil {
			t.Fatalf("failed to publish: %v", err)
		}

		if err := pq.Unsubscribe(ctx, "unsub-test", "sub-leaving"); err != nil {
			t.Fatalf("failed to unsubscribe: %v", err)
		}

		// Publish after unsubscribe — only sub-active gets subscription
		if _, err := pq.Publish(ctx, "unsub-test", []byte("after-unsub")); err != nil {
			t.Fatalf("failed to publish: %v", err)
		}

		// sub-active should get both messages
		for i := range 2 {
			msg, err := pq.ReceiveTopic(ctx, "unsub-test", "sub-active", pgqueue.WithVisibilityTimeout(30*time.Second))
			if err != nil {
				t.Fatalf("sub-active consume %d failed: %v", i, err)
			}
			if msg == nil {
				t.Fatalf("sub-active expected message %d, got nil", i)
			}
			if err := pq.Ack(ctx, msg.Receipt()); err != nil {
				t.Fatalf("sub-active ack failed: %v", err)
			}
		}

		// sub-leaving should only get the one published before unsubscribe
		msg, err := pq.ReceiveTopic(ctx, "unsub-test", "sub-leaving", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("sub-leaving consume failed: %v", err)
		}
		if msg == nil {
			t.Fatal("sub-leaving expected 1 message, got nil")
		}
		if string(msg.Payload) != "before-unsub" {
			t.Errorf("sub-leaving expected 'before-unsub', got '%s'", msg.Payload)
		}
		if err := pq.Ack(ctx, msg.Receipt()); err != nil {
			t.Fatalf("sub-leaving ack failed: %v", err)
		}

		// No more messages for sub-leaving
		_, err = pq.ReceiveTopic(ctx, "unsub-test", "sub-leaving", pgqueue.WithVisibilityTimeout(30*time.Second))
		if !errors.Is(err, pgqueue.ErrQueueEmpty) {
			t.Errorf("sub-leaving should have no more messages, got: %v", err)
		}
	})

	t.Run("unsubscribed_can_ack_pending", func(t *testing.T) {
		err := pq.CreateTopic(ctx, "unsub-ack")
		if err != nil {
			t.Fatalf("failed to create topic: %v", err)
		}
		if err := pq.Subscribe(ctx, "unsub-ack", "sub-x"); err != nil {
			t.Fatalf("failed to subscribe: %v", err)
		}

		if _, err := pq.Publish(ctx, "unsub-ack", []byte("pending-msg")); err != nil {
			t.Fatalf("failed to publish: %v", err)
		}

		// Consume (sets processing)
		msg, err := pq.ReceiveTopic(ctx, "unsub-ack", "sub-x", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("failed to consume: %v", err)
		}
		if msg == nil {
			t.Fatal("expected message, got nil")
		}

		// Unsubscribe while processing
		if err := pq.Unsubscribe(ctx, "unsub-ack", "sub-x"); err != nil {
			t.Fatalf("failed to unsubscribe: %v", err)
		}

		// Ack should still work
		if err := pq.Ack(ctx, msg.Receipt()); err != nil {
			t.Fatalf("ack after unsubscribe should succeed: %v", err)
		}
	})

	t.Run("resubscribe_reactivates", func(t *testing.T) {
		err := pq.CreateTopic(ctx, "unsub-resub")
		if err != nil {
			t.Fatalf("failed to create topic: %v", err)
		}
		if err := pq.Subscribe(ctx, "unsub-resub", "sub-r"); err != nil {
			t.Fatalf("failed to subscribe: %v", err)
		}

		if err := pq.Unsubscribe(ctx, "unsub-resub", "sub-r"); err != nil {
			t.Fatalf("failed to unsubscribe: %v", err)
		}

		// Verify inactive
		var active bool
		err = db.QueryRowContext(ctx,
			"SELECT active FROM pgqueue_subscribers WHERE topic_name = $1 AND subscriber_id = $2",
			"unsub-resub", "sub-r").Scan(&active)
		if err != nil {
			t.Fatalf("failed to query active state: %v", err)
		}
		if active {
			t.Error("expected subscriber to be inactive after unsubscribe")
		}

		// Re-subscribe
		if err := pq.Subscribe(ctx, "unsub-resub", "sub-r"); err != nil {
			t.Fatalf("failed to re-subscribe: %v", err)
		}

		// Publish and consume
		if _, err := pq.Publish(ctx, "unsub-resub", []byte("after-resub")); err != nil {
			t.Fatalf("failed to publish: %v", err)
		}
		msg, err := pq.ReceiveTopic(ctx, "unsub-resub", "sub-r", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("failed to consume after re-subscribe: %v", err)
		}
		if msg == nil {
			t.Fatal("expected message after re-subscribe, got nil")
		}
	})
}

func TestMessageMetadataRoundTrip(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("channel_metadata", func(t *testing.T) {
		err := pq.CreateChannel(ctx, "meta-ch")
		if err != nil {
			t.Fatalf("failed to create channel: %v", err)
		}

		metadata := map[string]any{
			"key":   "value",
			"count": float64(42),
			"nested": map[string]any{
				"a": "b",
			},
			"tags": []any{"x", "y"},
		}

		_, err = pq.Publish(ctx, "meta-ch", []byte("meta-payload"), pgqueue.WithMessageMetadata(metadata))
		if err != nil {
			t.Fatalf("failed to publish with metadata: %v", err)
		}

		msg, err := pq.ReceiveChannel(ctx, "meta-ch", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("failed to consume: %v", err)
		}
		if msg == nil {
			t.Fatal("expected message, got nil")
		}
		if msg.Metadata == nil {
			t.Fatal("expected metadata, got nil")
		}
		if msg.Metadata["key"] != "value" {
			t.Errorf("expected key='value', got %v", msg.Metadata["key"])
		}
		if msg.Metadata["count"] != float64(42) {
			t.Errorf("expected count=42, got %v", msg.Metadata["count"])
		}
		nested, ok := msg.Metadata["nested"].(map[string]any)
		if !ok {
			t.Fatalf("expected nested to be map, got %T", msg.Metadata["nested"])
		}
		if nested["a"] != "b" {
			t.Errorf("expected nested.a='b', got %v", nested["a"])
		}
		tags, ok := msg.Metadata["tags"].([]any)
		if !ok {
			t.Fatalf("expected tags to be slice, got %T", msg.Metadata["tags"])
		}
		if len(tags) != 2 || tags[0] != "x" || tags[1] != "y" {
			t.Errorf("expected tags=[x,y], got %v", tags)
		}
	})

	t.Run("topic_metadata", func(t *testing.T) {
		err := pq.CreateTopic(ctx, "meta-topic")
		if err != nil {
			t.Fatalf("failed to create topic: %v", err)
		}
		if err := pq.Subscribe(ctx, "meta-topic", "sub-m"); err != nil {
			t.Fatalf("failed to subscribe: %v", err)
		}

		metadata := map[string]any{"source": "test", "priority": float64(1)}
		_, err = pq.Publish(ctx, "meta-topic", []byte("topic-meta"), pgqueue.WithMessageMetadata(metadata))
		if err != nil {
			t.Fatalf("failed to publish: %v", err)
		}

		msg, err := pq.ReceiveTopic(ctx, "meta-topic", "sub-m", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("failed to consume: %v", err)
		}
		if msg == nil {
			t.Fatal("expected message, got nil")
		}
		if msg.Metadata["source"] != "test" {
			t.Errorf("expected source='test', got %v", msg.Metadata["source"])
		}
		if msg.Metadata["priority"] != float64(1) {
			t.Errorf("expected priority=1, got %v", msg.Metadata["priority"])
		}
	})

	t.Run("nil_metadata", func(t *testing.T) {
		err := pq.CreateChannel(ctx, "meta-nil")
		if err != nil {
			t.Fatalf("failed to create channel: %v", err)
		}

		_, err = pq.Publish(ctx, "meta-nil", []byte("no-meta"))
		if err != nil {
			t.Fatalf("failed to publish: %v", err)
		}

		msg, err := pq.ReceiveChannel(ctx, "meta-nil", pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil {
			t.Fatalf("failed to consume: %v", err)
		}
		if msg == nil {
			t.Fatal("expected message, got nil")
		}
		if msg.Metadata != nil {
			t.Errorf("expected nil metadata, got %v", msg.Metadata)
		}
	})
}

func TestPublishToTopicNoSubscribers(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateTopic(ctx, "no-subs-topic")
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Publish with zero subscribers — should succeed
	msgID, err := pq.Publish(ctx, "no-subs-topic", []byte("orphan-message"))
	if err != nil {
		t.Fatalf("failed to publish to topic with no subscribers: %v", err)
	}
	if msgID == (uuid.UUID{}) {
		t.Error("expected non-zero message ID")
	}

	// Message should exist in message table
	var msgCount int
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM pgqueue_msg_no_subs_topic").Scan(&msgCount)
	if err != nil {
		t.Fatalf("failed to count messages: %v", err)
	}
	if msgCount != 1 {
		t.Errorf("expected 1 message in table, got %d", msgCount)
	}

	// No subscription records should exist
	var subCount int
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM pgqueue_sub_no_subs_topic").Scan(&subCount)
	if err != nil {
		t.Fatalf("failed to count subscriptions: %v", err)
	}
	if subCount != 0 {
		t.Errorf("expected 0 subscription records, got %d", subCount)
	}

	// Late subscriber only sees messages published after subscribing
	if err := pq.Subscribe(ctx, "no-subs-topic", "late-sub"); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	if _, err := pq.Publish(ctx, "no-subs-topic", []byte("visible-message")); err != nil {
		t.Fatalf("failed to publish second message: %v", err)
	}

	msg, err := pq.ReceiveTopic(ctx, "no-subs-topic", "late-sub", pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}
	if msg == nil {
		t.Fatal("expected message for late subscriber, got nil")
	}
	if string(msg.Payload) != "visible-message" {
		t.Errorf("expected 'visible-message', got '%s'", msg.Payload)
	}

	// No more messages — the orphan message has no subscription record
	if err := pq.Ack(ctx, msg.Receipt()); err != nil {
		t.Fatalf("failed to ack: %v", err)
	}
	_, err = pq.ReceiveTopic(ctx, "no-subs-topic", "late-sub", pgqueue.WithVisibilityTimeout(30*time.Second))
	if !errors.Is(err, pgqueue.ErrQueueEmpty) {
		t.Errorf("expected no more messages for late subscriber, got: %v", err)
	}
}

func TestAckTopicWithoutConsume(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateTopic(ctx, "ack-no-consume")
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	if err := pq.Subscribe(ctx, "ack-no-consume", "sub-eager"); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	msgID, err := pq.Publish(ctx, "ack-no-consume", []byte("not-yet-consumed"))
	if err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// Try to ack without consuming — subscription is in 'pending' state, not 'processing'.
	// Ack requires status='processing', so rows=0 and it returns ErrMessageAlreadyAcked
	// (this sentinel covers both "never consumed" and "already acked" cases).
	err = pq.Ack(ctx, pgqueue.Receipt{MessageID: msgID, QueueName: "ack-no-consume", QueueType: pgqueue.QueueTypePubSub, SubscriberID: "sub-eager"})
	if !errors.Is(err, pgqueue.ErrMessageAlreadyAcked) {
		t.Fatalf("expected ErrMessageAlreadyAcked, got: %v", err)
	}

	// Message should still be consumable
	msg, err := pq.ReceiveTopic(ctx, "ack-no-consume", "sub-eager", pgqueue.WithVisibilityTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}
	if msg == nil {
		t.Fatal("expected message to still be consumable after failed ack")
	}
	if msg.ID != msgID {
		t.Errorf("expected message ID %s, got %s", msgID, msg.ID)
	}
}

// syncBuffer is a goroutine-safe bytes.Buffer adapter so a slog handler can be
// written to from background library goroutines without a data race under -race.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// TestCloseJoinsBackgroundGoroutines (R-08) verifies that Close() joins the GC
// and handler-consume background goroutines before returning: closing the DB
// immediately afterwards must not provoke any "query on closed database" error
// logged by a still-running library goroutine. A second Close() must be a no-op.
//
// Run under -race -count=2.
func TestCloseJoinsBackgroundGoroutines(t *testing.T) {
	db, containerCleanup := setupTestContainer(t)
	defer containerCleanup()

	ctx := context.Background()
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	logBuf := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	pq, err := pgqueue.New(ctx, db,
		pgqueue.WithLogger(logger),
		pgqueue.WithBackoffPolicy(pgqueue.BackoffPolicy{
			BaseDelay: time.Nanosecond, MaxDelay: time.Nanosecond, Multiplier: 1,
		}),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	const channelName = "close-join"
	if err := pq.CreateChannel(ctx, channelName); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// A GC that polls aggressively so it is genuinely running background work.
	gc := pgqueue.NewGarbageCollector(pq, pgqueue.GarbageCollectorConfig{
		Interval: 10 * time.Millisecond,
		DefaultPolicy: pgqueue.RetentionPolicy{
			CompletedMessageTTL: time.Millisecond,
		},
	})
	gc.Start(ctx)

	// A handler-consume loop owned by the Queue.
	for range 10 {
		if _, err := pq.Publish(ctx, channelName, []byte("work")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	go func() {
		_ = pq.ConsumeChannel(ctx, channelName,
			func(_ context.Context, _ *pgqueue.Message) error { return nil },
			pgqueue.WithPollInterval(10*time.Millisecond))
	}()

	// intentional: lets the GC and the consume loop run a few iterations before
	// Close is called. There is no observable DB state that confirms "enough
	// iterations have run"; the test is verifying the graceful-shutdown path,
	// not message delivery count.
	time.Sleep(200 * time.Millisecond) // intentional: let GC and consume loop run several iterations

	// Close() must join all background goroutines before returning.
	if err := pq.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}

	// Now close the DB. If Close() truly joined every goroutine, nothing can
	// issue a query against the now-closed pool.
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close() returned error: %v", err)
	}

	// intentional: a negative assertion — any goroutine still alive after
	// Close() would attempt a DB query and log an error; the sleep gives it
	// time to do so before we inspect the log buffer.
	time.Sleep(200 * time.Millisecond) // intentional: expose surviving goroutines via logged DB errors

	logs := logBuf.String()
	for _, bad := range []string{"closed", "bad connection", "sql: database is closed"} {
		if strings.Contains(strings.ToLower(logs), bad) {
			t.Errorf("library goroutine logged a post-Close DB error (%q):\n%s", bad, logs)
		}
	}

	// A second Close() must be a no-op returning nil.
	if err := pq.Close(); err != nil {
		t.Errorf("second Close() should be a no-op, got: %v", err)
	}
}

// TestNewBackoffAndConfigValidation (R-15) verifies that New completes a partial
// BackoffPolicy per-field (a policy supplying only MaxDelay is accepted), and
// that an invalid backoff policy or a negative safety-net poll is rejected at
// New with ErrInvalidConfig.
func TestNewBackoffAndConfigValidation(t *testing.T) {
	db, containerCleanup := setupTestContainer(t)
	defer containerCleanup()

	ctx := context.Background()
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// A partial backoff policy (only MaxDelay set) must be completed per-field
	// and accepted.
	pq, err := pgqueue.New(ctx, db,
		pgqueue.WithBackoffPolicy(pgqueue.BackoffPolicy{MaxDelay: 10 * time.Minute}))
	if err != nil {
		t.Fatalf("New with partial BackoffPolicy should succeed, got: %v", err)
	}
	_ = pq.Close()

	// A negative BaseDelay is an invalid policy and must be rejected.
	if _, err := pgqueue.New(ctx, db,
		pgqueue.WithBackoffPolicy(pgqueue.BackoffPolicy{BaseDelay: -1})); !errors.Is(err, pgqueue.ErrInvalidConfig) {
		t.Errorf("New with negative BaseDelay should return ErrInvalidConfig, got: %v", err)
	}

	// A negative safety-net poll must be rejected.
	if _, err := pgqueue.New(ctx, db,
		pgqueue.WithSafetyNetPoll(-1*time.Second)); !errors.Is(err, pgqueue.ErrInvalidConfig) {
		t.Errorf("New with negative WithSafetyNetPoll should return ErrInvalidConfig, got: %v", err)
	}
}
