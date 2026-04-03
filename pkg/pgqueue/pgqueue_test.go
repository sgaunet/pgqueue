package pgqueue_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sgaunet/pgqueue/pkg/pgqueue"
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
				WithStartupTimeout(testStartupTimeout)),
	)
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
func setupTestDB(t *testing.T) (*pgqueue.PGQueue, *sql.DB, func()) {
	t.Helper()
	ctx := context.Background()

	db, containerCleanup := setupTestContainer(t)

	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("failed to initialize schema: %v", err)
	}

	pq, err := pgqueue.Init(ctx, pgqueue.Config{
		DB:                db,
		MaxMessageSize:    testMaxMessageSize,
		DefaultMaxRetries: testDefaultMaxRetries,
	})
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
	err := pq.CreateChannel(ctx, "test-channel", pgqueue.ChannelOptions{
		MaxMessageSize: 2048,
	})
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

	if channels[0].QueueName != "test-channel" {
		t.Errorf("expected channel name 'test-channel', got '%s'", channels[0].QueueName)
	}

	// Try creating duplicate channel (should fail)
	err = pq.CreateChannel(ctx, "test-channel", pgqueue.ChannelOptions{})
	if err == nil {
		t.Fatal("expected error when creating duplicate channel")
	}
}

func TestPublishAndConsumeChannel(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a channel
	err := pq.CreateChannel(ctx, "orders", pgqueue.ChannelOptions{})
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
	msg, err := pq.ConsumeFromChannel(ctx, "orders", 30*time.Second)
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
	err = pq.AckChannel(ctx, "orders", msg.ID)
	if err != nil {
		t.Fatalf("failed to acknowledge message: %v", err)
	}

	// Try to consume again (should be empty)
	msg2, err := pq.ConsumeFromChannel(ctx, "orders", 30*time.Second)
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}
	if msg2 != nil {
		t.Error("expected no messages, but got one")
	}
}

func TestCreateTopic(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a topic
	err := pq.CreateTopic(ctx, "notifications", pgqueue.TopicOptions{})
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

	if topics[0].QueueName != "notifications" {
		t.Errorf("expected topic name 'notifications', got '%s'", topics[0].QueueName)
	}
}

func TestPubSubFanout(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a topic
	err := pq.CreateTopic(ctx, "events", pgqueue.TopicOptions{})
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
		msg, err := pq.ConsumeFromTopic(ctx, "events", sub, 30*time.Second)
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
		err = pq.AckTopic(ctx, "events", sub, msg.ID)
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
		err := pq.CreateChannel(ctx, "ordered-channel", pgqueue.ChannelOptions{})
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
			time.Sleep(time.Millisecond) // Small delay to ensure UUIDv7 ordering
		}

		// Consume messages and verify order
		for i := 0; i < messageCount; i++ {
			msg, err := pq.ConsumeFromChannel(ctx, "ordered-channel", 30*time.Second)
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
			err = pq.AckChannel(ctx, "ordered-channel", msg.ID)
			if err != nil {
				t.Fatalf("failed to ack message %d: %v", i, err)
			}
		}
	})

	t.Run("topic_ordering", func(t *testing.T) {
		// Create a topic
		err := pq.CreateTopic(ctx, "ordered-topic", pgqueue.TopicOptions{})
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
			time.Sleep(time.Millisecond) // Small delay to ensure UUIDv7 ordering
		}

		// Consume messages and verify order
		for i := 0; i < messageCount; i++ {
			msg, err := pq.ConsumeFromTopic(ctx, "ordered-topic", subscriberID, 30*time.Second)
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
			err = pq.AckTopic(ctx, "ordered-topic", subscriberID, msg.ID)
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
	err := pq.CreateChannel(ctx, "delete-me", pgqueue.ChannelOptions{})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	_, err = pq.Publish(ctx, "delete-me", []byte(`{"test": true}`))
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	// Delete the channel
	err = pq.DeleteChannel(ctx, "delete-me", true)
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
	err = pq.CreateChannel(ctx, "delete-me", pgqueue.ChannelOptions{})
	if err != nil {
		t.Fatalf("failed to recreate channel after delete: %v", err)
	}
}

func TestDeleteTopic(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a topic with a subscriber
	err := pq.CreateTopic(ctx, "delete-topic", pgqueue.TopicOptions{})
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
	err = pq.DeleteTopic(ctx, "delete-topic", true)
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

func TestDeleteChannelNotConfirmed(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "no-delete", pgqueue.ChannelOptions{})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Attempt delete without confirmation
	err = pq.DeleteChannel(ctx, "no-delete", false)
	if !errors.Is(err, pgqueue.ErrDeleteNotConfirmed) {
		t.Fatalf("expected ErrDeleteNotConfirmed, got: %v", err)
	}

	// Verify channel still exists
	channels, err := pq.ListChannels(ctx)
	if err != nil {
		t.Fatalf("failed to list channels: %v", err)
	}

	if len(channels) != 1 {
		t.Fatalf("expected channel to still exist, got %d channels", len(channels))
	}
}

func TestDeleteNonExistentQueue(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.DeleteChannel(ctx, "ghost-queue", true)
	if !errors.Is(err, pgqueue.ErrQueueNotFound) {
		t.Fatalf("expected ErrQueueNotFound, got: %v", err)
	}

	err = pq.DeleteTopic(ctx, "ghost-topic", true)
	if !errors.Is(err, pgqueue.ErrQueueNotFound) {
		t.Fatalf("expected ErrQueueNotFound, got: %v", err)
	}
}

func TestChannelTTLEnforcedOnConsume(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create a channel with a very short TTL
	err := pq.CreateChannel(ctx, "ttl-test", pgqueue.ChannelOptions{
		TTL: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish a message
	_, err = pq.Publish(ctx, "ttl-test", []byte("expires-fast"))
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	// Wait for TTL to expire
	time.Sleep(10 * time.Millisecond)

	// Consume should return nil (message expired)
	msg, err := pq.ConsumeFromChannel(ctx, "ttl-test", 30*time.Second)
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}
	if msg != nil {
		t.Error("expected nil message after TTL expiration, but got one")
	}

	// Verify the message still exists in the table (not deleted, just filtered)
	var count int
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM pgqueue_msg_ttl_test WHERE status = 'pending'",
	).Scan(&count)
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
	err := pq.CreateChannel(ctx, "no-ttl", pgqueue.ChannelOptions{})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	_, err = pq.Publish(ctx, "no-ttl", []byte("lives-forever"))
	if err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// Should still be consumable
	msg, err := pq.ConsumeFromChannel(ctx, "no-ttl", 30*time.Second)
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
	err := pq.CreateTopic(ctx, "ttl-topic", pgqueue.TopicOptions{
		TTL: 1 * time.Millisecond,
	})
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

	// Wait for TTL to expire
	time.Sleep(10 * time.Millisecond)

	// Consume should return nil (message expired)
	msg, err := pq.ConsumeFromTopic(ctx, "ttl-topic", "sub-1", 30*time.Second)
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}
	if msg != nil {
		t.Error("expected nil message after TTL expiration, but got one")
	}
}

func TestPauseResumeChannel(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := pq.CreateChannel(ctx, "pause-ch", pgqueue.ChannelOptions{})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish a message
	_, err = pq.Publish(ctx, "pause-ch", []byte("hello"))
	if err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// Pause the queue
	err = pq.PauseQueue(ctx, "pause-ch")
	if err != nil {
		t.Fatalf("failed to pause queue: %v", err)
	}

	// Consume should fail with ErrQueuePaused
	_, err = pq.ConsumeFromChannel(ctx, "pause-ch", 30*time.Second)
	if !errors.Is(err, pgqueue.ErrQueuePaused) {
		t.Fatalf("expected ErrQueuePaused, got: %v", err)
	}

	// Publishing should still work while paused
	_, err = pq.Publish(ctx, "pause-ch", []byte("world"))
	if err != nil {
		t.Fatalf("publishing should work while paused: %v", err)
	}

	// Resume the queue
	err = pq.ResumeQueue(ctx, "pause-ch")
	if err != nil {
		t.Fatalf("failed to resume queue: %v", err)
	}

	// Consume should work now
	msg, err := pq.ConsumeFromChannel(ctx, "pause-ch", 30*time.Second)
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

	err := pq.CreateTopic(ctx, "pause-topic", pgqueue.TopicOptions{})
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
	err = pq.PauseQueue(ctx, "pause-topic")
	if err != nil {
		t.Fatalf("failed to pause: %v", err)
	}

	// Consume should fail
	_, err = pq.ConsumeFromTopic(ctx, "pause-topic", "sub-1", 30*time.Second)
	if !errors.Is(err, pgqueue.ErrQueuePaused) {
		t.Fatalf("expected ErrQueuePaused, got: %v", err)
	}

	// Resume
	err = pq.ResumeQueue(ctx, "pause-topic")
	if err != nil {
		t.Fatalf("failed to resume: %v", err)
	}

	// Consume should work
	msg, err := pq.ConsumeFromTopic(ctx, "pause-topic", "sub-1", 30*time.Second)
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

	err := pq.CreateChannel(ctx, "pause-check", pgqueue.ChannelOptions{})
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Initially not paused
	paused, err := pq.IsQueuePaused(ctx, "pause-check")
	if err != nil {
		t.Fatalf("failed to check paused state: %v", err)
	}
	if paused {
		t.Error("expected queue to not be paused initially")
	}

	// Pause
	err = pq.PauseQueue(ctx, "pause-check")
	if err != nil {
		t.Fatalf("failed to pause: %v", err)
	}

	paused, err = pq.IsQueuePaused(ctx, "pause-check")
	if err != nil {
		t.Fatalf("failed to check paused state: %v", err)
	}
	if !paused {
		t.Error("expected queue to be paused")
	}

	// Resume
	err = pq.ResumeQueue(ctx, "pause-check")
	if err != nil {
		t.Fatalf("failed to resume: %v", err)
	}

	paused, err = pq.IsQueuePaused(ctx, "pause-check")
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

	err := pq.CreateTopic(ctx, "resub-test", pgqueue.TopicOptions{})
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
		"resub-test", "sub-1",
	).Scan(&originalCreatedAt)
	if err != nil {
		t.Fatalf("failed to get original created_at: %v", err)
	}

	// Unsubscribe
	if err := pq.Unsubscribe(ctx, "resub-test", "sub-1"); err != nil {
		t.Fatalf("failed to unsubscribe: %v", err)
	}

	// Wait to ensure timestamps would differ
	time.Sleep(10 * time.Millisecond)

	// Re-subscribe
	if err := pq.Subscribe(ctx, "resub-test", "sub-1"); err != nil {
		t.Fatalf("failed to re-subscribe: %v", err)
	}

	// Verify created_at was preserved
	var newCreatedAt time.Time
	err = db.QueryRowContext(ctx,
		"SELECT created_at FROM pgqueue_subscribers WHERE topic_name = $1 AND subscriber_id = $2",
		"resub-test", "sub-1",
	).Scan(&newCreatedAt)
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
		"resub-test", "sub-1",
	).Scan(&active)
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

	err := pq.PauseQueue(ctx, "ghost-queue")
	if !errors.Is(err, pgqueue.ErrQueueNotFound) {
		t.Fatalf("expected ErrQueueNotFound, got: %v", err)
	}
}
