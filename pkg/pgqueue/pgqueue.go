package pgqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"github.com/sgaunet/pgqueue/internal/db"
)

// baseSchemaSQL contains the DDL for creating the base schema tables required by pgqueue.
// This includes: pgqueue_metadata, pgqueue_subscribers, and pgqueue_replay_log.
const baseSchemaSQL = `
-- Enable UUID extension for UUIDv7 generation
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Metadata table to track all queues (topics and channels)
CREATE TABLE IF NOT EXISTS pgqueue_metadata (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_type TEXT NOT NULL CHECK (queue_type IN ('pubsub', 'channel')),
    queue_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(queue_type, queue_name)
);

CREATE INDEX IF NOT EXISTS idx_pgqueue_metadata_type_name ON pgqueue_metadata(queue_type, queue_name);
CREATE INDEX IF NOT EXISTS idx_pgqueue_metadata_table_name ON pgqueue_metadata(table_name);

-- Subscribers table for pub/sub topics
CREATE TABLE IF NOT EXISTS pgqueue_subscribers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic_name TEXT NOT NULL,
    subscriber_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE(topic_name, subscriber_id)
);

CREATE INDEX IF NOT EXISTS idx_pgqueue_subscribers_topic ON pgqueue_subscribers(topic_name) WHERE active = TRUE;

-- Replay audit log
CREATE TABLE IF NOT EXISTS pgqueue_replay_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_type TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    replay_type TEXT NOT NULL CHECK (replay_type IN ('timestamp', 'message_id', 'dlq')),
    replay_params JSONB NOT NULL,
    message_count INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_pgqueue_replay_log_queue ON pgqueue_replay_log(queue_type, queue_name);
CREATE INDEX IF NOT EXISTS idx_pgqueue_replay_log_created_at ON pgqueue_replay_log(created_at);
`

// PGQueue is the main struct for the message queue system
type PGQueue struct {
	db      *sql.DB
	queries *db.Queries
	config  Config
}

var (
	// queueNameRegex validates queue names (alphanumeric, underscore, dash)
	queueNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
)

// InitSchema initializes the base schema tables required by pgqueue.
// This function must be called once before creating any queues or topics.
//
// It creates three tables:
//   - pgqueue_metadata: Tracks all queues and topics with their configurations
//   - pgqueue_subscribers: Tracks pub/sub subscriptions for topics
//   - pgqueue_replay_log: Audit log for message replay operations
//
// The function is idempotent and uses CREATE TABLE IF NOT EXISTS, so it can be
// safely called multiple times without errors.
//
// Example usage:
//
//	db, err := sql.Open("pgx", "postgres://user:pass@localhost/dbname")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
//	// Initialize base schema (call once per database)
//	if err := pgqueue.InitSchema(ctx, db); err != nil {
//	    log.Fatal(err)
//	}
//
//	// Initialize pgqueue library
//	pq, err := pgqueue.Init(ctx, pgqueue.Config{
//	    DB:                db,
//	    MaxMessageSize:    1024 * 1024,
//	    DefaultMaxRetries: 3,
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
func InitSchema(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("database connection cannot be nil")
	}

	_, err := db.ExecContext(ctx, baseSchemaSQL)
	if err != nil {
		return fmt.Errorf("failed to initialize base schema: %w", err)
	}

	return nil
}

// Init initializes the PGQueue system with the provided configuration
func Init(ctx context.Context, cfg Config) (*PGQueue, error) {
	if cfg.DB == nil {
		return nil, fmt.Errorf("database connection is required")
	}

	// Set defaults
	if cfg.MaxMessageSize == 0 {
		cfg.MaxMessageSize = 1024 // 1KB default
	}
	if cfg.DefaultMaxRetries == 0 {
		cfg.DefaultMaxRetries = 3
	}

	// Test database connection
	if err := cfg.DB.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	pq := &PGQueue{
		db:      cfg.DB,
		queries: db.New(cfg.DB),
		config:  cfg,
	}

	return pq, nil
}

// CreateTopic creates a new pub/sub topic with the specified options
func (pq *PGQueue) CreateTopic(ctx context.Context, name string, opts TopicOptions) error {
	return pq.createQueue(ctx, QueueTypePubSub, name, opts)
}

// CreateChannel creates a new point-to-point channel with the specified options
func (pq *PGQueue) CreateChannel(ctx context.Context, name string, opts ChannelOptions) error {
	return pq.createQueue(ctx, QueueTypeChannel, name, opts)
}

// createQueue is the internal implementation for creating queues
func (pq *PGQueue) createQueue(ctx context.Context, queueType QueueType, name string, opts interface{}) error {
	// Validate queue name
	if !queueNameRegex.MatchString(name) {
		return fmt.Errorf("invalid queue name: must contain only alphanumeric characters, underscores, and dashes")
	}

	// Check if queue already exists
	existing, err := pq.queries.GetQueueMetadata(ctx, db.GetQueueMetadataParams{
		QueueType: string(queueType),
		QueueName: name,
	})
	if err == nil && existing.ID != [16]byte{} {
		return fmt.Errorf("queue already exists: %s/%s", queueType, name)
	}
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to check existing queue: %w", err)
	}

	// Sanitize table name
	tableName := sanitizeTableName(name)

	// Marshal options to JSON
	configJSON, err := json.Marshal(opts)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// Begin transaction
	tx, err := pq.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	qtx := pq.queries.WithTx(tx)

	// Create metadata entry
	_, err = qtx.CreateQueueMetadata(ctx, db.CreateQueueMetadataParams{
		QueueType: string(queueType),
		QueueName: name,
		TableName: tableName,
		Config:    configJSON,
	})
	if err != nil {
		return fmt.Errorf("failed to create queue metadata: %w", err)
	}

	// Create queue tables based on type
	if queueType == QueueTypePubSub {
		if err := pq.createPubSubTables(ctx, tx, tableName); err != nil {
			return err
		}
	} else {
		if err := pq.createChannelTables(ctx, tx, tableName); err != nil {
			return err
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// createPubSubTables creates message and subscription tables for a pub/sub topic
func (pq *PGQueue) createPubSubTables(ctx context.Context, tx *sql.Tx, tableName string) error {
	// Create message table
	messageTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS pgqueue_msg_%s (
			id UUID PRIMARY KEY,
			payload BYTEA NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			metadata JSONB
		)`, tableName)

	if _, err := tx.ExecContext(ctx, messageTable); err != nil {
		return fmt.Errorf("failed to create message table: %w", err)
	}

	// Create indexes
	createIndex := fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS idx_pgqueue_msg_%s_created_at
		ON pgqueue_msg_%s(created_at)`, tableName, tableName)

	if _, err := tx.ExecContext(ctx, createIndex); err != nil {
		return fmt.Errorf("failed to create message index: %w", err)
	}

	// Create subscription table
	subscriptionTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS pgqueue_sub_%s (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			message_id UUID NOT NULL,
			subscriber_id TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			acked_at TIMESTAMPTZ,
			visibility_timeout TIMESTAMPTZ,
			retry_count INT NOT NULL DEFAULT 0,
			error_message TEXT,
			FOREIGN KEY (message_id) REFERENCES pgqueue_msg_%s(id) ON DELETE CASCADE
		)`, tableName, tableName)

	if _, err := tx.ExecContext(ctx, subscriptionTable); err != nil {
		return fmt.Errorf("failed to create subscription table: %w", err)
	}

	// Create subscription indexes
	subIndexes := []string{
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_pgqueue_sub_%s_msg_id ON pgqueue_sub_%s(message_id)`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_pgqueue_sub_%s_subscriber ON pgqueue_sub_%s(subscriber_id, status)`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_pgqueue_sub_%s_status ON pgqueue_sub_%s(status) WHERE status = 'pending'`, tableName, tableName),
	}

	for _, idx := range subIndexes {
		if _, err := tx.ExecContext(ctx, idx); err != nil {
			return fmt.Errorf("failed to create subscription index: %w", err)
		}
	}

	// Create DLQ table for pub/sub
	return pq.createDLQTable(ctx, tx, tableName)
}

// createChannelTables creates message table for a point-to-point channel
func (pq *PGQueue) createChannelTables(ctx context.Context, tx *sql.Tx, tableName string) error {
	// Create message table
	messageTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS pgqueue_msg_%s (
			id UUID PRIMARY KEY,
			payload BYTEA NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			status TEXT NOT NULL DEFAULT 'pending',
			retry_count INT NOT NULL DEFAULT 0,
			max_retries INT,
			visibility_timeout TIMESTAMPTZ,
			ack_deadline TIMESTAMPTZ,
			processed_at TIMESTAMPTZ,
			error_message TEXT,
			metadata JSONB
		)`, tableName)

	if _, err := tx.ExecContext(ctx, messageTable); err != nil {
		return fmt.Errorf("failed to create message table: %w", err)
	}

	// Create indexes for efficient querying
	indexes := []string{
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_pgqueue_msg_%s_status_created
			ON pgqueue_msg_%s(status, created_at) WHERE status = 'pending'`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_pgqueue_msg_%s_visibility
			ON pgqueue_msg_%s(visibility_timeout) WHERE visibility_timeout IS NOT NULL`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_pgqueue_msg_%s_ack_deadline
			ON pgqueue_msg_%s(ack_deadline) WHERE ack_deadline IS NOT NULL`, tableName, tableName),
	}

	for _, idx := range indexes {
		if _, err := tx.ExecContext(ctx, idx); err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
	}

	// Create DLQ table
	return pq.createDLQTable(ctx, tx, tableName)
}

// createDLQTable creates a dead letter queue table
func (pq *PGQueue) createDLQTable(ctx context.Context, tx *sql.Tx, tableName string) error {
	dlqTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS pgqueue_dlq_%s (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			original_message_id UUID NOT NULL,
			payload BYTEA NOT NULL,
			failure_reason TEXT NOT NULL,
			retry_count INT NOT NULL,
			moved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			metadata JSONB
		)`, tableName)

	if _, err := tx.ExecContext(ctx, dlqTable); err != nil {
		return fmt.Errorf("failed to create DLQ table: %w", err)
	}

	// Create DLQ index
	dlqIndex := fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS idx_pgqueue_dlq_%s_moved_at
		ON pgqueue_dlq_%s(moved_at)`, tableName, tableName)

	if _, err := tx.ExecContext(ctx, dlqIndex); err != nil {
		return fmt.Errorf("failed to create DLQ index: %w", err)
	}

	return nil
}

// ListTopics returns all pub/sub topics
func (pq *PGQueue) ListTopics(ctx context.Context) ([]QueueMetadata, error) {
	return pq.listQueues(ctx, QueueTypePubSub)
}

// ListChannels returns all point-to-point channels
func (pq *PGQueue) ListChannels(ctx context.Context) ([]QueueMetadata, error) {
	return pq.listQueues(ctx, QueueTypeChannel)
}

// listQueues is the internal implementation for listing queues
func (pq *PGQueue) listQueues(ctx context.Context, queueType QueueType) ([]QueueMetadata, error) {
	rows, err := pq.queries.ListQueues(ctx, string(queueType))
	if err != nil {
		return nil, fmt.Errorf("failed to list queues: %w", err)
	}

	result := make([]QueueMetadata, 0, len(rows))
	for _, row := range rows {
		var config map[string]interface{}
		if err := json.Unmarshal(row.Config, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal config: %w", err)
		}

		result = append(result, QueueMetadata{
			ID:        uuid.UUID(row.ID),
			QueueType: QueueType(row.QueueType),
			QueueName: row.QueueName,
			TableName: row.TableName,
			Config:    config,
			CreatedAt: row.CreatedAt,
			UpdatedAt: row.UpdatedAt,
		})
	}

	return result, nil
}

// Close closes the database connection
func (pq *PGQueue) Close() error {
	return pq.db.Close()
}

// sanitizeTableName converts a queue name to a safe table name
func sanitizeTableName(name string) string {
	// Replace dashes with underscores and convert to lowercase
	return strings.ToLower(strings.ReplaceAll(name, "-", "_"))
}
