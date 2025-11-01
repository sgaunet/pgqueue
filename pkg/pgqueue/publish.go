package pgqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// Publish publishes a message to a topic or channel
// Returns the message ID (UUIDv7) on success
func (pq *PGQueue) Publish(ctx context.Context, queueName string, payload []byte) (uuid.UUID, error) {
	return pq.PublishWithID(ctx, queueName, uuid.UUID{}, payload, nil)
}

// PublishWithID publishes a message with a specific ID for deduplication
// If messageID is nil/zero, a new UUIDv7 will be generated
// metadata is optional and can be nil
func (pq *PGQueue) PublishWithID(ctx context.Context, queueName string, messageID uuid.UUID, payload []byte, metadata map[string]interface{}) (uuid.UUID, error) {
	// Get queue metadata to determine type and validate
	var queueMeta *QueueMetadata
	var err error

	// Try pub/sub first
	queueMeta, err = pq.getQueueMetadata(ctx, string(QueueTypePubSub), queueName)
	if err == sql.ErrNoRows {
		// Try channel
		queueMeta, err = pq.getQueueMetadata(ctx, string(QueueTypeChannel), queueName)
	}
	if err != nil {
		if err == sql.ErrNoRows {
			return uuid.UUID{}, fmt.Errorf("queue not found: %s", queueName)
		}
		return uuid.UUID{}, fmt.Errorf("failed to get queue metadata: %w", err)
	}

	// Parse config to get message size limit
	var config struct {
		MaxMessageSize int `json:"MaxMessageSize"`
	}
	if err := json.Unmarshal(queueMeta.Config, &config); err != nil {
		// Use default if config parsing fails
		config.MaxMessageSize = pq.config.MaxMessageSize
	}
	if config.MaxMessageSize == 0 {
		config.MaxMessageSize = pq.config.MaxMessageSize
	}

	// Validate message size
	if len(payload) > config.MaxMessageSize {
		return uuid.UUID{}, fmt.Errorf("message size %d exceeds limit %d", len(payload), config.MaxMessageSize)
	}

	// Generate message ID if not provided
	if messageID == uuid.Nil {
		messageID, err = NewUUIDv7()
		if err != nil {
			return uuid.UUID{}, fmt.Errorf("failed to generate message ID: %w", err)
		}
	}

	// Marshal metadata if provided
	var metadataJSON []byte
	if metadata != nil {
		metadataJSON, err = json.Marshal(metadata)
		if err != nil {
			return uuid.UUID{}, fmt.Errorf("failed to marshal metadata: %w", err)
		}
	}

	// Publish based on queue type
	queueType := QueueType(queueMeta.QueueType)
	if queueType == QueueTypePubSub {
		return messageID, pq.publishToPubSub(ctx, queueMeta.QueueName, queueMeta.TableName, messageID, payload, metadataJSON)
	}

	// Get max retries from queue config
	var channelOpts struct {
		MaxRetries int `json:"MaxRetries"`
	}
	maxRetries := pq.config.DefaultMaxRetries
	if err := json.Unmarshal(queueMeta.Config, &channelOpts); err == nil && channelOpts.MaxRetries > 0 {
		maxRetries = channelOpts.MaxRetries
	}

	return messageID, pq.publishToChannel(ctx, queueMeta.TableName, messageID, payload, metadataJSON, maxRetries)
}

// publishToPubSub publishes a message to a pub/sub topic
func (pq *PGQueue) publishToPubSub(ctx context.Context, topicName, tableName string, messageID uuid.UUID, payload []byte, metadata []byte) error {
	// Begin transaction
	tx, err := pq.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Check for duplicate message ID (deduplication)
	checkQuery := fmt.Sprintf(`SELECT id FROM pgqueue_msg_%s WHERE id = $1`, tableName)
	var existingID uuid.UUID
	err = tx.QueryRowContext(ctx, checkQuery, messageID).Scan(&existingID)
	if err == nil {
		return fmt.Errorf("duplicate message ID: %s", messageID)
	}
	if err != sql.ErrNoRows {
		return fmt.Errorf("failed to check for duplicate: %w", err)
	}

	// Insert message
	insertMsg := fmt.Sprintf(`
		INSERT INTO pgqueue_msg_%s (id, payload, metadata)
		VALUES ($1, $2, $3)
	`, tableName)

	_, err = tx.ExecContext(ctx, insertMsg, messageID, payload, metadata)
	if err != nil {
		return fmt.Errorf("failed to insert message: %w", err)
	}

	// Get active subscribers
	subscribers, err := pq.getActiveSubscribers(ctx, tx, topicName)
	if err != nil {
		return fmt.Errorf("failed to get active subscribers: %w", err)
	}

	// Create subscription records for each subscriber
	if len(subscribers) > 0 {
		insertSub := fmt.Sprintf(`
			INSERT INTO pgqueue_sub_%s (message_id, subscriber_id, status)
			VALUES ($1, $2, 'pending')
		`, tableName)

		stmt, err := tx.PrepareContext(ctx, insertSub)
		if err != nil {
			return fmt.Errorf("failed to prepare subscription insert: %w", err)
		}
		defer stmt.Close()

		for _, sub := range subscribers {
			if _, err := stmt.ExecContext(ctx, messageID, sub.SubscriberID); err != nil {
				return fmt.Errorf("failed to create subscription record: %w", err)
			}
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// publishToChannel publishes a message to a point-to-point channel
func (pq *PGQueue) publishToChannel(ctx context.Context, tableName string, messageID uuid.UUID, payload []byte, metadata []byte, maxRetries int) error {
	// Check for duplicate message ID (deduplication)
	checkQuery := fmt.Sprintf(`SELECT id FROM pgqueue_msg_%s WHERE id = $1`, tableName)
	var existingID uuid.UUID
	err := pq.db.QueryRowContext(ctx, checkQuery, messageID).Scan(&existingID)
	if err == nil {
		return fmt.Errorf("duplicate message ID: %s", messageID)
	}
	if err != sql.ErrNoRows {
		return fmt.Errorf("failed to check for duplicate: %w", err)
	}

	// Insert message
	insertMsg := fmt.Sprintf(`
		INSERT INTO pgqueue_msg_%s (id, payload, status, metadata, max_retries)
		VALUES ($1, $2, 'pending', $3, $4)
	`, tableName)

	_, err = pq.db.ExecContext(ctx, insertMsg, messageID, payload, metadata, maxRetries)
	if err != nil {
		return fmt.Errorf("failed to insert message: %w", err)
	}

	return nil
}

// reverseTableName converts a sanitized table name back to queue name
// This is a simple implementation that reverses sanitizeTableName
func reverseTableName(tableName string) string {
	// For now, just return as-is since we only lowercase and replace dashes
	// The actual topic name is stored in metadata
	return tableName
}
