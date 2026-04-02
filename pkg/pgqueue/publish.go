package pgqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// Publish publishes a message to a topic or channel.
// Returns the message ID (UUIDv7) on success.
func (pq *PGQueue) Publish(
	ctx context.Context,
	queueName string,
	payload []byte,
) (uuid.UUID, error) {
	return pq.PublishWithID(ctx, queueName, uuid.UUID{}, payload, nil)
}

// PublishWithID publishes a message with a specific ID for deduplication.
// If messageID is nil/zero, a new UUIDv7 will be generated.
// metadata is optional and can be nil.
func (pq *PGQueue) PublishWithID(
	ctx context.Context,
	queueName string,
	messageID uuid.UUID,
	payload []byte,
	metadata map[string]any,
) (uuid.UUID, error) {
	queueMeta, err := pq.resolveQueueMetadata(ctx, queueName)
	if err != nil {
		return uuid.UUID{}, err
	}

	if err := pq.validatePayloadSize(queueMeta, payload); err != nil {
		return uuid.UUID{}, err
	}

	// Generate message ID if not provided
	if messageID == uuid.Nil {
		messageID, err = NewUUIDv7()
		if err != nil {
			return uuid.UUID{}, fmt.Errorf(
				"failed to generate message ID: %w", err,
			)
		}
	}

	// Marshal metadata if provided
	var metadataJSON []byte
	if metadata != nil {
		metadataJSON, err = json.Marshal(metadata)
		if err != nil {
			return uuid.UUID{}, fmt.Errorf(
				"failed to marshal metadata: %w", err,
			)
		}
	}

	// Publish based on queue type
	queueType := QueueType(queueMeta.QueueType)
	if queueType == QueueTypePubSub {
		return messageID, pq.publishToPubSub(
			ctx, queueMeta.QueueName, queueMeta.TableName,
			messageID, payload, metadataJSON,
		)
	}

	maxRetries := pq.resolveMaxRetries(queueMeta)

	return messageID, pq.publishToChannel(
		ctx, queueMeta.TableName,
		messageID, payload, metadataJSON, maxRetries,
	)
}

func (pq *PGQueue) resolveQueueMetadata(
	ctx context.Context,
	queueName string,
) (*QueueMetadata, error) {
	// Try pub/sub first
	queueMeta, err := pq.getQueueMetadata(
		ctx, string(QueueTypePubSub), queueName,
	)
	if errors.Is(err, sql.ErrNoRows) {
		// Try channel
		queueMeta, err = pq.getQueueMetadata(
			ctx, string(QueueTypeChannel), queueName,
		)
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf(
				"%s: %w", queueName, ErrQueueNotFound,
			)
		}
		return nil, fmt.Errorf("failed to get queue metadata: %w", err)
	}

	return queueMeta, nil
}

func (pq *PGQueue) validatePayloadSize(
	queueMeta *QueueMetadata,
	payload []byte,
) error {
	var config struct {
		MaxMessageSize int `json:"MaxMessageSize"`
	}
	if err := json.Unmarshal(queueMeta.Config, &config); err != nil {
		config.MaxMessageSize = pq.config.MaxMessageSize
	}
	if config.MaxMessageSize == 0 {
		config.MaxMessageSize = pq.config.MaxMessageSize
	}

	if len(payload) > config.MaxMessageSize {
		return fmt.Errorf(
			"size %d exceeds limit %d: %w",
			len(payload), config.MaxMessageSize, ErrMessageSizeExceeded,
		)
	}

	return nil
}

func (pq *PGQueue) resolveMaxRetries(
	queueMeta *QueueMetadata,
) int {
	var channelOpts struct {
		MaxRetries int `json:"MaxRetries"`
	}
	maxRetries := pq.config.DefaultMaxRetries
	if err := json.Unmarshal(queueMeta.Config, &channelOpts); err == nil &&
		channelOpts.MaxRetries > 0 {
		maxRetries = channelOpts.MaxRetries
	}

	return maxRetries
}

// publishToPubSub publishes a message to a pub/sub topic.
func (pq *PGQueue) publishToPubSub(
	ctx context.Context,
	topicName, tableName string,
	messageID uuid.UUID,
	payload []byte,
	metadata []byte,
) error {
	// Begin transaction
	tx, err := pq.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := pq.checkDuplicateMessage(ctx, tx, tableName, messageID); err != nil {
		return err
	}

	// Insert message
	//nolint:gosec // G201: table name validated by queueNameRegex
	insertMsg := fmt.Sprintf(`
		INSERT INTO pgqueue_msg_%s (id, payload, metadata)
		VALUES ($1, $2, $3)
	`, tableName)

	_, err = tx.ExecContext(ctx, insertMsg, messageID, payload, metadata)
	if err != nil {
		return fmt.Errorf("failed to insert message: %w", err)
	}

	if err := pq.createSubscriptionRecords(
		ctx, tx, topicName, tableName, messageID,
	); err != nil {
		return err
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (pq *PGQueue) checkDuplicateMessage(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	messageID uuid.UUID,
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
	checkQuery := fmt.Sprintf(
		`SELECT id FROM pgqueue_msg_%s WHERE id = $1`, tableName,
	)
	var existingID uuid.UUID
	err := tx.QueryRowContext(ctx, checkQuery, messageID).Scan(&existingID)
	if err == nil {
		return fmt.Errorf("%s: %w", messageID, ErrDuplicateMessageID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to check for duplicate: %w", err)
	}

	return nil
}

func (pq *PGQueue) createSubscriptionRecords(
	ctx context.Context,
	tx *sql.Tx,
	topicName, tableName string,
	messageID uuid.UUID,
) error {
	// Get active subscribers
	subscribers, err := pq.getActiveSubscribers(ctx, tx, topicName)
	if err != nil {
		return fmt.Errorf("failed to get active subscribers: %w", err)
	}

	if len(subscribers) == 0 {
		return nil
	}

	//nolint:gosec // G201: table name validated by queueNameRegex
	insertSub := fmt.Sprintf(`
		INSERT INTO pgqueue_sub_%s (message_id, subscriber_id, status)
		VALUES ($1, $2, 'pending')
	`, tableName)

	stmt, err := tx.PrepareContext(ctx, insertSub)
	if err != nil {
		return fmt.Errorf("failed to prepare subscription insert: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, sub := range subscribers {
		if _, err := stmt.ExecContext(
			ctx, messageID, sub.SubscriberID,
		); err != nil {
			return fmt.Errorf(
				"failed to create subscription record: %w", err,
			)
		}
	}

	return nil
}

// publishToChannel publishes a message to a point-to-point channel.
func (pq *PGQueue) publishToChannel(
	ctx context.Context,
	tableName string,
	messageID uuid.UUID,
	payload []byte,
	metadata []byte,
	maxRetries int,
) error {
	// Check for duplicate message ID (deduplication)
	//nolint:gosec // G201: table name validated by queueNameRegex
	checkQuery := fmt.Sprintf(
		`SELECT id FROM pgqueue_msg_%s WHERE id = $1`, tableName,
	)
	var existingID uuid.UUID
	err := pq.db.QueryRowContext(ctx, checkQuery, messageID).Scan(&existingID)
	if err == nil {
		return fmt.Errorf("%s: %w", messageID, ErrDuplicateMessageID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to check for duplicate: %w", err)
	}

	// Insert message
	//nolint:gosec // G201: table name validated by queueNameRegex
	insertMsg := fmt.Sprintf(`
		INSERT INTO pgqueue_msg_%s (id, payload, status, metadata, max_retries)
		VALUES ($1, $2, 'pending', $3, $4)
	`, tableName)

	_, err = pq.db.ExecContext(
		ctx, insertMsg, messageID, payload, metadata, maxRetries,
	)
	if err != nil {
		return fmt.Errorf("failed to insert message: %w", err)
	}

	return nil
}
