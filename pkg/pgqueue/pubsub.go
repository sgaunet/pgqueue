package pgqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Subscribe registers a subscriber for a topic and returns a channel to consume messages.
func (pq *PGQueue) Subscribe(
	ctx context.Context,
	topicName, subscriberID string,
) error {
	// Verify topic exists
	_, err := pq.getQueueMetadata(ctx, string(QueueTypePubSub), topicName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%s: %w", topicName, ErrTopicNotFound)
		}
		return fmt.Errorf("failed to get topic metadata: %w", err)
	}

	// Register subscriber
	_, err = pq.registerSubscriber(ctx, topicName, subscriberID)
	if err != nil {
		return fmt.Errorf("failed to register subscriber: %w", err)
	}

	return nil
}

// Unsubscribe removes a subscriber from a topic.
func (pq *PGQueue) Unsubscribe(
	ctx context.Context,
	topicName, subscriberID string,
) error {
	err := pq.unregisterSubscriber(ctx, topicName, subscriberID)
	if err != nil {
		return fmt.Errorf("failed to unsubscribe: %w", err)
	}

	return nil
}

// ConsumeFromTopic retrieves the next available message for a subscriber from a topic.
// Returns nil message if no messages available.
func (pq *PGQueue) ConsumeFromTopic(
	ctx context.Context,
	topicName, subscriberID string,
	visibilityTimeout time.Duration,
) (*Message, error) {
	// Get queue metadata
	queueMeta, err := pq.getQueueMetadata(
		ctx, string(QueueTypePubSub), topicName,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic metadata: %w", err)
	}

	if queueMeta.Paused {
		return nil, ErrQueuePaused
	}

	// Begin transaction
	tx, err := pq.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	ttl := pq.getQueueTTL(queueMeta.Config)

	msg, visTimeout, err := pq.fetchPendingTopicMessage(
		ctx, tx, queueMeta.TableName, subscriberID, visibilityTimeout, ttl,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch pending topic message: %w", err)
	}
	if msg == nil {
		_ = tx.Rollback()
		return nil, nil //nolint:nilnil // nil message indicates no messages available
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	msg.VisibilityTimeout = visTimeout

	return msg, nil
}

// AckTopic acknowledges a message for a subscriber.
func (pq *PGQueue) AckTopic(
	ctx context.Context,
	topicName, subscriberID string,
	messageID uuid.UUID,
) error {
	queueMeta, err := pq.getQueueMetadata(
		ctx, string(QueueTypePubSub), topicName,
	)
	if err != nil {
		return fmt.Errorf("failed to get topic metadata: %w", err)
	}

	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		UPDATE pgqueue_sub_%s
		SET status = 'acked', acked_at = NOW()
		WHERE message_id = $1
		  AND subscriber_id = $2
		  AND status = 'processing'
	`, queueMeta.TableName)

	result, err := pq.db.ExecContext(ctx, query, messageID, subscriberID)
	if err != nil {
		return fmt.Errorf("failed to acknowledge message: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return ErrMessageAlreadyAcked
	}

	return nil
}

// NackTopic negatively acknowledges a message for a subscriber (retry).
func (pq *PGQueue) NackTopic(
	ctx context.Context,
	topicName, subscriberID string,
	messageID uuid.UUID,
	errorMsg string,
) error {
	queueMeta, err := pq.getQueueMetadata(
		ctx, string(QueueTypePubSub), topicName,
	)
	if err != nil {
		return fmt.Errorf("failed to get topic metadata: %w", err)
	}

	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		UPDATE pgqueue_sub_%s
		SET status = 'pending',
		    retry_count = retry_count + 1,
		    visibility_timeout = NULL,
		    error_message = $3
		WHERE message_id = $1
		  AND subscriber_id = $2
		  AND status = 'processing'
	`, queueMeta.TableName)

	result, err := pq.db.ExecContext(
		ctx, query, messageID, subscriberID, errorMsg,
	)
	if err != nil {
		return fmt.Errorf("failed to nack message: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return ErrMessageNotFound
	}

	return nil
}

func (pq *PGQueue) fetchPendingTopicMessage(
	ctx context.Context,
	tx *sql.Tx,
	tableName, subscriberID string,
	visibilityTimeout time.Duration,
	ttl time.Duration,
) (*Message, *time.Time, error) {
	ttlClause := ""
	args := []any{subscriberID}

	if ttl > 0 {
		ttlClause = "AND m.created_at > $2"
		args = append(args, time.Now().Add(-ttl))
	}

	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		SELECT s.id, s.message_id, m.payload, m.created_at,
		       s.retry_count, m.metadata
		FROM pgqueue_sub_%s s
		JOIN pgqueue_msg_%s m ON s.message_id = m.id
		WHERE s.subscriber_id = $1
		  AND s.status = 'pending'
		  AND (s.visibility_timeout IS NULL
		       OR s.visibility_timeout < NOW())
		  %s
		ORDER BY m.id
		LIMIT 1
		FOR UPDATE OF s SKIP LOCKED
	`, tableName, tableName, ttlClause)

	var subID uuid.UUID
	var msgID uuid.UUID
	var payload []byte
	var createdAt time.Time
	var retryCount int
	var metadataJSON sql.NullString

	err := tx.QueryRowContext(ctx, query, args...).Scan(
		&subID, &msgID, &payload, &createdAt,
		&retryCount, &metadataJSON,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query subscription: %w", err)
	}

	// Update subscription status and visibility timeout
	visTimeout := time.Now().Add(visibilityTimeout)

	//nolint:gosec // G201: table name validated by queueNameRegex
	updateQuery := fmt.Sprintf(`
		UPDATE pgqueue_sub_%s
		SET status = 'processing', visibility_timeout = $1
		WHERE id = $2
	`, tableName)

	_, err = tx.ExecContext(ctx, updateQuery, visTimeout, subID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to update subscription: %w", err)
	}

	msg := &Message{
		ID:         msgID,
		Payload:    payload,
		CreatedAt:  createdAt,
		Status:     MessageStatusProcessing,
		RetryCount: retryCount,
		Metadata:   parseMetadataJSON(metadataJSON),
	}

	return msg, &visTimeout, nil
}
