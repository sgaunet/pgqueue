package pgqueue

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sgaunet/pgqueue/internal/db"
)

// ConsumeFromChannel retrieves the next available message from a channel
// Returns nil if no messages are available
func (pq *PGQueue) ConsumeFromChannel(ctx context.Context, channelName string, visibilityTimeout time.Duration) (*Message, error) {
	// Get queue metadata
	queueMeta, err := pq.queries.GetQueueMetadata(ctx, db.GetQueueMetadataParams{
		QueueType: string(QueueTypeChannel),
		QueueName: channelName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get channel metadata: %w", err)
	}

	// Begin transaction
	tx, err := pq.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Get next pending message using FOR UPDATE SKIP LOCKED
	query := fmt.Sprintf(`
		SELECT id, payload, created_at, retry_count, max_retries, metadata
		FROM pgqueue_msg_%s
		WHERE status = 'pending'
		  AND (visibility_timeout IS NULL OR visibility_timeout < NOW())
		ORDER BY id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, queueMeta.TableName)

	var msgID uuid.UUID
	var payload []byte
	var createdAt time.Time
	var retryCount int
	var maxRetries sql.NullInt32
	var metadataJSON sql.NullString

	err = tx.QueryRowContext(ctx, query).Scan(
		&msgID, &payload, &createdAt, &retryCount, &maxRetries, &metadataJSON,
	)
	if err == sql.ErrNoRows {
		tx.Rollback()
		return nil, nil // No messages available
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query message: %w", err)
	}

	// Update message status and visibility timeout
	visTimeout := time.Now().Add(visibilityTimeout)
	updateQuery := fmt.Sprintf(`
		UPDATE pgqueue_msg_%s
		SET status = 'processing', visibility_timeout = $1
		WHERE id = $2
	`, queueMeta.TableName)

	_, err = tx.ExecContext(ctx, updateQuery, visTimeout, msgID)
	if err != nil {
		return nil, fmt.Errorf("failed to update message: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	msg := &Message{
		ID:                msgID,
		Payload:           payload,
		CreatedAt:         createdAt,
		Status:            MessageStatusProcessing,
		RetryCount:        retryCount,
		VisibilityTimeout: &visTimeout,
	}

	if maxRetries.Valid {
		msg.MaxRetries = int(maxRetries.Int32)
	}

	return msg, nil
}

// AckChannel acknowledges a message from a channel (marks as completed)
func (pq *PGQueue) AckChannel(ctx context.Context, channelName string, messageID uuid.UUID) error {
	queueMeta, err := pq.queries.GetQueueMetadata(ctx, db.GetQueueMetadataParams{
		QueueType: string(QueueTypeChannel),
		QueueName: channelName,
	})
	if err != nil {
		return fmt.Errorf("failed to get channel metadata: %w", err)
	}

	query := fmt.Sprintf(`
		UPDATE pgqueue_msg_%s
		SET status = 'completed', processed_at = NOW()
		WHERE id = $1 AND status = 'processing'
	`, queueMeta.TableName)

	result, err := pq.db.ExecContext(ctx, query, messageID)
	if err != nil {
		return fmt.Errorf("failed to acknowledge message: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("message not found or not in processing state")
	}

	return nil
}

// NackChannel negatively acknowledges a message from a channel (retry or move to DLQ)
func (pq *PGQueue) NackChannel(ctx context.Context, channelName string, messageID uuid.UUID, errorMsg string) error {
	queueMeta, err := pq.queries.GetQueueMetadata(ctx, db.GetQueueMetadataParams{
		QueueType: string(QueueTypeChannel),
		QueueName: channelName,
	})
	if err != nil {
		return fmt.Errorf("failed to get channel metadata: %w", err)
	}

	// Begin transaction
	tx, err := pq.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Get current message state
	query := fmt.Sprintf(`
		SELECT retry_count, max_retries, payload, metadata
		FROM pgqueue_msg_%s
		WHERE id = $1 AND status = 'processing'
		FOR UPDATE
	`, queueMeta.TableName)

	var retryCount int
	var maxRetries sql.NullInt32
	var payload []byte
	var metadataJSON sql.NullString

	err = tx.QueryRowContext(ctx, query, messageID).Scan(&retryCount, &maxRetries, &payload, &metadataJSON)
	if err == sql.ErrNoRows {
		tx.Rollback()
		return fmt.Errorf("message not found or not in processing state")
	}
	if err != nil {
		return fmt.Errorf("failed to query message: %w", err)
	}

	// Determine max retries (use default if not set)
	maxRetry := pq.config.DefaultMaxRetries
	if maxRetries.Valid && maxRetries.Int32 > 0 {
		maxRetry = int(maxRetries.Int32)
	}

	// Check if we've exceeded max retries
	if retryCount+1 > maxRetry {
		// Move to DLQ
		dlqQuery := fmt.Sprintf(`
			INSERT INTO pgqueue_dlq_%s (original_message_id, payload, failure_reason, retry_count, metadata)
			VALUES ($1, $2, $3, $4, $5)
		`, queueMeta.TableName)

		_, err = tx.ExecContext(ctx, dlqQuery, messageID, payload, errorMsg, retryCount+1, metadataJSON)
		if err != nil {
			return fmt.Errorf("failed to insert into DLQ: %w", err)
		}

		// Delete message from main queue
		deleteQuery := fmt.Sprintf(`DELETE FROM pgqueue_msg_%s WHERE id = $1`, queueMeta.TableName)
		_, err = tx.ExecContext(ctx, deleteQuery, messageID)
		if err != nil {
			return fmt.Errorf("failed to delete message: %w", err)
		}
	} else {
		// Retry: reset to pending
		updateQuery := fmt.Sprintf(`
			UPDATE pgqueue_msg_%s
			SET status = 'pending',
			    retry_count = retry_count + 1,
			    visibility_timeout = NULL,
			    error_message = $2
			WHERE id = $1
		`, queueMeta.TableName)

		_, err = tx.ExecContext(ctx, updateQuery, messageID, errorMsg)
		if err != nil {
			return fmt.Errorf("failed to update message: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
