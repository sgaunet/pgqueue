package pgqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	minVisibilityTimeout = 1 * time.Millisecond
	maxVisibilityTimeout = 24 * time.Hour

	// maxErrorMessageLength is the maximum stored length for nack error messages.
	maxErrorMessageLength = 1024
)

func validateVisibilityTimeout(d time.Duration) error {
	if d < minVisibilityTimeout || d > maxVisibilityTimeout {
		return ErrInvalidVisibilityTimeout
	}

	return nil
}

// ConsumeFromChannel retrieves the next available message from a channel.
// Returns nil if no messages are available.
func (pq *PGQueue) ConsumeFromChannel(
	ctx context.Context,
	channelName string,
	visibilityTimeout time.Duration,
) (*Message, error) {
	if err := validateVisibilityTimeout(visibilityTimeout); err != nil {
		return nil, err
	}

	// Get queue metadata
	queueMeta, err := pq.getQueueMetadata(
		ctx, string(QueueTypeChannel), channelName,
	)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return nil, fmt.Errorf("%s: %w", channelName, ErrQueueNotFound)
		}
		return nil, fmt.Errorf("failed to get channel metadata: %w", err)
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

	msg, visTimeout, err := pq.fetchPendingChannelMessage(
		ctx, tx, queueMeta.TableName, visibilityTimeout, ttl,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch pending channel message: %w", err)
	}
	if msg == nil {
		return nil, nil //nolint:nilnil // nil message indicates no messages available
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	msg.VisibilityTimeout = visTimeout

	return msg, nil
}

// AckChannel acknowledges a message from a channel (marks as completed).
func (pq *PGQueue) AckChannel(
	ctx context.Context,
	channelName string,
	messageID uuid.UUID,
) error {
	queueMeta, err := pq.getQueueMetadata(
		ctx, string(QueueTypeChannel), channelName,
	)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return fmt.Errorf("%s: %w", channelName, ErrQueueNotFound)
		}
		return fmt.Errorf("failed to get channel metadata: %w", err)
	}

	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		UPDATE pgqueue_msg_%s
		SET status = '%s', processed_at = NOW()
		WHERE id = $1 AND status = '%s'
	`, queueMeta.TableName, MessageStatusCompleted, MessageStatusProcessing)

	result, err := pq.db.ExecContext(ctx, query, messageID)
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

// NackChannel negatively acknowledges a message from a channel (retry or move to DLQ).
// The errorMsg is truncated to 1024 characters if it exceeds that length.
func (pq *PGQueue) NackChannel(
	ctx context.Context,
	channelName string,
	messageID uuid.UUID,
	errorMsg string,
) error {
	errorMsg = truncateErrorMsg(errorMsg)
	queueMeta, err := pq.getQueueMetadata(
		ctx, string(QueueTypeChannel), channelName,
	)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return fmt.Errorf("%s: %w", channelName, ErrQueueNotFound)
		}
		return fmt.Errorf("failed to get channel metadata: %w", err)
	}

	// Begin transaction
	tx, err := pq.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Get current message state
	msgState, err := pq.getProcessingMessageState(
		ctx, tx, queueMeta.TableName, messageID,
	)
	if err != nil {
		return fmt.Errorf("failed to get message state: %w", err)
	}

	if err := pq.handleNack(
		ctx, tx, queueMeta.TableName, messageID, errorMsg, msgState,
	); err != nil {
		return fmt.Errorf("failed to handle nack: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

type messageState struct {
	retryCount   int
	maxRetries   sql.NullInt32
	payload      []byte
	metadataJSON sql.NullString
}

func (pq *PGQueue) fetchPendingChannelMessage(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	visibilityTimeout time.Duration,
	ttl time.Duration,
) (*Message, *time.Time, error) {
	ttlClause := ""
	var args []any

	if ttl > 0 {
		ttlClause = "AND created_at > $1"
		args = append(args, time.Now().Add(-ttl))
	}

	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		SELECT id, payload, created_at, retry_count, max_retries, metadata,
		       processed_at, error_message
		FROM pgqueue_msg_%s
		WHERE status = '%s'
		  AND (visibility_timeout IS NULL OR visibility_timeout < NOW())
		  %s
		ORDER BY id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, tableName, MessageStatusPending, ttlClause)

	var msgID uuid.UUID
	var payload []byte
	var createdAt time.Time
	var retryCount int
	var maxRetries sql.NullInt32
	var metadataJSON sql.NullString
	var processedAt sql.NullTime
	var errorMessage sql.NullString

	err := tx.QueryRowContext(ctx, query, args...).Scan(
		&msgID, &payload, &createdAt,
		&retryCount, &maxRetries, &metadataJSON,
		&processedAt, &errorMessage,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query message: %w", err)
	}

	return pq.claimChannelMessage(
		ctx, tx, tableName, visibilityTimeout,
		msgID, payload, createdAt, retryCount, maxRetries, metadataJSON,
		processedAt, errorMessage,
	)
}

//nolint:gosec // G201: table name validated by queueNameRegex
func (pq *PGQueue) claimChannelMessage(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	visibilityTimeout time.Duration,
	msgID uuid.UUID,
	payload []byte,
	createdAt time.Time,
	retryCount int,
	maxRetries sql.NullInt32,
	metadataJSON sql.NullString,
	processedAt sql.NullTime,
	errorMessage sql.NullString,
) (*Message, *time.Time, error) {
	visTimeout := time.Now().Add(visibilityTimeout)

	updateQuery := fmt.Sprintf(`
		UPDATE pgqueue_msg_%s
		SET status = '%s', visibility_timeout = $1
		WHERE id = $2
	`, tableName, MessageStatusProcessing)

	_, err := tx.ExecContext(ctx, updateQuery, visTimeout, msgID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to update message: %w", err)
	}

	msg := &Message{
		ID:         msgID,
		Payload:    payload,
		CreatedAt:  createdAt,
		Status:     MessageStatusProcessing,
		RetryCount: retryCount,
		Metadata:   parseMetadataJSON(metadataJSON),
	}

	if maxRetries.Valid {
		msg.MaxRetries = int(maxRetries.Int32)
	}
	if processedAt.Valid {
		msg.ProcessedAt = &processedAt.Time
	}
	if errorMessage.Valid {
		msg.ErrorMessage = &errorMessage.String
	}

	return msg, &visTimeout, nil
}

func (pq *PGQueue) getProcessingMessageState(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	messageID uuid.UUID,
) (*messageState, error) {
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		SELECT retry_count, max_retries, payload, metadata
		FROM pgqueue_msg_%s
		WHERE id = $1 AND status = '%s'
		FOR UPDATE
	`, tableName, MessageStatusProcessing)

	var state messageState
	err := tx.QueryRowContext(ctx, query, messageID).Scan(
		&state.retryCount, &state.maxRetries,
		&state.payload, &state.metadataJSON,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrMessageNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query message: %w", err)
	}

	return &state, nil
}

func (pq *PGQueue) handleNack(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	messageID uuid.UUID,
	errorMsg string,
	state *messageState,
) error {
	// Determine max retries (use default if not set)
	maxRetry := pq.config.DefaultMaxRetries
	if state.maxRetries.Valid && state.maxRetries.Int32 > 0 {
		maxRetry = int(state.maxRetries.Int32)
	}

	// Check if we've exceeded max retries
	if state.retryCount+1 > maxRetry {
		return pq.moveToDLQ(
			ctx, tx, tableName, messageID, errorMsg,
			state.payload, state.retryCount+1, state.metadataJSON,
		)
	}

	return pq.retryMessage(ctx, tx, tableName, messageID, errorMsg)
}

func (pq *PGQueue) moveToDLQ(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	messageID uuid.UUID,
	errorMsg string,
	payload []byte,
	retryCount int,
	metadataJSON sql.NullString,
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
	dlqQuery := fmt.Sprintf(`
		INSERT INTO pgqueue_dlq_%s
			(original_message_id, payload, failure_reason, retry_count, metadata)
		VALUES ($1, $2, $3, $4, $5)
	`, tableName)

	_, err := tx.ExecContext(
		ctx, dlqQuery, messageID, payload, errorMsg,
		retryCount, metadataJSON,
	)
	if err != nil {
		return fmt.Errorf("failed to insert into DLQ: %w", err)
	}

	// Delete message from main queue
	//nolint:gosec // G201: table name validated by queueNameRegex
	deleteQuery := fmt.Sprintf(
		`DELETE FROM pgqueue_msg_%s WHERE id = $1`, tableName,
	)
	_, err = tx.ExecContext(ctx, deleteQuery, messageID)
	if err != nil {
		return fmt.Errorf("failed to delete message: %w", err)
	}

	return nil
}

func (pq *PGQueue) retryMessage(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	messageID uuid.UUID,
	errorMsg string,
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
	updateQuery := fmt.Sprintf(`
		UPDATE pgqueue_msg_%s
		SET status = '%s',
		    retry_count = retry_count + 1,
		    visibility_timeout = NULL,
		    error_message = $2
		WHERE id = $1
	`, tableName, MessageStatusPending)

	_, err := tx.ExecContext(ctx, updateQuery, messageID, errorMsg)
	if err != nil {
		return fmt.Errorf("failed to update message: %w", err)
	}

	return nil
}

func truncateErrorMsg(msg string) string {
	if len(msg) > maxErrorMessageLength {
		return msg[:maxErrorMessageLength]
	}
	return msg
}
