package pgqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

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
func (pq *Queue) ConsumeFromChannel(
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
//
// The receipt must carry the claim token issued by the ConsumeFromChannel call
// that delivered the message (use msg.Receipt()). If the claim has since
// expired — the visibility timeout lapsed and the message was redelivered to
// another consumer — AckChannel returns ErrClaimExpired and does nothing.
func (pq *Queue) AckChannel(
	ctx context.Context,
	channelName string,
	r Receipt,
) error {
	// Use the metadata cache for the table name: Ack does not need the mutable
	// paused flag or config, so a cache hit avoids the full metadata round-trip.
	tableName, err := pq.cachedTableName(ctx, string(QueueTypeChannel), channelName)
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
		WHERE id = $1 AND claim_id = $2 AND status = '%s'
	`, tableName, MessageStatusCompleted, MessageStatusProcessing)

	result, err := pq.db.ExecContext(ctx, query, r.MessageID, r.ClaimID)
	if err != nil {
		return fmt.Errorf("failed to acknowledge message: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return classifyChannelAckMiss(ctx, pq.db, tableName, r)
	}

	return nil
}

// NackChannel negatively acknowledges a message from a channel (retry or move to DLQ).
// The errorMsg is truncated to 1024 characters if it exceeds that length.
//
// The receipt must carry the claim token from the consume call that delivered
// the message; a stale claim returns ErrClaimExpired (see AckChannel).
func (pq *Queue) NackChannel(
	ctx context.Context,
	channelName string,
	r Receipt,
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
		ctx, tx, queueMeta.TableName, r,
	)
	if err != nil {
		return fmt.Errorf("failed to get message state: %w", err)
	}

	if err := pq.handleNack(
		ctx, tx, queueMeta.TableName, r.MessageID, errorMsg, msgState,
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

// errReasonVisibilityTimeout is the DLQ failure reason recorded when a message
// is moved to the dead-letter queue because it was reclaimed after a visibility
// timeout too many times without ever being acknowledged.
const errReasonVisibilityTimeout = "exceeded max retries: not acknowledged before visibility timeout"

// channelMaxRetries resolves the effective retry limit for a channel message:
// its per-message max_retries when set to a positive value, otherwise the
// configured default.
func channelMaxRetries(defaultMax int, maxRetries sql.NullInt32) int {
	if maxRetries.Valid && maxRetries.Int32 > 0 {
		return int(maxRetries.Int32)
	}
	return defaultMax
}

// channelCandidate is one consumable row scanned from a channel message table.
type channelCandidate struct {
	id           uuid.UUID
	payload      []byte
	createdAt    time.Time
	status       string
	retryCount   int
	maxRetries   sql.NullInt32
	metadataJSON sql.NullString
	processedAt  sql.NullTime
	errorMessage sql.NullString
}

func (pq *Queue) fetchPendingChannelMessage(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	visibilityTimeout time.Duration,
	ttl time.Duration,
) (*Message, *time.Time, error) {
	query, args := channelConsumeQuery(tableName, ttl)

	// Loop so that a message reclaimed after a visibility timeout which has
	// exhausted its retries is moved to the DLQ and skipped, rather than being
	// redelivered forever (a consumer that always crashes never sends a Nack).
	for {
		var row channelCandidate
		err := tx.QueryRowContext(ctx, query, args...).Scan(
			&row.id, &row.payload, &row.createdAt, &row.status,
			&row.retryCount, &row.maxRetries, &row.metadataJSON,
			&row.processedAt, &row.errorMessage,
		)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, nil
		}
		if err != nil {
			return nil, nil, fmt.Errorf("failed to query message: %w", err)
		}

		retryCount, dlqd, err := pq.reclaimChannelAttempt(ctx, tx, tableName, row)
		if err != nil {
			return nil, nil, err
		}
		if dlqd {
			continue
		}

		return pq.claimChannelMessage(
			ctx, tx, tableName, visibilityTimeout, row, retryCount,
		)
	}
}

// channelConsumeQuery builds the SELECT for the next consumable channel message.
// A message is consumable when it is pending, or when it is still marked
// processing but its visibility timeout has expired (the previous consumer
// crashed or never acked). Reclaiming timed-out messages here means redelivery
// does not depend on the GarbageCollector running.
func channelConsumeQuery(tableName string, ttl time.Duration) (string, []any) {
	ttlClause := ""
	var args []any
	if ttl > 0 {
		ttlClause = "AND created_at > NOW() - make_interval(secs => $1)"
		args = append(args, ttl.Seconds())
	}

	query := fmt.Sprintf(`
		SELECT id, payload, created_at, status, retry_count, max_retries,
		       metadata, processed_at, error_message
		FROM pgqueue_msg_%s
		WHERE (status = '%s'
		       OR (status = '%s' AND visibility_timeout < NOW()))
		  %s
		ORDER BY id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, tableName, MessageStatusPending, MessageStatusProcessing, ttlClause)

	return query, args
}

// reclaimChannelAttempt accounts for a redelivery when a candidate was picked
// up in 'processing' state — a visibility timeout where the previous consumer
// never acked. It returns the retry count to claim with; if the retries are now
// exhausted it moves the message to the DLQ and reports dlqd=true so the caller
// skips it.
func (pq *Queue) reclaimChannelAttempt(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	row channelCandidate,
) (int, bool, error) {
	retryCount := row.retryCount
	if MessageStatus(row.status) != MessageStatusProcessing {
		return retryCount, false, nil
	}

	retryCount++
	if retryCount > channelMaxRetries(pq.config.DefaultMaxRetries, row.maxRetries) {
		if err := pq.moveToDLQ(
			ctx, tx, tableName, row.id, errReasonVisibilityTimeout,
			row.payload, retryCount, row.metadataJSON,
		); err != nil {
			return 0, false, fmt.Errorf("failed to move timed-out message to DLQ: %w", err)
		}

		return 0, true, nil
	}

	return retryCount, false, nil
}

//nolint:gosec // G201: table name validated by queueNameRegex
func (pq *Queue) claimChannelMessage(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	visibilityTimeout time.Duration,
	row channelCandidate,
	retryCount int,
) (*Message, *time.Time, error) {
	// Compute the visibility deadline on the database clock (NOW()) rather than
	// the application clock, so consumers and the GC agree on expiry regardless
	// of clock skew between the app server and PostgreSQL. retry_count is
	// written back so a reclaim-driven increment is persisted. A fresh claim_id
	// is minted on every (re)delivery: it fences a previous consumer whose
	// visibility timeout lapsed from acknowledging this now-reassigned message.
	updateQuery := fmt.Sprintf(`
		UPDATE pgqueue_msg_%s
		SET status = '%s',
		    retry_count = $3,
		    claim_id = uuidv7(),
		    visibility_timeout = NOW() + make_interval(secs => $1)
		WHERE id = $2
		RETURNING visibility_timeout, claim_id
	`, tableName, MessageStatusProcessing)

	var visTimeout time.Time
	var claimID uuid.UUID
	if err := tx.QueryRowContext(
		ctx, updateQuery, visibilityTimeout.Seconds(), row.id, retryCount,
	).Scan(&visTimeout, &claimID); err != nil {
		return nil, nil, fmt.Errorf("failed to update message: %w", err)
	}

	msg := &Message{
		ID:         row.id,
		ClaimID:    claimID,
		Payload:    row.payload,
		CreatedAt:  row.createdAt,
		Status:     MessageStatusProcessing,
		RetryCount: retryCount,
		Metadata:   pq.parseMetadataJSON(row.metadataJSON),
	}

	if row.maxRetries.Valid {
		msg.MaxRetries = int(row.maxRetries.Int32)
	}
	if row.processedAt.Valid {
		msg.ProcessedAt = &row.processedAt.Time
	}
	if row.errorMessage.Valid {
		msg.ErrorMessage = &row.errorMessage.String
	}

	return msg, &visTimeout, nil
}

func (pq *Queue) getProcessingMessageState(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	r Receipt,
) (*messageState, error) {
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		SELECT retry_count, max_retries, payload, metadata
		FROM pgqueue_msg_%s
		WHERE id = $1 AND claim_id = $2 AND status = '%s'
		FOR UPDATE
	`, tableName, MessageStatusProcessing)

	var state messageState
	err := tx.QueryRowContext(ctx, query, r.MessageID, r.ClaimID).Scan(
		&state.retryCount, &state.maxRetries,
		&state.payload, &state.metadataJSON,
	)
	if errors.Is(err, sql.ErrNoRows) {
		// No processing row under this claim: explain why (expired vs. gone).
		return nil, classifyChannelAckMiss(ctx, tx, tableName, r)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query message: %w", err)
	}

	return &state, nil
}

func (pq *Queue) handleNack(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	messageID uuid.UUID,
	errorMsg string,
	state *messageState,
) error {
	// Determine max retries (use default if not set)
	maxRetry := channelMaxRetries(pq.config.DefaultMaxRetries, state.maxRetries)

	// Check if we've exceeded max retries
	if state.retryCount+1 > maxRetry {
		return pq.moveToDLQ(
			ctx, tx, tableName, messageID, errorMsg,
			state.payload, state.retryCount+1, state.metadataJSON,
		)
	}

	return pq.retryMessage(ctx, tx, tableName, messageID, errorMsg)
}

func (pq *Queue) moveToDLQ(
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

func (pq *Queue) retryMessage(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	messageID uuid.UUID,
	errorMsg string,
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
	// claim_id is cleared so a stale receipt held by the previous consumer
	// resolves to ErrClaimExpired rather than ErrMessageAlreadyAcked.
	updateQuery := fmt.Sprintf(`
		UPDATE pgqueue_msg_%s
		SET status = '%s',
		    claim_id = NULL,
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

// truncateErrorMsg caps an error message at maxErrorMessageLength bytes. It
// truncates on a UTF-8 rune boundary: slicing a multi-byte string at an
// arbitrary byte offset can split a rune and produce invalid UTF-8, which
// PostgreSQL rejects on a TEXT column. Any trailing partial sequence is dropped.
func truncateErrorMsg(msg string) string {
	if len(msg) <= maxErrorMessageLength {
		return msg
	}
	truncated := msg[:maxErrorMessageLength]
	for len(truncated) > 0 && !utf8.ValidString(truncated) {
		truncated = truncated[:len(truncated)-1]
	}
	return truncated
}
