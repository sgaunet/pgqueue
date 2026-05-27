package pgqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
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

// maxConsumeReclaimDefers bounds how many timed-out messages a single consume
// call will skip (defer under a backoff policy, or promote to the DLQ) before
// it gives up and returns empty. Without this cap a large crash-loop backlog
// would make one consume call walk — and row-lock — the entire backlog inside
// a single transaction. The caller's poll/notify loop retries promptly, and
// the garbage collector recovers timed-out messages in bulk regardless.
const maxConsumeReclaimDefers = 64

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
	if err := pq.checkClosed(); err != nil {
		return nil, err
	}
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
	tx, err := pq.db.BeginTx(ctx, readCommittedTxOptions)
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
	// Commit unconditionally — even with no message to deliver. The scan may
	// have deferred timed-out messages with a backoff delay (R-05), and those
	// UPDATEs must persist rather than be discarded by the deferred Rollback.
	// With no message and no deferral the transaction is read-only, so the
	// commit is harmless.
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	if msg == nil {
		return nil, nil //nolint:nilnil // nil message indicates no messages available
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
		UPDATE %s
		SET status = '%s', processed_at = NOW()
		WHERE id = $1 AND claim_id = $2 AND status = '%s'
	`, pq.msgTable(tableName), MessageStatusCompleted, MessageStatusProcessing)

	// Run the UPDATE and, on a miss, the classifying SELECT in one transaction
	// so the classification observes the same snapshot as the failed UPDATE —
	// a concurrent reclaim cannot slip in between and flip the error type (R-09).
	tx, err := pq.db.BeginTx(ctx, readCommittedTxOptions)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	result, err := tx.ExecContext(ctx, query, r.MessageID, r.ClaimID)
	if err != nil {
		return fmt.Errorf("failed to acknowledge message: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return classifyChannelAckMiss(ctx, tx, pq.msgTable(tableName), r)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
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
	return pq.nackChannelImpl(ctx, channelName, r, errorMsg, 0)
}

// nackChannelWithOpts is the option-aware nack used by the queue-agnostic Nack.
// WithRetryDelay overrides the computed backoff delay (FR-023).
func (pq *Queue) nackChannelWithOpts(
	ctx context.Context,
	channelName string,
	r Receipt,
	errorMsg string,
	opts ...NackOption,
) error {
	o := nackOpts{}
	for _, fn := range opts {
		fn(&o)
	}
	return pq.nackChannelImpl(ctx, channelName, r, errorMsg, o.retryDelay)
}

// nackChannelImpl is the shared nack body. retryDelay > 0 overrides the
// per-queue backoff policy for this single redelivery.
func (pq *Queue) nackChannelImpl(
	ctx context.Context,
	channelName string,
	r Receipt,
	errorMsg string,
	retryDelay time.Duration,
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
	tx, err := pq.db.BeginTx(ctx, readCommittedTxOptions)
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
		ctx, tx, queueMeta.TableName, r.MessageID, errorMsg, msgState, retryDelay,
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
// its per-message max_retries when present (including an explicit 0, meaning no
// retries), otherwise the configured default. max_retries is NULL only for
// rows reinstated by a DLQ replay, which fall back to the default.
func channelMaxRetries(defaultMax int, maxRetries sql.NullInt32) int {
	if maxRetries.Valid {
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
	query, args := channelConsumeQuery(pq.msgTable(tableName), ttl)

	// Loop so that a timed-out message that is skipped — its redelivery
	// deferred by the backoff policy (R-05), or it has exhausted its retries
	// and is promoted to the DLQ inline — does not make this consume call
	// return empty: the next eligible message is considered instead. The skip
	// count is capped (maxConsumeReclaimDefers) so one call cannot walk an
	// unbounded backlog.
	defers := 0
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

		retryCount, deferred, err := pq.reclaimChannelAttempt(ctx, tx, tableName, row)
		if err != nil {
			return nil, nil, err
		}
		if deferred {
			defers++
			if defers >= maxConsumeReclaimDefers {
				// Backlog too deep to drain in one call; return empty and let
				// the caller's poll/notify loop pick up where this left off.
				return nil, nil, nil
			}
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
//
// A timed-out 'processing' message that has exhausted its retries is also
// selected: reclaimChannelAttempt promotes it to the DLQ inline and the scan
// skips to the next candidate. This keeps an exhausted-by-timeout message from
// being stranded when no GarbageCollector is running, matching how the pub/sub
// consume path handles exhausted subscriptions (reclaimTopicAttempt).
func channelConsumeQuery(msgTable string, ttl time.Duration) (string, []any) {
	args := []any{}
	ttlClause := ""
	if ttl > 0 {
		ttlClause = "AND created_at > NOW() - make_interval(secs => $1)"
		args = append(args, ttl.Seconds())
	}

	// A pending message is only consumable once available_at has elapsed: this
	// is what enforces the retry backoff delay (FR-023) and any WithRetryDelay
	// override. Freshly published messages default available_at to now().
	query := fmt.Sprintf(`
		SELECT id, payload, created_at, status, retry_count, max_retries,
		       metadata, processed_at, error_message
		FROM %s
		WHERE ((status = '%s' AND available_at <= NOW())
		       OR (status = '%s' AND visibility_timeout < NOW()))
		  %s
		ORDER BY id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, msgTable, MessageStatusPending, MessageStatusProcessing, ttlClause)

	return query, args
}

// reclaimChannelAttempt accounts for a redelivery when a candidate was picked
// up in 'processing' state — a visibility timeout where the previous consumer
// never acked.
//
// It returns the retry count to claim with. The second return value reports
// that the candidate was consumed by this function rather than handed back to
// be delivered — the caller skips it and moves to the next message. That
// happens in two cases: a message that has now exhausted its retries is
// promoted to the DLQ inline (so it is not stranded when no GarbageCollector
// runs), and, when a backoff policy is configured (R-05), a non-exhausted
// timed-out message is returned to 'pending' with its redelivery deferred by
// the backoff delay.
func (pq *Queue) reclaimChannelAttempt(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	row channelCandidate,
) (int, bool, error) {
	// Only 'processing' rows need a reclaim accounting pass; 'pending' rows
	// are handed back to the caller verbatim. Any other status is unexpected
	// here and used to be silently treated as pending — surface it instead so
	// a future migration cannot regress this path (#65).
	switch MessageStatus(row.status) {
	case MessageStatusProcessing:
		// fall through to reclaim accounting below.
	case MessageStatusPending:
		return row.retryCount, false, nil
	default:
		return 0, false, fmt.Errorf(
			"unexpected channel message status %q for id %s",
			row.status, row.id,
		)
	}

	// A timeout reclaim counts as one redelivery attempt.
	retryCount := row.retryCount + 1

	// Exhausted by visibility timeout: dead-letter it inline and skip, exactly
	// as the pub/sub consume path does for exhausted subscriptions
	// (reclaimTopicAttempt). The GarbageCollector's promoteExhaustedChannelMessages
	// stays as a backstop for rows no consumer reaches.
	if retryCount > channelMaxRetries(pq.config.DefaultMaxRetries, row.maxRetries) {
		if err := pq.moveToDLQ(
			ctx, tx, tableName, row.id, errReasonVisibilityTimeout,
			row.payload, retryCount, row.metadataJSON,
		); err != nil {
			return 0, false, err
		}
		return 0, true, nil
	}

	if pq.cfg.backoffConfigured {
		// Return the message to pending with available_at pushed out by the
		// backoff delay, so a crash-looping consumer cannot drive tight,
		// backoff-free redelivery (R-05).
		delay := pq.computeRetryDelay(retryCount, 0)
		if err := pq.deferReclaimedChannelMessage(
			ctx, tx, tableName, row.id, retryCount, delay,
		); err != nil {
			return 0, false, err
		}
		return 0, true, nil
	}

	// No backoff policy configured: reclaim is immediate.
	return retryCount, false, nil
}

// deferReclaimedChannelMessage returns a timed-out 'processing' message to
// 'pending' with retry_count incremented and available_at pushed out by the
// backoff delay, so it is not redelivered until the delay elapses (R-05).
func (pq *Queue) deferReclaimedChannelMessage(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	messageID uuid.UUID,
	retryCount int,
	delay time.Duration,
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		UPDATE %s
		SET status = '%s',
		    claim_id = NULL,
		    retry_count = $2,
		    visibility_timeout = NULL,
		    available_at = NOW() + make_interval(secs => $3)
		WHERE id = $1
	`, pq.msgTable(tableName), MessageStatusPending)

	if _, err := tx.ExecContext(
		ctx, query, messageID, retryCount, delay.Seconds(),
	); err != nil {
		return fmt.Errorf("failed to defer reclaimed message: %w", err)
	}
	return nil
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
		UPDATE %s
		SET status = '%s',
		    retry_count = $3,
		    claim_id = uuidv7(),
		    visibility_timeout = NOW() + make_interval(secs => $1)
		WHERE id = $2
		RETURNING visibility_timeout, claim_id
	`, pq.msgTable(tableName), MessageStatusProcessing)

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

	// Report the effective retry limit, not the raw column: a NULL max_retries
	// (a DLQ-replayed row) falls back to the configured default — the same
	// resolution channelMaxRetries applies — so a consumer reading
	// msg.MaxRetries never sees a misleading 0 meaning "unknown".
	msg.MaxRetries = channelMaxRetries(pq.config.DefaultMaxRetries, row.maxRetries)
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
		FROM %s
		WHERE id = $1 AND claim_id = $2 AND status = '%s'
		FOR UPDATE
	`, pq.msgTable(tableName), MessageStatusProcessing)

	var state messageState
	err := tx.QueryRowContext(ctx, query, r.MessageID, r.ClaimID).Scan(
		&state.retryCount, &state.maxRetries,
		&state.payload, &state.metadataJSON,
	)
	if errors.Is(err, sql.ErrNoRows) {
		// No processing row under this claim: explain why (expired vs. gone).
		return nil, classifyChannelAckMiss(ctx, tx, pq.msgTable(tableName), r)
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
	retryDelay time.Duration,
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

	// The redelivery becomes eligible only after the backoff delay elapses.
	delay := pq.computeRetryDelay(state.retryCount+1, retryDelay)
	return pq.retryMessage(ctx, tx, tableName, messageID, errorMsg, delay)
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
		INSERT INTO %s
			(original_message_id, payload, failure_reason, retry_count, metadata)
		VALUES ($1, $2, $3, $4, $5)
	`, pq.dlqTable(tableName))

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
		`DELETE FROM %s WHERE id = $1`, pq.msgTable(tableName),
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
	delay time.Duration,
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
	// claim_id is cleared so the previous consumer's claim no longer matches:
	// a stale receipt for this message now resolves to ErrClaimExpired (see
	// classifyClaimMiss). available_at is pushed out by the backoff delay so
	// the message is not redelivered until the delay has elapsed (FR-023).
	updateQuery := fmt.Sprintf(`
		UPDATE %s
		SET status = '%s',
		    claim_id = NULL,
		    retry_count = retry_count + 1,
		    visibility_timeout = NULL,
		    available_at = NOW() + make_interval(secs => $3),
		    error_message = $2
		WHERE id = $1
	`, pq.msgTable(tableName), MessageStatusPending)

	_, err := tx.ExecContext(ctx, updateQuery, messageID, errorMsg, delay.Seconds())
	if err != nil {
		return fmt.Errorf("failed to update message: %w", err)
	}

	return nil
}

// truncateErrorMsg sanitizes a nack error message for storage in the
// error_message TEXT column. It guarantees the result is BOTH valid UTF-8 and
// at most maxErrorMessageLength bytes, for any input — a handler's error
// message is arbitrary bytes and may embed non-UTF-8 sequences, which
// PostgreSQL rejects on a TEXT column.
//
// Invalid byte sequences anywhere in the message are replaced with the Unicode
// replacement character. A single stray byte must not discard the surrounding
// diagnostic text: an earlier revision trimmed bytes from the end until the
// whole string was valid, which threw away every byte after the first invalid
// one — collapsing a long message to (in the worst case) the empty string.
//
// The sanitized message is then truncated on a UTF-8 rune boundary so the byte
// cap never splits a multi-byte rune.
func truncateErrorMsg(msg string) string {
	if utf8.ValidString(msg) && len(msg) <= maxErrorMessageLength {
		return msg
	}
	// Replace invalid byte sequences in place so valid content on either side
	// of a stray byte is preserved rather than discarded.
	if !utf8.ValidString(msg) {
		msg = strings.ToValidUTF8(msg, "�")
	}
	if len(msg) <= maxErrorMessageLength {
		return msg
	}
	// msg is now valid UTF-8, so the only thing that can make a byte-capped
	// prefix invalid is a split trailing rune — at most utf8.UTFMax-1 bytes.
	truncated := msg[:maxErrorMessageLength]
	for len(truncated) > 0 && !utf8.ValidString(truncated) {
		truncated = truncated[:len(truncated)-1]
	}
	return truncated
}
