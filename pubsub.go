package pgqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Subscribe registers a subscriber for a topic.
// The subscriberID must be 1-128 characters containing only alphanumeric characters,
// underscores, and dashes (matching the pattern [a-zA-Z0-9_-]+).
func (pq *Queue) Subscribe(
	ctx context.Context,
	topicName, subscriberID string,
) error {
	if err := validateSubscriberID(subscriberID); err != nil {
		return err
	}

	// Verify topic exists
	_, err := pq.getQueueMetadata(ctx, string(QueueTypePubSub), topicName)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
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
func (pq *Queue) Unsubscribe(
	ctx context.Context,
	topicName, subscriberID string,
) error {
	if err := validateSubscriberID(subscriberID); err != nil {
		return err
	}

	// Verify topic exists
	_, err := pq.getQueueMetadata(ctx, string(QueueTypePubSub), topicName)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return fmt.Errorf("%s: %w", topicName, ErrTopicNotFound)
		}
		return fmt.Errorf("failed to get topic metadata: %w", err)
	}

	// Unsubscribe is soft: it stops new messages from being routed to this
	// subscriber but leaves its outstanding rows so an in-flight message can
	// still be drained. The garbage collector reaps those leftover rows once
	// the subscriber stays inactive (see GarbageCollector.purgeInactiveSubscriptions),
	// so an abandoned subscriber cannot pin messages indefinitely.
	if err := pq.unregisterSubscriber(ctx, topicName, subscriberID); err != nil {
		return fmt.Errorf("failed to unsubscribe: %w", err)
	}

	return nil
}

// consumeTopicPreflight runs the cheap pre-flight checks shared by topic
// consumption: the Queue must be open, and the visibility timeout and
// subscriber ID must be valid.
func (pq *Queue) consumeTopicPreflight(subscriberID string, visibilityTimeout time.Duration) error {
	if err := pq.checkClosed(); err != nil {
		return err
	}
	if err := validateVisibilityTimeout(visibilityTimeout); err != nil {
		return err
	}
	return validateSubscriberID(subscriberID)
}

// ConsumeFromTopic retrieves the next available message for a subscriber from a topic.
// Returns nil message if no messages available.
func (pq *Queue) ConsumeFromTopic(
	ctx context.Context,
	topicName, subscriberID string,
	visibilityTimeout time.Duration,
) (*Message, error) {
	if err := pq.consumeTopicPreflight(subscriberID, visibilityTimeout); err != nil {
		return nil, err
	}

	// Get queue metadata
	queueMeta, err := pq.getQueueMetadata(
		ctx, string(QueueTypePubSub), topicName,
	)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return nil, fmt.Errorf("%s: %w", topicName, ErrTopicNotFound)
		}
		return nil, fmt.Errorf("failed to get topic metadata: %w", err)
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
	maxRetries := pq.resolveMaxRetries(queueMeta)

	msg, visTimeout, err := pq.fetchPendingTopicMessage(
		ctx, tx, queueMeta.TableName, subscriberID, visibilityTimeout, ttl, maxRetries,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch pending topic message: %w", err)
	}
	// Commit unconditionally — even with no message to deliver. The scan may
	// have deferred timed-out subscriptions with a backoff delay (R-05), and
	// those UPDATEs must persist rather than be discarded by the deferred
	// Rollback. With no message and no deferral the transaction is read-only,
	// so the commit is harmless.
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	if msg == nil {
		return nil, nil //nolint:nilnil // nil message indicates no messages available
	}

	msg.VisibilityTimeout = visTimeout

	return msg, nil
}

// AckTopic acknowledges a message for a subscriber.
//
// The receipt must carry the claim token issued by the ConsumeFromTopic call
// that delivered the message (use msg.Receipt()). If the claim has expired —
// the visibility timeout lapsed and the message was redelivered to this
// subscriber — AckTopic returns ErrClaimExpired and does nothing.
func (pq *Queue) AckTopic(
	ctx context.Context,
	topicName, subscriberID string,
	r Receipt,
) error {
	if err := validateSubscriberID(subscriberID); err != nil {
		return err
	}

	queueMeta, err := pq.getQueueMetadata(
		ctx, string(QueueTypePubSub), topicName,
	)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return fmt.Errorf("%s: %w", topicName, ErrTopicNotFound)
		}
		return fmt.Errorf("failed to get topic metadata: %w", err)
	}

	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		UPDATE %s
		SET status = '%s', acked_at = NOW()
		WHERE message_id = $1
		  AND subscriber_id = $2
		  AND claim_id = $3
		  AND status = '%s'
	`, pq.subTable(queueMeta.TableName), MessageStatusAcked, MessageStatusProcessing)

	// Run the UPDATE and, on a miss, the classifying SELECT in one transaction
	// so the classification observes the same snapshot as the failed UPDATE — a
	// concurrent reclaim cannot slip in between and flip the error type (R-09).
	tx, err := pq.db.BeginTx(ctx, readCommittedTxOptions)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	result, err := tx.ExecContext(ctx, query, r.MessageID, subscriberID, r.ClaimID)
	if err != nil {
		return fmt.Errorf("failed to acknowledge message: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return classifyTopicAckMiss(ctx, tx, pq.subTable(queueMeta.TableName), subscriberID, r)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// NackTopic negatively acknowledges a message for a subscriber (retry or move to DLQ).
// The errorMsg is truncated to 1024 characters if it exceeds that length.
//
// The receipt must carry the claim token from the consume call that delivered
// the message; a stale claim returns ErrClaimExpired (see AckTopic).
func (pq *Queue) NackTopic(
	ctx context.Context,
	topicName, subscriberID string,
	r Receipt,
	errorMsg string,
) error {
	return pq.nackTopicImpl(ctx, topicName, subscriberID, r, errorMsg, 0)
}

// nackTopicWithOpts is the option-aware topic nack used by the queue-agnostic
// Nack. WithRetryDelay overrides the computed backoff delay (FR-023).
func (pq *Queue) nackTopicWithOpts(
	ctx context.Context,
	topicName, subscriberID string,
	r Receipt,
	errorMsg string,
	opts ...NackOption,
) error {
	o := nackOpts{}
	for _, fn := range opts {
		fn(&o)
	}
	return pq.nackTopicImpl(ctx, topicName, subscriberID, r, errorMsg, o.retryDelay)
}

// nackTopicImpl is the shared topic-nack body. retryDelay > 0 overrides the
// per-queue backoff policy for this single redelivery.
func (pq *Queue) nackTopicImpl(
	ctx context.Context,
	topicName, subscriberID string,
	r Receipt,
	errorMsg string,
	retryDelay time.Duration,
) error {
	errorMsg = truncateErrorMsg(errorMsg)

	if err := validateSubscriberID(subscriberID); err != nil {
		return err
	}

	queueMeta, err := pq.getQueueMetadata(
		ctx, string(QueueTypePubSub), topicName,
	)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return fmt.Errorf("%s: %w", topicName, ErrTopicNotFound)
		}
		return fmt.Errorf("failed to get topic metadata: %w", err)
	}

	tx, err := pq.db.BeginTx(ctx, readCommittedTxOptions)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	state, err := pq.getProcessingSubState(
		ctx, tx, queueMeta.TableName, r, subscriberID,
	)
	if err != nil {
		return fmt.Errorf("failed to get subscription state: %w", err)
	}

	maxRetry := pq.resolveMaxRetries(queueMeta)

	if state.retryCount+1 > maxRetry {
		if err := pq.moveSubToDLQ(
			ctx, tx, queueMeta.TableName,
			r.MessageID, subscriberID, errorMsg, state,
		); err != nil {
			return fmt.Errorf("failed to move to DLQ: %w", err)
		}
	} else {
		delay := pq.computeRetryDelay(state.retryCount+1, retryDelay)
		if err := pq.retrySubscription(
			ctx, tx, queueMeta.TableName,
			r.MessageID, subscriberID, errorMsg, delay,
		); err != nil {
			return fmt.Errorf("failed to retry subscription: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

type subState struct {
	retryCount   int
	payload      []byte
	metadataJSON sql.NullString
}

func (pq *Queue) getProcessingSubState(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	r Receipt,
	subscriberID string,
) (*subState, error) {
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		SELECT s.retry_count, m.payload, m.metadata
		FROM %s s
		JOIN %s m ON s.message_id = m.id
		WHERE s.message_id = $1
		  AND s.subscriber_id = $2
		  AND s.claim_id = $3
		  AND s.status = '%s'
		FOR UPDATE OF s
	`, pq.subTable(tableName), pq.msgTable(tableName), MessageStatusProcessing)

	var state subState

	err := tx.QueryRowContext(ctx, query, r.MessageID, subscriberID, r.ClaimID).Scan(
		&state.retryCount, &state.payload, &state.metadataJSON,
	)
	if errors.Is(err, sql.ErrNoRows) {
		// No processing row under this claim: explain why (expired vs. gone).
		return nil, classifyTopicAckMiss(ctx, tx, pq.subTable(tableName), subscriberID, r)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query subscription: %w", err)
	}

	return &state, nil
}

func (pq *Queue) moveSubToDLQ(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	messageID uuid.UUID,
	subscriberID, errorMsg string,
	state *subState,
) error {
	// Defense-in-depth: mirror moveToDLQ — every entry point pre-truncates,
	// but enforcing the cap here ensures the DLQ table cannot grow without
	// bound if a future caller bypasses the nack path (#126).
	errorMsg = truncateErrorMsg(errorMsg)
	//nolint:gosec // G201: table name validated by queueNameRegex
	dlqQuery := fmt.Sprintf(`
		INSERT INTO %s
			(original_message_id, subscriber_id, payload, failure_reason, retry_count, metadata)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, pq.dlqTable(tableName))

	_, err := tx.ExecContext(
		ctx, dlqQuery, messageID, subscriberID, state.payload, errorMsg,
		state.retryCount+1, state.metadataJSON,
	)
	if err != nil {
		return fmt.Errorf("failed to insert into DLQ: %w", err)
	}

	//nolint:gosec // G201: table name validated by queueNameRegex
	deleteQuery := fmt.Sprintf(
		`DELETE FROM %s WHERE message_id = $1 AND subscriber_id = $2`,
		pq.subTable(tableName),
	)

	_, err = tx.ExecContext(ctx, deleteQuery, messageID, subscriberID)
	if err != nil {
		return fmt.Errorf("failed to delete subscription: %w", err)
	}

	return nil
}

func (pq *Queue) retrySubscription(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	messageID uuid.UUID,
	subscriberID, errorMsg string,
	delay time.Duration,
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
	// claim_id is cleared so the previous consumer's claim no longer matches:
	// a stale receipt for this subscription now resolves to ErrClaimExpired
	// (see classifyClaimMiss). available_at is pushed out by the backoff delay
	// (FR-023).
	query := fmt.Sprintf(`
		UPDATE %s
		SET status = '%s',
		    claim_id = NULL,
		    retry_count = retry_count + 1,
		    visibility_timeout = NULL,
		    available_at = NOW() + make_interval(secs => $4),
		    error_message = $3
		WHERE message_id = $1
		  AND subscriber_id = $2
	`, pq.subTable(tableName), MessageStatusPending)

	_, err := tx.ExecContext(ctx, query, messageID, subscriberID, errorMsg, delay.Seconds())
	if err != nil {
		return fmt.Errorf("failed to retry subscription: %w", err)
	}

	return nil
}

// topicCandidate is one consumable subscription row scanned from a topic.
type topicCandidate struct {
	subID        uuid.UUID
	msgID        uuid.UUID
	payload      []byte
	createdAt    time.Time
	status       string
	retryCount   int
	metadataJSON sql.NullString
	errorMessage sql.NullString
}

func (pq *Queue) fetchPendingTopicMessage(
	ctx context.Context,
	tx *sql.Tx,
	tableName, subscriberID string,
	visibilityTimeout time.Duration,
	ttl time.Duration,
	maxRetries int,
) (*Message, *time.Time, error) {
	query, args := topicConsumeQuery(pq.subTable(tableName), pq.msgTable(tableName), subscriberID, ttl)

	// Loop so an exhausted timed-out subscription is moved to the DLQ and
	// skipped rather than redelivered forever — see fetchPendingChannelMessage.
	// The skip count is capped (maxConsumeReclaimDefers) so one call cannot
	// walk — and row-lock — an unbounded backlog in a single transaction.
	skips := 0
	for {
		var row topicCandidate
		err := tx.QueryRowContext(ctx, query, args...).Scan(
			&row.subID, &row.msgID, &row.payload, &row.createdAt,
			&row.status, &row.retryCount, &row.metadataJSON, &row.errorMessage,
		)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, nil
		}
		if err != nil {
			return nil, nil, fmt.Errorf("failed to query subscription: %w", err)
		}

		retryCount, dlqd, err := pq.reclaimTopicAttempt(
			ctx, tx, tableName, subscriberID, row, maxRetries,
		)
		if err != nil {
			return nil, nil, err
		}
		if dlqd {
			skips++
			if skips >= maxConsumeReclaimDefers {
				// Backlog too deep to drain in one call; return empty and let
				// the caller's poll/notify loop pick up where this left off.
				return nil, nil, nil
			}
			continue
		}

		return pq.claimTopicSubscription(
			ctx, tx, tableName, visibilityTimeout, row, retryCount, maxRetries,
		)
	}
}

// topicConsumeQuery builds the SELECT for the next consumable subscription. A
// subscription is consumable when pending, or when still processing but its
// visibility timeout has expired — see channelConsumeQuery.
func topicConsumeQuery(subTable, msgTable, subscriberID string, ttl time.Duration) (string, []any) {
	ttlClause := ""
	args := []any{subscriberID}
	if ttl > 0 {
		ttlClause = "AND m.created_at > NOW() - make_interval(secs => $2)"
		args = append(args, ttl.Seconds())
	}

	// A pending subscription is only consumable once available_at has elapsed,
	// enforcing the retry backoff delay (FR-023) — see channelConsumeQuery.
	query := fmt.Sprintf(`
		SELECT s.id, s.message_id, m.payload, m.created_at,
		       s.status, s.retry_count, m.metadata, s.error_message
		FROM %s s
		JOIN %s m ON s.message_id = m.id
		WHERE s.subscriber_id = $1
		  AND ((s.status = '%s' AND s.available_at <= NOW())
		       OR (s.status = '%s' AND s.visibility_timeout < NOW()))
		  %s
		ORDER BY m.id
		LIMIT 1
		FOR UPDATE OF s SKIP LOCKED
	`, subTable, msgTable, MessageStatusPending, MessageStatusProcessing, ttlClause)

	return query, args
}

// reclaimTopicAttempt accounts for a redelivery when a subscription was picked
// up in 'processing' state — a visibility timeout where the previous consumer
// never acked. It returns the retry count to claim with; if the retries are now
// exhausted it moves the subscription to the DLQ and reports skip=true.
//
// When a backoff policy is configured (R-05) a non-exhausted timed-out
// subscription is instead returned to 'pending' with its redelivery deferred by
// the backoff delay; skip is then true and the caller moves on.
func (pq *Queue) reclaimTopicAttempt(
	ctx context.Context,
	tx *sql.Tx,
	tableName, subscriberID string,
	row topicCandidate,
	maxRetries int,
) (int, bool, error) {
	retryCount := row.retryCount
	// Mirror the channel reclaim path: only 'processing' rows need accounting;
	// 'pending' rows pass through; anything else used to be silently coerced
	// to pending and is now surfaced as an error so a future migration cannot
	// regress this path (#65).
	switch MessageStatus(row.status) {
	case MessageStatusProcessing:
		// fall through to reclaim accounting below.
	case MessageStatusPending:
		return retryCount, false, nil
	default:
		return 0, false, fmt.Errorf(
			"unexpected subscription status %q for message %s subscriber %s",
			row.status, row.msgID, subscriberID,
		)
	}

	if retryCount+1 > maxRetries {
		if err := pq.moveSubToDLQ(
			ctx, tx, tableName, row.msgID, subscriberID, errReasonVisibilityTimeout,
			&subState{retryCount: retryCount, payload: row.payload, metadataJSON: row.metadataJSON},
		); err != nil {
			return 0, false, fmt.Errorf("failed to move timed-out subscription to DLQ: %w", err)
		}

		return 0, true, nil
	}

	retryCount++

	if pq.cfg.backoffConfigured {
		delay := pq.computeRetryDelay(retryCount, 0)
		if err := pq.deferReclaimedSubscription(
			ctx, tx, tableName, row.subID, retryCount, delay,
		); err != nil {
			return 0, false, err
		}
		return 0, true, nil
	}

	return retryCount, false, nil
}

// deferReclaimedSubscription returns a timed-out 'processing' subscription to
// 'pending' with retry_count incremented and available_at pushed out by the
// backoff delay, so it is not redelivered until the delay elapses (R-05).
func (pq *Queue) deferReclaimedSubscription(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	subID uuid.UUID,
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
	`, pq.subTable(tableName), MessageStatusPending)

	if _, err := tx.ExecContext(
		ctx, query, subID, retryCount, delay.Seconds(),
	); err != nil {
		return fmt.Errorf("failed to defer reclaimed subscription: %w", err)
	}
	return nil
}

//nolint:gosec // G201: table name validated by queueNameRegex
func (pq *Queue) claimTopicSubscription(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	visibilityTimeout time.Duration,
	row topicCandidate,
	retryCount int,
	maxRetries int,
) (*Message, *time.Time, error) {
	// Visibility deadline computed on the database clock; see claimChannelMessage.
	// retry_count is written back so a reclaim-driven increment is persisted. A
	// fresh claim_id is minted on every (re)delivery so a previous consumer
	// whose visibility timeout lapsed cannot acknowledge this reassigned message.
	updateQuery := fmt.Sprintf(`
		UPDATE %s
		SET status = '%s',
		    retry_count = $3,
		    claim_id = uuidv7(),
		    visibility_timeout = NOW() + make_interval(secs => $1)
		WHERE id = $2
		RETURNING visibility_timeout, claim_id
	`, pq.subTable(tableName), MessageStatusProcessing)

	var visTimeout time.Time
	var claimID uuid.UUID
	if err := tx.QueryRowContext(
		ctx, updateQuery, visibilityTimeout.Seconds(), row.subID, retryCount,
	).Scan(&visTimeout, &claimID); err != nil {
		return nil, nil, fmt.Errorf("failed to update subscription: %w", err)
	}

	msg := &Message{
		ID:         row.msgID,
		ClaimID:    claimID,
		Payload:    row.payload,
		CreatedAt:  row.createdAt,
		Status:     MessageStatusProcessing,
		RetryCount: retryCount,
		MaxRetries: maxRetries,
		Metadata:   pq.parseMetadataJSON(row.metadataJSON),
	}

	if row.errorMessage.Valid {
		msg.ErrorMessage = &row.errorMessage.String
	}

	return msg, &visTimeout, nil
}
