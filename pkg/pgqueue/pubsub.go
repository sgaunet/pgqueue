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

// ConsumeFromTopic retrieves the next available message for a subscriber from a topic.
// Returns nil message if no messages available.
func (pq *Queue) ConsumeFromTopic(
	ctx context.Context,
	topicName, subscriberID string,
	visibilityTimeout time.Duration,
) (*Message, error) {
	if err := validateVisibilityTimeout(visibilityTimeout); err != nil {
		return nil, err
	}

	if err := validateSubscriberID(subscriberID); err != nil {
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
	tx, err := pq.db.BeginTx(ctx, nil)
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
		UPDATE pgqueue_sub_%s
		SET status = '%s', acked_at = NOW()
		WHERE message_id = $1
		  AND subscriber_id = $2
		  AND claim_id = $3
		  AND status = '%s'
	`, queueMeta.TableName, MessageStatusAcked, MessageStatusProcessing)

	result, err := pq.db.ExecContext(ctx, query, r.MessageID, subscriberID, r.ClaimID)
	if err != nil {
		return fmt.Errorf("failed to acknowledge message: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return classifyTopicAckMiss(ctx, pq.db, queueMeta.TableName, subscriberID, r)
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

	tx, err := pq.db.BeginTx(ctx, nil)
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
		if err := pq.retrySubscription(
			ctx, tx, queueMeta.TableName,
			r.MessageID, subscriberID, errorMsg,
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
		FROM pgqueue_sub_%s s
		JOIN pgqueue_msg_%s m ON s.message_id = m.id
		WHERE s.message_id = $1
		  AND s.subscriber_id = $2
		  AND s.claim_id = $3
		  AND s.status = '%s'
		FOR UPDATE OF s
	`, tableName, tableName, MessageStatusProcessing)

	var state subState

	err := tx.QueryRowContext(ctx, query, r.MessageID, subscriberID, r.ClaimID).Scan(
		&state.retryCount, &state.payload, &state.metadataJSON,
	)
	if errors.Is(err, sql.ErrNoRows) {
		// No processing row under this claim: explain why (expired vs. gone).
		return nil, classifyTopicAckMiss(ctx, tx, tableName, subscriberID, r)
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
	//nolint:gosec // G201: table name validated by queueNameRegex
	dlqQuery := fmt.Sprintf(`
		INSERT INTO pgqueue_dlq_%s
			(original_message_id, subscriber_id, payload, failure_reason, retry_count, metadata)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, tableName)

	_, err := tx.ExecContext(
		ctx, dlqQuery, messageID, subscriberID, state.payload, errorMsg,
		state.retryCount+1, state.metadataJSON,
	)
	if err != nil {
		return fmt.Errorf("failed to insert into DLQ: %w", err)
	}

	//nolint:gosec // G201: table name validated by queueNameRegex
	deleteQuery := fmt.Sprintf(
		`DELETE FROM pgqueue_sub_%s WHERE message_id = $1 AND subscriber_id = $2`,
		tableName,
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
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		UPDATE pgqueue_sub_%s
		SET status = '%s',
		    retry_count = retry_count + 1,
		    visibility_timeout = NULL,
		    error_message = $3
		WHERE message_id = $1
		  AND subscriber_id = $2
	`, tableName, MessageStatusPending)

	_, err := tx.ExecContext(ctx, query, messageID, subscriberID, errorMsg)
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
	query, args := topicConsumeQuery(tableName, subscriberID, ttl)

	// Loop so an exhausted timed-out subscription is moved to the DLQ and
	// skipped rather than redelivered forever — see fetchPendingChannelMessage.
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
func topicConsumeQuery(tableName, subscriberID string, ttl time.Duration) (string, []any) {
	ttlClause := ""
	args := []any{subscriberID}
	if ttl > 0 {
		ttlClause = "AND m.created_at > NOW() - make_interval(secs => $2)"
		args = append(args, ttl.Seconds())
	}

	query := fmt.Sprintf(`
		SELECT s.id, s.message_id, m.payload, m.created_at,
		       s.status, s.retry_count, m.metadata, s.error_message
		FROM pgqueue_sub_%s s
		JOIN pgqueue_msg_%s m ON s.message_id = m.id
		WHERE s.subscriber_id = $1
		  AND (s.status = '%s'
		       OR (s.status = '%s' AND s.visibility_timeout < NOW()))
		  %s
		ORDER BY m.id
		LIMIT 1
		FOR UPDATE OF s SKIP LOCKED
	`, tableName, tableName, MessageStatusPending, MessageStatusProcessing, ttlClause)

	return query, args
}

// reclaimTopicAttempt accounts for a redelivery when a subscription was picked
// up in 'processing' state — a visibility timeout where the previous consumer
// never acked. It returns the retry count to claim with; if the retries are now
// exhausted it moves the subscription to the DLQ and reports dlqd=true.
func (pq *Queue) reclaimTopicAttempt(
	ctx context.Context,
	tx *sql.Tx,
	tableName, subscriberID string,
	row topicCandidate,
	maxRetries int,
) (int, bool, error) {
	retryCount := row.retryCount
	if MessageStatus(row.status) != MessageStatusProcessing {
		return retryCount, false, nil
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

	return retryCount + 1, false, nil
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
		UPDATE pgqueue_sub_%s
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
