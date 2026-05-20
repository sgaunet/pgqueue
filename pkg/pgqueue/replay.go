package pgqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ReplayFrom resets messages after a specific timestamp to pending status.
func (pq *PGQueue) ReplayFrom(
	ctx context.Context,
	queueName string,
	queueType QueueType,
	since time.Time,
	opts ReplayOptions,
) (int, error) {
	if err := validateReplayOpts(opts); err != nil {
		return 0, fmt.Errorf("failed to validate replay options: %w", err)
	}

	metadata, err := pq.getReplayQueueMetadata(ctx, queueType, queueName)
	if err != nil {
		return 0, fmt.Errorf("failed to get queue metadata for replay: %w", err)
	}

	tableName := metadata.TableName

	if opts.DryRun {
		return pq.countReplayableMessages(
			ctx, tableName, queueType, since,
		)
	}

	count, err := pq.executeReplayFrom(
		ctx, tableName, queueType, since, opts.Limit,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to execute replay from timestamp: %w", err)
	}

	pq.logReplayIfNeeded(
		ctx, queueName, queueType, "timestamp",
		count, opts.PerformedBy,
		fmt.Sprintf("since: %s", since),
	)

	return count, nil
}

// ReplayMessage resets a specific channel message to pending status.
// Not supported for pub/sub queues (use ReplayFrom or ReplayDLQ instead).
func (pq *PGQueue) ReplayMessage(
	ctx context.Context,
	queueName string,
	queueType QueueType,
	messageID uuid.UUID,
	opts ReplayOptions,
) error {
	if queueType == QueueTypePubSub {
		return ErrReplayNotSupported
	}

	if err := validateReplayOpts(opts); err != nil {
		return fmt.Errorf("failed to validate replay options: %w", err)
	}

	metadata, err := pq.getReplayQueueMetadata(ctx, queueType, queueName)
	if err != nil {
		return fmt.Errorf("failed to get queue metadata for replay: %w", err)
	}

	tableName := metadata.TableName

	if opts.DryRun {
		return pq.checkMessageExists(ctx, tableName, messageID)
	}

	if err := pq.executeReplayMessage(
		ctx, tableName, messageID,
	); err != nil {
		return fmt.Errorf("failed to replay message: %w", err)
	}

	pq.logReplayIfNeeded(
		ctx, queueName, queueType, "message_id",
		1, opts.PerformedBy,
		fmt.Sprintf("message_id: %s", messageID),
	)

	return nil
}

// ReplayDLQ moves messages from DLQ back to the main queue.
// When opts.Limit is 0, up to 10,000 messages are replayed in a single transaction.
//
// For pub/sub topics: the original message must still exist in the message table.
// If CompletedMessageTTL is shorter than DLQRetention in your GC policy, the message
// row may be garbage-collected before the DLQ entry, causing a foreign key error on replay.
// Ensure DLQRetention does not exceed CompletedMessageTTL for pub/sub topics.
func (pq *PGQueue) ReplayDLQ(
	ctx context.Context,
	queueName string,
	queueType QueueType,
	opts ReplayOptions,
) (int, error) {
	if err := validateReplayOpts(opts); err != nil {
		return 0, fmt.Errorf("failed to validate replay options: %w", err)
	}

	metadata, err := pq.getReplayQueueMetadata(ctx, queueType, queueName)
	if err != nil {
		return 0, fmt.Errorf("failed to get queue metadata for replay: %w", err)
	}

	tableName := metadata.TableName

	if opts.DryRun {
		return pq.countDLQMessages(ctx, tableName)
	}

	count, err := pq.executeReplayDLQ(ctx, queueName, tableName, queueType, opts.Limit)
	if err != nil {
		return 0, fmt.Errorf("failed to execute DLQ replay: %w", err)
	}

	pq.logReplayIfNeeded(
		ctx, queueName, queueType, "dlq",
		count, opts.PerformedBy,
		fmt.Sprintf("replayed %d messages from DLQ", count),
	)

	return count, nil
}

// GetReplayHistory returns the replay history for a queue.
func (pq *PGQueue) GetReplayHistory(
	ctx context.Context,
	queueName string,
	queueType QueueType,
	limit int,
) ([]ReplayLog, error) {
	if limit <= 0 {
		limit = 100
	}

	return pq.getReplayHistory(
		ctx, string(queueType), queueName, limit,
	)
}

func validateReplayOpts(opts ReplayOptions) error {
	if !opts.Confirm && !opts.DryRun {
		return ErrConfirmationRequired
	}

	return nil
}

func (pq *PGQueue) getReplayQueueMetadata(
	ctx context.Context,
	queueType QueueType,
	queueName string,
) (*QueueMetadata, error) {
	metadata, err := pq.getQueueMetadata(
		ctx, string(queueType), queueName,
	)
	if errors.Is(err, ErrQueueNotFound) {
		return nil, fmt.Errorf(
			"%s/%s: %w", queueType, queueName, ErrQueueNotFound,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get queue metadata: %w", err)
	}

	return metadata, nil
}

func (pq *PGQueue) countReplayableMessages(
	ctx context.Context,
	tableName string,
	queueType QueueType,
	since time.Time,
) (int, error) {
	var countQuery string
	if queueType == QueueTypePubSub {
		countQuery = fmt.Sprintf(`
			SELECT COUNT(*) FROM pgqueue_sub_%s
			WHERE created_at >= $1
			AND status != '%s'
			AND status != '%s'
		`, tableName, MessageStatusPending, MessageStatusProcessing)
	} else {
		countQuery = fmt.Sprintf(`
			SELECT COUNT(*) FROM pgqueue_msg_%s
			WHERE created_at >= $1
			AND status != '%s'
			AND status != '%s'
		`, tableName, MessageStatusPending, MessageStatusProcessing)
	}

	var count int
	if err := pq.db.QueryRowContext(
		ctx, countQuery, since,
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count messages: %w", err)
	}

	return count, nil
}

func (pq *PGQueue) executeReplayFrom(
	ctx context.Context,
	tableName string,
	queueType QueueType,
	since time.Time,
	limit int,
) (int, error) {
	query := pq.buildReplayFromQuery(tableName, queueType, limit)

	result, err := pq.db.ExecContext(ctx, query, since)
	if err != nil {
		return 0, fmt.Errorf("failed to replay messages: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get affected rows: %w", err)
	}

	return int(rows), nil
}

func (pq *PGQueue) buildReplayFromQuery(
	tableName string,
	queueType QueueType,
	limit int,
) string {
	if queueType == QueueTypeChannel {
		return pq.buildChannelReplayQuery(tableName, limit)
	}

	return pq.buildPubSubReplayQuery(tableName, limit)
}

func (pq *PGQueue) buildChannelReplayQuery(tableName string, limit int) string {
	if limit > 0 {
		return fmt.Sprintf(`
			UPDATE pgqueue_msg_%s
			SET status = '%s',
			    retry_count = 0,
			    visibility_timeout = NULL,
			    processed_at = NULL,
			    error_message = NULL
			WHERE id IN (
				SELECT id FROM pgqueue_msg_%s
				WHERE created_at >= $1 AND status != '%s' AND status != '%s'
				LIMIT %d
			)
		`, tableName, MessageStatusPending, tableName, MessageStatusPending, MessageStatusProcessing, limit)
	}

	return fmt.Sprintf(`
		UPDATE pgqueue_msg_%s
		SET status = '%s',
		    retry_count = 0,
		    visibility_timeout = NULL,
		    processed_at = NULL,
		    error_message = NULL
		WHERE created_at >= $1
		AND status != '%s'
		AND status != '%s'
	`, tableName, MessageStatusPending, MessageStatusPending, MessageStatusProcessing)
}

func (pq *PGQueue) buildPubSubReplayQuery(tableName string, limit int) string {
	if limit > 0 {
		return fmt.Sprintf(`
			UPDATE pgqueue_sub_%s
			SET status = '%s',
			    retry_count = 0,
			    visibility_timeout = NULL,
			    acked_at = NULL,
			    error_message = NULL
			WHERE id IN (
				SELECT id FROM pgqueue_sub_%s
				WHERE created_at >= $1 AND status != '%s' AND status != '%s'
				LIMIT %d
			)
		`, tableName, MessageStatusPending, tableName, MessageStatusPending, MessageStatusProcessing, limit)
	}

	return fmt.Sprintf(`
		UPDATE pgqueue_sub_%s
		SET status = '%s',
		    retry_count = 0,
		    visibility_timeout = NULL,
		    acked_at = NULL,
		    error_message = NULL
		WHERE created_at >= $1
		AND status != '%s'
		AND status != '%s'
	`, tableName, MessageStatusPending, MessageStatusPending, MessageStatusProcessing)
}

func (pq *PGQueue) checkMessageExists(
	ctx context.Context,
	tableName string,
	messageID uuid.UUID,
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
	checkQuery := fmt.Sprintf(
		`SELECT status FROM pgqueue_msg_%s WHERE id = $1`, tableName,
	)

	var status string
	err := pq.db.QueryRowContext(ctx, checkQuery, messageID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%s: %w", messageID, ErrReplayMessageNotFound)
	}
	if err != nil {
		return fmt.Errorf("failed to check message: %w", err)
	}
	if MessageStatus(status) == MessageStatusProcessing {
		return fmt.Errorf("%s: %w", messageID, ErrMessageInProcessing)
	}

	return nil
}

func (pq *PGQueue) executeReplayMessage(
	ctx context.Context,
	tableName string,
	messageID uuid.UUID,
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		UPDATE pgqueue_msg_%s
		SET status = '%s',
		    retry_count = 0,
		    visibility_timeout = NULL,
		    processed_at = NULL,
		    error_message = NULL
		WHERE id = $1 AND status != '%s'
	`, tableName, MessageStatusPending, MessageStatusProcessing)

	result, err := pq.db.ExecContext(ctx, query, messageID)
	if err != nil {
		return fmt.Errorf("failed to replay message: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get affected rows: %w", err)
	}

	if rows == 0 {
		// Distinguish "not found" from "currently being processed"
		var status string
		checkQuery := fmt.Sprintf( //nolint:gosec // G201: table name validated by queueNameRegex
			`SELECT status FROM pgqueue_msg_%s WHERE id = $1`, tableName,
		)
		err := pq.db.QueryRowContext(ctx, checkQuery, messageID).Scan(&status)
		if err == nil && MessageStatus(status) == MessageStatusProcessing {
			return fmt.Errorf("%s: %w", messageID, ErrMessageInProcessing)
		}
		return fmt.Errorf("%s: %w", messageID, ErrReplayMessageNotFound)
	}

	return nil
}

func (pq *PGQueue) countDLQMessages(
	ctx context.Context,
	tableName string,
) (int, error) {
	countQuery := "SELECT COUNT(*) FROM pgqueue_dlq_" + tableName

	var count int
	if err := pq.db.QueryRowContext(ctx, countQuery).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count DLQ messages: %w", err)
	}

	return count, nil
}

func (pq *PGQueue) executeReplayDLQ(
	ctx context.Context,
	queueName, tableName string,
	queueType QueueType,
	limit int,
) (int, error) {
	tx, err := pq.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	dlqMessages, err := pq.fetchDLQMessages(ctx, tx, tableName, limit)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch DLQ messages: %w", err)
	}

	count, err := pq.reinsertDLQMessages(
		ctx, tx, queueName, tableName, queueType, dlqMessages,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to reinsert DLQ messages: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit replay: %w", err)
	}

	return count, nil
}

type dlqRow struct {
	id                uuid.UUID
	originalMessageID uuid.UUID
	subscriberID      sql.NullString // set for pub/sub DLQ entries, NULL for channels
	payload           []byte
	metadata          []byte
}

// defaultDLQReplayLimit prevents unbounded memory usage when replaying DLQ messages.
const defaultDLQReplayLimit = 10000

func (pq *PGQueue) fetchDLQMessages(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	limit int,
) ([]dlqRow, error) {
	if limit <= 0 {
		limit = defaultDLQReplayLimit
	}

	//nolint:gosec // G201: table name validated by queueNameRegex
	selectQuery := fmt.Sprintf(`
		SELECT id, original_message_id, subscriber_id, payload, metadata
		FROM pgqueue_dlq_%s
		LIMIT %d
	`, tableName, limit)

	rows, err := tx.QueryContext(ctx, selectQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query DLQ: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var dlqMessages []dlqRow
	for rows.Next() {
		var msg dlqRow
		if err := rows.Scan(
			&msg.id, &msg.originalMessageID, &msg.subscriberID,
			&msg.payload, &msg.metadata,
		); err != nil {
			return nil, fmt.Errorf("failed to scan DLQ message: %w", err)
		}
		dlqMessages = append(dlqMessages, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating DLQ messages: %w", err)
	}

	return dlqMessages, nil
}

func (pq *PGQueue) reinsertDLQMessages(
	ctx context.Context,
	tx *sql.Tx,
	queueName, tableName string,
	queueType QueueType,
	dlqMessages []dlqRow,
) (int, error) {
	if queueType == QueueTypePubSub {
		return pq.reinsertDLQPubSub(ctx, tx, queueName, tableName, dlqMessages)
	}

	return pq.reinsertDLQChannel(ctx, tx, tableName, dlqMessages)
}

func (pq *PGQueue) reinsertDLQChannel(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	dlqMessages []dlqRow,
) (int, error) {
	if len(dlqMessages) == 0 {
		return 0, nil
	}

	// Insert messages and collect which IDs were actually inserted (ON CONFLICT skips dupes).
	insertedIDs, err := pq.insertDLQChannelMessages(ctx, tx, tableName, dlqMessages)
	if err != nil {
		return 0, err
	}

	if len(insertedIDs) == 0 {
		return 0, nil
	}

	// Only delete DLQ entries whose messages were actually reinserted
	dlqIDs := make([]uuid.UUID, 0, len(insertedIDs))
	for _, msg := range dlqMessages {
		if _, ok := insertedIDs[msg.originalMessageID]; ok {
			dlqIDs = append(dlqIDs, msg.id)
		}
	}

	//nolint:gosec // G201: table name validated by queueNameRegex
	deleteQuery := fmt.Sprintf(
		`DELETE FROM pgqueue_dlq_%s WHERE id = ANY($1::uuid[])`, tableName,
	)
	if _, err := tx.ExecContext(ctx, deleteQuery, uuidSliceToStringSlice(dlqIDs)); err != nil {
		return 0, fmt.Errorf("failed to delete from DLQ: %w", err)
	}

	return len(insertedIDs), nil
}

// insertDLQChannelMessages batch-inserts DLQ messages back into the channel message table.
// Returns the set of message IDs that were actually inserted (ON CONFLICT skips duplicates).
func (pq *PGQueue) insertDLQChannelMessages(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	dlqMessages []dlqRow,
) (map[uuid.UUID]struct{}, error) {
	const paramsPerRow = 3 // id, payload, metadata

	var sb strings.Builder
	fmt.Fprintf(&sb,
		"INSERT INTO pgqueue_msg_%s (id, payload, created_at, status, retry_count, metadata) VALUES ",
		tableName,
	)

	args := make([]any, 0, len(dlqMessages)*paramsPerRow)
	for i, msg := range dlqMessages {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * paramsPerRow
		fmt.Fprintf(&sb, "($%d, $%d, NOW(), '%s', 0, $%d)",
			base+1, base+2, MessageStatusPending, base+3, //nolint:mnd // SQL placeholder arithmetic
		)
		args = append(args, msg.originalMessageID, msg.payload, msg.metadata)
	}
	sb.WriteString(" ON CONFLICT (id) DO NOTHING RETURNING id")

	rows, err := tx.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("failed to insert messages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	insertedIDs := make(map[uuid.UUID]struct{})
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan inserted message ID: %w", err)
		}
		insertedIDs[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate inserted messages: %w", err)
	}

	return insertedIDs, nil
}

// reinsertDLQPubSub re-creates subscription records for pub/sub DLQ messages.
// The original message still exists in pgqueue_msg_ (only the subscription was
// deleted when moved to DLQ), so only the subscription records are re-created.
//
// Each DLQ entry is replayed to the exact subscriber that failed (recorded in
// subscriber_id), so subscribers that processed the message successfully are
// never re-delivered it. ON CONFLICT DO NOTHING additionally protects any
// subscriber whose row still exists.
func (pq *PGQueue) reinsertDLQPubSub(
	ctx context.Context,
	tx *sql.Tx,
	queueName, tableName string,
	dlqMessages []dlqRow,
) (int, error) {
	if len(dlqMessages) == 0 {
		return 0, nil
	}

	records, replayedIDs, err := pq.resolvePubSubDLQRecords(ctx, tx, queueName, dlqMessages)
	if err != nil {
		return 0, err
	}
	if len(replayedIDs) == 0 {
		return 0, nil
	}

	if err := pq.insertSubscriptionRecords(ctx, tx, tableName, records); err != nil {
		return 0, fmt.Errorf("failed to create subscription records: %w", err)
	}

	//nolint:gosec // G201: table name validated by queueNameRegex
	deleteQuery := fmt.Sprintf(
		`DELETE FROM pgqueue_dlq_%s WHERE id = ANY($1::uuid[])`, tableName,
	)
	if _, err := tx.ExecContext(ctx, deleteQuery, uuidSliceToStringSlice(replayedIDs)); err != nil {
		return 0, fmt.Errorf("failed to delete from DLQ: %w", err)
	}

	return len(replayedIDs), nil
}

// resolvePubSubDLQRecords maps pub/sub DLQ rows to the subscription records to
// re-create, and returns the ids of the DLQ entries that can be deleted.
//
// Rows carrying a subscriber_id are replayed to exactly that subscriber. Legacy
// rows with a NULL subscriber_id (written before schema v2) fall back to all
// currently-active subscribers; such a row is only marked for deletion when at
// least one active subscriber exists, so a replay with no subscribers leaves it
// in the DLQ rather than silently dropping it.
func (pq *PGQueue) resolvePubSubDLQRecords(
	ctx context.Context,
	tx *sql.Tx,
	queueName string,
	dlqMessages []dlqRow,
) ([]subRecord, []uuid.UUID, error) {
	var legacySubscribers []Subscriber
	legacyLoaded := false

	records := make([]subRecord, 0, len(dlqMessages))
	replayedIDs := make([]uuid.UUID, 0, len(dlqMessages))

	for _, msg := range dlqMessages {
		if msg.subscriberID.Valid {
			records = append(records, subRecord{
				messageID:    msg.originalMessageID,
				subscriberID: msg.subscriberID.String,
			})
			replayedIDs = append(replayedIDs, msg.id)

			continue
		}

		// Legacy row with no recorded subscriber: load active subscribers once.
		if !legacyLoaded {
			subs, err := pq.getActiveSubscribers(ctx, tx, queueName)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to get active subscribers: %w", err)
			}
			legacySubscribers = subs
			legacyLoaded = true
		}
		if len(legacySubscribers) == 0 {
			continue // leave the DLQ row in place; nothing to replay to
		}
		for _, sub := range legacySubscribers {
			records = append(records, subRecord{
				messageID:    msg.originalMessageID,
				subscriberID: sub.SubscriberID,
			})
		}
		replayedIDs = append(replayedIDs, msg.id)
	}

	return records, replayedIDs, nil
}

func (pq *PGQueue) logReplayIfNeeded(
	ctx context.Context,
	queueName string,
	queueType QueueType,
	operation string,
	count int,
	performedBy, details string,
) {
	params, err := json.Marshal(map[string]string{"details": details})
	if err != nil {
		pq.logError("failed to marshal replay params", "error", err)
		return
	}

	var createdBy *string
	if performedBy != "" {
		createdBy = &performedBy
	}

	if err := pq.createReplayLog(
		ctx, string(queueType), queueName,
		operation, params, count, createdBy,
	); err != nil {
		pq.logError("failed to log replay operation", "error", err)
	}
}
