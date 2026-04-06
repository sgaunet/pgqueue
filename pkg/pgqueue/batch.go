package pgqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// SQL parameter counts per row for multi-row INSERT statements.
const (
	channelInsertParams = 4 // id, payload, metadata, max_retries
	pubsubInsertParams  = 3 // id, payload, metadata
	subInsertParams     = 2 // message_id, subscriber_id
	dlqInsertParams     = 5 // original_message_id, payload, failure_reason, retry_count, metadata
)

// PublishBatch publishes multiple messages to a channel or topic in a single operation.
// Returns message IDs in the same order as the input messages.
func (pq *PGQueue) PublishBatch(
	ctx context.Context,
	queueName string,
	messages []PublishMessage,
) ([]uuid.UUID, error) {
	if len(messages) == 0 {
		return []uuid.UUID{}, nil
	}
	if err := validateBatchSize(len(messages)); err != nil {
		return nil, err
	}

	queueMeta, err := pq.resolveQueueMetadata(ctx, queueName)
	if err != nil {
		return nil, err
	}

	// Validate all payloads upfront before any DB work
	for i := range messages {
		if err := pq.validatePayloadSize(queueMeta, messages[i].Payload); err != nil {
			return nil, err
		}
	}

	ids, metadataJSONs, err := prepareBatchMessages(messages)
	if err != nil {
		return nil, err
	}

	queueType := QueueType(queueMeta.QueueType)
	if queueType == QueueTypePubSub {
		return ids, pq.publishBatchToPubSub(
			ctx, queueMeta.QueueName, queueMeta.TableName,
			ids, messages, metadataJSONs,
		)
	}

	maxRetries := pq.resolveMaxRetries(queueMeta)

	return ids, pq.publishBatchToChannel(
		ctx, queueMeta.TableName,
		ids, messages, metadataJSONs, maxRetries,
	)
}

// AckChannelBatch acknowledges multiple messages from a channel in a single operation.
func (pq *PGQueue) AckChannelBatch(
	ctx context.Context,
	channelName string,
	messageIDs []uuid.UUID,
) error {
	if len(messageIDs) == 0 {
		return nil
	}
	if err := validateBatchSize(len(messageIDs)); err != nil {
		return err
	}

	queueMeta, err := pq.getQueueMetadata(
		ctx, string(QueueTypeChannel), channelName,
	)
	if err != nil {
		return fmt.Errorf("failed to get channel metadata: %w", err)
	}

	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		UPDATE pgqueue_msg_%s
		SET status = '%s', processed_at = NOW()
		WHERE id = ANY($1::uuid[])
		  AND status = '%s'
	`, queueMeta.TableName, MessageStatusCompleted, MessageStatusProcessing)

	result, err := pq.db.ExecContext(ctx, query, uuidSliceToStringSlice(messageIDs))
	if err != nil {
		return fmt.Errorf("failed to acknowledge messages: %w", err)
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

// AckTopicBatch acknowledges multiple messages for a subscriber in a single operation.
func (pq *PGQueue) AckTopicBatch(
	ctx context.Context,
	topicName string,
	subscriberID string,
	messageIDs []uuid.UUID,
) error {
	if err := validateSubscriberID(subscriberID); err != nil {
		return err
	}
	if len(messageIDs) == 0 {
		return nil
	}
	if err := validateBatchSize(len(messageIDs)); err != nil {
		return err
	}

	queueMeta, err := pq.getQueueMetadata(
		ctx, string(QueueTypePubSub), topicName,
	)
	if err != nil {
		return fmt.Errorf("failed to get topic metadata: %w", err)
	}

	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		UPDATE pgqueue_sub_%s
		SET status = '%s', acked_at = NOW()
		WHERE message_id = ANY($1::uuid[])
		  AND subscriber_id = $2
		  AND status = '%s'
	`, queueMeta.TableName, MessageStatusAcked, MessageStatusProcessing)

	result, err := pq.db.ExecContext(
		ctx, query, uuidSliceToStringSlice(messageIDs), subscriberID,
	)
	if err != nil {
		return fmt.Errorf("failed to acknowledge messages: %w", err)
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

// NackChannelBatch negatively acknowledges multiple messages from a channel.
// Messages that exceed max retries are moved to DLQ; others are retried.
func (pq *PGQueue) NackChannelBatch(
	ctx context.Context,
	channelName string,
	messageIDs []uuid.UUID,
	errorMsg string,
) error {
	if len(messageIDs) == 0 {
		return nil
	}
	if err := validateBatchSize(len(messageIDs)); err != nil {
		return err
	}

	queueMeta, err := pq.getQueueMetadata(
		ctx, string(QueueTypeChannel), channelName,
	)
	if err != nil {
		return fmt.Errorf("failed to get channel metadata: %w", err)
	}

	tx, err := pq.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	states, err := pq.fetchBatchMessageStates(
		ctx, tx, queueMeta.TableName, messageIDs,
	)
	if err != nil {
		return fmt.Errorf("failed to get message states: %w", err)
	}
	if len(states) == 0 {
		return ErrMessageNotFound
	}

	if err := pq.processNackBatch(ctx, tx, queueMeta.TableName, states, errorMsg); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// processNackBatch partitions messages into retry vs DLQ and processes each group.
func (pq *PGQueue) processNackBatch(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	states []batchMessageState,
	errorMsg string,
) error {
	var retryIDs []uuid.UUID
	var dlqMessages []batchDLQMessage

	for _, s := range states {
		maxRetry := pq.config.DefaultMaxRetries
		if s.maxRetries.Valid && s.maxRetries.Int32 > 0 {
			maxRetry = int(s.maxRetries.Int32)
		}

		if s.retryCount+1 > maxRetry {
			dlqMessages = append(dlqMessages, batchDLQMessage{
				id:           s.id,
				payload:      s.payload,
				retryCount:   s.retryCount + 1,
				metadataJSON: s.metadataJSON,
			})
		} else {
			retryIDs = append(retryIDs, s.id)
		}
	}

	if len(retryIDs) > 0 {
		if err := pq.batchRetryMessages(
			ctx, tx, tableName, retryIDs, errorMsg,
		); err != nil {
			return fmt.Errorf("failed to retry messages: %w", err)
		}
	}

	if len(dlqMessages) > 0 {
		if err := pq.batchMoveToDLQ(
			ctx, tx, tableName, dlqMessages, errorMsg,
		); err != nil {
			return fmt.Errorf("failed to move messages to DLQ: %w", err)
		}
	}

	return nil
}

// validateBatchSize checks if the batch size is within the allowed limit.
func validateBatchSize(size int) error {
	if size > MaxBatchSize {
		return fmt.Errorf(
			"batch size %d exceeds limit %d: %w",
			size, MaxBatchSize, ErrBatchTooLarge,
		)
	}
	return nil
}

// prepareBatchMessages generates IDs and marshals metadata for all messages.
func prepareBatchMessages(messages []PublishMessage) ([]uuid.UUID, [][]byte, error) {
	ids := make([]uuid.UUID, len(messages))
	metadataJSONs := make([][]byte, len(messages))

	for i := range messages {
		var err error
		ids[i], err = NewUUIDv7()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to generate message ID: %w", err)
		}

		if messages[i].Metadata != nil {
			metadataJSONs[i], err = json.Marshal(messages[i].Metadata)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to marshal metadata: %w", err)
			}
		}
	}

	return ids, metadataJSONs, nil
}

// publishBatchToChannel inserts multiple messages into a channel using multi-row INSERT.
func (pq *PGQueue) publishBatchToChannel(
	ctx context.Context,
	tableName string,
	ids []uuid.UUID,
	messages []PublishMessage,
	metadataJSONs [][]byte,
	maxRetries int,
) error {
	var sb strings.Builder
	fmt.Fprintf(&sb,
		"INSERT INTO pgqueue_msg_%s (id, payload, status, metadata, max_retries) VALUES ",
		tableName,
	)

	args := make([]any, 0, len(messages)*channelInsertParams)
	for i := range messages {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * channelInsertParams
		fmt.Fprintf(&sb, "($%d, $%d, '%s', $%d, $%d)",
			base+1, base+2, MessageStatusPending, base+3, base+4, //nolint:mnd // SQL placeholder arithmetic
		)
		args = append(args, ids[i], messages[i].Payload, metadataJSONs[i], maxRetries)
	}
	sb.WriteString(" ON CONFLICT (id) DO NOTHING")

	result, err := pq.db.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("failed to insert messages: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected < int64(len(messages)) {
		return fmt.Errorf(
			"some messages had duplicate IDs: %w", ErrDuplicateMessageID,
		)
	}

	return nil
}

// publishBatchToPubSub inserts multiple messages and subscription records in a transaction.
func (pq *PGQueue) publishBatchToPubSub(
	ctx context.Context,
	topicName, tableName string,
	ids []uuid.UUID,
	messages []PublishMessage,
	metadataJSONs [][]byte,
) error {
	tx, err := pq.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := pq.insertBatchPubSubMessages(ctx, tx, tableName, ids, messages, metadataJSONs); err != nil {
		return err
	}

	subscribers, err := pq.getActiveSubscribers(ctx, tx, topicName)
	if err != nil {
		return fmt.Errorf("failed to get active subscribers: %w", err)
	}

	if len(subscribers) > 0 {
		if err := pq.batchCreateSubscriptionRecords(
			ctx, tx, tableName, ids, subscribers,
		); err != nil {
			return fmt.Errorf("failed to create subscription records: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (pq *PGQueue) insertBatchPubSubMessages(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	ids []uuid.UUID,
	messages []PublishMessage,
	metadataJSONs [][]byte,
) error {
	var sb strings.Builder
	fmt.Fprintf(&sb,
		"INSERT INTO pgqueue_msg_%s (id, payload, metadata) VALUES ",
		tableName,
	)

	args := make([]any, 0, len(messages)*pubsubInsertParams)
	for i := range messages {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * pubsubInsertParams
		fmt.Fprintf(&sb, "($%d, $%d, $%d)",
			base+1, base+2, base+3, //nolint:mnd // SQL placeholder arithmetic
		)
		args = append(args, ids[i], messages[i].Payload, metadataJSONs[i])
	}
	sb.WriteString(" ON CONFLICT (id) DO NOTHING")

	result, err := tx.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("failed to insert messages: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected < int64(len(messages)) {
		return fmt.Errorf(
			"some messages had duplicate IDs: %w", ErrDuplicateMessageID,
		)
	}

	return nil
}

// batchCreateSubscriptionRecords creates subscription records for multiple messages and subscribers.
// Chunks inserts to stay within PostgreSQL's parameter limit.
func (pq *PGQueue) batchCreateSubscriptionRecords(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	messageIDs []uuid.UUID,
	subscribers []Subscriber,
) error {
	// Each row uses 2 params; PostgreSQL limit is 65535
	const maxRowsPerInsert = 16000

	type subRecord struct {
		messageID    uuid.UUID
		subscriberID string
	}

	records := make([]subRecord, 0, len(messageIDs)*len(subscribers))
	for _, msgID := range messageIDs {
		for _, sub := range subscribers {
			records = append(records, subRecord{
				messageID:    msgID,
				subscriberID: sub.SubscriberID,
			})
		}
	}

	for start := 0; start < len(records); start += maxRowsPerInsert {
		end := min(start+maxRowsPerInsert, len(records))
		chunk := records[start:end]

		var sb strings.Builder
		fmt.Fprintf(&sb,
			"INSERT INTO pgqueue_sub_%s (message_id, subscriber_id, status) VALUES ",
			tableName,
		)

		args := make([]any, 0, len(chunk)*subInsertParams)
		for i, rec := range chunk {
			if i > 0 {
				sb.WriteString(", ")
			}
			base := i * subInsertParams
			fmt.Fprintf(&sb, "($%d, $%d, '%s')", base+1, base+2, MessageStatusPending) //nolint:mnd // SQL placeholder arithmetic
			args = append(args, rec.messageID, rec.subscriberID)
		}

		if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
			return fmt.Errorf("failed to insert subscription records: %w", err)
		}
	}

	return nil
}

type batchMessageState struct {
	id           uuid.UUID
	retryCount   int
	maxRetries   sql.NullInt32
	payload      []byte
	metadataJSON sql.NullString
}

type batchDLQMessage struct {
	id           uuid.UUID
	payload      []byte
	retryCount   int
	metadataJSON sql.NullString
}

func (pq *PGQueue) fetchBatchMessageStates(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	messageIDs []uuid.UUID,
) ([]batchMessageState, error) {
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		SELECT id, retry_count, max_retries, payload, metadata
		FROM pgqueue_msg_%s
		WHERE id = ANY($1::uuid[])
		  AND status = '%s'
		FOR UPDATE
	`, tableName, MessageStatusProcessing)

	rows, err := tx.QueryContext(ctx, query, uuidSliceToStringSlice(messageIDs))
	if err != nil {
		return nil, fmt.Errorf("failed to query messages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var states []batchMessageState
	for rows.Next() {
		var s batchMessageState
		if err := rows.Scan(
			&s.id, &s.retryCount, &s.maxRetries,
			&s.payload, &s.metadataJSON,
		); err != nil {
			return nil, fmt.Errorf("failed to scan message: %w", err)
		}
		states = append(states, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate messages: %w", err)
	}

	return states, nil
}

func (pq *PGQueue) batchRetryMessages(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	messageIDs []uuid.UUID,
	errorMsg string,
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		UPDATE pgqueue_msg_%s
		SET status = '%s',
		    retry_count = retry_count + 1,
		    visibility_timeout = NULL,
		    error_message = $2
		WHERE id = ANY($1::uuid[])
	`, tableName, MessageStatusPending)

	_, err := tx.ExecContext(ctx, query, uuidSliceToStringSlice(messageIDs), errorMsg)
	if err != nil {
		return fmt.Errorf("failed to update messages: %w", err)
	}

	return nil
}

func (pq *PGQueue) batchMoveToDLQ(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	messages []batchDLQMessage,
	errorMsg string,
) error {
	var sb strings.Builder
	dlqInsert := "INSERT INTO pgqueue_dlq_" + tableName +
		" (original_message_id, payload, failure_reason, retry_count, metadata) VALUES "
	sb.WriteString(dlqInsert)

	args := make([]any, 0, len(messages)*dlqInsertParams)
	for i, msg := range messages {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * dlqInsertParams
		fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, //nolint:mnd // SQL placeholder arithmetic
		)
		args = append(args, msg.id, msg.payload, errorMsg, msg.retryCount, msg.metadataJSON)
	}

	if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("failed to insert into DLQ: %w", err)
	}

	dlqIDs := make([]uuid.UUID, len(messages))
	for i, msg := range messages {
		dlqIDs[i] = msg.id
	}

	//nolint:gosec // G201: table name validated by queueNameRegex
	deleteQuery := fmt.Sprintf(
		`DELETE FROM pgqueue_msg_%s WHERE id = ANY($1::uuid[])`,
		tableName,
	)
	if _, err := tx.ExecContext(ctx, deleteQuery, uuidSliceToStringSlice(dlqIDs)); err != nil {
		return fmt.Errorf("failed to delete messages: %w", err)
	}

	return nil
}

// uuidSliceToStringSlice converts UUIDs to a string slice for SQL array parameters.
func uuidSliceToStringSlice(ids []uuid.UUID) []string {
	result := make([]string, len(ids))
	for i, id := range ids {
		result[i] = id.String()
	}
	return result
}
