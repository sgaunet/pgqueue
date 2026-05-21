package pgqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// rowsAffectedOrErr returns result.RowsAffected, wrapping any driver error.
// The error must never be discarded: coercing an unavailable count to 0 would
// misreport a valid insert as a duplicate-ID failure, because the duplicate
// check keys off a zero/short row count (R-10).
func rowsAffectedOrErr(result sql.Result) (int64, error) {
	n, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}
	return n, nil
}

// Publish publishes a message to a topic or channel.
// Returns the message ID (UUIDv7) on success.
func (pq *Queue) Publish(
	ctx context.Context,
	queueName string,
	payload []byte,
) (uuid.UUID, error) {
	return pq.PublishWithID(ctx, queueName, uuid.UUID{}, payload, nil)
}

// PublishWithID publishes a message with a specific ID for deduplication.
// If messageID is the zero value (uuid.Nil), a new UUIDv7 will be generated.
// payload must not be nil (use []byte{} for an empty payload).
// metadata is optional and can be nil.
func (pq *Queue) PublishWithID(
	ctx context.Context,
	queueName string,
	messageID uuid.UUID,
	payload []byte,
	metadata map[string]any,
) (uuid.UUID, error) {
	if payload == nil {
		return uuid.UUID{}, ErrNilPayload
	}

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
	queueType := queueMeta.QueueType
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

func (pq *Queue) resolveQueueMetadata(
	ctx context.Context,
	queueName string,
) (*QueueMetadata, error) {
	//nolint:gosec // G201: schema-qualified internal table name, not user input
	query := fmt.Sprintf(`
		SELECT id, queue_type, queue_name, table_name, config, paused, created_at, updated_at
		FROM %s
		WHERE queue_name = $1
	`, pq.globalTable("pgqueue_metadata"))
	rows, err := pq.db.QueryContext(ctx, query, queueName)
	if err != nil {
		return nil, fmt.Errorf("failed to query queue metadata: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var results []QueueMetadata
	for rows.Next() {
		var meta QueueMetadata
		if err := rows.Scan(
			&meta.ID, &meta.QueueType, &meta.QueueName,
			&meta.TableName, &meta.Config, &meta.Paused,
			&meta.CreatedAt, &meta.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan queue metadata: %w", err)
		}
		results = append(results, meta)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate queue metadata: %w", err)
	}

	switch len(results) {
	case 0:
		return nil, fmt.Errorf("%s: %w", queueName, ErrQueueNotFound)
	case 1:
		return &results[0], nil
	default:
		return nil, fmt.Errorf(
			"%s: %w", queueName, ErrAmbiguousQueueName,
		)
	}
}

func (pq *Queue) validatePayloadSize(
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

func (pq *Queue) resolveMaxRetries(
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
func (pq *Queue) publishToPubSub(
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

	// Insert message atomically with conflict detection
	//nolint:gosec // G201: table name validated by queueNameRegex
	insertMsg := fmt.Sprintf(`
		INSERT INTO %s (id, payload, metadata)
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO NOTHING
	`, pq.msgTable(tableName))

	result, err := tx.ExecContext(ctx, insertMsg, messageID, payload, metadata)
	if err != nil {
		return fmt.Errorf("failed to insert message: %w", err)
	}

	rowsAffected, err := rowsAffectedOrErr(result)
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return fmt.Errorf("%s: %w", messageID, ErrDuplicateMessageID)
	}

	if err := pq.createSubscriptionRecords(
		ctx, tx, topicName, tableName, messageID,
	); err != nil {
		return fmt.Errorf("failed to create subscription records: %w", err)
	}

	// Wake any blocked consumer the instant this publish commits (FR-014).
	pq.emitNotify(ctx, tx, tableName)

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (pq *Queue) createSubscriptionRecords(
	ctx context.Context,
	tx *sql.Tx,
	topicName, tableName string,
	messageID uuid.UUID,
) error {
	subscribers, err := pq.getActiveSubscribers(ctx, tx, topicName)
	if err != nil {
		return fmt.Errorf("failed to get active subscribers: %w", err)
	}

	if len(subscribers) == 0 {
		return nil
	}

	// Reuse the chunked multi-row insert so a topic with many subscribers
	// costs one round trip per chunk instead of one per subscriber.
	records := make([]subRecord, 0, len(subscribers))
	for _, sub := range subscribers {
		records = append(records, subRecord{
			messageID:    messageID,
			subscriberID: sub.SubscriberID,
		})
	}

	if err := pq.insertSubscriptionRecords(ctx, tx, tableName, records); err != nil {
		return fmt.Errorf("failed to create subscription records: %w", err)
	}

	return nil
}

// PublishChannel publishes a single message to a point-to-point channel.
// Returns the message ID (UUIDv7) on success.
//
// Use WithMessageID to supply a deterministic ID for deduplication, and
// WithMessageMetadata to attach metadata to the message.
func (pq *Queue) PublishChannel(
	ctx context.Context,
	name string,
	payload []byte,
	opts ...PublishOption,
) (uuid.UUID, error) {
	if err := pq.checkClosed(); err != nil {
		return uuid.UUID{}, err
	}
	ctx, span := pq.startSpan(ctx, "pgqueue.publish",
		StringAttr("queue", name), StringAttr("queue_type", "channel"))
	o := applyPublishOptions(opts)
	id, err := pq.publishTyped(ctx, name, QueueTypeChannel, payload, o.messageID, o.metadata)
	endSpan(span, err)
	if err == nil {
		pq.recordPublish(name, 1)
	}
	return id, err
}

// PublishTopic publishes a single message to a pub/sub topic, delivering it
// to all active subscribers.
// Returns the message ID (UUIDv7) on success.
func (pq *Queue) PublishTopic(
	ctx context.Context,
	name string,
	payload []byte,
	opts ...PublishOption,
) (uuid.UUID, error) {
	if err := pq.checkClosed(); err != nil {
		return uuid.UUID{}, err
	}
	ctx, span := pq.startSpan(ctx, "pgqueue.publish",
		StringAttr("queue", name), StringAttr("queue_type", "pubsub"))
	o := applyPublishOptions(opts)
	id, err := pq.publishTyped(ctx, name, QueueTypePubSub, payload, o.messageID, o.metadata)
	endSpan(span, err)
	if err == nil {
		pq.recordPublish(name, 1)
	}
	return id, err
}

// publishTyped is the shared implementation for PublishChannel/PublishTopic.
func (pq *Queue) publishTyped(
	ctx context.Context,
	name string,
	queueType QueueType,
	payload []byte,
	messageID uuid.UUID,
	metadata map[string]any,
) (uuid.UUID, error) {
	if payload == nil {
		return uuid.UUID{}, ErrNilPayload
	}

	queueMeta, err := pq.getQueueMetadata(ctx, string(queueType), name)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("%s: %w", name, err)
	}
	if err := pq.validatePayloadSize(queueMeta, payload); err != nil {
		return uuid.UUID{}, err
	}
	if messageID == uuid.Nil {
		messageID, err = NewUUIDv7()
		if err != nil {
			return uuid.UUID{}, fmt.Errorf("failed to generate message ID: %w", err)
		}
	}
	var metadataJSON []byte
	if metadata != nil {
		metadataJSON, err = json.Marshal(metadata)
		if err != nil {
			return uuid.UUID{}, fmt.Errorf("failed to marshal metadata: %w", err)
		}
	}

	if queueType == QueueTypePubSub {
		return messageID, pq.publishToPubSub(
			ctx, queueMeta.QueueName, queueMeta.TableName,
			messageID, payload, metadataJSON,
		)
	}
	maxRetries := pq.resolveMaxRetries(queueMeta)
	return messageID, pq.publishToChannel(
		ctx, queueMeta.TableName, messageID, payload, metadataJSON, maxRetries,
	)
}

// PublishChannelBatch publishes multiple messages to a point-to-point channel
// in a single atomic operation. Returns message IDs in the same order as the
// input messages.
func (pq *Queue) PublishChannelBatch(
	ctx context.Context,
	name string,
	msgs []PublishMessage,
) ([]uuid.UUID, error) {
	if err := pq.checkClosed(); err != nil {
		return nil, err
	}
	// Delegate to the existing batch implementation which resolves queue type
	// from metadata.
	return pq.PublishBatch(ctx, name, msgs)
}

// PublishTopicBatch publishes multiple messages to a pub/sub topic in a single
// atomic operation. Returns message IDs in the same order as the input messages.
func (pq *Queue) PublishTopicBatch(
	ctx context.Context,
	name string,
	msgs []PublishMessage,
) ([]uuid.UUID, error) {
	if err := pq.checkClosed(); err != nil {
		return nil, err
	}
	return pq.PublishBatch(ctx, name, msgs)
}

// publishToChannel publishes a message to a point-to-point channel.
func (pq *Queue) publishToChannel(
	ctx context.Context,
	tableName string,
	messageID uuid.UUID,
	payload []byte,
	metadata []byte,
	maxRetries int,
) error {
	tx, err := pq.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Insert message atomically with conflict detection
	//nolint:gosec // G201: table name validated by queueNameRegex
	insertMsg := fmt.Sprintf(`
		INSERT INTO %s (id, payload, status, metadata, max_retries)
		VALUES ($1, $2, '%s', $3, $4)
		ON CONFLICT (id) DO NOTHING
	`, pq.msgTable(tableName), MessageStatusPending)

	result, err := tx.ExecContext(
		ctx, insertMsg, messageID, payload, metadata, maxRetries,
	)
	if err != nil {
		return fmt.Errorf("failed to insert message: %w", err)
	}

	rowsAffected, err := rowsAffectedOrErr(result)
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return fmt.Errorf("%s: %w", messageID, ErrDuplicateMessageID)
	}

	// Wake any blocked consumer the instant this publish commits (FR-014).
	pq.emitNotify(ctx, tx, tableName)

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
