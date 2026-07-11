package pgqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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

// Publish publishes a single message to the named channel or topic and returns
// the message ID (UUIDv7). The queue type is resolved from the queue's metadata,
// so the same call serves both channels and topics — the publisher does not need
// to know which it is talking to.
//
// Use WithMessageID to supply a deterministic ID for publish-side dedup and
// WithMessageMetadata to attach metadata. payload must not be nil (use []byte{}
// for an empty payload).
func (pq *Queue) Publish(
	ctx context.Context,
	queueName string,
	payload []byte,
	opts ...PublishOption,
) (uuid.UUID, error) {
	if err := pq.checkClosed(); err != nil {
		return uuid.UUID{}, err
	}
	if payload == nil {
		return uuid.UUID{}, ErrNilPayload
	}
	o := applyPublishOptions(opts)

	ctx, span := pq.startSpan(ctx, "pgqueue.publish", StringAttr("queue", queueName))
	id, err := pq.publishResolved(ctx, queueName, payload, o.messageID, o.metadata)
	pq.endSpan(span, err)
	if err == nil {
		pq.recordPublish(queueName, 1)
	}
	return id, err
}

// publishResolved resolves the queue metadata by name — the UNIQUE(table_name)
// constraint on pgqueue_metadata guarantees at most one match across both queue
// types — and inserts the message into the channel or pub/sub table accordingly.
func (pq *Queue) publishResolved(
	ctx context.Context,
	queueName string,
	payload []byte,
	messageID uuid.UUID,
	metadata map[string]any,
) (uuid.UUID, error) {
	queueMeta, err := pq.resolveQueueMetadata(ctx, queueName)
	if err != nil {
		return uuid.UUID{}, err
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
	metadataJSON, err := pq.marshalAndValidateMetadata(queueMeta, metadata)
	if err != nil {
		return uuid.UUID{}, err
	}
	if queueMeta.QueueType == QueueTypePubSub {
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

func (pq *Queue) resolveQueueMetadata(
	ctx context.Context,
	queueName string,
) (*queueMetadata, error) {
	// Type-agnostic lookup: Publish/PublishBatch does not know up-front whether
	// queueName is a channel or a topic — the returned queueMetadata carries the
	// resolved type and the caller dispatches on it. No queue_type filter is
	// added here because the caller has no expected type to filter on.
	//
	// The UNIQUE(table_name) constraint on pgqueue_metadata (see baseSchemaSQL)
	// guarantees at most one row matches: a channel and a topic with the same
	// queue_name would sanitize to the same physical table name and the second
	// CreateChannel/CreateTopic would be rejected at creation time.
	// The table name is a schema-qualified internal identifier, not user input,
	// so this interpolation is injection-safe.
	query := fmt.Sprintf(`
		SELECT id, queue_type, queue_name, table_name, config, paused, created_at, updated_at
		FROM %s
		WHERE queue_name = $1
	`, pq.globalTable("pgqueue_metadata"))
	var meta queueMetadata
	err := pq.db.QueryRowContext(ctx, query, queueName).Scan(
		&meta.ID, &meta.QueueType, &meta.QueueName,
		&meta.TableName, &meta.Config, &meta.Paused,
		&meta.CreatedAt, &meta.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		// No queue_type prefix: this path is type-agnostic so the type is unknown
		// at the time of the not-found return.
		return nil, fmt.Errorf("%s: %w", queueName, ErrQueueNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query queue metadata: %w", err)
	}
	return &meta, nil
}

// resolveMaxMessageSize returns the effective payload-size cap for a queue,
// preferring the per-queue MaxMessageSize from its stored config and falling
// back to the queue-wide cap.
func (pq *Queue) resolveMaxMessageSize(queueMeta *queueMetadata) int {
	var config struct {
		MaxMessageSize int `json:"MaxMessageSize"`
	}
	if err := json.Unmarshal(queueMeta.Config, &config); err != nil {
		// A corrupt config row is library-written, so this should never happen;
		// surface it rather than silently applying a possibly-different cap.
		pq.logWarn("failed to unmarshal queue config for max message size; using queue-wide default",
			"queue", queueMeta.QueueName, "error", err)
		config.MaxMessageSize = pq.cfg.maxMessageSize
	}
	if config.MaxMessageSize == 0 {
		config.MaxMessageSize = pq.cfg.maxMessageSize
	}
	return config.MaxMessageSize
}

// checkPayloadSize rejects a payload that exceeds the already-resolved cap.
func checkPayloadSize(maxMessageSize int, payload []byte) error {
	if len(payload) > maxMessageSize {
		return fmt.Errorf(
			"size %d exceeds limit %d: %w",
			len(payload), maxMessageSize, ErrMessageSizeExceeded,
		)
	}
	return nil
}

func (pq *Queue) validatePayloadSize(
	queueMeta *queueMetadata,
	payload []byte,
) error {
	return checkPayloadSize(pq.resolveMaxMessageSize(queueMeta), payload)
}

// resolveMaxMetadataSize returns the effective metadata-size cap for a queue,
// preferring the per-queue MaxMetadataSize from its stored config and falling
// back to the queue-wide cap.
func (pq *Queue) resolveMaxMetadataSize(queueMeta *queueMetadata) int {
	var config struct {
		MaxMetadataSize int `json:"MaxMetadataSize"`
	}
	if err := json.Unmarshal(queueMeta.Config, &config); err != nil {
		pq.logWarn("failed to unmarshal queue config for max metadata size; using queue-wide default",
			"queue", queueMeta.QueueName, "error", err)
	} else if config.MaxMetadataSize > 0 {
		return config.MaxMetadataSize
	}
	return pq.cfg.maxMetadataSize
}

// marshalAndValidateMetadata marshals message metadata to JSON and rejects it
// if the result exceeds the per-queue or queue-wide cap. A nil metadata map
// returns (nil, nil); the caller stores no JSONB value in that case.
func (pq *Queue) marshalAndValidateMetadata(
	queueMeta *queueMetadata,
	metadata map[string]any,
) ([]byte, error) {
	if metadata == nil {
		return nil, nil
	}
	return marshalMetadataWithLimit(pq.resolveMaxMetadataSize(queueMeta), metadata)
}

// marshalMetadataWithLimit marshals metadata to JSON and rejects it against an
// already-resolved cap (limit <= 0 disables the check). A nil metadata map
// returns (nil, nil). Batch callers resolve the cap once and reuse it across
// every message rather than re-unmarshaling the queue config per message.
func marshalMetadataWithLimit(limit int, metadata map[string]any) ([]byte, error) {
	if metadata == nil {
		return nil, nil
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}
	if limit > 0 && len(metadataJSON) > limit {
		return nil, fmt.Errorf(
			"size %d exceeds limit %d: %w",
			len(metadataJSON), limit, ErrMetadataSizeExceeded,
		)
	}
	return metadataJSON, nil
}

// resolveMaxRetries resolves the effective max-retry count for a queue from its
// stored config, falling back to the Queue-wide default.
//
// The resolution is backward compatible. New queues record MaxRetriesSet, so an
// explicit MaxRetries of 0 ("dead-letter on first failure") is honored. Queues
// created before MaxRetriesSet existed have no flag: there, a positive
// MaxRetries was an explicit cap and a zero MaxRetries meant "use the default"
// — an explicit 0 was not expressible, so nothing regresses.
func (pq *Queue) resolveMaxRetries(
	queueMeta *queueMetadata,
) int {
	var opts struct {
		MaxRetries    int  `json:"MaxRetries"`
		MaxRetriesSet bool `json:"MaxRetriesSet"`
	}
	if err := json.Unmarshal(queueMeta.Config, &opts); err != nil {
		pq.logWarn("failed to unmarshal queue config for max retries; using queue-wide default",
			"queue", queueMeta.QueueName, "error", err)
	} else {
		if opts.MaxRetriesSet {
			return opts.MaxRetries
		}
		if opts.MaxRetries > 0 {
			return opts.MaxRetries
		}
	}

	return pq.cfg.defaultMaxRetries
}

// publishToPubSub publishes a message to a pub/sub topic.
func (pq *Queue) publishToPubSub(
	ctx context.Context,
	topicName, tableName string,
	messageID uuid.UUID,
	payload []byte,
	metadata []byte,
) error {
	return pq.withTx(ctx, func(tx *sql.Tx) error {
		// Insert message atomically with conflict detection
		//nolint:gosec // G201: table name validated by queueNameRegex
		insertMsg := fmt.Sprintf(`
		INSERT INTO %s (id, payload, metadata)
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO NOTHING
	`, pq.msgTable(tableName))

		result, err := tx.ExecContext(ctx, insertMsg, messageID, payload, jsonbParam(metadata))
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

		return nil
	})
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

// publishToChannel publishes a message to a point-to-point channel.
func (pq *Queue) publishToChannel(
	ctx context.Context,
	tableName string,
	messageID uuid.UUID,
	payload []byte,
	metadata []byte,
	maxRetries int,
) error {
	return pq.withTx(ctx, func(tx *sql.Tx) error {
		// Insert message atomically with conflict detection
		//nolint:gosec // G201: table name validated by queueNameRegex
		insertMsg := fmt.Sprintf(`
		INSERT INTO %s (id, payload, status, metadata, max_retries)
		VALUES ($1, $2, '%s', $3, $4)
		ON CONFLICT (id) DO NOTHING
	`, pq.msgTable(tableName), MessageStatusPending)

		result, err := tx.ExecContext(
			ctx, insertMsg, messageID, payload, jsonbParam(metadata), maxRetries,
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

		return nil
	})
}
