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

// getQueueTTL extracts the TTL from queue config JSON, falling back to the
// default TTL from Queue config. Returns 0 if no TTL is configured.
func (pq *Queue) getQueueTTL(configJSON []byte) time.Duration {
	var cfg struct {
		TTL time.Duration `json:"TTL"`
	}

	if len(configJSON) > 0 {
		if err := json.Unmarshal(configJSON, &cfg); err != nil {
			pq.logError("failed to unmarshal queue config for TTL", "error", err)
		} else if cfg.TTL > 0 {
			return cfg.TTL
		}
	}

	return pq.config.DefaultTTL
}

// parseMetadataJSON parses a nullable JSON string into a metadata map. Corrupt
// metadata is logged and treated as absent rather than failing the surrounding
// consume, so a single bad row cannot wedge a consumer.
func (pq *Queue) parseMetadataJSON(s sql.NullString) map[string]any {
	if !s.Valid || s.String == "" {
		return nil
	}

	var m map[string]any
	if err := json.Unmarshal([]byte(s.String), &m); err != nil {
		pq.logError("failed to parse message metadata; dropping it", "error", err)
		return nil
	}

	return m
}

// classifyClaimMiss explains why an Ack/Nack matched no row. statusQuery must
// SELECT (status TEXT, claim_id UUID) for the targeted row. It returns
// ErrClaimExpired when the row is still processing under a different claim
// token, ErrMessageNotFound when the row no longer exists, and
// ErrMessageAlreadyAcked otherwise (already completed/acked).
func classifyClaimMiss(
	ctx context.Context,
	q queryRower,
	statusQuery string,
	expectedClaim uuid.UUID,
	args ...any,
) error {
	var status string
	var claimID uuid.NullUUID
	err := q.QueryRowContext(ctx, statusQuery, args...).Scan(&status, &claimID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrMessageNotFound
	}
	if err != nil {
		return fmt.Errorf("failed to classify claim miss: %w", err)
	}
	if MessageStatus(status) == MessageStatusProcessing &&
		claimID.Valid && claimID.UUID != expectedClaim {
		return ErrClaimExpired
	}

	return ErrMessageAlreadyAcked
}

// classifyChannelAckMiss classifies a failed channel Ack/Nack for the receipt.
func classifyChannelAckMiss(
	ctx context.Context,
	q queryRower,
	tableName string,
	r Receipt,
) error {
	query := fmt.Sprintf(
		`SELECT status, claim_id FROM pgqueue_msg_%s WHERE id = $1`, tableName,
	)

	return classifyClaimMiss(ctx, q, query, r.ClaimID, r.MessageID)
}

// classifyTopicAckMiss classifies a failed topic Ack/Nack for the given
// subscriber and receipt.
func classifyTopicAckMiss(
	ctx context.Context,
	q queryRower,
	tableName, subscriberID string,
	r Receipt,
) error {
	query := fmt.Sprintf(
		`SELECT status, claim_id FROM pgqueue_sub_%s WHERE message_id = $1 AND subscriber_id = $2`,
		tableName,
	)

	return classifyClaimMiss(ctx, q, query, r.ClaimID, r.MessageID, subscriberID)
}

// Metadata query methods

func (pq *Queue) getQueueMetadata(
	ctx context.Context,
	queueType, queueName string,
) (*QueueMetadata, error) {
	query := `
		SELECT id, queue_type, queue_name, table_name, config, paused, created_at, updated_at
		FROM pgqueue_metadata
		WHERE queue_type = $1 AND queue_name = $2
		LIMIT 1
	`

	var meta QueueMetadata
	err := pq.db.QueryRowContext(ctx, query, queueType, queueName).Scan(
		&meta.ID,
		&meta.QueueType,
		&meta.QueueName,
		&meta.TableName,
		&meta.Config,
		&meta.Paused,
		&meta.CreatedAt,
		&meta.UpdatedAt,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrQueueNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan queue metadata: %w", err)
	}

	// Populate the table-name cache. Only the immutable table_name field is
	// cached; mutable fields (paused, config) are always read fresh from the DB.
	if pq.mdcache != nil {
		pq.mdcache.set(queueType, queueName, meta.TableName)
	}

	return &meta, nil
}

// cachedTableName returns the physical table name for the given queue, using the
// metadata cache when available to avoid a database round-trip. When the cache
// misses it falls back to a targeted SELECT of just the table_name column.
func (pq *Queue) cachedTableName(ctx context.Context, queueType, queueName string) (string, error) {
	if pq.mdcache != nil {
		if name, ok := pq.mdcache.get(queueType, queueName); ok {
			return name, nil
		}
	}
	// Cache miss: fetch and store.
	meta, err := pq.getQueueMetadata(ctx, queueType, queueName)
	if err != nil {
		return "", err
	}
	return meta.TableName, nil
}

func (pq *Queue) createQueueMetadata(
	ctx context.Context,
	tx *sql.Tx,
	queueType, queueName, tableName string,
	config []byte,
) (*QueueMetadata, error) {
	query := `
		INSERT INTO pgqueue_metadata (queue_type, queue_name, table_name, config)
		VALUES ($1, $2, $3, $4)
		RETURNING id, queue_type, queue_name, table_name, config, paused, created_at, updated_at
	`

	var meta QueueMetadata
	err := tx.QueryRowContext(ctx, query, queueType, queueName, tableName, config).Scan(
		&meta.ID,
		&meta.QueueType,
		&meta.QueueName,
		&meta.TableName,
		&meta.Config,
		&meta.Paused,
		&meta.CreatedAt,
		&meta.UpdatedAt,
	)

	if err != nil {
		if isUniqueViolation(err) {
			return nil, fmt.Errorf("%s/%s: %w", queueType, queueName, ErrQueueAlreadyExists)
		}
		return nil, fmt.Errorf("failed to insert queue metadata: %w", err)
	}

	return &meta, nil
}

// countQueues returns the total number of queues (channels and topics) currently
// registered in pgqueue_metadata. The count runs inside the supplied transaction
// so it is consistent with the metadata insert that follows.
func (pq *Queue) countQueues(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM pgqueue_metadata`).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count queues: %w", err)
	}

	return count, nil
}

// pgUniqueViolation is the PostgreSQL SQLSTATE for a unique-constraint
// violation.
const pgUniqueViolation = "23505"

// sqlStater is satisfied by the error types of PostgreSQL drivers that expose
// the SQLSTATE as a string (notably pgx's *pgconn.PgError). Matching this local
// interface keeps pgqueue free of a compile-time dependency on any specific
// driver.
type sqlStater interface {
	SQLState() string
}

// isUniqueViolation reports whether err is a PostgreSQL unique-constraint
// violation (SQLSTATE 23505). It is driver-agnostic: pgx errors are matched via
// the sqlStater interface, while drivers whose SQLSTATE accessor has a
// different shape (such as lib/pq) are matched on the error text.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	var s sqlStater
	if errors.As(err, &s) {
		return s.SQLState() == pgUniqueViolation
	}
	errStr := err.Error()
	return strings.Contains(errStr, pgUniqueViolation) ||
		strings.Contains(errStr, "unique constraint")
}

// PostgreSQL SQLSTATE codes for transient failures that a retry can resolve.
const (
	pgSerializationFailure = "40001"
	pgDeadlockDetected     = "40P01"
)

// isTransientError reports whether err is a transient database failure that a
// bounded retry can plausibly resolve: a serialization failure (40001), a
// deadlock (40P01), or a connection-level error (FR-026). It is driver-agnostic
// — SQLSTATE is read via the sqlStater interface, with an error-text fallback
// for drivers (such as lib/pq) whose accessor has a different shape.
func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	var s sqlStater
	if errors.As(err, &s) {
		switch s.SQLState() {
		case pgSerializationFailure, pgDeadlockDetected:
			return true
		}
	}
	es := err.Error()
	return strings.Contains(es, pgSerializationFailure) ||
		strings.Contains(es, pgDeadlockDetected) ||
		strings.Contains(es, "connection refused") ||
		strings.Contains(es, "connection reset") ||
		strings.Contains(es, "bad connection") ||
		strings.Contains(es, "broken pipe")
}

func (pq *Queue) checkTableNameNotExists(
	ctx context.Context,
	tableName string,
) error {
	query := `SELECT queue_name FROM pgqueue_metadata WHERE table_name = $1 LIMIT 1`

	var existingName string

	err := pq.db.QueryRowContext(ctx, query, tableName).Scan(&existingName)
	if err == nil {
		return fmt.Errorf(
			"table name %q conflicts with existing queue %q: %w",
			tableName, existingName, ErrQueueAlreadyExists,
		)
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to check table name: %w", err)
	}

	return nil
}

func (pq *Queue) listQueuesRaw(
	ctx context.Context,
	queueType string,
) ([]QueueMetadata, error) {
	query := `
		SELECT id, queue_type, queue_name, table_name, config, paused, created_at, updated_at
		FROM pgqueue_metadata
		WHERE queue_type = $1
		ORDER BY created_at DESC
	`

	rows, err := pq.db.QueryContext(ctx, query, queueType)
	if err != nil {
		return nil, fmt.Errorf("failed to query queues: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := []QueueMetadata{}
	for rows.Next() {
		var meta QueueMetadata
		if err := rows.Scan(
			&meta.ID,
			&meta.QueueType,
			&meta.QueueName,
			&meta.TableName,
			&meta.Config,
			&meta.Paused,
			&meta.CreatedAt,
			&meta.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan queue row: %w", err)
		}
		items = append(items, meta)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate queue rows: %w", err)
	}

	return items, nil
}

// Subscriber query methods

func (pq *Queue) registerSubscriber(
	ctx context.Context,
	topicName, subscriberID string,
) (*Subscriber, error) {
	query := `
		INSERT INTO pgqueue_subscribers (topic_name, subscriber_id)
		VALUES ($1, $2)
		ON CONFLICT (topic_name, subscriber_id)
		DO UPDATE SET active = TRUE
		RETURNING id, topic_name, subscriber_id, created_at, active
	`

	var sub Subscriber
	err := pq.db.QueryRowContext(ctx, query, topicName, subscriberID).Scan(
		&sub.ID,
		&sub.TopicName,
		&sub.SubscriberID,
		&sub.CreatedAt,
		&sub.Active,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to scan subscriber: %w", err)
	}

	return &sub, nil
}

func (pq *Queue) unregisterSubscriber(
	ctx context.Context,
	topicName, subscriberID string,
) error {
	query := `
		UPDATE pgqueue_subscribers
		SET active = FALSE
		WHERE topic_name = $1 AND subscriber_id = $2 AND active = TRUE
	`
	result, err := pq.db.ExecContext(ctx, query, topicName, subscriberID)
	if err != nil {
		return fmt.Errorf("failed to unregister subscriber: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return ErrSubscriberNotFound
	}

	return nil
}

func (pq *Queue) getActiveSubscribers(
	ctx context.Context,
	tx *sql.Tx,
	topicName string,
) ([]Subscriber, error) {
	query := `
		SELECT id, topic_name, subscriber_id, created_at, active
		FROM pgqueue_subscribers
		WHERE topic_name = $1 AND active = TRUE
		ORDER BY created_at
	`

	var rows *sql.Rows
	var err error
	if tx != nil {
		rows, err = tx.QueryContext(ctx, query, topicName)
	} else {
		rows, err = pq.db.QueryContext(ctx, query, topicName)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query subscribers: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := []Subscriber{}
	for rows.Next() {
		var sub Subscriber
		if err := rows.Scan(
			&sub.ID,
			&sub.TopicName,
			&sub.SubscriberID,
			&sub.CreatedAt,
			&sub.Active,
		); err != nil {
			return nil, fmt.Errorf("failed to scan subscriber row: %w", err)
		}
		items = append(items, sub)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate subscriber rows: %w", err)
	}

	return items, nil
}

// Delete query methods

func (pq *Queue) deleteQueueMetadata(
	ctx context.Context,
	tx *sql.Tx,
	queueType, queueName string,
) error {
	// Delete replay log entries for this queue
	replayQuery := `DELETE FROM pgqueue_replay_log WHERE queue_type = $1 AND queue_name = $2`
	if _, err := tx.ExecContext(ctx, replayQuery, queueType, queueName); err != nil {
		return fmt.Errorf("failed to delete replay log entries: %w", err)
	}

	// For pub/sub, delete subscriber registrations
	if queueType == string(QueueTypePubSub) {
		subQuery := `DELETE FROM pgqueue_subscribers WHERE topic_name = $1`
		if _, err := tx.ExecContext(ctx, subQuery, queueName); err != nil {
			return fmt.Errorf("failed to delete subscriber registrations: %w", err)
		}
	}

	// Delete metadata entry
	metaQuery := `DELETE FROM pgqueue_metadata WHERE queue_type = $1 AND queue_name = $2`
	if _, err := tx.ExecContext(ctx, metaQuery, queueType, queueName); err != nil {
		return fmt.Errorf("failed to delete queue metadata: %w", err)
	}

	return nil
}

// Replay log query methods

// createReplayLog writes the replay audit row. It always runs inside the caller's
// transaction so the audit record commits atomically with the replay itself
// (FR-007): if the replay rolls back, the log row never appears.
func (pq *Queue) createReplayLog(
	ctx context.Context,
	tx *sql.Tx,
	queueType, queueName, replayType string,
	replayParams []byte,
	messageCount int,
	createdBy *string,
) error {
	query := `
		INSERT INTO pgqueue_replay_log (
			queue_type, queue_name, replay_type,
			replay_params, message_count, created_by
		)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	_, err := tx.ExecContext(
		ctx, query,
		queueType, queueName, replayType,
		replayParams, messageCount, createdBy,
	)
	if err != nil {
		return fmt.Errorf("failed to create replay log: %w", err)
	}

	return nil
}

func (pq *Queue) getReplayHistory(
	ctx context.Context,
	queueType, queueName string,
	limit int,
) ([]ReplayLog, error) {
	query := `
		SELECT id, queue_type, queue_name, replay_type,
		       replay_params, message_count, created_at, created_by
		FROM pgqueue_replay_log
		WHERE queue_type = $1 AND queue_name = $2
		ORDER BY created_at DESC
		LIMIT $3
	`

	rows, err := pq.db.QueryContext(ctx, query, queueType, queueName, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query replay history: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := []ReplayLog{}
	for rows.Next() {
		var rl ReplayLog
		if err := rows.Scan(
			&rl.ID,
			&rl.QueueType,
			&rl.QueueName,
			&rl.ReplayType,
			&rl.ReplayParams,
			&rl.MessageCount,
			&rl.CreatedAt,
			&rl.CreatedBy,
		); err != nil {
			return nil, fmt.Errorf("failed to scan replay log row: %w", err)
		}
		items = append(items, rl)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate replay log rows: %w", err)
	}

	return items, nil
}
