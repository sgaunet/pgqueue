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

	return pq.cfg.defaultTTL
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
// SELECT (status TEXT, claim_id UUID) for the targeted row. It returns:
//
//   - ErrMessageNotFound — the row no longer exists.
//   - ErrClaimExpired — the caller held a real claim token (a non-zero
//     ClaimID, as every delivery mints) but it no longer matches the row.
//     This covers every way the claim is fenced: redelivered to another
//     consumer (processing under a new token), or reset to pending by a
//     retry/nack or the garbage collector (claim_id cleared to NULL). The
//     caller's processing result is stale.
//   - ErrMessageAlreadyAcked — the catch-all "not in processing state under
//     your claim": the receipt's own claim still owns the row but it has left
//     'processing' (completed/acked), or the caller never held a claim at all
//     (a zero ClaimID — e.g. acking a message that was never consumed).
//
// Earlier revisions returned ErrClaimExpired only for the "processing under a
// new token" case, so a timed-out message reset to pending by the GC was
// misreported as ErrMessageAlreadyAcked; keying off the claim token fixes that
// without reclassifying a never-consumed (zero-claim) ack.
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
		return classifyClaimState(false, "", uuid.NullUUID{}, expectedClaim)
	}
	if err != nil {
		return fmt.Errorf("failed to classify claim miss: %w", err)
	}
	return classifyClaimState(true, status, claimID, expectedClaim)
}

// classifyClaimState is the pure decision shared by the single-receipt
// (classifyClaimMiss) and batch (classifyBatchMisses) ack/nack-miss paths, so
// both classify a receipt identically. found reports whether the targeted row
// still exists; status and claimID are its current values when found. It
// returns:
//
//   - ErrMessageNotFound — the row no longer exists.
//   - ErrMessageAlreadyAcked — the caller never held a real claim (a zero
//     expectedClaim — e.g. acking a message that was never consumed), or the
//     receipt's own claim still owns the row but it has left 'processing'
//     (completed/acked).
//   - ErrClaimExpired — the caller held a real claim token (non-zero, as every
//     delivery mints) but it no longer matches the row: fenced by a redelivery,
//     a retry/nack, or a garbage-collector reset. The processing result is stale.
func classifyClaimState(
	found bool,
	status string,
	claimID uuid.NullUUID,
	expectedClaim uuid.UUID,
) error {
	if !found {
		return ErrMessageNotFound
	}
	// No real claim was presented (zero ClaimID): the caller never legitimately
	// consumed this message, since every delivery mints a non-zero claim. This
	// is not an expired claim — report the generic sentinel.
	if expectedClaim == (uuid.UUID{}) {
		return ErrMessageAlreadyAcked
	}
	// The caller's own claim still owns the row, but it has left 'processing'.
	if claimID.Valid && claimID.UUID == expectedClaim &&
		MessageStatus(status) != MessageStatusProcessing {
		return ErrMessageAlreadyAcked
	}
	// A real claim that no longer matches the row: fenced by a redelivery, a
	// retry/nack, or a garbage-collector reset.
	return ErrClaimExpired
}

// classifyChannelAckMiss classifies a failed channel Ack/Nack for the receipt.
// msgTable is the schema-qualified channel message table.
func classifyChannelAckMiss(
	ctx context.Context,
	q queryRower,
	msgTable string,
	r Receipt,
) error {
	query := fmt.Sprintf(
		`SELECT status, claim_id FROM %s WHERE id = $1`, msgTable,
	)

	return classifyClaimMiss(ctx, q, query, r.ClaimID, r.MessageID)
}

// classifyTopicAckMiss classifies a failed topic Ack/Nack for the given
// subscriber and receipt. subTable is the schema-qualified subscription table.
func classifyTopicAckMiss(
	ctx context.Context,
	q queryRower,
	subTable, subscriberID string,
	r Receipt,
) error {
	query := fmt.Sprintf(
		`SELECT status, claim_id FROM %s WHERE message_id = $1 AND subscriber_id = $2`,
		subTable,
	)

	return classifyClaimMiss(ctx, q, query, r.ClaimID, r.MessageID, subscriberID)
}

// claimRowState is a row's current (status, claim_id) as read by the batch
// claim-miss classifier.
type claimRowState struct {
	status  string
	claimID uuid.NullUUID
}

// classifyBatchMisses classifies each receipt that did not match a live
// processing row, mirroring classifyClaimMiss per receipt over a single status
// SELECT (instead of one query per receipt). statusQuery must SELECT
// (id UUID, status TEXT, claim_id UUID) for the rows identified by its leading
// uuid-array parameter ($1::text::uuid[]); extraArgs supplies any trailing
// parameters (e.g. the subscriber_id for topics). Receipts whose ID is absent
// from the result are reported ErrMessageNotFound. The returned slice is index
// aligned with misses, preserving input order. Receipts are assumed unique.
func classifyBatchMisses(
	ctx context.Context,
	tx *sql.Tx,
	statusQuery string,
	misses []Receipt,
	extraArgs ...any,
) ([]FailedReceipt, error) {
	if len(misses) == 0 {
		return nil, nil
	}

	ids := make([]uuid.UUID, len(misses))
	for i, r := range misses {
		ids[i] = r.MessageID
	}
	args := append([]any{uuidArrayLiteral(ids)}, extraArgs...)

	rows, err := tx.QueryContext(ctx, statusQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to classify batch claim misses: %w", err)
	}
	defer func() { _ = rows.Close() }()

	states := make(map[uuid.UUID]claimRowState, len(misses))
	for rows.Next() {
		var id uuid.UUID
		var st claimRowState
		if err := rows.Scan(&id, &st.status, &st.claimID); err != nil {
			return nil, fmt.Errorf("failed to scan claim state: %w", err)
		}
		states[id] = st
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate claim states: %w", err)
	}

	failed := make([]FailedReceipt, len(misses))
	for i, r := range misses {
		st, found := states[r.MessageID]
		failed[i] = FailedReceipt{
			Receipt: r,
			Reason:  classifyClaimState(found, st.status, st.claimID, r.ClaimID),
		}
	}
	return failed, nil
}

// classifyChannelBatchMisses classifies receipts that missed a channel batch
// ack/nack. msgTable is the schema-qualified channel message table.
func classifyChannelBatchMisses(
	ctx context.Context,
	tx *sql.Tx,
	msgTable string,
	misses []Receipt,
) ([]FailedReceipt, error) {
	// G201 is not applicable: the query (with the regex-validated table name) is
	// passed to classifyBatchMisses as a parameter, not built at the exec site.
	query := fmt.Sprintf(
		`SELECT id, status, claim_id FROM %s WHERE id = ANY($1::text::uuid[])`, msgTable,
	)
	return classifyBatchMisses(ctx, tx, query, misses)
}

// classifyTopicBatchMisses classifies receipts that missed a topic batch
// ack/nack for the given subscriber. subTable is the schema-qualified
// subscription table.
func classifyTopicBatchMisses(
	ctx context.Context,
	tx *sql.Tx,
	subTable, subscriberID string,
	misses []Receipt,
) ([]FailedReceipt, error) {
	// G201 is not applicable: the query (with the regex-validated table name) is
	// passed to classifyBatchMisses as a parameter, not built at the exec site.
	query := fmt.Sprintf(
		`SELECT message_id, status, claim_id FROM %s `+
			`WHERE message_id = ANY($1::text::uuid[]) AND subscriber_id = $2`,
		subTable,
	)
	return classifyBatchMisses(ctx, tx, query, misses, subscriberID)
}

// Metadata query methods

func (pq *Queue) getQueueMetadata(
	ctx context.Context,
	queueType, queueName string,
) (*QueueMetadata, error) {
	// The table name is a schema-qualified internal identifier, not user input,
	// so this interpolation is injection-safe.
	query := fmt.Sprintf(`
		SELECT id, queue_type, queue_name, table_name, config, paused, created_at, updated_at
		FROM %s
		WHERE queue_type = $1 AND queue_name = $2
		LIMIT 1
	`, pq.globalTable("pgqueue_metadata"))

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
	//nolint:gosec // G201: schema-qualified internal table name, not user input
	query := fmt.Sprintf(`
		INSERT INTO %s (queue_type, queue_name, table_name, config)
		VALUES ($1, $2, $3, $4)
		RETURNING id, queue_type, queue_name, table_name, config, paused, created_at, updated_at
	`, pq.globalTable("pgqueue_metadata"))

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
	countQuery := `SELECT COUNT(*) FROM ` + pq.globalTable("pgqueue_metadata")
	if err := tx.QueryRowContext(ctx, countQuery).Scan(&count); err != nil {
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
	// Error-text fallback for drivers without a SQLState() accessor (lib/pq).
	// Match the human-readable phrases PostgreSQL uses rather than the bare
	// "23505" SQLSTATE: a five-digit code can appear incidentally in unrelated
	// error text (a message id, a table name), which would misclassify it.
	errStr := err.Error()
	return strings.Contains(errStr, "duplicate key value") ||
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
	// Error-text fallback for drivers without a SQLState() accessor (lib/pq).
	// Match PostgreSQL's human-readable phrases rather than the bare "40001" /
	// "40P01" SQLSTATE codes: a short code can appear incidentally in unrelated
	// error text, which would misclassify a permanent failure as transient and
	// drive a pointless retry loop.
	es := err.Error()
	return strings.Contains(es, "could not serialize access") ||
		strings.Contains(es, "deadlock detected") ||
		strings.Contains(es, "connection refused") ||
		strings.Contains(es, "connection reset") ||
		strings.Contains(es, "bad connection") ||
		strings.Contains(es, "broken pipe")
}

func (pq *Queue) checkTableNameNotExists(
	ctx context.Context,
	tableName string,
) error {
	// The table name is a schema-qualified internal identifier, not user input,
	// so this interpolation is injection-safe.
	query := fmt.Sprintf(
		`SELECT queue_name FROM %s WHERE table_name = $1 LIMIT 1`,
		pq.globalTable("pgqueue_metadata"),
	)

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

// listQueuesPageSize bounds the metadata rows fetched per round-trip in
// listQueuesRaw. The query is keyset-paginated by id (UUIDv7, time-ordered),
// so per-statement memory stays bounded even when an operator skipped
// WithMaxQueues and accumulated very many queues (#66).
const listQueuesPageSize = 1000

func (pq *Queue) listQueuesRaw(
	ctx context.Context,
	queueType string,
) ([]QueueMetadata, error) {
	query := fmt.Sprintf(`
		SELECT id, queue_type, queue_name, table_name, config, paused, created_at, updated_at
		FROM %s
		WHERE queue_type = $1
		  AND ($2::uuid IS NULL OR id > $2)
		ORDER BY id
		LIMIT $3
	`, pq.globalTable("pgqueue_metadata"))

	items := []QueueMetadata{}
	var afterID any
	for {
		pageStart := len(items)
		if err := pq.fetchQueueMetadataPage(
			ctx, query, queueType, afterID, &items,
		); err != nil {
			return nil, err
		}
		fetched := len(items) - pageStart
		if fetched < listQueuesPageSize {
			return items, nil
		}
		afterID = items[len(items)-1].ID
	}
}

// fetchQueueMetadataPage reads one keyset page of queue metadata into items.
// Split out of listQueuesRaw so the per-page rows.Close cleanup is local and
// the page loop stays focused.
func (pq *Queue) fetchQueueMetadataPage(
	ctx context.Context,
	query, queueType string,
	afterID any,
	items *[]QueueMetadata,
) error {
	rows, err := pq.db.QueryContext(ctx, query, queueType, afterID, listQueuesPageSize)
	if err != nil {
		return fmt.Errorf("failed to query queues: %w", err)
	}
	defer func() { _ = rows.Close() }()
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
			return fmt.Errorf("failed to scan queue row: %w", err)
		}
		*items = append(*items, meta)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate queue rows: %w", err)
	}
	return nil
}

// Subscriber query methods

func (pq *Queue) registerSubscriber(
	ctx context.Context,
	topicName, subscriberID string,
) (*Subscriber, error) {
	// Schema-qualified internal table name, not user input; injection-safe.
	query := fmt.Sprintf(`
		INSERT INTO %s (topic_name, subscriber_id)
		VALUES ($1, $2)
		ON CONFLICT (topic_name, subscriber_id)
		DO UPDATE SET active = TRUE
		RETURNING id, topic_name, subscriber_id, created_at, active
	`, pq.globalTable("pgqueue_subscribers"))

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
	// Schema-qualified internal table name, not user input; injection-safe.
	query := fmt.Sprintf(`
		UPDATE %s
		SET active = FALSE
		WHERE topic_name = $1 AND subscriber_id = $2 AND active = TRUE
	`, pq.globalTable("pgqueue_subscribers"))
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
	//nolint:gosec // G201: schema-qualified internal table name, not user input
	query := fmt.Sprintf(`
		SELECT id, topic_name, subscriber_id, created_at, active
		FROM %s
		WHERE topic_name = $1 AND active = TRUE
		ORDER BY created_at
	`, pq.globalTable("pgqueue_subscribers"))

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
	//nolint:gosec // G201: schema-qualified internal table name, not user input
	replayQuery := fmt.Sprintf(
		`DELETE FROM %s WHERE queue_type = $1 AND queue_name = $2`,
		pq.globalTable("pgqueue_replay_log"),
	)
	if _, err := tx.ExecContext(ctx, replayQuery, queueType, queueName); err != nil {
		return fmt.Errorf("failed to delete replay log entries: %w", err)
	}

	// For pub/sub, delete subscriber registrations
	if queueType == string(QueueTypePubSub) {
		//nolint:gosec // G201: schema-qualified internal table name, not user input
		subQuery := fmt.Sprintf(
			`DELETE FROM %s WHERE topic_name = $1`,
			pq.globalTable("pgqueue_subscribers"),
		)
		if _, err := tx.ExecContext(ctx, subQuery, queueName); err != nil {
			return fmt.Errorf("failed to delete subscriber registrations: %w", err)
		}
	}

	// Delete metadata entry
	//nolint:gosec // G201: schema-qualified internal table name, not user input
	metaQuery := fmt.Sprintf(
		`DELETE FROM %s WHERE queue_type = $1 AND queue_name = $2`,
		pq.globalTable("pgqueue_metadata"),
	)
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
	//nolint:gosec // G201: schema-qualified internal table name, not user input
	query := fmt.Sprintf(`
		INSERT INTO %s (
			queue_type, queue_name, replay_type,
			replay_params, message_count, created_by
		)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, pq.globalTable("pgqueue_replay_log"))

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
	// Schema-qualified internal table name, not user input; injection-safe.
	query := fmt.Sprintf(`
		SELECT id, queue_type, queue_name, replay_type,
		       replay_params, message_count, created_at, created_by
		FROM %s
		WHERE queue_type = $1 AND queue_name = $2
		ORDER BY created_at DESC
		LIMIT $3
	`, pq.globalTable("pgqueue_replay_log"))

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
