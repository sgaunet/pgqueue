package pgqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
)

// baseSchemaSQL contains the DDL for creating the base schema tables required by pgqueue.
// This includes: pgqueue_metadata, pgqueue_subscribers, and pgqueue_replay_log.
const baseSchemaSQL = `
-- Metadata table to track all queues (topics and channels)
CREATE TABLE IF NOT EXISTS pgqueue_metadata (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    queue_type TEXT NOT NULL CHECK (queue_type IN ('pubsub', 'channel')),
    queue_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    paused BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(queue_type, queue_name),
    -- UNIQUE(table_name) closes a collision where queue names differing only by
    -- dash vs. underscore (e.g. "a-b" and "a_b") sanitize to the same physical
    -- table name. Its index also serves table_name lookups.
    UNIQUE(table_name)
);

CREATE INDEX IF NOT EXISTS idx_pgqueue_metadata_type_name ON pgqueue_metadata(queue_type, queue_name);

-- Subscribers table for pub/sub topics
CREATE TABLE IF NOT EXISTS pgqueue_subscribers (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    topic_name TEXT NOT NULL,
    subscriber_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE(topic_name, subscriber_id)
);

CREATE INDEX IF NOT EXISTS idx_pgqueue_subscribers_topic ON pgqueue_subscribers(topic_name) WHERE active = TRUE;

-- Replay audit log
CREATE TABLE IF NOT EXISTS pgqueue_replay_log (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    queue_type TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    replay_type TEXT NOT NULL CHECK (replay_type IN ('timestamp', 'message_id', 'dlq')),
    replay_params JSONB NOT NULL,
    message_count INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_pgqueue_replay_log_queue ON pgqueue_replay_log(queue_type, queue_name);
CREATE INDEX IF NOT EXISTS idx_pgqueue_replay_log_created_at ON pgqueue_replay_log(created_at);
`

// Queue is the main struct for the message queue system.
type Queue struct {
	db       *sql.DB
	config   Config         // legacy config field; kept for backward-compat accessors
	cfg      queueConfig    // canonical config built from functional options
	logger   *slog.Logger
	closed   atomic.Bool    // set to true after Close() is called
	mdcache  *metadataCache // per-queue table-name cache (immutable fields only)
	notifier *notifier      // LISTEN/NOTIFY push delivery; nil when no Listener is set
}

// PGQueue is a backward-compatible alias for Queue.
//
// Deprecated: Use Queue instead.
type PGQueue = Queue

// queueNameRegex validates queue names (alphanumeric, underscore, dash).
var queueNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// InitSchema initializes and migrates the base schema required by pgqueue.
// This function must be called once at startup before creating or using any
// queues or topics.
//
// It creates four tables:
//   - pgqueue_metadata: Tracks all queues and topics with their configurations
//   - pgqueue_subscribers: Tracks pub/sub subscriptions for topics
//   - pgqueue_replay_log: Audit log for message replay operations
//   - pgqueue_schema_version: Tracks which schema migrations have been applied
//
// InitSchema is also the upgrade path: when a newer build of pgqueue introduces
// schema changes, InitSchema transparently applies the pending migrations to
// bring the database up to SchemaVersion. Callers do not need to do anything
// beyond continuing to call InitSchema at startup. See GetSchemaVersion to
// inspect the applied version.
//
// The function is idempotent and safe to run concurrently from multiple
// processes: migrations are serialized with a PostgreSQL advisory lock, so it
// can be called on every application instance at startup.
//
// Example usage:
//
//	db, err := sql.Open("pgx", "postgres://user:pass@localhost/dbname")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
//	// Initialize base schema (call once per database)
//	if err := pgqueue.InitSchema(ctx, db); err != nil {
//	    log.Fatal(err)
//	}
//
//	// Initialize pgqueue library
//	pq, err := pgqueue.Init(ctx, pgqueue.Config{
//	    DB:                db,
//	    MaxMessageSize:    1024 * 1024,
//	    DefaultMaxRetries: 3,
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
func InitSchema(ctx context.Context, db *sql.DB, opts ...Option) error {
	if db == nil {
		return ErrDBRequired
	}

	cfg := applyConfigOptions(opts)
	if err := validateSchemaName(cfg.schemaName); err != nil {
		return err
	}

	if err := runMigrations(ctx, db, cfg.schemaName); err != nil {
		return fmt.Errorf("failed to initialize base schema: %w", err)
	}

	return nil
}

// minPGVersionNum is PostgreSQL 18 in server_version_num form, the lowest
// version pgqueue supports (uuidv7() is native from PostgreSQL 18 onward).
const minPGVersionNum = 180000

// validateConfig rejects a Config with a negative numeric field: a negative
// MaxMessageSize would make every publish fail, and the others have no
// meaningful negative interpretation.
func validateConfig(cfg Config) error {
	if cfg.MaxMessageSize < 0 || cfg.DefaultMaxRetries < 0 ||
		cfg.DefaultTTL < 0 || cfg.MaxQueues < 0 {
		return ErrInvalidConfig
	}

	return nil
}

// checkDBReady verifies the database is reachable and runs a supported
// PostgreSQL version.
func checkDBReady(ctx context.Context, db *sql.DB) error {
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	var versionStr string
	if err := db.QueryRowContext(ctx, "SHOW server_version_num").Scan(&versionStr); err != nil {
		return fmt.Errorf("failed to check PostgreSQL version: %w", err)
	}
	versionNum, err := strconv.Atoi(versionStr)
	if err != nil {
		return fmt.Errorf("failed to parse PostgreSQL version number %q: %w", versionStr, err)
	}
	if versionNum < minPGVersionNum {
		return fmt.Errorf("%w: got %d", ErrUnsupportedPGVersion, versionNum)
	}

	return nil
}

// New creates a Queue using functional options. It is the preferred constructor.
//
// New returns ErrSchemaNotInitialized if InitSchema has not been run, and
// ErrSchemaOutdated if the database schema is behind the current SchemaVersion.
// Call InitSchema first on every application start to ensure the schema is
// up to date.
//
// Example:
//
//	pq, err := pgqueue.New(ctx, db,
//	    pgqueue.WithMaxMessageSize(1024*1024),
//	    pgqueue.WithDefaultMaxRetries(5),
//	)
func New(ctx context.Context, db *sql.DB, opts ...Option) (*Queue, error) {
	if db == nil {
		return nil, ErrDBRequired
	}

	cfg := applyConfigOptions(opts)
	if cfg.maxMessageSize < 0 || cfg.defaultMaxRetries < 0 ||
		int64(cfg.defaultTTL) < 0 || cfg.maxQueues < 0 {
		return nil, ErrInvalidConfig
	}
	if err := validateSchemaName(cfg.schemaName); err != nil {
		return nil, err
	}

	if err := checkDBReady(ctx, db); err != nil {
		return nil, err
	}

	// Build the legacy Config for backward-compat accessor methods.
	legacyCfg := Config{
		DB:                db,
		MaxMessageSize:    cfg.maxMessageSize,
		DefaultMaxRetries: cfg.defaultMaxRetries,
		DefaultTTL:        cfg.defaultTTL,
		MaxQueues:         cfg.maxQueues,
		Logger:            cfg.logger,
	}

	pq := &Queue{
		db:       db,
		config:   legacyCfg,
		cfg:      cfg,
		logger:   cfg.logger,
		mdcache:  newMetadataCache(),
		notifier: newNotifier(cfg.listener),
	}

	if err := pq.checkSchemaReady(ctx); err != nil {
		return nil, err
	}

	return pq, nil
}

// Init initializes the Queue system with the provided configuration.
//
// Deprecated: Use New(ctx, db, ...Option) with functional options instead.
// Init is retained for backward compatibility.
func Init(ctx context.Context, cfg Config) (*Queue, error) {
	if cfg.DB == nil {
		return nil, ErrDBRequired
	}
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	opts := configFromLegacy(cfg)
	return New(ctx, cfg.DB, opts...)
}

// CreateChannel creates a new point-to-point channel.
// Per-queue settings (TTL, max retries, message size) are controlled via
// WithQueueMaxRetries, WithQueueTTL, and WithQueueMaxMessageSize options.
func (pq *Queue) CreateChannel(
	ctx context.Context,
	name string,
	opts ...QueueOption,
) error {
	if err := pq.checkClosed(); err != nil {
		return err
	}
	o := applyQueueOptions(opts)
	co := ChannelOptions{
		MaxMessageSize: o.maxMessageSize,
		TTL:            o.ttl,
		MaxRetries:     o.maxRetries,
	}
	return pq.createQueue(ctx, QueueTypeChannel, name, co)
}

// CreateTopic creates a new pub/sub topic.
// Per-queue settings are controlled via WithQueueMaxRetries, WithQueueTTL, and
// WithQueueMaxMessageSize options.
func (pq *Queue) CreateTopic(
	ctx context.Context,
	name string,
	opts ...QueueOption,
) error {
	if err := pq.checkClosed(); err != nil {
		return err
	}
	o := applyQueueOptions(opts)
	to := TopicOptions{
		MaxMessageSize: o.maxMessageSize,
		TTL:            o.ttl,
		MaxRetries:     o.maxRetries,
	}
	return pq.createQueue(ctx, QueueTypePubSub, name, to)
}

// DeleteTopic deletes a pub/sub topic and all associated resources.
// Requires confirm=true as a safety measure to prevent accidental deletion.
func (pq *Queue) DeleteTopic(
	ctx context.Context,
	name string,
	confirm bool,
) error {
	return pq.deleteQueue(ctx, QueueTypePubSub, name, confirm)
}

// DeleteChannel deletes a point-to-point channel and all associated resources.
// Requires confirm=true as a safety measure to prevent accidental deletion.
func (pq *Queue) DeleteChannel(
	ctx context.Context,
	name string,
	confirm bool,
) error {
	return pq.deleteQueue(ctx, QueueTypeChannel, name, confirm)
}

// ListTopics returns the names of all pub/sub topics.
func (pq *Queue) ListTopics(ctx context.Context) ([]string, error) {
	if err := pq.checkClosed(); err != nil {
		return nil, err
	}
	rows, err := pq.listQueues(ctx, QueueTypePubSub)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(rows))
	for i, r := range rows {
		names[i] = r.QueueName
	}
	return names, nil
}

// ListChannels returns the names of all point-to-point channels.
func (pq *Queue) ListChannels(ctx context.Context) ([]string, error) {
	if err := pq.checkClosed(); err != nil {
		return nil, err
	}
	rows, err := pq.listQueues(ctx, QueueTypeChannel)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(rows))
	for i, r := range rows {
		names[i] = r.QueueName
	}
	return names, nil
}

// Close marks the Queue as closed and stops the LISTEN/NOTIFY listener (if one
// was registered with WithListener). After Close returns, all public operations
// will return ErrQueueClosed. It does NOT close the underlying *sql.DB, which
// is owned and managed by the caller. Stop any GarbageCollector before calling
// Close; example:
//
//	gc.Stop()       // stop GC first
//	pq.Close()      // then mark queue closed
//	db.Close()      // then close the database connection
//
// Close is idempotent: calling it multiple times is safe and returns nil.
func (pq *Queue) Close() error {
	pq.closed.Store(true)
	if pq.notifier != nil {
		if err := pq.notifier.close(); err != nil {
			return fmt.Errorf("failed to close notification listener: %w", err)
		}
	}
	return nil
}

// PauseQueue prevents new messages from being consumed from the specified queue.
// Publishing is still allowed while paused.
//
// Deprecated: Use PauseChannel or PauseTopic instead.
func (pq *Queue) PauseQueue(ctx context.Context, queueName string, queueType QueueType) error {
	return pq.setQueuePaused(ctx, queueName, queueType, true)
}

// ResumeQueue allows message consumption again for the specified queue.
//
// Deprecated: Use ResumeChannel or ResumeTopic instead.
func (pq *Queue) ResumeQueue(ctx context.Context, queueName string, queueType QueueType) error {
	return pq.setQueuePaused(ctx, queueName, queueType, false)
}

// PauseChannel pauses a point-to-point channel, preventing new messages from
// being consumed. Publishing is still allowed while paused.
func (pq *Queue) PauseChannel(ctx context.Context, name string) error {
	return pq.setQueuePaused(ctx, name, QueueTypeChannel, true)
}

// ResumeChannel resumes a paused channel, allowing message consumption again.
func (pq *Queue) ResumeChannel(ctx context.Context, name string) error {
	return pq.setQueuePaused(ctx, name, QueueTypeChannel, false)
}

// PauseTopic pauses a pub/sub topic, preventing new messages from being consumed
// by any subscriber. Publishing is still allowed while paused.
func (pq *Queue) PauseTopic(ctx context.Context, name string) error {
	return pq.setQueuePaused(ctx, name, QueueTypePubSub, true)
}

// ResumeTopic resumes a paused pub/sub topic, allowing message consumption again.
func (pq *Queue) ResumeTopic(ctx context.Context, name string) error {
	return pq.setQueuePaused(ctx, name, QueueTypePubSub, false)
}

// IsQueuePaused returns whether the specified queue is currently paused.
func (pq *Queue) IsQueuePaused(ctx context.Context, queueName string, queueType QueueType) (bool, error) {
	meta, err := pq.getQueueMetadata(ctx, string(queueType), queueName)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return false, fmt.Errorf("%s/%s: %w", queueType, queueName, ErrQueueNotFound)
		}
		return false, fmt.Errorf("failed to get queue metadata: %w", err)
	}

	return meta.Paused, nil
}

// checkClosed returns ErrQueueClosed if Close has been called.
func (pq *Queue) checkClosed() error {
	if pq.closed.Load() {
		return ErrQueueClosed
	}
	return nil
}

func (pq *Queue) setQueuePaused(ctx context.Context, queueName string, queueType QueueType, paused bool) error {
	result, err := pq.db.ExecContext(ctx,
		fmt.Sprintf(
			`UPDATE %s SET paused = $1, updated_at = NOW()
			 WHERE queue_type = $2 AND queue_name = $3`,
			pq.globalTable("pgqueue_metadata"),
		),
		paused, string(queueType), queueName,
	)
	if err != nil {
		return fmt.Errorf("failed to update queue paused state: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("%s/%s: %w", queueType, queueName, ErrQueueNotFound)
	}

	return nil
}

// checkSchemaReady verifies the database schema has been created by InitSchema
// and is at least at the version this build of pgqueue requires.
func (pq *Queue) checkSchemaReady(ctx context.Context) error {
	schemaVer, err := pq.GetSchemaVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to check schema version: %w", err)
	}
	if schemaVer == 0 {
		return ErrSchemaNotInitialized
	}
	if schemaVer < SchemaVersion {
		return fmt.Errorf(
			"%w: database at v%d, this build expects v%d",
			ErrSchemaOutdated, schemaVer, SchemaVersion)
	}

	return nil
}

func (pq *Queue) logInfo(msg string, args ...any) {
	if pq.logger != nil {
		pq.logger.Info(msg, args...)
	}
}

func (pq *Queue) logError(msg string, args ...any) {
	if pq.logger != nil {
		pq.logger.Error(msg, args...)
	}
}

// createQueue is the internal implementation for creating queues.
func (pq *Queue) createQueue(
	ctx context.Context,
	queueType QueueType,
	name string,
	opts any,
) error {
	if err := pq.validateQueueName(name); err != nil {
		return fmt.Errorf("failed to validate queue name: %w", err)
	}

	if err := pq.checkQueueNotExists(ctx, queueType, name); err != nil {
		return fmt.Errorf("failed to check queue existence: %w", err)
	}

	// Sanitize table name and check for collisions
	tableName := sanitizeTableName(name)

	if err := pq.checkTableNameNotExists(ctx, tableName); err != nil {
		return fmt.Errorf("failed to check table name collision: %w", err)
	}

	// Marshal options to JSON
	configJSON, err := json.Marshal(opts)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// Begin transaction
	tx, err := pq.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := pq.createQueueInTx(
		ctx, tx, queueType, name, tableName, configJSON,
	); err != nil {
		return err
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// createQueueInTx performs the transactional part of queue creation: it takes
// the migration lock, enforces the queue cap, and writes the metadata row and
// per-queue tables.
func (pq *Queue) createQueueInTx(
	ctx context.Context,
	tx *sql.Tx,
	queueType QueueType,
	name, tableName string,
	configJSON []byte,
) error {
	// Take a shared lock on the migration advisory key so queue creation cannot
	// run its DDL concurrently with a schema migration, which holds the same key
	// exclusively. A migration's dynamic fan-out across per-queue tables snapshots
	// pgqueue_metadata once; without this lock a queue created mid-migration could
	// be missed. Concurrent createQueue calls share the lock freely. The lock is
	// released when this transaction ends.
	if _, err := tx.ExecContext(ctx,
		"SELECT pg_advisory_xact_lock_shared($1)", migrationAdvisoryLockKey,
	); err != nil {
		return fmt.Errorf("failed to acquire migration lock: %w", err)
	}

	// Enforce the optional cap on the total number of queues to guard against
	// table-space exhaustion when queue creation is exposed to untrusted input.
	if err := pq.enforceMaxQueues(ctx, tx); err != nil {
		return err
	}

	// Create metadata entry
	if _, err := pq.createQueueMetadata(
		ctx, tx, string(queueType), name, tableName, configJSON,
	); err != nil {
		return fmt.Errorf("failed to create queue metadata: %w", err)
	}

	// Create queue tables based on type
	if err := pq.createQueueTables(ctx, tx, queueType, tableName); err != nil {
		return err
	}

	return nil
}

// createQueueAdvisoryLockKey serializes concurrent createQueue calls when a
// MaxQueues cap is configured, so the SELECT COUNT(*) check cannot race past
// the limit. The ASCII bytes spell "pgquecq" (pgqueue create-queue).
const createQueueAdvisoryLockKey int64 = 0x70677175_65_6371

// enforceMaxQueues serializes queue creation and checks the MaxQueues cap
// within the given transaction. It is a no-op when no cap is configured.
func (pq *Queue) enforceMaxQueues(ctx context.Context, tx *sql.Tx) error {
	// Use the functional-options config if available, else fall back to legacy.
	maxQueues := pq.cfg.maxQueues
	if maxQueues == 0 {
		maxQueues = pq.config.MaxQueues
	}
	if maxQueues <= 0 {
		return nil
	}
	// Serialize concurrent createQueue calls so the count check cannot race
	// past the cap. The lock is released automatically when this tx ends.
	if _, err := tx.ExecContext(ctx,
		"SELECT pg_advisory_xact_lock($1)", createQueueAdvisoryLockKey,
	); err != nil {
		return fmt.Errorf("failed to acquire queue-creation lock: %w", err)
	}
	count, err := pq.countQueues(ctx, tx)
	if err != nil {
		return err
	}
	if count >= maxQueues {
		return fmt.Errorf("limit is %d: %w", maxQueues, ErrMaxQueuesReached)
	}

	return nil
}

func (pq *Queue) createQueueTables(
	ctx context.Context,
	tx *sql.Tx,
	queueType QueueType,
	tableName string,
) error {
	if queueType == QueueTypePubSub {
		if err := pq.createPubSubTables(ctx, tx, tableName); err != nil {
			return fmt.Errorf("failed to create pub/sub tables: %w", err)
		}
	} else {
		if err := pq.createChannelTables(ctx, tx, tableName); err != nil {
			return fmt.Errorf("failed to create channel tables: %w", err)
		}
	}

	return nil
}

// deleteQueue is the internal implementation for deleting queues.
func (pq *Queue) deleteQueue(
	ctx context.Context,
	queueType QueueType,
	name string,
	confirm bool,
) error {
	if !confirm {
		return ErrDeleteNotConfirmed
	}

	if err := pq.validateQueueName(name); err != nil {
		return fmt.Errorf("failed to validate queue name: %w", err)
	}

	// Verify queue exists
	metadata, err := pq.getQueueMetadata(ctx, string(queueType), name)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return fmt.Errorf("%s/%s: %w", queueType, name, ErrQueueNotFound)
		}

		return fmt.Errorf("failed to get queue metadata: %w", err)
	}

	// Begin transaction
	tx, err := pq.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Drop queue-specific tables and clean up global tables
	if err := pq.executeDelete(ctx, tx, queueType, name, metadata.TableName); err != nil {
		return fmt.Errorf("failed to execute queue deletion: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Invalidate the metadata cache entry for the deleted queue so subsequent
	// operations do not serve stale table-name mappings.
	if pq.mdcache != nil {
		pq.mdcache.invalidate(string(queueType), name)
	}

	return nil
}

func (pq *Queue) executeDelete(
	ctx context.Context,
	tx *sql.Tx,
	queueType QueueType,
	name, tableName string,
) error {
	// For pub/sub, drop subscription table first (has FK to msg table)
	if queueType == QueueTypePubSub {
		dropSub := "DROP TABLE IF EXISTS " + pq.subTable(tableName)
		if _, err := tx.ExecContext(ctx, dropSub); err != nil {
			return fmt.Errorf("failed to drop subscription table: %w", err)
		}
	}

	// Drop DLQ table
	dropDLQ := "DROP TABLE IF EXISTS " + pq.dlqTable(tableName)
	if _, err := tx.ExecContext(ctx, dropDLQ); err != nil {
		return fmt.Errorf("failed to drop DLQ table: %w", err)
	}

	// Drop message table
	dropMsg := "DROP TABLE IF EXISTS " + pq.msgTable(tableName)
	if _, err := tx.ExecContext(ctx, dropMsg); err != nil {
		return fmt.Errorf("failed to drop message table: %w", err)
	}

	// Clean up global tables (metadata, subscribers, replay log)
	return pq.deleteQueueMetadata(ctx, tx, string(queueType), name)
}

// maxQueueNameLength limits queue names to avoid PostgreSQL's 63-byte identifier
// truncation. The binding constraint is not the table name ("pgqueue_msg_" + name)
// but the per-queue index names: the longest pattern is
// "idx_pgqueue_sub_" + name + "_consumable_timeout" (35 fixed characters), so a
// name must be at most 63 - 35 = 28 characters. A larger limit would let two
// distinct queues produce index names that truncate to the same string, causing
// CREATE INDEX IF NOT EXISTS to silently skip the second queue's index.
const maxQueueNameLength = 28

func (pq *Queue) validateQueueName(name string) error {
	if len(name) == 0 || len(name) > maxQueueNameLength {
		return fmt.Errorf("queue name must be 1-%d characters: %w", maxQueueNameLength, ErrInvalidQueueName)
	}
	if !queueNameRegex.MatchString(name) {
		return ErrInvalidQueueName
	}

	return nil
}

// maxSubscriberIDLength is the maximum allowed length for a subscriber ID.
const maxSubscriberIDLength = 128

func validateSubscriberID(id string) error {
	if len(id) == 0 || len(id) > maxSubscriberIDLength || !queueNameRegex.MatchString(id) {
		return ErrInvalidSubscriberID
	}

	return nil
}

func (pq *Queue) checkQueueNotExists(
	ctx context.Context,
	queueType QueueType,
	name string,
) error {
	existing, err := pq.getQueueMetadata(ctx, string(queueType), name)
	if err == nil && existing != nil {
		return fmt.Errorf("%s/%s: %w", queueType, name, ErrQueueAlreadyExists)
	}
	if err != nil && !errors.Is(err, ErrQueueNotFound) {
		return fmt.Errorf("failed to check existing queue: %w", err)
	}

	return nil
}

// createPubSubTables creates message and subscription tables for a pub/sub topic.
func (pq *Queue) createPubSubTables(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
) error {
	// Create message table
	messageTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id UUID PRIMARY KEY,
			payload BYTEA NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			metadata JSONB
		)`, pq.msgTable(tableName))

	if _, err := tx.ExecContext(ctx, messageTable); err != nil {
		return fmt.Errorf("failed to create message table: %w", err)
	}

	// Create indexes
	createIndex := fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS idx_pgqueue_msg_%s_created_at
		ON %s(created_at)`, tableName, pq.msgTable(tableName))

	if _, err := tx.ExecContext(ctx, createIndex); err != nil {
		return fmt.Errorf("failed to create message index: %w", err)
	}

	// Create subscription table
	//nolint:gosec // G201: table name validated by queueNameRegex
	subscriptionTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id UUID PRIMARY KEY DEFAULT uuidv7(),
			message_id UUID NOT NULL,
			subscriber_id TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT '%s',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			acked_at TIMESTAMPTZ,
			visibility_timeout TIMESTAMPTZ,
			available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			claim_id UUID,
			retry_count INT NOT NULL DEFAULT 0,
			error_message TEXT,
			UNIQUE(message_id, subscriber_id),
			FOREIGN KEY (message_id)
				REFERENCES %s(id) ON DELETE CASCADE
		)`, pq.subTable(tableName), MessageStatusPending, pq.msgTable(tableName))

	if _, err := tx.ExecContext(ctx, subscriptionTable); err != nil {
		return fmt.Errorf("failed to create subscription table: %w", err)
	}

	if err := pq.createPubSubIndexes(ctx, tx, tableName); err != nil {
		return fmt.Errorf("failed to create pub/sub indexes: %w", err)
	}

	// Create DLQ table for pub/sub
	return pq.createDLQTable(ctx, tx, tableName)
}

func (pq *Queue) createPubSubIndexes(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
) error {
	subTbl := pq.subTable(tableName)
	subIndexes := []string{
		fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS idx_pgqueue_sub_%s_msg_id
			 ON %s(message_id)`,
			tableName, subTbl,
		),
		fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS idx_pgqueue_sub_%s_subscriber
			 ON %s(subscriber_id, status)`,
			tableName, subTbl,
		),
		fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS idx_pgqueue_sub_%s_status
			 ON %s(status) WHERE status = '%s'`,
			tableName, subTbl, MessageStatusPending,
		),
		// Consumption-optimized indexes: split the OR condition on
		// visibility_timeout into two partial indexes for efficient
		// subscriber message fetching.
		fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS idx_pgqueue_sub_%s_consumable_null
			 ON %s(subscriber_id, message_id)
			 WHERE status = '%s' AND visibility_timeout IS NULL`,
			tableName, subTbl, MessageStatusPending,
		),
		// Reclaim-optimized index: covers timed-out 'processing' subscriptions
		// that ConsumeFromTopic redelivers once their visibility timeout expires.
		fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS idx_pgqueue_sub_%s_consumable_timeout
			 ON %s(subscriber_id, visibility_timeout, message_id)
			 WHERE status = '%s' AND visibility_timeout IS NOT NULL`,
			tableName, subTbl, MessageStatusProcessing,
		),
		// Backoff-optimized index: covers pending subscriptions awaiting
		// their scheduled redelivery time (available_at).
		fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS idx_pgqueue_sub_%s_available
			 ON %s(available_at)
			 WHERE status = '%s'`,
			tableName, subTbl, MessageStatusPending,
		),
	}

	for _, idx := range subIndexes {
		if _, err := tx.ExecContext(ctx, idx); err != nil {
			return fmt.Errorf("failed to create subscription index: %w", err)
		}
	}

	return nil
}

// createChannelTables creates message table for a point-to-point channel.
func (pq *Queue) createChannelTables(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
) error {
	// Create message table
	messageTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id UUID PRIMARY KEY,
			payload BYTEA NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			status TEXT NOT NULL DEFAULT '%s',
			retry_count INT NOT NULL DEFAULT 0,
			max_retries INT,
			visibility_timeout TIMESTAMPTZ,
			available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			claim_id UUID,
			processed_at TIMESTAMPTZ,
			error_message TEXT,
			metadata JSONB
		)`, pq.msgTable(tableName), MessageStatusPending)

	if _, err := tx.ExecContext(ctx, messageTable); err != nil {
		return fmt.Errorf("failed to create message table: %w", err)
	}

	if err := pq.createChannelIndexes(ctx, tx, tableName); err != nil {
		return fmt.Errorf("failed to create channel indexes: %w", err)
	}

	// Create DLQ table
	return pq.createDLQTable(ctx, tx, tableName)
}

func (pq *Queue) createChannelIndexes(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
) error {
	msgTbl := pq.msgTable(tableName)
	indexes := []string{
		fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS idx_pgqueue_msg_%s_status_created
			 ON %s(status, created_at)
			 WHERE status = '%s'`,
			tableName, msgTbl, MessageStatusPending,
		),
		fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS idx_pgqueue_msg_%s_visibility
			 ON %s(visibility_timeout)
			 WHERE visibility_timeout IS NOT NULL`,
			tableName, msgTbl,
		),
		// Consumption-optimized indexes: split the OR condition on
		// visibility_timeout into two partial indexes so PostgreSQL
		// can use an efficient index scan for each branch.
		fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS idx_pgqueue_msg_%s_consumable_null
			 ON %s(id)
			 WHERE status = '%s' AND visibility_timeout IS NULL`,
			tableName, msgTbl, MessageStatusPending,
		),
		// Reclaim-optimized index: covers timed-out 'processing' messages that
		// ConsumeFromChannel redelivers once their visibility timeout expires.
		fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS idx_pgqueue_msg_%s_consumable_timeout
			 ON %s(visibility_timeout, id)
			 WHERE status = '%s' AND visibility_timeout IS NOT NULL`,
			tableName, msgTbl, MessageStatusProcessing,
		),
		// Backoff-optimized index: covers pending messages awaiting their
		// scheduled redelivery time (available_at).
		fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS idx_pgqueue_msg_%s_available
			 ON %s(available_at)
			 WHERE status = '%s'`,
			tableName, msgTbl, MessageStatusPending,
		),
	}

	for _, idx := range indexes {
		if _, err := tx.ExecContext(ctx, idx); err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
	}

	return nil
}

// createDLQTable creates a dead letter queue table.
func (pq *Queue) createDLQTable(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
) error {
	// subscriber_id records which subscriber failed for pub/sub DLQ entries;
	// it is left NULL for channel DLQ entries, which have no subscriber.
	dlqTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id UUID PRIMARY KEY DEFAULT uuidv7(),
			original_message_id UUID NOT NULL,
			subscriber_id TEXT,
			payload BYTEA NOT NULL,
			failure_reason TEXT NOT NULL,
			retry_count INT NOT NULL,
			moved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			metadata JSONB
		)`, pq.dlqTable(tableName))

	if _, err := tx.ExecContext(ctx, dlqTable); err != nil {
		return fmt.Errorf("failed to create DLQ table: %w", err)
	}

	// Create DLQ index
	dlqIndex := fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS idx_pgqueue_dlq_%s_moved_at
		ON %s(moved_at)`, tableName, pq.dlqTable(tableName))

	if _, err := tx.ExecContext(ctx, dlqIndex); err != nil {
		return fmt.Errorf("failed to create DLQ index: %w", err)
	}

	return nil
}

// listQueues is the internal implementation for listing queues.
func (pq *Queue) listQueues(
	ctx context.Context,
	queueType QueueType,
) ([]QueueMetadata, error) {
	rows, err := pq.listQueuesRaw(ctx, string(queueType))
	if err != nil {
		return nil, fmt.Errorf("failed to list queues: %w", err)
	}

	result := make([]QueueMetadata, 0, len(rows))
	for _, row := range rows {
		result = append(result, QueueMetadata{
			ID:        row.ID,
			QueueType: row.QueueType,
			QueueName: row.QueueName,
			TableName: row.TableName,
			Config:    row.Config,
			Paused:    row.Paused,
			CreatedAt: row.CreatedAt,
			UpdatedAt: row.UpdatedAt,
		})
	}

	return result, nil
}

// sanitizeTableName converts a queue name to a safe table name.
func sanitizeTableName(name string) string {
	// Replace dashes with underscores and convert to lowercase
	return strings.ToLower(strings.ReplaceAll(name, "-", "_"))
}

// schemaNameRegex validates a PostgreSQL schema identifier. The name must be a
// plain unquoted identifier so it can be safely interpolated into DDL/DML
// (schema names cannot be passed as bind parameters).
var schemaNameRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// maxSchemaNameLength is PostgreSQL's identifier length limit.
const maxSchemaNameLength = 63

// validateSchemaName rejects a configured schema name that is not a plain
// unquoted PostgreSQL identifier. Because the schema name is interpolated
// directly into SQL, this validation is what keeps that interpolation safe.
func validateSchemaName(name string) error {
	if name == "" || len(name) > maxSchemaNameLength || !schemaNameRegex.MatchString(name) {
		return fmt.Errorf("invalid schema name %q: %w", name, ErrInvalidConfig)
	}

	return nil
}

// schemaTablePrefix returns the schema-qualification prefix for SQL identifiers.
// For the default "public" schema it returns the empty string, leaving SQL
// unqualified so existing databases and queries are unaffected; for any other
// schema it returns "<schema>." (FR-024).
func schemaTablePrefix(schema string) string {
	if schema == "" || schema == "public" {
		return ""
	}

	return schema + "."
}

// tablePrefix returns the schema-qualification prefix for this Queue.
func (pq *Queue) tablePrefix() string {
	return schemaTablePrefix(pq.cfg.schemaName)
}

// msgTable, dlqTable, and subTable return the schema-qualified physical table
// name for a queue's message, dead-letter, and subscription tables. tableName
// is the sanitized per-queue table name stored in pgqueue_metadata.table_name.
func (pq *Queue) msgTable(tableName string) string { return pq.tablePrefix() + "pgqueue_msg_" + tableName }
func (pq *Queue) dlqTable(tableName string) string { return pq.tablePrefix() + "pgqueue_dlq_" + tableName }
func (pq *Queue) subTable(tableName string) string { return pq.tablePrefix() + "pgqueue_sub_" + tableName }

// globalTable returns the schema-qualified name of a global pgqueue table
// (pgqueue_metadata, pgqueue_subscribers, pgqueue_replay_log,
// pgqueue_schema_version).
func (pq *Queue) globalTable(name string) string { return pq.tablePrefix() + name }
