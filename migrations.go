package pgqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
)

// errMalformedMigrations is the sentinel wrapped by every validateMigrations
// failure. It is never surfaced to callers — init panics on it — but having a
// static error keeps the wrapped-error linters satisfied and lets tests assert
// the failure with errors.Is.
var errMalformedMigrations = errors.New("malformed migrations slice")

// ErrSchemaTooNew is returned by InitSchema (and therefore by New via
// checkSchemaReady) when the database's recorded schema version is strictly
// greater than this binary's SchemaVersion constant. This indicates a
// rolling-deploy scenario where an older binary is starting against a database
// already migrated by a newer binary. Proceeding would risk data corruption or
// query errors against columns that the older code does not expect, so the
// binary aborts with this error instead of silently running against a schema it
// does not understand.
var ErrSchemaTooNew = errors.New(
	"pgqueue schema is newer than this binary: upgrade the binary or roll back the schema",
)

// SchemaVersion is the latest schema version this build of pgqueue knows how to
// produce. InitSchema migrates a database up to this version automatically.
//
// IMPORTANT: when adding a new entry to the migrations slice below, bump this
// constant to match that entry's version number. An init() check enforces that
// SchemaVersion equals the last migration's version.
const SchemaVersion = 7

// Advisory-lock key encoding scheme
//
// All pgqueue advisory-lock keys are int64 values encoded from the ASCII bytes
// of a short, human-readable tag so that pg_locks rows are identifiable from
// psql without a code lookup. The encoding concatenates the ASCII bytes of the
// tag in big-endian order; underscore separators are used in the literal for
// readability only and have no effect on the value.
//
// Current key assignments (tag → hexadecimal → decimal):
//
//	"pgqueue" → 0x70677175657565 → 31638934390142309  migrationAdvisoryLockKey
//	"pgquecq" → 0x70677175656371 → 31638934390137713  createQueueAdvisoryLockKey
//
// New keys must use a unique 7-byte ASCII tag so the values do not collide.

// migrationAdvisoryLockKey is a fixed PostgreSQL advisory-lock key (ASCII bytes
// of "pgqueue") used to serialize schema migrations across processes. Multiple
// application instances calling InitSchema concurrently will block on this lock
// so exactly one of them runs the DDL. createQueueInTx acquires the same key as
// a shared transaction-scoped lock so queue creation cannot run its DDL
// concurrently with a migration that iterates per-queue tables.
const migrationAdvisoryLockKey int64 = 0x7067717565_7565

// createQueueAdvisoryLockKey serializes concurrent createQueue calls when a
// MaxQueues cap is configured, so the SELECT COUNT(*) check cannot race past
// the cap. The ASCII bytes spell "pgquecq" (pgqueue create-queue).
const createQueueAdvisoryLockKey int64 = 0x70677175_65_6371

// schemaVersionTableSQL bootstraps the migration-tracking table. Unlike the
// queue tables, this table is not itself created by a migration: it must exist
// before any migration can be recorded, so it is created directly (idempotently)
// at the start of every migration run.
const schemaVersionTableSQL = `
CREATE TABLE IF NOT EXISTS pgqueue_schema_version (
    version     INTEGER PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    description TEXT NOT NULL
);`

// migration is a single, ordered, forward-only schema change.
//
// A migration has two optional phases; at least one must be set:
//
//   - applyNonTx runs first, on the dedicated migration *sql.Conn, OUTSIDE any
//     transaction. It exists for statements PostgreSQL forbids inside a
//     transaction block — chiefly CREATE INDEX CONCURRENTLY, which lets an
//     upgrade build an index without locking a large per-queue table against
//     writes. It is the right home for any DDL whose plain (transactional) form
//     would cause a publish outage on a populated database.
//   - apply runs second, inside the transaction that also records the version
//     row, so the apply work and the pgqueue_schema_version insert commit
//     atomically.
//
// CRASH SAFETY — applyNonTx runs before the version row is committed, so a
// crash between the two re-runs applyNonTx on the next InitSchema. applyNonTx
// therefore MUST be idempotent and cannot be rolled back; keep every statement
// individually safe to repeat. Use CREATE INDEX CONCURRENTLY IF NOT EXISTS.
// Footgun: an interrupted CREATE INDEX CONCURRENTLY leaves an *invalid* index
// that IF NOT EXISTS then silently skips forever — a robust concurrent
// migration drops an invalid leftover first (e.g. DROP INDEX CONCURRENTLY IF
// EXISTS, or detect invalidity via pg_index.indisvalid) before recreating it.
//
// IMPORTANT — per-queue tables are NOT covered by baseSchemaSQL. baseSchemaSQL
// (and therefore the v1 migration) creates only the four global tables. The
// per-queue tables (pgqueue_msg_*, pgqueue_dlq_*, pgqueue_sub_*) are created
// on demand by createChannelTables / createPubSubTables, which always emit the
// current column shape. Consequently, any migration that adds or alters a
// column on a per-queue table MUST patch the already-existing per-queue tables
// itself — newly created queues are fine, but pre-existing ones are not
// touched by anything else. Both phases can discover the live tables from
// pgqueue_metadata via listQueueTableNames, e.g.:
//
//	for _, t := range tableNames {
//		// apply:      ALTER TABLE pgqueue_msg_<t> ADD COLUMN ...
//		// applyNonTx: CREATE INDEX CONCURRENTLY IF NOT EXISTS ... ON pgqueue_msg_<t> ...
//	}
//
// Forgetting this step is the most likely way to ship a migration that works
// on fresh databases but breaks every upgraded one.
type migration struct {
	version int
	name    string
	// apply runs inside the version-recording transaction. Optional.
	apply func(ctx context.Context, tx *sql.Tx) error
	// applyNonTx runs on the migration connection outside any transaction,
	// before apply. Optional. See the type comment for its crash-safety
	// contract — it must be idempotent.
	applyNonTx func(ctx context.Context, conn *sql.Conn) error
}

// migrations is the ordered, append-only list of schema migrations. Each run of
// InitSchema applies every entry whose version is greater than the version
// currently recorded in the database.
//
// This is the v1 baseline: the project's pre-release migration history was
// collapsed into a single initial migration. From the first release onward this
// list is strictly append-only — never reorder, renumber, or edit the behaviour
// of an already-released entry.
var migrations = []migration{
	{
		version: 1,
		name:    "initial schema",
		apply: func(ctx context.Context, tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, baseSchemaSQL); err != nil {
				return fmt.Errorf("failed to create base schema: %w", err)
			}

			return nil
		},
	},
	{
		version: 2, //nolint:mnd // schema migration version number
		name:    "index dlq original_message_id",
		apply:   migrateIndexDLQOriginalMessageID,
	},
	{
		version: 3, //nolint:mnd // schema migration version number
		name:    "non-negative retry_count and max_retries",
		apply:   migrateNonNegativeRetryCounts,
	},
	{
		version: 4, //nolint:mnd // schema migration version number
		name:    "bigint retry counters",
		apply:   migrateBigintRetryCounts,
	},
	{
		version: 5, //nolint:mnd // schema migration version number
		name:    "max_retries NOT NULL with default 0",
		apply:   migrateMaxRetriesNotNull,
	},
	{
		version: 6, //nolint:mnd // schema migration version number
		name:    "metadata JSONB DEFAULT empty object",
		apply:   migrateMetadataDefault,
	},
	{
		version: 7, //nolint:mnd // schema migration version number
		name:    "pubsub DLQ subscriber_id partial index",
		apply:   migratePubSubDLQSubscriberIndex,
	},
}

// migrateIndexDLQOriginalMessageID is the v2 migration. It backfills the index
// on pgqueue_dlq_*.original_message_id for every queue that already exists:
// createDLQTable emits this index for new queues, but pre-existing per-queue
// tables are not touched by anything else (see the migration doc comment), so
// the migration discovers the live tables from pgqueue_metadata and patches
// each one. CREATE INDEX IF NOT EXISTS keeps it idempotent and harmless for
// tables created by a newer createDLQTable.
func migrateIndexDLQOriginalMessageID(ctx context.Context, tx *sql.Tx) error {
	// Drain the table-name list fully (and close its cursor) before issuing any
	// DDL: another statement cannot run on the same transaction while a result
	// set is still open.
	tableNames, err := listQueueTableNames(ctx, tx)
	if err != nil {
		return err
	}

	for _, tableName := range tableNames {
		// table_name is the sanitized per-queue identifier ([a-z0-9_]+, <= 28
		// chars) written by sanitizeTableName, so interpolating it is safe.
		stmt := fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS idx_pgqueue_dlq_%s_orig_msg `+
				`ON pgqueue_dlq_%s(original_message_id)`,
			tableName, tableName,
		)
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf(
				"failed to index DLQ for queue table %q: %w", tableName, err,
			)
		}
	}

	return nil
}

// migrateNonNegativeRetryCounts is the v3 migration. createChannelTables,
// createPubSubTables, and createDLQTable now emit CHECK constraints that pin
// retry_count (and the channel msg table's max_retries) to non-negative values
// so a stray direct-SQL UPDATE cannot wedge the retry/DLQ off-by-one guard in
// channel.go. Pre-existing per-queue tables predate those constraints, so
// discover them from pgqueue_metadata and ALTER each one. The constraints are
// added NOT VALID first (a brief catalog-only lock) and then VALIDATEd in a
// second statement (SHARE UPDATE EXCLUSIVE — concurrent reads and writes keep
// running) so the migration stays usable against populated, in-use tables.
// addAndValidateCheck swallows duplicate_object on ADD CONSTRAINT to stay
// idempotent for queues whose newer CREATE TABLE already emitted the check.
func migrateNonNegativeRetryCounts(ctx context.Context, tx *sql.Tx) error {
	tableNames, err := listQueueTableNames(ctx, tx)
	if err != nil {
		return err
	}

	for _, tableName := range tableNames {
		queueType, err := queueTypeForTable(ctx, tx, tableName)
		if err != nil {
			return err
		}
		if err := applyRetryCountChecks(ctx, tx, queueType, tableName); err != nil {
			return err
		}
	}

	return nil
}

// applyRetryCountChecks adds the v3 NOT VALID + VALIDATE pair for every
// retry_count / max_retries column the given queue actually has. tableName is
// the sanitized per-queue identifier ([a-z0-9_]+, <= 28 chars) written by
// sanitizeTableName, so direct interpolation is safe.
func applyRetryCountChecks(
	ctx context.Context, tx *sql.Tx, queueType QueueType, tableName string,
) error {
	checks := []struct{ table, column, expr string }{
		{"pgqueue_dlq_" + tableName, "retry_count", "retry_count >= 0"},
	}
	switch queueType {
	case QueueTypeChannel:
		checks = append(checks,
			struct{ table, column, expr string }{
				"pgqueue_msg_" + tableName, "retry_count", "retry_count >= 0",
			},
			struct{ table, column, expr string }{
				"pgqueue_msg_" + tableName, "max_retries",
				"max_retries IS NULL OR max_retries >= 0",
			},
		)
	case QueueTypePubSub:
		checks = append(checks, struct{ table, column, expr string }{
			"pgqueue_sub_" + tableName, "retry_count", "retry_count >= 0",
		})
	}

	for _, c := range checks {
		if err := addAndValidateCheck(ctx, tx, c.table, c.column, c.expr); err != nil {
			return fmt.Errorf(
				"failed to add %s check on %s: %w", c.column, c.table, err,
			)
		}
	}

	return nil
}

// addAndValidateCheck runs the standard NOT VALID + VALIDATE pair so the
// constraint goes in without blocking concurrent writes on a populated table.
// A duplicate_object error from ADD CONSTRAINT is treated as already-applied
// (a queue created after the new CREATE TABLE began emitting the constraint
// up front), and VALIDATE is still issued to cover an earlier interrupted run
// that left the constraint NOT VALID.
func addAndValidateCheck(
	ctx context.Context, tx *sql.Tx, table, column, expr string,
) error {
	constraint := table + "_" + column + "_nonneg"
	add := fmt.Sprintf(
		`ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s) NOT VALID`,
		table, constraint, expr,
	)
	if _, err := tx.ExecContext(ctx, add); err != nil && !isDuplicateObjectError(err) {
		return fmt.Errorf("add constraint %s: %w", constraint, err)
	}
	validate := fmt.Sprintf(
		`ALTER TABLE %s VALIDATE CONSTRAINT %s`, table, constraint,
	)
	if _, err := tx.ExecContext(ctx, validate); err != nil {
		return fmt.Errorf("validate constraint %s: %w", constraint, err)
	}

	return nil
}

// migrateBigintRetryCounts is the v4 migration. createChannelTables,
// createPubSubTables, and createDLQTable now declare retry_count (and the
// channel msg table's max_retries) as BIGINT instead of 32-bit INT. A
// pathological crash-loop could otherwise overflow the counter; once retry_count
// wraps negative the DLQ exhaustion test retryCount > channelMaxRetries(...) in
// channel.go becomes unreliable and a message could dodge the DLQ (#135).
// Pre-existing per-queue tables predate the wider type, so discover them from
// pgqueue_metadata and ALTER each one to match what a fresh CREATE TABLE emits.
//
// LOCK NOTE: integer -> bigint changes the on-disk column width, so PostgreSQL
// rewrites the table under an ACCESS EXCLUSIVE lock for the duration of the
// ALTER. There is no online path for a width change in vanilla PostgreSQL; this
// is accepted because the change is cheap on the small/empty tables of a
// not-yet-released schema ("trivial now, annoying later"). It runs in the
// transactional apply phase — it is plain DDL, not CREATE INDEX CONCURRENTLY.
// The ALTER preserves the column DEFAULT and the v3 _nonneg CHECK constraint
// (PostgreSQL re-derives the check against the wider type), so neither is
// dropped or rebuilt. alterColumnToBigint skips columns already BIGINT, keeping
// the migration idempotent and avoiding a needless lock on queues whose newer
// CREATE TABLE already emitted BIGINT.
func migrateBigintRetryCounts(ctx context.Context, tx *sql.Tx) error {
	tableNames, err := listQueueTableNames(ctx, tx)
	if err != nil {
		return err
	}

	for _, tableName := range tableNames {
		queueType, err := queueTypeForTable(ctx, tx, tableName)
		if err != nil {
			return err
		}
		if err := widenRetryCountsToBigint(ctx, tx, queueType, tableName); err != nil {
			return err
		}
	}

	return nil
}

// widenRetryCountsToBigint widens every retry_count / max_retries column the
// given queue actually has — the same column set the v3 migration constrains.
// tableName is the sanitized per-queue identifier ([a-z0-9_]+, <= 28 chars)
// written by sanitizeTableName, so direct interpolation is safe.
func widenRetryCountsToBigint(
	ctx context.Context, tx *sql.Tx, queueType QueueType, tableName string,
) error {
	cols := []struct{ table, column string }{
		{"pgqueue_dlq_" + tableName, "retry_count"},
	}
	switch queueType {
	case QueueTypeChannel:
		cols = append(cols,
			struct{ table, column string }{"pgqueue_msg_" + tableName, "retry_count"},
			struct{ table, column string }{"pgqueue_msg_" + tableName, "max_retries"},
		)
	case QueueTypePubSub:
		cols = append(cols, struct{ table, column string }{
			"pgqueue_sub_" + tableName, "retry_count",
		})
	}

	for _, c := range cols {
		if err := alterColumnToBigint(ctx, tx, c.table, c.column); err != nil {
			return fmt.Errorf(
				"failed to widen %s.%s to bigint: %w", c.table, c.column, err,
			)
		}
	}

	return nil
}

// alterColumnToBigint widens a column from integer to bigint, skipping the
// ALTER (and its ACCESS EXCLUSIVE lock and table rewrite) when the column is
// already bigint. The data_type probe is scoped to current_schema() so it
// resolves the same table the search_path-qualified ALTER will target (FR-024
// non-default schemas). table is built from the sanitized per-queue name and
// column is one of a fixed set of literals, so both are safe to interpolate.
func alterColumnToBigint(ctx context.Context, tx *sql.Tx, table, column string) error {
	var dataType string
	err := tx.QueryRowContext(ctx,
		`SELECT data_type FROM information_schema.columns
		  WHERE table_schema = current_schema()
		    AND table_name = $1 AND column_name = $2`,
		table, column,
	).Scan(&dataType)
	if errors.Is(err, sql.ErrNoRows) {
		// Column absent: nothing to widen. Defensive — callers only pass columns
		// the queue type is known to have.
		return nil
	}
	if err != nil {
		return fmt.Errorf("read data_type for %s.%s: %w", table, column, err)
	}
	if dataType == "bigint" {
		return nil
	}

	stmt := fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s TYPE BIGINT`, table, column)
	if _, err := tx.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("alter %s.%s to bigint: %w", table, column, err)
	}

	return nil
}

// queueTypeForTable reads the queue_type column from pgqueue_metadata for a
// given per-queue table name. The v3 migration uses it to decide whether to
// patch the channel msg table or the pub/sub sub table.
func queueTypeForTable(ctx context.Context, tx *sql.Tx, tableName string) (QueueType, error) {
	var qt string
	err := tx.QueryRowContext(ctx,
		`SELECT queue_type FROM pgqueue_metadata WHERE table_name = $1`,
		tableName,
	).Scan(&qt)
	if err != nil {
		return "", fmt.Errorf(
			"failed to read queue_type for table %q: %w", tableName, err,
		)
	}

	return QueueType(qt), nil
}

// pgDuplicateObject is the PostgreSQL SQLSTATE for a duplicate_object error
// (e.g. ADD CONSTRAINT for a constraint that already exists).
const pgDuplicateObject = "42710"

// isDuplicateObjectError reports whether err is PostgreSQL's duplicate_object
// SQLSTATE 42710. The v3 migration uses it to stay idempotent against a queue
// whose newer CREATE TABLE already emitted the CHECK constraint up front. It is
// driver-agnostic: pgx errors are matched on SQLSTATE via the sqlStater
// interface; drivers whose accessor has a different shape (lib/pq) fall back to
// the error text. The text fallback deliberately stays narrow — matching the
// bare phrase "already exists" only when no SQLSTATE is available — because that
// phrase also appears in duplicate_table/duplicate_index (42P07) messages.
func isDuplicateObjectError(err error) bool {
	if err == nil {
		return false
	}
	var s sqlStater
	if errors.As(err, &s) {
		return s.SQLState() == pgDuplicateObject
	}
	// Error-text fallback for drivers without a SQLState() accessor (lib/pq).
	return strings.Contains(err.Error(), "already exists")
}

// listQueueTableNames reads every per-queue table_name recorded in
// pgqueue_metadata. A migration that must patch the dynamic per-queue tables
// uses it to discover the live set; the result set is closed before the caller
// runs any further statement on the transaction.
func listQueueTableNames(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT table_name FROM pgqueue_metadata`)
	if err != nil {
		return nil, fmt.Errorf("failed to list queue tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan queue table name: %w", err)
		}
		tableNames = append(tableNames, tableName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate queue tables: %w", err)
	}

	return tableNames, nil
}

// init fails fast at process startup if the migrations slice is malformed.
func init() {
	if err := validateMigrations(migrations); err != nil {
		panic("pgqueue: " + err.Error())
	}
}

// validateMigrations enforces the structural invariants of the migrations
// slice: versions must be contiguous and ascending from 1, the last version
// must equal SchemaVersion, and every migration must define at least one of
// apply / applyNonTx. A gap or a stale SchemaVersion would silently skip
// migrations, and a migration with neither phase would record a version
// without doing any work.
func validateMigrations(ms []migration) error {
	for i := range ms {
		if want := i + 1; ms[i].version != want {
			return fmt.Errorf(
				"%w: must be contiguous and ascending; "+
					"index %d has version %d, want %d",
				errMalformedMigrations, i, ms[i].version, want)
		}
		if ms[i].apply == nil && ms[i].applyNonTx == nil {
			return fmt.Errorf(
				"%w: migration %d (%s) defines neither apply nor applyNonTx",
				errMalformedMigrations, ms[i].version, ms[i].name)
		}
	}
	if last := ms[len(ms)-1].version; last != SchemaVersion {
		return fmt.Errorf(
			"%w: SchemaVersion is %d but the last migration is %d",
			errMalformedMigrations, SchemaVersion, last)
	}

	return nil
}

// queryRower is satisfied by *sql.DB, *sql.Conn, and *sql.Tx, letting
// schemaVersion read the current version from any of them.
type queryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// schemaVersion returns the highest schema version recorded in the schema
// version table, or 0 if no migrations have been applied yet. versionTable is
// the (optionally schema-qualified) name of the pgqueue_schema_version table.
func schemaVersion(ctx context.Context, q queryRower, versionTable string) (int, error) {
	var version int
	err := q.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM `+versionTable,
	).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("failed to read schema version: %w", err)
	}

	return version, nil
}

// runMigrations brings the database schema up to the latest known version.
//
// A dedicated connection holds a session-level advisory lock for the whole run
// so that application instances starting concurrently serialize here instead of
// racing on the same DDL. Each pending migration is applied in its own
// transaction together with the row that records it, so a version is recorded
// if and only if its migration committed.
//
// When a non-default schema is configured (FR-024), the schema is created and
// the connection's search_path is pointed at it for the duration of the run, so
// the otherwise-unqualified base-schema DDL lands in that schema. search_path is
// reliable here because the run holds one dedicated connection; it is RESET
// before that connection returns to the pool.
//
// After the migrations apply, runMigrations repairs any pgqueue index left
// invalid by an interrupted build (B5/#136). This runs on every call, not just
// when a migration is pending, so a crash that invalidates an index is healed at
// the next startup. It reuses the advisory-locked connection so concurrent
// startups do not race on the drop/recreate. logger may be nil.
func runMigrations(ctx context.Context, db DB, schema string, logger *slog.Logger) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire migration connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	releaseLock, err := acquireMigrationLock(ctx, conn, logger)
	if err != nil {
		return err
	}
	defer releaseLock()

	prefix := schemaTablePrefix(schema)
	resetSearchPath, err := configureMigrationSchema(ctx, conn, schema)
	if err != nil {
		return err
	}
	defer resetSearchPath()

	current, err := loadSchemaVersion(ctx, conn, prefix+"pgqueue_schema_version")
	if err != nil {
		return err
	}

	for i := range migrations {
		m := migrations[i]
		if m.version <= current {
			continue
		}
		if err := applyMigration(ctx, conn, m); err != nil {
			return fmt.Errorf("schema migration %d (%s): %w", m.version, m.name, err)
		}
	}

	// Heal indexes left invalid by an interrupted build.
	if err := repairIndexesAfterMigrations(ctx, conn, schema, logger); err != nil {
		return err
	}

	return nil
}

// acquireMigrationLock takes the session-level advisory lock on conn that
// serializes concurrent migration runs and returns a release func. The lock is
// released explicitly rather than via conn.Close — Close returns the connection
// to the pool without ending the session, so a session-level lock would
// otherwise leak onto a pooled connection. context.WithoutCancel ensures the
// unlock still runs even if the caller's context was cancelled, and a non-nil
// unlock error is logged at WARN when a logger is configured (#137) rather than
// silently swallowed.
func acquireMigrationLock(
	ctx context.Context, conn *sql.Conn, logger *slog.Logger,
) (func(), error) {
	if _, err := conn.ExecContext(ctx,
		"SELECT pg_advisory_lock($1)", migrationAdvisoryLockKey,
	); err != nil {
		return nil, fmt.Errorf("failed to acquire migration lock: %w", err)
	}
	return func() {
		if _, unlockErr := conn.ExecContext(context.WithoutCancel(ctx),
			"SELECT pg_advisory_unlock($1)", migrationAdvisoryLockKey,
		); unlockErr != nil && logger != nil {
			logger.Warn("pgqueue: failed to release migration advisory lock",
				"err", unlockErr)
		}
	}, nil
}

// loadSchemaVersion ensures the schema-version bookkeeping table exists, reads
// the current applied version, and rejects a database newer than this binary
// understands (rolling-deploy guard, #53): an older binary must not silently run
// against a schema it was not built for, so it returns ErrSchemaTooNew with the
// version gap instead of proceeding with potentially incompatible DDL.
func loadSchemaVersion(
	ctx context.Context, conn *sql.Conn, versionTable string,
) (int, error) {
	if _, err := conn.ExecContext(ctx, schemaVersionTableSQL); err != nil {
		return 0, fmt.Errorf("failed to create schema version table: %w", err)
	}
	current, err := schemaVersion(ctx, conn, versionTable)
	if err != nil {
		return 0, err
	}
	if current > SchemaVersion {
		return 0, fmt.Errorf(
			"%w: database at v%d, this binary knows v%d",
			ErrSchemaTooNew, current, SchemaVersion,
		)
	}
	return current, nil
}

// configureMigrationSchema creates the configured non-default schema and points
// the migration connection's search_path at it, so the otherwise-unqualified
// base-schema DDL lands there. It returns a function that resets the search_path
// before the connection returns to the pool, preventing the change from leaking
// onto unrelated queries on that pooled connection. For the default public
// schema it is a no-op.
func configureMigrationSchema(ctx context.Context, conn *sql.Conn, schema string) (func(), error) {
	noop := func() {}
	if schemaTablePrefix(schema) == "" {
		return noop, nil
	}
	if _, err := conn.ExecContext(ctx,
		"CREATE SCHEMA IF NOT EXISTS "+schema,
	); err != nil {
		return noop, fmt.Errorf("failed to create schema %q: %w", schema, err)
	}
	if _, err := conn.ExecContext(ctx,
		"SET search_path TO "+schema,
	); err != nil {
		return noop, fmt.Errorf("failed to set search_path: %w", err)
	}
	return func() {
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), "RESET search_path")
	}, nil
}

// applyMigration runs a single migration and records its version.
//
// The optional applyNonTx phase runs first, directly on the migration
// connection outside any transaction (for statements PostgreSQL forbids inside
// a transaction block, e.g. CREATE INDEX CONCURRENTLY). The optional apply
// phase then runs inside the transaction that also inserts the
// pgqueue_schema_version row, so apply and the version record commit
// atomically. The advisory lock held by runMigrations spans this whole call,
// so concurrent InitSchema callers never run applyNonTx simultaneously.
func applyMigration(ctx context.Context, conn *sql.Conn, m migration) error {
	if m.applyNonTx != nil {
		if err := m.applyNonTx(ctx, conn); err != nil {
			return err
		}
	}

	tx, err := conn.BeginTx(ctx, readCommittedTxOptions)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if m.apply != nil {
		if err := m.apply(ctx, tx); err != nil {
			return err
		}
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO pgqueue_schema_version (version, description) VALUES ($1, $2)`,
		m.version, m.name,
	); err != nil {
		return fmt.Errorf("failed to record schema version: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// SchemaVersion returns the schema version currently applied to the database.
//
// It returns 0 if InitSchema has never created the pgqueue_schema_version table.
// Compare the result against the SchemaVersion constant to detect whether a
// newer library build would apply further migrations.
func (pq *PGQueue) SchemaVersion(ctx context.Context) (int, error) {
	// to_regclass resolves to NULL (without erroring) when the table does not
	// exist, so this stays safe to call before InitSchema. Referencing the
	// table directly would fail at query-planning time if it were missing.
	versionTable := pq.globalTable("pgqueue_schema_version")
	var exists bool
	if err := pq.db.QueryRowContext(ctx,
		`SELECT to_regclass($1) IS NOT NULL`, versionTable,
	).Scan(&exists); err != nil {
		return 0, fmt.Errorf("failed to check schema version table: %w", err)
	}
	if !exists {
		return 0, nil
	}

	return schemaVersion(ctx, pq.db, versionTable)
}

// migrateMaxRetriesNotNull is the v5 migration. The channel message table's
// max_retries column was previously BIGINT NULL with a CHECK that allowed NULL
// to mean "no limit". New tables emit it as BIGINT NOT NULL DEFAULT 0 (a
// concrete zero sentinel for "no limit"), which is less ambiguous and removes
// the special-case NULL path in the consumer code. Pre-existing channel message
// tables may still have the old nullable declaration and NULL rows, so:
//  1. Backfill all NULL max_retries rows to 0 (preserves behaviour: 0 retries
//     is handled as "use queue-level default" by the application).
//  2. Set NOT NULL on the column so new rows cannot be written as NULL.
//  3. Drop the old "max_retries IS NULL OR max_retries >= 0" CHECK constraint
//     (which the v3 migration added) and add the simpler "max_retries >= 0".
//
// Pub/sub and DLQ tables do not have a max_retries column, so only channel
// message tables are patched.
func migrateMaxRetriesNotNull(ctx context.Context, tx *sql.Tx) error {
	tableNames, err := listQueueTableNames(ctx, tx)
	if err != nil {
		return err
	}

	for _, tableName := range tableNames {
		queueType, err := queueTypeForTable(ctx, tx, tableName)
		if err != nil {
			return err
		}
		if queueType != QueueTypeChannel {
			continue
		}
		msgTable := "pgqueue_msg_" + tableName

		// Step 1: backfill NULLs to 0.
		//nolint:gosec // G201: validated pgqueue_metadata table name, not user input.
		backfill := fmt.Sprintf(
			`UPDATE %s SET max_retries = 0 WHERE max_retries IS NULL`,
			msgTable,
		)
		if _, err := tx.ExecContext(ctx, backfill); err != nil {
			return fmt.Errorf("backfill max_retries on %s: %w", msgTable, err)
		}

		// Step 2: set NOT NULL.
		setNotNull := fmt.Sprintf(
			`ALTER TABLE %s ALTER COLUMN max_retries SET NOT NULL`,
			msgTable,
		)
		if _, err := tx.ExecContext(ctx, setNotNull); err != nil {
			return fmt.Errorf("set max_retries NOT NULL on %s: %w", msgTable, err)
		}

		// Step 3: drop old nullable check and add the simple non-negative check.
		// The old constraint name matches the pattern the v3 migration used.
		oldConstraint := msgTable + "_max_retries_nonneg"
		dropOld := fmt.Sprintf(
			`ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s`,
			msgTable, oldConstraint,
		)
		if _, err := tx.ExecContext(ctx, dropOld); err != nil {
			return fmt.Errorf("drop old max_retries check on %s: %w", msgTable, err)
		}
		if err := addAndValidateCheck(ctx, tx, msgTable, "max_retries", "max_retries >= 0"); err != nil {
			return fmt.Errorf("add max_retries check on %s: %w", msgTable, err)
		}
	}

	return nil
}

// migrateMetadataDefault is the v6 migration. Older per-queue and DLQ tables
// declared metadata JSONB with no DEFAULT, whereas new tables emit
// metadata JSONB DEFAULT '{}'::jsonb. This migration standardizes that default
// on every existing table that carries the column (pubsub message, channel
// message, and DLQ tables) so a raw insert omitting the column gets the
// canonical empty object. The column is left NULLABLE (#125): the library's own
// inserts pass an explicit value, and parseMetadataJSON treats NULL and '{}'
// identically, so making it NOT NULL would break inserts that pass NULL.
// Setting an already-present default is a no-op, so the migration is idempotent.
func migrateMetadataDefault(ctx context.Context, tx *sql.Tx) error {
	tableNames, err := listQueueTableNames(ctx, tx)
	if err != nil {
		return err
	}

	for _, tableName := range tableNames {
		// Both queue types (channel and pubsub) have pgqueue_msg_* and
		// pgqueue_dlq_* tables with a metadata column.
		// setMetadataDefault probes information_schema first and skips tables
		// where the column does not exist, so it is safe to call unconditionally
		// for every table pattern.
		for _, tbl := range []string{
			"pgqueue_msg_" + tableName,
			"pgqueue_dlq_" + tableName,
		} {
			if err := setMetadataDefault(ctx, tx, tbl); err != nil {
				return err
			}
		}
	}

	return nil
}

// setMetadataDefault adds DEFAULT '{}'::jsonb to the metadata column of a single
// table so a raw insert that omits the column receives the canonical empty
// object (#125). The column is left NULLABLE on purpose: the library's own
// inserts always supply an explicit value (NULL when there is no metadata, via
// jsonbParam), and parseMetadataJSON treats NULL and '{}' identically — making
// the column NOT NULL would reject those NULL inserts. It is idempotent (setting
// an already-present default is a no-op). table must be a sanitized pgqueue
// table name (safe to interpolate).
func setMetadataDefault(ctx context.Context, tx *sql.Tx, table string) error {
	// Sub tables have no metadata column, so skip them silently.
	var colExists bool
	err := tx.QueryRowContext(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = current_schema()
			  AND table_name   = $1
			  AND column_name  = 'metadata'
		)`,
		table,
	).Scan(&colExists)
	if err != nil {
		return fmt.Errorf("check metadata column on %s: %w", table, err)
	}
	if !colExists {
		return nil
	}

	setDefault := fmt.Sprintf(
		`ALTER TABLE %s ALTER COLUMN metadata SET DEFAULT '{}'::jsonb`,
		table,
	)
	if _, err := tx.ExecContext(ctx, setDefault); err != nil {
		return fmt.Errorf("set metadata DEFAULT on %s: %w", table, err)
	}

	return nil
}

// migratePubSubDLQSubscriberIndex is the v7 migration. It adds a partial index
// on the subscriber_id column of each pub/sub DLQ table so queries that filter
// by subscriber (e.g. per-subscriber DLQ listing) avoid scanning NULL rows from
// channel DLQ entries. The index is WHERE subscriber_id IS NOT NULL, which
// covers only the pub/sub rows. Channel DLQ tables always have
// subscriber_id = NULL so the index would be empty for them; the migration
// therefore skips non-pubsub queues.
//
// CREATE INDEX IF NOT EXISTS keeps the migration idempotent: re-running on a
// table that already has the index (created by the new createDLQTable path) is
// safe.
func migratePubSubDLQSubscriberIndex(ctx context.Context, tx *sql.Tx) error {
	tableNames, err := listQueueTableNames(ctx, tx)
	if err != nil {
		return err
	}

	for _, tableName := range tableNames {
		queueType, err := queueTypeForTable(ctx, tx, tableName)
		if err != nil {
			return err
		}
		if queueType != QueueTypePubSub {
			continue
		}
		//nolint:gosec // G201: tableName is a pgqueue_metadata queue name validated by queueNameRegex, not user input.
		stmt := fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS idx_pgqueue_dlq_%s_subscriber_id `+
				`ON pgqueue_dlq_%s(subscriber_id) WHERE subscriber_id IS NOT NULL`,
			tableName, tableName,
		)
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf(
				"failed to create subscriber_id index on DLQ for queue %q: %w",
				tableName, err,
			)
		}
	}

	return nil
}
