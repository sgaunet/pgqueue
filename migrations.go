package pgqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// errMalformedMigrations is the sentinel wrapped by every validateMigrations
// failure. It is never surfaced to callers — init panics on it — but having a
// static error keeps the wrapped-error linters satisfied and lets tests assert
// the failure with errors.Is.
var errMalformedMigrations = errors.New("malformed migrations slice")

// SchemaVersion is the latest schema version this build of pgqueue knows how to
// produce. InitSchema migrates a database up to this version automatically.
//
// IMPORTANT: when adding a new entry to the migrations slice below, bump this
// constant to match that entry's version number. An init() check enforces that
// SchemaVersion equals the last migration's version.
const SchemaVersion = 3

// migrationAdvisoryLockKey is a fixed PostgreSQL advisory-lock key (the ASCII
// bytes of "pgqueue") used to serialize schema migrations across processes.
// Multiple application instances calling InitSchema concurrently will block on
// this lock so exactly one of them runs the DDL.
const migrationAdvisoryLockKey int64 = 0x7067717565_7565

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

// isDuplicateObjectError reports whether err is PostgreSQL's duplicate_object
// SQLSTATE 42710. Both pgx and lib/pq surface it as a generic error whose
// message contains "already exists", so the v3 migration matches on that to
// stay idempotent against a queue whose newer CREATE TABLE already emitted the
// CHECK constraint up front.
func isDuplicateObjectError(err error) bool {
	if err == nil {
		return false
	}
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
func runMigrations(ctx context.Context, db DB, schema string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire migration connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx,
		"SELECT pg_advisory_lock($1)", migrationAdvisoryLockKey,
	); err != nil {
		return fmt.Errorf("failed to acquire migration lock: %w", err)
	}
	// Release the lock explicitly: (*sql.Conn).Close returns the connection to
	// the pool without ending the session, so a session-level lock would
	// otherwise leak onto a pooled connection. context.WithoutCancel ensures
	// the unlock still runs even if the caller's context was cancelled.
	defer func() {
		_, _ = conn.ExecContext(context.WithoutCancel(ctx),
			"SELECT pg_advisory_unlock($1)", migrationAdvisoryLockKey)
	}()

	prefix := schemaTablePrefix(schema)
	resetSearchPath, err := configureMigrationSchema(ctx, conn, schema)
	if err != nil {
		return err
	}
	defer resetSearchPath()

	if _, err := conn.ExecContext(ctx, schemaVersionTableSQL); err != nil {
		return fmt.Errorf("failed to create schema version table: %w", err)
	}

	current, err := schemaVersion(ctx, conn, prefix+"pgqueue_schema_version")
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

	return nil
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

// GetSchemaVersion returns the schema version currently applied to the database.
//
// It returns 0 if InitSchema has never created the pgqueue_schema_version table.
// Compare the result against the SchemaVersion constant to detect whether a
// newer library build would apply further migrations.
func (pq *PGQueue) GetSchemaVersion(ctx context.Context) (int, error) {
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
