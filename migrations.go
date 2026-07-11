package pgqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
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
const SchemaVersion = 1

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
func (pq *Queue) SchemaVersion(ctx context.Context) (int, error) {
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

