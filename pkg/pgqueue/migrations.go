package pgqueue

import (
	"context"
	"database/sql"
	"fmt"
)

// SchemaVersion is the latest schema version this build of pgqueue knows how to
// produce. InitSchema migrates a database up to this version automatically.
//
// IMPORTANT: when adding a new entry to the migrations slice below, bump this
// constant to match that entry's version number. An init() check enforces that
// SchemaVersion equals the last migration's version.
const SchemaVersion = 1

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
// apply receives the transaction in which the migration runs. Because it has a
// full *sql.Tx, a migration is not limited to the static global tables: it can
// also patch the dynamically-named per-queue tables (pgqueue_msg_*, pgqueue_dlq_*,
// pgqueue_sub_*) by discovering them from pgqueue_metadata at apply time. Newly
// created queues already get the current table shape from createChannelTables /
// createPubSubTables, so a future migration only needs to patch pre-existing
// tables.
type migration struct {
	version int
	name    string
	apply   func(ctx context.Context, tx *sql.Tx) error
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

// init enforces that the migrations slice stays contiguous and ascending and
// that SchemaVersion matches the last migration. A gap or a stale SchemaVersion
// would silently skip migrations, so this fails fast at process startup.
func init() {
	for i := range migrations {
		if want := i + 1; migrations[i].version != want {
			panic(fmt.Sprintf(
				"pgqueue: migrations must be contiguous and ascending; "+
					"index %d has version %d, want %d",
				i, migrations[i].version, want))
		}
	}
	if last := migrations[len(migrations)-1].version; last != SchemaVersion {
		panic(fmt.Sprintf(
			"pgqueue: SchemaVersion is %d but the last migration is %d",
			SchemaVersion, last))
	}
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
func runMigrations(ctx context.Context, db *sql.DB, schema string) error {
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

// applyMigration runs a single migration and records its version atomically:
// the apply function and the pgqueue_schema_version insert share one
// transaction, so either both succeed or neither does.
func applyMigration(ctx context.Context, conn *sql.Conn, m migration) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := m.apply(ctx, tx); err != nil {
		return err
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
