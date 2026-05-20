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
const SchemaVersion = 2

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
// createPubSubTables, so a migration only needs to patch pre-existing tables.
//
// migrateV2 is a worked example of this dynamic fan-out: it uses queueTableNames
// to ALTER every existing per-queue DLQ table.
type migration struct {
	version int
	name    string
	apply   func(ctx context.Context, tx *sql.Tx) error
}

// migrations is the ordered, append-only list of schema migrations. Each run of
// InitSchema applies every entry whose version is greater than the version
// currently recorded in the database. Never reorder, renumber, or edit the
// behaviour of an already-released entry: only append.
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
		// The last migration's version must equal SchemaVersion (checked by init).
		version: SchemaVersion,
		name:    "table-name uniqueness and pub/sub DLQ subscriber tracking",
		apply:   migrateV2,
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

// migrateV2 enforces table-name uniqueness on pgqueue_metadata and adds a
// subscriber_id column to every existing dead-letter-queue table.
//
//   - The UNIQUE(table_name) constraint closes a collision where queue names
//     differing only by dash vs. underscore (e.g. "a-b" and "a_b") sanitize to
//     the same physical table name.
//   - subscriber_id lets pub/sub DLQ entries record which subscriber failed, so
//     ReplayDLQ can re-deliver to exactly that subscriber. The column is left
//     NULL for channel DLQ entries, which have no subscriber.
func migrateV2(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx,
		`ALTER TABLE pgqueue_metadata
		 ADD CONSTRAINT pgqueue_metadata_table_name_key UNIQUE (table_name)`,
	); err != nil {
		return fmt.Errorf("failed to add table_name unique constraint: %w", err)
	}

	tables, err := queueTableNames(ctx, tx)
	if err != nil {
		return err
	}
	for _, t := range tables {
		// t was validated by queueNameRegex when the queue was created.
		stmt := "ALTER TABLE pgqueue_dlq_" + t + //nolint:gosec // G201: validated table name.
			" ADD COLUMN IF NOT EXISTS subscriber_id TEXT"
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("failed to add subscriber_id to pgqueue_dlq_%s: %w", t, err)
		}
	}

	return nil
}

// queueTableNames returns the sanitized table_name of every queue registered in
// pgqueue_metadata. Migrations use it to fan schema changes out across the
// dynamically-created per-queue tables.
func queueTableNames(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT table_name FROM pgqueue_metadata`)
	if err != nil {
		return nil, fmt.Errorf("failed to list queue tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tables []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, fmt.Errorf("failed to scan queue table name: %w", err)
		}
		tables = append(tables, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate queue tables: %w", err)
	}

	return tables, nil
}

// queryRower is satisfied by *sql.DB, *sql.Conn, and *sql.Tx, letting
// schemaVersion read the current version from any of them.
type queryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// schemaVersion returns the highest schema version recorded in
// pgqueue_schema_version, or 0 if no migrations have been applied yet.
func schemaVersion(ctx context.Context, q queryRower) (int, error) {
	var version int
	err := q.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM pgqueue_schema_version`,
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
func runMigrations(ctx context.Context, db *sql.DB) error {
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

	if _, err := conn.ExecContext(ctx, schemaVersionTableSQL); err != nil {
		return fmt.Errorf("failed to create schema version table: %w", err)
	}

	current, err := schemaVersion(ctx, conn)
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
	var exists bool
	if err := pq.db.QueryRowContext(ctx,
		`SELECT to_regclass('pgqueue_schema_version') IS NOT NULL`,
	).Scan(&exists); err != nil {
		return 0, fmt.Errorf("failed to check schema version table: %w", err)
	}
	if !exists {
		return 0, nil
	}

	return schemaVersion(ctx, pq.db)
}
