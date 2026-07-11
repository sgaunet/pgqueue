package integration_test

// TestMigrationCrashRecovery* exercise the forward-only migration runner's
// ability to re-converge after a simulated mid-flight crash and prove the
// advisory-lock serialization keeps concurrent InitSchema calls from corrupting
// the schema.
//
// # What the runner does (root migrations.go)
//
//  1. Acquires pg_advisory_lock(migrationAdvisoryLockKey) on a dedicated
//     *sql.Conn so no other process runs DDL concurrently.
//  2. Creates pgqueue_schema_version (idempotent, outside a transaction).
//  3. For each migration whose version > recorded max-version:
//     a. If applyNonTx is set, runs it on the connection OUTSIDE a tx (for
//        statements PostgreSQL forbids in a transaction, e.g. CREATE INDEX
//        CONCURRENTLY). NOT rolled back on crash; every applyNonTx statement
//        must be individually idempotent (IF NOT EXISTS, etc.).
//     b. Opens a transaction, runs apply (if set), inserts the version row, and
//        commits atomically.
//  4. Releases the advisory lock.
//
// # Post-squash baseline
//
// The pre-release migration history was squashed into a single v1 baseline
// (SchemaVersion == 1) whose apply phase runs baseSchemaSQL — all CREATE ... IF
// NOT EXISTS, hence idempotent. There is no longer a multi-migration chain, so
// the crash scenarios below target the v1 baseline: a crash before the version
// row commits leaves the global tables present (created by an earlier attempt or
// by IF NOT EXISTS) but unversioned, and the next InitSchema must re-run the
// baseline safely and converge.
//
// # applyNonTx coverage
//
// The shipped baseline has no applyNonTx phase, so the runner's applyNonTx branch
// has no live migration to exercise end-to-end. Injecting one would require an
// exported test hook on the frozen v1 API (rejected) or a build-tagged hook; the
// applyNonTx struct contract is covered by TestValidateMigrationsValid's
// "applyNonTx only" case in the root module. This is tracked for when the first
// post-v1 applyNonTx migration lands (it will bring its own crash-recovery test).

import (
	"context"
	"database/sql"
	"testing"

	"github.com/sgaunet/pgqueue"
)

// TestMigrationCrashRecoveryBaseline simulates a crash after the baseline DDL ran
// but before its version row committed: the pgqueue_schema_version row is deleted
// and InitSchema is re-run. The runner must re-apply the idempotent baseline,
// re-record version 1, and leave a fully functional schema.
func TestMigrationCrashRecoveryBaseline(t *testing.T) {
	db, cleanup := setupTestContainer(t)
	defer cleanup()
	ctx := context.Background()

	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("first InitSchema failed: %v", err)
	}

	// Simulate the crash: the baseline DDL is committed (tables exist) but the
	// version row was never inserted. The runner resumes from
	// COALESCE(MAX(version), 0), so removing the row makes it re-run the baseline.
	if _, err := db.ExecContext(ctx, `DELETE FROM pgqueue_schema_version`); err != nil {
		t.Fatalf("delete version row to simulate crash: %v", err)
	}

	// Recovery run: re-applies baseSchemaSQL (CREATE ... IF NOT EXISTS) with no error.
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("recovery InitSchema failed — the baseline is not idempotent: %v", err)
	}

	assertSingleBaselineVersionRow(t, db)

	// The recovered schema is fully functional.
	pq, err := pgqueue.New(ctx, db)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = pq.Close() }()
	if err := pq.CreateChannel(ctx, "crash-recovery"); err != nil {
		t.Fatalf("CreateChannel failed: %v", err)
	}
	if _, err := pq.Publish(ctx, "crash-recovery", []byte("after-recovery")); err != nil {
		t.Fatalf("publish after recovery failed: %v", err)
	}
	msg, err := pq.ReceiveChannel(ctx, "crash-recovery")
	if err != nil {
		t.Fatalf("receive after recovery failed: %v", err)
	}
	if msg == nil || string(msg.Payload) != "after-recovery" {
		t.Errorf("unexpected message after recovery: %v", msg)
	}

	// A third InitSchema (idempotent re-run) must not add rows.
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("idempotent third InitSchema failed: %v", err)
	}
	assertSingleBaselineVersionRow(t, db)
}

// TestMigrationCrashRecoveryWithExistingQueues verifies the baseline re-apply is
// safe when per-queue tables already exist: creating queues, dropping the version
// row, and re-running InitSchema must not disturb the existing per-queue tables
// (the baseline only touches the global tables, all IF NOT EXISTS).
func TestMigrationCrashRecoveryWithExistingQueues(t *testing.T) {
	db, cleanup := setupTestContainer(t)
	defer cleanup()
	ctx := context.Background()

	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("first InitSchema failed: %v", err)
	}
	pq, err := pgqueue.New(ctx, db)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = pq.Close() }()

	if err := pq.CreateChannel(ctx, "idem-chan"); err != nil {
		t.Fatalf("CreateChannel failed: %v", err)
	}
	if err := pq.CreateTopic(ctx, "idem-topic"); err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}
	if err := pq.Subscribe(ctx, "idem-topic", "sub1"); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	if _, err := db.ExecContext(ctx, `DELETE FROM pgqueue_schema_version`); err != nil {
		t.Fatalf("delete version row: %v", err)
	}
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("recovery InitSchema with existing queues failed: %v", err)
	}
	assertSingleBaselineVersionRow(t, db)

	// The retry-counter columns the pre-release chain used to widen to BIGINT are
	// emitted as BIGINT directly by the current CREATE TABLE, and the baseline
	// re-apply must not disturb them.
	for _, tc := range []struct{ table, column string }{
		{"pgqueue_msg_idem_chan", "retry_count"},
		{"pgqueue_msg_idem_chan", "max_retries"},
		{"pgqueue_dlq_idem_chan", "retry_count"},
		{"pgqueue_sub_idem_topic", "retry_count"},
		{"pgqueue_dlq_idem_topic", "retry_count"},
	} {
		assertColumnIsBigint(t, db, tc.table, tc.column)
	}
}

// TestMigrationAdvisoryLockSerializes verifies concurrent InitSchema calls are
// serialized by the advisory lock: with the baseline version row removed so every
// caller sees work to do, exactly one re-applies it and no duplicate version rows
// result.
func TestMigrationAdvisoryLockSerializes(t *testing.T) {
	db, cleanup := setupTestContainer(t)
	defer cleanup()
	ctx := context.Background()

	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("first InitSchema failed: %v", err)
	}
	pq, err := pgqueue.New(ctx, db)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = pq.Close() }()
	if err := pq.CreateChannel(ctx, "lock-test"); err != nil {
		t.Fatalf("CreateChannel failed: %v", err)
	}

	if _, err := db.ExecContext(ctx, `DELETE FROM pgqueue_schema_version`); err != nil {
		t.Fatalf("delete version row: %v", err)
	}

	const concurrency = 5
	results := make(chan error, concurrency)
	for range concurrency {
		go func() { results <- pgqueue.InitSchema(ctx, db) }()
	}
	for range concurrency {
		if err := <-results; err != nil {
			t.Errorf("concurrent InitSchema failed: %v", err)
		}
	}

	assertSingleBaselineVersionRow(t, db)
}

// assertSingleBaselineVersionRow asserts pgqueue_schema_version holds exactly one
// row, at version == SchemaVersion (== 1 for the squashed baseline).
func assertSingleBaselineVersionRow(t *testing.T, db *sql.DB) {
	t.Helper()
	var count, maxVersion int
	if err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*), COALESCE(MAX(version), 0) FROM pgqueue_schema_version`,
	).Scan(&count, &maxVersion); err != nil {
		t.Fatalf("read pgqueue_schema_version: %v", err)
	}
	if count != 1 {
		t.Errorf("pgqueue_schema_version has %d rows, want exactly 1 (single baseline)", count)
	}
	if maxVersion != pgqueue.SchemaVersion {
		t.Errorf("recorded schema version = %d, want %d (SchemaVersion)", maxVersion, pgqueue.SchemaVersion)
	}
}
