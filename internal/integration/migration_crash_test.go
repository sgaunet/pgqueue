package integration_test

// TestMigrationCrashRecovery exercises the forward-only migration runner's
// ability to re-converge after a simulated mid-flight crash and proves the
// advisory-lock-based serialization keeps two concurrent InitSchema calls from
// corrupting the schema.
//
// # What the runner actually does (root migrations.go)
//
//  1. Acquires pg_advisory_lock(migrationAdvisoryLockKey) on a dedicated
//     *sql.Conn so no other process can run DDL concurrently.
//  2. Creates pgqueue_schema_version (idempotent, outside a transaction).
//  3. For each migration whose version > recorded max-version:
//     a. If applyNonTx is set, runs it on the connection OUTSIDE a tx
//        (for CREATE INDEX CONCURRENTLY etc.).  This phase is NOT rolled back
//        on crash; the contract is that every applyNonTx statement is
//        individually idempotent (IF NOT EXISTS, etc.).
//     b. Opens a transaction, runs apply (if set), inserts the version row,
//        and commits atomically.
//  4. Releases the advisory lock.
//
// # Real-world crash scenarios simulated here
//
//   - Scenario A – crash between version-row commit and the next migration:
//     Some migrations applied, pgqueue_schema_version row written, but later
//     ones were never run.  Simulated by deleting a version row for a
//     mid-range version so the runner re-applies only the missing migrations.
//
//   - Scenario B – crash after applyNonTx but before the transaction commit:
//     The non-tx work (e.g. an index) is present in the DB but the version row
//     was never inserted.  Every applyNonTx statement uses IF NOT EXISTS /
//     duplicate-object suppression, so re-running it must be a safe no-op.
//     Simulated by (1) running InitSchema fully, (2) deleting the version row
//     for a migration that has a transactional apply phase, so the runner will
//     re-run apply against already-applied DDL (idempotency check).
//
//   - Scenario C – concurrent InitSchema calls:
//     Two goroutines race to run InitSchema on a fresh DB.  Only one performs
//     the DDL; the other blocks on the advisory lock and then finds all
//     migrations already applied (no duplicate version rows, no errors).
//     Already covered by TestInitSchemaConcurrent; included here for
//     completeness and referenced below.
//
// # What CANNOT be covered from this package
//
//   - Injecting a panicking migration: the migrations slice is unexported and
//     not injectable from package integration_test, so we cannot register a
//     custom migration that panics inside apply.  The closest faithful
//     equivalent is manipulating pgqueue_schema_version directly to replay
//     real migrations (scenarios A & B above).
//
//   - Testing applyNonTx crash recovery end-to-end: the library currently has
//     no migration with an applyNonTx phase (all seven migrations use only the
//     transactional apply phase), so there is no live applyNonTx path to
//     exercise.  The test documents and validates the property indirectly by
//     confirming that re-running a migration that has already been applied is
//     safe (the idempotency guarantee the applyNonTx contract depends on).

import (
	"context"
	"testing"

	"github.com/sgaunet/pgqueue"
)

// TestMigrationCrashRecoveryPartialApply verifies Scenario A:
//
// A process ran InitSchema and committed versions 1–N, then crashed before
// committing version N+1.  On the next startup InitSchema must re-apply only
// the missing migrations, reach SchemaVersion, and leave exactly one row per
// applied version in pgqueue_schema_version.
func TestMigrationCrashRecoveryPartialApply(t *testing.T) {
	db, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Full initial run — all migrations applied.
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("first InitSchema failed: %v", err)
	}

	// Create a channel queue so per-queue tables exist. This exercises the
	// per-queue-table patching path that migrations 2–7 perform on pre-existing
	// tables (the path most likely to break on a partial re-run).
	pq, err := pgqueue.New(ctx, db)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = pq.Close() }()

	if err := pq.CreateChannel(ctx, "crash-recovery"); err != nil {
		t.Fatalf("CreateChannel failed: %v", err)
	}

	// ----- Simulate Scenario A -----
	// Delete every version row above v4 — as if the process crashed after
	// committing v4 but before committing v5, then was restarted repeatedly
	// without ever completing the later migrations. Deleting the whole suffix
	// (rather than a fixed list) keeps this robust as SchemaVersion grows: the
	// runner resumes from COALESCE(MAX(version), 0), so the gap must reach the top.
	if _, err := db.ExecContext(ctx,
		`DELETE FROM pgqueue_schema_version WHERE version > 4`); err != nil {
		t.Fatalf("delete version rows to simulate partial crash: %v", err)
	}

	var rowCount int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pgqueue_schema_version`).Scan(&rowCount); err != nil {
		t.Fatalf("count rows after partial delete: %v", err)
	}
	if rowCount != 4 {
		t.Fatalf("expected 4 version rows after partial delete, got %d", rowCount)
	}

	// ----- Recovery run -----
	// InitSchema must detect that versions 5, 6, 7 are missing and re-apply them
	// without error, even though their DDL (ALTER TABLE etc.) was already run by
	// the first full InitSchema call.  Each migration's apply func uses IF NOT
	// EXISTS / duplicate-object suppression to be idempotent.
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("recovery InitSchema failed: %v", err)
	}

	// ----- Post-recovery assertions -----

	// Every migration must have exactly one version row.
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pgqueue_schema_version`).Scan(&rowCount); err != nil {
		t.Fatalf("count version rows after recovery: %v", err)
	}
	if rowCount != pgqueue.SchemaVersion {
		t.Errorf("expected %d version rows after recovery, got %d",
			pgqueue.SchemaVersion, rowCount)
	}

	// The recorded max version must equal the library constant.
	var maxVersion int
	if err := db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM pgqueue_schema_version`).Scan(&maxVersion); err != nil {
		t.Fatalf("read max version after recovery: %v", err)
	}
	if maxVersion != pgqueue.SchemaVersion {
		t.Errorf("expected max schema version %d after recovery, got %d",
			pgqueue.SchemaVersion, maxVersion)
	}

	// The schema is fully functional: publish and receive must work.
	if _, err := pq.Publish(ctx, "crash-recovery", []byte("after-recovery")); err != nil {
		t.Fatalf("publish after recovery failed: %v", err)
	}
	msg, err := pq.ReceiveChannel(ctx, "crash-recovery")
	if err != nil {
		t.Fatalf("receive after recovery failed: %v", err)
	}
	if string(msg.Payload) != "after-recovery" {
		t.Errorf("unexpected payload %q after recovery", msg.Payload)
	}

	// A third InitSchema (idempotent re-run) must not add any rows.
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("idempotent third InitSchema failed: %v", err)
	}
	var rowCountAfter int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pgqueue_schema_version`).Scan(&rowCountAfter); err != nil {
		t.Fatalf("count rows after third run: %v", err)
	}
	if rowCountAfter != rowCount {
		t.Errorf("idempotent re-run changed version row count: %d -> %d",
			rowCount, rowCountAfter)
	}
}

// TestMigrationCrashRecoveryIdempotentReApply verifies Scenario B:
//
// A process ran the transactional apply of migration N (which patches
// pre-existing per-queue tables), the DB committed the DDL, but the version
// row was never inserted (the commit of the version-recording transaction
// failed, or the process crashed between the DDL commit and the tx commit).
// On the next startup InitSchema must be able to re-run apply safely — the
// migration must be idempotent.
//
// Current migrations 2–7 use ADD CONSTRAINT IF NOT EXISTS, ALTER TYPE with
// skip-if-already-bigint guards, CREATE INDEX IF NOT EXISTS, etc., so they
// satisfy this contract.  This test verifies that property end-to-end.
func TestMigrationCrashRecoveryIdempotentReApply(t *testing.T) {
	db, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Full initial run.
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("first InitSchema failed: %v", err)
	}
	pq, err := pgqueue.New(ctx, db)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = pq.Close() }()

	// Create queues so migrations 2–7 have pre-existing tables to patch.
	if err := pq.CreateChannel(ctx, "idem-chan"); err != nil {
		t.Fatalf("CreateChannel failed: %v", err)
	}
	if err := pq.CreateTopic(ctx, "idem-topic"); err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}
	if err := pq.Subscribe(ctx, "idem-topic", "sub1"); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// ----- Simulate Scenario B -----
	// Delete all version rows except v1 — as if every migration from v2 onward
	// ran its apply but the version-recording transaction was never committed.
	if _, err := db.ExecContext(ctx,
		`DELETE FROM pgqueue_schema_version WHERE version > 1`); err != nil {
		t.Fatalf("delete version rows to simulate B crash: %v", err)
	}

	// ----- Recovery run -----
	// All migrations v2–v7 will be re-applied on tables whose DDL was already
	// committed by the first full run.  None must return an error.
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("recovery InitSchema (scenario B) failed — migrations are not idempotent: %v", err)
	}

	// Every version must be recorded exactly once.
	var rowCount int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pgqueue_schema_version`).Scan(&rowCount); err != nil {
		t.Fatalf("count version rows: %v", err)
	}
	if rowCount != pgqueue.SchemaVersion {
		t.Errorf("expected %d version rows, got %d", pgqueue.SchemaVersion, rowCount)
	}

	// All retry-counter columns must remain BIGINT after the re-applied v4
	// migration (the widen-to-bigint migration must not corrupt already-bigint
	// columns).
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

// TestMigrationAdvisoryLockSerializes verifies Scenario C:
//
// Two goroutines call InitSchema concurrently on a freshly initialised schema
// whose version row for the latest migration has been deliberately removed,
// so both callers see work to do.  The advisory lock must ensure exactly one
// re-applies the migration; the other must block and then observe the schema
// already at the target version, returning nil without inserting a duplicate
// version row.
//
// This complements TestInitSchemaConcurrent (which races on a completely empty
// DB) by targeting the post-migration-table-exists path where both callers
// have real DDL to run.
func TestMigrationAdvisoryLockSerializes(t *testing.T) {
	db, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Full initial run.
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

	// Make the latest migration appear "missing" so both concurrent callers
	// believe they have work to do.
	if _, err := db.ExecContext(ctx,
		`DELETE FROM pgqueue_schema_version WHERE version = $1`,
		pgqueue.SchemaVersion); err != nil {
		t.Fatalf("delete latest version row: %v", err)
	}

	// Fire two concurrent InitSchema calls.
	const concurrency = 5
	type result struct{ err error }
	results := make(chan result, concurrency)
	for range concurrency {
		go func() {
			results <- result{err: pgqueue.InitSchema(ctx, db)}
		}()
	}

	for range concurrency {
		if r := <-results; r.err != nil {
			t.Errorf("concurrent InitSchema failed: %v", r.err)
		}
	}

	// The version row must exist exactly once despite the concurrent race.
	var count int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pgqueue_schema_version WHERE version = $1`,
		pgqueue.SchemaVersion).Scan(&count); err != nil {
		t.Fatalf("count version rows for latest version: %v", err)
	}
	if count != 1 {
		t.Errorf("expected exactly 1 row for version %d after concurrent init, got %d",
			pgqueue.SchemaVersion, count)
	}

	// Total row count must still be SchemaVersion (no extras from concurrent runs).
	var total int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pgqueue_schema_version`).Scan(&total); err != nil {
		t.Fatalf("count total version rows: %v", err)
	}
	if total != pgqueue.SchemaVersion {
		t.Errorf("expected %d total version rows, got %d", pgqueue.SchemaVersion, total)
	}
}
