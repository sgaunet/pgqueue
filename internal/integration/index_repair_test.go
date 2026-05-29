package integration_test

import (
	"context"
	"database/sql"
	"slices"
	"testing"

	"github.com/sgaunet/pgqueue"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// repairTargetIndex is one of the partial indexes createChannelIndexes emits for
// the "repairme" channel; sanitizeTableName leaves "repairme" unchanged.
const repairTargetIndex = "idx_pgqueue_msg_repairme_available"

// setupRepairTestDB starts a dedicated PostgreSQL container with
// allow_system_table_mods enabled. That postmaster flag is what lets the test
// flip pg_index.indisvalid to forge an invalid index — the only deterministic
// way to reproduce an interrupted CREATE INDEX. It is kept out of the shared
// setupTestContainer so the privileged flag does not leak into every test.
func setupRepairTestDB(t *testing.T) (*pgqueue.Queue, *sql.DB, func()) {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithDatabase(testDBName),
		postgres.WithUsername(testUser),
		postgres.WithPassword(testPass),
		testcontainers.WithCmdArgs("-c", "allow_system_table_mods=on"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(testWaitLogOccurrence).
				WithStartupTimeout(testStartupTimeout)))
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("failed to initialize schema: %v", err)
	}
	pq, err := pgqueue.New(ctx, db)
	if err != nil {
		t.Fatalf("failed to init pgqueue: %v", err)
	}

	cleanup := func() {
		_ = pq.Close()
		_ = db.Close()
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}

	return pq, db, cleanup
}

// TestRepairInvalidIndex verifies B5/#136: an index left invalid by an
// interrupted build is detected and rebuilt, both by the public RepairIndexes
// method and by the automatic pass InitSchema runs at startup.
func TestRepairInvalidIndex(t *testing.T) {
	pq, db, cleanup := setupRepairTestDB(t)
	defer cleanup()
	ctx := context.Background()

	if err := pq.CreateChannel(ctx, "repairme"); err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	if !indexValid(t, db, repairTargetIndex) {
		t.Fatalf("precondition: %s should be valid after CreateChannel", repairTargetIndex)
	}
	defBefore := indexDef(t, db, repairTargetIndex)

	// --- public path: RepairIndexes drops and recreates the invalid index ---
	invalidateIndex(t, db, repairTargetIndex)
	if indexValid(t, db, repairTargetIndex) {
		t.Fatalf("%s should read invalid after forced invalidation", repairTargetIndex)
	}

	res, err := pq.RepairIndexes(ctx)
	if err != nil {
		t.Fatalf("RepairIndexes: %v", err)
	}
	if !slices.Contains(res.Repaired, repairTargetIndex) {
		t.Fatalf("RepairIndexes.Repaired = %v, want it to contain %s", res.Repaired, repairTargetIndex)
	}
	if len(res.Failed) != 0 {
		t.Fatalf("RepairIndexes.Failed = %v, want empty", res.Failed)
	}
	if !indexValid(t, db, repairTargetIndex) {
		t.Fatalf("%s should be valid again after RepairIndexes", repairTargetIndex)
	}
	if got := indexDef(t, db, repairTargetIndex); got != defBefore {
		t.Fatalf("index definition changed after repair:\n before: %s\n after:  %s", defBefore, got)
	}

	// --- auto path: a fresh InitSchema heals the index with no extra call ---
	invalidateIndex(t, db, repairTargetIndex)
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("InitSchema (repair pass): %v", err)
	}
	if !indexValid(t, db, repairTargetIndex) {
		t.Fatalf("%s should be valid again after InitSchema repair pass", repairTargetIndex)
	}

	// --- no-op path: nothing invalid -> empty result, no DDL, no error ---
	res, err = pq.RepairIndexes(ctx)
	if err != nil {
		t.Fatalf("RepairIndexes (no-op): %v", err)
	}
	if len(res.Repaired) != 0 || len(res.Failed) != 0 {
		t.Fatalf("RepairIndexes (no-op) = %+v, want empty", res)
	}
}

// invalidateIndex forges an invalid index by clearing pg_index.indisvalid,
// reproducing what an interrupted CREATE INDEX leaves behind. Requires the
// allow_system_table_mods server flag set by setupRepairTestDB.
func invalidateIndex(t *testing.T, db *sql.DB, name string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`UPDATE pg_index SET indisvalid = false WHERE indexrelid = $1::regclass`,
		name,
	); err != nil {
		t.Fatalf("failed to invalidate index %s: %v", name, err)
	}
}

// indexValid reports pg_index.indisvalid for the named index.
func indexValid(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var valid bool
	if err := db.QueryRowContext(context.Background(),
		`SELECT i.indisvalid FROM pg_index i
		 JOIN pg_class c ON c.oid = i.indexrelid
		 WHERE c.relname = $1`,
		name,
	).Scan(&valid); err != nil {
		t.Fatalf("failed to read indisvalid for %s: %v", name, err)
	}

	return valid
}

// indexDef returns pg_get_indexdef for the named index, used to confirm a repair
// recreated the index identically.
func indexDef(t *testing.T, db *sql.DB, name string) string {
	t.Helper()
	var def string
	if err := db.QueryRowContext(context.Background(),
		`SELECT pg_get_indexdef(c.oid) FROM pg_class c WHERE c.relname = $1`,
		name,
	).Scan(&def); err != nil {
		t.Fatalf("failed to read pg_get_indexdef for %s: %v", name, err)
	}

	return def
}
