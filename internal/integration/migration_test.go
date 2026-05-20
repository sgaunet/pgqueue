package integration_test

import (
	"context"
	"sync"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sgaunet/pgqueue/pkg/pgqueue"
)

// TestSchemaVersionTracking verifies that InitSchema records the applied schema
// version and that re-running it does not record duplicate versions.
func TestSchemaVersionTracking(t *testing.T) {
	db, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("InitSchema failed: %v", err)
	}

	// The version-tracking table must exist and hold exactly one row per
	// applied migration.
	var rowCount, maxVersion int
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(MAX(version), 0) FROM pgqueue_schema_version`,
	).Scan(&rowCount, &maxVersion)
	if err != nil {
		t.Fatalf("failed to read pgqueue_schema_version: %v", err)
	}
	if maxVersion != pgqueue.SchemaVersion {
		t.Errorf("expected schema version %d, got %d", pgqueue.SchemaVersion, maxVersion)
	}
	if rowCount != pgqueue.SchemaVersion {
		t.Errorf("expected %d migration rows, got %d", pgqueue.SchemaVersion, rowCount)
	}

	// Re-running InitSchema must not re-apply migrations or duplicate rows.
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("second InitSchema failed: %v", err)
	}
	var rowCountAfter int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pgqueue_schema_version`,
	).Scan(&rowCountAfter); err != nil {
		t.Fatalf("failed to recount pgqueue_schema_version: %v", err)
	}
	if rowCountAfter != rowCount {
		t.Errorf("re-running InitSchema changed migration row count: %d -> %d",
			rowCount, rowCountAfter)
	}
}

// TestGetSchemaVersion verifies GetSchemaVersion before and after InitSchema.
func TestGetSchemaVersion(t *testing.T) {
	db, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	pq, err := pgqueue.Init(ctx, pgqueue.Config{DB: db})
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Before InitSchema, no version table exists: must report 0, not error.
	version, err := pq.GetSchemaVersion(ctx)
	if err != nil {
		t.Fatalf("GetSchemaVersion before InitSchema failed: %v", err)
	}
	if version != 0 {
		t.Errorf("expected version 0 before InitSchema, got %d", version)
	}

	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("InitSchema failed: %v", err)
	}

	version, err = pq.GetSchemaVersion(ctx)
	if err != nil {
		t.Fatalf("GetSchemaVersion after InitSchema failed: %v", err)
	}
	if version != pgqueue.SchemaVersion {
		t.Errorf("expected version %d after InitSchema, got %d", pgqueue.SchemaVersion, version)
	}
}

// TestInitSchemaConcurrent verifies that InitSchema is safe to call from many
// processes at once: the advisory lock serializes the migration run so no
// caller errors and no migration is applied twice.
func TestInitSchemaConcurrent(t *testing.T) {
	db, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	const concurrency = 10
	var wg sync.WaitGroup
	errs := make([]error, concurrency)
	for i := range concurrency {
		wg.Go(func() {
			errs[i] = pgqueue.InitSchema(ctx, db)
		})
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("concurrent InitSchema call %d failed: %v", i, err)
		}
	}

	var rowCount int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pgqueue_schema_version`,
	).Scan(&rowCount); err != nil {
		t.Fatalf("failed to read pgqueue_schema_version: %v", err)
	}
	if rowCount != pgqueue.SchemaVersion {
		t.Errorf("expected %d migration rows after concurrent init, got %d",
			pgqueue.SchemaVersion, rowCount)
	}
}
