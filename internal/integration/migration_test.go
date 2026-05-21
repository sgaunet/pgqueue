package integration_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

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
		`SELECT COUNT(*), COALESCE(MAX(version), 0) FROM pgqueue_schema_version`).Scan(&rowCount, &maxVersion)
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
		`SELECT COUNT(*) FROM pgqueue_schema_version`).Scan(&rowCountAfter); err != nil {
		t.Fatalf("failed to recount pgqueue_schema_version: %v", err)
	}
	if rowCountAfter != rowCount {
		t.Errorf("re-running InitSchema changed migration row count: %d -> %d",
			rowCount, rowCountAfter)
	}
}

// TestInitRequiresSchema verifies Init fails fast with a clear error when
// InitSchema has not been run.
func TestInitRequiresSchema(t *testing.T) {
	db, cleanup := setupTestContainer(t)
	defer cleanup()

	_, err := pgqueue.New(context.Background(), db)
	if !errors.Is(err, pgqueue.ErrSchemaNotInitialized) {
		t.Fatalf("expected ErrSchemaNotInitialized, got %v", err)
	}
}

// TestGetSchemaVersion verifies GetSchemaVersion reports the applied version.
func TestGetSchemaVersion(t *testing.T) {
	db, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("InitSchema failed: %v", err)
	}

	pq, err := pgqueue.New(ctx, db)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	version, err := pq.GetSchemaVersion(ctx)
	if err != nil {
		t.Fatalf("GetSchemaVersion failed: %v", err)
	}
	if version != pgqueue.SchemaVersion {
		t.Errorf("expected version %d, got %d", pgqueue.SchemaVersion, version)
	}
}

// TestVisibilityTimeoutReclaim verifies that a message whose visibility timeout
// expired is redelivered by ConsumeFromChannel itself, without any
// GarbageCollector running.
func TestVisibilityTimeoutReclaim(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	if err := pq.CreateChannel(ctx, "reclaim"); err != nil {
		t.Fatalf("CreateChannel failed: %v", err)
	}

	sent, err := pq.Publish(ctx, "reclaim", []byte("payload"))
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Consume but never ack, with a short visibility timeout.
	first, err := pq.ConsumeFromChannel(ctx, "reclaim", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("first consume failed: %v", err)
	}
	if first == nil || first.ID != sent {
		t.Fatalf("first consume did not return the published message")
	}

	// Before the timeout expires the message is invisible.
	if msg, cErr := pq.ConsumeFromChannel(ctx, "reclaim", time.Second); cErr != nil {
		t.Fatalf("consume during timeout failed: %v", cErr)
	} else if msg != nil {
		t.Fatalf("message should be invisible before its visibility timeout expires")
	}

	// After the timeout expires it is reclaimed by consume — no GC involved.
	time.Sleep(200 * time.Millisecond)
	second, err := pq.ConsumeFromChannel(ctx, "reclaim", time.Second)
	if err != nil {
		t.Fatalf("reclaim consume failed: %v", err)
	}
	if second == nil || second.ID != sent {
		t.Fatalf("timed-out message was not reclaimed by consume")
	}
}

// TestQueueNameLengthLimit verifies queue names that would overflow PostgreSQL's
// 63-byte identifier limit for index names are rejected.
func TestQueueNameLengthLimit(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	tooLong := strings.Repeat("a", 29)
	if err := pq.CreateChannel(ctx, tooLong); !errors.Is(
		err, pgqueue.ErrInvalidQueueName) {
		t.Fatalf("expected ErrInvalidQueueName for a 29-char name, got %v", err)
	}

	okName := strings.Repeat("a", 28)
	if err := pq.CreateChannel(ctx, okName); err != nil {
		t.Fatalf("28-char name should be accepted, got %v", err)
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
		`SELECT COUNT(*) FROM pgqueue_schema_version`).Scan(&rowCount); err != nil {
		t.Fatalf("failed to read pgqueue_schema_version: %v", err)
	}
	if rowCount != pgqueue.SchemaVersion {
		t.Errorf("expected %d migration rows after concurrent init, got %d",
			pgqueue.SchemaVersion, rowCount)
	}
}
