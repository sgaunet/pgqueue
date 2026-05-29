package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sgaunet/pgqueue"
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

// TestSchemaVersion verifies SchemaVersion reports the applied version.
func TestSchemaVersion(t *testing.T) {
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

	version, err := pq.SchemaVersion(ctx)
	if err != nil {
		t.Fatalf("SchemaVersion failed: %v", err)
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
	first, err := pq.ReceiveChannel(ctx, "reclaim", pgqueue.WithVisibilityTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatalf("first consume failed: %v", err)
	}
	if first == nil || first.ID != sent {
		t.Fatalf("first consume did not return the published message")
	}

	// Before the timeout expires the message is invisible.
	if msg, cErr := pq.ReceiveChannel(ctx, "reclaim", pgqueue.WithVisibilityTimeout(time.Second)); cErr != nil && !errors.Is(cErr, pgqueue.ErrQueueEmpty) {
		t.Fatalf("consume during timeout failed: %v", cErr)
	} else if msg != nil {
		t.Fatalf("message should be invisible before its visibility timeout expires")
	}

	// After the timeout expires it is reclaimed by consume — no GC involved.
	// intentional: the 100ms visibility timeout must have elapsed before the
	// reclaim consume; there is no DB state that confirms expiry without running
	// a consume (which is what we are testing).
	time.Sleep(200 * time.Millisecond) // intentional: let 100ms visibility timeout lapse
	second, err := pq.ReceiveChannel(ctx, "reclaim", pgqueue.WithVisibilityTimeout(time.Second))
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

// assertColumnIsBigint fails the test unless the given column reports
// data_type 'bigint' in information_schema (PostgreSQL's name for BIGINT). Tests
// run in the default public schema, so table_name + column_name uniquely
// identify the column.
func assertColumnIsBigint(t *testing.T, db *sql.DB, table, column string) {
	t.Helper()
	var dataType string
	err := db.QueryRowContext(context.Background(),
		`SELECT data_type FROM information_schema.columns
		  WHERE table_name = $1 AND column_name = $2`,
		table, column,
	).Scan(&dataType)
	if err != nil {
		t.Fatalf("read data_type for %s.%s: %v", table, column, err)
	}
	if dataType != "bigint" {
		t.Errorf("%s.%s: expected data_type bigint, got %q", table, column, dataType)
	}
}

// TestRetryCounterColumnsAreBigint verifies that freshly created channel and
// pub/sub tables declare every retry counter as BIGINT, so a pathological
// crash-loop cannot overflow a 32-bit counter and let a message dodge the DLQ
// (#135).
func TestRetryCounterColumnsAreBigint(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	if err := pq.CreateChannel(ctx, "bigintchan"); err != nil {
		t.Fatalf("CreateChannel failed: %v", err)
	}
	if err := pq.CreateTopic(ctx, "biginttopic"); err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}
	if err := pq.Subscribe(ctx, "biginttopic", "sub1"); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Channel message + DLQ counters.
	assertColumnIsBigint(t, db, "pgqueue_msg_bigintchan", "retry_count")
	assertColumnIsBigint(t, db, "pgqueue_msg_bigintchan", "max_retries")
	assertColumnIsBigint(t, db, "pgqueue_dlq_bigintchan", "retry_count")
	// Pub/sub subscription + DLQ counters.
	assertColumnIsBigint(t, db, "pgqueue_sub_biginttopic", "retry_count")
	assertColumnIsBigint(t, db, "pgqueue_dlq_biginttopic", "retry_count")
}

// TestMigrateBigintRetryCounts verifies the v4 migration widens the retry
// counters of pre-existing per-queue tables. It simulates a database created
// before v4 by reverting the columns to INT and deleting the v4-and-later rows
// from pgqueue_schema_version, then re-runs InitSchema and asserts every counter
// is BIGINT again.
func TestMigrateBigintRetryCounts(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	if err := pq.CreateChannel(ctx, "bigintmig"); err != nil {
		t.Fatalf("CreateChannel failed: %v", err)
	}
	if err := pq.CreateTopic(ctx, "bigintmigtopic"); err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}
	if err := pq.Subscribe(ctx, "bigintmigtopic", "sub1"); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Roll the per-queue counters back to INT to mimic a pre-v4 schema. ALTER
	// TYPE keeps the v3 _nonneg CHECK constraint and the column default.
	revertToInt := []struct{ table, column string }{
		{"pgqueue_msg_bigintmig", "retry_count"},
		{"pgqueue_msg_bigintmig", "max_retries"},
		{"pgqueue_dlq_bigintmig", "retry_count"},
		{"pgqueue_sub_bigintmigtopic", "retry_count"},
		{"pgqueue_dlq_bigintmigtopic", "retry_count"},
	}
	for _, c := range revertToInt {
		// #nosec G201 -- table/column are fixed test literals, not user input.
		stmt := "ALTER TABLE " + c.table + " ALTER COLUMN " + c.column + " TYPE INTEGER"
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("revert %s.%s to INTEGER: %v", c.table, c.column, err)
		}
	}

	// Drop v4 and every later recorded version so the forward-only runner (which
	// keys off MAX(version)) treats the database as pre-v4 and re-applies v4
	// onward. Deleting only the v4 row would leave MAX(version) at the latest
	// schema, and the runner would skip v4 as already applied.
	if _, err := db.ExecContext(ctx,
		`DELETE FROM pgqueue_schema_version WHERE version >= 4`); err != nil {
		t.Fatalf("delete v4+ schema_version rows: %v", err)
	}

	// Re-run the migration runner; it must re-apply v4 and widen the columns.
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("re-run InitSchema failed: %v", err)
	}

	for _, c := range revertToInt {
		assertColumnIsBigint(t, db, c.table, c.column)
	}

	// The v4 row is recorded again and InitSchema is back at the latest version.
	var maxVersion int
	if err := db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM pgqueue_schema_version`).Scan(&maxVersion); err != nil {
		t.Fatalf("read schema version: %v", err)
	}
	if maxVersion != pgqueue.SchemaVersion {
		t.Errorf("expected schema version %d after re-migration, got %d",
			pgqueue.SchemaVersion, maxVersion)
	}
}
