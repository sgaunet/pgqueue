package integration_test

import (
	"context"
	"strings"
	"testing"

	"github.com/sgaunet/pgqueue"
)

// TestDupConstraintAbortProbe builds the TRUE collision: a table that actually
// has the v3 _nonneg constraint, then re-issues ADD CONSTRAINT _nonneg + VALIDATE
// inside one tx with no savepoint — exactly what addAndValidateCheck does on a
// re-run against a table v3 already patched.
func TestDupConstraintAbortProbe(t *testing.T) {
	db, cleanup := setupTestContainer(t)
	defer cleanup()
	ctx := context.Background()

	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	pq, err := pgqueue.New(ctx, db)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = pq.Close() }()
	if err := pq.CreateChannel(ctx, "probe"); err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}

	// Manually add the _nonneg constraint (as a v3 patch would have on a legacy table).
	if _, err := db.ExecContext(ctx,
		`ALTER TABLE pgqueue_msg_probe ADD CONSTRAINT pgqueue_msg_probe_retry_count_nonneg CHECK (retry_count >= 0) NOT VALID`); err != nil {
		t.Fatalf("seed _nonneg: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`ALTER TABLE pgqueue_msg_probe VALIDATE CONSTRAINT pgqueue_msg_probe_retry_count_nonneg`); err != nil {
		t.Fatalf("validate seed: %v", err)
	}

	// Now mimic addAndValidateCheck re-run inside ONE tx (no savepoint).
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, addErr := tx.ExecContext(ctx,
		`ALTER TABLE pgqueue_msg_probe ADD CONSTRAINT pgqueue_msg_probe_retry_count_nonneg CHECK (retry_count >= 0) NOT VALID`)
	t.Logf("re-ADD error: %v", addErr)
	if addErr == nil {
		t.Fatalf("expected duplicate_object error on re-ADD, got nil")
	}

	_, valErr := tx.ExecContext(ctx,
		`ALTER TABLE pgqueue_msg_probe VALIDATE CONSTRAINT pgqueue_msg_probe_retry_count_nonneg`)
	t.Logf("VALIDATE-after-failed-ADD error: %v", valErr)
	if valErr != nil && strings.Contains(valErr.Error(), "25P02") {
		t.Logf("CONFIRMED-ABORT: VALIDATE failed with 25P02 (tx aborted)")
	}
	if valErr == nil {
		t.Logf("VALIDATE-SUCCEEDED despite prior failed ADD in same tx")
	}
}
