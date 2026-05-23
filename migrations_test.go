package pgqueue

import (
	"context"
	"database/sql"
	"testing"
)

// stubApply is a non-nil apply phase used to build well-formed test migrations
// without touching a database.
func stubApply(context.Context, *sql.Tx) error { return nil }

// stubApplyNonTx is a non-nil applyNonTx phase used to build well-formed test
// migrations without touching a database.
func stubApplyNonTx(context.Context, *sql.Conn) error { return nil }

// validMigrations builds a contiguous slice of length SchemaVersion whose last
// version equals SchemaVersion, so it satisfies validateMigrations' version
// rules. Every entry has a stub apply phase; callers tweak the last entry to
// exercise the at-least-one-phase rule.
func validMigrations() []migration {
	ms := make([]migration, SchemaVersion)
	for i := range ms {
		ms[i] = migration{version: i + 1, name: "stub", apply: stubApply}
	}

	return ms
}

// TestValidateMigrationsValid accepts the shipped migrations slice and
// synthetic slices exercising each phase combination, including a migration
// that defines only applyNonTx.
func TestValidateMigrationsValid(t *testing.T) {
	if err := validateMigrations(migrations); err != nil {
		t.Fatalf("the shipped migrations slice must validate: %v", err)
	}

	t.Run("applyNonTx only", func(t *testing.T) {
		ms := validMigrations()
		ms[len(ms)-1].apply = nil
		ms[len(ms)-1].applyNonTx = stubApplyNonTx
		if err := validateMigrations(ms); err != nil {
			t.Errorf("a migration with only applyNonTx must validate, got %v", err)
		}
	})

	t.Run("both phases", func(t *testing.T) {
		ms := validMigrations()
		ms[len(ms)-1].applyNonTx = stubApplyNonTx
		if err := validateMigrations(ms); err != nil {
			t.Errorf("a migration with both phases must validate, got %v", err)
		}
	})
}

// TestValidateMigrationsGap rejects a slice whose versions are not contiguous.
func TestValidateMigrationsGap(t *testing.T) {
	ms := []migration{
		{version: 1, name: "one", apply: stubApply},
		{version: 3, name: "three", apply: stubApply},
	}
	if err := validateMigrations(ms); err == nil {
		t.Fatal("expected a non-contiguous migrations slice to be rejected")
	}
}

// TestValidateMigrationsSchemaVersionMismatch rejects a slice whose last
// version does not equal the SchemaVersion constant.
func TestValidateMigrationsSchemaVersionMismatch(t *testing.T) {
	ms := append(validMigrations(),
		migration{version: SchemaVersion + 1, name: "extra", apply: stubApply})
	// validMigrations already ends at SchemaVersion; the extra entry pushes the
	// last version past it while keeping contiguity intact.
	if err := validateMigrations(ms); err == nil {
		t.Fatal("expected a SchemaVersion mismatch to be rejected")
	}
}

// TestValidateMigrationsNoPhase rejects a migration that defines neither apply
// nor applyNonTx — it would record a version without doing any work.
func TestValidateMigrationsNoPhase(t *testing.T) {
	ms := validMigrations()
	ms[len(ms)-1].apply = nil
	ms[len(ms)-1].applyNonTx = nil
	if err := validateMigrations(ms); err == nil {
		t.Fatal("expected a migration with no apply/applyNonTx to be rejected")
	}
}
