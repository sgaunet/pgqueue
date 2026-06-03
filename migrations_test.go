package pgqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
)

// fakeSQLStateErr is a driver-style error exposing a SQLSTATE via the same
// SQLState() accessor pgx's *pgconn.PgError uses, so isDuplicateObjectError can
// be exercised without a live database or a driver import.
type fakeSQLStateErr struct{ code string }

func (e fakeSQLStateErr) Error() string    { return "sqlstate " + e.code }
func (e fakeSQLStateErr) SQLState() string { return e.code }

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

// TestIsDuplicateObjectError covers the SQLSTATE-first classification: pgx-style
// errors are matched on 42710 exactly (so a duplicate_table 42P07 with the same
// "already exists" text is NOT misread as duplicate_object), while drivers
// without a SQLState() accessor (lib/pq) fall back to the error text.
func TestIsDuplicateObjectError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"sqlstate 42710", fakeSQLStateErr{code: "42710"}, true},
		{"wrapped sqlstate 42710", fmt.Errorf("apply v3: %w", fakeSQLStateErr{code: "42710"}), true},
		{"sqlstate 42P07 duplicate_table", fakeSQLStateErr{code: "42P07"}, false},
		{"lib/pq text fallback", errors.New(`constraint "c" for relation "r" already exists`), true},
		{"unrelated text", errors.New("connection refused"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isDuplicateObjectError(tc.err); got != tc.want {
				t.Errorf("isDuplicateObjectError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
