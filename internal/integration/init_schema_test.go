package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sgaunet/pgqueue"
)

func TestInitSchema(t *testing.T) {
	db, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("successful_initialization", func(t *testing.T) {
		err := pgqueue.InitSchema(ctx, db)
		if err != nil {
			t.Fatalf("InitSchema failed: %v", err)
		}

		// Verify pgqueue_metadata table exists
		var tableExists bool
		err = db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT FROM information_schema.tables
				WHERE table_schema = 'public'
				AND table_name = 'pgqueue_metadata'
			)
		`).Scan(&tableExists)
		if err != nil {
			t.Fatalf("failed to check pgqueue_metadata table: %v", err)
		}
		if !tableExists {
			t.Error("pgqueue_metadata table was not created")
		}

		// Verify pgqueue_subscribers table exists
		err = db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT FROM information_schema.tables
				WHERE table_schema = 'public'
				AND table_name = 'pgqueue_subscribers'
			)
		`).Scan(&tableExists)
		if err != nil {
			t.Fatalf("failed to check pgqueue_subscribers table: %v", err)
		}
		if !tableExists {
			t.Error("pgqueue_subscribers table was not created")
		}

		// Verify pgqueue_replay_log table exists
		err = db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT FROM information_schema.tables
				WHERE table_schema = 'public'
				AND table_name = 'pgqueue_replay_log'
			)
		`).Scan(&tableExists)
		if err != nil {
			t.Fatalf("failed to check pgqueue_replay_log table: %v", err)
		}
		if !tableExists {
			t.Error("pgqueue_replay_log table was not created")
		}
	})

	t.Run("idempotent_behavior", func(t *testing.T) {
		err := pgqueue.InitSchema(ctx, db)
		if err != nil {
			t.Fatalf("InitSchema second call failed: %v", err)
		}

		var tableCount int
		err = db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM information_schema.tables
			WHERE table_schema = 'public'
			AND table_name IN ('pgqueue_metadata', 'pgqueue_subscribers', 'pgqueue_replay_log')
		`).Scan(&tableCount)
		if err != nil {
			t.Fatalf("failed to count tables: %v", err)
		}
		if tableCount != 3 {
			t.Errorf("expected 3 tables, got %d", tableCount)
		}
	})
}

func TestInitSchemaNilDB(t *testing.T) {
	ctx := context.Background()

	err := pgqueue.InitSchema(ctx, nil)
	if err == nil {
		t.Fatal("InitSchema should fail with nil database")
	}

	expectedMsg := "database connection is required"
	if err.Error() != expectedMsg {
		t.Errorf("expected error message %q, got %q", expectedMsg, err.Error())
	}
}

func TestInitSchemaInvalidConnection(t *testing.T) {
	ctx := context.Background()

	db, err := sql.Open("pgx", "postgres://invalid:invalid@localhost:9999/invalid?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatalf("failed to create invalid db connection: %v", err)
	}
	defer db.Close()

	err = pgqueue.InitSchema(ctx, db)
	if err == nil {
		t.Fatal("InitSchema should fail with invalid connection")
	}

	// InitSchema verifies the database is reachable (and runs a supported
	// PostgreSQL version) before running any DDL, so an unreachable server
	// fails fast at the ping rather than deep inside the migration runner.
	if !strings.Contains(err.Error(), "failed to ping database") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// uncomparableListener is a pgqueue.Listener whose dynamic type is not
// comparable (it has a slice field). InitSchema must reject it without panicking
// when it inspects the supplied options.
type uncomparableListener struct{ marker []int }

func (uncomparableListener) Listen(context.Context, string) error { return nil }
func (uncomparableListener) Notifications() <-chan string         { return nil }
func (uncomparableListener) Close() error                         { return nil }

// TestInitSchemaRejectsUnhonoredOptions (R-14) verifies that InitSchema only
// honors WithSchema: passing any other Option (here WithMaxQueues, which
// InitSchema cannot act on) returns ErrInvalidConfig, while InitSchema with no
// options and InitSchema with WithSchema both still succeed.
func TestInitSchemaRejectsUnhonoredOptions(t *testing.T) {
	db, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// A non-schema option must be rejected.
	err := pgqueue.InitSchema(ctx, db, pgqueue.WithMaxQueues(5))
	if !errors.Is(err, pgqueue.ErrInvalidConfig) {
		t.Errorf("InitSchema with WithMaxQueues should return ErrInvalidConfig, got: %v", err)
	}

	// A WithListener carrying a listener whose dynamic type is not comparable
	// must be rejected cleanly with ErrInvalidConfig, never panic.
	err = pgqueue.InitSchema(ctx, db, pgqueue.WithListener(uncomparableListener{}))
	if !errors.Is(err, pgqueue.ErrInvalidConfig) {
		t.Errorf("InitSchema with WithListener should return ErrInvalidConfig, got: %v", err)
	}

	// No options: still succeeds.
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Errorf("InitSchema with no options should succeed, got: %v", err)
	}

	// WithSchema is the one honored option: still succeeds.
	if err := pgqueue.InitSchema(ctx, db, pgqueue.WithSchema("public")); err != nil {
		t.Errorf("InitSchema with WithSchema should succeed, got: %v", err)
	}
}
