package pgqueue_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sgaunet/pgqueue/pkg/pgqueue"
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

	if !strings.Contains(err.Error(), "failed to initialize base schema") {
		t.Errorf("unexpected error message: %v", err)
	}
}
