package pgqueue

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestInitSchema(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgresContainer, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2)),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}
	defer func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}()

	// Get connection string
	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	// Connect to database
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	t.Run("successful_initialization", func(t *testing.T) {
		// Initialize schema
		err := InitSchema(ctx, db)
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

		// Verify pgcrypto extension is enabled
		var extensionExists bool
		err = db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT FROM pg_extension
				WHERE extname = 'pgcrypto'
			)
		`).Scan(&extensionExists)
		if err != nil {
			t.Fatalf("failed to check pgcrypto extension: %v", err)
		}
		if !extensionExists {
			t.Error("pgcrypto extension was not enabled")
		}
	})

	t.Run("idempotent_behavior", func(t *testing.T) {
		// Call InitSchema again (should succeed without errors)
		err := InitSchema(ctx, db)
		if err != nil {
			t.Fatalf("InitSchema second call failed: %v", err)
		}

		// Verify tables still exist
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

	// Test with nil database connection
	err := InitSchema(ctx, nil)
	if err == nil {
		t.Fatal("InitSchema should fail with nil database")
	}

	expectedMsg := "database connection cannot be nil"
	if err.Error() != expectedMsg {
		t.Errorf("expected error message %q, got %q", expectedMsg, err.Error())
	}
}

func TestInitSchemaInvalidConnection(t *testing.T) {
	ctx := context.Background()

	// Create database connection with invalid connection string
	db, err := sql.Open("pgx", "postgres://invalid:invalid@localhost:9999/invalid?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatalf("failed to create invalid db connection: %v", err)
	}
	defer db.Close()

	// Test InitSchema with invalid connection
	err = InitSchema(ctx, db)
	if err == nil {
		t.Fatal("InitSchema should fail with invalid connection")
	}

	// Should contain error message about failed initialization
	if err.Error()[:30] != "failed to initialize base sche" {
		t.Errorf("unexpected error message: %v", err)
	}
}
