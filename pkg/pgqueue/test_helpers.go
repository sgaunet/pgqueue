package pgqueue

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // pgx database driver registration for database/sql
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

//nolint:unused // Used by test files
const (
	testDBName            = "testdb"
	testUser              = "testuser"
	testPass              = "testpass"
	testWaitLogOccurrence = 2
	testStartupTimeout    = 5 * time.Second
	testMaxMessageSize    = 1024 * 1024 // 1MB
	testDefaultMaxRetries = 3
)

//nolint:unused // Used by test files
// setupTestDB creates a PostgreSQL container and returns a PGQueue instance.
func setupTestDB(t *testing.T) (*PGQueue, func()) {
	t.Helper()
	ctx := context.Background()

	// Start PostgreSQL container
	postgresContainer, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithDatabase(testDBName),
		postgres.WithUsername(testUser),
		postgres.WithPassword(testPass),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(testWaitLogOccurrence).
				WithStartupTimeout(testStartupTimeout)),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

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

	// Initialize base schema
	if err := InitSchema(ctx, db); err != nil {
		t.Fatalf("failed to initialize schema: %v", err)
	}

	// Initialize PGQueue
	pq, err := Init(ctx, Config{
		DB:                db,
		MaxMessageSize:    testMaxMessageSize,
		DefaultMaxRetries: testDefaultMaxRetries,
	})
	if err != nil {
		t.Fatalf("failed to init pgqueue: %v", err)
	}

	// Cleanup function
	cleanup := func() {
		_ = pq.Close()
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}

	return pq, cleanup
}
