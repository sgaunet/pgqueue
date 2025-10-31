package pgqueue

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	testDBName = "testdb"
	testUser   = "testuser"
	testPass   = "testpass"
)

// setupTestDB creates a PostgreSQL container and returns a PGQueue instance
func setupTestDB(t *testing.T) (*PGQueue, func()) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgresContainer, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithDatabase(testDBName),
		postgres.WithUsername(testUser),
		postgres.WithPassword(testPass),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
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

	// Run migrations
	migrations := `
		CREATE EXTENSION IF NOT EXISTS pgcrypto;

		CREATE TABLE IF NOT EXISTS pgqueue_metadata (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			queue_type TEXT NOT NULL CHECK (queue_type IN ('pubsub', 'channel')),
			queue_name TEXT NOT NULL,
			table_name TEXT NOT NULL,
			config JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE(queue_type, queue_name)
		);

		CREATE INDEX idx_pgqueue_metadata_type_name ON pgqueue_metadata(queue_type, queue_name);
		CREATE INDEX idx_pgqueue_metadata_table_name ON pgqueue_metadata(table_name);

		CREATE TABLE IF NOT EXISTS pgqueue_subscribers (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			topic_name TEXT NOT NULL,
			subscriber_id TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			active BOOLEAN NOT NULL DEFAULT TRUE,
			UNIQUE(topic_name, subscriber_id)
		);

		CREATE INDEX idx_pgqueue_subscribers_topic ON pgqueue_subscribers(topic_name) WHERE active = TRUE;

		CREATE TABLE IF NOT EXISTS pgqueue_replay_log (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			queue_type TEXT NOT NULL,
			queue_name TEXT NOT NULL,
			replay_type TEXT NOT NULL CHECK (replay_type IN ('timestamp', 'message_id', 'dlq', 'replay_from', 'replay_message', 'replay_dlq')),
			replay_params JSONB NOT NULL,
			message_count INT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			created_by TEXT
		);

		CREATE INDEX idx_pgqueue_replay_log_queue ON pgqueue_replay_log(queue_type, queue_name);
		CREATE INDEX idx_pgqueue_replay_log_created_at ON pgqueue_replay_log(created_at);
	`

	if _, err := db.ExecContext(ctx, migrations); err != nil {
		t.Fatalf("failed to run migrations: %v", err)
	}

	// Initialize PGQueue
	pq, err := Init(ctx, Config{
		DB:                db,
		MaxMessageSize:    1024,
		DefaultMaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("failed to init pgqueue: %v", err)
	}

	// Cleanup function
	cleanup := func() {
		pq.Close()
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}

	return pq, cleanup
}
