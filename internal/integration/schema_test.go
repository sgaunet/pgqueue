package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/sgaunet/pgqueue"
)

// testSchemaName is the non-default PostgreSQL schema exercised by these tests.
const testSchemaName = "pgqueue_app"

// setupSchemaTestDB starts a container and initializes pgqueue inside the
// non-default schema testSchemaName, returning a Queue, the raw DB handle, and
// a cleanup func.
func setupSchemaTestDB(t *testing.T) (*pgqueue.Queue, *sql.DB, func()) {
	t.Helper()
	ctx := context.Background()

	db, containerCleanup := setupTestContainer(t)

	if err := pgqueue.InitSchema(ctx, db, pgqueue.WithSchema(testSchemaName)); err != nil {
		containerCleanup()
		t.Fatalf("failed to initialize schema: %v", err)
	}

	pq, err := pgqueue.New(ctx, db,
		pgqueue.WithSchema(testSchemaName),
		pgqueue.WithMaxMessageSize(testMaxMessageSize),
		pgqueue.WithDefaultMaxRetries(testDefaultMaxRetries),
		pgqueue.WithBackoffPolicy(pgqueue.BackoffPolicy{
			BaseDelay:  time.Nanosecond,
			MaxDelay:   time.Nanosecond,
			Multiplier: 1,
		}),
	)
	if err != nil {
		containerCleanup()
		t.Fatalf("failed to init pgqueue: %v", err)
	}

	cleanup := func() {
		_ = pq.Close()
		containerCleanup()
	}

	return pq, db, cleanup
}

// countTablesInSchema returns how many pgqueue_* tables exist in the given schema.
func countTablesInSchema(t *testing.T, db *sql.DB, schema string) int {
	t.Helper()
	var n int
	err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM pg_tables
		 WHERE schemaname = $1 AND tablename LIKE 'pgqueue\_%'`,
		schema,
	).Scan(&n)
	if err != nil {
		t.Fatalf("count tables in schema %q: %v", schema, err)
	}
	return n
}

// TestWithSchemaPlacesAllTablesInNonDefaultSchema verifies that, when WithSchema
// is configured, every pgqueue table (global and per-queue) is created in the
// configured schema and none leak into the default public schema (FR-024, T057).
func TestWithSchemaPlacesAllTablesInNonDefaultSchema(t *testing.T) {
	pq, db, cleanup := setupSchemaTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// The four global tables must already live in the configured schema.
	if got := countTablesInSchema(t, db, testSchemaName); got < 4 {
		t.Fatalf("expected >= 4 global pgqueue tables in %q, got %d", testSchemaName, got)
	}

	if err := pq.CreateChannel(ctx, "orders"); err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	if err := pq.CreateTopic(ctx, "events"); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Per-queue tables: channel adds msg+dlq (2), topic adds msg+dlq+sub (3).
	// Together with the 4 global tables that is 9 pgqueue_* tables.
	if got := countTablesInSchema(t, db, testSchemaName); got != 9 {
		t.Fatalf("expected 9 pgqueue tables in %q, got %d", testSchemaName, got)
	}

	// Nothing must have leaked into public.
	if got := countTablesInSchema(t, db, "public"); got != 0 {
		t.Fatalf("expected 0 pgqueue tables in public schema, got %d", got)
	}

	// The schema must be fully functional end-to-end: publish, consume, ack.
	if _, err := pq.Publish(ctx, "orders", []byte("hello")); err != nil {
		t.Fatalf("PublishChannel: %v", err)
	}
	msg, err := pq.ReceiveChannel(ctx, "orders")
	if err != nil {
		t.Fatalf("ReceiveChannel: %v", err)
	}
	if string(msg.Payload) != "hello" {
		t.Fatalf("payload = %q, want %q", msg.Payload, "hello")
	}
	if err := pq.Ack(ctx, msg.Receipt()); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// Pub/sub flow in the non-default schema.
	if err := pq.Subscribe(ctx, "events", "sub1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := pq.Publish(ctx, "events", []byte("evt")); err != nil {
		t.Fatalf("PublishTopic: %v", err)
	}
	tmsg, err := pq.ReceiveTopic(ctx, "events", "sub1")
	if err != nil {
		t.Fatalf("ReceiveTopic: %v", err)
	}
	if err := pq.Ack(ctx, tmsg.Receipt()); err != nil {
		t.Fatalf("Ack topic: %v", err)
	}
}

// TestWithSchemaDLQAndStats verifies the DLQ and stats paths resolve their
// tables in the configured schema.
func TestWithSchemaDLQAndStats(t *testing.T) {
	pq, _, cleanup := setupSchemaTestDB(t)
	defer cleanup()

	ctx := context.Background()

	if err := pq.CreateChannel(ctx, "failing"); err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	if _, err := pq.Publish(ctx, "failing", []byte("doomed")); err != nil {
		t.Fatalf("PublishChannel: %v", err)
	}

	// Nack past max retries so the message lands in the DLQ.
	for i := 0; i <= testDefaultMaxRetries; i++ {
		msg, err := pq.ReceiveChannel(ctx, "failing")
		if err != nil {
			t.Fatalf("ReceiveChannel attempt %d: %v", i, err)
		}
		if err := pq.Nack(ctx, msg.Receipt(), "boom", pgqueue.WithRetryDelay(1)); err != nil {
			t.Fatalf("Nack attempt %d: %v", i, err)
		}
	}

	msgs, _, err := pq.ListDLQMessages(ctx, "failing", pgqueue.QueueTypeChannel, pgqueue.DLQPage{})
	if err != nil {
		t.Fatalf("ListDLQMessages: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 DLQ message, got %d", len(msgs))
	}

	stats, err := pq.GetStats(ctx, "failing", pgqueue.QueueTypeChannel)
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.DLQCount != 1 {
		t.Fatalf("expected DLQCount 1, got %d", stats.DLQCount)
	}
}

// TestInvalidSchemaNameRejected verifies that an invalid schema name is rejected
// with ErrInvalidConfig by both InitSchema and New.
func TestInvalidSchemaNameRejected(t *testing.T) {
	db, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// An empty WithSchema value is not invalid: it falls back to the default
	// "public" schema. Only malformed identifiers are rejected.
	for _, bad := range []string{"bad-schema", "bad schema", "1schema", "drop;table"} {
		if err := pgqueue.InitSchema(ctx, db, pgqueue.WithSchema(bad)); !errors.Is(err, pgqueue.ErrInvalidConfig) {
			t.Fatalf("InitSchema(WithSchema(%q)): got %v, want ErrInvalidConfig", bad, err)
		}
		if _, err := pgqueue.New(ctx, db, pgqueue.WithSchema(bad)); !errors.Is(err, pgqueue.ErrInvalidConfig) {
			t.Fatalf("New(WithSchema(%q)): got %v, want ErrInvalidConfig", bad, err)
		}
	}
}
