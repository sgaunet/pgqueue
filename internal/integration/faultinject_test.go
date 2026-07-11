package integration_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/sgaunet/pgqueue"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// errBoom is the sentinel a faultInjector hands back for an injected
// failure. Tests assert on it directly with errors.Is rather than matching
// an error string, so there is no ambiguity about whether the failure that
// surfaced is the one the test injected.
var errBoom = errors.New("faultinject: injected database error")

// faultInjector is a one-shot, substring-matched fault trigger shared by a
// faultConnector's connections. Arm it with the substring of a query to
// fail and the error to fail it with; the next query issued through any
// connection sourced from the connector whose text contains that substring
// returns the configured error instead of reaching the real database, and
// the injector disarms itself so it doesn't affect anything downstream
// (retries, cleanup, subsequent tests reusing the pool).
//
// armed is the synchronization point: match/err are set by arm() before
// armed is stored, and check() only reads them after observing armed==true,
// so the sync/atomic Store/Load pair on armed establishes the happens-before
// relationship the Go memory model requires for match/err to be visible
// without their own synchronization.
type faultInjector struct {
	armed atomic.Bool
	match string
	err   error
}

// arm configures the injector to fail the next query containing match with
// err, and switches it on.
func (fi *faultInjector) arm(match string, err error) {
	fi.match = match
	fi.err = err
	fi.armed.Store(true)
}

// check reports the injected error (and disarms) if the injector is armed
// and query contains the configured substring; otherwise it returns nil so
// the caller should proceed to the real driver.
func (fi *faultInjector) check(query string) error {
	if !fi.armed.Load() {
		return nil
	}
	if !strings.Contains(query, fi.match) {
		return nil
	}
	// CompareAndSwap makes the fire-and-disarm atomic against a concurrent
	// query that might also match, so the fault fires exactly once.
	if !fi.armed.CompareAndSwap(true, false) {
		return nil
	}
	return fi.err
}

// faultConnector wraps a real driver.Connector (the pgx stdlib connector for
// a specific DSN) and hands out connections wrapped with fault injection.
// Using a Connector with sql.OpenDB, rather than sql.Register-ing a new
// named driver, keeps this entirely local to the test: no global driver
// registry entry that could race with other tests in the package.
type faultConnector struct {
	inner driver.Connector
	fi    *faultInjector
}

func (c *faultConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.inner.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &faultConn{Conn: conn, fi: c.fi}, nil
}

func (c *faultConnector) Driver() driver.Driver {
	return c.inner.Driver()
}

// faultConn wraps a pgx driver.Conn. It intercepts the Context-based query
// path and delegates everything else (Exec, Prepare, BeginTx, Ping, session
// reset, named-value checking) to the underlying pgx connection via type
// assertion, so database/sql keeps using pgx's real behavior for everything
// this test isn't deliberately breaking.
type faultConn struct {
	driver.Conn
	fi *faultInjector
}

var (
	_ driver.QueryerContext     = (*faultConn)(nil)
	_ driver.ExecerContext      = (*faultConn)(nil)
	_ driver.ConnPrepareContext = (*faultConn)(nil)
	_ driver.ConnBeginTx        = (*faultConn)(nil)
	_ driver.Pinger             = (*faultConn)(nil)
	_ driver.NamedValueChecker  = (*faultConn)(nil)
	_ driver.SessionResetter    = (*faultConn)(nil)
)

func (c *faultConn) QueryContext(
	ctx context.Context, query string, args []driver.NamedValue,
) (driver.Rows, error) {
	if err := c.fi.check(query); err != nil {
		return nil, err
	}
	q, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return q.QueryContext(ctx, query, args)
}

func (c *faultConn) ExecContext(
	ctx context.Context, query string, args []driver.NamedValue,
) (driver.Result, error) {
	e, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return e.ExecContext(ctx, query, args)
}

// PrepareContext wraps the resulting statement in a faultStmt too: if
// database/sql ever falls back to prepare-then-query instead of using
// QueryerContext directly (driver.ErrSkip, or a driver that doesn't
// implement QueryerContext), the fault predicate still applies at the
// statement's StmtQueryContext.
func (c *faultConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	p, ok := c.Conn.(driver.ConnPrepareContext)
	if !ok {
		//nolint:staticcheck // SA1019: fallback only for a driver.Conn without ConnPrepareContext; pgx has it.
		stmt, err := c.Conn.Prepare(query)
		if err != nil {
			return nil, err
		}
		return &faultStmt{Stmt: stmt, query: query, fi: c.fi}, nil
	}
	stmt, err := p.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &faultStmt{Stmt: stmt, query: query, fi: c.fi}, nil
}

func (c *faultConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	b, ok := c.Conn.(driver.ConnBeginTx)
	if !ok {
		//nolint:staticcheck // SA1019: fallback only for a driver.Conn without ConnBeginTx; pgx has it.
		return c.Conn.Begin()
	}
	return b.BeginTx(ctx, opts)
}

func (c *faultConn) Ping(ctx context.Context) error {
	p, ok := c.Conn.(driver.Pinger)
	if !ok {
		return nil
	}
	return p.Ping(ctx)
}

func (c *faultConn) CheckNamedValue(nv *driver.NamedValue) error {
	nc, ok := c.Conn.(driver.NamedValueChecker)
	if !ok {
		return driver.ErrSkip
	}
	return nc.CheckNamedValue(nv)
}

func (c *faultConn) ResetSession(ctx context.Context) error {
	r, ok := c.Conn.(driver.SessionResetter)
	if !ok {
		return nil
	}
	return r.ResetSession(ctx)
}

// faultStmt is the belt-and-braces fallback described on faultConn.PrepareContext.
type faultStmt struct {
	driver.Stmt
	query string
	fi    *faultInjector
}

var (
	_ driver.StmtQueryContext = (*faultStmt)(nil)
	_ driver.StmtExecContext  = (*faultStmt)(nil)
)

func (s *faultStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := s.fi.check(s.query); err != nil {
		return nil, err
	}
	q, ok := s.Stmt.(driver.StmtQueryContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return q.QueryContext(ctx, args)
}

func (s *faultStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	e, ok := s.Stmt.(driver.StmtExecContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return e.ExecContext(ctx, args)
}

// TestReplayMessageStatusCheckDBError is a regression test closing the
// coverage gap left by issue #129's follow-up fix in executeReplayMessage:
// when the replay UPDATE matches 0 rows, the re-SELECT that distinguishes
// "message absent" from "message currently processing" can itself fail with
// a genuine database error (not sql.ErrNoRows). The old code collapsed that
// case into ErrReplayMessageNotFound, masking a real DB failure as "message
// not found" and sending an operator hunting the wrong problem. The fix
// (replay.go's 4-way switch in executeReplayMessage) reports it as its own
// wrapped error instead.
//
// This test spins up its own container (rather than reusing setupTestDB)
// because it needs the raw connection string to build a second *sql.DB
// backed by a fault-injecting driver.Connector against the same database;
// setupTestDB does not expose the DSN.
//
// Message-id strategy: a never-published UUIDv7. The replay UPDATE's WHERE
// clause is "id = $1 AND status != 'processing'", so a row that was never
// inserted deterministically matches 0 rows on every run — no concurrent
// consumer or timing race is needed to reach the re-SELECT reliably. (The
// alternative — replaying a message left 'processing' by an unacked
// consume — would also reach the re-SELECT, but only by relying on nothing
// else touching the row in between; the absent-id path gets there for free.)
func TestReplayMessageStatusCheckDBError(t *testing.T) {
	ctx := context.Background()

	postgresContainer, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithDatabase(testDBName),
		postgres.WithUsername(testUser),
		postgres.WithPassword(testPass),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(testWaitLogOccurrence).
				WithStartupTimeout(testStartupTimeout)))
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}
	defer func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}()

	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	// Good connection: schema init + channel setup, exactly like setupTestDB.
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("failed to initialize schema: %v", err)
	}

	pq, err := pgqueue.New(ctx, db,
		pgqueue.WithMaxMessageSize(testMaxMessageSize),
		pgqueue.WithDefaultMaxRetries(testDefaultMaxRetries),
	)
	if err != nil {
		t.Fatalf("failed to init pgqueue: %v", err)
	}
	defer func() { _ = pq.Close() }()

	const queueName = "replay-msg-status-db-error"
	if err := pq.CreateChannel(ctx, queueName); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Second connection to the same database, routed through the
	// fault-injecting connector.
	drv, ok := stdlib.GetDefaultDriver().(driver.DriverContext)
	if !ok {
		t.Fatal("pgx stdlib driver does not implement driver.DriverContext")
	}
	innerConnector, err := drv.OpenConnector(connStr)
	if err != nil {
		t.Fatalf("failed to build pgx connector: %v", err)
	}

	fi := &faultInjector{}
	faultyDB := sql.OpenDB(&faultConnector{inner: innerConnector, fi: fi})
	defer func() { _ = faultyDB.Close() }()

	faultyPQ, err := pgqueue.New(ctx, faultyDB,
		pgqueue.WithMaxMessageSize(testMaxMessageSize),
		pgqueue.WithDefaultMaxRetries(testDefaultMaxRetries),
	)
	if err != nil {
		t.Fatalf("failed to init faulty pgqueue: %v", err)
	}
	defer func() { _ = faultyPQ.Close() }()

	missing, err := pgqueue.NewUUIDv7()
	if err != nil {
		t.Fatalf("failed to generate id: %v", err)
	}

	// Arm only immediately before the call under test, so the New()/
	// CreateChannel() queries above (and the metadata lookup at the top of
	// ReplayMessage) are unaffected — only the re-SELECT inside
	// executeReplayMessage matches this predicate.
	fi.arm("SELECT status FROM", errBoom)

	err = faultyPQ.ReplayMessage(ctx, queueName, pgqueue.QueueTypeChannel, missing, pgqueue.ReplayOptions{})

	if err == nil {
		t.Fatal("expected an error from ReplayMessage, got nil")
	}
	if errors.Is(err, pgqueue.ErrMessageNotFound) {
		t.Errorf(
			"ReplayMessage misreported the injected DB error as ErrMessageNotFound: %v", err,
		)
	}
	if errors.Is(err, pgqueue.ErrReplayMessageNotFound) {
		t.Errorf(
			"ReplayMessage misreported the injected DB error as ErrReplayMessageNotFound: %v", err,
		)
	}
	if !errors.Is(err, errBoom) {
		t.Errorf("expected ReplayMessage's error to wrap errBoom, got: %v", err)
	}
}
