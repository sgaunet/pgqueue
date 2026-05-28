package pgqueue

import (
	"context"
	"database/sql"
	"testing"
)

// TestSQLDBSatisfiesDB pins the central guarantee of the DB interface (#132):
// the standard library *sql.DB must satisfy it, so passing a *sql.DB to
// InitSchema/New stays a no-op for existing callers. The package-level
// `var _ DB = (*sql.DB)(nil)` already enforces this at compile time; this test
// makes the contract explicit and gives the assertion a named home.
func TestSQLDBSatisfiesDB(t *testing.T) {
	var _ DB = (*sql.DB)(nil)
}

// stubDB is a minimal hand-written DB implementation that is NOT a *sql.DB. It
// proves the interface is small enough for a consumer to satisfy with a wrapper
// or test double, and guards against the interface accidentally growing a
// method that only *sql.DB happens to provide. The method bodies are never
// executed — only the type's interface conformance is asserted.
type stubDB struct{}

func (stubDB) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, nil
}
func (stubDB) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, nil
}
func (stubDB) QueryRowContext(context.Context, string, ...any) *sql.Row {
	return nil
}
func (stubDB) BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error) {
	return nil, nil
}
func (stubDB) PingContext(context.Context) error            { return nil }
func (stubDB) Conn(context.Context) (*sql.Conn, error)      { return nil, nil }

// TestCustomTypeSatisfiesDB confirms a non-*sql.DB type can satisfy DB, so the
// interface delivers the intended substitutability (pool wrapper / instrumented
// handle / test double).
func TestCustomTypeSatisfiesDB(t *testing.T) {
	var _ DB = stubDB{}
}
