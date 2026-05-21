package pgqueue

import (
	"errors"
	"testing"
)

// fakeResult is a sql.Result whose RowsAffected can be made to fail, modelling
// a driver that cannot report the affected-row count.
type fakeResult struct {
	rows int64
	err  error
}

func (f fakeResult) LastInsertId() (int64, error) { return 0, nil }
func (f fakeResult) RowsAffected() (int64, error) { return f.rows, f.err }

// TestRowsAffectedOrErrPropagatesDriverError is the R-10 regression test: a
// RowsAffected driver error must be surfaced, never coerced to a zero count —
// coercing to zero would misreport a valid insert as ErrDuplicateMessageID.
func TestRowsAffectedOrErrPropagatesDriverError(t *testing.T) {
	driverErr := errors.New("driver: connection reset")

	n, err := rowsAffectedOrErr(fakeResult{rows: 0, err: driverErr})
	if err == nil {
		t.Fatal("expected an error from rowsAffectedOrErr, got nil")
	}
	if !errors.Is(err, driverErr) {
		t.Errorf("returned error must wrap the driver error; got %v", err)
	}
	if errors.Is(err, ErrDuplicateMessageID) {
		t.Error("a RowsAffected driver error must not be reported as ErrDuplicateMessageID")
	}
	if n != 0 {
		t.Errorf("count on error should be 0, got %d", n)
	}
}

// TestRowsAffectedOrErrReturnsCount confirms the happy path is unchanged: a
// successful RowsAffected is returned verbatim.
func TestRowsAffectedOrErrReturnsCount(t *testing.T) {
	n, err := rowsAffectedOrErr(fakeResult{rows: 5})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 5 {
		t.Errorf("rows affected = %d, want 5", n)
	}
}
