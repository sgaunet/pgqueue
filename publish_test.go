package pgqueue

import (
	"encoding/json"
	"errors"
	"strings"
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

// TestResolveMaxMetadataSizePrefersPerQueue verifies that a per-queue cap
// stored in pgqueue_metadata.config takes precedence over the queue-wide cap.
func TestResolveMaxMetadataSizePrefersPerQueue(t *testing.T) {
	pq := &Queue{cfg: queueConfig{maxMetadataSize: 1024}}

	perQueueConfig, _ := json.Marshal(channelOptions{MaxMetadataSize: 4096})
	meta := &queueMetadata{Config: perQueueConfig}
	if got := pq.resolveMaxMetadataSize(meta); got != 4096 {
		t.Errorf("per-queue 4096 + queue-wide 1024: got %d, want 4096", got)
	}

	emptyConfig := json.RawMessage(`{}`)
	if got := pq.resolveMaxMetadataSize(&queueMetadata{Config: emptyConfig}); got != 1024 {
		t.Errorf("empty per-queue config: got %d, want 1024 (queue-wide)", got)
	}
}

// TestMarshalAndValidateMetadataRejectsOversized verifies the DoS cap from
// issue #119: a marshaled-metadata size above the per-queue cap is rejected
// with ErrMetadataSizeExceeded rather than letting PG's 1 GiB hard limit be
// the only ceiling.
func TestMarshalAndValidateMetadataRejectsOversized(t *testing.T) {
	pq := &Queue{cfg: queueConfig{maxMetadataSize: 64}}
	meta := &queueMetadata{Config: json.RawMessage(`{}`)}

	// A 200-byte string value blows past the 64-byte cap once JSON-encoded.
	big := map[string]any{"k": strings.Repeat("x", 200)}
	_, err := pq.marshalAndValidateMetadata(meta, big)
	if !errors.Is(err, ErrMetadataSizeExceeded) {
		t.Fatalf("oversized metadata: err = %v, want ErrMetadataSizeExceeded", err)
	}

	// A small value fits and is returned as JSON.
	small := map[string]any{"k": "v"}
	out, err := pq.marshalAndValidateMetadata(meta, small)
	if err != nil {
		t.Fatalf("small metadata: unexpected err %v", err)
	}
	if len(out) == 0 {
		t.Error("small metadata: expected non-empty marshaled bytes")
	}

	// nil metadata is a no-op: (nil, nil).
	out, err = pq.marshalAndValidateMetadata(meta, nil)
	if err != nil || out != nil {
		t.Errorf("nil metadata: got (%v, %v), want (nil, nil)", out, err)
	}
}
