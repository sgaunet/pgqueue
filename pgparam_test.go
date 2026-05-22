package pgqueue

import (
	"testing"

	"github.com/google/uuid"
)

// The batch and replay paths pass array parameters as PostgreSQL array literals
// ("{a,b}") rather than Go slices, so they marshal on every database/sql driver
// (notably lib/pq, which rejects a raw []string). These tests pin the literal
// rendering.

func TestUUIDArrayLiteral(t *testing.T) {
	u1 := uuid.MustParse("0190d4e2-0000-7000-8000-000000000001")
	u2 := uuid.MustParse("0190d4e2-0000-7000-8000-000000000002")

	tests := []struct {
		name string
		in   []uuid.UUID
		want string
	}{
		{"empty", nil, "{}"},
		{"single", []uuid.UUID{u1}, "{" + u1.String() + "}"},
		{"multiple", []uuid.UUID{u1, u2}, "{" + u1.String() + "," + u2.String() + "}"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := uuidArrayLiteral(tc.in); got != tc.want {
				t.Errorf("uuidArrayLiteral(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestFloat64ArrayLiteral(t *testing.T) {
	tests := []struct {
		name string
		in   []float64
		want string
	}{
		{"empty", nil, "{}"},
		{"integral", []float64{2}, "{2}"},
		{"fractional", []float64{1.5, 0.25}, "{1.5,0.25}"},
		{"very small", []float64{1e-9}, "{1e-09}"},
		{"zero", []float64{0}, "{0}"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := float64ArrayLiteral(tc.in); got != tc.want {
				t.Errorf("float64ArrayLiteral(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestReceiptsToIDClaimLiterals proves the two literals stay index-aligned: the
// nth element of the ID literal pairs with the nth element of the claim literal,
// which the unnest(...) join in batch ack/nack relies on.
func TestReceiptsToIDClaimLiterals(t *testing.T) {
	m1 := uuid.MustParse("0190d4e2-0000-7000-8000-00000000000a")
	c1 := uuid.MustParse("0190d4e2-0000-7000-8000-00000000000b")
	m2 := uuid.MustParse("0190d4e2-0000-7000-8000-00000000000c")
	c2 := uuid.MustParse("0190d4e2-0000-7000-8000-00000000000d")

	receipts := []Receipt{
		{MessageID: m1, ClaimID: c1},
		{MessageID: m2, ClaimID: c2},
	}

	ids, claims := receiptsToIDClaimLiterals(receipts)

	wantIDs := "{" + m1.String() + "," + m2.String() + "}"
	wantClaims := "{" + c1.String() + "," + c2.String() + "}"
	if ids != wantIDs {
		t.Errorf("ids = %q, want %q", ids, wantIDs)
	}
	if claims != wantClaims {
		t.Errorf("claims = %q, want %q", claims, wantClaims)
	}

	if emptyIDs, emptyClaims := receiptsToIDClaimLiterals(nil); emptyIDs != "{}" || emptyClaims != "{}" {
		t.Errorf("receiptsToIDClaimLiterals(nil) = %q, %q, want {}, {}", emptyIDs, emptyClaims)
	}
}
