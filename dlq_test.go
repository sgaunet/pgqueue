package pgqueue

// dlq_test.go unit-tests the pure DLQ page-limit resolution logic
// (resolveDLQLimit) without a database. It is white box (package pgqueue,
// not pgqueue_test) because resolveDLQLimit is unexported and there is no
// exported seam to reach it from outside the package; the DB-dependent
// cursor behavior (empty page preserving AfterID) is covered by an
// integration test in internal/integration/dlq_test.go instead.

import "testing"

// TestDefaultAndMaxDLQPageSizeConstants pins the exported page-size
// constants so a future change to either is a deliberate, reviewed edit
// rather than an accidental one, and asserts the sane invariant that the
// default never exceeds the cap.
func TestDefaultAndMaxDLQPageSizeConstants(t *testing.T) {
	if DefaultDLQPageSize != 100 {
		t.Errorf("DefaultDLQPageSize = %d, want 100", DefaultDLQPageSize)
	}
	if MaxDLQPageSize != 1000 {
		t.Errorf("MaxDLQPageSize = %d, want 1000", MaxDLQPageSize)
	}
	if DefaultDLQPageSize > MaxDLQPageSize {
		t.Errorf("DefaultDLQPageSize (%d) exceeds MaxDLQPageSize (%d)",
			DefaultDLQPageSize, MaxDLQPageSize)
	}
}

// TestResolveDLQLimit covers the limit-resolution rules applied by
// ListDLQMessages before it allocates the result slice: non-positive values
// fall back to DefaultDLQPageSize, values within (0, MaxDLQPageSize] pass
// through unchanged, and values above MaxDLQPageSize are clamped down to it
// (never rejected — ListDLQMessages is read-only, so clamping rather than
// erroring follows the maxConcurrency precedent in consume.go).
func TestResolveDLQLimit(t *testing.T) {
	tests := []struct {
		name string
		n    int
		want int
	}{
		{"zero falls back to default", 0, DefaultDLQPageSize},
		{"negative falls back to default", -5, DefaultDLQPageSize},
		{"large negative falls back to default", -1_000_000, DefaultDLQPageSize},
		{"one passes through", 1, 1},
		{"mid-range value passes through", 500, 500},
		{"exactly the cap passes through", MaxDLQPageSize, MaxDLQPageSize},
		{"one above the cap is clamped", MaxDLQPageSize + 1, MaxDLQPageSize},
		{"far above the cap is clamped", 10_000_000, MaxDLQPageSize},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveDLQLimit(tt.n); got != tt.want {
				t.Errorf("resolveDLQLimit(%d) = %d, want %d", tt.n, got, tt.want)
			}
		})
	}
}
