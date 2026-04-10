package pgqueue_test

import (
	"testing"
	"time"

	"github.com/sgaunet/pgqueue/pkg/pgqueue"
)

func TestNewUUIDv7(t *testing.T) {
	u, err := pgqueue.NewUUIDv7()
	if err != nil {
		t.Fatalf("NewUUIDv7() error: %v", err)
	}

	// Check version bits (byte 6, upper nibble should be 0x7_)
	if version := u[6] >> 4; version != 7 {
		t.Errorf("expected version 7, got %d", version)
	}

	// Check variant bits (byte 8, upper 2 bits should be 10)
	if variant := u[8] >> 6; variant != 2 {
		t.Errorf("expected variant 2 (RFC 4122), got %d", variant)
	}
}

func TestExtractTimestampRoundTrip(t *testing.T) {
	before := time.Now()
	u, err := pgqueue.NewUUIDv7()
	if err != nil {
		t.Fatalf("NewUUIDv7() error: %v", err)
	}
	after := time.Now()

	ts := pgqueue.ExtractTimestamp(u)

	// Timestamp should be within the before/after window (truncated to ms)
	if ts.Before(before.Truncate(time.Millisecond)) {
		t.Errorf("extracted timestamp %v is before generation start %v", ts, before)
	}
	if ts.After(after.Truncate(time.Millisecond).Add(time.Millisecond)) {
		t.Errorf("extracted timestamp %v is after generation end %v", ts, after)
	}
}

func TestUUIDv7Ordering(t *testing.T) {
	const count = 100
	uuids := make([][16]byte, count)

	for i := range count {
		u, err := pgqueue.NewUUIDv7()
		if err != nil {
			t.Fatalf("NewUUIDv7() error at %d: %v", i, err)
		}
		uuids[i] = u
	}

	// UUIDs generated sequentially should be non-decreasing when compared as bytes
	for i := 1; i < count; i++ {
		prev := uuids[i-1]
		curr := uuids[i]

		// Compare first 6 bytes (timestamp portion) — should be non-decreasing
		for b := range 6 {
			if prev[b] < curr[b] {
				break
			}
			if prev[b] > curr[b] {
				t.Errorf("UUID[%d] timestamp is less than UUID[%d]", i, i-1)
				break
			}
		}
	}
}
