package pgqueue

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// NewUUIDv7 generates a new UUIDv7 with time-ordered properties
// UUIDv7 embeds a timestamp in the first 48 bits for natural ordering
func NewUUIDv7() (uuid.UUID, error) {
	var u uuid.UUID

	// Get current Unix timestamp in milliseconds
	now := time.Now()
	unixMs := uint64(now.UnixMilli())

	// Fill first 48 bits with timestamp (6 bytes)
	binary.BigEndian.PutUint16(u[0:2], uint16(unixMs>>32))
	binary.BigEndian.PutUint32(u[2:6], uint32(unixMs))

	// Fill remaining random bits (10 bytes)
	if _, err := rand.Read(u[6:]); err != nil {
		return uuid.UUID{}, fmt.Errorf("failed to generate random bytes: %w", err)
	}

	// Set version (7) and variant bits
	u[6] = (u[6] & 0x0f) | 0x70 // Version 7
	u[8] = (u[8] & 0x3f) | 0x80 // Variant 10

	return u, nil
}

// ExtractTimestamp extracts the timestamp from a UUIDv7
func ExtractTimestamp(u uuid.UUID) time.Time {
	// Extract first 48 bits as timestamp
	ms := uint64(binary.BigEndian.Uint16(u[0:2]))<<32 | uint64(binary.BigEndian.Uint32(u[2:6]))
	return time.UnixMilli(int64(ms))
}
