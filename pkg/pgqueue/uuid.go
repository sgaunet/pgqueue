package pgqueue

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// UUIDv7 bit layout constants.
const (
	uuidV7Version     = 0x70 // Version 7 marker bits.
	uuidV7VersionMask = 0x0f // Mask to clear version bits.
	uuidVariant       = 0x80 // RFC 4122 variant 10 marker bits.
	uuidVariantMask   = 0x3f // Mask to clear variant bits.
	uuidTimestampShift = 32  // Bit shift for 48-bit timestamp split.
)

// NewUUIDv7 generates a new UUIDv7 with time-ordered properties.
// UUIDv7 embeds a timestamp in the first 48 bits for natural ordering.
func NewUUIDv7() (uuid.UUID, error) {
	var u uuid.UUID

	// Get current Unix timestamp in milliseconds
	now := time.Now()
	unixMs := uint64(now.UnixMilli())

	// Fill first 48 bits with timestamp (6 bytes):
	// bytes 0-1: high 16 bits of the 48-bit timestamp
	// bytes 2-5: low 32 bits of the 48-bit timestamp
	//nolint:gosec // G115: timestamp fits in 48 bits
	binary.BigEndian.PutUint16(u[0:2], uint16(unixMs>>uuidTimestampShift))
	//nolint:gosec // G115: lower 32 bits of timestamp
	binary.BigEndian.PutUint32(u[2:6], uint32(unixMs))

	// Fill remaining random bits (10 bytes)
	if _, err := rand.Read(u[6:]); err != nil {
		return uuid.UUID{}, fmt.Errorf("failed to generate random bytes: %w", err)
	}

	// Set version (7) and variant bits
	u[6] = (u[6] & uuidV7VersionMask) | uuidV7Version
	u[8] = (u[8] & uuidVariantMask) | uuidVariant

	return u, nil
}

// ExtractTimestamp extracts the timestamp from a UUIDv7.
func ExtractTimestamp(u uuid.UUID) time.Time {
	// Extract first 48 bits as timestamp
	ms := uint64(binary.BigEndian.Uint16(u[0:2]))<<uuidTimestampShift |
		uint64(binary.BigEndian.Uint32(u[2:6]))

	return time.UnixMilli(int64(ms)) //nolint:gosec // G115: 48-bit timestamp fits in int64
}
