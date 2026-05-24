package pgqueue

import (
	"testing"
	"time"
)

func TestMetadataCacheSweepRemovesExpiredEntries(t *testing.T) {
	mc := newMetadataCache()
	mc.set("channel", "fresh", "pgqueue_msg_fresh")
	mc.set("channel", "stale", "pgqueue_msg_stale")

	// Backdate the stale entry past the TTL without going through set, so
	// sweep is exercised independently of wall-clock.
	staleKey := mc.key("channel", "stale")
	mc.items[staleKey].storedAt = time.Now().Add(-2 * metadataCacheTTL)

	mc.sweep()

	if _, ok := mc.get("channel", "stale"); ok {
		t.Errorf("sweep did not drop stale entry %q", staleKey)
	}
	if name, ok := mc.get("channel", "fresh"); !ok || name != "pgqueue_msg_fresh" {
		t.Errorf("sweep dropped fresh entry: got (%q, %v), want (pgqueue_msg_fresh, true)", name, ok)
	}
}

func TestMetadataCacheSweepEmpty(t *testing.T) {
	mc := newMetadataCache()
	mc.sweep() // must not panic on an empty cache.
}
