package pgqueue

import (
	"sync"
	"time"
)

// metadataCacheTTL bounds how long a cached table-name mapping is trusted. The
// mapping itself is immutable for the life of a queue, but a queue deleted by
// *another* process leaves this process's entry stale: the cache is only
// invalidated by the local DeleteChannel/DeleteTopic. Expiring entries caps
// that staleness window, so an operation on a remotely-deleted queue soon
// re-reads pgqueue_metadata and surfaces ErrQueueNotFound rather than an opaque
// "relation does not exist" against a dropped table.
//
// Cross-process staleness limitation: a TTL expiry is the only cross-process
// protection this cache has. A fuller fix would require either (a) a DB
// round-trip on every cache hit (re-querying pgqueue_metadata), or (b) storing
// a schema-level generation/version column in pgqueue_metadata and comparing it
// on each lookup — both require a new DB round-trip or a schema change
// respectively and are left as future work (issue #84).
const metadataCacheTTL = 1 * time.Minute

// metadataCacheMaxEntries bounds the number of cached table-name mappings so a
// long-lived process that touches many distinct queues cannot grow the cache
// without bound when no GarbageCollector sweep is running (M6). At capacity, a
// set first drops expired entries and, if still full, evicts the oldest-stored
// entry. Sized far above the tens-to-low-hundreds of queues the table-per-queue
// design targets, so it never evicts a live working set in practice — it is a
// safety ceiling, not an LRU tuned for churn.
const metadataCacheMaxEntries = 4096

// metadataCache caches the immutable per-queue identity: the table name
// resolved from pgqueue_metadata. Only immutable fields are cached; mutable
// state such as `paused` is always read fresh from the database to avoid stale
// reads under concurrent pause/resume operations.
//
// An entry is invalidated (deleted) when the local process deletes the queue.
// Otherwise expiration happens two ways, both bounded by metadataCacheTTL:
// opportunistically on the next get of the same key, and via a periodic sweep
// driven by the GarbageCollector ticker so an unread stale entry cannot pin
// the mapping indefinitely after a cross-process deletion. Independently of any
// GC, the map size is capped at metadataCacheMaxEntries: a set at capacity
// evicts expired-then-oldest entries, so a GC-less process touching many
// distinct queues cannot grow the cache without bound (M6).
type metadataCache struct {
	mu    sync.Mutex
	items map[string]*cachedQueueMeta // key: "<queue_type>/<queue_name>"
}

// cachedQueueMeta is the subset of queueMetadata that is safe to cache, plus
// the time the entry was stored so get can enforce metadataCacheTTL.
type cachedQueueMeta struct {
	tableName string
	storedAt  time.Time
}

// newMetadataCache returns an initialized metadataCache.
func newMetadataCache() *metadataCache {
	return &metadataCache{
		items: make(map[string]*cachedQueueMeta),
	}
}

// get returns the cached table name for the given queue type and name, or
// ("", false) if the entry is not cached or has expired. An expired entry is
// dropped so the next miss re-reads it fresh.
func (mc *metadataCache) get(queueType, queueName string) (string, bool) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	key := mc.key(queueType, queueName)
	item, ok := mc.items[key]
	if !ok {
		return "", false
	}
	if time.Since(item.storedAt) > metadataCacheTTL {
		delete(mc.items, key)
		return "", false
	}
	return item.tableName, true
}

// set stores the table name for the given queue type and name.
func (mc *metadataCache) set(queueType, queueName, tableName string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	key := mc.key(queueType, queueName)
	// Bound the map size independently of any GC sweep: only a genuinely new key
	// grows the map, so evict just before inserting one at capacity (M6).
	if _, exists := mc.items[key]; !exists && len(mc.items) >= metadataCacheMaxEntries {
		mc.evictLocked()
	}
	mc.items[key] = &cachedQueueMeta{
		tableName: tableName,
		storedAt:  time.Now(),
	}
}

// invalidate removes the cache entry for the given queue type and name.
// It is safe to call when no entry exists.
func (mc *metadataCache) invalidate(queueType, queueName string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	delete(mc.items, mc.key(queueType, queueName))
}

// sweep removes entries whose TTL has elapsed. Safe to call concurrently with
// get/set/invalidate; the mutex serialises all access.
func (mc *metadataCache) sweep() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	now := time.Now()
	for key, item := range mc.items {
		if now.Sub(item.storedAt) > metadataCacheTTL {
			delete(mc.items, key)
		}
	}
}

// evictLocked frees room for a new entry when the cache is at capacity. It first
// drops every expired entry — the common case, since a cache large enough to hit
// the cap is usually mostly stale — and only if that frees nothing evicts the
// single oldest-stored entry. The caller must hold mc.mu.
func (mc *metadataCache) evictLocked() {
	now := time.Now()
	before := len(mc.items)
	for key, item := range mc.items {
		if now.Sub(item.storedAt) > metadataCacheTTL {
			delete(mc.items, key)
		}
	}
	if len(mc.items) < before {
		return // expired entries freed room
	}
	var oldestKey string
	var oldestAt time.Time
	first := true
	for key, item := range mc.items {
		if first || item.storedAt.Before(oldestAt) {
			oldestKey, oldestAt, first = key, item.storedAt, false
		}
	}
	if !first {
		delete(mc.items, oldestKey)
	}
}

func (mc *metadataCache) key(queueType, queueName string) string {
	return queueType + "/" + queueName
}
