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

// metadataCache caches the immutable per-queue identity: the table name
// resolved from pgqueue_metadata. Only immutable fields are cached; mutable
// state such as `paused` is always read fresh from the database to avoid stale
// reads under concurrent pause/resume operations.
//
// An entry is invalidated (deleted) when the local process deletes the queue.
// Otherwise expiration happens two ways, both bounded by metadataCacheTTL:
// opportunistically on the next get of the same key, and via a periodic sweep
// driven by the GarbageCollector ticker so an unread stale entry cannot pin
// the mapping indefinitely after a cross-process deletion.
type metadataCache struct {
	mu    sync.Mutex
	items map[string]*cachedQueueMeta // key: "<queue_type>/<queue_name>"
}

// cachedQueueMeta is the subset of QueueMetadata that is safe to cache, plus
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
	mc.items[mc.key(queueType, queueName)] = &cachedQueueMeta{
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

func (mc *metadataCache) key(queueType, queueName string) string {
	return queueType + "/" + queueName
}
