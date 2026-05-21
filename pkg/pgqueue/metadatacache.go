package pgqueue

import "sync"

// metadataCache caches the immutable per-queue identity: the table name and
// internal ID resolved from pgqueue_metadata. Only immutable fields are cached;
// mutable state such as `paused` is always read fresh from the database to
// avoid stale reads under concurrent pause/resume operations.
//
// The cache is invalidated (entry deleted) whenever a queue is deleted or its
// metadata changes in a way that affects the table name.
type metadataCache struct {
	mu    sync.Mutex
	items map[string]*cachedQueueMeta // key: "<queue_type>/<queue_name>"
}

// cachedQueueMeta is the subset of QueueMetadata that is safe to cache.
type cachedQueueMeta struct {
	tableName string
}

// newMetadataCache returns an initialized metadataCache.
func newMetadataCache() *metadataCache {
	return &metadataCache{
		items: make(map[string]*cachedQueueMeta),
	}
}

// get returns the cached table name for the given queue type and name, or
// ("", false) if the entry is not cached.
func (mc *metadataCache) get(queueType, queueName string) (string, bool) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	item, ok := mc.items[mc.key(queueType, queueName)]
	if !ok {
		return "", false
	}
	return item.tableName, true
}

// set stores the table name for the given queue type and name.
func (mc *metadataCache) set(queueType, queueName, tableName string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.items[mc.key(queueType, queueName)] = &cachedQueueMeta{tableName: tableName}
}

// invalidate removes the cache entry for the given queue type and name.
// It is safe to call when no entry exists.
func (mc *metadataCache) invalidate(queueType, queueName string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	delete(mc.items, mc.key(queueType, queueName))
}

func (mc *metadataCache) key(queueType, queueName string) string {
	return queueType + "/" + queueName
}
