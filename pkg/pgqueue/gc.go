package pgqueue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultGCInterval         = 5 * time.Minute
	defaultGCMaxWorkers       = 10
	gcSlowCollectThreshold    = 100 * time.Millisecond // L5: named constant for slow GC logging
)

// GarbageCollector handles automatic cleanup of old messages.
type GarbageCollector struct {
	pq       *PGQueue
	config   GarbageCollectorConfig
	stopChan chan struct{}
	stopOnce sync.Once
	started  atomic.Bool
	wg       sync.WaitGroup
}

// NewGarbageCollector creates a new garbage collector instance.
func NewGarbageCollector(
	pq *PGQueue,
	config GarbageCollectorConfig,
) *GarbageCollector {
	// Set defaults
	if config.Interval == 0 {
		config.Interval = defaultGCInterval
	}
	if config.Policies == nil {
		config.Policies = make(map[string]RetentionPolicy)
	}
	if config.MaxWorkers <= 0 {
		config.MaxWorkers = defaultGCMaxWorkers
	}

	return &GarbageCollector{
		pq:       pq,
		config:   config,
		stopChan: make(chan struct{}),
	}
}

// Start begins the garbage collection loop in a background goroutine.
// Safe to call multiple times; only the first call starts the loop.
// Stop() can be called at any time after Start() returns.
// A GarbageCollector cannot be restarted after Stop() or context cancellation;
// create a new instance via NewGarbageCollector instead.
func (gc *GarbageCollector) Start(ctx context.Context) {
	if !gc.started.CompareAndSwap(false, true) {
		return
	}
	gc.wg.Add(1)
	go gc.run(ctx)
}

// Stop gracefully stops the garbage collector.
// Safe to call multiple times or before Start().
func (gc *GarbageCollector) Stop() {
	gc.stopOnce.Do(func() { close(gc.stopChan) })
	gc.wg.Wait()
}

// PurgeQueue immediately purges all messages from a queue (dangerous operation).
func (gc *GarbageCollector) PurgeQueue(
	ctx context.Context,
	queueName string,
	queueType QueueType,
	confirm bool,
) error {
	if !confirm {
		return ErrPurgeNotConfirmed
	}

	// Get queue metadata
	metadata, err := gc.pq.getQueueMetadata(
		ctx, string(queueType), queueName,
	)
	if errors.Is(err, ErrQueueNotFound) {
		return fmt.Errorf(
			"%s/%s: %w", queueType, queueName, ErrQueueNotFound,
		)
	}
	if err != nil {
		return fmt.Errorf("failed to get queue metadata: %w", err)
	}

	return gc.executePurge(ctx, metadata.TableName, queueType)
}

// Collect performs a single garbage collection pass.
func (gc *GarbageCollector) Collect(ctx context.Context) error {
	// Get all queues
	topics, err := gc.pq.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("failed to list topics: %w", err)
	}

	channels, err := gc.pq.ListChannels(ctx)
	if err != nil {
		return fmt.Errorf("failed to list channels: %w", err)
	}

	// Combine all queues
	allQueues := make(
		[]QueueMetadata, 0, len(topics)+len(channels),
	)
	allQueues = append(allQueues, topics...)
	allQueues = append(allQueues, channels...)

	var wg sync.WaitGroup
	sem := make(chan struct{}, gc.config.MaxWorkers)

	var mu sync.Mutex
	var errs []error

	for _, queue := range allQueues {
		select {
		case <-ctx.Done():
			wg.Wait()
			return fmt.Errorf("garbage collection cancelled: %w", ctx.Err())
		case sem <- struct{}{}:
			wg.Add(1)
			go func(q QueueMetadata) {
				defer wg.Done()
				defer func() { <-sem }()

				start := time.Now()
				if err := gc.collectQueue(ctx, q); err != nil {
					gc.pq.logError("failed to collect queue",
						"queue", q.QueueName, "error", err,
						"duration", time.Since(start),
					)
					mu.Lock()
					errs = append(errs, fmt.Errorf("queue %s: %w", q.QueueName, err))
					mu.Unlock()
				} else if d := time.Since(start); d > gcSlowCollectThreshold {
					gc.pq.logInfo("collected queue",
						"queue", q.QueueName, "duration", d,
					)
				}
			}(queue)
		}
	}

	wg.Wait()
	return errors.Join(errs...)
}

func (gc *GarbageCollector) run(ctx context.Context) {
	defer gc.wg.Done()

	ticker := time.NewTicker(gc.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-gc.stopChan:
			return
		case <-ticker.C:
			if err := gc.Collect(ctx); err != nil {
				gc.pq.logError("garbage collection error", "error", err)
			}
		}
	}
}

func (gc *GarbageCollector) executePurge(
	ctx context.Context,
	tableName string,
	queueType QueueType,
) error {
	tx, err := gc.pq.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// For pub/sub, delete subscriptions first (before messages, to avoid FK issues
	// if CASCADE is not relied upon). For channels, this table does not exist.
	if queueType == QueueTypePubSub {
		deleteSub := "DELETE FROM pgqueue_sub_" + tableName //nolint:gosec // G201: table name validated
		if _, err := tx.ExecContext(ctx, deleteSub); err != nil {
			return fmt.Errorf("failed to delete subscriptions: %w", err)
		}
	}

	// Delete all messages
	deleteMsg := "DELETE FROM pgqueue_msg_" + tableName //nolint:gosec // G201: table name validated
	if _, err := tx.ExecContext(ctx, deleteMsg); err != nil {
		return fmt.Errorf("failed to delete messages: %w", err)
	}

	// Delete all DLQ messages
	deleteDLQ := "DELETE FROM pgqueue_dlq_" + tableName //nolint:gosec // G201: table name validated
	if _, err := tx.ExecContext(ctx, deleteDLQ); err != nil {
		return fmt.Errorf("failed to delete DLQ messages: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit purge: %w", err)
	}

	return nil
}

// collectQueue performs garbage collection for a single queue.
func (gc *GarbageCollector) collectQueue(
	ctx context.Context,
	queue QueueMetadata,
) error {
	policy := gc.getPolicy(queue.QueueName)

	if err := gc.applyRetentionPolicy(ctx, queue, policy); err != nil {
		return fmt.Errorf("failed to apply retention policy: %w", err)
	}

	return gc.resetTimedOutEntries(ctx, queue)
}

func (gc *GarbageCollector) applyRetentionPolicy(
	ctx context.Context,
	queue QueueMetadata,
	policy RetentionPolicy,
) error {
	queueType := queue.QueueType

	if policy.CompletedMessageTTL > 0 {
		if err := gc.purgeCompletedMessages(
			ctx, queue.TableName, queueType, policy.CompletedMessageTTL,
		); err != nil {
			return fmt.Errorf("failed to purge completed messages: %w", err)
		}
	}

	if policy.MaxPendingAge > 0 {
		if err := gc.purgeOldPendingMessages(
			ctx, queue.TableName, queueType, policy.MaxPendingAge,
		); err != nil {
			return fmt.Errorf("failed to purge old pending messages: %w", err)
		}
	}

	if policy.DLQRetention > 0 {
		if err := gc.purgeDLQMessages(
			ctx, queue.TableName, policy.DLQRetention,
		); err != nil {
			return fmt.Errorf("failed to purge DLQ messages: %w", err)
		}
	}

	return nil
}

func (gc *GarbageCollector) resetTimedOutEntries(
	ctx context.Context,
	queue QueueMetadata,
) error {
	if queue.QueueType == QueueTypeChannel {
		if err := gc.resetTimedOutMessages(ctx, queue.TableName); err != nil {
			return fmt.Errorf("failed to reset timed-out messages: %w", err)
		}
	}

	if queue.QueueType == QueueTypePubSub {
		if err := gc.resetTimedOutSubscriptions(ctx, queue.TableName); err != nil {
			return fmt.Errorf(
				"failed to reset timed-out subscriptions: %w", err,
			)
		}
	}

	return nil
}

// getPolicy returns the retention policy for a queue.
func (gc *GarbageCollector) getPolicy(queueName string) RetentionPolicy {
	if policy, exists := gc.config.Policies[queueName]; exists {
		return policy
	}
	return gc.config.DefaultPolicy
}

func (gc *GarbageCollector) purgeCompletedMessages(
	ctx context.Context,
	tableName string,
	queueType QueueType,
	ttl time.Duration,
) error {
	cutoff := time.Now().Add(-ttl)

	var query string
	if queueType == QueueTypePubSub {
		query = fmt.Sprintf(`
			DELETE FROM pgqueue_msg_%s m
			WHERE m.created_at < $1
			AND NOT EXISTS (
				SELECT 1 FROM pgqueue_sub_%s s
				WHERE s.message_id = m.id AND s.status != '%s'
			)
		`, tableName, tableName, MessageStatusAcked)
	} else {
		query = fmt.Sprintf(`
			DELETE FROM pgqueue_msg_%s
			WHERE status = '%s'
			AND processed_at < $1
		`, tableName, MessageStatusCompleted)
	}

	result, err := gc.pq.db.ExecContext(ctx, query, cutoff)
	if err != nil {
		return fmt.Errorf("failed to purge completed messages: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows > 0 {
		gc.pq.logInfo("purged completed messages", "count", rows, "table", tableName)
	}

	return nil
}

// purgeOldPendingMessages deletes messages that have been pending longer than maxAge.
// For pub/sub, a message is deleted if ANY subscriber still has it pending past the cutoff.
// WARNING: For pub/sub, deleting the message row cascades to ALL subscription records,
// including those already acked by other subscribers. This differs from
// purgeCompletedMessages which requires ALL subscribers to have acked.
func (gc *GarbageCollector) purgeOldPendingMessages(
	ctx context.Context,
	tableName string,
	queueType QueueType,
	maxAge time.Duration,
) error {
	cutoff := time.Now().Add(-maxAge)

	var query string
	if queueType == QueueTypePubSub {
		query = fmt.Sprintf(`
			DELETE FROM pgqueue_msg_%s m
			WHERE m.created_at < $1
			AND EXISTS (
				SELECT 1 FROM pgqueue_sub_%s s
				WHERE s.message_id = m.id AND s.status = '%s'
			)
		`, tableName, tableName, MessageStatusPending)
	} else {
		query = fmt.Sprintf(`
			DELETE FROM pgqueue_msg_%s
			WHERE status = '%s'
			AND created_at < $1
		`, tableName, MessageStatusPending)
	}

	result, err := gc.pq.db.ExecContext(ctx, query, cutoff)
	if err != nil {
		return fmt.Errorf("failed to purge old pending messages: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows > 0 {
		gc.pq.logInfo("purged old pending messages", "count", rows, "table", tableName)
	}

	return nil
}

// purgeDLQMessages deletes DLQ messages older than retention period.
func (gc *GarbageCollector) purgeDLQMessages(
	ctx context.Context,
	tableName string,
	retention time.Duration,
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		DELETE FROM pgqueue_dlq_%s
		WHERE moved_at < $1
	`, tableName)

	cutoff := time.Now().Add(-retention)
	result, err := gc.pq.db.ExecContext(ctx, query, cutoff)
	if err != nil {
		return fmt.Errorf("failed to purge DLQ messages: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows > 0 {
		gc.pq.logInfo("purged DLQ messages", "count", rows, "table", tableName)
	}

	return nil
}

// resetTimedOutMessages resets messages with expired visibility timeouts.
func (gc *GarbageCollector) resetTimedOutMessages(
	ctx context.Context,
	tableName string,
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		UPDATE pgqueue_msg_%s
		SET status = '%s',
		    visibility_timeout = NULL
		WHERE status = '%s'
		AND visibility_timeout IS NOT NULL
		AND visibility_timeout < $1
	`, tableName, MessageStatusPending, MessageStatusProcessing)

	result, err := gc.pq.db.ExecContext(ctx, query, time.Now())
	if err != nil {
		return fmt.Errorf("failed to reset timed-out messages: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows > 0 {
		gc.pq.logInfo("reset timed-out messages", "count", rows, "table", tableName)
	}

	return nil
}

// resetTimedOutSubscriptions resets subscriptions with expired visibility timeouts.
func (gc *GarbageCollector) resetTimedOutSubscriptions(
	ctx context.Context,
	tableName string,
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		UPDATE pgqueue_sub_%s
		SET status = '%s',
		    visibility_timeout = NULL
		WHERE status = '%s'
		AND visibility_timeout IS NOT NULL
		AND visibility_timeout < $1
	`, tableName, MessageStatusPending, MessageStatusProcessing)

	result, err := gc.pq.db.ExecContext(ctx, query, time.Now())
	if err != nil {
		return fmt.Errorf(
			"failed to reset timed-out subscriptions: %w", err,
		)
	}

	rows, _ := result.RowsAffected()
	if rows > 0 {
		gc.pq.logInfo("reset timed-out subscriptions", "count", rows, "table", tableName)
	}

	return nil
}
