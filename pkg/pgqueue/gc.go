package pgqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const defaultGCInterval = 5 * time.Minute

// GarbageCollector handles automatic cleanup of old messages.
type GarbageCollector struct {
	pq       *PGQueue
	config   GarbageCollectorConfig
	stopChan chan struct{}
	doneChan chan struct{}
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

	return &GarbageCollector{
		pq:       pq,
		config:   config,
		stopChan: make(chan struct{}),
		doneChan: make(chan struct{}),
	}
}

// Start begins the garbage collection loop.
func (gc *GarbageCollector) Start(ctx context.Context) {
	ticker := time.NewTicker(gc.config.Interval)
	defer ticker.Stop()
	defer close(gc.doneChan)

	for {
		select {
		case <-ctx.Done():
			return
		case <-gc.stopChan:
			return
		case <-ticker.C:
			if err := gc.collect(ctx); err != nil {
				gc.pq.logError("garbage collection error", "error", err)
			}
		}
	}
}

// Stop gracefully stops the garbage collector.
func (gc *GarbageCollector) Stop() {
	close(gc.stopChan)
	<-gc.doneChan
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
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf(
			"%s/%s: %w", queueType, queueName, ErrQueueNotFound,
		)
	}
	if err != nil {
		return fmt.Errorf("failed to get queue metadata: %w", err)
	}

	return gc.executePurge(ctx, metadata.TableName, queueType)
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

	// For pub/sub, delete subscriptions
	if queueType == QueueTypePubSub {
		deleteSub := "DELETE FROM pgqueue_sub_" + tableName //nolint:gosec // G201: table name validated
		if _, err := tx.ExecContext(ctx, deleteSub); err != nil {
			return fmt.Errorf("failed to delete subscriptions: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit purge: %w", err)
	}

	return nil
}

// collect performs a single garbage collection pass.
func (gc *GarbageCollector) collect(ctx context.Context) error {
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

	for _, queue := range allQueues {
		if err := gc.collectQueue(ctx, queue); err != nil {
			gc.pq.logError("failed to collect queue",
				"queue", queue.QueueName, "error", err,
			)
			continue
		}
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
		return err
	}

	return gc.resetTimedOutEntries(ctx, queue)
}

func (gc *GarbageCollector) applyRetentionPolicy(
	ctx context.Context,
	queue QueueMetadata,
	policy RetentionPolicy,
) error {
	if policy.CompletedMessageTTL > 0 {
		if err := gc.purgeCompletedMessages(
			ctx, queue.TableName, policy.CompletedMessageTTL,
		); err != nil {
			return fmt.Errorf("failed to purge completed messages: %w", err)
		}
	}

	if policy.MaxPendingAge > 0 {
		if err := gc.purgeOldPendingMessages(
			ctx, queue.TableName, policy.MaxPendingAge,
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
	if queue.QueueType == string(QueueTypeChannel) {
		if err := gc.resetTimedOutMessages(ctx, queue.TableName); err != nil {
			return fmt.Errorf("failed to reset timed-out messages: %w", err)
		}
	}

	if queue.QueueType == string(QueueTypePubSub) {
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

// purgeCompletedMessages deletes completed messages older than TTL.
func (gc *GarbageCollector) purgeCompletedMessages(
	ctx context.Context,
	tableName string,
	ttl time.Duration,
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		DELETE FROM pgqueue_msg_%s
		WHERE status = 'completed'
		AND processed_at < $1
	`, tableName)

	cutoff := time.Now().Add(-ttl)
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

// purgeOldPendingMessages deletes pending messages older than max age.
func (gc *GarbageCollector) purgeOldPendingMessages(
	ctx context.Context,
	tableName string,
	maxAge time.Duration,
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		DELETE FROM pgqueue_msg_%s
		WHERE status = 'pending'
		AND created_at < $1
	`, tableName)

	cutoff := time.Now().Add(-maxAge)
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
		SET status = 'pending',
		    visibility_timeout = NULL
		WHERE status = 'processing'
		AND visibility_timeout IS NOT NULL
		AND visibility_timeout < $1
	`, tableName)

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
		SET status = 'pending',
		    visibility_timeout = NULL
		WHERE status = 'processing'
		AND visibility_timeout IS NOT NULL
		AND visibility_timeout < $1
	`, tableName)

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
