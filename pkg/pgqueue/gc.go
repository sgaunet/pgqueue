package pgqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	defaultGCInterval      = 5 * time.Minute
	defaultGCMaxWorkers    = 10
	maxGCMaxWorkers        = 100                    // upper bound to cap goroutine/connection fan-out
	gcSlowCollectThreshold = 100 * time.Millisecond // L5: named constant for slow GC logging
)

// GarbageCollector handles automatic cleanup of old messages.
type GarbageCollector struct {
	pq       *Queue
	config   GarbageCollectorConfig
	stopChan chan struct{}
	stopOnce sync.Once
	started  atomic.Bool
	wg       sync.WaitGroup
	mu       sync.Mutex // serializes Start and Stop so wg.Add never races wg.Wait
}

// NewGarbageCollector creates a new garbage collector instance.
func NewGarbageCollector(
	pq *Queue,
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
	if config.MaxWorkers > maxGCMaxWorkers {
		config.MaxWorkers = maxGCMaxWorkers
	}

	gc := &GarbageCollector{
		pq:       pq,
		config:   config,
		stopChan: make(chan struct{}),
	}
	// Back-register on the Queue so Queue.Close stops this GC even if the
	// caller forgets to (R-08). Stop is idempotent, so a caller that also
	// stops it stays safe.
	pq.registerGC(gc)
	return gc
}

// Start begins the garbage collection loop in a background goroutine.
// Safe to call multiple times; only the first call starts the loop.
// Stop() can be called at any time after Start() returns.
// A GarbageCollector cannot be restarted after Stop() or context cancellation;
// create a new instance via NewGarbageCollector instead.
func (gc *GarbageCollector) Start(ctx context.Context) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	if !gc.started.CompareAndSwap(false, true) {
		return
	}
	gc.wg.Add(1)
	go gc.run(ctx)
}

// Stop gracefully stops the garbage collector.
// Safe to call multiple times or before Start().
func (gc *GarbageCollector) Stop() {
	// Release the mutex before waiting on the WaitGroup: holding it across
	// wg.Wait() would block a concurrent Start() (which needs the lock to call
	// wg.Add) and risk a deadlock.
	gc.mu.Lock()
	gc.stopOnce.Do(func() { close(gc.stopChan) })
	gc.mu.Unlock()
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
	topics, err := gc.pq.listQueues(ctx, QueueTypePubSub)
	if err != nil {
		return fmt.Errorf("failed to list topics: %w", err)
	}

	channels, err := gc.pq.listQueues(ctx, QueueTypeChannel)
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

	// Run an initial pass immediately so cleanup and timed-out-message recovery
	// do not wait a full interval after Start.
	select {
	case <-ctx.Done():
		return
	case <-gc.stopChan:
		return
	default:
		if err := gc.Collect(ctx); err != nil {
			gc.pq.logError("garbage collection error", "error", err)
		}
	}

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
		deleteSub := "DELETE FROM " + gc.pq.subTable(tableName) //nolint:gosec // G201: table name validated
		if _, err := tx.ExecContext(ctx, deleteSub); err != nil {
			return fmt.Errorf("failed to delete subscriptions: %w", err)
		}
	}

	// Delete all messages
	deleteMsg := "DELETE FROM " + gc.pq.msgTable(tableName) //nolint:gosec // G201: table name validated
	if _, err := tx.ExecContext(ctx, deleteMsg); err != nil {
		return fmt.Errorf("failed to delete messages: %w", err)
	}

	// Delete all DLQ messages
	deleteDLQ := "DELETE FROM " + gc.pq.dlqTable(tableName) //nolint:gosec // G201: table name validated
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

	if queue.QueueType == QueueTypePubSub {
		if err := gc.purgeInactiveSubscriptions(
			ctx, queue.QueueName, queue.TableName,
		); err != nil {
			return fmt.Errorf("failed to purge inactive subscriptions: %w", err)
		}
		if err := gc.reclaimOrphanTopicMessages(ctx, queue.TableName); err != nil {
			return fmt.Errorf("failed to reclaim orphan topic messages: %w", err)
		}
	}

	// Promote timed-out-and-exhausted channel messages to the DLQ. The consume
	// path no longer does this inline (R-12), so the GC owns it; run it before
	// resetTimedOutEntries so a still-exhausted message is dead-lettered rather
	// than reset back to pending.
	if queue.QueueType == QueueTypeChannel {
		if err := gc.promoteExhaustedChannelMessages(ctx, queue.TableName); err != nil {
			return fmt.Errorf("failed to promote exhausted messages: %w", err)
		}
	}

	return gc.resetTimedOutEntries(ctx, queue)
}

// promoteExhaustedChannelMessagesPageSize bounds the rows promoted to the DLQ
// per transaction, keeping the GC pass's footprint bounded on a pathological
// backlog.
const promoteExhaustedChannelMessagesPageSize = 100

// promoteExhaustedChannelMessages moves channel messages that have timed out in
// 'processing' state and exhausted their retries to the dead-letter queue
// (R-12). retry_count >= effective max is exactly the condition under which a
// further reclaim would breach the retryCount+1 > maxRetry DLQ guard. Work is
// done in bounded keyset-free pages — moveToDLQ deletes each promoted row, so a
// fresh SELECT never re-sees it.
func (gc *GarbageCollector) promoteExhaustedChannelMessages(
	ctx context.Context,
	tableName string,
) error {
	defaultMax := gc.pq.config.DefaultMaxRetries
	selectQuery := fmt.Sprintf(`
		SELECT id, payload, retry_count, metadata
		FROM %s
		WHERE status = '%s'
		  AND visibility_timeout IS NOT NULL
		  AND visibility_timeout < NOW()
		  AND retry_count >= COALESCE(NULLIF(max_retries, 0), $1)
		ORDER BY id
		LIMIT %d
		FOR UPDATE SKIP LOCKED
	`, gc.pq.msgTable(tableName), MessageStatusProcessing, promoteExhaustedChannelMessagesPageSize)

	for {
		promoted, err := gc.promoteExhaustedChannelPage(ctx, tableName, selectQuery, defaultMax)
		if err != nil {
			return err
		}
		if promoted < promoteExhaustedChannelMessagesPageSize {
			return nil // backlog exhausted
		}
	}
}

// exhaustedChannelMessage is one timed-out, retry-exhausted channel message
// awaiting promotion to the dead-letter queue.
type exhaustedChannelMessage struct {
	id           uuid.UUID
	payload      []byte
	retryCount   int
	metadataJSON sql.NullString
}

// promoteExhaustedChannelPage promotes one page of exhausted messages to the
// DLQ in a single transaction and returns how many it moved.
func (gc *GarbageCollector) promoteExhaustedChannelPage(
	ctx context.Context,
	tableName, selectQuery string,
	defaultMax int,
) (int, error) {
	tx, err := gc.pq.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	batch, err := scanExhaustedChannelMessages(ctx, tx, selectQuery, defaultMax)
	if err != nil {
		return 0, err
	}

	for _, e := range batch {
		if err := gc.pq.moveToDLQ(
			ctx, tx, tableName, e.id, errReasonVisibilityTimeout,
			e.payload, e.retryCount+1, e.metadataJSON,
		); err != nil {
			return 0, fmt.Errorf("failed to move exhausted message to DLQ: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit exhausted-message promotion: %w", err)
	}

	if len(batch) > 0 {
		gc.pq.logInfo("promoted exhausted messages to DLQ",
			"count", len(batch), "table", tableName)
	}
	return len(batch), nil
}

// scanExhaustedChannelMessages reads (and FOR UPDATE locks) one page of
// exhausted messages. The rows are fully drained and closed before the caller
// runs any further statement on the transaction.
func scanExhaustedChannelMessages(
	ctx context.Context,
	tx *sql.Tx,
	selectQuery string,
	defaultMax int,
) ([]exhaustedChannelMessage, error) {
	rows, err := tx.QueryContext(ctx, selectQuery, defaultMax)
	if err != nil {
		return nil, fmt.Errorf("failed to query exhausted messages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var batch []exhaustedChannelMessage
	for rows.Next() {
		var e exhaustedChannelMessage
		if err := rows.Scan(&e.id, &e.payload, &e.retryCount, &e.metadataJSON); err != nil {
			return nil, fmt.Errorf("failed to scan exhausted message: %w", err)
		}
		batch = append(batch, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating exhausted messages: %w", err)
	}
	return batch, nil
}

// reclaimOrphanTopicMessages deletes pub/sub messages that can never be
// delivered: a message published to a topic that had zero subscribers at
// publish time gets no subscription rows (subscription rows are created
// atomically with the message), so it would otherwise occupy storage forever.
//
// Messages whose subscriptions were all moved to the DLQ are NOT orphans — the
// DLQ entry still references the message for a possible replay — so the delete
// also excludes any message referenced by a DLQ row (FR-027).
func (gc *GarbageCollector) reclaimOrphanTopicMessages(
	ctx context.Context,
	tableName string,
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		DELETE FROM %s m
		WHERE NOT EXISTS (
			SELECT 1 FROM %s s WHERE s.message_id = m.id
		)
		AND NOT EXISTS (
			SELECT 1 FROM %s d WHERE d.original_message_id = m.id
		)
	`, gc.pq.msgTable(tableName), gc.pq.subTable(tableName), gc.pq.dlqTable(tableName))

	if _, err := gc.pq.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to delete orphan topic messages: %w", err)
	}
	return nil
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
	var query string
	if queueType == QueueTypePubSub {
		query = fmt.Sprintf(`
			DELETE FROM %s m
			WHERE m.created_at < NOW() - make_interval(secs => $1)
			AND NOT EXISTS (
				SELECT 1 FROM %s s
				WHERE s.message_id = m.id AND s.status != '%s'
			)
		`, gc.pq.msgTable(tableName), gc.pq.subTable(tableName), MessageStatusAcked)
	} else {
		query = fmt.Sprintf(`
			DELETE FROM %s
			WHERE status = '%s'
			AND processed_at < NOW() - make_interval(secs => $1)
		`, gc.pq.msgTable(tableName), MessageStatusCompleted)
	}

	result, err := gc.pq.db.ExecContext(ctx, query, ttl.Seconds())
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
	var query string
	if queueType == QueueTypePubSub {
		query = fmt.Sprintf(`
			DELETE FROM %s m
			WHERE m.created_at < NOW() - make_interval(secs => $1)
			AND EXISTS (
				SELECT 1 FROM %s s
				WHERE s.message_id = m.id AND s.status = '%s'
			)
		`, gc.pq.msgTable(tableName), gc.pq.subTable(tableName), MessageStatusPending)
	} else {
		query = fmt.Sprintf(`
			DELETE FROM %s
			WHERE status = '%s'
			AND created_at < NOW() - make_interval(secs => $1)
		`, gc.pq.msgTable(tableName), MessageStatusPending)
	}

	result, err := gc.pq.db.ExecContext(ctx, query, maxAge.Seconds())
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
		DELETE FROM %s
		WHERE moved_at < NOW() - make_interval(secs => $1)
	`, gc.pq.dlqTable(tableName))

	result, err := gc.pq.db.ExecContext(ctx, query, retention.Seconds())
	if err != nil {
		return fmt.Errorf("failed to purge DLQ messages: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows > 0 {
		gc.pq.logInfo("purged DLQ messages", "count", rows, "table", tableName)
	}

	return nil
}

// resetTimedOutMessages resets messages with expired visibility timeouts back to
// pending. The GC intentionally does NOT increment retry_count: the consume-path
// reclaim (fetchPendingChannelMessage / reclaimChannelAttempt) is the sole place
// that increments retry_count. The GC only flips status→pending, clears claim_id
// and visibility_timeout so the message becomes available again. FOR UPDATE SKIP
// LOCKED prevents racing with a consumer that is mid-reclaim on the same row.
func (gc *GarbageCollector) resetTimedOutMessages(
	ctx context.Context,
	tableName string,
) error {
	msgTbl := gc.pq.msgTable(tableName)
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		UPDATE %s
		SET status = '%s',
		    claim_id = NULL,
		    visibility_timeout = NULL
		WHERE id IN (
			SELECT id FROM %s
			WHERE status = '%s'
			  AND visibility_timeout IS NOT NULL
			  AND visibility_timeout < NOW()
			FOR UPDATE SKIP LOCKED
		)
	`, msgTbl, MessageStatusPending, msgTbl, MessageStatusProcessing)

	result, err := gc.pq.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to reset timed-out messages: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows > 0 {
		gc.pq.logInfo("reset timed-out messages", "count", rows, "table", tableName)
	}

	return nil
}

// resetTimedOutSubscriptions resets subscriptions with expired visibility
// timeouts back to pending. Like resetTimedOutMessages, the GC does NOT
// increment retry_count — the consume-path reclaim is the sole incrementer.
// FOR UPDATE SKIP LOCKED prevents racing with a concurrent consumer.
func (gc *GarbageCollector) resetTimedOutSubscriptions(
	ctx context.Context,
	tableName string,
) error {
	subTbl := gc.pq.subTable(tableName)
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		UPDATE %s
		SET status = '%s',
		    claim_id = NULL,
		    visibility_timeout = NULL
		WHERE id IN (
			SELECT id FROM %s
			WHERE status = '%s'
			  AND visibility_timeout IS NOT NULL
			  AND visibility_timeout < NOW()
			FOR UPDATE SKIP LOCKED
		)
	`, subTbl, MessageStatusPending, subTbl, MessageStatusProcessing)

	result, err := gc.pq.db.ExecContext(ctx, query)
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

// purgeInactiveSubscriptions deletes leftover subscription rows belonging to
// subscribers that have unsubscribed (pgqueue_subscribers.active = FALSE).
//
// Unsubscribe is intentionally soft — it lets an in-flight message be drained —
// so it leaves these rows behind. Without this reaping step an abandoned
// subscriber's un-acked rows would pin their parent messages forever, because
// purgeCompletedMessages only deletes a message once every subscription row for
// it is acked. A re-subscribe flips the subscriber back to active before the
// next GC pass, sparing its rows.
func (gc *GarbageCollector) purgeInactiveSubscriptions(
	ctx context.Context,
	queueName, tableName string,
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE subscriber_id IN (
			SELECT subscriber_id FROM %s
			WHERE topic_name = $1 AND active = FALSE
		)
	`, gc.pq.subTable(tableName), gc.pq.globalTable("pgqueue_subscribers"))

	result, err := gc.pq.db.ExecContext(ctx, query, queueName)
	if err != nil {
		return fmt.Errorf("failed to purge inactive subscriptions: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows > 0 {
		gc.pq.logInfo("purged inactive subscriptions", "count", rows, "table", tableName)
	}

	return nil
}
