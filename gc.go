package pgqueue

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"runtime/debug"
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

	defaultCompletedMessageTTL = 24 * time.Hour      // retain completed messages for a day
	defaultDLQRetention        = 30 * 24 * time.Hour // retain DLQ entries for a month
)

// defaultRetentionPolicy is substituted for an all-zero DefaultPolicy by
// NewGarbageCollector, so a GarbageCollector created without a policy still
// bounds table growth (issue #47). MaxPendingAge is deliberately left unbounded:
// pending messages are live, unprocessed data, and purging them by default
// would silently lose messages whenever a consumer is down.
var defaultRetentionPolicy = RetentionPolicy{
	CompletedMessageTTL: defaultCompletedMessageTTL,
	MaxPendingAge:       0,
	DLQRetention:        defaultDLQRetention,
}

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

// NewGarbageCollector creates a new garbage collector instance, returning an
// error for an out-of-range configuration (consistent with New rather than
// silently clamping a mistake).
//
// Defaults and validation:
//   - Interval: zero means "unset" and uses the default (5m); a negative value
//     is an error (it would otherwise crash time.NewTicker in the background
//     goroutine).
//   - MaxWorkers: zero means "unset" and uses the default (10); a value outside
//     [1, 100] is an error (an out-of-range worker count was previously clamped).
//   - RetentionPolicy durations (DefaultPolicy and every Policies entry): each
//     field must be >= 0 or exactly KeepForever (-1); any other negative value
//     is an error.
//   - An all-zero DefaultPolicy is treated as unconfigured and replaced with
//     default retention (CompletedMessageTTL 24h, DLQRetention 30d; MaxPendingAge
//     stays unbounded) so the GC bounds table growth out of the box (issue #47).
//     A DefaultPolicy with any field set — including a KeepForever field — is
//     honored verbatim; set fields to KeepForever to retain everything.
//
// The GC back-registers on the Queue so Queue.Close stops it automatically. A
// GC created after the Queue is already closed is inert: it is not registered
// and Start is a no-op.
func NewGarbageCollector(
	pq *Queue,
	config GarbageCollectorConfig,
) (*GarbageCollector, error) {
	config, err := normalizeGCConfig(config)
	if err != nil {
		return nil, err
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
	return gc, nil
}

// normalizeGCConfig validates a GarbageCollectorConfig and fills in defaults for
// unset (zero) fields. It rejects values that are outside their documented range
// rather than silently clamping them (D7/M13). Zero Interval / MaxWorkers are the
// "unset" sentinels and are replaced with defaults; only genuinely invalid values
// (negative interval, worker count outside [1,100], a retention duration that is
// negative but not KeepForever) return an error.
func normalizeGCConfig(config GarbageCollectorConfig) (GarbageCollectorConfig, error) {
	if err := validateGCConfig(config); err != nil {
		return config, err
	}

	// Apply defaults for the unset (zero) sentinels. Validation above has already
	// rejected genuinely-invalid values, so an all-zero DefaultPolicy here is the
	// "unconfigured" case and is replaced with default retention (issue #47).
	if config.Interval == 0 {
		config.Interval = defaultGCInterval
	}
	if config.MaxWorkers == 0 {
		config.MaxWorkers = defaultGCMaxWorkers
	}
	if config.DefaultPolicy == (RetentionPolicy{}) {
		config.DefaultPolicy = defaultRetentionPolicy
	}
	if config.Policies == nil {
		config.Policies = make(map[string]RetentionPolicy)
	}
	return config, nil
}

// validateGCConfig rejects out-of-range configuration without mutating it.
func validateGCConfig(config GarbageCollectorConfig) error {
	if config.Interval < 0 {
		return fmt.Errorf(
			"%w: GarbageCollectorConfig.Interval must be positive (zero uses the %v default), got %v",
			ErrInvalidConfig, defaultGCInterval, config.Interval)
	}
	if config.MaxWorkers < 0 || config.MaxWorkers > maxGCMaxWorkers {
		return fmt.Errorf(
			"%w: GarbageCollectorConfig.MaxWorkers must be between 1 and %d (zero uses the %d default), got %d",
			ErrInvalidConfig, maxGCMaxWorkers, defaultGCMaxWorkers, config.MaxWorkers)
	}
	if err := validateRetentionPolicy("DefaultPolicy", config.DefaultPolicy); err != nil {
		return err
	}
	for name, policy := range config.Policies {
		if err := validateRetentionPolicy(fmt.Sprintf("Policies[%q]", name), policy); err != nil {
			return err
		}
	}
	return nil
}

// validateRetentionPolicy rejects any retention duration that is negative but not
// exactly KeepForever (-1). A 0 field means "keep forever" for the rows it governs
// and is valid; KeepForever is the only permitted negative. field names the config
// location for the error message.
func validateRetentionPolicy(field string, policy RetentionPolicy) error {
	durations := []struct {
		name string
		val  time.Duration
	}{
		{"CompletedMessageTTL", policy.CompletedMessageTTL},
		{"MaxPendingAge", policy.MaxPendingAge},
		{"DLQRetention", policy.DLQRetention},
	}
	for _, d := range durations {
		if d.val < 0 && d.val != KeepForever {
			return fmt.Errorf(
				"%w: GarbageCollectorConfig.%s.%s must be >= 0 or exactly KeepForever, got %v",
				ErrInvalidConfig, field, d.name, d.val)
		}
	}
	return nil
}

// Start begins the garbage collection loop in a background goroutine.
// Safe to call multiple times; only the first call starts the loop.
// Stop() can be called at any time after Start() returns.
// A GarbageCollector cannot be restarted after Stop() or context cancellation;
// create a new instance via NewGarbageCollector instead.
func (gc *GarbageCollector) Start(ctx context.Context) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	// Refuse to start once the owning Queue is closed: Close has already joined
	// its background goroutines, so a loop started now would never be stopped.
	if gc.pq.closed.Load() {
		return
	}
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

// PurgeQueue immediately and irreversibly deletes every message from a queue,
// leaving the queue and its tables in place.
func (gc *GarbageCollector) PurgeQueue(
	ctx context.Context,
	queueName string,
	queueType QueueType,
) error {
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

// maxAccumulatedCollectErrs caps the per-queue errors recorded by a single
// Collect pass. Under a sustained database outage every queue's worker fails;
// without this cap, the joined error grows to len(allQueues) on every tick
// and the GC ticker keeps spewing megabyte-sized logs (#62). The cap+1th
// entry is a single truncation sentinel so the operator can tell errors were
// dropped.
const maxAccumulatedCollectErrs = 100

// errCollectErrsTruncated is the static sentinel appended in place of the
// (cap+1)th per-queue error in a single Collect pass (#62).
var errCollectErrsTruncated = errors.New(
	"(further per-queue errors truncated; check logs for the rest)",
)

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
		[]queueMetadata, 0, len(topics)+len(channels),
	)
	allQueues = append(allQueues, topics...)
	allQueues = append(allQueues, channels...)

	return errors.Join(gc.dispatchCollect(ctx, allQueues)...)
}

// dispatchCollect fans queues out to a bounded worker pool and aggregates
// per-queue errors. It is the body of Collect, factored out to keep that
// function under the funlen ceiling.
func (gc *GarbageCollector) dispatchCollect(
	ctx context.Context,
	queues []queueMetadata,
) []error {
	// dispatchCtx lets a worker short-circuit the dispatch loop when it sees
	// a sentinel that means the pool is dead (sql.ErrConnDone, driver bad
	// connection). In-flight workers keep running against the outer ctx for
	// correctness; we just stop queueing new ones (#62).
	dispatchCtx, cancelDispatch := context.WithCancel(ctx)
	defer cancelDispatch()

	var wg sync.WaitGroup
	sem := make(chan struct{}, gc.config.MaxWorkers)

	var mu sync.Mutex
	var errs []error
	recordErr := func(queueName string, qerr error) {
		mu.Lock()
		defer mu.Unlock()
		errs = appendCappedErr(errs, queueName, qerr)
	}

	for _, queue := range queues {
		select {
		case <-dispatchCtx.Done():
			wg.Wait()
			// Preserve errors from workers that already ran so they are
			// not silently lost on cancellation. If the dispatch was
			// short-circuited by a pool-dead sentinel (not the outer
			// ctx) ctx.Err is still nil — skip the cancellation note in
			// that case.
			mu.Lock()
			cancelErrs := errs
			if outerErr := ctx.Err(); outerErr != nil {
				//nolint:gocritic // intentional separate slice so the append
				// does not alias errs.
				cancelErrs = append(errs,
					fmt.Errorf("garbage collection cancelled: %w", outerErr))
			}
			mu.Unlock()
			return cancelErrs
		case sem <- struct{}{}:
			wg.Add(1)
			go func(q queueMetadata) {
				defer wg.Done()
				gc.collectQueueWorker(ctx, q, sem, recordErr, cancelDispatch)
			}(queue)
		}
	}

	wg.Wait()
	// The outer ctx may have been cancelled while workers ran even though every
	// queue still got dispatched (the loop never took the dispatchCtx.Done()
	// branch). Annotate the cancellation here too so both exit paths report it
	// consistently. wg.Wait() established happens-before for every worker's writes
	// to errs, so reading it without mu is safe.
	if outerErr := ctx.Err(); outerErr != nil {
		return append(errs,
			fmt.Errorf("garbage collection cancelled: %w", outerErr))
	}
	return errs
}

// collectQueueWorker runs one queue's collection inside a dispatchCollect worker
// goroutine. It recovers panics so a bug in collection cannot crash the host
// process (the recovered panic is surfaced through recordErr like any other
// per-queue error), releases its semaphore slot on return, and short-circuits
// the dispatch loop via cancelDispatch when it detects a dead pool. The caller
// owns wg.Done.
func (gc *GarbageCollector) collectQueueWorker(
	ctx context.Context,
	q queueMetadata,
	sem <-chan struct{},
	recordErr func(string, error),
	cancelDispatch context.CancelFunc,
) {
	defer func() { <-sem }()
	defer func() {
		if err := gc.recoverPanic(recover(), "collectQueue["+q.QueueName+"]"); err != nil {
			recordErr(q.QueueName, err)
		}
	}()

	start := time.Now()
	if err := gc.collectQueue(ctx, q); err != nil {
		gc.pq.logError("failed to collect queue",
			"queue", q.QueueName, "error", err,
			"duration", time.Since(start),
		)
		recordErr(q.QueueName, err)
		if isPoolDead(err) {
			cancelDispatch()
		}
	} else if d := time.Since(start); d > gcSlowCollectThreshold {
		gc.pq.logInfo("collected queue",
			"queue", q.QueueName, "duration", d,
		)
	}
}

// isPoolDead reports whether err signals an unrecoverable connection-pool
// state — there is no value in queueing more workers against a dead pool,
// they will all fail with the same error and bloat the joined result.
func isPoolDead(err error) bool {
	return errors.Is(err, sql.ErrConnDone) || errors.Is(err, driver.ErrBadConn)
}

// appendCappedErr adds qerr to errs as "queue <name>: <qerr>" until the slice
// reaches maxAccumulatedCollectErrs, then once appends a single truncation
// sentinel and ignores every subsequent call. Split out for direct unit
// testing (#62).
func appendCappedErr(errs []error, queueName string, qerr error) []error {
	switch {
	case len(errs) < maxAccumulatedCollectErrs:
		return append(errs, fmt.Errorf("queue %s: %w", queueName, qerr))
	case len(errs) == maxAccumulatedCollectErrs:
		return append(errs, errCollectErrsTruncated)
	default:
		return errs
	}
}

// errGCPanic wraps a value recovered from a panicking GC background goroutine.
var errGCPanic = errors.New("garbage collector panic")

// recoverPanic turns a value recovered from a GC background goroutine into a
// logged error so a bug in an unattended maintenance task cannot crash the host
// process, mirroring the callHandler/safeHook/pump precedent. Pass the result of
// recover(); it returns nil when there was no panic, otherwise an error wrapping
// errGCPanic.
func (gc *GarbageCollector) recoverPanic(r any, where string) error {
	if r == nil {
		return nil
	}
	gc.pq.logError("recovered panic in garbage collector",
		"where", where,
		"panic", r,
		"stack", string(debug.Stack()),
	)
	return fmt.Errorf("%w: %s: %v", errGCPanic, where, r)
}

func (gc *GarbageCollector) run(ctx context.Context) {
	defer gc.wg.Done()
	defer func() { _ = gc.recoverPanic(recover(), "run") }()

	// Derive a context that is also cancelled when Stop closes stopChan, and
	// run Collect under it. Without this, Stop (and Queue.Close, which joins
	// the GC) would block until an in-progress Collect over a large backlog
	// finished on its own — Collect's queue workers watch only their context.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go gc.cancelOnStop(runCtx, cancel)

	ticker := time.NewTicker(gc.config.Interval)
	defer ticker.Stop()

	// Run an initial pass immediately so cleanup and timed-out-message recovery
	// do not wait a full interval after Start.
	if gc.stopRequested(runCtx) {
		return
	}
	gc.collectOnce(runCtx)
	gc.sweepMetadataCache()

	for {
		select {
		case <-runCtx.Done():
			return
		case <-gc.stopChan:
			return
		case <-ticker.C:
			gc.collectOnce(runCtx)
			gc.sweepMetadataCache()
		}
	}
}

// sweepMetadataCache drops expired entries from the table-name cache. Piggybacks
// on the GC ticker so a queue deleted by another process cannot pin a stale
// mapping in this process indefinitely (issue #54).
func (gc *GarbageCollector) sweepMetadataCache() {
	if gc.pq.mdcache != nil {
		gc.pq.mdcache.sweep()
	}
}

// cancelOnStop cancels the GC's run context when Stop closes stopChan, so an
// in-progress Collect winds down promptly. It exits when the run context is
// done (run returning cancels it via defer).
func (gc *GarbageCollector) cancelOnStop(runCtx context.Context, cancel context.CancelFunc) {
	select {
	case <-gc.stopChan:
		cancel()
	case <-runCtx.Done():
	}
}

// stopRequested reports whether the GC has been asked to wind down, either via
// Stop (stopChan) or context cancellation.
func (gc *GarbageCollector) stopRequested(runCtx context.Context) bool {
	select {
	case <-runCtx.Done():
		return true
	case <-gc.stopChan:
		return true
	default:
		return false
	}
}

// collectOnce runs a single Collect pass, logging any error.
func (gc *GarbageCollector) collectOnce(ctx context.Context) {
	if err := gc.Collect(ctx); err != nil {
		gc.pq.logError("garbage collection error", "error", err)
	}
}

func (gc *GarbageCollector) executePurge(
	ctx context.Context,
	tableName string,
	queueType QueueType,
) error {
	return gc.pq.withTx(ctx, func(tx *sql.Tx) error {
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

		return nil
	})
}

// collectQueue performs garbage collection for a single queue and emits a
// RecordGCRun metric with the duration, reclaimed row count, purged row count,
// and any error.
func (gc *GarbageCollector) collectQueue(
	ctx context.Context,
	queue queueMetadata,
) error {
	start := time.Now()
	purged, reclaimed, err := gc.doCollectQueue(ctx, queue)
	gc.pq.recordGCRun(ctx, queue.QueueName, time.Since(start), reclaimed, purged, err)
	return err
}

// doCollectQueue is the body of collectQueue. It returns the total rows
// deleted by the retention policy (purged) and the total timed-out messages
// reclaimed — reset to pending by resetTimedOutEntries plus those moved to the
// DLQ by the promote* passes (#11) — plus any error. Factored out so
// collectQueue can record the metric unconditionally, even on error.
func (gc *GarbageCollector) doCollectQueue(
	ctx context.Context,
	queue queueMetadata,
) (int64, int64, error) {
	policy := gc.getPolicy(queue.QueueName)

	purged, retErr := gc.applyRetentionPolicy(ctx, queue, policy)
	if retErr != nil {
		return purged, 0, fmt.Errorf("failed to apply retention policy: %w", retErr)
	}

	// promoted accumulates rows dead-lettered by the promote* passes below.
	// RecordGCRun documents reclaimed as rows "reset to pending or moved to the
	// DLQ", so these promotions count toward the reclaimed total alongside the
	// resetTimedOutEntries reset count (#11).
	var promoted int64

	if queue.QueueType == QueueTypePubSub {
		// Both purges delete rows for pub/sub topics; fold their counts into purged
		// so the GC metric reflects every deleted row, not just retention-policy
		// deletions (mirrors how the promote* passes feed promoted).
		inactivePurged, err := gc.purgeInactiveSubscriptions(
			ctx, queue.QueueName, queue.TableName,
		)
		purged += inactivePurged
		if err != nil {
			return purged, 0, fmt.Errorf("failed to purge inactive subscriptions: %w", err)
		}
		orphanPurged, err := gc.reclaimOrphanTopicMessages(ctx, queue.TableName)
		purged += orphanPurged
		if err != nil {
			return purged, 0, fmt.Errorf("failed to reclaim orphan topic messages: %w", err)
		}
		// Promote timed-out-and-exhausted subscriptions to the DLQ before
		// resetTimedOutEntries runs, so a still-exhausted subscription is
		// dead-lettered rather than reset back to pending.
		n, err := gc.promoteExhaustedTopicSubscriptions(ctx, queue)
		if err != nil {
			return purged, n, fmt.Errorf("failed to promote exhausted subscriptions: %w", err)
		}
		promoted += n
	}

	// Promote timed-out-and-exhausted channel messages to the DLQ. This is the
	// backstop for exhausted rows that no consumer reaches: the consume path
	// (reclaimChannelAttempt, channel.go) already dead-letters an exhausted row
	// inline when a consumer hits it, but a backlog nobody polls needs the GC.
	// Run it before resetTimedOutEntries so a still-exhausted message is
	// dead-lettered rather than reset back to pending.
	if queue.QueueType == QueueTypeChannel {
		n, err := gc.promoteExhaustedChannelMessages(ctx, queue.TableName)
		if err != nil {
			return purged, n, fmt.Errorf("failed to promote exhausted messages: %w", err)
		}
		promoted += n
	}

	reclaimed, resetErr := gc.resetTimedOutEntries(ctx, queue)
	if resetErr != nil {
		return purged, promoted + reclaimed, resetErr
	}
	return purged, promoted + reclaimed, nil
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
) (int64, error) {
	defaultMax := gc.pq.cfg.defaultMaxRetries
	selectQuery := fmt.Sprintf(`
		SELECT id, payload, retry_count, metadata
		FROM %s
		WHERE status = '%s'
		  AND visibility_timeout IS NOT NULL
		  AND visibility_timeout < NOW()
		  AND retry_count >= COALESCE(max_retries, $1)
		ORDER BY id
		LIMIT %d
		FOR UPDATE SKIP LOCKED
	`, gc.pq.msgTable(tableName), MessageStatusProcessing, promoteExhaustedChannelMessagesPageSize)

	var total int64
	for {
		promoted, err := gc.promoteExhaustedChannelPage(ctx, tableName, selectQuery, defaultMax)
		if err != nil {
			return total, err
		}
		total += int64(promoted)
		if promoted < promoteExhaustedChannelMessagesPageSize {
			return total, nil // backlog exhausted
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
	var batch []exhaustedChannelMessage
	if err := gc.pq.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		batch, err = scanExhaustedChannelMessages(ctx, tx, selectQuery, defaultMax)
		if err != nil {
			return err
		}

		for _, e := range batch {
			if err := gc.pq.moveToDLQ(
				ctx, tx, tableName, e.id, errReasonVisibilityTimeout,
				e.payload, e.retryCount+1, e.metadataJSON,
			); err != nil {
				return fmt.Errorf("failed to move exhausted message to DLQ: %w", err)
			}
		}

		return nil
	}); err != nil {
		return 0, err
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

// promoteExhaustedTopicSubscriptionsPageSize bounds the subscription rows
// promoted to the DLQ per transaction, keeping the GC pass's footprint bounded
// on a pathological backlog.
const promoteExhaustedTopicSubscriptionsPageSize = 100

// promoteExhaustedTopicSubscriptions moves pub/sub subscription rows that have
// timed out in 'processing' state and exhausted their retries to the
// dead-letter queue. The consume path (reclaimTopicAttempt) already does this
// for any subscriber actively consuming; this GC pass is the backstop for a
// subscriber that has gone idle, so its exhausted rows are dead-lettered
// rather than reset to 'pending' indefinitely by resetTimedOutSubscriptions.
//
// retry_count >= effective max is exactly the condition under which a further
// reclaim would breach the retryCount+1 > maxRetries guard — the same test
// reclaimTopicAttempt applies. Work is done in bounded pages — moveSubToDLQ
// deletes each promoted row, so a fresh SELECT never re-sees it.
func (gc *GarbageCollector) promoteExhaustedTopicSubscriptions(
	ctx context.Context,
	queue queueMetadata,
) (int64, error) {
	maxRetries := gc.pq.resolveMaxRetries(&queue)
	tableName := queue.TableName
	selectQuery := fmt.Sprintf(`
		SELECT s.message_id, s.subscriber_id, s.retry_count, m.payload, m.metadata
		FROM %s s
		JOIN %s m ON s.message_id = m.id
		WHERE s.status = '%s'
		  AND s.visibility_timeout IS NOT NULL
		  AND s.visibility_timeout < NOW()
		  AND s.retry_count >= $1
		ORDER BY s.id
		LIMIT %d
		FOR UPDATE OF s SKIP LOCKED
	`, gc.pq.subTable(tableName), gc.pq.msgTable(tableName),
		MessageStatusProcessing, promoteExhaustedTopicSubscriptionsPageSize)

	var total int64
	for {
		promoted, err := gc.promoteExhaustedTopicPage(ctx, tableName, selectQuery, maxRetries)
		if err != nil {
			return total, err
		}
		total += int64(promoted)
		if promoted < promoteExhaustedTopicSubscriptionsPageSize {
			return total, nil // backlog exhausted
		}
	}
}

// exhaustedTopicSubscription is one timed-out, retry-exhausted subscription row
// awaiting promotion to the dead-letter queue.
type exhaustedTopicSubscription struct {
	messageID    uuid.UUID
	subscriberID string
	retryCount   int
	payload      []byte
	metadataJSON sql.NullString
}

// promoteExhaustedTopicPage promotes one page of exhausted subscriptions to the
// DLQ in a single transaction and returns how many it moved.
func (gc *GarbageCollector) promoteExhaustedTopicPage(
	ctx context.Context,
	tableName, selectQuery string,
	maxRetries int,
) (int, error) {
	var batch []exhaustedTopicSubscription
	if err := gc.pq.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		batch, err = scanExhaustedTopicSubscriptions(ctx, tx, selectQuery, maxRetries)
		if err != nil {
			return err
		}

		for _, e := range batch {
			// moveSubToDLQ counts the timeout reclaim itself (state.retryCount+1),
			// so the raw stored retry_count is passed — the same contract
			// reclaimTopicAttempt relies on.
			if err := gc.pq.moveSubToDLQ(
				ctx, tx, tableName, e.messageID, e.subscriberID, errReasonVisibilityTimeout,
				&subState{retryCount: e.retryCount, payload: e.payload, metadataJSON: e.metadataJSON},
			); err != nil {
				return fmt.Errorf("failed to move exhausted subscription to DLQ: %w", err)
			}
		}

		return nil
	}); err != nil {
		return 0, err
	}

	if len(batch) > 0 {
		gc.pq.logInfo("promoted exhausted subscriptions to DLQ",
			"count", len(batch), "table", tableName)
	}
	return len(batch), nil
}

// scanExhaustedTopicSubscriptions reads (and FOR UPDATE locks) one page of
// exhausted subscriptions. The rows are fully drained and closed before the
// caller runs any further statement on the transaction.
func scanExhaustedTopicSubscriptions(
	ctx context.Context,
	tx *sql.Tx,
	selectQuery string,
	maxRetries int,
) ([]exhaustedTopicSubscription, error) {
	rows, err := tx.QueryContext(ctx, selectQuery, maxRetries)
	if err != nil {
		return nil, fmt.Errorf("failed to query exhausted subscriptions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var batch []exhaustedTopicSubscription
	for rows.Next() {
		var e exhaustedTopicSubscription
		if err := rows.Scan(
			&e.messageID, &e.subscriberID, &e.retryCount,
			&e.payload, &e.metadataJSON,
		); err != nil {
			return nil, fmt.Errorf("failed to scan exhausted subscription: %w", err)
		}
		batch = append(batch, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating exhausted subscriptions: %w", err)
	}
	return batch, nil
}

// retentionPurgePageSize bounds each retention DELETE to a small lock window
// and WAL footprint (issue #50). The first GC pass after a retention policy is
// finally enabled on a long-running queue with millions of expired rows would
// otherwise issue one giant unbounded DELETE — long row-lock window, large WAL
// spike, replication lag, risk of statement timeout. Paginating bounds those
// costs per statement.
//
// Sized larger than promoteExhausted*PageSize because each page here is a
// single DELETE statement (no per-row processing like the promote pages, which
// also insert into the DLQ); the larger batch amortises round-trip overhead on
// huge backlogs while still keeping each statement's lock window and WAL spike
// bounded.
const retentionPurgePageSize = 1000

// runPagedPurge executes a paginated DELETE in a loop, breaking when a page
// returns fewer than retentionPurgePageSize rows. The caller passes a
// fully-formed query that uses LIMIT retentionPurgePageSize and FOR UPDATE …
// SKIP LOCKED in its inner SELECT. PostgreSQL releases the subquery's row
// locks at statement commit, so no surrounding transaction is required.
// Returns the total rows deleted across all pages.
func (gc *GarbageCollector) runPagedPurge(
	ctx context.Context,
	query string,
	args ...any,
) (int64, error) {
	var total int64
	for {
		result, err := gc.pq.db.ExecContext(ctx, query, args...)
		if err != nil {
			return total, err //nolint:wrapcheck // caller wraps with operation context
		}
		n, err := rowsAffectedOrErr(result)
		if err != nil {
			// A discarded error here used to make n=0, which terminates the
			// page loop on the first iteration and reports the purge as
			// successful while skipping the rest of the backlog (#67).
			return total, fmt.Errorf("paged purge rows affected: %w", err)
		}
		total += n
		if n < int64(retentionPurgePageSize) {
			return total, nil // backlog exhausted
		}
		if err := ctx.Err(); err != nil {
			return total, fmt.Errorf("retention purge cancelled: %w", err)
		}
	}
}

// reclaimOrphanTopicMessages deletes pub/sub messages that can never be
// delivered: a message published to a topic that had zero subscribers at
// publish time gets no subscription rows (subscription rows are created
// atomically with the message), so it would otherwise occupy storage forever.
//
// Messages whose subscriptions were all moved to the DLQ are NOT orphans — the
// DLQ entry still references the message for a possible replay — so the delete
// also excludes any message referenced by a DLQ row (FR-027).
//
// Work is paginated (issue #50): each iteration deletes at most
// retentionPurgePageSize rows by id, using FOR UPDATE OF m SKIP LOCKED in the
// subquery so concurrent workers never block each other. PostgreSQL holds the
// subquery's row locks only for the duration of the surrounding DELETE
// statement, so no explicit transaction is required around each page.
func (gc *GarbageCollector) reclaimOrphanTopicMessages(
	ctx context.Context,
	tableName string,
) (int64, error) {
	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE id IN (
			SELECT m.id FROM %s m
			WHERE NOT EXISTS (
				SELECT 1 FROM %s s WHERE s.message_id = m.id
			)
			AND NOT EXISTS (
				SELECT 1 FROM %s d WHERE d.original_message_id = m.id
			)
			ORDER BY m.id
			LIMIT %d
			FOR UPDATE OF m SKIP LOCKED
		)
	`, gc.pq.msgTable(tableName), gc.pq.msgTable(tableName),
		gc.pq.subTable(tableName), gc.pq.dlqTable(tableName),
		retentionPurgePageSize)

	total, err := gc.runPagedPurge(ctx, query)
	if err != nil {
		return total, fmt.Errorf("failed to delete orphan topic messages: %w", err)
	}
	if total > 0 {
		gc.pq.logInfo("reclaimed orphan topic messages", "count", total, "table", tableName)
	}
	return total, nil
}

// applyRetentionPolicy runs the TTL/age/DLQ purges for one queue and returns
// the total number of rows deleted across all three purge phases.
func (gc *GarbageCollector) applyRetentionPolicy(
	ctx context.Context,
	queue queueMetadata,
	policy RetentionPolicy,
) (int64, error) {
	queueType := queue.QueueType
	var total int64

	if policy.CompletedMessageTTL > 0 {
		n, err := gc.purgeCompletedMessages(
			ctx, queue.TableName, queueType, policy.CompletedMessageTTL,
		)
		total += n
		if err != nil {
			return total, fmt.Errorf("failed to purge completed messages: %w", err)
		}
	}

	if policy.MaxPendingAge > 0 {
		n, err := gc.purgeOldPendingMessages(
			ctx, queue.TableName, queueType, policy.MaxPendingAge,
		)
		total += n
		if err != nil {
			return total, fmt.Errorf("failed to purge old pending messages: %w", err)
		}
	}

	if policy.DLQRetention > 0 {
		n, err := gc.purgeDLQMessages(
			ctx, queue.TableName, policy.DLQRetention,
		)
		total += n
		if err != nil {
			return total, fmt.Errorf("failed to purge DLQ messages: %w", err)
		}
	}

	return total, nil
}

// resetTimedOutEntries resets timed-out messages/subscriptions for the queue
// and returns the total number of rows reset (the "reclaimed" count).
func (gc *GarbageCollector) resetTimedOutEntries(
	ctx context.Context,
	queue queueMetadata,
) (int64, error) {
	if queue.QueueType == QueueTypeChannel {
		n, err := gc.resetTimedOutMessages(ctx, queue.TableName)
		if err != nil {
			return n, fmt.Errorf("failed to reset timed-out messages: %w", err)
		}
		return n, nil
	}

	if queue.QueueType == QueueTypePubSub {
		n, err := gc.resetTimedOutSubscriptions(ctx, queue.TableName)
		if err != nil {
			return n, fmt.Errorf(
				"failed to reset timed-out subscriptions: %w", err,
			)
		}
		return n, nil
	}

	return 0, nil
}

// getPolicy returns the retention policy for a queue.
func (gc *GarbageCollector) getPolicy(queueName string) RetentionPolicy {
	if policy, exists := gc.config.Policies[queueName]; exists {
		return policy
	}
	return gc.config.DefaultPolicy
}

// purgeCompletedMessages deletes messages older than ttl whose work is finished
// and returns the total number of rows deleted.
//
// Work is paginated (issue #50): each iteration deletes at most
// retentionPurgePageSize rows by id, using FOR UPDATE SKIP LOCKED in the
// subquery so concurrent GC workers never block each other.
func (gc *GarbageCollector) purgeCompletedMessages(
	ctx context.Context,
	tableName string,
	queueType QueueType,
	ttl time.Duration,
) (int64, error) {
	var query string
	if queueType == QueueTypePubSub {
		// A message still referenced by a DLQ row is kept so the DLQ entry stays
		// replayable — pub/sub DLQ replay re-creates subscription rows that
		// foreign-key the message (FR-027). Without this guard a message whose
		// only non-acked subscriber was dead-lettered would be purged here,
		// silently orphaning its DLQ entry.
		query = fmt.Sprintf(`
			DELETE FROM %s
			WHERE id IN (
				SELECT m.id FROM %s m
				WHERE m.created_at < NOW() - make_interval(secs => $1)
				AND NOT EXISTS (
					SELECT 1 FROM %s s
					WHERE s.message_id = m.id AND s.status != '%s'
				)
				AND NOT EXISTS (
					SELECT 1 FROM %s d WHERE d.original_message_id = m.id
				)
				ORDER BY m.id
				LIMIT %d
				FOR UPDATE OF m SKIP LOCKED
			)
		`, gc.pq.msgTable(tableName), gc.pq.msgTable(tableName),
			gc.pq.subTable(tableName), MessageStatusAcked,
			gc.pq.dlqTable(tableName), retentionPurgePageSize)
	} else {
		query = fmt.Sprintf(`
			DELETE FROM %s
			WHERE id IN (
				SELECT id FROM %s
				WHERE status = '%s'
				AND processed_at < NOW() - make_interval(secs => $1)
				ORDER BY id
				LIMIT %d
				FOR UPDATE SKIP LOCKED
			)
		`, gc.pq.msgTable(tableName), gc.pq.msgTable(tableName),
			MessageStatusCompleted, retentionPurgePageSize)
	}

	total, err := gc.runPagedPurge(ctx, query, ttl.Seconds())
	if err != nil {
		return total, fmt.Errorf("failed to purge completed messages: %w", err)
	}
	if total > 0 {
		gc.pq.logInfo("purged completed messages", "count", total, "table", tableName)
	}
	return total, nil
}

// purgeOldPendingMessages drops deliveries that have been pending longer than
// maxAge.
//
// For channels the unit is the message, so the message row itself is deleted.
//
// For pub/sub the unit is the per-subscriber delivery: only the stale *pending
// subscription rows* are deleted, never the shared message row. A subscriber
// too slow to process a message within maxAge gives up on that one message;
// other subscribers — whether already acked, still processing, or simply less
// far behind — are untouched. The message row is left for reclaimOrphanTopicMessages
// (once every subscription is gone) or purgeCompletedMessages (once the rest are
// acked) to remove, so no DLQ guard is needed here: a subscription moved to the
// DLQ no longer has a row in the subscription table.
// Work is paginated (issue #50): each iteration deletes at most
// retentionPurgePageSize rows by id, using FOR UPDATE [OF s] SKIP LOCKED in the
// subquery so concurrent workers never block each other. The pub/sub branch
// paginates on the subscription-row id, the channel branch on the message-row
// id.
func (gc *GarbageCollector) purgeOldPendingMessages(
	ctx context.Context,
	tableName string,
	queueType QueueType,
	maxAge time.Duration,
) (int64, error) {
	var query string
	if queueType == QueueTypePubSub {
		// Delete only the stale pending subscription rows, joined to the message
		// for its publish-time cutoff. The message row and every other
		// subscriber's rows are deliberately left in place.
		query = fmt.Sprintf(`
			DELETE FROM %s
			WHERE id IN (
				SELECT s.id FROM %s s
				JOIN %s m ON s.message_id = m.id
				WHERE s.status = '%s'
				AND m.created_at < NOW() - make_interval(secs => $1)
				ORDER BY s.id
				LIMIT %d
				FOR UPDATE OF s SKIP LOCKED
			)
		`, gc.pq.subTable(tableName), gc.pq.subTable(tableName),
			gc.pq.msgTable(tableName), MessageStatusPending,
			retentionPurgePageSize)
	} else {
		query = fmt.Sprintf(`
			DELETE FROM %s
			WHERE id IN (
				SELECT id FROM %s
				WHERE status = '%s'
				AND created_at < NOW() - make_interval(secs => $1)
				ORDER BY id
				LIMIT %d
				FOR UPDATE SKIP LOCKED
			)
		`, gc.pq.msgTable(tableName), gc.pq.msgTable(tableName),
			MessageStatusPending, retentionPurgePageSize)
	}

	total, err := gc.runPagedPurge(ctx, query, maxAge.Seconds())
	if err != nil {
		return total, fmt.Errorf("failed to purge old pending messages: %w", err)
	}
	if total > 0 {
		gc.pq.logInfo("purged old pending messages", "count", total, "table", tableName)
	}
	return total, nil
}

// purgeDLQMessages deletes DLQ messages older than retention period.
//
// Work is paginated (issue #50): each iteration deletes at most
// retentionPurgePageSize rows by id, using FOR UPDATE SKIP LOCKED in the
// subquery so concurrent workers never block each other.
func (gc *GarbageCollector) purgeDLQMessages(
	ctx context.Context,
	tableName string,
	retention time.Duration,
) (int64, error) {
	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE id IN (
			SELECT id FROM %s
			WHERE moved_at < NOW() - make_interval(secs => $1)
			ORDER BY id
			LIMIT %d
			FOR UPDATE SKIP LOCKED
		)
	`, gc.pq.dlqTable(tableName), gc.pq.dlqTable(tableName),
		retentionPurgePageSize)

	total, err := gc.runPagedPurge(ctx, query, retention.Seconds())
	if err != nil {
		return total, fmt.Errorf("failed to purge DLQ messages: %w", err)
	}
	if total > 0 {
		gc.pq.logInfo("purged DLQ messages", "count", total, "table", tableName)
	}
	return total, nil
}

// resetTimedOutMessages resets messages with expired visibility timeouts back to
// pending and counts the timeout as one delivery attempt (retry_count + 1), so
// the visibility-timeout reclaim counts toward max_retries whether it is the GC
// or the consume-path reclaim (fetchPendingChannelMessage / reclaimChannelAttempt)
// that handles a given row. Each timed-out row is claimed by exactly one of the
// two (FOR UPDATE SKIP LOCKED plus the status transition), so the increment is
// applied exactly once. A row reset here becomes immediately available rather
// than backoff-delayed: unlike the inline reclaim (deferReclaimedChannelMessage,
// which pushes available_at by the backoff delay when a BackoffPolicy is set),
// this bulk UPDATE deliberately skips the per-row jittered backoff — it cannot
// be expressed as a set-based SQL expression, and the skipped delay is bounded
// by the multi-minute GC interval, which already exceeds the default backoff
// cap. Under active consumption the inline path wins the row first and applies
// the backoff; the GC path only runs when no consumer has polled for a full GC
// interval. Returns the number of rows updated.
func (gc *GarbageCollector) resetTimedOutMessages(
	ctx context.Context,
	tableName string,
) (int64, error) {
	msgTbl := gc.pq.msgTable(tableName)
	// The table name comes from a queueNameRegex-validated queue name, so this
	// interpolation is injection-safe. Paginated (M3): each page resets at most
	// retentionPurgePageSize timed-out rows via ORDER BY visibility_timeout, id
	// LIMIT + FOR UPDATE SKIP LOCKED, bounding the lock window and WAL footprint of
	// a huge crash-reclaim backlog per statement while concurrent workers never
	// block each other. Ordering by (visibility_timeout, id) matches the
	// _consumable_timeout index's leading range key (#11), so still-in-flight rows
	// (visibility_timeout > NOW()) are never scanned. A reset row flips to
	// 'pending' and no longer matches the status='processing' predicate, so a fresh
	// page never re-sees it — the same paging invariant runPagedPurge relies on
	// (and it propagates a RowsAffected error rather than silently reporting
	// nothing reclaimed, #67).
	query := fmt.Sprintf(`
		UPDATE %s
		SET status = '%s',
		    claim_id = NULL,
		    visibility_timeout = NULL,
		    retry_count = retry_count + 1
		WHERE id IN (
			SELECT id FROM %s
			WHERE status = '%s'
			  AND visibility_timeout IS NOT NULL
			  AND visibility_timeout < NOW()
			ORDER BY visibility_timeout, id
			LIMIT %d
			FOR UPDATE SKIP LOCKED
		)
	`, msgTbl, MessageStatusPending, msgTbl, MessageStatusProcessing, retentionPurgePageSize)

	total, err := gc.runPagedPurge(ctx, query)
	if err != nil {
		return total, fmt.Errorf("failed to reset timed-out messages: %w", err)
	}
	if total > 0 {
		gc.pq.logInfo("reset timed-out messages", "count", total, "table", tableName)
	}

	return total, nil
}

// resetTimedOutSubscriptions resets subscriptions with expired visibility
// timeouts back to pending and counts the timeout as one delivery attempt
// (retry_count + 1) — see resetTimedOutMessages for why the increment is applied
// exactly once. FOR UPDATE SKIP LOCKED prevents racing with a concurrent consumer.
// resetTimedOutSubscriptions resets timed-out pub/sub subscriptions back to
// pending and returns the number of rows updated.
func (gc *GarbageCollector) resetTimedOutSubscriptions(
	ctx context.Context,
	tableName string,
) (int64, error) {
	subTbl := gc.pq.subTable(tableName)
	// The table name comes from a queueNameRegex-validated queue name, so this
	// interpolation is injection-safe. Paginated (M3) on the subscription-row id,
	// mirroring resetTimedOutMessages: each page resets at most
	// retentionPurgePageSize rows via ORDER BY id LIMIT + FOR UPDATE SKIP LOCKED,
	// bounding the lock window on a huge backlog. A reset row flips to 'pending'
	// so a fresh page never re-sees it, and runPagedPurge propagates a
	// RowsAffected error rather than silently under-reporting (#67).
	query := fmt.Sprintf(`
		UPDATE %s
		SET status = '%s',
		    claim_id = NULL,
		    visibility_timeout = NULL,
		    retry_count = retry_count + 1
		WHERE id IN (
			SELECT id FROM %s
			WHERE status = '%s'
			  AND visibility_timeout IS NOT NULL
			  AND visibility_timeout < NOW()
			ORDER BY id
			LIMIT %d
			FOR UPDATE SKIP LOCKED
		)
	`, subTbl, MessageStatusPending, subTbl, MessageStatusProcessing, retentionPurgePageSize)

	total, err := gc.runPagedPurge(ctx, query)
	if err != nil {
		return total, fmt.Errorf(
			"failed to reset timed-out subscriptions: %w", err,
		)
	}
	if total > 0 {
		gc.pq.logInfo("reset timed-out subscriptions", "count", total, "table", tableName)
	}

	return total, nil
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
//
// Rows still being processed within a live claim window (status = 'processing'
// AND visibility_timeout in the future) are preserved: that is exactly the
// in-flight message a soft Unsubscribe promises to let drain. Deleting it would
// destroy the row out from under a consumer that is still working on it — its
// later Ack/Nack would match nothing and the message would be silently lost
// (no DLQ, no retry). Such a row is reaped on a later pass once it is acked or
// its claim expires (after which resetTimedOutSubscriptions / the DLQ-promotion
// step take over).
func (gc *GarbageCollector) purgeInactiveSubscriptions(
	ctx context.Context,
	queueName, tableName string,
) (int64, error) {
	// Paginated like reclaimOrphanTopicMessages (#3): select at most
	// retentionPurgePageSize matching sub rows by id, lock them with FOR UPDATE
	// OF s SKIP LOCKED so concurrent workers never block each other, and delete
	// by those ids. The WHERE predicate is preserved exactly — only rows owned by
	// an inactive subscriber, never a live-claim processing row whose
	// visibility_timeout is still in the future.
	//
	// Table name derived from a queueNameRegex-validated queue name; injection-safe.
	subTbl := gc.pq.subTable(tableName)
	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE id IN (
			SELECT s.id FROM %s s
			WHERE s.subscriber_id IN (
				SELECT subscriber_id FROM %s
				WHERE topic_name = $1 AND active = FALSE
			)
			AND NOT (s.status = '%s' AND s.visibility_timeout > NOW())
			ORDER BY s.id
			LIMIT %d
			FOR UPDATE OF s SKIP LOCKED
		)
	`, subTbl, subTbl, gc.pq.globalTable("pgqueue_subscribers"),
		MessageStatusProcessing, retentionPurgePageSize)

	total, err := gc.runPagedPurge(ctx, query, queueName)
	if err != nil {
		return total, fmt.Errorf("failed to purge inactive subscriptions: %w", err)
	}
	if total > 0 {
		gc.pq.logInfo("purged inactive subscriptions", "count", total, "table", tableName)
	}

	return total, nil
}
