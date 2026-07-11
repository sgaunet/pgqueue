package pgqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
)

// ReplayFrom resets messages after a specific timestamp to pending status.
func (pq *Queue) ReplayFrom(
	ctx context.Context,
	queueName string,
	queueType QueueType,
	since time.Time,
	opts ReplayOptions,
) (int64, error) {
	if err := pq.checkClosed(); err != nil {
		return 0, err
	}

	ctx, span := pq.startSpan(ctx, "pgqueue.replay",
		StringAttr("queue", queueName), StringAttr("replay_type", "timestamp"))
	defer span.End()

	if err := validateReplayOpts(opts); err != nil {
		span.SetError(err)
		return 0, fmt.Errorf("failed to validate replay options: %w", err)
	}

	metadata, err := pq.getReplayQueueMetadata(ctx, queueType, queueName)
	if err != nil {
		span.SetError(err)
		return 0, fmt.Errorf("failed to get queue metadata for replay: %w", err)
	}

	tableName := metadata.TableName

	if opts.DryRun {
		count, err := pq.countReplayableMessages(ctx, tableName, queueType, since)
		if err != nil {
			return 0, err
		}
		// A real run with a limit caps the rows it touches; reflect that here
		// so the dry-run count matches what a subsequent replay would do.
		if opts.Limit > 0 && count > opts.Limit {
			count = opts.Limit
		}

		return count, nil
	}

	count, err := pq.executeReplayFrom(
		ctx, queueName, tableName, queueType, since, opts,
	)
	if err != nil {
		span.SetError(err)
		return 0, fmt.Errorf("failed to execute replay from timestamp: %w", err)
	}

	return count, nil
}

// ReplayMessage resets a specific channel message to pending status.
// Not supported for pub/sub queues (use ReplayFrom or ReplayDLQ instead).
func (pq *Queue) ReplayMessage(
	ctx context.Context,
	queueName string,
	queueType QueueType,
	messageID uuid.UUID,
	opts ReplayOptions,
) error {
	if err := pq.checkClosed(); err != nil {
		return err
	}

	ctx, span := pq.startSpan(ctx, "pgqueue.replay",
		StringAttr("queue", queueName), StringAttr("replay_type", "message_id"))
	defer span.End()

	if queueType == QueueTypePubSub {
		span.SetError(ErrReplayNotSupported)
		return ErrReplayNotSupported
	}

	if err := validateReplayOpts(opts); err != nil {
		span.SetError(err)
		return fmt.Errorf("failed to validate replay options: %w", err)
	}

	metadata, err := pq.getReplayQueueMetadata(ctx, queueType, queueName)
	if err != nil {
		span.SetError(err)
		return fmt.Errorf("failed to get queue metadata for replay: %w", err)
	}

	tableName := metadata.TableName

	if opts.DryRun {
		return pq.checkMessageExists(ctx, tableName, messageID)
	}

	if err := pq.executeReplayMessage(
		ctx, queueName, tableName, queueType, messageID, opts,
	); err != nil {
		span.SetError(err)
		return fmt.Errorf("failed to replay message: %w", err)
	}

	return nil
}

// ReplayDLQResult reports the outcome of a ReplayDLQ call.
type ReplayDLQResult struct {
	// Replayed is the number of messages reinstated from the DLQ onto their
	// live queue.
	Replayed int64
	// Skipped is the number of DLQ rows examined but not replayable — for
	// example a row whose original message id is still live, or a legacy
	// pub/sub row with no active subscribers. Skipped rows are left in the DLQ.
	Skipped int64
}

// ReplayDLQ moves messages from the dead-letter queue back to the main queue.
// It processes the DLQ in keyset-paginated pages, each in its own transaction,
// so memory and lock footprint stay bounded regardless of backlog size.
//
// When opts.Limit is set, the loop terminates once Limit DLQ rows have been
// *examined* (Replayed + Skipped), not when Limit messages have been replayed —
// so a DLQ full of un-replayable rows still returns promptly rather than
// scanning the whole table.
//
// For pub/sub topics replay re-creates the failed subscriber's subscription
// row, which foreign-keys the original message row. That row is kept alive for
// you: the garbage collector never purges a message while a DLQ entry still
// references it (see purgeCompletedMessages / reclaimOrphanTopicMessages), so
// the DLQ entry is always purged first and CompletedMessageTTL may safely be
// shorter than DLQRetention. A DLQ entry whose message was removed by some
// other means (manual SQL, PurgeQueue) is skipped, not replayed, so one stale
// entry can never abort the whole replay.
//
// DryRun caveat: a dry run walks the real paginated path with each page rolled
// back, so inserts from earlier pages are not visible to later ones. If two DLQ
// entries share the same original_message_id and fall in different pages, a real
// run would replay the first and skip the second (ON CONFLICT), but a dry run
// counts both as Replayed. The preview can therefore over-report Replayed (and
// under-report Skipped) for duplicate ids; a real run is always exact.
func (pq *Queue) ReplayDLQ(
	ctx context.Context,
	queueName string,
	queueType QueueType,
	opts ReplayOptions,
) (ReplayDLQResult, error) {
	if err := pq.checkClosed(); err != nil {
		return ReplayDLQResult{}, err
	}

	ctx, span := pq.startSpan(ctx, "pgqueue.replay",
		StringAttr("queue", queueName), StringAttr("replay_type", "dlq"))
	defer span.End()

	if err := validateReplayOpts(opts); err != nil {
		span.SetError(err)
		return ReplayDLQResult{}, fmt.Errorf("failed to validate replay options: %w", err)
	}

	metadata, err := pq.getReplayQueueMetadata(ctx, queueType, queueName)
	if err != nil {
		span.SetError(err)
		return ReplayDLQResult{}, fmt.Errorf("failed to get queue metadata for replay: %w", err)
	}

	tableName := metadata.TableName

	// A dry run reports exactly what a real run would replay (and skip) without
	// mutating anything: it walks the same paginated replay path with dryRun=true
	// so each page transaction is rolled back instead of committed. This counts
	// only genuinely replayable rows — a raw COUNT(*) would overstate the result
	// because the real run skips rows whose original message is still live, rows
	// with no active subscriber, and duplicate ids deferred to a later call.
	// Channel DLQ rows carry no max_retries column, so a reinstated message must
	// be given the queue's configured cap explicitly. Otherwise it inherits the
	// message-table column default (0), which channelMaxRetries reads as "no
	// retries", and the message is dead-lettered again on its first failed
	// delivery — silently losing its retry budget on every DLQ round-trip.
	maxRetries := pq.resolveMaxRetries(metadata)
	result, err := pq.executeReplayDLQ(ctx, queueName, tableName, queueType, opts, maxRetries, opts.DryRun)
	if err != nil {
		span.SetError(err)
		if opts.DryRun {
			return ReplayDLQResult{}, fmt.Errorf("failed to preview DLQ replay: %w", err)
		}
		return result, fmt.Errorf("failed to execute DLQ replay: %w", err)
	}

	return result, nil
}

// ReplayHistory returns the replay history for a queue.
func (pq *Queue) ReplayHistory(
	ctx context.Context,
	queueName string,
	queueType QueueType,
	limit int,
) ([]ReplayLog, error) {
	if err := pq.checkClosed(); err != nil {
		return nil, err
	}

	if limit <= 0 {
		limit = 100
	}

	return pq.getReplayHistory(
		ctx, string(queueType), queueName, limit,
	)
}

// MaxPerformedByLen caps the byte length of ReplayOptions.PerformedBy so a
// misconfigured caller cannot fill pgqueue_replay_log.created_by with megabyte
// strings. Chosen to fit common audit identifiers (an operator email, a
// service name, a JWT subject) with comfortable headroom.
const MaxPerformedByLen = 256

func validateReplayOpts(opts ReplayOptions) error {
	// A negative limit is meaningless and would otherwise reach PostgreSQL as an
	// invalid LIMIT, surfacing an opaque database error instead of a clear one.
	if opts.Limit < 0 {
		return ErrInvalidConfig
	}
	// PerformedBy is stored verbatim in pgqueue_replay_log; reject anything that
	// would either bloat the table or break later inspection via psql/grep. It is
	// bound as a parameter when written, so this is not an injection guard — it
	// rejects every control character (NUL, CR, LF, tab, ESC, and other C0/C1
	// controls) so terminal escape sequences cannot corrupt log/audit output.
	if len(opts.PerformedBy) > MaxPerformedByLen {
		return ErrInvalidPerformedBy
	}
	if strings.ContainsFunc(opts.PerformedBy, unicode.IsControl) {
		return ErrInvalidPerformedBy
	}

	return nil
}

func (pq *Queue) getReplayQueueMetadata(
	ctx context.Context,
	queueType QueueType,
	queueName string,
) (*queueMetadata, error) {
	metadata, err := pq.getQueueMetadata(
		ctx, string(queueType), queueName,
	)
	if errors.Is(err, ErrQueueNotFound) {
		return nil, fmt.Errorf(
			"%s/%s: %w", queueType, queueName, ErrQueueNotFound,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get queue metadata: %w", err)
	}

	return metadata, nil
}

// countReplayableMessages counts the rows ReplayFrom would reinstate for a
// since timestamp. For pub/sub the predicate is on the message-table
// created_at — the message publish time — joined to the subscription rows, so
// it agrees exactly with what executeReplayFrom updates (R-03).
func (pq *Queue) countReplayableMessages(
	ctx context.Context,
	tableName string,
	queueType QueueType,
	since time.Time,
) (int64, error) {
	var countQuery string
	if queueType == QueueTypePubSub {
		countQuery = fmt.Sprintf(`
			SELECT COUNT(*)
			FROM %s s
			JOIN %s m ON s.message_id = m.id
			WHERE m.created_at >= $1
			AND s.status != '%s'
			AND s.status != '%s'
		`, pq.subTable(tableName), pq.msgTable(tableName),
			MessageStatusPending, MessageStatusProcessing)
	} else {
		countQuery = fmt.Sprintf(`
			SELECT COUNT(*) FROM %s
			WHERE created_at >= $1
			AND status != '%s'
			AND status != '%s'
		`, pq.msgTable(tableName), MessageStatusPending, MessageStatusProcessing)
	}

	var count int64
	if err := pq.db.QueryRowContext(
		ctx, countQuery, since,
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count messages: %w", err)
	}

	return count, nil
}

// defaultReplayPageSize bounds the rows held in memory and locked per
// ReplayFrom page, so a large backlog replays with O(page) footprint rather
// than O(backlog) (R-02).
const defaultReplayPageSize = 100

// replayFromPageResult is the outcome of replaying one keyset page.
//
// The page is keyed on the unit that is actually reinstated: message ids for
// channels, subscription-row ids for topics. Paging on subscription ids is
// what lets opts.Limit cap topic replays exactly — a topic message fans out to
// one subscription row per subscriber, so paging on message ids would let one
// page reinstate up to (subscribers × pageSize) rows and overshoot the limit.
type replayFromPageResult struct {
	replayed int64     // rows reinstated this page
	lastID   uuid.UUID // highest candidate id examined — the next page's cursor
	fetched  int64     // candidate ids examined this page; zero means exhausted
}

// executeReplayFrom replays messages published since a timestamp in
// keyset-paginated pages (defaultReplayPageSize per page), each page in its own
// transaction, so memory and lock footprint stay bounded regardless of backlog
// size (R-02). An explicit opts.Limit still caps the total.
func (pq *Queue) executeReplayFrom(
	ctx context.Context,
	queueName, tableName string,
	queueType QueueType,
	since time.Time,
	opts ReplayOptions,
) (int64, error) {
	var afterID uuid.UUID
	var total int64

	for {
		var pageLimit int64 = defaultReplayPageSize
		if opts.Limit > 0 {
			remaining := opts.Limit - total
			if remaining <= 0 {
				break
			}
			if remaining < pageLimit {
				pageLimit = remaining
			}
		}

		page, err := pq.replayFromPage(
			ctx, queueName, tableName, queueType, since, afterID, pageLimit, opts.PerformedBy,
		)
		if err != nil {
			return total, err
		}
		total += page.replayed
		if page.fetched == 0 {
			break // backlog exhausted
		}
		afterID = page.lastID
	}

	return total, nil
}

// replayFromPage replays one keyset page in a single transaction. It selects a
// page of candidate ids (message ids for channels, subscription ids for
// topics) published since the cursor, reinstates the corresponding rows, and
// writes a per-page audit row inside the same transaction.
func (pq *Queue) replayFromPage(
	ctx context.Context,
	queueName, tableName string,
	queueType QueueType,
	since time.Time,
	afterID uuid.UUID,
	pageLimit int64,
	performedBy string,
) (replayFromPageResult, error) {
	var result replayFromPageResult
	if err := pq.withTx(ctx, func(tx *sql.Tx) error {
		ids, err := pq.fetchReplayCandidateIDs(ctx, tx, tableName, queueType, since, afterID, pageLimit)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			result = replayFromPageResult{lastID: afterID}
			return nil
		}

		replayed, err := pq.applyReplayFrom(ctx, tx, tableName, queueType, ids)
		if err != nil {
			return err
		}

		if replayed > 0 {
			if err := pq.writeReplayLog(
				ctx, tx, queueName, queueType, "timestamp",
				replayed, performedBy, fmt.Sprintf("since: %s", since),
			); err != nil {
				return err
			}
		}

		result = replayFromPageResult{
			replayed: replayed,
			lastID:   ids[len(ids)-1],
			fetched:  int64(len(ids)),
		}
		return nil
	}); err != nil {
		return replayFromPageResult{}, err
	}
	return result, nil
}

// fetchReplayCandidateIDs returns one keyset page of replay-candidate ids
// published since the given timestamp, ordered by id and strictly after
// afterID. The time predicate is always on the message table's created_at —
// the authoritative publish time — for both channels and topics (R-03).
//
// The id paged on is the unit applyReplayFrom reinstates: the message id for
// channels, the subscription-row id for topics. Paging topics on subscription
// ids (rather than message ids) keeps one row of work per fetched id, so
// opts.Limit caps the replay exactly instead of overshooting by a factor of
// the subscriber count.
func (pq *Queue) fetchReplayCandidateIDs(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	queueType QueueType,
	since time.Time,
	afterID uuid.UUID,
	pageLimit int64,
) ([]uuid.UUID, error) {
	var after any
	if afterID != (uuid.UUID{}) {
		after = afterID
	}

	var query string
	if queueType == QueueTypeChannel {
		// Channels carry status on the message table; only non-pending,
		// non-processing messages are replay candidates.
		query = fmt.Sprintf(`
			SELECT id FROM %s
			WHERE created_at >= $1
			  AND status != '%s' AND status != '%s'
			  AND ($3::uuid IS NULL OR id > $3)
			ORDER BY id
			LIMIT $2
		`, pq.msgTable(tableName), MessageStatusPending, MessageStatusProcessing)
	} else {
		// Topics keep status on the per-subscriber subscription rows. Page over
		// the subscription rows themselves (joined to the message for the
		// publish-time cutoff) so each fetched id is exactly one row of work.
		query = fmt.Sprintf(`
			SELECT s.id
			FROM %s s
			JOIN %s m ON s.message_id = m.id
			WHERE m.created_at >= $1
			  AND s.status != '%s' AND s.status != '%s'
			  AND ($3::uuid IS NULL OR s.id > $3)
			ORDER BY s.id
			LIMIT $2
		`, pq.subTable(tableName), pq.msgTable(tableName),
			MessageStatusPending, MessageStatusProcessing)
	}

	rows, err := tx.QueryContext(ctx, query, since, pageLimit, after)
	if err != nil {
		return nil, fmt.Errorf("failed to query replay candidates: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan replay candidate id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating replay candidates: %w", err)
	}

	return ids, nil
}

// applyReplayFrom reinstates the given candidate ids to pending and returns the
// number of rows actually changed. For channels the ids are message ids and
// the message rows are reset; for topics the ids are subscription-row ids and
// those subscription rows are reset (R-03).
func (pq *Queue) applyReplayFrom(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	queueType QueueType,
	ids []uuid.UUID,
) (int64, error) {
	var query string
	if queueType == QueueTypeChannel {
		query = fmt.Sprintf(`
			UPDATE %s
			SET status = '%s',
			    retry_count = 0,
			    visibility_timeout = NULL,
			    claim_id = NULL,
			    processed_at = NULL,
			    error_message = NULL
			WHERE id = ANY($1::text::uuid[])
			  AND status != '%s' AND status != '%s'
		`, pq.msgTable(tableName), MessageStatusPending, MessageStatusPending, MessageStatusProcessing)
	} else {
		query = fmt.Sprintf(`
			UPDATE %s
			SET status = '%s',
			    retry_count = 0,
			    visibility_timeout = NULL,
			    claim_id = NULL,
			    acked_at = NULL,
			    error_message = NULL
			WHERE id = ANY($1::text::uuid[])
			  AND status != '%s' AND status != '%s'
		`, pq.subTable(tableName), MessageStatusPending, MessageStatusPending, MessageStatusProcessing)
	}

	result, err := tx.ExecContext(ctx, query, uuidArrayLiteral(ids))
	if err != nil {
		return 0, fmt.Errorf("failed to replay messages: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get affected rows: %w", err)
	}
	return rows, nil
}

func (pq *Queue) checkMessageExists(
	ctx context.Context,
	tableName string,
	messageID uuid.UUID,
) error {
	// Table name derived from a queueNameRegex-validated queue name; injection-safe.
	checkQuery := fmt.Sprintf(
		`SELECT status FROM %s WHERE id = $1`, pq.msgTable(tableName),
	)

	var status string
	err := pq.db.QueryRowContext(ctx, checkQuery, messageID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%s: %w", messageID, ErrReplayMessageNotFound)
	}
	if err != nil {
		return fmt.Errorf("failed to check message: %w", err)
	}
	if MessageStatus(status) == MessageStatusProcessing {
		return fmt.Errorf("%s: %w", messageID, ErrMessageInProcessing)
	}

	return nil
}

func (pq *Queue) executeReplayMessage(
	ctx context.Context,
	queueName, tableName string,
	queueType QueueType,
	messageID uuid.UUID,
	opts ReplayOptions,
) error {
	return pq.withTx(ctx, func(tx *sql.Tx) error {
		//nolint:gosec // G201: table name validated by queueNameRegex
		query := fmt.Sprintf(`
		UPDATE %s
		SET status = '%s',
		    retry_count = 0,
		    visibility_timeout = NULL,
		    claim_id = NULL,
		    available_at = NOW(),
		    processed_at = NULL,
		    error_message = NULL
		WHERE id = $1 AND status != '%s'
	`, pq.msgTable(tableName), MessageStatusPending, MessageStatusProcessing)

		result, err := tx.ExecContext(ctx, query, messageID)
		if err != nil {
			return fmt.Errorf("failed to replay message: %w", err)
		}

		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get affected rows: %w", err)
		}

		if rows == 0 {
			// Distinguish "not found" from "currently being processed"
			var status string
			checkQuery := fmt.Sprintf( //nolint:gosec // G201: table name validated by queueNameRegex
				`SELECT status FROM %s WHERE id = $1`, pq.msgTable(tableName),
			)
			err := tx.QueryRowContext(ctx, checkQuery, messageID).Scan(&status)
			if err == nil && MessageStatus(status) == MessageStatusProcessing {
				return fmt.Errorf("%s: %w", messageID, ErrMessageInProcessing)
			}
			return fmt.Errorf("%s: %w", messageID, ErrReplayMessageNotFound)
		}

		if err := pq.writeReplayLog(
			ctx, tx, queueName, queueType, "message_id",
			1, opts.PerformedBy, fmt.Sprintf("message_id: %s", messageID),
		); err != nil {
			return err
		}

		return nil
	})
}

// executeReplayDLQ replays the dead-letter queue in keyset-paginated pages
// (defaultDLQReplayPageSize per page), each page in its own transaction, so
// memory stays bounded by the page size regardless of backlog size (FR-025).
//
// When opts.Limit is set the loop terminates once Limit DLQ rows have been
// examined — Replayed + Skipped — not when Limit messages have been replayed,
// so a DLQ full of un-replayable rows still returns promptly (R-04).
func (pq *Queue) executeReplayDLQ(
	ctx context.Context,
	queueName, tableName string,
	queueType QueueType,
	opts ReplayOptions,
	maxRetries int,
	dryRun bool,
) (ReplayDLQResult, error) {
	var afterID uuid.UUID
	var result ReplayDLQResult

	for {
		var pageLimit int64 = defaultDLQReplayPageSize
		if opts.Limit > 0 {
			remaining := opts.Limit - result.Replayed - result.Skipped
			if remaining <= 0 {
				break
			}
			if remaining < pageLimit {
				pageLimit = remaining
			}
		}

		page, err := pq.replayDLQPage(
			ctx, queueName, tableName, queueType, afterID, pageLimit, opts.PerformedBy, maxRetries, dryRun,
		)
		if err != nil {
			return result, err
		}
		result.Replayed += page.replayed
		// Rows fetched but not reinstated this call (live id still present, no
		// active subscriber, or a duplicate id deferred to a later call) are
		// reported as skipped.
		result.Skipped += page.fetched - page.replayed
		if page.fetched == 0 {
			break // DLQ exhausted
		}
		afterID = page.lastID
	}

	return result, nil
}

// dlqPageResult is the outcome of replaying one keyset page of the DLQ.
type dlqPageResult struct {
	replayed int64     // messages reinstated onto the main queue
	lastID   uuid.UUID // highest DLQ id seen — the next page's cursor
	fetched  int64     // rows fetched this page; zero means the DLQ is exhausted
}

// replayDLQPage replays one keyset page of the DLQ in a single transaction. The
// per-page audit row is written inside that same transaction, so the audit
// trail stays consistent with the messages actually replayed even if the
// process crashes mid-replay (R-11).
// errDryRunRollback is returned from the replayDLQPage transaction body when
// dryRun is set, so withTx rolls the page back instead of committing. It never
// escapes replayDLQPage — it is the signal that the page ran successfully and
// its counts should be kept without persisting any of its writes.
var errDryRunRollback = errors.New("pgqueue: dry-run page rollback")

func (pq *Queue) replayDLQPage(
	ctx context.Context,
	queueName, tableName string,
	queueType QueueType,
	afterID uuid.UUID,
	pageLimit int64,
	performedBy string,
	maxRetries int,
	dryRun bool,
) (dlqPageResult, error) {
	var page dlqPageResult
	if err := pq.withTx(ctx, func(tx *sql.Tx) error {
		dlqMessages, err := pq.fetchDLQMessages(ctx, tx, tableName, afterID, pageLimit)
		if err != nil {
			return fmt.Errorf("failed to fetch DLQ messages: %w", err)
		}
		if len(dlqMessages) == 0 {
			page = dlqPageResult{lastID: afterID}
			return nil
		}

		replayed, err := pq.reinsertDLQMessages(ctx, tx, queueName, tableName, queueType, maxRetries, dlqMessages)
		if err != nil {
			return fmt.Errorf("failed to reinsert DLQ messages: %w", err)
		}

		// Audit row per page, committed atomically with the page it describes.
		// Skipped on a dry run — the whole transaction is about to be rolled back.
		if replayed > 0 && !dryRun {
			if err := pq.writeReplayLog(
				ctx, tx, queueName, queueType, "dlq",
				replayed, performedBy,
				fmt.Sprintf("replayed %d messages from DLQ", replayed),
			); err != nil {
				return err
			}
		}

		// Rows are ordered by id, so the last one is the highest id this page saw.
		page = dlqPageResult{
			replayed: replayed,
			lastID:   dlqMessages[len(dlqMessages)-1].id,
			fetched:  int64(len(dlqMessages)),
		}
		// A dry run exercises the exact replay predicate (reinsertDLQMessages runs
		// the real INSERT/DELETE, including the original-message and active-
		// subscriber filters) but persists nothing: returning the sentinel makes
		// withTx roll back. The keyset cursor still advances by lastID, so the
		// next page moves forward exactly as a committed run would.
		if dryRun {
			return errDryRunRollback
		}
		return nil
	}); err != nil {
		// errDryRunRollback is only ever returned on a dry run; it signals the
		// page ran successfully and was deliberately rolled back, so keep counts.
		if errors.Is(err, errDryRunRollback) {
			return page, nil
		}
		return dlqPageResult{}, err
	}
	return page, nil
}

type dlqRow struct {
	id                uuid.UUID
	originalMessageID uuid.UUID
	subscriberID      sql.NullString // set for pub/sub DLQ entries, NULL for channels
	payload           []byte
	metadata          []byte
}

// defaultDLQReplayPageSize bounds the rows held in memory per ReplayDLQ page,
// so a large DLQ backlog replays with O(page) memory rather than O(backlog).
const defaultDLQReplayPageSize = 100

// fetchDLQMessages reads one keyset page of DLQ rows with id greater than
// afterID. ORDER BY id makes the page deterministic; FOR UPDATE SKIP LOCKED
// lets concurrent ReplayDLQ calls each claim a disjoint set of rows so no DLQ
// entry is replayed twice or silently dropped.
func (pq *Queue) fetchDLQMessages(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	afterID uuid.UUID,
	limit int64,
) ([]dlqRow, error) {
	if limit <= 0 {
		limit = defaultDLQReplayPageSize
	}

	var after any
	if afterID != (uuid.UUID{}) {
		after = afterID
	}

	//nolint:gosec // G201: table name validated by queueNameRegex
	selectQuery := fmt.Sprintf(`
		SELECT id, original_message_id, subscriber_id, payload, metadata
		FROM %s
		WHERE ($1::uuid IS NULL OR id > $1)
		ORDER BY id
		LIMIT %d
		FOR UPDATE SKIP LOCKED
	`, pq.dlqTable(tableName), limit)

	rows, err := tx.QueryContext(ctx, selectQuery, after)
	if err != nil {
		return nil, fmt.Errorf("failed to query DLQ: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var dlqMessages []dlqRow
	for rows.Next() {
		var msg dlqRow
		if err := rows.Scan(
			&msg.id, &msg.originalMessageID, &msg.subscriberID,
			&msg.payload, &msg.metadata,
		); err != nil {
			return nil, fmt.Errorf("failed to scan DLQ message: %w", err)
		}
		dlqMessages = append(dlqMessages, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating DLQ messages: %w", err)
	}

	return dlqMessages, nil
}

func (pq *Queue) reinsertDLQMessages(
	ctx context.Context,
	tx *sql.Tx,
	queueName, tableName string,
	queueType QueueType,
	maxRetries int,
	dlqMessages []dlqRow,
) (int64, error) {
	if queueType == QueueTypePubSub {
		// Pub/sub message tables have no max_retries column — per-subscriber
		// retry state is resolved from queue config at consume time — so the
		// resolved cap is not needed here.
		return pq.reinsertDLQPubSub(ctx, tx, queueName, tableName, dlqMessages)
	}

	return pq.reinsertDLQChannel(ctx, tx, tableName, maxRetries, dlqMessages)
}

// dedupeDLQByMessageID returns the DLQ rows keeping only the first occurrence
// of each original_message_id. A message table can hold at most one row per id,
// so duplicate DLQ entries for the same id cannot all be replayed at once; the
// dropped ones are left in the DLQ for a later replay.
func dedupeDLQByMessageID(dlqMessages []dlqRow) []dlqRow {
	seen := make(map[uuid.UUID]struct{}, len(dlqMessages))
	unique := make([]dlqRow, 0, len(dlqMessages))
	for _, msg := range dlqMessages {
		if _, dup := seen[msg.originalMessageID]; dup {
			continue
		}
		seen[msg.originalMessageID] = struct{}{}
		unique = append(unique, msg)
	}

	return unique
}

func (pq *Queue) reinsertDLQChannel(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	maxRetries int,
	dlqMessages []dlqRow,
) (int64, error) {
	if len(dlqMessages) == 0 {
		return 0, nil
	}

	// Two DLQ rows can carry the same original_message_id (e.g. a message was
	// DLQ'd, then re-published with the same ID and DLQ'd again). Only one row
	// with a given id can exist in the message table, so deduplicate: the first
	// DLQ row per id is replayed and the rest are left in the DLQ, rather than
	// being deleted alongside it without their payload ever being restored.
	unique := dedupeDLQByMessageID(dlqMessages)

	// Insert messages and collect which IDs were actually inserted (ON CONFLICT skips dupes).
	insertedIDs, err := pq.insertDLQChannelMessages(ctx, tx, tableName, maxRetries, unique)
	if err != nil {
		return 0, err
	}

	if len(insertedIDs) == 0 {
		return 0, nil
	}

	// Only delete DLQ entries whose messages were actually reinserted
	dlqIDs := make([]uuid.UUID, 0, len(insertedIDs))
	for _, msg := range unique {
		if _, ok := insertedIDs[msg.originalMessageID]; ok {
			dlqIDs = append(dlqIDs, msg.id)
		}
	}

	//nolint:gosec // G201: table name validated by queueNameRegex
	deleteQuery := fmt.Sprintf(
		`DELETE FROM %s WHERE id = ANY($1::text::uuid[])`, pq.dlqTable(tableName),
	)
	if _, err := tx.ExecContext(ctx, deleteQuery, uuidArrayLiteral(dlqIDs)); err != nil {
		return 0, fmt.Errorf("failed to delete from DLQ: %w", err)
	}

	return int64(len(insertedIDs)), nil
}

// insertDLQChannelMessages batch-inserts DLQ messages back into the channel message table.
// Returns the set of message IDs that were actually inserted (ON CONFLICT skips duplicates).
//
// max_retries is set explicitly to the queue's resolved cap rather than left to
// the column default (0): the DLQ table carries no max_retries column to restore
// from, and a stored 0 reads as "no retries", which would dead-letter the
// message again on its first failed delivery. maxRetries is a library-resolved
// int, so it is interpolated directly (no injection surface).
func (pq *Queue) insertDLQChannelMessages(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	maxRetries int,
	dlqMessages []dlqRow,
) (map[uuid.UUID]struct{}, error) {
	const paramsPerRow = 3 // id, payload, metadata

	var sb strings.Builder
	fmt.Fprintf(&sb,
		"INSERT INTO %s (id, payload, created_at, status, retry_count, max_retries, metadata) VALUES ",
		pq.msgTable(tableName),
	)

	args := make([]any, 0, len(dlqMessages)*paramsPerRow)
	for i, msg := range dlqMessages {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * paramsPerRow
		fmt.Fprintf(&sb, "($%d, $%d, NOW(), '%s', 0, %d, $%d)",
			base+1, base+2, MessageStatusPending, maxRetries, base+3, //nolint:mnd // SQL placeholder arithmetic
		)
		args = append(args, msg.originalMessageID, msg.payload, jsonbParam(msg.metadata))
	}
	sb.WriteString(" ON CONFLICT (id) DO NOTHING RETURNING id")

	rows, err := tx.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("failed to insert messages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	insertedIDs := make(map[uuid.UUID]struct{})
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan inserted message ID: %w", err)
		}
		insertedIDs[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate inserted messages: %w", err)
	}

	return insertedIDs, nil
}

// reinsertDLQPubSub re-creates subscription records for pub/sub DLQ messages.
// The original message normally still exists in pgqueue_msg_ (only the
// subscription was deleted when moved to DLQ), so only the subscription records
// are re-created.
//
// DLQ entries whose original message has since been garbage-collected are
// skipped and left in the DLQ: re-creating a subscription row for a missing
// message would raise a foreign-key error that rolls back the entire replay,
// so one stale entry could otherwise block every other entry.
//
// Each DLQ entry is replayed to the exact subscriber that failed (recorded in
// subscriber_id), so subscribers that processed the message successfully are
// never re-delivered it. ON CONFLICT DO NOTHING additionally protects any
// subscriber whose row still exists.
func (pq *Queue) reinsertDLQPubSub(
	ctx context.Context,
	tx *sql.Tx,
	queueName, tableName string,
	dlqMessages []dlqRow,
) (int64, error) {
	if len(dlqMessages) == 0 {
		return 0, nil
	}

	// Drop DLQ rows whose original message no longer exists so the foreign-key
	// insert below cannot fail and abort the whole replay.
	live, err := pq.filterExistingMessages(ctx, tx, tableName, dlqMessages)
	if err != nil {
		return 0, err
	}
	if len(live) == 0 {
		return 0, nil
	}

	records, replayedIDs, err := pq.resolvePubSubDLQRecords(ctx, tx, queueName, live)
	if err != nil {
		return 0, err
	}
	if len(replayedIDs) == 0 {
		return 0, nil
	}

	if err := pq.insertSubscriptionRecords(ctx, tx, tableName, records); err != nil {
		return 0, fmt.Errorf("failed to create subscription records: %w", err)
	}

	//nolint:gosec // G201: table name validated by queueNameRegex
	deleteQuery := fmt.Sprintf(
		`DELETE FROM %s WHERE id = ANY($1::text::uuid[])`, pq.dlqTable(tableName),
	)
	if _, err := tx.ExecContext(ctx, deleteQuery, uuidArrayLiteral(replayedIDs)); err != nil {
		return 0, fmt.Errorf("failed to delete from DLQ: %w", err)
	}

	// Report the number of DLQ rows actually replayed (and deleted), not the
	// number of subscription records created — a legacy NULL-subscriber row
	// fans out to many records, which would otherwise inflate the count past
	// the page's fetched total and yield a negative Skipped.
	return int64(len(replayedIDs)), nil
}

// filterExistingMessages returns the subset of DLQ rows whose original message
// still exists in the per-queue message table.
func (pq *Queue) filterExistingMessages(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	dlqMessages []dlqRow,
) ([]dlqRow, error) {
	ids := make([]uuid.UUID, 0, len(dlqMessages))
	for _, msg := range dlqMessages {
		ids = append(ids, msg.originalMessageID)
	}

	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(
		`SELECT id FROM %s WHERE id = ANY($1::text::uuid[])`, pq.msgTable(tableName),
	)
	rows, err := tx.QueryContext(ctx, query, uuidArrayLiteral(ids))
	if err != nil {
		return nil, fmt.Errorf("failed to check existing messages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	existing := make(map[uuid.UUID]struct{})
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan message id: %w", err)
		}
		existing[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate message ids: %w", err)
	}

	live := make([]dlqRow, 0, len(dlqMessages))
	for _, msg := range dlqMessages {
		if _, ok := existing[msg.originalMessageID]; ok {
			live = append(live, msg)
		}
	}

	return live, nil
}

// resolvePubSubDLQRecords maps pub/sub DLQ rows to the subscription records to
// re-create, and returns the ids of the DLQ entries that can be deleted.
//
// Rows carrying a subscriber_id are replayed to exactly that subscriber. Legacy
// rows with a NULL subscriber_id (written before schema v2) fall back to all
// currently-active subscribers; such a row is only marked for deletion when at
// least one active subscriber exists, so a replay with no subscribers leaves it
// in the DLQ rather than silently dropping it.
func (pq *Queue) resolvePubSubDLQRecords(
	ctx context.Context,
	tx *sql.Tx,
	queueName string,
	dlqMessages []dlqRow,
) ([]subRecord, []uuid.UUID, error) {
	var legacySubscribers []subscriber
	legacyLoaded := false

	// seen deduplicates subscription records: two DLQ entries can describe the
	// same (message, subscriber) failed delivery, which needs re-creating only
	// once. The redundant DLQ rows are still deleted.
	seen := make(map[subRecord]struct{}, len(dlqMessages))
	records := make([]subRecord, 0, len(dlqMessages))
	replayedIDs := make([]uuid.UUID, 0, len(dlqMessages))

	addRecord := func(rec subRecord) {
		if _, dup := seen[rec]; dup {
			return
		}
		seen[rec] = struct{}{}
		records = append(records, rec)
	}

	for _, msg := range dlqMessages {
		if msg.subscriberID.Valid {
			addRecord(subRecord{
				messageID:    msg.originalMessageID,
				subscriberID: msg.subscriberID.String,
			})
			replayedIDs = append(replayedIDs, msg.id)

			continue
		}

		// Legacy row with no recorded subscriber: load active subscribers once.
		if !legacyLoaded {
			subs, err := pq.getActiveSubscribers(ctx, tx, queueName)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to get active subscribers: %w", err)
			}
			legacySubscribers = subs
			legacyLoaded = true
		}
		if len(legacySubscribers) == 0 {
			continue // leave the DLQ row in place; nothing to replay to
		}
		for _, sub := range legacySubscribers {
			addRecord(subRecord{
				messageID:    msg.originalMessageID,
				subscriberID: sub.SubscriberID,
			})
		}
		replayedIDs = append(replayedIDs, msg.id)
	}

	return records, replayedIDs, nil
}

// writeReplayLog records a replay in pgqueue_replay_log within the caller's
// transaction. The audit row therefore commits atomically with the replay it
// describes (FR-007) — a failed log write rolls the whole replay back.
//
// ReplayFrom and ReplayDLQ process the backlog in keyset-paginated pages, each
// in its own transaction; this function is called once per page, so a paged
// replay produces one audit row per page rather than a single total row. Each
// row's message_count is that page's count, keeping the audit trail consistent
// with the messages actually replayed even if the process crashes mid-replay
// (R-11).
func (pq *Queue) writeReplayLog(
	ctx context.Context,
	tx *sql.Tx,
	queueName string,
	queueType QueueType,
	operation string,
	count int64,
	performedBy, details string,
) error {
	params, err := json.Marshal(map[string]string{"details": details})
	if err != nil {
		return fmt.Errorf("failed to marshal replay params: %w", err)
	}

	var createdBy *string
	if performedBy != "" {
		createdBy = &performedBy
	}

	if err := pq.createReplayLog(
		ctx, tx, string(queueType), queueName,
		operation, params, int(count), createdBy,
	); err != nil {
		return fmt.Errorf("failed to log replay operation: %w", err)
	}

	return nil
}
