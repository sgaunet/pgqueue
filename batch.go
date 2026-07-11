package pgqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// resolveNackRetryDelay collapses a slice of NackOption down to the single
// retry-delay override they may carry (WithRetryDelay); zero means "use the
// configured backoff policy".
func resolveNackRetryDelay(opts []NackOption) time.Duration {
	o := nackOpts{}
	for _, fn := range opts {
		fn(&o)
	}
	return o.retryDelay
}

// SQL parameter counts per row for multi-row INSERT statements.
const (
	channelInsertParams = 4 // id, payload, metadata, max_retries
	pubsubInsertParams  = 3 // id, payload, metadata
	subInsertParams     = 2 // message_id, subscriber_id
	dlqInsertParams     = 5 // original_message_id, payload, failure_reason, retry_count, metadata
	subDLQInsertParams  = 6 // dlqInsertParams + subscriber_id
)

// subFanoutChunkRows bounds both the in-memory fan-out working set (H5) and each
// subscription multi-row INSERT: the messages×subscribers cross product is
// streamed into the insert this many rows at a time instead of being fully
// materialized first. Each row binds subInsertParams (2) parameters and
// PostgreSQL's bind-parameter limit is 65535, so 16000 rows stays well under it.
const subFanoutChunkRows = 16000

// maxBatchTransientRetries bounds how many times a batch ack/nack is retried when
// its transaction aborts with a transient error — chiefly a deadlock (40P01).
// Canonical lock ordering (receiptsToIDClaimLiterals sorts its arrays and the
// FOR UPDATE state fetches ORDER BY id) makes deadlocks rare; this retry absorbs
// the residual and any serialization/connection blip (M2/FR-026).
const maxBatchTransientRetries = 3

// batchRetryBackoffStep is the base of the linear backoff between batch retries;
// attempt i waits (i+1)*step so a deadlock victim does not immediately re-collide.
const batchRetryBackoffStep = 10 * time.Millisecond

// withBatchRetry runs a batch ack/nack attempt, retrying up to
// maxBatchTransientRetries times when it fails with a transient error. Each
// attempt runs its own transaction (the callee begins it and rolls it back on
// error), so a retry re-does the work against a clean snapshot — safe because
// nothing from a deadlocked/aborted attempt was committed. Success, a
// non-transient error, or a cancelled context ends the loop immediately.
func withBatchRetry(ctx context.Context, attempt func() (BatchResult, error)) (BatchResult, error) {
	var res BatchResult
	var err error
	for i := 0; i <= maxBatchTransientRetries; i++ {
		res, err = attempt()
		if err == nil || !isTransientError(err) || ctx.Err() != nil {
			return res, err
		}
		select {
		case <-ctx.Done():
			return res, err
		case <-time.After(time.Duration(i+1) * batchRetryBackoffStep):
		}
	}
	return res, err
}

// PublishBatch publishes multiple messages to a channel or topic in a single operation.
// Returns message IDs in the same order as the input messages.
//
// The operation is atomic (all-or-nothing): if any message has a duplicate ID,
// the entire batch is rolled back and ErrDuplicateMessageID is returned.
func (pq *Queue) PublishBatch(
	ctx context.Context,
	queueName string,
	messages []PublishMessage,
) ([]uuid.UUID, error) {
	if err := pq.checkClosed(); err != nil {
		return nil, err
	}
	if len(messages) == 0 {
		return []uuid.UUID{}, nil
	}
	if err := validateBatchSize(len(messages)); err != nil {
		return nil, err
	}

	queueMeta, err := pq.resolveQueueMetadata(ctx, queueName)
	if err != nil {
		return nil, err
	}

	ctx, span := pq.startSpan(ctx, "pgqueue.publish_batch", StringAttr("queue", queueName))
	ids, err := pq.publishBatchResolved(ctx, queueMeta, messages)
	pq.endSpan(span, err)
	if err == nil {
		pq.recordPublish(queueName, len(messages))
	}
	return ids, err
}

// publishBatchResolved publishes a batch once the queue metadata has been
// resolved. It validates every payload, generates message IDs, and dispatches
// to the channel or pub/sub batch insert based on queueMeta.QueueType. The
// caller is responsible for the closed-state, empty-slice, and batch-size
// checks before calling this.
func (pq *Queue) publishBatchResolved(
	ctx context.Context,
	queueMeta *queueMetadata,
	messages []PublishMessage,
) ([]uuid.UUID, error) {
	// Resolve the payload-size cap once for the whole batch rather than
	// re-unmarshaling the queue config per message in the hot path.
	maxMsgSize := pq.resolveMaxMessageSize(queueMeta)

	// Validate all payloads upfront before any DB work, accumulating the batch
	// total so an aggregate WithMaxBatchBytes ceiling can reject an abusively
	// large batch before it reaches the database (L8).
	var totalBytes int64
	for i := range messages {
		if messages[i].Payload == nil {
			return nil, ErrNilPayload
		}
		if err := checkPayloadSize(maxMsgSize, messages[i].Payload); err != nil {
			return nil, err
		}
		totalBytes += int64(len(messages[i].Payload))
	}
	if maxBatchBytes := pq.cfg.maxBatchBytes; maxBatchBytes > 0 && totalBytes > int64(maxBatchBytes) {
		return nil, fmt.Errorf(
			"batch payload total %d bytes exceeds limit %d: %w",
			totalBytes, maxBatchBytes, ErrBatchTooLarge,
		)
	}

	ids, metadataJSONs, err := pq.prepareBatchMessages(queueMeta, messages)
	if err != nil {
		return nil, err
	}

	if queueMeta.QueueType == QueueTypePubSub {
		return ids, pq.publishBatchToPubSub(
			ctx, queueMeta.QueueName, queueMeta.TableName,
			ids, messages, metadataJSONs,
		)
	}

	maxRetries := pq.resolveMaxRetries(queueMeta)

	return ids, pq.publishBatchToChannel(
		ctx, queueMeta.TableName,
		ids, messages, metadataJSONs, maxRetries,
	)
}

// ackChannelBatch acknowledges multiple messages from a channel in a single
// operation and returns the per-receipt outcome. Receipts that matched a live
// processing claim are acked and appear in BatchResult.Succeeded; the rest are
// classified (ErrClaimExpired / ErrMessageAlreadyAcked / ErrMessageNotFound)
// and appear in BatchResult.Failed. Partial success is not an error. Each
// failed receipt increments the RecordAckAfterExpired metric so operators can
// detect the corresponding redeliveries. A non-nil error is operational (bad
// batch size, missing queue, or a database failure).
func (pq *Queue) ackChannelBatch(
	ctx context.Context,
	channelName string,
	receipts []Receipt,
) (BatchResult, error) {
	if len(receipts) == 0 {
		return BatchResult{}, nil
	}
	if err := validateBatchSize(len(receipts)); err != nil {
		return BatchResult{}, err
	}

	queueMeta, err := pq.getQueueMetadata(
		ctx, string(QueueTypeChannel), channelName,
	)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return BatchResult{}, fmt.Errorf("channel/%s: %w", channelName, ErrQueueNotFound)
		}
		return BatchResult{}, fmt.Errorf("failed to get channel metadata: %w", err)
	}

	// Run the UPDATE (with RETURNING, to learn which receipts matched) and the
	// classifying SELECT for the misses in one transaction so the classification
	// observes the same snapshot as the UPDATE — a concurrent reclaim cannot
	// flip a receipt's reason between the two (matches single-ack R-09).
	tx, err := pq.db.BeginTx(ctx, readCommittedTxOptions)
	if err != nil {
		return BatchResult{}, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// G201 is not applicable: the query (with the regex-validated table name) is
	// passed to queryMatchedIDs as a parameter, not built at the exec call site.
	query := fmt.Sprintf(`
		UPDATE %s AS m
		SET status = '%s', processed_at = NOW()
		FROM unnest($1::text::uuid[], $2::text::uuid[]) AS u(id, claim_id)
		WHERE m.id = u.id
		  AND m.claim_id = u.claim_id
		  AND m.status = '%s'
		RETURNING m.id, m.claim_id
	`, pq.msgTable(queueMeta.TableName), MessageStatusCompleted, MessageStatusProcessing)

	ids, claims := receiptsToIDClaimLiterals(receipts)
	matched, err := queryMatchedIDs(ctx, tx, query, ids, claims)
	if err != nil {
		return BatchResult{}, fmt.Errorf("failed to acknowledge messages: %w", err)
	}

	msgTable := pq.msgTable(queueMeta.TableName)
	return pq.finishBatch(tx, channelName, receipts, matched,
		func(misses []Receipt) ([]FailedReceipt, error) {
			return classifyChannelBatchMisses(ctx, tx, msgTable, misses)
		})
}

// matchKey identifies a receipt by the full (MessageID, ClaimID) pair. Keying
// the matched set on the message ID alone would misclassify a stale receipt
// (same message, older claim) as Succeeded when a live receipt for the same
// message is also in the batch; both the ack UPDATE and the nack state JOIN
// match on the full pair, so the matched set must too. Index 0 is the message
// id, index 1 the claim id.
type matchKey [2]uuid.UUID

func receiptMatchKey(r Receipt) matchKey {
	return matchKey{r.MessageID, r.ClaimID}
}

// queryMatchedIDs runs an UPDATE ... RETURNING id, claim_id (or a SELECT) and
// collects the returned (id, claim_id) pairs into a set, used to learn which
// receipts a batch statement actually matched.
func queryMatchedIDs(
	ctx context.Context,
	tx *sql.Tx,
	query string,
	args ...any,
) (map[matchKey]bool, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query matched ids: %w", err)
	}
	defer func() { _ = rows.Close() }()

	matched := make(map[matchKey]bool)
	for rows.Next() {
		var id, claimID uuid.UUID
		if err := rows.Scan(&id, &claimID); err != nil {
			return nil, fmt.Errorf("failed to scan matched id: %w", err)
		}
		matched[matchKey{id, claimID}] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate matched ids: %w", err)
	}
	return matched, nil
}

// finishBatch partitions receipts into Succeeded (those whose (MessageID,
// ClaimID) pair is in matched) and Failed (classified via classify), commits
// tx, and records the skipped-receipt metric. Input order is preserved within
// Succeeded and Failed. classify is invoked only when there are misses.
func (pq *Queue) finishBatch(
	tx *sql.Tx,
	queueName string,
	receipts []Receipt,
	matched map[matchKey]bool,
	classify func(misses []Receipt) ([]FailedReceipt, error),
) (BatchResult, error) {
	var res BatchResult
	var misses []Receipt
	for _, r := range receipts {
		// Membership is keyed on the full (MessageID, ClaimID) pair so a stale
		// receipt — same message, older claim, e.g. after a redelivery — falls
		// through to the classify path and is reported ErrClaimExpired rather
		// than Succeeded.
		if matched[receiptMatchKey(r)] {
			res.Succeeded = append(res.Succeeded, r)
		} else {
			misses = append(misses, r)
		}
	}

	if len(misses) > 0 {
		failed, err := classify(misses)
		if err != nil {
			return BatchResult{}, err
		}
		res.Failed = failed
	}

	if err := tx.Commit(); err != nil {
		return BatchResult{}, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Count only genuinely expired claims toward the redelivery metric. A miss
	// can also be ErrMessageAlreadyAcked or ErrMessageNotFound (purged), neither
	// of which redelivers, so including them would overstate at-least-twice
	// delivery.
	expired := 0
	for _, f := range res.Failed {
		if errors.Is(f.Reason, ErrClaimExpired) {
			expired++
		}
	}
	pq.recordAckAfterExpired(queueName, expired)
	return res, nil
}

// ackTopicBatch acknowledges multiple messages for a subscriber in a single
// operation and returns the per-receipt outcome. Receipts that matched a live
// processing claim are acked and appear in BatchResult.Succeeded; the rest are
// classified (ErrClaimExpired / ErrMessageAlreadyAcked / ErrMessageNotFound)
// and appear in BatchResult.Failed. Partial success is not an error. Each
// failed receipt increments the RecordAckAfterExpired metric so operators can
// detect the corresponding redeliveries. A non-nil error is operational.
func (pq *Queue) ackTopicBatch(
	ctx context.Context,
	topicName string,
	subscriberID string,
	receipts []Receipt,
) (BatchResult, error) {
	if err := validateSubscriberID(subscriberID); err != nil {
		return BatchResult{}, err
	}
	if len(receipts) == 0 {
		return BatchResult{}, nil
	}
	if err := validateBatchSize(len(receipts)); err != nil {
		return BatchResult{}, err
	}

	queueMeta, err := pq.getQueueMetadata(
		ctx, string(QueueTypePubSub), topicName,
	)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return BatchResult{}, fmt.Errorf("%s: %w", topicName, ErrTopicNotFound)
		}
		return BatchResult{}, fmt.Errorf("failed to get topic metadata: %w", err)
	}

	tx, err := pq.db.BeginTx(ctx, readCommittedTxOptions)
	if err != nil {
		return BatchResult{}, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// G201 is not applicable: the query (with the regex-validated table name) is
	// passed to queryMatchedIDs as a parameter, not built at the exec call site.
	query := fmt.Sprintf(`
		UPDATE %s AS s
		SET status = '%s', acked_at = NOW()
		FROM unnest($1::text::uuid[], $2::text::uuid[]) AS u(message_id, claim_id)
		WHERE s.message_id = u.message_id
		  AND s.claim_id = u.claim_id
		  AND s.subscriber_id = $3
		  AND s.status = '%s'
		RETURNING s.message_id, s.claim_id
	`, pq.subTable(queueMeta.TableName), MessageStatusAcked, MessageStatusProcessing)

	ids, claims := receiptsToIDClaimLiterals(receipts)
	matched, err := queryMatchedIDs(ctx, tx, query, ids, claims, subscriberID)
	if err != nil {
		return BatchResult{}, fmt.Errorf("failed to acknowledge messages: %w", err)
	}

	subTable := pq.subTable(queueMeta.TableName)
	return pq.finishBatch(tx, topicName, receipts, matched,
		func(misses []Receipt) ([]FailedReceipt, error) {
			return classifyTopicBatchMisses(ctx, tx, subTable, subscriberID, misses)
		})
}

// nackChannelBatch negatively acknowledges multiple messages from a channel and
// returns the per-receipt outcome. Receipts that matched a live processing
// claim are retried or moved to DLQ (when retries are exhausted) and appear in
// BatchResult.Succeeded; the rest are classified (ErrClaimExpired /
// ErrMessageAlreadyAcked / ErrMessageNotFound) and appear in
// BatchResult.Failed. Partial success is not an error. The errorMsg is
// truncated to 1024 characters if it exceeds that length. A WithRetryDelay
// option overrides the computed backoff delay for the batch. Each failed
// receipt increments the RecordAckAfterExpired metric.
func (pq *Queue) nackChannelBatch(
	ctx context.Context,
	channelName string,
	receipts []Receipt,
	errorMsg string,
	opts ...NackOption,
) (BatchResult, error) {
	errorMsg = truncateErrorMsg(errorMsg)
	retryDelay := resolveNackRetryDelay(opts)

	if len(receipts) == 0 {
		return BatchResult{}, nil
	}
	if err := validateBatchSize(len(receipts)); err != nil {
		return BatchResult{}, err
	}

	queueMeta, err := pq.getQueueMetadata(
		ctx, string(QueueTypeChannel), channelName,
	)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return BatchResult{}, fmt.Errorf("channel/%s: %w", channelName, ErrQueueNotFound)
		}
		return BatchResult{}, fmt.Errorf("failed to get channel metadata: %w", err)
	}

	tx, err := pq.db.BeginTx(ctx, readCommittedTxOptions)
	if err != nil {
		return BatchResult{}, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	states, err := pq.fetchBatchMessageStates(
		ctx, tx, queueMeta.TableName, receipts,
	)
	if err != nil {
		return BatchResult{}, fmt.Errorf("failed to get message states: %w", err)
	}

	if err := pq.processNackBatch(ctx, tx, queueMeta.TableName, states, errorMsg, retryDelay); err != nil {
		return BatchResult{}, err
	}

	matched := make(map[matchKey]bool, len(states))
	for _, s := range states {
		matched[matchKey{s.id, s.claimID}] = true
	}

	msgTable := pq.msgTable(queueMeta.TableName)
	return pq.finishBatch(tx, channelName, receipts, matched,
		func(misses []Receipt) ([]FailedReceipt, error) {
			return classifyChannelBatchMisses(ctx, tx, msgTable, misses)
		})
}

// nackTopicBatch negatively acknowledges multiple messages for a subscriber
// from a topic and returns the per-receipt outcome. Subscriptions that matched
// a live processing claim are retried or moved to DLQ (when retries are
// exhausted) and appear in BatchResult.Succeeded; the rest are classified
// (ErrClaimExpired / ErrMessageAlreadyAcked / ErrMessageNotFound) and appear in
// BatchResult.Failed. Partial success is not an error. The errorMsg is
// truncated to 1024 characters if it exceeds that length. A WithRetryDelay
// option overrides the computed backoff delay for the batch. Each failed
// receipt increments the RecordAckAfterExpired metric.
func (pq *Queue) nackTopicBatch(
	ctx context.Context,
	topicName string,
	subscriberID string,
	receipts []Receipt,
	errorMsg string,
	opts ...NackOption,
) (BatchResult, error) {
	errorMsg = truncateErrorMsg(errorMsg)
	retryDelay := resolveNackRetryDelay(opts)

	if err := validateSubscriberID(subscriberID); err != nil {
		return BatchResult{}, err
	}
	if len(receipts) == 0 {
		return BatchResult{}, nil
	}
	if err := validateBatchSize(len(receipts)); err != nil {
		return BatchResult{}, err
	}

	queueMeta, err := pq.getTopicMetadata(ctx, topicName)
	if err != nil {
		return BatchResult{}, err
	}

	tx, err := pq.db.BeginTx(ctx, readCommittedTxOptions)
	if err != nil {
		return BatchResult{}, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	states, err := pq.fetchBatchSubStates(
		ctx, tx, queueMeta.TableName, receipts, subscriberID,
	)
	if err != nil {
		return BatchResult{}, fmt.Errorf("failed to get subscription states: %w", err)
	}

	maxRetry := pq.resolveMaxRetries(queueMeta)

	if err := pq.processNackTopicBatch(
		ctx, tx, queueMeta.TableName, subscriberID, states, maxRetry, errorMsg, retryDelay,
	); err != nil {
		return BatchResult{}, err
	}

	matched := make(map[matchKey]bool, len(states))
	for _, s := range states {
		matched[matchKey{s.messageID, s.claimID}] = true
	}

	subTable := pq.subTable(queueMeta.TableName)
	return pq.finishBatch(tx, topicName, receipts, matched,
		func(misses []Receipt) ([]FailedReceipt, error) {
			return classifyTopicBatchMisses(ctx, tx, subTable, subscriberID, misses)
		})
}

// getTopicMetadata retrieves topic metadata, translating not-found to ErrTopicNotFound.
func (pq *Queue) getTopicMetadata(
	ctx context.Context,
	topicName string,
) (*queueMetadata, error) {
	queueMeta, err := pq.getQueueMetadata(
		ctx, string(QueueTypePubSub), topicName,
	)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return nil, fmt.Errorf("%s: %w", topicName, ErrTopicNotFound)
		}
		return nil, fmt.Errorf("failed to get topic metadata: %w", err)
	}
	return queueMeta, nil
}

// processNackBatch partitions messages into retry vs DLQ and processes each
// group. retryDelay, when positive, overrides the computed backoff delay for
// every retried message (WithRetryDelay); zero uses the queue's BackoffPolicy.
func (pq *Queue) processNackBatch(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	states []batchMessageState,
	errorMsg string,
	retryDelay time.Duration,
) error {
	var retryIDs []uuid.UUID
	var retryDelays []float64
	var dlqMessages []batchDLQMessage

	for _, s := range states {
		maxRetry := pq.cfg.defaultMaxRetries
		if s.maxRetries.Valid {
			maxRetry = int(s.maxRetries.Int64)
		}

		if s.retryCount+1 > maxRetry {
			dlqMessages = append(dlqMessages, batchDLQMessage{
				id:           s.id,
				payload:      s.payload,
				retryCount:   s.retryCount + 1,
				metadataJSON: s.metadataJSON,
			})
		} else {
			// Each retried message carries its own backoff delay so a batch
			// nack honors the queue's BackoffPolicy — or the WithRetryDelay
			// override — exactly as a single Nack does (FR-023).
			retryIDs = append(retryIDs, s.id)
			retryDelays = append(retryDelays, pq.computeRetryDelay(s.retryCount+1, retryDelay).Seconds())
		}
	}

	if len(retryIDs) > 0 {
		if err := pq.batchRetryMessages(
			ctx, tx, tableName, retryIDs, retryDelays, errorMsg,
		); err != nil {
			return fmt.Errorf("failed to retry messages: %w", err)
		}
	}

	if len(dlqMessages) > 0 {
		if err := pq.batchMoveToDLQ(
			ctx, tx, tableName, dlqMessages, errorMsg,
		); err != nil {
			return fmt.Errorf("failed to move messages to DLQ: %w", err)
		}
	}

	return nil
}

// validateBatchSize checks if the batch size is within the allowed limit.
func validateBatchSize(size int) error {
	if size > MaxBatchSize {
		return fmt.Errorf(
			"batch size %d exceeds limit %d: %w",
			size, MaxBatchSize, ErrBatchTooLarge,
		)
	}
	return nil
}

// prepareBatchMessages generates IDs and marshals metadata for all messages,
// rejecting any message whose marshaled metadata exceeds the configured cap.
func (pq *Queue) prepareBatchMessages(
	queueMeta *queueMetadata,
	messages []PublishMessage,
) ([]uuid.UUID, [][]byte, error) {
	ids := make([]uuid.UUID, len(messages))
	metadataJSONs := make([][]byte, len(messages))

	// Resolve the metadata-size cap once for the whole batch rather than
	// re-unmarshaling the queue config per message in the hot path.
	maxMetaSize := pq.resolveMaxMetadataSize(queueMeta)

	for i := range messages {
		// A caller-supplied ID is honored for publish-side dedup; uuid.Nil means
		// auto-generate a UUIDv7. Duplicate IDs are rejected by the INSERT's
		// ON CONFLICT (id) DO NOTHING + rowsAffected check (ErrDuplicateMessageID).
		id := messages[i].ID
		if id == uuid.Nil {
			var err error
			id, err = NewUUIDv7()
			if err != nil {
				return nil, nil, fmt.Errorf("failed to generate message ID: %w", err)
			}
		}
		ids[i] = id

		var err error
		metadataJSONs[i], err = marshalMetadataWithLimit(maxMetaSize, messages[i].Metadata)
		if err != nil {
			return nil, nil, err
		}
	}

	return ids, metadataJSONs, nil
}

// publishBatchToChannel inserts multiple messages into a channel using multi-row INSERT.
func (pq *Queue) publishBatchToChannel(
	ctx context.Context,
	tableName string,
	ids []uuid.UUID,
	messages []PublishMessage,
	metadataJSONs [][]byte,
	maxRetries int,
) error {
	return pq.withTx(ctx, func(tx *sql.Tx) error {
		var sb strings.Builder
		fmt.Fprintf(&sb,
			"INSERT INTO %s (id, payload, status, metadata, max_retries) VALUES ",
			pq.msgTable(tableName),
		)

		args := make([]any, 0, len(messages)*channelInsertParams)
		for i := range messages {
			if i > 0 {
				sb.WriteString(", ")
			}
			base := i * channelInsertParams
			fmt.Fprintf(&sb, "($%d, $%d, '%s', $%d, $%d)",
				base+1, base+2, MessageStatusPending, base+3, base+4, //nolint:mnd // SQL placeholder arithmetic
			)
			args = append(args, ids[i], messages[i].Payload, jsonbParam(metadataJSONs[i]), maxRetries)
		}
		sb.WriteString(" ON CONFLICT (id) DO NOTHING")

		result, err := tx.ExecContext(ctx, sb.String(), args...)
		if err != nil {
			return fmt.Errorf("failed to insert messages: %w", err)
		}

		rowsAffected, err := rowsAffectedOrErr(result)
		if err != nil {
			return err
		}
		if rowsAffected < int64(len(messages)) {
			return fmt.Errorf(
				"some messages had duplicate IDs: %w", ErrDuplicateMessageID,
			)
		}

		// Wake any blocked consumer the instant this batch publish commits (FR-014).
		pq.emitNotify(ctx, tx, tableName)

		return nil
	})
}

// publishBatchToPubSub inserts multiple messages and subscription records in a transaction.
func (pq *Queue) publishBatchToPubSub(
	ctx context.Context,
	topicName, tableName string,
	ids []uuid.UUID,
	messages []PublishMessage,
	metadataJSONs [][]byte,
) error {
	return pq.withTx(ctx, func(tx *sql.Tx) error {
		if err := pq.insertBatchPubSubMessages(ctx, tx, tableName, ids, messages, metadataJSONs); err != nil {
			return err
		}

		subscribers, err := pq.getActiveSubscribers(ctx, tx, topicName)
		if err != nil {
			return fmt.Errorf("failed to get active subscribers: %w", err)
		}

		if len(subscribers) > 0 {
			if err := pq.batchCreateSubscriptionRecords(
				ctx, tx, tableName, ids, subscribers,
			); err != nil {
				return fmt.Errorf("failed to create subscription records: %w", err)
			}
		}

		// Wake any blocked consumer the instant this batch publish commits (FR-014).
		pq.emitNotify(ctx, tx, tableName)

		return nil
	})
}

func (pq *Queue) insertBatchPubSubMessages(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	ids []uuid.UUID,
	messages []PublishMessage,
	metadataJSONs [][]byte,
) error {
	var sb strings.Builder
	fmt.Fprintf(&sb,
		"INSERT INTO %s (id, payload, metadata) VALUES ",
		pq.msgTable(tableName),
	)

	args := make([]any, 0, len(messages)*pubsubInsertParams)
	for i := range messages {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * pubsubInsertParams
		fmt.Fprintf(&sb, "($%d, $%d, $%d)",
			base+1, base+2, base+3, //nolint:mnd // SQL placeholder arithmetic
		)
		args = append(args, ids[i], messages[i].Payload, jsonbParam(metadataJSONs[i]))
	}
	sb.WriteString(" ON CONFLICT (id) DO NOTHING")

	result, err := tx.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("failed to insert messages: %w", err)
	}

	rowsAffected, err := rowsAffectedOrErr(result)
	if err != nil {
		return err
	}
	if rowsAffected < int64(len(messages)) {
		return fmt.Errorf(
			"some messages had duplicate IDs: %w", ErrDuplicateMessageID,
		)
	}

	return nil
}

// subRecord is a (message, subscriber) pair to be written to a pgqueue_sub_ table.
type subRecord struct {
	messageID    uuid.UUID
	subscriberID string
}

// batchCreateSubscriptionRecords creates subscription records for the cross
// product of the given messages and subscribers.
func (pq *Queue) batchCreateSubscriptionRecords(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	messageIDs []uuid.UUID,
	subscribers []subscriber,
) error {
	// Stream the messages×subscribers cross product into the insert in bounded
	// chunks instead of materializing the whole product first: a large batch
	// fanned out to a large subscriber set is len(messageIDs)*len(subscribers)
	// rows, which could be tens of millions of subRecords held in memory at once
	// (H5). Capping the working set to subFanoutChunkRows keeps that footprint
	// flat regardless of batch or subscriber count.
	buf := make([]subRecord, 0, subFanoutChunkRows)
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		if err := pq.insertSubscriptionRecords(ctx, tx, tableName, buf); err != nil {
			return err
		}
		buf = buf[:0]
		return nil
	}
	for _, msgID := range messageIDs {
		for _, sub := range subscribers {
			buf = append(buf, subRecord{messageID: msgID, subscriberID: sub.SubscriberID})
			if len(buf) == subFanoutChunkRows {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
	return flush()
}

// insertSubscriptionRecords inserts subscription rows, chunked to stay within
// PostgreSQL's parameter limit. ON CONFLICT DO NOTHING makes it idempotent and
// tolerant of duplicate pairs within records.
func (pq *Queue) insertSubscriptionRecords(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	records []subRecord,
) error {
	for start := 0; start < len(records); start += subFanoutChunkRows {
		end := min(start+subFanoutChunkRows, len(records))
		chunk := records[start:end]

		var sb strings.Builder
		fmt.Fprintf(&sb,
			"INSERT INTO %s (message_id, subscriber_id, status) VALUES ",
			pq.subTable(tableName),
		)

		args := make([]any, 0, len(chunk)*subInsertParams)
		for i, rec := range chunk {
			if i > 0 {
				sb.WriteString(", ")
			}
			base := i * subInsertParams
			fmt.Fprintf(&sb, "($%d, $%d, '%s')", base+1, base+2, MessageStatusPending) //nolint:mnd // SQL placeholder arithmetic
			args = append(args, rec.messageID, rec.subscriberID)
		}
		sb.WriteString(" ON CONFLICT (message_id, subscriber_id) DO NOTHING")

		if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
			return fmt.Errorf("failed to insert subscription records: %w", err)
		}
	}

	return nil
}

type batchMessageState struct {
	id           uuid.UUID
	claimID      uuid.UUID
	retryCount   int
	maxRetries   sql.NullInt64
	payload      []byte
	metadataJSON sql.NullString
}

type batchDLQMessage struct {
	id           uuid.UUID
	payload      []byte
	retryCount   int
	metadataJSON sql.NullString
}

func (pq *Queue) fetchBatchMessageStates(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	receipts []Receipt,
) ([]batchMessageState, error) {
	// Match (id, claim_id) pairs: a receipt whose claim token no longer matches
	// (the message was reclaimed by another consumer) is simply not returned.
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		SELECT m.id, m.claim_id, m.retry_count, m.max_retries, m.payload, m.metadata
		FROM %s AS m
		JOIN unnest($1::text::uuid[], $2::text::uuid[]) AS u(id, claim_id)
		  ON m.id = u.id AND m.claim_id = u.claim_id
		WHERE m.status = '%s'
		ORDER BY m.id
		FOR UPDATE OF m
	`, pq.msgTable(tableName), MessageStatusProcessing)

	ids, claims := receiptsToIDClaimLiterals(receipts)
	rows, err := tx.QueryContext(ctx, query, ids, claims)
	if err != nil {
		return nil, fmt.Errorf("failed to query messages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var states []batchMessageState
	for rows.Next() {
		var s batchMessageState
		if err := rows.Scan(
			&s.id, &s.claimID, &s.retryCount, &s.maxRetries,
			&s.payload, &s.metadataJSON,
		); err != nil {
			return nil, fmt.Errorf("failed to scan message: %w", err)
		}
		states = append(states, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate messages: %w", err)
	}

	return states, nil
}

// batchRetryMessages reinstates a batch of channel messages to pending. The
// messageIDs and delaySeconds slices are index-aligned: each message's
// available_at is pushed out by its own backoff delay so a batch nack respects
// the BackoffPolicy per message (FR-023).
func (pq *Queue) batchRetryMessages(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	messageIDs []uuid.UUID,
	delaySeconds []float64,
	errorMsg string,
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
	// claim_id is cleared so stale receipts from the previous consumer
	// resolve to ErrClaimExpired rather than ErrMessageAlreadyAcked.
	query := fmt.Sprintf(`
		UPDATE %s AS m
		SET status = '%s',
		    claim_id = NULL,
		    retry_count = retry_count + 1,
		    visibility_timeout = NULL,
		    available_at = NOW() + make_interval(secs => u.delay),
		    error_message = $3
		FROM unnest($1::text::uuid[], $2::text::float8[]) AS u(id, delay)
		WHERE m.id = u.id
	`, pq.msgTable(tableName), MessageStatusPending)

	_, err := tx.ExecContext(
		ctx, query, uuidArrayLiteral(messageIDs), float64ArrayLiteral(delaySeconds), errorMsg,
	)
	if err != nil {
		return fmt.Errorf("failed to update messages: %w", err)
	}

	return nil
}

func (pq *Queue) batchMoveToDLQ(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	messages []batchDLQMessage,
	errorMsg string,
) error {
	// Defense-in-depth: the DLQ failure_reason column is the durable sink, so
	// bound the message here regardless of whether the caller already truncated
	// it (L4). truncateErrorMsg is idempotent, so re-truncating an already-bounded
	// message from nackChannelBatch is a no-op.
	errorMsg = truncateErrorMsg(errorMsg)

	dlqInsert := fmt.Sprintf(
		`INSERT INTO %s (original_message_id, payload, failure_reason, retry_count, metadata) VALUES `,
		pq.dlqTable(tableName),
	)

	var sb strings.Builder
	sb.WriteString(dlqInsert)

	args := make([]any, 0, len(messages)*dlqInsertParams)
	for i, msg := range messages {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * dlqInsertParams
		fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, //nolint:mnd // SQL placeholder arithmetic
		)
		args = append(args, msg.id, msg.payload, errorMsg, msg.retryCount, msg.metadataJSON)
	}

	if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("failed to insert into DLQ: %w", err)
	}

	dlqIDs := make([]uuid.UUID, len(messages))
	for i, msg := range messages {
		dlqIDs[i] = msg.id
	}

	//nolint:gosec // G201: table name validated by queueNameRegex
	delQuery := fmt.Sprintf(
		`DELETE FROM %s WHERE id = ANY($1::text::uuid[])`,
		pq.msgTable(tableName),
	)
	if _, err := tx.ExecContext(ctx, delQuery, uuidArrayLiteral(dlqIDs)); err != nil {
		return fmt.Errorf("failed to delete messages: %w", err)
	}

	return nil
}

type batchSubState struct {
	messageID    uuid.UUID
	claimID      uuid.UUID
	retryCount   int
	payload      []byte
	metadataJSON sql.NullString
}

func (pq *Queue) fetchBatchSubStates(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	receipts []Receipt,
	subscriberID string,
) ([]batchSubState, error) {
	// Match (message_id, claim_id) pairs: a receipt whose claim token no longer
	// matches (the subscription was reclaimed) is simply not returned.
	//nolint:gosec // G201: table name validated by queueNameRegex
	query := fmt.Sprintf(`
		SELECT s.message_id, s.claim_id, s.retry_count, m.payload, m.metadata
		FROM %s s
		JOIN %s m ON s.message_id = m.id
		JOIN unnest($1::text::uuid[], $2::text::uuid[]) AS u(message_id, claim_id)
		  ON s.message_id = u.message_id AND s.claim_id = u.claim_id
		WHERE s.subscriber_id = $3
		  AND s.status = '%s'
		ORDER BY s.message_id
		FOR UPDATE OF s
	`, pq.subTable(tableName), pq.msgTable(tableName), MessageStatusProcessing)

	ids, claims := receiptsToIDClaimLiterals(receipts)
	rows, err := tx.QueryContext(ctx, query, ids, claims, subscriberID)
	if err != nil {
		return nil, fmt.Errorf("failed to query subscriptions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var states []batchSubState
	for rows.Next() {
		var s batchSubState
		if err := rows.Scan(
			&s.messageID, &s.claimID, &s.retryCount,
			&s.payload, &s.metadataJSON,
		); err != nil {
			return nil, fmt.Errorf("failed to scan subscription: %w", err)
		}
		states = append(states, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate subscriptions: %w", err)
	}

	return states, nil
}

// processNackTopicBatch partitions subscriptions into retry vs DLQ and
// processes each group. retryDelay, when positive, overrides the computed
// backoff delay for every retried subscription (WithRetryDelay).
func (pq *Queue) processNackTopicBatch(
	ctx context.Context,
	tx *sql.Tx,
	tableName, subscriberID string,
	states []batchSubState,
	maxRetry int,
	errorMsg string,
	retryDelay time.Duration,
) error {
	var retryIDs []uuid.UUID
	var retryDelays []float64
	var dlqMessages []batchDLQMessage

	for _, s := range states {
		if s.retryCount+1 > maxRetry {
			dlqMessages = append(dlqMessages, batchDLQMessage{
				id:           s.messageID,
				payload:      s.payload,
				retryCount:   s.retryCount + 1,
				metadataJSON: s.metadataJSON,
			})
		} else {
			// Per-subscription backoff delay so a batch nack honors the
			// BackoffPolicy — or the WithRetryDelay override — exactly as a
			// single Nack does (FR-023).
			retryIDs = append(retryIDs, s.messageID)
			retryDelays = append(retryDelays, pq.computeRetryDelay(s.retryCount+1, retryDelay).Seconds())
		}
	}

	if len(retryIDs) > 0 {
		if err := pq.batchRetrySubscriptions(
			ctx, tx, tableName, retryIDs, retryDelays, subscriberID, errorMsg,
		); err != nil {
			return fmt.Errorf("failed to retry subscriptions: %w", err)
		}
	}

	if len(dlqMessages) > 0 {
		if err := pq.batchMoveSubToDLQ(
			ctx, tx, tableName, subscriberID, dlqMessages, errorMsg,
		); err != nil {
			return fmt.Errorf("failed to move subscriptions to DLQ: %w", err)
		}
	}

	return nil
}

// batchRetrySubscriptions reinstates a batch of subscriptions to pending. The
// messageIDs and delaySeconds slices are index-aligned: each subscription's
// available_at is pushed out by its own backoff delay (FR-023).
func (pq *Queue) batchRetrySubscriptions(
	ctx context.Context,
	tx *sql.Tx,
	tableName string,
	messageIDs []uuid.UUID,
	delaySeconds []float64,
	subscriberID, errorMsg string,
) error {
	//nolint:gosec // G201: table name validated by queueNameRegex
	// claim_id is cleared so stale receipts from the previous consumer
	// resolve to ErrClaimExpired rather than ErrMessageAlreadyAcked.
	query := fmt.Sprintf(`
		UPDATE %s AS s
		SET status = '%s',
		    claim_id = NULL,
		    retry_count = retry_count + 1,
		    visibility_timeout = NULL,
		    available_at = NOW() + make_interval(secs => u.delay),
		    error_message = $4
		FROM unnest($1::text::uuid[], $2::text::float8[]) AS u(message_id, delay)
		WHERE s.message_id = u.message_id
		  AND s.subscriber_id = $3
	`, pq.subTable(tableName), MessageStatusPending)

	_, err := tx.ExecContext(
		ctx, query, uuidArrayLiteral(messageIDs), float64ArrayLiteral(delaySeconds), subscriberID, errorMsg,
	)
	if err != nil {
		return fmt.Errorf("failed to update subscriptions: %w", err)
	}

	return nil
}

func (pq *Queue) batchMoveSubToDLQ(
	ctx context.Context,
	tx *sql.Tx,
	tableName, subscriberID string,
	messages []batchDLQMessage,
	errorMsg string,
) error {
	// Defense-in-depth truncation at the durable DLQ failure_reason sink (L4);
	// idempotent, so the entry-point truncation in nackTopicBatch is preserved.
	errorMsg = truncateErrorMsg(errorMsg)

	// Batch insert into DLQ, recording which subscriber failed.
	dlqPrefix := fmt.Sprintf(
		`INSERT INTO %s `+
			`(original_message_id, subscriber_id, payload, failure_reason, retry_count, metadata) VALUES `,
		pq.dlqTable(tableName),
	)

	var sb strings.Builder
	sb.WriteString(dlqPrefix)

	args := make([]any, 0, len(messages)*subDLQInsertParams)
	for i, msg := range messages {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * subDLQInsertParams
		fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, //nolint:mnd // SQL placeholder arithmetic
		)
		args = append(args, msg.id, subscriberID, msg.payload, errorMsg, msg.retryCount, msg.metadataJSON)
	}

	if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("failed to insert into DLQ: %w", err)
	}

	// Delete subscription records
	dlqIDs := make([]uuid.UUID, len(messages))
	for i, msg := range messages {
		dlqIDs[i] = msg.id
	}

	//nolint:gosec // G201: table name validated by queueNameRegex
	deleteQuery := fmt.Sprintf(
		`DELETE FROM %s WHERE message_id = ANY($1::text::uuid[]) AND subscriber_id = $2`,
		pq.subTable(tableName),
	)
	if _, err := tx.ExecContext(ctx, deleteQuery, uuidArrayLiteral(dlqIDs), subscriberID); err != nil {
		return fmt.Errorf("failed to delete subscriptions: %w", err)
	}

	return nil
}
