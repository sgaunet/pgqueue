package pgqueue

// consume.go holds the three layered consume APIs: single-shot (ReceiveChannel,
// ReceiveTopic), iterator (ChannelMessages, TopicMessages), and handler-based
// (ConsumeChannel, ConsumeTopic). It also holds the queue-agnostic Ack/Nack and
// their batch variants.

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"runtime/debug"
	"sync"
	"time"
)

// defaultPollInterval is how long a blocking consume loop waits between polls
// when the queue is empty and no WithPollInterval override was supplied. Push
// delivery via LISTEN/NOTIFY (US3) shortens the effective latency well below
// this; the poll is the safety net.
const defaultPollInterval = 1 * time.Second

// ackGracePeriod bounds the auto-ack/nack issued after a handler returns. The
// ack runs on a context detached from cancellation so a handler that finished
// just as shutdown began still records its result; the timeout keeps a hung
// database from making that detached ack — and therefore Queue.Close, which
// joins the handler loops — block forever.
const ackGracePeriod = 30 * time.Second

// Transient-error retry bounds for the consume loops (FR-026): a transient
// database failure is retried with a short escalating backoff up to
// maxConsumeTransientRetries consecutive times before it is treated as fatal.
const (
	maxConsumeTransientRetries = 5
	transientBackoffStep       = 200 * time.Millisecond
	transientBackoffCap        = 5 * time.Second
)

// maxConcurrency bounds WithConcurrency so a mistaken huge value cannot spawn an
// unbounded worker pool — and open that many pooled database connections. A
// value above it is clamped to it rather than rejected (M8).
const maxConcurrency = 1024

// validateConsumeOptions rejects clearly-invalid per-consume options at the
// handler-loop entry points (ConsumeChannel/ConsumeTopic), which can surface the
// error — rather than silently coercing a caller mistake (L10). Zero means "use
// the default"; a negative concurrency or poll interval is a bug.
func validateConsumeOptions(o consumeOpts) error {
	if o.concurrency < 0 {
		return fmt.Errorf(
			"%w: WithConcurrency must be >= 1 (0 uses the default of 1), got %d",
			ErrInvalidConfig, o.concurrency)
	}
	if o.pollInterval < 0 {
		return fmt.Errorf(
			"%w: WithPollInterval must be > 0 (0 uses the default), got %v",
			ErrInvalidConfig, o.pollInterval)
	}
	return nil
}

// transientBackoff returns the wait before the attempt-th consecutive
// transient-error retry: a linear escalation capped at transientBackoffCap.
func transientBackoff(attempt int) time.Duration {
	d := time.Duration(attempt) * transientBackoffStep
	if d > transientBackoffCap {
		return transientBackoffCap
	}
	return d
}

// Handler is a callback invoked by the handler-based consume loop
// (ConsumeChannel/ConsumeTopic) for each delivered message. Return nil to
// auto-ack; return any non-nil error to auto-nack (the error string is recorded
// as the failure reason).
//
// The handler runs under the consume loop's context. That context is cancelled
// when the caller's context is cancelled or when Queue.Close is called, and
// Close blocks until every in-flight handler returns (it joins the worker
// loops). A handler that ignores ctx cancellation therefore delays Close by up
// to its own remaining run time, so long-running handlers should honor ctx (L2).
type Handler func(ctx context.Context, msg *Message) error

// ReceiveChannel retrieves the next available message from a point-to-point
// channel. Returns ErrQueueEmpty when no message is currently available.
//
// The visibility timeout (default 30 s, override with WithVisibilityTimeout)
// controls how long the caller has to Ack or Nack the message before it
// becomes eligible for redelivery.
//
// The returned Message's Receipt() is pre-populated with the channel binding so
// the queue-agnostic Ack/Nack methods can be used without supplying the queue
// name again.
func (pq *Queue) ReceiveChannel(
	ctx context.Context,
	name string,
	opts ...ConsumeOption,
) (*Message, error) {
	if err := pq.checkClosed(); err != nil {
		return nil, err
	}
	// applyConsumeOptions fills the default when WithVisibilityTimeout is absent;
	// an explicit out-of-range value (including 0) flows through to
	// consumeFromChannel and is rejected with ErrInvalidVisibilityTimeout.
	o := applyConsumeOptions(opts)

	msg, err := pq.consumeFromChannel(ctx, name, o.visibilityTimeout)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, ErrQueueEmpty
	}

	// Stamp queue binding onto the message so Receipt() returns a full Receipt.
	msg.SetReceipt(Receipt{
		MessageID: msg.ID,
		ClaimID:   msg.ClaimID,
		QueueName: name,
		QueueType: QueueTypeChannel,
	})

	return msg, nil
}

// ReceiveTopic retrieves the next available message for a subscriber from a
// pub/sub topic. Returns ErrQueueEmpty when no message is currently available.
//
// See ReceiveChannel for visibility-timeout semantics.
func (pq *Queue) ReceiveTopic(
	ctx context.Context,
	name, subscriberID string,
	opts ...ConsumeOption,
) (*Message, error) {
	if err := pq.checkClosed(); err != nil {
		return nil, err
	}
	o := applyConsumeOptions(opts)

	msg, err := pq.consumeFromTopic(ctx, name, subscriberID, o.visibilityTimeout)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, ErrQueueEmpty
	}

	msg.SetReceipt(Receipt{
		MessageID:    msg.ID,
		ClaimID:      msg.ClaimID,
		QueueName:    name,
		QueueType:    QueueTypePubSub,
		SubscriberID: subscriberID,
	})

	return msg, nil
}

// Ack acknowledges a message using the Receipt returned by ReceiveChannel or
// ReceiveTopic. The Receipt must carry the queue binding (QueueName + QueueType)
// which is set automatically by those methods.
//
// Returns ErrClaimExpired if the visibility timeout lapsed and the message was
// redelivered to another consumer.
func (pq *Queue) Ack(ctx context.Context, r Receipt) error {
	if err := pq.checkClosed(); err != nil {
		return err
	}
	return pq.ackReceipt(ctx, r)
}

// ackReceipt performs the acknowledgement without the closed-state gate that
// the public Ack applies. Queue.Close marks the Queue closed *before* it joins
// the handler-based consume loops, so the auto-ack dispatchToHandler issues for
// a handler that finished just as shutdown began must bypass checkClosed —
// otherwise every in-flight message at shutdown would fail to ack and be
// needlessly redelivered after its visibility timeout. Direct callers still go
// through Ack and keep the closed gate.
func (pq *Queue) ackReceipt(ctx context.Context, r Receipt) error {
	ctx, span := pq.startSpan(ctx, "pgqueue.ack",
		StringAttr("queue", r.QueueName),
		StringAttr("message_id", r.MessageID.String()))

	var err error
	switch r.QueueType {
	case QueueTypeChannel:
		err = pq.ackChannel(ctx, r.QueueName, r)
	case QueueTypePubSub:
		err = pq.ackTopic(ctx, r.QueueName, r.SubscriberID, r)
	default:
		err = ErrReceiptMissingQueueType
	}

	pq.endSpan(span, err)
	if err == nil {
		pq.recordAck(r.QueueName, true)
		return nil
	}
	return receiptError(r, err)
}

// Nack negatively acknowledges a message using the Receipt returned by
// ReceiveChannel or ReceiveTopic. The message is retried (or moved to the DLQ
// if max retries are exhausted). reason is recorded as the failure reason.
//
// Returns ErrClaimExpired if the visibility timeout lapsed.
func (pq *Queue) Nack(
	ctx context.Context,
	r Receipt,
	reason string,
	opts ...NackOption,
) error {
	if err := pq.checkClosed(); err != nil {
		return err
	}
	return pq.nackReceipt(ctx, r, reason, opts...)
}

// nackReceipt performs the negative acknowledgement without the closed-state
// gate that the public Nack applies — see ackReceipt for why the handler-loop
// auto-nack must bypass it.
func (pq *Queue) nackReceipt(
	ctx context.Context,
	r Receipt,
	reason string,
	opts ...NackOption,
) error {
	ctx, span := pq.startSpan(ctx, "pgqueue.nack",
		StringAttr("queue", r.QueueName),
		StringAttr("message_id", r.MessageID.String()))

	var err error
	switch r.QueueType {
	case QueueTypeChannel:
		err = pq.nackChannelWithOpts(ctx, r.QueueName, r, reason, opts...)
	case QueueTypePubSub:
		err = pq.nackTopicWithOpts(ctx, r.QueueName, r.SubscriberID, r, reason, opts...)
	default:
		err = ErrReceiptMissingQueueType
	}

	pq.endSpan(span, err)
	if err == nil {
		pq.recordAck(r.QueueName, false)
		return nil
	}
	return receiptError(r, err)
}

// receiptError enriches a single-receipt Ack/Nack failure with the queue,
// message, and (for topics) subscriber it concerns, so a bare ErrClaimExpired /
// ErrMessageNotFound is actionable from a log line without the caller
// re-deriving the binding (D8). The sentinel stays matchable via errors.Is
// because err is wrapped with %w. A receipt with no valid QueueType has no
// binding to attach, so its error is returned unwrapped.
func receiptError(r Receipt, err error) error {
	switch r.QueueType {
	case QueueTypeChannel:
		return fmt.Errorf("channel %s, message %s: %w", r.QueueName, r.MessageID, err)
	case QueueTypePubSub:
		return fmt.Errorf("topic %s, subscriber %s, message %s: %w",
			r.QueueName, r.SubscriberID, r.MessageID, err)
	default:
		return err
	}
}

// receiptGroupKey identifies the set of receipts that target the same queue
// (and, for topics, the same subscriber), so AckBatch/NackBatch can issue one
// batch call per group.
type receiptGroupKey struct {
	qt           QueueType
	queueName    string
	subscriberID string
}

// groupReceiptsByQueue partitions receipts by their queue binding. It returns
// ErrReceiptMissingQueueType if any receipt carries no valid QueueType, so a
// malformed receipt fails fast — and consistently with the single-message
// Ack/Nack — rather than being silently dropped (which would leave its message
// un-acked, to be needlessly redelivered after the visibility timeout).
func groupReceiptsByQueue(rs []Receipt) (map[receiptGroupKey][]Receipt, error) {
	groups := make(map[receiptGroupKey][]Receipt)
	// Collapse exact-duplicate receipts so a caller that passes the same receipt
	// twice does not drive its message through the batch twice. A duplicated
	// receipt in a Nack batch otherwise unnest-joins to two state rows for the one
	// locked message and moves it to the DLQ more than once — the set-based Ack and
	// retry UPDATEs touch each target row once and are immune, but the row-by-row
	// DLQ insert is not. BatchResult already documents receipts as unique within a
	// batch, so collapsing duplicates here honors that contract rather than
	// changing it. Receipt is fully comparable, so its value is the dedup key.
	seen := make(map[Receipt]bool, len(rs))
	for _, r := range rs {
		if r.QueueType != QueueTypeChannel && r.QueueType != QueueTypePubSub {
			return nil, ErrReceiptMissingQueueType
		}
		if seen[r] {
			continue
		}
		seen[r] = true
		k := receiptGroupKey{qt: r.QueueType, queueName: r.QueueName, subscriberID: r.SubscriberID}
		groups[k] = append(groups[k], r)
	}
	return groups, nil
}

// AckBatch acknowledges multiple messages using their Receipts. Receipts are
// grouped by queue and acknowledged in a single batch operation per queue. Each
// Receipt must carry the queue binding set by ReceiveChannel/ReceiveTopic.
//
// The returned BatchResult enumerates the per-receipt outcome: Succeeded lists
// the receipts that were acked, and Failed lists those whose claim no longer
// matched a processing message, each with the reason (ErrClaimExpired,
// ErrMessageAlreadyAcked, or ErrMessageNotFound). Partial success is not an
// error: a batch where some claims have expired returns a nil error with those
// receipts in Failed.
//
// Receipts are grouped by queue and each group is acked in its own transaction,
// so an operational failure in one group (a missing queue, a database error)
// does not roll back the groups that already committed. Such a failure is
// returned as a joined error while BatchResult still carries the succeeded and
// failed receipts of every group that did commit — the result is never silently
// zeroed (M1). Pre-flight failures that reject the whole call before any group
// runs (the queue is closed, a receipt is missing its queue binding) still
// return a zero BatchResult.
func (pq *Queue) AckBatch(ctx context.Context, rs []Receipt) (BatchResult, error) {
	if err := pq.checkClosed(); err != nil {
		return BatchResult{}, err
	}
	if len(rs) == 0 {
		return BatchResult{}, nil
	}
	groups, err := groupReceiptsByQueue(rs)
	if err != nil {
		return BatchResult{}, err
	}
	var combined BatchResult
	var errs []error
	for k, receipts := range groups {
		var res BatchResult
		var err error
		switch k.qt {
		case QueueTypeChannel:
			res, err = withBatchRetry(ctx, func() (BatchResult, error) {
				return pq.ackChannelBatch(ctx, k.queueName, receipts)
			})
		case QueueTypePubSub:
			res, err = withBatchRetry(ctx, func() (BatchResult, error) {
				return pq.ackTopicBatch(ctx, k.queueName, k.subscriberID, receipts)
			})
		default:
			continue
		}
		if err != nil {
			// Each group commits in its own transaction, so a failure here does
			// not undo groups that already committed. Record it and keep going;
			// the caller gets the joined error alongside the committed results
			// rather than a silently zeroed BatchResult (M1).
			errs = append(errs, err)
			continue
		}
		combined.Succeeded = append(combined.Succeeded, res.Succeeded...)
		combined.Failed = append(combined.Failed, res.Failed...)
	}
	return combined, errors.Join(errs...)
}

// NackBatch negatively acknowledges multiple messages using their Receipts.
// Messages that have exhausted retries are moved to the DLQ; others are
// retried. reason is recorded as the failure reason. A WithRetryDelay option
// overrides the computed backoff delay for every message in the batch.
//
// The returned BatchResult enumerates the per-receipt outcome the same way as
// AckBatch: Succeeded lists the receipts that were nacked (retried or moved to
// DLQ), and Failed lists those whose claim no longer matched, each with the
// reason. Partial success is not an error. As with AckBatch, receipts are
// grouped by queue and each group is nacked in its own transaction: an
// operational failure in one group is returned as a joined error while
// BatchResult still carries every already-committed group's results, rather than
// being zeroed (M1). Pre-flight failures (closed queue, missing queue binding)
// still return a zero BatchResult.
func (pq *Queue) NackBatch(
	ctx context.Context,
	rs []Receipt,
	reason string,
	opts ...NackOption,
) (BatchResult, error) {
	if err := pq.checkClosed(); err != nil {
		return BatchResult{}, err
	}
	if len(rs) == 0 {
		return BatchResult{}, nil
	}
	groups, err := groupReceiptsByQueue(rs)
	if err != nil {
		return BatchResult{}, err
	}
	var combined BatchResult
	var errs []error
	for k, receipts := range groups {
		var res BatchResult
		var err error
		switch k.qt {
		case QueueTypeChannel:
			res, err = withBatchRetry(ctx, func() (BatchResult, error) {
				return pq.nackChannelBatch(ctx, k.queueName, receipts, reason, opts...)
			})
		case QueueTypePubSub:
			res, err = withBatchRetry(ctx, func() (BatchResult, error) {
				return pq.nackTopicBatch(ctx, k.queueName, k.subscriberID, receipts, reason, opts...)
			})
		default:
			continue
		}
		if err != nil {
			// Independent per-group transaction: preserve committed groups'
			// results and join the error rather than zeroing everything (M1).
			errs = append(errs, err)
			continue
		}
		combined.Succeeded = append(combined.Succeeded, res.Succeeded...)
		combined.Failed = append(combined.Failed, res.Failed...)
	}
	return combined, errors.Join(errs...)
}

// isStopSignal reports whether err is the expected, non-fatal signal that a
// blocking consume loop should stop quietly: a cancelled context. It is not a
// failure — the caller asked the loop to end.
func isStopSignal(ctx context.Context, err error) bool {
	return ctx.Err() != nil ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}

// armWake returns the push-delivery wake channel for notifyChannel, or nil when
// push delivery is unavailable. It is deliberately called *before* each receive
// attempt (see fetchNext): arming the waker across the receive closes the
// lost-wakeup window where a NOTIFY firing between an empty receive() and the
// subsequent wait would otherwise be missed and cost a full poll interval.
func (pq *Queue) armWake(ctx context.Context, notifyChannel string) <-chan struct{} {
	if pq.notifier == nil || notifyChannel == "" {
		return nil
	}
	return pq.notifier.wakeChan(ctx, notifyChannel)
}

// waitForWork blocks until there may be a message to consume: the pre-armed
// LISTEN/NOTIFY wake channel closing, the safety-net poll interval d, or ctx
// cancellation. It reports false only when ctx ended, signalling the caller to
// stop. A spurious wake just triggers one extra fetch attempt, which is cheap.
// A nil wake channel blocks forever, so that case only fires under push
// delivery.
func (pq *Queue) waitForWork(ctx context.Context, d time.Duration, wake <-chan struct{}) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	case <-wake:
		return true
	}
}

// effectivePoll resolves the safety-net poll interval: the per-consume
// WithPollInterval override, else the queue-wide WithSafetyNetPoll, else the
// built-in default.
func (pq *Queue) effectivePoll(o consumeOpts) time.Duration {
	if o.pollInterval > 0 {
		return o.pollInterval
	}
	if pq.cfg.safetyNetPoll > 0 {
		return pq.cfg.safetyNetPoll
	}
	return defaultPollInterval
}

// resolveNotifyChannel returns the LISTEN/NOTIFY channel name for a queue, or
// "" when push delivery is unavailable (no Listener registered, or the queue's
// table name could not be resolved — the loop then relies on the poll).
func (pq *Queue) resolveNotifyChannel(ctx context.Context, qt QueueType, name string) string {
	if pq.notifier == nil {
		return ""
	}
	tableName, err := pq.cachedTableName(ctx, string(qt), name)
	if err != nil {
		return ""
	}
	return notifyChannelName(tableName)
}

// ChannelMessages returns a range-over-func iterator (Go 1.23+) over messages
// from a point-to-point channel. The caller is responsible for acknowledging
// each message with Ack or Nack — msg.Receipt() carries the queue binding.
//
// The iterator blocks while the channel is empty, polling at WithPollInterval
// (default 1s), and ends cleanly when ctx is cancelled. A genuine error is
// yielded once with a nil message, after which the iterator stops.
func (pq *Queue) ChannelMessages(
	ctx context.Context,
	name string,
	opts ...ConsumeOption,
) iter.Seq2[*Message, error] {
	notifyCh := pq.resolveNotifyChannel(ctx, QueueTypeChannel, name)
	return pq.messagesIter(ctx, opts, notifyCh, func(c context.Context) (*Message, error) {
		return pq.ReceiveChannel(c, name, opts...)
	})
}

// TopicMessages returns a range-over-func iterator over messages for a
// subscriber on a pub/sub topic. See ChannelMessages for blocking and
// error-delivery semantics.
func (pq *Queue) TopicMessages(
	ctx context.Context,
	name, subscriberID string,
	opts ...ConsumeOption,
) iter.Seq2[*Message, error] {
	notifyCh := pq.resolveNotifyChannel(ctx, QueueTypePubSub, name)
	return pq.messagesIter(ctx, opts, notifyCh, func(c context.Context) (*Message, error) {
		return pq.ReceiveTopic(c, name, subscriberID, opts...)
	})
}

// isEmptySignal reports whether err is the expected "nothing to consume right
// now" signal (an empty or paused queue) — the loop should wait and retry.
func isEmptySignal(err error) bool {
	return errors.Is(err, ErrQueueEmpty) || errors.Is(err, ErrQueuePaused)
}

// fetchOutcome is what fetchNext resolved to: a message to deliver, an
// unrecoverable error, or a clean stop (both fields nil).
type fetchOutcome struct {
	msg *Message // non-nil only when a message is ready to deliver
	err error    // non-nil only on an unrecoverable failure
}

// deliver reports whether the outcome carries a message to hand to the caller.
func (o fetchOutcome) deliver() bool { return o.msg != nil }

// fetchNext blocks until a message is available, the context is cancelled, or
// an unrecoverable error occurs. It absorbs empty/paused waits and bounded
// transient-error retries internally (FR-026). A clean stop (context
// cancellation) yields a zero fetchOutcome; a fatal failure yields one with
// err set.
func (pq *Queue) fetchNext(
	ctx context.Context,
	receive func(context.Context) (*Message, error),
	poll time.Duration,
	notifyChannel string,
	transientFails *int,
) fetchOutcome {
	for {
		if ctx.Err() != nil {
			return fetchOutcome{}
		}
		// Arm the push-delivery waker before receiving so a NOTIFY that fires
		// while receive() runs closes this generation's wake channel rather
		// than being lost in the gap before the wait (see armWake).
		wake := pq.armWake(ctx, notifyChannel)
		msg, err := receive(ctx)
		switch {
		case err == nil:
			*transientFails = 0
			return fetchOutcome{msg: msg}
		case isEmptySignal(err):
			*transientFails = 0
			if !pq.waitForWork(ctx, poll, wake) {
				return fetchOutcome{}
			}
		case isStopSignal(ctx, err):
			return fetchOutcome{}
		case isTransientError(err):
			if o, keepGoing := pq.retryTransient(ctx, err, transientFails); !keepGoing {
				return o
			}
		default:
			return fetchOutcome{err: err}
		}
	}
}

// retryTransient handles a transient receive error (FR-026): it advances the
// consecutive-failure counter and waits out the escalating backoff. keepGoing
// is false when the loop must stop — retries exhausted (the returned outcome
// carries err) or the context was cancelled (a zero outcome).
func (pq *Queue) retryTransient(
	ctx context.Context,
	err error,
	fails *int,
) (fetchOutcome, bool) {
	*fails++
	if *fails > maxConsumeTransientRetries {
		return fetchOutcome{err: err}, false
	}
	// A transient-error backoff is a plain timed wait — no push-delivery waker.
	if !pq.waitForWork(ctx, transientBackoff(*fails), nil) {
		return fetchOutcome{}, false
	}
	return fetchOutcome{}, true
}

// messagesIter is the shared iterator body for ChannelMessages/TopicMessages.
func (pq *Queue) messagesIter(
	ctx context.Context,
	opts []ConsumeOption,
	notifyChannel string,
	receive func(context.Context) (*Message, error),
) iter.Seq2[*Message, error] {
	poll := pq.effectivePoll(applyConsumeOptions(opts))

	return func(yield func(*Message, error) bool) {
		transientFails := 0
		for {
			o := pq.fetchNext(ctx, receive, poll, notifyChannel, &transientFails)
			if !o.deliver() {
				if o.err != nil {
					yield(nil, o.err)
				}
				return
			}
			if !yield(o.msg, nil) {
				return
			}
		}
	}
}

// ConsumeChannel runs a handler-driven consume loop over a point-to-point
// channel. pgqueue owns the loop: it fetches messages, invokes h, and then
// acknowledges automatically — Ack when h returns nil, Nack (recording the
// error as the failure reason, feeding the retry backoff) when h returns an
// error.
//
// With WithConcurrency(n) the loop runs n parallel workers. ConsumeChannel
// blocks until ctx is cancelled, at which point it drains its workers and
// returns nil. It returns a non-nil error only on an unrecoverable fetch
// failure.
func (pq *Queue) ConsumeChannel(
	ctx context.Context,
	name string,
	h Handler,
	opts ...ConsumeOption,
) error {
	if err := pq.checkClosed(); err != nil {
		return err
	}
	if err := validateConsumeOptions(applyConsumeOptions(opts)); err != nil {
		return err
	}
	notifyCh := pq.resolveNotifyChannel(ctx, QueueTypeChannel, name)
	return pq.runHandlerLoop(ctx, opts, notifyCh, h, func(c context.Context) (*Message, error) {
		return pq.ReceiveChannel(c, name, opts...)
	})
}

// ConsumeTopic runs a handler-driven consume loop for a subscriber on a pub/sub
// topic. See ConsumeChannel for loop ownership, auto-ack/nack, concurrency, and
// shutdown semantics.
func (pq *Queue) ConsumeTopic(
	ctx context.Context,
	name, subscriberID string,
	h Handler,
	opts ...ConsumeOption,
) error {
	if err := pq.checkClosed(); err != nil {
		return err
	}
	if err := validateConsumeOptions(applyConsumeOptions(opts)); err != nil {
		return err
	}
	notifyCh := pq.resolveNotifyChannel(ctx, QueueTypePubSub, name)
	return pq.runHandlerLoop(ctx, opts, notifyCh, h, func(c context.Context) (*Message, error) {
		return pq.ReceiveTopic(c, name, subscriberID, opts...)
	})
}

// runHandlerLoop is the shared worker-pool body for ConsumeChannel/ConsumeTopic.
// Each worker independently fetches, runs the handler, and ack/nacks; the
// visibility timeout plus FOR UPDATE SKIP LOCKED keep workers from colliding on
// the same message.
func (pq *Queue) runHandlerLoop(
	ctx context.Context,
	opts []ConsumeOption,
	notifyChannel string,
	h Handler,
	receive func(context.Context) (*Message, error),
) error {
	// Register on the Queue's worker WaitGroup so Close can join this loop
	// before the caller closes the database (R-08).
	if !pq.trackWorker() {
		return ErrQueueClosed
	}
	defer pq.workerWG.Done()

	o := applyConsumeOptions(opts)
	workers := o.concurrency
	if workers <= 0 {
		workers = 1
	}
	if workers > maxConcurrency {
		workers = maxConcurrency
	}
	poll := pq.effectivePoll(o)

	// fatalErr is set once by the first worker to hit an unrecoverable fetch
	// error; that worker cancels the shared context so the others wind down.
	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Close cancels bgCtx; mirror that onto loopCtx so the loop also winds
	// down on Close, not only on the caller's own context.
	stopWatch := make(chan struct{})
	defer close(stopWatch)
	go func() {
		select {
		case <-pq.bgCtx.Done():
			cancel()
		case <-stopWatch:
		}
	}()

	var (
		mu       sync.Mutex
		fatalErr error
		wg       sync.WaitGroup
	)
	setFatal := func(err error) {
		mu.Lock()
		if fatalErr == nil {
			fatalErr = err
		}
		mu.Unlock()
		cancel()
	}

	for range workers {
		wg.Go(func() {
			pq.handlerWorker(loopCtx, poll, notifyChannel, h, receive, setFatal)
		})
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	return fatalErr
}

// handlerWorker is one worker of the handler-based consume pool. It fetches
// messages via fetchNext (which absorbs empty waits and transient-error
// retries) and dispatches each to the handler until the loop is told to stop.
func (pq *Queue) handlerWorker(
	ctx context.Context,
	poll time.Duration,
	notifyChannel string,
	h Handler,
	receive func(context.Context) (*Message, error),
	setFatal func(error),
) {
	transientFails := 0
	for {
		o := pq.fetchNext(ctx, receive, poll, notifyChannel, &transientFails)
		if !o.deliver() {
			if o.err != nil {
				setFatal(o.err)
			}
			return
		}
		pq.dispatchToHandler(ctx, h, o.msg)
	}
}

// dispatchToHandler invokes the handler for one message and applies the
// automatic ack/nack. The ack/nack uses a context detached from cancellation so
// a handler that finished just as shutdown began still records its result
// rather than leaving the message to time out and redeliver.
func (pq *Queue) dispatchToHandler(ctx context.Context, h Handler, msg *Message) {
	receipt := msg.Receipt()
	ctx, span := pq.startSpan(ctx, "pgqueue.consume",
		StringAttr("queue", receipt.QueueName),
		StringAttr("message_id", msg.ID.String()))

	start := time.Now()
	herr := pq.callHandler(ctx, h, msg)
	pq.recordHandle(receipt.QueueName, time.Since(start))
	pq.recordDeliveryLatency(receipt.QueueName, msg.CreatedAt, start)
	pq.endSpan(span, herr)

	// Detach from cancellation so a handler that finished as shutdown began
	// still records its result, but cap the wait: an uncancellable, unbounded
	// ack against a hung database would block this worker — and Queue.Close,
	// which joins it — forever. ackReceipt/nackReceipt are used in place of the
	// public Ack/Nack so the auto-ack is not rejected by the closed-state gate:
	// Close marks the Queue closed before joining these loops, and a handler
	// finishing in that window must still record its result rather than leave
	// the message to time out and redeliver.
	ackCtx, ackCancel := context.WithTimeout(context.WithoutCancel(ctx), ackGracePeriod)
	defer ackCancel()
	if herr != nil {
		err := pq.nackReceipt(ackCtx, receipt, herr.Error())
		switch {
		case err == nil:
		case errors.Is(err, ErrClaimExpired):
			pq.signalAckAfterExpired(receipt, msg, "nack")
		default:
			pq.logError("failed to nack message after handler error",
				"queue", receipt.QueueName,
				"message_id", msg.ID.String(),
				"error", err)
		}
		return
	}
	// A failed ack is logged but not fatal: the message simply redelivers, which
	// at-least-once tolerates. ErrClaimExpired is the expected outcome when the
	// handler outran the visibility timeout — surface it via the
	// pgqueue_ack_after_expired metric and a WARN log so operators can detect
	// at-least-twice delivery driven by slow handlers (issue #71).
	err := pq.ackReceipt(ackCtx, receipt)
	switch {
	case err == nil:
	case errors.Is(err, ErrClaimExpired):
		pq.signalAckAfterExpired(receipt, msg, "ack")
	default:
		pq.logError("failed to ack message after successful handler",
			"queue", receipt.QueueName,
			"message_id", msg.ID.String(),
			"error", err)
	}
}

// signalAckAfterExpired records one ErrClaimExpired observed at auto-ack/nack
// time so operators see it: the per-receipt metric increments and a WARN log
// names the queue, message, and which side (ack or nack) hit the stale claim.
// The message will be redelivered by another consumer; the WARN line is the
// only application-visible signal that already-completed work will run again.
func (pq *Queue) signalAckAfterExpired(receipt Receipt, msg *Message, op string) {
	pq.recordAckAfterExpired(receipt.QueueName, 1)
	pq.logWarn("claim expired before auto-"+op+"; message will redeliver",
		"queue", receipt.QueueName,
		"message_id", msg.ID.String(),
		"retry_count", msg.RetryCount)
}

// errHandlerPanic is the static base error for a recovered handler panic; the
// recovered value is attached as wrapping context.
var errHandlerPanic = errors.New("handler panic")

// callHandler invokes the user handler, recovering any panic so one bad
// message cannot crash the process or take sibling workers down with it
// (R-01). A recovered panic is converted to an error and routed to the normal
// Nack path — the message is retried (or dead-lettered once retries are
// exhausted) exactly as a returned error would be. The stack is logged at
// ERROR for diagnosis.
func (pq *Queue) callHandler(ctx context.Context, h Handler, msg *Message) (herr error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		if e, ok := r.(error); ok {
			herr = fmt.Errorf("%w: %w", errHandlerPanic, e)
		} else {
			herr = fmt.Errorf("%w: %v", errHandlerPanic, r)
		}
		pq.logError("recovered panic in message handler",
			"message_id", msg.ID.String(),
			"panic", r,
			"stack", string(debug.Stack()),
		)
	}()
	return h(ctx, msg)
}
