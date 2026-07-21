// Package fake provides an in-memory implementation of the pgqueue published
// interfaces (pgqueue.Publisher, pgqueue.ChannelConsumer, pgqueue.TopicConsumer)
// so code that depends on pgqueue can be unit-tested without a PostgreSQL
// database or Docker (FR-021).
//
// fake.Queue reproduces the documented queue semantics — publish (including
// WithMessageID publish-side dedup and WithMessageMetadata propagation),
// single-shot and handler-based consume, ack/nack, retry with DLQ promotion at
// exactly max-retries, fan-out per subscriber, and pause/resume.
//
// One narrow dedup divergence: the fake tracks live messages (pending, claimed,
// DLQ) rather than a persistent message table, so a duplicate WithMessageID is
// rejected with ErrDuplicateMessageID only while the original is still present.
// Re-publishing the same ID after it was acked and removed succeeds against the
// fake, whereas the real Queue keeps rejecting it until the row is garbage
// collected. Tests that depend on that long-lived rejection should use the real
// Queue (see the internal/integration suite). For topics the fake derives this
// dedup from per-subscriber delivery state rather than a topic-level message
// table, so a duplicate publish to a topic that currently has no subscribers is
// never rejected (there is no subscriber state to check against), whereas the
// real Queue would reject it against the persistent message table.
//
// The fake also applies a single global retry cap (WithMaxRetries) to every
// channel and topic. It cannot reproduce the real Queue's per-queue
// WithQueueMaxRetries override, and the reported Message.MaxRetries always
// reflects the global cap rather than a per-queue value.
//
// It deliberately does NOT model visibility-timeout reclamation on a wall
// clock: a claimed message stays claimed until it is explicitly acked or
// nacked, and never reappears on its own. The fake therefore cannot reproduce
// pgqueue's at-least-once redelivery contract — the timeout-driven and
// crash-driven redeliveries that make duplicate delivery possible. This is a
// deliberate design choice (deterministic, no background timers), not a bug.
//
// Consequently the fake is unsuitable for testing handler idempotency under
// redelivery. Tests that must verify that contract should drive the real Queue
// against PostgreSQL: consume a message with a short visibility timeout, let it
// lapse without acking (or simulate a crashed consumer), and assert the message
// is redelivered. See the internal/integration suite (testcontainers-backed)
// for the canonical redelivery tests.
//
// Scope: the fake implements only the three published interfaces (single-message
// publish/consume/ack). Batch operations (PublishBatch, AckBatch, NackBatch), the
// message iterators (ChannelMessages, TopicMessages), and the admin/DLQ/replay/
// stats APIs live on the concrete *Queue only and are not modeled here; code
// exercising those must be tested against the real Queue.
//
//	q := fake.New()
//	q.CreateChannel(ctx, "orders")
//	q.Publish(ctx, "orders", []byte(`{"id":1}`))
//	// run code under test against the pgqueue interfaces, backed by q
package fake

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sgaunet/pgqueue"
)

// defaultMaxRetries matches the core library default.
const defaultMaxRetries = 3

// Queue is an in-memory pgqueue test double. The zero value is not usable; call
// New. All methods are safe for concurrent use.
type Queue struct {
	mu         sync.Mutex
	maxRetries int
	channels   map[string]*channel
	topics     map[string]*topic
}

// Option configures a fake Queue.
type Option func(*Queue)

// entry is one in-memory message with its delivery state.
type entry struct {
	id         uuid.UUID
	claimID    uuid.UUID
	payload    []byte
	metadata   map[string]any
	status     pgqueue.MessageStatus
	retryCount int
}

// channel is an in-memory point-to-point channel.
type channel struct {
	paused  bool
	pending []*entry
	claimed map[uuid.UUID]*entry // by message ID
	dlq     []*entry
}

func newChannel() *channel {
	return &channel{claimed: make(map[uuid.UUID]*entry)}
}

// hasMessage reports whether an entry with id currently lives in this channel's
// pending, claimed, or DLQ state. It backs the fake's publish-side dedup, which
// mirrors the real Queue's ON CONFLICT (id) DO NOTHING rejection. Like the real
// message table it only sees messages still present: an entry that was acked and
// removed is no longer tracked, so a same-ID re-publish after full consumption is
// not detected — a deliberate divergence noted in the package doc.
func (c *channel) hasMessage(id uuid.UUID) bool {
	for _, e := range c.pending {
		if e.id == id {
			return true
		}
	}
	if _, ok := c.claimed[id]; ok {
		return true
	}
	for _, e := range c.dlq {
		if e.id == id {
			return true
		}
	}
	return false
}

// topic is an in-memory pub/sub topic: one payload store, fan-out per subscriber.
type topic struct {
	paused bool
	subs   map[string]*channel // subscriberID -> its own delivery state
}

func newTopic() *topic {
	return &topic{subs: make(map[string]*channel)}
}

// WithMaxRetries sets the retry limit before a message is moved to the DLQ.
// The default is 3, matching the core library.
//
// An explicit zero is honored: WithMaxRetries(0) dead-letters a message on its
// first failed delivery, mirroring the core library's WithDefaultMaxRetries(0).
// A negative value is meaningless and is ignored.
func WithMaxRetries(n int) Option {
	return func(q *Queue) {
		if n >= 0 {
			q.maxRetries = n
		}
	}
}

// New returns an empty in-memory Queue.
func New(opts ...Option) *Queue {
	q := &Queue{
		maxRetries: defaultMaxRetries,
		channels:   make(map[string]*channel),
		topics:     make(map[string]*topic),
	}
	for _, o := range opts {
		o(q)
	}
	return q
}

// compile-time checks: *Queue satisfies every published pgqueue interface.
var (
	_ pgqueue.Publisher       = (*Queue)(nil)
	_ pgqueue.ChannelConsumer = (*Queue)(nil)
	_ pgqueue.TopicConsumer   = (*Queue)(nil)
)

// CreateChannel registers a channel. It is idempotent.
func (q *Queue) CreateChannel(_ context.Context, name string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.channels[name]; !ok {
		q.channels[name] = newChannel()
	}
	return nil
}

// CreateTopic registers a topic. It is idempotent.
func (q *Queue) CreateTopic(_ context.Context, name string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.topics[name]; !ok {
		q.topics[name] = newTopic()
	}
	return nil
}

// Subscribe registers a subscriber on a topic. It is idempotent.
func (q *Queue) Subscribe(_ context.Context, topicName, subscriberID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.topics[topicName]
	if !ok {
		t = newTopic()
		q.topics[topicName] = t
	}
	if _, ok := t.subs[subscriberID]; !ok {
		t.subs[subscriberID] = newChannel()
	}
	return nil
}

// Publish publishes a message to the named channel or topic. The destination
// type is resolved by name — mirroring the real Queue, where a name is unique
// across both types. A name registered as a topic fans the message out to a
// copy in each current subscriber's queue; a name registered as a channel
// receives the message. An unregistered name returns ErrQueueNotFound, matching
// the real Queue rather than implicitly creating a channel (M9/FR-028).
func (q *Queue) Publish(
	_ context.Context, name string, payload []byte, opts ...pgqueue.PublishOption,
) (uuid.UUID, error) {
	if payload == nil {
		return uuid.UUID{}, pgqueue.ErrNilPayload
	}
	// Honor WithMessageID / WithMessageMetadata exactly as the real Queue does:
	// resolve them through the library's own resolver so the fake cannot silently
	// diverge from publish-side dedup and metadata propagation.
	id, meta := pgqueue.ResolvePublishOptions(opts...)
	if id == uuid.Nil {
		// No caller-supplied ID: mint a uuidv7() like the production schema so
		// "ORDER BY id" reflects insertion order; v4 would break that contract
		// for any consumer that exercises ordering against the fake.
		var err error
		id, err = pgqueue.NewUUIDv7()
		if err != nil {
			return uuid.UUID{}, fmt.Errorf("fake: generate message id: %w", err)
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	if t, ok := q.topics[name]; ok {
		// The real Queue dedups against the topic's single persistent message
		// table. The fake has no such table, so it derives dedup from per-
		// subscriber delivery state: a duplicate ID is rejected if any current
		// subscriber still holds it (pending, claimed, or DLQ). Check every
		// subscriber and reject before mutating any. Two known divergences follow
		// from this (both noted in the package doc): a re-publish after every
		// subscriber has acked the original succeeds, and a duplicate publish to a
		// subscriber-less topic is never rejected.
		for _, sub := range t.subs {
			if sub.hasMessage(id) {
				return uuid.UUID{}, fmt.Errorf("%s: %w", id, pgqueue.ErrDuplicateMessageID)
			}
		}
		for _, sub := range t.subs {
			// Each subscriber gets an independent copy of the payload and metadata
			// so a consumer mutating one delivered message cannot corrupt a fan-out
			// sibling (R-16).
			sub.pending = append(sub.pending, &entry{
				id:       id,
				payload:  cloneBytes(payload),
				metadata: cloneMetadata(meta),
				status:   pgqueue.MessageStatusPending,
			})
		}
		return id, nil
	}

	ch, ok := q.channels[name]
	if !ok {
		// Match the real Queue (publish.go resolveQueueMetadata): publishing to a
		// name that was never created as a channel or topic returns
		// ErrQueueNotFound rather than implicitly creating it (M9/FR-028), so a
		// test that would fail against PostgreSQL also fails against the fake.
		return uuid.UUID{}, fmt.Errorf("%s: %w", name, pgqueue.ErrQueueNotFound)
	}
	if ch.hasMessage(id) {
		return uuid.UUID{}, fmt.Errorf("%s: %w", id, pgqueue.ErrDuplicateMessageID)
	}
	ch.pending = append(ch.pending, &entry{
		id:       id,
		payload:  cloneBytes(payload),
		metadata: cloneMetadata(meta),
		status:   pgqueue.MessageStatusPending,
	})
	return id, nil
}

// ReceiveChannel returns the next available channel message, or ErrQueueEmpty.
func (q *Queue) ReceiveChannel(
	_ context.Context, name string, _ ...pgqueue.ConsumeOption,
) (*pgqueue.Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	ch, ok := q.channels[name]
	if !ok {
		return nil, pgqueue.ErrQueueNotFound
	}
	if ch.paused {
		return nil, pgqueue.ErrQueuePaused
	}
	return q.claim(ch, pgqueue.Receipt{QueueName: name, QueueType: pgqueue.QueueTypeChannel})
}

// ReceiveTopic returns the next available message for a subscriber, or
// ErrQueueEmpty.
func (q *Queue) ReceiveTopic(
	_ context.Context, name, subscriberID string, _ ...pgqueue.ConsumeOption,
) (*pgqueue.Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.topics[name]
	if !ok {
		return nil, pgqueue.ErrQueueNotFound
	}
	if t.paused {
		return nil, pgqueue.ErrQueuePaused
	}
	sub, ok := t.subs[subscriberID]
	if !ok {
		return nil, pgqueue.ErrSubscriberNotFound
	}
	return q.claim(sub, pgqueue.Receipt{
		QueueName:    name,
		QueueType:    pgqueue.QueueTypePubSub,
		SubscriberID: subscriberID,
	})
}

// Ack acknowledges a message. A stale claim resolves to ErrClaimExpired.
func (q *Queue) Ack(_ context.Context, r pgqueue.Receipt) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	ch, err := q.resolve(r)
	if err != nil {
		return err
	}
	e, ok := ch.claimed[r.MessageID]
	if !ok || e.claimID != r.ClaimID {
		return pgqueue.ErrClaimExpired
	}
	delete(ch.claimed, r.MessageID)
	return nil
}

// ExtendVisibility resets an in-flight message's visibility lease. The fake
// models claims explicitly rather than by wall-clock expiry, so a held claim has
// no timer to reset: it validates the duration bound and claim ownership, then
// returns nil (or ErrClaimExpired if the claim is no longer held). This preserves
// the observable contract — invalid durations and stale claims still error — so
// handler code exercising ExtendVisibility can be unit-tested against the fake.
func (q *Queue) ExtendVisibility(_ context.Context, r pgqueue.Receipt, d time.Duration) error {
	if d < time.Millisecond || d > 24*time.Hour {
		return pgqueue.ErrInvalidVisibilityTimeout
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	ch, err := q.resolve(r)
	if err != nil {
		return err
	}
	e, ok := ch.claimed[r.MessageID]
	if !ok || e.claimID != r.ClaimID {
		return pgqueue.ErrClaimExpired
	}
	return nil
}

// Nack negatively acknowledges a message: it is retried, or moved to the DLQ
// once retryCount+1 exceeds the retry limit. A stale claim resolves to
// ErrClaimExpired.
func (q *Queue) Nack(
	_ context.Context, r pgqueue.Receipt, _ string, _ ...pgqueue.NackOption,
) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	ch, err := q.resolve(r)
	if err != nil {
		return err
	}
	e, ok := ch.claimed[r.MessageID]
	if !ok || e.claimID != r.ClaimID {
		return pgqueue.ErrClaimExpired
	}
	delete(ch.claimed, r.MessageID)

	// DLQ promotion uses the same retryCount+1 > max guard as the core library.
	if e.retryCount+1 > q.maxRetries {
		e.status = pgqueue.MessageStatusCompleted
		ch.dlq = append(ch.dlq, e)
		return nil
	}
	e.retryCount++
	e.claimID = uuid.UUID{}
	e.status = pgqueue.MessageStatusPending
	// The real Queue re-selects pending rows with ORDER BY id (UUIDv7,
	// chronological), and a nacked message keeps its original id, so it sorts
	// back to its original position ahead of later-published messages. Mirror
	// that by inserting into ch.pending sorted by id rather than tail-appending;
	// the head-claim in claim() then still pops the oldest pending entry.
	insertSortedByID(ch, e)
	return nil
}

// insertSortedByID inserts e into ch.pending keeping the slice ordered by id,
// placing e before the first existing entry whose id is greater. This mirrors
// the real Queue's ORDER BY id selection so a redelivered (nacked) message
// keeps its chronological position relative to later-published messages.
func insertSortedByID(ch *channel, e *entry) {
	i := 0
	for i < len(ch.pending) && bytesLess(ch.pending[i].id, e.id) {
		i++
	}
	ch.pending = append(ch.pending, nil)
	copy(ch.pending[i+1:], ch.pending[i:])
	ch.pending[i] = e
}

// bytesLess reports whether UUID a sorts before UUID b in byte order, matching
// PostgreSQL's ordering of the uuid type (and therefore ORDER BY id).
func bytesLess(a, b uuid.UUID) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

// ConsumeChannel runs a handler-driven loop over a channel: it fetches each
// message, calls h, then auto-acks (nil) or auto-nacks (error). It returns when
// ctx is cancelled.
func (q *Queue) ConsumeChannel(
	ctx context.Context, name string, h pgqueue.Handler, opts ...pgqueue.ConsumeOption,
) error {
	return q.consumeLoop(ctx, h, func() (*pgqueue.Message, error) {
		return q.ReceiveChannel(ctx, name, opts...)
	})
}

// ConsumeTopic runs a handler-driven loop for one subscriber on a topic. See
// ConsumeChannel for loop and ack/nack semantics.
func (q *Queue) ConsumeTopic(
	ctx context.Context, name, subscriberID string, h pgqueue.Handler, opts ...pgqueue.ConsumeOption,
) error {
	return q.consumeLoop(ctx, h, func() (*pgqueue.Message, error) {
		return q.ReceiveTopic(ctx, name, subscriberID, opts...)
	})
}

// PauseChannel pauses consumption from a channel.
func (q *Queue) PauseChannel(_ context.Context, name string) error {
	return q.setChannelPaused(name, true)
}

// ResumeChannel resumes consumption from a paused channel.
func (q *Queue) ResumeChannel(_ context.Context, name string) error {
	return q.setChannelPaused(name, false)
}

// PauseTopic pauses consumption from a topic.
func (q *Queue) PauseTopic(_ context.Context, name string) error {
	return q.setTopicPaused(name, true)
}

// ResumeTopic resumes consumption from a paused topic.
func (q *Queue) ResumeTopic(_ context.Context, name string) error {
	return q.setTopicPaused(name, false)
}

// ChannelDLQ returns the payloads currently in a channel's dead-letter queue.
// It is a test-inspection helper with no equivalent on the real Queue.
func (q *Queue) ChannelDLQ(name string) [][]byte {
	q.mu.Lock()
	defer q.mu.Unlock()
	ch, ok := q.channels[name]
	if !ok {
		return nil
	}
	out := make([][]byte, 0, len(ch.dlq))
	for _, e := range ch.dlq {
		out = append(out, e.payload)
	}
	return out
}

// claim pops the head pending entry of ch, marks it processing under a fresh
// claim, and returns it as a pgqueue.Message with a bound receipt.
func (q *Queue) claim(ch *channel, base pgqueue.Receipt) (*pgqueue.Message, error) {
	if len(ch.pending) == 0 {
		return nil, pgqueue.ErrQueueEmpty
	}
	e := ch.pending[0]
	ch.pending = ch.pending[1:]
	claimID, err := pgqueue.NewUUIDv7()
	if err != nil {
		ch.pending = append([]*entry{e}, ch.pending...)
		return nil, fmt.Errorf("fake: generate claim id: %w", err)
	}
	e.claimID = claimID
	e.status = pgqueue.MessageStatusProcessing
	ch.claimed[e.id] = e

	// Hand the consumer copies of the payload and metadata so mutating a
	// delivered message cannot corrupt the stored entry — and therefore cannot
	// affect a later redelivery of the same message (R-16).
	msg := &pgqueue.Message{
		ID:         e.id,
		ClaimID:    e.claimID,
		Payload:    cloneBytes(e.payload),
		Metadata:   cloneMetadata(e.metadata),
		Status:     pgqueue.MessageStatusProcessing,
		RetryCount: e.retryCount,
		MaxRetries: q.maxRetries,
	}
	base.MessageID = e.id
	base.ClaimID = e.claimID
	msg.SetReceipt(base)
	return msg, nil
}

// cloneBytes returns an independent copy of b, preserving a nil/empty
// distinction is unnecessary here — a nil slice clones to nil.
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// cloneMetadata returns a shallow copy of m (a nil map clones to nil). Values
// are copied by reference, matching how the database-backed Queue round-trips
// metadata through JSON: callers must not mutate nested values in place.
func cloneMetadata(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	c := make(map[string]any, len(m))
	maps.Copy(c, m)
	return c
}

// resolve locates the channel (or subscriber queue) a receipt refers to.
func (q *Queue) resolve(r pgqueue.Receipt) (*channel, error) {
	switch r.QueueType {
	case pgqueue.QueueTypeChannel:
		ch, ok := q.channels[r.QueueName]
		if !ok {
			return nil, pgqueue.ErrQueueNotFound
		}
		return ch, nil
	case pgqueue.QueueTypePubSub:
		t, ok := q.topics[r.QueueName]
		if !ok {
			return nil, pgqueue.ErrQueueNotFound
		}
		sub, ok := t.subs[r.SubscriberID]
		if !ok {
			return nil, pgqueue.ErrSubscriberNotFound
		}
		return sub, nil
	default:
		return nil, pgqueue.ErrReceiptMissingQueueType
	}
}

// consumeLoop is the shared handler-based consume body.
func (q *Queue) consumeLoop(
	ctx context.Context, h pgqueue.Handler, receive func() (*pgqueue.Message, error),
) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		msg, err := receive()
		switch {
		case errors.Is(err, pgqueue.ErrQueueEmpty), errors.Is(err, pgqueue.ErrQueuePaused):
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Millisecond):
			}
			continue
		case err != nil:
			return err
		}
		if herr := h(ctx, msg); herr != nil {
			_ = q.Nack(ctx, msg.Receipt(), herr.Error())
		} else {
			_ = q.Ack(ctx, msg.Receipt())
		}
	}
}

func (q *Queue) setChannelPaused(name string, paused bool) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	ch, ok := q.channels[name]
	if !ok {
		return pgqueue.ErrQueueNotFound
	}
	ch.paused = paused
	return nil
}

func (q *Queue) setTopicPaused(name string, paused bool) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.topics[name]
	if !ok {
		return pgqueue.ErrQueueNotFound
	}
	t.paused = paused
	return nil
}
