package pgqueue

import (
	"time"

	"github.com/google/uuid"
)

// QueueOption is a per-queue creation option applied when calling CreateChannel
// or CreateTopic.
type QueueOption func(*queueCreateOpts)

// queueCreateOpts holds the resolved per-queue creation options.
type queueCreateOpts struct {
	maxMessageSize  int
	maxMetadataSize int
	ttl             time.Duration
	maxRetries      int
	maxRetriesSet   bool // true when WithQueueMaxRetries was supplied
}

// WithQueueMaxRetries overrides the default maximum retry count for a specific
// channel or topic.
//
// An explicit zero is honored: WithQueueMaxRetries(0) dead-letters a message on
// its first failed delivery instead of retrying it.
func WithQueueMaxRetries(n int) QueueOption {
	return func(o *queueCreateOpts) {
		o.maxRetries = n
		o.maxRetriesSet = true
	}
}

// WithQueueTTL overrides the default message TTL for a specific channel or topic.
// Zero means no expiry.
//
// TTL only hides expired messages from consumers; it does not delete them. Use a
// GarbageCollector RetentionPolicy (MaxPendingAge) to reclaim their storage.
func WithQueueTTL(d time.Duration) QueueOption {
	return func(o *queueCreateOpts) {
		o.ttl = d
	}
}

// WithQueueMaxMessageSize overrides the maximum payload size for a specific
// channel or topic.
//
// Zero (the default) inherits the queue-wide cap configured via
// WithMaxMessageSize. Any positive value up to MaxAllowedMessageSize
// (PostgreSQL's bytea per-value limit) is honored verbatim. Negative values
// and values above MaxAllowedMessageSize make CreateChannel/CreateTopic
// return ErrInvalidConfig.
func WithQueueMaxMessageSize(bytes int) QueueOption {
	return func(o *queueCreateOpts) {
		o.maxMessageSize = bytes
	}
}

// WithQueueMaxMetadataSize overrides the maximum marshaled metadata size for a
// specific channel or topic.
//
// Zero (the default) inherits the queue-wide cap configured via
// WithMaxMetadataSize. Any positive value up to MaxAllowedMetadataSize
// (PostgreSQL's JSONB per-value limit) is honored verbatim. Negative values
// and values above MaxAllowedMetadataSize make CreateChannel/CreateTopic
// return ErrInvalidConfig.
func WithQueueMaxMetadataSize(bytes int) QueueOption {
	return func(o *queueCreateOpts) {
		o.maxMetadataSize = bytes
	}
}

// applyQueueOptions applies functional options onto a zero queueCreateOpts.
func applyQueueOptions(opts []QueueOption) queueCreateOpts {
	o := queueCreateOpts{}
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// PublishOption is a per-publish option applied to a single publish call.
type PublishOption func(*publishOpts)

// publishOpts holds the resolved per-message publish options.
type publishOpts struct {
	messageID uuid.UUID
	metadata  map[string]any
}

// WithMessageID sets a specific message ID for deduplication. If not set, a
// new UUIDv7 is generated automatically. When a message with the same ID
// already exists, ErrDuplicateMessageID is returned.
func WithMessageID(id uuid.UUID) PublishOption {
	return func(o *publishOpts) {
		o.messageID = id
	}
}

// WithMessageMetadata attaches arbitrary metadata to a published message. The
// metadata is stored as JSONB and returned with consumed messages.
func WithMessageMetadata(m map[string]any) PublishOption {
	return func(o *publishOpts) {
		o.metadata = m
	}
}

// applyPublishOptions applies functional options onto a zero publishOpts.
func applyPublishOptions(opts []PublishOption) publishOpts {
	o := publishOpts{}
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// ConsumeOption is a per-consume option applied to Receive*/Consume* calls.
type ConsumeOption func(*consumeOpts)

// consumeOpts holds the resolved per-consume options.
type consumeOpts struct {
	visibilityTimeout time.Duration
	concurrency       int
	pollInterval      time.Duration
}

// WithVisibilityTimeout sets the visibility timeout for a consumed message.
// The message becomes eligible for redelivery if not acknowledged within this
// duration.
func WithVisibilityTimeout(d time.Duration) ConsumeOption {
	return func(o *consumeOpts) {
		o.visibilityTimeout = d
	}
}

// WithConcurrency sets the number of parallel workers for handler-based consume
// APIs (ConsumeChannel/ConsumeTopic). It is ignored by single-shot
// ReceiveChannel/ReceiveTopic.
func WithConcurrency(n int) ConsumeOption {
	return func(o *consumeOpts) {
		o.concurrency = n
	}
}

// WithPollInterval sets the polling interval between successive consume attempts
// when no message is available.
func WithPollInterval(d time.Duration) ConsumeOption {
	return func(o *consumeOpts) {
		o.pollInterval = d
	}
}

// defaultVisibilityTimeout is the default visibility timeout used by
// ReceiveChannel and ReceiveTopic when WithVisibilityTimeout is not provided.
const defaultVisibilityTimeout = 30 * time.Second

// applyConsumeOptions applies functional options onto a consumeOpts with
// defaults filled in.
func applyConsumeOptions(opts []ConsumeOption) consumeOpts {
	o := consumeOpts{
		visibilityTimeout: defaultVisibilityTimeout,
	}
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// NackOption is a per-nack option applied to Nack and NackBatch calls.
type NackOption func(*nackOpts)

// nackOpts holds the resolved per-nack options.
type nackOpts struct {
	retryDelay time.Duration
}

// WithRetryDelay overrides the computed backoff delay before the nacked message
// becomes eligible for redelivery (FR-023).
//
// Only a strictly positive d takes effect: d > 0 pins the redelivery delay to
// exactly that duration, bypassing the queue's BackoffPolicy. A non-positive
// value (0 or negative) is silently ignored and the queue's BackoffPolicy is
// used instead, as if WithRetryDelay had not been called. A caller passing a
// negative value almost certainly has a bug; pgqueue treats it identically to
// zero rather than returning an error because Nack is already in a failure path.
func WithRetryDelay(d time.Duration) NackOption {
	return func(o *nackOpts) {
		o.retryDelay = d
	}
}
