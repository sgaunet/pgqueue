package pgqueue

import (
	"log/slog"
	"time"
)

// defaultMaxMessageSize is 256 KiB, per the spec (FR-032).
const defaultMaxMessageSize = 256 * 1024

// MaxAllowedMessageSize is the largest payload pgqueue will accept, in bytes.
// It matches PostgreSQL's hard per-value limit for the bytea column used to
// store payloads (1 GiB). WithMaxMessageSize and WithQueueMaxMessageSize
// reject any value above this with ErrInvalidConfig.
//
// Payloads near this ceiling stress driver buffers, WAL, and replication —
// pick the smallest size that fits your workload.
const MaxAllowedMessageSize = 1 << 30

// defaultMaxMetadataSize is 16 KiB. It bounds the marshaled JSON size of a
// message's metadata map so an unbounded callsite cannot exhaust storage or
// bloat per-queue JSONB scans.
const defaultMaxMetadataSize = 16 * 1024

// MaxAllowedMetadataSize is the largest marshaled metadata size pgqueue will
// accept, in bytes. It matches PostgreSQL's hard per-value limit for the JSONB
// column used to store metadata (1 GiB). WithMaxMetadataSize and
// WithQueueMaxMetadataSize reject any value above this with ErrInvalidConfig.
const MaxAllowedMetadataSize = 1 << 30

// Option is a functional configuration option for New and InitSchema.
type Option func(*queueConfig)

// queueConfig holds the resolved configuration for a Queue instance.
// It is private; callers build it via functional options.
type queueConfig struct {
	maxMessageSize    int
	maxMetadataSize   int
	defaultMaxRetries int
	maxRetriesSet     bool // true when WithDefaultMaxRetries was supplied
	defaultTTL        time.Duration
	maxQueues         int
	schemaName        string
	logger            *slog.Logger
	tracer            Tracer
	metrics           MetricsRecorder
	backoffPolicy     BackoffPolicy
	backoffConfigured bool // true when WithBackoffPolicy was supplied
	safetyNetPoll     time.Duration
	listener          Listener
}

// WithMaxMessageSize sets the maximum allowed message payload size in bytes.
//
// Zero (the default) selects the built-in default of 256 KiB. Any positive
// value up to MaxAllowedMessageSize (PostgreSQL's bytea per-value limit) is
// honored verbatim. Negative values and values above MaxAllowedMessageSize
// make New return ErrInvalidConfig.
//
// To accept the largest payload PostgreSQL can store, use
// WithMaxMessageSize(MaxAllowedMessageSize).
func WithMaxMessageSize(bytes int) Option {
	return func(c *queueConfig) {
		c.maxMessageSize = bytes
	}
}

// WithMaxMetadataSize sets the maximum allowed size of a message's marshaled
// JSON metadata, in bytes.
//
// Zero (the default) selects the built-in default of 16 KiB. Any positive
// value up to MaxAllowedMetadataSize (PostgreSQL's JSONB per-value limit) is
// honored verbatim. Negative values and values above MaxAllowedMetadataSize
// make New return ErrInvalidConfig.
func WithMaxMetadataSize(bytes int) Option {
	return func(c *queueConfig) {
		c.maxMetadataSize = bytes
	}
}

// WithDefaultMaxRetries sets the default number of delivery attempts before a
// message is moved to the dead-letter queue. The default is 3.
//
// An explicit zero is honored: WithDefaultMaxRetries(0) means a message is
// dead-lettered on its first failed delivery rather than retried.
func WithDefaultMaxRetries(n int) Option {
	return func(c *queueConfig) {
		c.defaultMaxRetries = n
		c.maxRetriesSet = true
	}
}

// WithDefaultTTL sets the default time-to-live for messages. A zero value
// means messages never expire.
//
// TTL only hides expired messages from consumers; it does not delete them. To
// reclaim storage, configure a RetentionPolicy (MaxPendingAge) on a
// GarbageCollector.
func WithDefaultTTL(d time.Duration) Option {
	return func(c *queueConfig) {
		c.defaultTTL = d
	}
}

// WithMaxQueues limits the total number of queues (channels + topics) that may
// be created. Zero (the default) means unlimited.
func WithMaxQueues(n int) Option {
	return func(c *queueConfig) {
		c.maxQueues = n
	}
}

// WithSchema sets the PostgreSQL schema that all pgqueue tables (global and
// per-queue) live in and are qualified with in every DDL/DML statement. The
// default is "public" (FR-024).
//
// The same WithSchema value must be passed to both InitSchema and New: the
// schema is created by InitSchema and all subsequent operations qualify their
// SQL with it. The name must be a plain unquoted PostgreSQL identifier
// (^[a-zA-Z_][a-zA-Z0-9_]*$, at most 63 characters); an invalid name makes
// InitSchema and New return ErrInvalidConfig.
func WithSchema(name string) Option {
	return func(c *queueConfig) {
		c.schemaName = name
	}
}

// WithLogger attaches a structured logger to the Queue. When nil (the default),
// all log output is suppressed.
func WithLogger(l *slog.Logger) Option {
	return func(c *queueConfig) {
		c.logger = l
	}
}

// WithTracer registers a Tracer for emitting distributed tracing spans (FR-017).
func WithTracer(t Tracer) Option {
	return func(c *queueConfig) {
		c.tracer = t
	}
}

// WithMetrics registers a MetricsRecorder for emitting queue metrics (FR-018).
func WithMetrics(m MetricsRecorder) Option {
	return func(c *queueConfig) {
		c.metrics = m
	}
}

// WithBackoffPolicy overrides the decorrelated-jitter backoff policy used when
// a message is nacked (FR-023). The default is DefaultBackoffPolicy().
//
// Supplying a policy also enables backoff on visibility-timeout reclaim: a
// message whose claim times out is returned to the queue with the configured
// backoff delay before it becomes eligible for redelivery (R-05). Without
// WithBackoffPolicy, timeout reclaim is immediate.
func WithBackoffPolicy(p BackoffPolicy) Option {
	return func(c *queueConfig) {
		c.backoffPolicy = p
		c.backoffConfigured = true
	}
}

// WithSafetyNetPoll sets the polling interval used as a fallback when
// LISTEN/NOTIFY notifications are missed (FR-016). Zero disables the safety-net
// poll.
//
// Disabling the poll is only safe when no Listener is registered for push
// delivery, or when latency on a missed notification is acceptable. A Listener
// can miss notifications — for example NOTIFYs that fire while it is
// reconnecting after a dropped connection are lost — and the safety-net poll is
// what recovers delivery in that window. With both a Listener registered and
// the poll disabled, a consumer can stall until the next publish wakes it.
func WithSafetyNetPoll(d time.Duration) Option {
	return func(c *queueConfig) {
		c.safetyNetPoll = d
	}
}

// WithListener registers a Listener for push-based delivery via PostgreSQL
// LISTEN/NOTIFY (FR-014). With one registered, blocking consume loops wake
// immediately when a message is published instead of waiting for the next
// safety-net poll. Without one, consumption falls back to polling. Concrete
// driver-backed Listener implementations ship as optional sub-packages.
func WithListener(l Listener) Option {
	return func(c *queueConfig) {
		c.listener = l
	}
}

// applyConfigOptions applies functional options on top of a zero config, then
// fills in defaults for unset fields.
func applyConfigOptions(opts []Option) queueConfig {
	c := queueConfig{}
	for _, o := range opts {
		o(&c)
	}
	if c.maxMessageSize == 0 {
		c.maxMessageSize = defaultMaxMessageSize
	}
	if c.maxMetadataSize == 0 {
		c.maxMetadataSize = defaultMaxMetadataSize
	}
	// Apply the default of 3 only when WithDefaultMaxRetries was not supplied,
	// so an explicit WithDefaultMaxRetries(0) is honored as "no retries".
	if !c.maxRetriesSet {
		c.defaultMaxRetries = 3
	}
	if c.schemaName == "" {
		c.schemaName = "public"
	}
	// Complete the backoff policy per-field so a partially-specified policy
	// (e.g. only MaxDelay set) still has sane BaseDelay/Multiplier values
	// (R-15). normalized() is a no-op on an already-complete policy.
	c.backoffPolicy = c.backoffPolicy.normalized()
	return c
}
