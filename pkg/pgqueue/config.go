package pgqueue

import (
	"log/slog"
	"time"
)

// defaultMaxMessageSize is 256 KiB, per the spec (FR-032).
const defaultMaxMessageSize = 256 * 1024

// Option is a functional configuration option for New and InitSchema.
type Option func(*queueConfig)

// queueConfig holds the resolved configuration for a Queue instance.
// It is private; callers build it via functional options.
type queueConfig struct {
	maxMessageSize    int
	defaultMaxRetries int
	defaultTTL        time.Duration
	maxQueues         int
	schemaName        string
	logger            *slog.Logger
	tracer            Tracer
	metrics           MetricsRecorder
	backoffPolicy     BackoffPolicy
	safetyNetPoll     time.Duration
}

// WithMaxMessageSize sets the maximum allowed message payload size in bytes.
// The default is 256 KiB.
func WithMaxMessageSize(bytes int) Option {
	return func(c *queueConfig) {
		c.maxMessageSize = bytes
	}
}

// WithDefaultMaxRetries sets the default number of delivery attempts before a
// message is moved to the dead-letter queue. The default is 3.
func WithDefaultMaxRetries(n int) Option {
	return func(c *queueConfig) {
		c.defaultMaxRetries = n
	}
}

// WithDefaultTTL sets the default time-to-live for messages. A zero value
// means messages never expire.
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

// WithSchema sets the PostgreSQL schema that pgqueue tables will be qualified
// with when using schema-qualified DDL/DML. The default is "public" (FR-024).
//
// NOTE: Full DML schema-qualification across all SQL statements is a later task
// (T062). This option documents the intent and stores the value; it is not yet
// wired into all queries.
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
func WithBackoffPolicy(p BackoffPolicy) Option {
	return func(c *queueConfig) {
		c.backoffPolicy = p
	}
}

// WithSafetyNetPoll sets the polling interval used as a fallback when
// LISTEN/NOTIFY notifications are missed (FR-016). Zero disables the safety-net
// poll.
func WithSafetyNetPoll(d time.Duration) Option {
	return func(c *queueConfig) {
		c.safetyNetPoll = d
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
	if c.defaultMaxRetries == 0 {
		c.defaultMaxRetries = 3
	}
	if c.schemaName == "" {
		c.schemaName = "public"
	}
	if c.backoffPolicy == (BackoffPolicy{}) {
		c.backoffPolicy = DefaultBackoffPolicy()
	}
	return c
}

// configFromLegacy converts the old Config struct fields into functional options.
// It is used by the backward-compatible Init constructor.
func configFromLegacy(cfg Config) []Option {
	opts := []Option{
		WithMaxMessageSize(cfg.MaxMessageSize),
		WithDefaultMaxRetries(cfg.DefaultMaxRetries),
		WithDefaultTTL(cfg.DefaultTTL),
		WithMaxQueues(cfg.MaxQueues),
	}
	if cfg.Logger != nil {
		opts = append(opts, WithLogger(cfg.Logger))
	}
	return opts
}
