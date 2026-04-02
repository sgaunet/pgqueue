package pgqueue

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
)

// QueueType represents the type of queue.
type QueueType string

const (
	// QueueTypePubSub represents a fan-out pub/sub topic.
	QueueTypePubSub QueueType = "pubsub"
	// QueueTypeChannel represents a point-to-point channel.
	QueueTypeChannel QueueType = "channel"
)

// MessageStatus represents the current state of a message.
type MessageStatus string

const (
	// MessageStatusPending indicates a message is waiting to be consumed.
	MessageStatusPending MessageStatus = "pending"
	// MessageStatusProcessing indicates a message is currently being processed.
	MessageStatusProcessing MessageStatus = "processing"
	// MessageStatusCompleted indicates a message has been successfully processed.
	MessageStatusCompleted MessageStatus = "completed"
	// MessageStatusFailed indicates a message has failed processing.
	MessageStatusFailed MessageStatus = "failed"
)

// Config holds the configuration for PGQueue.
type Config struct {
	DB                *sql.DB       // Database connection (user-managed)
	MaxMessageSize    int           // Maximum message size in bytes (default: 1024)
	DefaultMaxRetries int           // Default maximum retry attempts (default: 3)
	DefaultTTL        time.Duration // Default message TTL (0 = no expiration)
}

// TopicOptions holds configuration for a pub/sub topic.
type TopicOptions struct {
	MaxMessageSize int           // Maximum message size (0 = use default)
	TTL            time.Duration // Message time-to-live (0 = no expiration)
	MaxRetries     int           // Maximum retry attempts per subscriber (0 = use default)
	RetentionTTL   time.Duration // How long to retain completed messages (0 = immediate cleanup)
}

// ChannelOptions holds configuration for a point-to-point channel.
type ChannelOptions struct {
	MaxMessageSize       int           // Maximum message size (0 = use default)
	TTL                  time.Duration // Message time-to-live (0 = no expiration)
	MaxRetries           int           // Maximum retry attempts (0 = use default)
	RetentionTTL         time.Duration // How long to retain completed messages (0 = immediate cleanup)
	MaxConcurrency       int           // Maximum concurrent consumers (0 = unlimited)
	VisibilityTimeout    time.Duration // How long a message is invisible after being consumed (default: 30s)
	AcknowledgmentTimeout time.Duration // Maximum time to acknowledge a message (0 = no deadline)
}

// Message represents a message in the queue.
type Message struct {
	ID                uuid.UUID
	Payload           []byte
	CreatedAt         time.Time
	Status            MessageStatus
	RetryCount        int
	MaxRetries        int
	VisibilityTimeout *time.Time
	AckDeadline       *time.Time
	ProcessedAt       *time.Time
	ErrorMessage      *string
	Metadata          map[string]any
}

// Subscription represents a pub/sub subscription record.
type Subscription struct {
	ID           uuid.UUID
	MessageID    uuid.UUID
	SubscriberID string
	Status       MessageStatus
	CreatedAt    time.Time
	AckedAt      *time.Time
}

// QueueMetadata holds information about a queue (database model).
type QueueMetadata struct {
	ID        uuid.UUID
	QueueType string
	QueueName string
	TableName string
	Config    []byte // json.RawMessage from database
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Subscriber represents a pub/sub subscriber registration (database model).
type Subscriber struct {
	ID           uuid.UUID
	TopicName    string
	SubscriberID string
	CreatedAt    time.Time
	Active       bool
}

// ReplayLog represents an audit log entry for replay operations (database model).
type ReplayLog struct {
	ID           uuid.UUID
	QueueType    string
	QueueName    string
	ReplayType   string
	ReplayParams []byte // json.RawMessage from database
	MessageCount int
	CreatedAt    time.Time
	CreatedBy    *string // nullable string
}

// QueueStats holds statistics about a queue.
type QueueStats struct {
	QueueName         string
	PendingCount      int64
	ProcessingCount   int64
	CompletedCount    int64
	FailedCount       int64
	DLQCount          int64
	AvgProcessingTime *time.Duration
	OldestPendingAge  *time.Duration
}

// DLQMessage represents a message in the dead letter queue.
type DLQMessage struct {
	ID                uuid.UUID
	OriginalMessageID uuid.UUID
	Payload           []byte
	FailureReason     string
	RetryCount        int
	MovedAt           time.Time
	Metadata          map[string]any
}

// RetentionPolicy defines how messages should be garbage collected.
type RetentionPolicy struct {
	CompletedMessageTTL time.Duration // How long to keep completed messages (0 = forever)
	MaxPendingAge       time.Duration // Maximum age for pending messages (0 = no limit)
	DLQRetention        time.Duration // How long to keep DLQ messages (0 = forever)
}

// GarbageCollectorConfig holds configuration for the garbage collector.
type GarbageCollectorConfig struct {
	Interval time.Duration          // How often to run garbage collection
	Policies map[string]RetentionPolicy // Policies per queue (queue name -> policy)
	DefaultPolicy RetentionPolicy    // Default policy for queues without specific policy
}

// ReplayOptions holds options for replay operations.
type ReplayOptions struct {
	DryRun      bool   // If true, return count without performing replay
	Limit       int    // Maximum number of messages to replay (0 = no limit)
	Confirm     bool   // Explicit confirmation required
	PerformedBy string // Who initiated the replay (for audit log)
}
