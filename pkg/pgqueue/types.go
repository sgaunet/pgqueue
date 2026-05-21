package pgqueue

import (
	"database/sql"
	"encoding/json"
	"log/slog"
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
	// MessageStatusAcked indicates a pub/sub subscription has been acknowledged.
	MessageStatusAcked MessageStatus = "acked"
)

// Config holds the configuration for Queue.
//
// Deprecated: Use New(ctx, db, ...Option) with functional options instead.
// Config is retained for backward compatibility with existing callers.
type Config struct {
	DB                *sql.DB       // Database connection (user-managed)
	MaxMessageSize    int           // Maximum message size in bytes (default: 1024)
	DefaultMaxRetries int           // Default maximum retry attempts (default: 3)
	DefaultTTL        time.Duration // Default message TTL (0 = no expiration)
	MaxQueues         int           // Maximum number of queues (channels + topics) that may exist (0 = unlimited)
	Logger            *slog.Logger  // Optional structured logger (nil = silent, the default)
}

// TopicOptions holds configuration for a pub/sub topic.
type TopicOptions struct {
	MaxMessageSize int           // Maximum message size (0 = use default)
	TTL            time.Duration // Message time-to-live (0 = no expiration)
	MaxRetries     int           // Maximum retry attempts per subscriber (0 = use default)
}

// ChannelOptions holds configuration for a point-to-point channel.
type ChannelOptions struct {
	MaxMessageSize int           // Maximum message size (0 = use default)
	TTL            time.Duration // Message time-to-live (0 = no expiration)
	MaxRetries     int           // Maximum retry attempts (0 = use default)
}

// Message represents a message in the queue.
type Message struct {
	ID                uuid.UUID
	ClaimID           uuid.UUID // Per-claim fencing token; rotates on every (re)delivery.
	Payload           []byte
	CreatedAt         time.Time
	Status            MessageStatus
	RetryCount        int
	MaxRetries        int
	VisibilityTimeout *time.Time
	ProcessedAt       *time.Time
	ErrorMessage      *string
	Metadata          map[string]any

	// receipt is the pre-populated Receipt set by ReceiveChannel/ReceiveTopic.
	// When set it carries the queue binding so the queue-agnostic Ack/Nack
	// methods work without a queue-name argument. It is unexported so the
	// struct can still be instantiated by callers without this field.
	receipt Receipt
}

// Receipt is the credential a consumer must present to acknowledge a message.
// It pairs the message ID with the claim token (ClaimID) issued by the consume
// call that delivered the message. Ack/Nack only act on a message whose current
// claim token still matches: if the consumer's visibility timeout lapsed and the
// message was redelivered to someone else, the stale receipt is rejected with
// ErrClaimExpired instead of corrupting the new consumer's work.
//
// QueueName and QueueType carry the queue binding so the queue-agnostic Ack/Nack
// methods do not require the caller to supply the queue name again.
// SubscriberID is populated for topic messages; it is empty for channel messages.
type Receipt struct {
	MessageID    uuid.UUID
	ClaimID      uuid.UUID
	QueueName    string    // queue/topic name; populated by Receive* methods
	QueueType    QueueType // QueueTypeChannel or QueueTypePubSub
	SubscriberID string    // populated for topic subscriptions; empty for channels
}

// Receipt returns the acknowledgement credential for this message. When the
// message was obtained via ReceiveChannel or ReceiveTopic, the returned Receipt
// carries the queue binding and can be passed directly to the queue-agnostic
// Ack/Nack methods.
//
// When using the legacy ConsumeFromChannel/ConsumeFromTopic APIs, the Receipt
// will not carry the queue binding; pass it to AckChannel/NackChannel or
// AckTopic/NackTopic instead.
func (m *Message) Receipt() Receipt {
	// If a pre-populated receipt was set by Receive*, return it.
	if m.receipt.QueueName != "" {
		return m.receipt
	}
	return Receipt{MessageID: m.ID, ClaimID: m.ClaimID}
}

// QueueMetadata holds information about a queue.
type QueueMetadata struct {
	ID        uuid.UUID
	QueueType QueueType
	QueueName string
	TableName string
	Config    json.RawMessage
	Paused    bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Subscriber represents a pub/sub subscriber registration.
type Subscriber struct {
	ID           uuid.UUID
	TopicName    string
	SubscriberID string
	CreatedAt    time.Time
	Active       bool
}

// ReplayLog represents an audit log entry for replay operations.
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
//
// WARNING for pub/sub topics: MaxPendingAge deletes the parent message row, which
// cascades to ALL subscription records for that message — including those already
// acked by other subscribers. Use with caution on pub/sub topics where subscribers
// process at different speeds.
type RetentionPolicy struct {
	CompletedMessageTTL time.Duration // How long to keep completed messages (0 = forever)
	MaxPendingAge       time.Duration // Maximum age for pending messages (0 = no limit; see WARNING above for pub/sub)
	DLQRetention        time.Duration // How long to keep DLQ messages (0 = forever)
}

// GarbageCollectorConfig holds configuration for the garbage collector.
type GarbageCollectorConfig struct {
	Interval      time.Duration              // How often to run garbage collection
	Policies      map[string]RetentionPolicy // Policies per queue (queue name -> policy)
	DefaultPolicy RetentionPolicy            // Default policy for queues without specific policy
	MaxWorkers    int                        // Max concurrent GC operations (default: 10)
}

// MaxBatchSize is the maximum number of messages allowed in a single batch operation.
const MaxBatchSize = 1000

// PublishMessage represents a message to be published in a batch operation.
type PublishMessage struct {
	Payload  []byte         // Message payload (required)
	Metadata map[string]any // Optional message metadata
}

// ReplayOptions holds options for replay operations.
type ReplayOptions struct {
	DryRun      bool   // If true, return count without performing replay
	Limit       int    // Maximum number of messages to replay (0 = no limit)
	Confirm     bool   // Explicit confirmation required
	PerformedBy string // Who initiated the replay (for audit log)
}

// SubscriberHealth holds health information for a subscriber.
type SubscriberHealth struct {
	TopicName       string
	SubscriberID    string
	PendingMessages int64
	StuckMessages   int64
	OldestPending   *time.Time
	LastActivity    *time.Time
}

// SubscriberLag holds lag information for a subscriber.
type SubscriberLag struct {
	SubscriberID     string
	TopicName        string
	PendingCount     int64
	ProcessingCount  int64
	AckedCount       int64
	OldestPendingAge *time.Duration
}

// DLQStats holds statistics about the dead letter queue.
type DLQStats struct {
	QueueName     string
	TotalCount    int64
	OldestMovedAt *time.Time
	NewestMovedAt *time.Time
	AvgRetryCount float64
}
