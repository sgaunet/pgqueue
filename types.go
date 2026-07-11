package pgqueue

import (
	"encoding/json"
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

// topicOptions holds configuration for a pub/sub topic.
type topicOptions struct {
	MaxMessageSize  int           // Maximum message size (0 = use default)
	MaxMetadataSize int           // Maximum marshaled metadata size (0 = use default)
	TTL             time.Duration // Message time-to-live (0 = no expiration)
	MaxRetries      int           // Maximum retry attempts per subscriber
	// MaxRetriesSet records whether MaxRetries was set explicitly, so an
	// explicit MaxRetries of 0 ("no retries") is distinguishable from "unset".
	MaxRetriesSet bool
}

// channelOptions holds configuration for a point-to-point channel.
type channelOptions struct {
	MaxMessageSize  int           // Maximum message size (0 = use default)
	MaxMetadataSize int           // Maximum marshaled metadata size (0 = use default)
	TTL             time.Duration // Message time-to-live (0 = no expiration)
	MaxRetries      int           // Maximum retry attempts
	// MaxRetriesSet records whether MaxRetries was set explicitly, so an
	// explicit MaxRetries of 0 ("no retries") is distinguishable from "unset".
	MaxRetriesSet bool
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
	// receiptSet records whether a queue-aware receipt was explicitly bound via
	// SetReceipt. It is the authoritative flag — a receipt is honored even when
	// its QueueName is empty, which a bare receipt.QueueName != "" check missed.
	receiptSet bool
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

// Receipt returns the acknowledgement credential for this message. Messages
// obtained via ReceiveChannel or ReceiveTopic carry the queue binding
// (QueueName + QueueType, and SubscriberID for topics), so the returned Receipt
// can be passed directly to the queue-agnostic Ack, Nack, AckBatch and
// NackBatch methods.
func (m *Message) Receipt() Receipt {
	// If a queue-aware receipt was bound by Receive* (or SetReceipt), return it
	// verbatim — including the case of an empty QueueName, which a test double
	// may legitimately use.
	if m.receiptSet {
		return m.receipt
	}
	return Receipt{MessageID: m.ID, ClaimID: m.ClaimID}
}

// BatchResult reports the per-receipt outcome of AckBatch / NackBatch. When
// those methods return a nil error the batch was processed and this enumerates
// the result: Succeeded lists the receipts whose ack/nack committed, and Failed
// lists the receipts that did not match a live claim, each with the reason. A
// non-nil error from AckBatch / NackBatch signals an operational failure (the
// queue is closed, the batch is too large, a receipt is missing its queue
// binding, the queue does not exist, or a database error occurred); in that
// case the returned BatchResult is zero and should be ignored.
//
// Partial success is not an error: a batch where some claims have expired
// returns a nil error with those receipts in Failed, so the caller can retry or
// discard them individually.
type BatchResult struct {
	// Succeeded holds the receipts whose ack/nack was applied, in input order
	// within each queue.
	Succeeded []Receipt
	// Failed holds the receipts that did not match a live processing claim,
	// each paired with the reason. Receipts are assumed unique within a batch.
	Failed []FailedReceipt
}

// FailedReceipt pairs a receipt with the reason its ack/nack did not apply. The
// Reason is one of ErrClaimExpired (redelivered to another consumer),
// ErrMessageAlreadyAcked (already acked/nacked, or never legitimately
// consumed), or ErrMessageNotFound (no such message), classified identically to
// the single-receipt Ack/Nack path. Test it with errors.Is.
type FailedReceipt struct {
	Receipt Receipt
	Reason  error
}

// SetReceipt binds a queue-aware Receipt onto a Message so that m.Receipt()
// returns the full binding. ReceiveChannel and ReceiveTopic call this
// internally; it is exported chiefly so in-memory test doubles (the
// fake package) can build messages that behave identically to ones
// returned by a real Queue. It is a method to pair with the (m *Message)
// Receipt() getter.
func (m *Message) SetReceipt(r Receipt) {
	m.receipt = r
	m.receiptSet = true
}

// queueMetadata holds information about a queue.
type queueMetadata struct {
	ID        uuid.UUID
	QueueType QueueType
	QueueName string
	TableName string
	Config    json.RawMessage
	Paused    bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

// subscriber represents a pub/sub subscriber registration.
type subscriber struct {
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
	MessageCount int64
	CreatedAt    time.Time
	CreatedBy    *string // nullable string
}

// QueueStats holds statistics about a queue.
//
// PendingCount is a raw status breakdown: it counts every row in the pending
// state, including any whose TTL has elapsed and which are therefore no longer
// consumable. For the consumable depth (TTL-expired rows excluded) use
// QueueDepth instead.
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

// KeepForever is a RetentionPolicy field value that disables cleanup for that
// field — the rows it governs are never purged. It exists so a completely
// empty GarbageCollectorConfig.DefaultPolicy can be distinguished from one that
// deliberately wants infinite retention: NewGarbageCollector replaces an
// all-zero DefaultPolicy with default retention, but leaves any policy that
// sets at least one field (including a KeepForever field) untouched.
const KeepForever time.Duration = -1

// RetentionPolicy defines how messages should be garbage collected.
//
// A field set to 0 or KeepForever disables cleanup for the rows it governs (a
// positive duration enables it). The one exception is an all-zero policy used
// as GarbageCollectorConfig.DefaultPolicy: NewGarbageCollector treats that as
// "unconfigured" and substitutes default retention, so a GarbageCollector
// created without a policy still bounds table growth. Per-queue Policies
// entries and any DefaultPolicy with at least one field set are used verbatim.
// To run a GarbageCollector that keeps everything forever, set the fields
// explicitly to KeepForever.
//
// For pub/sub topics, MaxPendingAge applies per subscriber: it drops only the
// stale pending subscription rows of a subscriber too slow to process a
// message within the age limit. The shared message row and every other
// subscriber's rows are left intact, so a slow subscriber can no longer cause
// message loss for its peers.
//
// MaxPendingAge is anchored at the message's creation time (created_at, i.e.
// publish time), not its last delivery attempt: a message still pending
// MaxPendingAge after it was first published is dropped regardless of how many
// times it was redelivered in between — redelivery does not reset the clock
// (L6/L7). Set MaxPendingAge comfortably longer than the worst-case processing
// time plus all retries, or leave it 0/KeepForever to never age out live work.
//
// CompletedMessageTTL and DLQRetention may be set independently for pub/sub
// topics — in particular DLQRetention may exceed CompletedMessageTTL, which is
// the usual and default arrangement (keep failures around longer than
// transient successes). The garbage collector never purges a message row while
// a DLQ entry still references it, so the DLQ entry is always reaped first and
// pub/sub DLQ replay always finds its parent message.
//
// CompletedMessageTTL is measured from different anchors per queue type. For
// channels it is the age since completion (processed_at). For pub/sub topics the
// message row has no completion timestamp, so it is the age since publish
// (created_at); a topic message is still only purged once every subscriber has
// acked it (and no DLQ entry references it), so this affects when a fully-acked
// message is reaped, not whether an in-flight one is.
type RetentionPolicy struct {
	CompletedMessageTTL time.Duration // How long to keep completed messages (0 or KeepForever = forever)
	MaxPendingAge       time.Duration // Maximum age for pending messages/deliveries (0 or KeepForever = no limit)
	DLQRetention        time.Duration // How long to keep DLQ messages (0 or KeepForever = forever)
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
	// ID optionally sets the message ID for publish-side dedup. The zero value
	// (uuid.Nil) auto-generates a UUIDv7.
	ID       uuid.UUID
	Payload  []byte         // Message payload (required)
	Metadata map[string]any // Optional message metadata
}

// ReplayOptions holds options for replay operations.
type ReplayOptions struct {
	DryRun      bool   // If true, return count without performing replay
	Limit       int64  // Maximum number of messages to replay (0 = no limit)
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
