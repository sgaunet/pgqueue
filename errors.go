package pgqueue

import (
	"errors"
	"fmt"
)

// errInvalidSubscriberID is the package-internal sentinel built from the actual
// length constant so the message stays in sync with validation (issue #128).
// The exported ErrInvalidSubscriberID alias below preserves errors.Is semantics.
var errInvalidSubscriberID = fmt.Errorf(
	"invalid subscriber ID: must be 1-%d characters, alphanumeric, underscores, and dashes only",
	maxSubscriberIDLength,
)

// Sentinel errors returned by pgqueue operations.
var (
	// ErrDBRequired is returned when a nil database connection is provided.
	ErrDBRequired = errors.New("database connection is required")

	// ErrInvalidConfig is returned by New, InitSchema, and the Create/Replay
	// APIs when a configuration value is out of range. This covers more than
	// negative numbers: sizes above MaxAllowedMessageSize/MaxAllowedMetadataSize,
	// an invalid schema name, an invalid backoff policy (negative delays,
	// multiplier in (0,1), or MaxDelay < BaseDelay), a negative safety-net poll
	// or per-queue max-retries, a negative replay limit, and InitSchema being
	// given an option other than WithSchema/WithLogger.
	ErrInvalidConfig = errors.New("invalid config")

	// ErrInvalidQueueName is returned when a queue name contains invalid characters.
	ErrInvalidQueueName = errors.New(
		"invalid queue name: must contain only alphanumeric characters, underscores, and dashes",
	)

	// ErrQueueAlreadyExists is returned when attempting to create a queue that already exists.
	ErrQueueAlreadyExists = errors.New("queue already exists")

	// ErrQueueNotFound is returned when a queue or topic cannot be found.
	// Because ErrTopicNotFound wraps it, errors.Is(err, ErrQueueNotFound)
	// also matches a missing topic.
	ErrQueueNotFound = errors.New("queue not found")

	// ErrTopicNotFound is returned when a topic cannot be found. It wraps
	// ErrQueueNotFound so callers can match either the topic-specific sentinel
	// or the general not-found one with errors.Is, while ErrTopicNotFound still
	// distinguishes the topic-specific path.
	ErrTopicNotFound = fmt.Errorf("topic %w", ErrQueueNotFound)

	// ErrDuplicateMessageID is returned when publishing a message with an ID that already exists.
	ErrDuplicateMessageID = errors.New("duplicate message ID")

	// ErrMessageSizeExceeded is returned when a message payload exceeds the configured limit.
	ErrMessageSizeExceeded = errors.New("message size exceeds limit")

	// ErrMetadataSizeExceeded is returned when the marshaled JSON metadata of a
	// message exceeds the configured per-queue or queue-wide cap.
	ErrMetadataSizeExceeded = errors.New("message metadata size exceeds limit")

	// ErrMessageNotFound is returned when a message cannot be found or is not in the expected state.
	ErrMessageNotFound = errors.New("message not found or not in processing state")

	// ErrMessageAlreadyAcked is returned when attempting to ack a message that was already acknowledged.
	ErrMessageAlreadyAcked = errors.New("message not found or already acknowledged")

	// ErrClaimExpired is returned by Ack/Nack when the receipt's claim token no
	// longer matches the message: its visibility timeout lapsed and it was
	// redelivered to another consumer. The caller's processing result must be
	// discarded — the message now belongs to whoever holds the current claim.
	ErrClaimExpired = errors.New("message claim expired: reassigned to another consumer")

	// ErrReplayMessageNotFound is returned when a message targeted for replay
	// cannot be found. It wraps ErrMessageNotFound so callers can match either
	// the specific replay sentinel or the general one with errors.Is.
	ErrReplayMessageNotFound = fmt.Errorf("replay: %w", ErrMessageNotFound)

	// ErrMessageInProcessing is returned when attempting to replay a message
	// that is currently being processed.
	ErrMessageInProcessing = errors.New("message is currently being processed and cannot be replayed")

	// ErrReplayNotSupported is returned when ReplayMessage is called on a pub/sub queue.
	// Pub/sub message tables do not track status; use ReplayFrom or ReplayDLQ instead.
	ErrReplayNotSupported = errors.New("ReplayMessage is not supported for pub/sub queues; use ReplayFrom or ReplayDLQ")

	// ErrNilPayload is returned when a nil payload is provided to Publish.
	ErrNilPayload = errors.New("payload must not be nil")

	// ErrBatchTooLarge is returned when a batch operation exceeds the maximum batch size.
	ErrBatchTooLarge = errors.New("batch size exceeds maximum allowed")

	// ErrQueuePaused is returned when attempting to consume from a paused queue.
	ErrQueuePaused = errors.New("queue is paused")

	// ErrSubscriberNotFound is returned when a subscriber cannot be found or is already inactive.
	ErrSubscriberNotFound = errors.New("subscriber not found or already inactive")

	// ErrInvalidSubscriberID is returned when a subscriber ID is empty, too long,
	// or contains invalid characters. The message is built from
	// maxSubscriberIDLength so it cannot drift from the validation (issue #128).
	ErrInvalidSubscriberID = errInvalidSubscriberID

	// ErrUnsupportedPGVersion is returned when the PostgreSQL server version is below 18.
	ErrUnsupportedPGVersion = errors.New("pgqueue requires PostgreSQL 18+")

	// ErrMaxQueuesReached is returned when creating a queue would exceed the
	// configured Config.MaxQueues limit.
	ErrMaxQueuesReached = errors.New("maximum number of queues reached")

	// ErrInvalidVisibilityTimeout is returned when the visibility timeout is out of bounds.
	ErrInvalidVisibilityTimeout = errors.New(
		"visibility timeout must be between 1ms and 24h",
	)

	// ErrSchemaNotInitialized is returned by Init when the pgqueue schema is
	// absent from the database. Call InitSchema before Init.
	ErrSchemaNotInitialized = errors.New(
		"pgqueue schema is not initialized: call InitSchema first",
	)

	// ErrSchemaOutdated is returned by Init when the database schema is older
	// than the version this build of pgqueue requires. Run InitSchema to migrate.
	ErrSchemaOutdated = errors.New(
		"pgqueue schema is outdated: run InitSchema to migrate",
	)

	// ErrQueueEmpty is returned by single-shot consume operations when no
	// message is currently available. It is an expected, non-fatal signal:
	// callers should treat it as "try again later", not as a failure.
	ErrQueueEmpty = errors.New("queue is empty: no message currently available")

	// ErrQueueClosed is returned by any operation invoked after Close has been
	// called on the pgqueue handle.
	ErrQueueClosed = errors.New("pgqueue handle is closed")

	// ErrReceiptMissingQueueType is returned by the queue-agnostic Ack/Nack
	// when a Receipt was not populated by ReceiveChannel or ReceiveTopic and
	// therefore does not carry the required queue binding.
	ErrReceiptMissingQueueType = errors.New(
		"receipt missing QueueType: obtain the receipt via ReceiveChannel or ReceiveTopic",
	)

	// ErrInvalidPerformedBy is returned when ReplayOptions.PerformedBy is
	// longer than MaxPerformedByLen bytes or contains NUL/CR/LF, which would
	// either bloat or break later inspection of pgqueue_replay_log.
	ErrInvalidPerformedBy = errors.New(
		"invalid PerformedBy: must be at most 256 bytes and contain no NUL/CR/LF",
	)

	// ErrUnexpectedMessageStatus is returned by reclaim paths when a row is
	// found in a status that is neither pending nor processing — a state that
	// indicates schema drift or a regressed migration (#65).
	ErrUnexpectedMessageStatus = errors.New("unexpected message status")
)
