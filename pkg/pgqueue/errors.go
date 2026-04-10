package pgqueue

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by pgqueue operations.
var (
	// ErrDBRequired is returned when a nil database connection is provided.
	ErrDBRequired = errors.New("database connection is required")

	// ErrInvalidQueueName is returned when a queue name contains invalid characters.
	ErrInvalidQueueName = errors.New(
		"invalid queue name: must contain only alphanumeric characters, underscores, and dashes",
	)

	// ErrQueueAlreadyExists is returned when attempting to create a queue that already exists.
	ErrQueueAlreadyExists = errors.New("queue already exists")

	// ErrQueueNotFound is returned when a queue or topic cannot be found.
	ErrQueueNotFound = errors.New("queue not found")

	// ErrTopicNotFound is returned when a topic cannot be found.
	ErrTopicNotFound = errors.New("topic not found")

	// ErrConfirmationRequired is returned when a destructive operation is attempted without confirmation.
	// ErrPurgeNotConfirmed and ErrDeleteNotConfirmed wrap this error, so callers can use
	// errors.Is(err, ErrConfirmationRequired) to catch all confirmation-related errors.
	ErrConfirmationRequired = errors.New(
		"operation requires explicit confirmation or dry-run mode",
	)

	// ErrPurgeNotConfirmed is returned when PurgeQueue is called without confirm=true.
	ErrPurgeNotConfirmed = fmt.Errorf("purge operation requires explicit confirmation: %w", ErrConfirmationRequired)

	// ErrDeleteNotConfirmed is returned when DeleteChannel/DeleteTopic is called without confirm=true.
	ErrDeleteNotConfirmed = fmt.Errorf("delete operation requires explicit confirmation: %w", ErrConfirmationRequired)

	// ErrDuplicateMessageID is returned when publishing a message with an ID that already exists.
	ErrDuplicateMessageID = errors.New("duplicate message ID")

	// ErrMessageSizeExceeded is returned when a message payload exceeds the configured limit.
	ErrMessageSizeExceeded = errors.New("message size exceeds limit")

	// ErrMessageNotFound is returned when a message cannot be found or is not in the expected state.
	ErrMessageNotFound = errors.New("message not found or not in processing state")

	// ErrMessageAlreadyAcked is returned when attempting to ack a message that was already acknowledged.
	ErrMessageAlreadyAcked = errors.New("message not found or already acknowledged")

	// ErrReplayMessageNotFound is returned when a message targeted for replay cannot be found.
	ErrReplayMessageNotFound = errors.New("message not found")

	// ErrMessageInProcessing is returned when attempting to replay a message
	// that is currently being processed.
	ErrMessageInProcessing = errors.New("message is currently being processed and cannot be replayed")

	// ErrAmbiguousQueueName is returned when a queue name exists as both a channel and topic.
	ErrAmbiguousQueueName = errors.New("ambiguous queue name: exists as both channel and topic")

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
	// or contains invalid characters.
	ErrInvalidSubscriberID = errors.New(
		"invalid subscriber ID: must be 1-128 characters, alphanumeric, underscores, and dashes only",
	)

	// ErrUnsupportedPGVersion is returned when the PostgreSQL server version is below 18.
	ErrUnsupportedPGVersion = errors.New("pgqueue requires PostgreSQL 18+")

	// ErrInvalidVisibilityTimeout is returned when the visibility timeout is out of bounds.
	ErrInvalidVisibilityTimeout = errors.New(
		"visibility timeout must be between 1ms and 24h",
	)
)
