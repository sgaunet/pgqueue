/*
Package pgqueue provides a PostgreSQL-based message queue library with
at-least-once delivery.

pgqueue supports two messaging patterns:
  - Channels: Point-to-point queuing where each message is consumed by a single worker
  - Pub/Sub: Fan-out messaging where all subscribers receive each message

Key Features:
  - At-least-once delivery via visibility timeouts; a message whose consumer
    crashes before acknowledging is redelivered (handlers should be idempotent).
    Publishing with an explicit ID deduplicates enqueues.
  - UUIDv7 time-ordered message IDs
  - Dead letter queue (DLQ) for failed messages
  - Message replay capabilities
  - Opt-in garbage collection for storage retention (see NewGarbageCollector)
  - Built on PostgreSQL 18+ native features

Basic Usage:

	_ = pgqueue.InitSchema(ctx, db) // once per database; also migrates
	pq, err := pgqueue.New(ctx, db)
	// handle err
	defer pq.Close()
	_ = pq.CreateChannel(ctx, "orders")
	msgID, err := pq.PublishChannel(ctx, "orders", []byte("order-123"))
	// handle err
	msg, err := pq.ReceiveChannel(ctx, "orders") // ErrQueueEmpty if none
	// handle err
	_ = pq.Ack(ctx, msg.Receipt())

Per-queue creation options (TTL, max retries, message size) are functional
options passed to CreateChannel/CreateTopic — WithQueueTTL, WithQueueMaxRetries,
WithQueueMaxMessageSize.

Storage retention is opt-in. Message redelivery on a crashed consumer and DLQ
promotion of retry-exhausted messages happen without any extra setup, but old
rows (completed messages, DLQ entries, acked subscriptions) are only reclaimed
by a GarbageCollector: construct one with NewGarbageCollector, give it a
RetentionPolicy with positive durations (the zero value keeps rows forever),
and call Start. See NewGarbageCollector and RetentionPolicy.

For complete examples, see the examples/ directory.
*/
package pgqueue
