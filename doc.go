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
	msgID, err := pq.Publish(ctx, "orders", []byte("order-123"))
	// handle err
	msg, err := pq.ReceiveChannel(ctx, "orders") // ErrQueueEmpty if none
	// handle err
	_ = pq.Ack(ctx, msg.Receipt())

Per-queue creation options (TTL, max retries, message size) are functional
options passed to CreateChannel/CreateTopic — WithQueueTTL, WithQueueMaxRetries,
WithQueueMaxMessageSize.

The default message-size cap is 256 KiB. To allow larger payloads, pass an
explicit size to WithMaxMessageSize or WithQueueMaxMessageSize, up to
MaxAllowedMessageSize (PostgreSQL's bytea per-value limit, 1 GiB).

Storage retention is opt-in. Message redelivery on a crashed consumer and DLQ
promotion of retry-exhausted messages happen without any extra setup, but old
rows (completed messages, DLQ entries, acked subscriptions) are only reclaimed
by a GarbageCollector: construct one with NewGarbageCollector and call Start.
NewGarbageCollector substitutes default retention when given an empty
DefaultPolicy, so the GC bounds table growth out of the box; pass an explicit
RetentionPolicy to tune it, or KeepForever fields to retain rows indefinitely.
See NewGarbageCollector and RetentionPolicy.

Transaction isolation: every internal transaction is opened with an explicit
READ COMMITTED isolation level (the PostgreSQL default). The library's
correctness arguments — FOR UPDATE SKIP LOCKED for ack races, statement-level
snapshots for paged purges, claim-id matching for visibility-timeout
reclamation — are written against READ COMMITTED. Operators must not change
the pool's default_transaction_isolation to a higher level expecting pgqueue
to inherit it; that setting is ignored by design (#64).

Scalability ceiling: each queue creates 2-3 tables plus 6-7 indexes, and
admin operations (GarbageCollector.Collect, ListChannels, ListTopics,
GetUnhealthySubscribers) scale linearly with queue count. The table-per-queue
design targets tens to low hundreds of queues per database; it is not
appropriate for per-tenant/per-user queues at multi-tenant scale. Use
WithMaxQueues to enforce a deliberate cap. See ADR-002 in ADR.md.

For complete examples, see the examples/ directory.
*/
package pgqueue
