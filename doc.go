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

A finite TTL (WithQueueTTL or the queue-wide WithDefaultTTL) must comfortably
exceed the worst-case backoff horizon a message can accumulate across its
retries (BaseDelay * Multiplier^MaxRetries, capped at MaxDelay — see
BackoffPolicy). Otherwise a message that keeps failing can have its
available_at pushed past created_at+TTL by repeated backoff before it exhausts
its retries: the consume queries' TTL cutoff then excludes it from every future
delivery, but the garbage collector only dead-letters on retry-count
exhaustion, not on TTL, so the message is neither redelivered nor
dead-lettered — it is silently stranded. WithQueueTTL(0) (no expiry) avoids
this entirely.

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

Close shutdown order: Close marks the Queue closed, then cancels the
background context that signals handler-based consume loops
(ConsumeChannel/ConsumeTopic) to wind down, then stops each GarbageCollector
created via NewGarbageCollector (Close calls gc.Stop(), which itself blocks
until the GC goroutine exits), then waits for all consume-loop workers to
drain (workerWG.Wait), and finally closes the LISTEN/NOTIFY listener. An
in-progress GC purge yields at its next checkpoint, not immediately, so
callers should allow a grace period before treating a slow Close as stuck.
After Close returns, no Queue-owned goroutine issues a database query and the
underlying DB handle can be safely closed.

Pub/Sub subscribe-before-publish ordering: messages are fanned out to the
set of active subscribers recorded at publish time. A subscriber registered
after a message is published does not receive that message. For guaranteed
delivery, the Subscribe call must complete and become visible before the
first Publish or PublishBatch call.
Concurrent registration and publish are a race under READ COMMITTED: whether
the newly registered subscriber is included in the fan-out depends on
transaction commit order. This same race applies to PublishBatch: the batch
transaction snapshots active subscribers before it commits, so a subscriber
that registers concurrently may or may not be included. Batched publishes are
not atomic with subscriber registration.

Advisory-lock key design: pgqueue uses two PostgreSQL advisory-lock keys.
Both are encoded from the ASCII bytes of short human-readable tags
("pgqueue" and "pgquecq") so that pg_locks rows are identifiable from psql
without a code lookup. The tags were chosen to avoid collisions with the
advisory-lock ranges used by common PostgreSQL extensions (pg_partman,
pglogical, citus). The keys are not registered with any PostgreSQL extension
registry; operators sharing the advisory-lock space with other software that
uses keys in the same numeric range should review migrations.go for the
exact values and the encoding scheme.

Scalability ceiling: each queue creates 2-3 tables (a channel: message + DLQ;
a pub/sub topic: message + subscription + DLQ) plus their indexes — 8 per
channel and 13 per topic, counting primary keys and the subscription table's
UNIQUE(message_id, subscriber_id) constraint — and admin operations
(GarbageCollector.Collect, ListChannels, ListTopics, UnhealthySubscribers)
scale linearly with queue count. The table-per-queue design targets tens to low
hundreds of queues per database; it is not appropriate for per-tenant/per-user
queues at multi-tenant scale. Use WithMaxQueues to enforce a deliberate cap.
See ADR-002 in ADR.md.

Queue names are trusted input, not a security boundary. They are validated to
^[a-zA-Z0-9_-]+$ and to 1-28 bytes (the length cap keeps the longest generated
index name inside PostgreSQL's 63-byte identifier limit, so two distinct queues
cannot truncate to the same index name), and they become physical table names,
but pgqueue does not isolate one caller's names from another's: the sanitizer
replaces dashes with underscores and lowercases, so names differing only by
dash/underscore or by case ("orders", "Orders", "ord-ers", "ord_ers") all
sanitize to the same table and collide, and a creation-time collision error
echoes the conflicting queue's original name. Do
not derive queue names directly from untrusted or per-tenant input — map
external identifiers to your own vetted queue names instead.

For complete examples, see the examples/ directory.
*/
package pgqueue
