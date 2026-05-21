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
  - Garbage collection for automatic cleanup
  - Built on PostgreSQL 18+ native features

Basic Usage:

	_ = pgqueue.InitSchema(ctx, db) // once per database
	pq, err := pgqueue.Init(ctx, pgqueue.Config{DB: db})
	// handle err
	_ = pq.CreateChannel(ctx, "orders", pgqueue.ChannelOptions{})
	msgID, err := pq.Publish(ctx, "orders", []byte("order-123"))
	// handle err
	msg, err := pq.ConsumeFromChannel(ctx, "orders", 30*time.Second)
	// handle err
	_ = pq.AckChannel(ctx, "orders", msg.Receipt())

For complete examples, see the examples/ directory.
*/
package pgqueue
