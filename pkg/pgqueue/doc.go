/*
Package pgqueue provides a PostgreSQL-based message queue library with exactly-once delivery guarantees.

pgqueue supports two messaging patterns:
  - Channels: Point-to-point queuing where each message is consumed by a single worker
  - Pub/Sub: Fan-out messaging where all subscribers receive each message

Key Features:
  - Exactly-once delivery semantics using visibility timeouts
  - UUIDv7 time-ordered message IDs
  - Dead letter queue (DLQ) for failed messages
  - Message replay capabilities
  - Garbage collection for automatic cleanup
  - Built on PostgreSQL 18+ native features

Basic Usage:

	pq, _ := pgqueue.Init(ctx, pgqueue.Config{DB: db})
	pq.CreateChannel(ctx, "orders", pgqueue.ChannelOptions{})
	msgID, _ := pq.Publish(ctx, "orders", []byte("order-123"))
	msg, _ := pq.ConsumeFromChannel(ctx, "orders", 30*time.Second)
	pq.AckChannel(ctx, "orders", msg.ID)

For complete examples, see the examples/ directory.
*/
package pgqueue
