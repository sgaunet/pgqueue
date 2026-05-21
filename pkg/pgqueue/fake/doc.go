// Package fake provides an in-memory implementation of the pgqueue consumer
// interfaces for unit testing.
//
// It lets code that depends on pgqueue be tested without a PostgreSQL database
// or container, reproducing the documented publish, consume, ack/nack, retry,
// and dead-letter semantics in memory.
//
// The in-memory implementation is added by task T054.
package fake
