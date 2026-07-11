package pgqueue

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Publisher is the interface for publishing messages to channels and topics.
// *Queue satisfies this interface. Use it to decouple code from the concrete
// implementation or to substitute a fake.Queue in unit tests.
type Publisher interface {
	Publish(ctx context.Context, name string, payload []byte, opts ...PublishOption) (uuid.UUID, error)
}

// ChannelConsumer is the interface for consuming messages from a point-to-point
// channel, covering all three consume styles plus acknowledgement. *Queue
// satisfies this interface; the fake provides an in-memory double.
type ChannelConsumer interface {
	ConsumeChannel(ctx context.Context, name string, h Handler, opts ...ConsumeOption) error
	ReceiveChannel(ctx context.Context, name string, opts ...ConsumeOption) (*Message, error)
	Ack(ctx context.Context, r Receipt) error
	Nack(ctx context.Context, r Receipt, reason string, opts ...NackOption) error
	ExtendVisibility(ctx context.Context, r Receipt, d time.Duration) error
}

// TopicConsumer is the interface for consuming messages from a pub/sub topic.
// *Queue satisfies this interface; the fake provides an in-memory double.
type TopicConsumer interface {
	ConsumeTopic(ctx context.Context, name, subscriberID string, h Handler, opts ...ConsumeOption) error
	ReceiveTopic(ctx context.Context, name, subscriberID string, opts ...ConsumeOption) (*Message, error)
	Ack(ctx context.Context, r Receipt) error
	Nack(ctx context.Context, r Receipt, reason string, opts ...NackOption) error
	ExtendVisibility(ctx context.Context, r Receipt, d time.Duration) error
}

// Compile-time assertions: *Queue must satisfy every published interface
// (FR-020). The fake package carries the same assertions.
var (
	_ Publisher       = (*Queue)(nil)
	_ ChannelConsumer = (*Queue)(nil)
	_ TopicConsumer   = (*Queue)(nil)
)
