package pgqueue

import (
	"context"

	"github.com/google/uuid"
)

// Publisher is the interface for publishing messages to channels and topics.
// *Queue satisfies this interface. Use it to decouple code from the concrete
// implementation or to substitute a fake.Queue in unit tests.
type Publisher interface {
	PublishChannel(ctx context.Context, name string, payload []byte, opts ...PublishOption) (uuid.UUID, error)
	PublishTopic(ctx context.Context, name string, payload []byte, opts ...PublishOption) (uuid.UUID, error)
}

// ChannelConsumer is the interface for consuming messages from a point-to-point
// channel. *Queue satisfies this interface.
type ChannelConsumer interface {
	ReceiveChannel(ctx context.Context, name string, opts ...ConsumeOption) (*Message, error)
	Ack(ctx context.Context, r Receipt) error
	Nack(ctx context.Context, r Receipt, reason string, opts ...NackOption) error
}

// TopicConsumer is the interface for consuming messages from a pub/sub topic.
// *Queue satisfies this interface.
type TopicConsumer interface {
	ReceiveTopic(ctx context.Context, name, subscriberID string, opts ...ConsumeOption) (*Message, error)
	Ack(ctx context.Context, r Receipt) error
	Nack(ctx context.Context, r Receipt, reason string, opts ...NackOption) error
}

// Compile-time assertions: *Queue must satisfy all published interfaces.
var (
	_ Publisher      = (*Queue)(nil)
	_ ChannelConsumer = (*Queue)(nil)
	_ TopicConsumer  = (*Queue)(nil)
)
