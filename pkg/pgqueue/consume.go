// Package pgqueue provides consume.go — single-shot consume APIs (ReceiveChannel,
// ReceiveTopic) plus queue-agnostic Ack/Nack and their batch variants.
// Handler-based and iterator-based consume loops are reserved for Phase 4 (US2).
package pgqueue

import "context"

// Handler is a callback invoked by the handler-based consume loop for each
// delivered message. Return nil to auto-ack; return any non-nil error to
// auto-nack (the error string is recorded as the failure reason).
//
// Handler-based consumption is a future feature (Phase 4, US2). The type is
// declared here so published interfaces can reference it.
type Handler func(ctx context.Context, msg *Message) error

// ReceiveChannel retrieves the next available message from a point-to-point
// channel. Returns ErrQueueEmpty when no message is currently available.
//
// The visibility timeout (default 30 s, override with WithVisibilityTimeout)
// controls how long the caller has to Ack or Nack the message before it
// becomes eligible for redelivery.
//
// The returned Message's Receipt() is pre-populated with the channel binding so
// the queue-agnostic Ack/Nack methods can be used without supplying the queue
// name again.
func (pq *Queue) ReceiveChannel(
	ctx context.Context,
	name string,
	opts ...ConsumeOption,
) (*Message, error) {
	if err := pq.checkClosed(); err != nil {
		return nil, err
	}
	o := applyConsumeOptions(opts)
	vt := o.visibilityTimeout
	if vt <= 0 {
		vt = defaultVisibilityTimeout
	}

	msg, err := pq.ConsumeFromChannel(ctx, name, vt)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, ErrQueueEmpty
	}

	// Stamp queue binding onto the message so Receipt() returns a full Receipt.
	msg.receipt = Receipt{
		MessageID: msg.ID,
		ClaimID:   msg.ClaimID,
		QueueName: name,
		QueueType: QueueTypeChannel,
	}

	return msg, nil
}

// ReceiveTopic retrieves the next available message for a subscriber from a
// pub/sub topic. Returns ErrQueueEmpty when no message is currently available.
//
// See ReceiveChannel for visibility-timeout semantics.
func (pq *Queue) ReceiveTopic(
	ctx context.Context,
	name, subscriberID string,
	opts ...ConsumeOption,
) (*Message, error) {
	if err := pq.checkClosed(); err != nil {
		return nil, err
	}
	o := applyConsumeOptions(opts)
	vt := o.visibilityTimeout
	if vt <= 0 {
		vt = defaultVisibilityTimeout
	}

	msg, err := pq.ConsumeFromTopic(ctx, name, subscriberID, vt)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, ErrQueueEmpty
	}

	msg.receipt = Receipt{
		MessageID:    msg.ID,
		ClaimID:      msg.ClaimID,
		QueueName:    name,
		QueueType:    QueueTypePubSub,
		SubscriberID: subscriberID,
	}

	return msg, nil
}

// Ack acknowledges a message using the Receipt returned by ReceiveChannel or
// ReceiveTopic. The Receipt must carry the queue binding (QueueName + QueueType)
// which is set automatically by those methods.
//
// Returns ErrClaimExpired if the visibility timeout lapsed and the message was
// redelivered to another consumer.
func (pq *Queue) Ack(ctx context.Context, r Receipt) error {
	if err := pq.checkClosed(); err != nil {
		return err
	}
	switch r.QueueType {
	case QueueTypeChannel:
		return pq.AckChannel(ctx, r.QueueName, r)
	case QueueTypePubSub:
		return pq.AckTopic(ctx, r.QueueName, r.SubscriberID, r)
	default:
		return ErrReceiptMissingQueueType
	}
}

// Nack negatively acknowledges a message using the Receipt returned by
// ReceiveChannel or ReceiveTopic. The message is retried (or moved to the DLQ
// if max retries are exhausted). reason is recorded as the failure reason.
//
// Returns ErrClaimExpired if the visibility timeout lapsed.
func (pq *Queue) Nack(
	ctx context.Context,
	r Receipt,
	reason string,
	_ ...NackOption, // reserved: backoff override is wired in Phase 8 (US6)
) error {
	if err := pq.checkClosed(); err != nil {
		return err
	}
	switch r.QueueType {
	case QueueTypeChannel:
		return pq.NackChannel(ctx, r.QueueName, r, reason)
	case QueueTypePubSub:
		return pq.NackTopic(ctx, r.QueueName, r.SubscriberID, r, reason)
	default:
		return ErrReceiptMissingQueueType
	}
}

// AckBatch acknowledges multiple messages using their Receipts. Receipts are
// grouped by queue and acknowledged in a single batch operation per queue.
// Each Receipt must carry the queue binding set by ReceiveChannel/ReceiveTopic.
func (pq *Queue) AckBatch(ctx context.Context, rs []Receipt) error {
	if err := pq.checkClosed(); err != nil {
		return err
	}
	if len(rs) == 0 {
		return nil
	}
	type groupKey struct {
		qt           QueueType
		queueName    string
		subscriberID string
	}
	groups := make(map[groupKey][]Receipt)
	for _, r := range rs {
		k := groupKey{qt: r.QueueType, queueName: r.QueueName, subscriberID: r.SubscriberID}
		groups[k] = append(groups[k], r)
	}
	for k, receipts := range groups {
		var err error
		switch k.qt {
		case QueueTypeChannel:
			err = pq.AckChannelBatch(ctx, k.queueName, receipts)
		case QueueTypePubSub:
			err = pq.AckTopicBatch(ctx, k.queueName, k.subscriberID, receipts)
		default:
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// NackBatch negatively acknowledges multiple messages using their Receipts.
// Messages that have exhausted retries are moved to the DLQ; others are
// retried. reason is recorded as the failure reason.
func (pq *Queue) NackBatch(
	ctx context.Context,
	rs []Receipt,
	reason string,
	_ ...NackOption, // reserved: backoff override is wired in Phase 8 (US6)
) error {
	if err := pq.checkClosed(); err != nil {
		return err
	}
	if len(rs) == 0 {
		return nil
	}
	type groupKey struct {
		qt           QueueType
		queueName    string
		subscriberID string
	}
	groups := make(map[groupKey][]Receipt)
	for _, r := range rs {
		k := groupKey{qt: r.QueueType, queueName: r.QueueName, subscriberID: r.SubscriberID}
		groups[k] = append(groups[k], r)
	}
	for k, receipts := range groups {
		var err error
		switch k.qt {
		case QueueTypeChannel:
			err = pq.NackChannelBatch(ctx, k.queueName, receipts, reason)
		case QueueTypePubSub:
			err = pq.NackTopicBatch(ctx, k.queueName, k.subscriberID, receipts, reason)
		default:
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}
