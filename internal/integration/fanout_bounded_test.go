package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/sgaunet/pgqueue"
)

// TestFanOutBounded verifies the H5/L8 fan-out DoS guards:
//   - under WithMaxSubscribersPerTopic, a large PublishBatch still fans every
//     message out to every subscriber correctly — the working set is inserted in
//     bounded chunks, never truncated;
//   - WithMaxBatchBytes rejects an oversized batch before any database work;
//   - once the active-subscriber count exceeds the cap, a publish is rejected
//     with ErrTooManySubscribers rather than fanning out an unbounded row set.
func TestFanOutBounded(t *testing.T) {
	ctx := context.Background()
	db, cleanup := setupTestContainer(t)
	defer cleanup()
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	const subCap = 3
	pq, err := pgqueue.New(ctx, db,
		pgqueue.WithMaxSubscribersPerTopic(subCap),
		pgqueue.WithMaxBatchBytes(64),
	)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	defer func() { _ = pq.Close() }()

	if err := pq.CreateTopic(ctx, "fanout"); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Under the cap: fan-out works. Subscribe subCap subscribers, publish a
	// batch, and assert every subscriber receives every message.
	subs := []string{"s0", "s1", "s2"} // len == subCap
	for _, s := range subs {
		if err := pq.Subscribe(ctx, "fanout", s); err != nil {
			t.Fatalf("subscribe %s: %v", s, err)
		}
	}
	const nMsgs = 5
	msgs := make([]pgqueue.PublishMessage, nMsgs)
	for i := range msgs {
		msgs[i] = pgqueue.PublishMessage{Payload: []byte(fmt.Sprintf("m%d", i))}
	}
	if _, err := pq.PublishBatch(ctx, "fanout", msgs); err != nil {
		t.Fatalf("publish batch under cap: %v", err)
	}
	for _, s := range subs {
		got := 0
		for {
			msg, rerr := pq.ReceiveTopic(ctx, "fanout", s)
			if errors.Is(rerr, pgqueue.ErrQueueEmpty) {
				break
			}
			if rerr != nil {
				t.Fatalf("receive %s: %v", s, rerr)
			}
			got++
			_ = pq.Ack(ctx, msg.Receipt())
		}
		if got != nMsgs {
			t.Fatalf("subscriber %s: want %d fanned-out messages, got %d", s, nMsgs, got)
		}
	}

	// WithMaxBatchBytes rejects an oversized batch before any DB work (the
	// per-message cap is far larger; only the aggregate ceiling trips here).
	oversized := []pgqueue.PublishMessage{{Payload: make([]byte, 100)}}
	if _, err := pq.PublishBatch(ctx, "fanout", oversized); !errors.Is(err, pgqueue.ErrBatchTooLarge) {
		t.Fatalf("oversized batch: want ErrBatchTooLarge, got %v", err)
	}

	// Over the cap: one more subscriber tips the topic past the limit, so a
	// publish is rejected instead of fanning out an unbounded row set.
	if err := pq.Subscribe(ctx, "fanout", "s3"); err != nil {
		t.Fatalf("subscribe s3: %v", err)
	}
	if _, err := pq.Publish(ctx, "fanout", []byte("overflow")); !errors.Is(err, pgqueue.ErrTooManySubscribers) {
		t.Fatalf("publish over cap: want ErrTooManySubscribers, got %v", err)
	}
}
