package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sgaunet/pgqueue"
)

// TestExtendVisibility verifies ExtendVisibility resets an in-flight message's
// visibility lease to d from now, fenced on the claim, without counting as a
// retry — covering both channels and topics.
func TestExtendVisibility(t *testing.T) {
	pq, db, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	// extend_prevents_reclaim: extending before the original lease lapses keeps
	// the message invisible past the original timeout, and the original receipt
	// still acks (the claim token is unchanged by an extend).
	t.Run("channel_extend_prevents_reclaim", func(t *testing.T) {
		const ch = "ext-prevent"
		mustCreateChannel(t, pq, ch)
		if _, err := pq.Publish(ctx, ch, []byte("payload")); err != nil {
			t.Fatalf("publish: %v", err)
		}
		msg, err := pq.ReceiveChannel(ctx, ch, pgqueue.WithVisibilityTimeout(200*time.Millisecond))
		if err != nil || msg == nil {
			t.Fatalf("consume: msg=%v err=%v", msg, err)
		}
		if err := pq.ExtendVisibility(ctx, msg.Receipt(), 5*time.Second); err != nil {
			t.Fatalf("ExtendVisibility: %v", err)
		}
		// intentional: sleep past the original 200ms lease; the 5s extension holds.
		time.Sleep(400 * time.Millisecond)
		if other, err := pq.ReceiveChannel(ctx, ch); !errors.Is(err, pgqueue.ErrQueueEmpty) {
			t.Fatalf("reclaim attempt: msg=%v err=%v, want ErrQueueEmpty (still held)", other, err)
		}
		if err := pq.Ack(ctx, msg.Receipt()); err != nil {
			t.Errorf("Ack with original receipt after extend: %v", err)
		}
	})

	// extend_then_lapse_reclaims: extending to a shorter lease (NOW()+d, not
	// additive) is honored — once the extended lease lapses the message is
	// reclaimed with a fresh claim and the original receipt is expired.
	t.Run("channel_extend_then_lapse_reclaims", func(t *testing.T) {
		const ch = "ext-lapse"
		mustCreateChannel(t, pq, ch)
		if _, err := pq.Publish(ctx, ch, []byte("payload")); err != nil {
			t.Fatalf("publish: %v", err)
		}
		msgA, err := pq.ReceiveChannel(ctx, ch, pgqueue.WithVisibilityTimeout(1*time.Second))
		if err != nil || msgA == nil {
			t.Fatalf("consume A: msg=%v err=%v", msgA, err)
		}
		// Reset the lease to a short 100ms from now (shorter than the original 1s).
		if err := pq.ExtendVisibility(ctx, msgA.Receipt(), 100*time.Millisecond); err != nil {
			t.Fatalf("ExtendVisibility: %v", err)
		}
		// intentional: let the 100ms extended lease lapse.
		time.Sleep(250 * time.Millisecond)
		msgB, err := pq.ReceiveChannel(ctx, ch, pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil || msgB == nil {
			t.Fatalf("reclaim B: msg=%v err=%v", msgB, err)
		}
		if msgA.ClaimID == msgB.ClaimID {
			t.Fatal("expected B to receive a fresh claim token")
		}
		if err := pq.Ack(ctx, msgA.Receipt()); !errors.Is(err, pgqueue.ErrClaimExpired) {
			t.Errorf("stale Ack after extended lease lapsed: got %v, want ErrClaimExpired", err)
		}
		if err := pq.Ack(ctx, msgB.Receipt()); err != nil {
			t.Errorf("current Ack: %v", err)
		}
	})

	// extend_stale_claim: extending a claim already reclaimed by another consumer
	// fails with ErrClaimExpired.
	t.Run("channel_extend_stale_claim", func(t *testing.T) {
		const ch = "ext-stale"
		mustCreateChannel(t, pq, ch)
		if _, err := pq.Publish(ctx, ch, []byte("payload")); err != nil {
			t.Fatalf("publish: %v", err)
		}
		msgA, err := pq.ReceiveChannel(ctx, ch, pgqueue.WithVisibilityTimeout(50*time.Millisecond))
		if err != nil || msgA == nil {
			t.Fatalf("consume A: msg=%v err=%v", msgA, err)
		}
		// intentional: let A's 50ms lease lapse so B can reclaim.
		time.Sleep(100 * time.Millisecond)
		msgB, err := pq.ReceiveChannel(ctx, ch, pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil || msgB == nil {
			t.Fatalf("reclaim B: msg=%v err=%v", msgB, err)
		}
		if err := pq.ExtendVisibility(ctx, msgA.Receipt(), 1*time.Second); !errors.Is(err, pgqueue.ErrClaimExpired) {
			t.Errorf("extend stale claim: got %v, want ErrClaimExpired", err)
		}
		if err := pq.Ack(ctx, msgB.Receipt()); err != nil {
			t.Errorf("current Ack: %v", err)
		}
	})

	// retry_count_untouched: a successful extend is not a delivery attempt.
	t.Run("channel_extend_does_not_retry", func(t *testing.T) {
		const ch = "ext-noretry"
		mustCreateChannel(t, pq, ch)
		if _, err := pq.Publish(ctx, ch, []byte("payload")); err != nil {
			t.Fatalf("publish: %v", err)
		}
		msg, err := pq.ReceiveChannel(ctx, ch, pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil || msg == nil {
			t.Fatalf("consume: msg=%v err=%v", msg, err)
		}
		msgTable := "pgqueue_msg_" + queueTableName(t, db, ch)
		before := channelRetryCount(t, db, msgTable, msg.ID)
		if err := pq.ExtendVisibility(ctx, msg.Receipt(), 1*time.Second); err != nil {
			t.Fatalf("ExtendVisibility: %v", err)
		}
		if after := channelRetryCount(t, db, msgTable, msg.ID); after != before {
			t.Errorf("retry_count changed by extend: before=%d after=%d, want unchanged", before, after)
		}
	})

	// invalid_duration: bounds are enforced (1ms..24h) before any DB work.
	t.Run("channel_extend_invalid_duration", func(t *testing.T) {
		const ch = "ext-invalid"
		mustCreateChannel(t, pq, ch)
		if _, err := pq.Publish(ctx, ch, []byte("payload")); err != nil {
			t.Fatalf("publish: %v", err)
		}
		msg, err := pq.ReceiveChannel(ctx, ch, pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil || msg == nil {
			t.Fatalf("consume: msg=%v err=%v", msg, err)
		}
		for _, d := range []time.Duration{0, 25 * time.Hour} {
			if err := pq.ExtendVisibility(ctx, msg.Receipt(), d); !errors.Is(err, pgqueue.ErrInvalidVisibilityTimeout) {
				t.Errorf("ExtendVisibility(%v): got %v, want ErrInvalidVisibilityTimeout", d, err)
			}
		}
	})

	t.Run("topic_extend_prevents_reclaim", func(t *testing.T) {
		const (
			tp  = "ext-topic-prevent"
			sub = "sub-1"
		)
		mustCreateTopicSub(t, pq, tp, sub)
		if _, err := pq.Publish(ctx, tp, []byte("payload")); err != nil {
			t.Fatalf("publish: %v", err)
		}
		msg, err := pq.ReceiveTopic(ctx, tp, sub, pgqueue.WithVisibilityTimeout(200*time.Millisecond))
		if err != nil || msg == nil {
			t.Fatalf("consume: msg=%v err=%v", msg, err)
		}
		if err := pq.ExtendVisibility(ctx, msg.Receipt(), 5*time.Second); err != nil {
			t.Fatalf("ExtendVisibility: %v", err)
		}
		// intentional: sleep past the original 200ms lease; the 5s extension holds.
		time.Sleep(400 * time.Millisecond)
		if other, err := pq.ReceiveTopic(ctx, tp, sub); !errors.Is(err, pgqueue.ErrQueueEmpty) {
			t.Fatalf("reclaim attempt: msg=%v err=%v, want ErrQueueEmpty (still held)", other, err)
		}
		if err := pq.Ack(ctx, msg.Receipt()); err != nil {
			t.Errorf("Ack with original receipt after extend: %v", err)
		}
	})

	t.Run("topic_extend_stale_claim", func(t *testing.T) {
		const (
			tp  = "ext-topic-stale"
			sub = "sub-1"
		)
		mustCreateTopicSub(t, pq, tp, sub)
		if _, err := pq.Publish(ctx, tp, []byte("payload")); err != nil {
			t.Fatalf("publish: %v", err)
		}
		msgA, err := pq.ReceiveTopic(ctx, tp, sub, pgqueue.WithVisibilityTimeout(50*time.Millisecond))
		if err != nil || msgA == nil {
			t.Fatalf("consume A: msg=%v err=%v", msgA, err)
		}
		// intentional: let A's 50ms lease lapse so B can reclaim.
		time.Sleep(100 * time.Millisecond)
		msgB, err := pq.ReceiveTopic(ctx, tp, sub, pgqueue.WithVisibilityTimeout(30*time.Second))
		if err != nil || msgB == nil {
			t.Fatalf("reclaim B: msg=%v err=%v", msgB, err)
		}
		if err := pq.ExtendVisibility(ctx, msgA.Receipt(), 1*time.Second); !errors.Is(err, pgqueue.ErrClaimExpired) {
			t.Errorf("extend stale claim: got %v, want ErrClaimExpired", err)
		}
		if err := pq.Ack(ctx, msgB.Receipt()); err != nil {
			t.Errorf("current Ack: %v", err)
		}
	})
}

func mustCreateChannel(t *testing.T, pq *pgqueue.Queue, name string) {
	t.Helper()
	if err := pq.CreateChannel(context.Background(), name); err != nil {
		t.Fatalf("create channel %s: %v", name, err)
	}
}

func mustCreateTopicSub(t *testing.T, pq *pgqueue.Queue, topic, sub string) {
	t.Helper()
	ctx := context.Background()
	if err := pq.CreateTopic(ctx, topic); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	if err := pq.Subscribe(ctx, topic, sub); err != nil {
		t.Fatalf("subscribe %s/%s: %v", topic, sub, err)
	}
}

func channelRetryCount(t *testing.T, db *sql.DB, msgTable string, id uuid.UUID) int {
	t.Helper()
	//nolint:gosec // G201: msgTable is a library-sanitized table name, not user input
	q := fmt.Sprintf("SELECT retry_count FROM %s WHERE id = $1", msgTable)
	var rc int
	if err := db.QueryRowContext(context.Background(), q, id).Scan(&rc); err != nil {
		t.Fatalf("read retry_count for %s: %v", id, err)
	}
	return rc
}
