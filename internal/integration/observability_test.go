package integration_test

import (
	"context"
	"errors"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sgaunet/pgqueue"
)

// recordingTracer is a test pgqueue.Tracer that records every span name.
type recordingTracer struct {
	mu    sync.Mutex
	spans []string
}

func (rt *recordingTracer) StartSpan(
	ctx context.Context, name string, _ ...pgqueue.Attr,
) (context.Context, pgqueue.Span) {
	rt.mu.Lock()
	rt.spans = append(rt.spans, name)
	rt.mu.Unlock()
	return ctx, recordingSpan{}
}

func (rt *recordingTracer) sawSpan(name string) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return slices.Contains(rt.spans, name)
}

type recordingSpan struct{}

func (recordingSpan) End()                 {}
func (recordingSpan) SetError(error)       {}
func (recordingSpan) SetAttr(...pgqueue.Attr) {}

// recordingMetrics is a test pgqueue.MetricsRecorder that counts every call.
type recordingMetrics struct {
	mu                  sync.Mutex
	publishes           int
	handles             int
	deliveries          int
	acks                int
	nacks               int
	ackAfterExpired     int
	metadataParseErrors int
	gcRuns              int
	missedNotifications int
}

func (rm *recordingMetrics) RecordPublish(_ string, count int) {
	rm.mu.Lock()
	rm.publishes += count
	rm.mu.Unlock()
}

func (rm *recordingMetrics) RecordHandle(_ string, _ time.Duration) {
	rm.mu.Lock()
	rm.handles++
	rm.mu.Unlock()
}

func (rm *recordingMetrics) RecordDeliveryLatency(_ string, _ time.Duration) {
	rm.mu.Lock()
	rm.deliveries++
	rm.mu.Unlock()
}

func (rm *recordingMetrics) RecordAck(_ string, ok bool) {
	rm.mu.Lock()
	if ok {
		rm.acks++
	} else {
		rm.nacks++
	}
	rm.mu.Unlock()
}

func (rm *recordingMetrics) RecordAckAfterExpired(_ string) {
	rm.mu.Lock()
	rm.ackAfterExpired++
	rm.mu.Unlock()
}

func (rm *recordingMetrics) RecordMetadataParseError(_ string) {
	rm.mu.Lock()
	rm.metadataParseErrors++
	rm.mu.Unlock()
}

func (rm *recordingMetrics) RecordGCRun(_ string, _ time.Duration, _, _ int64, _ error) {
	rm.mu.Lock()
	rm.gcRuns++
	rm.mu.Unlock()
}

func (rm *recordingMetrics) RecordMissedNotification(_ string) {
	rm.mu.Lock()
	rm.missedNotifications++
	rm.mu.Unlock()
}

func (rm *recordingMetrics) ObserveQueueDepth(string, int64) {}
func (rm *recordingMetrics) ObserveDLQSize(string, int64)    {}

func (rm *recordingMetrics) snapshot() (publishes, handles, acks, nacks int) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.publishes, rm.handles, rm.acks, rm.nacks
}

func (rm *recordingMetrics) ackAfterExpiredCount() int {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.ackAfterExpired
}

func (rm *recordingMetrics) deliveryCount() int {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.deliveries
}

// TestObservabilityHooksReceiveSpansAndMetrics verifies that a registered
// Tracer and MetricsRecorder receive spans and metric calls for publish,
// consume, ack, nack and replay operations.
func TestObservabilityHooksReceiveSpansAndMetrics(t *testing.T) {
	db, containerCleanup := setupTestContainer(t)
	defer containerCleanup()

	ctx := context.Background()
	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("failed to initialize schema: %v", err)
	}

	tracer := &recordingTracer{}
	metrics := &recordingMetrics{}
	pq, err := pgqueue.New(ctx, db,
		pgqueue.WithTracer(tracer),
		pgqueue.WithMetrics(metrics),
	)
	if err != nil {
		t.Fatalf("failed to init pgqueue: %v", err)
	}
	defer func() { _ = pq.Close() }()

	const channelName = "obs-channel"
	if err := pq.CreateChannel(ctx, channelName,
		pgqueue.WithQueueMaxRetries(1)); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}

	// Publish two messages: one handled OK, one always failing (drives a nack).
	if _, err := pq.Publish(ctx, channelName, []byte("ok")); err != nil {
		t.Fatalf("publish ok: %v", err)
	}
	if _, err := pq.Publish(ctx, channelName, []byte("bad")); err != nil {
		t.Fatalf("publish bad: %v", err)
	}

	gotOK := make(chan struct{}, 1)
	gotBad := make(chan struct{}, 1)
	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		_ = pq.ConsumeChannel(consumeCtx, channelName,
			func(_ context.Context, msg *pgqueue.Message) error {
				if string(msg.Payload) == "bad" {
					select {
					case gotBad <- struct{}{}:
					default:
					}
					return errors.New("intentional failure")
				}
				select {
				case gotOK <- struct{}{}:
				default:
				}
				return nil
			}, pgqueue.WithPollInterval(50*time.Millisecond))
	}()

	waitClosed(t, gotOK, "ok message never handled")
	waitClosed(t, gotBad, "bad message never handled")
	// intentional: ack/nack are issued asynchronously after the handler returns;
	// we need them to have been sent to the DB before asserting on metric counts.
	// There is no observable DB state that confirms the ack/nack have been
	// processed — the handler channel closure only signals handler completion.
	time.Sleep(300 * time.Millisecond) // intentional: allow async ack/nack to reach DB
	cancel()

	// Replay drives a replay span.
	if _, err := pq.ReplayDLQ(ctx, channelName, pgqueue.QueueTypeChannel,
		pgqueue.ReplayOptions{}); err != nil {
		t.Fatalf("replay DLQ: %v", err)
	}

	for _, span := range []string{
		"pgqueue.publish", "pgqueue.consume", "pgqueue.ack",
		"pgqueue.nack", "pgqueue.replay",
	} {
		if !tracer.sawSpan(span) {
			t.Errorf("expected span %q to be recorded", span)
		}
	}

	publishes, handles, acks, nacks := metrics.snapshot()
	if publishes < 2 {
		t.Errorf("expected >=2 publish metrics, got %d", publishes)
	}
	if handles < 2 {
		t.Errorf("expected >=2 handle metrics, got %d", handles)
	}
	if deliveries := metrics.deliveryCount(); deliveries < 2 {
		t.Errorf("expected >=2 delivery-latency metrics, got %d", deliveries)
	}
	if acks < 1 {
		t.Errorf("expected >=1 ack metric, got %d", acks)
	}
	if nacks < 1 {
		t.Errorf("expected >=1 nack metric, got %d", nacks)
	}
}

// TestAutoAckSurfacesClaimExpired is the issue #71 regression: when a handler
// returns nil but the row's claim_id changed before the auto-ack ran (a slow
// handler whose visibility timeout lapsed and was reclaimed), the auto-ack
// fails with ErrClaimExpired and pgqueue must surface that as a
// RecordAckAfterExpired emission rather than silently swallowing the error.
// The test fakes the claim mismatch by UPDATE'ing claim_id inside the
// handler so the path is exercised deterministically, without timing.
func TestAutoAckSurfacesClaimExpired(t *testing.T) {
	ctx := context.Background()
	db, containerCleanup := setupTestContainer(t)
	defer containerCleanup()

	if err := pgqueue.InitSchema(ctx, db); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	metrics := &recordingMetrics{}
	pq, err := pgqueue.New(ctx, db, pgqueue.WithMetrics(metrics))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() { _ = pq.Close() }()

	const channelName = "ack_expired_surface"
	if err := pq.CreateChannel(ctx, channelName); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := pq.Publish(ctx, channelName, []byte("payload")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	handled := make(chan struct{}, 1)
	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		_ = pq.ConsumeChannel(consumeCtx, channelName,
			func(_ context.Context, msg *pgqueue.Message) error {
				// Forge a claim mismatch so the upcoming auto-ack sees a
				// claim_id different from the receipt and returns
				// ErrClaimExpired. This is the same observable state a
				// real visibility-timeout reclaim would leave behind.
				if _, err := db.ExecContext(ctx,
					"UPDATE pgqueue_msg_"+channelName+
						" SET claim_id = uuidv7() WHERE id = $1",
					msg.ID,
				); err != nil {
					t.Errorf("forge claim mismatch: %v", err)
				}
				select {
				case handled <- struct{}{}:
				default:
				}
				return nil
			}, pgqueue.WithPollInterval(50*time.Millisecond))
	}()

	waitClosed(t, handled, "handler never ran")

	// Auto-ack runs after the handler returns; poll for the metric.
	eventually(t, 3*time.Second, 20*time.Millisecond,
		func() bool { return metrics.ackAfterExpiredCount() >= 1 },
		"RecordAckAfterExpired never emitted (want 1)",
	)
	if got := metrics.ackAfterExpiredCount(); got != 1 {
		t.Fatalf("RecordAckAfterExpired emissions = %d, want 1", got)
	}
}

// TestObservabilityNoopWhenUnregistered verifies that a Queue created without
// any Tracer or MetricsRecorder operates normally — the hooks are pure no-ops.
func TestObservabilityNoopWhenUnregistered(t *testing.T) {
	pq, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	const channelName = "obs-noop"
	if err := pq.CreateChannel(ctx, channelName); err != nil {
		t.Fatalf("failed to create channel: %v", err)
	}
	if _, err := pq.Publish(ctx, channelName, []byte("payload")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	msg, err := pq.ReceiveChannel(ctx, channelName)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if err := pq.Ack(ctx, msg.Receipt()); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

// TestCoreHasNoObservabilityDependencies verifies FR-019: the core pgqueue
// package's dependency graph contains no OpenTelemetry or Prometheus packages.
func TestCoreHasNoObservabilityDependencies(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", ".")
	cmd.Dir = "../.." // repo root, where the core module lives
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go list failed: %v\n%s", err, out)
	}
	for line := range strings.SplitSeq(string(out), "\n") {
		if strings.Contains(line, "go.opentelemetry.io") ||
			strings.Contains(line, "prometheus/client_golang") {
			t.Errorf("core module unexpectedly depends on %q", line)
		}
	}
}
