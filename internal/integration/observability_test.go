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
	mu              sync.Mutex
	publishes       int
	consumes        int
	acks            int
	nacks           int
	ackAfterExpired int
}

func (rm *recordingMetrics) RecordPublish(_ string, count int) {
	rm.mu.Lock()
	rm.publishes += count
	rm.mu.Unlock()
}

func (rm *recordingMetrics) RecordConsume(_ string, _ time.Duration) {
	rm.mu.Lock()
	rm.consumes++
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

func (rm *recordingMetrics) ObserveQueueDepth(string, int64) {}
func (rm *recordingMetrics) ObserveDLQSize(string, int64)    {}

func (rm *recordingMetrics) snapshot() (publishes, consumes, acks, nacks int) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.publishes, rm.consumes, rm.acks, rm.nacks
}

func (rm *recordingMetrics) ackAfterExpiredCount() int {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.ackAfterExpired
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
	if _, err := pq.PublishChannel(ctx, channelName, []byte("ok")); err != nil {
		t.Fatalf("publish ok: %v", err)
	}
	if _, err := pq.PublishChannel(ctx, channelName, []byte("bad")); err != nil {
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
	time.Sleep(300 * time.Millisecond) // let ack/nack settle
	cancel()

	// Replay drives a replay span.
	if _, err := pq.ReplayDLQ(ctx, channelName, pgqueue.QueueTypeChannel,
		pgqueue.ReplayOptions{Confirm: true}); err != nil {
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

	publishes, consumes, acks, nacks := metrics.snapshot()
	if publishes < 2 {
		t.Errorf("expected >=2 publish metrics, got %d", publishes)
	}
	if consumes < 2 {
		t.Errorf("expected >=2 consume metrics, got %d", consumes)
	}
	if acks < 1 {
		t.Errorf("expected >=1 ack metric, got %d", acks)
	}
	if nacks < 1 {
		t.Errorf("expected >=1 nack metric, got %d", nacks)
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
	if _, err := pq.PublishChannel(ctx, channelName, []byte("payload")); err != nil {
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
