package pgqueue

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeListener is a Listener whose Notifications channel the test drives
// directly. Close is a no-op; Listen ignores its arguments. Used to feed the
// pump goroutine a controllable stream.
type fakeListener struct {
	ch chan string
}

func (l *fakeListener) Listen(context.Context, string) error { return nil }
func (l *fakeListener) Notifications() <-chan string         { return l.ch }
func (l *fakeListener) Close() error                         { return nil }

// failingListener is a Listener whose Listen always returns an error, so
// wakeChan exercises the LISTEN-failure path. Notifications is a no-op
// channel; the pump is irrelevant to the LISTEN-failure tests.
type failingListener struct {
	err error
	ch  chan string
}

func (l *failingListener) Listen(context.Context, string) error { return l.err }
func (l *failingListener) Notifications() <-chan string {
	if l.ch == nil {
		l.ch = make(chan string)
	}
	return l.ch
}
func (l *failingListener) Close() error { return nil }

// TestPumpRecoversPanicAndWakesAll is the issue #69 regression: a panic in
// the pump-goroutine loop body must be recovered, logged at ERROR, and every
// currently-blocked consumer must be woken so they fall back to the
// safety-net poll instead of waiting for a NOTIFY that will never arrive.
//
// The test forces the panic by pre-closing the waker's internal channel so
// pump's w.signal() panics on double-close. That mirrors the failure-mode
// shape (a broken waker / Listener combination) without needing a malicious
// third-party Listener.
func TestPumpRecoversPanicAndWakesAll(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError}))

	fl := &fakeListener{ch: make(chan string, 1)}
	n := newNotifier(fl, logger)
	if n == nil {
		t.Fatal("newNotifier returned nil")
	}

	// Register a broken waker for "doomed" — channel pre-closed so signal()
	// double-closes and panics — and a healthy waker for "victim" that we
	// expect wakeAll to fire after the panic.
	broken := newWaker()
	close(broken.ch)
	victim := newWaker()
	n.mu.Lock()
	n.wakers["doomed"] = broken
	n.wakers["victim"] = victim
	n.mu.Unlock()

	victimWait := victim.wait()

	// Start the pump and feed it the panic-inducing channel.
	var pumpDone sync.WaitGroup
	pumpDone.Go(n.pump)
	fl.ch <- "doomed"

	// pump's deferred recover should have fired and called wakeAll, which
	// signals victim. The pump goroutine then unwinds because the deferred
	// recovery returns from pump (the range loop never resumes).
	select {
	case <-victimWait:
	case <-time.After(2 * time.Second):
		t.Fatal("victim waker was not signaled after pump panic")
	}

	close(fl.ch) // ensure pump can exit cleanly if it ever resumed (it won't).
	pumpDone.Wait()

	logged := buf.String()
	if !strings.Contains(logged, "level=ERROR") ||
		!strings.Contains(logged, "notifications pump panicked") {
		t.Errorf("pump panic was not logged at ERROR with the expected message: %q", logged)
	}
}

// TestWakeChanLogsListenFailures is the issue #68 regression: a failing
// Listener.Listen used to silently leave the channel unregistered. Every
// failure must now log at WARN with the channel and underlying error, and
// every listenEscalateThreshold consecutive failures must re-fire an ERROR
// so a persistently misconfigured Listener is impossible to miss.
func TestWakeChanLogsListenFailures(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	listenErr := errors.New("permission denied")
	n := newNotifier(&failingListener{err: listenErr}, logger)
	if n == nil {
		t.Fatal("newNotifier returned nil")
	}

	const channel = "pgqueue_msg_x"

	// Drive listenEscalateThreshold failures; each one bumps the per-channel
	// counter and the threshold-th attempt escalates to ERROR.
	for i := 1; i <= listenEscalateThreshold; i++ {
		if got := n.wakeChan(context.Background(), channel); got == nil {
			t.Fatalf("wakeChan returned nil at attempt %d", i)
		}
	}

	out := buf.String()
	warns := strings.Count(out, "level=WARN")
	errs := strings.Count(out, "level=ERROR")
	if warns != listenEscalateThreshold {
		t.Errorf("WARN count = %d, want %d", warns, listenEscalateThreshold)
	}
	if errs != 1 {
		t.Errorf("ERROR escalation count = %d, want 1", errs)
	}
	if !strings.Contains(out, `channel=`+channel) {
		t.Errorf("log line did not include the channel name: %q", out)
	}
	if !strings.Contains(out, "permission denied") {
		t.Errorf("log line did not include the underlying error: %q", out)
	}

	// One more failure escalates to a fresh ERROR? No — we're at threshold+1,
	// which is not a multiple, so only a WARN should land.
	if got := n.wakeChan(context.Background(), channel); got == nil {
		t.Fatal("wakeChan returned nil at attempt 11")
	}
	if got := strings.Count(buf.String(), "level=ERROR"); got != 1 {
		t.Errorf("ERROR count after attempt 11 = %d, want 1", got)
	}
}

// TestWakeChanResetsFailureCountOnSuccess confirms the per-channel failure
// counter clears on a successful Listen, so a recovered transient failure
// does not contribute to a future escalation.
func TestWakeChanResetsFailureCountOnSuccess(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{},
		&slog.HandlerOptions{Level: slog.LevelWarn}))

	fl := &failingListener{err: errors.New("transient")}
	n := newNotifier(fl, logger)
	const channel = "ch"
	for range 3 {
		_ = n.wakeChan(context.Background(), channel)
	}
	if got := n.listenFailures[channel]; got != 3 {
		t.Fatalf("listenFailures = %d, want 3", got)
	}
	// Flip the Listener to succeed; next wakeChan resets the counter.
	fl.err = nil
	_ = n.wakeChan(context.Background(), channel)
	if got, ok := n.listenFailures[channel]; ok {
		t.Errorf("listenFailures still tracks %q with value %d after success", channel, got)
	}
	if !n.listening[channel] {
		t.Errorf("listening[%q] = false after a successful Listen", channel)
	}
}

// TestPumpRecoveryIsSilentWithoutLogger confirms the pump panic-recovery does
// not panic again when no logger is configured (logger is nil).
func TestPumpRecoveryIsSilentWithoutLogger(t *testing.T) {
	fl := &fakeListener{ch: make(chan string, 1)}
	n := newNotifier(fl, nil)
	if n == nil {
		t.Fatal("newNotifier returned nil")
	}

	broken := newWaker()
	close(broken.ch)
	n.mu.Lock()
	n.wakers["doomed"] = broken
	n.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		n.pump()
	}()
	fl.ch <- "doomed"
	close(fl.ch)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pump did not exit after panic recovery")
	}
}
