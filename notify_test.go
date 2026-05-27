package pgqueue

import (
	"bytes"
	"context"
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
