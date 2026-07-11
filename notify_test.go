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

// blockingListener is a Listener whose Listen blocks until released (or ctx is
// cancelled), modelling the now-synchronous confirmation: it lets a test prove
// wakeChan does not block on Listen and that exactly one confirmListen goroutine
// is spawned per channel while one is in flight. calls counts Listen entries.
type blockingListener struct {
	release chan struct{}
	err     error
	ch      chan string

	mu    sync.Mutex
	calls int
}

func (l *blockingListener) Listen(ctx context.Context, _ string) error {
	l.mu.Lock()
	l.calls++
	l.mu.Unlock()
	select {
	case <-l.release:
		return l.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *blockingListener) callCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

func (l *blockingListener) Notifications() <-chan string {
	if l.ch == nil {
		l.ch = make(chan string)
	}
	return l.ch
}
func (l *blockingListener) Close() error { return nil }

// registerWaker installs a fresh waker for channel and returns it, mirroring what
// wakeChan does under the lock. Tests that drive confirmListen directly pass the
// returned waker as the "captured" waker so the staleness guard treats the call
// as the live owner of the channel's bookkeeping.
func registerWaker(n *notifier, channel string) *waker {
	n.mu.Lock()
	defer n.mu.Unlock()
	w := newWaker()
	n.wakers[channel] = w
	return w
}

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

// TestConfirmListenLogsListenFailures is the issue #68 / #134 regression: a
// failing synchronous Listen must surface through confirmListen. Every failure
// logs at WARN with the channel and underlying error, and every
// listenEscalateThreshold consecutive failures re-fires an ERROR so a
// persistently degraded LISTEN is impossible to miss. confirmListen is driven
// directly here because wakeChan now spawns it asynchronously (and guards
// against more than one in-flight goroutine per channel).
func TestConfirmListenLogsListenFailures(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	listenErr := errors.New("permission denied")
	n := newNotifier(&failingListener{err: listenErr}, logger)
	if n == nil {
		t.Fatal("newNotifier returned nil")
	}

	const channel = "pgqueue_msg_x"
	w := registerWaker(n, channel)

	// Drive listenEscalateThreshold failures; each one bumps the per-channel
	// counter and the threshold-th attempt escalates to ERROR.
	for range listenEscalateThreshold {
		n.confirmListen(channel, w)
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
	n.confirmListen(channel, w)
	if got := strings.Count(buf.String(), "level=ERROR"); got != 1 {
		t.Errorf("ERROR count after attempt 11 = %d, want 1", got)
	}
}

// TestConfirmListenResetsFailureCountOnSuccess confirms the per-channel failure
// counter clears on a successful Listen, so a recovered transient failure does
// not contribute to a future escalation, and the channel is marked listening.
func TestConfirmListenResetsFailureCountOnSuccess(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{},
		&slog.HandlerOptions{Level: slog.LevelWarn}))

	fl := &failingListener{err: errors.New("transient")}
	n := newNotifier(fl, logger)
	const channel = "ch"
	w := registerWaker(n, channel)
	for range 3 {
		n.confirmListen(channel, w)
	}
	if got := n.listenFailures[channel]; got != 3 {
		t.Fatalf("listenFailures = %d, want 3", got)
	}
	// Flip the Listener to succeed; next confirmListen resets the counter.
	fl.err = nil
	n.confirmListen(channel, w)
	if got, ok := n.listenFailures[channel]; ok {
		t.Errorf("listenFailures still tracks %q with value %d after success", channel, got)
	}
	if !n.listening[channel] {
		t.Errorf("listening[%q] = false after a successful Listen", channel)
	}
}

// TestWakeChanConfirmsListenAsync proves wakeChan does not block on the now
// synchronous Listen (#134): it returns a waker immediately while Listen is
// still blocked, spawns exactly one confirmation goroutine per channel even
// across concurrent callers, and marks the channel listening once Listen
// returns.
func TestWakeChanConfirmsListenAsync(t *testing.T) {
	bl := &blockingListener{release: make(chan struct{})}
	n := newNotifier(bl, nil)
	if n == nil {
		t.Fatal("newNotifier returned nil")
	}
	const channel = "pgqueue_msg_async"

	// wakeChan must return without waiting for the blocked Listen.
	done := make(chan (<-chan struct{}), 1)
	go func() { done <- n.wakeChan(context.Background(), channel) }()
	select {
	case w := <-done:
		if w == nil {
			t.Fatal("wakeChan returned nil waker while push delivery is available")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("wakeChan blocked on the synchronous Listen")
	}

	// A second arm while Listen is still in flight must not spawn a second
	// goroutine.
	_ = n.wakeChan(context.Background(), channel)
	time.Sleep(50 * time.Millisecond)
	if got := bl.callCount(); got != 1 {
		t.Fatalf("Listen called %d times while one confirmation was in flight, want 1", got)
	}

	// Release Listen; the channel should flip to listening.
	close(bl.release)
	waitFor(t, func() bool {
		n.mu.Lock()
		defer n.mu.Unlock()
		return n.listening[channel] && !n.listenInFlight[channel]
	}, "channel was not marked listening after Listen returned")
}

// TestConfirmListenStaleAfterForgetDoesNotRevive is the regression for the
// confirmListen race: if forget runs while Listen is in flight (queue deleted)
// and the channel is then recreated under the same name, the stale confirmListen
// must NOT mark the channel listening (which would make a later wakeChan skip
// re-establishing the server-side LISTEN and silently lose push delivery) nor
// delete the new owner's listenInFlight flag.
func TestConfirmListenStaleAfterForgetDoesNotRevive(t *testing.T) {
	bl := &blockingListener{release: make(chan struct{})}
	n := newNotifier(bl, nil)
	if n == nil {
		t.Fatal("newNotifier returned nil")
	}
	const channel = "pgqueue_msg_stale"

	// Model wakeChan having spawned confirmListen for the original waker.
	w1 := registerWaker(n, channel)
	n.mu.Lock()
	n.listenInFlight[channel] = true
	n.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		n.confirmListen(channel, w1) // blocks inside Listen
	}()
	waitFor(t, func() bool { return bl.callCount() == 1 }, "Listen was not entered")

	// Queue deleted: forget drops w1 and all per-channel bookkeeping (bl is not an
	// Unlistener, so forget just clears the maps and bumps the waker identity).
	n.forget(context.Background(), channel)

	// Queue recreated under the same name: a new wakeChan installs a fresh waker
	// and marks a new confirmation in flight. Model that resulting state directly.
	w2 := registerWaker(n, channel)
	n.mu.Lock()
	n.listenInFlight[channel] = true
	n.mu.Unlock()

	// Release the original (now stale) Listen; it returns success.
	close(bl.release)
	<-done

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.listening[channel] {
		t.Error("stale confirmListen revived listening for a forgotten channel")
	}
	if !n.listenInFlight[channel] {
		t.Error("stale confirmListen deleted the new owner's listenInFlight flag")
	}
	if n.wakers[channel] != w2 {
		t.Error("the new owner's waker was clobbered")
	}
}

// TestConfirmListenStaleFailureDoesNotCorruptCounter ensures a stale FAILING
// confirmListen does not increment the new owner's failure counter or fire a
// missed-notification metric for the recreated channel.
func TestConfirmListenStaleFailureDoesNotCorruptCounter(t *testing.T) {
	bl := &blockingListener{release: make(chan struct{}), err: errors.New("listen failed")}
	n := newNotifier(bl, nil)
	if n == nil {
		t.Fatal("newNotifier returned nil")
	}
	var missed int
	n.onMissedNotification = func(string) { missed++ }
	const channel = "pgqueue_msg_stalefail"

	w1 := registerWaker(n, channel)
	n.mu.Lock()
	n.listenInFlight[channel] = true
	n.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		n.confirmListen(channel, w1) // blocks, then returns an error
	}()
	waitFor(t, func() bool { return bl.callCount() == 1 }, "Listen was not entered")

	n.forget(context.Background(), channel)
	registerWaker(n, channel) // new owner installs a fresh waker
	n.mu.Lock()
	n.listenInFlight[channel] = true
	n.mu.Unlock()

	close(bl.release)
	<-done

	n.mu.Lock()
	defer n.mu.Unlock()
	if got := n.listenFailures[channel]; got != 0 {
		t.Errorf("stale failure incremented the new owner's failure counter to %d, want 0", got)
	}
	if missed != 0 {
		t.Errorf("stale failure fired onMissedNotification %d times, want 0", missed)
	}
}

// waitFor polls cond until it holds or a short deadline passes.
func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(msg)
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
