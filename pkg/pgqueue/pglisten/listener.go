// Package pglisten provides a pgx-backed implementation of pgqueue.Listener for
// push-based delivery via PostgreSQL LISTEN/NOTIFY.
//
// It is a separate Go module so the pgx dependency stays out of the core
// pgqueue dependency graph: consumers who do not need push delivery never pull
// pgx. Register a listener with pgqueue.WithListener:
//
//	l, err := pglisten.New(ctx, connString)
//	if err != nil { ... }
//	q, err := pgqueue.New(ctx, db, pgqueue.WithListener(l))
//	defer q.Close() // closes the listener too
package pglisten

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sgaunet/pgqueue/pkg/pgqueue"
)

// Default reconnect-backoff parameters (R-07).
const (
	defaultReconnectBaseDelay  = 1 * time.Second
	defaultReconnectMaxDelay   = 30 * time.Second
	defaultReconnectMultiplier = 2.0
	// maxReconnectBackoffSteps caps the exponent so a long outage cannot make
	// the delay computation loop run unbounded; the window has saturated at
	// MaxDelay long before this.
	maxReconnectBackoffSteps = 32
)

// notifyBuffer sizes the buffered notifications channel. A slow consumer cannot
// stall the receive loop until this many notifications are outstanding.
const notifyBuffer = 64

// ReconnectPolicy controls the backoff between LISTEN/NOTIFY reconnect attempts
// after a connection loss. The delay for attempt n is full-jittered over the
// window min(BaseDelay * Multiplier^n, MaxDelay), so many application instances
// recovering at once do not reconnect in lockstep.
type ReconnectPolicy struct {
	// BaseDelay is the backoff window for the first retry (default 1s).
	BaseDelay time.Duration
	// MaxDelay caps the backoff window (default 30s).
	MaxDelay time.Duration
	// Multiplier grows the window each attempt; clamped to >= 1 (default 2).
	Multiplier float64
}

// normalized returns a copy of p with any zero or invalid field replaced by
// its default, so a partially-specified policy is still usable.
func (p ReconnectPolicy) normalized() ReconnectPolicy {
	if p.BaseDelay <= 0 {
		p.BaseDelay = defaultReconnectBaseDelay
	}
	if p.Multiplier < 1 {
		p.Multiplier = defaultReconnectMultiplier
	}
	if p.MaxDelay < p.BaseDelay {
		p.MaxDelay = defaultReconnectMaxDelay
	}
	if p.MaxDelay < p.BaseDelay {
		p.MaxDelay = p.BaseDelay
	}
	return p
}

// Option configures a Listener at construction time.
type Option func(*Listener)

// WithReconnectPolicy overrides the default exponential-backoff reconnect
// policy. Zero or invalid fields fall back to their defaults.
func WithReconnectPolicy(p ReconnectPolicy) Option {
	return func(l *Listener) { l.reconnectPolicy = p.normalized() }
}

// WithLogger attaches a structured logger. When set, each reconnect attempt is
// logged at WARN. When nil (the default) the Listener is silent.
func WithLogger(logger *slog.Logger) Option {
	return func(l *Listener) { l.logger = logger }
}

// errListenerClosed is returned by Listen after the Listener has been closed.
var errListenerClosed = errors.New("pglisten: listener is closed")

// Listener is a pgx-backed pgqueue.Listener. It owns a dedicated PostgreSQL
// connection, issues LISTEN for each requested channel, streams NOTIFY channel
// names, and transparently reconnects (re-issuing every LISTEN) if the
// connection drops.
type Listener struct {
	connString string
	notifs     chan string

	reconnectPolicy ReconnectPolicy
	logger          *slog.Logger

	mu        sync.Mutex
	conn      *pgx.Conn
	channels  map[string]bool // every channel to (re-)LISTEN on
	pending   []string        // LISTEN requests not yet issued
	closed    bool
	done      chan struct{}
	closeOnce sync.Once

	// waitMu guards the cancellation state of the in-progress
	// WaitForNotification. requestInterrupt invokes waitCancel directly to
	// break the wait, replacing a per-notification canceller goroutine.
	waitMu           sync.Mutex
	waitCancel       context.CancelFunc // cancels the active wait, if any
	interruptPending bool               // a LISTEN/Close arrived with no active wait
}

// compile-time check: *Listener satisfies the pgqueue.Listener hook interface.
var _ pgqueue.Listener = (*Listener)(nil)

// New opens a dedicated connection for LISTEN/NOTIFY and starts the receive
// loop. connString is a standard PostgreSQL connection string (the same form
// accepted by pgx.Connect). Optional Options configure reconnect backoff and
// logging; omitting them yields sane exponential-backoff defaults.
func New(ctx context.Context, connString string, opts ...Option) (*Listener, error) {
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("pglisten: connect: %w", err)
	}
	l := &Listener{
		connString:      connString,
		notifs:          make(chan string, notifyBuffer),
		conn:            conn,
		channels:        make(map[string]bool),
		done:            make(chan struct{}),
		reconnectPolicy: ReconnectPolicy{}.normalized(),
	}
	for _, o := range opts {
		o(l)
	}
	// run outlives this call; New's ctx scopes only the initial connect, so the
	// detached goroutine intentionally uses fresh contexts for its DB calls.
	go l.run() //nolint:contextcheck // detached goroutine; New's ctx does not scope it
	return l, nil
}

// Listen registers a channel for LISTEN. The actual LISTEN statement is issued
// asynchronously by the run loop; the request is also remembered so it survives
// a reconnect. Listen is safe for concurrent use.
func (l *Listener) Listen(_ context.Context, channel string) error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return errListenerClosed
	}
	if !l.channels[channel] {
		l.channels[channel] = true
		l.pending = append(l.pending, channel)
	}
	l.mu.Unlock()

	// Break the in-progress wait so the run loop drains the request promptly.
	l.requestInterrupt()
	return nil
}

// requestInterrupt breaks the in-progress WaitForNotification, if any, so the
// run loop re-checks its state and drains pending work. When no wait is in
// progress the request is remembered (interruptPending) so the next
// receiveOne honors it instead of blocking — this preserves the buffered
// "pending nudge" semantics the previous channel-based design relied on. It is
// invoked both when a LISTEN is registered and when the Listener is closed.
func (l *Listener) requestInterrupt() {
	l.waitMu.Lock()
	l.interruptPending = true
	if l.waitCancel != nil {
		l.waitCancel()
	}
	l.waitMu.Unlock()
}

// Notifications returns the stream of notification channel names. It is closed
// when the Listener is closed.
func (l *Listener) Notifications() <-chan string {
	return l.notifs
}

// Close stops the receive loop and releases the database connection. It is
// idempotent.
func (l *Listener) Close() error {
	l.closeOnce.Do(func() {
		l.mu.Lock()
		l.closed = true
		l.mu.Unlock()
		close(l.done)
		// Break any in-progress WaitForNotification so the run loop observes
		// the closed state at once instead of blocking until the next NOTIFY.
		l.requestInterrupt()
	})
	return nil
}

// run owns the connection: it issues pending LISTEN statements, waits for
// notifications, and reconnects on failure. It is a detached background
// goroutine with no caller context, so its database calls intentionally use
// fresh contexts.
func (l *Listener) run() {
	defer close(l.notifs)
	defer l.closeConn()

	for {
		if l.isDone() {
			return
		}
		if err := l.drainPending(); err != nil {
			if !l.reconnect() {
				return
			}
			continue
		}
		if !l.receiveOne() {
			return
		}
	}
}

// receiveOne waits for a single notification and forwards it. It returns false
// only when the listener is shutting down. A connection error triggers a
// reconnect; an interrupt (a new LISTEN request, or Close) just returns true so
// the run loop re-checks its state.
//
// The in-progress wait is cancelled by requestInterrupt invoking the stored
// waitCancel directly — no per-notification canceller goroutine.
func (l *Listener) receiveOne() bool {
	l.waitMu.Lock()
	if l.interruptPending {
		// A LISTEN request (or Close) arrived with no wait to cancel; consume
		// the flag and let the run loop drain it without blocking on a wait.
		l.interruptPending = false
		l.waitMu.Unlock()
		return true
	}
	waitCtx, cancel := context.WithCancel(context.Background())
	l.waitCancel = cancel
	l.waitMu.Unlock()

	n, err := l.currentConn().WaitForNotification(waitCtx)

	l.waitMu.Lock()
	l.waitCancel = nil
	l.waitMu.Unlock()
	cancel()

	if l.isDone() {
		return false
	}
	if err != nil {
		// A cancelled wait context means an interrupt (expected: a new LISTEN
		// arrived); any other error is a genuine connection failure.
		if waitCtx.Err() != nil {
			return true
		}
		return l.reconnect()
	}

	select {
	case l.notifs <- n.Channel:
	case <-l.done:
		return false
	}
	return true
}

// drainPending issues every queued LISTEN statement.
func (l *Listener) drainPending() error {
	l.mu.Lock()
	pending := l.pending
	l.pending = nil
	conn := l.conn
	l.mu.Unlock()

	for _, ch := range pending {
		// LISTEN takes no parameters; the channel name is a validated pgqueue
		// identifier, quoted so it is matched verbatim against pg_notify.
		if _, err := conn.Exec(context.Background(), `LISTEN "`+ch+`"`); err != nil {
			// Re-queue the unissued channels so a reconnect retries them.
			l.requeue(pending)
			return fmt.Errorf("pglisten: LISTEN %q: %w", ch, err)
		}
	}
	return nil
}

// reconnect re-establishes the connection and re-issues every known LISTEN. It
// returns false when the listener is closed while reconnecting. Failed attempts
// back off exponentially with full jitter (R-07).
func (l *Listener) reconnect() bool {
	l.closeConn()
	attempt := 0
	for {
		if l.isDone() {
			return false
		}
		connectCtx, cancelConnect := l.connectContext()
		conn, err := pgx.Connect(connectCtx, l.connString)
		cancelConnect()
		if err != nil {
			delay := l.reconnectBackoff(attempt)
			l.logWarn("pglisten: reconnect attempt failed; retrying",
				"attempt", attempt+1, "delay", delay, "error", err)
			attempt++
			select {
			case <-l.done:
				return false
			case <-time.After(delay):
				continue
			}
		}
		l.mu.Lock()
		l.conn = conn
		// Re-LISTEN every channel ever requested.
		l.pending = l.pending[:0]
		for ch := range l.channels {
			l.pending = append(l.pending, ch)
		}
		l.mu.Unlock()
		return true
	}
}

// reconnectBackoff returns the jittered delay before reconnect attempt n
// (0-based): a uniform random duration in [0, window), where window grows
// exponentially as min(BaseDelay * Multiplier^n, MaxDelay).
func (l *Listener) reconnectBackoff(attempt int) time.Duration {
	p := l.reconnectPolicy
	window := float64(p.BaseDelay)
	steps := min(attempt, maxReconnectBackoffSteps)
	for range steps {
		window *= p.Multiplier
		if window >= float64(p.MaxDelay) {
			window = float64(p.MaxDelay)
			break
		}
	}
	//nolint:gosec // G404: reconnect jitter is not security-sensitive.
	return time.Duration(rand.Float64() * window)
}

// logWarn logs at WARN when a logger is configured; otherwise it is a no-op.
func (l *Listener) logWarn(msg string, args ...any) {
	if l.logger != nil {
		l.logger.Warn(msg, args...)
	}
}

func (l *Listener) requeue(channels []string) {
	l.mu.Lock()
	l.pending = append(channels, l.pending...)
	l.mu.Unlock()
}

func (l *Listener) currentConn() *pgx.Conn {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.conn
}

func (l *Listener) closeConn() {
	l.mu.Lock()
	conn := l.conn
	l.conn = nil
	l.mu.Unlock()
	if conn != nil {
		_ = conn.Close(context.Background())
	}
}

func (l *Listener) isDone() bool {
	select {
	case <-l.done:
		return true
	default:
		return false
	}
}

// connectContext returns a context cancelled when the Listener is closed, so a
// pgx.Connect blocked on an unreachable host during reconnect is interrupted
// by Close instead of leaking the run goroutine until the OS-level timeout.
// The caller must invoke the returned cancel to release the watcher goroutine;
// it must be called per attempt (not deferred) so watchers do not accumulate
// across the reconnect loop.
func (l *Listener) connectContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-l.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}
