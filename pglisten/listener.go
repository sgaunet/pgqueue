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
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sgaunet/pgqueue"
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

// Default keepalive/liveness-probe parameters for the LISTEN connection (#49).
// keepaliveInterval bounds how long WaitForNotification may block so a
// silently-dead TCP connection (NAT idle drop, firewall close with no RST,
// hung backend) is detected within the interval instead of the OS-level
// ~2h TCP timeout. pingTimeout caps the probe itself so it cannot inherit
// the very hang it is meant to detect.
const (
	defaultKeepaliveInterval = 30 * time.Second
	defaultPingTimeout       = 5 * time.Second
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

// normalizeKeepaliveInterval applies the default to a zero or negative
// interval. There is no escape hatch to disable the keepalive: disabling
// reintroduces the silent-TCP-death bug this guards against (#49).
func normalizeKeepaliveInterval(d time.Duration) time.Duration {
	if d <= 0 {
		return defaultKeepaliveInterval
	}
	return d
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

// WithKeepaliveInterval bounds how long the Listener blocks on
// WaitForNotification before probing the underlying connection with Ping.
// Detects a silently-dead TCP connection (NAT or firewall idle drop, a
// half-open socket with no RST, a hung PG backend) within the chosen
// interval instead of waiting on the OS-level ~2h TCP timeout. A failed
// probe triggers the normal reconnect flow.
//
// Zero or negative values fall back to the 30s default. The keepalive
// cannot be disabled — disabling reintroduces the bug it guards against.
// See https://github.com/sgaunet/pgqueue/issues/49.
func WithKeepaliveInterval(d time.Duration) Option {
	return func(l *Listener) { l.keepaliveInterval = normalizeKeepaliveInterval(d) }
}

// WithOnReconnect registers a callback invoked each time the Listener
// successfully re-establishes its PostgreSQL connection after a failure.
// attempt is the 1-based attempt number (1 = first successful reconnect after
// a drop, 2 = second, etc.). The callback runs in the Listener's internal
// goroutine and must not block for long; any heavy work should be dispatched
// to another goroutine. Registering a Prometheus counter increment here is a
// common use case:
//
//	reconnects := promauto.NewCounter(prometheus.CounterOpts{
//	    Name: "pglisten_reconnects_total",
//	    Help: "PostgreSQL LISTEN/NOTIFY reconnections.",
//	})
//	l, _ := pglisten.New(ctx, dsn,
//	    pglisten.WithOnReconnect(func(attempt int) { reconnects.Inc() }),
//	)
func WithOnReconnect(fn func(attempt int)) Option {
	return func(l *Listener) { l.onReconnect = fn }
}

// errListenerClosed is returned by Listen after the Listener has been closed.
var errListenerClosed = errors.New("pglisten: listener is closed")

// listenReq is a queued LISTEN request. done carries the outcome back to a
// synchronous Listen caller: nil once the LISTEN is confirmed on the server, or
// the error if issuing it failed. It is buffered (cap 1) so drainPending can
// always deliver the result even if the caller already returned via ctx or
// Close. A re-LISTEN queued by reconnect carries a nil done (no waiter).
type listenReq struct {
	channel string
	done    chan error
}

// signal delivers the outcome to the waiting Listen caller, if any. The buffered
// channel makes the send non-blocking; a nil done (a reconnect re-LISTEN) is a
// no-op.
func (r listenReq) signal(err error) {
	if r.done != nil {
		r.done <- err
	}
}

// Listener is a pgx-backed pgqueue.Listener. It owns a dedicated PostgreSQL
// connection, issues LISTEN for each requested channel, streams NOTIFY channel
// names, and transparently reconnects (re-issuing every LISTEN) if the
// connection drops.
type Listener struct {
	connString string
	notifs     chan string

	reconnectPolicy   ReconnectPolicy
	logger            *slog.Logger
	keepaliveInterval time.Duration
	onReconnect       func(attempt int) // optional hook; see WithOnReconnect

	mu        sync.Mutex
	conn      *pgx.Conn
	channels  map[string]bool // every channel to (re-)LISTEN on
	confirmed map[string]bool // channels whose LISTEN is live on the current session
	pending   []listenReq     // LISTEN requests not yet issued
	unpending []string        // UNLISTEN requests not yet issued (#52)
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

// compile-time check: *Listener satisfies the pgqueue.Listener hook interface,
// and also the optional Unlistener capability so queue deletions release the
// LISTEN registration on the backend session (#52).
var (
	_ pgqueue.Listener   = (*Listener)(nil)
	_ pgqueue.Unlistener = (*Listener)(nil)
)

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
		connString:        connString,
		notifs:            make(chan string, notifyBuffer),
		conn:              conn,
		channels:          make(map[string]bool),
		confirmed:         make(map[string]bool),
		done:              make(chan struct{}),
		reconnectPolicy:   ReconnectPolicy{}.normalized(),
		keepaliveInterval: defaultKeepaliveInterval,
	}
	for _, o := range opts {
		o(l)
	}
	if l.onReconnect == nil {
		l.onReconnect = func(int) {} // default no-op so the reconnect path needs no nil check
	}
	// run outlives this call; New's ctx scopes only the initial connect, so the
	// detached goroutine intentionally uses fresh contexts for its DB calls.
	go l.run() //nolint:contextcheck // detached goroutine; New's ctx does not scope it
	return l, nil
}

// Listen registers a channel for LISTEN and blocks until the LISTEN has been
// confirmed on the server, ctx is cancelled, or the Listener is closed. A nil
// return guarantees the channel is subscribed: notifications will now be
// delivered. The channel is remembered so the subscription survives a
// reconnect. Listen is idempotent and safe for concurrent use; a repeat call
// for an already-confirmed channel returns nil immediately.
//
// The LISTEN itself is issued by the run loop (which owns the connection); this
// method queues the request, nudges the loop, and waits for the outcome. ctx
// bounds that wait — on cancellation it returns an error wrapping ctx.Err()
// (still errors.Is-matchable against context.Canceled/DeadlineExceeded) while
// the request stays queued, so the LISTEN may still take effect and a later
// call observes it as already confirmed.
//
// The channel name is splice-quoted into LISTEN — PostgreSQL does not accept
// parameter binding there — with PG-correct identifier escaping (#70). It is
// still the caller's responsibility to keep names within PostgreSQL's
// identifier limits: at most 63 bytes (NAMEDATALEN-1) and no NUL bytes.
// pgqueue's own callers always satisfy this via validateQueueName; third
// parties using pglisten standalone should impose their own validation.
func (l *Listener) Listen(ctx context.Context, channel string) error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return errListenerClosed
	}
	if l.confirmed[channel] {
		l.mu.Unlock()
		return nil
	}
	l.channels[channel] = true
	done := make(chan error, 1)
	l.pending = append(l.pending, listenReq{channel: channel, done: done})
	l.mu.Unlock()

	// Break the in-progress wait so the run loop drains the request promptly.
	l.requestInterrupt()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("pglisten: LISTEN %q canceled: %w", channel, ctx.Err())
	case <-l.done:
		return errListenerClosed
	}
}

// Unlisten removes a channel from the LISTEN set so subsequent reconnects do
// not re-issue it, and asynchronously UNLISTENs it on the active connection
// to release the server-side registration. It is the inverse of Listen and is
// idempotent: unknown channels are a no-op. Called from pgqueue's queue-delete
// path so a process that churns queues does not leak LISTENs (#52).
//
// The same identifier rules as Listen apply: the channel name is splice-quoted
// into UNLISTEN with PG-correct escaping (#70), and the caller is responsible
// for keeping names within PostgreSQL's identifier limits.
//
// Unlike Listen, Unlisten does not wait for server-side confirmation: it is
// deliberately fire-and-forget. A failed UNLISTEN is harmless — the channel is
// already gone from the active set so no reconnect re-LISTENs it, and the next
// reconnect brings a clean session with no residue regardless — so there is
// nothing a caller could usefully do with a confirmation. The ctx is accepted
// for interface symmetry but the work is asynchronous.
func (l *Listener) Unlisten(_ context.Context, channel string) error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return errListenerClosed
	}
	if !l.channels[channel] {
		l.mu.Unlock()
		return nil
	}
	delete(l.channels, channel)
	delete(l.confirmed, channel)
	l.unpending = append(l.unpending, channel)
	l.mu.Unlock()

	// Break the in-progress wait so the run loop drains the UNLISTEN promptly.
	l.requestInterrupt()
	return nil
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
// The wait is bounded by the keepalive interval (#49): when the deadline
// fires we probe the connection with Ping to distinguish "live but idle"
// from "silently dead". The in-progress wait is also cancelled directly by
// requestInterrupt invoking the stored waitCancel.
func (l *Listener) receiveOne() bool {
	l.waitMu.Lock()
	if l.interruptPending {
		// A LISTEN request (or Close) arrived with no wait to cancel; consume
		// the flag and let the run loop drain it without blocking on a wait.
		l.interruptPending = false
		l.waitMu.Unlock()
		return true
	}
	interval := normalizeKeepaliveInterval(l.keepaliveInterval)
	waitCtx, cancel := context.WithTimeout(context.Background(), interval)
	l.waitCancel = cancel
	l.waitMu.Unlock()

	n, err := l.currentConn().WaitForNotification(waitCtx)

	l.waitMu.Lock()
	l.waitCancel = nil
	interrupted := l.interruptPending
	l.waitMu.Unlock()
	cancel()

	if l.isDone() {
		return false
	}
	if err != nil {
		// Three branches keyed on the wait-context error:
		//   - Canceled         => requestInterrupt() fired (LISTEN/Close).
		//   - DeadlineExceeded => keepalive tick — probe the connection.
		//   - nil              => genuine connection error — reconnect.
		// The `interrupted` snapshot covers the benign race where the
		// deadline and requestInterrupt fire in the same instant: if an
		// interrupt is pending we honor it instead of running a Ping the
		// next receiveOne would just retry anyway.
		switch {
		case interrupted, errors.Is(waitCtx.Err(), context.Canceled):
			return true
		case errors.Is(waitCtx.Err(), context.DeadlineExceeded):
			return l.keepaliveProbe()
		default:
			return l.reconnect()
		}
	}

	select {
	case l.notifs <- n.Channel:
	case <-l.done:
		return false
	}
	return true
}

// keepaliveProbe runs a short-bounded Ping on the listener's connection to
// distinguish "live but idle" from "silently dead". Safe to call here
// because the run goroutine has sole ownership of the connection and
// WaitForNotification has already returned; pgx leaves the connection
// usable for subsequent calls after a deadline-cancelled wait.
func (l *Listener) keepaliveProbe() bool {
	pingCtx, cancel := context.WithTimeout(context.Background(), defaultPingTimeout)
	defer cancel()
	if err := l.currentConn().Ping(pingCtx); err != nil {
		l.logWarn("pglisten: keepalive probe failed; reconnecting",
			"interval", normalizeKeepaliveInterval(l.keepaliveInterval),
			"error", err)
		return l.reconnect()
	}
	return true
}

// drainPending issues every queued LISTEN and UNLISTEN statement.
func (l *Listener) drainPending() error {
	l.mu.Lock()
	pending := l.pending
	l.pending = nil
	unpending := l.unpending
	l.unpending = nil
	conn := l.conn
	l.mu.Unlock()

	for i, req := range pending {
		// LISTEN takes no parameters, so the channel name must be interpolated.
		// quoteListenIdent applies PG-correct identifier escaping defensively:
		// pgqueue's own callers pre-validate channel names, but pglisten is an
		// exported package and third-party callers may pass arbitrary names
		// (#70).
		if _, err := conn.Exec(context.Background(), `LISTEN `+quoteListenIdent(req.channel)); err != nil {
			wrapped := fmt.Errorf("pglisten: LISTEN %q: %w", req.channel, err)
			// Tell this request's caller the LISTEN failed, then re-queue the
			// unissued requests so a reconnect retries them. The already-issued
			// ones are skipped; reconnect rebuilds them from the channels map.
			req.signal(wrapped)
			l.requeue(pending[i+1:])
			return wrapped
		}
		l.markConfirmed(req.channel)
		req.signal(nil)
	}
	for _, ch := range unpending {
		// UNLISTEN is best-effort: a failure here just means a stale
		// server-side registration. The channel is already gone from the
		// channels map, so reconnect will not re-LISTEN it; and the next
		// reconnect brings a fresh session with no residue regardless.
		if _, err := conn.Exec(context.Background(), `UNLISTEN `+quoteListenIdent(ch)); err != nil {
			l.logWarn("pglisten: UNLISTEN failed", "channel", ch, "error", err)
		}
	}
	return nil
}

// quoteListenIdent renders ch as a quoted PostgreSQL identifier suitable for
// LISTEN/UNLISTEN, doubling any embedded double quote per the SQL grammar.
// LISTEN cannot use parameter binding, so this is the only safe way to splice
// a channel name into the statement when callers may supply arbitrary values
// (#70).
func quoteListenIdent(ch string) string {
	return `"` + strings.ReplaceAll(ch, `"`, `""`) + `"`
}

// reconnect re-establishes the connection and re-issues every known LISTEN. It
// returns false when the listener is closed while reconnecting. Failed attempts
// back off exponentially with full jitter (R-07). On success the onReconnect
// hook (if set) is invoked with the 1-based success count.
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
		// A fresh session carries no live LISTENs, so nothing is confirmed and
		// any queued UNLISTENs are moot (#52).
		l.confirmed = make(map[string]bool)
		l.unpending = nil
		// Re-LISTEN every channel still in the active set. Carry over the done
		// channel of any request still waiting for confirmation so its Listen
		// caller is signalled once the re-LISTEN lands instead of blocking
		// until its ctx fires; channels with no waiter re-LISTEN with nil done.
		waiters := make(map[string]chan error, len(l.pending))
		for _, req := range l.pending {
			if req.done != nil {
				waiters[req.channel] = req.done
			}
		}
		l.pending = make([]listenReq, 0, len(l.channels))
		for ch := range l.channels {
			l.pending = append(l.pending, listenReq{channel: ch, done: waiters[ch]})
			delete(waiters, ch)
		}
		// Any leftover waiter is for a channel no longer in the active set
		// (concurrently Unlistened); signal it so the caller unblocks.
		for _, done := range waiters {
			done <- nil
		}
		l.mu.Unlock()
		// Notify the hook after releasing the lock so the callback never
		// blocks while holding l.mu. onReconnect is never nil (New defaults it
		// to a no-op).
		l.onReconnect(attempt + 1)
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

func (l *Listener) requeue(reqs []listenReq) {
	l.mu.Lock()
	l.pending = append(reqs, l.pending...)
	l.mu.Unlock()
}

// markConfirmed records that channel's LISTEN is live on the current session,
// so a repeat Listen returns immediately without re-queueing. reconnect clears
// the set because a fresh session carries no live LISTENs.
func (l *Listener) markConfirmed(channel string) {
	l.mu.Lock()
	l.confirmed[channel] = true
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
