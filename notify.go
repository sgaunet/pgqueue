package pgqueue

// notify.go implements push-based delivery via PostgreSQL LISTEN/NOTIFY.
//
// On publish, pgqueue issues a NOTIFY inside the publishing transaction so a
// blocked consumer wakes the instant the message becomes visible. Receiving
// notifications needs driver-specific code (pgx's WaitForNotification, lib/pq's
// pq.Listener), so the core defines only the Listener hook interface here;
// concrete driver-backed implementations live in optional sub-packages. When no
// Listener is registered, consume loops fall back to the safety-net poll.

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
)

// notifyChannelPrefix prefixes the PostgreSQL NOTIFY channel derived from a
// queue's physical table name.
const notifyChannelPrefix = "pgqueue_"

// notifyChannelName returns the LISTEN/NOTIFY channel name for a queue's
// per-queue table. Table names are already validated identifiers, so the
// derived channel name is safe.
func notifyChannelName(tableName string) string {
	return notifyChannelPrefix + tableName
}

// emitNotify issues a NOTIFY on a queue's channel inside the publishing
// transaction. PostgreSQL delivers the notification only on commit, so a
// blocked consumer is woken exactly when the message becomes visible and never
// for a rolled-back publish (FR-014). A failed NOTIFY is logged but never fails
// the publish — delivery still happens via the safety-net poll.
func (pq *Queue) emitNotify(ctx context.Context, tx *sql.Tx, tableName string) {
	// pg_notify (the function) is used instead of the NOTIFY statement so the
	// channel name is passed as a bound parameter.
	if _, err := tx.ExecContext(
		ctx, "SELECT pg_notify($1, '')", notifyChannelName(tableName),
	); err != nil {
		pq.logError("failed to emit notify", "error", err)
	}
}

// Listener is the hook interface for push-based delivery. When one is registered
// with WithListener, blocking consume loops wake immediately on a NOTIFY instead
// of waiting for the next safety-net poll. The core stays driver-agnostic:
// concrete implementations (pgx, lib/pq) ship as optional sub-packages.
type Listener interface {
	// Listen begins listening on the given PostgreSQL notification channel. It
	// is called once per distinct channel and must be safe for concurrent use.
	Listen(ctx context.Context, channel string) error
	// Notifications returns a stream of notification channel names, one per
	// received NOTIFY. The returned channel is closed when the Listener closes.
	Notifications() <-chan string
	// Close releases the listener's database connection and resources.
	Close() error
}

// Unlistener is an optional capability that a Listener may implement. When a
// queue is deleted the notifier calls Unlisten so the implementation can drop
// its per-channel bookkeeping and issue UNLISTEN on the server, keeping a
// process that churns queues from accumulating residue (#52). Implementations
// that omit it are unaffected — the leak is bounded by channels-ever-consumed
// and the safety-net poll still covers correctness.
type Unlistener interface {
	Unlisten(ctx context.Context, channel string) error
}

// waker is a one-to-many wake-up primitive: any number of consume loops select
// on wait(); signal() releases all of them and arms a fresh generation.
type waker struct {
	mu sync.Mutex
	ch chan struct{}
}

func newWaker() *waker {
	return &waker{ch: make(chan struct{})}
}

// wait returns the current generation's channel; it is closed by the next
// signal(). Callers must re-fetch it after each wake.
func (w *waker) wait() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ch
}

// signal wakes every current waiter and arms the next generation.
func (w *waker) signal() {
	w.mu.Lock()
	defer w.mu.Unlock()
	close(w.ch)
	w.ch = make(chan struct{})
}

// listenEscalateThreshold is the number of consecutive Listen failures on the
// same channel after which the notifier escalates from WARN to ERROR. Every
// further multiple of this threshold re-fires an ERROR line so a persistently
// degraded LISTEN stays visible in operator dashboards without spamming.
const listenEscalateThreshold = 10

// notifier demultiplexes a Listener's single notification stream into per-queue
// wakers and lazily issues LISTEN for each queue a consumer blocks on.
type notifier struct {
	listener Listener
	logger   *slog.Logger

	mu             sync.Mutex
	wakers         map[string]*waker // notification channel -> waker
	listening      map[string]bool   // notification channel -> LISTEN issued
	listenFailures map[string]int    // notification channel -> consecutive Listen failures (#68)
	closed         bool

	pumpOnce sync.Once
}

// newNotifier wraps a Listener. It returns nil when listener is nil so callers
// can treat "no push delivery" as a simple nil check. The logger is used to
// report a pump-goroutine panic and persistent LISTEN failures; nil silences
// both reports.
func newNotifier(listener Listener, logger *slog.Logger) *notifier {
	if listener == nil {
		return nil
	}
	return &notifier{
		listener:       listener,
		logger:         logger,
		wakers:         make(map[string]*waker),
		listening:      make(map[string]bool),
		listenFailures: make(map[string]int),
	}
}

// wakeChan returns the channel a consume loop selects on to learn that a
// message may have arrived on notifyChannel. It lazily issues LISTEN and starts
// the demux pump. A nil return means push delivery is unavailable; the caller
// then relies solely on the safety-net poll.
func (n *notifier) wakeChan(ctx context.Context, notifyChannel string) <-chan struct{} {
	// Hold n.mu across the whole decide -> LISTEN -> record sequence so the
	// check-then-act is atomic: concurrent wakeChan callers for the same
	// channel cannot double-issue LISTEN, and a caller cannot issue LISTEN
	// after close() — close() takes the same lock (R-06).
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.closed {
		return nil
	}

	w, ok := n.wakers[notifyChannel]
	if !ok {
		w = newWaker()
		n.wakers[notifyChannel] = w
	}

	// Start the demux pump once. pumpOnce.Do does not take n.mu; the spawned
	// pump goroutine blocks on n.mu until this call returns, so calling it
	// under the lock is safe.
	n.pumpOnce.Do(func() { go n.pump() })

	if !n.listening[notifyChannel] {
		// Issue LISTEN exactly once per channel, atomically with recording it.
		// On error the channel stays unregistered: the safety-net poll still
		// delivers the message and a later wakeChan call retries LISTEN. A
		// persistently failing LISTEN used to be invisible to operators; now
		// every failure logs at WARN and the count is escalated to ERROR
		// every listenEscalateThreshold consecutive failures so a
		// misconfigured Listener is impossible to miss (#68).
		if err := n.listener.Listen(ctx, notifyChannel); err == nil {
			n.listening[notifyChannel] = true
			delete(n.listenFailures, notifyChannel)
		} else {
			n.listenFailures[notifyChannel]++
			n.logListenFailure(notifyChannel, err, n.listenFailures[notifyChannel])
		}
	}
	return w.wait()
}

// logListenFailure reports a Listen call failure. It always logs at WARN with
// the channel, the underlying error and the consecutive-failure count, and
// re-escalates to ERROR every listenEscalateThreshold failures so a
// persistently degraded LISTEN remains visible in operator dashboards (#68).
// Called with n.mu held.
func (n *notifier) logListenFailure(channel string, err error, attempt int) {
	if n.logger == nil {
		return
	}
	n.logger.Warn("pgqueue: LISTEN failed; consumers degrade to safety-net poll",
		"channel", channel,
		"attempt", attempt,
		"error", err)
	if attempt%listenEscalateThreshold == 0 {
		n.logger.Error("pgqueue: LISTEN keeps failing on this channel",
			"channel", channel,
			"consecutive_failures", attempt)
	}
}

// forget drops the waker and LISTEN bookkeeping for a notify channel whose
// queue has been deleted, so the wakers map does not grow without bound as
// queues are created and destroyed over a long-lived process. When the
// underlying Listener implements Unlistener, forget also asks it to release
// its own registration so the pgqueue.Listener implementation does not
// accumulate residue either (#52). The Unlisten call is best-effort: an
// error is silenced and the local bookkeeping is dropped regardless, since
// the safety-net poll covers any wakeup we miss. A consumer still blocked on
// the just-deleted queue stops receiving NOTIFY wakeups; that is acceptable,
// since deleting a queue with a live consumer is a caller error and the
// safety-net poll surfaces the dropped table on its next tick.
func (n *notifier) forget(ctx context.Context, notifyChannel string) {
	n.mu.Lock()
	delete(n.wakers, notifyChannel)
	delete(n.listening, notifyChannel)
	delete(n.listenFailures, notifyChannel)
	closed := n.closed
	listener := n.listener
	n.mu.Unlock()

	if closed {
		return
	}
	if u, ok := listener.(Unlistener); ok {
		_ = u.Unlisten(ctx, notifyChannel)
	}
}

// pump fans the Listener's notification stream out to the per-channel wakers.
// A panic from a misbehaving third-party Listener (sending on a closed channel,
// etc.) is recovered: the pump cannot restart, but waking every consumer one
// last time lets them fall back to the safety-net poll instead of blocking on
// a NOTIFY that will never arrive (#69).
func (n *notifier) pump() {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		if n.logger != nil {
			n.logger.Error("pgqueue: listener notifications pump panicked",
				"panic", r,
				"stack", string(debug.Stack()))
		}
		n.wakeAll()
	}()
	for channel := range n.listener.Notifications() {
		n.mu.Lock()
		w := n.wakers[channel]
		n.mu.Unlock()
		if w != nil {
			w.signal()
		}
	}
}

// wakeAll signals every currently-registered waker. Used by close() for the
// orderly shutdown path and by the pump's panic-recovery so consumers that
// were blocked on a now-dead notification stream fall back to the safety-net
// poll instead of waiting forever (#69). Each signal is guarded with its own
// recover so one broken waker cannot prevent the others from being woken.
func (n *notifier) wakeAll() {
	n.mu.Lock()
	wakers := make([]*waker, 0, len(n.wakers))
	for _, w := range n.wakers {
		wakers = append(wakers, w)
	}
	n.mu.Unlock()
	for _, w := range wakers {
		func(w *waker) {
			defer func() { _ = recover() }()
			w.signal()
		}(w)
	}
}

// close stops the notifier and closes the underlying Listener. It wakes every
// blocked consumer a final time so they re-check their context and exit.
func (n *notifier) close() error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	n.mu.Unlock()

	err := n.listener.Close()
	n.wakeAll()
	if err != nil {
		return fmt.Errorf("listener close: %w", err)
	}
	return nil
}
