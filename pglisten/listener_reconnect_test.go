package pglisten

import "testing"

// TestRebuildPendingForReconnectPreservesAllWaiters is the regression for the
// reconnect waiter-loss bug. Two concurrent Listen calls for the same channel
// queue two pending requests, each with its own done channel. A reconnect must
// carry BOTH done channels over so neither caller is stranded — the old code
// keyed waiters by channel name in a map[string]chan error and kept only the
// last, dropping the first waiter forever even though the channel was re-LISTENed.
func TestRebuildPendingForReconnectPreservesAllWaiters(t *testing.T) {
	done1 := make(chan error, 1)
	done2 := make(chan error, 1)
	l := &Listener{
		channels:  map[string]bool{"A": true},
		confirmed: map[string]bool{},
		pending: []listenReq{
			{channel: "A", done: done1},
			{channel: "A", done: done2},
		},
	}

	l.rebuildPendingForReconnect()

	carried := make([]chan error, 0, 2)
	for _, req := range l.pending {
		if req.channel == "A" && req.done != nil {
			carried = append(carried, req.done)
		}
	}
	if len(carried) != 2 {
		t.Fatalf("expected both waiters carried as pending re-LISTEN requests, got %d", len(carried))
	}
	// Both original done channels must be present (order is not significant).
	seen := map[chan error]bool{carried[0]: true, carried[1]: true}
	if !seen[done1] || !seen[done2] {
		t.Fatal("carried requests do not reference the two original done channels")
	}

	// Active-channel waiters are carried for drainPending to signal once the
	// re-LISTEN lands; rebuild must not signal them itself.
	select {
	case <-done1:
		t.Fatal("done1 was signalled during rebuild; it should be carried, not signalled")
	case <-done2:
		t.Fatal("done2 was signalled during rebuild; it should be carried, not signalled")
	default:
	}
}

// TestRebuildPendingForReconnectSignalsOrphanedWaiter verifies a waiter whose
// channel was concurrently Unlistened (no longer in the active set) is signalled
// nil so its Listen caller unblocks instead of hanging until ctx/Close.
func TestRebuildPendingForReconnectSignalsOrphanedWaiter(t *testing.T) {
	done := make(chan error, 1)
	l := &Listener{
		channels:  map[string]bool{}, // "A" was Unlistened before reconnect
		confirmed: map[string]bool{},
		pending:   []listenReq{{channel: "A", done: done}},
	}

	l.rebuildPendingForReconnect()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("orphaned waiter signalled with %v, want nil", err)
		}
	default:
		t.Fatal("orphaned waiter was not signalled; its Listen caller would hang")
	}
	if len(l.pending) != 0 {
		t.Fatalf("expected no pending re-LISTEN for an empty active set, got %d", len(l.pending))
	}
}
