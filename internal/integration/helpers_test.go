package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sgaunet/pgqueue"
)

// eventually polls cond every interval until it returns true or timeout elapses.
// If the condition is not met within timeout the test is failed with msg.
//
// Use this instead of a fixed time.Sleep when waiting for an asynchronous
// side-effect that should become observable quickly once it has happened (e.g.
// a row count reaching a target, a status column flipping, a metric counter
// incrementing). The polling reduces unnecessary wait time while keeping the
// test deterministic.
//
// For "negative" assertions — proving that something does NOT happen within a
// window — keep the plain time.Sleep and add an "// intentional:" comment
// explaining the reason.
func eventually(t *testing.T, timeout, interval time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(msg)
			return
		}
		time.Sleep(interval)
	}
}

// crashedConsumerTimeout is the visibility timeout used by crashedConsumerClaim;
// it is short so redelivery-eligibility tests do not wait long.
const crashedConsumerTimeout = 200 * time.Millisecond

// crashedConsumerClaim simulates a consumer that crashes mid-processing: it
// claims the next available channel message with the supplied visibility
// timeout and returns it WITHOUT ever acking or nacking it. The caller can then
// assert on redelivery behaviour once the visibility timeout lapses.
//
// It is the shared scaffolding for the R-05 (backoff-on-reclaim) and R-08
// (Close joins background goroutines) regression tests.
func crashedConsumerClaim(
	t *testing.T,
	pq *pgqueue.Queue,
	channelName string,
	visibilityTimeout time.Duration,
) *pgqueue.Message {
	t.Helper()
	ctx := context.Background()

	msg, err := pq.ReceiveChannel(ctx, channelName,
		pgqueue.WithVisibilityTimeout(visibilityTimeout))
	if err != nil {
		t.Fatalf("crashedConsumerClaim: receive failed: %v", err)
	}
	if msg == nil {
		t.Fatal("crashedConsumerClaim: receive returned nil message")
	}
	// Intentionally never Ack/Nack: the "consumer" has crashed.
	return msg
}

// panicPayload is the marker error wrapped by a panicking handler when it
// panics with an error value.
var panicPayload = errors.New("handler panic: simulated crash")

// newPanickingHandler returns a pgqueue.Handler that panics whenever the
// delivered message's payload exactly matches one of panicOn. For every other
// payload it returns nil (auto-ack).
//
// If panicWithError is true the handler panics with an error value
// (panicPayload); otherwise it panics with a non-error value (a plain string).
// Both variants exercise the panic-to-error conversion path in R-01.
//
// The seen channel, when non-nil, receives every payload the handler is invoked
// with (including the panicking ones, before the panic) so tests can observe
// progress.
func newPanickingHandler(
	panicOn []string,
	panicWithError bool,
	seen chan<- string,
) pgqueue.Handler {
	panicSet := make(map[string]struct{}, len(panicOn))
	for _, p := range panicOn {
		panicSet[p] = struct{}{}
	}
	return func(_ context.Context, msg *pgqueue.Message) error {
		body := string(msg.Payload)
		if seen != nil {
			select {
			case seen <- body:
			default:
			}
		}
		if _, shouldPanic := panicSet[body]; shouldPanic {
			if panicWithError {
				panic(panicPayload)
			}
			panic("handler panic: non-error value for payload " + body)
		}
		return nil
	}
}
