package otelpgqueue_test

import (
	"context"
	"errors"
	"testing"

	"github.com/sgaunet/pgqueue"
	"github.com/sgaunet/pgqueue/otelpgqueue"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"
)

// nilSpanTracer is a trace.Tracer whose Start returns a nil span — modelling a
// misbehaving third-party TracerProvider.
type nilSpanTracer struct{ embedded.Tracer }

func (nilSpanTracer) Start(
	ctx context.Context, _ string, _ ...trace.SpanStartOption,
) (context.Context, trace.Span) {
	return ctx, nil
}

// nilSpanProvider hands out nilSpanTracer instances.
type nilSpanProvider struct{ embedded.TracerProvider }

func (nilSpanProvider) Tracer(string, ...trace.TracerOption) trace.Tracer {
	return nilSpanTracer{}
}

// TestStartSpanNilSpanNoPanic is the R-23 regression test: when the underlying
// OpenTelemetry tracer returns a nil span, starting and ending the span — and
// recording an error or attributes on it — must not panic.
func TestStartSpanNilSpanNoPanic(t *testing.T) {
	tr := otelpgqueue.NewTracer(nilSpanProvider{})

	ctx, span := tr.StartSpan(context.Background(), "pgqueue.test")
	if ctx == nil {
		t.Fatal("StartSpan returned a nil context")
	}
	if span == nil {
		t.Fatal("StartSpan returned a nil pgqueue.Span")
	}

	// None of these must panic despite the nil underlying span.
	span.SetAttr(pgqueue.StringAttr("queue", "orders"))
	span.SetError(errors.New("synthetic failure"))
	span.End()
}

// TestStartSpanRealProviderNoPanic confirms the normal path (a real no-op
// provider via NewTracer(nil)) also works end to end.
func TestStartSpanRealProviderNoPanic(t *testing.T) {
	tr := otelpgqueue.NewTracer(nil)
	_, span := tr.StartSpan(context.Background(), "pgqueue.test")
	if span == nil {
		t.Fatal("StartSpan returned a nil pgqueue.Span")
	}
	span.SetError(nil) // nil error must be a no-op
	span.End()
}
