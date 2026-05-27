package otelpgqueue_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/sgaunet/pgqueue"
	"github.com/sgaunet/pgqueue/otelpgqueue"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"
)

// stringyType is a custom type with a String method, used to exercise the
// fmt.Stringer escape hatch in the attribute type switch.
type stringyType struct{ s string }

func (s stringyType) String() string { return s.s }

// recordingSpan is a trace.Span that stores every SetAttributes call so tests
// can assert what the attribute conversion produced.
type recordingSpan struct {
	embedded.Span
	attrs []attribute.KeyValue
}

func (r *recordingSpan) SetAttributes(kv ...attribute.KeyValue)      { r.attrs = append(r.attrs, kv...) }
func (*recordingSpan) End(...trace.SpanEndOption)                    {}
func (*recordingSpan) AddEvent(string, ...trace.EventOption)         {}
func (*recordingSpan) AddLink(trace.Link)                            {}
func (*recordingSpan) IsRecording() bool                             { return true }
func (*recordingSpan) RecordError(error, ...trace.EventOption)       {}
func (*recordingSpan) SpanContext() trace.SpanContext                { return trace.SpanContext{} }
func (*recordingSpan) SetStatus(codes.Code, string)                  {}
func (*recordingSpan) SetName(string)                                {}
func (*recordingSpan) TracerProvider() trace.TracerProvider          { return nil }

// recordingTracer hands out one recordingSpan and remembers the attributes that
// were passed to Start (those go through the SpanStartConfig).
type recordingTracer struct {
	embedded.Tracer
	span      *recordingSpan
	startAttr []attribute.KeyValue
}

func (r *recordingTracer) Start(
	ctx context.Context, _ string, opts ...trace.SpanStartOption,
) (context.Context, trace.Span) {
	cfg := trace.NewSpanStartConfig(opts...)
	r.startAttr = cfg.Attributes()
	return ctx, r.span
}

type recordingProvider struct {
	embedded.TracerProvider
	tracer *recordingTracer
}

func (p recordingProvider) Tracer(string, ...trace.TracerOption) trace.Tracer { return p.tracer }

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

// TestAttrTypeCoverage exercises every attribute value type the adapter now
// understands. The recorded attribute.KeyValue is asserted against the
// expected OTel rendering so a future regression to "unsupported" (issue #93)
// is caught.
func TestAttrTypeCoverage(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 27, 10, 0, 0, 123456789, time.UTC)
	cases := []struct {
		name string
		in   pgqueue.Attr
		want attribute.KeyValue
	}{
		{"string", pgqueue.Attr{Key: "k", Value: "v"}, attribute.String("k", "v")},
		{"bool", pgqueue.Attr{Key: "k", Value: true}, attribute.Bool("k", true)},
		{"int", pgqueue.Attr{Key: "k", Value: 7}, attribute.Int("k", 7)},
		{"int8", pgqueue.Attr{Key: "k", Value: int8(7)}, attribute.Int64("k", 7)},
		{"int16", pgqueue.Attr{Key: "k", Value: int16(7)}, attribute.Int64("k", 7)},
		{"int32", pgqueue.Attr{Key: "k", Value: int32(7)}, attribute.Int64("k", 7)},
		{"int64", pgqueue.Attr{Key: "k", Value: int64(7)}, attribute.Int64("k", 7)},
		{"uint8", pgqueue.Attr{Key: "k", Value: uint8(7)}, attribute.Int64("k", 7)},
		{"uint16", pgqueue.Attr{Key: "k", Value: uint16(7)}, attribute.Int64("k", 7)},
		{"uint32", pgqueue.Attr{Key: "k", Value: uint32(7)}, attribute.Int64("k", 7)},
		{"uint_in_range", pgqueue.Attr{Key: "k", Value: uint(7)}, attribute.Int64("k", 7)},
		{"uint64_in_range", pgqueue.Attr{Key: "k", Value: uint64(7)}, attribute.Int64("k", 7)},
		{"uint64_overflow", pgqueue.Attr{Key: "k", Value: ^uint64(0)},
			attribute.String("k", "18446744073709551615")},
		{"float32", pgqueue.Attr{Key: "k", Value: float32(1.5)}, attribute.Float64("k", 1.5)},
		{"float64", pgqueue.Attr{Key: "k", Value: 1.5}, attribute.Float64("k", 1.5)},
		{"duration", pgqueue.Attr{Key: "k", Value: 2 * time.Second}, attribute.Int64("k", int64(2*time.Second))},
		{"time", pgqueue.Attr{Key: "k", Value: now},
			attribute.String("k", now.Format(time.RFC3339Nano))},
		{"error", pgqueue.Attr{Key: "k", Value: errors.New("boom")}, attribute.String("k", "boom")},
		{"stringer", pgqueue.Attr{Key: "k", Value: stringyType{s: "hi"}}, attribute.String("k", "hi")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			span := &recordingSpan{}
			tr := otelpgqueue.NewTracer(recordingProvider{tracer: &recordingTracer{span: span}})
			_, s := tr.StartSpan(context.Background(), "test")
			s.SetAttr(tc.in)
			s.End()
			if len(span.attrs) != 1 {
				t.Fatalf("want 1 recorded attribute, got %d", len(span.attrs))
			}
			got := span.attrs[0]
			if got.Key != tc.want.Key || got.Value.Type() != tc.want.Value.Type() ||
				got.Value.Emit() != tc.want.Value.Emit() {
				t.Fatalf("attr mismatch:\n  got:  %s=%s (%s)\n  want: %s=%s (%s)",
					got.Key, got.Value.Emit(), got.Value.Type(),
					tc.want.Key, tc.want.Value.Emit(), tc.want.Value.Type())
			}
		})
	}
}

// TestAttrFallbackLogsWarning verifies that an unsupported value type still
// produces an attribute (so the span is not lost), and that when a logger is
// configured via WithTracerLogger the adapter emits a WARN line naming the
// offending key — the missing half of the old "silently coerced" behavior.
func TestAttrFallbackLogsWarning(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	span := &recordingSpan{}
	tr := otelpgqueue.NewTracer(
		recordingProvider{tracer: &recordingTracer{span: span}},
		otelpgqueue.WithTracerLogger(logger),
	)
	_, s := tr.StartSpan(context.Background(), "test")
	type weird struct{ X int }
	s.SetAttr(pgqueue.Attr{Key: "custom", Value: weird{X: 1}})
	s.End()

	if len(span.attrs) != 1 {
		t.Fatalf("want 1 attribute, got %d", len(span.attrs))
	}
	if got := span.attrs[0]; got.Key != "custom" || got.Value.Type() != attribute.STRING {
		t.Fatalf("fallback should emit a string attribute, got %v=%v (%s)",
			got.Key, got.Value.Emit(), got.Value.Type())
	}
	if got := span.attrs[0].Value.AsString(); !strings.Contains(got, "{1}") {
		t.Fatalf("fallback value should reflect fmt.Sprintf rendering, got %q", got)
	}
	log := buf.String()
	if !strings.Contains(log, "level=WARN") || !strings.Contains(log, `key=custom`) {
		t.Fatalf("expected WARN log naming key=custom, got: %s", log)
	}
}

// TestAttrFallbackSilentWithoutLogger confirms that the adapter degrades
// gracefully (no log, no panic) when no logger is configured.
func TestAttrFallbackSilentWithoutLogger(t *testing.T) {
	t.Parallel()
	span := &recordingSpan{}
	tr := otelpgqueue.NewTracer(recordingProvider{tracer: &recordingTracer{span: span}})
	_, s := tr.StartSpan(context.Background(), "test")
	s.SetAttr(pgqueue.Attr{Key: "k", Value: struct{ X int }{X: 1}})
	if len(span.attrs) != 1 || span.attrs[0].Value.Type() != attribute.STRING {
		t.Fatalf("fallback should produce a string attribute, got %#v", span.attrs)
	}
}
