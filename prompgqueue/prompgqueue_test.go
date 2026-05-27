package prompgqueue_test

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sgaunet/pgqueue/prompgqueue"
)

// matchingDescCollector is a hand-rolled prometheus.Collector whose Describe
// emits the same *Desc as pgqueue's publish counter, so registering pgqueue's
// CounterVec after this collector lands in the AlreadyRegisteredError path
// where ExistingCollector is of a different Go type. Used to exercise the
// issue #92 regression — registerCollector must surface the type mismatch as
// an error instead of returning the unregistered new collector.
type matchingDescCollector struct{ desc *prometheus.Desc }

func (c *matchingDescCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }
func (*matchingDescCollector) Collect(chan<- prometheus.Metric)      {}

// TestNewMetricsRegistrationError is the R-17 regression test: a descriptor
// collision (a different collector already registered under one of pgqueue's
// metric names) must be surfaced as a non-nil error, not silently swallowed.
func TestNewMetricsRegistrationError(t *testing.T) {
	reg := prometheus.NewRegistry()

	// Pre-register a conflicting collector: same fully-qualified name as one of
	// pgqueue's metrics but an incompatible descriptor (a gauge, not a counter
	// vector). Registering pgqueue's real collector then fails with an error
	// that is NOT prometheus.AlreadyRegisteredError.
	clash := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pgqueue_publish_messages_total",
		Help: "conflicting descriptor for the R-17 test",
	})
	if err := reg.Register(clash); err != nil {
		t.Fatalf("pre-register clash collector: %v", err)
	}

	m, err := prompgqueue.NewMetrics(reg)
	if err == nil {
		t.Fatal("NewMetrics should return an error on a descriptor collision")
	}
	if m != nil {
		t.Error("NewMetrics should return a nil *Metrics alongside the error")
	}
}

// TestNewMetricsAlreadyRegisteredResolves is the R-17 regression test for the
// benign case: calling NewMetrics twice against the same registerer yields
// prometheus.AlreadyRegisteredError internally, which must resolve cleanly
// (reuse the existing collectors, no error).
func TestNewMetricsAlreadyRegisteredResolves(t *testing.T) {
	reg := prometheus.NewRegistry()

	m1, err := prompgqueue.NewMetrics(reg)
	if err != nil {
		t.Fatalf("first NewMetrics: %v", err)
	}
	m2, err := prompgqueue.NewMetrics(reg)
	if err != nil {
		t.Fatalf("second NewMetrics should resolve AlreadyRegisteredError cleanly: %v", err)
	}
	if m1 == nil || m2 == nil {
		t.Fatal("both NewMetrics calls should return a non-nil *Metrics")
	}

	// The reused collectors must still record without panicking.
	m2.RecordPublish("orders", 3)
	m2.RecordAck("orders", true)
}

// TestNewMetricsFreshRegistry confirms the happy path: all collectors register
// on a clean registry without error.
func TestNewMetricsFreshRegistry(t *testing.T) {
	if _, err := prompgqueue.NewMetrics(prometheus.NewRegistry()); err != nil {
		t.Fatalf("NewMetrics with a fresh registry: %v", err)
	}
}

// TestNewMetricsRejectsTypeMismatch is the issue #92 regression: when a
// caller has pre-registered a collector with the same descriptor as one
// pgqueue ships but a different Go type, NewMetrics must return an error.
// The previous behavior silently returned an unregistered CounterVec instance
// — writes vanished while the pre-registered collector kept scraping its
// initial values.
func TestNewMetricsRejectsTypeMismatch(t *testing.T) {
	reg := prometheus.NewRegistry()

	// Match pgqueue's publish-counter descriptor exactly: same FQ name, same
	// help, same variable label — so prometheus reports AlreadyRegistered
	// (not a generic descriptor mismatch).
	clash := &matchingDescCollector{
		desc: prometheus.NewDesc(
			"pgqueue_publish_messages_total",
			"Messages published to pgqueue queues.",
			[]string{"queue"}, nil,
		),
	}
	if err := reg.Register(clash); err != nil {
		t.Fatalf("pre-register clash collector: %v", err)
	}

	m, err := prompgqueue.NewMetrics(reg)
	if err == nil {
		t.Fatal("NewMetrics should reject a type-mismatched pre-registration")
	}
	if m != nil {
		t.Error("NewMetrics should return a nil *Metrics on error")
	}
	if !strings.Contains(err.Error(), "already registered as") ||
		!strings.Contains(err.Error(), "matchingDescCollector") {
		t.Errorf("error should name the conflicting collector type, got: %v", err)
	}
}
