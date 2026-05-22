package prompgqueue_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sgaunet/pgqueue/prompgqueue"
)

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
