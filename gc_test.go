package pgqueue

import (
	"testing"
	"time"
)

// NewGarbageCollector substitutes default retention for an all-zero
// DefaultPolicy so a GarbageCollector created without a policy still bounds
// table growth (issue #47). These tests pin that defaulting and its escape
// hatches; they need no database — the constructor only reads/writes config.

func TestNewGarbageCollectorDefaultsEmptyPolicy(t *testing.T) {
	gc := NewGarbageCollector(&Queue{}, GarbageCollectorConfig{})

	if gc.config.DefaultPolicy != defaultRetentionPolicy {
		t.Errorf("empty DefaultPolicy: got %+v, want default %+v",
			gc.config.DefaultPolicy, defaultRetentionPolicy)
	}
	// Pending messages are live data: the default must never auto-purge them.
	if defaultRetentionPolicy.MaxPendingAge != 0 {
		t.Errorf("default MaxPendingAge = %v, want 0 (pending messages kept)",
			defaultRetentionPolicy.MaxPendingAge)
	}
}

func TestNewGarbageCollectorKeepsConfiguredPolicy(t *testing.T) {
	want := RetentionPolicy{
		CompletedMessageTTL: time.Hour,
		MaxPendingAge:       2 * time.Hour,
		DLQRetention:        3 * time.Hour,
	}
	gc := NewGarbageCollector(&Queue{}, GarbageCollectorConfig{DefaultPolicy: want})

	if gc.config.DefaultPolicy != want {
		t.Errorf("configured DefaultPolicy: got %+v, want %+v", gc.config.DefaultPolicy, want)
	}
}

// TestNewGarbageCollectorKeepForeverNotOverridden proves a policy that opts a
// field into KeepForever is not mistaken for "unconfigured": it makes the
// struct non-zero, so the constructor leaves it — and the other zero fields
// keep their "forever" meaning rather than picking up defaults.
func TestNewGarbageCollectorKeepForeverNotOverridden(t *testing.T) {
	policy := RetentionPolicy{CompletedMessageTTL: KeepForever}
	gc := NewGarbageCollector(&Queue{}, GarbageCollectorConfig{DefaultPolicy: policy})

	if gc.config.DefaultPolicy != policy {
		t.Errorf("KeepForever policy was overridden: got %+v, want %+v",
			gc.config.DefaultPolicy, policy)
	}
}
