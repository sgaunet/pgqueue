package pgqueue

import (
	"context"
	"testing"
)

// TestApplyConfigOptionsMaxRetries verifies that the default retry count is
// applied only when WithDefaultMaxRetries was not supplied, so an explicit zero
// ("no retries") survives instead of being coerced to the default of 3.
func TestApplyConfigOptionsMaxRetries(t *testing.T) {
	if got := applyConfigOptions(nil).defaultMaxRetries; got != 3 {
		t.Errorf("no option: defaultMaxRetries = %d, want 3", got)
	}

	zero := applyConfigOptions([]Option{WithDefaultMaxRetries(0)})
	if !zero.maxRetriesSet {
		t.Error("WithDefaultMaxRetries(0): maxRetriesSet = false, want true")
	}
	if zero.defaultMaxRetries != 0 {
		t.Errorf("WithDefaultMaxRetries(0): defaultMaxRetries = %d, want 0", zero.defaultMaxRetries)
	}

	if got := applyConfigOptions([]Option{WithDefaultMaxRetries(7)}).defaultMaxRetries; got != 7 {
		t.Errorf("WithDefaultMaxRetries(7): defaultMaxRetries = %d, want 7", got)
	}
}

// TestConfigFromLegacyMaxRetries verifies that the legacy Config keeps its
// documented "0 = use default" semantics: a zero DefaultMaxRetries resolves to
// the default of 3, while a positive value is carried through.
func TestConfigFromLegacyMaxRetries(t *testing.T) {
	legacyZero := applyConfigOptions(configFromLegacy(Config{DefaultMaxRetries: 0}))
	if legacyZero.defaultMaxRetries != 3 {
		t.Errorf("legacy DefaultMaxRetries=0: resolved to %d, want 3", legacyZero.defaultMaxRetries)
	}

	legacyFive := applyConfigOptions(configFromLegacy(Config{DefaultMaxRetries: 5}))
	if legacyFive.defaultMaxRetries != 5 {
		t.Errorf("legacy DefaultMaxRetries=5: resolved to %d, want 5", legacyFive.defaultMaxRetries)
	}
}

// uncomparableTestListener is a Listener whose dynamic type is not comparable
// (it has a slice field). onlySchemaOption must handle it without panicking.
type uncomparableTestListener struct{ marker []int }

func (uncomparableTestListener) Listen(context.Context, string) error { return nil }
func (uncomparableTestListener) Notifications() <-chan string         { return nil }
func (uncomparableTestListener) Close() error                         { return nil }

// TestOnlySchemaOption verifies that InitSchema's option gate accepts only
// WithSchema and, crucially, does not panic when an option carries a value
// whose dynamic type is not comparable (a non-comparable Listener).
func TestOnlySchemaOption(t *testing.T) {
	if !onlySchemaOption(nil) {
		t.Error("nil options: onlySchemaOption = false, want true")
	}
	if !onlySchemaOption([]Option{WithSchema("custom")}) {
		t.Error("WithSchema only: onlySchemaOption = false, want true")
	}
	if onlySchemaOption([]Option{WithMaxQueues(5)}) {
		t.Error("WithMaxQueues: onlySchemaOption = true, want false")
	}
	// Must return false, not panic, for a non-comparable listener value.
	if onlySchemaOption([]Option{WithListener(uncomparableTestListener{})}) {
		t.Error("WithListener: onlySchemaOption = true, want false")
	}
}
