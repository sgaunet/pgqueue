package pgqueue

import (
	"context"
	"errors"
	"log/slog"
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

// uncomparableTestListener is a Listener whose dynamic type is not comparable
// (it has a slice field). onlySchemaOrLoggerOption must handle it without
// panicking.
type uncomparableTestListener struct{ marker []int }

func (uncomparableTestListener) Listen(context.Context, string) error { return nil }
func (uncomparableTestListener) Notifications() <-chan string         { return nil }
func (uncomparableTestListener) Close() error                         { return nil }

// TestOnlySchemaOrLoggerOption verifies that InitSchema's option gate accepts
// WithSchema and WithLogger, rejects everything else, and, crucially, does not
// panic when an option carries a value whose dynamic type is not comparable (a
// non-comparable Listener).
func TestOnlySchemaOrLoggerOption(t *testing.T) {
	if !onlySchemaOrLoggerOption(nil) {
		t.Error("nil options: onlySchemaOrLoggerOption = false, want true")
	}
	if !onlySchemaOrLoggerOption([]Option{WithSchema("custom")}) {
		t.Error("WithSchema only: onlySchemaOrLoggerOption = false, want true")
	}
	if !onlySchemaOrLoggerOption([]Option{WithLogger(slog.Default())}) {
		t.Error("WithLogger only: onlySchemaOrLoggerOption = false, want true")
	}
	if !onlySchemaOrLoggerOption([]Option{WithSchema("custom"), WithLogger(slog.Default())}) {
		t.Error("WithSchema+WithLogger: onlySchemaOrLoggerOption = false, want true")
	}
	if onlySchemaOrLoggerOption([]Option{WithMaxQueues(5)}) {
		t.Error("WithMaxQueues: onlySchemaOrLoggerOption = true, want false")
	}
	// Must return false, not panic, for a non-comparable listener value.
	if onlySchemaOrLoggerOption([]Option{WithListener(uncomparableTestListener{})}) {
		t.Error("WithListener: onlySchemaOrLoggerOption = true, want false")
	}
}

// TestApplyConfigOptionsMaxMessageSize verifies the 0-coerces-to-default
// behavior and that an explicit positive value (including MaxAllowedMessageSize)
// is preserved verbatim, so a caller asking for a larger payload cap is not
// silently downgraded to 256 KiB.
func TestApplyConfigOptionsMaxMessageSize(t *testing.T) {
	if got := applyConfigOptions(nil).maxMessageSize; got != defaultMaxMessageSize {
		t.Errorf("no option: maxMessageSize = %d, want %d", got, defaultMaxMessageSize)
	}

	max := applyConfigOptions([]Option{WithMaxMessageSize(MaxAllowedMessageSize)}).maxMessageSize
	if max != MaxAllowedMessageSize {
		t.Errorf("WithMaxMessageSize(MaxAllowedMessageSize): maxMessageSize = %d, want %d",
			max, MaxAllowedMessageSize)
	}

	custom := applyConfigOptions([]Option{WithMaxMessageSize(2 << 20)}).maxMessageSize
	if custom != 2<<20 {
		t.Errorf("WithMaxMessageSize(2 MiB): maxMessageSize = %d, want %d", custom, 2<<20)
	}
}

// TestValidateMaxMessageSize covers the boundary behavior of the shared
// guard used by New and CreateChannel/CreateTopic: zero is allowed (resolves to
// default downstream), MaxAllowedMessageSize is the inclusive upper bound, and
// anything outside that range returns ErrInvalidConfig.
func TestValidateMaxMessageSize(t *testing.T) {
	cases := []struct {
		name    string
		size    int
		wantErr bool
	}{
		{"zero", 0, false},
		{"small positive", 1024, false},
		{"at ceiling", MaxAllowedMessageSize, false},
		{"above ceiling", MaxAllowedMessageSize + 1, true},
		{"negative", -1, true},
		{"large negative", -1 << 20, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateMaxMessageSize(tc.size)
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidConfig) {
					t.Errorf("size=%d: err = %v, want ErrInvalidConfig", tc.size, err)
				}
			} else if err != nil {
				t.Errorf("size=%d: unexpected err %v", tc.size, err)
			}
		})
	}
}

// TestValidateMaxRetries pins that a per-queue max-retries override must be
// non-negative. A negative cap would make every message dead-letter on its
// first failure; channels are guarded by a DB CHECK but topics are not, so this
// validation is the only thing that fails the topic path loudly.
func TestValidateMaxRetries(t *testing.T) {
	cases := []struct {
		name    string
		n       int
		wantErr bool
	}{
		{"zero", 0, false},
		{"positive", 5, false},
		{"negative", -1, true},
		{"large negative", -1 << 20, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateMaxRetries(tc.n)
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidConfig) {
					t.Errorf("n=%d: err = %v, want ErrInvalidConfig", tc.n, err)
				}
			} else if err != nil {
				t.Errorf("n=%d: unexpected err %v", tc.n, err)
			}
		})
	}
}

// TestApplyConfigOptionsMaxMetadataSize verifies the 0-coerces-to-default
// behavior and that an explicit positive value (including MaxAllowedMetadataSize)
// is preserved verbatim.
func TestApplyConfigOptionsMaxMetadataSize(t *testing.T) {
	if got := applyConfigOptions(nil).maxMetadataSize; got != defaultMaxMetadataSize {
		t.Errorf("no option: maxMetadataSize = %d, want %d", got, defaultMaxMetadataSize)
	}

	max := applyConfigOptions([]Option{WithMaxMetadataSize(MaxAllowedMetadataSize)}).maxMetadataSize
	if max != MaxAllowedMetadataSize {
		t.Errorf("WithMaxMetadataSize(MaxAllowedMetadataSize): maxMetadataSize = %d, want %d",
			max, MaxAllowedMetadataSize)
	}

	custom := applyConfigOptions([]Option{WithMaxMetadataSize(64 << 10)}).maxMetadataSize
	if custom != 64<<10 {
		t.Errorf("WithMaxMetadataSize(64 KiB): maxMetadataSize = %d, want %d", custom, 64<<10)
	}
}

// TestValidateMaxMetadataSize covers the boundary behavior of the metadata
// cap guard: zero is allowed (resolves to default downstream),
// MaxAllowedMetadataSize is the inclusive upper bound, and anything outside
// that range returns ErrInvalidConfig.
func TestValidateMaxMetadataSize(t *testing.T) {
	cases := []struct {
		name    string
		size    int
		wantErr bool
	}{
		{"zero", 0, false},
		{"small positive", 1024, false},
		{"at ceiling", MaxAllowedMetadataSize, false},
		{"above ceiling", MaxAllowedMetadataSize + 1, true},
		{"negative", -1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateMaxMetadataSize(tc.size)
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidConfig) {
					t.Errorf("size=%d: err = %v, want ErrInvalidConfig", tc.size, err)
				}
			} else if err != nil {
				t.Errorf("size=%d: unexpected err %v", tc.size, err)
			}
		})
	}
}

// TestOnlySchemaOrLoggerOptionRejectsMaxMetadataSize confirms WithMaxMetadataSize
// is not silently accepted by InitSchema.
func TestOnlySchemaOrLoggerOptionRejectsMaxMetadataSize(t *testing.T) {
	if onlySchemaOrLoggerOption([]Option{WithMaxMetadataSize(1024)}) {
		t.Error("WithMaxMetadataSize: onlySchemaOrLoggerOption = true, want false")
	}
}

