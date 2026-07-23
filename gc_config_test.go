package pgqueue

import (
	"errors"
	"testing"
	"time"
)

// TestNewGarbageCollectorConfigValidation pins the constructor's config
// validation (D7/M13): NewGarbageCollector now returns an error for an
// out-of-range configuration instead of silently clamping it. Pure config
// validation — no database required.
func TestNewGarbageCollectorConfigValidation(t *testing.T) {
	cases := []struct {
		name    string
		config  GarbageCollectorConfig
		wantErr bool
	}{
		{"empty config defaults", GarbageCollectorConfig{}, false},
		{
			"fully specified valid",
			GarbageCollectorConfig{
				Interval:      time.Minute,
				MaxWorkers:    4,
				DefaultPolicy: RetentionPolicy{CompletedMessageTTL: time.Hour},
			},
			false,
		},
		{"workers at upper bound 100", GarbageCollectorConfig{MaxWorkers: maxGCMaxWorkers}, false},
		{"workers at lower bound 1", GarbageCollectorConfig{MaxWorkers: 1}, false},
		{"workers over 100", GarbageCollectorConfig{MaxWorkers: maxGCMaxWorkers + 1}, true},
		{"negative workers", GarbageCollectorConfig{MaxWorkers: -1}, true},
		{"negative interval", GarbageCollectorConfig{Interval: -time.Second}, true},
		{
			"negative CompletedMessageTTL (not KeepForever)",
			GarbageCollectorConfig{DefaultPolicy: RetentionPolicy{CompletedMessageTTL: -5 * time.Hour}},
			true,
		},
		{
			"negative MaxPendingAge",
			GarbageCollectorConfig{DefaultPolicy: RetentionPolicy{MaxPendingAge: -time.Minute}},
			true,
		},
		{
			"KeepForever retention is valid",
			GarbageCollectorConfig{DefaultPolicy: RetentionPolicy{CompletedMessageTTL: KeepForever}},
			false,
		},
		{
			"negative retention in a Policies entry",
			GarbageCollectorConfig{Policies: map[string]RetentionPolicy{
				"orders": {DLQRetention: -time.Hour},
			}},
			true,
		},
		{
			"KeepForever in a Policies entry is valid",
			GarbageCollectorConfig{Policies: map[string]RetentionPolicy{
				"orders": {DLQRetention: KeepForever},
			}},
			false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gc, err := NewGarbageCollector(&Queue{}, tc.config)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got nil")
				}
				if !errors.Is(err, ErrInvalidConfig) {
					t.Errorf("error = %v, want it to wrap ErrInvalidConfig", err)
				}
				if gc != nil {
					t.Errorf("expected a nil GarbageCollector on error, got %v", gc)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gc == nil {
				t.Fatal("expected a non-nil GarbageCollector")
			}
		})
	}
}
