package pgqueue

import (
	"database/sql"
	"math"
	"testing"
	"time"
)

// TestSecondsToProcessingTime is the #116 regression: large, NaN, and Inf
// float values must not overflow int64 nanoseconds and produce a negative or
// nonsensical duration.
func TestSecondsToProcessingTime(t *testing.T) {
	oneHour := time.Hour
	maxExpected := time.Duration(maxProcessingTimeSeconds * float64(time.Second))

	tests := []struct {
		name    string
		input   sql.NullFloat64
		wantNil bool
		wantMin time.Duration
		wantMax time.Duration
	}{
		{
			name:    "null",
			input:   sql.NullFloat64{},
			wantNil: true,
		},
		{
			name:    "normal one hour",
			input:   sql.NullFloat64{Float64: 3600, Valid: true},
			wantMin: oneHour,
			wantMax: oneHour,
		},
		{
			name:    "zero",
			input:   sql.NullFloat64{Float64: 0, Valid: true},
			wantMin: 0,
			wantMax: 0,
		},
		{
			name:    "negative clamped to zero",
			input:   sql.NullFloat64{Float64: -1, Valid: true},
			wantMin: 0,
			wantMax: 0,
		},
		{
			name:    "NaN clamped to zero",
			input:   sql.NullFloat64{Float64: math.NaN(), Valid: true},
			wantMin: 0,
			wantMax: 0,
		},
		{
			name:    "+Inf clamped to max",
			input:   sql.NullFloat64{Float64: math.Inf(1), Valid: true},
			wantMin: maxExpected,
			wantMax: maxExpected,
		},
		{
			name:    "-Inf clamped to zero",
			input:   sql.NullFloat64{Float64: math.Inf(-1), Valid: true},
			wantMin: 0,
			wantMax: 0,
		},
		{
			name:    "absurdly large clamped to max",
			input:   sql.NullFloat64{Float64: 1e300, Valid: true},
			wantMin: maxExpected,
			wantMax: maxExpected,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := secondsToProcessingTime(tc.input)
			if tc.wantNil {
				if got != nil {
					t.Errorf("want nil, got %v", got)
				}
				return
			}
			if got == nil {
				t.Fatal("got nil, want non-nil duration")
			}
			if *got < 0 {
				t.Errorf("duration is negative: %v", *got)
			}
			if *got < tc.wantMin || *got > tc.wantMax {
				t.Errorf("duration %v outside [%v, %v]", *got, tc.wantMin, tc.wantMax)
			}
		})
	}
}
