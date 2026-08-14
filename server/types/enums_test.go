package types

import (
	"errors"
	"testing"
)

func TestParseExecutionMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		raw      string
		expected ExecutionMode
		wantErr  bool
	}{
		{name: "compiled", raw: "compiled", expected: ExecutionModeCompiled},
		{name: "discover at run", raw: "discover_at_run", expected: ExecutionModeDiscoverAtRun},
		{name: "empty", raw: "", expected: ExecutionModeUnknown, wantErr: true},
		{name: "unknown sentinel", raw: "unknown", expected: ExecutionModeUnknown, wantErr: true},
		{name: "future value", raw: "autonomous", expected: ExecutionModeUnknown, wantErr: true},
		{name: "leading whitespace", raw: " compiled", expected: ExecutionModeUnknown, wantErr: true},
		{name: "wrong case", raw: "Compiled", expected: ExecutionModeUnknown, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseExecutionMode(tt.raw)
			if got != tt.expected {
				t.Fatalf("ParseExecutionMode(%q) = %q, want %q", tt.raw, got, tt.expected)
			}
			if tt.wantErr {
				if !errors.Is(err, ErrValidation) {
					t.Fatalf("ParseExecutionMode(%q) error = %v, want ErrValidation", tt.raw, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseExecutionMode(%q) error = %v", tt.raw, err)
			}
		})
	}
}

func TestExecutionMode_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mode     ExecutionMode
		expected bool
	}{
		{name: "zero value", mode: "", expected: false},
		{name: "unknown sentinel", mode: ExecutionModeUnknown, expected: false},
		{name: "compiled", mode: ExecutionModeCompiled, expected: true},
		{name: "discover at run", mode: ExecutionModeDiscoverAtRun, expected: true},
		{name: "unrecognized", mode: "other", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.mode.Valid(); got != tt.expected {
				t.Fatalf("ExecutionMode(%q).Valid() = %v, want %v", tt.mode, got, tt.expected)
			}
		})
	}
}
