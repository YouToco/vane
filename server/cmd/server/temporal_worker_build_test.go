package main

import "testing"

func TestIsLowerHexForTemporalWorkerBuildID(t *testing.T) {
	for _, test := range []struct {
		value string
		want  bool
	}{
		{"0123456789abcdef", true},
		{"", true},
		{"ABC", false},
		{"xyz", false},
		{"abc-123", false},
	} {
		if got := isLowerHex(test.value); got != test.want {
			t.Errorf("isLowerHex(%q)=%v want %v", test.value, got, test.want)
		}
	}
}
