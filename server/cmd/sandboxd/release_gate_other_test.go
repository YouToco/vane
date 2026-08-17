//go:build !linux

package main

import (
	"bytes"
	"testing"
)

func TestReleaseGateNeverDegradesToPortableSuccess(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := runReleaseGate(t.Context(), nil, &stdout, &stderr); code != 1 {
		t.Fatalf("portable release Gate code=%d", code)
	}
}
