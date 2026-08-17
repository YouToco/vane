//go:build !linux

package sandbox

import "testing"

func TestGateNetNSNeverDegradesToPortableSuccess(t *testing.T) {
	if _, _, err := CreateGateNetNS(t.TempDir()); err == nil {
		t.Fatal("portable host claimed a real network namespace")
	}
}
