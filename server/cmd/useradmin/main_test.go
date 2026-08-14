package main

import "testing"

func TestAdministrativeSubcommandsRejectMissingScope(t *testing.T) {
	if got := runTenant(nil); got != 2 {
		t.Fatalf("runTenant(nil)=%d, want 2", got)
	}
	if got := runQuota(nil); got != 2 {
		t.Fatalf("runQuota(nil)=%d, want 2", got)
	}
}
