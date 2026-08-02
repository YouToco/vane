package main

import (
	"os"
	"strings"
	"testing"
)

// This is an intentional release fence, not the end-state architecture.  The
// server-runtime constructor may only return after the recovery catalog and
// normal request graph have a real PostgreSQL test proving that RLS cannot
// turn an authorized read into a vacuous empty result.
func TestPrimaryStoreDefersServerRuntimeCutover(t *testing.T) {
	payload, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(payload)
	if !strings.Contains(source, "store.NewWithResearchRuntimeCapability(") {
		t.Fatal("primary Store compatibility constructor is missing")
	}
	if strings.Contains(source, "store.NewServerRuntimeWithResearchRuntimeCapability(") {
		t.Fatal("primary Store entered server runtime before the RLS graph gate")
	}
}

func TestOwnerCompatibilityReleaseContract(t *testing.T) {
	const want = "vane.server-release-contract/v1 primary_store=owner_compat_v1 research_store=restricted_v1"
	if serverReleaseContractV1 != want {
		t.Fatalf("release contract=%q want %q", serverReleaseContractV1, want)
	}
}
