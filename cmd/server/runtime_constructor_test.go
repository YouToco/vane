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
	if !strings.Contains(source, "store.New(ctx, cfg.DB.URL)") {
		t.Fatal("owner-compatible primary Store constructor is missing")
	}
	if strings.Contains(source, "store.NewWithResearchRuntimeCapability(") {
		t.Fatal("primary Store still carries a research runtime pool")
	}
}

func TestResearchV3UsesIndependentRestrictedControlStore(t *testing.T) {
	payload, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(payload)
	for _, required := range []string{
		"store.NewServerRuntimeWithResearchRuntimeCapability(",
		"ctx, cfg.DB.ResearchControlURL, cfg.DB.ResearchRuntimeURL,",
		"researchControlStore, gatewayClient, researchExecutor,",
		"researchControlStore.LoadQuotaRule(",
		"researchControlStore, push,",
		"newResearchV3DeliveryTargetResolver(researchControlStore, manager)",
		"closeStores := func() { closeServerStores(st, researchControlStore) }",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("research control Store wiring is missing %q", required)
		}
	}
	if strings.Contains(source, "st.Close()") {
		t.Fatal("an error or shutdown path bypasses dual-Store close")
	}
}

func TestOwnerCompatibilityReleaseContract(t *testing.T) {
	const want = "vane.server-release-contract/v2 primary_store=owner_compat_v1 research_control_store=restricted_v1 research_store=restricted_v1"
	if serverReleaseContractV2 != want {
		t.Fatalf("release contract=%q want %q", serverReleaseContractV2, want)
	}
}
