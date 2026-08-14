package researchtools

import (
	"encoding/json"
	"testing"

	"github.com/YouToco/vane/server/acquisitiontool"
	"github.com/YouToco/vane/server/runtimepolicy"
)

func exaCatalogForTest(t *testing.T) runtimepolicy.CapabilityCatalogV1 {
	t.Helper()
	return runtimepolicy.CapabilityCatalogV1{
		SchemaVersion: runtimepolicy.CapabilityCatalogSchemaVersionV1,
		Allowed: []runtimepolicy.CapabilityV1{
			{
				Platform: "web", Capability: "contents", Kind: "page_content",
				ImplementationVersion: runtimepolicy.CapabilityImplementationExaV1,
				CredentialRef: runtimepolicy.CredentialRefV1{
					ID: runtimepolicy.CredentialIDExaPrimaryV1, Generation: 7,
				}, DependencyCredentialRefs: []runtimepolicy.CredentialRefV1{},
			},
			{
				Platform: "web", Capability: "search", Kind: "article",
				ImplementationVersion: runtimepolicy.CapabilityImplementationExaV1,
				CredentialRef: runtimepolicy.CredentialRefV1{
					ID: runtimepolicy.CredentialIDExaPrimaryV1, Generation: 7,
				}, DependencyCredentialRefs: []runtimepolicy.CredentialRefV1{},
			},
		},
	}
}

func TestRegistryV3RejectsNonExactOfficialProductStatusRoutes(t *testing.T) {
	model, ok := acquisitiontool.LookupModelToolDefinitionV1("web_product_status")
	if !ok {
		t.Fatal("missing model tool")
	}
	policy, err := runtimepolicy.BuildResearchToolPolicyV3([]runtimepolicy.ResearchToolDefinitionV3{{
		Name: "web_product_status", Description: model.Description, Parameters: model.ArgumentsSchema,
		Implementation: runtimepolicy.ResearchToolKimiProductStatusV3, ImplementationGeneration: 1,
		Provider: "kimi", Effects: []runtimepolicy.ResearchToolEffectV3{
			runtimepolicy.ResearchToolEffectNetworkReadV3, runtimepolicy.ResearchToolEffectTrustTaintV3,
		}, ResultTrust: runtimepolicy.ResearchToolTrustOfficialV3,
		BudgetBucket: "official_calls", MaxCostMicroUSD: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewRegistryV3(policy)
	if err != nil {
		t.Fatal(err)
	}
	valid := json.RawMessage(`{"page_url":"https://www.kimi.com/membership/pricing"}`)
	if _, _, err := registry.Canonicalize(registry.Digest(), "web_product_status", valid); err != nil {
		t.Fatal(err)
	}
	for _, pageURL := range []string{
		"https://www.kimi.com:443/membership/pricing",
		"https://www.kimi.com/membership/pricing?x=1",
		"https://www.kimi.com/membership/pricing#x",
		"https://user@www.kimi.com/membership/pricing",
		"https://www.kimi.com/membership/%70ricing",
		"https://kimi.com/membership/pricing",
		"https://www.kimi.com/membership/pricing/",
	} {
		raw, _ := json.Marshal(map[string]string{"page_url": pageURL})
		if _, _, err := registry.Canonicalize(registry.Digest(), "web_product_status", raw); err == nil {
			t.Fatalf("non-exact official route accepted: %s", pageURL)
		}
	}
}

func TestRegistryV3FreezesAndCanonicalizesScheduledWebTools(t *testing.T) {
	policy, err := BuildScheduledWebPolicyV3(exaCatalogForTest(t), 50_000)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewRegistryV3(policy)
	if err != nil || len(registry.Policy().AllowedTools) != 2 {
		t.Fatalf("registry=%+v err=%v", registry, err)
	}
	canonical, tool, err := registry.Canonicalize(registry.Digest(), "web_search",
		json.RawMessage(`{"include_domains":["kimi.com"],"query":"Kimi plan"}`))
	if err != nil || string(canonical) != `{"include_domains":["kimi.com"],"query":"Kimi plan"}` ||
		tool.CredentialRef.Generation != 7 {
		t.Fatalf("canonical=%s tool=%+v err=%v", canonical, tool, err)
	}
	if _, _, err := registry.Canonicalize("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"web_search", json.RawMessage(`{"query":"Kimi"}`)); err == nil {
		t.Fatal("catalog drift accepted")
	}
	if _, _, err := registry.Canonicalize(registry.Digest(), "web_search",
		json.RawMessage(`{"query":"Kimi","write":true}`)); err == nil {
		t.Fatal("unknown argument accepted")
	}
}

func TestBuildScheduledWebPolicyV3FailsClosedWithoutExactRoute(t *testing.T) {
	catalog := exaCatalogForTest(t)
	catalog.Allowed = catalog.Allowed[:1]
	if _, err := BuildScheduledWebPolicyV3(catalog, 50_000); err == nil {
		t.Fatal("missing search route accepted")
	}
}
