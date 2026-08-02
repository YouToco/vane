package researchtools

import (
	"encoding/json"
	"testing"

	"github.com/YouToco/vane/runtimepolicy"
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
