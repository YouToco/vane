package runtimepolicy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
)

func researchToolForTest(t *testing.T, name string) ResearchToolDefinitionV3 {
	t.Helper()
	implementation := ResearchToolExaSearchV3
	if name == "web_contents" {
		implementation = ResearchToolExaContentsV3
	}
	schema := json.RawMessage(`{"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false}`)
	canonical, err := canonicalResearchToolSchemaV3(schema)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(canonical)
	return ResearchToolDefinitionV3{
		Name: name, Description: "Trusted local definition",
		Parameters: canonical, SchemaDigest: hex.EncodeToString(sum[:]),
		Implementation: implementation, ImplementationGeneration: 1,
		Provider: "exa", Effects: []ResearchToolEffectV3{
			ResearchToolEffectBillableV3,
			ResearchToolEffectNetworkReadV3,
			ResearchToolEffectTrustTaintV3,
		},
		ResultTrust: ResearchToolTrustExternalV3, BudgetBucket: "exa_calls",
		CredentialRef:   CredentialRefV1{ID: CredentialIDExaPrimaryV1, Generation: 1},
		MaxCostMicroUSD: 10_000,
	}
}

func officialResearchToolForTest(t *testing.T) ResearchToolDefinitionV3 {
	t.Helper()
	schema := json.RawMessage(`{"type":"object","properties":{"page_url":{"type":"string","enum":["https://www.kimi.com/membership/pricing"]}},"required":["page_url"],"additionalProperties":false}`)
	canonical, err := canonicalResearchToolSchemaV3(schema)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(canonical)
	return ResearchToolDefinitionV3{
		Name: "web_product_status", Description: "Read the allowlisted official catalog.",
		Parameters: canonical, SchemaDigest: hex.EncodeToString(sum[:]),
		Implementation: ResearchToolKimiProductStatusV3, ImplementationGeneration: 1,
		Provider: "kimi", Effects: []ResearchToolEffectV3{
			ResearchToolEffectNetworkReadV3, ResearchToolEffectTrustTaintV3,
		},
		ResultTrust: ResearchToolTrustOfficialV3, BudgetBucket: "official_calls",
		MaxCostMicroUSD: 1,
	}
}

func TestResearchToolPolicyV3CanonicalRoundTrip(t *testing.T) {
	policy, err := BuildResearchToolPolicyV3([]ResearchToolDefinitionV3{
		researchToolForTest(t, "web_search"), researchToolForTest(t, "web_contents"),
		officialResearchToolForTest(t),
	})
	if err != nil || policy.AllowedTools[0].Name != "web_contents" {
		t.Fatalf("policy=%+v err=%v", policy, err)
	}
	payload, err := EncodeResearchToolPolicyV3(policy)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeResearchToolPolicyV3(payload)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := DigestResearchToolPolicyV3(decoded)
	if err != nil || len(digest) != 64 {
		t.Fatalf("digest=%q err=%v", digest, err)
	}
}

func TestResearchToolPolicyV3OfficialRouteCannotMasqueradeAsExa(t *testing.T) {
	valid := officialResearchToolForTest(t)
	if _, err := BuildResearchToolPolicyV3([]ResearchToolDefinitionV3{valid}); err != nil {
		t.Fatal(err)
	}
	mutations := []func(*ResearchToolDefinitionV3){
		func(tool *ResearchToolDefinitionV3) { tool.Provider = "exa" },
		func(tool *ResearchToolDefinitionV3) { tool.ResultTrust = ResearchToolTrustExternalV3 },
		func(tool *ResearchToolDefinitionV3) { tool.BudgetBucket = "exa_calls" },
		func(tool *ResearchToolDefinitionV3) {
			tool.CredentialRef = CredentialRefV1{ID: CredentialIDExaPrimaryV1, Generation: 1}
		},
		func(tool *ResearchToolDefinitionV3) {
			tool.Effects = append([]ResearchToolEffectV3{ResearchToolEffectBillableV3}, tool.Effects...)
		},
	}
	for index, mutate := range mutations {
		tampered := valid
		tampered.Effects = append([]ResearchToolEffectV3(nil), valid.Effects...)
		mutate(&tampered)
		if _, err := BuildResearchToolPolicyV3([]ResearchToolDefinitionV3{tampered}); err == nil {
			t.Fatalf("official route mutation %d accepted", index)
		}
	}
}

func TestResearchToolPolicyV3RejectsUnsafeAndDriftedGrants(t *testing.T) {
	unsafe := researchToolForTest(t, "web_search")
	unsafe.Effects = append(unsafe.Effects, ResearchToolEffectV3("state_write"))
	if _, err := BuildResearchToolPolicyV3([]ResearchToolDefinitionV3{unsafe}); err == nil {
		t.Fatal("state-writing grant accepted")
	}
	drifted := researchToolForTest(t, "web_search")
	drifted.SchemaDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := BuildResearchToolPolicyV3([]ResearchToolDefinitionV3{drifted}); err == nil {
		t.Fatal("schema drift accepted")
	}
	ephemeral := researchToolForTest(t, "web_search")
	ephemeral.Name = "read_endpoint_result"
	if _, err := BuildResearchToolPolicyV3([]ResearchToolDefinitionV3{ephemeral}); err == nil {
		t.Fatal("ephemeral handle tool accepted")
	}
}

func TestResearchToolPolicyV3StrictDecode(t *testing.T) {
	policy, err := BuildResearchToolPolicyV3([]ResearchToolDefinitionV3{researchToolForTest(t, "web_search")})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := EncodeResearchToolPolicyV3(policy)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]any
	if err := json.Unmarshal(payload, &object); err != nil {
		t.Fatal(err)
	}
	object["unexpected"] = true
	tampered, _ := json.Marshal(object)
	if _, err := DecodeResearchToolPolicyV3(tampered); err == nil {
		t.Fatal("unknown policy field accepted")
	}
}
