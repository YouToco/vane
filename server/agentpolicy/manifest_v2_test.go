package agentpolicy

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/skillruntime"
)

func TestManifestV2CanonicalLowerTrustReferences(t *testing.T) {
	compiled, err := CompileV1(CurrentOwnerV1("deepseek", "deepseek-v4-flash"),
		ToolCatalogV1{SchemaVersion: ToolCatalogSchemaVersionV1})
	if err != nil {
		t.Fatal(err)
	}
	refs := []LowerTrustCapabilityRefV2{
		testLowerTrustRefV2("mcp", "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
		testLowerTrustRefV2("skill", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
	}
	manifest, err := BuildManifestV2(compiled.Manifest, refs)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.CapabilityRefs[0].Kind != CapabilityKindMCPV2 ||
		manifest.CapabilityRefs[1].Kind != CapabilityKindSkillV2 {
		t.Fatalf("refs not canonically sorted: %+v", manifest.CapabilityRefs)
	}
	payload, digest1, err := EncodeManifestV2(manifest)
	if err != nil {
		t.Fatal(err)
	}
	decoded, digest2, err := DecodeManifestV2(payload)
	if err != nil || digest1 != digest2 || len(decoded.CapabilityRefs) != 2 {
		t.Fatalf("decoded=%+v digests=%q/%q err=%v", decoded, digest1, digest2, err)
	}
	for _, forbidden := range []string{
		OwnerCoreSystemPromptV1, "deepseek", "https://", "SKILL.md", "credential", "description",
	} {
		if bytes.Contains(payload, []byte(forbidden)) {
			t.Fatalf("manifest v2 leaked forbidden %q: %s", forbidden, payload)
		}
	}
	for _, candidate := range [][]byte{
		append([]byte(" "), payload...),
		bytes.Replace(payload, []byte(`"capability_refs"`), []byte(`"unknown":true,"capability_refs"`), 1),
		bytes.Replace(payload, []byte(`"version":1`), []byte(`"version":1.0`), 1),
	} {
		if _, _, err := DecodeManifestV2(candidate); !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("payload=%s err=%v", candidate, err)
		}
	}
}

func TestManifestV2RejectsTrustEscalationOrAmbiguousVersion(t *testing.T) {
	compiled, err := CompileV1(CurrentA2AV1("deepseek", "deepseek-v4-flash"),
		ToolCatalogV1{SchemaVersion: ToolCatalogSchemaVersionV1})
	if err != nil {
		t.Fatal(err)
	}
	base := testLowerTrustRefV2("skill", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "cccccccc-cccc-4ccc-8ccc-cccccccccccc")
	for name, mutate := range map[string]func(*LowerTrustCapabilityRefV2){
		"trusted":      func(v *LowerTrustCapabilityRefV2) { v.Trust = "trusted" },
		"zero version": func(v *LowerTrustCapabilityRefV2) { v.Version = 0 },
		"uppercase id": func(v *LowerTrustCapabilityRefV2) { v.CapabilityID = strings.ToUpper(v.CapabilityID) },
		"unknown kind": func(v *LowerTrustCapabilityRefV2) { v.Kind = "script" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := base
			mutate(&candidate)
			if _, err := BuildManifestV2(compiled.Manifest, []LowerTrustCapabilityRefV2{candidate}); !errors.Is(err, ErrInvalidPolicy) {
				t.Fatalf("ref=%+v err=%v", candidate, err)
			}
		})
	}
}

func TestManifestV2NormalizesEmptySetAndRejectsNullWire(t *testing.T) {
	compiled, err := CompileV1(CurrentOwnerV1("deepseek", "deepseek-v4-flash"),
		ToolCatalogV1{SchemaVersion: ToolCatalogSchemaVersionV1})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := BuildManifestV2(compiled.Manifest, nil)
	if err != nil {
		t.Fatal(err)
	}
	payload, _, err := EncodeManifestV2(manifest)
	if err != nil || !bytes.Contains(payload, []byte(`"capability_refs":[]`)) {
		t.Fatalf("empty manifest=%s err=%v", payload, err)
	}
	nullWire := bytes.Replace(payload, []byte(`"capability_refs":[]`),
		[]byte(`"capability_refs":null`), 1)
	if _, _, err := DecodeManifestV2(nullWire); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("null capability set err=%v", err)
	}
}

func TestManifestV2RejectsInvalidBaseOrderingDuplicatesAndEmptyWire(t *testing.T) {
	if _, err := BuildManifestV2(ManifestV1{}, nil); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("invalid base err=%v", err)
	}
	compiled, err := CompileV1(CurrentOwnerV1("deepseek", "deepseek-v4-flash"),
		ToolCatalogV1{SchemaVersion: ToolCatalogSchemaVersionV1})
	if err != nil {
		t.Fatal(err)
	}
	first := testLowerTrustRefV2("mcp", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
	second := testLowerTrustRefV2("skill", "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
		"dddddddd-dddd-4ddd-8ddd-dddddddddddd")
	manifest, err := BuildManifestV2(compiled.Manifest, []LowerTrustCapabilityRefV2{first, second})
	if err != nil {
		t.Fatal(err)
	}
	manifest.CapabilityRefs[0], manifest.CapabilityRefs[1] = manifest.CapabilityRefs[1], manifest.CapabilityRefs[0]
	if _, _, err := EncodeManifestV2(manifest); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("unsorted manifest err=%v", err)
	}
	if _, err := BuildManifestV2(compiled.Manifest, []LowerTrustCapabilityRefV2{first, first}); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("duplicate manifest err=%v", err)
	}
	if _, _, err := DecodeManifestV2(nil); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("empty manifest err=%v", err)
	}
}

func TestLowerTrustSkillRefV2BindsExactSkillIdentity(t *testing.T) {
	ref := skillruntime.SkillRefV1{
		SchemaVersion: skillruntime.SkillRefSchemaVersionV1,
		TenantID:      1, OwnerUserID: 2,
		CapabilityID:        uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
		CapabilityVersionID: uuid.MustParse("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
		Version:             7, Visibility: skillruntime.VisibilityWorkspaceV1,
		PayloadDigest: strings.Repeat("a", 64), SkillMDDigest: strings.Repeat("b", 64),
		FileManifestDigest: strings.Repeat("c", 64), Compatible: true,
	}
	projected, err := LowerTrustSkillRefV2(ref)
	if err != nil {
		t.Fatal(err)
	}
	if projected.ReferenceDigest != skillruntime.DigestRefV1(ref) ||
		projected.CapabilityVersionID != ref.CapabilityVersionID.String() ||
		projected.Trust != CapabilityTrustLowerV2 || projected.Kind != CapabilityKindSkillV2 {
		t.Fatalf("projected=%+v", projected)
	}
	ref.ContainsScripts = true
	if _, err := LowerTrustSkillRefV2(ref); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("script-bearing ref projection err=%v", err)
	}
}

func testLowerTrustRefV2(kind, capabilityID, versionID string) LowerTrustCapabilityRefV2 {
	return LowerTrustCapabilityRefV2{
		Kind: kind, Visibility: CapabilityPersonalV2,
		CapabilityID: capabilityID, CapabilityVersionID: versionID, Version: 1,
		ReferenceDigest: strings.Repeat("a", 64), PayloadDigest: strings.Repeat("b", 64),
		Trust: CapabilityTrustLowerV2,
	}
}
