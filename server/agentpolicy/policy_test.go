package agentpolicy

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestCompileV1ContentAddressesEveryPolicySurface(t *testing.T) {
	def := CurrentOwnerV1("deepseek", "deepseek-v4-flash")
	catalog := ToolCatalogV1{
		SchemaVersion:         ToolCatalogSchemaVersionV1,
		DeferredCatalogDigest: strings.Repeat("a", 64),
		Tools: []ToolV1{{
			Name: "z_tool", Description: "Z", Parameters: json.RawMessage(`{"type":"object"}`),
			Policy: ToolPolicyV1{Effects: 1, Authorization: 2},
		}, {
			Name: "a_tool", Description: "A", Parameters: json.RawMessage(`{"type": "object"}`),
			Policy: ToolPolicyV1{Effects: 1, Authorization: 2},
		}},
	}
	compiled, err := CompileV1(def, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if compiled.SystemPrompt != OwnerCoreSystemPromptV1 ||
		compiled.Manifest.Lane != LaneOwner ||
		len(compiled.Manifest.ModuleRefs) != 1 {
		t.Fatalf("compiled=%+v", compiled)
	}

	reordered := catalog
	reordered.Tools = []ToolV1{catalog.Tools[1], catalog.Tools[0]}
	same, err := CompileV1(def, reordered)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(same.Manifest, compiled.Manifest) {
		t.Fatalf("input tool order changed manifest: %+v != %+v", same.Manifest, compiled.Manifest)
	}

	mutations := []struct {
		name  string
		edit  func(*DefinitionV1, *ToolCatalogV1)
		field func(ManifestV1) string
	}{
		{"prompt", func(d *DefinitionV1, _ *ToolCatalogV1) { d.Modules[0].Body += "x" }, func(m ManifestV1) string { return m.DefinitionDigest }},
		{"model", func(d *DefinitionV1, _ *ToolCatalogV1) { d.ModelRoute.Model = "deepseek-v4-pro" }, func(m ManifestV1) string { return m.ModelRouteDigest }},
		{"tool schema", func(_ *DefinitionV1, c *ToolCatalogV1) {
			c.Tools[0].Parameters = json.RawMessage(`{"type":"object","required":[]}`)
		}, func(m ManifestV1) string { return m.ToolCatalogDigest }},
		{"tool policy", func(_ *DefinitionV1, c *ToolCatalogV1) { c.Tools[0].Policy.Effects++ }, func(m ManifestV1) string { return m.ToolCatalogDigest }},
		{"deferred catalog", func(_ *DefinitionV1, c *ToolCatalogV1) { c.DeferredCatalogDigest = strings.Repeat("b", 64) }, func(m ManifestV1) string { return m.ToolCatalogDigest }},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			d, c := def, catalog
			d.Modules = append([]ModuleV1(nil), def.Modules...)
			c.Tools = append([]ToolV1(nil), catalog.Tools...)
			tc.edit(&d, &c)
			got, compileErr := CompileV1(d, c)
			if compileErr != nil {
				t.Fatal(compileErr)
			}
			if tc.field(got.Manifest) == tc.field(compiled.Manifest) {
				t.Fatalf("mutation did not change manifest: %+v", got.Manifest)
			}
		})
	}
}

func TestCompileV1RejectsAmbiguousOrRemoteAuthorityShapes(t *testing.T) {
	base := CurrentOwnerV1("deepseek", "deepseek-v4-flash")
	validCatalog := ToolCatalogV1{SchemaVersion: ToolCatalogSchemaVersionV1}
	tests := []struct {
		name string
		def  DefinitionV1
		cat  ToolCatalogV1
	}{
		{"unknown lane", func() DefinitionV1 { d := base; d.Lane = "external"; return d }(), validCatalog},
		{"duplicate module", func() DefinitionV1 { d := base; d.Modules = append(d.Modules, d.Modules[0]); return d }(), validCatalog},
		{"disabled thinking", func() DefinitionV1 { d := base; d.ModelRoute.Thinking = "disabled"; return d }(), validCatalog},
		{"invalid deferred digest", base, ToolCatalogV1{SchemaVersion: ToolCatalogSchemaVersionV1, DeferredCatalogDigest: "remote"}},
		{"non object schema", base, ToolCatalogV1{SchemaVersion: ToolCatalogSchemaVersionV1, Tools: []ToolV1{{Name: "tool", Description: "x", Parameters: json.RawMessage(`[]`)}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := CompileV1(tc.def, tc.cat); !errors.Is(err, ErrInvalidPolicy) {
				t.Fatalf("CompileV1 error=%v, want ErrInvalidPolicy", err)
			}
		})
	}
}

func TestEncodeManifestV1IsCanonicalAndContainsNoPolicyBody(t *testing.T) {
	compiled, err := CompileV1(
		CurrentOwnerV1("deepseek", "deepseek-v4-flash"),
		ToolCatalogV1{SchemaVersion: ToolCatalogSchemaVersionV1},
	)
	if err != nil {
		t.Fatal(err)
	}
	payload, manifestDigest, err := EncodeManifestV1(compiled.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	second, secondDigest, err := EncodeManifestV1(compiled.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != string(second) || manifestDigest != secondDigest ||
		!validDigest(manifestDigest) {
		t.Fatalf("manifest encoding is not deterministic: %q %q", manifestDigest, secondDigest)
	}
	for _, forbidden := range []string{
		OwnerCoreSystemPromptV1,
		"deepseek-v4-flash",
		"deepseek",
	} {
		if strings.Contains(string(payload), forbidden) {
			t.Fatalf("manifest leaked policy body or route value %q: %s", forbidden, payload)
		}
	}

	invalid := compiled.Manifest
	invalid.ModuleRefs = append(invalid.ModuleRefs, invalid.ModuleRefs[0])
	if _, _, err := EncodeManifestV1(invalid); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("duplicate module error=%v, want ErrInvalidPolicy", err)
	}
}
