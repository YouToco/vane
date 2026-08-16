package agentpolicy

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/internal/strictjson"
	"github.com/YouToco/vane/server/skillruntime"
)

const (
	ManifestSchemaVersionV2 = "vane.interactive-agent-policy-manifest/v2"
	CapabilityTrustLowerV2  = "lower_trust"
	CapabilityKindSkillV2   = "skill"
	CapabilityKindMCPV2     = "mcp"
	CapabilityPersonalV2    = "personal"
	CapabilityWorkspaceV2   = "workspace"

	maxManifestV2Bytes = 256 << 10
)

// LowerTrustCapabilityRefV2 records an exact immutable extension identity.
// It deliberately excludes Skill bytes, MCP endpoints, credentials, schemas,
// prompt bodies and tool descriptions. Trust is fixed rather than inherited
// from any remote/user annotation.
type LowerTrustCapabilityRefV2 struct {
	Kind                string `json:"kind"`
	Visibility          string `json:"visibility"`
	CapabilityID        string `json:"capability_id"`
	CapabilityVersionID string `json:"capability_version_id"`
	Version             int    `json:"version"`
	ReferenceDigest     string `json:"reference_digest"`
	PayloadDigest       string `json:"payload_digest"`
	Trust               string `json:"trust"`
}

// ManifestV2 extends the immutable V1 audit identity with lower-trust
// capability references. No production Agent path consumes V2 in the dark
// slice; V1 remains the exact wire while the feature gate is off.
type ManifestV2 struct {
	SchemaVersion     string                      `json:"schema_version"`
	Lane              string                      `json:"lane"`
	DefinitionDigest  string                      `json:"definition_digest"`
	ModuleRefs        []ModuleRefV1               `json:"module_refs"`
	ModelRouteDigest  string                      `json:"model_route_digest"`
	ToolCatalogDigest string                      `json:"tool_catalog_digest"`
	CapabilityRefs    []LowerTrustCapabilityRefV2 `json:"capability_refs"`
}

// LowerTrustSkillRefV2 projects a validated exact Skill reference into the
// generic manifest identity without copying any Skill bytes.
func LowerTrustSkillRefV2(ref skillruntime.SkillRefV1) (LowerTrustCapabilityRefV2, error) {
	if err := ref.Validate(); err != nil {
		return LowerTrustCapabilityRefV2{}, invalid("Skill capability ref")
	}
	return LowerTrustCapabilityRefV2{
		Kind: CapabilityKindSkillV2, Visibility: ref.Visibility,
		CapabilityID: ref.CapabilityID.String(), CapabilityVersionID: ref.CapabilityVersionID.String(),
		Version: ref.Version, ReferenceDigest: skillruntime.DigestRefV1(ref),
		PayloadDigest: ref.PayloadDigest, Trust: CapabilityTrustLowerV2,
	}, nil
}

func BuildManifestV2(base ManifestV1, refs []LowerTrustCapabilityRefV2) (ManifestV2, error) {
	if err := validateManifestV1(base); err != nil {
		return ManifestV2{}, err
	}
	manifest := ManifestV2{
		SchemaVersion: ManifestSchemaVersionV2, Lane: base.Lane,
		DefinitionDigest: base.DefinitionDigest, ModuleRefs: slices.Clone(base.ModuleRefs),
		ModelRouteDigest: base.ModelRouteDigest, ToolCatalogDigest: base.ToolCatalogDigest,
		CapabilityRefs: make([]LowerTrustCapabilityRefV2, len(refs)),
	}
	copy(manifest.CapabilityRefs, refs)
	slices.SortFunc(manifest.CapabilityRefs, compareLowerTrustCapabilityRefV2)
	if err := validateManifestV2(manifest); err != nil {
		return ManifestV2{}, err
	}
	return manifest, nil
}

func EncodeManifestV2(manifest ManifestV2) ([]byte, string, error) {
	if err := validateManifestV2(manifest); err != nil {
		return nil, "", err
	}
	normalized := manifest
	normalized.ModuleRefs = slices.Clone(manifest.ModuleRefs)
	normalized.CapabilityRefs = slices.Clone(manifest.CapabilityRefs)
	slices.SortFunc(normalized.CapabilityRefs, compareLowerTrustCapabilityRefV2)
	if !slices.Equal(normalized.CapabilityRefs, manifest.CapabilityRefs) {
		return nil, "", invalid("manifest capability order")
	}
	payload, err := json.Marshal(normalized)
	if err != nil || len(payload) > maxManifestV2Bytes {
		return nil, "", invalid("encode manifest v2")
	}
	return payload, digest(payload), nil
}

func DecodeManifestV2(payload []byte) (ManifestV2, string, error) {
	if len(payload) == 0 || len(payload) > maxManifestV2Bytes {
		return ManifestV2{}, "", invalid("manifest v2 size")
	}
	var manifest ManifestV2
	if err := strictjson.DecodeExact(payload, &manifest); err != nil {
		return ManifestV2{}, "", invalid("manifest v2 JSON")
	}
	canonical, manifestDigest, err := EncodeManifestV2(manifest)
	if err != nil {
		return ManifestV2{}, "", err
	}
	if !bytes.Equal(canonical, payload) {
		return ManifestV2{}, "", invalid("manifest v2 is not canonical")
	}
	return manifest, manifestDigest, nil
}

func validateManifestV2(manifest ManifestV2) error {
	base := ManifestV1{
		SchemaVersion: ManifestSchemaVersionV1, Lane: manifest.Lane,
		DefinitionDigest: manifest.DefinitionDigest, ModuleRefs: manifest.ModuleRefs,
		ModelRouteDigest: manifest.ModelRouteDigest, ToolCatalogDigest: manifest.ToolCatalogDigest,
	}
	if manifest.SchemaVersion != ManifestSchemaVersionV2 || validateManifestV1(base) != nil ||
		manifest.CapabilityRefs == nil ||
		len(manifest.CapabilityRefs) > 256 {
		return invalid("manifest v2 header")
	}
	seen := make(map[string]struct{}, len(manifest.CapabilityRefs))
	for i, ref := range manifest.CapabilityRefs {
		capabilityID, capabilityErr := uuid.Parse(ref.CapabilityID)
		versionID, versionErr := uuid.Parse(ref.CapabilityVersionID)
		if (ref.Kind != CapabilityKindSkillV2 && ref.Kind != CapabilityKindMCPV2) ||
			(ref.Visibility != CapabilityPersonalV2 && ref.Visibility != CapabilityWorkspaceV2) ||
			capabilityErr != nil || capabilityID == uuid.Nil || capabilityID.String() != ref.CapabilityID ||
			versionErr != nil || versionID == uuid.Nil || versionID.String() != ref.CapabilityVersionID ||
			ref.Version <= 0 || !validDigest(ref.ReferenceDigest) ||
			!validDigest(ref.PayloadDigest) || ref.Trust != CapabilityTrustLowerV2 {
			return invalid("manifest v2 capability ref")
		}
		key := ref.Kind + "/" + ref.CapabilityID + "/" + ref.CapabilityVersionID
		if _, duplicate := seen[key]; duplicate {
			return invalid("duplicate manifest v2 capability ref")
		}
		seen[key] = struct{}{}
		if i > 0 && compareLowerTrustCapabilityRefV2(manifest.CapabilityRefs[i-1], ref) >= 0 {
			return invalid("manifest v2 capability order")
		}
	}
	return nil
}

func compareLowerTrustCapabilityRefV2(a, b LowerTrustCapabilityRefV2) int {
	for _, pair := range [][2]string{
		{a.Kind, b.Kind}, {a.CapabilityID, b.CapabilityID},
		{a.CapabilityVersionID, b.CapabilityVersionID},
	} {
		if compared := strings.Compare(pair[0], pair[1]); compared != 0 {
			return compared
		}
	}
	return 0
}
