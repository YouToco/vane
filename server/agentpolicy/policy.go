// Package agentpolicy compiles the interactive Agent's non-secret prompt,
// model and tool surface into a deterministic, content-addressed policy.
//
// The package is deliberately data-only. It cannot execute tools, resolve
// credentials, choose a tenant or promote a candidate policy. Those
// authorities remain in the Vane harness and deployment control plane.
package agentpolicy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"
)

const (
	DefinitionSchemaVersionV1  = "vane.interactive-agent-policy/v1"
	ManifestSchemaVersionV1    = "vane.interactive-agent-policy-manifest/v1"
	ToolCatalogSchemaVersionV1 = "vane.interactive-agent-tool-catalog/v1"

	LaneOwner = "owner"
	LaneA2A   = "a2a"

	ThinkingEnabled  = "enabled"
	ReasoningHigh    = "high"
	ReasoningMaximum = "max"
)

var ErrInvalidPolicy = errors.New("agentpolicy: invalid policy")

// ModuleV1 is one ordered model-visible system-policy fragment. Body is kept
// in the content-addressed definition; the public manifest exposes only its
// digest so logs never need to duplicate prompt text.
type ModuleV1 struct {
	ID      string `json:"id"`
	Version string `json:"version"`
	Body    string `json:"body"`
}

// ModelRouteV1 freezes the provider-neutral model request policy used by one
// interactive lane. Max output is intentionally absent: current owner/A2A
// requests defer to the model's native context contract.
type ModelRouteV1 struct {
	Provider        string `json:"provider"`
	Model           string `json:"model"`
	Thinking        string `json:"thinking"`
	ReasoningEffort string `json:"reasoning_effort"`
}

// DefinitionV1 is the source policy selected by the trusted composition root.
// User Skill/MCP configuration will be compiled into lower-trust capability
// inputs in later versions; it must never overwrite these system modules.
type DefinitionV1 struct {
	SchemaVersion string       `json:"schema_version"`
	Lane          string       `json:"lane"`
	Modules       []ModuleV1   `json:"modules"`
	ModelRoute    ModelRouteV1 `json:"model_route"`
}

// ToolPolicyV1 mirrors only the trusted, local policy bits that affect the
// model-visible execution surface. Remote MCP annotations are never accepted
// as this structure's authority.
type ToolPolicyV1 struct {
	Effects                uint16 `json:"effects"`
	Authorization          uint8  `json:"authorization"`
	Budget                 uint8  `json:"budget"`
	Retry                  uint8  `json:"retry"`
	Concurrency            uint8  `json:"concurrency"`
	Exposure               uint8  `json:"exposure"`
	Intents                uint16 `json:"intents"`
	ResultTrust            uint8  `json:"result_trust"`
	RoutingConfigured      bool   `json:"routing_configured"`
	DirectOnExplicitIntent bool   `json:"direct_on_explicit_intent"`
}

// ToolV1 is the exact static definition and trusted policy compiled into the
// interactive tool-catalog digest. Parameters must be a JSON object.
type ToolV1 struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
	Policy      ToolPolicyV1    `json:"policy"`
}

// ToolCatalogV1 binds the static tools and an optional independently
// content-addressed deferred catalog such as TikHub tool_search.
type ToolCatalogV1 struct {
	SchemaVersion         string   `json:"schema_version"`
	Tools                 []ToolV1 `json:"tools"`
	DeferredCatalogDigest string   `json:"deferred_catalog_digest,omitempty"`
}

type ModuleRefV1 struct {
	ID      string `json:"id"`
	Version string `json:"version"`
	Digest  string `json:"digest"`
}

// ManifestV1 is safe to persist with every model call. It contains no prompt
// body, credential, endpoint URL or user content.
type ManifestV1 struct {
	SchemaVersion     string        `json:"schema_version"`
	Lane              string        `json:"lane"`
	DefinitionDigest  string        `json:"definition_digest"`
	ModuleRefs        []ModuleRefV1 `json:"module_refs"`
	ModelRouteDigest  string        `json:"model_route_digest"`
	ToolCatalogDigest string        `json:"tool_catalog_digest"`
}

// CompiledV1 is immutable after construction and supplies the exact request
// values consumed by the Vane harness.
type CompiledV1 struct {
	Definition   DefinitionV1
	SystemPrompt string
	Manifest     ManifestV1
}

// EncodeManifestV1 returns the canonical, non-secret audit payload and its
// SHA-256 digest. The payload is safe to attach to llm_calls: it contains only
// policy identities and digests, never prompt bodies, user content or secrets.
func EncodeManifestV1(manifest ManifestV1) ([]byte, string, error) {
	if err := validateManifestV1(manifest); err != nil {
		return nil, "", err
	}
	normalized := manifest
	normalized.ModuleRefs = slices.Clone(manifest.ModuleRefs)
	payload, err := json.Marshal(normalized)
	if err != nil {
		return nil, "", invalid("encode manifest")
	}
	return payload, digest(payload), nil
}

func CompileV1(def DefinitionV1, catalog ToolCatalogV1) (CompiledV1, error) {
	normalized, err := normalizeDefinition(def)
	if err != nil {
		return CompiledV1{}, err
	}
	normalizedCatalog, err := normalizeToolCatalog(catalog)
	if err != nil {
		return CompiledV1{}, err
	}
	definitionPayload, err := json.Marshal(normalized)
	if err != nil {
		return CompiledV1{}, invalid("encode definition")
	}
	catalogPayload, err := json.Marshal(normalizedCatalog)
	if err != nil {
		return CompiledV1{}, invalid("encode tool catalog")
	}
	modelPayload, err := json.Marshal(normalized.ModelRoute)
	if err != nil {
		return CompiledV1{}, invalid("encode model route")
	}

	refs := make([]ModuleRefV1, len(normalized.Modules))
	var prompt strings.Builder
	for i, module := range normalized.Modules {
		prompt.WriteString(module.Body)
		refs[i] = ModuleRefV1{
			ID: module.ID, Version: module.Version,
			Digest: digest([]byte(module.Body)),
		}
	}
	return CompiledV1{
		Definition:   normalized,
		SystemPrompt: prompt.String(),
		Manifest: ManifestV1{
			SchemaVersion:     ManifestSchemaVersionV1,
			Lane:              normalized.Lane,
			DefinitionDigest:  digest(definitionPayload),
			ModuleRefs:        refs,
			ModelRouteDigest:  digest(modelPayload),
			ToolCatalogDigest: digest(catalogPayload),
		},
	}, nil
}

func validateManifestV1(manifest ManifestV1) error {
	if manifest.SchemaVersion != ManifestSchemaVersionV1 ||
		(manifest.Lane != LaneOwner && manifest.Lane != LaneA2A) ||
		!validDigest(manifest.DefinitionDigest) ||
		!validDigest(manifest.ModelRouteDigest) ||
		!validDigest(manifest.ToolCatalogDigest) ||
		len(manifest.ModuleRefs) == 0 {
		return invalid("manifest header")
	}
	seen := make(map[string]struct{}, len(manifest.ModuleRefs))
	for _, ref := range manifest.ModuleRefs {
		if !validIdentifier(ref.ID) || !validVersion(ref.Version) ||
			!validDigest(ref.Digest) {
			return invalid("manifest module")
		}
		key := ref.ID + "@" + ref.Version
		if _, exists := seen[key]; exists {
			return invalid("duplicate manifest module")
		}
		seen[key] = struct{}{}
	}
	return nil
}

func normalizeDefinition(def DefinitionV1) (DefinitionV1, error) {
	if def.SchemaVersion != DefinitionSchemaVersionV1 ||
		(def.Lane != LaneOwner && def.Lane != LaneA2A) ||
		len(def.Modules) == 0 {
		return DefinitionV1{}, invalid("definition header")
	}
	seen := make(map[string]struct{}, len(def.Modules))
	modules := slices.Clone(def.Modules)
	for i, module := range modules {
		if !validIdentifier(module.ID) || !validVersion(module.Version) ||
			module.Body == "" || !utf8.ValidString(module.Body) ||
			strings.ContainsRune(module.Body, 0) {
			return DefinitionV1{}, invalid(fmt.Sprintf("module %d", i))
		}
		key := module.ID + "@" + module.Version
		if _, exists := seen[key]; exists {
			return DefinitionV1{}, invalid("duplicate module")
		}
		seen[key] = struct{}{}
	}
	if strings.TrimSpace(def.ModelRoute.Provider) == "" ||
		(strings.TrimSpace(def.ModelRoute.Model) == "" &&
			def.ModelRoute.Provider != "compatibility") ||
		def.ModelRoute.Thinking != ThinkingEnabled ||
		(def.ModelRoute.ReasoningEffort != ReasoningHigh &&
			def.ModelRoute.ReasoningEffort != ReasoningMaximum) ||
		!safeText(def.ModelRoute.Provider) || !safeText(def.ModelRoute.Model) {
		return DefinitionV1{}, invalid("model route")
	}
	def.Modules = modules
	return def, nil
}

func normalizeToolCatalog(catalog ToolCatalogV1) (ToolCatalogV1, error) {
	if catalog.SchemaVersion != ToolCatalogSchemaVersionV1 ||
		(catalog.DeferredCatalogDigest != "" &&
			!validDigest(catalog.DeferredCatalogDigest)) {
		return ToolCatalogV1{}, invalid("tool catalog header")
	}
	tools := slices.Clone(catalog.Tools)
	slices.SortFunc(tools, func(a, b ToolV1) int {
		return strings.Compare(a.Name, b.Name)
	})
	for i := range tools {
		tool := &tools[i]
		if !validIdentifier(tool.Name) || tool.Description == "" ||
			!safeText(tool.Description) || len(tool.Parameters) == 0 {
			return ToolCatalogV1{}, invalid("tool definition")
		}
		if i > 0 && tools[i-1].Name == tool.Name {
			return ToolCatalogV1{}, invalid("duplicate tool")
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(tool.Parameters, &object); err != nil || object == nil {
			return ToolCatalogV1{}, invalid("tool schema")
		}
		canonical, err := json.Marshal(object)
		if err != nil {
			return ToolCatalogV1{}, invalid("tool schema encoding")
		}
		tool.Parameters = canonical
	}
	catalog.Tools = tools
	return catalog, nil
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for i := range len(value) {
		b := value[i]
		if (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') ||
			b == '-' || b == '_' || b == '.' || b == '/' {
			continue
		}
		return false
	}
	return true
}

func validVersion(value string) bool {
	return len(value) >= 2 && len(value) <= 32 && value[0] == 'v' &&
		validIdentifier(value[1:])
}

func safeText(value string) bool {
	return utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

func digest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func invalid(part string) error {
	return fmt.Errorf("%w: %s", ErrInvalidPolicy, part)
}
