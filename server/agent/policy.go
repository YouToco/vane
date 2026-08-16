package agent

import (
	"fmt"
	"slices"

	"github.com/YouToco/vane/server/agentpolicy"
	"github.com/YouToco/vane/server/llm"
)

func compileInteractivePolicy(
	d Deps,
	tools map[string]ToolSpec,
) (agentpolicy.CompiledV1, llm.ReasoningEffort, error) {
	var definition agentpolicy.DefinitionV1
	if d.Policy != nil {
		if d.Model != "" || d.SystemPrompt != "" {
			return agentpolicy.CompiledV1{}, "", fmt.Errorf(
				"agent: policy cannot be combined with legacy model/system prompt",
			)
		}
		definition = *d.Policy
		definition.Modules = slices.Clone(d.Policy.Modules)
	} else {
		prompt := d.SystemPrompt
		lane := agentpolicy.LaneA2A
		moduleID := agentpolicy.A2AChatModuleIDV1
		if d.OwnerAgent {
			lane = agentpolicy.LaneOwner
			moduleID = agentpolicy.OwnerCoreModuleIDV1
		}
		if prompt == "" {
			prompt = systemPrompt
		}
		definition = agentpolicy.DefinitionV1{
			SchemaVersion: agentpolicy.DefinitionSchemaVersionV1,
			Lane:          lane,
			Modules: []agentpolicy.ModuleV1{{
				ID: moduleID, Version: "v1", Body: prompt,
			}},
			ModelRoute: agentpolicy.ModelRouteV1{
				Provider: "compatibility", Model: d.Model,
				Thinking:        agentpolicy.ThinkingEnabled,
				ReasoningEffort: agentpolicy.ReasoningHigh,
			},
		}
	}
	if d.OwnerAgent && definition.Lane != agentpolicy.LaneOwner {
		return agentpolicy.CompiledV1{}, "", fmt.Errorf("agent: owner Agent requires owner policy")
	}
	if !d.OwnerAgent && d.Policy != nil && definition.Lane != agentpolicy.LaneA2A {
		return agentpolicy.CompiledV1{}, "", fmt.Errorf("agent: non-owner Agent requires a2a policy")
	}

	// Capability notes are ordinary versioned modules selected from the actual
	// authorized surface. A missing tool can therefore never be advertised by
	// the system prompt, and any module drift changes the definition digest.
	if d.Endpoints != nil {
		if _, ok := tools["tool_search"]; ok {
			definition.Modules = append(definition.Modules, agentpolicy.ModuleV1{
				ID:      agentpolicy.EndpointSearchModuleIDV1,
				Version: "v1", Body: endpointSystemNote(d.Endpoints),
			})
		}
	}
	if _, ok := tools["web_search"]; ok {
		definition.Modules = append(definition.Modules, agentpolicy.ModuleV1{
			ID:      agentpolicy.WebResearchModuleIDV1,
			Version: "v1", Body: exaAdHocAgentFirstSystemNote,
		})
	}

	catalog := agentpolicy.ToolCatalogV1{
		SchemaVersion: agentpolicy.ToolCatalogSchemaVersionV1,
		Tools:         make([]agentpolicy.ToolV1, 0, len(tools)),
	}
	if d.Endpoints != nil && d.Endpoints.catalog != nil {
		catalog.DeferredCatalogDigest = d.Endpoints.catalog.Digest()
	}
	for _, spec := range tools {
		catalog.Tools = append(catalog.Tools, agentpolicy.ToolV1{
			Name: spec.Name(), Description: spec.Description(),
			Parameters: slices.Clone(spec.Parameters()),
			Policy: agentpolicy.ToolPolicyV1{
				Effects:                uint16(spec.Policy.Effects),
				Authorization:          uint8(spec.Policy.Authorization),
				Budget:                 uint8(spec.Policy.Budget),
				Retry:                  uint8(spec.Policy.Retry),
				Concurrency:            uint8(spec.Policy.Concurrency),
				Exposure:               uint8(spec.Policy.Exposure),
				Intents:                uint16(spec.Policy.Intents),
				ResultTrust:            uint8(spec.Policy.ResultTrust),
				RoutingConfigured:      spec.Policy.RoutingConfigured,
				DirectOnExplicitIntent: spec.Policy.DirectOnExplicitIntent,
			},
		})
	}
	compiled, err := agentpolicy.CompileV1(definition, catalog)
	if err != nil {
		return agentpolicy.CompiledV1{}, "", fmt.Errorf("agent: compile interactive policy: %w", err)
	}
	effort := llm.ReasoningEffort(compiled.Definition.ModelRoute.ReasoningEffort)
	return compiled, effort, nil
}

// PolicyManifest returns the non-secret immutable manifest selected at Loop
// construction. It is the durable join key for later observation/evaluation
// work; prompt bodies and user content are intentionally absent.
func (l *Loop) PolicyManifest() agentpolicy.ManifestV1 {
	if l == nil {
		return agentpolicy.ManifestV1{}
	}
	manifest := l.policy.Manifest
	manifest.ModuleRefs = slices.Clone(manifest.ModuleRefs)
	return manifest
}
