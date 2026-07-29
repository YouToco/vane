package runcontext

import (
	"reflect"

	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

// ToolBindingV1 freezes the selected runtime-policy route for one exact
// approved invocation. CapabilityV1 includes the implementation revision and
// opaque credential generation references, never credential values.
type ToolBindingV1 struct {
	InvocationDigest string                     `json:"invocation_digest"`
	Contract         ToolContractBindingV1      `json:"contract"`
	Capability       runtimepolicy.CapabilityV1 `json:"capability"`
}

// ToolContractBindingV1 freezes the logical contract selected by the current
// write registry so replay never needs that mutable registry.
type ToolContractBindingV1 struct {
	ToolName              string                                     `json:"tool_name"`
	ToolContractVersion   string                                     `json:"tool_contract_version"`
	Platform              string                                     `json:"platform"`
	Capability            string                                     `json:"capability"`
	Kind                  string                                     `json:"kind"`
	ImplementationVersion runtimepolicy.CapabilityImplementationIDV1 `json:"implementation_version"`
}

// CompiledSnapshotV2 is the Source-free Activity-only runtime view. Temporal
// Workflow history carries Ref only.
type CompiledSnapshotV2 struct {
	Ref                            types.RunSnapshotRefV2
	Mode                           types.ExecutionMode
	DefinitionVersion              int64
	AdaptiveVersion                int64
	AdaptiveDigest                 string
	AdaptiveBasisDefinitionVersion int64
	AdaptiveBasisDefinitionDigest  string
	ObservationRollout             observation.RolloutMode
	Budget                         types.PlannerBudget
	Definition                     taskstate.ApprovedDefinitionV2
	Adaptive                       taskstate.AdaptiveStateV2
	ToolBindings                   []ToolBindingV1
	Policy                         runtimepolicy.BundleV1
}

// ValidateFor verifies the frozen Source-free execution view without
// consulting the current Tool registry. Retired V1 Tool contracts therefore
// remain replayable, while every invocation must still match the policy route
// and adaptive state sealed in this snapshot.
func (s CompiledSnapshotV2) ValidateFor(expected types.RunIdentity) error {
	if s.Ref.ValidateFor(expected) != nil ||
		s.Mode != types.ExecutionModeCompiled ||
		s.Ref.Mode != s.Mode ||
		s.DefinitionVersion <= 0 ||
		s.AdaptiveVersion != s.Ref.AdaptiveVersion ||
		s.AdaptiveBasisDefinitionVersion != s.DefinitionVersion ||
		s.AdaptiveBasisDefinitionDigest != s.Ref.DefinitionDigest ||
		!s.ObservationRollout.Valid() ||
		s.Budget != (types.PlannerBudget{}) ||
		s.Definition.Validate() != nil ||
		s.Adaptive.Validate() != nil ||
		s.Policy.Validate() != nil ||
		s.Definition.TenantID != expected.TenantID ||
		s.Definition.UserID != expected.UserID ||
		s.Definition.TaskID != expected.TaskID ||
		s.Adaptive.TenantID != expected.TenantID ||
		s.Adaptive.UserID != expected.UserID ||
		s.Adaptive.TaskID != expected.TaskID ||
		len(s.Definition.ToolCalls) != len(s.ToolBindings) ||
		len(s.Definition.ToolCalls) != len(s.Adaptive.InvocationStates) {
		return types.NewAppError(types.CodeValidation,
			"compiled Tool snapshot is invalid", nil)
	}
	definitionDigest, err :=
		taskstate.DigestApprovedDefinitionV2(s.Definition)
	if err != nil || definitionDigest != s.Ref.DefinitionDigest {
		return types.NewAppError(types.CodeValidation,
			"compiled Tool snapshot definition differs", nil)
	}
	adaptiveDigest, err := taskstate.DigestAdaptiveStateV2(s.Adaptive)
	if err != nil || adaptiveDigest != s.AdaptiveDigest {
		return types.NewAppError(types.CodeValidation,
			"compiled Tool snapshot adaptive state differs", nil)
	}
	bindings := make(map[string]ToolBindingV1, len(s.ToolBindings))
	for _, binding := range s.ToolBindings {
		if _, duplicate := bindings[binding.InvocationDigest]; duplicate {
			return types.NewAppError(types.CodeValidation,
				"compiled Tool snapshot binding is duplicated", nil)
		}
		bindings[binding.InvocationDigest] = binding
	}
	states := make(map[string]struct{}, len(s.Adaptive.InvocationStates))
	for _, state := range s.Adaptive.InvocationStates {
		states[state.InvocationDigest] = struct{}{}
	}
	for _, call := range s.Definition.ToolCalls {
		binding, ok := bindings[call.Digest]
		if !ok ||
			binding.Contract.ToolName != call.ToolName ||
			binding.Contract.ToolContractVersion !=
				call.ToolContractVersion ||
			binding.Contract.Platform != binding.Capability.Platform ||
			binding.Contract.Capability != binding.Capability.Capability ||
			binding.Contract.Kind != binding.Capability.Kind ||
			binding.Contract.ImplementationVersion !=
				binding.Capability.ImplementationVersion {
			return types.NewAppError(types.CodeValidation,
				"compiled Tool snapshot binding differs", nil)
		}
		if _, ok := states[call.Digest]; !ok ||
			!capabilityFrozenInPolicy(
				binding.Capability, s.Policy.CapabilityCatalog.Allowed) {
			return types.NewAppError(types.CodeValidation,
				"compiled Tool snapshot route is not frozen", nil)
		}
	}
	seal, err := SealCompiledSnapshotV2(s)
	if err != nil ||
		seal.DefinitionDigest != s.Ref.DefinitionDigest ||
		seal.AdaptiveDigest != s.AdaptiveDigest ||
		seal.PlanDigest != s.Ref.PlanDigest ||
		seal.PolicyDigests != s.Ref.Policy ||
		seal.PayloadDigest != s.Ref.PayloadDigest {
		return types.NewAppError(types.CodeValidation,
			"compiled Tool snapshot seal differs", nil)
	}
	return nil
}

func capabilityFrozenInPolicy(
	want runtimepolicy.CapabilityV1,
	allowed []runtimepolicy.CapabilityV1,
) bool {
	for _, capability := range allowed {
		if reflect.DeepEqual(capability, want) {
			return true
		}
	}
	return false
}
