package runcontext

import (
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
	AdaptiveBasisDefinitionVersion int64
	AdaptiveBasisDefinitionDigest  string
	ObservationRollout             observation.RolloutMode
	Budget                         types.PlannerBudget
	Definition                     taskstate.ApprovedDefinitionV2
	Adaptive                       taskstate.AdaptiveStateV2
	ToolBindings                   []ToolBindingV1
	Policy                         runtimepolicy.BundleV1
}
