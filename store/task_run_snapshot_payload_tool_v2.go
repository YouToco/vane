package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/YouToco/vane/acquisitiontool"
	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

const (
	taskRunSnapshotPayloadSchemaV2 = "vane.task-run-snapshot-payload/v2"
	taskRunToolPlanDigestVersionV1 = "vane.task-run-tool-plan/v1"
)

type taskRunSnapshotPayloadV2 struct {
	SchemaVersion                  string                         `json:"schema_version"`
	TenantID                       int64                          `json:"tenant_id"`
	UserID                         int64                          `json:"user_id"`
	TaskID                         string                         `json:"task_id"`
	RunKind                        types.RunSnapshotKind          `json:"run_kind"`
	Mode                           types.ExecutionMode            `json:"mode"`
	DefinitionVersion              int64                          `json:"definition_version"`
	DefinitionDigest               string                         `json:"definition_digest"`
	AdaptiveVersion                int64                          `json:"adaptive_version"`
	AdaptiveDigest                 string                         `json:"adaptive_digest"`
	AdaptiveBasisDefinitionVersion int64                          `json:"adaptive_basis_definition_version"`
	AdaptiveBasisDefinitionDigest  string                         `json:"adaptive_basis_definition_digest"`
	ObservationRollout             observation.RolloutMode        `json:"observation_rollout"`
	Policies                       taskRunPolicyPayloads          `json:"policies"`
	Budget                         taskRunBudget                  `json:"budget"`
	Definition                     taskstate.ApprovedDefinitionV2 `json:"definition"`
	Adaptive                       taskstate.AdaptiveStateV2      `json:"adaptive"`
	ToolBindings                   []runcontext.ToolBindingV1     `json:"tool_bindings"`
	ReferenceSchemaVersion         string                         `json:"reference_schema_version"`
}

type taskRunToolPlanDigestEnvelopeV1 struct {
	Version      string                       `json:"version"`
	ToolCalls    []taskstate.ToolInvocationV1 `json:"tool_calls"`
	ToolBindings []runcontext.ToolBindingV1   `json:"tool_bindings"`
}

type taskRunSnapshotPayloadV2Read struct {
	Payload       taskRunSnapshotPayloadV2
	Canonical     []byte
	PlanDigest    string
	PolicyDigests taskRunPolicyDigestSet
	Policy        runtimepolicy.BundleV1
}

func buildTaskRunToolBindingsV1(
	definition taskstate.ApprovedDefinitionV2,
	policy runtimepolicy.BundleV1,
) ([]runcontext.ToolBindingV1, error) {
	byCapability := make(map[string]runtimepolicy.CapabilityV1,
		len(policy.CapabilityCatalog.Allowed))
	for _, capability := range policy.CapabilityCatalog.Allowed {
		key := capability.Platform + "/" + capability.Capability
		if _, duplicate := byCapability[key]; duplicate {
			return nil, errors.New("runtime policy capability is duplicated")
		}
		byCapability[key] = capability
	}
	bindings := make([]runcontext.ToolBindingV1, 0, len(definition.ToolCalls))
	for _, call := range definition.ToolCalls {
		if call.ToolContractVersion != "v1" {
			return nil, errors.New("tool contract version is unsupported")
		}
		contract, ok := acquisitiontool.LookupToolContractV1(call.ToolName)
		if !ok {
			return nil, errors.New("tool contract is unavailable")
		}
		capability, ok := byCapability[string(contract.Platform)+"/"+string(contract.Capability)]
		if !ok {
			return nil, errors.New("runtime policy does not bind approved Tool call")
		}
		if capability.Kind != string(contract.Kind) ||
			capability.ImplementationVersion != contract.ImplementationVersion {
			return nil, errors.New(
				"runtime policy binds an incompatible Tool implementation")
		}
		bindings = append(bindings, runcontext.ToolBindingV1{
			InvocationDigest: call.Digest,
			Contract: runcontext.ToolContractBindingV1{
				ToolName: call.ToolName, ToolContractVersion: call.ToolContractVersion,
				Platform: string(contract.Platform), Capability: string(contract.Capability),
				Kind:                  string(contract.Kind),
				ImplementationVersion: contract.ImplementationVersion,
			},
			Capability: capability,
		})
	}
	return bindings, nil
}

func digestTaskRunToolPlanV1(
	calls []taskstate.ToolInvocationV1,
	bindings []runcontext.ToolBindingV1,
) (string, error) {
	payload, err := json.Marshal(taskRunToolPlanDigestEnvelopeV1{
		Version:   taskRunToolPlanDigestVersionV1,
		ToolCalls: calls, ToolBindings: bindings,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func encodeTaskRunSnapshotPayloadV2(
	payload taskRunSnapshotPayloadV2,
) (taskRunSnapshotPayloadV2Read, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return taskRunSnapshotPayloadV2Read{}, err
	}
	return readTaskRunSnapshotPayloadV2(raw)
}

func readTaskRunSnapshotPayloadV2(
	raw []byte,
) (taskRunSnapshotPayloadV2Read, error) {
	if len(raw) == 0 || len(raw) > maxTaskRunPayloadBytes ||
		strictjson.Validate(raw) != nil {
		return taskRunSnapshotPayloadV2Read{}, errors.New("task run v2 payload is invalid")
	}
	var payload taskRunSnapshotPayloadV2
	if err := strictjson.DecodeExact(raw, &payload); err != nil {
		return taskRunSnapshotPayloadV2Read{}, errors.New("task run v2 payload is invalid")
	}
	if payload.SchemaVersion != taskRunSnapshotPayloadSchemaV2 ||
		payload.TenantID <= 0 || payload.UserID <= 0 ||
		!validTaskRunTaskID(payload.TaskID) ||
		payload.RunKind != types.RunSnapshotKindScheduled ||
		payload.Mode != types.ExecutionModeCompiled ||
		payload.DefinitionVersion <= 0 || payload.AdaptiveVersion <= 0 ||
		payload.AdaptiveBasisDefinitionVersion <= 0 ||
		!validTaskStateDigest(payload.DefinitionDigest) ||
		!validTaskStateDigest(payload.AdaptiveDigest) ||
		!validTaskStateDigest(payload.AdaptiveBasisDefinitionDigest) ||
		payload.AdaptiveBasisDefinitionVersion != payload.DefinitionVersion ||
		!constantTimeTaskStateDigestEqual(
			payload.AdaptiveBasisDefinitionDigest, payload.DefinitionDigest) ||
		payload.ReferenceSchemaVersion != types.RunSnapshotSchemaVersionV2 ||
		payload.Budget != (taskRunBudget{}) ||
		!payload.ObservationRollout.Valid() {
		return taskRunSnapshotPayloadV2Read{}, errors.New("task run v2 envelope is invalid")
	}
	if payload.Definition.TenantID != payload.TenantID ||
		payload.Definition.UserID != payload.UserID ||
		payload.Definition.TaskID != payload.TaskID ||
		payload.Definition.ExecutionMode != payload.Mode {
		return taskRunSnapshotPayloadV2Read{}, errors.New("task run v2 definition scope differs")
	}
	definitionDigest, err := taskstate.DigestApprovedDefinitionV2(payload.Definition)
	if err != nil || !constantTimeTaskStateDigestEqual(
		definitionDigest, payload.DefinitionDigest) {
		return taskRunSnapshotPayloadV2Read{}, errors.New("task run v2 definition digest differs")
	}
	if payload.Adaptive.TenantID != payload.TenantID ||
		payload.Adaptive.UserID != payload.UserID ||
		payload.Adaptive.TaskID != payload.TaskID {
		return taskRunSnapshotPayloadV2Read{}, errors.New("task run v2 adaptive scope differs")
	}
	adaptiveDigest, err := taskstate.DigestAdaptiveStateV2(payload.Adaptive)
	if err != nil || !constantTimeTaskStateDigestEqual(
		adaptiveDigest, payload.AdaptiveDigest) {
		return taskRunSnapshotPayloadV2Read{}, errors.New("task run v2 adaptive digest differs")
	}
	if !adaptiveInvocationSetMatchesDefinitionV2(
		payload.Adaptive, payload.Definition) {
		return taskRunSnapshotPayloadV2Read{}, errors.New("task run v2 adaptive invocation set differs")
	}
	policy, policyDigests, err := decodeTaskRunPolicyBundleV1(&payload.Policies)
	if err != nil {
		return taskRunSnapshotPayloadV2Read{}, err
	}
	if !validFrozenTaskRunToolBindingsV1(
		payload.Definition, payload.ToolBindings, policy) {
		return taskRunSnapshotPayloadV2Read{}, errors.New("task run v2 Tool bindings differ")
	}
	planDigest, err := digestTaskRunToolPlanV1(
		payload.Definition.ToolCalls, payload.ToolBindings)
	if err != nil {
		return taskRunSnapshotPayloadV2Read{}, err
	}
	canonical, err := json.Marshal(payload)
	if err != nil || !bytes.Equal(canonical, raw) {
		return taskRunSnapshotPayloadV2Read{}, errors.New("task run v2 payload is not canonical")
	}
	return taskRunSnapshotPayloadV2Read{
		Payload: payload, Canonical: canonical, PlanDigest: planDigest,
		PolicyDigests: policyDigests, Policy: policy,
	}, nil
}

// validFrozenTaskRunToolBindingsV1 deliberately does not consult the current
// acquisition Tool registry. The write path selected and froze each logical
// Tool-to-capability route; a later binary must still replay that immutable
// route after a Tool is retired from the current-write registry.
func validFrozenTaskRunToolBindingsV1(
	definition taskstate.ApprovedDefinitionV2,
	bindings []runcontext.ToolBindingV1,
	policy runtimepolicy.BundleV1,
) bool {
	if len(bindings) != len(definition.ToolCalls) {
		return false
	}
	allowed := make(map[string]runtimepolicy.CapabilityV1,
		len(policy.CapabilityCatalog.Allowed))
	for _, capability := range policy.CapabilityCatalog.Allowed {
		key := capability.Platform + "\x00" + capability.Capability
		if _, duplicate := allowed[key]; duplicate {
			return false
		}
		allowed[key] = capability
	}
	for i, binding := range bindings {
		call := definition.ToolCalls[i]
		if binding.InvocationDigest != call.Digest ||
			binding.Contract.ToolName != call.ToolName ||
			binding.Contract.ToolContractVersion != call.ToolContractVersion ||
			binding.Contract.Platform != binding.Capability.Platform ||
			binding.Contract.Capability != binding.Capability.Capability ||
			binding.Contract.Kind != binding.Capability.Kind ||
			binding.Contract.ImplementationVersion !=
				binding.Capability.ImplementationVersion {
			return false
		}
		key := binding.Capability.Platform + "\x00" +
			binding.Capability.Capability
		frozen, ok := allowed[key]
		if !ok {
			return false
		}
		want, err := json.Marshal(frozen)
		if err != nil {
			return false
		}
		got, err := json.Marshal(binding.Capability)
		if err != nil || !bytes.Equal(want, got) {
			return false
		}
	}
	return true
}

func decodeTaskRunPolicyBundleV1(
	policies *taskRunPolicyPayloads,
) (runtimepolicy.BundleV1, taskRunPolicyDigestSet, error) {
	if policies == nil {
		return runtimepolicy.BundleV1{}, taskRunPolicyDigestSet{},
			errors.New("task run v2 policies are missing")
	}
	digests, err := canonicalizeTaskRunPolicyPayloads(policies)
	if err != nil {
		return runtimepolicy.BundleV1{}, taskRunPolicyDigestSet{}, err
	}
	capability, err := runtimepolicy.DecodeCapabilityCatalogV1(
		policies.CapabilityCatalog)
	if err != nil {
		return runtimepolicy.BundleV1{}, taskRunPolicyDigestSet{}, err
	}
	tools, err := runtimepolicy.DecodeToolPolicyV1(policies.ToolPolicy)
	if err != nil {
		return runtimepolicy.BundleV1{}, taskRunPolicyDigestSet{}, err
	}
	prompts, err := runtimepolicy.DecodePromptPolicyV1(policies.PromptPolicy)
	if err != nil {
		return runtimepolicy.BundleV1{}, taskRunPolicyDigestSet{}, err
	}
	models, err := runtimepolicy.DecodeModelPolicyV1(policies.ModelPolicy)
	if err != nil {
		return runtimepolicy.BundleV1{}, taskRunPolicyDigestSet{}, err
	}
	quotas, err := runtimepolicy.DecodeQuotaPolicyV1(policies.QuotaPolicy)
	if err != nil {
		return runtimepolicy.BundleV1{}, taskRunPolicyDigestSet{}, err
	}
	if !sameRuntimePolicyV1(policies.CapabilityCatalog,
		runtimepolicy.EncodeCapabilityCatalogV1, capability) ||
		!sameRuntimePolicyV1(policies.ToolPolicy,
			runtimepolicy.EncodeToolPolicyV1, tools) ||
		!sameRuntimePolicyV1(policies.PromptPolicy,
			runtimepolicy.EncodePromptPolicyV1, prompts) ||
		!sameRuntimePolicyV1(policies.ModelPolicy,
			runtimepolicy.EncodeModelPolicyV1, models) ||
		!sameRuntimePolicyV1(policies.QuotaPolicy,
			runtimepolicy.EncodeQuotaPolicyV1, quotas) {
		return runtimepolicy.BundleV1{}, taskRunPolicyDigestSet{},
			errors.New("task run v2 policies are not canonical")
	}
	return runtimepolicy.BundleV1{
		SchemaVersion:     runtimepolicy.BundleSchemaVersionV1,
		CapabilityCatalog: capability, ToolPolicy: tools,
		PromptPolicy: prompts, ModelPolicy: models, QuotaPolicy: quotas,
	}, digests, nil
}

func adaptiveInvocationSetMatchesDefinitionV2(
	adaptive taskstate.AdaptiveStateV2,
	definition taskstate.ApprovedDefinitionV2,
) bool {
	if len(adaptive.InvocationStates) != len(definition.ToolCalls) {
		return false
	}
	allowed := make(map[string]struct{}, len(definition.ToolCalls))
	for _, call := range definition.ToolCalls {
		allowed[call.Digest] = struct{}{}
	}
	for _, state := range adaptive.InvocationStates {
		if _, ok := allowed[state.InvocationDigest]; !ok {
			return false
		}
	}
	return true
}
