package runcontext

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"

	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

const (
	CompiledSnapshotPayloadSchemaV2 = "vane.task-run-snapshot-payload/v2"
	ToolPlanDigestVersionV1         = "vane.task-run-tool-plan/v1"
	runtimePolicyDigestVersionV1    = "vane.runtime-policy-digest/v1"
)

// PolicyPayloadsV1 is the exact policy wire nested in the V2 run payload.
type PolicyPayloadsV1 struct {
	CapabilityCatalog json.RawMessage `json:"capability_catalog"`
	ToolPolicy        json.RawMessage `json:"tool_policy"`
	PromptPolicy      json.RawMessage `json:"prompt_policy"`
	ModelPolicy       json.RawMessage `json:"model_policy"`
	QuotaPolicy       json.RawMessage `json:"quota_policy"`
}

// CompiledSnapshotPayloadV2 is the frozen Source-free payload wire. Store
// aliases this type so persistence and Activity validation cannot drift.
type CompiledSnapshotPayloadV2 struct {
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
	Policies                       PolicyPayloadsV1               `json:"policies"`
	Budget                         types.PlannerBudget            `json:"budget"`
	Definition                     taskstate.ApprovedDefinitionV2 `json:"definition"`
	Adaptive                       taskstate.AdaptiveStateV2      `json:"adaptive"`
	ToolBindings                   []ToolBindingV1                `json:"tool_bindings"`
	ReferenceSchemaVersion         string                         `json:"reference_schema_version"`
}

type toolPlanDigestEnvelopeV1 struct {
	Version      string                       `json:"version"`
	ToolCalls    []taskstate.ToolInvocationV1 `json:"tool_calls"`
	ToolBindings []ToolBindingV1              `json:"tool_bindings"`
}

type runtimePolicyDigestEnvelopeV1 struct {
	Version string          `json:"version"`
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

type CompiledSnapshotSealV2 struct {
	CanonicalPayload []byte
	DefinitionDigest string
	AdaptiveDigest   string
	PlanDigest       string
	PolicyDigests    types.RuntimePolicyDigests
	PayloadDigest    string
}

// EncodePolicyBundleV1 returns the exact canonical policy bytes, their
// reference digests and the normalized typed bundle.
func EncodePolicyBundleV1(
	policy runtimepolicy.BundleV1,
) (PolicyPayloadsV1, types.RuntimePolicyDigests, runtimepolicy.BundleV1, error) {
	if err := policy.Validate(); err != nil {
		return PolicyPayloadsV1{}, types.RuntimePolicyDigests{},
			runtimepolicy.BundleV1{}, err
	}
	payloads := PolicyPayloadsV1{}
	var err error
	payloads.CapabilityCatalog, err =
		runtimepolicy.EncodeCapabilityCatalogV1(policy.CapabilityCatalog)
	if err != nil {
		return PolicyPayloadsV1{}, types.RuntimePolicyDigests{},
			runtimepolicy.BundleV1{}, err
	}
	payloads.ToolPolicy, err =
		runtimepolicy.EncodeToolPolicyV1(policy.ToolPolicy)
	if err != nil {
		return PolicyPayloadsV1{}, types.RuntimePolicyDigests{},
			runtimepolicy.BundleV1{}, err
	}
	payloads.PromptPolicy, err =
		runtimepolicy.EncodePromptPolicyV1(policy.PromptPolicy)
	if err != nil {
		return PolicyPayloadsV1{}, types.RuntimePolicyDigests{},
			runtimepolicy.BundleV1{}, err
	}
	payloads.ModelPolicy, err =
		runtimepolicy.EncodeModelPolicyV1(policy.ModelPolicy)
	if err != nil {
		return PolicyPayloadsV1{}, types.RuntimePolicyDigests{},
			runtimepolicy.BundleV1{}, err
	}
	payloads.QuotaPolicy, err =
		runtimepolicy.EncodeQuotaPolicyV1(policy.QuotaPolicy)
	if err != nil {
		return PolicyPayloadsV1{}, types.RuntimePolicyDigests{},
			runtimepolicy.BundleV1{}, err
	}
	fields := []*json.RawMessage{
		&payloads.CapabilityCatalog,
		&payloads.ToolPolicy,
		&payloads.PromptPolicy,
		&payloads.ModelPolicy,
		&payloads.QuotaPolicy,
	}
	for _, field := range fields {
		*field, err = canonicalJSONObjectV1(*field)
		if err != nil {
			return PolicyPayloadsV1{}, types.RuntimePolicyDigests{},
				runtimepolicy.BundleV1{}, err
		}
	}
	digests, err := digestPolicyPayloadsV1(payloads)
	if err != nil {
		return PolicyPayloadsV1{}, types.RuntimePolicyDigests{},
			runtimepolicy.BundleV1{}, err
	}
	normalized, err := decodePolicyPayloadsV1(payloads)
	if err != nil {
		return PolicyPayloadsV1{}, types.RuntimePolicyDigests{},
			runtimepolicy.BundleV1{}, err
	}
	return payloads, digests, normalized, nil
}

func decodePolicyPayloadsV1(
	payloads PolicyPayloadsV1,
) (runtimepolicy.BundleV1, error) {
	capabilities, err :=
		runtimepolicy.DecodeCapabilityCatalogV1(payloads.CapabilityCatalog)
	if err != nil {
		return runtimepolicy.BundleV1{}, err
	}
	tools, err := runtimepolicy.DecodeToolPolicyV1(payloads.ToolPolicy)
	if err != nil {
		return runtimepolicy.BundleV1{}, err
	}
	prompts, err := runtimepolicy.DecodePromptPolicyV1(payloads.PromptPolicy)
	if err != nil {
		return runtimepolicy.BundleV1{}, err
	}
	models, err := runtimepolicy.DecodeModelPolicyV1(payloads.ModelPolicy)
	if err != nil {
		return runtimepolicy.BundleV1{}, err
	}
	quotas, err := runtimepolicy.DecodeQuotaPolicyV1(payloads.QuotaPolicy)
	if err != nil {
		return runtimepolicy.BundleV1{}, err
	}
	return runtimepolicy.BundleV1{
		SchemaVersion:     runtimepolicy.BundleSchemaVersionV1,
		CapabilityCatalog: capabilities,
		ToolPolicy:        tools, PromptPolicy: prompts,
		ModelPolicy: models, QuotaPolicy: quotas,
	}, nil
}

func digestPolicyPayloadsV1(
	payloads PolicyPayloadsV1,
) (types.RuntimePolicyDigests, error) {
	digest := func(
		kind string,
		payload json.RawMessage,
	) (string, error) {
		return digestCanonicalV1(runtimePolicyDigestEnvelopeV1{
			Version: runtimePolicyDigestVersionV1,
			Kind:    kind, Payload: payload,
		})
	}
	capabilities, err := digest(
		"capability_catalog", payloads.CapabilityCatalog)
	if err != nil {
		return types.RuntimePolicyDigests{}, err
	}
	tools, err := digest("tool_policy", payloads.ToolPolicy)
	if err != nil {
		return types.RuntimePolicyDigests{}, err
	}
	prompts, err := digest("prompt_policy", payloads.PromptPolicy)
	if err != nil {
		return types.RuntimePolicyDigests{}, err
	}
	models, err := digest("model_policy", payloads.ModelPolicy)
	if err != nil {
		return types.RuntimePolicyDigests{}, err
	}
	quotas, err := digest("quota_policy", payloads.QuotaPolicy)
	if err != nil {
		return types.RuntimePolicyDigests{}, err
	}
	return types.RuntimePolicyDigests{
		CapabilityCatalogDigest: capabilities,
		ToolPolicyDigest:        tools,
		PromptPolicyDigest:      prompts,
		ModelPolicyDigest:       models,
		QuotaPolicyDigest:       quotas,
	}, nil
}

// DigestToolPlanV1 binds the logical calls to their exact frozen routes.
func DigestToolPlanV1(
	calls []taskstate.ToolInvocationV1,
	bindings []ToolBindingV1,
) (string, error) {
	return digestCanonicalV1(toolPlanDigestEnvelopeV1{
		Version:   ToolPlanDigestVersionV1,
		ToolCalls: calls, ToolBindings: bindings,
	})
}

// SealCompiledSnapshotV2 deterministically reconstructs every digest carried
// by RunSnapshotRefV2 from the Activity execution view.
func SealCompiledSnapshotV2(
	snapshot CompiledSnapshotV2,
) (CompiledSnapshotSealV2, error) {
	if snapshot.Mode != types.ExecutionModeCompiled ||
		snapshot.DefinitionVersion <= 0 ||
		snapshot.AdaptiveVersion <= 0 ||
		snapshot.AdaptiveBasisDefinitionVersion !=
			snapshot.DefinitionVersion ||
		!snapshot.ObservationRollout.Valid() ||
		snapshot.Budget != (types.PlannerBudget{}) ||
		snapshot.Definition.Validate() != nil ||
		snapshot.Adaptive.Validate() != nil {
		return CompiledSnapshotSealV2{},
			errors.New("compiled Tool snapshot cannot be sealed")
	}
	definitionDigest, err :=
		taskstate.DigestApprovedDefinitionV2(snapshot.Definition)
	if err != nil {
		return CompiledSnapshotSealV2{}, err
	}
	adaptiveDigest, err :=
		taskstate.DigestAdaptiveStateV2(snapshot.Adaptive)
	if err != nil {
		return CompiledSnapshotSealV2{}, err
	}
	policyPayloads, policyDigests, _, err :=
		EncodePolicyBundleV1(snapshot.Policy)
	if err != nil {
		return CompiledSnapshotSealV2{}, err
	}
	planDigest, err := DigestToolPlanV1(
		snapshot.Definition.ToolCalls, snapshot.ToolBindings)
	if err != nil {
		return CompiledSnapshotSealV2{}, err
	}
	payload := CompiledSnapshotPayloadV2{
		SchemaVersion:                  CompiledSnapshotPayloadSchemaV2,
		TenantID:                       snapshot.Definition.TenantID,
		UserID:                         snapshot.Definition.UserID,
		TaskID:                         snapshot.Definition.TaskID,
		RunKind:                        types.RunSnapshotKindScheduled,
		Mode:                           snapshot.Mode,
		DefinitionVersion:              snapshot.DefinitionVersion,
		DefinitionDigest:               definitionDigest,
		AdaptiveVersion:                snapshot.AdaptiveVersion,
		AdaptiveDigest:                 adaptiveDigest,
		AdaptiveBasisDefinitionVersion: snapshot.AdaptiveBasisDefinitionVersion,
		AdaptiveBasisDefinitionDigest:  snapshot.AdaptiveBasisDefinitionDigest,
		ObservationRollout:             snapshot.ObservationRollout,
		Policies:                       policyPayloads, Budget: snapshot.Budget,
		Definition: snapshot.Definition, Adaptive: snapshot.Adaptive,
		ToolBindings:           snapshot.ToolBindings,
		ReferenceSchemaVersion: types.RunSnapshotSchemaVersionV2,
	}
	canonical, err := json.Marshal(payload)
	if err != nil {
		return CompiledSnapshotSealV2{}, err
	}
	sum := sha256.Sum256(canonical)
	return CompiledSnapshotSealV2{
		CanonicalPayload: canonical,
		DefinitionDigest: definitionDigest,
		AdaptiveDigest:   adaptiveDigest,
		PlanDigest:       planDigest,
		PolicyDigests:    policyDigests,
		PayloadDigest:    hex.EncodeToString(sum[:]),
	}, nil
}

func digestCanonicalV1(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", errors.New("compiled Tool value cannot be encoded")
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func canonicalJSONObjectV1(raw json.RawMessage) (json.RawMessage, error) {
	var object map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil || object == nil {
		return nil, errors.New("compiled Tool policy is not a JSON object")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("compiled Tool policy has trailing JSON")
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		return nil, errors.New("compiled Tool policy cannot be canonicalized")
	}
	return canonical, nil
}
