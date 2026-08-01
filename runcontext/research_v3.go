package runcontext

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

const (
	ResearchSnapshotPayloadSchemaV3 = "vane.research-run-snapshot-payload/v3"
	ResearchExecutionPlanSchemaV3   = "vane.research-execution-plan/v3"
)

const (
	maxResearchPlanBytes       = 256 << 10
	maxResearchPlanSteps       = 16
	maxResearchStepArguments   = 64 << 10
	maxResearchInvocationBytes = 255
)

type ResearchPlanStepV3 struct {
	InvocationID string          `json:"invocation_id"`
	ToolName     string          `json:"tool_name"`
	Arguments    json.RawMessage `json:"arguments"`
}

type ResearchExecutionPlanV3 struct {
	SchemaVersion           string               `json:"schema_version"`
	DefinitionDigest        string               `json:"definition_digest"`
	CapabilityCatalogDigest string               `json:"capability_catalog_digest"`
	Steps                   []ResearchPlanStepV3 `json:"steps"`
}

type ResearchSnapshotV3 struct {
	Identity          types.RunIdentity
	DefinitionVersion int64
	HistoryThroughUTC string
	Definition        taskstate.ApprovedDefinitionV3
	Policy            runtimepolicy.BundleV1
}

type ResearchSnapshotPayloadV3 struct {
	SchemaVersion          string                         `json:"schema_version"`
	TenantID               int64                          `json:"tenant_id"`
	UserID                 int64                          `json:"user_id"`
	TaskID                 string                         `json:"task_id"`
	TemporalWorkflowID     string                         `json:"temporal_workflow_id"`
	TemporalRunID          string                         `json:"temporal_run_id"`
	RunKind                types.RunSnapshotKind          `json:"run_kind"`
	Mode                   types.ExecutionMode            `json:"mode"`
	DefinitionVersion      int64                          `json:"definition_version"`
	DefinitionDigest       string                         `json:"definition_digest"`
	HistoryThroughUTC      string                         `json:"history_through_utc"`
	Policies               PolicyPayloadsV1               `json:"policies"`
	PlannerBudget          types.PlannerBudget            `json:"planner_budget"`
	Definition             taskstate.ApprovedDefinitionV3 `json:"definition"`
	ReferenceSchemaVersion string                         `json:"reference_schema_version"`
}

type ResearchSnapshotSealV3 struct {
	CanonicalPayload []byte
	DefinitionDigest string
	PolicyDigests    types.RuntimePolicyDigests
	PayloadDigest    string
	Payload          ResearchSnapshotPayloadV3
	Policy           runtimepolicy.BundleV1
}

func SealResearchSnapshotV3(snapshot ResearchSnapshotV3) (ResearchSnapshotSealV3, error) {
	identity := snapshot.Identity
	if identity.RunKind != types.RunSnapshotKindScheduled || identity.TenantID <= 0 ||
		identity.UserID <= 0 || identity.TaskID == "" || identity.TemporalWorkflowID == "" ||
		identity.TemporalRunID == "" || snapshot.DefinitionVersion <= 0 ||
		snapshot.Definition.Validate() != nil ||
		snapshot.Definition.ExecutionMode != types.ExecutionModeDiscoverAtRun ||
		snapshot.Definition.TenantID != identity.TenantID ||
		snapshot.Definition.UserID != identity.UserID ||
		snapshot.Definition.TaskID != identity.TaskID {
		return ResearchSnapshotSealV3{}, invalidResearchPlan("snapshot identity is invalid")
	}
	through, err := time.Parse(time.RFC3339Nano, snapshot.HistoryThroughUTC)
	if err != nil || through.Location() != time.UTC || through.Format(time.RFC3339Nano) != snapshot.HistoryThroughUTC {
		return ResearchSnapshotSealV3{}, invalidResearchPlan("history cutoff is invalid")
	}
	definitionDigest, err := taskstate.DigestApprovedDefinitionV3(snapshot.Definition)
	if err != nil {
		return ResearchSnapshotSealV3{}, err
	}
	policyPayloads, policyDigests, normalizedPolicy, err := EncodePolicyBundleV1(snapshot.Policy)
	if err != nil {
		return ResearchSnapshotSealV3{}, err
	}
	payload := ResearchSnapshotPayloadV3{
		SchemaVersion: ResearchSnapshotPayloadSchemaV3,
		TenantID:      identity.TenantID, UserID: identity.UserID, TaskID: identity.TaskID,
		TemporalWorkflowID: identity.TemporalWorkflowID,
		TemporalRunID:      identity.TemporalRunID, RunKind: identity.RunKind,
		Mode:              types.ExecutionModeDiscoverAtRun,
		DefinitionVersion: snapshot.DefinitionVersion,
		DefinitionDigest:  definitionDigest, HistoryThroughUTC: snapshot.HistoryThroughUTC,
		Policies: policyPayloads, PlannerBudget: snapshot.Definition.PlannerBudget,
		Definition:             snapshot.Definition,
		ReferenceSchemaVersion: types.ResearchRunSnapshotRefSchemaV3,
	}
	canonical, err := json.Marshal(payload)
	if err != nil || len(canonical) == 0 || len(canonical) > 2<<20 {
		return ResearchSnapshotSealV3{}, invalidResearchPlan("snapshot payload size is invalid")
	}
	return ResearchSnapshotSealV3{
		CanonicalPayload: canonical, DefinitionDigest: definitionDigest,
		PolicyDigests: policyDigests, PayloadDigest: researchPayloadDigestV3(canonical),
		Payload: payload, Policy: normalizedPolicy,
	}, nil
}

func DecodeResearchSnapshotPayloadV3(payload []byte) (ResearchSnapshotSealV3, error) {
	if len(payload) == 0 || len(payload) > 2<<20 {
		return ResearchSnapshotSealV3{}, invalidResearchPlan("snapshot payload size is invalid")
	}
	var decoded ResearchSnapshotPayloadV3
	if err := strictjson.DecodeExact(payload, &decoded); err != nil {
		return ResearchSnapshotSealV3{}, invalidResearchPlan("snapshot payload JSON is invalid")
	}
	policy, err := decodePolicyPayloadsV1(decoded.Policies)
	if err != nil {
		return ResearchSnapshotSealV3{}, invalidResearchPlan("snapshot policy is invalid")
	}
	sealed, err := SealResearchSnapshotV3(ResearchSnapshotV3{
		Identity: types.RunIdentity{
			TemporalWorkflowID: decoded.TemporalWorkflowID,
			TemporalRunID:      decoded.TemporalRunID, RunKind: decoded.RunKind,
			TenantID: decoded.TenantID, UserID: decoded.UserID, TaskID: decoded.TaskID,
		},
		DefinitionVersion: decoded.DefinitionVersion,
		HistoryThroughUTC: decoded.HistoryThroughUTC,
		Definition:        decoded.Definition, Policy: policy,
	})
	if err != nil || decoded.SchemaVersion != ResearchSnapshotPayloadSchemaV3 ||
		decoded.Mode != types.ExecutionModeDiscoverAtRun ||
		decoded.ReferenceSchemaVersion != types.ResearchRunSnapshotRefSchemaV3 ||
		decoded.DefinitionDigest != sealed.DefinitionDigest ||
		decoded.PlannerBudget != decoded.Definition.PlannerBudget ||
		!bytes.Equal(payload, sealed.CanonicalPayload) {
		return ResearchSnapshotSealV3{}, invalidResearchPlan("snapshot payload integrity is invalid")
	}
	return sealed, nil
}

func researchPayloadDigestV3(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

type ResearchToolCanonicalizerV3 func(string, json.RawMessage) (json.RawMessage, error)

type researchExecutionPlanWireV3 ResearchExecutionPlanV3

func BuildResearchExecutionPlanV3(
	definitionDigest, capabilityCatalogDigest string,
	steps []ResearchPlanStepV3,
	canonicalize ResearchToolCanonicalizerV3,
) (ResearchExecutionPlanV3, error) {
	if canonicalize == nil {
		return ResearchExecutionPlanV3{}, invalidResearchPlan("Tool canonicalizer is unavailable")
	}
	prepared := make([]ResearchPlanStepV3, len(steps))
	copy(prepared, steps)
	for index := range prepared {
		canonical, err := canonicalize(prepared[index].ToolName, prepared[index].Arguments)
		if err != nil {
			return ResearchExecutionPlanV3{}, invalidResearchPlan("Tool arguments are not approved")
		}
		prepared[index].Arguments = canonical
	}
	plan, err := normalizeResearchExecutionPlanV3(ResearchExecutionPlanV3{
		SchemaVersion:           ResearchExecutionPlanSchemaV3,
		DefinitionDigest:        definitionDigest,
		CapabilityCatalogDigest: capabilityCatalogDigest,
		Steps:                   prepared,
	})
	if err != nil {
		return ResearchExecutionPlanV3{}, err
	}
	return plan, nil
}

func (p ResearchExecutionPlanV3) Validate() error {
	_, err := normalizeResearchExecutionPlanV3(p)
	return err
}

func (p ResearchExecutionPlanV3) MarshalJSON() ([]byte, error) {
	normalized, err := normalizeResearchExecutionPlanV3(p)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(researchExecutionPlanWireV3(normalized))
	if err != nil || len(payload) == 0 || len(payload) > maxResearchPlanBytes {
		return nil, invalidResearchPlan("encoded plan size is invalid")
	}
	return payload, nil
}

func (p *ResearchExecutionPlanV3) UnmarshalJSON(payload []byte) error {
	if p == nil || len(payload) == 0 || len(payload) > maxResearchPlanBytes {
		return invalidResearchPlan("plan JSON size is invalid")
	}
	var wire researchExecutionPlanWireV3
	if err := strictjson.DecodeExact(payload, &wire); err != nil {
		return invalidResearchPlan("plan JSON is invalid")
	}
	normalized, err := normalizeResearchExecutionPlanV3(ResearchExecutionPlanV3(wire))
	if err != nil {
		return err
	}
	*p = normalized
	return nil
}

func EncodeResearchExecutionPlanV3(plan ResearchExecutionPlanV3) ([]byte, error) {
	return json.Marshal(plan)
}

func DecodeResearchExecutionPlanV3(payload []byte) (ResearchExecutionPlanV3, error) {
	var plan ResearchExecutionPlanV3
	if err := json.Unmarshal(payload, &plan); err != nil {
		return ResearchExecutionPlanV3{}, invalidResearchPlan("plan JSON is invalid")
	}
	return plan, nil
}

func DigestResearchExecutionPlanV3(plan ResearchExecutionPlanV3) (string, error) {
	payload, err := EncodeResearchExecutionPlanV3(plan)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func normalizeResearchExecutionPlanV3(
	plan ResearchExecutionPlanV3,
) (ResearchExecutionPlanV3, error) {
	if plan.SchemaVersion != ResearchExecutionPlanSchemaV3 ||
		!validResearchDigest(plan.DefinitionDigest) ||
		!validResearchDigest(plan.CapabilityCatalogDigest) ||
		len(plan.Steps) == 0 || len(plan.Steps) > maxResearchPlanSteps {
		return ResearchExecutionPlanV3{}, invalidResearchPlan("plan envelope is invalid")
	}
	plan.Steps = append([]ResearchPlanStepV3(nil), plan.Steps...)
	seen := make(map[string]struct{}, len(plan.Steps))
	for index := range plan.Steps {
		step := &plan.Steps[index]
		if !validResearchText(step.InvocationID, maxResearchInvocationBytes) ||
			!validResearchText(step.ToolName, maxResearchInvocationBytes) ||
			len(step.Arguments) == 0 || len(step.Arguments) > maxResearchStepArguments {
			return ResearchExecutionPlanV3{}, invalidResearchPlan("plan step is invalid")
		}
		if _, duplicate := seen[step.InvocationID]; duplicate {
			return ResearchExecutionPlanV3{}, invalidResearchPlan("plan invocation is duplicated")
		}
		seen[step.InvocationID] = struct{}{}
		var object map[string]any
		if err := strictjson.Decode(step.Arguments, &object); err != nil || object == nil {
			return ResearchExecutionPlanV3{}, invalidResearchPlan("plan arguments must be a strict JSON object")
		}
		canonical, err := json.Marshal(object)
		if err != nil || !bytes.Equal(canonical, step.Arguments) {
			return ResearchExecutionPlanV3{}, invalidResearchPlan("plan arguments are not canonical")
		}
		step.Arguments = append(json.RawMessage(nil), canonical...)
	}
	return plan, nil
}

func validResearchText(value string, max int) bool {
	return value != "" && strings.TrimSpace(value) == value &&
		len(value) <= max && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

func validResearchDigest(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func invalidResearchPlan(message string) error {
	return types.NewAppError(types.CodeValidation, "research plan 无效: "+message, types.ErrValidation)
}
