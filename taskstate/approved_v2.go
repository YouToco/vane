package taskstate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/types"
)

const ApprovedDefinitionSchemaVersionV2 = "vane.task-approved-definition/v2"

// ApprovedDefinitionV2 is the first Source-free approved definition. The task
// manual remains the user-facing contract; ToolCalls are the compiler-owned,
// versioned executable interpretation frozen when that manual is approved.
type ApprovedDefinitionV2 struct {
	SchemaVersion  string               `json:"schema_version"`
	TenantID       int64                `json:"tenant_id"`
	UserID         int64                `json:"user_id"`
	TaskID         string               `json:"task_id"`
	NLDescription  string               `json:"nl_description"`
	SpecJSON       json.RawMessage      `json:"spec_json"`
	ScopeJSON      json.RawMessage      `json:"scope_json"`
	TaskManual     string               `json:"task_manual"`
	Strictness     types.PushStrictness `json:"strictness"`
	ToolCalls      []ToolInvocationV1   `json:"tool_calls"`
	ExecutionMode  types.ExecutionMode  `json:"execution_mode"`
	DeliveryPolicy DeliveryPolicy       `json:"delivery_policy"`
	BudgetPolicy   BudgetPolicy         `json:"budget_policy"`
}

type ApprovedDefinitionInputV2 struct {
	TenantID       int64
	UserID         int64
	TaskID         string
	NLDescription  string
	SpecJSON       json.RawMessage
	ScopeJSON      json.RawMessage
	TaskManual     string
	Strictness     types.PushStrictness
	ToolCalls      []ToolInvocationV1
	ExecutionMode  types.ExecutionMode
	DeliveryPolicy DeliveryPolicy
	BudgetPolicy   BudgetPolicy
}

type approvedDefinitionV2Wire ApprovedDefinitionV2

func BuildApprovedDefinitionV2(input ApprovedDefinitionInputV2) (ApprovedDefinitionV2, error) {
	definition, err := normalizeApprovedDefinitionV2(ApprovedDefinitionV2{
		SchemaVersion: ApprovedDefinitionSchemaVersionV2,
		TenantID:      input.TenantID, UserID: input.UserID, TaskID: input.TaskID,
		NLDescription: input.NLDescription,
		SpecJSON:      input.SpecJSON, ScopeJSON: input.ScopeJSON,
		TaskManual: input.TaskManual, Strictness: input.Strictness,
		ToolCalls: input.ToolCalls, ExecutionMode: input.ExecutionMode,
		DeliveryPolicy: input.DeliveryPolicy, BudgetPolicy: input.BudgetPolicy,
	})
	if err != nil {
		return ApprovedDefinitionV2{}, err
	}
	if err := ValidateApprovedDefinitionV2ForWrite(definition); err != nil {
		return ApprovedDefinitionV2{}, err
	}
	return definition, nil
}

func (d ApprovedDefinitionV2) Validate() error {
	_, err := normalizeApprovedDefinitionV2(d)
	return err
}

// ValidateApprovedDefinitionV2ForWrite applies today's compiler registry
// without changing the frozen V2 reader. Retired tools therefore remain
// readable and replayable even after new definitions can no longer select
// them.
func ValidateApprovedDefinitionV2ForWrite(definition ApprovedDefinitionV2) error {
	normalized, err := normalizeApprovedDefinitionV2(definition)
	if err != nil {
		return err
	}
	for _, call := range normalized.ToolCalls {
		if call.ToolContractVersion != "v1" ||
			!validApprovedAcquisitionToolV2ForWrite(call.ToolName) {
			return invalidState("approved tool call is not writable")
		}
	}
	return nil
}

func (d ApprovedDefinitionV2) MarshalJSON() ([]byte, error) {
	normalized, err := normalizeApprovedDefinitionV2(d)
	if err != nil {
		return nil, err
	}
	return marshalBounded(approvedDefinitionV2Wire(normalized), maxDefinitionBytes,
		"approved definition")
}

func (d *ApprovedDefinitionV2) UnmarshalJSON(payload []byte) error {
	if d == nil || len(payload) == 0 || len(payload) > maxDefinitionBytes {
		return invalidState("approved definition json size is invalid")
	}
	var wire approvedDefinitionV2Wire
	if err := strictjson.DecodeExact(payload, &wire); err != nil {
		return invalidState("approved definition json is invalid")
	}
	normalized, err := normalizeApprovedDefinitionV2(ApprovedDefinitionV2(wire))
	if err != nil {
		return err
	}
	*d = normalized
	return nil
}

func EncodeApprovedDefinitionV2(definition ApprovedDefinitionV2) ([]byte, error) {
	return json.Marshal(definition)
}

func DecodeApprovedDefinitionV2(payload []byte) (ApprovedDefinitionV2, error) {
	if len(payload) == 0 || len(payload) > maxDefinitionBytes {
		return ApprovedDefinitionV2{}, invalidState("approved definition json size is invalid")
	}
	var definition ApprovedDefinitionV2
	if err := json.Unmarshal(payload, &definition); err != nil {
		return ApprovedDefinitionV2{}, invalidState("approved definition json is invalid")
	}
	return definition, nil
}

func DigestApprovedDefinitionV2(definition ApprovedDefinitionV2) (string, error) {
	canonical, err := EncodeApprovedDefinitionV2(definition)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func normalizeApprovedDefinitionV2(
	definition ApprovedDefinitionV2,
) (ApprovedDefinitionV2, error) {
	if definition.SchemaVersion != ApprovedDefinitionSchemaVersionV2 {
		return ApprovedDefinitionV2{}, invalidState("approved definition schema version is unsupported")
	}
	if definition.TenantID <= 0 || definition.UserID <= 0 ||
		!validIdentifier(definition.TaskID, maxTaskIDBytes) {
		return ApprovedDefinitionV2{}, invalidState("approved definition identity is invalid")
	}
	if !validMultilineText(definition.NLDescription, maxDescriptionBytes, false) ||
		!validMultilineText(definition.TaskManual, maxPlaybookBytes, false) {
		return ApprovedDefinitionV2{}, invalidState("approved definition text is invalid")
	}
	if definition.Strictness == "" {
		definition.Strictness = types.PushStrictness("loose")
	}
	if !validStrictnessV1(definition.Strictness) ||
		definition.ExecutionMode != types.ExecutionModeCompiled ||
		definition.DeliveryPolicy != DeliveryPolicyOwnerFeishu ||
		definition.BudgetPolicy != BudgetPolicyInheritTenantQuota {
		return ApprovedDefinitionV2{}, invalidState("approved definition policy is unsupported")
	}
	var err error
	definition.SpecJSON, err = canonicalJSONObject(definition.SpecJSON, "spec")
	if err != nil {
		return ApprovedDefinitionV2{}, err
	}
	definition.ScopeJSON, err = canonicalJSONObject(definition.ScopeJSON, "scope")
	if err != nil {
		return ApprovedDefinitionV2{}, err
	}
	if definition.ToolCalls == nil || len(definition.ToolCalls) == 0 ||
		len(definition.ToolCalls) > maxToolCallCount {
		return ApprovedDefinitionV2{}, invalidState("approved tool call count is invalid")
	}
	definition.ToolCalls = slices.Clone(definition.ToolCalls)
	seen := make(map[string]struct{}, len(definition.ToolCalls))
	for index := range definition.ToolCalls {
		normalized, err := normalizeToolInvocationV1(definition.ToolCalls[index])
		if err != nil {
			return ApprovedDefinitionV2{}, err
		}
		if _, duplicate := seen[normalized.Digest]; duplicate {
			return ApprovedDefinitionV2{}, invalidState("approved tool call is duplicated")
		}
		seen[normalized.Digest] = struct{}{}
		definition.ToolCalls[index] = normalized
	}
	if _, err := marshalBounded(approvedDefinitionV2Wire(definition),
		maxDefinitionBytes, "approved definition"); err != nil {
		return ApprovedDefinitionV2{}, err
	}
	return definition, nil
}

func validApprovedAcquisitionToolV2ForWrite(name string) bool {
	switch name {
	case "web_search", "web_feed", "web_contents", "x_user_posts",
		"xhs_search", "xhs_user_posts", "xhs_hot_list", "xhs_topic_feed",
		"xhs_faved_notes", "weibo_user_posts", "weibo_hot_list",
		"wechat_mp_user_posts":
		return true
	default:
		return false
	}
}
