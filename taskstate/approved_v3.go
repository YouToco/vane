package taskstate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/types"
	"github.com/robfig/cron"
)

const ApprovedDefinitionSchemaVersionV3 = "vane.task-approved-definition/v3"

type NotificationThresholdV3 string

const (
	NotificationThresholdMajorV3     NotificationThresholdV3 = "major_updates_only"
	NotificationThresholdQualifiedV3 NotificationThresholdV3 = "all_qualified_updates"
)

type NotificationPolicyV3 struct {
	MinimumSignificance NotificationThresholdV3 `json:"minimum_significance"`
	SuppressEmpty       bool                    `json:"suppress_empty"`
}

type OutputLanguageV3 string

const (
	OutputLanguageAutoV3 OutputLanguageV3 = "auto"
	OutputLanguageZhCNV3 OutputLanguageV3 = "zh-CN"
	OutputLanguageEnV3   OutputLanguageV3 = "en"
)

type OutputFormatV3 string

const (
	OutputFormatExecutiveBriefV3 OutputFormatV3 = "executive_brief"
	OutputFormatConciseBriefV3   OutputFormatV3 = "concise_brief"
)

type OutputPreferenceV3 struct {
	Language             OutputLanguageV3 `json:"language"`
	Format               OutputFormatV3   `json:"format"`
	Instructions         string           `json:"instructions"`
	IncludeEvidenceLinks bool             `json:"include_evidence_links"`
}

// ApprovedDefinitionV3 freezes only the owner's durable research intent and
// policies. The executable research plan is created independently for every
// run from this manual and the frozen runtime capability catalog.
type ApprovedDefinitionV3 struct {
	SchemaVersion      string               `json:"schema_version"`
	TenantID           int64                `json:"tenant_id"`
	UserID             int64                `json:"user_id"`
	TaskID             string               `json:"task_id"`
	TaskName           string               `json:"task_name"`
	TaskManual         string               `json:"task_manual"`
	SpecJSON           json.RawMessage      `json:"spec_json"`
	ExecutionMode      types.ExecutionMode  `json:"execution_mode"`
	Notification       NotificationPolicyV3 `json:"notification"`
	Output             OutputPreferenceV3   `json:"output"`
	PlannerBudget      types.PlannerBudget  `json:"planner_budget"`
	DeliveryPolicy     DeliveryPolicy       `json:"delivery_policy"`
	TenantBudgetPolicy BudgetPolicy         `json:"tenant_budget_policy"`
}

type ApprovedDefinitionInputV3 struct {
	TenantID           int64
	UserID             int64
	TaskID             string
	TaskName           string
	TaskManual         string
	SpecJSON           json.RawMessage
	ExecutionMode      types.ExecutionMode
	Notification       NotificationPolicyV3
	Output             OutputPreferenceV3
	PlannerBudget      types.PlannerBudget
	DeliveryPolicy     DeliveryPolicy
	TenantBudgetPolicy BudgetPolicy
}

type approvedDefinitionV3Wire ApprovedDefinitionV3

// scheduleSpecV3Wire is the retained allowlist for the only structured value
// inside a V3 definition. Keeping this wire local prevents spec_json from
// becoming an untyped escape hatch for retired execution state.
type scheduleSpecV3Wire struct {
	Cron         string `json:"cron,omitempty"`
	EverySeconds int    `json:"every_seconds,omitempty"`
	AnchorAt     string `json:"anchor_at,omitempty"`
	TZ           string `json:"tz,omitempty"`
}

func BuildApprovedDefinitionV3(input ApprovedDefinitionInputV3) (ApprovedDefinitionV3, error) {
	definition, err := normalizeApprovedDefinitionV3(ApprovedDefinitionV3{
		SchemaVersion: ApprovedDefinitionSchemaVersionV3,
		TenantID:      input.TenantID, UserID: input.UserID, TaskID: input.TaskID,
		TaskName: input.TaskName, TaskManual: input.TaskManual,
		SpecJSON: input.SpecJSON, ExecutionMode: input.ExecutionMode,
		Notification: input.Notification, Output: input.Output,
		PlannerBudget: input.PlannerBudget, DeliveryPolicy: input.DeliveryPolicy,
		TenantBudgetPolicy: input.TenantBudgetPolicy,
	})
	if err != nil {
		return ApprovedDefinitionV3{}, err
	}
	return definition, nil
}

func (d ApprovedDefinitionV3) Validate() error {
	_, err := normalizeApprovedDefinitionV3(d)
	return err
}

func (d ApprovedDefinitionV3) MarshalJSON() ([]byte, error) {
	normalized, err := normalizeApprovedDefinitionV3(d)
	if err != nil {
		return nil, err
	}
	return marshalBounded(approvedDefinitionV3Wire(normalized), maxDefinitionBytes,
		"approved definition v3")
}

func (d *ApprovedDefinitionV3) UnmarshalJSON(payload []byte) error {
	if d == nil || len(payload) == 0 || len(payload) > maxDefinitionBytes {
		return invalidState("approved definition v3 json size is invalid")
	}
	var wire approvedDefinitionV3Wire
	if err := strictjson.DecodeExact(payload, &wire); err != nil {
		return invalidState("approved definition v3 json is invalid")
	}
	normalized, err := normalizeApprovedDefinitionV3(ApprovedDefinitionV3(wire))
	if err != nil {
		return err
	}
	*d = normalized
	return nil
}

func EncodeApprovedDefinitionV3(definition ApprovedDefinitionV3) ([]byte, error) {
	return json.Marshal(definition)
}

func DecodeApprovedDefinitionV3(payload []byte) (ApprovedDefinitionV3, error) {
	if len(payload) == 0 || len(payload) > maxDefinitionBytes {
		return ApprovedDefinitionV3{}, invalidState("approved definition v3 json size is invalid")
	}
	var definition ApprovedDefinitionV3
	if err := json.Unmarshal(payload, &definition); err != nil {
		return ApprovedDefinitionV3{}, invalidState("approved definition v3 json is invalid")
	}
	return definition, nil
}

func DigestApprovedDefinitionV3(definition ApprovedDefinitionV3) (string, error) {
	canonical, err := EncodeApprovedDefinitionV3(definition)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func normalizeApprovedDefinitionV3(
	definition ApprovedDefinitionV3,
) (ApprovedDefinitionV3, error) {
	if definition.SchemaVersion != ApprovedDefinitionSchemaVersionV3 {
		return ApprovedDefinitionV3{}, invalidState("approved definition v3 schema version is unsupported")
	}
	if definition.TenantID <= 0 || definition.UserID <= 0 ||
		!validIdentifier(definition.TaskID, maxTaskIDBytes) {
		return ApprovedDefinitionV3{}, invalidState("approved definition v3 identity is invalid")
	}
	if !validSingleLineText(definition.TaskName, maxDescriptionBytes, false) ||
		!validMultilineText(definition.TaskManual, maxPlaybookBytes, false) {
		return ApprovedDefinitionV3{}, invalidState("approved definition v3 text is invalid")
	}
	var err error
	definition.SpecJSON, err = canonicalScheduleSpecV3(definition.SpecJSON)
	if err != nil {
		return ApprovedDefinitionV3{}, err
	}
	if definition.ExecutionMode != types.ExecutionModeDiscoverAtRun ||
		definition.DeliveryPolicy != DeliveryPolicyOwnerFeishu ||
		definition.TenantBudgetPolicy != BudgetPolicyInheritTenantQuota {
		return ApprovedDefinitionV3{}, invalidState("approved definition v3 policy is unsupported")
	}
	if !validNotificationPolicyV3(definition.Notification) ||
		!validOutputPreferenceV3(definition.Output) {
		return ApprovedDefinitionV3{}, invalidState("approved definition v3 presentation policy is invalid")
	}
	if err := definition.PlannerBudget.ValidateForMode(definition.ExecutionMode); err != nil {
		return ApprovedDefinitionV3{}, invalidState("approved definition v3 planner budget is invalid")
	}
	if _, err := marshalBounded(approvedDefinitionV3Wire(definition),
		maxDefinitionBytes, "approved definition v3"); err != nil {
		return ApprovedDefinitionV3{}, err
	}
	return definition, nil
}

func canonicalScheduleSpecV3(payload json.RawMessage) (json.RawMessage, error) {
	if len(payload) == 0 || len(payload) > maxDefinitionBytes {
		return nil, invalidState("approved definition v3 schedule spec is invalid")
	}
	var spec scheduleSpecV3Wire
	if err := strictjson.DecodeExact(payload, &spec); err != nil {
		return nil, invalidState("approved definition v3 schedule spec is invalid")
	}
	if strings.TrimSpace(spec.Cron) != spec.Cron || strings.TrimSpace(spec.TZ) != spec.TZ ||
		strings.TrimSpace(spec.AnchorAt) != spec.AnchorAt || len(spec.Cron) > 256 ||
		len(spec.TZ) > 128 || len(spec.AnchorAt) > 64 || spec.EverySeconds < 0 {
		return nil, invalidState("approved definition v3 schedule spec is invalid")
	}
	hasCron, hasEvery := spec.Cron != "", spec.EverySeconds > 0
	if hasCron == hasEvery {
		return nil, invalidState("approved definition v3 schedule requires exactly one timing mode")
	}
	zone := spec.TZ
	if zone == "" {
		zone = "Asia/Shanghai"
	}
	if _, err := time.LoadLocation(zone); err != nil {
		return nil, invalidState("approved definition v3 schedule time zone is invalid")
	}
	if hasEvery {
		if spec.EverySeconds < 3600 ||
			int64(spec.EverySeconds) > int64((time.Duration(1<<63-1))/time.Second) {
			return nil, invalidState("approved definition v3 schedule interval is invalid")
		}
		if spec.AnchorAt != "" {
			anchor, err := time.Parse(time.RFC3339, spec.AnchorAt)
			if err != nil || anchor.Nanosecond() != 0 {
				return nil, invalidState("approved definition v3 schedule anchor is invalid")
			}
		}
	} else {
		if spec.AnchorAt != "" {
			return nil, invalidState("approved definition v3 cron cannot have an anchor")
		}
		fields := strings.Fields(spec.Cron)
		if len(fields) != 5 || strings.ContainsAny(fields[0], "*/,-") {
			return nil, invalidState("approved definition v3 cron is invalid")
		}
		minute, err := strconv.Atoi(fields[0])
		if err != nil || minute < 0 || minute > 59 {
			return nil, invalidState("approved definition v3 cron is invalid")
		}
		if _, err := cron.ParseStandard(spec.Cron); err != nil {
			return nil, invalidState("approved definition v3 cron is invalid")
		}
	}
	canonical, err := json.Marshal(spec)
	if err != nil {
		return nil, invalidState("approved definition v3 schedule spec is invalid")
	}
	return canonical, nil
}

func validNotificationPolicyV3(policy NotificationPolicyV3) bool {
	if !policy.SuppressEmpty {
		return false
	}
	switch policy.MinimumSignificance {
	case NotificationThresholdMajorV3, NotificationThresholdQualifiedV3:
		return true
	default:
		return false
	}
}

func validOutputPreferenceV3(preference OutputPreferenceV3) bool {
	if !validMultilineText(preference.Instructions, maxDescriptionBytes, true) {
		return false
	}
	switch preference.Language {
	case OutputLanguageAutoV3, OutputLanguageZhCNV3, OutputLanguageEnV3:
	default:
		return false
	}
	switch preference.Format {
	case OutputFormatExecutiveBriefV3, OutputFormatConciseBriefV3:
		return true
	default:
		return false
	}
}
