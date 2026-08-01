package types

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	ResearchRunSnapshotRefSchemaV3 = "vane.research-run-snapshot-ref/v3"
	ResearchRunPlanRefSchemaV3     = "vane.research-run-plan-ref/v3"
)

// ResearchRunSnapshotRefV3 freezes the V3 definition and policy identities
// while keeping all bodies, prompts, model settings and Tool schemas out of
// Temporal history.
type ResearchRunSnapshotRefV3 struct {
	SchemaVersion           string          `json:"schema_version"`
	SnapshotID              int64           `json:"snapshot_id"`
	TemporalWorkflowID      string          `json:"temporal_workflow_id"`
	TemporalRunID           string          `json:"temporal_run_id"`
	RunKind                 RunSnapshotKind `json:"run_kind"`
	TenantID                int64           `json:"tenant_id"`
	UserID                  int64           `json:"user_id"`
	TaskID                  string          `json:"task_id"`
	DefinitionVersion       int64           `json:"definition_version"`
	DefinitionDigest        string          `json:"definition_digest"`
	CapabilityCatalogDigest string          `json:"capability_catalog_digest"`
	ToolPolicyDigest        string          `json:"tool_policy_digest"`
	PromptPolicyDigest      string          `json:"prompt_policy_digest"`
	ModelPolicyDigest       string          `json:"model_policy_digest"`
	QuotaPolicyDigest       string          `json:"quota_policy_digest"`
	PlannerBudget           PlannerBudget   `json:"planner_budget"`
	HistoryThroughUTC       string          `json:"history_through_utc"`
	PayloadDigest           string          `json:"payload_digest"`
	ReferenceDigest         string          `json:"reference_digest"`
}

type researchRunSnapshotRefDigestV3 struct {
	SchemaVersion           string          `json:"schema_version"`
	SnapshotID              int64           `json:"snapshot_id"`
	TemporalWorkflowID      string          `json:"temporal_workflow_id"`
	TemporalRunID           string          `json:"temporal_run_id"`
	RunKind                 RunSnapshotKind `json:"run_kind"`
	TenantID                int64           `json:"tenant_id"`
	UserID                  int64           `json:"user_id"`
	TaskID                  string          `json:"task_id"`
	DefinitionVersion       int64           `json:"definition_version"`
	DefinitionDigest        string          `json:"definition_digest"`
	CapabilityCatalogDigest string          `json:"capability_catalog_digest"`
	ToolPolicyDigest        string          `json:"tool_policy_digest"`
	PromptPolicyDigest      string          `json:"prompt_policy_digest"`
	ModelPolicyDigest       string          `json:"model_policy_digest"`
	QuotaPolicyDigest       string          `json:"quota_policy_digest"`
	PlannerBudget           PlannerBudget   `json:"planner_budget"`
	HistoryThroughUTC       string          `json:"history_through_utc"`
	PayloadDigest           string          `json:"payload_digest"`
}

func SealResearchRunSnapshotRefV3(
	ref ResearchRunSnapshotRefV3,
) (ResearchRunSnapshotRefV3, error) {
	ref.SchemaVersion = ResearchRunSnapshotRefSchemaV3
	ref.ReferenceDigest = ""
	if err := validateResearchRunSnapshotRefFieldsV3(ref, false); err != nil {
		return ResearchRunSnapshotRefV3{}, err
	}
	digest, err := researchRunSnapshotReferenceDigestV3(ref)
	if err != nil {
		return ResearchRunSnapshotRefV3{}, err
	}
	ref.ReferenceDigest = digest
	return ref, nil
}

func (r ResearchRunSnapshotRefV3) Identity() RunIdentity {
	return RunIdentity{
		TemporalWorkflowID: r.TemporalWorkflowID, TemporalRunID: r.TemporalRunID,
		RunKind: r.RunKind, TenantID: r.TenantID, UserID: r.UserID, TaskID: r.TaskID,
	}
}

func (r ResearchRunSnapshotRefV3) ValidateFor(identity RunIdentity) error {
	if err := validateResearchRunSnapshotRefFieldsV3(r, true); err != nil {
		return err
	}
	if r.Identity() != identity {
		return NewAppError(CodeValidation, "research snapshot 引用范围不匹配", ErrValidation)
	}
	expected, err := researchRunSnapshotReferenceDigestV3(r)
	if err != nil || subtle.ConstantTimeCompare([]byte(expected), []byte(r.ReferenceDigest)) != 1 {
		return NewAppError(CodeValidation, "research snapshot 引用摘要无效", ErrValidation)
	}
	return nil
}

func validateResearchRunSnapshotRefFieldsV3(
	r ResearchRunSnapshotRefV3, requireDigest bool,
) error {
	through, throughErr := time.Parse(time.RFC3339Nano, r.HistoryThroughUTC)
	if r.SchemaVersion != ResearchRunSnapshotRefSchemaV3 || r.SnapshotID <= 0 ||
		r.RunKind != RunSnapshotKindScheduled || r.TenantID <= 0 || r.UserID <= 0 ||
		r.DefinitionVersion <= 0 || !boundedResearchRefText(r.TaskID, 255) ||
		!boundedResearchRefText(r.TemporalWorkflowID, 512) ||
		!boundedResearchRefText(r.TemporalRunID, 512) ||
		!researchSHA256(r.DefinitionDigest) ||
		!researchSHA256(r.CapabilityCatalogDigest) ||
		!researchSHA256(r.ToolPolicyDigest) || !researchSHA256(r.PromptPolicyDigest) ||
		!researchSHA256(r.ModelPolicyDigest) || !researchSHA256(r.QuotaPolicyDigest) ||
		!researchSHA256(r.PayloadDigest) || (requireDigest && !researchSHA256(r.ReferenceDigest)) ||
		strings.TrimSpace(r.HistoryThroughUTC) != r.HistoryThroughUTC ||
		throughErr != nil || through.Location() != time.UTC ||
		through.Format(time.RFC3339Nano) != r.HistoryThroughUTC ||
		r.PlannerBudget.ValidateForMode(ExecutionModeDiscoverAtRun) != nil {
		return NewAppError(CodeValidation, "research snapshot 引用无效", ErrValidation)
	}
	return nil
}

func researchRunSnapshotReferenceDigestV3(r ResearchRunSnapshotRefV3) (string, error) {
	payload, err := json.Marshal(researchRunSnapshotRefDigestV3{
		SchemaVersion: r.SchemaVersion, SnapshotID: r.SnapshotID,
		TemporalWorkflowID: r.TemporalWorkflowID, TemporalRunID: r.TemporalRunID,
		RunKind: r.RunKind, TenantID: r.TenantID, UserID: r.UserID, TaskID: r.TaskID,
		DefinitionVersion: r.DefinitionVersion, DefinitionDigest: r.DefinitionDigest,
		CapabilityCatalogDigest: r.CapabilityCatalogDigest,
		ToolPolicyDigest:        r.ToolPolicyDigest, PromptPolicyDigest: r.PromptPolicyDigest,
		ModelPolicyDigest: r.ModelPolicyDigest, QuotaPolicyDigest: r.QuotaPolicyDigest,
		PlannerBudget: r.PlannerBudget, HistoryThroughUTC: r.HistoryThroughUTC,
		PayloadDigest: r.PayloadDigest,
	})
	if err != nil {
		return "", fmt.Errorf("encode research snapshot reference: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

// ResearchRunPlanRefV3 is the only plan value allowed in Temporal history.
// It deliberately excludes the task manual, Tool arguments and plan payload.
type ResearchRunPlanRefV3 struct {
	SchemaVersion           string `json:"schema_version"`
	PlanID                  int64  `json:"plan_id"`
	RunSnapshotID           int64  `json:"run_snapshot_id"`
	TemporalWorkflowID      string `json:"temporal_workflow_id"`
	TemporalRunID           string `json:"temporal_run_id"`
	TenantID                int64  `json:"tenant_id"`
	UserID                  int64  `json:"user_id"`
	TaskID                  string `json:"task_id"`
	DefinitionDigest        string `json:"definition_digest"`
	CapabilityCatalogDigest string `json:"capability_catalog_digest"`
	PlanDigest              string `json:"plan_digest"`
	StepCount               int    `json:"step_count"`
	ReferenceDigest         string `json:"reference_digest"`
}

type researchRunPlanRefDigestV3 struct {
	SchemaVersion           string `json:"schema_version"`
	PlanID                  int64  `json:"plan_id"`
	RunSnapshotID           int64  `json:"run_snapshot_id"`
	TemporalWorkflowID      string `json:"temporal_workflow_id"`
	TemporalRunID           string `json:"temporal_run_id"`
	TenantID                int64  `json:"tenant_id"`
	UserID                  int64  `json:"user_id"`
	TaskID                  string `json:"task_id"`
	DefinitionDigest        string `json:"definition_digest"`
	CapabilityCatalogDigest string `json:"capability_catalog_digest"`
	PlanDigest              string `json:"plan_digest"`
	StepCount               int    `json:"step_count"`
}

func SealResearchRunPlanRefV3(ref ResearchRunPlanRefV3) (ResearchRunPlanRefV3, error) {
	ref.SchemaVersion = ResearchRunPlanRefSchemaV3
	ref.ReferenceDigest = ""
	if err := validateResearchRunPlanRefFieldsV3(ref, false); err != nil {
		return ResearchRunPlanRefV3{}, err
	}
	digest, err := researchRunPlanReferenceDigestV3(ref)
	if err != nil {
		return ResearchRunPlanRefV3{}, err
	}
	ref.ReferenceDigest = digest
	return ref, nil
}

func (r ResearchRunPlanRefV3) ValidateFor(identity RunIdentity, snapshotID int64) error {
	if err := validateResearchRunPlanRefFieldsV3(r, true); err != nil {
		return err
	}
	if snapshotID <= 0 || r.RunSnapshotID != snapshotID ||
		r.TemporalWorkflowID != identity.TemporalWorkflowID ||
		r.TemporalRunID != identity.TemporalRunID ||
		r.TenantID != identity.TenantID || r.UserID != identity.UserID ||
		r.TaskID != identity.TaskID || identity.RunKind != RunSnapshotKindScheduled {
		return NewAppError(CodeValidation, "research plan 引用范围不匹配", ErrValidation)
	}
	expected, err := researchRunPlanReferenceDigestV3(r)
	if err != nil || subtle.ConstantTimeCompare([]byte(expected), []byte(r.ReferenceDigest)) != 1 {
		return NewAppError(CodeValidation, "research plan 引用摘要无效", ErrValidation)
	}
	return nil
}

func validateResearchRunPlanRefFieldsV3(r ResearchRunPlanRefV3, requireDigest bool) error {
	if r.SchemaVersion != ResearchRunPlanRefSchemaV3 || r.PlanID <= 0 ||
		r.RunSnapshotID <= 0 || r.TenantID <= 0 || r.UserID <= 0 ||
		!boundedResearchRefText(r.TaskID, 255) ||
		!boundedResearchRefText(r.TemporalWorkflowID, 512) ||
		!boundedResearchRefText(r.TemporalRunID, 512) ||
		!researchSHA256(r.DefinitionDigest) ||
		!researchSHA256(r.CapabilityCatalogDigest) || !researchSHA256(r.PlanDigest) ||
		r.StepCount <= 0 || r.StepCount > 16 ||
		(requireDigest && !researchSHA256(r.ReferenceDigest)) {
		return NewAppError(CodeValidation, "research plan 引用无效", ErrValidation)
	}
	return nil
}

func researchRunPlanReferenceDigestV3(r ResearchRunPlanRefV3) (string, error) {
	payload, err := json.Marshal(researchRunPlanRefDigestV3{
		SchemaVersion: r.SchemaVersion, PlanID: r.PlanID,
		RunSnapshotID: r.RunSnapshotID, TemporalWorkflowID: r.TemporalWorkflowID,
		TemporalRunID: r.TemporalRunID, TenantID: r.TenantID, UserID: r.UserID,
		TaskID: r.TaskID, DefinitionDigest: r.DefinitionDigest,
		CapabilityCatalogDigest: r.CapabilityCatalogDigest, PlanDigest: r.PlanDigest,
		StepCount: r.StepCount,
	})
	if err != nil {
		return "", fmt.Errorf("encode research plan reference: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func boundedResearchRefText(value string, max int) bool {
	return value != "" && strings.TrimSpace(value) == value && len(value) <= max
}

func researchSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}
