package store

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/types"
)

const (
	taskRunReferenceSchemaVersionV1  = "vane.run-snapshot-ref/v1"
	taskRunReferenceKindV1           = "scheduled"
	taskRunReferenceModeV1           = "compiled"
	taskRunScheduledWorkflowPrefixV1 = "wf-"
	maxTaskRunReferenceBytesV1       = 512
	maxTaskRunReferenceTaskIDV1      = 255
)

// validScheduledTaskWorkflowExecutionIDV1 accepts the retained bare Action ID
// and Temporal Schedule's default execution ID. Temporal appends the nominal
// time after truncating it to a UTC second; accepting any broader prefix would
// let an unrelated workflow impersonate a scheduled task.
func validScheduledTaskWorkflowExecutionIDV1(taskID, workflowID string) bool {
	if !validTaskRunReferenceTextV1(taskID, maxTaskRunReferenceTaskIDV1) ||
		!validTaskRunReferenceTextV1(workflowID, maxTaskRunReferenceBytesV1) {
		return false
	}
	base := taskRunScheduledWorkflowPrefixV1 + taskID
	if workflowID == base {
		return true
	}
	const timestampLayout = "2006-01-02T15:04:05Z"
	if len(workflowID) != len(base)+1+len(timestampLayout) ||
		workflowID[:len(base)] != base ||
		workflowID[len(base)] != '-' {
		return false
	}
	timestamp := workflowID[len(base)+1:]
	parsed, err := time.Parse(timestampLayout, timestamp)
	return err == nil && parsed.UTC().Format(timestampLayout) == timestamp
}

func validManualTaskWorkflowExecutionIDV1(workflowID string) bool {
	if !strings.HasPrefix(workflowID, types.ManualTaskWorkflowPrefix) {
		return false
	}
	raw := strings.TrimPrefix(workflowID, types.ManualTaskWorkflowPrefix)
	parsed, err := uuid.Parse(raw)
	return err == nil && parsed.String() == raw &&
		types.ManualTaskWorkflowPrefix+raw == workflowID
}

func validTaskRunWorkflowExecutionIDV1(taskID, workflowID string) bool {
	return validScheduledTaskWorkflowExecutionIDV1(taskID, workflowID) ||
		validManualTaskWorkflowExecutionIDV1(workflowID)
}

func validateTaskRunSnapshotReferenceForExpectedV1(
	ref types.RunSnapshotRef,
	expected types.RunIdentity,
) (taskRunSnapshotReferenceV1, error) {
	if ref.SchemaVersion != taskRunReferenceSchemaVersionV1 ||
		validateTaskRunExpectedIdentityV1(expected) != nil {
		return taskRunSnapshotReferenceV1{}, errors.New("invalid expected v1 task run identity")
	}
	pinned := taskRunReferenceFromCurrentV1(ref)
	if err := pinned.validateSealed(); err != nil ||
		pinned.Identity.TemporalWorkflowID != expected.TemporalWorkflowID ||
		pinned.Identity.TemporalRunID != expected.TemporalRunID ||
		pinned.Identity.RunKind != string(expected.RunKind) ||
		pinned.Identity.TenantID != expected.TenantID ||
		pinned.Identity.UserID != expected.UserID ||
		pinned.Identity.TaskID != expected.TaskID {
		return taskRunSnapshotReferenceV1{}, errors.New(
			"v1 task run reference does not match expected identity")
	}
	return pinned, nil
}

func validateTaskRunExpectedIdentityV1(expected types.RunIdentity) error {
	if expected.TenantID <= 0 || expected.UserID <= 0 ||
		string(expected.RunKind) != taskRunReferenceKindV1 ||
		!validTaskRunReferenceTextV1(expected.TemporalWorkflowID, maxTaskRunReferenceBytesV1) ||
		!validTaskRunReferenceTextV1(expected.TemporalRunID, maxTaskRunReferenceBytesV1) ||
		!validTaskRunReferenceTextV1(expected.TaskID, maxTaskRunReferenceTaskIDV1) ||
		!validTaskRunWorkflowExecutionIDV1(
			expected.TaskID, expected.TemporalWorkflowID) {
		return errors.New("invalid expected v1 task run identity")
	}
	return nil
}

func taskRunReferenceFromCurrentV1(ref types.RunSnapshotRef) taskRunSnapshotReferenceV1 {
	return taskRunSnapshotReferenceV1{
		SchemaVersion: ref.SchemaVersion,
		SnapshotID:    ref.SnapshotID,
		Identity: taskRunReferenceIdentityV1{
			TemporalWorkflowID: ref.TemporalWorkflowID,
			TemporalRunID:      ref.TemporalRunID,
			RunKind:            string(ref.RunKind),
			TenantID:           ref.TenantID,
			UserID:             ref.UserID,
			TaskID:             ref.TaskID,
		},
		Mode:             string(ref.Mode),
		DefinitionDigest: ref.DefinitionDigest,
		PlanDigest:       ref.PlanDigest,
		AdaptiveVersion:  ref.AdaptiveVersion,
		Policy: taskRunReferencePolicyV1{
			CapabilityCatalogDigest: ref.Policy.CapabilityCatalogDigest,
			ToolPolicyDigest:        ref.Policy.ToolPolicyDigest,
			PromptPolicyDigest:      ref.Policy.PromptPolicyDigest,
			ModelPolicyDigest:       ref.Policy.ModelPolicyDigest,
			QuotaPolicyDigest:       ref.Policy.QuotaPolicyDigest,
		},
		PlannerBudget: taskRunBudgetV1{
			MaxPlannerRounds: ref.PlannerBudget.MaxPlannerRounds,
			MaxToolCalls:     ref.PlannerBudget.MaxToolCalls,
			MaxTokens:        ref.PlannerBudget.MaxTokens,
			MaxCostMicroUSD:  ref.PlannerBudget.MaxCostMicroUSD,
			DurationMs:       ref.PlannerBudget.DurationMs,
		},
		PayloadDigest:   ref.PayloadDigest,
		ReferenceDigest: ref.ReferenceDigest,
	}
}

type taskRunReferenceIdentityV1 struct {
	TemporalWorkflowID string `json:"temporal_workflow_id"`
	TemporalRunID      string `json:"temporal_run_id"`
	RunKind            string `json:"run_kind"`
	TenantID           int64  `json:"tenant_id"`
	UserID             int64  `json:"user_id"`
	TaskID             string `json:"task_id"`
}

type taskRunReferencePolicyV1 struct {
	CapabilityCatalogDigest string `json:"capability_catalog_digest"`
	ToolPolicyDigest        string `json:"tool_policy_digest"`
	PromptPolicyDigest      string `json:"prompt_policy_digest"`
	ModelPolicyDigest       string `json:"model_policy_digest"`
	QuotaPolicyDigest       string `json:"quota_policy_digest"`
}

type taskRunSnapshotReferenceV1 struct {
	SchemaVersion    string                     `json:"schema_version"`
	SnapshotID       int64                      `json:"snapshot_id"`
	Identity         taskRunReferenceIdentityV1 `json:"identity"`
	Mode             string                     `json:"mode"`
	DefinitionDigest string                     `json:"definition_digest"`
	PlanDigest       string                     `json:"plan_digest"`
	AdaptiveVersion  int64                      `json:"adaptive_version"`
	Policy           taskRunReferencePolicyV1   `json:"policy"`
	PlannerBudget    taskRunBudgetV1            `json:"planner_budget"`
	PayloadDigest    string                     `json:"payload_digest"`
	ReferenceDigest  string                     `json:"-"`
}

func (s *taskRunSnapshot) safeRef() (types.RunSnapshotRef, error) {
	return s.safeRefV1()
}

func (s *taskRunSnapshot) safeRefV1() (types.RunSnapshotRef, error) {
	if s == nil || s.ReferenceSchemaVersion != taskRunReferenceSchemaVersionV1 {
		return types.RunSnapshotRef{}, taskRunIntegrityError()
	}
	budget, canonicalBudget, err := readTaskRunBudgetV1(s.BudgetJSON)
	if err != nil {
		return types.RunSnapshotRef{}, taskRunIntegrityError()
	}
	pinned := taskRunReferenceFromSnapshotV1(s, budget)
	pinned.ReferenceDigest = s.ReferenceDigest
	if err := pinned.validateSealed(); err != nil {
		return types.RunSnapshotRef{}, taskRunIntegrityError()
	}
	s.BudgetJSON = canonicalBudget
	return pinned.toCurrent(), nil
}

func sealTaskRunSnapshotReferenceV1(
	snapshot *taskRunSnapshot,
	budget taskRunBudgetV1,
) (types.RunSnapshotRef, error) {
	if snapshot == nil || snapshot.ReferenceSchemaVersion != taskRunReferenceSchemaVersionV1 {
		return types.RunSnapshotRef{}, taskRunIntegrityError()
	}
	pinned := taskRunReferenceFromSnapshotV1(snapshot, budget)
	if err := pinned.validateUnsealed(); err != nil {
		return types.RunSnapshotRef{}, taskRunIntegrityError()
	}
	digest, err := pinned.digest()
	if err != nil {
		return types.RunSnapshotRef{}, taskRunIntegrityError()
	}
	pinned.ReferenceDigest = digest
	return pinned.toCurrent(), nil
}

func taskRunReferenceFromSnapshotV1(
	s *taskRunSnapshot,
	budget taskRunBudgetV1,
) taskRunSnapshotReferenceV1 {
	return taskRunSnapshotReferenceV1{
		SchemaVersion: s.ReferenceSchemaVersion,
		SnapshotID:    s.ID,
		Identity: taskRunReferenceIdentityV1{
			TemporalWorkflowID: s.TemporalWorkflowID,
			TemporalRunID:      s.TemporalRunID,
			RunKind:            string(s.RunKind),
			TenantID:           s.TenantID,
			UserID:             s.UserID,
			TaskID:             s.TaskID,
		},
		Mode:             string(s.Mode),
		DefinitionDigest: s.DefinitionDigest,
		PlanDigest:       s.PlanDigest,
		AdaptiveVersion:  s.AdaptiveVersion,
		Policy: taskRunReferencePolicyV1{
			CapabilityCatalogDigest: s.CapabilityCatalogDigest,
			ToolPolicyDigest:        s.ToolPolicyDigest,
			PromptPolicyDigest:      s.PromptPolicyDigest,
			ModelPolicyDigest:       s.ModelPolicyDigest,
			QuotaPolicyDigest:       s.QuotaPolicyDigest,
		},
		PlannerBudget: budget,
		PayloadDigest: s.PayloadDigest,
	}
}

func (r taskRunSnapshotReferenceV1) validateUnsealed() error {
	if r.SchemaVersion != taskRunReferenceSchemaVersionV1 || r.SnapshotID <= 0 ||
		r.Identity.RunKind != taskRunReferenceKindV1 ||
		r.Identity.TenantID <= 0 || r.Identity.UserID <= 0 ||
		!validTaskRunReferenceTextV1(r.Identity.TemporalWorkflowID, maxTaskRunReferenceBytesV1) ||
		!validTaskRunReferenceTextV1(r.Identity.TemporalRunID, maxTaskRunReferenceBytesV1) ||
		!validTaskRunReferenceTextV1(r.Identity.TaskID, maxTaskRunReferenceTaskIDV1) ||
		r.Mode != taskRunReferenceModeV1 || r.AdaptiveVersion != 0 ||
		r.PlannerBudget != (taskRunBudgetV1{}) ||
		!validTaskRunDigestV1(r.DefinitionDigest) ||
		!validTaskRunDigestV1(r.PlanDigest) || !validTaskRunDigestV1(r.PayloadDigest) ||
		!validTaskRunDigestV1(r.Policy.CapabilityCatalogDigest) ||
		!validTaskRunDigestV1(r.Policy.ToolPolicyDigest) ||
		!validTaskRunDigestV1(r.Policy.PromptPolicyDigest) ||
		!validTaskRunDigestV1(r.Policy.ModelPolicyDigest) ||
		!validTaskRunDigestV1(r.Policy.QuotaPolicyDigest) {
		return errors.New("invalid v1 task run reference")
	}
	return nil
}

func (r taskRunSnapshotReferenceV1) validateSealed() error {
	if err := r.validateUnsealed(); err != nil || !validTaskRunDigestV1(r.ReferenceDigest) {
		return errors.New("invalid sealed v1 task run reference")
	}
	expected, err := r.digest()
	if err != nil {
		return err
	}
	actualBytes, _ := hex.DecodeString(r.ReferenceDigest)
	expectedBytes, _ := hex.DecodeString(expected)
	if subtle.ConstantTimeCompare(actualBytes, expectedBytes) != 1 {
		return errors.New("v1 task run reference digest mismatch")
	}
	return nil
}

func (r taskRunSnapshotReferenceV1) digest() (string, error) {
	copy := r
	copy.ReferenceDigest = ""
	payload, err := json.Marshal(copy)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func (r taskRunSnapshotReferenceV1) toCurrent() types.RunSnapshotRef {
	return types.RunSnapshotRef{
		SchemaVersion: r.SchemaVersion, SnapshotID: r.SnapshotID,
		TemporalWorkflowID: r.Identity.TemporalWorkflowID,
		TemporalRunID:      r.Identity.TemporalRunID,
		RunKind:            types.RunSnapshotKind(r.Identity.RunKind),
		TenantID:           r.Identity.TenantID,
		UserID:             r.Identity.UserID,
		TaskID:             r.Identity.TaskID,
		Mode:               types.ExecutionMode(r.Mode),
		DefinitionDigest:   r.DefinitionDigest,
		PlanDigest:         r.PlanDigest,
		AdaptiveVersion:    r.AdaptiveVersion,
		Policy: types.RuntimePolicyDigests{
			CapabilityCatalogDigest: r.Policy.CapabilityCatalogDigest,
			ToolPolicyDigest:        r.Policy.ToolPolicyDigest,
			PromptPolicyDigest:      r.Policy.PromptPolicyDigest,
			ModelPolicyDigest:       r.Policy.ModelPolicyDigest,
			QuotaPolicyDigest:       r.Policy.QuotaPolicyDigest,
		},
		PlannerBudget: types.PlannerBudget{
			MaxPlannerRounds: r.PlannerBudget.MaxPlannerRounds,
			MaxToolCalls:     r.PlannerBudget.MaxToolCalls,
			MaxTokens:        r.PlannerBudget.MaxTokens,
			MaxCostMicroUSD:  r.PlannerBudget.MaxCostMicroUSD,
			DurationMs:       r.PlannerBudget.DurationMs,
		},
		PayloadDigest:   r.PayloadDigest,
		ReferenceDigest: r.ReferenceDigest,
	}
}

func readTaskRunBudgetV1(raw json.RawMessage) (taskRunBudgetV1, json.RawMessage, error) {
	if len(raw) == 0 || len(raw) > maxTaskRunJSONBytesV1 {
		return taskRunBudgetV1{}, nil, errors.New("invalid v1 task run budget size")
	}
	var budget *taskRunBudgetV1
	if err := strictjson.DecodeExact(raw, &budget); err != nil || budget == nil ||
		*budget != (taskRunBudgetV1{}) {
		return taskRunBudgetV1{}, nil, errors.New("invalid v1 compiled task run budget")
	}
	canonical, err := json.Marshal(budget)
	if err != nil {
		return taskRunBudgetV1{}, nil, err
	}
	return *budget, canonical, nil
}

func validTaskRunReferenceTextV1(value string, maxBytes int) bool {
	if value == "" || strings.TrimSpace(value) != value || len(value) > maxBytes ||
		!utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf) {
			return false
		}
	}
	return true
}

func validTaskRunDigestV1(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && !bytes.Equal(decoded, make([]byte, sha256.Size))
}
