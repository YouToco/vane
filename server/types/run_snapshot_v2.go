package types

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
)

// RunSnapshotRefV2 is the Source-free safe reference. It is a distinct Go
// type so no retained V1-only authorization path can accept it accidentally.
type RunSnapshotRefV2 struct {
	SchemaVersion      string               `json:"schema_version"`
	SnapshotID         int64                `json:"snapshot_id"`
	TemporalWorkflowID string               `json:"temporal_workflow_id"`
	TemporalRunID      string               `json:"temporal_run_id"`
	RunKind            RunSnapshotKind      `json:"run_kind"`
	TenantID           int64                `json:"tenant_id"`
	UserID             int64                `json:"user_id"`
	TaskID             string               `json:"task_id"`
	Mode               ExecutionMode        `json:"mode"`
	DefinitionDigest   string               `json:"definition_digest"`
	PlanDigest         string               `json:"plan_digest"`
	AdaptiveVersion    int64                `json:"adaptive_version"`
	Policy             RuntimePolicyDigests `json:"policy"`
	PlannerBudget      PlannerBudget        `json:"planner_budget"`
	PayloadDigest      string               `json:"payload_digest"`
	ReferenceDigest    string               `json:"reference_digest"`
}

func (s RunSnapshotRefV2) Identity() RunIdentity {
	return RunIdentity{
		TemporalWorkflowID: s.TemporalWorkflowID,
		TemporalRunID:      s.TemporalRunID,
		RunKind:            s.RunKind,
		TenantID:           s.TenantID,
		UserID:             s.UserID,
		TaskID:             s.TaskID,
	}
}

func (s RunSnapshotRefV2) Seal() (RunSnapshotRefV2, error) {
	digest, err := ReferenceDigestV2(s)
	if err != nil {
		return RunSnapshotRefV2{}, err
	}
	s.ReferenceDigest = digest
	return s, nil
}

func (s RunSnapshotRefV2) Validate() error {
	if err := s.validateUnsealed(); err != nil {
		return err
	}
	if err := validateRunSnapshotDigest(
		"reference digest", s.ReferenceDigest); err != nil {
		return err
	}
	expected, err := ReferenceDigestV2(s)
	if err != nil {
		return err
	}
	actualBytes, _ := hex.DecodeString(s.ReferenceDigest)
	expectedBytes, _ := hex.DecodeString(expected)
	if subtle.ConstantTimeCompare(actualBytes, expectedBytes) != 1 {
		return runSnapshotValidationError(
			"run snapshot v2 reference digest does not match")
	}
	return nil
}

func (s RunSnapshotRefV2) ValidateFor(expected RunIdentity) error {
	if err := expected.Validate(); err != nil {
		return err
	}
	if err := s.Validate(); err != nil {
		return err
	}
	if s.Identity() != expected {
		return runSnapshotValidationError(
			"run snapshot v2 identity does not match the expected run")
	}
	return nil
}

func ReferenceDigestV2(s RunSnapshotRefV2) (string, error) {
	if err := s.validateUnsealed(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(runSnapshotReferenceEnvelope{
		SchemaVersion:    s.SchemaVersion,
		SnapshotID:       s.SnapshotID,
		Identity:         s.Identity(),
		Mode:             s.Mode,
		DefinitionDigest: s.DefinitionDigest,
		PlanDigest:       s.PlanDigest,
		AdaptiveVersion:  s.AdaptiveVersion,
		Policy:           s.Policy,
		PlannerBudget:    s.PlannerBudget,
		PayloadDigest:    s.PayloadDigest,
	})
	if err != nil {
		return "", runSnapshotValidationError(
			"run snapshot v2 reference envelope cannot be encoded")
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func (s RunSnapshotRefV2) validateUnsealed() error {
	if s.SchemaVersion != RunSnapshotSchemaVersionV2 {
		return runSnapshotValidationError(
			"run snapshot v2 schema version is unsupported")
	}
	if s.SnapshotID <= 0 {
		return runSnapshotValidationError(
			"run snapshot v2 id must be positive")
	}
	if err := s.Identity().Validate(); err != nil {
		return err
	}
	if s.Mode != ExecutionModeCompiled {
		return runSnapshotValidationError(
			"run snapshot v2 mode is unsupported")
	}
	if err := validateRunSnapshotDigest(
		"definition digest", s.DefinitionDigest); err != nil {
		return err
	}
	if err := validateRunSnapshotDigest(
		"plan digest", s.PlanDigest); err != nil {
		return err
	}
	if s.AdaptiveVersion <= 0 {
		return runSnapshotValidationError(
			"run snapshot v2 adaptive version must be positive")
	}
	if err := s.Policy.Validate(); err != nil {
		return err
	}
	if s.PlannerBudget != (PlannerBudget{}) {
		return runSnapshotValidationError(
			"run snapshot v2 planner budget must be zero")
	}
	return validateRunSnapshotDigest("payload digest", s.PayloadDigest)
}
