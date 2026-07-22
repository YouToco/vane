package types

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// RunSnapshotSchemaVersion identifies the stable safe-reference payload. A new
// incompatible shape must use a new value rather than reinterpret persisted
// Temporal history.
const RunSnapshotSchemaVersion = "vane.run-snapshot-ref/v1"

const (
	maxRunSnapshotRefBytes    = 512
	maxRunSnapshotTaskIDBytes = 255

	maxPlannerRounds       = 8
	maxPlannerToolCalls    = 16
	maxPlannerTokens       = 32_768
	maxPlannerCostMicroUSD = 1_000_000
	maxPlannerDurationMs   = 300_000
)

// RunSnapshotKind identifies how a snapshotted run was started. C0/C1 only
// snapshots scheduled tasks; ad-hoc runs require a separate contract and must
// not be inferred from a missing TaskID.
type RunSnapshotKind string

const RunSnapshotKindScheduled RunSnapshotKind = "scheduled"

// Valid reports whether k is supported by the current snapshot contract.
func (k RunSnapshotKind) Valid() bool {
	return k == RunSnapshotKindScheduled
}

// RunIdentity is the complete trusted Temporal and tenant scope expected by a
// caller. Every field is mandatory in C0/C1.
type RunIdentity struct {
	TemporalWorkflowID string          `json:"temporal_workflow_id"`
	TemporalRunID      string          `json:"temporal_run_id"`
	RunKind            RunSnapshotKind `json:"run_kind"`
	TenantID           int64           `json:"tenant_id"`
	UserID             int64           `json:"user_id"`
	TaskID             string          `json:"task_id"`
}

// Validate rejects incomplete scope and all non-scheduled run kinds.
func (i RunIdentity) Validate() error {
	if err := validateRunSnapshotText("temporal workflow id", i.TemporalWorkflowID, maxRunSnapshotRefBytes); err != nil {
		return err
	}
	if err := validateRunSnapshotText("temporal run id", i.TemporalRunID, maxRunSnapshotRefBytes); err != nil {
		return err
	}
	if !i.RunKind.Valid() {
		return runSnapshotValidationError("run snapshot kind is unsupported")
	}
	if i.TenantID <= 0 || i.UserID <= 0 {
		return runSnapshotValidationError("run snapshot tenant and user ids must be positive")
	}
	return validateRunSnapshotText("task id", i.TaskID, maxRunSnapshotTaskIDBytes)
}

// RuntimePolicyDigests freezes content-addressed policy references for one run.
// Actual model names, prompts, schemas, and quotas remain in the durable
// payload and are never copied into Workflow history. Credentials never enter
// either payload: a future integration may freeze only a non-sensitive secret
// reference/version digest, then resolve the secret from the controlled secret
// store at execution time.
type RuntimePolicyDigests struct {
	CapabilityCatalogDigest string `json:"capability_catalog_digest"`
	ToolPolicyDigest        string `json:"tool_policy_digest"`
	PromptPolicyDigest      string `json:"prompt_policy_digest"`
	ModelPolicyDigest       string `json:"model_policy_digest"`
	QuotaPolicyDigest       string `json:"quota_policy_digest"`
}

// Validate requires every policy reference to be a lowercase SHA-256 digest.
func (p RuntimePolicyDigests) Validate() error {
	checks := []struct {
		name  string
		value string
	}{
		{name: "capability catalog digest", value: p.CapabilityCatalogDigest},
		{name: "tool policy digest", value: p.ToolPolicyDigest},
		{name: "prompt policy digest", value: p.PromptPolicyDigest},
		{name: "model policy digest", value: p.ModelPolicyDigest},
		{name: "quota policy digest", value: p.QuotaPolicyDigest},
	}
	for _, check := range checks {
		if err := validateRunSnapshotDigest(check.name, check.value); err != nil {
			return err
		}
	}
	return nil
}

// PlannerBudget is the complete bounded PlanFetch allowance frozen at run
// start. MaxCostMicroUSD uses millionths of one US dollar and covers both the
// planner LLM and paid tools. A Compiled run has no planner, so its value is
// exactly zero; quota policy remains frozen independently by QuotaPolicyDigest.
type PlannerBudget struct {
	MaxPlannerRounds int   `json:"max_planner_rounds"`
	MaxToolCalls     int   `json:"max_tool_calls"`
	MaxTokens        int   `json:"max_tokens"`
	MaxCostMicroUSD  int64 `json:"max_cost_micro_usd"`
	DurationMs       int64 `json:"duration_ms"`
}

// ValidateForMode enforces both positive DiscoverAtRun allowances and hard
// ceilings. Config cannot turn a nominal maximum into a practically unbounded
// planner.
func (b PlannerBudget) ValidateForMode(mode ExecutionMode) error {
	switch mode {
	case ExecutionModeCompiled:
		if b != (PlannerBudget{}) {
			return runSnapshotValidationError("compiled mode requires an all-zero planner budget")
		}
		return nil
	case ExecutionModeDiscoverAtRun:
		if b.MaxPlannerRounds <= 0 || b.MaxPlannerRounds > maxPlannerRounds {
			return runSnapshotValidationError("planner rounds are outside the allowed range")
		}
		if b.MaxToolCalls <= 0 || b.MaxToolCalls > maxPlannerToolCalls {
			return runSnapshotValidationError("planner tool calls are outside the allowed range")
		}
		if b.MaxTokens <= 0 || b.MaxTokens > maxPlannerTokens {
			return runSnapshotValidationError("planner tokens are outside the allowed range")
		}
		if b.MaxCostMicroUSD <= 0 || b.MaxCostMicroUSD > maxPlannerCostMicroUSD {
			return runSnapshotValidationError("planner cost is outside the allowed range")
		}
		if b.DurationMs <= 0 || b.DurationMs > maxPlannerDurationMs {
			return runSnapshotValidationError("planner duration is outside the allowed range")
		}
		return nil
	default:
		return runSnapshotValidationError("execution mode is not executable")
	}
}

// RunSnapshotRef is the immutable, safe-reference-only result of PrepareRun.
// It deliberately excludes definition bodies, playbooks, source URLs/config,
// prompts, model names, tool schemas, and secrets. ReferenceDigest binds every
// field except itself to a stable envelope.
type RunSnapshotRef struct {
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
	// PayloadDigest declares which durable payload must be loaded. Validate only
	// proves that the reference is well-formed and bound by ReferenceDigest; the
	// Store load path must hash the loaded payload and compare it independently.
	PayloadDigest   string `json:"payload_digest"`
	ReferenceDigest string `json:"reference_digest"`
}

// Identity returns the complete scope carried by s.
func (s RunSnapshotRef) Identity() RunIdentity {
	return RunIdentity{
		TemporalWorkflowID: s.TemporalWorkflowID,
		TemporalRunID:      s.TemporalRunID,
		RunKind:            s.RunKind,
		TenantID:           s.TenantID,
		UserID:             s.UserID,
		TaskID:             s.TaskID,
	}
}

// Seal validates every field except ReferenceDigest and returns a copy sealed
// with the stable reference-envelope digest. Existing digest bytes are ignored.
func (s RunSnapshotRef) Seal() (RunSnapshotRef, error) {
	digest, err := ReferenceDigest(s)
	if err != nil {
		return RunSnapshotRef{}, err
	}
	s.ReferenceDigest = digest
	return s, nil
}

// Validate rejects malformed fields and detects any mutation of the sealed
// safe-reference envelope with a constant-time digest comparison.
func (s RunSnapshotRef) Validate() error {
	if err := s.validateUnsealed(); err != nil {
		return err
	}
	if err := validateRunSnapshotDigest("reference digest", s.ReferenceDigest); err != nil {
		return err
	}
	expected, err := ReferenceDigest(s)
	if err != nil {
		return err
	}
	actualBytes, _ := hex.DecodeString(s.ReferenceDigest)
	expectedBytes, _ := hex.DecodeString(expected)
	if subtle.ConstantTimeCompare(actualBytes, expectedBytes) != 1 {
		return runSnapshotValidationError("run snapshot reference digest does not match")
	}
	return nil
}

// ValidateFor additionally binds s to the caller-observed Temporal and tenant
// scope. expected must itself be complete; no missing field acts as a wildcard.
func (s RunSnapshotRef) ValidateFor(expected RunIdentity) error {
	if err := expected.Validate(); err != nil {
		return err
	}
	if err := s.Validate(); err != nil {
		return err
	}
	if s.Identity() != expected {
		return runSnapshotValidationError("run snapshot identity does not match the expected run")
	}
	return nil
}

// ReferenceDigest computes the lowercase SHA-256 digest of every safe-reference
// field in s except ReferenceDigest itself. It is shared by Store insert/load
// and Workflow validation so the wire envelope cannot drift between packages.
func ReferenceDigest(s RunSnapshotRef) (string, error) {
	if err := s.validateUnsealed(); err != nil {
		return "", err
	}
	envelope := runSnapshotReferenceEnvelope{
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
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return "", runSnapshotValidationError("run snapshot reference envelope cannot be encoded")
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

type runSnapshotReferenceEnvelope struct {
	SchemaVersion    string               `json:"schema_version"`
	SnapshotID       int64                `json:"snapshot_id"`
	Identity         RunIdentity          `json:"identity"`
	Mode             ExecutionMode        `json:"mode"`
	DefinitionDigest string               `json:"definition_digest"`
	PlanDigest       string               `json:"plan_digest"`
	AdaptiveVersion  int64                `json:"adaptive_version"`
	Policy           RuntimePolicyDigests `json:"policy"`
	PlannerBudget    PlannerBudget        `json:"planner_budget"`
	PayloadDigest    string               `json:"payload_digest"`
}

func (s RunSnapshotRef) validateUnsealed() error {
	if s.SchemaVersion != RunSnapshotSchemaVersion {
		return runSnapshotValidationError("run snapshot schema version is unsupported")
	}
	if s.SnapshotID <= 0 {
		return runSnapshotValidationError("run snapshot id must be positive")
	}
	if err := s.Identity().Validate(); err != nil {
		return err
	}
	if !s.Mode.Valid() {
		return runSnapshotValidationError("execution mode is not executable")
	}
	if err := validateRunSnapshotDigest("definition digest", s.DefinitionDigest); err != nil {
		return err
	}
	if s.Mode == ExecutionModeCompiled {
		if err := validateRunSnapshotDigest("plan digest", s.PlanDigest); err != nil {
			return err
		}
	} else if s.PlanDigest != "" {
		// A first DiscoverAtRun execution may have no last-known-good plan yet.
		if err := validateRunSnapshotDigest("plan digest", s.PlanDigest); err != nil {
			return err
		}
	}
	if s.AdaptiveVersion < 0 {
		return runSnapshotValidationError("adaptive version must not be negative")
	}
	if err := s.Policy.Validate(); err != nil {
		return err
	}
	if err := s.PlannerBudget.ValidateForMode(s.Mode); err != nil {
		return err
	}
	return validateRunSnapshotDigest("payload digest", s.PayloadDigest)
}

func validateRunSnapshotText(name, value string, maxBytes int) error {
	if value == "" || strings.TrimSpace(value) != value ||
		len(value) > maxBytes || !utf8.ValidString(value) {
		return runSnapshotValidationError(fmt.Sprintf("%s is invalid", name))
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf) {
			return runSnapshotValidationError(fmt.Sprintf("%s is invalid", name))
		}
	}
	return nil
}

func validateRunSnapshotDigest(name, value string) error {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return runSnapshotValidationError(fmt.Sprintf("%s must be a lowercase sha-256 digest", name))
	}
	if _, err := hex.DecodeString(value); err != nil {
		return runSnapshotValidationError(fmt.Sprintf("%s must be a lowercase sha-256 digest", name))
	}
	return nil
}

func runSnapshotValidationError(message string) error {
	return NewAppError(CodeValidation, message, nil)
}
