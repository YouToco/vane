package types

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

const (
	runSnapshotTestDigestA = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	runSnapshotTestDigestB = "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

func validRunSnapshot(mode ExecutionMode) RunSnapshotRef {
	snapshot := RunSnapshotRef{
		SchemaVersion:      RunSnapshotSchemaVersion,
		SnapshotID:         41,
		TemporalWorkflowID: "wf-task-v1-abc-20260721",
		TemporalRunID:      "019f-run-abc",
		RunKind:            RunSnapshotKindScheduled,
		TenantID:           7,
		UserID:             9,
		TaskID:             "task-v1-abc",
		Mode:               mode,
		DefinitionDigest:   runSnapshotTestDigestA,
		PlanDigest:         runSnapshotTestDigestA,
		AdaptiveVersion:    0,
		Policy: RuntimePolicyDigests{
			CapabilityCatalogDigest: runSnapshotTestDigestA,
			ToolPolicyDigest:        runSnapshotTestDigestA,
			PromptPolicyDigest:      runSnapshotTestDigestA,
			ModelPolicyDigest:       runSnapshotTestDigestA,
			QuotaPolicyDigest:       runSnapshotTestDigestA,
		},
		PayloadDigest: runSnapshotTestDigestA,
	}
	if mode == ExecutionModeDiscoverAtRun {
		snapshot.PlannerBudget = PlannerBudget{
			MaxPlannerRounds: 4,
			MaxToolCalls:     8,
			MaxTokens:        12_000,
			MaxCostMicroUSD:  500_000,
			DurationMs:       90_000,
		}
	}
	return snapshot
}

func sealedRunSnapshot(t *testing.T, mode ExecutionMode) RunSnapshotRef {
	t.Helper()

	snapshot, err := validRunSnapshot(mode).Seal()
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	return snapshot
}

func differentRunSnapshotDigest(value string) string {
	if value[0] == '0' {
		return "1" + value[1:]
	}
	return "0" + value[1:]
}

func TestRunSnapshotKind_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		kind     RunSnapshotKind
		expected bool
	}{
		{name: "scheduled", kind: RunSnapshotKindScheduled, expected: true},
		{name: "zero value", kind: ""},
		{name: "ad hoc is not in c0", kind: "ad_hoc"},
		{name: "future value", kind: "future"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.kind.Valid(); got != tt.expected {
				t.Fatalf("Valid() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestRunIdentity_Validate(t *testing.T) {
	t.Parallel()

	valid := validRunSnapshot(ExecutionModeCompiled).Identity()
	tests := []struct {
		name   string
		mutate func(*RunIdentity)
		valid  bool
	}{
		{name: "complete scheduled identity", mutate: func(*RunIdentity) {}, valid: true},
		{name: "missing workflow id", mutate: func(i *RunIdentity) { i.TemporalWorkflowID = "" }},
		{name: "missing run id", mutate: func(i *RunIdentity) { i.TemporalRunID = "" }},
		{name: "hidden run id", mutate: func(i *RunIdentity) { i.TemporalRunID = "run\u2066id" }},
		{name: "ad hoc", mutate: func(i *RunIdentity) { i.RunKind = "ad_hoc" }},
		{name: "missing tenant", mutate: func(i *RunIdentity) { i.TenantID = 0 }},
		{name: "missing user", mutate: func(i *RunIdentity) { i.UserID = 0 }},
		{name: "missing task", mutate: func(i *RunIdentity) { i.TaskID = "" }},
		{name: "padded task", mutate: func(i *RunIdentity) { i.TaskID = " task-v1-abc" }},
		{name: "task at byte limit", mutate: func(i *RunIdentity) { i.TaskID = strings.Repeat("t", maxRunSnapshotTaskIDBytes) }, valid: true},
		{name: "task above byte limit", mutate: func(i *RunIdentity) { i.TaskID = strings.Repeat("t", maxRunSnapshotTaskIDBytes+1) }},
		{name: "workflow id at byte limit", mutate: func(i *RunIdentity) { i.TemporalWorkflowID = strings.Repeat("w", maxRunSnapshotRefBytes) }, valid: true},
		{name: "workflow id above byte limit", mutate: func(i *RunIdentity) { i.TemporalWorkflowID = strings.Repeat("w", maxRunSnapshotRefBytes+1) }},
		{name: "run id at byte limit", mutate: func(i *RunIdentity) { i.TemporalRunID = strings.Repeat("r", maxRunSnapshotRefBytes) }, valid: true},
		{name: "run id above byte limit", mutate: func(i *RunIdentity) { i.TemporalRunID = strings.Repeat("r", maxRunSnapshotRefBytes+1) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			input := valid
			tt.mutate(&input)
			err := input.Validate()
			if tt.valid {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if !errors.Is(err, ErrValidation) {
				t.Fatalf("Validate() error = %v, want ErrValidation", err)
			}
		})
	}
}

func TestRuntimePolicyDigests_Validate(t *testing.T) {
	t.Parallel()

	valid := validRunSnapshot(ExecutionModeCompiled).Policy
	tests := []struct {
		name   string
		mutate func(*RuntimePolicyDigests)
		valid  bool
	}{
		{name: "all content addressed", mutate: func(*RuntimePolicyDigests) {}, valid: true},
		{name: "missing capability catalog", mutate: func(p *RuntimePolicyDigests) { p.CapabilityCatalogDigest = "" }},
		{name: "missing tool policy", mutate: func(p *RuntimePolicyDigests) { p.ToolPolicyDigest = "" }},
		{name: "missing prompt policy", mutate: func(p *RuntimePolicyDigests) { p.PromptPolicyDigest = "" }},
		{name: "missing model policy", mutate: func(p *RuntimePolicyDigests) { p.ModelPolicyDigest = "" }},
		{name: "missing quota policy", mutate: func(p *RuntimePolicyDigests) { p.QuotaPolicyDigest = "" }},
		{name: "uppercase digest", mutate: func(p *RuntimePolicyDigests) { p.ModelPolicyDigest = strings.ToUpper(runSnapshotTestDigestA) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			input := valid
			tt.mutate(&input)
			err := input.Validate()
			if tt.valid {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if !errors.Is(err, ErrValidation) {
				t.Fatalf("Validate() error = %v, want ErrValidation", err)
			}
		})
	}
}

func TestPlannerBudget_ValidateForMode(t *testing.T) {
	t.Parallel()

	discover := validRunSnapshot(ExecutionModeDiscoverAtRun).PlannerBudget
	maxInt := int(^uint(0) >> 1)
	maxInt64 := int64(^uint64(0) >> 1)
	tests := []struct {
		name   string
		mode   ExecutionMode
		budget PlannerBudget
		valid  bool
	}{
		{name: "compiled zero", mode: ExecutionModeCompiled, valid: true},
		{name: "compiled planner allowance", mode: ExecutionModeCompiled, budget: discover},
		{name: "discover bounded", mode: ExecutionModeDiscoverAtRun, budget: discover, valid: true},
		{name: "discover hard caps", mode: ExecutionModeDiscoverAtRun, budget: PlannerBudget{
			MaxPlannerRounds: maxPlannerRounds, MaxToolCalls: maxPlannerToolCalls,
			MaxTokens: maxPlannerTokens, MaxCostMicroUSD: maxPlannerCostMicroUSD,
			DurationMs: maxPlannerDurationMs,
		}, valid: true},
		{name: "zero rounds", mode: ExecutionModeDiscoverAtRun, budget: func() PlannerBudget { b := discover; b.MaxPlannerRounds = 0; return b }()},
		{name: "rounds above cap", mode: ExecutionModeDiscoverAtRun, budget: func() PlannerBudget { b := discover; b.MaxPlannerRounds = maxPlannerRounds + 1; return b }()},
		{name: "rounds max int", mode: ExecutionModeDiscoverAtRun, budget: func() PlannerBudget { b := discover; b.MaxPlannerRounds = maxInt; return b }()},
		{name: "zero tools", mode: ExecutionModeDiscoverAtRun, budget: func() PlannerBudget { b := discover; b.MaxToolCalls = 0; return b }()},
		{name: "tools above cap", mode: ExecutionModeDiscoverAtRun, budget: func() PlannerBudget { b := discover; b.MaxToolCalls = maxPlannerToolCalls + 1; return b }()},
		{name: "tools max int", mode: ExecutionModeDiscoverAtRun, budget: func() PlannerBudget { b := discover; b.MaxToolCalls = maxInt; return b }()},
		{name: "zero tokens", mode: ExecutionModeDiscoverAtRun, budget: func() PlannerBudget { b := discover; b.MaxTokens = 0; return b }()},
		{name: "tokens above cap", mode: ExecutionModeDiscoverAtRun, budget: func() PlannerBudget { b := discover; b.MaxTokens = maxPlannerTokens + 1; return b }()},
		{name: "tokens max int", mode: ExecutionModeDiscoverAtRun, budget: func() PlannerBudget { b := discover; b.MaxTokens = maxInt; return b }()},
		{name: "zero cost", mode: ExecutionModeDiscoverAtRun, budget: func() PlannerBudget { b := discover; b.MaxCostMicroUSD = 0; return b }()},
		{name: "cost above cap", mode: ExecutionModeDiscoverAtRun, budget: func() PlannerBudget { b := discover; b.MaxCostMicroUSD = maxPlannerCostMicroUSD + 1; return b }()},
		{name: "cost max int64", mode: ExecutionModeDiscoverAtRun, budget: func() PlannerBudget { b := discover; b.MaxCostMicroUSD = maxInt64; return b }()},
		{name: "zero duration", mode: ExecutionModeDiscoverAtRun, budget: func() PlannerBudget { b := discover; b.DurationMs = 0; return b }()},
		{name: "duration above cap", mode: ExecutionModeDiscoverAtRun, budget: func() PlannerBudget { b := discover; b.DurationMs = maxPlannerDurationMs + 1; return b }()},
		{name: "duration max int64", mode: ExecutionModeDiscoverAtRun, budget: func() PlannerBudget { b := discover; b.DurationMs = maxInt64; return b }()},
		{name: "unknown mode", mode: ExecutionModeUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.budget.ValidateForMode(tt.mode)
			if tt.valid {
				if err != nil {
					t.Fatalf("ValidateForMode() error = %v", err)
				}
				return
			}
			if !errors.Is(err, ErrValidation) {
				t.Fatalf("ValidateForMode() error = %v, want ErrValidation", err)
			}
		})
	}
}

func TestRunSnapshotRef_SealAndValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		input  RunSnapshotRef
		mutate func(*RunSnapshotRef)
		valid  bool
	}{
		{name: "compiled scheduled", input: validRunSnapshot(ExecutionModeCompiled), valid: true},
		{name: "discover scheduled with fallback", input: validRunSnapshot(ExecutionModeDiscoverAtRun), valid: true},
		{name: "discover first run without fallback", input: validRunSnapshot(ExecutionModeDiscoverAtRun), mutate: func(s *RunSnapshotRef) { s.PlanDigest = "" }, valid: true},
		{name: "ad hoc compiled", input: validRunSnapshot(ExecutionModeCompiled), mutate: func(s *RunSnapshotRef) { s.RunKind = "ad_hoc" }},
		{name: "ad hoc discover", input: validRunSnapshot(ExecutionModeDiscoverAtRun), mutate: func(s *RunSnapshotRef) { s.RunKind = "ad_hoc" }},
		{name: "unknown mode", input: validRunSnapshot(ExecutionModeCompiled), mutate: func(s *RunSnapshotRef) { s.Mode = ExecutionModeUnknown }},
		{name: "compiled missing plan", input: validRunSnapshot(ExecutionModeCompiled), mutate: func(s *RunSnapshotRef) { s.PlanDigest = "" }},
		{name: "invalid payload digest", input: validRunSnapshot(ExecutionModeCompiled), mutate: func(s *RunSnapshotRef) { s.PayloadDigest = "payload-v1" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			input := tt.input
			if tt.mutate != nil {
				tt.mutate(&input)
			}
			sealed, err := input.Seal()
			if tt.valid {
				if err != nil {
					t.Fatalf("Seal() error = %v", err)
				}
				if sealed.ReferenceDigest == "" {
					t.Fatal("Seal() left reference digest empty")
				}
				if err := sealed.Validate(); err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if !errors.Is(err, ErrValidation) {
				t.Fatalf("Seal() error = %v, want ErrValidation", err)
			}
		})
	}
}

func TestRunSnapshotRef_ReferenceDigestBindsEveryField(t *testing.T) {
	t.Parallel()

	sealed := sealedRunSnapshot(t, ExecutionModeDiscoverAtRun)
	tests := []struct {
		name   string
		mutate func(*RunSnapshotRef)
	}{
		{name: "schema version", mutate: func(s *RunSnapshotRef) { s.SchemaVersion = "vane.run-snapshot-ref/v2" }},
		{name: "snapshot id", mutate: func(s *RunSnapshotRef) { s.SnapshotID++ }},
		{name: "workflow id", mutate: func(s *RunSnapshotRef) { s.TemporalWorkflowID += "-other" }},
		{name: "run id", mutate: func(s *RunSnapshotRef) { s.TemporalRunID += "-other" }},
		{name: "run kind", mutate: func(s *RunSnapshotRef) { s.RunKind = "future" }},
		{name: "tenant id", mutate: func(s *RunSnapshotRef) { s.TenantID++ }},
		{name: "user id", mutate: func(s *RunSnapshotRef) { s.UserID++ }},
		{name: "task id", mutate: func(s *RunSnapshotRef) { s.TaskID += "-other" }},
		{name: "mode", mutate: func(s *RunSnapshotRef) { s.Mode = ExecutionModeCompiled }},
		{name: "definition digest", mutate: func(s *RunSnapshotRef) { s.DefinitionDigest = runSnapshotTestDigestB }},
		{name: "plan digest", mutate: func(s *RunSnapshotRef) { s.PlanDigest = runSnapshotTestDigestB }},
		{name: "adaptive version", mutate: func(s *RunSnapshotRef) { s.AdaptiveVersion++ }},
		{name: "capability catalog digest", mutate: func(s *RunSnapshotRef) { s.Policy.CapabilityCatalogDigest = runSnapshotTestDigestB }},
		{name: "tool policy digest", mutate: func(s *RunSnapshotRef) { s.Policy.ToolPolicyDigest = runSnapshotTestDigestB }},
		{name: "prompt policy digest", mutate: func(s *RunSnapshotRef) { s.Policy.PromptPolicyDigest = runSnapshotTestDigestB }},
		{name: "model policy digest", mutate: func(s *RunSnapshotRef) { s.Policy.ModelPolicyDigest = runSnapshotTestDigestB }},
		{name: "quota policy digest", mutate: func(s *RunSnapshotRef) { s.Policy.QuotaPolicyDigest = runSnapshotTestDigestB }},
		{name: "planner rounds", mutate: func(s *RunSnapshotRef) { s.PlannerBudget.MaxPlannerRounds++ }},
		{name: "planner tool calls", mutate: func(s *RunSnapshotRef) { s.PlannerBudget.MaxToolCalls++ }},
		{name: "planner tokens", mutate: func(s *RunSnapshotRef) { s.PlannerBudget.MaxTokens++ }},
		{name: "planner cost", mutate: func(s *RunSnapshotRef) { s.PlannerBudget.MaxCostMicroUSD++ }},
		{name: "planner duration", mutate: func(s *RunSnapshotRef) { s.PlannerBudget.DurationMs++ }},
		{name: "payload digest", mutate: func(s *RunSnapshotRef) { s.PayloadDigest = runSnapshotTestDigestB }},
		{name: "reference digest", mutate: func(s *RunSnapshotRef) { s.ReferenceDigest = differentRunSnapshotDigest(s.ReferenceDigest) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mutated := sealed
			tt.mutate(&mutated)
			if err := mutated.Validate(); !errors.Is(err, ErrValidation) {
				t.Fatalf("Validate() after mutation error = %v, want ErrValidation", err)
			}
		})
	}
}

func TestReferenceDigest_GoldenEnvelope(t *testing.T) {
	t.Parallel()

	got, err := ReferenceDigest(validRunSnapshot(ExecutionModeDiscoverAtRun))
	if err != nil {
		t.Fatalf("ReferenceDigest() error = %v", err)
	}
	const expected = "7feb52d7cd156f3c876b917a8b544dd585291d889012c9f1c9a06716fdf637e7"
	if got != expected {
		t.Fatalf("ReferenceDigest() = %q, want stable golden %q", got, expected)
	}
}

func TestRunSnapshotRef_ValidateFor(t *testing.T) {
	t.Parallel()

	snapshot := sealedRunSnapshot(t, ExecutionModeCompiled)
	expected := snapshot.Identity()
	if err := snapshot.ValidateFor(expected); err != nil {
		t.Fatalf("ValidateFor(valid identity) error = %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*RunIdentity)
	}{
		{name: "workflow id", mutate: func(i *RunIdentity) { i.TemporalWorkflowID += "-other" }},
		{name: "run id", mutate: func(i *RunIdentity) { i.TemporalRunID += "-other" }},
		{name: "run kind", mutate: func(i *RunIdentity) { i.RunKind = "future" }},
		{name: "tenant id", mutate: func(i *RunIdentity) { i.TenantID++ }},
		{name: "user id", mutate: func(i *RunIdentity) { i.UserID++ }},
		{name: "task id", mutate: func(i *RunIdentity) { i.TaskID += "-other" }},
		{name: "missing expected scope", mutate: func(i *RunIdentity) { i.TaskID = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			identity := expected
			tt.mutate(&identity)
			if err := snapshot.ValidateFor(identity); !errors.Is(err, ErrValidation) {
				t.Fatalf("ValidateFor() error = %v, want ErrValidation", err)
			}
		})
	}
}

func TestRunSnapshotRef_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	want := sealedRunSnapshot(t, ExecutionModeDiscoverAtRun)
	payload, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var got RunSnapshotRef
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip mismatch:\n got: %#v\nwant: %#v", got, want)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("round-tripped snapshot Validate() error = %v", err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatalf("decode field names: %v", err)
	}
	for _, name := range []string{
		"schema_version", "snapshot_id", "temporal_workflow_id", "temporal_run_id",
		"run_kind", "tenant_id", "user_id", "task_id", "mode", "definition_digest",
		"plan_digest", "adaptive_version", "policy", "planner_budget", "payload_digest",
		"reference_digest",
	} {
		if _, ok := fields[name]; !ok {
			t.Errorf("stable JSON field %q is missing", name)
		}
	}
}

func TestRunSnapshotRef_JSONUnknownModeFailsValidation(t *testing.T) {
	t.Parallel()

	payload, err := json.Marshal(sealedRunSnapshot(t, ExecutionModeCompiled))
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatalf("Unmarshal fields error = %v", err)
	}
	fields["mode"] = json.RawMessage(`"future_mode"`)
	payload, err = json.Marshal(fields)
	if err != nil {
		t.Fatalf("Marshal mutated fields error = %v", err)
	}
	var got RunSnapshotRef
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("Unmarshal unknown mode error = %v", err)
	}
	if err := got.Validate(); !errors.Is(err, ErrValidation) {
		t.Fatalf("unknown mode Validate() error = %v, want ErrValidation", err)
	}
}
