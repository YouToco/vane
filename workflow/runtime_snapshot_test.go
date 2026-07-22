package workflow

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/YouToco/vane/types"
)

const workflowSnapshotTestDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func validWorkflowRunSnapshot(t *testing.T) RunSnapshotRef {
	t.Helper()

	snapshot, err := (RunSnapshotRef{
		SchemaVersion:      RunSnapshotSchemaVersion,
		SnapshotID:         41,
		TemporalWorkflowID: "wf-task-v1-abc-20260721",
		TemporalRunID:      "019f-run-abc",
		RunKind:            RunSnapshotKindScheduled,
		TenantID:           7,
		UserID:             9,
		TaskID:             "task-v1-abc",
		Mode:               types.ExecutionModeCompiled,
		DefinitionDigest:   workflowSnapshotTestDigest,
		PlanDigest:         workflowSnapshotTestDigest,
		Policy: RuntimePolicySnapshot{
			CapabilityCatalogDigest: workflowSnapshotTestDigest,
			ToolPolicyDigest:        workflowSnapshotTestDigest,
			PromptPolicyDigest:      workflowSnapshotTestDigest,
			ModelPolicyDigest:       workflowSnapshotTestDigest,
			QuotaPolicyDigest:       workflowSnapshotTestDigest,
		},
		PlannerBudget: PlannerBudgetSnapshot{},
		PayloadDigest: workflowSnapshotTestDigest,
	}).Seal()
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	return snapshot
}

func TestPrepareRunResult_Validate(t *testing.T) {
	t.Parallel()

	valid := validWorkflowRunSnapshot(t)
	tests := []struct {
		name  string
		input PrepareRunResult
		valid bool
	}{
		{name: "unauthorized zero snapshot", input: PrepareRunResult{}, valid: true},
		{name: "unauthorized populated snapshot", input: PrepareRunResult{Snapshot: valid}},
		{name: "authorized valid snapshot", input: PrepareRunResult{Authorized: true, Snapshot: valid}, valid: true},
		{name: "authorized zero snapshot", input: PrepareRunResult{Authorized: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.input.Validate()
			if tt.valid {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if !errors.Is(err, types.ErrValidation) {
				t.Fatalf("Validate() error = %v, want ErrValidation", err)
			}
		})
	}
}

func TestPrepareRunResult_ValidateFor(t *testing.T) {
	t.Parallel()

	snapshot := validWorkflowRunSnapshot(t)
	expected := snapshot.Identity()
	tests := []struct {
		name   string
		input  PrepareRunResult
		mutate func(*RunIdentity)
		valid  bool
	}{
		{name: "authorized exact identity", input: PrepareRunResult{Authorized: true, Snapshot: snapshot}, mutate: func(*RunIdentity) {}, valid: true},
		{name: "unauthorized exact expected identity", input: PrepareRunResult{}, mutate: func(*RunIdentity) {}, valid: true},
		{name: "authorized different run", input: PrepareRunResult{Authorized: true, Snapshot: snapshot}, mutate: func(i *RunIdentity) { i.TemporalRunID += "-other" }},
		{name: "unauthorized incomplete expected identity", input: PrepareRunResult{}, mutate: func(i *RunIdentity) { i.TaskID = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			identity := expected
			tt.mutate(&identity)
			err := tt.input.ValidateFor(identity)
			if tt.valid {
				if err != nil {
					t.Fatalf("ValidateFor() error = %v", err)
				}
				return
			}
			if !errors.Is(err, types.ErrValidation) {
				t.Fatalf("ValidateFor() error = %v, want ErrValidation", err)
			}
		})
	}
}

func TestPrepareRunResult_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	want := PrepareRunResult{Authorized: true, Snapshot: validWorkflowRunSnapshot(t)}
	payload, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var got PrepareRunResult
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip mismatch:\n got: %#v\nwant: %#v", got, want)
	}
	if err := got.ValidateFor(want.Snapshot.Identity()); err != nil {
		t.Fatalf("round-tripped result ValidateFor() error = %v", err)
	}
}
