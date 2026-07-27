package runoutcome

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

type recoveryStoreFake struct {
	mu         sync.Mutex
	candidates []store.RunOutcomeRecoveryCandidateV1
	claims     []types.RunOutcomeClaimV1
}

func (f *recoveryStoreFake) ListStaleRunOutcomeCandidatesV1(
	_ context.Context, after *store.RunOutcomeRecoveryCursorV1, limit int,
) ([]store.RunOutcomeRecoveryCandidateV1, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	start := 0
	if after != nil {
		for start < len(f.candidates) &&
			(f.candidates[start].CreatedAt.Before(after.CreatedAt) ||
				f.candidates[start].CreatedAt.Equal(after.CreatedAt) &&
					f.candidates[start].Marker.ID <= after.ID) {
			start++
		}
	}
	end := min(start+limit, len(f.candidates))
	return append([]store.RunOutcomeRecoveryCandidateV1(nil),
		f.candidates[start:end]...), nil
}

func (f *recoveryStoreFake) FinalizeRecoveredRunOutcomeClaimV1(
	_ context.Context, _ types.RunIdentity, claim types.RunOutcomeClaimV1,
) (types.RunOutcomeV1, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims = append(f.claims, claim)
	return claim.SealAt(time.Now())
}

type inspectorFake struct {
	mu         sync.Mutex
	executions map[string]Execution
	active     atomic.Int32
	peak       atomic.Int32
	delay      time.Duration
}

func (f *inspectorFake) Inspect(
	ctx context.Context, workflowID, _ string,
) (Execution, error) {
	active := f.active.Add(1)
	defer f.active.Add(-1)
	for {
		peak := f.peak.Load()
		if active <= peak || f.peak.CompareAndSwap(peak, active) {
			break
		}
	}
	if f.delay > 0 {
		timer := time.NewTimer(f.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return Execution{}, ctx.Err()
		case <-timer.C:
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.executions[workflowID], nil
}

func recoveryCandidate(id int64, workflowID string) store.RunOutcomeRecoveryCandidateV1 {
	return store.RunOutcomeRecoveryCandidateV1{
		Marker: types.RunOutcomeMarkerV1{
			ID: id, SchemaVersion: types.RunOutcomeSchemaVersionV1,
			RunSnapshotID: id + 100, TenantID: 1, UserID: 2,
			TaskID: "task-1",
		},
		Identity: types.RunIdentity{
			TemporalWorkflowID: workflowID,
			TemporalRunID:      "run-" + workflowID,
			RunKind:            types.RunSnapshotKindScheduled,
			TenantID:           1, UserID: 2, TaskID: "task-1",
		},
		CreatedAt: time.Unix(id, 0).UTC(),
	}
}

func TestRunnerFinalizesOnlyExactTerminalExecutions(t *testing.T) {
	st := &recoveryStoreFake{candidates: []store.RunOutcomeRecoveryCandidateV1{
		recoveryCandidate(1, "running"),
		recoveryCandidate(2, "completed"),
		recoveryCandidate(3, "failed"),
		recoveryCandidate(4, "terminated"),
	}}
	inspector := &inspectorFake{executions: map[string]Execution{
		"running":   {Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
		"completed": {Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED},
		"failed": {
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			Err: temporal.NewApplicationError(
				"sanitized model failure", string(types.CodeLLMUnavailable)),
		},
		"terminated": {Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED},
	}}
	runner, err := NewRunner(st, inspector, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.RunStartup(t.Context()); err != nil {
		t.Fatal(err)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.claims) != 3 {
		t.Fatalf("finalized claims = %d, want 3", len(st.claims))
	}
	claims := make(map[int64]types.RunOutcomeClaimV1, len(st.claims))
	for _, claim := range st.claims {
		claims[claim.ID] = claim
	}
	if claim := claims[2]; claim.FailureCode !=
		"outcome_missing_terminal_receipt" ||
		claim.Result != types.RunResultFailed {
		t.Fatalf("completed claim = %+v", claim)
	}
	if claim := claims[3]; claim.FailureCode !=
		string(types.CodeLLMUnavailable) ||
		claim.FailureMessage != "sanitized model failure" {
		t.Fatalf("failed claim = %+v", claim)
	}
	if claim := claims[4]; claim.FailureCode != "workflow_terminated" ||
		claim.Result != types.RunResultInterrupted {
		t.Fatalf("terminated claim = %+v", claim)
	}
}

func TestRunnerBoundsTemporalConcurrencyAtFour(t *testing.T) {
	var candidates []store.RunOutcomeRecoveryCandidateV1
	executions := make(map[string]Execution)
	for i := int64(1); i <= 12; i++ {
		id := "wf-" + time.Unix(i, 0).Format("150405")
		candidates = append(candidates, recoveryCandidate(i, id))
		executions[id] = Execution{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		}
	}
	st := &recoveryStoreFake{candidates: candidates}
	inspector := &inspectorFake{
		executions: executions, delay: 20 * time.Millisecond,
	}
	runner, err := NewRunner(st, inspector, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.RunStartup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if peak := inspector.peak.Load(); peak != RecoveryConcurrency {
		t.Fatalf("peak query concurrency = %d, want %d",
			peak, RecoveryConcurrency)
	}
}

func TestRecoveryClaimDoesNotPersistUnknownFailureText(t *testing.T) {
	claim, terminal := recoveryClaim(
		recoveryCandidate(1, "wf").Marker,
		Execution{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			Err:    temporal.NewApplicationError("provider secret text", "ProviderError"),
		},
	)
	if !terminal || claim.FailureCode != "workflow_failed" ||
		claim.FailureMessage !=
			"workflow failed before a reliable terminal result" {
		t.Fatalf("unknown failure claim = %+v terminal=%t", claim, terminal)
	}
}
