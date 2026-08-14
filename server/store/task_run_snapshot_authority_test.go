package store

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/runtimepolicy"
	"github.com/YouToco/vane/server/types"
)

func TestAuthoritativeCompiledTaskRunSnapshotMarkerSelectsImmutableBody(
	t *testing.T,
) {
	f := newTaskRunSnapshotCutoverFixture(t)

	canaryExpected, canaryRef := cutoverRunIdentityAndRef(
		t, f.canary)
	_, authority, err :=
		f.base.st.LoadAuthoritativeCompiledTaskRunSnapshot(
			t.Context(), canaryExpected, canaryRef)
	if err != nil {
		t.Fatalf("load NULL-marker audit shadow: %v", err)
	}
	if authority != CompiledRunSnapshotAuthorityV1 {
		t.Fatalf("NULL-marker authority = %q, want %q",
			authority, CompiledRunSnapshotAuthorityV1)
	}

	eventID := f.eventID
	markedParams := authoritativeSnapshotParams(
		t, f,
		"authority-marked-"+uuid.NewString())
	marked, err := f.base.st.createOrGetTaskRunSnapshotWithAuthorityV2(
		t.Context(),
		markedParams,
		true, &eventID,
	)
	if err != nil {
		t.Fatalf("create marked run: %v", err)
	}
	markedExpected, markedRef := cutoverRunIdentityAndRef(t, marked)
	markedBody, authority, err :=
		f.base.st.LoadAuthoritativeCompiledTaskRunSnapshot(
			t.Context(), markedExpected, markedRef)
	if err != nil {
		t.Fatalf("load marked run: %v", err)
	}
	if authority != CompiledRunSnapshotAuthorityV2 {
		t.Fatalf("marked authority = %q, want %q",
			authority, CompiledRunSnapshotAuthorityV2)
	}
	retainedMarked, err := f.base.st.LoadCompiledTaskRunSnapshotV1(
		t.Context(), markedExpected, markedRef)
	if err != nil {
		t.Fatalf("load retained v1 comparison body: %v", err)
	}
	if !compiledSnapshotV1ExactEqual(retainedMarked, markedBody) {
		t.Fatal("controlled match v2 did not materialize the exact typed body")
	}

	tx, err := f.base.st.beginTx(
		t.Context(), pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackCompiledTaskTx(t.Context(), tx)
	if len(markedBody.Definition.Sources) != 1 {
		t.Fatalf("marked sources = %d, want 1",
			len(markedBody.Definition.Sources))
	}
	sourceID := markedBody.Definition.Sources[0].SourceID
	frozen, err := loadAuthoritativeTaskRunSource(
		t.Context(), tx, markedExpected, markedRef, sourceID)
	if err != nil {
		t.Fatalf("load marked source through side-effect resolver: %v", err)
	}
	if frozen.SourceID != sourceID {
		t.Fatalf("authoritative source id = %d, want %d",
			frozen.SourceID, sourceID)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}

	insertRollbackEvent(t, f, f.eventID, 2)
	if err := f.base.st.DeleteSchedule(
		t.Context(), f.taskID, f.base.userID); err != nil {
		t.Fatalf("delete rolled-back schedule: %v", err)
	}
	afterDelete, authority, err :=
		f.base.st.LoadAuthoritativeCompiledTaskRunSnapshot(
			t.Context(), markedExpected, markedRef)
	if err != nil {
		t.Fatalf("load marked run after rollback/delete: %v", err)
	}
	if authority != CompiledRunSnapshotAuthorityV2 ||
		!compiledSnapshotV1ExactEqual(markedBody, afterDelete) {
		t.Fatalf("historical marked authority/body drifted after rollback/delete: %q",
			authority)
	}
}

func TestAuthoritativeCompiledTaskRunSnapshotFailsClosedOnlyWhenMarked(
	t *testing.T,
) {
	t.Run("NULL marker ignores missing audit shadow", func(t *testing.T) {
		f := newTaskRunSnapshotCutoverFixture(t)
		expected, ref := cutoverRunIdentityAndRef(t, f.canary)
		if _, err := f.base.st.pool.Exec(t.Context(),
			`DELETE FROM task_run_snapshot_v2_shadows
			  WHERE run_snapshot_id=$1`,
			f.canary.ID); err != nil {
			t.Fatal(err)
		}
		_, authority, err :=
			f.base.st.LoadAuthoritativeCompiledTaskRunSnapshot(
				t.Context(), expected, ref)
		if err != nil {
			t.Fatalf("NULL-marker load depended on audit sidecar: %v", err)
		}
		if authority != CompiledRunSnapshotAuthorityV1 {
			t.Fatalf("NULL-marker authority = %q", authority)
		}
	})

	t.Run("marker requires exact retained v2", func(t *testing.T) {
		f := newTaskRunSnapshotCutoverFixture(t)
		eventID := f.eventID
		markedParams := authoritativeSnapshotParams(
			t, f,
			"authority-corrupt-"+uuid.NewString())
		marked, err := f.base.st.createOrGetTaskRunSnapshotWithAuthorityV2(
			t.Context(),
			markedParams,
			true, &eventID,
		)
		if err != nil {
			t.Fatal(err)
		}
		expected, ref := cutoverRunIdentityAndRef(t, marked)
		if _, err := f.base.st.pool.Exec(t.Context(),
			`DELETE FROM task_run_snapshot_v2_shadows
			  WHERE run_snapshot_id=$1`,
			marked.ID); err != nil {
			t.Fatal(err)
		}
		_, _, err =
			f.base.st.LoadAuthoritativeCompiledTaskRunSnapshot(
				t.Context(), expected, ref)
		assertAppCode(t, err, types.CodeInternal)
	})
}

func authoritativeSnapshotParams(
	t *testing.T,
	f taskRunSnapshotCutoverFixture,
	runID string,
) CreateOrGetTaskRunSnapshotParams {
	t.Helper()
	policy := testCompiledRunPolicyV1(t)
	capabilityCatalog, capabilityErr :=
		runtimepolicy.EncodeCapabilityCatalogV1(policy.CapabilityCatalog)
	toolPolicy, toolErr := runtimepolicy.EncodeToolPolicyV1(policy.ToolPolicy)
	promptPolicy, promptErr :=
		runtimepolicy.EncodePromptPolicyV1(policy.PromptPolicy)
	modelPolicy, modelErr := runtimepolicy.EncodeModelPolicyV1(policy.ModelPolicy)
	quotaPolicy, quotaErr := runtimepolicy.EncodeQuotaPolicyV1(policy.QuotaPolicy)
	if capabilityErr != nil || toolErr != nil || promptErr != nil ||
		modelErr != nil || quotaErr != nil {
		t.Fatal("encode authoritative test policy")
	}
	budget, err := json.Marshal(types.PlannerBudget{})
	if err != nil {
		t.Fatal(err)
	}
	return CreateOrGetTaskRunSnapshotParams{
		TenantID:              f.base.tenantID,
		UserID:                f.base.userID,
		TaskID:                f.taskID,
		TemporalWorkflowID:    scheduledTaskWorkflowID(f.taskID),
		TemporalRunID:         runID,
		Mode:                  types.ExecutionModeCompiled,
		AdaptiveVersion:       0,
		CapabilityCatalogJSON: capabilityCatalog,
		ToolPolicyJSON:        toolPolicy,
		PromptPolicyJSON:      promptPolicy,
		ModelPolicyJSON:       modelPolicy,
		QuotaPolicyJSON:       quotaPolicy,
		BudgetJSON:            budget,
	}
}

func cutoverRunIdentityAndRef(
	t *testing.T,
	snapshot *taskRunSnapshot,
) (types.RunIdentity, types.RunSnapshotRef) {
	t.Helper()
	ref, err := snapshot.safeRef()
	if err != nil {
		t.Fatalf("seal cutover run reference: %v", err)
	}
	return types.RunIdentity{
		TemporalWorkflowID: snapshot.TemporalWorkflowID,
		TemporalRunID:      snapshot.TemporalRunID,
		RunKind:            snapshot.RunKind,
		TenantID:           snapshot.TenantID,
		UserID:             snapshot.UserID,
		TaskID:             snapshot.TaskID,
	}, ref
}
