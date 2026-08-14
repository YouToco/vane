package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/taskstate"
	"github.com/YouToco/vane/server/types"
)

func researchV3PreparePolicyForTest() taskstate.ResearchV3DefinitionPrepareParams {
	return taskstate.ResearchV3DefinitionPrepareParams{
		Notification: taskstate.NotificationPolicyV3{
			MinimumSignificance: taskstate.NotificationThresholdMajorV3,
			SuppressEmpty:       true,
		},
		Output: taskstate.OutputPreferenceV3{
			Language:             taskstate.OutputLanguageZhCNV3,
			Format:               taskstate.OutputFormatExecutiveBriefV3,
			IncludeEvidenceLinks: true,
		},
		PlannerBudget: types.PlannerBudget{
			MaxPlannerRounds: 8, MaxToolCalls: 16, MaxTokens: 32768,
			MaxCostMicroUSD: 1_000_000, DurationMs: 300_000,
		},
	}
}

func researchV3CutoverParamsForTest(tenantID, userID int64, taskID, key string,
	head types.ResearchV3DefinitionHead,
) types.BeginResearchV3CutoverParams {
	frozen, token, target := []byte("frozen-schedule"), []byte("conflict-token"), []byte("target-action")
	digest := func(payload []byte) string { sum := sha256.Sum256(payload); return hex.EncodeToString(sum[:]) }
	return types.BeginResearchV3CutoverParams{
		TenantID: tenantID, UserID: userID, TaskID: taskID, IdempotencyKey: key,
		Definition: head, FrozenSchedule: frozen, FrozenScheduleDigest: digest(frozen),
		FrozenConflictToken: token, ConflictTokenDigest: digest(token), TargetAction: target,
		TargetActionDigest: digest(target), ActionAuthorizationDigest: strings.Repeat("0", 64),
		OriginalScheduleStatus: types.ScheduleStatusActive,
		PreflightDigest:        strings.Repeat("1", 64),
	}
}

func researchV3PrepareFixture(t *testing.T) (*Store, int64, int64, string) {
	t.Helper()
	st := tenantTestStore(t)
	useOwnerResearchRuntimeForTest(st)
	userID := testUser(t, st)
	var tenantID int64
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO tenants(status,plan) VALUES ('active','free') RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`, tenantID, userID); err != nil {
		t.Fatal(err)
	}
	taskID := "v3-prepare-" + uuid.NewString()
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO schedules(id,tenant_id,user_id,nl_description,spec_json,scope_json,status,push_strictness)
		 VALUES($1,$2,$3,'Kimi 套餐可购买性','{"cron":"0 9 * * 1","tz":"Asia/Shanghai"}',
		        '{"top_n":5}','active','strict')`, taskID, tenantID, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO schedule_playbooks(schedule_id,content,fetch_plan)
		 VALUES($1,'检查 Kimi 官方套餐与购买入口；交叉核验；没有重大更新不推送。','{}')`, taskID); err != nil {
		t.Fatal(err)
	}
	call, err := taskstate.BuildToolInvocationV1(
		"web_search", "v1", json.RawMessage(`{"query":"Kimi official pricing"}`))
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := taskstate.BuildApprovedDefinitionV2(taskstate.ApprovedDefinitionInputV2{
		TenantID: tenantID, UserID: userID, TaskID: taskID,
		NLDescription: "Kimi 套餐可购买性",
		SpecJSON:      json.RawMessage(`{"cron":"0 9 * * 1","tz":"Asia/Shanghai"}`),
		ScopeJSON:     json.RawMessage(`{"top_n":5}`),
		TaskManual:    "检查 Kimi 官方套餐与购买入口；交叉核验；没有重大更新不推送。",
		Strictness:    types.StrictnessStrict, ToolCalls: []taskstate.ToolInvocationV1{call},
		ExecutionMode:  types.ExecutionModeCompiled,
		DeliveryPolicy: taskstate.DeliveryPolicyOwnerFeishu,
		BudgetPolicy:   taskstate.BudgetPolicyInheritTenantQuota,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := taskstate.EncodeApprovedDefinitionV2(legacy)
	if err != nil {
		t.Fatal(err)
	}
	legacyDigest := digestTaskStatePayload(payload)
	if _, err := st.pool.Exec(t.Context(), `INSERT INTO task_approved_definition_versions
		(tenant_id,user_id,task_id,version,schema_version,execution_mode,
		 definition_digest,payload,operation_ref)
		VALUES($1,$2,$3,1,$4,'compiled',$5,$6,$7)`, tenantID, userID, taskID,
		legacy.SchemaVersion, legacyDigest, payload, "v3-prepare-legacy:"+taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(), `UPDATE schedules SET
		approved_definition_version=1,approved_definition_digest=$1 WHERE id=$2`,
		legacyDigest, taskID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		if _, err := st.PurgeTenant(ctx, tenantID, false); err != nil {
			t.Errorf("purge prepare fixture: %v", err)
		}
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id=$1`, userID)
	})
	return st, tenantID, userID, taskID
}

func TestResearchV3DefinitionPrepareSidecarPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL is required")
	}
	st, tenantID, userID, taskID := researchV3PrepareFixture(t)
	ctx := t.Context()
	var beforeSpec, beforeScope, beforeStatus, beforeMode string
	var beforeVersion *int64
	var beforeDigest *string
	if err := st.pool.QueryRow(ctx,
		`SELECT spec_json::text,scope_json::text,status,execution_mode,
		        approved_definition_version,approved_definition_digest
		   FROM schedules WHERE id=$1`, taskID).Scan(&beforeSpec, &beforeScope, &beforeStatus, &beforeMode, &beforeVersion, &beforeDigest); err != nil {
		t.Fatal(err)
	}
	p := researchV3PreparePolicyForTest()
	p.TenantID, p.UserID, p.TaskID, p.IdempotencyKey = tenantID, userID, taskID, "prepare-1"
	op, err := st.PrepareResearchV3Definition(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	if op.OriginalMode != types.ExecutionModeCompiled || op.OriginalHead == nil ||
		op.OriginalHead.Version != *beforeVersion || op.OriginalHead.Digest != *beforeDigest ||
		!validDigestSyntaxV3(op.SourceBaselineDigest) {
		t.Fatalf("prepare did not freeze exact V2 head: %+v", op)
	}
	replayed, err := st.PrepareResearchV3Definition(ctx, p)
	if err != nil || replayed.ID != op.ID || replayed.Target != op.Target {
		t.Fatalf("replay=%+v err=%v", replayed, err)
	}
	head, err := st.LoadPreparedResearchV3DefinitionHead(ctx, tenantID, userID, taskID)
	if err != nil || head != op.Target {
		t.Fatalf("head=%+v op=%+v err=%v", head, op.Target, err)
	}
	var afterSpec, afterScope, afterStatus, afterMode string
	var afterVersion *int64
	var afterDigest *string
	if err := st.pool.QueryRow(ctx, `SELECT spec_json::text,scope_json::text,status,execution_mode,approved_definition_version,approved_definition_digest FROM schedules WHERE id=$1`, taskID).Scan(&afterSpec, &afterScope, &afterStatus, &afterMode, &afterVersion, &afterDigest); err != nil {
		t.Fatal(err)
	}
	if beforeSpec != afterSpec || beforeScope != afterScope || beforeStatus != afterStatus ||
		beforeMode != afterMode || beforeVersion == nil || afterVersion == nil ||
		*beforeVersion != *afterVersion || beforeDigest == nil || afterDigest == nil ||
		*beforeDigest != *afterDigest {
		t.Fatalf("prepare mutated production schedule: before=%q/%q/%q/%q after=%q/%q/%q/%q", beforeSpec, beforeScope, beforeStatus, beforeMode, afterSpec, afterScope, afterStatus, afterMode)
	}

	identity := types.RunIdentity{TemporalWorkflowID: "research-v3-shadow-" + strings.Repeat("0", 64), TemporalRunID: "run-" + uuid.NewString(), RunKind: types.RunSnapshotKindScheduled, TenantID: tenantID, UserID: userID, TaskID: taskID}
	ref, err := st.CreateOrGetResearchRunSnapshotV3(ctx, identity, testCompiledRunPolicyV1(t), testResearchToolPolicyStoreV3(t), testResearchModelPolicyStoreV3(t))
	if err != nil || ref.DefinitionVersion != head.Version || ref.DefinitionDigest != head.Digest || ref.AuthorityGeneration != 0 {
		t.Fatalf("shadow ref=%+v err=%v", ref, err)
	}

	rolledBack, err := st.RollbackResearchV3DefinitionPrepare(ctx, tenantID, userID, taskID, p.IdempotencyKey)
	if err != nil || rolledBack.Phase != types.ResearchV3DefinitionRolledBack {
		t.Fatalf("rollback=%+v err=%v", rolledBack, err)
	}
	if _, err := st.LoadPreparedResearchV3DefinitionHead(ctx, tenantID, userID, taskID); types.CodeOf(err) != types.CodeNotFound {
		t.Fatalf("rolled-back head err=%v", err)
	}
	var immutableCount int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM task_approved_definition_versions WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND version=$4`, tenantID, userID, taskID, head.Version).Scan(&immutableCount); err != nil || immutableCount != 1 {
		t.Fatalf("immutable definition count=%d err=%v", immutableCount, err)
	}
}

func TestResearchV3DefinitionPrepareRequiresOwnerPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL is required")
	}
	st, tenantID, userID, taskID := researchV3PrepareFixture(t)
	if _, err := st.pool.Exec(t.Context(), `UPDATE memberships SET role='member' WHERE tenant_id=$1 AND user_id=$2`, tenantID, userID); err != nil {
		t.Fatal(err)
	}
	p := researchV3PreparePolicyForTest()
	p.TenantID, p.UserID, p.TaskID, p.IdempotencyKey = tenantID, userID, taskID, "not-owner"
	if _, err := st.PrepareResearchV3Definition(t.Context(), p); types.CodeOf(err) != types.CodeNotFound {
		t.Fatalf("non-owner prepare err=%v", err)
	}
}

func TestResearchV3ShadowAdmissionFencesConcurrentOwnerRevocationPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL is required")
	}
	tests := []struct {
		name   string
		mutate string
	}{
		{name: "tenant_suspended", mutate: `UPDATE tenants SET status='suspended' WHERE id=$1`},
		{name: "owner_downgraded", mutate: `UPDATE memberships SET role='member' WHERE tenant_id=$1 AND user_id=$2`},
	}
	for index, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st, tenantID, userID, taskID := researchV3PrepareFixture(t)
			p := researchV3PreparePolicyForTest()
			p.TenantID, p.UserID, p.TaskID, p.IdempotencyKey =
				tenantID, userID, taskID, "admission-fence"
			if _, err := st.PrepareResearchV3Definition(t.Context(), p); err != nil {
				t.Fatal(err)
			}

			mutation, err := st.pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = mutation.Rollback(t.Context()) }()
			args := []any{tenantID}
			if tc.name == "owner_downgraded" {
				args = append(args, userID)
			}
			identity := types.RunIdentity{
				TemporalWorkflowID: "research-v3-shadow-" + strings.Repeat(string(rune('6'+index)), 64),
				TemporalRunID:      "admission-" + uuid.NewString(), RunKind: types.RunSnapshotKindScheduled,
				TenantID: tenantID, UserID: userID, TaskID: taskID,
			}
			policy := testCompiledRunPolicyV1(t)
			tools := testResearchToolPolicyStoreV3(t)
			model := testResearchModelPolicyStoreV3(t)
			mutationReady := make(chan error, 1)
			go func() {
				_, mutateErr := mutation.Exec(t.Context(), tc.mutate, args...)
				mutationReady <- mutateErr
			}()
			select {
			case mutateErr := <-mutationReady:
				if mutateErr != nil {
					t.Fatal(mutateErr)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("authorization mutation did not acquire its task fence")
			}
			result := make(chan error, 1)
			go func() {
				_, createErr := st.CreateOrGetResearchRunSnapshotV3(
					t.Context(), identity, policy, tools, model)
				result <- createErr
			}()

			deadline := time.Now().Add(5 * time.Second)
			blockedAtAdmission := false
			for !blockedAtAdmission && time.Now().Before(deadline) {
				select {
				case createErr := <-result:
					t.Fatalf("snapshot escaped concurrent authorization fence before commit: %v", createErr)
				default:
				}
				if err := st.pool.QueryRow(t.Context(), `SELECT EXISTS (
					SELECT 1 FROM pg_stat_activity
					 WHERE datname=current_database() AND pid<>pg_backend_pid()
					   AND state='active' AND wait_event_type='Lock' AND wait_event='advisory'
				)`).Scan(&blockedAtAdmission); err != nil {
					t.Fatal(err)
				}
				if !blockedAtAdmission {
					time.Sleep(10 * time.Millisecond)
				}
			}
			if !blockedAtAdmission {
				t.Fatal("snapshot did not reach the authorization admission lock")
			}
			if err := mutation.Commit(t.Context()); err != nil {
				t.Fatal(err)
			}
			select {
			case createErr := <-result:
				if createErr == nil {
					t.Fatal("snapshot was admitted after authorization revocation committed")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("snapshot remained blocked after authorization mutation committed")
			}
			var count int
			if err := st.pool.QueryRow(t.Context(), `SELECT count(*) FROM task_run_snapshots
				WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND temporal_run_id=$4`,
				tenantID, userID, taskID, identity.TemporalRunID).Scan(&count); err != nil || count != 0 {
				t.Fatalf("revoked shadow snapshot count=%d err=%v", count, err)
			}
		})
	}
}

func TestResearchV3PreparedBindingRejectsSourceDriftPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL is required")
	}
	tests := []struct {
		name   string
		mutate string
	}{
		{"name", `UPDATE schedules SET nl_description='changed name' WHERE id=$1`},
		{"manual", `UPDATE schedule_playbooks SET content='changed manual' WHERE schedule_id=$1`},
		{"spec", `UPDATE schedules SET spec_json='{"cron":"5 9 * * 1","tz":"Asia/Shanghai"}' WHERE id=$1`},
		{"policy", `UPDATE schedules SET push_strictness='loose' WHERE id=$1`},
		{"legacy head", `UPDATE schedules SET approved_definition_version=NULL,approved_definition_digest=NULL WHERE id=$1`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st, tenantID, userID, taskID := researchV3PrepareFixture(t)
			p := researchV3PreparePolicyForTest()
			p.TenantID, p.UserID, p.TaskID, p.IdempotencyKey = tenantID, userID, taskID, "drift"
			original, err := st.PrepareResearchV3Definition(t.Context(), p)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.pool.Exec(t.Context(), tc.mutate, taskID); err != nil {
				t.Fatal(err)
			}
			identity := types.RunIdentity{TemporalWorkflowID: "research-v3-shadow-" + strings.Repeat("1", 64), TemporalRunID: "run-" + uuid.NewString(), RunKind: types.RunSnapshotKindScheduled, TenantID: tenantID, UserID: userID, TaskID: taskID}
			if _, err := st.CreateOrGetResearchRunSnapshotV3(t.Context(), identity,
				testCompiledRunPolicyV1(t), testResearchToolPolicyStoreV3(t),
				testResearchModelPolicyStoreV3(t)); types.CodeOf(err) != types.CodeConflict {
				t.Fatalf("drift shadow err=%v", err)
			}
			var count int
			if err := st.pool.QueryRow(t.Context(), `SELECT count(*) FROM task_run_snapshots
				WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND temporal_run_id=$4`,
				tenantID, userID, taskID, identity.TemporalRunID).Scan(&count); err != nil || count != 0 {
				t.Fatalf("drift persisted snapshot count=%d err=%v", count, err)
			}
			p.IdempotencyKey = "drift-reprepare"
			reprepared, err := st.PrepareResearchV3Definition(t.Context(), p)
			if err != nil || reprepared.Target == original.Target ||
				reprepared.SourceBaselineDigest == original.SourceBaselineDigest {
				t.Fatalf("reprepare=%+v original=%+v err=%v", reprepared, original, err)
			}
			identity.TemporalWorkflowID = "research-v3-shadow-" + strings.Repeat("4", 64)
			identity.TemporalRunID = "run-" + uuid.NewString()
			ref, err := st.CreateOrGetResearchRunSnapshotV3(t.Context(), identity,
				testCompiledRunPolicyV1(t), testResearchToolPolicyStoreV3(t),
				testResearchModelPolicyStoreV3(t))
			if err != nil || ref.DefinitionVersion != reprepared.Target.Version ||
				ref.DefinitionDigest != reprepared.Target.Digest {
				t.Fatalf("reprepared shadow=%+v err=%v", ref, err)
			}
		})
	}
}

func TestResearchV3CutoverRevalidatesDriftAndFencesPrepareRollbackPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL is required")
	}
	st, tenantID, userID, taskID := researchV3PrepareFixture(t)
	p := researchV3PreparePolicyForTest()
	p.TenantID, p.UserID, p.TaskID, p.IdempotencyKey = tenantID, userID, taskID, "fenced"
	prepared, err := st.PrepareResearchV3Definition(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	op, err := st.BeginResearchV3Cutover(t.Context(),
		researchV3CutoverParamsForTest(tenantID, userID, taskID, "fenced-cutover", prepared.Target))
	if err != nil {
		t.Fatal(err)
	}
	op, err = st.AdvanceResearchV3Cutover(t.Context(), op,
		types.ResearchV3CutoverPrepared, types.ResearchV3CutoverPauseRequested)
	if err != nil {
		t.Fatal(err)
	}
	op, err = st.AdvanceResearchV3Cutover(t.Context(), op,
		types.ResearchV3CutoverPauseRequested, types.ResearchV3CutoverPaused)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.RollbackResearchV3DefinitionPrepare(t.Context(), tenantID, userID,
		taskID, p.IdempotencyKey); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("in-flight prepare rollback err=%v", err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`UPDATE schedules SET push_strictness='normal' WHERE id=$1`, taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PromoteResearchV3PreparedDefinition(t.Context(), op); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("post-pause drift promotion err=%v", err)
	}
	var mode string
	var version int64
	if err := st.pool.QueryRow(t.Context(), `SELECT execution_mode,approved_definition_version
		FROM schedules WHERE id=$1`, taskID).Scan(&mode, &version); err != nil {
		t.Fatal(err)
	}
	if mode != "compiled" || version != 1 {
		t.Fatalf("failed promotion mutated legacy head: mode=%s version=%d", mode, version)
	}
}

func TestResearchV3CutoverBeginRejectsDriftAfterSuccessfulShadowPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL is required")
	}
	st, tenantID, userID, taskID := researchV3PrepareFixture(t)
	p := researchV3PreparePolicyForTest()
	p.TenantID, p.UserID, p.TaskID, p.IdempotencyKey = tenantID, userID, taskID, "shadow-then-drift"
	p.ResearchScope = &taskstate.ResearchScopeV3{
		Mode:            taskstate.ResearchScopeEventWindowV3,
		LookbackSeconds: taskstate.ResearchScopeWeekSecondsV3,
	}
	prepared, err := st.PrepareResearchV3Definition(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	identity := types.RunIdentity{TemporalWorkflowID: "research-v3-shadow-" + strings.Repeat("3", 64),
		TemporalRunID: "run-" + uuid.NewString(), RunKind: types.RunSnapshotKindScheduled,
		TenantID: tenantID, UserID: userID, TaskID: taskID}
	if _, err := st.CreateOrGetResearchRunSnapshotV3(t.Context(), identity,
		testCompiledRunPolicyV1(t), testResearchToolPolicyStoreV3(t),
		testScopedResearchGroundingModelPolicyV36Base(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(), `UPDATE schedule_playbooks
		SET content='manual edited after shadow' WHERE schedule_id=$1`, taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.BeginResearchV3Cutover(t.Context(),
		researchV3CutoverParamsForTest(tenantID, userID, taskID, "begin-after-drift", prepared.Target)); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("begin after drift err=%v", err)
	}
	var count int
	if err := st.pool.QueryRow(t.Context(), `SELECT count(*) FROM research_v3_cutover_operations
		WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`, tenantID, userID, taskID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("drift began cutover count=%d err=%v", count, err)
	}
}

func TestResearchV3PrepareJournalDoesNotBlockTaskDeletionPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL is required")
	}
	for _, rolledBack := range []bool{false, true} {
		name := "prepared"
		if rolledBack {
			name = "rolled_back"
		}
		t.Run(name, func(t *testing.T) {
			st, tenantID, userID, taskID := researchV3PrepareFixture(t)
			p := researchV3PreparePolicyForTest()
			p.TenantID, p.UserID, p.TaskID, p.IdempotencyKey = tenantID, userID, taskID, "delete"
			if _, err := st.PrepareResearchV3Definition(t.Context(), p); err != nil {
				t.Fatal(err)
			}
			if rolledBack {
				if _, err := st.RollbackResearchV3DefinitionPrepare(
					t.Context(), tenantID, userID, taskID, p.IdempotencyKey); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := st.pool.Exec(t.Context(), `DELETE FROM schedules
				WHERE tenant_id=$1 AND user_id=$2 AND id=$3`, tenantID, userID, taskID); err != nil {
				t.Fatalf("delete task with %s prepare journal: %v", name, err)
			}
			var operations, definitions int
			if err := st.pool.QueryRow(t.Context(), `SELECT
				(SELECT count(*) FROM research_v3_definition_prepare_operations
				 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3),
				(SELECT count(*) FROM task_approved_definition_versions
				 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3)`, tenantID, userID, taskID).Scan(
				&operations, &definitions); err != nil || operations != 0 || definitions != 0 {
				t.Fatalf("delete cascade operations=%d definitions=%d err=%v",
					operations, definitions, err)
			}
		})
	}
}

func TestResearchV3CutoverPromotesAndRestoresDefinitionHeadPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL is required")
	}
	st, tenantID, userID, taskID := researchV3PrepareFixture(t)
	ctx := t.Context()
	p := researchV3PreparePolicyForTest()
	p.TenantID, p.UserID, p.TaskID, p.IdempotencyKey = tenantID, userID, taskID, "cutover-prepare"
	prepared, err := st.PrepareResearchV3Definition(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	frozen, token, target := []byte("frozen-schedule"), []byte("conflict-token"), []byte("target-action")
	digest := func(payload []byte) string { sum := sha256.Sum256(payload); return hex.EncodeToString(sum[:]) }
	op, err := st.BeginResearchV3Cutover(ctx, types.BeginResearchV3CutoverParams{TenantID: tenantID, UserID: userID, TaskID: taskID, IdempotencyKey: "cutover", Definition: prepared.Target, FrozenSchedule: frozen, FrozenScheduleDigest: digest(frozen), FrozenConflictToken: token, ConflictTokenDigest: digest(token), TargetAction: target, TargetActionDigest: digest(target), ActionAuthorizationDigest: strings.Repeat("0", 64), OriginalScheduleStatus: types.ScheduleStatusActive, PreflightDigest: strings.Repeat("1", 64)})
	if err != nil {
		t.Fatal(err)
	}
	op, err = st.AdvanceResearchV3Cutover(ctx, op, types.ResearchV3CutoverPrepared, types.ResearchV3CutoverPauseRequested)
	if err != nil {
		t.Fatal(err)
	}
	op, err = st.AdvanceResearchV3Cutover(ctx, op, types.ResearchV3CutoverPauseRequested, types.ResearchV3CutoverPaused)
	if err != nil {
		t.Fatal(err)
	}
	op, err = st.PromoteResearchV3PreparedDefinition(ctx, op)
	if err != nil || op.Phase != types.ResearchV3CutoverDefinitionPromoted {
		t.Fatalf("promotion=%+v err=%v", op, err)
	}
	var mode, status, spec string
	var version int64
	var headDigest string
	if err := st.pool.QueryRow(ctx, `SELECT execution_mode,status,spec_json::text,approved_definition_version,approved_definition_digest FROM schedules WHERE id=$1`, taskID).Scan(&mode, &status, &spec, &version, &headDigest); err != nil {
		t.Fatal(err)
	}
	if mode != "discover_at_run" || status != "active" || spec != `{"tz": "Asia/Shanghai", "cron": "0 9 * * 1"}` || version != prepared.Target.Version || headDigest != prepared.Target.Digest {
		t.Fatalf("promoted schedule=%s/%s/%s/%d/%s", mode, status, spec, version, headDigest)
	}
	if err := st.RecheckResearchV3CutoverDefinition(ctx, op); err != nil {
		t.Fatalf("post-promotion stage-aware recheck: %v", err)
	}
	shadowIdentity := types.RunIdentity{TemporalWorkflowID: "research-v3-shadow-" + strings.Repeat("5", 64),
		TemporalRunID: "post-promote-" + uuid.NewString(), RunKind: types.RunSnapshotKindScheduled,
		TenantID: tenantID, UserID: userID, TaskID: taskID}
	ref, err := st.CreateOrGetResearchRunSnapshotV3(ctx, shadowIdentity,
		testCompiledRunPolicyV1(t), testResearchToolPolicyStoreV3(t),
		testResearchModelPolicyStoreV3(t))
	if err != nil || ref.DefinitionDigest != prepared.Target.Digest {
		t.Fatalf("post-promotion delivery-dark shadow=%+v err=%v", ref, err)
	}
	if err := st.RevokeResearchV3DeliveryAuthority(ctx, op); err != nil {
		t.Fatal(err)
	}
	op, err = st.AdvanceResearchV3Cutover(ctx, op, types.ResearchV3CutoverDefinitionPromoted, types.ResearchV3CutoverRollbackPaused)
	if err != nil {
		t.Fatal(err)
	}
	op, err = st.RestoreResearchV3OriginalDefinition(ctx, op)
	if err != nil || op.Phase != types.ResearchV3CutoverDefinitionRestored {
		t.Fatalf("restore=%+v err=%v", op, err)
	}
	var restoredVersion *int64
	var restoredDigest *string
	if err := st.pool.QueryRow(ctx, `SELECT execution_mode,status,spec_json::text,approved_definition_version,approved_definition_digest FROM schedules WHERE id=$1`, taskID).Scan(&mode, &status, &spec, &restoredVersion, &restoredDigest); err != nil {
		t.Fatal(err)
	}
	if mode != "compiled" || status != "active" || restoredVersion == nil ||
		restoredDigest == nil || *restoredVersion != 1 ||
		*restoredDigest != prepared.OriginalHead.Digest {
		t.Fatalf("restored schedule=%s/%s/%v/%v", mode, status, restoredVersion, restoredDigest)
	}
}

func TestResearchV3CutoverKeepsLegacySnapshotPinDormantAndReversiblePostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL is required")
	}
	fixture := newTaskRunSnapshotCutoverFixture(t)
	st := fixture.base.st
	useOwnerResearchRuntimeForTest(st)
	ctx := t.Context()

	policy := researchV3PreparePolicyForTest()
	policy.TenantID = fixture.base.tenantID
	policy.UserID = fixture.base.userID
	policy.TaskID = fixture.taskID
	policy.IdempotencyKey = "dormant-pin-prepare"
	prepared, err := st.PrepareResearchV3Definition(ctx, policy)
	if err != nil {
		t.Fatal(err)
	}
	params := researchV3CutoverParamsForTest(
		fixture.base.tenantID, fixture.base.userID, fixture.taskID,
		"dormant-pin-cutover", prepared.Target)
	op, err := st.BeginResearchV3Cutover(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	op, err = st.AdvanceResearchV3Cutover(ctx, op,
		types.ResearchV3CutoverPrepared, types.ResearchV3CutoverPauseRequested)
	if err != nil {
		t.Fatal(err)
	}
	op, err = st.AdvanceResearchV3Cutover(ctx, op,
		types.ResearchV3CutoverPauseRequested, types.ResearchV3CutoverPaused)
	if err != nil {
		t.Fatal(err)
	}
	op, err = st.PromoteResearchV3PreparedDefinition(ctx, op)
	if err != nil {
		t.Fatalf("promote with dormant V2 pin: %v", err)
	}
	var mode, action string
	var pointer int64
	if err := st.pool.QueryRow(ctx, `SELECT s.execution_mode,
		s.run_snapshot_cutover_event_id,e.action
		FROM schedules s JOIN task_run_snapshot_v2_cutover_events e
		  ON e.id=s.run_snapshot_cutover_event_id
		WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3`,
		fixture.base.tenantID, fixture.base.userID, fixture.taskID,
	).Scan(&mode, &pointer, &action); err != nil {
		t.Fatal(err)
	}
	if mode != string(types.ExecutionModeDiscoverAtRun) ||
		pointer != fixture.eventID || action != "activate" {
		t.Fatalf("promoted dormant pin mode=%s pointer=%d action=%s",
			mode, pointer, action)
	}

	if err := st.RevokeResearchV3DeliveryAuthority(ctx, op); err != nil {
		t.Fatal(err)
	}
	op, err = st.AdvanceResearchV3Cutover(ctx, op,
		types.ResearchV3CutoverDefinitionPromoted,
		types.ResearchV3CutoverRollbackPaused)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = st.RestoreResearchV3OriginalDefinition(ctx, op); err != nil {
		t.Fatalf("restore dormant V2 pin: %v", err)
	}
	if err := st.pool.QueryRow(ctx, `SELECT s.execution_mode,
		s.run_snapshot_cutover_event_id,e.action
		FROM schedules s JOIN task_run_snapshot_v2_cutover_events e
		  ON e.id=s.run_snapshot_cutover_event_id
		WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3`,
		fixture.base.tenantID, fixture.base.userID, fixture.taskID,
	).Scan(&mode, &pointer, &action); err != nil {
		t.Fatal(err)
	}
	if mode != string(types.ExecutionModeCompiled) ||
		pointer != fixture.eventID || action != "activate" {
		t.Fatalf("restored active pin mode=%s pointer=%d action=%s",
			mode, pointer, action)
	}
}

func TestResearchV3PausedCutoverPreservesPausedSchedulePostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL is required")
	}
	st, tenantID, userID, taskID := researchV3PrepareFixture(t)
	if _, err := st.pool.Exec(t.Context(),
		`UPDATE schedules SET status='paused' WHERE id=$1`, taskID); err != nil {
		t.Fatal(err)
	}
	policy := researchV3PreparePolicyForTest()
	policy.TenantID, policy.UserID, policy.TaskID = tenantID, userID, taskID
	policy.IdempotencyKey = "paused-prepare"
	prepared, err := st.PrepareResearchV3Definition(t.Context(), policy)
	if err != nil {
		t.Fatal(err)
	}
	params := researchV3CutoverParamsForTest(
		tenantID, userID, taskID, "paused-cutover", prepared.Target)
	params.OriginalPaused = true
	params.OriginalScheduleStatus = types.ScheduleStatusPaused
	op, err := st.BeginResearchV3Cutover(t.Context(), params)
	if err != nil {
		t.Fatal(err)
	}
	op, err = st.AdvanceResearchV3Cutover(t.Context(), op,
		types.ResearchV3CutoverPrepared, types.ResearchV3CutoverPauseRequested)
	if err != nil {
		t.Fatal(err)
	}
	op, err = st.AdvanceResearchV3Cutover(t.Context(), op,
		types.ResearchV3CutoverPauseRequested, types.ResearchV3CutoverPaused)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.PromoteResearchV3PreparedDefinition(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	var status, mode, spec string
	if err := st.pool.QueryRow(t.Context(), `SELECT status,execution_mode,spec_json::text
		FROM schedules WHERE id=$1`, taskID).Scan(&status, &mode, &spec); err != nil {
		t.Fatal(err)
	}
	if status != "paused" || mode != "discover_at_run" ||
		spec != `{"tz": "Asia/Shanghai", "cron": "0 9 * * 1"}` {
		t.Fatalf("paused promotion status=%s mode=%s spec=%s", status, mode, spec)
	}
}
