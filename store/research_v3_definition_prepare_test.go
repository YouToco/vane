package store

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
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
	if beforeSpec != afterSpec || beforeScope != afterScope || beforeStatus != afterStatus || beforeMode != afterMode || beforeVersion != nil || afterVersion != nil || beforeDigest != nil || afterDigest != nil {
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
	op, err := st.BeginResearchV3Cutover(ctx, types.BeginResearchV3CutoverParams{TenantID: tenantID, UserID: userID, TaskID: taskID, IdempotencyKey: "cutover", Definition: prepared.Target, FrozenSchedule: frozen, FrozenScheduleDigest: digest(frozen), FrozenConflictToken: token, ConflictTokenDigest: digest(token), TargetAction: target, TargetActionDigest: digest(target), ActionAuthorizationDigest: strings.Repeat("0", 64)})
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
	if mode != "compiled" || status != "active" || restoredVersion != nil || restoredDigest != nil {
		t.Fatalf("restored schedule=%s/%s/%v/%v", mode, status, restoredVersion, restoredDigest)
	}
}
