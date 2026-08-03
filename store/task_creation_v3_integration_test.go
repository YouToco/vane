package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func TestNativeResearchTaskCreationV3PostgreSQLAtomicLifecycle(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	if err := Migrate(t.Context(), scratchURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)

	user, err := st.UpsertUserByOpenID(t.Context(), "native-v3-"+uuid.NewString(), "owner")
	if err != nil {
		t.Fatal(err)
	}
	var tenantID int64
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO tenants(status,plan) VALUES ('active','free') RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`,
		tenantID, user.ID); err != nil {
		t.Fatal(err)
	}

	operationID := "native-v3-" + uuid.NewString()
	taskID := nativeResearchTaskIDV3Test(tenantID, user.ID, operationID)
	definition, err := taskstate.BuildApprovedDefinitionV3(taskstate.ApprovedDefinitionInputV3{
		TenantID: tenantID, UserID: user.ID, TaskID: taskID,
		TaskName:      "Kimi 套餐可购买状态",
		TaskManual:    "检查 Kimi 官方套餐页，交叉核验并和历史结论比较；没有重大更新不推送。",
		SpecJSON:      json.RawMessage(`{"tz":"Asia/Shanghai","cron":"0 9 * * 1"}`),
		ExecutionMode: types.ExecutionModeDiscoverAtRun,
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
		DeliveryPolicy:     taskstate.DeliveryPolicyOwnerFeishu,
		TenantBudgetPolicy: taskstate.BudgetPolicyInheritTenantQuota,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := taskstate.EncodeApprovedDefinitionV3(definition)
	digest, _ := taskstate.DigestApprovedDefinitionV3(definition)
	createParams := types.CreateResearchTaskCreationOperationV3Params{
		ID: operationID, TenantID: tenantID, UserID: user.ID,
		Args: payload, Summary: definition.TaskName,
		ExpiresAt: time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond),
	}
	op, err := st.CreateResearchTaskCreationOperationV3(t.Context(), createParams)
	if err != nil || op.ExecutionVersion != types.TaskCreationExecutionVersionV2 {
		t.Fatalf("operation=%+v err=%v", op, err)
	}
	if replay, err := st.CreateResearchTaskCreationOperationV3(
		t.Context(), createParams); err != nil || replay.ID != op.ID {
		t.Fatalf("operation replay=%+v err=%v", replay, err)
	}
	prepared, receipt, action := []byte(`{"prepared":true}`), []byte(`{"ensured":true}`), []byte(`{"runtime":"research-v3"}`)
	leaseOwner := "native-v3-test-owner"
	if _, err := st.pool.Exec(t.Context(), `
		UPDATE task_creation_operations
		   SET status='executing',phase='schedule_ensured',lease_owner=$2,
		       lease_until=clock_timestamp()+interval '1 hour',
		       takeover_not_before=clock_timestamp()+interval '2 hours',
		       fence=1,attempt=1,compiled_definition=$3,compiled_digest=$4,
		       prepared_schedule=$5,ensure_receipt=$6,task_id=$7
		 WHERE id=$1 AND execution_version=2`, operationID, leaseOwner, payload,
		digest, prepared, receipt, taskID); err != nil {
		t.Fatal(err)
	}
	lease := types.TaskCreationLease{
		ID: operationID, TenantID: tenantID, UserID: user.ID,
		LeaseOwner: leaseOwner, Fence: 1,
	}
	params := types.CommitPausedResearchTaskDefinitionV3ForCreationParams{
		Lease: lease, TaskID: taskID, DefinitionPayload: payload,
		DefinitionDigest: digest, PreparedSchedule: prepared, EnsureReceipt: receipt,
		TargetAction: action, TargetActionDigest: hexDigestV3Test(action),
		ActionAuthorizationDigest: hexDigestV3Test([]byte("authorization")),
	}
	if err := st.CommitPausedResearchTaskDefinitionV3ForCreation(t.Context(), params); err != nil {
		t.Fatal(err)
	}
	// Response-loss replay must adopt the exact four-row aggregate.
	if err := st.CommitPausedResearchTaskDefinitionV3ForCreation(t.Context(), params); err != nil {
		t.Fatalf("commit replay: %v", err)
	}

	var status, mode, playbook, authorityStatus string
	var version, authorityCount, fetchTargetCount int64
	if err := st.pool.QueryRow(t.Context(), `
		SELECT schedule.status,schedule.execution_mode,playbook.content,
		       schedule.approved_definition_version,
		       (SELECT count(*) FROM research_v3_delivery_authorities authority
		         WHERE authority.tenant_id=schedule.tenant_id AND authority.user_id=schedule.user_id
		           AND authority.task_id=schedule.id AND authority.status='staged'),
		       (SELECT count(*) FROM task_fetch_targets target WHERE target.schedule_id=schedule.id)
		  FROM schedules schedule JOIN schedule_playbooks playbook ON playbook.schedule_id=schedule.id
		 WHERE schedule.id=$1`, taskID).Scan(
		&status, &mode, &playbook, &version, &authorityCount, &fetchTargetCount); err != nil {
		t.Fatal(err)
	}
	if status != "paused" || mode != "discover_at_run" || version != 1 ||
		playbook != definition.TaskManual || authorityCount != 1 || fetchTargetCount != 0 {
		t.Fatalf("paused aggregate status=%s mode=%s version=%d authority=%d targets=%d playbook=%q",
			status, mode, version, authorityCount, fetchTargetCount, playbook)
	}

	started, err := st.BeginResearchTaskCreationActivationV3(t.Context(), lease, taskID)
	if err != nil || !started {
		t.Fatalf("begin activation started=%v err=%v", started, err)
	}
	started, err = st.BeginResearchTaskCreationActivationV3(t.Context(), lease, taskID)
	if err != nil || started {
		t.Fatalf("begin replay started=%v err=%v", started, err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`UPDATE tenants SET status='suspended' WHERE id=$1`, tenantID); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitResearchTaskCreationActivationV3(
		t.Context(), lease, taskID); err == nil {
		t.Fatal("activation enabled a task after owner scope was suspended")
	}
	if err := st.pool.QueryRow(t.Context(), `
		SELECT schedule.status,authority.status
		  FROM schedules schedule JOIN research_v3_delivery_authorities authority
		    ON authority.tenant_id=schedule.tenant_id AND authority.user_id=schedule.user_id
		   AND authority.task_id=schedule.id AND authority.generation=1
		 WHERE schedule.id=$1`, taskID).Scan(&status, &authorityStatus); err != nil {
		t.Fatal(err)
	}
	if status != "paused" || authorityStatus != "staged" {
		t.Fatalf("failed activation was not atomic: schedule=%s authority=%s", status, authorityStatus)
	}
	if _, err := st.pool.Exec(t.Context(),
		`UPDATE tenants SET status='active' WHERE id=$1`, tenantID); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitResearchTaskCreationActivationV3(t.Context(), lease, taskID); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitResearchTaskCreationActivationV3(t.Context(), lease, taskID); err != nil {
		t.Fatalf("activation replay: %v", err)
	}
	var operationPhase string
	if err := st.pool.QueryRow(t.Context(), `
		SELECT operation.phase,schedule.status,authority.status
		  FROM task_creation_operations operation
		  JOIN schedules schedule ON schedule.id=operation.task_id
		  JOIN research_v3_delivery_authorities authority
		    ON authority.tenant_id=operation.tenant_id AND authority.user_id=operation.user_id
		   AND authority.task_id=operation.task_id AND authority.generation=1
		 WHERE operation.id=$1`, operationID).Scan(
		&operationPhase, &status, &authorityStatus); err != nil {
		t.Fatal(err)
	}
	if operationPhase != "activated" || status != "active" || authorityStatus != "enabled" {
		t.Fatalf("activation was not atomic: phase=%s schedule=%s authority=%s",
			operationPhase, status, authorityStatus)
	}
}

func hexDigestV3Test(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func nativeResearchTaskIDV3Test(tenantID, userID int64, operationID string) string {
	payload, _ := json.Marshal(struct {
		Version     string `json:"version"`
		TenantID    int64  `json:"tenant_id"`
		UserID      int64  `json:"user_id"`
		OperationID string `json:"operation_id"`
	}{"v1", tenantID, userID, operationID})
	return "task-v1-" + hexDigestV3Test(payload)
}
