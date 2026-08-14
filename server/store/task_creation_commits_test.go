package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/acquisitiontool"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

var errInjectedTaskCreationCommit = errors.New("injected task creation commit failure")

func TestCreateTaskCreationOperation_V1Boundary(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	ctx := t.Context()
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_creation_receipts WHERE tenant_id = $1`, f.tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_creation_operations WHERE tenant_id = $1`, f.tenantID)
	})

	p := taskCreationCreateParams(f, uuid.NewString())
	op, err := st.CreateTaskCreationOperation(ctx, p)
	if err != nil {
		t.Fatalf("CreateTaskCreationOperation() 失败: %v", err)
	}
	if op.ExecutionVersion != types.TaskCreationExecutionVersionV1 ||
		op.ToolName != "create_schedule" || op.Status != types.TaskOperationStatusPending ||
		op.Phase != "" || op.Fence != 0 || op.TenantID != f.tenantID ||
		op.UserID != f.userID {
		t.Fatalf("v1 pending 初态错误: %+v", op)
	}
	replayed, err := st.CreateTaskCreationOperation(ctx, p)
	if err != nil || replayed.ID != op.ID {
		t.Fatalf("exact create replay 应采用原行: op=%+v err=%v", replayed, err)
	}
	different := p
	different.Summary = "不同摘要"
	if _, err := st.CreateTaskCreationOperation(ctx, different); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("相同 id 不同 payload 必须 conflict: %v", err)
	}
	unchanged, err := st.LoadTaskCreationOperation(ctx, op.ID, f.tenantID, f.userID)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Status != types.TaskOperationStatusPending || unchanged.Phase != "" ||
		unchanged.TombstonedAt != nil || unchanged.Fence != 0 || unchanged.Attempt != 0 {
		t.Fatalf("v1 初态必须逐字段不变: %+v", unchanged)
	}

	duplicate := taskCreationCreateParams(f, uuid.NewString())
	duplicate.Args = json.RawMessage(`{"intent":"a","intent":"b"}`)
	if _, err := st.CreateTaskCreationOperation(ctx, duplicate); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("重复 JSON key 必须被拒绝: %v", err)
	}
	var duplicateRows int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_creation_operations WHERE id = $1`, duplicate.ID,
	).Scan(&duplicateRows); err != nil {
		t.Fatal(err)
	}
	if duplicateRows != 0 {
		t.Fatalf("非法 args 不得落行: %d", duplicateRows)
	}
	invalidBoundaries := []struct {
		name   string
		mutate func(*types.CreateTaskCreationOperationParams)
	}{
		{"oversized args", func(p *types.CreateTaskCreationOperationParams) {
			p.Args = json.RawMessage(`{"payload":"` + string(make([]byte, maxTaskCreationArgsBytes)) + `"}`)
		}},
		{"empty summary", func(p *types.CreateTaskCreationOperationParams) { p.Summary = "" }},
		{"summary outer whitespace", func(p *types.CreateTaskCreationOperationParams) { p.Summary = " bad " }},
		{"oversized summary", func(p *types.CreateTaskCreationOperationParams) {
			p.Summary = string(make([]byte, maxTaskCreationSummaryBytes+1))
		}},
		{"invalid utf8 summary", func(p *types.CreateTaskCreationOperationParams) {
			p.Summary = string([]byte{0xff})
		}},
	}
	for _, tc := range invalidBoundaries {
		t.Run(tc.name, func(t *testing.T) {
			candidate := taskCreationCreateParams(f, uuid.NewString())
			tc.mutate(&candidate)
			if _, err := st.CreateTaskCreationOperation(ctx, candidate); !errors.Is(err, types.ErrValidation) {
				t.Fatalf("非法入库边界必须拒绝: %v", err)
			}
		})
	}

	sessionOwner := testUser(t, st)
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO memberships (tenant_id, user_id, role) VALUES ($1, $2, 'member')`,
		f.tenantID, sessionOwner); err != nil {
		t.Fatal(err)
	}
	session, err := st.CreateAgentSession(ctx, sessionOwner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_creation_receipts WHERE tenant_id = $1`, f.tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM agent_sessions WHERE user_id = $1`, sessionOwner)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM memberships WHERE tenant_id = $1 AND user_id = $2`,
			f.tenantID, sessionOwner)
		cleanupExec(cleanupCtx, t, st, `DELETE FROM users WHERE id = $1`, sessionOwner)
	})
	crossSession := taskCreationCreateParams(f, uuid.NewString())
	crossSession.SessionID = &session.ID
	if _, err := st.CreateTaskCreationOperation(ctx, crossSession); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("跨用户 session 必须拒绝: %v", err)
	}

	orphan := testUser(t, st)
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st, `DELETE FROM users WHERE id = $1`, orphan)
	})
	wrongScope := taskCreationCreateParams(f, uuid.NewString())
	wrongScope.UserID = orphan
	if _, err := st.CreateTaskCreationOperation(ctx, wrongScope); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("无 membership 的显式 scope 必须拒绝: %v", err)
	}

	if _, err := st.pool.Exec(ctx,
		`UPDATE tenants SET status = 'suspended' WHERE id = $1`, f.tenantID); err != nil {
		t.Fatal(err)
	}
	inactive := taskCreationCreateParams(f, uuid.NewString())
	if _, err := st.CreateTaskCreationOperation(ctx, inactive); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("inactive tenant 必须拒绝: %v", err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE tenants SET status = 'active' WHERE id = $1`, f.tenantID); err != nil {
		t.Fatal(err)
	}

	commitID := uuid.NewString()
	faultStore := *st
	var wrapped *compiledTaskFaultTx
	faultStore.beginTx = func(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
		realTx, err := st.pool.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		wrapped = &compiledTaskFaultTx{Tx: realTx, commitErr: errInjectedTaskCreationCommit}
		return wrapped, nil
	}
	if _, err := faultStore.CreateTaskCreationOperation(
		ctx, taskCreationCreateParams(f, commitID)); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("commit fault 应返回 database: %v", err)
	}
	if wrapped == nil || wrapped.rollbackCalls != 1 {
		t.Fatalf("commit fault 必须 rollback: %+v", wrapped)
	}
	var commitRows int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_creation_operations WHERE id = $1`, commitID,
	).Scan(&commitRows); err != nil {
		t.Fatal(err)
	}
	if commitRows != 0 {
		t.Fatalf("拒绝提交后不得残留 operation: %d", commitRows)
	}

	lostID := uuid.NewString()
	lostParams := taskCreationCreateParams(f, lostID)
	lostStore := storeWithCommitResponseLost(st)
	if _, err := lostStore.CreateTaskCreationOperation(ctx, lostParams); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("create response lost 应返回 database: %v", err)
	}
	if replay, err := st.CreateTaskCreationOperation(ctx, lostParams); err != nil || replay.ID != lostID {
		t.Fatalf("create response lost replay 未 exact-adopt: op=%+v err=%v", replay, err)
	}
}

func TestCreateTaskCreationOperation_CrossTenantReplayProbeDoesNotLockForeignOperation(
	t *testing.T,
) {
	st := tenantTestStore(t)
	tenantA := newCompiledTaskFixture(t, st)
	tenantB := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, tenantA)
	cleanupA5Fixture(t, st, tenantB)
	ctx := t.Context()

	paramsA := taskCreationCreateParams(tenantA, uuid.NewString())
	if _, err := st.CreateTaskCreationOperation(ctx, paramsA); err != nil {
		t.Fatalf("创建 tenant A operation: %v", err)
	}
	paramsB := taskCreationCreateParams(tenantB, paramsA.ID)

	blocker, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(context.WithoutCancel(ctx)) }()
	var userID int64
	if err := blocker.QueryRow(ctx, `
		SELECT user_id FROM memberships
		 WHERE tenant_id=$1 AND user_id=$2
		 FOR UPDATE`,
		tenantB.tenantID, tenantB.userID,
	).Scan(&userID); err != nil {
		t.Fatalf("锁 tenant B membership blocker: %v", err)
	}

	foreignProbeDone := make(chan error, 1)
	go func() {
		_, createErr := st.CreateTaskCreationOperation(ctx, paramsB)
		foreignProbeDone <- createErr
	}()
	waitForDatabaseLockQuery(
		t, st,
		"%FROM memberships m%",
		"tenant B replay probe 未在 scoped lookup 后等待自身 membership",
	)

	var operationID string
	if err := st.pool.QueryRow(ctx, `
		SELECT id FROM task_creation_operations
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		 FOR UPDATE NOWAIT`,
		paramsA.ID, tenantA.tenantID, tenantA.userID,
	).Scan(&operationID); err != nil {
		t.Fatalf("tenant B probe 不得锁定 tenant A operation: %v", err)
	}

	if err := blocker.Rollback(ctx); err != nil {
		t.Fatalf("释放 tenant B membership blocker: %v", err)
	}
	select {
	case err := <-foreignProbeDone:
		if !errors.Is(err, types.ErrConflict) {
			t.Fatalf("跨租户全局 operation ID 冲突必须安全拒绝: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("tenant B 全局 ID 冲突未收敛")
	}

	replayed, err := st.CreateTaskCreationOperation(ctx, paramsA)
	if err != nil || replayed.ID != paramsA.ID ||
		replayed.TenantID != tenantA.tenantID || replayed.UserID != tenantA.userID {
		t.Fatalf("tenant A exact replay 未保持可用: op=%+v err=%v", replayed, err)
	}
	var tenantBRows int
	if err := st.pool.QueryRow(ctx, `
		SELECT count(*) FROM task_creation_operations
		 WHERE id=$1 AND tenant_id=$2`,
		paramsA.ID, tenantB.tenantID,
	).Scan(&tenantBRows); err != nil {
		t.Fatal(err)
	}
	if tenantBRows != 0 {
		t.Fatalf("跨租户全局 ID 冲突不得创建或采用 tenant B row: %d", tenantBRows)
	}
}

func TestTaskCreationReplayRootQueryRequiresExactTenantProtocolScope(t *testing.T) {
	raw, err := os.ReadFile("task_creation_operations.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	start := strings.Index(source, "func loadTaskCreationOperationForCreationReplay(")
	if start < 0 {
		t.Fatal("缺少 Task Creation replay 根查询 helper")
	}
	endOffset := strings.Index(
		source[start:], "func validateTaskCreationOperationCreationScope(",
	)
	if endOffset < 0 {
		t.Fatal("无法界定 Task Creation replay 根查询 helper")
	}
	helper := source[start : start+endOffset]
	for _, required := range []string{
		"WHERE id = $1 AND tenant_id = $2 AND user_id = $3",
		"tool_name = 'create_schedule' AND execution_version = $4",
		"id, tenantID, userID, types.TaskCreationExecutionVersionV1",
		"FOR SHARE /* task creation replay operation lock order */",
	} {
		if !strings.Contains(helper, required) {
			t.Fatalf("Task Creation replay 根查询退化，缺少 %q", required)
		}
	}
}

func TestTaskCreationAcquire_ExpiresDurablyAtDatabaseClock(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	ctx := t.Context()
	params := taskCreationCreateParams(f, uuid.NewString())
	if _, err := st.CreateTaskCreationOperation(ctx, params); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE task_creation_operations SET expires_at = clock_timestamp() WHERE id = $1`,
		params.ID); err != nil {
		t.Fatal(err)
	}
	acquire := types.AcquireTaskCreationOperationParams{
		ID: params.ID, TenantID: f.tenantID, UserID: f.userID,
		LeaseOwner: "expiry-" + uuid.NewString(), LeaseDuration: time.Minute,
		ReceiptProvider: "feishu_message_patch", ReceiptTarget: "om-expiry",
	}
	lostStore := storeWithCommitResponseLost(st)
	if _, err := lostStore.AcquireTaskCreationOperation(ctx, acquire); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("expiry commit response lost 应返回 database: %v", err)
	}
	if _, err := st.AcquireTaskCreationOperation(ctx, acquire); !errors.Is(err, types.ErrTaskCreationTerminal) {
		t.Fatalf("expired tombstone retry 必须 terminal: %v", err)
	}
	op, err := st.LoadTaskCreationOperationByUser(ctx, params.ID, f.userID)
	if err != nil {
		t.Fatal(err)
	}
	if op.Status != types.TaskOperationStatusExpired ||
		op.Phase != types.TaskCreationPhaseExpired || op.TombstonedAt == nil ||
		op.LeaseUntil != nil || op.TakeoverNotBefore != nil {
		t.Fatalf("expiry 未耐久线性化: %+v", op)
	}
	assertTaskCreationReceiptExactlyOne(t, st, params.ID)
}

func TestTaskCreationRecoveryTenantEnumeration_OnlyTrulyStale(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	ctx := t.Context()
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_creation_operations WHERE tenant_id = $1`, f.tenantID)
	})

	stale := createAndAcquireA5Operation(t, st, f, "recovery-stale")
	active := createAndAcquireA5Operation(t, st, f, "recovery-active")
	corrupt := createAndAcquireA5Operation(t, st, f, "recovery-corrupt")
	if _, err := st.pool.Exec(ctx,
		`UPDATE task_creation_operations
		    SET lease_until = clock_timestamp() - interval '2 minutes',
		        takeover_not_before = clock_timestamp() - interval '1 minute'
		  WHERE id = $1`, stale.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE task_creation_operations
		    SET lease_until = clock_timestamp() - interval '2 minutes',
		        takeover_not_before = clock_timestamp() - interval '1 minute',
		        lease_owner = ''
		  WHERE id = $1`, corrupt.ID); err != nil {
		t.Fatal(err)
	}

	operations, err := st.ListStaleTaskCreationOperations(
		ctx, f.tenantID, time.Now().Add(time.Hour), 100)
	if err != nil {
		t.Fatal(err)
	}
	if !containsOperation(operations, stale.ID) || containsOperation(operations, active.ID) ||
		containsOperation(operations, corrupt.ID) {
		t.Fatalf("tenant stale scan 错误: %+v", operations)
	}

	// Tenant suspension stops new side effects, but must not make already-owned
	// remote work disappear from recovery. In particular, ensured/ambiguous
	// schedules still need deterministic cleanup or quarantine.
	if _, err := st.pool.Exec(ctx,
		`UPDATE tenants SET status = 'suspended' WHERE id = $1`, f.tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`UPDATE tenants SET status = 'active' WHERE id = $1`, f.tenantID)
	})
	operations, err = st.ListStaleTaskCreationOperations(
		ctx, f.tenantID, time.Now().Add(time.Hour), 100)
	if err != nil || !containsOperation(operations, stale.ID) {
		t.Fatalf("suspended tenant 的 stale operation 仍须扫描: operations=%+v err=%v", operations, err)
	}
	if _, err := st.AcquireTaskCreationOperation(ctx, types.AcquireTaskCreationOperationParams{
		ID: stale.ID, TenantID: f.tenantID, UserID: f.userID,
		LeaseOwner: "suspended-recovery-" + uuid.NewString(), LeaseDuration: time.Minute,
		ReceiptProvider: stale.ReceiptProvider, ReceiptTarget: stale.ReceiptTarget,
	}); err != nil {
		t.Fatalf("suspended tenant 的 recovery takeover 必须可收敛: %v", err)
	}
}

func TestTaskCreationDefinitionActivation_AtomicAndVisibleOnlyWhenComplete(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	ctx := t.Context()
	p := preparedA5Commit(t, st, f, "happy")

	if err := st.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); err != nil {
		t.Fatalf("CommitPaused...() 失败: %v", err)
	}
	if err := st.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); err != nil {
		t.Fatalf("definition exact replay 失败: %v", err)
	}
	assertTaskCreationApprovedDefinition(t, st, p)
	assertCreationPhaseAndScheduleStatus(t, st, p.Lease.ID, p.Definition.TaskID,
		types.TaskCreationPhaseDefinitionCommitted, types.ScheduleStatusPaused)
	assertScheduleVisibility(t, st, f.userID, p.Definition.TaskID, false)
	assertProvisioningScheduleIsNotUserManageable(t, st, f.userID, p.Definition.TaskID)

	started, err := st.BeginTaskCreationActivation(ctx, p.Lease, p.Definition.TaskID)
	if err != nil || !started {
		t.Fatalf("BeginActivation: started=%v err=%v", started, err)
	}
	started, err = st.BeginTaskCreationActivation(ctx, p.Lease, p.Definition.TaskID)
	if err != nil || started {
		t.Fatalf("BeginActivation replay: started=%v err=%v", started, err)
	}
	if err := st.CommitTaskCreationActivation(ctx, p.Lease, p.Definition.TaskID); err != nil {
		t.Fatalf("CommitActivation: %v", err)
	}
	if err := st.CommitTaskCreationActivation(ctx, p.Lease, p.Definition.TaskID); err != nil {
		t.Fatalf("CommitActivation replay: %v", err)
	}
	assertCreationPhaseAndScheduleStatus(t, st, p.Lease.ID, p.Definition.TaskID,
		types.TaskCreationPhaseActivated, types.ScheduleStatusActive)
	assertScheduleVisibility(t, st, f.userID, p.Definition.TaskID, false)

	if err := st.CompleteTaskCreationOperation(
		ctx, p.Lease, p.Definition.TaskID, json.RawMessage(`{"created":true}`)); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	assertScheduleVisibility(t, st, f.userID, p.Definition.TaskID, true)
	if _, err := st.GetSchedule(ctx, p.Definition.TaskID, f.userID); err != nil {
		t.Fatalf("completed schedule 应恢复 direct-id 读取: %v", err)
	}
	if ok, err := st.UpsertSchedulePlaybook(
		ctx, f.userID, p.Definition.TaskID, "completed-playbook"); err != nil || !ok {
		t.Fatalf("completed schedule 应恢复手册修改: ok=%v err=%v", ok, err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE schedules SET status = 'paused' WHERE id = $1`, p.Definition.TaskID); err != nil {
		t.Fatal(err)
	}
	assertScheduleVisibility(t, st, f.userID, p.Definition.TaskID, true)

	legacyTask := "legacy-paused-" + uuid.NewString()
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO schedules
		    (id, tenant_id, user_id, spec_json, scope_json, status)
		 VALUES ($1, $2, $3, '{}', '{}', 'paused')`,
		legacyTask, f.tenantID, f.userID); err != nil {
		t.Fatal(err)
	}
	assertScheduleVisibility(t, st, f.userID, legacyTask, true)
}

func TestTaskCreationDefinitionCommit_ToolPlanCreatesNoSourceEntity(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	query := "source-free-" + uuid.NewString()
	target := validA5PlanSource(t, query, "Source-free Tool call")
	target.ToolName = "web_search"
	target.ToolArgs = json.RawMessage(`{"query":"` + query + `"}`)
	p := preparedA5CommitWithSources(t, st, f, "tool-source-free", target)

	if err := st.CommitPausedCompiledTaskDefinitionForCreation(t.Context(), p); err != nil {
		t.Fatalf("CommitPausedCompiledTaskDefinitionForCreation: %v", err)
	}
	if err := st.CommitPausedCompiledTaskDefinitionForCreation(t.Context(), p); err != nil {
		t.Fatalf("Source-free definition replay: %v", err)
	}
	record, err := st.GetCurrentToolApprovedDefinition(
		t.Context(), p.Lease.TenantID, p.Lease.UserID, p.Definition.TaskID)
	if err != nil {
		t.Fatalf("GetCurrentToolApprovedDefinition: %v", err)
	}
	if record.Definition.SchemaVersion != taskstate.ApprovedDefinitionSchemaVersionV2 ||
		len(record.Definition.ToolCalls) != 1 ||
		record.Definition.ToolCalls[0].ToolName != "web_search" ||
		record.Definition.TaskManual != p.Definition.PlaybookContent {
		t.Fatalf("Source-free approved definition differs: %+v", record.Definition)
	}
	adaptive, err := st.GetToolAdaptiveStateForDefinition(
		t.Context(), p.Lease.TenantID, p.Lease.UserID, p.Definition.TaskID,
		ApprovedDefinitionFence{Version: record.Version, Digest: record.Digest})
	if err != nil {
		t.Fatalf("GetToolAdaptiveStateForDefinition: %v", err)
	}
	if adaptive.Version != 1 ||
		adaptive.State.SchemaVersion != taskstate.AdaptiveStateSchemaVersionV2 ||
		len(adaptive.State.InvocationStates) != 1 ||
		adaptive.State.InvocationStates[0].InvocationDigest !=
			record.Definition.ToolCalls[0].Digest ||
		adaptive.State.InvocationStates[0].Status != taskstate.InvocationStatusActive {
		t.Fatalf("initial invocation-scoped adaptive state differs: %+v", adaptive)
	}
	var taskLinks, globalTargets int
	var projectionPlan []byte
	if err := st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM task_fetch_targets WHERE schedule_id=$1`,
		p.Definition.TaskID).Scan(&taskLinks); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM fetch_targets WHERE url=$1`,
		target.URL).Scan(&globalTargets); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(t.Context(),
		`SELECT fetch_plan FROM schedule_playbooks WHERE schedule_id=$1`,
		p.Definition.TaskID).Scan(&projectionPlan); err != nil {
		t.Fatal(err)
	}
	if taskLinks != 0 || globalTargets != 0 {
		t.Fatalf("Tool plan created Source-era rows: links=%d targets=%d",
			taskLinks, globalTargets)
	}
	if !taskCreationJSONEqual(projectionPlan, []byte(`{}`)) ||
		bytes.Contains(projectionPlan, []byte(`tool_arguments`)) ||
		bytes.Contains(projectionPlan, []byte(`url`)) {
		t.Fatalf("mutable schedule projection retained private Tool plan: %s",
			projectionPlan)
	}
}

func TestTaskCreationCompiledSagaRejectsDynamicAggregate(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	ctx := t.Context()

	setDynamic := func(t *testing.T, taskID string) {
		t.Helper()
		payload := []byte(`{"test":"dynamic-saga-mode-fence"}`)
		digest := digestOf(payload)
		tx, err := st.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, `
			INSERT INTO task_approved_definition_versions (
				tenant_id, user_id, task_id, version, schema_version,
				execution_mode, definition_digest, payload, operation_ref
			) VALUES ($1, $2, $3, 2, 'test.dynamic/v1', $4, $5, $6, $7)`,
			f.tenantID, f.userID, taskID, types.ExecutionModeDiscoverAtRun,
			digest, payload, "test-dynamic:"+taskID); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE schedules
			   SET execution_mode=$2, approved_definition_version=2,
			       approved_definition_digest=$3
			 WHERE id=$1`, taskID, types.ExecutionModeDiscoverAtRun, digest); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("definition replay", func(t *testing.T) {
		p := preparedA5Commit(t, st, f, "dynamic-definition-replay")
		if err := st.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); err != nil {
			t.Fatal(err)
		}
		setDynamic(t, p.Definition.TaskID)
		if err := st.CommitPausedCompiledTaskDefinitionForCreation(
			ctx, p); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("compiled definition replay adopted dynamic aggregate: %v", err)
		}
	})

	t.Run("activation begin", func(t *testing.T) {
		p := preparedA5Commit(t, st, f, "dynamic-activation-begin")
		if err := st.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); err != nil {
			t.Fatal(err)
		}
		setDynamic(t, p.Definition.TaskID)
		if _, err := st.BeginTaskCreationActivation(
			ctx, p.Lease, p.Definition.TaskID); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("compiled activation began on dynamic aggregate: %v", err)
		}
	})

	t.Run("activation commit", func(t *testing.T) {
		p := preparedA5Commit(t, st, f, "dynamic-activation-commit")
		if err := st.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); err != nil {
			t.Fatal(err)
		}
		started, err := st.BeginTaskCreationActivation(ctx, p.Lease, p.Definition.TaskID)
		if err != nil || !started {
			t.Fatalf("begin activation started=%v err=%v", started, err)
		}
		setDynamic(t, p.Definition.TaskID)
		if err := st.CommitTaskCreationActivation(
			ctx, p.Lease, p.Definition.TaskID); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("compiled activation committed dynamic aggregate: %v", err)
		}
		var status types.ScheduleStatus
		if err := st.pool.QueryRow(ctx,
			`SELECT status FROM schedules WHERE id=$1`, p.Definition.TaskID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != types.ScheduleStatusPaused {
			t.Fatalf("rejected dynamic activation changed status to %q", status)
		}
	})
}

func TestTaskCreationDefinitionReplayRejectsApprovedHeadDrift(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	ctx := t.Context()
	p := preparedA5Commit(t, st, f, "approved-head-drift")
	if err := st.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); err != nil {
		t.Fatal(err)
	}
	current := assertTaskCreationApprovedDefinition(t, st, p)
	drifted := current.Definition
	drifted.Intent += "（未由本次创建批准）"
	payload, err := taskstate.EncodeApprovedDefinitionV1(drifted)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := taskstate.DigestApprovedDefinitionV1(drifted)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
		INSERT INTO task_approved_definition_versions (
			tenant_id, user_id, task_id, version, schema_version,
			execution_mode, definition_digest, payload, operation_ref
		) VALUES ($1, $2, $3, 2, $4, $5, $6, $7, $8)`,
		p.Lease.TenantID, p.Lease.UserID, p.Definition.TaskID,
		drifted.SchemaVersion, types.ExecutionModeCompiled, digest, payload,
		"test-approved-head-drift:"+p.Definition.TaskID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE schedules
		   SET approved_definition_version=2, approved_definition_digest=$2
		 WHERE id=$1`, p.Definition.TaskID, digest); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	if err := st.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("creation replay adopted a drifted approved head: %v", err)
	}
}

func TestTaskCreationActivation_ResponseLostExactAdopt(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	ctx := t.Context()
	p := preparedA5Commit(t, st, f, "activation-lost")
	if err := st.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); err != nil {
		t.Fatal(err)
	}
	lostStore := storeWithCommitResponseLost(st)
	started, err := lostStore.BeginTaskCreationActivation(ctx, p.Lease, p.Definition.TaskID)
	if started || !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("BeginActivation lost response: started=%v err=%v", started, err)
	}
	started, err = st.BeginTaskCreationActivation(ctx, p.Lease, p.Definition.TaskID)
	if started || err != nil {
		t.Fatalf("BeginActivation replay 应 adopt: started=%v err=%v", started, err)
	}
	assertCreationPhaseAndScheduleStatus(t, st, p.Lease.ID, p.Definition.TaskID,
		types.TaskCreationPhaseActivationStarted, types.ScheduleStatusPaused)

	if err := lostStore.CommitTaskCreationActivation(ctx, p.Lease, p.Definition.TaskID); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("CommitActivation lost response: %v", err)
	}
	if err := st.CommitTaskCreationActivation(ctx, p.Lease, p.Definition.TaskID); err != nil {
		t.Fatalf("CommitActivation replay 应 exact-adopt: %v", err)
	}
	assertCreationPhaseAndScheduleStatus(t, st, p.Lease.ID, p.Definition.TaskID,
		types.TaskCreationPhaseActivated, types.ScheduleStatusActive)
}

func TestTaskCreationCompletion_ResponseLostExactAdoptKeepsActiveAggregate(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	ctx := t.Context()
	p := preparedA5Commit(t, st, f, "completion-lost")
	if err := st.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); err != nil {
		t.Fatal(err)
	}
	started, err := st.BeginTaskCreationActivation(ctx, p.Lease, p.Definition.TaskID)
	if err != nil || !started {
		t.Fatalf("BeginActivation: started=%v err=%v", started, err)
	}
	if err := st.CommitTaskCreationActivation(ctx, p.Lease, p.Definition.TaskID); err != nil {
		t.Fatal(err)
	}
	result := json.RawMessage(`{"version":"test/v1","task_id":"` + p.Definition.TaskID + `"}`)
	lostStore := storeWithCommitResponseLost(st)
	if err := lostStore.CompleteTaskCreationOperation(
		ctx, p.Lease, p.Definition.TaskID, result); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("completion response lost 应返回 database: %v", err)
	}
	if err := st.CompleteTaskCreationOperation(
		ctx, p.Lease, p.Definition.TaskID, result); err != nil {
		t.Fatalf("completion exact replay: %v", err)
	}
	op, err := st.LoadTaskCreationOperation(ctx, p.Lease.ID, p.Lease.TenantID, p.Lease.UserID)
	if err != nil {
		t.Fatal(err)
	}
	if op.Status != types.TaskOperationStatusExecuted ||
		op.Phase != types.TaskCreationPhaseCompleted || op.TombstonedAt == nil {
		t.Fatalf("completion tombstone incomplete: %+v", op)
	}
	authorized, err := st.AuthorizeScheduledRun(ctx, p.Definition.TaskID, f.userID)
	if err != nil || !authorized {
		t.Fatalf("completed active aggregate 应被授权: authorized=%v err=%v", authorized, err)
	}
}

func TestTaskCreationActivation_RequiresCurrentActiveMembership(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	ctx := t.Context()
	p := preparedA5Commit(t, st, f, "activation-membership")
	if err := st.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); err != nil {
		t.Fatal(err)
	}
	setTenantStatus := func(status types.TenantStatus) {
		t.Helper()
		if _, err := st.pool.Exec(ctx,
			`UPDATE tenants SET status = $2 WHERE id = $1`, f.tenantID, status); err != nil {
			t.Fatal(err)
		}
	}
	setTenantStatus(types.TenantStatusSuspended)
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`UPDATE tenants SET status = 'active' WHERE id = $1`, f.tenantID)
	})
	if started, err := st.BeginTaskCreationActivation(
		ctx, p.Lease, p.Definition.TaskID,
	); started || !errors.Is(err, types.ErrValidation) {
		t.Fatalf("inactive membership 不得授权 Activate: started=%v err=%v", started, err)
	}
	assertCreationPhaseAndScheduleStatus(t, st, p.Lease.ID, p.Definition.TaskID,
		types.TaskCreationPhaseDefinitionCommitted, types.ScheduleStatusPaused)

	setTenantStatus(types.TenantStatusActive)
	started, err := st.BeginTaskCreationActivation(ctx, p.Lease, p.Definition.TaskID)
	if err != nil || !started {
		t.Fatalf("reactivated membership 应可授权 Activate: started=%v err=%v", started, err)
	}
	setTenantStatus(types.TenantStatusSuspended)
	if err := st.CommitTaskCreationActivation(
		ctx, p.Lease, p.Definition.TaskID,
	); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("Activate RPC 后 membership 失效不得镜像 active: %v", err)
	}
	assertCreationPhaseAndScheduleStatus(t, st, p.Lease.ID, p.Definition.TaskID,
		types.TaskCreationPhaseActivationStarted, types.ScheduleStatusPaused)
}

func TestAuthorizeScheduledRun_FailsClosedUntilMatureAndMembershipValid(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	ctx := t.Context()
	p := preparedA5Commit(t, st, f, "run-authorization")
	if err := st.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); err != nil {
		t.Fatal(err)
	}
	assertAuthorized := func(want bool) {
		t.Helper()
		got, err := st.AuthorizeScheduledRun(ctx, p.Definition.TaskID, f.userID)
		if err != nil || got != want {
			t.Fatalf("AuthorizeScheduledRun=%v want=%v err=%v", got, want, err)
		}
	}
	assertAuthorized(false) // paused + provisioning
	if _, err := st.pool.Exec(ctx,
		`UPDATE schedules SET status = 'active' WHERE id = $1`, p.Definition.TaskID); err != nil {
		t.Fatal(err)
	}
	assertAuthorized(false) // active mirror alone cannot bypass unfinished v1
	if _, err := st.pool.Exec(ctx,
		`UPDATE schedules SET status = 'paused' WHERE id = $1`, p.Definition.TaskID); err != nil {
		t.Fatal(err)
	}
	started, err := st.BeginTaskCreationActivation(ctx, p.Lease, p.Definition.TaskID)
	if err != nil || !started {
		t.Fatalf("BeginActivation: started=%v err=%v", started, err)
	}
	if err := st.CommitTaskCreationActivation(ctx, p.Lease, p.Definition.TaskID); err != nil {
		t.Fatal(err)
	}
	assertAuthorized(false) // operation not completed yet
	if err := st.CompleteTaskCreationOperation(
		ctx, p.Lease, p.Definition.TaskID, json.RawMessage(`{"created":true}`)); err != nil {
		t.Fatal(err)
	}
	assertAuthorized(true)
	if got, err := st.AuthorizeScheduledRun(
		ctx, p.Definition.TaskID, f.userID+9999); err != nil || got {
		t.Fatalf("wrong user authorization must fail closed: got=%v err=%v", got, err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE tenants SET status = 'suspended' WHERE id = $1`, f.tenantID); err != nil {
		t.Fatal(err)
	}
	assertAuthorized(false)
	if _, err := st.pool.Exec(ctx,
		`UPDATE tenants SET status = 'active' WHERE id = $1`, f.tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`DELETE FROM memberships WHERE tenant_id = $1 AND user_id = $2`,
		f.tenantID, f.userID); err != nil {
		t.Fatal(err)
	}
	assertAuthorized(false)
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO memberships (tenant_id, user_id, role) VALUES ($1, $2, 'owner')`,
		f.tenantID, f.userID); err != nil {
		t.Fatal(err)
	}
	assertAuthorized(true)
}

func TestTaskCreationDefinitionCommit_ResponseLostAndRollback(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	ctx := t.Context()

	t.Run("coordinator mapping drift is rejected", func(t *testing.T) {
		p := preparedA5Commit(t, st, f, "mapping-drift")
		p.Definition.NLDescription = "被错误映射的描述"
		if err := st.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("definition_digest 必须拒绝映射漂移: %v", err)
		}
		assertNoCompiledTaskAggregate(t, st, p.Definition.TaskID)
	})

	t.Run("duplicate definition digest field is rejected", func(t *testing.T) {
		p := preparedA5Commit(t, st, f, "duplicate-digest")
		corrupt := fmt.Appendf(nil,
			`{"definition_digest":"%064d","definition_digest":"%064d"}`, 1, 2)
		corruptDigest := digestOf(corrupt)
		if _, err := st.pool.Exec(ctx,
			`UPDATE task_creation_operations
			    SET compiled_definition = $2, compiled_digest = $3
			  WHERE id = $1`, p.Lease.ID, corrupt, corruptDigest); err != nil {
			t.Fatal(err)
		}
		p.CompiledDigest = corruptDigest
		if err := st.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("重复 definition_digest 必须拒绝: %v", err)
		}
		assertNoCompiledTaskAggregate(t, st, p.Definition.TaskID)
	})

	t.Run("statement fault rolls aggregate and phase back together", func(t *testing.T) {
		p := preparedA5Commit(t, st, f, "statement-fault")
		faultStore := *st
		faultStore.beginTx = func(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
			realTx, err := st.pool.BeginTx(ctx, opts)
			if err != nil {
				return nil, err
			}
			return &compiledTaskFaultTx{
				Tx: realTx, failContains: "SET task_id = $6", commitErr: errInjectedTaskCreationCommit,
			}, nil
		}
		if err := faultStore.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); !errors.Is(err, types.ErrDatabase) {
			t.Fatalf("injected statement fault: %v", err)
		}
		assertNoCompiledTaskAggregate(t, st, p.Definition.TaskID)
		op, err := st.LoadTaskCreationOperation(ctx, p.Lease.ID, p.Lease.TenantID, p.Lease.UserID)
		if err != nil {
			t.Fatal(err)
		}
		if op.Phase != types.TaskCreationPhaseScheduleEnsured {
			t.Fatalf("rollback 后 phase 漂移: %s", op.Phase)
		}
	})

	t.Run("commit response lost exact-adopts", func(t *testing.T) {
		p := preparedA5Commit(t, st, f, "response-lost")
		lostStore := storeWithCommitResponseLost(st)
		if err := lostStore.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); !errors.Is(err, types.ErrDatabase) {
			t.Fatalf("response lost 应返回 database: %v", err)
		}
		assertCreationPhaseAndScheduleStatus(t, st, p.Lease.ID, p.Definition.TaskID,
			types.TaskCreationPhaseDefinitionCommitted, types.ScheduleStatusPaused)
		assertTaskCreationApprovedDefinition(t, st, p)
		if err := st.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); err != nil {
			t.Fatalf("lost-response replay 应 exact-adopt: %v", err)
		}
		assertTaskCreationApprovedDefinition(t, st, p)
	})
}

func TestTaskCreationDefinitionCommit_PreservesApprovedPlanOrderAndMaterializedIDs(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	first := validA5PlanSource(t, "z-first-in-plan-"+uuid.NewString(), "计划源 Z")
	second := validA5PlanSource(t, "a-second-in-plan-"+uuid.NewString(), "计划源 A")
	p := preparedA5CommitWithSources(t, st, f, "approved-order", first, second)

	if err := st.CommitPausedCompiledTaskDefinitionForCreation(t.Context(), p); err != nil {
		t.Fatalf("CommitPausedCompiledTaskDefinitionForCreation: %v", err)
	}
	record := assertTaskCreationApprovedDefinition(t, st, p)
	var plan taskstate.FetchPlanV1
	if err := json.Unmarshal(record.Definition.FetchPlan, &plan); err != nil {
		t.Fatal(err)
	}
	if len(plan.Sources) != 2 || plan.Sources[0].URL != first.URL ||
		plan.Sources[1].URL != second.URL {
		t.Fatalf("approved plan order drifted: %+v", plan.Sources)
	}
	if len(record.Definition.Sources) != 2 ||
		record.Definition.Sources[0].URL != second.URL ||
		record.Definition.Sources[1].URL != first.URL {
		t.Fatalf("approved source canonical order drifted: %+v", record.Definition.Sources)
	}
}

func TestTaskCreationDefinitionCommit_PreservesLooseStrictness(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	p := preparedA5CommitWithStrictness(t, st, f, "approved-loose", types.StrictnessLoose)

	if err := st.CommitPausedCompiledTaskDefinitionForCreation(t.Context(), p); err != nil {
		t.Fatalf("CommitPausedCompiledTaskDefinitionForCreation: %v", err)
	}
	assertTaskCreationApprovedDefinition(t, st, p)
}

func TestTaskCreationDefinitionCommit_LockOrderAvoidsCreateReplayDeadlock(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	p := preparedA5Commit(t, st, f, "lock-order")
	op, err := st.LoadTaskCreationOperation(ctx, p.Lease.ID, p.Lease.TenantID, p.Lease.UserID)
	if err != nil {
		t.Fatal(err)
	}
	createParams := types.CreateTaskCreationOperationParams{
		ID: op.ID, TenantID: op.TenantID, UserID: op.UserID, SessionID: op.SessionID,
		Args: op.Args, Summary: op.Summary, ExpiresAt: op.ExpiresAt,
	}
	operationLocked := make(chan struct{})
	releaseCommit := make(chan struct{})
	membershipLockedByCreate := make(chan struct{}, 1)
	commitStore := *st
	commitStore.beginTx = func(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
		tx, err := st.pool.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		return &taskCreationObservedTx{
			Tx: tx, pauseAfter: "FROM task_creation_operations", paused: operationLocked,
			release: releaseCommit,
		}, nil
	}
	createStore := *st
	createStore.beginTx = func(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
		tx, err := st.pool.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		return &taskCreationObservedTx{
			Tx: tx, notifyAfter: "FROM memberships m", notified: membershipLockedByCreate,
		}, nil
	}
	commitResult := make(chan error, 1)
	go func() {
		commitResult <- commitStore.CommitPausedCompiledTaskDefinitionForCreation(ctx, p)
	}()
	select {
	case <-operationLocked:
	case <-ctx.Done():
		t.Fatalf("commit 未取得 operation lock: %v", ctx.Err())
	}
	createResult := make(chan error, 1)
	go func() {
		_, err := createStore.CreateTaskCreationOperation(ctx, createParams)
		createResult <- err
	}()
	// With the correct global order, Commit already owns the stronger membership
	// lock and Create waits there. Under the old inverse order, this notification
	// proves Create owns membership while waiting for Commit's operation row.
	select {
	case <-membershipLockedByCreate:
	case <-time.After(150 * time.Millisecond):
	}
	close(releaseCommit)
	if err := <-commitResult; err != nil {
		t.Fatalf("definition commit lock ordering failed: %v", err)
	}
	if err := <-createResult; !errors.Is(err, types.ErrConflict) {
		t.Fatalf("progressed create replay 应 conflict 而非 deadlock/database: %v", err)
	}
}

func TestTaskCreationCapacity_SerializesNineteenToTwenty(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	ctx := t.Context()
	var secondTenantID int64
	if err := st.pool.QueryRow(ctx,
		`INSERT INTO tenants (status, plan) VALUES ('active', 'free') RETURNING id`,
	).Scan(&secondTenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO memberships (tenant_id, user_id, role) VALUES ($1, $2, 'owner')`,
		secondTenantID, f.userID); err != nil {
		t.Fatal(err)
	}
	f2 := &compiledTaskFixture{
		st: st, tenantID: secondTenantID, userID: f.userID,
		urlRoot: "vane://a5-second-tenant/" + uuid.NewString(),
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_creation_operations WHERE tenant_id = $1`, secondTenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM schedules WHERE tenant_id = $1`, secondTenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM memberships WHERE tenant_id = $1 AND user_id = $2`,
			secondTenantID, f.userID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM fetch_targets WHERE url LIKE $1`, f2.urlRoot+"%")
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM tenants WHERE id = $1`, secondTenantID)
	})
	st2, err := New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st2)

	for i := range 19 {
		if _, err := st.pool.Exec(ctx,
			`INSERT INTO schedules
			    (id, tenant_id, user_id, spec_json, scope_json, status)
			 VALUES ($1, $2, $3, '{}', '{}', 'active')`,
			fmt.Sprintf("capacity-active-%02d-%s", i, uuid.NewString()),
			f.tenantID, f.userID); err != nil {
			t.Fatal(err)
		}
	}
	// A user's ordinary paused task is terminal state, not a provisioning slot.
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO schedules
		    (id, tenant_id, user_id, spec_json, scope_json, status)
		 VALUES ($1, $2, $3, '{}', '{}', 'paused')`,
		"capacity-user-paused-"+uuid.NewString(), f.tenantID, f.userID); err != nil {
		t.Fatal(err)
	}

	p1 := preparedA5Commit(t, st, f, "race-a")
	p2 := preparedA5Commit(t, st, f2, "race-b")
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, candidate := range []struct {
		st *Store
		p  types.CommitPausedCompiledTaskDefinitionForCreationParams
	}{{st, p1}, {st2, p2}} {
		go func(candidate struct {
			st *Store
			p  types.CommitPausedCompiledTaskDefinitionForCreationParams
		}) {
			<-start
			results <- candidate.st.CommitPausedCompiledTaskDefinitionForCreation(ctx, candidate.p)
		}(candidate)
	}
	close(start)
	wins, limited := 0, 0
	for range 2 {
		err := <-results
		switch {
		case err == nil:
			wins++
		case errors.Is(err, types.ErrTaskCreationLimit):
			limited++
		default:
			t.Fatalf("并发 capacity 非预期错误: %v", err)
		}
	}
	if wins != 1 || limited != 1 {
		t.Fatalf("19->并发2 应 1 success/1 limit，实得 %d/%d", wins, limited)
	}
	var committed int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_creation_operations
		  WHERE id IN ($1, $2) AND phase = $3`,
		p1.Lease.ID, p2.Lease.ID, types.TaskCreationPhaseDefinitionCommitted,
	).Scan(&committed); err != nil {
		t.Fatal(err)
	}
	if committed != 1 {
		t.Fatalf("应只预留一个 slot: %d", committed)
	}

	// User-facing listing correctly hides the provisioning winner, so the old
	// scheduler's preflight still sees only 19 active rows. The final Store write
	// must nevertheless reject its attempted 21st slot and let scheduler perform
	// its existing Temporal compensation.
	listed, err := st.ListSchedulesByUser(ctx, f.userID)
	if err != nil {
		t.Fatal(err)
	}
	visibleActive := 0
	for _, schedule := range listed {
		if schedule.Status == types.ScheduleStatusActive {
			visibleActive++
		}
	}
	if visibleActive != 19 {
		t.Fatalf("fixture 应让 legacy preflight 只见 19 active，实得 %d", visibleActive)
	}
	legacyID := "capacity-legacy-" + uuid.NewString()
	err = st.InsertSchedule(ctx, &types.Schedule{
		ID: legacyID, UserID: f.userID, Status: types.ScheduleStatusActive,
		SpecJSON: json.RawMessage(`{}`), ScopeJSON: json.RawMessage(`{}`),
	})
	if !errors.Is(err, types.ErrTaskCreationLimit) {
		t.Fatalf("legacy final insert 必须看见 A5 reservation 并拒绝: %v", err)
	}
	var legacyRows int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM schedules WHERE id = $1`, legacyID).Scan(&legacyRows); err != nil {
		t.Fatal(err)
	}
	if legacyRows != 0 {
		t.Fatalf("超限 legacy 镜像不得落库: %d", legacyRows)
	}
}

func TestTaskCreationCapacity_QuarantineRetainsReservation(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	ctx := t.Context()
	for i := range 19 {
		if _, err := st.pool.Exec(ctx,
			`INSERT INTO schedules
			    (id, tenant_id, user_id, spec_json, scope_json, status)
			 VALUES ($1, $2, $3, '{}', '{}', 'active')`,
			fmt.Sprintf("quarantine-capacity-%02d-%s", i, uuid.NewString()),
			f.tenantID, f.userID); err != nil {
			t.Fatal(err)
		}
	}
	quarantined := preparedA5Commit(t, st, f, "quarantine-capacity-reservation")
	if err := st.BlockTaskCreationOperationAfterSideEffect(
		ctx, quarantined.Lease, quarantined.Definition.TaskID,
		"ENSURE_AMBIGUOUS", "remote state retained",
	); err != nil {
		t.Fatal(err)
	}
	candidate := preparedA5Commit(t, st, f, "quarantine-capacity-candidate")
	if err := st.CommitPausedCompiledTaskDefinitionForCreation(
		ctx, candidate,
	); !errors.Is(err, types.ErrTaskCreationLimit) {
		t.Fatalf("retained quarantine 必须继续占第 20 个 slot: %v", err)
	}
	unknownLease, _ := preparedA5ScheduleOnly(t, st, f, "quarantine-capacity-unknown")
	if _, err := st.pool.Exec(ctx,
		`UPDATE task_creation_operations SET prepared_schedule = $2 WHERE id = $1`,
		unknownLease.ID, []byte(`{"task_id":"a","task_id":"b"}`)); err != nil {
		t.Fatal(err)
	}
	if err := st.BlockTaskCreationOperationAfterSideEffect(
		ctx, unknownLease, "", "CHECKPOINT_INVALID", "unknown remote task retained",
	); err != nil {
		t.Fatal(err)
	}
	// Unknown quarantines are counted per operation, not silently dropped merely
	// because no trustworthy task_id can be written.
	used, err := func() (int, error) {
		tx, err := st.pool.Begin(ctx)
		if err != nil {
			return 0, err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		return countTaskCreationCapacity(ctx, tx, f.userID)
	}()
	if err != nil || used != 21 {
		t.Fatalf("known+unknown quarantine reservation count=%d want=21 err=%v", used, err)
	}
}

func TestTaskCreationCleanup_FencedAtomicAndReplayable(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	ctx := t.Context()
	p := preparedA5Commit(t, st, f, "cleanup")
	if err := st.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); err != nil {
		t.Fatal(err)
	}
	var sourceID int64
	if err := st.pool.QueryRow(ctx,
		`SELECT fetch_target_id FROM task_fetch_targets WHERE schedule_id = $1`, p.Definition.TaskID,
	).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	started, err := st.BeginTaskCreationCleanup(
		ctx, p.Lease, p.Definition.TaskID, "ACTIVATE_REJECTED", "activation rejected")
	if err != nil || !started {
		t.Fatalf("BeginCleanup: started=%v err=%v", started, err)
	}
	started, err = st.BeginTaskCreationCleanup(
		ctx, p.Lease, p.Definition.TaskID, "ACTIVATE_REJECTED", "activation rejected")
	if err != nil || started {
		t.Fatalf("BeginCleanup replay: started=%v err=%v", started, err)
	}

	stale := p.Lease
	stale.Fence++
	if err := st.FinishTaskCreationCleanup(
		ctx, stale, p.Definition.TaskID, types.TaskOperationStatusFailed); !errors.Is(err, types.ErrTaskCreationLeaseLost) {
		t.Fatalf("stale fence 不得清理: %v", err)
	}
	if err := st.FinishTaskCreationCleanup(
		ctx, p.Lease, p.Definition.TaskID, types.TaskOperationStatusExecuted); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("cleanup terminal 只收 failed/blocked: %v", err)
	}

	faultStore := *st
	faultStore.beginTx = func(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
		realTx, err := st.pool.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		return &compiledTaskFaultTx{
			Tx: realTx, failContains: "SET status = $6", commitErr: errInjectedTaskCreationCommit,
		}, nil
	}
	if err := faultStore.FinishTaskCreationCleanup(
		ctx, p.Lease, p.Definition.TaskID, types.TaskOperationStatusBlocked); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("cleanup statement fault: %v", err)
	}
	assertCreationPhaseAndScheduleStatus(t, st, p.Lease.ID, p.Definition.TaskID,
		types.TaskCreationPhaseCleanupPending, types.ScheduleStatusPaused)

	lostStore := storeWithCommitResponseLost(st)
	if err := lostStore.FinishTaskCreationCleanup(
		ctx, p.Lease, p.Definition.TaskID, types.TaskOperationStatusBlocked); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("cleanup response lost 应返回 database: %v", err)
	}
	if err := st.FinishTaskCreationCleanup(
		ctx, p.Lease, p.Definition.TaskID, types.TaskOperationStatusBlocked); err != nil {
		t.Fatalf("cleanup exact replay: %v", err)
	}
	assertTaskCreationReceiptExactlyOne(t, st, p.Lease.ID)
	for _, table := range []string{"schedules", "schedule_playbooks", "task_fetch_targets"} {
		column := "id"
		if table != "schedules" {
			column = "schedule_id"
		}
		var count int
		if err := st.pool.QueryRow(ctx,
			fmt.Sprintf(`SELECT count(*) FROM %s WHERE %s = $1`, table, column),
			p.Definition.TaskID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("cleanup 后 %s 残留 %d", table, count)
		}
	}
	var sourceCount int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM fetch_targets WHERE id = $1`, sourceID).Scan(&sourceCount); err != nil {
		t.Fatal(err)
	}
	if sourceCount != 1 {
		t.Fatalf("cleanup 不得删除全局 source: %d", sourceCount)
	}
}

func TestTaskCreationCleanupBegin_ResponseLostExactAdopt(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	ctx := t.Context()
	p := preparedA5Commit(t, st, f, "cleanup-begin-lost")
	if err := st.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); err != nil {
		t.Fatal(err)
	}
	lostStore := storeWithCommitResponseLost(st)
	started, err := lostStore.BeginTaskCreationCleanup(
		ctx, p.Lease, p.Definition.TaskID, "TEMPORAL_DELETE", "delete required")
	if started || !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("BeginCleanup lost response: started=%v err=%v", started, err)
	}
	started, err = st.BeginTaskCreationCleanup(
		ctx, p.Lease, p.Definition.TaskID, "TEMPORAL_DELETE", "delete required")
	if started || err != nil {
		t.Fatalf("BeginCleanup replay 应 exact-adopt: started=%v err=%v", started, err)
	}
	assertCreationPhaseAndScheduleStatus(t, st, p.Lease.ID, p.Definition.TaskID,
		types.TaskCreationPhaseCleanupPending, types.ScheduleStatusPaused)
}

func TestTaskCreationCleanup_PreEnsureDeterministicDeleteCheckpoint(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	ctx := t.Context()

	t.Run("absent aggregate is checkpointed and replayable", func(t *testing.T) {
		lease, taskID := preparedA5ScheduleOnly(t, st, f, "cleanup-pre-ensure")
		started, err := st.BeginTaskCreationCleanup(
			ctx, lease, taskID, "MEMBERSHIP_INACTIVE", "workspace is inactive")
		if err != nil || !started {
			t.Fatalf("schedule_prepared cleanup: started=%v err=%v", started, err)
		}
		op, err := st.LoadTaskCreationOperation(ctx, lease.ID, lease.TenantID, lease.UserID)
		if err != nil {
			t.Fatal(err)
		}
		marker, markerErr := decodeTaskCreationCleanupMarker(op.Result)
		if markerErr != nil || marker.AggregateExpected || op.TaskID != taskID ||
			op.Phase != types.TaskCreationPhaseCleanupPending {
			t.Fatalf("pre-ensure cleanup marker/task binding 错误: op=%+v marker=%+v err=%v",
				op, marker, markerErr)
		}
		started, err = st.BeginTaskCreationCleanup(
			ctx, lease, taskID, "MEMBERSHIP_INACTIVE", "workspace is inactive")
		if err != nil || started {
			t.Fatalf("schedule_prepared cleanup replay: started=%v err=%v", started, err)
		}
		if err := st.FinishTaskCreationCleanup(
			ctx, lease, taskID, types.TaskOperationStatusFailed); err != nil {
			t.Fatalf("pre-ensure NotFound delete 应收敛: %v", err)
		}
	})

	t.Run("same-scope row is not adopted before ensure", func(t *testing.T) {
		lease, taskID := preparedA5ScheduleOnly(t, st, f, "cleanup-pre-ensure-foreign")
		if _, err := st.pool.Exec(ctx,
			`INSERT INTO schedules
			    (id, tenant_id, user_id, spec_json, scope_json, status)
			 VALUES ($1, $2, $3, '{}', '{}', 'paused')`,
			taskID, f.tenantID, f.userID); err != nil {
			t.Fatal(err)
		}
		if _, err := st.BeginTaskCreationCleanup(
			ctx, lease, taskID, "MEMBERSHIP_INACTIVE", "workspace is inactive",
		); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("pre-ensure 同 scope 同 TaskID 行也不得被认领: %v", err)
		}
		var count int
		if err := st.pool.QueryRow(ctx,
			`SELECT count(*) FROM schedules WHERE id = $1`, taskID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatal("拒绝的 pre-ensure aggregate 被误删")
		}
	})
}

func TestTaskCreationCleanup_ScheduleEnsuredNeverAdoptsLateAggregate(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	ctx := t.Context()

	t.Run("aggregate already present at begin", func(t *testing.T) {
		p := preparedA5Commit(t, st, f, "cleanup-ensured-present")
		if _, err := st.pool.Exec(ctx,
			`INSERT INTO schedules
			    (id, tenant_id, user_id, spec_json, scope_json, status)
			 VALUES ($1, $2, $3, '{}', '{}', 'paused')`,
			p.Definition.TaskID, f.tenantID, f.userID); err != nil {
			t.Fatal(err)
		}
		if _, err := st.BeginTaskCreationCleanup(
			ctx, p.Lease, p.Definition.TaskID, "ENSURE_FAILED", "ensure failed",
		); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("schedule_ensured 不得认领非原子 A2 aggregate: %v", err)
		}
	})

	t.Run("aggregate appears after begin", func(t *testing.T) {
		p := preparedA5Commit(t, st, f, "cleanup-ensured-late")
		started, err := st.BeginTaskCreationCleanup(
			ctx, p.Lease, p.Definition.TaskID, "ENSURE_FAILED", "ensure failed")
		if err != nil || !started {
			t.Fatalf("BeginCleanup: started=%v err=%v", started, err)
		}
		if _, err := st.pool.Exec(ctx,
			`INSERT INTO schedules
			    (id, tenant_id, user_id, spec_json, scope_json, status)
			 VALUES ($1, $2, $3, '{}', '{}', 'paused')`,
			p.Definition.TaskID, f.tenantID, f.userID); err != nil {
			t.Fatal(err)
		}
		if err := st.FinishTaskCreationCleanup(
			ctx, p.Lease, p.Definition.TaskID, types.TaskOperationStatusFailed,
		); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("late aggregate 必须保留并阻止 tombstone: %v", err)
		}
		var count int
		if err := st.pool.QueryRow(ctx,
			`SELECT count(*) FROM schedules WHERE id = $1`, p.Definition.TaskID,
		).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatal("late aggregate 被 cleanup 误删")
		}
	})
}

func TestTaskCreationCleanup_RefusesSameScopeReplacementGeneration(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	ctx := t.Context()
	p := preparedA5Commit(t, st, f, "cleanup-replacement-generation")
	if err := st.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); err != nil {
		t.Fatal(err)
	}
	started, err := st.BeginTaskCreationCleanup(
		ctx, p.Lease, p.Definition.TaskID, "ACTIVATE_REJECTED", "activation rejected")
	if err != nil || !started {
		t.Fatalf("BeginCleanup: started=%v err=%v", started, err)
	}
	op, err := st.LoadTaskCreationOperation(ctx, p.Lease.ID, p.Lease.TenantID, p.Lease.UserID)
	if err != nil {
		t.Fatal(err)
	}
	marker, err := decodeTaskCreationCleanupMarker(op.Result)
	if err != nil || !marker.AggregateExpected || marker.AggregateGeneration == "" {
		t.Fatalf("owned aggregate generation 未固化: marker=%+v err=%v", marker, err)
	}
	tx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, statement := range []string{
		`DELETE FROM task_fetch_targets WHERE schedule_id = $1`,
		`DELETE FROM schedule_playbooks WHERE schedule_id = $1`,
		`DELETE FROM schedules WHERE id = $1`,
	} {
		if _, err := tx.Exec(ctx, statement, p.Definition.TaskID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO schedules
		    (id, tenant_id, user_id, nl_description, spec_json, scope_json, status)
		 VALUES ($1, $2, $3, 'replacement', '{"replacement":true}', '{}', 'paused')`,
		p.Definition.TaskID, f.tenantID, f.userID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.FinishTaskCreationCleanup(
		ctx, p.Lease, p.Definition.TaskID, types.TaskOperationStatusFailed,
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("same-scope replacement generation 必须拒绝: %v", err)
	}
	var description string
	if err := st.pool.QueryRow(ctx,
		`SELECT nl_description FROM schedules WHERE id = $1`, p.Definition.TaskID,
	).Scan(&description); err != nil {
		t.Fatal(err)
	}
	if description != "replacement" {
		t.Fatalf("replacement aggregate 被修改/删除: %q", description)
	}
}

func TestTaskCreationCleanup_RefusesActiveOrForeignAggregate(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	ctx := t.Context()
	p := preparedA5Commit(t, st, f, "cleanup-unsafe")
	if err := st.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE schedules SET status = 'active' WHERE id = $1`, p.Definition.TaskID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.BeginTaskCreationCleanup(
		ctx, p.Lease, p.Definition.TaskID, "ERROR", "unsafe"); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("active aggregate 不得进入 cleanup: %v", err)
	}
	var count int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM schedules WHERE id = $1`, p.Definition.TaskID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("unsafe cleanup 删除了 active aggregate")
	}
}

func TestBlockTaskCreationOperationAfterSideEffect_QuarantinesWithoutDeletion(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	ctx := t.Context()

	t.Run("task and scope binding", func(t *testing.T) {
		p := preparedA5Commit(t, st, f, "block-binding")
		if err := st.BlockTaskCreationOperationAfterSideEffect(
			ctx, p.Lease, "foreign-task", "OWNERSHIP_UNKNOWN", "cannot prove owner"); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("foreign task id 必须拒绝: %v", err)
		}
		foreignLease := p.Lease
		foreignLease.UserID++
		if err := st.BlockTaskCreationOperationAfterSideEffect(
			ctx, foreignLease, p.Definition.TaskID, "OWNERSHIP_UNKNOWN", "cannot prove owner"); !errors.Is(err, types.ErrNotFound) {
			t.Fatalf("foreign lease scope 必须拒绝: %v", err)
		}
	})

	t.Run("response lost exact adopt preserves aggregate", func(t *testing.T) {
		p := preparedA5Commit(t, st, f, "block-preserve")
		if err := st.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); err != nil {
			t.Fatal(err)
		}
		lostStore := storeWithCommitResponseLost(st)
		if err := lostStore.BlockTaskCreationOperationAfterSideEffect(
			ctx, p.Lease, p.Definition.TaskID,
			"OWNERSHIP_UNKNOWN", "cannot prove owner"); !errors.Is(err, types.ErrDatabase) {
			t.Fatalf("response lost 应返回 database: %v", err)
		}
		if err := st.BlockTaskCreationOperationAfterSideEffect(
			ctx, p.Lease, p.Definition.TaskID,
			"OWNERSHIP_UNKNOWN", "cannot prove owner"); err != nil {
			t.Fatalf("quarantine exact replay: %v", err)
		}
		var status types.TaskOperationStatus
		var phase types.TaskCreationPhase
		var scheduleCount int
		if err := st.pool.QueryRow(ctx,
			`SELECT status, phase FROM task_creation_operations WHERE id = $1`, p.Lease.ID,
		).Scan(&status, &phase); err != nil {
			t.Fatal(err)
		}
		if err := st.pool.QueryRow(ctx,
			`SELECT count(*) FROM schedules WHERE id = $1`, p.Definition.TaskID,
		).Scan(&scheduleCount); err != nil {
			t.Fatal(err)
		}
		if status != types.TaskOperationStatusBlocked ||
			phase != types.TaskCreationPhaseBlocked || scheduleCount != 1 {
			t.Fatalf("quarantine 不得删除证据: status=%s phase=%s schedules=%d",
				status, phase, scheduleCount)
		}
		assertTaskCreationReceiptExactlyOne(t, st, p.Lease.ID)
		assertScheduleVisibility(t, st, f.userID, p.Definition.TaskID, false)
	})

	t.Run("schedule prepared response lost exact adopt", func(t *testing.T) {
		lease, taskID := preparedA5ScheduleOnly(t, st, f, "block-prepared")
		lostStore := storeWithCommitResponseLost(st)
		if err := lostStore.BlockTaskCreationOperationAfterSideEffect(
			ctx, lease, taskID, "CHECKPOINT_INVALID", "cannot prove ensure result",
		); !errors.Is(err, types.ErrDatabase) {
			t.Fatalf("schedule_prepared quarantine response lost: %v", err)
		}
		if err := st.BlockTaskCreationOperationAfterSideEffect(
			ctx, lease, taskID, "CHECKPOINT_INVALID", "cannot prove ensure result",
		); err != nil {
			t.Fatalf("schedule_prepared quarantine exact replay: %v", err)
		}
		op, err := st.LoadTaskCreationOperation(ctx, lease.ID, lease.TenantID, lease.UserID)
		if err != nil {
			t.Fatal(err)
		}
		marker, ok := decodeTaskCreationQuarantineMarker(op.Result)
		if !ok || !marker.TaskIDKnown || op.TaskID != taskID {
			t.Fatalf("prepared quarantine marker/binding 错误: op=%+v marker=%+v", op, marker)
		}
	})

	t.Run("cleanup pending from prepared response lost exact adopt", func(t *testing.T) {
		lease, taskID := preparedA5ScheduleOnly(t, st, f, "block-prepared-cleanup")
		if started, err := st.BeginTaskCreationCleanup(
			ctx, lease, taskID, "MEMBERSHIP_INACTIVE", "workspace is inactive",
		); err != nil || !started {
			t.Fatalf("BeginCleanup: started=%v err=%v", started, err)
		}
		lostStore := storeWithCommitResponseLost(st)
		if err := lostStore.BlockTaskCreationOperationAfterSideEffect(
			ctx, lease, taskID, "MEMBERSHIP_INACTIVE", "workspace is inactive",
		); !errors.Is(err, types.ErrDatabase) {
			t.Fatalf("cleanup_pending quarantine response lost: %v", err)
		}
		if err := st.BlockTaskCreationOperationAfterSideEffect(
			ctx, lease, taskID, "MEMBERSHIP_INACTIVE", "workspace is inactive",
		); err != nil {
			t.Fatalf("cleanup_pending quarantine exact replay: %v", err)
		}
	})

	t.Run("cleanup delete conflict escalates once and preserves primary failure", func(t *testing.T) {
		p := preparedA5Commit(t, st, f, "block-cleanup-escalation")
		if err := st.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); err != nil {
			t.Fatal(err)
		}
		if started, err := st.BeginTaskCreationCleanup(
			ctx, p.Lease, p.Definition.TaskID,
			"TASK_LIMIT_REACHED", "task limit reached",
		); err != nil || !started {
			t.Fatalf("BeginCleanup: started=%v err=%v", started, err)
		}
		if err := st.BlockTaskCreationOperationAfterSideEffect(
			ctx, p.Lease, p.Definition.TaskID,
			"UNSAFE_CLEANUP_TARGET", "delete target cannot be proven",
		); err != nil {
			t.Fatalf("cleanup escalation must quarantine: %v", err)
		}
		op, err := st.LoadTaskCreationOperation(ctx, p.Lease.ID, p.Lease.TenantID, p.Lease.UserID)
		if err != nil {
			t.Fatal(err)
		}
		marker, ok := decodeTaskCreationQuarantineMarker(op.Result)
		if !ok || marker.PrimaryErrorCode != "TASK_LIMIT_REACHED" ||
			marker.PrimaryErrorMessage != "task limit reached" ||
			op.ErrorCode != "UNSAFE_CLEANUP_TARGET" ||
			op.Status != types.TaskOperationStatusBlocked {
			t.Fatalf("cleanup escalation audit marker 错误: op=%+v marker=%+v", op, marker)
		}
		stale, err := st.ListStaleTaskCreationOperations(
			ctx, f.tenantID, time.Now().Add(time.Hour), 100)
		if err != nil {
			t.Fatal(err)
		}
		if containsOperation(stale, p.Lease.ID) {
			t.Fatal("quarantined cleanup 不得继续 stale loop")
		}
		assertCreationPhaseAndScheduleStatus(t, st, p.Lease.ID, p.Definition.TaskID,
			types.TaskCreationPhaseBlocked, types.ScheduleStatusPaused)
	})

	t.Run("corrupt prepared checkpoint uses explicit unknown reservation", func(t *testing.T) {
		lease, _ := preparedA5ScheduleOnly(t, st, f, "block-prepared-unknown")
		if _, err := st.pool.Exec(ctx,
			`UPDATE task_creation_operations SET prepared_schedule = $2 WHERE id = $1`,
			lease.ID, []byte(`{"task_id":"broken","task_id":"duplicate"}`)); err != nil {
			t.Fatal(err)
		}
		if err := st.BlockTaskCreationOperationAfterSideEffect(
			ctx, lease, "", "CHECKPOINT_INVALID", "task id cannot be proven",
		); err != nil {
			t.Fatalf("unknown quarantine: %v", err)
		}
		op, err := st.LoadTaskCreationOperation(ctx, lease.ID, lease.TenantID, lease.UserID)
		if err != nil {
			t.Fatal(err)
		}
		marker, ok := decodeTaskCreationQuarantineMarker(op.Result)
		if !ok || marker.TaskIDKnown || op.TaskID != "" {
			t.Fatalf("unknown quarantine marker 不正确: op=%+v marker=%+v", op, marker)
		}
		if err := st.BlockTaskCreationOperationAfterSideEffect(
			ctx, lease, "", "CHECKPOINT_INVALID", "task id cannot be proven",
		); err != nil {
			t.Fatalf("unknown quarantine exact replay: %v", err)
		}
	})

	t.Run("activated completion failure is quarantined without deleting active task", func(t *testing.T) {
		p := preparedA5Commit(t, st, f, "block-activated")
		if err := st.CommitPausedCompiledTaskDefinitionForCreation(ctx, p); err != nil {
			t.Fatal(err)
		}
		started, err := st.BeginTaskCreationActivation(ctx, p.Lease, p.Definition.TaskID)
		if err != nil || !started {
			t.Fatalf("BeginActivation: %v %v", started, err)
		}
		if err := st.CommitTaskCreationActivation(ctx, p.Lease, p.Definition.TaskID); err != nil {
			t.Fatal(err)
		}
		if err := st.CompleteTaskCreationOperation(
			ctx, p.Lease, "different-task", json.RawMessage(`{"created":true}`),
		); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("deterministic completion mismatch 应 conflict: %v", err)
		}
		if err := st.BlockTaskCreationOperationAfterSideEffect(
			ctx, p.Lease, p.Definition.TaskID,
			"COMPLETION_INVALID", "completion checkpoint cannot be proven"); err != nil {
			t.Fatalf("activated completion failure 应 quarantine: %v", err)
		}
		assertCreationPhaseAndScheduleStatus(t, st, p.Lease.ID, p.Definition.TaskID,
			types.TaskCreationPhaseBlocked, types.ScheduleStatusActive)
		authorized, err := st.AuthorizeScheduledRun(ctx, p.Definition.TaskID, f.userID)
		if err != nil || authorized {
			t.Fatalf("quarantined active aggregate 必须停止 run authorization: %v %v", authorized, err)
		}
		var (
			activeRows          int
			reservationRetained bool
		)
		if err := st.pool.QueryRow(ctx,
			`SELECT
			   (SELECT count(*) FROM schedules WHERE id = $1 AND status = 'active'),
			   COALESCE((SELECT (result->>'reservation_retained')::boolean
			               FROM task_creation_operations WHERE id = $2), false)`,
			p.Definition.TaskID, p.Lease.ID,
		).Scan(&activeRows, &reservationRetained); err != nil {
			t.Fatal(err)
		}
		if activeRows != 1 || !reservationRetained {
			t.Fatalf("active quarantine 应保留可与 active schedule 去重的 durable reservation: rows=%d reservation=%v",
				activeRows, reservationRetained)
		}
	})
}

func taskCreationCreateParams(
	f *compiledTaskFixture,
	id string,
) types.CreateTaskCreationOperationParams {
	return types.CreateTaskCreationOperationParams{
		ID:       id,
		TenantID: f.tenantID,
		UserID:   f.userID,
		Args: json.RawMessage(
			`{"intent":"monitor official updates","approved_fetch_plan":{"sources":[]}}`),
		Summary:   "创建测试任务",
		ExpiresAt: time.Now().Add(time.Hour),
	}
}

func createAndAcquireA5Operation(
	t *testing.T,
	st *Store,
	f *compiledTaskFixture,
	owner string,
) *types.TaskCreationOperation {
	t.Helper()
	created, err := st.CreateTaskCreationOperation(
		t.Context(), taskCreationCreateParams(f, uuid.NewString()))
	if err != nil {
		t.Fatal(err)
	}
	op, err := st.AcquireTaskCreationOperation(t.Context(), types.AcquireTaskCreationOperationParams{
		ID: created.ID, TenantID: f.tenantID, UserID: f.userID,
		LeaseOwner: owner + "-" + uuid.NewString(), LeaseDuration: 10 * time.Minute,
		ReceiptProvider: "feishu_message_patch", ReceiptTarget: "om-a5-" + created.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func preparedA5Commit(
	t *testing.T,
	st *Store,
	f *compiledTaskFixture,
	suffix string,
) types.CommitPausedCompiledTaskDefinitionForCreationParams {
	return preparedA5CommitWithStrictness(t, st, f, suffix, types.StrictnessStrict)
}

func preparedA5CommitWithStrictness(
	t *testing.T,
	st *Store,
	f *compiledTaskFixture,
	suffix string,
	strictness types.PushStrictness,
) types.CommitPausedCompiledTaskDefinitionForCreationParams {
	return preparedA5CommitWithSourcesAndStrictness(t, st, f, suffix, strictness,
		validA5PlanSource(t, "a5-test-"+suffix+"-"+uuid.NewString(), "计划源 1"))
}

func preparedA5CommitWithSources(
	t *testing.T,
	st *Store,
	f *compiledTaskFixture,
	suffix string,
	sources ...compiledPlanTarget,
) types.CommitPausedCompiledTaskDefinitionForCreationParams {
	return preparedA5CommitWithSourcesAndStrictness(
		t, st, f, suffix, types.StrictnessStrict, sources...)
}

func preparedA5CommitWithSourcesAndStrictness(
	t *testing.T,
	st *Store,
	f *compiledTaskFixture,
	suffix string,
	strictness types.PushStrictness,
	sources ...compiledPlanTarget,
) types.CommitPausedCompiledTaskDefinitionForCreationParams {
	t.Helper()
	op := createAndAcquireA5Operation(t, st, f, "a5-"+suffix)
	lease := op.Lease()
	plan, err := json.Marshal(compiledFetchPlan{Targets: sources})
	if err != nil {
		t.Fatal(err)
	}
	def := types.PausedCompiledTaskDefinition{
		TaskID: f.taskID(), TenantID: f.tenantID, UserID: f.userID,
		NLDescription:   "每天早上监控指定主题",
		SpecJSON:        json.RawMessage(`{"cron":"0 8 * * *","tz":"Asia/Shanghai"}`),
		ScopeJSON:       json.RawMessage(`{"max_items":5}`),
		PlaybookContent: "只看官方与高可信来源。", FetchPlan: plan,
		Strictness: strictness,
	}
	definitionDigest, err := types.DigestPausedCompiledTaskDefinition(def)
	if err != nil {
		t.Fatal(err)
	}
	protocol := compiledTaskDefinitionProtocolV1
	allToolCalls := len(sources) > 0
	for _, source := range sources {
		allToolCalls = allToolCalls && source.ToolName != "" &&
			len(bytes.TrimSpace(source.ToolArgs)) > 0
	}
	if allToolCalls {
		protocol = compiledTaskDefinitionProtocolV2
	}
	definition, err := json.Marshal(struct {
		Version          string                             `json:"version"`
		DefinitionDigest string                             `json:"definition_digest"`
		Definition       types.PausedCompiledTaskDefinition `json:"definition"`
	}{
		Version: protocol, DefinitionDigest: definitionDigest, Definition: def,
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := json.Marshal(struct {
		TaskID   string `json:"task_id"`
		Prepared bool   `json:"prepared"`
		Revision int    `json:"revision"`
	}{TaskID: def.TaskID, Prepared: true, Revision: 1})
	if err != nil {
		t.Fatal(err)
	}
	receipt := []byte(`{"ensured":true,"paused":true}`)
	if err := st.SealTaskCreationCommand(t.Context(), lease, []byte(`{"intent":"test"}`)); err != nil {
		t.Fatal(err)
	}
	started, err := st.BeginTaskCreationTranslation(t.Context(), lease)
	if err != nil || !started {
		t.Fatalf("BeginTranslation: %v %v", started, err)
	}
	digest := digestOf(definition)
	if err := st.CheckpointTaskCreationDefinition(t.Context(), lease, definition, digest); err != nil {
		t.Fatal(err)
	}
	if err := st.CheckpointTaskCreationSchedule(t.Context(), lease, prepared); err != nil {
		t.Fatal(err)
	}
	if err := st.CheckpointTaskCreationEnsureReceipt(
		t.Context(), lease, receipt, def.TaskID); err != nil {
		t.Fatal(err)
	}
	return types.CommitPausedCompiledTaskDefinitionForCreationParams{
		Lease: lease, Definition: def, CompiledDigest: digest,
		PreparedSchedule: prepared, EnsureReceipt: receipt,
	}
}

func validA5PlanSource(t *testing.T, query, title string) compiledPlanTarget {
	t.Helper()
	source, message := acquisitiontool.BuildTarget(acquisitiontool.Requirement{
		Platform: "web", Capability: "search", Title: title,
		Params: map[string]string{"query": query},
	})
	if message != "" || source == nil {
		t.Fatalf("build valid A5 source: source=%+v message=%q", source, message)
	}
	return compiledPlanTarget{
		Platform: string(source.Platform), Capability: string(source.Capability),
		Title: source.Title, URL: source.URL,
		Config: append(json.RawMessage(nil), source.Config...),
	}
}

func assertTaskCreationApprovedDefinition(
	t *testing.T,
	st *Store,
	p types.CommitPausedCompiledTaskDefinitionForCreationParams,
) ApprovedDefinitionVersionRecord {
	t.Helper()
	record, err := st.GetCurrentApprovedDefinition(
		t.Context(), p.Lease.TenantID, p.Lease.UserID, p.Definition.TaskID)
	if err != nil {
		t.Fatalf("GetCurrentApprovedDefinition: %v", err)
	}
	if record.Version != initialApprovedDefinitionVersion ||
		record.OperationRef != taskCreationOperationRefPrefix+p.Lease.ID ||
		record.Definition.ExecutionMode != types.ExecutionModeCompiled ||
		record.Definition.SourceScope != taskstate.SourceScopeApprovedPlan ||
		record.Definition.Intent != p.Definition.PlaybookContent ||
		record.Definition.PlaybookContent != p.Definition.PlaybookContent ||
		record.Definition.NLDescription != p.Definition.NLDescription {
		t.Fatalf("approved definition differs: %+v", record)
	}
	var currentPlan compiledFetchPlan
	if err := json.Unmarshal(p.Definition.FetchPlan, &currentPlan); err != nil {
		t.Fatalf("decode current fetch-target plan: %v", err)
	}
	legacyPlan := taskstate.FetchPlanV1{
		Sources: make([]taskstate.PlanSourceV1, 0, len(currentPlan.Targets)),
	}
	expectedSources := make([]taskstate.ApprovedSourceV1, 0, len(currentPlan.Targets))
	for _, target := range currentPlan.Targets {
		var sourceID int64
		if err := st.pool.QueryRow(t.Context(),
			`SELECT src.id
			   FROM task_fetch_targets ss JOIN fetch_targets src ON src.id=ss.fetch_target_id
			  WHERE ss.schedule_id=$1 AND src.url=$2`,
			p.Definition.TaskID, target.URL,
		).Scan(&sourceID); err != nil {
			t.Fatal(err)
		}
		legacyPlan.Sources = append(legacyPlan.Sources, taskstate.PlanSourceV1{
			Platform:   types.Platform(target.Platform),
			Capability: types.Capability(target.Capability),
			Title:      target.Title, URL: target.URL,
			Config: append(json.RawMessage(nil), target.Config...),
		})
		expectedSources = append(expectedSources, taskstate.ApprovedSourceV1{
			SourceID: sourceID, Platform: types.Platform(target.Platform),
			Capability: types.Capability(target.Capability),
			Title:      target.Title, URL: target.URL,
			Config: append(json.RawMessage(nil), target.Config...),
		})
	}
	legacyPlanJSON, err := json.Marshal(legacyPlan)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := taskstate.BuildApprovedDefinitionV1(taskstate.ApprovedDefinitionInputV1{
		TenantID: p.Definition.TenantID, UserID: p.Definition.UserID,
		TaskID: p.Definition.TaskID, Intent: p.Definition.PlaybookContent,
		NLDescription: p.Definition.NLDescription,
		SpecJSON:      p.Definition.SpecJSON, ScopeJSON: p.Definition.ScopeJSON,
		PlaybookContent: p.Definition.PlaybookContent,
		SourceScope:     taskstate.SourceScopeApprovedPlan,
		FetchPlan:       legacyPlanJSON, Strictness: p.Definition.Strictness,
		Sources: expectedSources, ExecutionMode: types.ExecutionModeCompiled,
		DeliveryPolicy: taskstate.DeliveryPolicyOwnerFeishu,
		BudgetPolicy:   taskstate.BudgetPolicyInheritTenantQuota,
	})
	if err != nil {
		t.Fatalf("build independently expected approved definition: %v", err)
	}
	expectedPayload, err := taskstate.EncodeApprovedDefinitionV1(expected)
	if err != nil {
		t.Fatal(err)
	}
	expectedDigest, err := taskstate.DigestApprovedDefinitionV1(expected)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(record.Payload, expectedPayload) || record.Digest != expectedDigest ||
		record.Definition.Strictness != p.Definition.Strictness ||
		record.Definition.DeliveryPolicy != taskstate.DeliveryPolicyOwnerFeishu ||
		record.Definition.BudgetPolicy != taskstate.BudgetPolicyInheritTenantQuota {
		t.Fatalf("approved definition exact projection differs:\n got=%s\nwant=%s",
			record.Payload, expectedPayload)
	}
	var versionCount, adaptiveCount int
	if err := st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM task_approved_definition_versions
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		p.Lease.TenantID, p.Lease.UserID, p.Definition.TaskID,
	).Scan(&versionCount); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM task_adaptive_states
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		p.Lease.TenantID, p.Lease.UserID, p.Definition.TaskID,
	).Scan(&adaptiveCount); err != nil {
		t.Fatal(err)
	}
	if versionCount != 1 || adaptiveCount != 0 {
		t.Fatalf("definition/adaptive rows=%d/%d, want 1/0", versionCount, adaptiveCount)
	}
	for _, source := range record.Definition.Sources {
		var sourceID int64
		if err := st.pool.QueryRow(t.Context(),
			`SELECT src.id
			   FROM task_fetch_targets ss JOIN fetch_targets src ON src.id=ss.fetch_target_id
			  WHERE ss.schedule_id=$1 AND src.url=$2`,
			p.Definition.TaskID, source.URL,
		).Scan(&sourceID); err != nil {
			t.Fatal(err)
		}
		if source.SourceID != sourceID {
			t.Fatalf("approved source %s id=%d, materialized id=%d",
				source.URL, source.SourceID, sourceID)
		}
	}
	return record
}

func preparedA5ScheduleOnly(
	t *testing.T,
	st *Store,
	f *compiledTaskFixture,
	suffix string,
) (types.TaskCreationLease, string) {
	t.Helper()
	op := createAndAcquireA5Operation(t, st, f, "a5-prepared-"+suffix)
	lease := op.Lease()
	def := f.definition(f.taskID(), types.StrictnessNormal, f.url(suffix))
	definitionDigest, err := types.DigestPausedCompiledTaskDefinition(def)
	if err != nil {
		t.Fatal(err)
	}
	definition, err := json.Marshal(struct {
		Version          string                             `json:"version"`
		DefinitionDigest string                             `json:"definition_digest"`
		Definition       types.PausedCompiledTaskDefinition `json:"definition"`
	}{
		Version:          compiledTaskDefinitionProtocolV1,
		DefinitionDigest: definitionDigest, Definition: def,
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := json.Marshal(struct {
		TaskID   string `json:"task_id"`
		Prepared bool   `json:"prepared"`
		Revision int    `json:"revision"`
	}{TaskID: def.TaskID, Prepared: true, Revision: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SealTaskCreationCommand(
		t.Context(), lease, []byte(`{"intent":"test"}`)); err != nil {
		t.Fatal(err)
	}
	started, err := st.BeginTaskCreationTranslation(t.Context(), lease)
	if err != nil || !started {
		t.Fatalf("BeginTranslation: %v %v", started, err)
	}
	digest := digestOf(definition)
	if err := st.CheckpointTaskCreationDefinition(
		t.Context(), lease, definition, digest); err != nil {
		t.Fatal(err)
	}
	if err := st.CheckpointTaskCreationSchedule(t.Context(), lease, prepared); err != nil {
		t.Fatal(err)
	}
	return lease, def.TaskID
}

func cleanupA5Fixture(t *testing.T, st *Store, f *compiledTaskFixture) {
	t.Helper()
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_creation_receipts WHERE tenant_id = $1`, f.tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_creation_operations WHERE tenant_id = $1`, f.tenantID)
	})
}

func assertCreationPhaseAndScheduleStatus(
	t *testing.T,
	st *Store,
	operationID string,
	taskID string,
	wantPhase types.TaskCreationPhase,
	wantStatus types.ScheduleStatus,
) {
	t.Helper()
	var phase types.TaskCreationPhase
	var status types.ScheduleStatus
	if err := st.pool.QueryRow(t.Context(),
		`SELECT p.phase, s.status
		   FROM task_creation_operations p JOIN schedules s ON s.id = p.task_id
		  WHERE p.id = $1 AND s.id = $2`, operationID, taskID,
	).Scan(&phase, &status); err != nil {
		t.Fatal(err)
	}
	if phase != wantPhase || status != wantStatus {
		t.Fatalf("phase/status=%s/%s want=%s/%s", phase, status, wantPhase, wantStatus)
	}
}

func assertScheduleVisibility(t *testing.T, st *Store, userID int64, taskID string, visible bool) {
	t.Helper()
	schedules, err := st.ListSchedulesByUser(t.Context(), userID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, schedule := range schedules {
		if schedule.ID == taskID {
			found = true
			break
		}
	}
	if found != visible {
		t.Fatalf("schedule %s visibility=%v want=%v", taskID, found, visible)
	}
}

func assertProvisioningScheduleIsNotUserManageable(
	t *testing.T,
	st *Store,
	userID int64,
	taskID string,
) {
	t.Helper()
	ctx := t.Context()
	var (
		nlDescription string
		specJSON      []byte
		strictness    *string
		playbook      string
		fetchPlan     []byte
		linkCount     int
	)
	if err := st.pool.QueryRow(ctx,
		`SELECT s.nl_description, s.spec_json, s.push_strictness,
		        p.content, p.fetch_plan,
		        (SELECT count(*) FROM task_fetch_targets ss WHERE ss.schedule_id = s.id)
		   FROM schedules s JOIN schedule_playbooks p ON p.schedule_id = s.id
		  WHERE s.id = $1`, taskID,
	).Scan(&nlDescription, &specJSON, &strictness, &playbook, &fetchPlan, &linkCount); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetSchedule(ctx, taskID, userID); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("provisioning schedule direct Get 必须隐藏: %v", err)
	}
	changedDescription := "must-not-change"
	if err := st.UpdateScheduleSpec(
		ctx, taskID, json.RawMessage(`{"changed":true}`), &changedDescription,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("provisioning schedule direct Update 必须隐藏: %v", err)
	}
	if err := st.DeleteSchedule(ctx, taskID, userID); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("provisioning schedule direct Delete 必须隐藏: %v", err)
	}
	if err := st.SetScheduleStrictness(
		ctx, taskID, userID, types.StrictnessStrict,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("provisioning schedule strictness 修改必须隐藏: %v", err)
	}
	if _, err := st.GetSchedulePlaybook(ctx, userID, taskID); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("provisioning playbook Get 必须隐藏: %v", err)
	}
	if ok, err := st.UpsertSchedulePlaybook(
		ctx, userID, taskID, "must-not-change"); err != nil || ok {
		t.Fatalf("provisioning playbook Upsert 必须无写入: ok=%v err=%v", ok, err)
	}
	if ok, err := st.SetFetchPlan(
		ctx, userID, taskID, json.RawMessage(`{"targets":[]}`)); err != nil || ok {
		t.Fatalf("provisioning fetch plan 修改必须无写入: ok=%v err=%v", ok, err)
	}
	if err := st.ReplaceTaskFetchTargets(ctx, userID, taskID, nil); err != nil {
		t.Fatalf("provisioning source Replace 应安全 no-op: %v", err)
	}
	ids, err := st.ListTaskFetchTargetIDs(ctx, userID, taskID)
	if err != nil || len(ids) != 0 {
		t.Fatalf("provisioning source List 必须隐藏: ids=%v err=%v", ids, err)
	}
	var (
		gotNL         string
		gotSpec       []byte
		gotStrictness *string
		gotPlaybook   string
		gotFetchPlan  []byte
		gotLinks      int
	)
	if err := st.pool.QueryRow(ctx,
		`SELECT s.nl_description, s.spec_json, s.push_strictness,
		        p.content, p.fetch_plan,
		        (SELECT count(*) FROM task_fetch_targets ss WHERE ss.schedule_id = s.id)
		   FROM schedules s JOIN schedule_playbooks p ON p.schedule_id = s.id
		  WHERE s.id = $1`, taskID,
	).Scan(&gotNL, &gotSpec, &gotStrictness, &gotPlaybook, &gotFetchPlan, &gotLinks); err != nil {
		t.Fatalf("provisioning direct-id 操作删除或损坏了 aggregate: %v", err)
	}
	if gotNL != nlDescription || !taskCreationJSONEqual(gotSpec, specJSON) ||
		!nullableStringsEqual(gotStrictness, strictness) || gotPlaybook != playbook ||
		!taskCreationJSONEqual(gotFetchPlan, fetchPlan) || gotLinks != linkCount {
		t.Fatalf("provisioning direct-id 操作产生写入: nl=%q/%q spec=%s/%s strict=%v/%v playbook=%q/%q fetch=%s/%s links=%d/%d",
			gotNL, nlDescription, gotSpec, specJSON, gotStrictness, strictness,
			gotPlaybook, playbook, gotFetchPlan, fetchPlan, gotLinks, linkCount)
	}
}

func containsOperation(operations []types.TaskCreationOperation, id string) bool {
	for _, operation := range operations {
		if operation.ID == id {
			return true
		}
	}
	return false
}

type commitResponseLostTx struct {
	pgx.Tx
	err  error
	once sync.Once
}

type taskCreationObservedTx struct {
	pgx.Tx
	pauseAfter  string
	paused      chan struct{}
	release     chan struct{}
	notifyAfter string
	notified    chan struct{}
	pauseOnce   sync.Once
	notifyOnce  sync.Once
}

func (tx *taskCreationObservedTx) QueryRow(
	ctx context.Context,
	sql string,
	args ...any,
) pgx.Row {
	row := tx.Tx.QueryRow(ctx, sql, args...)
	return &taskCreationObservedRow{
		Row:    row,
		pause:  strings.Contains(sql, tx.pauseAfter) && tx.pauseAfter != "",
		paused: tx.paused, release: tx.release, pauseOnce: &tx.pauseOnce,
		notify:   strings.Contains(sql, tx.notifyAfter) && tx.notifyAfter != "",
		notified: tx.notified, notifyOnce: &tx.notifyOnce,
	}
}

type taskCreationObservedRow struct {
	pgx.Row
	pause      bool
	paused     chan struct{}
	release    chan struct{}
	notify     bool
	notified   chan struct{}
	pauseOnce  *sync.Once
	notifyOnce *sync.Once
}

func (row *taskCreationObservedRow) Scan(dest ...any) error {
	if err := row.Row.Scan(dest...); err != nil {
		return err
	}
	if row.notify && row.notified != nil {
		row.notifyOnce.Do(func() {
			select {
			case row.notified <- struct{}{}:
			default:
			}
		})
	}
	if row.pause {
		row.pauseOnce.Do(func() {
			close(row.paused)
			<-row.release
		})
	}
	return nil
}

func (tx *commitResponseLostTx) Commit(ctx context.Context) error {
	var commitErr error
	tx.once.Do(func() { commitErr = tx.Tx.Commit(ctx) })
	if commitErr != nil {
		return commitErr
	}
	return tx.err
}

func storeWithCommitResponseLost(st *Store) *Store {
	lost := *st
	lost.beginTx = func(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
		tx, err := st.pool.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		return &commitResponseLostTx{Tx: tx, err: errInjectedTaskCreationCommit}, nil
	}
	return &lost
}
