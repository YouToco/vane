package store

import (
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
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/agentledger"
	"github.com/YouToco/vane/types"
)

func purgeStore(t *testing.T) *Store {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过租户清理集成测试")
	}
	ctx := t.Context()
	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 失败: %v", err)
	}
	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 失败: %v", err)
	}
	registerStoreClose(t, st)
	return st
}

// TestInvariant_PurgeListCoversAllTenantTables 是红线 I-A3 的**上半条**守卫。
//
// 任何新增带 tenant_id 的表，如果没被加进 purgeOrder，硬删就会把它的数据静默漏下——
// 一个"已彻底删除"的租户的数据继续留在库里，而没有任何报错。
//
// 靠 review 记住"加表时别忘了改清单"是靠不住的：加表的人未必知道有这个清单存在。
// 这条把它变成结构性保证——对账数据库的真实 schema，不是对账文档。
func TestInvariant_PurgeListCoversAllTenantTables(t *testing.T) {
	st := purgeStore(t)
	rows, err := st.pool.Query(t.Context(),
		`SELECT table_name FROM information_schema.columns
		  WHERE table_schema = 'public' AND column_name = 'tenant_id'
		    AND table_name <> 'tenants'
		  ORDER BY table_name`)
	if err != nil {
		t.Fatalf("查询带 tenant_id 的表失败: %v", err)
	}
	defer rows.Close()

	inList := map[string]bool{}
	for _, s := range purgeOrder {
		inList[s.table] = true
	}
	for rows.Next() {
		var tbl string
		if err := rows.Scan(&tbl); err != nil {
			t.Fatalf("扫描失败: %v", err)
		}
		if !inList[tbl] {
			t.Errorf("表 %s 有 tenant_id 却不在 purgeOrder 里 —— 硬删会把它的数据静默漏下，"+
				"一个「已彻底删除」的租户的数据继续留在库里且无任何报错。"+
				"请把它加进 store/tenant_purge.go 的 purgeOrder（注意外键顺序）", tbl)
		}
	}
}

// TestInvariant_PurgeListNeverTouchesSharedFacts 是红线 I-A3 的**下半条**，也是更危险的那半。
//
// sources / content_items / content_sources 是跨租户客观事实：
// 同一篇文章可能被多个租户的信源指向。把它们写进 purgeOrder 意味着删一个租户会
// **删掉别的租户还在用的内容**——不可逆，且受害者毫不知情。
//
// 上半条漏了的后果是"没删干净"（可修复）；这半条漏了的后果是"删了别人的数据"。
// 代价不对称，所以清单必须是白名单，且这条守卫必须存在。
func TestInvariant_PurgeListNeverTouchesSharedFacts(t *testing.T) {
	inList := map[string]bool{}
	for _, s := range purgeOrder {
		inList[s.table] = true
	}
	for _, shared := range purgeSharedTables {
		if inList[shared] {
			t.Errorf("**红线 I-A3 被破坏**：%s 是跨租户客观事实（同一篇内容被多个租户的信源指向），"+
				"出现在 purgeOrder 里意味着删一个租户会删掉别的租户还在用的数据——不可逆", shared)
		}
	}
}

// TestInvariant_PurgeCoversScheduleChildren 补的是「按 tenant_id 列对账」看不见的那一类。
//
// schedule_playbooks / schedule_sources **没有 tenant_id 列**，只经 schedules 反查归属。
// 上面那条守卫扫的是 tenant_id 列，扫不到它们——2026-07-19 实测发现这个盲区。
// 漏删它们的后果：删 schedules 时撞外键（整个清理事务失败），或残留孤儿行。
func TestInvariant_PurgeCoversScheduleChildren(t *testing.T) {
	st := purgeStore(t)
	rows, err := st.pool.Query(t.Context(),
		`SELECT DISTINCT tc.table_name
		   FROM information_schema.table_constraints tc
		   JOIN information_schema.constraint_column_usage ccu
		     ON ccu.constraint_name = tc.constraint_name
		  WHERE tc.constraint_type = 'FOREIGN KEY'
		    AND tc.table_schema = 'public'
		    AND ccu.table_name = 'schedules'
		    AND tc.table_name <> 'schedules'`)
	if err != nil {
		t.Fatalf("查询 schedules 的子表失败: %v", err)
	}
	defer rows.Close()

	inList := map[string]bool{}
	for _, s := range purgeOrder {
		inList[s.table] = true
	}
	for rows.Next() {
		var tbl string
		if err := rows.Scan(&tbl); err != nil {
			t.Fatalf("扫描失败: %v", err)
		}
		if !inList[tbl] {
			t.Errorf("表 %s 外键引用 schedules 却不在 purgeOrder 里。它没有 tenant_id 列，"+
				"所以「按 tenant_id 对账」那条守卫扫不到它——漏删会让清理事务撞外键失败", tbl)
		}
	}
}

// seedPurgeTenant 建一个带完整数据的租户，返回 tenantID。
func seedPurgeTenant(t *testing.T, st *Store) int64 {
	t.Helper()
	ctx := t.Context()
	u, err := st.UpsertUserByOpenID(ctx, "purge_"+uuid.NewString(), "清理测试")
	if err != nil {
		t.Fatalf("建用户失败: %v", err)
	}
	code := uniqueCode(t, "purge")
	if _, err := st.IssueInvite(ctx, code, nil, 1, nil); err != nil {
		t.Fatalf("签发邀请码失败: %v", err)
	}
	tn, err := st.CreateTenantWithInvite(ctx, code, u.ID)
	if err != nil {
		t.Fatalf("建租户失败: %v", err)
	}
	// 塞一点租户数据，让清理有东西可删。
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO profiles (user_id, tenant_id, summary) VALUES ($1, $2, '测试画像')`,
		u.ID, tn.ID); err != nil {
		t.Fatalf("建画像失败: %v", err)
	}
	taskID := "purge-task-" + uuid.NewString()
	editOperationID := "purge-definition-edit-" + uuid.NewString()
	tx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("开启 Approved/Adaptive 夹具事务失败: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var editSessionID int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO agent_sessions (tenant_id, user_id)
		VALUES ($1, $2) RETURNING id`, tn.ID, u.ID,
	).Scan(&editSessionID); err != nil {
		t.Fatalf("建 Definition Edit session 清理夹具失败: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO schedules (
			id, tenant_id, user_id, nl_description, status, execution_mode
		) VALUES ($1, $2, $3, '清理测试任务', 'paused', 'compiled')`,
		taskID, tn.ID, u.ID); err != nil {
		t.Fatalf("建清理任务夹具失败: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO task_approved_definition_versions (
			tenant_id, user_id, task_id, version, schema_version,
			execution_mode, definition_digest, payload, approval_ref
		) VALUES (
			$2, $3, $1, 1, 'approved-definition/v1', 'compiled',
			encode(sha256(convert_to('{"task":"purge"}', 'UTF8')), 'hex'),
			convert_to('{"task":"purge"}', 'UTF8'), $4
		)`, taskID, tn.ID, u.ID, "purge-approval-"+uuid.NewString()); err != nil {
		t.Fatalf("建 Approved Definition 清理夹具失败: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE schedules
		   SET approved_definition_version = 1,
		       approved_definition_digest =
		           encode(sha256(convert_to('{"task":"purge"}', 'UTF8')), 'hex')
		 WHERE id = $1`, taskID); err != nil {
		t.Fatalf("绑定 Approved Definition head 失败: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO task_adaptive_states (
			tenant_id, user_id, task_id, version, schema_version,
			payload_digest, payload, basis_definition_version,
			basis_definition_digest, last_known_good_definition_version
		) VALUES (
			$2, $3, $1, 1, 'adaptive-state/v1',
			encode(sha256(convert_to('{"queries":["purge"]}', 'UTF8')), 'hex'),
			convert_to('{"queries":["purge"]}', 'UTF8'), 1,
			encode(sha256(convert_to('{"task":"purge"}', 'UTF8')), 'hex'), 1
		)`, taskID, tn.ID, u.ID); err != nil {
		t.Fatalf("建 Adaptive State 清理夹具失败: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO task_definition_edit_operations (
			id, tenant_id, user_id, target_tenant_id, target_user_id,
				task_id, session_id, approval_ref, expires_at, original_status,
			base_definition_version, base_definition_digest, base_definition,
			target_definition_version, target_definition_digest,
			target_definition,
			canonical_proposal, proposal_digest,
			prepared_edit, prepared_edit_digest,
			base_snapshot, base_snapshot_digest,
				receipt_provider, receipt_target
			) VALUES (
				$1, $2, $3, $2, $3, $4, $5, $6,
				clock_timestamp()+interval '1 day', 'paused', 1,
			encode(sha256(convert_to('{"base":"purge"}', 'UTF8')), 'hex'),
			convert_to('{"base":"purge"}', 'UTF8'), 2,
			encode(sha256(convert_to('{"target":"purge"}', 'UTF8')), 'hex'),
			convert_to('{"target":"purge"}', 'UTF8'),
			convert_to('{"proposal":"purge"}', 'UTF8'),
			encode(sha256(convert_to('{"proposal":"purge"}', 'UTF8')), 'hex'),
			convert_to('{"prepared":"purge"}', 'UTF8'),
			encode(sha256(convert_to('{"prepared":"purge"}', 'UTF8')), 'hex'),
			convert_to('{"base_snapshot":"purge"}', 'UTF8'),
			encode(sha256(convert_to('{"base_snapshot":"purge"}', 'UTF8')), 'hex'),
				'feishu_card_patch:purge', 'om_purge'
			)`, editOperationID, tn.ID, u.ID, taskID, editSessionID,
		"purge-edit-approval-"+uuid.NewString()); err != nil {
		t.Fatalf("建 Definition Edit operation 清理夹具失败: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE task_definition_edit_operations
		   SET status='cancelled', tombstoned_at=clock_timestamp()
		 WHERE id=$1`, editOperationID); err != nil {
		t.Fatalf("终态化 Definition Edit operation 清理夹具失败: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO task_definition_edit_receipts (
			operation_id, tenant_id, user_id, session_id,
			provider, target, provider_key, status, payload, payload_digest,
			fence, attempt, session_recorded_at, session_messages_digest,
			provider_message_id, sent_at
		) VALUES (
			$1, $2, $3, $4, 'feishu_card_patch:purge', 'om_purge', $5,
			'sent', convert_to('{"status":"cancelled"}', 'UTF8'),
			encode(sha256(convert_to('{"status":"cancelled"}', 'UTF8')), 'hex'),
			1, 1, clock_timestamp(), repeat('a', 64), 'om_purge', clock_timestamp()
		)`, editOperationID, tn.ID, u.ID, editSessionID, uuid.NewString()); err != nil {
		t.Fatalf("建 Definition Edit receipt 清理夹具失败: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("提交 Approved/Adaptive 清理夹具失败: %v", err)
	}
	snapshotTx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("开启运行快照清理夹具事务失败: %v", err)
	}
	if _, err := snapshotTx.Exec(ctx,
		`SELECT set_config('app.tenant_id', $1, true)`,
		fmt.Sprintf("%d", tn.ID)); err != nil {
		_ = snapshotTx.Rollback(ctx)
		t.Fatalf("设置运行快照清理夹具租户失败: %v", err)
	}
	if _, err := snapshotTx.Exec(ctx,
		`INSERT INTO task_run_snapshots (
			tenant_id, user_id, task_id, temporal_workflow_id, temporal_run_id,
			run_kind, execution_mode, adaptive_version,
			capability_catalog_digest, tool_policy_digest, prompt_policy_digest,
			model_policy_digest, quota_policy_digest, definition_digest, plan_digest,
			payload_digest, reference_digest, reference_schema_version, payload, budget
		 ) VALUES (
			$1, $2, $3, $4, $5, 'scheduled', 'compiled', 0,
			repeat('0', 64), repeat('1', 64), repeat('2', 64), repeat('3', 64),
			repeat('4', 64), repeat('5', 64), repeat('6', 64), repeat('7', 64),
			repeat('8', 64), 'purge-fixture/v1', convert_to('{}', 'UTF8'), '{}'::jsonb
		 )`,
		tn.ID, u.ID, taskID, "purge-workflow-"+uuid.NewString(),
		"purge-run-"+uuid.NewString()); err != nil {
		_ = snapshotTx.Rollback(ctx)
		t.Fatalf("建运行快照清理夹具失败: %v", err)
	}
	if err := snapshotTx.Commit(ctx); err != nil {
		t.Fatalf("提交运行快照清理夹具失败: %v", err)
	}
	if _, err := st.CommitAgentSessionAppend(
		ctx,
		u.ID,
		editSessionID,
		"purge-fixture:"+uuid.NewString(),
		json.RawMessage(
			`[{"role":"user","content":"purge fixture"}]`,
		),
	); err != nil {
		t.Fatalf("建 Agent event 清理夹具失败: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := cleanupContext()
		defer cancel()
		cleanupExec(c, t, st, `DELETE FROM agent_events WHERE tenant_id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM task_definition_edit_receipts WHERE tenant_id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM task_definition_edit_operations WHERE tenant_id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM task_adaptive_states WHERE tenant_id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM task_approved_definition_versions WHERE tenant_id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM schedules WHERE tenant_id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM task_run_snapshots WHERE tenant_id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM profiles WHERE tenant_id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM agent_sessions WHERE tenant_id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM tenant_quota WHERE tenant_id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM memberships WHERE tenant_id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM invites WHERE code = $1`, code)
		cleanupExec(c, t, st, `DELETE FROM tenants WHERE id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM users WHERE id = $1`, u.ID)
	})
	return tn.ID
}

// TestPurgeTenant_DryRunChangesNothing：试运行必须真的什么都不改。
//
// 而它同时**真正执行了全部 DELETE**（最后回滚）——所以返回的行数是真实的，
// 且外键顺序、约束都被验证过。只数行数的 dry-run 验不了"顺序错了删不动"，
// 而那恰恰是这条路径最可能出的错。
func TestPurgeTenant_DryRunChangesNothing(t *testing.T) {
	st := purgeStore(t)
	tenantID := seedPurgeTenant(t, st)
	ctx := t.Context()

	rep, err := st.PurgeTenant(ctx, tenantID, true)
	if err != nil {
		t.Fatalf("试运行失败（外键顺序错了会在这里暴露）: %v", err)
	}
	if !rep.DryRun {
		t.Error("报告应标记为试运行")
	}
	if rep.Total == 0 {
		t.Error("试运行应报告将删除的行数，实得 0 —— 说明它什么都没执行，验不了外键顺序")
	}
	if rep.Rows["task_run_snapshots"] != 1 {
		t.Errorf("试运行报告必须包含运行快照，实得 %d", rep.Rows["task_run_snapshots"])
	}
	if rep.Rows["task_adaptive_states"] != 1 ||
		rep.Rows["task_approved_definition_versions"] != 1 {
		t.Errorf("试运行报告必须包含 Adaptive/Approved，实得 %d/%d",
			rep.Rows["task_adaptive_states"],
			rep.Rows["task_approved_definition_versions"])
	}
	if rep.Rows["task_definition_edit_operations"] != 1 ||
		rep.Rows["task_definition_edit_receipts"] != 1 {
		t.Errorf("试运行报告必须包含 Definition Edit operation/receipt，实得 %d/%d",
			rep.Rows["task_definition_edit_operations"],
			rep.Rows["task_definition_edit_receipts"])
	}
	if rep.Rows["agent_events"] != 3 {
		t.Errorf("试运行报告必须包含 Agent event，实得 %d",
			rep.Rows["agent_events"])
	}

	// 租户与数据必须原封不动。
	if _, err := st.GetTenant(ctx, tenantID); err != nil {
		t.Errorf("试运行后租户不该消失: %v", err)
	}
	var n int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM profiles WHERE tenant_id = $1`, tenantID).Scan(&n); err != nil {
		t.Fatalf("查画像失败: %v", err)
	}
	if n != 1 {
		t.Errorf("试运行后画像应还在，实得 %d 行 —— 事务没回滚，这是不可逆的数据损失", n)
	}
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_run_snapshots WHERE tenant_id = $1`, tenantID).Scan(&n); err != nil {
		t.Fatalf("查运行快照失败: %v", err)
	}
	if n != 1 {
		t.Errorf("试运行后运行快照应还在，实得 %d 行 —— 事务没回滚", n)
	}
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_adaptive_states WHERE tenant_id = $1`, tenantID).Scan(&n); err != nil {
		t.Fatalf("查 adaptive state 失败: %v", err)
	}
	if n != 1 {
		t.Errorf("试运行后 adaptive state 应还在，实得 %d 行 —— 事务没回滚", n)
	}
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_approved_definition_versions WHERE tenant_id = $1`, tenantID).Scan(&n); err != nil {
		t.Fatalf("查 approved definition 失败: %v", err)
	}
	if n != 1 {
		t.Errorf("试运行后 approved definition 应还在，实得 %d 行 —— 事务没回滚", n)
	}
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_definition_edit_operations WHERE tenant_id = $1`, tenantID,
	).Scan(&n); err != nil {
		t.Fatalf("查 Definition Edit operation 失败: %v", err)
	}
	if n != 1 {
		t.Errorf("试运行后 Definition Edit operation 应还在，实得 %d 行 —— 事务没回滚", n)
	}
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_definition_edit_receipts WHERE tenant_id = $1`, tenantID,
	).Scan(&n); err != nil {
		t.Fatalf("查 Definition Edit receipt 失败: %v", err)
	}
	if n != 1 {
		t.Errorf("试运行后 Definition Edit receipt 应还在，实得 %d 行 —— 事务没回滚", n)
	}
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_events WHERE tenant_id = $1`, tenantID,
	).Scan(&n); err != nil {
		t.Fatalf("查 Agent event 失败: %v", err)
	}
	if n != 3 {
		t.Errorf("试运行后 Agent event 应还在，实得 %d 行 —— 事务没回滚", n)
	}
}

// TestPurgeTenant_RealDeleteRemovesTenantData：真删要真的删干净。
func TestPurgeTenant_RealDeleteRemovesTenantData(t *testing.T) {
	st := purgeStore(t)
	tenantID := seedPurgeTenant(t, st)
	ctx := t.Context()

	rep, err := st.PurgeTenant(ctx, tenantID, false)
	if err != nil {
		t.Fatalf("清理失败: %v", err)
	}
	if rep.Rows["tenants"] != 1 {
		t.Errorf("租户行应被删除，实得 %d", rep.Rows["tenants"])
	}
	if rep.Rows["task_run_snapshots"] != 1 {
		t.Errorf("清理报告必须包含运行快照，实得 %d", rep.Rows["task_run_snapshots"])
	}
	if rep.Rows["task_adaptive_states"] != 1 ||
		rep.Rows["task_approved_definition_versions"] != 1 {
		t.Errorf("清理报告必须包含 Adaptive/Approved，实得 %d/%d",
			rep.Rows["task_adaptive_states"],
			rep.Rows["task_approved_definition_versions"])
	}
	if rep.Rows["task_definition_edit_operations"] != 1 ||
		rep.Rows["task_definition_edit_receipts"] != 1 {
		t.Errorf("清理报告必须包含 Definition Edit operation/receipt，实得 %d/%d",
			rep.Rows["task_definition_edit_operations"],
			rep.Rows["task_definition_edit_receipts"])
	}
	if rep.Rows["agent_events"] != 3 {
		t.Errorf("清理报告必须包含 Agent event，实得 %d",
			rep.Rows["agent_events"])
	}
	if _, err := st.GetTenant(ctx, tenantID); err == nil {
		t.Error("清理后租户仍存在")
	}
	var n int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM profiles WHERE tenant_id = $1`, tenantID).Scan(&n); err != nil {
		t.Fatalf("查画像失败: %v", err)
	}
	if n != 0 {
		t.Errorf("租户数据应被清空，profiles 仍有 %d 行", n)
	}
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_run_snapshots WHERE tenant_id = $1`, tenantID).Scan(&n); err != nil {
		t.Fatalf("查运行快照失败: %v", err)
	}
	if n != 0 {
		t.Errorf("租户数据应被清空，task_run_snapshots 仍有 %d 行", n)
	}
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_adaptive_states WHERE tenant_id = $1`, tenantID).Scan(&n); err != nil {
		t.Fatalf("查 adaptive state 失败: %v", err)
	}
	if n != 0 {
		t.Errorf("租户数据应被清空，task_adaptive_states 仍有 %d 行", n)
	}
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_approved_definition_versions WHERE tenant_id = $1`, tenantID).Scan(&n); err != nil {
		t.Fatalf("查 approved definition 失败: %v", err)
	}
	if n != 0 {
		t.Errorf("租户数据应被清空，task_approved_definition_versions 仍有 %d 行", n)
	}
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_definition_edit_operations WHERE tenant_id = $1`, tenantID,
	).Scan(&n); err != nil {
		t.Fatalf("查 Definition Edit operation 失败: %v", err)
	}
	if n != 0 {
		t.Errorf("租户数据应被清空，task_definition_edit_operations 仍有 %d 行", n)
	}
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_definition_edit_receipts WHERE tenant_id = $1`, tenantID,
	).Scan(&n); err != nil {
		t.Fatalf("查 Definition Edit receipt 失败: %v", err)
	}
	if n != 0 {
		t.Errorf("租户数据应被清空，task_definition_edit_receipts 仍有 %d 行", n)
	}
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_events WHERE tenant_id = $1`, tenantID,
	).Scan(&n); err != nil {
		t.Fatalf("查 Agent event 失败: %v", err)
	}
	if n != 0 {
		t.Errorf("租户数据应被清空，agent_events 仍有 %d 行", n)
	}
}

func TestPurgeTenant_DefinitionEditLockOrderDoesNotDeadlock(t *testing.T) {
	st := purgeStore(t)
	tenantID := seedPurgeTenant(t, st)
	ctx := t.Context()

	var taskID, operationID string
	if err := st.pool.QueryRow(ctx, `
		SELECT s.id, o.id
		  FROM schedules s
		  JOIN task_definition_edit_operations o
		    ON o.tenant_id=s.tenant_id AND o.user_id=s.user_id AND o.task_id=s.id
		 WHERE s.tenant_id=$1`, tenantID).Scan(&taskID, &operationID); err != nil {
		t.Fatalf("查 Definition Edit purge 锁序夹具失败: %v", err)
	}
	if _, err := st.pool.Exec(ctx, `
		UPDATE task_definition_edit_operations
		   SET status='executing', phase='db_quiesced',
		       confirmed_at=clock_timestamp(), tombstoned_at=NULL,
		       lease_owner='purge-lock-order-worker',
		       lease_until=clock_timestamp()+interval '10 minutes',
		       takeover_not_before=clock_timestamp()+interval '11 minutes',
		       fence=1, attempt=1
		 WHERE id=$1 AND tenant_id=$2`, operationID, tenantID); err != nil {
		t.Fatalf("激活 Definition Edit purge 锁序夹具失败: %v", err)
	}
	if _, err := st.pool.Exec(ctx, `
		UPDATE schedules
		   SET definition_edit_operation_id=$2, definition_edit_fence=1
		 WHERE tenant_id=$1 AND id=$3`, tenantID, operationID, taskID); err != nil {
		t.Fatalf("安装 Definition Edit purge marker 失败: %v", err)
	}

	editTx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = editTx.Rollback(ctx) }()
	if _, err := editTx.Exec(ctx, `SET LOCAL lock_timeout = '750ms'`); err != nil {
		t.Fatal(err)
	}
	if err := editTx.QueryRow(ctx, `
		SELECT id FROM schedules
		 WHERE tenant_id=$1 AND id=$2
		 FOR UPDATE`, tenantID, taskID).Scan(&taskID); err != nil {
		t.Fatalf("editor 锁 schedule 失败: %v", err)
	}

	type purgeResult struct {
		report *PurgeReport
		err    error
	}
	purgeDone := make(chan purgeResult, 1)
	go func() {
		report, purgeErr := st.PurgeTenant(ctx, tenantID, false)
		purgeDone <- purgeResult{report: report, err: purgeErr}
	}()
	waitForTenantPurgeDefinitionEditLock(t, st)

	var lockedOperationID string
	lockErr := editTx.QueryRow(ctx, `
		SELECT id FROM task_definition_edit_operations
		 WHERE tenant_id=$1 AND id=$2
		 FOR UPDATE`, tenantID, operationID).Scan(&lockedOperationID)
	if rollbackErr := editTx.Rollback(ctx); rollbackErr != nil {
		t.Fatalf("释放 editor 锁失败: %v", rollbackErr)
	}

	var result purgeResult
	select {
	case result = <-purgeDone:
	case <-time.After(10 * time.Second):
		t.Fatal("tenant purge 未在 editor 释放锁后收敛")
	}
	if lockErr != nil {
		t.Fatalf("purge 反向持有 operation，editor schedule→operation 锁失败: %v", lockErr)
	}
	if lockedOperationID != operationID {
		t.Fatalf("editor 锁到 operation=%q，want %q", lockedOperationID, operationID)
	}
	if result.err != nil {
		t.Fatalf("tenant purge 与 Definition Edit 并发失败: %v", result.err)
	}
	if result.report == nil || result.report.Rows["tenants"] != 1 {
		t.Fatalf("tenant purge 报告异常: %+v", result.report)
	}
}

func TestPurgeTenant_DefinitionEditReceiptWorkerDoesNotDeadlock(t *testing.T) {
	st := purgeStore(t)
	tenantID := seedPurgeTenant(t, st)
	ctx := t.Context()

	var receiptID, userID, sessionID int64
	if err := st.pool.QueryRow(ctx, `
		UPDATE task_definition_edit_receipts
		   SET status='pending', next_attempt_at=clock_timestamp()-interval '1 second',
		       lease_owner='', lease_until=NULL, takeover_not_before=NULL,
		       session_recorded_at=NULL, session_messages_digest='',
		       provider_message_id='', sent_at=NULL
		 WHERE tenant_id=$1
		 RETURNING id, user_id, session_id`,
		tenantID,
	).Scan(&receiptID, &userID, &sessionID); err != nil {
		t.Fatalf("准备 Definition Edit receipt worker 夹具失败: %v", err)
	}
	receipt, err := st.AcquireTaskDefinitionEditReceipt(
		ctx, types.AcquireTaskDefinitionEditReceiptParams{
			ID: receiptID, TenantID: tenantID, UserID: userID,
			LeaseOwner:    "purge-definition-receipt-worker",
			LeaseDuration: 10 * time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("领取 Definition Edit receipt 失败: %v", err)
	}
	report := runPurgeAgainstPausedReceiptWorker(
		t, st, tenantID,
		"task_definition_edit_receipts",
		"%tenant purge definition-edit receipt lock order%",
		func(workerStore *Store) error {
			return workerStore.RecordTaskDefinitionEditReceiptSessionMessages(
				ctx, receipt.Lease(),
				json.RawMessage(`[{"role":"user","content":"definition receipt"}]`),
			)
		},
	)
	if report.Rows["task_definition_edit_receipts"] != 1 ||
		report.Rows["agent_sessions"] == 0 {
		t.Fatalf("Definition Edit receipt purge report=%+v", report)
	}
	assertPurgedReceiptWorkerRows(t, st, tenantID)
}

func TestPurgeTenant_TaskCreationReceiptWorkerDoesNotDeadlock(t *testing.T) {
	st := purgeStore(t)
	tenantID := seedPurgeTenant(t, st)
	ctx := t.Context()

	var userID, sessionID int64
	if err := st.pool.QueryRow(ctx, `
		SELECT user_id, id
		  FROM agent_sessions
		 WHERE tenant_id=$1
		 ORDER BY id LIMIT 1`,
		tenantID,
	).Scan(&userID, &sessionID); err != nil {
		t.Fatalf("查 Task Creation receipt worker scope 失败: %v", err)
	}
	fixture := &compiledTaskFixture{
		st: st, tenantID: tenantID, userID: userID,
	}
	operationID := uuid.NewString()
	create := taskCreationCreateParams(fixture, operationID)
	create.SessionID = &sessionID
	if _, err := st.CreateTaskCreationOperation(ctx, create); err != nil {
		t.Fatalf("建 Task Creation operation 失败: %v", err)
	}
	if _, err := st.CancelTaskCreationOperation(
		ctx,
		taskCreationCancelParams(
			operationID, tenantID, userID, "om_purge_creation_receipt",
		),
	); err != nil {
		t.Fatalf("终态化 Task Creation operation 失败: %v", err)
	}
	receipt, err := st.LoadTaskCreationReceiptByOperation(
		ctx, operationID, tenantID, userID,
	)
	if err != nil {
		t.Fatalf("读取 Task Creation receipt 失败: %v", err)
	}
	if _, err := st.pool.Exec(ctx, `
		UPDATE task_creation_receipts
		   SET next_attempt_at=clock_timestamp()-interval '1 second'
		 WHERE id=$1`,
		receipt.ID,
	); err != nil {
		t.Fatalf("设置 Task Creation receipt 到期失败: %v", err)
	}
	receipt, err = st.AcquireTaskCreationReceipt(
		ctx, types.AcquireTaskCreationReceiptParams{
			ID: receipt.ID, TenantID: tenantID, UserID: userID,
			LeaseOwner:    "purge-creation-receipt-worker",
			LeaseDuration: 10 * time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("领取 Task Creation receipt 失败: %v", err)
	}
	report := runPurgeAgainstPausedReceiptWorker(
		t, st, tenantID,
		"task_creation_receipts",
		"%tenant purge task-creation receipt lock order%",
		func(workerStore *Store) error {
			return workerStore.RecordTaskCreationReceiptSessionMessages(
				ctx, receipt.Lease(),
				json.RawMessage(`[{"role":"user","content":"creation receipt"}]`),
			)
		},
	)
	if report.Rows["task_creation_receipts"] != 1 ||
		report.Rows["pending_actions"] != 1 ||
		report.Rows["agent_sessions"] == 0 {
		t.Fatalf("Task Creation receipt purge report=%+v", report)
	}
	assertPurgedReceiptWorkerRows(t, st, tenantID)
}

func TestPurgeTenant_TaskCreationCoordinatorOperationFirstDoesNotDeadlock(t *testing.T) {
	st := purgeStore(t)
	fixture := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, fixture)
	ctx := t.Context()
	commit := preparedA5Commit(t, st, fixture, "purge-operation-first")

	blocker, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(context.WithoutCancel(ctx)) }()
	var membershipUserID int64
	if err := blocker.QueryRow(ctx, `
		SELECT user_id FROM memberships
		 WHERE tenant_id=$1 AND user_id=$2
		 FOR UPDATE`,
		fixture.tenantID, fixture.userID,
	).Scan(&membershipUserID); err != nil {
		t.Fatalf("锁 Task Creation membership blocker 失败: %v", err)
	}

	coordinatorDone := make(chan error, 1)
	go func() {
		coordinatorDone <- st.CommitPausedCompiledTaskDefinitionForCreation(
			ctx, commit,
		)
	}()
	waitForDatabaseLockQuery(
		t, st,
		"%task creation membership lock order%",
		"Task Creation coordinator 未在持有 operation 后等待 membership",
	)

	var lockedOperationID string
	err = st.pool.QueryRow(ctx, `
		SELECT id FROM pending_actions
		 WHERE id=$1 AND tenant_id=$2
		 FOR UPDATE NOWAIT`,
		commit.Lease.ID, fixture.tenantID,
	).Scan(&lockedOperationID)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("coordinator 未真实先持有 operation UPDATE 锁: id=%q err=%v",
			lockedOperationID, err)
	}

	type purgeResult struct {
		report *PurgeReport
		err    error
	}
	purgeDone := make(chan purgeResult, 1)
	go func() {
		report, purgeErr := st.PurgeTenant(ctx, fixture.tenantID, false)
		purgeDone <- purgeResult{report: report, err: purgeErr}
	}()
	waitForDatabaseLockQuery(
		t, st,
		"%tenant purge task-creation lock order%",
		"tenant purge 未先等待 coordinator 的 operation 根锁",
	)

	if err := blocker.Rollback(ctx); err != nil {
		t.Fatalf("释放 Task Creation membership blocker 失败: %v", err)
	}
	select {
	case err := <-coordinatorDone:
		if err != nil {
			t.Fatalf("Task Creation coordinator 与 purge 并发失败: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Task Creation coordinator 未在 membership blocker 释放后收敛")
	}
	select {
	case result := <-purgeDone:
		if result.err != nil {
			t.Fatalf("tenant purge 与 Task Creation coordinator 并发失败: %v",
				result.err)
		}
		if result.report == nil || result.report.Rows["tenants"] != 1 ||
			result.report.Rows["pending_actions"] != 1 {
			t.Fatalf("tenant purge 报告异常: %+v", result.report)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("tenant purge 未在 coordinator 释放根锁后收敛")
	}
	assertPurgedReceiptWorkerRows(t, st, fixture.tenantID)
}

func TestPurgeTenant_DefinitionEditScheduleThenTenantDoesNotDeadlock(t *testing.T) {
	fixture := newTaskDefinitionEditEntrypointFixture(t, true)
	st := fixture.store
	ctx := t.Context()
	frozen := fixture.buildProposal(
		t, fixture.databaseNow(t).Add(time.Hour), "purge-definition-edit-lock-order",
	)

	blocker, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(context.WithoutCancel(ctx)) }()
	var tenantID int64
	if err := blocker.QueryRow(ctx, `
		SELECT id FROM tenants
		 WHERE id=$1
		 FOR UPDATE`,
		fixture.base.TenantID,
	).Scan(&tenantID); err != nil {
		t.Fatalf("锁 Definition Edit tenant blocker 失败: %v", err)
	}

	editDone := make(chan error, 1)
	go func() {
		_, createErr := st.CreateTaskDefinitionEditOperation(
			ctx, taskDefinitionEditCreateParams(frozen),
		)
		editDone <- createErr
	}()
	waitForDatabaseLockQuery(
		t, st,
		"%task definition edit tenant lock order%",
		"Definition Edit creation 未在持有 schedule 后等待 tenant",
	)

	var lockedTaskID string
	err = st.pool.QueryRow(ctx, `
		SELECT id FROM schedules
		 WHERE tenant_id=$1 AND id=$2
		 FOR UPDATE NOWAIT`,
		fixture.base.TenantID, fixture.base.TaskID,
	).Scan(&lockedTaskID)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("Definition Edit creation 未真实持有 schedule UPDATE 锁: id=%q err=%v",
			lockedTaskID, err)
	}

	type purgeResult struct {
		report *PurgeReport
		err    error
	}
	purgeDone := make(chan purgeResult, 1)
	go func() {
		report, purgeErr := st.PurgeTenant(ctx, fixture.base.TenantID, false)
		purgeDone <- purgeResult{report: report, err: purgeErr}
	}()
	waitForDatabaseLockQuery(
		t, st,
		"%tenant purge task-creation lock order%",
		"tenant purge 未等待 Definition Edit 已持有的 provenance 根锁",
	)

	if err := blocker.Rollback(ctx); err != nil {
		t.Fatalf("释放 Definition Edit tenant blocker 失败: %v", err)
	}
	select {
	case err := <-editDone:
		if err != nil {
			t.Fatalf("Definition Edit creation 与 purge 并发失败: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Definition Edit creation 未在 tenant blocker 释放后收敛")
	}
	select {
	case result := <-purgeDone:
		if result.err != nil {
			t.Fatalf("tenant purge 与 Definition Edit creation 并发失败: %v",
				result.err)
		}
		if result.report == nil || result.report.Rows["tenants"] != 1 ||
			result.report.Rows["task_definition_edit_operations"] != 1 {
			t.Fatalf("tenant purge 报告异常: %+v", result.report)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("tenant purge 未在 Definition Edit creation 释放锁后收敛")
	}
	assertPurgedReceiptWorkerRows(t, st, fixture.base.TenantID)
}

func TestPurgeTenantRemovesAgentTurnCommittedBeforeSessionFence(t *testing.T) {
	st := purgeStore(t)
	tenantID := seedPurgeTenant(t, st)
	ctx := t.Context()

	var scope agentledger.Scope
	var taskID string
	var base agentledger.SessionProjection
	if err := st.pool.QueryRow(ctx, `
		SELECT m.tenant_id, m.user_id,
		       (SELECT id FROM schedules WHERE tenant_id=m.tenant_id LIMIT 1)
		  FROM memberships m
		 WHERE m.tenant_id=$1
		 ORDER BY m.user_id
		 LIMIT 1`,
		tenantID,
	).Scan(
		&scope.TenantID, &scope.UserID, &taskID,
	); err != nil {
		t.Fatalf("查 purge/Commit 并发夹具失败: %v", err)
	}
	if err := st.pool.QueryRow(ctx, `
		INSERT INTO agent_sessions (tenant_id, user_id)
		VALUES ($1, $2)
		RETURNING id, messages, turn_count, activated_tools`,
		scope.TenantID, scope.UserID,
	).Scan(
		&scope.SessionID, &base.Messages, &base.TurnCount, &base.ActivatedTools,
	); err != nil {
		t.Fatalf("建 purge/Commit 干净 session 夹具失败: %v", err)
	}
	desired := agentledger.SessionProjection{
		Messages: json.RawMessage(
			`[{"role":"user","content":"concurrent purge"}]`,
		),
		TurnCount:      1,
		ActivatedTools: json.RawMessage(`[]`),
	}
	batch := projectionSnapshotBatch(
		t, scope, "turn-concurrent-purge", base, desired, "",
	)

	blocker, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(context.WithoutCancel(ctx)) }()
	var lockedTenantID int64
	if err := blocker.QueryRow(ctx, `
		SELECT id FROM tenants
		 WHERE id=$1
		 FOR UPDATE`,
		tenantID,
	).Scan(&lockedTenantID); err != nil {
		t.Fatalf("锁 Agent turn tenant FK blocker 失败: %v", err)
	}

	commitDone := make(chan error, 1)
	go func() {
		_, commitErr := st.CommitAgentSessionTurn(ctx, desired, batch)
		commitDone <- commitErr
	}()
	waitForDatabaseLockQuery(
		t, st,
		"%agent event tenant FK lock order%",
		"Agent turn 未在持有 session 后等待 event tenant FK",
	)

	var lockedSessionID int64
	err = st.pool.QueryRow(ctx, `
		SELECT id FROM agent_sessions
		 WHERE id=$1 AND tenant_id=$2
		 FOR UPDATE NOWAIT`,
		scope.SessionID, tenantID,
	).Scan(&lockedSessionID)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("Agent turn 未真实持有 session UPDATE 锁: id=%d err=%v",
			lockedSessionID, err)
	}

	type purgeResult struct {
		report *PurgeReport
		err    error
	}
	purgeDone := make(chan purgeResult, 1)
	go func() {
		report, purgeErr := st.PurgeTenant(ctx, tenantID, false)
		purgeDone <- purgeResult{report: report, err: purgeErr}
	}()
	waitForDatabaseLockQuery(
		t, st,
		"%tenant purge agent-session lock order%",
		"tenant purge 未在 tenant delete 前等待 Agent turn session 锁",
	)

	if err := blocker.Rollback(ctx); err != nil {
		t.Fatalf("释放 Agent turn tenant FK blocker 失败: %v", err)
	}
	select {
	case commitErr := <-commitDone:
		if commitErr != nil {
			t.Fatalf("Agent turn 与 purge 并发失败: %v", commitErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Agent turn 未在 tenant FK blocker 释放后收敛")
	}

	select {
	case result := <-purgeDone:
		if result.err != nil {
			t.Fatalf("tenant purge 失败: %v", result.err)
		}
		if result.report == nil || result.report.Rows["tenants"] != 1 {
			t.Fatalf("tenant purge 报告异常: %+v", result.report)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("tenant purge 未在 blocker 释放后收敛")
	}

	var events, sessions, tenants int
	if err := st.pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM agent_events WHERE tenant_id=$1),
			(SELECT count(*) FROM agent_sessions WHERE tenant_id=$1),
			(SELECT count(*) FROM tenants WHERE id=$1)`,
		tenantID,
	).Scan(&events, &sessions, &tenants); err != nil {
		t.Fatal(err)
	}
	if events != 0 || sessions != 0 || tenants != 0 {
		t.Fatalf("purge residue events=%d sessions=%d tenants=%d",
			events, sessions, tenants)
	}
}

func TestPurgeTenantRemovesSideWriterCommittedBeforeSessionFence(
	t *testing.T,
) {
	st := purgeStore(t)
	tenantID := seedPurgeTenant(t, st)
	ctx := t.Context()

	var userID, sessionID int64
	if err := st.pool.QueryRow(ctx, `
		SELECT user_id
		  FROM memberships
		 WHERE tenant_id=$1
		 ORDER BY user_id
		 LIMIT 1`,
		tenantID,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx, `
		INSERT INTO agent_sessions (tenant_id,user_id)
		VALUES ($1,$2) RETURNING id`,
		tenantID, userID,
	).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}

	blocker, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(context.WithoutCancel(ctx)) }()
	var lockedTenantID int64
	if err := blocker.QueryRow(ctx, `
		SELECT id FROM tenants
		 WHERE id=$1
		 FOR UPDATE`,
		tenantID,
	).Scan(&lockedTenantID); err != nil {
		t.Fatal(err)
	}

	commitDone := make(chan error, 1)
	go func() {
		_, commitErr := st.CommitAgentSessionAppend(
			ctx,
			userID,
			sessionID,
			"feedback-click:purge-lock-order",
			json.RawMessage(
				`[{"role":"user","content":"concurrent side writer"}]`,
			),
		)
		commitDone <- commitErr
	}()
	waitForDatabaseLockQuery(
		t, st,
		"%agent event tenant FK lock order%",
		"side writer 未在持有 session 后等待 event tenant FK",
	)

	var lockedSessionID int64
	err = st.pool.QueryRow(ctx, `
		SELECT id FROM agent_sessions
		 WHERE id=$1 AND tenant_id=$2
		 FOR UPDATE NOWAIT`,
		sessionID, tenantID,
	).Scan(&lockedSessionID)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("side writer 未真实持有 session UPDATE 锁: id=%d err=%v",
			lockedSessionID, err)
	}

	type purgeResult struct {
		report *PurgeReport
		err    error
	}
	purgeDone := make(chan purgeResult, 1)
	go func() {
		report, purgeErr := st.PurgeTenant(ctx, tenantID, false)
		purgeDone <- purgeResult{report: report, err: purgeErr}
	}()
	waitForDatabaseLockQuery(
		t, st,
		"%tenant purge agent-session lock order%",
		"tenant purge 未在 tenant delete 前等待 side-writer session 锁",
	)

	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case commitErr := <-commitDone:
		if commitErr != nil {
			t.Fatalf("side writer 与 purge 并发失败: %v", commitErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("side writer 未在 tenant FK blocker 释放后收敛")
	}
	select {
	case result := <-purgeDone:
		if result.err != nil {
			t.Fatalf("tenant purge 失败: %v", result.err)
		}
		if result.report == nil || result.report.Rows["tenants"] != 1 {
			t.Fatalf("tenant purge 报告异常: %+v", result.report)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("tenant purge 未在 side writer 释放锁后收敛")
	}
	var events, sessions, tenants int
	if err := st.pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM agent_events WHERE tenant_id=$1),
			(SELECT count(*) FROM agent_sessions WHERE tenant_id=$1),
			(SELECT count(*) FROM tenants WHERE id=$1)`,
		tenantID,
	).Scan(&events, &sessions, &tenants); err != nil {
		t.Fatal(err)
	}
	if events != 0 || sessions != 0 || tenants != 0 {
		t.Fatalf("purge residue events=%d sessions=%d tenants=%d",
			events, sessions, tenants)
	}
}

type pauseAfterReceiptLockTx struct {
	pgx.Tx
	table   string
	locked  chan struct{}
	resume  <-chan struct{}
	lockOne sync.Once
}

func (tx *pauseAfterReceiptLockTx) QueryRow(
	ctx context.Context,
	sql string,
	args ...any,
) pgx.Row {
	row := tx.Tx.QueryRow(ctx, sql, args...)
	if !strings.Contains(sql, "FROM "+tx.table+" r") ||
		!strings.Contains(sql, "FOR UPDATE OF r") {
		return row
	}
	return pauseAfterReceiptLockRow{
		Row: row,
		pause: func() {
			tx.lockOne.Do(func() {
				close(tx.locked)
				<-tx.resume
			})
		},
	}
}

type pauseAfterReceiptLockRow struct {
	pgx.Row
	pause func()
}

func (row pauseAfterReceiptLockRow) Scan(dest ...any) error {
	if err := row.Row.Scan(dest...); err != nil {
		return err
	}
	row.pause()
	return nil
}

func runPurgeAgainstPausedReceiptWorker(
	t *testing.T,
	st *Store,
	tenantID int64,
	receiptTable string,
	purgeWaitPattern string,
	worker func(*Store) error,
) *PurgeReport {
	t.Helper()
	receiptLocked := make(chan struct{})
	resumeWorker := make(chan struct{})
	var resumeOnce sync.Once
	resume := func() {
		resumeOnce.Do(func() {
			close(resumeWorker)
		})
	}
	t.Cleanup(resume)
	workerStore := *st
	workerStore.beginTx = func(
		ctx context.Context,
		options pgx.TxOptions,
	) (pgx.Tx, error) {
		tx, err := st.pool.BeginTx(ctx, options)
		if err != nil {
			return nil, err
		}
		return &pauseAfterReceiptLockTx{
			Tx: tx, table: receiptTable,
			locked: receiptLocked, resume: resumeWorker,
		}, nil
	}

	workerDone := make(chan error, 1)
	go func() {
		workerDone <- worker(&workerStore)
	}()
	select {
	case <-receiptLocked:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s worker 未持有真实 receipt FOR UPDATE 锁", receiptTable)
	}

	type purgeResult struct {
		report *PurgeReport
		err    error
	}
	purgeDone := make(chan purgeResult, 1)
	go func() {
		report, err := st.PurgeTenant(t.Context(), tenantID, false)
		purgeDone <- purgeResult{report: report, err: err}
	}()
	waitForDatabaseLockQuery(
		t, st, purgeWaitPattern,
		"tenant purge 未在 "+receiptTable+" 等待 worker receipt 锁",
	)
	resume()

	select {
	case err := <-workerDone:
		if err != nil {
			t.Fatalf("%s worker 与 purge 并发失败: %v", receiptTable, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("%s worker 未在 purge 持有 session 前收敛", receiptTable)
	}
	select {
	case result := <-purgeDone:
		if result.err != nil {
			t.Fatalf("tenant purge 与 %s worker 并发失败: %v",
				receiptTable, result.err)
		}
		if result.report == nil || result.report.Rows["tenants"] != 1 {
			t.Fatalf("tenant purge 报告异常: %+v", result.report)
		}
		return result.report
	case <-time.After(10 * time.Second):
		t.Fatalf("tenant purge 未在 %s worker 释放锁后收敛", receiptTable)
		return nil
	}
}

func assertPurgedReceiptWorkerRows(
	t *testing.T,
	st *Store,
	tenantID int64,
) {
	t.Helper()
	var (
		editReceipts, editOperations, creationReceipts int
		creationOperations, schedules, sessions        int
		events, tenants                                int
	)
	if err := st.pool.QueryRow(t.Context(), `
		SELECT
			(SELECT count(*) FROM task_definition_edit_receipts WHERE tenant_id=$1),
			(SELECT count(*) FROM task_definition_edit_operations WHERE tenant_id=$1),
			(SELECT count(*) FROM task_creation_receipts WHERE tenant_id=$1),
			(SELECT count(*) FROM pending_actions WHERE tenant_id=$1),
			(SELECT count(*) FROM schedules WHERE tenant_id=$1),
			(SELECT count(*) FROM agent_sessions WHERE tenant_id=$1),
			(SELECT count(*) FROM agent_events WHERE tenant_id=$1),
			(SELECT count(*) FROM tenants WHERE id=$1)`,
		tenantID,
	).Scan(
		&editReceipts, &editOperations, &creationReceipts,
		&creationOperations, &schedules, &sessions, &events, &tenants,
	); err != nil {
		t.Fatal(err)
	}
	if editReceipts != 0 || editOperations != 0 ||
		creationReceipts != 0 || creationOperations != 0 ||
		schedules != 0 || sessions != 0 || events != 0 || tenants != 0 {
		t.Fatalf(
			"purge residue edit_receipts=%d edit_operations=%d "+
				"creation_receipts=%d creation_operations=%d schedules=%d "+
				"sessions=%d events=%d tenants=%d",
			editReceipts, editOperations, creationReceipts, creationOperations,
			schedules, sessions, events, tenants,
		)
	}
}

func waitForTenantPurgeDefinitionEditLock(t *testing.T, st *Store) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		if err := st.pool.QueryRow(t.Context(), `
			SELECT EXISTS (
				SELECT 1
				  FROM pg_stat_activity
				 WHERE datname=current_database()
				   AND pid<>pg_backend_pid()
				   AND wait_event_type='Lock'
				   AND query LIKE '%tenant purge definition-edit lock order%'
			)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("tenant purge 未按 schedule-first 锁序等待")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForDatabaseLockQuery(
	t *testing.T,
	st *Store,
	queryPattern string,
	timeoutMessage string,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		if err := st.pool.QueryRow(t.Context(), `
			SELECT EXISTS (
				SELECT 1
				  FROM pg_stat_activity
				 WHERE datname=current_database()
				   AND pid<>pg_backend_pid()
				   AND wait_event_type='Lock'
				   AND query LIKE $1
			)`,
			queryPattern,
		).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(timeoutMessage)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestPurgeTenant_LeavesSharedFactsIntact 是红线 I-A3 的**行为级**验证。
//
// 前面两条守卫比对的是清单；这一条真的跑一遍清理，然后确认共享内容一条没少。
// 清单对而实现错（比如某条 where 写宽了）这类问题，只有它能抓到。
func TestPurgeTenant_LeavesSharedFactsIntact(t *testing.T) {
	st := purgeStore(t)
	tenantID := seedPurgeTenant(t, st)
	ctx := t.Context()

	srcID := seedSource(t, st)
	item := types.ContentItem{
		SourceID: srcID, ExternalID: "purge-" + uuid.NewString(),
		URL: "https://example.com/shared-" + uuid.NewString(), Title: "跨租户共享内容",
		Content: "这条内容是客观事实，不属于任何租户", Kind: types.KindArticle,
		CanonicalKey: "purge-shared-" + uuid.NewString(), FetchedAt: time.Now().UTC(),
	}
	itemID, _, err := st.UpsertContentItem(ctx, &item)
	if err != nil {
		t.Fatalf("建内容失败: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := cleanupContext()
		defer cancel()
		cleanupExec(c, t, st, `DELETE FROM content_sources WHERE content_item_id = $1`, itemID)
		cleanupExec(c, t, st, `DELETE FROM content_items WHERE id = $1`, itemID)
	})

	if _, err := st.PurgeTenant(ctx, tenantID, false); err != nil {
		t.Fatalf("清理失败: %v", err)
	}

	var n int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM content_items WHERE id = $1`, itemID).Scan(&n); err != nil {
		t.Fatalf("查内容失败: %v", err)
	}
	if n != 1 {
		t.Errorf("**红线 I-A3 被破坏**：清理一个租户删掉了跨租户的客观事实内容（content_items）。"+
			"同一篇内容可能被多个租户的信源指向，删了别人也看不到了——不可逆。实得 %d 行", n)
	}
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM sources WHERE id = $1`, srcID).Scan(&n); err != nil {
		t.Fatalf("查信源失败: %v", err)
	}
	if n != 1 {
		t.Errorf("**红线 I-A3 被破坏**：清理删掉了共享信源行（sources），实得 %d 行", n)
	}
}

// TestListPurgeableTenants_OnlyExpired：只有真正过了保留期的才可清理。
// 判据错了会把还在保留期内、随时可能被恢复的租户直接清掉——D9 承诺的"可恢复"就成了空话。
func TestListPurgeableTenants_OnlyExpired(t *testing.T) {
	st := purgeStore(t)
	tenantID := seedPurgeTenant(t, st)
	ctx := t.Context()

	// 刚注销：保留期内，不该出现在待清理列表。
	if _, err := st.SoftDeleteTenant(ctx, tenantID); err != nil {
		t.Fatalf("注销失败: %v", err)
	}
	ids, err := st.ListPurgeableTenants(ctx)
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if contains(ids, tenantID) {
		t.Error("保留期内的租户不得进入待清理列表——那会让 D9 承诺的「保留期内可恢复」变成空话")
	}

	// 把保留期拨到过去，模拟到期。
	if _, err := st.pool.Exec(ctx,
		`UPDATE tenants SET purge_after = now() - interval '1 day' WHERE id = $1`, tenantID); err != nil {
		t.Fatalf("构造到期状态失败: %v", err)
	}
	ids, err = st.ListPurgeableTenants(ctx)
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if !contains(ids, tenantID) {
		t.Error("已过保留期的租户应进入待清理列表")
	}
}

func contains(ids []int64, want int64) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}
