package store

import (
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

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
// sources / content_items / content_sources / page_snapshots 是跨租户客观事实：
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
	tx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("开启 Approved/Adaptive 夹具事务失败: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
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
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("提交 Approved/Adaptive 清理夹具失败: %v", err)
	}
	if _, err := st.pool.Exec(ctx,
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
		tn.ID, u.ID, "purge-task-"+uuid.NewString(), "purge-workflow-"+uuid.NewString(),
		"purge-run-"+uuid.NewString()); err != nil {
		t.Fatalf("建运行快照清理夹具失败: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := cleanupContext()
		defer cancel()
		cleanupExec(c, t, st, `DELETE FROM task_adaptive_states WHERE tenant_id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM task_approved_definition_versions WHERE tenant_id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM schedules WHERE tenant_id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM task_run_snapshots WHERE tenant_id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM profiles WHERE tenant_id = $1`, tn.ID)
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
