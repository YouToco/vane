package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// RLS 策略的正确性证明（migration 021）。
//
// 为什么必须有这组用例：RLS 是**沉默的**防线——策略写错、写漏、或因某个细节
// 未生效时，系统不会报错，只会安静地按「没有隔离」继续跑。审查里反复出现的
// 教训是「策略写了却不生效」（表 owner 绕过、缺 WITH CHECK、restrictive 无
// permissive 配套…）。所以这里不测「策略存在」，只测**行为**：
// 在受限角色下，能不能读到别人的行、能不能写出别人的行。

// asTenant 在受限角色 + 指定租户上下文里跑一段查询。
//
// 用事务 + SET LOCAL 而非连接级设置：SET LOCAL 随事务结束自动还原，
// 不会污染连接池里的连接（pgx 作者对 AfterConnect 的裁定同理——那会永久改变连接）。
// tenantID <= 0 表示**不设**租户上下文，用于验证 fail-closed。
func asTenant(t *testing.T, st *Store, tenantID int64, fn func(tx pgx.Tx)) {
	t.Helper()
	ctx := t.Context()
	tx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("开启事务失败: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if tenantID > 0 {
		if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id', $1, true)`,
			fmt.Sprint(tenantID)); err != nil {
			t.Fatalf("设置租户上下文失败: %v", err)
		}
	}
	// SET LOCAL ROLE 让本事务受 RLS 约束：生产连接用的是表 owner，而 owner
	// 默认绕过 RLS——不切角色就等于什么都没测。
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatalf("切换受限角色失败: %v", err)
	}
	fn(tx)
}

// seedTwoTenants 造两个租户，各带一个用户与一条订阅，返回两租户 id。
func seedTwoTenants(t *testing.T, st *Store) (int64, int64, int64, int64) {
	t.Helper()
	ctx := t.Context()
	mk := func(tag string) (tenantID, userID int64) {
		var tid int64
		if err := st.pool.QueryRow(ctx,
			`INSERT INTO tenants DEFAULT VALUES RETURNING id`).Scan(&tid); err != nil {
			t.Fatalf("建租户失败: %v", err)
		}
		u, err := st.UpsertUserByOpenID(ctx,
			fmt.Sprintf("ou_rls_%s_%d", tag, time.Now().UnixNano()), "rls-"+tag)
		if err != nil {
			t.Fatalf("建用户失败: %v", err)
		}
		if _, err := st.pool.Exec(ctx,
			`INSERT INTO memberships (tenant_id, user_id, role) VALUES ($1, $2, 'owner')`,
			tid, u.ID); err != nil {
			t.Fatalf("挂租户失败: %v", err)
		}
		if err := st.AddSubscription(ctx, u.ID, seedSource(t, st)); err != nil {
			t.Fatalf("加订阅失败: %v", err)
		}
		t.Cleanup(func() {
			c := context.WithoutCancel(ctx)
			_, _ = st.pool.Exec(c, `DELETE FROM subscriptions WHERE tenant_id = $1`, tid)
			_, _ = st.pool.Exec(c, `DELETE FROM users WHERE id = $1`, u.ID)
			_, _ = st.pool.Exec(c, `DELETE FROM tenants WHERE id = $1`, tid)
		})
		return tid, u.ID
	}
	tA, uA := mk("a")
	tB, uB := mk("b")
	return tA, uA, tB, uB
}

// TestRLS_ReadIsolation 是本组最核心的用例：**在受限角色下，一个租户读不到另一个的行**。
//
// 刻意用不带任何 WHERE 的 `SELECT *` ——这正是「Go 代码忘了加租户过滤」的场景。
// 若 RLS 生效，数据库自己把别人的行挡在外面；若没生效，这条查询会把两个租户的
// 订阅全捞回来，用例立刻变红。
func TestRLS_ReadIsolation(t *testing.T) {
	st := tenantTestStore(t)
	tA, _, tB, _ := seedTwoTenants(t, st)

	count := func(tenantID int64) int {
		var n int
		asTenant(t, st, tenantID, func(tx pgx.Tx) {
			// 无 WHERE：模拟应用层漏了谓词。
			if err := tx.QueryRow(t.Context(), `SELECT count(*) FROM subscriptions`).Scan(&n); err != nil {
				t.Fatalf("查询失败: %v", err)
			}
		})
		return n
	}
	var leakedA, leakedB int
	asTenant(t, st, tA, func(tx pgx.Tx) {
		_ = tx.QueryRow(t.Context(),
			`SELECT count(*) FROM subscriptions WHERE tenant_id = $1`, tB).Scan(&leakedA)
	})
	asTenant(t, st, tB, func(tx pgx.Tx) {
		_ = tx.QueryRow(t.Context(),
			`SELECT count(*) FROM subscriptions WHERE tenant_id = $1`, tA).Scan(&leakedB)
	})

	if leakedA != 0 {
		t.Errorf("租户 A 读到了 B 的 %d 行 —— RLS 未生效", leakedA)
	}
	if leakedB != 0 {
		t.Errorf("租户 B 读到了 A 的 %d 行 —— RLS 未生效", leakedB)
	}
	// 各自应恰好看到自己的 1 行（既不能多，也不能因策略过严而看不到自己的）。
	if got := count(tA); got != 1 {
		t.Errorf("租户 A 无谓词查询应只见自己的 1 行，实得 %d", got)
	}
	if got := count(tB); got != 1 {
		t.Errorf("租户 B 无谓词查询应只见自己的 1 行，实得 %d", got)
	}
}

// TestRLS_FailClosedWithoutContext：**没设租户上下文 = 什么都看不到**。
//
// 这条守的是 `current_setting(..., true)` 的 missing_ok 语义：未设置时返回 NULL，
// NULL 与任何值比较都非真，于是零行。若哪天有人把 missing_ok 去掉、或把谓词写成
// `COALESCE(..., tenant_id)` 之类「宽容」的形式，隔离会静默失效——本用例会红。
func TestRLS_FailClosedWithoutContext(t *testing.T) {
	st := tenantTestStore(t)
	seedTwoTenants(t, st)

	var n int
	asTenant(t, st, 0, func(tx pgx.Tx) { // 0 = 不设上下文
		if err := tx.QueryRow(t.Context(), `SELECT count(*) FROM subscriptions`).Scan(&n); err != nil {
			t.Fatalf("查询失败: %v", err)
		}
	})
	if n != 0 {
		t.Errorf("未设租户上下文时应零行可见（fail-closed），实得 %d 行", n)
	}
}

// TestRLS_WriteIsolation：**租户写不出标着别人 tenant_id 的行**。
//
// 这条守的是 WITH CHECK。只写 USING 时读被拦住了，写却没有——租户可以插入
// 一行 tenant_id 指向别人的数据，然后……自己也看不见它，但它真实存在于别人的
// 视图里。这是比读泄漏更隐蔽的一种污染。
func TestRLS_WriteIsolation(t *testing.T) {
	st := tenantTestStore(t)
	tA, uA, tB, _ := seedTwoTenants(t, st)
	srcID := seedSource(t, st)

	asTenant(t, st, tA, func(tx pgx.Tx) {
		// 冒充租户 B 写入。
		_, err := tx.Exec(t.Context(),
			`INSERT INTO subscriptions (tenant_id, user_id, source_id) VALUES ($1, $2, $3)`,
			tB, uA, srcID)
		if err == nil {
			t.Error("租户 A 不应能写出 tenant_id=B 的行 —— WITH CHECK 未生效")
		}
	})

	// 反向确认：写自己的租户是允许的（策略不能过严到把正常写入也挡了）。
	asTenant(t, st, tA, func(tx pgx.Tx) {
		if _, err := tx.Exec(t.Context(),
			`INSERT INTO subscriptions (tenant_id, user_id, source_id) VALUES ($1, $2, $3)`,
			tA, uA, srcID); err != nil {
			t.Errorf("租户 A 写自己的行不应被拦: %v", err)
		}
	})
}

// TestRLS_UpdateCannotMoveRowAcrossTenants：不能把自己的行「改嫁」给别的租户。
//
// UPDATE 的 WITH CHECK 管的是**改完之后**的值。缺了它，租户可以把自己的行
// tenant_id 改成别人的——数据凭空出现在别人账下，而原主再也看不到它。
func TestRLS_UpdateCannotMoveRowAcrossTenants(t *testing.T) {
	st := tenantTestStore(t)
	tA, _, tB, _ := seedTwoTenants(t, st)

	asTenant(t, st, tA, func(tx pgx.Tx) {
		_, err := tx.Exec(t.Context(),
			`UPDATE subscriptions SET tenant_id = $1 WHERE tenant_id = $2`, tB, tA)
		if err == nil {
			t.Error("不应能把自己的行改嫁给别的租户 —— UPDATE 的 WITH CHECK 未生效")
		}
	})
}

// TestRLS_SharedTablesNotRestricted：共享事实表**不受** RLS 约束。
//
// 这是不变量 I-T1 的运行时体现：sources / content_items 是跨租户共享的客观事实，
// 若误加了租户策略，每个租户只看得见自己抓来的内容——全局去重与 TikHub 付费闸门
// 一起失效。本用例保证没人「顺手」把它们也保护起来。
func TestRLS_SharedTablesNotRestricted(t *testing.T) {
	st := tenantTestStore(t)
	tA, _, _, _ := seedTwoTenants(t, st)
	seedSource(t, st) // 至少有一行

	for _, tbl := range []string{"sources", "content_items", "content_sources"} {
		var enabled bool
		if err := st.pool.QueryRow(t.Context(),
			`SELECT relrowsecurity FROM pg_class WHERE relname = $1`, tbl).Scan(&enabled); err != nil {
			t.Fatalf("查 %s 的 RLS 状态失败: %v", tbl, err)
		}
		if enabled {
			t.Errorf("%s 是跨租户共享的客观事实表，不应启用 RLS（见不变量 I-T1）", tbl)
		}
	}
	// 行为确认：受限角色在租户上下文里照样读得到共享表。
	var n int
	asTenant(t, st, tA, func(tx pgx.Tx) {
		if err := tx.QueryRow(t.Context(), `SELECT count(*) FROM sources`).Scan(&n); err != nil {
			t.Fatalf("读共享表失败: %v", err)
		}
	})
	if n == 0 {
		t.Error("受限角色应能读到共享信源表，实得 0 行 —— 共享语义被破坏")
	}
}

// TestRLS_PoliciesCoverAllTenantTables：所有租户所有表都必须有这两条策略。
// 漏掉一张表 = 那张表完全没有兜底防线，而这种遗漏在代码评审里极难看出来。
func TestRLS_PoliciesCoverAllTenantTables(t *testing.T) {
	st := tenantTestStore(t)
	want := []string{
		"subscriptions", "push_batches", "deliveries", "feedbacks",
		"profiles", "schedules", "agent_sessions", "pending_actions",
		"llm_calls", "tool_calls", "task_run_snapshots",
		"task_approved_definition_versions", "task_adaptive_states",
		"task_definition_edit_operations", "task_definition_edit_receipts",
		"agent_events",
	}
	for _, tbl := range want {
		var enabled bool
		if err := st.pool.QueryRow(t.Context(),
			`SELECT relrowsecurity FROM pg_class WHERE relname = $1`, tbl).Scan(&enabled); err != nil {
			t.Fatalf("查 %s 失败: %v", tbl, err)
		}
		if !enabled {
			t.Errorf("%s 未启用 RLS", tbl)
			continue
		}
		var restrictive int
		if err := st.pool.QueryRow(t.Context(),
			`SELECT count(*) FROM pg_policies
			  WHERE tablename = $1 AND policyname = 'tenant_isolation' AND permissive = 'RESTRICTIVE'`,
			tbl).Scan(&restrictive); err != nil {
			t.Fatalf("查 %s 策略失败: %v", tbl, err)
		}
		if restrictive != 1 {
			t.Errorf("%s 缺少 RESTRICTIVE 的 tenant_isolation 策略 —— "+
				"只有 PERMISSIVE 策略的话，后人加一条 USING(true) 就能把隔离整个抹掉", tbl)
		}
	}
}

// TestRLS_SystemLevelRowsWritableWithoutTenant 钉住可空租户列的语义。
//
// 这条是「想清楚怎么激活 RLS」时发现的缺陷：llm_calls / tool_calls 的 tenant_id
// 可空（系统级调用无归属租户），而策略若写成 `tenant_id = 当前租户`，NULL 恒不匹配
// ——**系统级记账的写入会被 WITH CHECK 拒绝**，一旦激活，后台任务的记账全线失败。
// 已在真库复现过。改用 IS NOT DISTINCT FROM 后 NULL 匹配 NULL，语义才对。
func TestRLS_SystemLevelRowsWritableWithoutTenant(t *testing.T) {
	st := tenantTestStore(t)
	ctx := t.Context()

	// 无租户上下文（系统路径）写一条无归属的记账行：必须成功。
	asTenant(t, st, 0, func(tx pgx.Tx) {
		if _, err := tx.Exec(ctx,
			`INSERT INTO llm_calls (span_name, provider, model) VALUES ('rls-sys-test', 'x', 'y')`); err != nil {
			t.Errorf("系统级记账（tenant_id 为 NULL）应可写入，实得: %v", err)
		}
	})
	t.Cleanup(func() {
		_, _ = st.pool.Exec(context.WithoutCancel(ctx),
			`DELETE FROM llm_calls WHERE span_name = 'rls-sys-test'`)
	})

	// 但租户上下文里**看不到**系统级行——它不属于任何租户。
	tA, _, _, _ := seedTwoTenants(t, st)
	var n int
	asTenant(t, st, tA, func(tx pgx.Tx) {
		if err := tx.QueryRow(ctx,
			`SELECT count(*) FROM llm_calls WHERE span_name = 'rls-sys-test'`).Scan(&n); err != nil {
			t.Fatalf("查询失败: %v", err)
		}
	})
	if n != 0 {
		t.Errorf("租户不应看到系统级记账行，实得 %d 行", n)
	}
}
