package store

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

const latestMigrationVersion int64 = 57

// wantTables 是全部迁移建出的业务表，迁移完成后必须全部存在。
// 与 TestMigrationsCoverWantTables 双向对账：加表必须同步补账，漏一张 CI 红。
var wantTables = []string{
	// 001 Step 5 schema 九张业务表
	"users",
	"sources",
	"subscriptions",
	"content_items",
	"push_batches",
	"deliveries",
	"feedbacks",
	"task_event_qualification_steps",
	"task_observed_events",
	"feedback_freshness_triage",
	"profiles",
	"llm_calls",
	// 002 M2 settings / 003 M3 schedules
	"settings",
	"schedules",
	// 005 M4 agent（欠账补记，a2a-contract §9.5）
	"agent_sessions",
	"pending_actions",
	// 007 内容身份关联表（欠账补记）
	"content_sources",
	// 013 A2A server 任务持久化
	"a2a_tasks",
	// 018 租户地基（企业级契约 §1.2）
	"tenants",
	"memberships",
	"invites",
	// 019 邮箱+密码身份与会话（决议 D2′）
	"user_sessions",
	// 015 agent 工具调用记账（TikHub 端点注册表契约 §6）
	"tool_calls",
	// 017 情报任务手册（Task Playbook P0）
	"schedule_playbooks",
	// 020 任务手册 P1b：「任务 ↔ 源」软范围绑定
	"schedule_sources",
	// 026 per-tenant 配额（企业级契约 §2.7，D3 下的财务护栏）
	"tenant_quota",
	// 027 probewatch 告警指纹落盘（M5 契约 §16 修订，探针实现债 P2）
	"probewatch_state",
	// 029 create_schedule v1 耐久用户回执 outbox（A6）
	"task_creation_receipts",
	// 030 每次定时任务运行的不可变执行快照（Agent Runtime C0）
	"task_run_snapshots",
	// 032 Approved Definition / Adaptive State 分离（Agent Runtime C2a）
	"task_approved_definition_versions",
	"task_adaptive_states",
	// 033 Approved Definition 耐久编辑 operation + 用户回执（C2b3-2a）
	"task_definition_edit_operations",
	"task_definition_edit_receipts",
	// 035 append-only Agent semantic event ledger (7.7-A).
	"agent_events",
	// 053 exact-session append-only Agent projection authority.
	"agent_session_projection_authority_events",
	// 054 durable Web schedule command intents and idempotency tombstones.
	"schedule_commands",
	// 055 immutable Agent context shadow candidates with seal-time watermarks.
	"agent_turn_context_snapshots",
	// 056 durable business-fact to exact Agent-session continuation.
	"agent_session_fact_outbox",
	// 036 C2c-2 immutable run-snapshot v2 shadow sidecar.
	"task_run_snapshot_v2_shadows",
	// 037 C2c-3b-1 durable per-run v2 authority fence.
	// 036 C2c-2 immutable run-snapshot v2 shadow sidecar.
	"task_run_snapshot_v2_cutover_events",
	// 038 adds only restricted cutover functions/role; no new table.
	// 039 durable external push effect/checkpoint substrate.
	"push_effects",
	// 050 physically pinned one-shot legacy batch 63 repair adjudication.
	"legacy_batch63_repair_events",
}

// droppedTables 是"曾被某迁移 CREATE、又被后续迁移 DROP"的表：它们出现在迁移的
// CREATE TABLE 集合里，但迁移跑完后不该存在，故不进 wantTables。
// 单独列出而非默默忽略，是为了让"建了表却没记账"的守卫（下方双向对账）依然对
// 真正的漏账生效——只豁免这里显式声明的、有意下线的表。
//   - page_snapshots：011 为 page_watch 建，016 随 page_watch 下线删除。
var droppedTables = map[string]bool{
	"page_snapshots": true,
}

// TestMigrationsCoverWantTables 是对账守卫（a2a-contract §9.5）：扫 migrations/*.sql 的
// CREATE TABLE 表名集合，与 wantTables 双向比对。守卫自 M4 起失守——005/007/011 三批
// 新表都没进 wantTables，TestMigrate 的存在性断言对它们形同虚设——本测试把"加表忘补账"
// 变成 CI 红灯。纯读 embed FS，无需数据库。
func TestMigrationsCoverWantTables(t *testing.T) {
	// 行首（含缩进）的 CREATE TABLE 才算数：SQL 注释行以 -- 开头，不会误匹配。
	re := regexp.MustCompile(`(?mi)^\s*CREATE TABLE\s+(?:IF NOT EXISTS\s+)?([A-Za-z_][A-Za-z0-9_]*)`)

	files, err := fs.Glob(migrationsFS, "migrations/*.sql")
	if err != nil {
		t.Fatalf("枚举迁移文件失败: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("embed FS 里没有任何迁移文件（go:embed 路径变了？）")
	}

	created := make(map[string]string) // 表名 → 建它的迁移文件
	for _, name := range files {
		data, err := fs.ReadFile(migrationsFS, name)
		if err != nil {
			t.Fatalf("读迁移文件 %s 失败: %v", name, err)
		}
		for _, m := range re.FindAllStringSubmatch(string(data), -1) {
			created[strings.ToLower(m[1])] = name
		}
	}

	want := make(map[string]bool, len(wantTables))
	for _, tbl := range wantTables {
		want[tbl] = true
	}
	for tbl, file := range created {
		if !want[tbl] && !droppedTables[tbl] {
			t.Errorf("%s 建了表 %s，但 wantTables 没记账", file, tbl)
		}
	}
	for tbl := range want {
		if _, ok := created[tbl]; !ok {
			t.Errorf("wantTables 记了表 %s，但没有任何迁移建它", tbl)
		}
	}
}

func TestObservationMigrationHasDurableDowngradeFence(t *testing.T) {
	raw, err := fs.ReadFile(migrationsFS, "migrations/040_observation_feedback.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(raw)
	for _, required := range []string{
		"LOCK TABLE task_observed_events, task_event_qualification_steps, feedbacks",
		"IN ACCESS EXCLUSIVE MODE",
		"refusing downgrade while durable observation state exists",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("migration 040 missing downgrade fence fragment %q", required)
		}
	}
}

// TestMigrate 是集成测试：依赖真实 Postgres（CI 的 test job 提供 service 与 DATABASE_URL）。
func TestMigrate(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过迁移集成测试")
	}
	ctx := t.Context()

	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 执行失败: %v", err)
	}

	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 建池失败: %v", err)
	}
	defer st.Close()

	versionAfterFirst := gooseVersion(t, st)
	if versionAfterFirst != latestMigrationVersion {
		t.Fatalf("迁移版本=%d，want %d", versionAfterFirst, latestMigrationVersion)
	}

	// 迁移必须幂等：重复执行不报错，且不重复应用（goose 版本号不变）。
	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 重复执行失败（应幂等）: %v", err)
	}
	if v := gooseVersion(t, st); v != versionAfterFirst {
		t.Errorf("重复迁移改变了 goose 版本号: %d -> %d（迁移被重复应用）", versionAfterFirst, v)
	}

	for _, table := range wantTables {
		var exists bool
		err := st.pool.QueryRow(ctx,
			`SELECT EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = 'public' AND table_name = $1
			)`, table).Scan(&exists)
		if err != nil {
			t.Fatalf("查询 information_schema.tables 失败（表 %s）: %v", table, err)
		}
		if !exists {
			t.Errorf("迁移后缺少表: %s", table)
		}
	}
}

// TestMigrateConcurrentFreshDB 回归 go test ./... 的迁移竞态：store/evolver/feishu
// 三个测试进程各自调用 Migrate，全新库上后到者在先到者的 001 事务提交前读到
// 空 goose_db_version，跟着应用 001 报 "relation already exists"。
//
// DATABASE_URL 指向的库通常已迁移过（窗口不存在），所以另建一次性全新库；
// 每个 Migrate 各自 sql.Open 独立建连，session 语义与跨进程并发等价。
// 窗口本身极窄（先到者执行 001 的几百毫秒），靠自然时序复现会 flaky，
// 这里用 admin 事务预建 users 表且不提交来占住表锁——模拟"先到者 001 执行中"：
// 全部并发 Migrate 越过版本表检查、读到版本 0、阻塞在 001 的 CREATE TABLE users
// 上，回滚放行后同时竞争建表，无串行化保护时除赢家外必然全部报错。
func TestMigrateConcurrentFreshDB(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过迁移集成测试")
	}
	ctx := t.Context()

	admin, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatalf("打开管理连接失败: %v", err)
	}

	freshName := fmt.Sprintf("vane_migrate_race_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+freshName); err != nil {
		admin.Close()
		t.Skipf("无 CREATE DATABASE 权限，跳过并发迁移竞态测试: %v", err)
	}
	// admin 的关闭放同一个 Cleanup（而非 defer）：defer 先于 Cleanup 执行，
	// 若在 defer 里关连接，DROP 时连接已不可用。
	t.Cleanup(func() {
		defer admin.Close()
		// WITH (FORCE) 兜底断开残留连接（PG 13+），避免临时库删不掉。
		if _, err := admin.ExecContext(context.Background(),
			"DROP DATABASE "+freshName+" WITH (FORCE)"); err != nil {
			t.Errorf("清理临时库 %s 失败: %v", freshName, err)
		}
	})

	u, err := url.Parse(dbURL)
	if err != nil {
		t.Fatalf("解析 DATABASE_URL 失败: %v", err)
	}
	u.Path = "/" + freshName
	freshURL := u.String()

	blocker, err := sql.Open("pgx", freshURL)
	if err != nil {
		t.Fatalf("打开临时库占锁连接失败: %v", err)
	}
	defer blocker.Close()
	blockTx, err := blocker.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("开启占锁事务失败: %v", err)
	}
	if _, err := blockTx.ExecContext(ctx, "CREATE TABLE users (id bigint)"); err != nil {
		t.Fatalf("占锁事务预建 users 失败: %v", err)
	}

	const n = 4
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = Migrate(ctx, freshURL)
		}()
	}
	// 并发 Migrate 同时首建版本表会冲突，goose 内部按 1s 间隔重试；等一轮
	// 重试完成、全部抵达 users 表锁后再回滚放行。
	time.Sleep(3 * time.Second)
	if err := blockTx.Rollback(); err != nil {
		t.Fatalf("回滚占锁事务失败: %v", err)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("并发 Migrate #%d 失败: %v", i, err)
		}
	}

	// 并发迁移完成后库必须处于与单次迁移相同的终态。
	fresh, err := sql.Open("pgx", freshURL)
	if err != nil {
		t.Fatalf("打开临时库连接失败: %v", err)
	}
	defer fresh.Close()
	var version int64
	if err := fresh.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version_id), 0) FROM goose_db_version`).Scan(&version); err != nil {
		t.Fatalf("查询 goose_db_version 失败: %v", err)
	}
	if version != latestMigrationVersion {
		t.Errorf("并发迁移后 goose_db_version=%d，want %d", version, latestMigrationVersion)
	}
	for _, table := range wantTables {
		var exists bool
		err := fresh.QueryRowContext(ctx,
			`SELECT EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = 'public' AND table_name = $1
			)`, table).Scan(&exists)
		if err != nil {
			t.Fatalf("查询 information_schema.tables 失败（表 %s）: %v", table, err)
		}
		if !exists {
			t.Errorf("并发迁移后缺少表: %s", table)
		}
	}
}

// gooseVersion 读取 goose 版本表的当前最大版本号。
func gooseVersion(t *testing.T, st *Store) int64 {
	t.Helper()
	var v int64
	err := st.pool.QueryRow(t.Context(),
		`SELECT COALESCE(MAX(version_id), 0) FROM goose_db_version`).Scan(&v)
	if err != nil {
		t.Fatalf("查询 goose_db_version 失败: %v", err)
	}
	return v
}
