package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestMigration024BackfillsCallTables 验证 024 真的补上了漏写的 tenant_id。
//
// 为什么不能只跑一遍迁移看它不报错：空库上 024 是**空操作**（没有 NULL 行可补），
// 那样跑一万次也证明不了它会回填。必须先造出「021 之后、#87 之前」那段窗口的形状
// ——有 user_id 但 tenant_id 为 NULL 的行——再单独执行回填语句。
//
// 同时钉死边界：user_id 也为空的行**不该**被填。那是真的系统级调用，
// 给它编一个租户比留 NULL 危险得多（看起来受了隔离，实际归属是假的）。
func TestMigration024BackfillsCallTables(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过迁移回填集成测试")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("连接数据库失败: %v", err)
	}
	defer pool.Close()
	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("迁移失败: %v", err)
	}

	var tenantID, userID int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO tenants DEFAULT VALUES RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatalf("建租户失败: %v", err)
	}
	if err := pool.QueryRow(ctx,
		// 邮箱必须唯一：uq_users_email_lower 是 lower(email) 上的唯一约束，
		// 硬编码值会让这个测试**一个数据库只能跑一次**——CI 每次新库所以绿，
		// 本地重跑必红（2026-07-19 实测撞上）。与本包其它用例一样用时间戳后缀。
		`INSERT INTO users (email, password_hash) VALUES ($1, 'x') RETURNING id`,
		fmt.Sprintf("backfill024-%d@test.local", time.Now().UnixNano())).
		Scan(&userID); err != nil {
		t.Fatalf("建用户失败: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO memberships (user_id, tenant_id, role) VALUES ($1, $2, 'owner')`,
		userID, tenantID); err != nil {
		t.Fatalf("建 membership 失败: %v", err)
	}

	// 造 #87 之前那段窗口的行：有归属但 tenant_id 漏写。
	var orphanLLM, orphanTool int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO llm_calls (trace_id, span_name, model, user_id, tenant_id)
		 VALUES ('t-024', 'score', 'm', $1, NULL) RETURNING id`, userID).Scan(&orphanLLM); err != nil {
		t.Fatalf("造 llm_calls 孤儿行失败: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`INSERT INTO tool_calls (trace_id, tool_name, tool_kind, user_id, tenant_id)
		 VALUES ('t-024', 'x', 'read', $1, NULL) RETURNING id`, userID).Scan(&orphanTool); err != nil {
		t.Fatalf("造 tool_calls 孤儿行失败: %v", err)
	}
	// 真·系统级调用：无 user_id，回填后必须仍为 NULL。
	var sysLLM int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO llm_calls (trace_id, span_name, model, user_id, tenant_id)
		 VALUES ('t-024-sys', 'score', 'm', NULL, NULL) RETURNING id`).Scan(&sysLLM); err != nil {
		t.Fatalf("造系统级行失败: %v", err)
	}

	// 重放 024 的回填语句（迁移本身已在上面跑过，此处验的是语句语义）。
	for _, q := range []string{
		`UPDATE llm_calls  t SET tenant_id = m.tenant_id FROM memberships m WHERE m.user_id = t.user_id AND t.tenant_id IS NULL`,
		`UPDATE tool_calls t SET tenant_id = m.tenant_id FROM memberships m WHERE m.user_id = t.user_id AND t.tenant_id IS NULL`,
	} {
		if _, err := pool.Exec(ctx, q); err != nil {
			t.Fatalf("回填失败: %v", err)
		}
	}

	assertTenant := func(table string, id int64, want *int64) {
		t.Helper()
		var got *int64
		if err := pool.QueryRow(ctx,
			`SELECT tenant_id FROM `+table+` WHERE id = $1`, id).Scan(&got); err != nil {
			t.Fatalf("查 %s#%d 失败: %v", table, id, err)
		}
		switch {
		case want == nil && got != nil:
			t.Errorf("%s#%d 无 user_id 却被填了 tenant_id=%d——系统级调用的归属被编造了", table, id, *got)
		case want != nil && got == nil:
			t.Errorf("%s#%d 有 user_id 却仍是 NULL——回填没生效", table, id)
		case want != nil && got != nil && *want != *got:
			t.Errorf("%s#%d tenant_id=%d，期望 %d", table, id, *got, *want)
		}
	}
	assertTenant("llm_calls", orphanLLM, &tenantID)
	assertTenant("tool_calls", orphanTool, &tenantID)
	assertTenant("llm_calls", sysLLM, nil)
}
