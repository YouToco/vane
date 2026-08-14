package store

import (
	"os"
	"testing"

	"github.com/YouToco/vane/server/types"
)

// TestTenantDerive_CallTables 锁死 llm_calls / tool_calls 的 tenant_id 写入。
//
// 为什么单独为这两张表写测试：migration 021 给 10 张表加了 tenant_id，其中 8 张
// 设了 NOT NULL——漏写会被约束当场拦下，不可能静默。只有这两张刻意可空
// （系统级调用确实无归属），于是 021 漏改 INSERT 时没有任何东西拦得住，
// 上线后整整一个部署周期里每一行都是 NULL，直到查生产才发现：
// 有值的 883 行全在部署前、NULL 的 62 行全在部署后，零重叠。
//
// 本测试就是那道缺失的约束：可空列没有 NOT NULL 兜底，就得靠用例兜。
//
// 两个方向都要锁——
//   - 有归属的调用必须落到租户（否则 per-tenant 成本归集拿不到数据，
//     且 022 的 RLS 一旦激活，用户会看不见自己的调用记录）；
//   - 系统级调用（user_id 为 NULL）必须仍是 NULL，不能为了"补全"而乱认租户。
func TestTenantDerive_CallTables(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 tenant_id 推导集成测试")
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

	uid := testUserWithTenant(t, st, "tenant-derive")

	t.Cleanup(func() {
		cctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cctx, t, st, `DELETE FROM llm_calls WHERE trace_id LIKE 'tenant-derive-%'`)
		cleanupExec(cctx, t, st, `DELETE FROM tool_calls WHERE trace_id LIKE 'tenant-derive-%'`)
		cleanupExec(cctx, t, st, `DELETE FROM memberships WHERE user_id = $1`, uid)
		cleanupExec(cctx, t, st, `DELETE FROM users WHERE id = $1`, uid)
	})

	// 期望的租户号：attachTenant 把用户挂在平台租户 1 上。
	var wantTenant int64
	if err := st.pool.QueryRow(ctx,
		`SELECT tenant_id FROM memberships WHERE user_id = $1`, uid).Scan(&wantTenant); err != nil {
		t.Fatalf("读取 memberships 失败: %v", err)
	}

	t.Run("llm_calls 有归属写租户", func(t *testing.T) {
		id, err := st.InsertLLMCall(ctx, &types.LLMCall{
			TraceID: "tenant-derive-llm", SpanName: "score", UserID: &uid,
		})
		if err != nil {
			t.Fatalf("InsertLLMCall() 失败: %v", err)
		}
		var got *int64
		if err := st.pool.QueryRow(ctx,
			`SELECT tenant_id FROM llm_calls WHERE id = $1`, id).Scan(&got); err != nil {
			t.Fatalf("回读失败: %v", err)
		}
		if got == nil {
			t.Fatal("tenant_id 为 NULL——021 的漏写回归了：" +
				"有归属用户的 LLM 调用必须落到租户，否则成本归集与 RLS 都会失效")
		}
		if *got != wantTenant {
			t.Errorf("tenant_id = %d，期望 %d", *got, wantTenant)
		}
	})

	t.Run("llm_calls 系统级保持 NULL", func(t *testing.T) {
		id, err := st.InsertLLMCall(ctx, &types.LLMCall{
			TraceID: "tenant-derive-llm-sys", SpanName: "score", UserID: nil,
		})
		if err != nil {
			t.Fatalf("InsertLLMCall() 失败: %v", err)
		}
		var got *int64
		if err := st.pool.QueryRow(ctx,
			`SELECT tenant_id FROM llm_calls WHERE id = $1`, id).Scan(&got); err != nil {
			t.Fatalf("回读失败: %v", err)
		}
		if got != nil {
			t.Errorf("系统级调用的 tenant_id = %d，应为 NULL"+
				"（021 注释：一次系统级 LLM 调用确实不属于任何租户）", *got)
		}
	})

	t.Run("tool_calls 有归属写租户", func(t *testing.T) {
		id, err := st.InsertToolCall(ctx, &types.ToolCall{
			TraceID: "tenant-derive-tool", UserID: &uid,
			ToolName: "view_profile", ToolKind: types.ToolCallKindStatic,
		})
		if err != nil {
			t.Fatalf("InsertToolCall() 失败: %v", err)
		}
		var got *int64
		if err := st.pool.QueryRow(ctx,
			`SELECT tenant_id FROM tool_calls WHERE id = $1`, id).Scan(&got); err != nil {
			t.Fatalf("回读失败: %v", err)
		}
		if got == nil {
			t.Fatal("tenant_id 为 NULL——021 的漏写回归了")
		}
		if *got != wantTenant {
			t.Errorf("tenant_id = %d，期望 %d", *got, wantTenant)
		}
	})

	t.Run("tool_calls 系统级保持 NULL", func(t *testing.T) {
		id, err := st.InsertToolCall(ctx, &types.ToolCall{
			TraceID: "tenant-derive-tool-sys", UserID: nil,
			ToolName: "view_profile", ToolKind: types.ToolCallKindStatic,
		})
		if err != nil {
			t.Fatalf("InsertToolCall() 失败: %v", err)
		}
		var got *int64
		if err := st.pool.QueryRow(ctx,
			`SELECT tenant_id FROM tool_calls WHERE id = $1`, id).Scan(&got); err != nil {
			t.Fatalf("回读失败: %v", err)
		}
		if got != nil {
			t.Errorf("系统级调用的 tenant_id = %d，应为 NULL", *got)
		}
	})
}
