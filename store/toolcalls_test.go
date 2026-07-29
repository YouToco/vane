package store

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
)

// TestToolCalls_InsertAndCount 是 tool_calls 的库内往返 + 每日限额口径测试
// （需 DATABASE_URL，模式同 TestMigrate）。重点锁两件事：
//  1. 全字段写入不丢（含 candidate_tools 数组与可空 http_status）；
//  2. CountTikHubEndpointCallsSince 的口径——只数打到上游的端点调用：
//     排除 static/search kind、排除 invalid_args/budget_exceeded、排除窗口外。
func TestToolCalls_InsertAndCount(t *testing.T) {
	ctx := t.Context()
	st := tenantTestStore(t)

	// 基线：测试可能跑在有存量数据的库上，计数断言用差值。
	base, err := st.CountTikHubEndpointCallsSince(ctx, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("基线计数失败: %v", err)
	}

	uid := testUserWithTenant(t, st, "toolcalls")
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st, `DELETE FROM tool_calls WHERE user_id = $1`, uid)
		cleanupExec(cleanupCtx, t, st, `DELETE FROM memberships WHERE user_id = $1`, uid)
		cleanupExec(cleanupCtx, t, st, `DELETE FROM users WHERE id = $1`, uid)
	})
	status := 200
	full := &types.ToolCall{
		TraceID: "trace-tc-1", UserID: &uid,
		ToolName: "xiaohongshu_app_v2_search_notes", ToolKind: types.ToolCallKindTikHubEndpoint,
		EndpointPath:  "/api/v1/xiaohongshu/app_v2/search_notes",
		Arguments:     json.RawMessage(`{"keyword":"AI"}`),
		ResultPreview: "{...}", ResultSize: 12345, HTTPStatus: &status,
		DurationMs: 321,
	}
	id, err := st.InsertToolCall(ctx, full)
	if err != nil || id <= 0 {
		t.Fatalf("InsertToolCall 失败: id=%d err=%v", id, err)
	}

	// 回读全字段（含数组与 NULL 语义）。
	var got types.ToolCall
	var cands []string
	var gotTenantID, membershipTenantID int64
	err = st.pool.QueryRow(ctx,
		`SELECT trace_id, tc.user_id, tool_name, tool_kind, endpoint_path, arguments,
		        result_preview, result_size, http_status, error_type, duration_ms,
		        retrieval_query, candidate_tools, tc.tenant_id, m.tenant_id
		 FROM tool_calls tc
		 JOIN memberships m ON m.user_id = tc.user_id
		 WHERE tc.id = $1`, id).Scan(
		&got.TraceID, &got.UserID, &got.ToolName, &got.ToolKind, &got.EndpointPath,
		&got.Arguments, &got.ResultPreview, &got.ResultSize, &got.HTTPStatus,
		&got.ErrorType, &got.DurationMs, &got.RetrievalQuery, &cands,
		&gotTenantID, &membershipTenantID)
	if err != nil {
		t.Fatalf("回读失败: %v", err)
	}
	if got.ToolKind != types.ToolCallKindTikHubEndpoint || got.HTTPStatus == nil || *got.HTTPStatus != 200 ||
		got.ResultSize != 12345 || got.TraceID != "trace-tc-1" {
		t.Errorf("回读不符: %+v", got)
	}
	if len(cands) != 0 {
		t.Errorf("未提供 candidate_tools 应为空数组，实际 %v", cands)
	}
	if gotTenantID != membershipTenantID {
		t.Errorf("tenant 归属应由 user membership 推导：tool_call=%d membership=%d",
			gotTenantID, membershipTenantID)
	}

	// search 记录带候选数组。
	if _, err := st.InsertToolCall(ctx, &types.ToolCall{
		ToolName: "search_endpoints", ToolKind: types.ToolCallKindTikHubSearch,
		RetrievalQuery: "小红书 搜索", CandidateTools: []string{"a", "b"},
	}); err != nil {
		t.Fatalf("search 记录写入失败: %v", err)
	}

	// 不计入限额的三类：static、invalid_args、budget_exceeded。
	for _, c := range []*types.ToolCall{
		{ToolName: "view_profile", ToolKind: types.ToolCallKindStatic},
		{ToolName: "ep", ToolKind: types.ToolCallKindTikHubEndpoint, ErrorType: types.ToolErrInvalidArgs},
		{ToolName: "ep", ToolKind: types.ToolCallKindTikHubEndpoint, ErrorType: types.ToolErrBudgetExceeded},
	} {
		if _, err := st.InsertToolCall(ctx, c); err != nil {
			t.Fatalf("写入失败: %v", err)
		}
	}
	// 计入限额的失败调用（HTTP 错误照样计费）。
	if _, err := st.InsertToolCall(ctx, &types.ToolCall{
		ToolName: "ep", ToolKind: types.ToolCallKindTikHubEndpoint, ErrorType: types.ToolErrHTTP,
	}); err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	n, err := st.CountTikHubEndpointCallsSince(ctx, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("计数失败: %v", err)
	}
	// 本测试新增的应计入行：full（成功）+ http_error = 2。
	if n-base != 2 {
		t.Errorf("限额口径不符：新增应计 2（成功+HTTP 错误），实际差值 %d", n-base)
	}
}

// TestToolCalls_ExplicitTenantReceiptSurvivesMembershipChanges proves that a
// paid compiled fetch is accounted to the tenant frozen before the network
// effect. The receipt deliberately does not re-derive membership afterward:
// one user may belong to multiple tenants and may be revoked while the request
// is in flight.
func TestToolCalls_ExplicitTenantReceiptSurvivesMembershipChanges(t *testing.T) {
	ctx := t.Context()
	st := tenantTestStore(t)
	uid := testUser(t, st)

	var tenantA, tenantB int64
	for _, dst := range []*int64{&tenantA, &tenantB} {
		if err := st.pool.QueryRow(ctx,
			`INSERT INTO tenants (status, plan) VALUES ('active', 'free') RETURNING id`,
		).Scan(dst); err != nil {
			t.Fatalf("create tenant: %v", err)
		}
		if _, err := st.pool.Exec(ctx,
			`INSERT INTO memberships (tenant_id, user_id, role) VALUES ($1, $2, 'owner')`,
			*dst, uid); err != nil {
			t.Fatalf("attach tenant %d: %v", *dst, err)
		}
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st, `DELETE FROM tool_calls WHERE user_id = $1`, uid)
		cleanupExec(cleanupCtx, t, st, `DELETE FROM memberships WHERE user_id = $1`, uid)
		cleanupExec(cleanupCtx, t, st, `DELETE FROM tenants WHERE id IN ($1, $2)`, tenantA, tenantB)
		cleanupExec(cleanupCtx, t, st, `DELETE FROM users WHERE id = $1`, uid)
	})

	insert := func(trace string) int64 {
		t.Helper()
		id, err := st.InsertToolCall(ctx, &types.ToolCall{
			TraceID: trace, TenantID: &tenantA, UserID: &uid,
			ToolName: "exa:search", ToolKind: types.ToolCallKindExaFetch,
		})
		if err != nil {
			t.Fatalf("insert exact receipt %q: %v", trace, err)
		}
		return id
	}
	assertTenant := func(id int64) {
		t.Helper()
		var got int64
		if err := st.pool.QueryRow(ctx,
			`SELECT tenant_id FROM tool_calls WHERE id = $1`, id).Scan(&got); err != nil {
			t.Fatalf("read exact receipt: %v", err)
		}
		if got != tenantA {
			t.Fatalf("receipt tenant=%d, want frozen tenant %d", got, tenantA)
		}
	}

	// Ambiguous live memberships must not influence the exact receipt.
	assertTenant(insert("compiled-before-revoke"))

	// Revocation after the upstream effect must not erase or move its receipt.
	if _, err := st.pool.Exec(ctx,
		`DELETE FROM memberships WHERE tenant_id = $1 AND user_id = $2`, tenantA, uid); err != nil {
		t.Fatalf("revoke membership: %v", err)
	}
	assertTenant(insert("compiled-after-revoke"))

	if _, err := st.InsertToolCall(ctx, &types.ToolCall{TenantID: &tenantA}); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("tenant without user error=%v, want validation", err)
	}
	zero := int64(0)
	if _, err := st.InsertToolCall(ctx, &types.ToolCall{TenantID: &zero, UserID: &uid}); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("non-positive tenant error=%v, want validation", err)
	}
}
