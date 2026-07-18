package store

import (
	"encoding/json"
	"os"
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
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 tool_calls 集成测试")
	}
	ctx := t.Context()
	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 失败: %v", err)
	}
	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 失败: %v", err)
	}
	defer st.Close()

	// 基线：测试可能跑在有存量数据的库上，计数断言用差值。
	base, err := st.CountTikHubEndpointCallsSince(ctx, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("基线计数失败: %v", err)
	}

	uid := int64(1)
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
	err = st.pool.QueryRow(ctx,
		`SELECT trace_id, user_id, tool_name, tool_kind, endpoint_path, arguments,
		        result_preview, result_size, http_status, error_type, duration_ms,
		        retrieval_query, candidate_tools
		 FROM tool_calls WHERE id = $1`, id).Scan(
		&got.TraceID, &got.UserID, &got.ToolName, &got.ToolKind, &got.EndpointPath,
		&got.Arguments, &got.ResultPreview, &got.ResultSize, &got.HTTPStatus,
		&got.ErrorType, &got.DurationMs, &got.RetrievalQuery, &cands)
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

	// search 记录带候选数组。
	if _, err := st.InsertToolCall(ctx, &types.ToolCall{
		ToolName: "search_endpoints", ToolKind: types.ToolCallKindTikHubSearch,
		RetrievalQuery: "小红书 搜索", CandidateTools: []string{"a", "b"},
	}); err != nil {
		t.Fatalf("search 记录写入失败: %v", err)
	}

	// 不计入限额的三类：static、invalid_args、budget_exceeded。
	for _, c := range []*types.ToolCall{
		{ToolName: "list_sources", ToolKind: types.ToolCallKindStatic},
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
