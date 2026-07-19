package store

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/YouToco/vane/types"
)

// InsertToolCall 写入一条工具调用记录，返回新 id。
// 列清单与 015_tool_calls.sql 全列对齐（id/created_at 除外，理由同 InsertLLMCall）。
// candidate_tools 为 nil 时写空数组：列 NOT NULL DEFAULT '{}'，nil 直传 pgx 会写 NULL。
//
// tenant_id 同 InsertLLMCall：021 只回填存量、没改 INSERT，上线后全是 NULL
// （生产实证 13 行有值全在部署前、28 行 NULL 全在部署后）。用 tenantOfUser 反查
// 的理由见 tenantderive.go。llm_calls 与 tool_calls 是 021 加了 tenant_id 却
// **没设 NOT NULL** 的仅有两张表——其余 8 张漏写会被 NOT NULL 当场拦下，
// 这两张因可空而静默漏了整整一个部署周期。
func (s *Store) InsertToolCall(ctx context.Context, c *types.ToolCall) (int64, error) {
	cands := c.CandidateTools
	if cands == nil {
		cands = []string{}
	}
	var id int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO tool_calls (
			trace_id, user_id, session_id, tool_name, tool_kind,
			endpoint_path, arguments, result_preview, result_size, http_status,
			error_type, error, duration_ms, retrieval_query, candidate_tools,
			cost_usd, source_id, tenant_id
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15,
			$16, $17, `+tenantOfUser+`$2)
		) RETURNING id`,
		c.TraceID, c.UserID, c.SessionID, c.ToolName, c.ToolKind,
		c.EndpointPath, c.Arguments, c.ResultPreview, c.ResultSize, c.HTTPStatus,
		c.ErrorType, c.Error, c.DurationMs, c.RetrievalQuery, cands,
		c.CostUSD, c.SourceID,
	).Scan(&id)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "写入 tool_calls 记录", err)
	}
	return id, nil
}

// CountTikHubEndpointCallsSince 统计 since 以来 TikHub 端点（按次计费面）的调用次数，
// 供每日限额判定（滚动 24h 窗口，见 agent 端点工具）。系统级不分用户：TikHub 计费
// 是全局成本，单 owner MVP 下二者等价。命中 idx_tool_calls_kind_created。
//
// 限额判定读的就是记账表——记账与限额同源，不会出现「拦截口径与账本对不上」。
// 口径：**打到上游的调用**都计入（含 HTTP 错误/超时——失败同样计费）；
// 排除 invalid_args（校验拦下，没发请求）与 budget_exceeded（限额拦下，没发请求）
// ——否则被拒绝的调用本身会把限额越顶越死，永远解不开。
func (s *Store) CountTikHubEndpointCallsSince(ctx context.Context, since time.Time) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM tool_calls
		 WHERE tool_kind = $1 AND created_at >= $2
		   AND error_type NOT IN ($3, $4)`,
		types.ToolCallKindTikHubEndpoint, since,
		types.ToolErrInvalidArgs, types.ToolErrBudgetExceeded).Scan(&n)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "统计 TikHub 端点调用量", err)
	}
	return n, nil
}

// RecordBindingCall 实现 fetcher.BindingCallRecorder：绑定引擎（调度面）的每次上游
// 调用同步落一行 tool_calls（endpoint-binding-contract.md §5）。失败只记日志——
// 记账是旁路可观测性，绝不放大成抓取失败（与 agent ToolCallRecorder 同一纪律）。
func (s *Store) RecordBindingCall(ctx context.Context, rec *types.ToolCall) {
	// 兜底净化（引擎侧已净化参数，此处双保险）：上游错误文案可能带 NUL/非法 UTF-8，
	// Postgres TEXT 拒收会让整行记账丢失——恰好丢掉最该记账的失败调用。
	rec.Error = strings.ToValidUTF8(strings.ReplaceAll(rec.Error, "\x00", ""), "�")
	rec.ResultPreview = strings.ToValidUTF8(strings.ReplaceAll(rec.ResultPreview, "\x00", ""), "�")
	if _, err := s.InsertToolCall(ctx, rec); err != nil {
		slog.Warn("tool_calls 绑定抓取记账失败（旁路，不影响抓取）",
			"tool", rec.ToolName, "endpoint", rec.EndpointPath, "err", err)
	}
}
