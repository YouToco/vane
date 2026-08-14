package store

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/YouToco/vane/server/types"
	"github.com/jackc/pgx/v5"
)

// InsertToolCall 写入一条工具调用记录，返回新 id。
// 列清单与 015_tool_calls.sql 全列对齐（id/created_at 除外，理由同 InsertLLMCall）。
// candidate_tools 为 nil 时写空数组：列 NOT NULL DEFAULT '{}'，nil 直传 pgx 会写 NULL。
//
// tenant_id 同 InsertLLMCall：compiled runtime 显式携带冻结租户，保证多租户成员关系
// 与调用后撤权都不会让付费回执串租户；legacy 调用才用 tenantOfUser 反查。
// 021 只回填存量、没改 INSERT，上线后曾全部为 NULL（生产实证 13 行有值全在
// 部署前、28 行 NULL 全在部署后）。llm_calls 与 tool_calls 是 021 加了 tenant_id 却
// **没设 NOT NULL** 的仅有两张表——其余 8 张漏写会被 NOT NULL 当场拦下，
// 这两张因可空而静默漏了整整一个部署周期。
func (s *Store) InsertToolCall(ctx context.Context, c *types.ToolCall) (int64, error) {
	if c == nil {
		return 0, types.NewAppError(types.CodeValidation,
			"tool_calls 记录不能为空", types.ErrValidation)
	}
	if c.TenantID != nil && (*c.TenantID <= 0 || c.UserID == nil || *c.UserID <= 0) {
		return 0, types.NewAppError(types.CodeValidation,
			"显式 tool_calls 租户归属必须同时包含正数 tenant_id 与 user_id",
			types.ErrValidation)
	}
	if c.RunSnapshotID != nil &&
		(*c.RunSnapshotID <= 0 || c.TenantID == nil || c.UserID == nil) {
		return 0, types.NewAppError(types.CodeValidation,
			"tool_calls 运行快照归属必须包含正数 snapshot/tenant/user",
			types.ErrValidation)
	}
	c.Provider = strings.ToLower(strings.TrimSpace(c.Provider))
	if c.UsageQuantity <= 0 {
		c.UsageQuantity = 1
	}
	cands := c.CandidateTools
	if cands == nil {
		cands = []string{}
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "开始写入 tool_calls 记录", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))`,
		providerPricingLedgerLock,
	); err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "锁定供应商价格账本", err)
	}
	var id int64
	err = tx.QueryRow(ctx,
		`WITH stamp AS (
		   SELECT statement_timestamp() AS at
		 ),
		 billable AS (
		   SELECT $11 = '' AND (
		            ($19 = 'tikhub' AND $10 = 200)
		         OR ($19 <> 'tikhub' AND $10 BETWEEN 200 AND 299)
		          ) AS ok
		 ),
		 price AS (
		   SELECT pr.id, pr.currency,
		          pr.request_unit_price
		          + GREATEST($20::numeric - pr.request_included_quantity, 0)
		            * pr.request_additional_unit_price AS amount,
		          pr.resource = '*' AS wildcard
		     FROM provider_price_rules pr, stamp
		    WHERE pr.provider = $19
		      AND pr.meter = 'request'
		      AND pr.resource IN ($6, '*')
		      AND pr.effective_from <= stamp.at
		      AND (pr.effective_to IS NULL OR pr.effective_to > stamp.at)
		    ORDER BY (pr.resource = $6) DESC, pr.effective_from DESC, pr.id DESC
		    LIMIT 1
		 )
		 INSERT INTO tool_calls (
			trace_id, user_id, session_id, tool_name, tool_kind,
			endpoint_path, arguments, result_preview, result_size, http_status,
			error_type, error, duration_ms, retrieval_query, candidate_tools,
			cost_usd, source_id, tenant_id,
			provider, usage_quantity, pricing_rule_id, pricing_status,
			cost_amount, cost_currency, run_snapshot_id, created_at
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15,
			CASE
			  WHEN $16::numeric IS NOT NULL THEN $16::numeric
			  WHEN (SELECT ok FROM billable)
			       AND (SELECT currency FROM price) = 'USD'
			    THEN (SELECT amount FROM price)
			  ELSE NULL
			END,
			$17,
			CASE WHEN $18::bigint IS NULL THEN `+tenantOfUser+`$2) ELSE $18 END,
			$19, $20,
			CASE WHEN $16::numeric IS NOT NULL THEN NULL
			     WHEN (SELECT ok FROM billable) THEN (SELECT id FROM price)
			     ELSE NULL END,
			CASE
			  WHEN $16::numeric IS NOT NULL THEN 'provider_reported'
			  WHEN $19 = '' OR NOT COALESCE((SELECT ok FROM billable), false)
			    THEN 'unpriced'
			  WHEN NOT EXISTS (SELECT 1 FROM price) THEN 'unpriced'
			  WHEN (SELECT wildcard FROM price) THEN 'estimated'
			  ELSE 'calculated'
			END,
			CASE
			  WHEN $16::numeric IS NOT NULL THEN $16::numeric
			  WHEN (SELECT ok FROM billable) THEN (SELECT amount FROM price)
			  ELSE NULL
			END,
			CASE
			  WHEN $16::numeric IS NOT NULL THEN 'USD'
			  WHEN (SELECT ok FROM billable) THEN (SELECT currency FROM price)
			  ELSE NULL
			END,
			$21,
			(SELECT at FROM stamp)
		) RETURNING id`,
		c.TraceID, c.UserID, c.SessionID, c.ToolName, c.ToolKind,
		c.EndpointPath, c.Arguments, c.ResultPreview, c.ResultSize, c.HTTPStatus,
		c.ErrorType, c.Error, c.DurationMs, c.RetrievalQuery, cands,
		c.CostUSD, c.SourceID, c.TenantID, c.Provider, c.UsageQuantity,
		c.RunSnapshotID,
	).Scan(&id)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "写入 tool_calls 记录", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "提交 tool_calls 记录", err)
	}
	return id, nil
}

// insertToolCallTx writes the legacy observability row inside a caller-owned
// transaction. AgentTurnRecordV1 uses this to commit tool_calls and the exact
// model-visible evidence as one atomic unit.
func insertToolCallTx(ctx context.Context, tx pgx.Tx, c *types.ToolCall) (int64, error) {
	if c == nil || c.TenantID == nil || *c.TenantID <= 0 ||
		c.UserID == nil || *c.UserID <= 0 || c.SessionID == nil || *c.SessionID <= 0 {
		return 0, types.NewAppError(types.CodeValidation,
			"Agent tool_calls 必须携带 exact tenant/user/session", types.ErrValidation)
	}
	c.Provider = strings.ToLower(strings.TrimSpace(c.Provider))
	if c.UsageQuantity <= 0 {
		c.UsageQuantity = 1
	}
	cands := c.CandidateTools
	if cands == nil {
		cands = []string{}
	}
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))`,
		providerPricingLedgerLock,
	); err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "锁定供应商价格账本", err)
	}
	var id int64
	err := tx.QueryRow(ctx,
		`WITH stamp AS (
		   SELECT statement_timestamp() AS at
		 ),
		 billable AS (
		   SELECT $11 = '' AND (
		            ($19 = 'tikhub' AND $10 = 200)
		         OR ($19 <> 'tikhub' AND $10 BETWEEN 200 AND 299)
		          ) AS ok
		 ),
		 price AS (
		   SELECT pr.id, pr.currency,
		          pr.request_unit_price
		          + GREATEST($20::numeric - pr.request_included_quantity, 0)
		            * pr.request_additional_unit_price AS amount,
		          pr.resource = '*' AS wildcard
		     FROM provider_price_rules pr, stamp
		    WHERE pr.provider = $19
		      AND pr.meter = 'request'
		      AND pr.resource IN ($6, '*')
		      AND pr.effective_from <= stamp.at
		      AND (pr.effective_to IS NULL OR pr.effective_to > stamp.at)
		    ORDER BY (pr.resource = $6) DESC, pr.effective_from DESC, pr.id DESC
		    LIMIT 1
		 )
		 INSERT INTO tool_calls (
			trace_id, user_id, session_id, tool_name, tool_kind,
			endpoint_path, arguments, result_preview, result_size, http_status,
			error_type, error, duration_ms, retrieval_query, candidate_tools,
			cost_usd, source_id, tenant_id,
			provider, usage_quantity, pricing_rule_id, pricing_status,
			cost_amount, cost_currency, run_snapshot_id, created_at
		 ) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15,
			CASE
			  WHEN $16::numeric IS NOT NULL THEN $16::numeric
			  WHEN (SELECT ok FROM billable)
			       AND (SELECT currency FROM price) = 'USD'
			    THEN (SELECT amount FROM price)
			  ELSE NULL
			END,
			$17, $18,
			$19, $20,
			CASE WHEN $16::numeric IS NOT NULL THEN NULL
			     WHEN (SELECT ok FROM billable) THEN (SELECT id FROM price)
			     ELSE NULL END,
			CASE
			  WHEN $16::numeric IS NOT NULL THEN 'provider_reported'
			  WHEN $19 = '' OR NOT COALESCE((SELECT ok FROM billable), false)
			    THEN 'unpriced'
			  WHEN NOT EXISTS (SELECT 1 FROM price) THEN 'unpriced'
			  WHEN (SELECT wildcard FROM price) THEN 'estimated'
			  ELSE 'calculated'
			END,
			CASE
			  WHEN $16::numeric IS NOT NULL THEN $16::numeric
			  WHEN (SELECT ok FROM billable) THEN (SELECT amount FROM price)
			  ELSE NULL
			END,
			CASE
			  WHEN $16::numeric IS NOT NULL THEN 'USD'
			  WHEN (SELECT ok FROM billable) THEN (SELECT currency FROM price)
			  ELSE NULL
			END,
			$21,(SELECT at FROM stamp)
		 ) RETURNING id`,
		c.TraceID, c.UserID, c.SessionID, c.ToolName, c.ToolKind,
		c.EndpointPath, c.Arguments, c.ResultPreview, c.ResultSize, c.HTTPStatus,
		c.ErrorType, c.Error, c.DurationMs, c.RetrievalQuery, cands,
		c.CostUSD, c.SourceID, c.TenantID, c.Provider, c.UsageQuantity,
		c.RunSnapshotID,
	).Scan(&id)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "写入 Agent tool_calls 记录", err)
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

// CountExaAdHocCallsSince 统计 since 以来 Exa ad-hoc 工具（web_search/read_page）
// 的调用次数，供每日限额判定（滚动 24h 窗口，见 agent/exa_tools.go 头注）。
//
// 按 agent 层记账的 tool_name 计数而非 tool_kind='exa_fetch'：fetcher 层记账
// （exa:search/exa:contents）含信源周期抓取与 enrich 补全——那些不是 ad-hoc 对话
// 调用，混进来会把对话限额误顶爆。系统级不分用户（同 TikHub 限额：Exa 计费是全局
// 成本，单 owner MVP 下二者等价）。
// 口径同 CountTikHubEndpointCallsSince：打到上游的调用都计入（含失败——失败同样
// 计费）；排除 invalid_args 与 budget_exceeded（没打上游的拒绝不越顶越死）。
func (s *Store) CountExaAdHocCallsSince(ctx context.Context, since time.Time) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM tool_calls
		 WHERE tool_name IN ('web_search', 'read_page') AND created_at >= $1
		   AND error_type NOT IN ($2, $3)`,
		since, types.ToolErrInvalidArgs, types.ToolErrBudgetExceeded).Scan(&n)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "统计 Exa ad-hoc 调用量", err)
	}
	return n, nil
}

// RecordBindingCall 实现 fetcher.BindingCallRecorder：绑定引擎（调度面）的每次上游
// 调用同步落一行 tool_calls（endpoint-binding-contract.md §5）。失败只记日志——
// 记账是旁路可观测性，绝不放大成抓取失败（与 agent ToolCallRecorder 同一纪律）。
func (s *Store) RecordBindingCall(ctx context.Context, rec *types.ToolCall) {
	if rec == nil {
		slog.Warn("tool_calls 绑定抓取记账收到空记录（旁路，不影响抓取）")
		return
	}
	// 兜底净化（引擎侧已净化参数，此处双保险）：上游错误文案可能带 NUL/非法 UTF-8，
	// Postgres TEXT 拒收会让整行记账丢失——恰好丢掉最该记账的失败调用。
	rec.Error = strings.ToValidUTF8(strings.ReplaceAll(rec.Error, "\x00", ""), "�")
	rec.ResultPreview = strings.ToValidUTF8(strings.ReplaceAll(rec.ResultPreview, "\x00", ""), "�")
	if _, err := s.InsertToolCall(ctx, rec); err != nil {
		slog.Warn("tool_calls 绑定抓取记账失败（旁路，不影响抓取）",
			"tool", rec.ToolName, "endpoint", rec.EndpointPath, "err", err)
	}
}
