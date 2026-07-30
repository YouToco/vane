package store

import (
	"context"

	"github.com/YouToco/vane/types"
)

// InsertLLMCall 写入一条 LLM 调用记录，返回新 id。
// 列清单与 001_init.sql 的 llm_calls 全列对齐（id/created_at 除外：
// id 自增、created_at 以数据库 now() 为准，避免应用与 DB 时钟漂移）。
// user_id / ref_id / prefix_cache_hit / temperature / max_tokens 均为指针，
// nil 时写 NULL 保留"未设置/未报告"的三态语义（001 审查裁决 2026-07-14）。
//
// tenant_id 对 compiled runtime 由调用方显式传入冻结的租户与用户，
// 使一个用户属于多个租户、或上游请求后成员关系被撤销时，付费回执
// 仍精确归属到授权时的租户。legacy 调用才由 user_id 反查 memberships：
// 迁移 021 给本表加列时只回填了存量、没改这条 INSERT，也没建触发器/默认值，
// 于是**上线后每一行的 tenant_id 都是 NULL**（生产实证：021 部署时刻前后
// 零重叠，883 行有值全在前、62 行 NULL 全在后）。后果有二——
// 一是 per-tenant 成本归集拿不到数据；二是 022 的 RLS 一旦切连接角色激活，
// `tenant_id = current_setting('app.tenant_id')` 对 NULL 恒不成立，
// 用户会看到自己的 LLM 调用记录**全部消失**。
//
// legacy 路径继续复用 tenantOfUser：本表的旧调用点（scorer/cardgen/
// agent/evolver/deepdive/feishu/playbook_translate）手里都只有 userID、没有租户
// 上下文，逐个改要动整条 Temporal 活动参数链；而 021 的回填、以及 push_batches
// 等 8 张表的写入，用的都是这同一条规则，此处照用才不会漂移（理由详见
// tenantderive.go）。user_id 为 NULL（系统级调用）时子查询返回 NULL——
// 这正是 021 注释所说「一次系统级 LLM 调用确实不属于任何租户」。
func (s *Store) InsertLLMCall(ctx context.Context, c *types.LLMCall) (int64, error) {
	if c == nil {
		return 0, types.NewAppError(types.CodeValidation,
			"llm_calls 记录不能为空", types.ErrValidation)
	}
	if c.TenantID != nil && (*c.TenantID <= 0 || c.UserID == nil || *c.UserID <= 0) {
		return 0, types.NewAppError(types.CodeValidation,
			"显式 llm_calls 租户归属必须同时包含正数 tenant_id 与 user_id",
			types.ErrValidation)
	}
	if (c.PromptCacheHitTokens == nil) != (c.PromptCacheMissTokens == nil) {
		return 0, types.NewAppError(types.CodeValidation,
			"缓存命中与未命中 token 必须同时提供或同时缺省", types.ErrValidation)
	}
	if c.PromptCacheHitTokens != nil &&
		(*c.PromptCacheHitTokens < 0 || *c.PromptCacheMissTokens < 0 ||
			*c.PromptCacheHitTokens+*c.PromptCacheMissTokens != c.PromptTokens) {
		return 0, types.NewAppError(types.CodeValidation,
			"缓存 token 明细必须非负且合计等于 prompt_tokens", types.ErrValidation)
	}
	if c.ReasoningTokens != nil &&
		(*c.ReasoningTokens < 0 || *c.ReasoningTokens > c.CompletionTokens) {
		return 0, types.NewAppError(types.CodeValidation,
			"reasoning_tokens 必须是 completion_tokens 的非负子集", types.ErrValidation)
	}
	var id int64
	err := s.pool.QueryRow(ctx,
		`WITH stamp AS (
		   SELECT statement_timestamp() AS at
		 ),
		 price AS (
		   SELECT pr.id, pr.currency,
		          (
		            COALESCE($19::int, 0)::numeric * pr.input_cache_hit_per_million
		            + (CASE WHEN $20::int IS NULL THEN $11::int ELSE $20::int END)::numeric
		              * pr.input_cache_miss_per_million
		            + $12::int::numeric * pr.output_per_million
		          ) / 1000000::numeric AS amount
		     FROM provider_price_rules pr, stamp
		    WHERE pr.provider = lower(btrim($6))
		      AND pr.resource = btrim($7)
		      AND pr.meter = 'llm_tokens'
		      AND pr.effective_from <= stamp.at
		      AND (pr.effective_to IS NULL OR pr.effective_to > stamp.at)
		    ORDER BY pr.effective_from DESC, pr.id DESC
		    LIMIT 1
		 )
		 INSERT INTO llm_calls (
			trace_id, span_name, user_id, ref_type, ref_id,
			provider, model, system_prompt, user_prompt, completion,
			prompt_tokens, completion_tokens, latency_ms, cost_usd,
			prefix_cache_hit, temperature, max_tokens, error,
			tenant_id, prompt_cache_hit_tokens, prompt_cache_miss_tokens,
			reasoning_tokens, pricing_rule_id, pricing_status,
			cost_amount, cost_currency, created_at
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10,
			$11, $12, $13,
			CASE
			  WHEN $14::numeric > 0 THEN $14::numeric
			  WHEN (SELECT currency FROM price) = 'USD' THEN COALESCE((SELECT amount FROM price), 0)
			  ELSE 0
			END,
			$15, $16, $17, $18,
			CASE WHEN $22::bigint IS NULL THEN `+tenantOfUser+`$3) ELSE $22 END,
			$19, $20, $21,
			CASE WHEN $14::numeric > 0 THEN NULL ELSE (SELECT id FROM price) END,
			CASE
			  WHEN $14::numeric > 0 THEN 'provider_reported'
			  WHEN NOT EXISTS (SELECT 1 FROM price) THEN 'unpriced'
			  WHEN $19::int IS NULL THEN 'estimated'
			  ELSE 'calculated'
			END,
			CASE
			  WHEN $14::numeric > 0 THEN $14::numeric
			  ELSE (SELECT amount FROM price)
			END,
			CASE
			  WHEN $14::numeric > 0 THEN 'USD'
			  ELSE (SELECT currency FROM price)
			END,
			(SELECT at FROM stamp)
		) RETURNING id`,
		c.TraceID, c.SpanName, c.UserID, c.RefType, c.RefID,
		c.Provider, c.Model, c.SystemPrompt, c.UserPrompt, c.Completion,
		c.PromptTokens, c.CompletionTokens, c.LatencyMs, c.CostUSD,
		c.PrefixCacheHit, c.Temperature, c.MaxTokens, c.Error,
		c.PromptCacheHitTokens, c.PromptCacheMissTokens, c.ReasoningTokens, c.TenantID,
	).Scan(&id)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "写入 llm_calls 记录", err)
	}
	return id, nil
}
