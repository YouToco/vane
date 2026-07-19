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
// tenant_id 由 user_id 反查 memberships 得出，而不是由调用方传入：
// 迁移 021 给本表加列时只回填了存量、没改这条 INSERT，也没建触发器/默认值，
// 于是**上线后每一行的 tenant_id 都是 NULL**（生产实证：021 部署时刻前后
// 零重叠，883 行有值全在前、62 行 NULL 全在后）。后果有二——
// 一是 per-tenant 成本归集拿不到数据；二是 022 的 RLS 一旦切连接角色激活，
// `tenant_id = current_setting('app.tenant_id')` 对 NULL 恒不成立，
// 用户会看到自己的 LLM 调用记录**全部消失**。
//
// 复用 tenantOfUser 而不是给调用方加形参：本表的 7 个调用点（scorer/cardgen/
// agent/evolver/deepdive/feishu/playbook_translate）手里都只有 userID、没有租户
// 上下文，逐个改要动整条 Temporal 活动参数链；而 021 的回填、以及 push_batches
// 等 8 张表的写入，用的都是这同一条规则，此处照用才不会漂移（理由详见
// tenantderive.go）。user_id 为 NULL（系统级调用）时子查询返回 NULL——
// 这正是 021 注释所说「一次系统级 LLM 调用确实不属于任何租户」。
func (s *Store) InsertLLMCall(ctx context.Context, c *types.LLMCall) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO llm_calls (
			trace_id, span_name, user_id, ref_type, ref_id,
			provider, model, system_prompt, user_prompt, completion,
			prompt_tokens, completion_tokens, latency_ms, cost_usd,
			prefix_cache_hit, temperature, max_tokens, error,
			tenant_id
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10,
			$11, $12, $13, $14,
			$15, $16, $17, $18,
			`+tenantOfUser+`$3)
		) RETURNING id`,
		c.TraceID, c.SpanName, c.UserID, c.RefType, c.RefID,
		c.Provider, c.Model, c.SystemPrompt, c.UserPrompt, c.Completion,
		c.PromptTokens, c.CompletionTokens, c.LatencyMs, c.CostUSD,
		c.PrefixCacheHit, c.Temperature, c.MaxTokens, c.Error,
	).Scan(&id)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "写入 llm_calls 记录", err)
	}
	return id, nil
}
