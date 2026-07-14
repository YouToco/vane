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
func (s *Store) InsertLLMCall(ctx context.Context, c *types.LLMCall) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO llm_calls (
			trace_id, span_name, user_id, ref_type, ref_id,
			provider, model, system_prompt, user_prompt, completion,
			prompt_tokens, completion_tokens, latency_ms, cost_usd,
			prefix_cache_hit, temperature, max_tokens, error
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10,
			$11, $12, $13, $14,
			$15, $16, $17, $18
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
