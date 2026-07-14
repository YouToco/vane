package llm

import (
	"context"
	"time"

	"github.com/YouToco/vane/types"
)

// CallMeta 是一次调用的记账元信息，由业务层（feishu handler 等）填写。
type CallMeta struct {
	TraceID  string
	SpanName string        // 调用环节，如 "chat_reply"
	UserID   *int64        // 系统级调用可为 nil
	RefType  types.RefType // 无关联业务对象可为空串
	RefID    *int64
}

// Do 调用 + 记账一体：计时 → Complete → 构造 LLMCall → Record。
// 失败也要记账（error 字段填原因），否则限流/超时这类最需要观测的
// 失败反而在 llm_calls 里不可见。
func Do(ctx context.Context, c *Client, rec *Recorder, meta CallMeta, req Request) (*Response, error) {
	start := time.Now()
	resp, err := c.Complete(ctx, req)

	call := &types.LLMCall{
		TraceID:      meta.TraceID,
		SpanName:     meta.SpanName,
		UserID:       meta.UserID,
		RefType:      meta.RefType,
		RefID:        meta.RefID,
		Provider:     c.provider,
		Model:        c.model, // 成功路径下面会覆盖为上游回报的实际模型名
		SystemPrompt: req.System,
		UserPrompt:   req.User,
		Temperature:  req.Temperature,
		MaxTokens:    req.MaxTokens,
	}

	if err != nil {
		call.Error = err.Error()
		// Complete 失败拿不到 resp.LatencyMs，用 Do 自己的计时兜底。
		call.LatencyMs = int(time.Since(start).Milliseconds())
	} else {
		call.Model = resp.Model
		call.Completion = resp.Content
		call.PromptTokens = resp.PromptTokens
		call.CompletionTokens = resp.CompletionTokens
		call.LatencyMs = resp.LatencyMs

		// prefix_cache_hit 三态判定：Response 只有 int 字段、没有"字段是否
		// 存在"的信号，借助 DeepSeek 的不变量 hit+miss == prompt_tokens 推断
		// 缓存字段是否被上游返回——恒等式成立视为返回了（hit>0 即命中），
		// 不成立视为该 provider 未报告缓存信息，落 NULL。
		hitTokens, missTokens := resp.CacheHitTokens, resp.CacheMissTokens
		cacheReported := resp.PromptTokens > 0 && hitTokens+missTokens == resp.PromptTokens
		if cacheReported {
			hit := hitTokens > 0
			call.PrefixCacheHit = &hit
		} else {
			// 未报告缓存时按全量未命中计价：宁可略高估也不低估成本。
			hitTokens, missTokens = 0, resp.PromptTokens
		}
		call.CostUSD = CostUSD(hitTokens, missTokens, resp.CompletionTokens)
	}

	// 失败路径的 ctx 往往已经超时/取消，直接用它写库必然失败，
	// 与"失败也要记账"矛盾——记账用 WithoutCancel 剥离取消信号。
	rec.Record(context.WithoutCancel(ctx), call)
	return resp, err
}
