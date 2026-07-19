package llm

import (
	"context"
	"errors"
	"github.com/YouToco/vane/store"
	"log/slog"
	"time"
	"unicode/utf8"

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
	// 配额闸门在**发请求之前**（契约 §2.7，D3 财务护栏）。
	//
	// llm 包有**两个**发请求的入口，两个都必须装闸门：Do（单轮，打分/出卡/演化）
	// 与 DoChat（多轮 function calling，agent/深挖/A2A）。第一版只装了 Do 并在注释里
	// 断言它是"唯一咽喉"——那句话是错的，而且漏掉的恰恰是最贵的一条：生产 7 天实测
	// agent 的 prompt 均值 4381、峰值 44871，是打分的 10 倍与 42 倍。
	// TestInvariant_EveryUpstreamEntryHasQuotaGate 守住"新增入口必须装闸门"。
	estimate := estimateTokens(utf8.RuneCountInString(req.System)+utf8.RuneCountInString(req.User), req.MaxTokens)
	if err := rec.CheckQuota(ctx, meta.UserID, estimate); err != nil {
		switch {
		case errors.Is(err, store.ErrQuotaExceeded):
			return nil, types.NewAppError(types.CodeQuotaExceeded,
				"本租户的 LLM 额度已用尽，稍后会随时间自动恢复", nil)

		case errors.Is(err, store.ErrAmbiguousTenant):
			// 归属不明 ⇒ **拒绝**，不放行。此刻我们根本不知道该记谁的账，
			// 而花一笔无法归属的钱正是这道护栏存在的理由。
			// 且它是确定性的：重试一万次还是多行，放行等于给该用户无限额度。
			slog.Error("llm: 用户归属多个租户，无法判定配额归属，拒绝调用",
				"user_id", *meta.UserID)
			return nil, types.NewAppError(types.CodeInternal,
				"账号归属异常，暂时无法处理，请联系管理员", err)

		default:
			// 其余（数据库抖动等）：**放行**。这是旁路闸门——让 DB 抖动升级成
			// 全局 LLM 停摆，比"超额一点"糟糕得多。用 Error 而非 Warn：
			// 配额查询失败意味着此刻护栏是失效的，这件事必须在日志里显眼。
			slog.Error("llm: 配额查询失败，本次放行（护栏此刻失效）", "err", err)
		}
	}

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
		call.CostUSD = CostUSD(resp.Model, hitTokens, missTokens, resp.CompletionTokens)
	}

	// 失败路径的 ctx 往往已经超时/取消，直接用它写库必然失败，
	// 与"失败也要记账"矛盾——记账用 WithoutCancel 剥离取消信号。
	rec.Record(context.WithoutCancel(ctx), call)
	// 对账实际用量：用 WithoutCancel，理由同记账——调用已完成，不该因为上游 ctx
	// 被取消就漏账，那会让取消变成一条绕过配额的免费通道。
	// 失败/取消时 call.*Tokens 为 0，于是这里把预扣的估算**全额退还**——
	// 这是刻意的：上游若真的计了费，我们也没有任何字段能知道，凭空扣一笔
	// 猜出来的量会让账目更不可信。
	rec.ReconcileQuota(context.WithoutCancel(ctx), meta.UserID, estimate, call.PromptTokens+call.CompletionTokens)
	return resp, err
}
