package llm

import (
	"context"
	"errors"
	"log/slog"
	"time"
	"unicode/utf8"

	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

// CallMeta 是一次调用的记账元信息，由业务层（feishu handler 等）填写。
type CallMeta struct {
	TraceID string
	// TenantID is required with QuotaRule and pins both financial accounting
	// and the llm_calls receipt to the prepared run's tenant.
	TenantID *int64
	SpanName string        // 调用环节，如 "chat_reply"
	UserID   *int64        // 系统级调用可为 nil
	RefType  types.RefType // 无关联业务对象可为空串
	RefID    *int64
	// QuotaRule is non-nil only for a compiled run and is copied from its
	// immutable snapshot. Legacy/chat calls keep using the live tenant rule.
	QuotaRule *runtimepolicy.QuotaBucketV1
	// BeforeSpend atomically re-authorizes a compiled task run and reserves its
	// estimated live tenant quota. Do invokes it through Request.BeforeSend,
	// after Client's semaphore wait and immediately before upstream HTTP.
	BeforeSpend func(context.Context, float64) error
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
	callerBeforeSend := req.BeforeSend
	reserved := 0.0
	reconcileQuota := false
	req.BeforeSend = func(sendCtx context.Context) error {
		if callerBeforeSend != nil {
			if err := callerBeforeSend(sendCtx); err != nil {
				return err
			}
		}
		if meta.BeforeSpend != nil {
			if meta.QuotaRule == nil || meta.TenantID == nil || *meta.TenantID <= 0 ||
				meta.UserID == nil || rec == nil || rec.st == nil {
				return types.NewAppError(types.CodeInternal,
					"运行配额暂时无法校验，本次未调用模型", nil)
			}
			if _, ok := rec.st.(exactTenantRecorderStore); !ok {
				return types.NewAppError(types.CodeInternal,
					"运行配额暂时无法对账，本次未调用模型", nil)
			}
			if err := validateLLMQuotaPolicyV1(*meta.QuotaRule); err != nil {
				return err
			}
			if err := meta.BeforeSpend(sendCtx, estimate); err != nil {
				return mapQuotaGateError(meta.UserID, err)
			}
			reserved = estimate
			reconcileQuota = true
			return nil
		}
		if meta.QuotaRule != nil {
			return types.NewAppError(types.CodeInternal,
				"运行配额暂时无法校验，本次未调用模型", nil)
		}
		if err := rec.CheckQuota(sendCtx, meta.UserID, estimate); err != nil {
			if errors.Is(err, store.ErrQuotaExceeded) || errors.Is(err, store.ErrAmbiguousTenant) {
				return mapQuotaGateError(meta.UserID, err)
			}
			// Legacy calls retain their availability-first behavior. No amount
			// was reserved, so a recovered post-call database tail charges the
			// full actual usage (reservation=0) instead of applying a bogus refund.
			slog.Error("llm: 配额查询失败，本次放行（护栏此刻失效）", "err", err)
			reserved = 0
			reconcileQuota = true
			return nil
		}
		reserved = estimate
		reconcileQuota = true
		return nil
	}

	start := time.Now()
	resp, err := c.Complete(ctx, req)

	call := &types.LLMCall{
		TenantID:     meta.TenantID,
		TraceID:      meta.TraceID,
		SpanName:     meta.SpanName,
		UserID:       meta.UserID,
		RefType:      meta.RefType,
		RefID:        meta.RefID,
		Provider:     c.provider,
		Model:        c.requestModel(req.Model), // 成功路径下面会覆盖为上游回报的实际模型名
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

	// 失败路径的 ctx 往往已经超时/取消；记账与配额对账共享一个有硬上限的
	// detached tail。调用已完成，不该漏账，也不能让 DB stall 无限拖住 Activity。
	// 失败/取消时 call.*Tokens 为 0，于是这里把预扣的估算**全额退还**——
	// 这是刻意的：上游若真的计了费，我们也没有任何字段能知道，凭空扣一笔
	// 猜出来的量会让账目更不可信。
	rec.finishCallAccountingWithReservation(ctx, call, meta.TenantID, meta.UserID, reserved,
		call.PromptTokens+call.CompletionTokens, meta.QuotaRule, reconcileQuota)
	return resp, err
}

func mapQuotaGateError(userID *int64, err error) error {
	if errors.Is(err, store.ErrQuotaExceeded) {
		return types.NewAppError(types.CodeQuotaExceeded,
			"本租户的 LLM 额度已用尽，稍后会随时间自动恢复", nil)
	}
	if errors.Is(err, store.ErrAmbiguousTenant) {
		if userID != nil {
			slog.Error("llm: 用户归属多个租户，无法判定配额归属，拒绝调用",
				"user_id", *userID)
		}
		return types.NewAppError(types.CodeInternal,
			"账号归属异常，暂时无法处理，请联系管理员", err)
	}
	return err
}
