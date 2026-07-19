package llm

import (
	"context"
	"errors"
	"log/slog"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

// Recorder 负责把每次 LLM 调用同步写入 llm_calls 表。
// 单独成类型而不是让 Client 直接持有 store：调用与记账解耦，
// 单测 Client 时不需要数据库。
type Recorder struct {
	st *store.Store
}

// NewRecorder 构造记账器。
func NewRecorder(st *store.Store) *Recorder {
	return &Recorder{st: st}
}

// Record 同步写库。写失败只记日志、绝不向调用方返回错误：
// 记账是旁路可观测性，记账故障不能放大成业务调用失败
// （否则数据库抖动会让本已成功的 LLM 回复被误判为失败）。
func (r *Recorder) Record(ctx context.Context, call *types.LLMCall) {
	if r == nil || r.st == nil || call == nil {
		return
	}
	if _, err := r.st.InsertLLMCall(ctx, call); err != nil {
		slog.Error("llm 记账写库失败",
			"trace_id", call.TraceID,
			"span_name", call.SpanName,
			"model", call.Model,
			"err", err)
	}
}

// quotaProbeTokens 是调用前的"探针"扣减量：只用来证明桶里还有余额，不代表真实用量。
//
// 为什么必须有事前这一步：LLM 的 token 用量要调用完才知道，而**纯事后扣减是无效的
// 护栏**——桶空了之后，事后扣减同样失败，于是它只是"不再减少"，一次也拦不住。
// 事前探针把"还有没有额度"这个判断挪到了花钱之前。
//
// 取 1 而非用 MaxTokens 估算：估算需要在调用后退还差额，多一条容易写错的路径；
// 而探针的代价只是"最多超支一次调用的量"——单次调用的 token 本就有 MaxTokens 封顶，
// 这个超支是有界的，换来的是实现上没有退款路径。
const quotaProbeTokens = 1

// CheckQuota 在调用前确认租户还有 LLM 额度。
// 系统级调用（UserID 为 nil）不计入任何租户，直接放行。
func (r *Recorder) CheckQuota(ctx context.Context, userID *int64) error {
	if r == nil || r.st == nil || userID == nil {
		return nil
	}
	return r.st.TryConsumeForUser(ctx, *userID, store.QuotaLLMTokens, quotaProbeTokens)
}

// ConsumeQuota 在调用后扣掉实际用量（已扣的探针量除外）。
//
// **失败只记日志、不影响调用结果**：调用已经发生、钱已经花了，此时报错除了
// 把一次成功的回复变成失败之外没有任何用处。少扣的那部分会在下次事前探针时
// 由"桶里还有多少"自然反映——护栏的有效性来自事前那一步，不来自这一步的精确。
func (r *Recorder) ConsumeQuota(ctx context.Context, userID *int64, totalTokens int) {
	if r == nil || r.st == nil || userID == nil || totalTokens <= quotaProbeTokens {
		return
	}
	if err := r.st.TryConsumeForUser(ctx, *userID, store.QuotaLLMTokens,
		float64(totalTokens-quotaProbeTokens)); err != nil && !errors.Is(err, store.ErrQuotaExceeded) {
		slog.Warn("llm: 配额扣减失败（调用已完成，不影响结果）",
			"user_id", *userID, "tokens", totalTokens, "err", err)
	}
}
