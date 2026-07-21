package llm

import (
	"context"
	"log/slog"
	"time"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

// Recorder 负责把每次 LLM 调用同步写入 llm_calls 表。
// 单独成类型而不是让 Client 直接持有 store：调用与记账解耦，
// 单测 Client 时不需要数据库。
type Recorder struct {
	st recorderStore
}

type recorderStore interface {
	InsertLLMCall(context.Context, *types.LLMCall) (int64, error)
	TryConsumeForUser(context.Context, int64, store.QuotaBucket, float64) error
	AdjustForUser(context.Context, int64, store.QuotaBucket, float64) error
}

// NewRecorder 构造记账器。
func NewRecorder(st *store.Store) *Recorder {
	// Do not box a typed nil *store.Store into recorderStore: a non-nil
	// interface would bypass every nil guard and panic on the first call.
	if st == nil {
		return &Recorder{}
	}
	return &Recorder{st: st}
}

// postCallAccountingTimeout bounds the detached durability tail after an LLM
// request. The caller context may already be canceled, but bare WithoutCancel
// would let a database stall keep a Temporal Activity alive past worker stop.
const postCallAccountingTimeout = 10 * time.Second

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

// ============================================================
// 配额（契约 §2.7）：事前按估算预扣、事后按实际对账
// ============================================================
//
// 为什么不能只在事后扣：LLM 的用量要调用完才知道，而**纯事后扣减拦不住任何东西**——
// 它没有"调用前"这个决策点，桶空了也只是继续记账，第一次超支之后每一次都是超支。
//
// 为什么不能只在事前扣一个象征值（本文件的前一版就是这么做的，扣 1 个令牌）：
// 那样桶几乎永远扣不空。2026-07-19 审查用真库实测出它的威力——余额一旦低于
// 单次用量，事后的全或无扣减每次都失败并被静默丢弃，桶只减少那 1 个探针令牌，
// 而按秒补充的速率远快于此，**桶不降反升**，放行 4.9 倍日额度且无上界。
//
// 现在的做法：
//   事前 —— 按 estimateTokens(prompt 字符数 + MaxTokens) 预扣，全或无。
//            预扣是真扣，所以并发调用会互相看见，扣不动就拒绝。
//   事后 —— 按 (估算 - 实际) 做**无条件有符号**调整：高估退还、低估补扣（可扣成负数）。
//
// 关键性质：**估算不准只影响收敛快慢，不影响正确性**。因为事后那一步是无条件的，
// 真实用量最终一定被如实记进桶里；估算偏差只决定它是在调用前还是调用后被反映。
// 这让估算函数可以粗糙而不必精确——精确估算需要 tokenizer，而 tokenizer 会随模型
// 漂移，漂移的表现是"配额悄悄变松"，比粗糙可怕得多。

// 估算参数。取值依据是 2026-07-19 的生产实测（7 天 llm_calls）：
//
//	span            调用数   prompt 均/最大    completion 均/最大
//	score            751      390 / 1064          3 / 16
//	cardgen          169      428 / 1005        110 / 400
//	agent            156     4381 / 44871        75 / 423
//	profile_evolve    10      901 / 1183        196 / 287
//
// agent（走 DoChat）的 prompt 是打分的 10 倍、峰值 42 倍——多轮对话把历史累积进
// prompt。这正是配额必须覆盖 DoChat 的原因，而第一版漏了它。
const (
	// runesPerToken 按 1 个字符 ≈ 1 个 token 估算。对中文接近真实，对英文**高估**
	// （英文约 4 字符/token）——高估是安全方向：预扣多了会在事后立刻退还，
	// 而低估只是让欠账晚一步被记录。
	runesPerToken = 1.0
	// defaultCompletionEstimate 是 MaxTokens 未设时对输出长度的估算。
	// 生产实测最大 completion 为 508，取 1024 留一倍余量。
	defaultCompletionEstimate = 1024
	// minEstimate 是估算下限。防的是"prompt 极短且 MaxTokens 很小"时预扣趋近于 0，
	// 退化成前一版那个拦不住任何东西的象征性探针。
	minEstimate = 64
)

// estimateTokens 估算一次调用的总 token 数（prompt + completion）。
func estimateTokens(promptRunes int, maxTokens *int) float64 {
	completion := defaultCompletionEstimate
	if maxTokens != nil && *maxTokens > 0 {
		completion = *maxTokens
	}
	return max(float64(promptRunes)/runesPerToken+float64(completion), minEstimate)
}

// CheckQuota 在调用前按估算预扣额度。
// 系统级调用（UserID 为 nil）不计入任何租户，直接放行。
//
// 返回 store.ErrQuotaExceeded 表示额度不足，调用方**必须终止调用**——
// 这是整个配额系统唯一真正拦得住花钱的地方。
func (r *Recorder) CheckQuota(ctx context.Context, userID *int64, estimate float64) error {
	if r == nil || r.st == nil || userID == nil {
		return nil
	}
	return r.st.TryConsumeForUser(ctx, *userID, store.QuotaLLMTokens, estimate)
}

// ReconcileQuota 在调用后把预扣的估算换成实际用量。
//
// **失败只记日志、不影响调用结果**：调用已经发生、钱已经花了，此时报错除了
// 把一次成功的回复变成失败之外没有任何用处。
func (r *Recorder) ReconcileQuota(ctx context.Context, userID *int64, estimate float64, actualTokens int) {
	if r == nil || r.st == nil || userID == nil {
		return
	}
	// delta > 0 = 高估，退还；delta < 0 = 低估，补扣（可把余额扣成负数=欠账）。
	if err := r.st.AdjustForUser(ctx, *userID, store.QuotaLLMTokens, estimate-float64(actualTokens)); err != nil {
		slog.Warn("llm: 配额对账失败（调用已完成，不影响结果）",
			"user_id", *userID, "estimate", estimate, "actual", actualTokens, "err", err)
	}
}

// finishCallAccounting records the call and reconciles quota under one bounded
// context detached from the already-finished request. Sharing one deadline
// prevents two sequential 10s tails while preserving failure/cancel accounting.
func (r *Recorder) finishCallAccounting(ctx context.Context, call *types.LLMCall, userID *int64, estimate float64, actualTokens int) {
	r.finishCallAccountingWithin(ctx, call, userID, estimate, actualTokens, postCallAccountingTimeout)
}

func (r *Recorder) finishCallAccountingWithin(ctx context.Context, call *types.LLMCall, userID *int64, estimate float64, actualTokens int, timeout time.Duration) {
	tailCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	r.Record(tailCtx, call)
	r.ReconcileQuota(tailCtx, userID, estimate, actualTokens)
}
