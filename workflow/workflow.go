package workflow

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/YouToco/vane/types"
)

// PushPipelineWorkflow 是推送管道的编排：Fetch→Dedup→Score→Select→CardGen→Push
// 六步顺序执行，每步一个 Activity。函数体只做编排与纯计算，所有 I/O 在 Activity 内
// （Temporal 确定性约束）。任一步失败即整体失败（由各步 RetryPolicy 先行重试）；
// 中途"无内容可推"是正常终态，直接成功返回、不视为错误。
func PushPipelineWorkflow(ctx workflow.Context, p PushParams) error {
	log := workflow.GetLogger(ctx)

	// traceID 必须确定性生成：workflow 内直接 uuid.New() 是非确定性副作用，
	// 重放（replay）时会得到不同值破坏历史一致性。SideEffect 把首次生成结果
	// 固化进事件历史，重放时回放同一值。它贯穿 Score/CardGen 的 llm 记账。
	var traceID string
	if err := workflow.SideEffect(ctx, func(workflow.Context) any {
		return uuid.NewString()
	}).Get(&traceID); err != nil {
		return err
	}
	log.Info("push pipeline 开始", "user_id", p.UserID, "trace_id", traceID)

	// a 仅用于让 ExecuteActivity 通过方法值解析出 Activity 名字；Temporal 按名
	// 路由到 worker 注册的真实实例，nil 接收者不会被解引用（标准用法）。
	var a *Activities

	// 1. Fetch —— 网络 I/O。
	var items []types.ContentItem
	fetchCtx := workflow.WithActivityOptions(ctx, ioActivityOptions())
	if err := workflow.ExecuteActivity(fetchCtx, a.Fetch, p).Get(fetchCtx, &items); err != nil {
		return err
	}
	if len(items) == 0 {
		log.Info("无新内容，pipeline 结束", "trace_id", traceID)
		return nil
	}

	// 2. Dedup —— 纯计算 + 少量查库（simhash 窗口）。
	var deduped []types.ContentItem
	dedupCtx := workflow.WithActivityOptions(ctx, quickActivityOptions())
	if err := workflow.ExecuteActivity(dedupCtx, a.Dedup, DedupIn{UserID: p.UserID, TraceID: traceID, Items: items}).Get(dedupCtx, &deduped); err != nil {
		return err
	}
	if len(deduped) == 0 {
		log.Info("去重后无内容，pipeline 结束", "trace_id", traceID)
		return nil
	}

	// 3. Score —— LLM 调用，超时给足。
	var scored []types.ScoredItem
	scoreCtx := workflow.WithActivityOptions(ctx, llmActivityOptions())
	if err := workflow.ExecuteActivity(scoreCtx, a.Score, ScoreIn{UserID: p.UserID, TraceID: traceID, Items: deduped}).Get(scoreCtx, &scored); err != nil {
		return err
	}
	if len(scored) == 0 {
		log.Info("打分后无内容，pipeline 结束", "trace_id", traceID)
		return nil
	}

	// 4. Select —— 纯排序取 TopN。
	topN := p.Scope.TopN
	if topN <= 0 {
		topN = defaultTopN
	}
	var selected []types.ScoredItem
	selectCtx := workflow.WithActivityOptions(ctx, quickActivityOptions())
	if err := workflow.ExecuteActivity(selectCtx, a.Select, SelectIn{UserID: p.UserID, TraceID: traceID, TopN: topN, Scored: scored}).Get(selectCtx, &selected); err != nil {
		return err
	}
	if len(selected) == 0 {
		log.Info("择优后无内容，pipeline 结束", "trace_id", traceID)
		return nil
	}

	// 5. CardGen —— LLM 调用。
	var cards []GeneratedCard
	cardCtx := workflow.WithActivityOptions(ctx, llmActivityOptions())
	if err := workflow.ExecuteActivity(cardCtx, a.CardGen, CardGenIn{UserID: p.UserID, TraceID: traceID, Items: selected}).Get(cardCtx, &cards); err != nil {
		return err
	}
	if len(cards) == 0 {
		log.Info("卡片生成后无内容，pipeline 结束", "trace_id", traceID)
		return nil
	}

	// 6. Push —— 网络 I/O，主动推送飞书卡片。
	pushCtx := workflow.WithActivityOptions(ctx, ioActivityOptions())
	if err := workflow.ExecuteActivity(pushCtx, a.Push, PushIn{UserID: p.UserID, TraceID: traceID, Cards: cards}).Get(pushCtx, nil); err != nil {
		return err
	}

	log.Info("push pipeline 完成", "user_id", p.UserID, "trace_id", traceID, "pushed", len(cards))
	return nil
}

// Register 把 workflow 与全部 Activity 注册进 worker。注册结构体实例即注册其
// 所有导出方法（Fetch/Dedup/…/Push），与 workflow 内 a.Fetch 的名字一一对应。
// 供 cmd/server 装配时调用，避免各处重复逐个注册漏掉某个 Activity。
func Register(w worker.Registry, a *Activities) {
	w.RegisterWorkflow(PushPipelineWorkflow)
	w.RegisterActivity(a)
}

// nonRetryableCodes 是确定性失败错误码：这类错误重试无意义（重试只是重复失败），
// 交给 Temporal 的 NonRetryableErrorTypes 直接终止。要生效需 Activity 侧把
// AppError 的 Code 作为 ApplicationError 的 Type 抛出（见报告"待主控复核"）。
var nonRetryableCodes = []string{
	string(types.CodeValidation),
	string(types.CodeNotFound),
	string(types.CodeConflict),
	string(types.CodeLLMBadRequest),
	string(types.CodeDBConstraint),
}

// nonRetryable 把确定性失败的 AppError 包成 Temporal ApplicationError，并把
// AppError.Code 作为 ApplicationError 的 Type。为什么必须这一层：Temporal 的
// NonRetryableErrorTypes 是按错误的 Type 字符串匹配的，而 Activity 直接返回的
// 裸 *AppError，其 error.Error() 文本才是 Type（形如 "NOT_FOUND: ..."），
// 永远匹配不到 nonRetryableCodes 里的纯 Code——于是"不可重试"的错误仍被重试到上限。
// 经此包装后 Type 恰为 string(Code)，NonRetryableErrorTypes 才真正生效、立即终止。
// 签名依 SDK v1.46.0 的 temporal.NewApplicationErrorWithOptions(msg, errType, opts) 编译验证。
func nonRetryable(err error) error {
	var ae *types.AppError
	if errors.As(err, &ae) {
		return temporal.NewApplicationErrorWithOptions(ae.Message, string(ae.Code),
			temporal.ApplicationErrorOptions{NonRetryable: true, Cause: err})
	}
	return err
}

// retryableOrNot 依据 types.IsRetryable 自动决定是否包成不可重试错误：
// 可重试的错误原样返回（交给 RetryPolicy 正常退避重试），确定性失败的走 nonRetryable。
// 用于 Activity 里"错误码不确定但想统一交给策略判定"的返回点。
func retryableOrNot(err error) error {
	if err == nil {
		return nil
	}
	if types.IsRetryable(err) {
		return err
	}
	return nonRetryable(err)
}

// ioActivityOptions 用于网络 I/O 型 Activity（Fetch/Push）。
// 120s：Fetch 串行抓多源、单源超时兜底 20s，60s 预算 3 个慢源就撞墙
// （审查 #串行预算）；多源接入后源数增长，放宽到 120s。配合 due 过滤
// （已成功源不重抓），重试也不重复计费。真正的并发扇出留到源数上百再做。
func ioActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: 120 * time.Second,
		RetryPolicy:         defaultRetryPolicy(),
	}
}

// llmActivityOptions 用于 LLM 型 Activity（Score/CardGen）：生成耗时波动大，超时给足。
func llmActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: 120 * time.Second,
		RetryPolicy:         defaultRetryPolicy(),
	}
}

// quickActivityOptions 用于纯计算型 Activity（Dedup/Select）：短超时、少重试。
func quickActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2.0,
			MaximumInterval:        30 * time.Second,
			MaximumAttempts:        2, // int32
			NonRetryableErrorTypes: nonRetryableCodes,
		},
	}
}

// defaultRetryPolicy 是 I/O / LLM 步骤的默认重试策略。MaximumAttempts 是 int32。
func defaultRetryPolicy() *temporal.RetryPolicy {
	return &temporal.RetryPolicy{
		InitialInterval:        time.Second,
		BackoffCoefficient:     2.0,
		MaximumInterval:        100 * time.Second,
		MaximumAttempts:        3, // int32
		NonRetryableErrorTypes: nonRetryableCodes,
	}
}
