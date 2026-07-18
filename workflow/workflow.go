package workflow

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/YouToco/vane/types"
)

// PushPipelineWorkflow 是推送管道的编排：EvolveProfile 前置步（失败只 Warn 不阻断）
// 之后 Fetch→Dedup→Score→Select→CardGen→Push 六步顺序执行，每步一个 Activity。
// 函数体只做编排与纯计算，所有 I/O 在 Activity 内（Temporal 确定性约束）。
// 六主步任一失败即整体失败（由各步 RetryPolicy 先行重试）；
// 中途"无内容可推"是正常终态，直接成功返回、不视为错误。
//
// 五处"无内容可推"的提前退出现在各留一行空批次（009 / 契约 §16 修订记录
// 「空批次缺口」）：此前它们全部静默 return nil，push_batches 零行，五种语义
// 完全不同的结局在库里塌缩成同一个"什么都没有"——"今早为什么没推送"这件事
// 不是查询查不到，是数据不存在。记账仍不改变终态：**空批次依然是成功**。
//
// Temporal 兼容（契约 §8.2）：EvolveProfile 插入、Push 入参变更、以及本次在五处
// 闸门插入 RecordEmptyBatch，对 in-flight workflow 都是非确定性变更（重放到闸门处
// 会发现历史里没有这个 Activity）。沿用既有先例：推送是秒级短工作流，发布窗口
// 避开 08:30 定时任务即可，刻意不做版本化（不引入 workflow.GetVersion——
// 版本分支一旦种下就永远删不掉，为一个秒级工作流背这个包袱不划算）。
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

	// counts 是漏斗快照，每步拿到结果后立刻累加（纯计算，确定性无碍）。
	// 它让"抓到 20 条但全被去重掉"与"压根没抓到新内容"在库里可区分——两者此前
	// 都只是"没有行"。未跑到的阶段保持 nil，不是 0（见 types.PipelineCounts：
	// "没跑"和"跑了得 0"是两种不同的事故，用零值记录等于重造一次本 PR 要修的混淆）。
	var counts types.PipelineCounts

	// recordEmpty 把一次"跑完了但没东西可推"落库。闭包而非方法：Activities 的方法
	// 值只用于解析 Activity 名，编排逻辑属于 workflow 函数体。
	//
	// 吞错是**刻意的**，与下方 EvolveProfile 步骤同一条原则（其 log.Warn 失败只提示不阻断）：
	// 记账是给人看的附加信息，而"无内容可推是正常终态"是产品语义（红线）。让一次记账
	// 失败把一个本来正常的空批次变成 workflow 失败，等于为了记录这件事而破坏这件事
	// 本身——那会制造出比"库里没行"更坏的东西：一条假的失败告警。
	//
	// 下面把原始 err 整个塞进 log.Warn 是**有意的，且没有脱敏**——别以为这里处理过。
	// 红线 3（错误卫生）管的是**用户文案与模型上下文**这两个出口，日志不在其列：
	// 排障恰恰需要完整错误链，剥成 AppError.Message 只会让日志变成"记账失败了"这种
	// 废话。同款先例：下方 EvolveProfile 步骤的 log.Warn、cmd/gate 里 config.Load 失败的 slog.Error。
	// 真正受红线 3 约束的是 activities.go 的 retryableOrNot——它只放 AppError.Message
	// 进 ApplicationError，那才是会被展示/喂进上下文的那一层（activities_test.go 有钉）。
	recordEmpty := func(gate types.BatchExitGate) {
		// 用 quick 档：纯一条 INSERT，且它无论如何都不该拖长一次"其实没事干"的运行。
		recCtx := workflow.WithActivityOptions(ctx, quickActivityOptions())
		in := RecordEmptyIn{UserID: p.UserID, TraceID: traceID, Gate: gate, Counts: counts}
		if err := workflow.ExecuteActivity(recCtx, a.RecordEmptyBatch, in).Get(recCtx, nil); err != nil {
			log.Warn("空批次记账失败，本次仍按正常终态结束",
				"user_id", p.UserID, "trace_id", traceID, "gate", gate, "err", err)
		}
	}

	// 0. EvolveProfile —— 画像演化前置步（Boss 拍板①：每次推送前批量消费反馈）。
	// 红线：演化失败永不阻断推送——重试耗尽后错误吞掉只 Warn，pipeline 照常走。
	evolveCtx := workflow.WithActivityOptions(ctx, llmActivityOptions())
	if err := workflow.ExecuteActivity(evolveCtx, a.EvolveProfile, EvolveIn{UserID: p.UserID, TraceID: traceID}).Get(evolveCtx, nil); err != nil {
		log.Warn("画像演化失败，继续推送", "user_id", p.UserID, "trace_id", traceID, "err", err)
	}

	// 1. Fetch —— 网络 I/O。
	var items []types.ContentItem
	fetchCtx := workflow.WithActivityOptions(ctx, ioActivityOptions())
	if err := workflow.ExecuteActivity(fetchCtx, a.Fetch, p).Get(fetchCtx, &items); err != nil {
		return err
	}
	counts = counts.WithFetched(len(items))
	if len(items) == 0 {
		recordEmpty(types.BatchExitGateFetch)
		log.Info("无新内容，pipeline 结束", "trace_id", traceID)
		return nil
	}

	// 2. Dedup —— 纯计算 + 少量查库（simhash 窗口）。
	var deduped []types.ContentItem
	dedupCtx := workflow.WithActivityOptions(ctx, quickActivityOptions())
	if err := workflow.ExecuteActivity(dedupCtx, a.Dedup, DedupIn{UserID: p.UserID, TraceID: traceID, Items: items}).Get(dedupCtx, &deduped); err != nil {
		return err
	}
	counts = counts.WithDeduped(len(deduped))
	if len(deduped) == 0 {
		recordEmpty(types.BatchExitGateDedup)
		log.Info("去重后无内容，pipeline 结束", "trace_id", traceID)
		return nil
	}

	// 3. Score —— LLM 调用，超时给足。
	var scored []types.ScoredItem
	scoreCtx := workflow.WithActivityOptions(ctx, llmActivityOptions())
	if err := workflow.ExecuteActivity(scoreCtx, a.Score, ScoreIn{UserID: p.UserID, TraceID: traceID, Items: deduped}).Get(scoreCtx, &scored); err != nil {
		return err
	}
	// 本闸门当前**够不着**（core review 时核实，不是猜的）：Score 只在
	// len(in.Items)==0 时返回空切片，而 in.Items 就是上面刚校验过非空的 deduped；
	// 整批打分全失败走的是 activities.go 的 CodeLLMUnavailable 错误分支，不是空切片。
	// Select/CardGen 两个闸门同理（各自注释另有说明）。
	//
	// 那为什么还要接线而不是删掉它？因为"够不着"是**下游活动当前实现**的性质，
	// 不是编排的约定：Score 一旦加上"低于阈值不推"的过滤（M6 很可能要做），
	// 这个闸门立刻变成热路径。闸门本身是廉价的防御，删了它 = 把"打分后没东西"
	// 重新变回静默 return nil ——正是本 PR 在修的那个洞。接线让它在被走到的**第一天**
	// 就有记录，而不是等到出事后再回来补。
	counts = counts.WithScored(len(scored))
	if len(scored) == 0 {
		recordEmpty(types.BatchExitGateScore)
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
	// 同样够不着：selector.RankTopN 只在 n<=0 或输入为空时返回空切片
	// （selector.go:65-67），而 topN 上面刚归一到 >=1、scored 刚校验非空。
	counts = counts.WithSelected(len(selected))
	if len(selected) == 0 {
		recordEmpty(types.BatchExitGateSelect)
		log.Info("择优后无内容，pipeline 结束", "trace_id", traceID)
		return nil
	}

	// 5. CardGen —— LLM 调用。
	var cards []GeneratedCard
	cardCtx := workflow.WithActivityOptions(ctx, llmActivityOptions())
	if err := workflow.ExecuteActivity(cardCtx, a.CardGen, CardGenIn{UserID: p.UserID, TraceID: traceID, Items: selected}).Get(cardCtx, &cards); err != nil {
		return err
	}
	// 同 Score：CardGen 整批全失败走 CodeLLMUnavailable 错误分支，空切片只在
	// 入参为空时出现，而 selected 刚校验非空。
	counts = counts.WithCards(len(cards))
	if len(cards) == 0 {
		recordEmpty(types.BatchExitGateCardGen)
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
