package workflow

import (
	"errors"
	"strings"
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
// Temporal 兼容（契约 §8.2）：新增命令必须通过 replay baseline。运行授权 Activity
// 由 GetVersion 保护：历史执行重放走 DefaultVersion，不期待不存在的 Activity；新执行
// 记录版本 marker 后先授权。这个分支必须保留到所有旧历史超过 retention。
func PushPipelineWorkflow(ctx workflow.Context, p PushParams) (retErr error) {
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
	var compiledRun *CompiledRunInputV1
	var outcomeMarker *types.RunOutcomeMarkerV1
	outcomeCoverage := types.RunCompletenessPartial
	outcomeProcessing := types.RunCompletenessComplete
	var outcomeTerminal *runOutcomeTerminalV1

	// New-format scheduled Actions carry an explicit tenant and a rollout-
	// approved mode. GetVersion preserves already-started histories; the exact
	// all-zero pre-C1b envelope below also keeps the live-authorized legacy path
	// until asynchronous Action reconciliation succeeds.
	runtimeEnvelopeVersion := workflow.DefaultVersion
	if p.RunKind == PushRunKindScheduled {
		runtimeEnvelopeVersion = workflow.GetVersion(
			ctx, "scheduled-runtime-envelope-v1", workflow.DefaultVersion, 1)
	}
	// A pre-C1b durable Action has all three envelope fields at their zero
	// values. ReconcileActions upgrades it asynchronously, so a trigger may
	// legitimately freeze that old shape during deployment or after one
	// best-effort reconcile failure. Preserve the existing AuthorizeRun path for
	// that exact legacy shape; it still validates schedule_id + user_id against
	// live Postgres before any effect. Once any envelope field is present, the
	// tuple is all-or-nothing and fails closed when partial or unknown.
	hasRuntimeEnvelopeV1 := p.TenantID != 0 || p.ExecutionMode != "" || p.RuntimeVersion != ""
	if runtimeEnvelopeVersion >= 1 && hasRuntimeEnvelopeV1 {
		if p.TenantID <= 0 {
			return types.NewAppError(types.CodeValidation,
				"scheduled run is missing an explicit tenant", nil)
		}
		if p.ExecutionMode != types.ExecutionModeCompiled {
			return types.NewAppError(types.CodeValidation,
				"scheduled run execution mode must be compiled", nil)
		}
		if p.RuntimeVersion != "" && !IsCompiledRuntimeV1(p.RuntimeVersion) {
			return types.NewAppError(types.CodeValidation,
				"scheduled run runtime version is not supported", nil)
		}
	}

	// C1b: only trusted scheduled Actions with an explicit tenant enter the
	// immutable snapshot path. The command sequence is separately versioned;
	// existing histories and ad-hoc/legacy inputs keep their old sequence.
	if p.RunKind == PushRunKindScheduled && p.ExecutionMode == types.ExecutionModeCompiled &&
		IsCompiledRuntimeV1(p.RuntimeVersion) &&
		workflow.GetVersion(ctx, "compiled-run-snapshot-v1", workflow.DefaultVersion, 1) >= 1 {
		prepareCtx := workflow.WithActivityOptions(ctx, quickActivityOptions())
		var prepared PrepareRunResult
		if err := workflow.ExecuteActivity(prepareCtx, a.PrepareRun, p).Get(prepareCtx, &prepared); err != nil {
			return err
		}
		info := workflow.GetInfo(ctx).WorkflowExecution
		expected := types.RunIdentity{
			TemporalWorkflowID: info.ID,
			TemporalRunID:      info.RunID,
			RunKind:            types.RunSnapshotKindScheduled,
			TenantID:           p.TenantID,
			UserID:             p.UserID,
			TaskID:             p.ScheduleID,
		}
		if err := prepared.ValidateFor(expected); err != nil {
			return err
		}
		if !prepared.Authorized {
			log.Warn("push pipeline 未获快照运行授权，零外部副作用退出",
				"tenant_id", p.TenantID, "user_id", p.UserID,
				"schedule_id", p.ScheduleID, "trace_id", traceID)
			return nil
		}
		ref := prepared.Snapshot
		p.Snapshot = &ref
		compiledRun = &CompiledRunInputV1{
			TenantID: p.TenantID,
			TaskID:   p.ScheduleID,
			Snapshot: ref,
		}
	} else {
		// Activation safety gate: Temporal Unpause and the Postgres active mirror
		// cannot share a transaction. A run may start in that narrow window (or
		// after membership revocation), so the first DB Activity must authorize the
		// exact schedule before profile LLM, fetch, scoring, cards, or push can run.
		if workflow.GetVersion(ctx, "scheduled-run-authorization", workflow.DefaultVersion, 1) >= 1 {
			var authorized bool
			authorizeCtx := workflow.WithActivityOptions(ctx, quickActivityOptions())
			if err := workflow.ExecuteActivity(authorizeCtx, a.AuthorizeRun, p).Get(authorizeCtx, &authorized); err != nil {
				return err
			}
			if !authorized {
				log.Warn("push pipeline 未获任务运行授权，零外部副作用退出",
					"user_id", p.UserID, "schedule_id", p.ScheduleID, "trace_id", traceID)
				return nil
			}
		}
	}

	// P1-B starts only after immutable authorization succeeds, yet before
	// Evolve/Fetch/LLM/notification/push can schedule any external side effect.
	// The separate version marker leaves pre-P1-B compiled histories unchanged.
	if compiledRun != nil && HasRunOutcomeV1(p.RuntimeVersion) &&
		workflow.GetVersion(
			ctx, "run-outcome-lifecycle-v1", workflow.DefaultVersion, 1,
		) >= 1 {
		beginCtx := workflow.WithActivityOptions(ctx, quickActivityOptions())
		var marker types.RunOutcomeMarkerV1
		if err := workflow.ExecuteActivity(
			beginCtx, a.BeginRunOutcomeV1,
			RunOutcomeBeginIn{UserID: p.UserID, Run: *compiledRun},
		).Get(beginCtx, &marker); err != nil {
			return err
		}
		if err := marker.Validate(); err != nil ||
			marker.RunSnapshotID != compiledRun.Snapshot.SnapshotID ||
			marker.TenantID != compiledRun.TenantID ||
			marker.UserID != p.UserID ||
			marker.TaskID != compiledRun.TaskID {
			return types.NewAppError(
				types.CodeValidation, "run outcome marker differs from run", err)
		}
		outcomeMarker = &marker
		defer func() {
			terminal := outcomeTerminal
			if terminal == nil {
				terminal = terminalRunOutcomeForError(retErr)
			}
			processing := outcomeProcessing
			if retErr != nil && outcomeTerminal == nil {
				processing = types.RunCompletenessPartial
			}
			claim := types.RunOutcomeClaimV1{
				RunOutcomeMarkerV1: marker,
				Result:             terminal.result,
				SourceCoverage:     outcomeCoverage,
				Processing:         processing,
				FailureCode:        terminal.failureCode,
				FailureMessage:     terminal.failureMessage,
			}
			finalizeCtx, cancel := workflow.NewDisconnectedContext(ctx)
			defer cancel()
			finalizeCtx = workflow.WithActivityOptions(
				finalizeCtx, quickActivityOptions())
			err := workflow.ExecuteActivity(
				finalizeCtx, a.FinalizeRunOutcomeV1,
				RunOutcomeFinalizeIn{
					UserID: p.UserID, Run: *compiledRun, Claim: claim,
				},
			).Get(finalizeCtx, nil)
			if err != nil {
				log.Error("run outcome finalization failed",
					"snapshot_id", marker.RunSnapshotID, "err", err)
				if retErr == nil {
					retErr = err
				}
			}
		}()
	}

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
	// userTriggered：本次运行是否用户主动触发（"现在推"按钮 push-adhoc-* / agent 工具
	// push-agent-*）。定时调度的 workflow ID 是 wf-push-{schedule_id}-{ts}，不命中。
	// workflow ID 在重放中恒定，据此分支是确定性的。
	wfID := workflow.GetInfo(ctx).WorkflowExecution.ID
	userTriggered := strings.HasPrefix(wfID, "push-agent-") || strings.HasPrefix(wfID, "push-adhoc-")

	// maxScore 携带门槛上下文（仅 Select gate 过滤致空时 >=0，其余闸门传 -1）：
	// 由 workflow 从 scored 纯计算（确定性），档位由 NotifyEmptyResult 自行查库。
	recordEmpty := func(gate types.BatchExitGate, maxScore float64) {
		// 用 quick 档：纯一条 INSERT，且它无论如何都不该拖长一次"其实没事干"的运行。
		recCtx := workflow.WithActivityOptions(ctx, quickActivityOptions())
		in := RecordEmptyIn{UserID: p.UserID, ScheduleID: p.ScheduleID, TraceID: traceID, Gate: gate, Counts: counts, Run: compiledRun}
		if err := workflow.ExecuteActivity(recCtx, a.RecordEmptyBatch, in).Get(recCtx, nil); err != nil {
			log.Warn("空批次记账失败，本次仍按正常终态结束",
				"user_id", p.UserID, "trace_id", traceID, "gate", gate, "err", err)
		}
		// 用户主动触发时补一张"本次没有新内容"通知卡（2026-07-18：Boss 点了立即推送
		// 等不到任何回音来查服务器——空结果和故障在用户侧必须可区分）。
		//
		// 定时任务对 fetch/dedup 等"世界上没新东西"的空保持静默（每天"今天没新闻"
		// 是噪音）。两个例外**恒通知**，它们是同一条道理的两个实例——
		// 「系统正常但没东西给你」和「系统坏了」在用户侧长得一模一样，
		// 而分辨它们所需的信息只有服务端有：
		//   · 门槛过滤致空（Select，Boss 拍板 2026-07-19）：内容明明有、是门槛把它们
		//     全滤了。这是门槛机制的反馈面，用户须知道门槛在工作、且能调松。
		//   · 额度用尽（Quota）：系统这条路走不通了，且要人处理。它主要就发生在
		//     定时批上（定时批是最大的一笔消耗），门控恰好把最需要通知的场景全挡住；
		//     挡住的结果是早报无声消失，用户无从判断是没新闻、服务挂了、还是自己欠费。
		//
		// 失败同样只 Warn：通知是附加信息，不能把正常空终态变成失败。
		if userTriggered || gate == types.BatchExitGateSelect || gate == types.BatchExitGateQuota {
			ntCtx := workflow.WithActivityOptions(ctx, ioActivityOptions())
			nin := NotifyEmptyIn{UserID: p.UserID, TraceID: traceID, Gate: gate, Counts: counts, Run: compiledRun}
			if gate == types.BatchExitGateSelect && maxScore >= 0 {
				nin.ScheduleID = p.ScheduleID
				nin.MaxScore = maxScore
			}
			if err := workflow.ExecuteActivity(ntCtx, a.NotifyEmptyResult, nin).Get(ntCtx, nil); err != nil {
				log.Warn("空结果通知失败（不阻断）", "trace_id", traceID, "err", err)
			}
		}
	}

	// 0. EvolveProfile —— 画像演化前置步（Boss 拍板①：每次推送前批量消费反馈）。
	// 红线：演化失败永不阻断推送——重试耗尽后错误吞掉只 Warn，pipeline 照常走。
	evolveCtx := workflow.WithActivityOptions(ctx, llmActivityOptions())
	if err := workflow.ExecuteActivity(evolveCtx, a.EvolveProfile, EvolveIn{UserID: p.UserID, TraceID: traceID, Run: compiledRun}).Get(evolveCtx, nil); err != nil {
		log.Warn("画像演化失败，继续推送", "user_id", p.UserID, "trace_id", traceID, "err", err)
		if outcomeMarker != nil {
			outcomeProcessing = types.RunCompletenessPartial
		}
	}

	// 1. Fetch —— 网络 I/O。
	var items []types.ContentItem
	fetchCtx := workflow.WithActivityOptions(ctx, ioActivityOptions())
	if outcomeMarker != nil {
		var result FetchOutcomeResult
		if err := workflow.ExecuteActivity(
			fetchCtx, a.FetchOutcomeV1, p).Get(fetchCtx, &result); err != nil {
			return err
		}
		items = result.Items
		outcomeCoverage = result.SourceCoverage
	} else if err := workflow.ExecuteActivity(
		fetchCtx, a.Fetch, p).Get(fetchCtx, &items); err != nil {
		return err
	}
	counts = counts.WithFetched(len(items))
	if len(items) == 0 {
		recordEmpty(types.BatchExitGateFetch, -1)
		outcomeTerminal = quietRunOutcomeV1()
		log.Info("无新内容，pipeline 结束", "trace_id", traceID)
		return nil
	}

	// 2. Dedup —— 纯计算 + 少量查库（simhash 窗口）。
	var deduped []types.ContentItem
	dedupCtx := workflow.WithActivityOptions(ctx, quickActivityOptions())
	if err := workflow.ExecuteActivity(dedupCtx, a.Dedup, DedupIn{UserID: p.UserID, TraceID: traceID, Items: items, Run: compiledRun}).Get(dedupCtx, &deduped); err != nil {
		return err
	}
	counts = counts.WithDeduped(len(deduped))
	if len(deduped) == 0 {
		recordEmpty(types.BatchExitGateDedup, -1)
		outcomeTerminal = quietRunOutcomeV1()
		log.Info("去重后无内容，pipeline 结束", "trace_id", traceID)
		return nil
	}

	// 2.5 Observation —— 新鲜度/事件判定。旧历史没有该 Activity，
	// GetVersion 让 replay 保持原命令序列；新运行由 Activity 内的 exact-task
	// shadow/authority 路由决定是否真正过滤。
	if workflow.GetVersion(
		ctx, "observation-qualification-v1", workflow.DefaultVersion, 1,
	) >= 1 {
		qualifyCtx := workflow.WithActivityOptions(ctx, llmActivityOptions())
		var qualified QualifyEventsResult
		if err := workflow.ExecuteActivity(qualifyCtx, a.QualifyEvents, QualifyEventsIn{
			UserID: p.UserID, TraceID: traceID, ScheduleID: p.ScheduleID,
			Items: deduped, Run: compiledRun,
		}).Get(qualifyCtx, &qualified); err != nil {
			return err
		}
		counts = counts.WithQualified(len(qualified.Items))
		if len(qualified.Items) == 0 {
			gate := types.BatchExitGateObservationNoMatch
			if qualified.Outcome == "uncertain" {
				gate = types.BatchExitGateObservationUncertain
				outcomeProcessing = types.RunCompletenessPartial
			}
			recordEmpty(gate, -1)
			outcomeTerminal = quietRunOutcomeV1()
			log.Info("观察策略判定后无可推事件",
				"trace_id", traceID, "outcome", qualified.Outcome)
			return nil
		}
		deduped = qualified.Items
	}

	// 3. Score —— LLM 调用，超时给足。
	var scored []types.ScoredItem
	scoreCtx := workflow.WithActivityOptions(ctx, llmActivityOptions())
	scoreIn := ScoreIn{
		UserID: p.UserID, TraceID: traceID, Items: deduped, ScheduleID: p.ScheduleID, Run: compiledRun,
	}
	var scoreErr error
	if outcomeMarker != nil {
		var result ScoreOutcomeResult
		scoreErr = workflow.ExecuteActivity(
			scoreCtx, a.ScoreOutcomeV1, scoreIn).Get(scoreCtx, &result)
		scored = result.Items
		if result.Processing == types.RunCompletenessPartial {
			outcomeProcessing = types.RunCompletenessPartial
		}
	} else {
		scoreErr = workflow.ExecuteActivity(
			scoreCtx, a.Score, scoreIn).Get(scoreCtx, &scored)
	}
	if scoreErr != nil {
		// 额度用尽不是故障，是**正常终态**——和"没有新内容"同一类，只是原因不同。
		// 让它走 workflow 失败会：① 在 Temporal 里堆一串红色的失败记录，
		// ② 用户什么提示都收不到（失败路径不发通知），只能干等着纳闷。
		// 导向空批次 + 专属闸门，用户会收到「额度用尽、会自动恢复」的人话。
		if isQuotaFailure(scoreErr) {
			recordEmpty(types.BatchExitGateQuota, -1)
			outcomeProcessing = types.RunCompletenessPartial
			outcomeTerminal = failedRunOutcomeV1(
				string(types.CodeQuotaExceeded),
				"本租户 LLM 额度已用尽，本轮跳过打分")
			log.Info("额度用尽，本轮跳过打分", "trace_id", traceID)
			return nil
		}
		return scoreErr
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
		recordEmpty(types.BatchExitGateScore, -1)
		outcomeTerminal = quietRunOutcomeV1()
		log.Info("打分后无内容，pipeline 结束", "trace_id", traceID)
		return nil
	}

	// 4. Select —— 门槛过滤 + 排序取 TopN（契约 §6 修订）。返回类型保持裸切片
	// 是重放兼容性钉死的（见 Select 注释与 replay_test 基线）。
	topN := p.Scope.TopN
	if topN <= 0 {
		topN = defaultTopN
	}
	var selected []types.ScoredItem
	selectCtx := workflow.WithActivityOptions(ctx, quickActivityOptions())
	if err := workflow.ExecuteActivity(selectCtx, a.Select,
		SelectIn{UserID: p.UserID, TraceID: traceID, TopN: topN, Scored: scored, ScheduleID: p.ScheduleID, Run: compiledRun}).Get(selectCtx, &selected); err != nil {
		return err
	}
	// 本闸门自门槛过滤落地起是**热路径**（此前纯 TopN 时够不着，见 git 史）：
	// 整批低于任务门槛 → 全滤 → 空。上方 recordEmpty 对本闸门恒发通知卡；
	// maxScore 在 workflow 内从 scored 纯计算（确定性），空批卡靠它回答"最高才几分"。
	counts = counts.WithSelected(len(selected))
	if len(selected) == 0 {
		maxScore := 0.0
		for _, si := range scored {
			if si.Score > maxScore {
				maxScore = si.Score
			}
		}
		recordEmpty(types.BatchExitGateSelect, maxScore)
		outcomeTerminal = quietRunOutcomeV1()
		log.Info("门槛过滤后无内容，pipeline 结束", "trace_id", traceID, "max_score", maxScore)
		return nil
	}

	// 5. CardGen —— LLM 调用。
	var cards []GeneratedCard
	cardCtx := workflow.WithActivityOptions(ctx, llmActivityOptions())
	cardIn := CardGenIn{
		UserID: p.UserID, TraceID: traceID, Items: selected, ScheduleID: p.ScheduleID, Run: compiledRun,
	}
	var cardErr error
	if outcomeMarker != nil {
		var result CardGenOutcomeResult
		cardErr = workflow.ExecuteActivity(
			cardCtx, a.CardGenOutcomeV1, cardIn).Get(cardCtx, &result)
		cards = result.Cards
		if result.Processing == types.RunCompletenessPartial {
			outcomeProcessing = types.RunCompletenessPartial
		}
	} else {
		cardErr = workflow.ExecuteActivity(
			cardCtx, a.CardGen, cardIn).Get(cardCtx, &cards)
	}
	if cardErr != nil {
		if isQuotaFailure(cardErr) {
			recordEmpty(types.BatchExitGateQuota, -1)
			outcomeProcessing = types.RunCompletenessPartial
			outcomeTerminal = failedRunOutcomeV1(
				string(types.CodeQuotaExceeded),
				"本租户 LLM 额度已用尽，本轮跳过出卡")
			log.Info("额度用尽，本轮跳过出卡", "trace_id", traceID)
			return nil
		}
		return cardErr
	}
	// 同 Score：CardGen 整批全失败走 CodeLLMUnavailable 错误分支，空切片只在
	// 入参为空时出现，而 selected 刚校验非空。
	counts = counts.WithCards(len(cards))
	if len(cards) == 0 {
		recordEmpty(types.BatchExitGateCardGen, -1)
		outcomeTerminal = quietRunOutcomeV1()
		log.Info("卡片生成后无内容，pipeline 结束", "trace_id", traceID)
		return nil
	}

	// 6. Push —— 网络 I/O，主动推送飞书卡片。
	pushCtx := workflow.WithActivityOptions(ctx, ioActivityOptions())
	if err := workflow.ExecuteActivity(pushCtx, a.Push, PushIn{UserID: p.UserID, ScheduleID: p.ScheduleID, TraceID: traceID, Cards: cards, TaskTitle: p.NLDesc, Run: compiledRun}).Get(pushCtx, nil); err != nil {
		outcomeProcessing = types.RunCompletenessPartial
		if !temporal.IsCanceledError(err) {
			outcomeTerminal = contentRunOutcomeV1()
		}
		return err
	}

	outcomeTerminal = contentRunOutcomeV1()
	log.Info("push pipeline 完成", "user_id", p.UserID, "trace_id", traceID, "pushed", len(cards))
	return nil
}

type runOutcomeTerminalV1 struct {
	result         types.RunResultV1
	failureCode    string
	failureMessage string
}

func quietRunOutcomeV1() *runOutcomeTerminalV1 {
	return &runOutcomeTerminalV1{result: types.RunResultQuiet}
}

func contentRunOutcomeV1() *runOutcomeTerminalV1 {
	return &runOutcomeTerminalV1{result: types.RunResultContent}
}

func failedRunOutcomeV1(code, message string) *runOutcomeTerminalV1 {
	return &runOutcomeTerminalV1{
		result:         types.RunResultFailed,
		failureCode:    code,
		failureMessage: boundedOutcomeFailureMessageV1(message),
	}
}

func terminalRunOutcomeForError(err error) *runOutcomeTerminalV1 {
	if err == nil {
		return failedRunOutcomeV1(
			"outcome_missing_terminal_receipt",
			"workflow completed without a terminal outcome receipt")
	}
	if temporal.IsCanceledError(err) {
		return &runOutcomeTerminalV1{
			result:         types.RunResultInterrupted,
			failureCode:    "workflow_canceled",
			failureMessage: "workflow was canceled",
		}
	}
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) && knownOutcomeFailureCodeV1(appErr.Type()) {
		return failedRunOutcomeV1(appErr.Type(), appErr.Message())
	}
	return failedRunOutcomeV1(
		"workflow_failed", "workflow failed before a reliable terminal result")
}

func knownOutcomeFailureCodeV1(code string) bool {
	switch types.ErrCode(code) {
	case types.CodeNotFound, types.CodeConflict, types.CodeValidation,
		types.CodeDatabase, types.CodeInternal, types.CodeLLMRateLimit,
		types.CodeLLMBadRequest, types.CodeLLMUnavailable,
		types.CodeQuotaExceeded, types.CodeFetchTimeout,
		types.CodeFetchRateLimit, types.CodePushFailed,
		types.CodeDBDeadlock, types.CodeDBConnLost, types.CodeDBConstraint:
		return true
	default:
		return false
	}
}

func boundedOutcomeFailureMessageV1(message string) string {
	const maxBytes = 4096
	if len(message) <= maxBytes {
		return message
	}
	cut := maxBytes
	for cut > 0 && message[cut]&0xc0 == 0x80 {
		cut--
	}
	return message[:cut]
}

// isQuotaFailure 判定 activity 错误是否为额度用尽。
//
// 跨 activity 边界后错误已被 Temporal 包成 ApplicationError，原始类型丢失，
// 只剩 Type 字符串——而 nonRetryable 正是把 AppError.Code 放进 Type 的。
// 所以这里比对的是 Type，不是 errors.As。
func isQuotaFailure(err error) bool {
	var ae *temporal.ApplicationError
	return errors.As(err, &ae) && ae.Type() == string(types.CodeQuotaExceeded)
}

// nonRetryableCodes 是确定性失败错误码：这类错误重试无意义（重试只是重复失败），
// 交给 Temporal 的 NonRetryableErrorTypes 直接终止。要生效需 Activity 侧把
// AppError 的 Code 作为 ApplicationError 的 Type 抛出（见报告"待主控复核"）。
var nonRetryableCodes = []string{
	// 额度按秒缓慢补充，而 Temporal 的重试在秒级内完成——重试三次只会失败三次，
	// 白白制造噪音，还把"额度用尽"这个明确原因埋进一串重试错误里。
	string(types.CodeQuotaExceeded),
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

// retryableOrNot preserves a controlled AppError code as Temporal's
// ApplicationError Type on both retryable and permanent paths. Retryability is
// still governed by NonRetryable, while terminal RunOutcome mapping can now
// retain the exact sanitized code after retries are exhausted.
func retryableOrNot(err error) error {
	if err == nil {
		return nil
	}
	var ae *types.AppError
	if errors.As(err, &ae) {
		return temporal.NewApplicationErrorWithOptions(
			ae.Message, string(ae.Code),
			temporal.ApplicationErrorOptions{
				NonRetryable: !ae.Retryable,
				Cause:        err,
			},
		)
	}
	return err
}

// ioActivityOptions 用于网络 I/O 型 Activity（Fetch/Push）。
// 120s：Fetch 串行抓多源、单源超时兜底 20s（wechat_mp 模板声明 30s，2026-07-23 起），
// 60s 预算 3 个慢源就撞墙（审查 #串行预算）；多源接入后源数增长，放宽到 120s。
// 按最坏 30s 推算 4 个慢公众号源即打满——当前源数下余量足，源数增长时再议提额或
// 并行化。配合 due 过滤（已成功源不重抓），重试也不重复计费。
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
