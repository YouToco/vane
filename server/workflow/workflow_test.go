package workflow

import (
	"context"
	"errors"
	"fmt"
	"go.temporal.io/sdk/client"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/YouToco/vane/types"
)

// 本文件测的是**编排**：各处"无内容可推"闸门各自留下一行空批次（009 /
// 契约 §16 修订记录「空批次缺口」），且记账失败不改变正常终态。
//
// 为什么用桩替换全部 Activity，而不是注入 fake 依赖跑真 Activity：
// 其中**有三个（score/select/cardgen）用真 Activity 根本走不到**——
// Score/CardGen 整批失败时返回的是 CodeLLMUnavailable 错误而非空切片
// （activities.go 的 `len(x)==0 && len(in.Items)>0` 分支），selector.RankTopN
// 在 n>=1 且输入非空时恒返回非空（selector.go:65-67）。它们是**防御性**闸门：
// 一旦 Score 加上"低于阈值不推"这类过滤（M6 很可能要做）就立刻变成热路径。
// 桩正是为此存在——它钉住的是"编排层看到空切片就必须记账"这条约定，
// 与下游活动今天碰巧能不能吐出空切片解耦。真 Activity 的行为由
// activities_test.go 另测，分层如此。
//
// 桩用名字注册（activity.RegisterOptions{Name:...}）而非注册 *Activities 结构体：
// workflow 侧 `a.Fetch` 解析出的就是方法名 "Fetch"，Temporal 按名路由，故同名桩
// 可完整顶替。此路不引入 testify（当前仅是 go.mod 里的间接依赖，不因测试而升级）。

// gateOut 是各步桩要吐出的返回值（摆出想要的闸门形状）。
// 与 gateStubs 分开，是为了让用例表能安全按值拷贝——gateStubs 带锁，
// 放进表里再 range 就是 copylocks（go vet 会拦，且拷出来的锁本就没意义）。
type gateOut struct {
	items          []types.ContentItem
	deduped        []types.ContentItem
	qualifyEmpty   bool
	qualifyOutcome string
	scored         []types.ScoredItem
	selected       []types.ScoredItem
	cards          []GeneratedCard

	recErr error // 非 nil = 模拟记账活动失败
}

// gateStubs 是全套 Activity 桩：按 out 吐结果，并记录 RecordEmptyBatch / Push
// 实际收到了什么。带锁：Temporal 测试环境在独立 goroutine 上执行 Activity，
// 断言侧读取必须与执行侧写入互斥（与 activities_test.go 的替身同款纪律）。
type gateStubs struct {
	out gateOut

	mu       sync.Mutex
	recorded []RecordEmptyIn
	pushed   []PushIn
	notified []NotifyEmptyIn // NotifyEmptyResult 实际收到的入参（select 闸门恒通知断言用）
}

func (g *gateStubs) register(env *testsuite.TestWorkflowEnvironment) {
	reg := func(name string, fn any) {
		env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
	reg("AuthorizeRun", func(context.Context, PushParams) (bool, error) { return true, nil })
	reg("EvolveProfile", func(context.Context, EvolveIn) error { return nil })
	reg("Fetch", func(context.Context, PushParams) ([]types.ContentItem, error) { return g.out.items, nil })
	reg("Dedup", func(context.Context, DedupIn) ([]types.ContentItem, error) { return g.out.deduped, nil })
	reg("QualifyEvents", func(_ context.Context, in QualifyEventsIn) (QualifyEventsResult, error) {
		if g.out.qualifyEmpty {
			return QualifyEventsResult{Outcome: g.out.qualifyOutcome}, nil
		}
		return QualifyEventsResult{Items: in.Items, Outcome: "not_configured"}, nil
	})
	reg("Score", func(context.Context, ScoreIn) ([]types.ScoredItem, error) { return g.out.scored, nil })
	reg("Select", func(context.Context, SelectIn) ([]types.ScoredItem, error) { return g.out.selected, nil })
	reg("CardGen", func(context.Context, CardGenIn) ([]GeneratedCard, error) { return g.out.cards, nil })
	reg("Push", func(_ context.Context, in PushIn) error {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.pushed = append(g.pushed, in)
		return nil
	})
	reg("RecordEmptyBatch", func(_ context.Context, in RecordEmptyIn) error {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.recorded = append(g.recorded, in)
		return g.out.recErr
	})
	// 此前刻意不注册：非用户触发的路径永远走不到它，未注册的失败被 workflow 吞掉
	// 恰好证明这一点。门槛过滤落地后 select 闸门**恒通知**（含定时触发），
	// 桩转为记录入参供断言——"恒通知"不能再靠"没注册也没炸"来间接证明。
	reg("NotifyEmptyResult", func(_ context.Context, in NotifyEmptyIn) error {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.notified = append(g.notified, in)
		return nil
	})
}

func (g *gateStubs) snapshot() (recorded []RecordEmptyIn, pushed []PushIn) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]RecordEmptyIn(nil), g.recorded...), append([]PushIn(nil), g.pushed...)
}

func (g *gateStubs) notifiedCalls() []NotifyEmptyIn {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]NotifyEmptyIn(nil), g.notified...)
}

func TestPushPipelineWorkflow_UnauthorizedScheduleStopsBeforeAllSideEffects(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	g := gateStubs{out: gateOut{items: []types.ContentItem{{ID: 1}}}}
	g.register(env)
	var evolveCalls atomic.Int32
	var fetchCalls atomic.Int32
	env.RegisterActivityWithOptions(
		func(context.Context, PushParams) (bool, error) { return false, nil },
		activity.RegisterOptions{Name: "AuthorizeRun"},
	)
	env.RegisterActivityWithOptions(
		func(context.Context, EvolveIn) error { evolveCalls.Add(1); return nil },
		activity.RegisterOptions{Name: "EvolveProfile"},
	)
	env.RegisterActivityWithOptions(
		func(context.Context, PushParams) ([]types.ContentItem, error) {
			fetchCalls.Add(1)
			return g.out.items, nil
		},
		activity.RegisterOptions{Name: "Fetch"},
	)

	env.ExecuteWorkflow(PushPipelineWorkflow, PushParams{
		UserID: 7, ScheduleID: "provisioning-task",
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unauthorized run should converge normally: %v", err)
	}
	recorded, pushed := g.snapshot()
	if evolveCalls.Load() != 0 || fetchCalls.Load() != 0 ||
		len(recorded) != 0 || len(pushed) != 0 {
		t.Fatalf("unauthorized run leaked side effects: evolve=%d fetch=%d record=%d push=%d",
			evolveCalls.Load(), fetchCalls.Load(), len(recorded), len(pushed))
	}
}

// countsStr 把漏斗渲染成可读串，**nil 字段直接不出现**——这正是要断言的语义边界：
// "scored 缺席"（没跑）与 "scored=0"（跑了得 0）必须是两个不同的串，
// 用 reflect.DeepEqual 比指针虽然也对，但失败信息是一堆地址，读不出差在哪。
func countsStr(c types.PipelineCounts) string {
	var parts []string
	add := func(name string, v *int) {
		if v != nil {
			parts = append(parts, fmt.Sprintf("%s=%d", name, *v))
		}
	}
	add("fetched", c.Fetched)
	add("deduped", c.Deduped)
	add("qualified", c.Qualified)
	add("scored", c.Scored)
	add("selected", c.Selected)
	add("cards", c.Cards)
	return strings.Join(parts, " ")
}

func items(n int) []types.ContentItem {
	out := make([]types.ContentItem, n)
	for i := range out {
		out[i] = types.ContentItem{ID: int64(i + 1), Title: fmt.Sprintf("t%d", i)}
	}
	return out
}

func scoredItems(n int) []types.ScoredItem {
	out := make([]types.ScoredItem, n)
	for i := range out {
		out[i] = types.ScoredItem{Item: types.ContentItem{ID: int64(i + 1)}, Score: 80}
	}
	return out
}

func cardsOf(n int) []GeneratedCard {
	out := make([]GeneratedCard, n)
	for i := range out {
		out[i] = GeneratedCard{Scored: types.ScoredItem{Item: types.ContentItem{ID: int64(i + 1)}}, BodyMD: "md"}
	}
	return out
}

// TestPushPipelineWorkflow_EmptyBatchExitGates 各闸门的退出路径：
// 恰好记一行空批次、闸门对得上、漏斗停在该停的地方、且 workflow 仍是成功终态。
func TestPushPipelineWorkflow_EmptyBatchExitGates(t *testing.T) {
	tests := []struct {
		name       string
		out        gateOut
		wantGate   types.BatchExitGate
		wantCounts string
	}{
		{
			// 压根没抓到新内容。fetched=0 是"抓取跑了、返回 0 条"；
			// 下游一步没跑，故漏斗到此为止。
			name:       "fetch 闸门：无新内容",
			out:        gateOut{},
			wantGate:   types.BatchExitGateFetch,
			wantCounts: "fetched=0",
		},
		{
			// 本 PR 的招牌用例：抓到 20 条但全被去重掉。与上面那条在 009 之前
			// 库里完全同形（都是零行），现在靠 fetched=20 vs fetched=0 一眼分开。
			name:       "dedup 闸门：抓到 20 条但全是重复",
			out:        gateOut{items: items(20)},
			wantGate:   types.BatchExitGateDedup,
			wantCounts: "fetched=20 deduped=0",
		},
		{
			name: "observation 闸门：本周期无新事件",
			out: gateOut{
				items: items(20), deduped: items(18),
				qualifyEmpty: true, qualifyOutcome: "no_match",
			},
			wantGate:   types.BatchExitGateObservationNoMatch,
			wantCounts: "fetched=20 deduped=18 qualified=0",
		},
		{
			name: "observation 闸门：证据不足",
			out: gateOut{
				items: items(20), deduped: items(18),
				qualifyEmpty: true, qualifyOutcome: "uncertain",
			},
			wantGate:   types.BatchExitGateObservationUncertain,
			wantCounts: "fetched=20 deduped=18 qualified=0",
		},
		{
			name:       "score 闸门：打分后无内容",
			out:        gateOut{items: items(20), deduped: items(18)},
			wantGate:   types.BatchExitGateScore,
			wantCounts: "fetched=20 deduped=18 qualified=18 scored=0",
		},
		{
			name: "select 闸门：择优后无内容",
			out: gateOut{
				items: items(20), deduped: items(18), scored: scoredItems(18),
			},
			wantGate:   types.BatchExitGateSelect,
			wantCounts: "fetched=20 deduped=18 qualified=18 scored=18 selected=0",
		},
		{
			name: "cardgen 闸门：卡片生成后无内容",
			out: gateOut{
				items: items(20), deduped: items(18), scored: scoredItems(18),
				selected: scoredItems(5),
			},
			wantGate:   types.BatchExitGateCardGen,
			wantCounts: "fetched=20 deduped=18 qualified=18 scored=18 selected=5 cards=0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			g := gateStubs{out: tc.out}
			g.register(env)

			env.ExecuteWorkflow(PushPipelineWorkflow, PushParams{UserID: 7, ScheduleID: "push-7-x"})

			if !env.IsWorkflowCompleted() {
				t.Fatal("workflow 未结束")
			}
			// 红线（workflow.go:19）：无内容可推是**正常终态**，记账不改变这一点。
			if err := env.GetWorkflowError(); err != nil {
				t.Fatalf("空批次必须是成功终态，实得错误: %v", err)
			}

			recorded, pushed := g.snapshot()
			if len(pushed) != 0 {
				t.Errorf("提前退出不得走到 Push，实得 %d 次", len(pushed))
			}
			if len(recorded) != 1 {
				t.Fatalf("应恰好记一行空批次，实得 %d 行: %+v", len(recorded), recorded)
			}
			got := recorded[0]
			if got.Gate != tc.wantGate {
				t.Errorf("闸门不符: want %q, got %q", tc.wantGate, got.Gate)
			}
			if s := countsStr(got.Counts); s != tc.wantCounts {
				t.Errorf("漏斗不符:\n want %q\n  got %q", tc.wantCounts, s)
			}
			if got.UserID != 7 {
				t.Errorf("user_id 应透传，want 7, got %d", got.UserID)
			}
			// P1b b1：schedule_id 也须经编排层穿到空批次记账（PushParams→RecordEmptyIn）。
			if got.ScheduleID != "push-7-x" {
				t.Errorf("schedule_id 应透传到空批次，want push-7-x, got %q", got.ScheduleID)
			}
			// 幂等键即 workflow 的确定性 traceID（workflow.go:37-40 的 SideEffect）。
			// 它非空是 store 侧幂等的前提：空键落在 004 部分唯一索引之外
			// （WHERE idempotency_key <> ''），重试会长出第二行。
			if got.TraceID == "" {
				t.Error("空批次必须带 traceID 作幂等键，实得空串")
			}
			// 门槛过滤落地（契约 §6.1）：select 闸门**恒通知**（本测试 workflow ID 是
			// default-test-workflow-id，非用户触发——通知发生即证明"恒"），其余闸门
			// 非用户触发保持静默。通知须带门槛上下文：ScheduleID 供活动查档位、
			// MaxScore=过滤前最高分（scoredItems 全 80 分）。
			notified := g.notifiedCalls()
			if tc.wantGate == types.BatchExitGateSelect {
				if len(notified) != 1 {
					t.Fatalf("select 闸门应恒发空批通知（含定时触发），实得 %d 次", len(notified))
				}
				n := notified[0]
				if n.Gate != types.BatchExitGateSelect || n.ScheduleID != "push-7-x" || n.MaxScore != 80 {
					t.Errorf("空批通知上下文不符: gate=%q schedule_id=%q max_score=%v", n.Gate, n.ScheduleID, n.MaxScore)
				}
			} else if len(notified) != 0 {
				t.Errorf("非用户触发的 %s 闸门不该发通知，实得 %d 次", tc.wantGate, len(notified))
			}
		})
	}
}

// TestPushPipelineWorkflow_FullRunRecordsNothing 有内容可推时不记空批次——
// 空批次行必须只对应"真的没东西推"，否则这张表自己就成了噪声源。
func TestPushPipelineWorkflow_FullRunRecordsNothing(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	g := gateStubs{out: gateOut{
		items: items(20), deduped: items(18), scored: scoredItems(18),
		selected: scoredItems(5), cards: cardsOf(5),
	}}
	g.register(env)

	env.ExecuteWorkflow(PushPipelineWorkflow, PushParams{UserID: 7, ScheduleID: "push-7-x"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("正常推送 workflow 意外失败: %v", err)
	}
	recorded, pushed := g.snapshot()
	if len(recorded) != 0 {
		t.Errorf("跑到 Push 的运行不得记空批次，实得: %+v", recorded)
	}
	if len(pushed) != 1 || len(pushed[0].Cards) != 5 {
		t.Fatalf("应恰好推一次、带 5 张卡，实得: %+v", pushed)
	}
	// P1b b1：schedule_id 须经编排层穿到 Push（PushParams→PushIn），b3 据此按任务隔离。
	if pushed[0].ScheduleID != "push-7-x" {
		t.Errorf("schedule_id 应透传到 Push，want push-7-x, got %q", pushed[0].ScheduleID)
	}
	// Push 与空批次记账共用同一个 traceID 作幂等键；两者在一次运行里互斥，
	// 故一个 traceID 在 push_batches 里恒只对应一行（store 侧护栏据此成立）。
	if pushed[0].TraceID == "" {
		t.Error("Push 应带 traceID 作幂等键")
	}
}

// TestPushPipelineWorkflow_RecordFailureDoesNotBreakNormalExit 记账活动失败
// （重试耗尽）绝不能把一次**正常的**空批次变成 workflow 失败。
//
// 这是本 PR 最重要的一条负向约束：记录是给人看的附加信息，"无内容可推是正常终态"
// 才是产品语义（workflow.go:19 红线）。搞反了就会在"今早没新闻"这种最平常的日子里
// 制造一条假的失败告警——比什么都不记更坏。参照 EvolveProfile 步骤的同款处理
// （其 log.Warn：失败只提示不阻断）。
func TestPushPipelineWorkflow_RecordFailureDoesNotBreakNormalExit(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	g := gateStubs{out: gateOut{recErr: errors.New("库挂了")}} // items 为空 → fetch 闸门
	g.register(env)

	env.ExecuteWorkflow(PushPipelineWorkflow, PushParams{UserID: 7})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow 未结束")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("记账失败不得把正常空批次变成 workflow 失败，实得: %v", err)
	}
	recorded, _ := g.snapshot()
	if len(recorded) == 0 {
		t.Fatal("记账活动应被调用过（哪怕注定失败）")
	}
}

// TestPushPipelineWorkflow_QuotaExitsAsNormalTerminal：额度用尽必须走**正常终态 +
// 专属闸门**，而不是 workflow 失败。
//
// 这条用例守的是一个用户直接能感受到的谎：额度用尽若混进 CodeLLMUnavailable，
// workflow 会失败（用户什么提示都收不到，只能干等）；若混进 BatchExitGateScore，
// 用户收到的是「打分后没有达标的」——那会让人跑去改画像、换信源，白折腾一圈还找不到原因。
// 真相是额度用完了，而且会自动恢复。
func TestPushPipelineWorkflow_QuotaExitsAsNormalTerminal(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stage string // 在哪个 activity 上撞额度
	}{
		{"打分阶段撞额度", "Score"},
		{"出卡阶段撞额度", "CardGen"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			g := gateStubs{out: gateOut{
				items:    make([]types.ContentItem, 20),
				deduped:  make([]types.ContentItem, 18),
				scored:   make([]types.ScoredItem, 18),
				selected: make([]types.ScoredItem, 5),
			}}
			g.register(env)
			// 覆盖目标 activity，让它返回额度错误——与 activities.go 里
			// 「整批全因配额失败」时返回的是同一个错误码。
			// 与 activities.go 里真实返回的形态一致：经 nonRetryable 包装，
			// Type 为错误码。桩若返回裸 AppError，测的就是一个生产中不存在的形态。
			quotaErr := nonRetryable(types.NewAppError(types.CodeQuotaExceeded, "本租户 LLM 额度已用尽", nil))
			switch tc.stage {
			case "Score":
				env.RegisterActivityWithOptions(
					func(context.Context, ScoreIn) ([]types.ScoredItem, error) { return nil, quotaErr },
					activity.RegisterOptions{Name: "Score"})
			case "CardGen":
				env.RegisterActivityWithOptions(
					func(context.Context, CardGenIn) ([]GeneratedCard, error) { return nil, quotaErr },
					activity.RegisterOptions{Name: "CardGen"})
			}

			env.ExecuteWorkflow(PushPipelineWorkflow, PushParams{UserID: 7, ScheduleID: "push-7-x"})

			if !env.IsWorkflowCompleted() {
				t.Fatal("workflow 未结束")
			}
			if err := env.GetWorkflowError(); err != nil {
				t.Fatalf("额度用尽是正常终态而非故障，实得错误: %v —— "+
					"走失败路径用户收不到任何提示，只能干等着纳闷", err)
			}
			recorded, pushed := g.snapshot()
			if len(pushed) != 0 {
				t.Errorf("额度用尽不得走到 Push，实得 %d 次", len(pushed))
			}
			if len(recorded) != 1 {
				t.Fatalf("应恰好记一行空批次，实得 %d 行", len(recorded))
			}
			if got := recorded[0].Gate; got != types.BatchExitGateQuota {
				t.Errorf("闸门必须是 quota，实得 %q —— 记成 score 会让用户收到"+
					"「打分后没有达标的」这句假话，跑去改画像换信源也找不到原因", got)
			}
		})
	}
}

// TestEmptyResultMarkdown_QuotaIsHonest：额度闸门的文案不得说"没有新内容"。
// 内容很可能是有的，只是没额度去处理它。
func TestEmptyResultMarkdown_QuotaIsHonest(t *testing.T) {
	txt := emptyResultMarkdown(
		NotifyEmptyIn{Gate: types.BatchExitGateQuota, Counts: types.PipelineCounts{}},
		types.StrictnessNormal)
	if strings.Contains(txt, "没有新内容") {
		t.Errorf("额度文案不得说「没有新内容」——内容可能是有的，只是没额度处理：%q", txt)
	}
	if !strings.Contains(txt, "额度") {
		t.Errorf("额度文案必须点明原因，否则用户无从判断该做什么：%q", txt)
	}
	if !strings.Contains(txt, "恢复") {
		t.Errorf("必须说明会自动恢复，否则用户以为坏了会去反复重试：%q", txt)
	}
}

// TestPushPipelineWorkflow_QuotaNotifiesEvenOnScheduledRun 守住"额度用尽必须通知"，
// 尤其是**定时批**——而定时批恰恰是被 userTriggered 门控挡掉的那一类。
//
// 这条防的是一个完全静默的故障：额度用尽 → 空批次正常终态 → 通知被门控挡掉 →
// 用户的早报无声消失。他无从判断是没新闻、服务挂了、还是自己欠费，
// 三种情况在他那里长得一模一样。而额度用尽主要就发生在定时批上（它是最大的一笔消耗），
// 也就是说门控恰好把最需要通知的场景全挡住了。
func TestPushPipelineWorkflow_QuotaNotifiesEvenOnScheduledRun(t *testing.T) {
	for _, tc := range []struct {
		name       string
		wfID       string
		gate       types.BatchExitGate
		wantNotify bool
	}{
		{"定时批撞额度必须通知", "wf-push-1-abc-2026-07-20T00:30:00Z", types.BatchExitGateQuota, true},
		{"定时批没新内容保持静默", "wf-push-1-abc-2026-07-20T00:30:00Z", types.BatchExitGateFetch, false},
		{"用户触发没新内容也通知", "push-adhoc-1-xyz", types.BatchExitGateFetch, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: tc.wfID})

			var notified atomic.Int64
			g := gateStubs{}
			if tc.gate == types.BatchExitGateQuota {
				// 撞额度：给足前置数据，让流程走到 Score 才失败。
				g.out = gateOut{items: make([]types.ContentItem, 5), deduped: make([]types.ContentItem, 5)}
			}
			g.register(env)
			env.RegisterActivityWithOptions(
				func(context.Context, NotifyEmptyIn) error { notified.Add(1); return nil },
				activity.RegisterOptions{Name: "NotifyEmptyResult"})
			if tc.gate == types.BatchExitGateQuota {
				env.RegisterActivityWithOptions(
					func(context.Context, ScoreIn) ([]types.ScoredItem, error) {
						return nil, nonRetryable(types.NewAppError(types.CodeQuotaExceeded, "额度用尽", nil))
					}, activity.RegisterOptions{Name: "Score"})
			}

			env.ExecuteWorkflow(PushPipelineWorkflow, PushParams{UserID: 7, ScheduleID: "s1"})
			if err := env.GetWorkflowError(); err != nil {
				t.Fatalf("应为正常终态: %v", err)
			}

			got := notified.Load() > 0
			if got != tc.wantNotify {
				if tc.wantNotify {
					t.Errorf("闸门 %s 必须通知用户，实际静默 —— "+
						"早报无声消失，用户分不清是没新闻、服务挂了、还是欠费", tc.gate)
				} else {
					t.Errorf("闸门 %s 在定时批上应保持静默（每天一条「今天没新闻」是噪音），实际发了通知", tc.gate)
				}
			}
		})
	}
}
