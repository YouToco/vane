package workflow

import (
	"io"
	"log/slog"
	"strconv"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/sdk/converter"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"

	"github.com/YouToco/vane/types"
)

// 本文件把契约 §8.2 的重放兼容性从**散文**变成**测试**。
//
// 此前它一直是注释里的一句断言（"推送是秒级短工作流，发布窗口避开 08:30 即可"），
// 三个候选实现的作者都只是这么写了一遍、没人真跑过。下面两个用例是一对，缺一不可：
//
//   - HappyPath：009 之前的历史（跑到 Push 成功）用**当前**代码重放，必须 PASS。
//     这是真正有 in-flight 风险的那条路——Score/CardGen 是 LLM 调用，秒级到十秒级，
//     发布正好撞上时 worker 就得拿新代码重放旧历史。
//   - GateHistoryIsIncompatible：009 之前停在闸门的历史用当前代码重放，必须 FAIL。
//     它是 HappyPath 的**校准器**：证明这套历史夹具真能喂进重放器、且重放器真会因为
//     多出来的 Activity 报非确定性。没有它，HappyPath 绿了也说明不了任何事——一个
//     恒绿的测试等于没写。
//
// 为什么"闸门历史重放失败"是**可接受**的而不是 bug（见 workflow.go 顶部注释）：
// 闸门分支 return 前才有 RecordEmptyBatch，而闸门本身**紧接着就结束 workflow**。
// 一个 workflow 只有还在运行时才谈得上"被重放"，而走到闸门的运行在毫秒内就 Completed
// 了——它没有 in-flight 窗口。故新 Activity 只出现在"注定马上终止"的分支上，任何
// **仍然在飞**的 workflow 按定义都在 HappyPath 那条路上，命令序列逐字未变。
// 这不是运气，是这个设计的性质；上面这对用例就是把这条性质钉死。
//
// 历史夹具是手写的（`historyBuilder`）而不是从真 Temporal 导出的 JSON：本仓无
// Temporal 依赖的单测环境，而 testsuite.TestWorkflowEnvironment 不导出历史。
// 手写的风险是"夹具本身不忠实于基线"——校准手段是**在基线代码上跑**（把 PR2 改动
// stash 掉）：夹具若写歪（活动名/顺序/事件 ID 错位），基线上就红了。本文件的两个
// 用例都刻意不引用任何 009 新增符号，正是为了能在基线上原样编译通过、供此校准。
//
// **已实跑的校准结果**（2026-07-16，非推演——两个方向都验过）：
//
//	                        基线代码(stash PR2)   当前代码(含 009)
//	HappyPath                    PASS                PASS      ← 不变量：这条路没被动过
//	GateHistoryIsIncompatible    FAIL                PASS      ← 变量：009 的 delta 在此现形
//
// 左列证明夹具忠实于基线（HappyPath 在基线上就能过，说明活动名/顺序/事件 ID/marker
// 全对）；右列与左列**不同**证明这对用例真在测 009 的差异，而不是一对恒真断言。
// 当前代码上 GateHistory 报的原话是：
//
//	[TMPRL1100] nondeterministic workflow: extra replay command for
//	ScheduleActivityTask: (ActivityId:18, ActivityType:(Name:RecordEmptyBatch), ...)
//
// 基线复现方法：`git stash push` 掉 PR2 改动，把引用了 009 新符号的
// workflow_test.go 移开，补一个含 items/scoredItems/cardsOf 的临时垫片即可编译。

const replayTaskQueue = "vane-push"

// historyBuilder 拼一份 Temporal 事件历史。事件 ID 自增，且刻意复刻 SDK 的
// ID 预测规则——重放器靠它把历史事件与工作流生成的命令对上：
//   - WorkflowTaskStarted(id=N) 之后，下一个命令事件的 ID 是 N+2
//     （N+1 恒为 WorkflowTaskCompleted），见 SDK commandsHelper.
//     setCurrentWorkflowTaskStartedEventID。
//   - Activity 未显式指定 ID 时，ActivityID = 其 ActivityTaskScheduled 事件的
//     eventID 的十进制串（SDK: parameters.ActivityID = getStringID(ScheduleID)）。
//
// 这两条不需要背——写错了下面的用例会在基线上就红，这正是校准的意义。
type historyBuilder struct {
	t      *testing.T
	events []*historypb.HistoryEvent
	nextID int64
}

func newHistoryBuilder(t *testing.T, input any) *historyBuilder {
	t.Helper()
	b := &historyBuilder{t: t, nextID: 1}
	b.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				WorkflowType: &commonpb.WorkflowType{Name: "PushPipelineWorkflow"},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: replayTaskQueue},
				Input:        b.payloads(input),
			},
		},
	})
	b.workflowTask()
	return b
}

func (b *historyBuilder) add(ev *historypb.HistoryEvent) int64 {
	ev.EventId = b.nextID
	b.nextID++
	b.events = append(b.events, ev)
	return ev.EventId
}

// payloads 编码一个值；nil 编成"无载荷"（只返回 error 的 Activity 就是这个形状）。
func (b *historyBuilder) payloads(v any) *commonpb.Payloads {
	b.t.Helper()
	if v == nil {
		return nil
	}
	p, err := converter.GetDefaultDataConverter().ToPayloads(v)
	if err != nil {
		b.t.Fatalf("编码历史载荷失败: %v", err)
	}
	return p
}

// workflowTask 追加一轮完整的 WorkflowTask（Scheduled→Started→Completed）。
func (b *historyBuilder) workflowTask() {
	sched := b.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
			WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
				TaskQueue: &taskqueuepb.TaskQueue{Name: replayTaskQueue},
			},
		},
	})
	started := b.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{
			WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
				ScheduledEventId: sched,
			},
		},
	})
	b.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
			WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
				ScheduledEventId: sched,
				StartedEventId:   started,
			},
		},
	})
}

// sideEffect 追加一条 SideEffect marker —— 即 workflow.go:38 那次 uuid 生成。
// 少了它，重放会 panic「No cached result found for side effectID」：SideEffect
// 在重放时只从历史取值，绝不重新执行（这正是 traceID 必须走 SideEffect 的原因）。
func (b *historyBuilder) sideEffect(id int64, value any) {
	b.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_MARKER_RECORDED,
		Attributes: &historypb.HistoryEvent_MarkerRecordedEventAttributes{
			MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{
				MarkerName: "SideEffect", // SDK: sideEffectMarkerName
				Details: map[string]*commonpb.Payloads{
					"side-effect-id": b.payloads(id),    // SDK: sideEffectMarkerIDName
					"data":           b.payloads(value), // SDK: sideEffectMarkerDataName
				},
			},
		},
	})
}

// activity 追加一个跑完的 Activity（Scheduled→Started→Completed）及其后的 WorkflowTask。
func (b *historyBuilder) activity(name string, input, result any) {
	attrs := &historypb.ActivityTaskScheduledEventAttributes{
		ActivityType: &commonpb.ActivityType{Name: name},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: replayTaskQueue},
		Input:        b.payloads(input),
	}
	sched := b.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: attrs,
		},
	})
	attrs.ActivityId = strconv.FormatInt(sched, 10) // 见类型注释：ID = 本事件 eventID
	started := b.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
			ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
				ScheduledEventId: sched,
			},
		},
	})
	b.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
			ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
				ScheduledEventId: sched,
				StartedEventId:   started,
				Result:           b.payloads(result),
			},
		},
	})
	b.workflowTask()
}

// complete 收尾成功终态。重放器见到末事件是 WorkflowExecutionCompleted 时，会额外
// 校验工作流确实产出了 CompleteWorkflowExecution 命令——故本夹具是端到端的，
// 不只是"没报非确定性"。
func (b *historyBuilder) complete() *historypb.History {
	b.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
			WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{},
		},
	})
	return &historypb.History{Events: b.events}
}

func replayLogger() sdklog.Logger {
	return sdklog.NewStructuredLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func replay(t *testing.T, h *historypb.History) error {
	t.Helper()
	r := worker.NewWorkflowReplayer()
	r.RegisterWorkflow(PushPipelineWorkflow)
	return r.ReplayWorkflowHistory(replayLogger(), h)
}

// baselineHappyPathHistory 是一份 **009 之前**的历史：EvolveProfile → Fetch(20 条)
// → Dedup(18) → Score(18) → Select(5) → CardGen(5) → Push → Completed。
// 它里面**没有** RecordEmptyBatch —— 009 之前那个 Activity 还不存在。
func baselineHappyPathHistory(t *testing.T) *historypb.History {
	t.Helper()
	p := PushParams{UserID: 7}
	const traceID = "9f1d6c5e-0000-4000-8000-baselinehistory"

	b := newHistoryBuilder(t, p)
	b.sideEffect(1, traceID) // SideEffect 计数从 1 起（SDK: getNextSideEffectID）
	b.activity("EvolveProfile", EvolveIn{UserID: p.UserID, TraceID: traceID}, nil)
	b.activity("Fetch", p, items(20))
	b.activity("Dedup", DedupIn{UserID: p.UserID, TraceID: traceID, Items: items(20)}, items(18))
	b.activity("Score", ScoreIn{UserID: p.UserID, TraceID: traceID, Items: items(18)}, scoredItems(18))
	b.activity("Select", SelectIn{UserID: p.UserID, TraceID: traceID, TopN: defaultTopN, Scored: scoredItems(18)}, scoredItems(5))
	b.activity("CardGen", CardGenIn{UserID: p.UserID, TraceID: traceID, Items: scoredItems(5)}, cardsOf(5))
	b.activity("Push", PushIn{UserID: p.UserID, TraceID: traceID, Cards: cardsOf(5)}, nil)
	return b.complete()
}

// TestPushPipelineWorkflow_ReplayBaselineHappyPath 009 **之前**的成功推送历史，
// 用**当前**（已插入五处 RecordEmptyBatch 的）workflow 重放——必须无非确定性。
//
// 这是唯一真正有风险的重放场景：一个 in-flight workflow 只可能停在某个 Activity 上
// （Score/CardGen 是 LLM 调用，秒级到十秒级，发布撞上就得拿新代码重放旧历史），
// 而停在 Activity 上就意味着它走的是"每步都有内容"的这条路——五个闸门一个都没进，
// 于是新代码在这条路上一条 RecordEmptyBatch 命令都不会生成，命令序列与历史逐字一致。
func TestPushPipelineWorkflow_ReplayBaselineHappyPath(t *testing.T) {
	if err := replay(t, baselineHappyPathHistory(t)); err != nil {
		t.Fatalf("009 之前的成功推送历史必须能被当前 workflow 无损重放，实得: %v", err)
	}
}

// TestPushPipelineWorkflow_ReplayBaselineGateHistoryIsIncompatible 是上面那条的
// **校准器**，也是对 workflow.go 顶部"这是非确定性变更、刻意不做版本化"那句声明的
// 兑现：009 之前停在 fetch 闸门的历史（Fetch 返回 0 条后直接 Completed），用当前代码
// 重放**必然**报非确定性——新代码在这里会先发一条 RecordEmptyBatch，而历史里只有
// "直接完成"。
//
// 断言它**失败**有两个作用：
//  1. 证明 HappyPath 那条不是恒绿的假测试——同一套夹具、同一个重放器，此处会咬人；
//  2. 把"这个不兼容是已知且被接受的"写成可执行的记录。它可接受，是因为走到闸门的运行
//     毫秒内就 Completed，几乎不存在"停在闸门上等发布"的 in-flight 窗口；真撞上了，
//     代价是这一条本就没内容可推的运行失败一次，下一次定时触发照常。若哪天要消除它，
//     手段是 workflow.GetVersion，而那个包袱的成本见 workflow.go 顶部注释。
//
// 若本用例哪天变绿了：要么有人给闸门加了版本化（那就把这里改成断言 PASS 并删掉这段），
// 要么夹具/重放器坏了——**别直接删掉它了事**，那会连带让 HappyPath 失去意义。
func TestPushPipelineWorkflow_ReplayBaselineGateHistoryIsIncompatible(t *testing.T) {
	p := PushParams{UserID: 7}
	const traceID = "9f1d6c5e-0000-4000-8000-gatehistory00"

	b := newHistoryBuilder(t, p)
	b.sideEffect(1, traceID)
	b.activity("EvolveProfile", EvolveIn{UserID: p.UserID, TraceID: traceID}, nil)
	b.activity("Fetch", p, []types.ContentItem{}) // 抓到 0 条 → 基线在此直接 return nil
	h := b.complete()

	err := replay(t, h)
	if err == nil {
		t.Fatal("停在闸门的旧历史用当前代码重放**应当**报非确定性——它没报，" +
			"说明夹具或重放器没在真的检查命令序列，那样 HappyPath 的绿也不可信")
	}
	t.Logf("符合预期的非确定性错误（这正是 §8.2 已接受的那个不兼容）: %v", err)
}
