package workflow

import (
	"encoding/json"
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
	sdkworkflow "go.temporal.io/sdk/workflow"

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

// versionWithSearchAttributes 复刻 GetVersion 新执行的正常双事件：Version marker
// 后紧跟 TemporalChangeVersion upsert。IndexedFields 里的键不可省；Go SDK 用它把
// upsert 识别为 marker 的伴生事件并一起跳过命令序号。
func (b *historyBuilder) versionWithSearchAttributes(changeID string, version int) {
	b.t.Helper()
	b.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_MARKER_RECORDED,
		Attributes: &historypb.HistoryEvent_MarkerRecordedEventAttributes{
			MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{
				MarkerName: "Version",
				Details: map[string]*commonpb.Payloads{
					"change-id": b.payloads(changeID),
					"version":   b.payloads(version),
				},
			},
		},
	})
	changeVersion, err := converter.GetDefaultDataConverter().ToPayload(
		[]string{changeID + "-" + strconv.Itoa(version)},
	)
	if err != nil {
		b.t.Fatalf("编码 TemporalChangeVersion: %v", err)
	}
	b.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
		Attributes: &historypb.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes{
			UpsertWorkflowSearchAttributesEventAttributes: &historypb.UpsertWorkflowSearchAttributesEventAttributes{
				SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
					"TemporalChangeVersion": changeVersion,
				}},
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

func replayWithExecution(
	t *testing.T,
	h *historypb.History,
	execution sdkworkflow.Execution,
) error {
	t.Helper()
	r := worker.NewWorkflowReplayer()
	r.RegisterWorkflow(PushPipelineWorkflow)
	return r.ReplayWorkflowHistoryWithOptions(replayLogger(), h,
		worker.ReplayWorkflowHistoryOptions{OriginalExecution: execution})
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

// preP1cScoreIn / preP1cCardGenIn 必须是历史年代自己的 wire schema，不能拿
// 当前类型填零值再依赖 omitempty：后者会被未来的 json tag 或默认值改动悄悄改写，
// 让所谓“旧历史”其实跟着新代码漂移。
type preP1cScoreIn struct {
	UserID  int64               `json:"user_id"`
	TraceID string              `json:"trace_id"`
	Items   []types.ContentItem `json:"items"`
}

type preP1cCardGenIn struct {
	UserID  int64              `json:"user_id"`
	TraceID string             `json:"trace_id"`
	Items   []types.ScoredItem `json:"items"`
}

// postP1bPreP1cScheduledHappyPathHistory 是 P1b 已上线、P1c 尚未开发时的一份
// 完整定时任务历史。它与上面的 009 基线守不同的发布边界：
//
//   - PushParams 已有非空 ScheduleID，Fetch / Select / Push 都消费任务身份；
//   - ScoreIn / CardGenIn 仍是 P1c 之前的旧载荷，不含任务身份。
//
// 这份历史刻意取 P1b→A5 之间的真实形状（没有 scheduled-run-authorization
// Version marker）；当前 workflow 重放时 GetVersion 返回 DefaultVersion，故不会凭空
// 期待 AuthorizeRun。P1c 给现有 Activity input 增字段也不需要再套 GetVersion：Go SDK
// v1.46 的 ScheduleActivity replay matcher 比对 ActivityID+Type，不比 input。已经调度的
// Activity 继续收到历史旧载荷；尚未调度的 Activity 由新代码生成含任务身份的新载荷。
func postP1bPreP1cScheduledHappyPathHistory(t *testing.T) *historypb.History {
	t.Helper()
	p := PushParams{
		UserID:     7,
		ScheduleID: "push-7-playbook",
		NLDesc:     "每日 AI 情报",
	}
	const traceID = "9f1d6c5e-0000-4000-8000-prep1chistory0"

	b := newHistoryBuilder(t, p)
	b.sideEffect(1, traceID)
	b.activity("EvolveProfile", EvolveIn{UserID: p.UserID, TraceID: traceID}, nil)
	b.activity("Fetch", p, items(20))
	b.activity("Dedup", DedupIn{UserID: p.UserID, TraceID: traceID, Items: items(20)}, items(18))
	// P1c 前这两个载荷刻意没有 ScheduleID；这不是漏写，是本基线的核心。
	b.activity("Score", preP1cScoreIn{UserID: p.UserID, TraceID: traceID, Items: items(18)}, scoredItems(18))
	b.activity("Select", SelectIn{
		UserID: p.UserID, TraceID: traceID, TopN: defaultTopN,
		Scored: scoredItems(18), ScheduleID: p.ScheduleID,
	}, scoredItems(5))
	b.activity("CardGen", preP1cCardGenIn{UserID: p.UserID, TraceID: traceID, Items: scoredItems(5)}, cardsOf(5))
	b.activity("Push", PushIn{
		UserID: p.UserID, ScheduleID: p.ScheduleID, TraceID: traceID,
		Cards: cardsOf(5), TaskTitle: p.NLDesc,
	}, nil)
	return b.complete()
}

func TestPushPipelineWorkflow_ReplayPostP1bPreP1cScheduledHappyPath(t *testing.T) {
	if err := replay(t, postP1bPreP1cScheduledHappyPathHistory(t)); err != nil {
		t.Fatalf("P1b 后、P1c 前的定时任务历史必须能被当前 workflow 无损重放，实得: %v", err)
	}
}

func TestPreP1cReplayFixturesPhysicallyOmitScheduleID(t *testing.T) {
	for name, history := range map[string]*historypb.History{
		"post P1b pre A5": postP1bPreP1cScheduledHappyPathHistory(t),
		"post A5":         postA5PreP1cScheduledHappyPathHistory(t),
	} {
		t.Run(name, func(t *testing.T) {
			seen := map[string]bool{"Score": false, "CardGen": false}
			for _, event := range history.Events {
				attrs := event.GetActivityTaskScheduledEventAttributes()
				activityType := attrs.GetActivityType().GetName()
				if _, relevant := seen[activityType]; !relevant {
					continue
				}
				payloads := attrs.GetInput().GetPayloads()
				if len(payloads) != 1 {
					t.Fatalf("%s 历史输入 payload 数 = %d，期望 1", activityType, len(payloads))
				}
				var wire map[string]json.RawMessage
				if err := json.Unmarshal(payloads[0].GetData(), &wire); err != nil {
					t.Fatalf("解析 %s 历史输入 JSON: %v", activityType, err)
				}
				if _, exists := wire["schedule_id"]; exists {
					t.Fatalf("%s 的 pre-P1c 历史物理上不得含 schedule_id: %s",
						activityType, payloads[0].GetData())
				}
				seen[activityType] = true
			}
			for activityType, ok := range seen {
				if !ok {
					t.Fatalf("历史中未找到 %s Activity", activityType)
				}
			}
		})
	}
}

// postA5PreP1cScheduledHappyPathHistory 覆盖 P1c 发布前最近的一代历史：A5 已加入
// 授权 marker/Activity，但 Score/CardGen 仍使用物理上没有 ScheduleID 的旧 schema。
// 与 pre-A5 夹具并存，是为了同时守住存量 retention 尾部和真实发布窗口。
func postA5PreP1cScheduledHappyPathHistory(t *testing.T) *historypb.History {
	t.Helper()
	p := PushParams{
		UserID:     7,
		RunKind:    PushRunKindScheduled,
		ScheduleID: "push-7-playbook-a5",
		NLDesc:     "每日 AI 情报",
	}
	const traceID = "9f1d6c5e-0000-4000-8000-posta5history0"

	b := newHistoryBuilder(t, p)
	b.sideEffect(1, traceID)
	b.versionWithSearchAttributes("scheduled-run-authorization", 1)
	b.activity("AuthorizeRun", p, true)
	b.activity("EvolveProfile", EvolveIn{UserID: p.UserID, TraceID: traceID}, nil)
	b.activity("Fetch", p, items(20))
	b.activity("Dedup", DedupIn{UserID: p.UserID, TraceID: traceID, Items: items(20)}, items(18))
	b.activity("Score", preP1cScoreIn{UserID: p.UserID, TraceID: traceID, Items: items(18)}, scoredItems(18))
	b.activity("Select", SelectIn{
		UserID: p.UserID, TraceID: traceID, TopN: defaultTopN,
		Scored: scoredItems(18), ScheduleID: p.ScheduleID,
	}, scoredItems(5))
	b.activity("CardGen", preP1cCardGenIn{UserID: p.UserID, TraceID: traceID, Items: scoredItems(5)}, cardsOf(5))
	b.activity("Push", PushIn{
		UserID: p.UserID, ScheduleID: p.ScheduleID, TraceID: traceID,
		Cards: cardsOf(5), TaskTitle: p.NLDesc,
	}, nil)
	return b.complete()
}

func TestPushPipelineWorkflow_ReplayPostA5PreP1cScheduledHappyPath(t *testing.T) {
	if err := replay(t, postA5PreP1cScheduledHappyPathHistory(t)); err != nil {
		t.Fatalf("A5 后、P1c 前的定时任务历史必须能被当前 workflow 无损重放，实得: %v", err)
	}
}

// compiledV1HappyPathHistory is the first C1b history generation. It pins the
// new GetVersion marker and proves that PrepareRun replaces (rather than
// supplements) AuthorizeRun, while every later Activity receives only one
// sealed reference plus the existing stage payload.
func compiledV1HappyPathHistory(
	t *testing.T,
	execution sdkworkflow.Execution,
) *historypb.History {
	t.Helper()
	p := PushParams{
		TenantID: 7, UserID: 9, RunKind: PushRunKindScheduled,
		ExecutionMode: types.ExecutionModeCompiled, RuntimeVersion: CompiledRuntimeSnapshotV1,
		ScheduleID: "task-c1b-replay", NLDesc: "每日冻结情报",
	}
	identity := types.RunIdentity{
		TemporalWorkflowID: execution.ID,
		TemporalRunID:      execution.RunID,
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           p.TenantID,
		UserID:             p.UserID,
		TaskID:             p.ScheduleID,
	}
	ref := mustCompiledRunRef(identity, 101)
	run := &CompiledRunInputV1{
		TenantID: p.TenantID, TaskID: p.ScheduleID, Snapshot: ref,
	}
	preparedParams := p
	preparedParams.Snapshot = &ref
	const traceID = "9f1d6c5e-0000-4000-8000-c1breplay000"

	b := newHistoryBuilder(t, p)
	b.sideEffect(1, traceID)
	b.versionWithSearchAttributes("scheduled-runtime-envelope-v1", 1)
	b.versionWithSearchAttributes("compiled-run-snapshot-v1", 1)
	b.activity("PrepareRun", p, PrepareRunResult{Authorized: true, Snapshot: ref})
	b.activity("EvolveProfile", EvolveIn{UserID: p.UserID, TraceID: traceID, Run: run}, nil)
	b.activity("Fetch", preparedParams, items(20))
	b.activity("Dedup", DedupIn{
		UserID: p.UserID, TraceID: traceID, Items: items(20), Run: run,
	}, items(18))
	b.activity("Score", ScoreIn{
		UserID: p.UserID, TraceID: traceID, Items: items(18),
		ScheduleID: p.ScheduleID, Run: run,
	}, scoredItems(18))
	b.activity("Select", SelectIn{
		UserID: p.UserID, TraceID: traceID, TopN: defaultTopN,
		Scored: scoredItems(18), ScheduleID: p.ScheduleID, Run: run,
	}, scoredItems(5))
	b.activity("CardGen", CardGenIn{
		UserID: p.UserID, TraceID: traceID, Items: scoredItems(5),
		ScheduleID: p.ScheduleID, Run: run,
	}, cardsOf(5))
	b.activity("Push", PushIn{
		UserID: p.UserID, ScheduleID: p.ScheduleID, TraceID: traceID,
		Cards: cardsOf(5), TaskTitle: p.NLDesc, Run: run,
	}, nil)
	return b.complete()
}

func TestPushPipelineWorkflow_ReplayCompiledV1HappyPath(t *testing.T) {
	execution := sdkworkflow.Execution{
		ID: "wf-task-c1b-replay", RunID: "00000000-0000-4000-8000-000000000101",
	}
	if err := replayWithExecution(t, compiledV1HappyPathHistory(t, execution), execution); err != nil {
		t.Fatalf("C1b compiled history must replay exactly: %v", err)
	}
}

func compiledRunOutcomeV1HappyPathHistory(
	t *testing.T,
	execution sdkworkflow.Execution,
) *historypb.History {
	t.Helper()
	p := PushParams{
		TenantID: 7, UserID: 9, RunKind: PushRunKindScheduled,
		ExecutionMode:  types.ExecutionModeCompiled,
		RuntimeVersion: CompiledRuntimeRunOutcomeV1,
		ScheduleID:     "task-p1b-replay", NLDesc: "每日冻结情报",
	}
	identity := types.RunIdentity{
		TemporalWorkflowID: execution.ID, TemporalRunID: execution.RunID,
		RunKind:  types.RunSnapshotKindScheduled,
		TenantID: p.TenantID, UserID: p.UserID, TaskID: p.ScheduleID,
	}
	ref := mustCompiledRunRef(identity, 102)
	run := &CompiledRunInputV1{
		TenantID: p.TenantID, TaskID: p.ScheduleID, Snapshot: ref,
	}
	preparedParams := p
	preparedParams.Snapshot = &ref
	marker := types.RunOutcomeMarkerV1{
		ID: 202, SchemaVersion: types.RunOutcomeSchemaVersionV1,
		RunSnapshotID: ref.SnapshotID, TenantID: p.TenantID,
		UserID: p.UserID, TaskID: p.ScheduleID,
	}
	const traceID = "9f1d6c5e-0000-4000-8000-p1breplay000"
	b := newHistoryBuilder(t, p)
	b.sideEffect(1, traceID)
	b.versionWithSearchAttributes("scheduled-runtime-envelope-v1", 1)
	b.versionWithSearchAttributes("compiled-run-snapshot-v1", 1)
	b.activity("PrepareRun", p,
		PrepareRunResult{Authorized: true, Snapshot: ref})
	b.versionWithSearchAttributes("run-outcome-lifecycle-v1", 1)
	b.activity("BeginRunOutcomeV1",
		RunOutcomeBeginIn{UserID: p.UserID, Run: *run}, marker)
	b.activity("EvolveProfile",
		EvolveIn{UserID: p.UserID, TraceID: traceID, Run: run}, nil)
	b.activity("FetchOutcomeV1", preparedParams, FetchOutcomeResult{
		Items: items(20), SourceCoverage: types.RunCompletenessComplete,
	})
	b.activity("Dedup", DedupIn{
		UserID: p.UserID, TraceID: traceID, Items: items(20), Run: run,
	}, items(18))
	b.versionWithSearchAttributes("observation-qualification-v1", 1)
	b.activity("QualifyEvents", QualifyEventsIn{
		UserID: p.UserID, TraceID: traceID, ScheduleID: p.ScheduleID,
		Items: items(18), Run: run,
	}, QualifyEventsResult{Items: items(18), Outcome: "not_configured"})
	b.activity("ScoreOutcomeV1", ScoreIn{
		UserID: p.UserID, TraceID: traceID, Items: items(18),
		ScheduleID: p.ScheduleID, Run: run,
	}, ScoreOutcomeResult{
		Items: scoredItems(18), Processing: types.RunCompletenessComplete,
	})
	b.activity("Select", SelectIn{
		UserID: p.UserID, TraceID: traceID, TopN: defaultTopN,
		Scored: scoredItems(18), ScheduleID: p.ScheduleID, Run: run,
	}, scoredItems(5))
	b.activity("CardGenOutcomeV1", CardGenIn{
		UserID: p.UserID, TraceID: traceID, Items: scoredItems(5),
		ScheduleID: p.ScheduleID, Run: run,
	}, CardGenOutcomeResult{
		Cards: cardsOf(5), Processing: types.RunCompletenessComplete,
	})
	b.activity("Push", PushIn{
		UserID: p.UserID, ScheduleID: p.ScheduleID, TraceID: traceID,
		Cards: cardsOf(5), TaskTitle: p.NLDesc, Run: run,
	}, nil)
	b.activity("FinalizeRunOutcomeV1", RunOutcomeFinalizeIn{
		UserID: p.UserID, Run: *run,
		Claim: types.RunOutcomeClaimV1{
			RunOutcomeMarkerV1: marker,
			Result:             types.RunResultContent,
			SourceCoverage:     types.RunCompletenessComplete,
			Processing:         types.RunCompletenessComplete,
		},
	}, nil)
	return b.complete()
}

func TestPushPipelineWorkflow_ReplayCompiledRunOutcomeV1HappyPath(t *testing.T) {
	execution := sdkworkflow.Execution{
		ID:    "wf-task-p1b-replay",
		RunID: "00000000-0000-4000-8000-000000000102",
	}
	if err := replayWithExecution(
		t, compiledRunOutcomeV1HappyPathHistory(t, execution), execution,
	); err != nil {
		t.Fatalf("P1-B history must replay exactly: %v", err)
	}
}

func TestPushPipelineWorkflow_ReplayCompiledV1RejectsPrepareMutation(t *testing.T) {
	execution := sdkworkflow.Execution{
		ID: "wf-task-c1b-replay", RunID: "00000000-0000-4000-8000-000000000101",
	}
	history := compiledV1HappyPathHistory(t, execution)
	mutated := false
	for _, event := range history.Events {
		attributes := event.GetActivityTaskScheduledEventAttributes()
		if attributes.GetActivityType().GetName() == "PrepareRun" {
			attributes.ActivityType.Name = "AuthorizeRun"
			mutated = true
			break
		}
	}
	if !mutated {
		t.Fatal("compiled replay fixture has no PrepareRun Activity")
	}
	if err := replayWithExecution(t, history, execution); err == nil {
		t.Fatal("mutating C1b PrepareRun command identity must trigger nondeterminism")
	}
}

func TestPushPipelineWorkflow_ReplayCompiledV1RejectsRuntimeRouteMutation(t *testing.T) {
	execution := sdkworkflow.Execution{
		ID: "wf-task-c1b-replay", RunID: "00000000-0000-4000-8000-000000000101",
	}
	history := compiledV1HappyPathHistory(t, execution)
	started := history.Events[0].GetWorkflowExecutionStartedEventAttributes()
	var input PushParams
	if err := converter.GetDefaultDataConverter().FromPayloads(started.GetInput(), &input); err != nil {
		t.Fatalf("decode compiled workflow input: %v", err)
	}
	input.RuntimeVersion = ""
	payloads, err := converter.GetDefaultDataConverter().ToPayloads(input)
	if err != nil {
		t.Fatalf("encode mutated compiled workflow input: %v", err)
	}
	started.Input = payloads

	if err := replayWithExecution(t, history, execution); err == nil {
		t.Fatal("removing C1b runtime route from a compiled history must trigger nondeterminism")
	}
}

// TestPushPipelineWorkflow_ReplayPostP1bRejectsActivityTypeMutation 是上条历史的
// 校准器：Activity input 漂移在本 SDK 版本合法，但类型/命令序列漂移仍必须被咬住。
// 若本用例变绿，上面那个 PASS 只说明重放器失效，不能再作为发布证据。
func TestPushPipelineWorkflow_ReplayPostP1bRejectsActivityTypeMutation(t *testing.T) {
	h := postP1bPreP1cScheduledHappyPathHistory(t)
	mutated := false
	for _, event := range h.Events {
		attrs := event.GetActivityTaskScheduledEventAttributes()
		if attrs.GetActivityType().GetName() == "Score" {
			attrs.ActivityType.Name = "ScoreBeforeP1c"
			mutated = true
			break
		}
	}
	if !mutated {
		t.Fatal("校准夹具中没有找到 Score Activity")
	}
	if err := replay(t, h); err == nil {
		t.Fatal("篡改历史 Activity Type 后重放应失败；未失败说明 replay 夹具没有约束命令身份")
	} else {
		t.Logf("符合预期的 Activity Type 非确定性错误: %v", err)
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
