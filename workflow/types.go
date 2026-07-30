// Package workflow 定义见微 Vane 的推送管道：一个可被 Temporal 直接触发的
// PushPipelineWorkflow（EvolveProfile 前置步 + Fetch→Dedup→Score→Select→
// CardGen→Push 六步）及其 Activity。
//
// 设计铁律（Temporal 确定性约束）：
//   - workflow 函数体只做编排（ExecuteActivity + 纯计算），绝不直接碰 HTTP/DB；
//     所有 I/O 关进 Activity。非确定性来源（uuid、时钟）走 SideEffect / workflow API。
//   - 本包定义"消费方接口"（fetcher/scorer/cardgen/pusher/store/feishu），
//     由 cmd/server 在装配时注入具体实现，从而与业务包解耦、便于替身测试。
//
// 跨包类型（PushParams/PushScope/GeneratedCard）放本文件；打分后的条目统一用
// types.ScoredItem（定义在 types 包，无 import 环）——scorer/cardgen/selector 与
// workflow 都 import types，彼此不直接依赖，从根上避免了环，也让 Select Activity
// 能直接复用 selector.RankTopN 而非各写一份 TopN。
package workflow

import (
	cardgenpkg "github.com/YouToco/vane/cardgen"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflowruntime"
)

// defaultTopN 是每批默认推送条数（PushScope.TopN 为 0 时取此值，见规格 B1）。
const defaultTopN = 5

// PushParams 是 PushPipelineWorkflow 的唯一入参，也是 Schedule.Action.Args 的元素。
// 铁律：只放稳定标识符（UserID+ScheduleID+Scope），绝不放候选内容 / batch_id——每次触发时
// 由 Fetch Activity 在触发时读取任务冻结范围，否则定时任务会反复推送陈旧内容。
//
// RunKind 显式区分定时与即时执行。不能再用 ScheduleID=="" 推断即时执行：存量
// Temporal Schedule 的冻结 Action 在 reconcile 前同样可能缺 ScheduleID，把它当即时
// 执行会绕过 A5 激活门并产生付费/推送副作用。未知零值一律 fail-closed；历史 workflow
// replay 由 workflow.go 的 GetVersion 分支兼容。
type PushRunKind string

const (
	PushRunKindScheduled PushRunKind = "scheduled"
	PushRunKindAdHoc     PushRunKind = "ad_hoc"
)

// ScheduleID 是触发本次推送的定时任务 id（任务手册 P1b）：定时触发时由 scheduler 填入其
// schedule id，据此让"按任务的取材目标抓/挑/投"成为可能。
// 但其身份由 RunKind=ad_hoc 明确声明，而不是从空串猜测。
type PushParams struct {
	// TenantID is populated only by a trusted scheduled-task Action. It stays
	// zero for ad-hoc runs and pre-C1b durable Actions, which continue through
	// the replay-compatible legacy path. C1b never derives tenant scope from a
	// snapshot reference or from mutable current task state.
	TenantID int64       `json:"tenant_id,omitempty"`
	UserID   int64       `json:"user_id"`
	RunKind  PushRunKind `json:"run_kind,omitempty"`
	// ExecutionMode is frozen into the durable Schedule Action by the
	// scheduler rollout policy. Unknown preserves legacy execution; Compiled
	// enables C1b. DiscoverAtRun remains fail-closed until C3 lands.
	ExecutionMode types.ExecutionMode `json:"execution_mode,omitempty"`
	// RuntimeVersion selects the implementation used for the already-approved
	// execution mode. It is deliberately distinct from ExecutionMode: a task is
	// still semantically Compiled while C1b is dark/canarying. Empty preserves
	// the legacy fixed pipeline; CompiledRuntimeSnapshotV1 enables PrepareRun.
	RuntimeVersion string    `json:"runtime_version,omitempty"`
	ScheduleID     string    `json:"schedule_id,omitempty"`
	Scope          PushScope `json:"scope"`
	// NLDesc 触发本次推送的调度的自然语言描述（聚合卡 header 的任务名）。
	// 存量调度的 Temporal Action 里没有本字段，解出零值空串——聚合卡落兜底标题，
	// 行为安全；新建调度由 scheduler.CreatePush 填入。
	NLDesc string `json:"nl_desc,omitempty"`
	// Snapshot is nil in every durable Schedule Action. PrepareRun populates it
	// in workflow memory and only the sealed reference crosses into downstream
	// Activity inputs; prompt/source/policy bodies never enter history here.
	Snapshot *RunSnapshotRef `json:"run_snapshot,omitempty"`
}

// CompiledRuntimeSnapshotV1 is the first immutable-snapshot implementation of
// the Compiled execution mode. Unknown is never used as a rollout label.
const CompiledRuntimeSnapshotV1 = workflowruntime.CompiledSnapshotV1

// CompiledRuntimeRunOutcomeV1 selects the same compiled snapshot runtime plus
// P1-B. A separate durable label preserves PushParams' frozen wire layout and
// keeps every pre-P1-B Action/history on CompiledRuntimeSnapshotV1.
const CompiledRuntimeRunOutcomeV1 = workflowruntime.RunOutcomeV1

// CompiledRuntimeCanonicalBriefV1 adds P1-C's durable pre-render Brief draft
// and atomic outcome+Brief seal. Keeping a distinct durable label prevents
// already-started P1-B histories from acquiring a new Activity command.
const CompiledRuntimeCanonicalBriefV1 = workflowruntime.CanonicalBriefV1

// CompiledRuntimeStructuredInsightV1 adds Phase 2-A's one-call structured
// CardGen policy and optional immutable Insight extension.
const CompiledRuntimeStructuredInsightV1 = workflowruntime.StructuredInsightV1

// CompiledRuntimeStructuredEventEvidenceV1 adds P2-B1's ordered multi-source
// event evidence Activity and optional immutable Brief provenance extension.
const CompiledRuntimeStructuredEventEvidenceV1 = workflowruntime.StructuredEventEvidenceV1

// CompiledRuntimeExecutiveBriefV1 adds P2-D's one-call executive synthesis.
// It is valid only after the structured event evidence runtime.
const CompiledRuntimeExecutiveBriefV1 = workflowruntime.ExecutiveBriefV1

// CompiledRuntimeToolSnapshotV2 selects the Source-free task Tool snapshot
// protocol. Its versioned Tool execution and observation provenance Activities
// serve both recurring and command-bound manual task runs.
const CompiledRuntimeToolSnapshotV2 = workflowruntime.CompiledToolSnapshotV2

func IsCompiledToolRuntimeV2(version string) bool {
	return workflowruntime.IsCompiledToolV2(version)
}

func IsCompiledRuntimeV1(version string) bool {
	return workflowruntime.IsCompiledV1(version)
}

func HasRunOutcomeV1(version string) bool {
	return version == CompiledRuntimeRunOutcomeV1 ||
		version == CompiledRuntimeCanonicalBriefV1 ||
		version == CompiledRuntimeStructuredInsightV1 ||
		version == CompiledRuntimeStructuredEventEvidenceV1 ||
		version == CompiledRuntimeExecutiveBriefV1
}

func HasCanonicalBriefV1(version string) bool {
	return version == CompiledRuntimeCanonicalBriefV1 ||
		version == CompiledRuntimeStructuredInsightV1 ||
		version == CompiledRuntimeStructuredEventEvidenceV1 ||
		version == CompiledRuntimeExecutiveBriefV1
}

func HasExecutiveBriefV1(version string) bool {
	return version == CompiledRuntimeExecutiveBriefV1
}

// CompiledRunInputV1 carries the trusted stable scope copied from the Schedule
// Action plus the sealed reference returned by PrepareRun. It contains no
// approved definition or runtime-policy body and is therefore safe for
// Temporal history. UserID stays on each existing Activity input to preserve
// its replay-compatible wire shape; consumers bind both values independently.
type CompiledRunInputV1 struct {
	TenantID int64          `json:"tenant_id"`
	TaskID   string         `json:"task_id"`
	Snapshot RunSnapshotRef `json:"snapshot"`
}

// PushScope 推送范围过滤。
//
// SourceIDs 只允许进一步收窄当前任务的冻结 fetch target 集合，绝不能扩大到
// 任务之外。零值表示使用该任务完整冻结范围。
type PushScope struct {
	SourceIDs []int64 `json:"source_ids,omitempty"` // 空=任务完整范围；非空=进一步收窄
	TopN      int     `json:"top_n,omitempty"`      // 每批最多推几条；0=defaultTopN
}

// GeneratedCard 是生成解读后的推送载荷（CardGen→Push 之间传递）。
// 保留 Scored 是因为 Push 建 Delivery 时要回填 score 与 content_item_id。
type GeneratedCard struct {
	Scored types.ScoredItem `json:"scored"`
	// BodyMD 解读正文 markdown（不含阅读原文链接，由构卡函数加）。最终卡片 JSON 由 Push 在拿到
	// delivery_id 后经注入的 buildCard 构造。json tag 沿用 "card_json"：换 tag
	// 会让停在 CardGen 之后的 in-flight workflow 重放时解出空正文、静默推空卡
	// （契约 §8.2 重放兼容）。
	BodyMD string `json:"card_json"`
	// Structured is absent on every historical/runtime-v1 payload. The
	// omitempty tag keeps their encoded wire shape stable; a future versioned
	// CardGen Activity may populate it without changing legacy replay.
	Structured *types.StructuredInsightV1 `json:"structured,omitempty"`
	// EventEvidence is populated only by the separately versioned P2-B1
	// CardGen Activity. It freezes the bounded corpus and inventory metadata
	// that the later Brief Activity independently validates.
	EventEvidence []cardgenpkg.EventEvidenceSourceV1 `json:"event_evidence,omitempty"`
}
