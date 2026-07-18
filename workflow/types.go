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
	"github.com/YouToco/vane/types"
)

// defaultTopN 是每批默认推送条数（PushScope.TopN 为 0 时取此值，见规格 B1）。
const defaultTopN = 5

// PushParams 是 PushPipelineWorkflow 的唯一入参，也是 Schedule.Action.Args 的元素。
// 铁律：只放稳定标识符（UserID+Scope），绝不放候选内容 / batch_id——每次触发时
// 由 Fetch Activity 在"触发时刻"现查订阅源，否则定时任务会反复推送陈旧内容。
type PushParams struct {
	UserID int64     `json:"user_id"`
	Scope  PushScope `json:"scope"`
	// NLDesc 触发本次推送的调度的自然语言描述（聚合卡 header 的任务名）。
	// 存量调度的 Temporal Action 里没有本字段，解出零值空串——聚合卡落兜底标题，
	// 行为安全；新建调度由 scheduler.CreatePush 填入。
	NLDesc string `json:"nl_desc,omitempty"`
}

// PushScope 推送范围过滤。
//
// ⚠️ SourceIDs 目前只约束「本轮去抓哪些源」（Fetch Activity 的 filterSources），
// 不约束候选检索——候选一律走 ListUnpushedByUser 捞该用户全部订阅的未投递内容。
// 即：非空 SourceIDs = 只抓这些源，但推的是所有源的积压，不是「只推这些源」。
// 当前生产调用方（agent push_now、前端）都传零值 scope，故此语义差异未暴露；
// 若将来要「真正只推指定源」，需给 ListUnpushedByUser 加 sourceIDs 过滤参数，
// 别指望改这里的注释就够（见代码审计 D-4）。
type PushScope struct {
	SourceIDs []int64 `json:"source_ids,omitempty"` // 空=全部订阅；非空=只【抓取】这些源（见上：不过滤候选）
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
}
