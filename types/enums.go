package types

// 本文件集中定义 typed string 枚举，与数据库 TEXT 列一一对应。
// 用 typed string 而非 iota：DB 存文本、JSON 序列化直观、增值不破坏兼容。

// Platform 内容平台（sources.platform）。
type Platform string

const (
	PlatformWeb Platform = "web" // 开放网页：身份=url，协议=HTTP
	PlatformX   Platform = "x"   // X / Twitter：身份=tweet_id
	PlatformXHS Platform = "xhs" // 小红书：身份=note_id
)

// Capability 从平台取什么（sources.capability）。
type Capability string

const (
	CapFeed      Capability = "feed"       // 订阅 RSS/Atom feed
	CapSearch    Capability = "search"     // 关键词/语义搜索
	CapUserPosts Capability = "user_posts" // 某账号的新发布
	CapPageWatch Capability = "page_watch" // 页面变化监控
)

// Kind 内容种类（content_items.kind）。决定下游 pipeline 怎么对待它——
// Dedup 的 simhash 近似去重对 change 是灾难性的（设计目的「改动少量文字仍判重复」
// 与 change 的信号「改动少量文字」直接对立）。
type Kind string

const (
	KindArticle Kind = "article" // 一篇内容（默认；存量全是这个）
	KindChange  Kind = "change"  // 一次变化事件（page_watch 产出）
)

// SourceType 旧信源类型枚举，008 迁移后 DB 不再有 type 列。
// 仅供 sourcespec.BuildLegacy 和 api 兼容层使用，不得出现在新代码中。
type SourceType string

const (
	SourceTypeRSS       SourceType = "rss"
	SourceTypeExa       SourceType = "exa"
	SourceTypeTikHubXHS SourceType = "tikhub_xhs"
)

// SourceStatus 信源状态（sources.status）。
type SourceStatus string

const (
	SourceStatusActive   SourceStatus = "active"   // 正常抓取
	SourceStatusDisabled SourceStatus = "disabled" // 已停用（手动或连续失败自动停用）
)

// SubscriptionStatus 订阅状态（subscriptions.status）。
type SubscriptionStatus string

const (
	SubscriptionStatusActive   SubscriptionStatus = "active"   // 生效中
	SubscriptionStatusInactive SubscriptionStatus = "inactive" // 已取消（保留记录）
)

// BatchStatus 推送批次状态（push_batches.status），
// 取值与规格 Step 5 CHECK 约束对齐：pending / pushing / done / failed。
type BatchStatus string

const (
	BatchStatusPending BatchStatus = "pending" // 已创建待处理
	BatchStatusPushing BatchStatus = "pushing" // 推送进行中
	BatchStatusDone    BatchStatus = "done"    // 全部完成
	BatchStatusFailed  BatchStatus = "failed"  // 失败终态
)

// DeliveryStatus 单条投递状态（deliveries.status）。
type DeliveryStatus string

const (
	DeliveryStatusPending DeliveryStatus = "pending" // 待发送
	DeliveryStatusSent    DeliveryStatus = "sent"    // 已发送（sent_at 已写入）
	DeliveryStatusFailed  DeliveryStatus = "failed"  // 发送失败
)

// FeedbackAction 用户反馈动作（feedbacks.action）。
type FeedbackAction string

const (
	FeedbackActionInterested    FeedbackAction = "interested"     // 感兴趣
	FeedbackActionNotInterested FeedbackAction = "not_interested" // 不感兴趣
	FeedbackActionMisjudged     FeedbackAction = "misjudged"      // 推送判断失误
	FeedbackActionDeepDive      FeedbackAction = "deep_dive"      // 想深入了解
	FeedbackActionQuestion      FeedbackAction = "question"       // 提问 / 文字追问
)

// RefType LLM 调用的多态关联对象类型（llm_calls.ref_type），
// 取值与规格 Step 2 LLMCall 实体对齐。
type RefType string

const (
	RefTypePushBatch   RefType = "push_batch"   // 关联推送批次
	RefTypeFeedback    RefType = "feedback"     // 关联文字反馈解读
	RefTypeContentItem RefType = "content_item" // 关联单条内容（摘要等）
	RefTypeProfile     RefType = "profile"      // 关联用户画像（画像演化记账）
)

// AgentSessionStatus agent 会话状态（agent_sessions.status，M4 契约 §3）。
type AgentSessionStatus string

const (
	AgentSessionStatusActive  AgentSessionStatus = "active"  // 进行中，TTL 窗口内可续聊
	AgentSessionStatusExpired AgentSessionStatus = "expired" // 超时过期（读取路径惰性翻转）
	AgentSessionStatusClosed  AgentSessionStatus = "closed"  // 主动关闭
)

// PendingActionStatus 待确认动作状态（pending_actions.status，M4 契约 §3）。
type PendingActionStatus string

const (
	PendingActionStatusPending   PendingActionStatus = "pending"   // 待用户确认
	PendingActionStatusExecuted  PendingActionStatus = "executed"  // 用户已确认，Claim 原子置位
	PendingActionStatusCancelled PendingActionStatus = "cancelled" // 用户点了取消
	PendingActionStatusExpired   PendingActionStatus = "expired"   // 超时未确认
)
