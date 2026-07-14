package types

// 本文件集中定义 typed string 枚举，与数据库 TEXT 列一一对应。
// 用 typed string 而非 iota：DB 存文本、JSON 序列化直观、增值不破坏兼容。

// SourceType 信源类型（sources.type）。
type SourceType string

const (
	SourceTypeRSS       SourceType = "rss"        // RSS / Atom 订阅源
	SourceTypeExa       SourceType = "exa"        // Exa 语义搜索（按 query 抓最新结果）
	SourceTypeTikHubXHS SourceType = "tikhub_xhs" // TikHub 小红书接口
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
