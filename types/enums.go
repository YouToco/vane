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
	CapContents  Capability = "contents"   // 监控指定 URL 的内容变化（Exa /contents 抓取）
)

// Kind 内容种类（content_items.kind）。当前只有 article 一种——page_watch 的
// change 已下线（改由 Exa fetch 覆盖）。仍保留 typed 枚举与 kind 列：将来若再引入
// 非 article 的内容种类（下游 pipeline 需要区别对待），这套承载不必重建。
//
// 【强制语义，M6 契约 §3.3.1，仍生效】Kind 必须活着走完 DB 往返：
// **任何返回 []types.ContentItem 或 *types.ContentItem 的 store 方法，
// 其 SELECT 列清单与 Scan 都必须带 kind。** 抓取器在构造 item 处显式赋 Kind，
// fetcher.finalize 对空 Kind 一律拒绝（契约 §7.2(b)）；写入侧忘赋值 → Go 零值 ""
// 覆盖 DB 的 DEFAULT 'article' 的坑真实发生过（2026-07-16，012 回填）。
type Kind string

const (
	KindArticle Kind = "article" // 一篇内容（当前唯一种类）
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

// BatchStatus 推送批次状态（push_batches.status）。
//
// 真实生命周期只有两条：
//   - 有内容可推：pending（Push 活动内短暂存在）→ done | failed
//   - 无内容可推：直接 empty（009 起，见下）——**这是正常终态，不是失败**。
//
// "pushing" 已于 009 删除：它从 001 起就是死枚举，全仓零赋值、无任何 SQL 写过它，
// 库里永远不会出现。PR1 只在注释里标注了它（store/push_batches.go），刻意把删除
// 留给本次写入侧变更——在新增 empty 这个**真状态**的同时留着一个**假状态**，
// 会让下一个人把"卡在 pushing"当成需要排查的形状，而那个形状根本不存在。
type BatchStatus string

const (
	BatchStatusPending BatchStatus = "pending" // 已创建待处理（仅 Push 活动内部短暂存在）
	BatchStatusDone    BatchStatus = "done"    // 至少推成一张卡
	BatchStatusFailed  BatchStatus = "failed"  // 建了批次但一张都没推成
	// BatchStatusEmpty 无内容可推的正常终态（009 / M5 契约 §16 修订记录「空批次缺口」）。
	// 与 failed 的区别是根本性的：failed 是"该推却推不出去"（有 cards、推送炸了），
	// empty 是"跑完了确实没东西可推"（pipeline 在 Push 之前就没候选了）。
	// 把两者混成一个值，就等于把"飞书挂了"和"今天没新闻"报成同一件事。
	// 具体停在哪一步由 push_batches.exit_gate 承载（见 BatchExitGate）。
	BatchStatusEmpty BatchStatus = "empty"
)

// BatchExitGate 是"pipeline 从哪个闸门提前退出"（push_batches.exit_gate）。
//
// 为什么与 status 分列两列而不是把 status 拆成 empty_fetch/empty_dedup/…：
// 两者是**正交的两个维度**——status 回答"这批的结局是什么"（现有消费方：
// 看板徽标、探针、deliveries 关联都按它分支），exit_gate 回答"为什么是这个结局"。
// 塞进 status 会让每个现有 status 消费方都要认识 5 个新值，且"empty"这个结局
// 本身反而没法一句话查（得 IN 五个值）。
//
// 空串 = 没有提前退出（跑到了 Push）。009 之前的历史行全部如此，故列默认 ”
// 恰好就是它们的真实语义，无需回填。
//
// 取值与 workflow/workflow.go 的五处提前退出一一对应；顺序即 pipeline 顺序，
// 故 exit_gate 天然可比较"死得多早"。注意闸门名 = 产出该结果的**上一步活动名**
// （不是"下一步没跑成"）：BatchExitGateDedup 意为"Dedup 跑完后没剩下东西"。
type BatchExitGate string

const (
	BatchExitGateFetch   BatchExitGate = "fetch"   // 抓取后无候选：压根没抓到新内容
	BatchExitGateDedup   BatchExitGate = "dedup"   // 去重后无候选：抓到了但全是重复
	BatchExitGateScore   BatchExitGate = "score"   // 打分后无候选
	BatchExitGateSelect  BatchExitGate = "select"  // 择优后无候选
	BatchExitGateCardGen BatchExitGate = "cardgen" // 卡片生成后无候选
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
