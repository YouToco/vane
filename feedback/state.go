// Package feedback 处理推送卡反馈按钮点击与追问上下文包装（M5 契约 §10/§11）。
// 依赖边界：只依赖 llm/types 与调用方注入的窄接口，不得 import
// feishu/agent/store/workflow——workflow 与 feishu 反向引用本包（CardState、
// FeedbackRunner），import 环由这条边界封死。
package feedback

import (
	"time"

	"github.com/YouToco/vane/types"
)

// CardState 推送卡状态行的渲染输入（契约 §10.2），由 feishu.BuildDeliveryCard 消费。
// 三字段均以库内查询为准、最终一致：同卡并发点击时两版卡片以飞书到达序为准，
// 短暂缺项在下次点击时自愈。
type CardState struct {
	// Preference 最新态度：零值 ""（未表态）/ interested / not_interested。
	// 查询方恒传 {interested, not_interested} 双值集合取最新一条（审查 F5：
	// 传单值会命中旧行、复刻被否决的唯一索引 bug）。
	Preference types.FeedbackAction
	// Misjudged 是否已标记误判（独立于态度、可并存，MVP 不可撤销）。
	Misjudged bool
	// DeepDiveRequested 是否已请求深度解读（此行定格后不再变，生成失败也不回退）。
	DeepDiveRequested bool
}

// CardInput 构卡函数的全量输入（卡片改版扩展签名，替代原 (bodyMD, deliveryID, state)
// 三参数）。反馈重建路径按 best-effort 查库填充：内容/源查不到时字段为零值，
// 构卡函数据此降级渲染（标题空则 header 回退默认、subtitle 缺字段则省略对应段）。
type CardInput struct {
	BodyMD      string
	DeliveryID  int64
	State       CardState
	Title       string         // content_items.title → header title
	Score       int            // round(deliveries.score) → ⚡ tag
	URL         string         // content_items.url → 阅读原文按钮
	SourceTitle string         // sources.title → subtitle 栏目
	Platform    types.Platform // sources.platform → subtitle emoji
	PublishedAt *time.Time     // content_items.published_at → subtitle 相对时间
}
