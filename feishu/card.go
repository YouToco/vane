package feishu

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/YouToco/vane/feedback"
	"github.com/YouToco/vane/types"
)

// cardTitle 是所有回复卡片的统一标题：让用户在会话流里一眼认出这是 Vane 的回复。
const cardTitle = "见微 Vane"

// BuildReplyCard 把 markdown 正文包装成飞书交互卡片（JSON 2.0 schema）的
// 序列化字符串，供 Im.Message.Reply / Create 的 Content 字段直接使用
// （SDK 要求 Content 恒为 JSON 字符串而非对象）。
//
// 选 2.0 schema 而非 v1 的原因：markdown 在 2.0 里是 body.elements 下的
// 一等元素，不必像 v1 那样绕 div+lark_md 的嵌套写法（见 M2 事实基准）。
func BuildReplyCard(markdown string) string {
	card := map[string]any{
		"schema": "2.0",
		"config": map[string]any{},
		"header": map[string]any{
			"title": map[string]any{"tag": "plain_text", "content": cardTitle},
		},
		"body": map[string]any{
			"elements": []any{
				map[string]any{"tag": "markdown", "content": markdown},
			},
		},
	}
	// map[string]any + 纯字符串值不会触发 Marshal 错误，忽略 err 是安全的。
	raw, _ := json.Marshal(card)
	return string(raw)
}

// vane_action 的三个合法取值：卡片按钮 value 与回调分发共用（契约 §9、M5 §10.1），
// 常量化避免构卡与解析两处魔法字符串漂移。
// confirm/cancel 走 M4 的确认卡链路（pending_actions）；fb 走 M5 的推送卡反馈链路
// （feedbacks），两条链路在 onCardAction 里按此值分发，互不影响。
const (
	cardActionConfirm        = "confirm"
	cardActionCancel         = "cancel"
	cardActionFeedback       = "fb"
	cardActionFeedbackReason = "fbr" // form 提交：misjudged + detail
)

// BuildConfirmCard 构造写操作确认卡（JSON 2.0 schema）：markdown 正文展示
// 工具名+参数摘要，确认/取消两个 callback 按钮。按钮 value 只携带
// vane_action 与 action_id——参数以服务端 pending_actions 为准，
// 杜绝客户端篡改（契约 §10）。
//
// 2.0 下按钮交互挂在 behaviors（v1 的 value 直挂按钮写法不生效）；
// 两个按钮经 column_set 并排，避免默认纵向堆叠的松散观感。
func BuildConfirmCard(summary, actionID string) string {
	confirmBtn := map[string]any{
		"tag":   "button",
		"type":  "primary",
		"width": "default",
		"text":  map[string]any{"tag": "plain_text", "content": "确认"},
		"behaviors": []any{map[string]any{
			"type":  "callback",
			"value": map[string]any{"vane_action": cardActionConfirm, "action_id": actionID},
		}},
	}
	cancelBtn := map[string]any{
		"tag":   "button",
		"type":  "default",
		"width": "default",
		"text":  map[string]any{"tag": "plain_text", "content": "取消"},
		"behaviors": []any{map[string]any{
			"type":  "callback",
			"value": map[string]any{"vane_action": cardActionCancel, "action_id": actionID},
		}},
	}
	card := map[string]any{
		"schema": "2.0",
		"config": map[string]any{},
		"header": map[string]any{
			"title": map[string]any{"tag": "plain_text", "content": cardTitle},
		},
		"body": map[string]any{
			"elements": []any{
				map[string]any{"tag": "markdown", "content": summary},
				map[string]any{
					"tag": "column_set",
					"columns": []any{
						map[string]any{"tag": "column", "width": "auto", "elements": []any{confirmBtn}},
						map[string]any{"tag": "column", "width": "auto", "elements": []any{cancelBtn}},
					},
				},
			},
		},
	}
	raw, _ := json.Marshal(card)
	return string(raw)
}

// feedbackButtons 推送卡的三个 emoji 反馈按钮（卡片改版：从四文字按钮精简为三图标）。
// 误判入口折叠进 👎 后的 form：点 👎 = not_interested，form 提交 = misjudged + detail。
var feedbackButtons = []struct {
	action types.FeedbackAction
	label  string
}{
	{types.FeedbackActionInterested, "👍"},
	{types.FeedbackActionNotInterested, "👎"},
	{types.FeedbackActionDeepDive, "🔍 深挖"},
}

// BuildDeliveryCard 构造带反馈按钮的推送解读卡（JSON 2.0 schema）。
// 首发（Push 活动，零值 state）与按钮点击后的原地更新共用本函数：
// 同一构卡函数 + 不同 state = 同一张卡的不同版本，解读正文 bodyMD 永远原样保留。
//
// 卡片改版（2026-07-15 设计定稿）：header 展示内容标题 + 栏目/来源 subtitle，
// body 含 ⚡ 分数标签、解读正文、emoji 反馈按钮、条件 form。
func BuildDeliveryCard(input feedback.CardInput) string {
	idStr := strconv.FormatInt(input.DeliveryID, 10)

	// --- header ---
	title := input.Title
	if title == "" {
		title = cardTitle
	}
	header := map[string]any{
		"title": map[string]any{"tag": "plain_text", "content": title},
	}
	if sub := buildSubtitle(input); sub != "" {
		header["subtitle"] = map[string]any{"tag": "plain_text", "content": sub}
	}
	if input.Score > 0 {
		header["text_tag_list"] = []any{
			map[string]any{
				"tag":   "text_tag",
				"text":  map[string]any{"tag": "plain_text", "content": "⚡ " + strconv.Itoa(input.Score)},
				"color": "orange",
			},
		}
	}

	// --- body ---
	elements := make([]any, 0, 6)
	elements = append(elements, map[string]any{"tag": "markdown", "content": input.BodyMD})

	// 反馈按钮行：[阅读原文] [👍] [👎] [🔍 深挖]
	btnColumns := make([]any, 0, 4)
	if u := strings.TrimSpace(input.URL); u != "" {
		btnColumns = append(btnColumns, map[string]any{
			"tag": "column", "width": "auto",
			"elements": []any{map[string]any{
				"tag": "button", "type": "primary", "width": "default",
				"text": map[string]any{"tag": "plain_text", "content": "阅读原文"},
				"behaviors": []any{map[string]any{
					"type":        "open_url",
					"default_url": u,
				}},
			}},
		})
	}
	for _, b := range feedbackButtons {
		btnColumns = append(btnColumns, map[string]any{
			"tag": "column", "width": "auto",
			"elements": []any{map[string]any{
				"tag": "button", "type": "default", "width": "default",
				"text": map[string]any{"tag": "plain_text", "content": b.label},
				"behaviors": []any{map[string]any{
					"type": "callback",
					"value": map[string]any{
						"vane_action": cardActionFeedback,
						"fb":          string(b.action),
						"delivery_id": idStr,
					},
				}},
			}},
		})
	}
	elements = append(elements, map[string]any{"tag": "column_set", "columns": btnColumns})

	// 状态行（有反馈后出现）
	if line := feedbackStateLine(input.State); line != "" {
		elements = append(elements, map[string]any{"tag": "markdown", "content": line})
	}

	// 点 👎 后出现 form（可跳过的原因收集）；误判落库后收起——
	// 状态行已有「⚠️ 已标记误判」，form 留着会让人以为还能再提。
	if input.State.Preference == types.FeedbackActionNotInterested && !input.State.Misjudged {
		elements = append(elements, map[string]any{
			"tag":  "form",
			"name": "feedback_reason",
			"elements": []any{
				map[string]any{
					"tag":  "input",
					"name": "reason",
					"placeholder": map[string]any{
						"tag":     "plain_text",
						"content": "哪里不对？说一句，下次就准了（可跳过）",
					},
					"max_length": 500,
				},
				map[string]any{
					"tag":  "button",
					// form 内的交互组件必须有 name（缺失报 200530）；且 form 容器要求
					// 至少一个 form_action_type=submit 的提交按钮——缺失时整卡非法：
					// 发消息被拒（300123），作为回调响应返回则客户端报 200673
					//（"返回了错误的卡片"），按钮永久转圈且回调被重推。
					"name":             "submit_reason",
					"form_action_type": "submit",
					"text":             map[string]any{"tag": "plain_text", "content": "提交"},
					"type":             "primary",
					"behaviors": []any{map[string]any{
						"type": "callback",
						"value": map[string]any{
							"vane_action": cardActionFeedbackReason,
							"delivery_id": idStr,
						},
					}},
				},
			},
		})
	}

	card := map[string]any{
		"schema": "2.0",
		"config": map[string]any{},
		"header": header,
		"body":   map[string]any{"elements": elements},
	}
	raw, _ := json.Marshal(card)
	return string(raw)
}

// buildSubtitle 拼装 header subtitle：{emoji} {栏目} · {域名} · {相对时间}。
// 任一字段缺失则省略对应段（subtitle 始终可读）。
func buildSubtitle(input feedback.CardInput) string {
	var parts []string
	if input.SourceTitle != "" {
		prefix := platformEmoji(input.Platform)
		parts = append(parts, prefix+" "+input.SourceTitle)
	}
	if d := domainFromURL(input.URL); d != "" {
		parts = append(parts, d)
	}
	if input.PublishedAt != nil {
		parts = append(parts, relativeTime(*input.PublishedAt))
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " · ")
}

// platformEmoji 按信源平台派生 emoji 前缀。
func platformEmoji(p types.Platform) string {
	switch p {
	case types.PlatformWeb:
		return "🤖"
	case types.PlatformXHS:
		return "📕"
	case types.PlatformX:
		return "🐦"
	default:
		return "📰"
	}
}

// domainFromURL 从 URL 提取主域名（去 www.）。
func domainFromURL(rawURL string) string {
	if rawURL == "" {
		return ""
	}
	// 简化解析：找 :// 后取到下一个 /
	idx := strings.Index(rawURL, "://")
	if idx < 0 {
		return ""
	}
	host := rawURL[idx+3:]
	if slash := strings.Index(host, "/"); slash > 0 {
		host = host[:slash]
	}
	if port := strings.LastIndex(host, ":"); port > 0 {
		host = host[:port]
	}
	host = strings.TrimPrefix(host, "www.")
	return host
}

// relativeTime 格式化相对时间（今天/昨天/N 天前）。
func relativeTime(t time.Time) string {
	days := int(time.Since(t).Hours() / 24)
	switch {
	case days <= 0:
		return "今天"
	case days == 1:
		return "昨天"
	case days < 30:
		return strconv.Itoa(days) + " 天前"
	case days < 365:
		return strconv.Itoa(days/30) + " 月前"
	default:
		return strconv.Itoa(days/365) + " 年前"
	}
}

// feedbackStateLine 渲染状态行（卡片改版措辞对齐"已记录"）。
// 零值 state 返回空串（无状态行）。
func feedbackStateLine(st feedback.CardState) string {
	var parts []string
	switch st.Preference {
	case types.FeedbackActionInterested:
		parts = append(parts, "✅ 已记录：感兴趣")
	case types.FeedbackActionNotInterested:
		parts = append(parts, "🚫 已记录：不感兴趣")
	}
	if st.Misjudged {
		parts = append(parts, "⚠️ 已标记误判")
	}
	if st.DeepDiveRequested {
		parts = append(parts, "📖 已请求深度解读（结果以回复消息送达）")
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " · ")
}
