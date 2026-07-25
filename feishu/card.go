package feishu

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/YouToco/vane/feedback"
	"github.com/YouToco/vane/promptguard"
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
		// A6 的耐久创建回执会通过 Im.Message.Patch 原地兑现最终结果。
		// 飞书只允许更新显式声明为共享卡片的消息；第一次 Patch 已在服务端
		// 成功、但响应丢失时，dispatcher 还必须能对同一消息安全重试。
		"config": map[string]any{"update_multi": true},
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

// BuildConfirmCard 构造写操作确认卡（JSON 2.0 schema）：plain_text 正文展示
// 工具名+参数摘要，确认/取消两个 callback 按钮。摘要可能包含共享信源标题等
// 不可信元数据，绝不能作为 Markdown 解释，否则能视觉伪造确认范围。按钮 value 只携带
// vane_action 与 action_id——参数以服务端 pending_actions 为准，
// 杜绝客户端篡改（契约 §10）。
//
// 2.0 下按钮交互挂在 behaviors（v1 的 value 直挂按钮写法不生效）；
// 两个按钮经 column_set 并排，避免默认纵向堆叠的松散观感。
func BuildConfirmCard(summary, actionID string) string {
	// plain_text prevents Markdown interpretation, but Unicode bidi/Cf controls
	// are still honored by renderers and can visually reorder trusted labels.
	// Apply this at the final card boundary so every mutating tool is covered.
	summary = promptguard.StripInvisible(summary)
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
		// 确认后最终结果原地更新这张卡，故首发时就必须声明 update_multi。
		// 该能力不能等到终态卡才补：未声明的原卡根本不可 Patch。
		"config": map[string]any{"update_multi": true},
		"header": map[string]any{
			"title": map[string]any{"tag": "plain_text", "content": cardTitle},
		},
		"body": map[string]any{
			"elements": []any{
				map[string]any{
					"tag":  "div",
					"text": map[string]any{"tag": "plain_text", "content": summary},
				},
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

// feedbackButtons 推送卡的三个 emoji 反馈按钮。历史 callback value 仍用
// not_interested，但服务端把 👎 解释为“打开问题面板”，点击本身不落反馈。
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

	if input.State.BadFeedbackOpen && !input.State.Misjudged {
		elements = append(elements, feedbackProblemForm(idStr, false))
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

var feedbackReasonOptions = []struct {
	code  types.FeedbackReason
	label string
}{
	{types.FeedbackReasonOutdated, "过时或超出任务时间范围"},
	{types.FeedbackReasonNotRelevant, "与任务无关"},
	{types.FeedbackReasonDuplicate, "重复或已经推过"},
	{types.FeedbackReasonFactWrong, "事实或结论错误"},
	{types.FeedbackReasonPoorSource, "来源或证据质量差"},
	{types.FeedbackReasonOther, "其他（请填写说明）"},
}

// feedbackProblemForm uses only input and submit buttons already exercised by
// production cards. Each reason button submits exactly one fixed code; the
// shared detail is optional except for “other”.
func feedbackProblemForm(idStr string, aggregate bool) map[string]any {
	formName, inputName, submitPrefix := "feedback_problem", "detail", "submit_reason_"
	if aggregate {
		formName = "fbr_" + idStr
		inputName = "detail_" + idStr
		submitPrefix = "submit_" + idStr + "_"
	}
	elements := []any{
		map[string]any{
			"tag":  "markdown",
			"content": "**这条推送哪里有问题？** 请选择一个原因；如需补充，可填写说明。",
		},
		map[string]any{
			"tag":  "input",
			"name": inputName,
			"placeholder": map[string]any{
				"tag": "plain_text", "content": "补充说明（选择“其他”时必填）",
			},
			"max_length": 500,
		},
	}
	for _, option := range feedbackReasonOptions {
		elements = append(elements, map[string]any{
			"tag":              "button",
			"name":             submitPrefix + string(option.code),
			"form_action_type": "submit",
			"text":             map[string]any{"tag": "plain_text", "content": option.label},
			"type":             "default",
			"behaviors": []any{map[string]any{
				"type": "callback",
				"value": map[string]any{
					"vane_action": cardActionFeedbackReason,
					"delivery_id": idStr,
					"reason_code": string(option.code),
				},
			}},
		})
	}
	return map[string]any{"tag": "form", "name": formName, "elements": elements}
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

// cardTZ 是卡片相对日期的用户时区。产品面向中文用户、调度默认同为 Asia/Shanghai
// （scheduler.ScheduleSpec.TZ 缺省值）。用 FixedZone 而非 LoadLocation("Asia/Shanghai")：
// 中国标准时间自 1991 年起恒为 UTC+8 无 DST，二者语义等价，而 FixedZone 零 tzdata
// 依赖——LoadLocation 在无 zoneinfo 的最小容器里会失败回退，日期悄悄偏 8 小时
// （审查 UNCHECKED 项收编为结构性免疫）。
var cardTZ = time.FixedZone("UTC+8", 8*3600)

// relativeTime 格式化相对时间（今天/昨天/N 天前），按**用户时区的日历日**计算。
//
// bug 狩猎 2026-07-19 MEDIUM 修复：原实现 time.Since(t)/24h 是"流逝时长"语义——
// 北京 23:00 发布的文章，用户次日 01:00 看卡片流逝仅 2h 仍标"今天"，但用户感知
// 已跨日。红线 6 说"换算只在前端"，而飞书卡片没有前端层：这里生成的就是终端
// 展示文本，时区换算必须在此完成。
func relativeTime(t time.Time) string {
	return relativeTimeAt(t, time.Now())
}

// relativeTimeAt 是 relativeTime 的可测内核：now 由调用方注入，单测能钉死跨日边界
// （23:59 发布 vs 00:01 观看必须是"昨天"——正是本次修复的那条 bug 的形状）。
func relativeTimeAt(t, now time.Time) string {
	now = now.In(cardTZ)
	tt := t.In(cardTZ)
	nowDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, cardTZ)
	tDay := time.Date(tt.Year(), tt.Month(), tt.Day(), 0, 0, 0, 0, cardTZ)
	days := int(nowDay.Sub(tDay).Hours() / 24)
	switch {
	case days <= 0: // 同日；未来时间戳（脏数据）也归"今天"，不显示负数天
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
