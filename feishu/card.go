package feishu

import (
	"encoding/json"
	"strconv"
	"strings"

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
	cardActionConfirm  = "confirm"
	cardActionCancel   = "cancel"
	cardActionFeedback = "fb"
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

// feedbackButtons 是推送卡的四个反馈按钮文案（M5 契约 §10.2），
// 顺序即卡片上的左右顺序：两个 P0 态度在前，两个 P1 在后。
var feedbackButtons = []struct {
	action types.FeedbackAction
	label  string
}{
	{types.FeedbackActionInterested, "感兴趣"},
	{types.FeedbackActionNotInterested, "不感兴趣"},
	{types.FeedbackActionMisjudged, "误判"},
	{types.FeedbackActionDeepDive, "深度解读"},
}

// BuildDeliveryCard 构造带反馈按钮的推送解读卡（JSON 2.0 schema）。
// 首发（Push 活动，零值 state）与按钮点击后的原地更新共用本函数：
// 同一构卡函数 + 不同 state = 同一张卡的不同版本，解读正文 bodyMD 永远原样保留。
// 这与确认卡"整卡替换成结果文本"刻意不同——确认卡是一次性动作的载体，
// 推送卡是长期驻留的内容本身，把正文换掉等于把用户读的东西弄丢。
//
// 按钮 value 只携带 fb 与 delivery_id，且 delivery_id 恒为字符串：SDK 把 value
// 解成 map[string]interface{}，JSON number 会变 float64，大 id 有精度隐患。
// 值本身只当线索——动作合法性、归属、正文全部以服务端库内为准（契约 §10.1）。
func BuildDeliveryCard(bodyMD string, deliveryID int64, st feedback.CardState) string {
	idStr := strconv.FormatInt(deliveryID, 10)
	columns := make([]any, 0, len(feedbackButtons))
	for _, b := range feedbackButtons {
		columns = append(columns, map[string]any{
			"tag":   "column",
			"width": "auto",
			"elements": []any{map[string]any{
				"tag":   "button",
				"type":  "default",
				"width": "default",
				"text":  map[string]any{"tag": "plain_text", "content": b.label},
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

	elements := []any{
		map[string]any{"tag": "markdown", "content": bodyMD},
		map[string]any{"tag": "column_set", "columns": columns},
	}
	// 状态行只在有反馈后出现：首发卡与 M5 之前的观感一致，只多一排按钮。
	if line := feedbackStateLine(st); line != "" {
		elements = append(elements, map[string]any{"tag": "markdown", "content": line})
	}

	card := map[string]any{
		"schema": "2.0",
		"config": map[string]any{},
		"header": map[string]any{
			"title": map[string]any{"tag": "plain_text", "content": cardTitle},
		},
		"body": map[string]any{"elements": elements},
	}
	raw, _ := json.Marshal(card)
	return string(raw)
}

// feedbackStateLine 渲染状态行（M5 契约 §10.2）：零值 state 返回空串（无状态行）。
// 按钮不置灰也不消失——态度可改是产品语义，而 2.0 按钮没有原生选中态，
// 用状态行表达当前态度比伪造按钮样式诚实。
// 深度解读的措辞刻意无时态（"已请求"而非"生成中"）：延迟更新卡片需要 cardkit API，
// MVP 不引入，此行一旦生成就不会再变——写"生成中"会永久说谎。
func feedbackStateLine(st feedback.CardState) string {
	var parts []string
	switch st.Preference {
	case types.FeedbackActionInterested:
		parts = append(parts, "✅ 已反馈：感兴趣")
	case types.FeedbackActionNotInterested:
		parts = append(parts, "🚫 已反馈：不感兴趣")
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
