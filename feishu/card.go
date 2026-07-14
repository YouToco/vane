package feishu

import "encoding/json"

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

// vane_action 的两个合法取值：确认卡按钮 value 与回调分发共用（契约 §9），
// 常量化避免构卡与解析两处魔法字符串漂移。
const (
	cardActionConfirm = "confirm"
	cardActionCancel  = "cancel"
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
