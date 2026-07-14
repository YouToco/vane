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
