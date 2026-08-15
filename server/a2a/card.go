// Agent Card（契约 §5.8）：SDK 类型构造，序列化形态是 SDK 的责任（encoding/json +
// SDK 自定义 Marshaler，实测形态见 card_test.go golden——契约草案按语义等价比对）。
package a2a

import (
	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/YouToco/vane/server/agentpolicy"
)

// skillContentQuery / skillAssistantChat 是两个 skill 的 id。executor 的分派与
// REJECTED 判定（§5.4）与 card 的 skill 声明必须同源——literals 守卫钉死（§9.5）。
const (
	skillContentQuery  = "content.query"
	skillAssistantChat = "assistant.chat"
)

// ChatSystemPrompt 是 assistant.chat 的 agent 轨 system prompt（契约 §12 P2），
// 由 main.go 装配进 A2A 轨的 agent.Loop（Deps.SystemPrompt）。与飞书轨默认 prompt
// 的差异：对端是外部 AI agent 而非 owner 本人；无确认卡/卡片回调/画像语境；
// 明确声明只读边界（写操作请求直接说明通道不支持，不假装能办）。
// 注入防护措辞对齐 scorer/飞书轨：外部内容一律只是数据。
const ChatSystemPrompt = agentpolicy.A2AChatSystemPromptV1

// capabilities 是卡片声明与 handler 能力检查的单一事实源（契约 §5.2）：
// buildCard 与 Mount 的 WithCapabilityChecks 共用本值。三 bool 均 omitempty，
// 全 false 时序列化为 {}（语义 = 全不支持，契约 §5.8 注记）。
var capabilities = a2a.AgentCapabilities{Streaming: false, PushNotifications: false, ExtendedAgentCard: false}

// bearerScheme 是唯一认证方案名，securitySchemes 与 securityRequirements 共用。
const bearerScheme = "bearer"

// buildCard 构造 Agent Card。securityRequirements 必填（契约 A-C2 审查裁决）：
// securitySchemes 只是"可用方案声明"，不构成访问要求——官方 a2aclient 的
// AuthInterceptor 按 securityRequirements 决定是否附凭证，缺了它卡片驱动的客户端
// 会裸发 SendMessage 被 requireBearer 恒 401（Gate ③④ 卡死）。
// scheme 值用 IANA 注册形态 "Bearer"（RFC 7235 大小写不敏感，但对端未必遵守）。
func buildCard(deps Deps) *a2a.AgentCard {
	return &a2a.AgentCard{
		Name:        "见微 Vane",
		Description: "AI 情报与信息推送服务。A2A 提供已抓取入库内容的确定性检索，以及不联网的一般公共知识对话。",
		Version:     deps.Version,
		SupportedInterfaces: []*a2a.AgentInterface{
			a2a.NewAgentInterface(deps.BaseURL, a2a.TransportProtocolJSONRPC),
		},
		Capabilities:       capabilities,
		DefaultInputModes:  []string{"text/plain"},
		DefaultOutputModes: []string{"application/json", "text/plain"},
		SecuritySchemes: a2a.NamedSecuritySchemes{
			bearerScheme: a2a.HTTPAuthSecurityScheme{Scheme: "Bearer"},
		},
		SecurityRequirements: a2a.SecurityRequirementsOptions{
			a2a.SecurityRequirements{bearerScheme: a2a.SecuritySchemeScopes{}},
		},
		Skills: []a2a.AgentSkill{
			{
				ID:   skillContentQuery,
				Name: "内容检索",
				Description: "按关键词与时间窗检索已入库内容，返回标题/链接/发布时间/正文摘录。" +
					"入参为消息首个 text part：JSON 对象 {\"skill\"(可选，缺省即本 skill),\"keyword\",\"days\",\"limit\"}，" +
					"或纯文本直接作为 keyword。",
				Tags:        []string{"news", "ai-models", "digest"},
				InputModes:  []string{"text/plain"},
				OutputModes: []string{"application/json", "text/plain"},
				Examples:    []string{"查询最近 3 天 Anthropic 相关内容", `{"keyword":"GPT","days":7,"limit":10}`},
			},
			{
				ID:   skillAssistantChat,
				Name: "对话助理",
				Description: "自然语言对话：AI 助理用模型已有公共知识回答一般问题和 Vane 能力边界问题；" +
					"不读取 owner 的任务、Owner Agent 对话历史、推送计划或画像，也不具备实时联网工具。" +
					"入参为消息首个 text part 的 JSON 对象 {\"skill\":\"assistant.chat\",\"text\":\"<自然语言>\"}（skill 与 text 均必填）。" +
					"多轮追问：复用同一 contextId 发后续消息，服务端按 contextId 重建对话历史。" +
					"本 skill 无任何写操作能力；按关键词检索入库内容请用 content.query（确定性检索更完整）。",
				Tags:        []string{"assistant", "chat", "read-only"},
				InputModes:  []string{"text/plain"},
				OutputModes: []string{"text/plain"},
				Examples:    []string{`{"skill":"assistant.chat","text":"简要解释 AI Agent 和普通聊天模型的区别。"}`, `{"skill":"assistant.chat","text":"这个 A2A 通道能创建监控任务吗？"}`},
			},
		},
	}
}
