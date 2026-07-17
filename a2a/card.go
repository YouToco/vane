// Agent Card（契约 §5.8）：SDK 类型构造，序列化形态是 SDK 的责任（encoding/json +
// SDK 自定义 Marshaler，实测形态见 card_test.go golden——契约草案按语义等价比对）。
package a2a

import (
	"github.com/a2aproject/a2a-go/v2/a2a"
)

// skillContentQuery 是第一期唯一 skill 的 id。executor 的 REJECTED 判定（§5.4）与
// card 的 skill 声明必须同源——literals_test.go 正则守卫钉死（§9.5）。
const skillContentQuery = "content.query"

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
		Description: "AI 个性化信息推送服务。提供已抓取入库的多信源内容检索（AI 模型厂商动态等）。",
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
		},
	}
}
