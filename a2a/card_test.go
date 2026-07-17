package a2a

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestBuildCard 契约 §9.1：SDK 序列化后按语义等价断言（三 bool 均 omitempty，
// 字段缺省 = false——不逐字比对 §5.8 草案）。
func TestBuildCard(t *testing.T) {
	deps := Deps{BaseURL: "https://api.vane.zhuoqidev.com/a2a", Version: "0.5.0"}
	card := buildCard(deps)
	raw, err := json.Marshal(card)
	if err != nil {
		t.Fatalf("card 序列化失败: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	t.Logf("SDK 序列化实测形态（契约 §5.8 回填依据）: %s", raw)

	// skill id 与 supportedInterfaces url。
	skills := m["skills"].([]any)
	if len(skills) != 1 || skills[0].(map[string]any)["id"] != skillContentQuery {
		t.Errorf("应恰有一个 skill %q，实际 %v", skillContentQuery, skills)
	}
	ifaces := m["supportedInterfaces"].([]any)
	iface := ifaces[0].(map[string]any)
	if iface["url"] != deps.BaseURL || iface["protocolBinding"] != "JSONRPC" {
		t.Errorf("supportedInterfaces 不符: %v", iface)
	}
	if iface["protocolVersion"] != "1.0" {
		t.Errorf("protocolVersion 应 1.0，实际 %v", iface["protocolVersion"])
	}
	if m["version"] != "0.5.0" {
		t.Errorf("version 应透传 Deps.Version，实际 %v", m["version"])
	}

	// securityRequirements 非空且含 bearer（A-C2：缺了它卡片驱动客户端裸发被 401）。
	// SDK 实测序列化形态：[{"schemes":{"bearer":[]}}]（SecurityRequirementsOptions
	// 自定义 Marshaler 包一层 schemes 键）。
	reqs, ok := m["securityRequirements"].([]any)
	if !ok || len(reqs) == 0 {
		t.Fatalf("securityRequirements 必须非空，实际 %v", m["securityRequirements"])
	}
	reqJSON, _ := json.Marshal(reqs[0])
	if !strings.Contains(string(reqJSON), `"`+bearerScheme+`"`) {
		t.Errorf("securityRequirements 应含 %q，实际 %s", bearerScheme, reqJSON)
	}
	// securitySchemes 含 bearer 的 HTTP auth 方案（SDK 形态：httpAuthSecurityScheme 包裹）。
	schemes, _ := json.Marshal(m["securitySchemes"])
	if !strings.Contains(string(schemes), `"httpAuthSecurityScheme"`) ||
		!strings.Contains(string(schemes), `"Bearer"`) {
		t.Errorf("securitySchemes 应含 Bearer HTTP auth 方案，实际 %s", schemes)
	}

	// capabilities 语义等价：全 false（omitempty 缺省 = false，序列化为 {}）。
	caps, ok := m["capabilities"].(map[string]any)
	if !ok {
		t.Fatalf("capabilities 应为对象，实际 %v", m["capabilities"])
	}
	for _, k := range []string{"streaming", "pushNotifications", "extendedAgentCard"} {
		if v, present := caps[k]; present && v != false {
			t.Errorf("capabilities.%s 语义应为 false，实际 %v", k, v)
		}
	}
}

// TestCardCapabilitiesSameSource buildCard 与 WithCapabilityChecks 同源（契约 §9.1）：
// 卡片声明与 handler 能力检查共用同一包级变量，杜绝"卡片说不支持、handler 却放行"。
func TestCardCapabilitiesSameSource(t *testing.T) {
	card := buildCard(Deps{})
	// AgentCapabilities 含切片不可 == 比较，逐字段断言与包级变量一致。
	if card.Capabilities.Streaming != capabilities.Streaming ||
		card.Capabilities.PushNotifications != capabilities.PushNotifications ||
		card.Capabilities.ExtendedAgentCard != capabilities.ExtendedAgentCard {
		t.Fatalf("card.Capabilities 必须取自包级 capabilities 变量: card=%+v pkg=%+v",
			card.Capabilities, capabilities)
	}
	if capabilities.Streaming || capabilities.PushNotifications || capabilities.ExtendedAgentCard {
		t.Fatal("第一期 capabilities 必须全 false（契约 §0 非目标）")
	}
}
