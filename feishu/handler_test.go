package feishu

import (
	"context"
	"testing"

	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher/callback"
)

// confirmValue 构造一个合法确认按钮的回调 value（契约 §9 形态）。
func confirmValue() map[string]interface{} {
	return map[string]interface{}{"vane_action": "confirm", "action_id": "act-1"}
}

// cardEvent 构造带 operator 的卡片回调事件。
func cardEvent(operatorOpenID string, value map[string]interface{}) *callback.CardActionTriggerEvent {
	return &callback.CardActionTriggerEvent{
		Event: &callback.CardActionTriggerRequest{
			Operator: &callback.Operator{OpenID: operatorOpenID},
			Action:   &callback.CallBackAction{Value: value},
		},
	}
}

// assertToast 断言回调响应只带指定文案的 toast（不更新卡片）。
func assertToast(t *testing.T, resp *callback.CardActionTriggerResponse, wantContent string) {
	t.Helper()
	if resp == nil || resp.Toast == nil {
		t.Fatalf("期望 toast 响应，实际 %+v", resp)
	}
	if resp.Toast.Content != wantContent {
		t.Errorf("toast = %q, 期望 %q", resp.Toast.Content, wantContent)
	}
	if resp.Card != nil {
		t.Errorf("拒绝路径不应更新卡片，实际 card = %+v", resp.Card)
	}
}

// TestOnCardActionOwnerCheck 钉死回调的 owner 白名单（契约 §10）：
// 只有已捕获的 owner 本人能触发确认/取消，其余一律 toast 拒绝——
// 这些路径都在查库之前短路，无需数据库即可单测。
func TestOnCardActionOwnerCheck(t *testing.T) {
	t.Run("非 owner 操作被拒", func(t *testing.T) {
		m := NewManager(nil, nil, nil)
		m.setOwner("ou_owner", "主人")
		h := newHandler(m, context.Background())

		resp, err := h.onCardAction(context.Background(), cardEvent("ou_intruder", confirmValue()))
		if err != nil {
			t.Fatalf("onCardAction 不应返回 error（避免飞书重推），实际: %v", err)
		}
		assertToast(t, resp, "仅主人可操作")
	})

	t.Run("owner 未捕获时一律拒绝", func(t *testing.T) {
		// owner 缓存为空（如进程重启后未预热）：宁可拒绝也不留白名单空窗。
		m := NewManager(nil, nil, nil)
		h := newHandler(m, context.Background())

		resp, err := h.onCardAction(context.Background(), cardEvent("ou_anyone", confirmValue()))
		if err != nil {
			t.Fatalf("onCardAction 不应返回 error，实际: %v", err)
		}
		assertToast(t, resp, "仅主人可操作")
	})

	t.Run("operator 缺失时拒绝", func(t *testing.T) {
		m := NewManager(nil, nil, nil)
		m.setOwner("ou_owner", "主人")
		h := newHandler(m, context.Background())

		ev := &callback.CardActionTriggerEvent{
			Event: &callback.CardActionTriggerRequest{
				Action: &callback.CallBackAction{Value: confirmValue()},
			},
		}
		resp, err := h.onCardAction(context.Background(), ev)
		if err != nil {
			t.Fatalf("onCardAction 不应返回 error，实际: %v", err)
		}
		assertToast(t, resp, "仅主人可操作")
	})

	t.Run("owner 本人但 agent 未注入", func(t *testing.T) {
		m := NewManager(nil, nil, nil)
		m.setOwner("ou_owner", "主人")
		h := newHandler(m, context.Background())

		resp, err := h.onCardAction(context.Background(), cardEvent("ou_owner", confirmValue()))
		if err != nil {
			t.Fatalf("onCardAction 不应返回 error，实际: %v", err)
		}
		assertToast(t, resp, "助手尚未就绪，请稍后重试")
	})
}

// TestOnCardActionIgnoresForeignCallback 验证非 Vane 确认卡的回调（value 里
// 没有 vane_action/action_id，或取值不识别）被静默忽略：返回空响应而非错误
// toast——同一机器人后续可能有其他交互卡片，误弹错误会打扰用户。
func TestOnCardActionIgnoresForeignCallback(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.setOwner("ou_owner", "主人")
	h := newHandler(m, context.Background())

	cases := []struct {
		name  string
		value map[string]interface{}
	}{
		{"value 为空", nil},
		{"缺 action_id", map[string]interface{}{"vane_action": "confirm"}},
		{"vane_action 不识别", map[string]interface{}{"vane_action": "detonate", "action_id": "act-1"}},
		{"值类型被篡改为非字符串", map[string]interface{}{"vane_action": 1, "action_id": []string{"x"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := h.onCardAction(context.Background(), cardEvent("ou_owner", tc.value))
			if err != nil {
				t.Fatalf("onCardAction 不应返回 error，实际: %v", err)
			}
			if resp == nil || resp.Toast != nil || resp.Card != nil {
				t.Errorf("外来回调应静默忽略（空响应），实际 %+v", resp)
			}
		})
	}
}

// TestOnCardActionMissingEvent 验证事件结构缺失时的兜底 toast（不 panic）。
func TestOnCardActionMissingEvent(t *testing.T) {
	m := NewManager(nil, nil, nil)
	h := newHandler(m, context.Background())

	for _, ev := range []*callback.CardActionTriggerEvent{
		nil,
		{},
		{Event: &callback.CardActionTriggerRequest{}},
	} {
		resp, err := h.onCardAction(context.Background(), ev)
		if err != nil {
			t.Fatalf("onCardAction 不应返回 error，实际: %v", err)
		}
		assertToast(t, resp, "回调数据缺失")
	}
}
