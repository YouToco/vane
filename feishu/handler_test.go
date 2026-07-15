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

// TestParsePostContent 钉死 post（富文本）消息的纯文本提取规则：取文本性
// 节点（text/md/code_block/a 锚文本）、段落间换行、title 作首行、行内空白
// 原样保留、无文字返回空串（调用方据此回退"暂只支持文本消息"文案）。
// 结构依据：飞书接收侧 post content 顶层为 {title, content}（无发送 API 的
// zh_cn 语言包装），content 是段落二维数组，节点靠 tag 区分；at 节点的
// 占位符在 user_id 字段（"@_user_1"）而非正文（官方文档 + lark-cli 实测）。
func TestParsePostContent(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "粘贴多段纯文本",
			raw:  `{"title":"","content":[[{"tag":"text","text":"第一段"}],[{"tag":"text","text":"第二段"}]]}`,
			want: "第一段\n第二段",
		},
		{
			name: "混合节点取文本性节点忽略其余",
			raw: `{"title":"","content":[` +
				`[{"tag":"text","text":"看这个 "},{"tag":"a","text":"页面","href":"https://example.com"},{"tag":"text","text":" 的说明","style":[]}],` +
				`[{"tag":"img","image_key":"img_v2_x"}],` +
				`[{"tag":"at","user_id":"@_user_1","user_name":"某人"},{"tag":"text","text":"结论在最后"}]]}`,
			want: "看这个 页面 的说明\n结论在最后",
		},
		{
			name: "同段落多个 text 节点直接相接",
			raw:  `{"content":[[{"tag":"text","text":"前半"},{"tag":"text","text":"后半","style":["bold"]}]]}`,
			want: "前半后半",
		},
		{
			name: "md 与 code_block 节点也是正文",
			raw:  `{"content":[[{"tag":"md","text":"**加粗** 内容"}],[{"tag":"code_block","language":"GO","text":"fmt.Println(1)"}]]}`,
			want: "**加粗** 内容\nfmt.Println(1)",
		},
		{
			name: "纯图片返回空串",
			raw:  `{"title":"","content":[[{"tag":"img","image_key":"img_v2_a"}],[{"tag":"img","image_key":"img_v2_b"}]]}`,
			want: "",
		},
		{
			name: "空段落不产生空行",
			raw:  `{"content":[[{"tag":"text","text":"上"}],[],[{"tag":"text","text":"下"}]]}`,
			want: "上\n下",
		},
		{
			name: "只含非文本节点的段落不产生空行",
			raw:  `{"content":[[{"tag":"text","text":"上"}],[{"tag":"hr"}],[{"tag":"text","text":"下"}]]}`,
			want: "上\n下",
		},
		{
			name: "title 非空时作为首行",
			raw:  `{"title":"周报","content":[[{"tag":"text","text":"本周进展"}]]}`,
			want: "周报\n本周进展",
		},
		{
			name: "仅 title 也算有文字",
			raw:  `{"title":"只有标题","content":[]}`,
			want: "只有标题",
		},
		{
			name: "保留行内缩进",
			raw:  `{"content":[[{"tag":"text","text":"func main() {"}],[{"tag":"text","text":"\tfmt.Println(1)"}],[{"tag":"text","text":"}"}]]}`,
			want: "func main() {\n\tfmt.Println(1)\n}",
		},
		{
			name: "正文含 @_user_N 字样不被误删",
			raw:  `{"content":[[{"tag":"text","text":"日志里出现 @_user_1 占位符"}]]}`,
			want: "日志里出现 @_user_1 占位符",
		},
		{
			name: "非法 JSON 返回空串",
			raw:  `{"title":`,
			want: "",
		},
		{
			name: "空对象返回空串",
			raw:  `{}`,
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parsePostContent(tc.raw); got != tc.want {
				t.Errorf("parsePostContent() = %q, 期望 %q", got, tc.want)
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
