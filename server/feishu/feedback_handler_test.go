package feishu

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher/callback"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/feedback"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

// wrapCall 记录一次 WrapQuestion 调用的入参（追问链路的分界面：
// handler 传进去的是不是飞书事件里的 ParentId/RootId/原文）。
type wrapCall struct {
	userID      int64
	appIdentity string
	inboundID   string
	parentID    string
	rootID      string
	text        string
}

// fakeFeedbackRunner 是 FeedbackRunner 的假实现：记录调用并回放预设结果。
// 「零调用」本身是断言对象（越权点击不得驱动任何反馈处理），所以调用计数
// 与入参都要留痕；handler 会从多个 goroutine 调它，全程加锁。
type fakeFeedbackRunner struct {
	mu sync.Mutex

	// HandleClick 的留痕与预设。
	clicks        []feedback.Click
	clickUsers    []int64
	reasonSubmits []feedback.ReasonSubmit
	result        feedback.ClickResult
	err           error
	delay         time.Duration // 模拟慢处理，触发 2.5s 同步预算
	panicMsg      string        // 非空则 panic，验证 recover 兜底

	// WrapQuestion 的留痕与预设。
	wraps       []wrapCall
	wrapText    string
	wrapMatched bool
	wrapErr     error
}

func (f *fakeFeedbackRunner) HandleClick(_ context.Context, principal auth.Principal, click feedback.Click) (feedback.ClickResult, error) {
	f.mu.Lock()
	f.clicks = append(f.clicks, click)
	f.clickUsers = append(f.clickUsers, principal.UserID)
	delay, panicMsg, res, err := f.delay, f.panicMsg, f.result, f.err
	f.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}
	if panicMsg != "" {
		panic(panicMsg)
	}
	return res, err
}

func (f *fakeFeedbackRunner) HandleReasonSubmit(_ context.Context, principal auth.Principal, submit feedback.ReasonSubmit) (feedback.ClickResult, error) {
	f.mu.Lock()
	f.reasonSubmits = append(f.reasonSubmits, submit)
	f.clickUsers = append(f.clickUsers, principal.UserID)
	res, err := f.result, f.err
	f.mu.Unlock()
	return res, err
}

func (f *fakeFeedbackRunner) WrapQuestion(
	_ context.Context,
	principal auth.Principal,
	appIdentity string,
	inboundMsgID string,
	parentMsgID string,
	rootMsgID string,
	text string,
) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.wraps = append(f.wraps, wrapCall{
		userID: principal.UserID, appIdentity: appIdentity, inboundID: inboundMsgID,
		parentID: parentMsgID, rootID: rootMsgID, text: text,
	})
	if f.wrapErr != nil {
		return "", false, f.wrapErr
	}
	if !f.wrapMatched {
		return "", false, nil
	}
	return f.wrapText, true, nil
}

func (f *fakeFeedbackRunner) clickCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.clicks)
}

func (f *fakeFeedbackRunner) recordedClicks() ([]feedback.Click, []int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]feedback.Click(nil), f.clicks...), append([]int64(nil), f.clickUsers...)
}

func (f *fakeFeedbackRunner) recordedWraps() []wrapCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]wrapCall(nil), f.wraps...)
}

// fbValue 构造一个合法反馈按钮的回调 value（契约 §10.1 形态）。
func fbValue(action types.FeedbackAction, deliveryID string) map[string]interface{} {
	return map[string]interface{}{
		"vane_action": "fb",
		"fb":          string(action),
		"delivery_id": deliveryID,
	}
}

func newFeedbackTestManager() *Manager {
	m := NewManager(nil, nil, nil)
	bindTestUserPrincipal(m)
	return m
}

func TestFeedbackReasonSubmitUsesBoundWorkspacePrincipal(t *testing.T) {
	cardJSON := BuildDeliveryCard(feedback.CardInput{
		BodyMD: "**正文**", DeliveryID: 42,
		State: feedback.CardState{Preference: types.FeedbackActionNotInterested},
	})
	m := newFeedbackTestManager()
	fb := &fakeFeedbackRunner{result: feedback.ClickResult{
		Toast: "原因已记录", ToastOK: true, CardJSON: cardJSON,
	}}
	m.SetFeedback(fb)
	h := newHandler(m, context.Background())

	resp := h.onFeedbackReasonSubmit(37, 42, types.FeedbackReasonNotRelevant, "不相关")
	if resp == nil || resp.Toast == nil || resp.Toast.Type != "success" || resp.Toast.Content != "原因已记录" {
		t.Fatalf("reason response = %+v", resp)
	}
	assertRawCard(t, resp, cardJSON)

	fb.mu.Lock()
	defer fb.mu.Unlock()
	if len(fb.reasonSubmits) != 1 || fb.reasonSubmits[0] != (feedback.ReasonSubmit{
		DeliveryID: 42, ReasonCode: types.FeedbackReasonNotRelevant, Detail: "不相关",
	}) {
		t.Fatalf("reason submits = %+v", fb.reasonSubmits)
	}
	if len(fb.clickUsers) != 1 || fb.clickUsers[0] != 37 {
		t.Fatalf("reason users = %v", fb.clickUsers)
	}
}

// assertRawCard 断言回调响应原地把卡片更新为指定的整卡 JSON。
// SDK 的 Card.Data 是 interface{}（要同时容纳 template/raw 两种载荷），
// raw 模式下约定放 json.RawMessage——放错类型飞书侧会静默不更新卡片，
// 所以类型与字节都断言。
func assertRawCard(t *testing.T, resp *callback.CardActionTriggerResponse, wantJSON string) {
	t.Helper()
	if resp == nil || resp.Card == nil {
		t.Fatalf("期望原地更新卡片，实际 card = nil")
	}
	if resp.Card.Type != "raw" {
		t.Errorf("card.type = %q, 期望 \"raw\"", resp.Card.Type)
	}
	data, ok := resp.Card.Data.(json.RawMessage)
	if !ok {
		t.Fatalf("card.data 类型 = %T, 期望 json.RawMessage", resp.Card.Data)
	}
	if string(data) != wantJSON {
		t.Errorf("card.data 与期望的整卡 JSON 不一致:\n得到 %s\n期望 %s", data, wantJSON)
	}
}

// TestParseFeedbackValue 钉死反馈按钮 value 的解析容错（契约 §10.1/§14）。
// value 完全由客户端提供、可伪造：解析层只做形状校验，任何不符一律 ok=false
// 由调用方静默忽略——绝不 panic，也绝不把可疑输入放行到下游。
func TestParseFeedbackValue(t *testing.T) {
	cases := []struct {
		name       string
		value      map[string]interface{}
		wantAction types.FeedbackAction
		wantID     int64
		wantOK     bool
	}{
		// —— 四个合法动作全部通过 ——
		{
			name:       "感兴趣",
			value:      fbValue(types.FeedbackActionInterested, "42"),
			wantAction: types.FeedbackActionInterested, wantID: 42, wantOK: true,
		},
		{
			name:       "不感兴趣",
			value:      fbValue(types.FeedbackActionNotInterested, "42"),
			wantAction: types.FeedbackActionNotInterested, wantID: 42, wantOK: true,
		},
		{
			name:       "误判",
			value:      fbValue(types.FeedbackActionMisjudged, "7"),
			wantAction: types.FeedbackActionMisjudged, wantID: 7, wantOK: true,
		},
		{
			name:       "深度解读",
			value:      fbValue(types.FeedbackActionDeepDive, "1"),
			wantAction: types.FeedbackActionDeepDive, wantID: 1, wantOK: true,
		},
		{
			// 字符串承载的意义：2^53+1 逐位无损（走 JSON number 会被舍成 ...992）。
			name:       "大 id 逐位无损",
			value:      fbValue(types.FeedbackActionInterested, "9007199254740993"),
			wantAction: types.FeedbackActionInterested, wantID: 9007199254740993, wantOK: true,
		},

		// —— vane_action 分流 ——
		{
			name:  "vane_action 是 confirm（M4 确认卡不得被反馈链路截胡）",
			value: map[string]interface{}{"vane_action": "confirm", "action_id": "act-1"},
		},
		{
			name:  "vane_action 是 cancel",
			value: map[string]interface{}{"vane_action": "cancel", "action_id": "act-1"},
		},
		{
			name:  "vane_action 缺失",
			value: map[string]interface{}{"fb": "interested", "delivery_id": "42"},
		},
		{
			name:  "vane_action 非字符串",
			value: map[string]interface{}{"vane_action": 1, "fb": "interested", "delivery_id": "42"},
		},

		// —— fb 白名单 ——
		{
			// question 由回复消息产生（契约 §11），不该出现在按钮上：
			// 放行它等于让客户端伪造一条追问反馈。
			name:  "fb 是 question（不在按钮白名单内）",
			value: fbValue(types.FeedbackActionQuestion, "42"),
		},
		{
			name:  "fb 不识别",
			value: fbValue("detonate", "42"),
		},
		{
			name:  "fb 空串",
			value: fbValue("", "42"),
		},
		{
			name:  "fb 缺失",
			value: map[string]interface{}{"vane_action": "fb", "delivery_id": "42"},
		},
		{
			name:  "fb 非字符串",
			value: map[string]interface{}{"vane_action": "fb", "fb": 1, "delivery_id": "42"},
		},
		{
			name:  "fb 大小写不匹配",
			value: fbValue("Interested", "42"),
		},

		// —— delivery_id 形状 ——
		{
			// SDK 用 encoding/json 把 value 解成 map[string]interface{}：
			// JSON number 恒变 float64。构卡侧写字符串，收到数字=非本系统构造
			// 或被篡改，一律拒绝。
			name:  "delivery_id 是数字类型（float64，模拟 SDK 解析 JSON number）",
			value: map[string]interface{}{"vane_action": "fb", "fb": "interested", "delivery_id": float64(42)},
		},
		{
			name:  "delivery_id 是整数类型",
			value: map[string]interface{}{"vane_action": "fb", "fb": "interested", "delivery_id": 42},
		},
		{
			name:  "delivery_id 空串",
			value: fbValue(types.FeedbackActionInterested, ""),
		},
		{
			name:  "delivery_id 非数字",
			value: fbValue(types.FeedbackActionInterested, "abc"),
		},
		{
			name:  "delivery_id 为 0",
			value: fbValue(types.FeedbackActionInterested, "0"),
		},
		{
			name:  "delivery_id 为负数",
			value: fbValue(types.FeedbackActionInterested, "-1"),
		},
		{
			name:  "delivery_id 带空白",
			value: fbValue(types.FeedbackActionInterested, " 42"),
		},
		{
			name:  "delivery_id 溢出 int64",
			value: fbValue(types.FeedbackActionInterested, "99999999999999999999"),
		},
		{
			name:  "delivery_id 缺失",
			value: map[string]interface{}{"vane_action": "fb", "fb": "interested"},
		},

		// —— 结构性缺失不得 panic ——
		{
			name:  "value 为 nil",
			value: nil,
		},
		{
			name:  "value 为空 map",
			value: map[string]interface{}{},
		},
		{
			name:  "value 全字段类型被篡改",
			value: map[string]interface{}{"vane_action": []string{"fb"}, "fb": map[string]int{}, "delivery_id": true},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			action, id, ok := parseFeedbackValue(tc.value)
			if ok != tc.wantOK {
				t.Fatalf("parseFeedbackValue() ok = %v, 期望 %v", ok, tc.wantOK)
			}
			if action != tc.wantAction {
				t.Errorf("parseFeedbackValue() action = %q, 期望 %q", action, tc.wantAction)
			}
			if id != tc.wantID {
				t.Errorf("parseFeedbackValue() deliveryID = %d, 期望 %d", id, tc.wantID)
			}
		})
	}
}

func TestParseFeedbackReasonValue(t *testing.T) {
	cases := []struct {
		name       string
		value      map[string]interface{}
		wantID     int64
		wantReason types.FeedbackReason
		wantOK     bool
	}{
		{
			name: "当前卡固定过时原因", value: map[string]interface{}{
				"vane_action": "fbr", "delivery_id": "42", "reason_code": "outdated_or_out_of_window",
			}, wantID: 42, wantReason: types.FeedbackReasonOutdated, wantOK: true,
		},
		{
			name: "历史卡空原因兼容", value: map[string]interface{}{
				"vane_action": "fbr", "delivery_id": "42",
			}, wantID: 42, wantOK: true,
		},
		{name: "伪造原因拒绝", value: map[string]interface{}{
			"vane_action": "fbr", "delivery_id": "42", "reason_code": "not_interested",
		}},
		{name: "非字符串投递ID拒绝", value: map[string]interface{}{
			"vane_action": "fbr", "delivery_id": 42, "reason_code": "duplicate",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, reason, ok := parseFeedbackReasonValue(tc.value)
			if id != tc.wantID || reason != tc.wantReason || ok != tc.wantOK {
				t.Fatalf("parseFeedbackReasonValue() = (%d,%q,%v), want (%d,%q,%v)", id, reason, ok, tc.wantID, tc.wantReason, tc.wantOK)
			}
		})
	}
}

// TestBuildDeliveryCardValueRoundTrip 闭合构卡与解析两侧的协议（契约 §10.1：
// 「构卡与解析共用常量」）。把真实卡片 JSON 里的 value 按 SDK 的方式还原成
// map[string]interface{} 再喂给 parseFeedbackValue——两侧任何一边漂移
// （改字段名、把 delivery_id 写成数字、动白名单）都会在这里断掉，
// 而不是等到用户点了按钮没反应。
func TestBuildDeliveryCardValueRoundTrip(t *testing.T) {
	const deliveryID = int64(9007199254740993) // 顺带覆盖大 id 的端到端无损
	card := decodeDeliveryCard(t, BuildDeliveryCard(feedback.CardInput{BodyMD: "正文", DeliveryID: deliveryID, State: feedback.CardState{}}))
	btns := decodeButtonColumns(t, card.Body.Elements[1])

	wantActions := []types.FeedbackAction{
		types.FeedbackActionInterested,
		types.FeedbackActionNotInterested,
		types.FeedbackActionDeepDive,
	}
	for i, btn := range btns {
		// SDK 收到回调时正是这样解 value：JSON string→string、JSON number→float64。
		rawValue, err := json.Marshal(btn.Behaviors[0].Value)
		if err != nil {
			t.Fatalf("按钮[%d] value 重新序列化失败: %v", i, err)
		}
		var sdkValue map[string]interface{}
		if err := json.Unmarshal(rawValue, &sdkValue); err != nil {
			t.Fatalf("按钮[%d] value 按 SDK 方式解析失败: %v", i, err)
		}

		action, id, ok := parseFeedbackValue(sdkValue)
		if !ok {
			t.Fatalf("按钮[%d]（%s）构卡产出的 value 解析失败: %v", i, btn.Text.Content, sdkValue)
		}
		if action != wantActions[i] {
			t.Errorf("按钮[%d] 解析出 action = %q, 期望 %q", i, action, wantActions[i])
		}
		if id != deliveryID {
			t.Errorf("按钮[%d] 解析出 deliveryID = %d, 期望 %d", i, id, deliveryID)
		}
	}
}

// TestOnCardActionFeedbackOwnerCheck 钉死反馈按钮共用 M4 的 owner 白名单
// （契约 §10.1 纵深校验①）：反馈会驱动画像演化与深度解读（付费调用），
// 非 owner 一律拒绝。断言点不止 toast，还有 **FeedbackRunner 零调用**——
// 「拒绝」必须发生在任何处理之前，越权零副作用（M4 §10 红线）。
func TestOnCardActionFeedbackOwnerCheck(t *testing.T) {
	cases := []struct {
		name     string
		owner    string // 空表示 owner 未捕获
		operator string
	}{
		{name: "非 owner 点反馈按钮被拒", owner: "ou_owner", operator: "ou_intruder"},
		{name: "owner 未捕获时一律拒绝", owner: "", operator: "ou_anyone"},
		{name: "operator 缺失时拒绝", owner: "ou_owner", operator: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// store 为 nil：拒绝路径必须在 UpsertUserByOpenID 之前短路，
			// 走到查库就会 panic——这本身就是「零副作用」的证明。
			m := newFeedbackTestManager()
			if tc.owner != "" {
				m.setOwner(tc.owner, "主人")
			}
			fb := &fakeFeedbackRunner{result: feedback.ClickResult{Toast: "不该出现", ToastOK: true}}
			m.SetFeedback(fb)
			h := newHandler(m, context.Background())

			resp, err := h.onCardAction(context.Background(),
				cardEvent(tc.operator, fbValue(types.FeedbackActionDeepDive, "42")))
			if err != nil {
				t.Fatalf("onCardAction 不应返回 error（避免飞书重推），实际: %v", err)
			}
			assertToast(t, resp, "仅主人可操作")
			if n := fb.clickCount(); n != 0 {
				t.Errorf("越权点击不得驱动反馈处理，实际 HandleClick 被调用 %d 次", n)
			}
		})
	}
}

// TestOnCardActionIgnoresBadFeedbackValue 验证形状不合法的反馈 value 被**静默忽略**
// （空响应、不弹错误 toast、不碰 FeedbackRunner）：同一机器人后续可能有别的交互卡片，
// 误弹错误会打扰用户；而放行会把伪造输入送进下游。
func TestOnCardActionIgnoresBadFeedbackValue(t *testing.T) {
	cases := []struct {
		name  string
		value map[string]interface{}
	}{
		{"fb 是 question（不在按钮白名单）", fbValue(types.FeedbackActionQuestion, "42")},
		{"fb 不识别", fbValue("detonate", "42")},
		{"delivery_id 是数字（SDK float64）", map[string]interface{}{
			"vane_action": "fb", "fb": "interested", "delivery_id": float64(42)}},
		{"delivery_id 非法", fbValue(types.FeedbackActionInterested, "abc")},
		{"delivery_id 为 0", fbValue(types.FeedbackActionInterested, "0")},
		{"缺 fb 字段", map[string]interface{}{"vane_action": "fb", "delivery_id": "42"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newFeedbackTestManager()
			m.setOwner("ou_owner", "主人")
			fb := &fakeFeedbackRunner{}
			m.SetFeedback(fb)
			h := newHandler(m, context.Background())

			resp, err := h.onCardAction(context.Background(), cardEvent("ou_owner", tc.value))
			if err != nil {
				t.Fatalf("onCardAction 不应返回 error，实际: %v", err)
			}
			if resp == nil || resp.Toast != nil || resp.Card != nil {
				t.Errorf("不合法反馈 value 应静默忽略（空响应），实际 %+v", resp)
			}
			if n := fb.clickCount(); n != 0 {
				t.Errorf("不合法 value 不得驱动反馈处理，实际 HandleClick 被调用 %d 次", n)
			}
		})
	}
}

// TestOnFeedbackActionNotReady 验证 FeedbackRunner 未注入时的兜底 toast
// （契约 §10.3）：装配不全时按钮点击要给人话，不能崩也不能沉默。
func TestOnFeedbackActionNotReady(t *testing.T) {
	m := newFeedbackTestManager()
	h := newHandler(m, context.Background())

	resp := h.onFeedbackAction(1, types.FeedbackActionInterested, 42)
	assertToast(t, resp, "反馈功能尚未就绪，请稍后重试")
	if resp.Toast.Type != "error" {
		t.Errorf("toast.type = %q, 期望 \"error\"", resp.Toast.Type)
	}
}

// TestOnFeedbackActionSuccess 验证预算内完成的正常路径（契约 §10.3）：
// 卡片更新用 feedback 服务返回的**整卡 JSON**（正文原样保留），
// 而非 M4 那样把卡片替换成结果文本——推送卡是长期驻留的内容本身。
func TestOnFeedbackActionSuccess(t *testing.T) {
	cardJSON := BuildDeliveryCard(feedback.CardInput{BodyMD: "**解读正文**", DeliveryID: 42,
		State: feedback.CardState{Preference: types.FeedbackActionInterested}})

	m := newFeedbackTestManager()
	fb := &fakeFeedbackRunner{
		result: feedback.ClickResult{Toast: "已记录：感兴趣", ToastOK: true, CardJSON: cardJSON},
	}
	m.SetFeedback(fb)
	h := newHandler(m, context.Background())

	resp := h.onFeedbackAction(7, types.FeedbackActionInterested, 42)

	if resp == nil || resp.Toast == nil {
		t.Fatalf("期望 toast 响应，实际 %+v", resp)
	}
	if resp.Toast.Type != "success" {
		t.Errorf("toast.type = %q, 期望 \"success\"（ToastOK=true）", resp.Toast.Type)
	}
	if resp.Toast.Content != "已记录：感兴趣" {
		t.Errorf("toast.content = %q, 期望服务返回的原文 %q", resp.Toast.Content, "已记录：感兴趣")
	}
	// type=raw 表示 data 是完整卡片 JSON（SDK callback.Card 约定）。
	assertRawCard(t, resp, cardJSON)

	// 入参透传：userID / action / deliveryID 一个都不能串。
	clicks, users := fb.recordedClicks()
	if len(clicks) != 1 {
		t.Fatalf("HandleClick 应恰好被调用 1 次，实际 %d 次", len(clicks))
	}
	if clicks[0] != (feedback.Click{Action: types.FeedbackActionInterested, DeliveryID: 42}) {
		t.Errorf("HandleClick 收到 click = %+v, 期望 {interested, 42}", clicks[0])
	}
	if users[0] != 7 {
		t.Errorf("HandleClick 收到 userID = %d, 期望 7", users[0])
	}
}

// TestOnFeedbackActionToastVariants 覆盖业务结果的两种 toast 样式与
// 「不更新卡片」的降级：ToastOK=false → error 样式；CardJSON 为空
// （feedback 侧重查状态失败时的降级）→ 只回 toast 不动卡片。
func TestOnFeedbackActionToastVariants(t *testing.T) {
	cases := []struct {
		name      string
		result    feedback.ClickResult
		wantType  string
		wantCard  bool
		wantToast string
	}{
		{
			name:      "业务拒绝（越权/找不到）走 error 样式且不更新卡片",
			result:    feedback.ClickResult{Toast: "找不到这条推送或不属于你"},
			wantType:  "error",
			wantToast: "找不到这条推送或不属于你",
		},
		{
			name:      "幂等命中仍重建卡",
			result:    feedback.ClickResult{Toast: "已记录过", ToastOK: true, CardJSON: `{"schema":"2.0"}`},
			wantType:  "success",
			wantCard:  true,
			wantToast: "已记录过",
		},
		{
			name:      "CardJSON 为空时只回 toast",
			result:    feedback.ClickResult{Toast: "已标记误判，将用于修正推送判断", ToastOK: true},
			wantType:  "success",
			wantToast: "已标记误判，将用于修正推送判断",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newFeedbackTestManager()
			m.SetFeedback(&fakeFeedbackRunner{result: tc.result})
			h := newHandler(m, context.Background())

			resp := h.onFeedbackAction(1, types.FeedbackActionMisjudged, 42)
			if resp == nil || resp.Toast == nil {
				t.Fatalf("期望 toast 响应，实际 %+v", resp)
			}
			if resp.Toast.Type != tc.wantType {
				t.Errorf("toast.type = %q, 期望 %q", resp.Toast.Type, tc.wantType)
			}
			if resp.Toast.Content != tc.wantToast {
				t.Errorf("toast.content = %q, 期望 %q", resp.Toast.Content, tc.wantToast)
			}
			if got := resp.Card != nil; got != tc.wantCard {
				t.Errorf("是否更新卡片 = %v, 期望 %v", got, tc.wantCard)
			}
		})
	}
}

// TestOnFeedbackActionError 验证 HandleClick 报错时翻成人话 error toast
// （不是把内部错误原样吐给用户，也不是沉默）。
func TestOnFeedbackActionError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "AppError 展示其人话 message",
			err:  types.NewAppError(types.CodeDatabase, "数据库暂时不可用", nil),
			want: "处理失败：数据库暂时不可用",
		},
		{
			name: "裸 error 走通用兜底文案",
			err:  fmt.Errorf("connection reset by peer"),
			want: "处理失败：内部错误，请稍后重试。",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newFeedbackTestManager()
			m.SetFeedback(&fakeFeedbackRunner{err: tc.err})
			h := newHandler(m, context.Background())

			resp := h.onFeedbackAction(1, types.FeedbackActionInterested, 42)
			assertToast(t, resp, tc.want)
			if resp.Toast.Type != "error" {
				t.Errorf("toast.type = %q, 期望 \"error\"", resp.Toast.Type)
			}
		})
	}
}

// TestOnFeedbackActionPanic 验证反馈处理 panic 被 goroutine 内的 recover 兜住：
// WS 回调链上的 panic 会带崩整个进程，这里必须只丢单次点击并给用户 toast。
func TestOnFeedbackActionPanic(t *testing.T) {
	m := newFeedbackTestManager()
	m.SetFeedback(&fakeFeedbackRunner{panicMsg: "boom"})
	h := newHandler(m, context.Background())

	resp := h.onFeedbackAction(1, types.FeedbackActionDeepDive, 42)
	assertToast(t, resp, "内部错误，请稍后重试")
	if resp.Toast.Type != "error" {
		t.Errorf("toast.type = %q, 期望 \"error\"（ToastOK 零值）", resp.Toast.Type)
	}
}

// TestOnFeedbackActionSyncBudgetTimeout 验证 2.5s 同步预算超时的降级
// （契约 §10.3）：飞书要求回调 3s 内响应，超时回「处理中，可稍后重新点击」
// 且**结果丢弃不补发**——反馈幂等，重点一次即自愈；这与 M4 确认卡
// 「Claim 后不可重复、必须补发」的机理刻意不同。
func TestOnFeedbackActionSyncBudgetTimeout(t *testing.T) {
	if testing.Short() {
		requireLongRunningCapability(t)
	}
	m := newFeedbackTestManager()
	fb := &fakeFeedbackRunner{
		delay:  feedbackCallbackSyncBudget + 500*time.Millisecond,
		result: feedback.ClickResult{Toast: "已记录：感兴趣", ToastOK: true, CardJSON: `{"schema":"2.0"}`},
	}
	m.SetFeedback(fb)
	h := newHandler(m, context.Background())

	start := time.Now()
	resp := h.onFeedbackAction(1, types.FeedbackActionInterested, 42)
	elapsed := time.Since(start)

	assertToast(t, resp, "处理中，可稍后重新点击")
	if resp.Toast.Type != "info" {
		t.Errorf("toast.type = %q, 期望 \"info\"", resp.Toast.Type)
	}
	// 必须在飞书 3s 红线内返回，而不是干等 HandleClick 跑完。
	if elapsed >= 3*time.Second {
		t.Errorf("同步预算超时后耗时 %v，超过飞书 3s 回调红线", elapsed)
	}
	// 「结果丢弃」不等于「不执行」：处理已经启动（并会在后台跑完落库），
	// 超时只是不把结果回给这次回调——用户重看/重点即见最新状态。
	if n := fb.clickCount(); n != 1 {
		t.Errorf("HandleClick 应已被调用 1 次（结果丢弃≠不执行），实际 %d 次", n)
	}
}

// replyEvent 构造一条"回复某消息"的 P2 消息接收事件（追问的输入形态）：
// 相比 receiveEvent 多了 ParentId/RootId——这正是 WrapQuestion 反查 delivery 的线索。
func replyEvent(msgID, text, senderOpenID, parentID, rootID string) *larkim.P2MessageReceiveV1 {
	ev := receiveEvent(msgID, "text", fmt.Sprintf(`{"text":%q}`, text), senderOpenID)
	if parentID != "" {
		ev.Event.Message.ParentId = &parentID
	}
	if rootID != "" {
		ev.Event.Message.RootId = &rootID
	}
	return ev
}

// TestHandleQuestionWrapping 钉死追问上下文的插入点（契约 §11）：
// owner 校验之后、进 agent 之前调 WrapQuestion，matched 则以包装后文本替换原文。
// 断言点是 **agent 收到什么**——这正是"追问上下文进对话链路"的分界面。
// 依赖真实 Postgres（handle 必经 UpsertUserByOpenID），CI 的 test job 提供
// DATABASE_URL；无则跳过（同 TestHandleMessageRouting）。
func TestHandleQuestionWrapping(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		requireDatabaseCapability(t)
	}
	ctx := t.Context()
	if err := store.Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 执行失败: %v", err)
	}
	st, err := store.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("store.New() 建池失败: %v", err)
	}
	registerStoreClose(t, st)

	// owner 预置为发送者本人：白名单放行，且 captureOwnerIfFirst 因缓存已捕获
	// 而跳过写库，不污染共享测试库的 feishu_owner 设置。
	//
	// open_id 带 uuid 后缀（同 store 包）：handle 必经 UpsertUserByOpenID 写 users，
	// 固定 open_id 会让并行打同一个测试库的两条流水线互删对方正在用的行。
	owner := "ou_test_question_wrap_" + uuid.NewString()
	cleanupTestUser(t, dbURL, owner)
	const wrapped = "[追问上下文] 用户正在追问一条历史推送（delivery_id=42）…\n[追问上下文结束]\n用户的追问：这篇原文说了什么"

	t.Run("命中追问时 agent 收到包装后文本", func(t *testing.T) {
		const appIdentity = "cli_question_wrap_test"
		m := NewManager(st, nil, nil)
		bindTestUserPrincipal(m)
		m.setOwner(owner, "测试")
		runner := &fakeRunner{}
		m.SetAgent(runner)
		fb := &fakeFeedbackRunner{wrapMatched: true, wrapText: wrapped}
		m.SetFeedback(fb)
		h := newHandlerForApp(m, context.Background(), appIdentity)

		h.handle(context.Background(), replyEvent(
			"om_test_q_wrap_hit", "这篇原文说了什么", owner, "om_parent_card", "om_root_card"))

		got := runner.received()
		if len(got) != 1 {
			t.Fatalf("agent loop 应恰好收到 1 条文本，实际 %d 条: %q", len(got), got)
		}
		if got[0] != wrapped {
			t.Errorf("agent loop 收到 %q, 期望包装后文本 %q", got[0], wrapped)
		}
		if trust := runner.receivedExternal(); len(trust) != 1 || !trust[0] {
			t.Fatalf("追问包装含外部推送正文，必须走 external-context 入口，实得 %v", trust)
		}

		// WrapQuestion 拿到的必须是飞书事件里的 ParentId/RootId 与原始文本。
		wraps := fb.recordedWraps()
		if len(wraps) != 1 {
			t.Fatalf("WrapQuestion 应恰好被调用 1 次，实际 %d 次", len(wraps))
		}
		if wraps[0].parentID != "om_parent_card" || wraps[0].rootID != "om_root_card" {
			t.Errorf("WrapQuestion 收到 parent/root = %q/%q, 期望 %q/%q",
				wraps[0].parentID, wraps[0].rootID, "om_parent_card", "om_root_card")
		}
		if wraps[0].text != "这篇原文说了什么" {
			t.Errorf("WrapQuestion 收到 text = %q, 期望原始消息文本", wraps[0].text)
		}
		if wraps[0].userID == 0 {
			t.Error("WrapQuestion 收到 userID = 0，期望 upsert 出的内部 user.ID")
		}
		if wraps[0].appIdentity != appIdentity ||
			wraps[0].inboundID != "om_test_q_wrap_hit" {
			t.Errorf("WrapQuestion app/inbound=%q/%q, want %q/%q",
				wraps[0].appIdentity, wraps[0].inboundID,
				appIdentity, "om_test_q_wrap_hit")
		}
	})

	t.Run("追问事实持久化失败时不进入 agent", func(t *testing.T) {
		m := NewManager(st, nil, nil)
		bindTestUserPrincipal(m)
		m.setOwner(owner, "测试")
		runner := &fakeRunner{}
		m.SetAgent(runner)
		m.SetFeedback(&fakeFeedbackRunner{
			wrapErr: types.NewAppError(
				types.CodeDatabase, "activity failed", nil),
		})
		h := newHandlerForApp(
			m, context.Background(), "cli_question_failure_test")

		h.handle(context.Background(), replyEvent(
			"om_test_q_wrap_failure", "这条说了什么", owner,
			"om_parent_card", "om_root_card"))

		if got := runner.received(); len(got) != 0 {
			t.Fatalf("追问事实失败不得进入 agent: %q", got)
		}
	})

	t.Run("未命中时原文进 agent", func(t *testing.T) {
		// 回复的是普通消息/聊天卡：降级为普通聊天，不得改动用户原话。
		m := NewManager(st, nil, nil)
		bindTestUserPrincipal(m)
		m.setOwner(owner, "测试")
		runner := &fakeRunner{}
		m.SetAgent(runner)
		m.SetFeedback(&fakeFeedbackRunner{wrapMatched: false, wrapText: "不该被采用"})
		h := newHandler(m, context.Background())

		h.handle(context.Background(), replyEvent(
			"om_test_q_wrap_miss", "今天天气如何", owner, "om_parent_chat", ""))

		got := runner.received()
		if len(got) != 1 {
			t.Fatalf("agent loop 应恰好收到 1 条文本，实际 %d 条: %q", len(got), got)
		}
		if got[0] != "今天天气如何" {
			t.Errorf("agent loop 收到 %q, 期望原样 %q", got[0], "今天天气如何")
		}
		if trust := runner.receivedExternal(); len(trust) != 1 || trust[0] {
			t.Fatalf("未命中且引用拉取降级时应走普通入口，实得 %v", trust)
		}
	})

	t.Run("未命中但引用 API 成功时走 external-context 入口", func(t *testing.T) {
		const parentID = "om_parent_quote_success"
		client, closeServer := newQuotedMessageTestClient(t, parentID, "引用卡片里的外部正文")
		defer closeServer()

		m := NewManager(st, nil, nil)
		bindTestUserPrincipal(m)
		m.setOwner(owner, "测试")
		m.apiClient = client
		runner := &fakeRunner{}
		m.SetAgent(runner)
		m.SetFeedback(&fakeFeedbackRunner{wrapMatched: false})
		h := newHandler(m, context.Background())

		h.handle(context.Background(), replyEvent(
			"om_test_quote_success", "请解释一下", owner, parentID, ""))

		got := runner.received()
		want := "[用户引用的消息]\n引用卡片里的外部正文\n[用户的回复]\n请解释一下"
		if len(got) != 1 || got[0] != want {
			t.Fatalf("agent loop 收到 %q，期望 %q", got, want)
		}
		if trust := runner.receivedExternal(); len(trust) != 1 || !trust[0] {
			t.Fatalf("引用 API 成功混入外部正文后必须走 external-context，实得 %v", trust)
		}
	})

	t.Run("FeedbackRunner 未注入时不 panic 且走普通路径", func(t *testing.T) {
		// 灰度装配形态：反馈未就绪时追问降级为普通消息，消息链不能崩。
		m := NewManager(st, nil, nil)
		bindTestUserPrincipal(m)
		m.setOwner(owner, "测试")
		runner := &fakeRunner{}
		m.SetAgent(runner)
		h := newHandler(m, context.Background()) // 刻意不 SetFeedback

		h.handle(context.Background(), replyEvent(
			"om_test_q_wrap_nil", "你好", owner, "om_parent_card", "om_root_card"))

		got := runner.received()
		if len(got) != 1 {
			t.Fatalf("agent loop 应恰好收到 1 条文本，实际 %d 条: %q", len(got), got)
		}
		if got[0] != "你好" {
			t.Errorf("agent loop 收到 %q, 期望原样 %q", got[0], "你好")
		}
		if trust := runner.receivedExternal(); len(trust) != 1 || trust[0] {
			t.Fatalf("无反馈包装且引用拉取降级时应走普通入口，实得 %v", trust)
		}
	})

	t.Run("非回复消息也照常询问追问（双 id 由 WrapQuestion 自行降级）", func(t *testing.T) {
		// 插入点不做"是不是回复"的预判：空 id 的短路归 WrapQuestion 内部
		// （契约 §11 ①），handler 只负责把线索原样递过去。
		m := NewManager(st, nil, nil)
		bindTestUserPrincipal(m)
		m.setOwner(owner, "测试")
		runner := &fakeRunner{}
		m.SetAgent(runner)
		fb := &fakeFeedbackRunner{wrapMatched: false}
		m.SetFeedback(fb)
		h := newHandler(m, context.Background())

		h.handle(context.Background(), receiveEvent(
			"om_test_q_wrap_plain", "text", `{"text":"随便聊聊"}`, owner))

		wraps := fb.recordedWraps()
		if len(wraps) != 1 {
			t.Fatalf("WrapQuestion 应被调用 1 次，实际 %d 次", len(wraps))
		}
		if wraps[0].parentID != "" || wraps[0].rootID != "" {
			t.Errorf("非回复消息的 parent/root 应为空串，实际 %q/%q", wraps[0].parentID, wraps[0].rootID)
		}
		if got := runner.received(); len(got) != 1 || got[0] != "随便聊聊" {
			t.Errorf("agent loop 收到 %q, 期望原样 [\"随便聊聊\"]", got)
		}
		if trust := runner.receivedExternal(); len(trust) != 1 || trust[0] {
			t.Fatalf("普通文本不应被标成 external-context，实得 %v", trust)
		}
	})
}

// TestOnCardActionFeedbackRoute 是 fb 分流的端到端集成测试（契约 §10.3）：
// 走完 parseFeedbackValue → owner 校验 → UpsertUserByOpenID → FeedbackRunner，
// 断言 HandleClick 收到的 userID 是**库里 upsert 出来的内部 id**（而非 open_id
// 或 0）——这段只有真库能验。无 DATABASE_URL 时跳过。
func TestOnCardActionFeedbackRoute(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		requireDatabaseCapability(t)
	}
	ctx := t.Context()
	if err := store.Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 执行失败: %v", err)
	}
	st, err := store.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("store.New() 建池失败: %v", err)
	}
	registerStoreClose(t, st)

	// open_id 带 uuid 后缀，理由同 TestHandleQuestionWrapping。
	owner := "ou_test_fb_route_" + uuid.NewString()
	cleanupTestUser(t, dbURL, owner)
	user, err := st.UpsertUserByOpenID(ctx, owner, "测试")
	if err != nil {
		t.Fatalf("UpsertUserByOpenID() 失败: %v", err)
	}

	cardJSON := BuildDeliveryCard(feedback.CardInput{BodyMD: "**正文**", DeliveryID: 42,
		State: feedback.CardState{Preference: types.FeedbackActionNotInterested}})
	m := NewManager(st, nil, nil)
	bindTestUserPrincipal(m)
	m.setOwner(owner, "测试")
	fb := &fakeFeedbackRunner{
		result: feedback.ClickResult{Toast: "已记录：不感兴趣", ToastOK: true, CardJSON: cardJSON},
	}
	m.SetFeedback(fb)
	h := newHandler(m, context.Background())

	resp, err := h.onCardAction(context.Background(),
		cardEvent(owner, fbValue(types.FeedbackActionNotInterested, "42")))
	if err != nil {
		t.Fatalf("onCardAction 不应返回 error，实际: %v", err)
	}
	if resp == nil || resp.Toast == nil || resp.Toast.Content != "已记录：不感兴趣" {
		t.Fatalf("toast = %+v, 期望内容 %q", resp.Toast, "已记录：不感兴趣")
	}
	assertRawCard(t, resp, cardJSON)

	clicks, users := fb.recordedClicks()
	if len(clicks) != 1 {
		t.Fatalf("HandleClick 应恰好被调用 1 次，实际 %d 次", len(clicks))
	}
	if clicks[0] != (feedback.Click{Action: types.FeedbackActionNotInterested, DeliveryID: 42}) {
		t.Errorf("HandleClick 收到 click = %+v, 期望 {not_interested, 42}", clicks[0])
	}
	// 反馈不依赖 agent loop：这条路径全程没注入 AgentRunner，仍须走通。
	if users[0] != user.ID {
		t.Errorf("HandleClick 收到 userID = %d, 期望库内 user.ID = %d", users[0], user.ID)
	}
}
