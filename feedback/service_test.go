package feedback

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
)

// ============================================================
// 态度反馈（契约 §10.4 / §15）
// ============================================================

// 审查 F5 定向用例：interested → not_interested → interested。
// 第三次点击的态度确实变了（上一条是 not_interested），必须插行。
// 若 LatestFeedbackAction 被传成单值集合 {interested}，第三次会命中第一行、
// 被误判成"重复点击"而不插行——最新态度就此丢失（被否决的唯一索引 bug 复刻）。
func TestHandleClick_AttitudeTripleToggleInsertsThirdRow(t *testing.T) {
	h := newHarness(t)

	seq := []types.FeedbackAction{
		types.FeedbackActionInterested,
		types.FeedbackActionNotInterested,
		types.FeedbackActionInterested,
	}
	wantToast := []string{"已记录：感兴趣", "已记录：不感兴趣", "已记录：感兴趣"}

	for i, action := range seq {
		res := h.click(t, action)
		if res.Toast != wantToast[i] || !res.ToastOK {
			t.Fatalf("第 %d 次点击 toast = %q(ok=%v), 期望 %q(ok=true)", i+1, res.Toast, res.ToastOK, wantToast[i])
		}
		// 每次都返回重建卡，且状态行的最新态度 = 本次点击。
		card := decodeCard(t, res.CardJSON)
		if card.Pref != string(action) {
			t.Fatalf("第 %d 次点击后卡片 Preference = %q, 期望 %q", i+1, card.Pref, action)
		}
		if card.BodyMD != testBodyMD || card.DeliveryID != testDeliveryID {
			t.Fatalf("重建卡应携带原正文与 delivery_id, 实得 %+v", card)
		}
	}

	// 三行都在，且顺序 = 点击顺序（追加式事件日志、最新为准）。
	rows := h.st.allRows()
	if len(rows) != 3 {
		t.Fatalf("三连击应插 3 行（第三次是态度反转，必须插行）, 实得 %d 行: %+v", len(rows), rows)
	}
	for i, row := range rows {
		if row.Action != seq[i] {
			t.Fatalf("第 %d 行 action = %q, 期望 %q", i+1, row.Action, seq[i])
		}
		if row.UserID != testUserID || row.DeliveryID != testDeliveryID || row.Detail != "" {
			t.Fatalf("态度行字段不符（detail 应为空）: %+v", row)
		}
	}

	// 库内最新态度 = interested：这正是三连击必须插行才能保住的事实。
	latest, err := h.st.LatestFeedbackAction(context.Background(), testDeliveryID, attitudeActions)
	if err != nil || latest != types.FeedbackActionInterested {
		t.Fatalf("最新态度 = %q(err=%v), 期望 interested", latest, err)
	}
}

// 重复点同一态度：幂等——不插行、toast「已记录过」，但仍返回重建卡
// （并发窗口下状态行的短暂缺项靠重复点击自愈）。
func TestHandleClick_SameAttitudeIsIdempotent(t *testing.T) {
	h := newHarness(t)

	h.click(t, types.FeedbackActionNotInterested)
	if got := len(h.st.allRows()); got != 1 {
		t.Fatalf("首次点击应插 1 行, 实得 %d", got)
	}

	res := h.click(t, types.FeedbackActionNotInterested)
	if res.Toast != "已记录过" || !res.ToastOK {
		t.Fatalf("重复点同一态度 toast = %q(ok=%v), 期望「已记录过」(ok=true)", res.Toast, res.ToastOK)
	}
	if got := len(h.st.allRows()); got != 1 {
		t.Fatalf("重复点同一态度不得插行, 实得 %d 行", got)
	}
	card := decodeCard(t, res.CardJSON)
	if card.Pref != string(types.FeedbackActionNotInterested) {
		t.Fatalf("幂等路径同样要重建卡且状态行不变, 实得 %+v", card)
	}
	// 幂等路径不通告会话（没有新事实）。
	if n := h.notifier.all(); len(n) != 1 {
		t.Fatalf("只有首次点击应通告会话, 实得 %d 条: %+v", len(n), n)
	}
}

// 态度点击要把「[卡片回调]」通告写进 agent 会话（契约 §12.4 文案）。
func TestHandleClick_NotifiesSessionWithTitle(t *testing.T) {
	h := newHarness(t)
	h.click(t, types.FeedbackActionNotInterested)

	all := h.notifier.all()
	if len(all) != 1 {
		t.Fatalf("应通告 1 条, 实得 %d", len(all))
	}
	if all[0].userID != testUserID {
		t.Fatalf("通告 user_id = %d, 期望 %d", all[0].userID, testUserID)
	}
	txt := all[0].text
	for _, want := range []string{"[卡片回调]", "delivery_id=42", "《" + testTitle + "》", "「不感兴趣」"} {
		if !strings.Contains(txt, want) {
			t.Fatalf("通告应含 %q, 实得 %q", want, txt)
		}
	}
}

// 内容已清理时通告降级为不带书名号标题，反馈本身照常记录（标题只是辅助上下文）。
func TestHandleClick_NotifyWithoutTitleWhenContentPurged(t *testing.T) {
	h := newHarness(t)
	h.delivery().ContentItemID = nil

	res := h.click(t, types.FeedbackActionInterested)
	if res.Toast != "已记录：感兴趣" {
		t.Fatalf("toast = %q, 期望正常记录", res.Toast)
	}
	if got := len(h.st.allRows()); got != 1 {
		t.Fatalf("内容已清理不影响态度落行, 实得 %d 行", got)
	}
	txt := h.notifier.all()[0].text
	if strings.Contains(txt, "《") {
		t.Fatalf("无标题时不应出现书名号, 实得 %q", txt)
	}
	if !strings.Contains(txt, "delivery_id=42") || !strings.Contains(txt, "「感兴趣」") {
		t.Fatalf("通告主体仍应完整, 实得 %q", txt)
	}
}

// ============================================================
// 误判（一次性 + 与态度并存）
// ============================================================

// 误判是一次性信号：第二次点击不插行、toast「已标记过误判」。
func TestHandleClick_MisjudgedIsOneShot(t *testing.T) {
	h := newHarness(t)

	first := h.click(t, types.FeedbackActionMisjudged)
	if first.Toast != "已标记误判，将用于修正推送判断" || !first.ToastOK {
		t.Fatalf("首次误判 toast = %q(ok=%v)", first.Toast, first.ToastOK)
	}
	if card := decodeCard(t, first.CardJSON); !card.Misjudged {
		t.Fatalf("首次误判后卡片应带误判标记, 实得 %+v", card)
	}

	second := h.click(t, types.FeedbackActionMisjudged)
	if second.Toast != "已标记过误判" || !second.ToastOK {
		t.Fatalf("重复误判 toast = %q(ok=%v), 期望「已标记过误判」(ok=true)", second.Toast, second.ToastOK)
	}
	if rows := h.st.rows(testDeliveryID, types.FeedbackActionMisjudged); len(rows) != 1 {
		t.Fatalf("误判只应有 1 行, 实得 %d 行", len(rows))
	}
	if card := decodeCard(t, second.CardJSON); !card.Misjudged {
		t.Fatalf("幂等路径同样重建卡且保留误判标记, 实得 %+v", card)
	}
	if n := h.notifier.all(); len(n) != 1 {
		t.Fatalf("重复误判不应二次通告, 实得 %d 条", len(n))
	}
}

// 误判独立于态度、可并存：状态行两者都在。
func TestHandleClick_MisjudgedCoexistsWithAttitude(t *testing.T) {
	h := newHarness(t)

	h.click(t, types.FeedbackActionNotInterested)
	res := h.click(t, types.FeedbackActionMisjudged)

	card := decodeCard(t, res.CardJSON)
	if card.Pref != string(types.FeedbackActionNotInterested) || !card.Misjudged {
		t.Fatalf("状态行应同时含最新态度与误判, 实得 %+v", card)
	}
	if card.DeepDive {
		t.Fatalf("未请求深度解读, 状态行不应带该项: %+v", card)
	}

	// 误判后仍可改态度：误判不参与态度集合，不影响 F5 语义。
	after := h.click(t, types.FeedbackActionInterested)
	card = decodeCard(t, after.CardJSON)
	if card.Pref != string(types.FeedbackActionInterested) || !card.Misjudged {
		t.Fatalf("改态度后误判标记应保留、态度应翻转, 实得 %+v", card)
	}
	if got := len(h.st.allRows()); got != 3 {
		t.Fatalf("应为 not_interested + misjudged + interested 共 3 行, 实得 %d", got)
	}
}

// ============================================================
// 越权 / 不存在（M4 §10 红线：零副作用）
// ============================================================

func TestHandleClick_ForeignOrMissingDeliveryHasNoSideEffect(t *testing.T) {
	cases := []struct {
		name       string
		userID     int64
		deliveryID int64
	}{
		{"越权：投递属于别人", 999, testDeliveryID},
		{"不存在的 delivery_id", testUserID, 12345},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			for _, action := range []types.FeedbackAction{
				types.FeedbackActionInterested,
				types.FeedbackActionMisjudged,
				types.FeedbackActionDeepDive,
			} {
				res, err := h.svc.HandleClick(context.Background(), tc.userID,
					Click{Action: action, DeliveryID: tc.deliveryID})
				if err != nil {
					t.Fatalf("%s 应返回人话 toast 而非 error: %v", action, err)
				}
				if res.Toast != "找不到这条推送或不属于你" || res.ToastOK {
					t.Fatalf("%s toast = %q(ok=%v), 期望「找不到这条推送或不属于你」(ok=false)", action, res.Toast, res.ToastOK)
				}
				if res.CardJSON != "" {
					t.Fatalf("%s 越权不得回卡, 实得 %q", action, res.CardJSON)
				}
			}
			// 零副作用：不落行、不构卡、不发消息、不通告、不烧钱。
			if rows := h.st.allRows(); len(rows) != 0 {
				t.Fatalf("越权不得写反馈, 实得 %+v", rows)
			}
			if h.cards.count() != 0 || h.sender.count() != 0 || len(h.notifier.all()) != 0 || h.llm.callCount() != 0 {
				t.Fatalf("越权应零副作用, 实得 cards=%d sends=%d notices=%d llm=%d",
					h.cards.count(), h.sender.count(), len(h.notifier.all()), h.llm.callCount())
			}
		})
	}
}

// 未知动作是纵深兜底（feishu 侧白名单已挡）：人话 toast、零副作用。
func TestHandleClick_UnknownActionRejected(t *testing.T) {
	h := newHarness(t)
	res, err := h.svc.HandleClick(context.Background(), testUserID,
		Click{Action: types.FeedbackAction("rm -rf"), DeliveryID: testDeliveryID})
	if err != nil {
		t.Fatalf("未知动作不应报错: %v", err)
	}
	if res.Toast != "未知操作" {
		t.Fatalf("toast = %q, 期望「未知操作」", res.Toast)
	}
	if rows := h.st.allRows(); len(rows) != 0 {
		t.Fatalf("未知动作不得写反馈, 实得 %+v", rows)
	}
}

// ============================================================
// 降级路径
// ============================================================

// Notifier 为 nil（灰度装配/无 agent）时不得 panic，反馈照常记录。
func TestHandleClick_NilNotifierDoesNotPanic(t *testing.T) {
	h := newHarness(t)
	h.svc = New(Deps{
		Store:     h.st,
		Sender:    h.sender,
		Notifier:  nil, // 显式不注入
		BuildCard: h.cards.build,
	})

	res := h.click(t, types.FeedbackActionInterested)
	if res.Toast != "已记录：感兴趣" || res.CardJSON == "" {
		t.Fatalf("无 Notifier 时反馈应照常完成, 实得 %+v", res)
	}
	res = h.click(t, types.FeedbackActionMisjudged)
	if res.Toast != "已标记误判，将用于修正推送判断" {
		t.Fatalf("无 Notifier 时误判应照常完成, 实得 %+v", res)
	}
	if got := len(h.st.allRows()); got != 2 {
		t.Fatalf("应落 2 行, 实得 %d", got)
	}
}

// DB 故障（非 NotFound）如实上抛，由 feishu 侧翻译成兜底 toast；不得静默成功。
func TestHandleClick_DatabaseErrorPropagates(t *testing.T) {
	h := newHarness(t)
	h.st.getDeliveryErr = databaseErr("fake: 连接断开")

	if _, err := h.svc.HandleClick(context.Background(), testUserID,
		Click{Action: types.FeedbackActionInterested, DeliveryID: testDeliveryID}); err == nil {
		t.Fatal("DB 故障应上抛 error")
	}
}

// 状态重查失败只降级为"不更新卡片"：主操作已成功，不能把它报成失败。
func TestHandleClick_CardStateQueryFailureDegradesToNoCard(t *testing.T) {
	h := newHarness(t)
	// 先让插入成功，再让状态重查（HasFeedback）失败。
	h.st.hasErr = databaseErr("fake: 状态重查失败")

	res, err := h.svc.HandleClick(context.Background(), testUserID,
		Click{Action: types.FeedbackActionInterested, DeliveryID: testDeliveryID})
	if err != nil {
		t.Fatalf("重查失败不应把已成功的反馈报成失败: %v", err)
	}
	if res.Toast != "已记录：感兴趣" || !res.ToastOK {
		t.Fatalf("toast 应为成功文案, 实得 %q(ok=%v)", res.Toast, res.ToastOK)
	}
	if res.CardJSON != "" {
		t.Fatalf("状态重查失败时应不更新卡片, 实得 %q", res.CardJSON)
	}
	if got := len(h.st.allRows()); got != 1 {
		t.Fatalf("反馈仍应落行, 实得 %d", got)
	}
}

// ============================================================
// 聚合卡重建（附录 A.4）
// ============================================================

// TestRebuilt_聚合分流与状态隔离 同一 message 承载两条投递时点击走聚合重建：
// ① 兄弟条目各查各的状态、互不串染（force 只作用被点的那条）；
// ② header 从库存 card_json 原样解析回填（不随点击漂移）；
// ③ 历史单条卡（message 只有 1 条投递）仍走单条构卡，外观零变化。
func TestRebuilt_聚合分流与状态隔离(t *testing.T) {
	h := newHarness(t)
	// 兄弟投递：同 message、不同 delivery。
	sibID := testDeliveryID + 1
	sibItem := testItemID
	h.st.deliveries[sibID] = &types.Delivery{
		ID: sibID, UserID: testUserID, ContentItemID: &sibItem,
		Score: 70, BodyMD: "兄弟正文", FeishuMessageID: testMsgID,
		Status: types.DeliveryStatusSent, CreatedAt: time.Now(),
	}
	// 库存卡 JSON 带聚合 header（重建应原样解析回填）。
	h.delivery().CardJSON = []byte(`{"header":{"title":{"content":"🔮 测试任务 · 今日 2 条"},"template":"purple"}}`)

	var gotAgg *AggregateCardInput
	h.svc.deps.BuildAggCard = func(in AggregateCardInput) string {
		gotAgg = &in
		return `{"agg":"rebuilt"}`
	}

	res := h.click(t, types.FeedbackActionInterested)
	if res.CardJSON != `{"agg":"rebuilt"}` {
		t.Fatalf("双投递 message 应走聚合重建，实得 %q", res.CardJSON)
	}
	if gotAgg == nil {
		t.Fatal("BuildAggCard 未被调用")
	}
	if gotAgg.HeaderTitle != "🔮 测试任务 · 今日 2 条" || gotAgg.HeaderTemplate != "purple" {
		t.Errorf("header 应从库存 card_json 原样回填，实得 %q/%q", gotAgg.HeaderTitle, gotAgg.HeaderTemplate)
	}
	if len(gotAgg.Items) != 2 {
		t.Fatalf("聚合重建应含全部兄弟条目，实得 %d", len(gotAgg.Items))
	}
	// 被点的 42 状态应为 interested；兄弟 43 未表态（零值）——状态不串染。
	var clicked, sibling *CardInput
	for i := range gotAgg.Items {
		switch gotAgg.Items[i].DeliveryID {
		case testDeliveryID:
			clicked = &gotAgg.Items[i]
		case sibID:
			sibling = &gotAgg.Items[i]
		}
	}
	if clicked == nil || sibling == nil {
		t.Fatalf("聚合条目缺失: %+v", gotAgg.Items)
	}
	if clicked.State.Preference != types.FeedbackActionInterested {
		t.Errorf("被点条目状态应为 interested，实得 %q", clicked.State.Preference)
	}
	if sibling.State.Preference != "" || sibling.State.Misjudged {
		t.Errorf("兄弟条目状态不得被串染，实得 %+v", sibling.State)
	}
}

// TestRebuilt_单投递message仍走单条构卡 历史卡兼容：message 只有 1 条投递时
// 走原 BuildCard，聚合构卡不掺和——旧卡外观与行为零变化。
func TestRebuilt_单投递message仍走单条构卡(t *testing.T) {
	h := newHarness(t)
	aggCalled := false
	h.svc.deps.BuildAggCard = func(in AggregateCardInput) string {
		aggCalled = true
		return "{}"
	}
	res := h.click(t, types.FeedbackActionInterested)
	if aggCalled {
		t.Error("单投递 message 不该走聚合构卡（历史卡外观必须零变化）")
	}
	if res.CardJSON == "" {
		t.Error("单条路径仍应重建卡片")
	}
}
