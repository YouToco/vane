package feishu

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/YouToco/vane/feedback"
	"github.com/YouToco/vane/types"
)

// deliveryCard 是推送卡的顶层解析结构。elements 用 RawMessage 承接：
// 元素是异质的（markdown / column_set），且元素个数本身就是断言对象
// （零值 state 不得多出状态行）。
type deliveryCard struct {
	Schema string `json:"schema"`
	Header struct {
		Title struct {
			Tag     string `json:"tag"`
			Content string `json:"content"`
		} `json:"title"`
	} `json:"header"`
	Body struct {
		Elements []json.RawMessage `json:"elements"`
	} `json:"body"`
}

// deliveryCardButton 是推送卡反馈按钮的解析结构（2.0 schema：交互挂 behaviors）。
//
// value 刻意解成 map[string]json.RawMessage 而非定型 struct：delivery_id 必须是
// 带引号的 JSON 字符串（契约 §10.1——SDK 把 value 解成 map[string]interface{}，
// JSON number 会变 float64，大 id 丢精度），只有比对原始字节才能把引号钉死；
// 顺带能断言 value 的字段集合恰好是协议规定的三个。
type deliveryCardButton struct {
	Tag   string `json:"tag"`
	Type  string `json:"type"`
	Width string `json:"width"`
	Text  struct {
		Tag     string `json:"tag"`
		Content string `json:"content"`
	} `json:"text"`
	Behaviors []struct {
		Type  string                     `json:"type"`
		Value map[string]json.RawMessage `json:"value"`
	} `json:"behaviors"`
}

// decodeDeliveryCard 解析 BuildDeliveryCard 的产物并断言顶层结构。
func decodeDeliveryCard(t *testing.T, raw string) deliveryCard {
	t.Helper()
	var card deliveryCard
	if err := json.Unmarshal([]byte(raw), &card); err != nil {
		t.Fatalf("推送卡不是合法 JSON: %v\n原文: %s", err, raw)
	}
	if card.Schema != "2.0" {
		t.Errorf("schema = %q, 期望 \"2.0\"", card.Schema)
	}
	if card.Header.Title.Tag != "plain_text" || card.Header.Title.Content != cardTitle {
		t.Errorf("header.title = {%q, %q}, 期望 {\"plain_text\", %q}",
			card.Header.Title.Tag, card.Header.Title.Content, cardTitle)
	}
	return card
}

// decodeMarkdownElement 解析一个 markdown 元素并返回其正文。
func decodeMarkdownElement(t *testing.T, raw json.RawMessage, what string) string {
	t.Helper()
	var md struct {
		Tag     string `json:"tag"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(raw, &md); err != nil {
		t.Fatalf("%s 元素解析失败: %v", what, err)
	}
	if md.Tag != "markdown" {
		t.Errorf("%s.tag = %q, 期望 \"markdown\"", what, md.Tag)
	}
	return md.Content
}

// decodeButtonColumns 解析按钮所在的 column_set，返回每列的唯一按钮。
// 契约 §10.2 要求四个按钮在**同一个 column_set**里（各列 width:auto，
// 对齐 BuildConfirmCard 写法）——散在 body 里会堆成四行，观感与协议都不对。
func decodeButtonColumns(t *testing.T, raw json.RawMessage) []deliveryCardButton {
	t.Helper()
	var colSet struct {
		Tag     string `json:"tag"`
		Columns []struct {
			Tag      string               `json:"tag"`
			Width    string               `json:"width"`
			Elements []deliveryCardButton `json:"elements"`
		} `json:"columns"`
	}
	if err := json.Unmarshal(raw, &colSet); err != nil {
		t.Fatalf("column_set 元素解析失败: %v", err)
	}
	if colSet.Tag != "column_set" {
		t.Fatalf("按钮容器 tag = %q, 期望 \"column_set\"", colSet.Tag)
	}
	if len(colSet.Columns) != 3 {
		t.Fatalf("columns 长度 = %d, 期望 3（三个反馈按钮）", len(colSet.Columns))
	}
	btns := make([]deliveryCardButton, 0, 3)
	for i, col := range colSet.Columns {
		if col.Tag != "column" {
			t.Errorf("columns[%d].tag = %q, 期望 \"column\"", i, col.Tag)
		}
		if col.Width != "auto" {
			t.Errorf("columns[%d].width = %q, 期望 \"auto\"", i, col.Width)
		}
		if len(col.Elements) != 1 {
			t.Fatalf("columns[%d].elements 长度 = %d, 期望 1", i, len(col.Elements))
		}
		btns = append(btns, col.Elements[0])
	}
	return btns
}

// TestBuildDeliveryCardStructure 钉死推送卡的 JSON 2.0 结构与按钮 value 协议
// （契约 §10.1/§10.2）。结构错了飞书不会报错、只会静默渲染失败或回调不响应，
// 所以逐字段断言而非等联调发现。
func TestBuildDeliveryCardStructure(t *testing.T) {
	// 刻意含引号、换行、Markdown 语法与中文：验证 JSON 转义不破坏解读正文
	// ——正文永不丢失是契约 §0 的红线。
	const bodyMD = "**AI 芯片新政**\n\n一句话摘要：\"出口管制\" 再收紧。\n\n[阅读原文](https://example.com/a?x=1&y=2)"
	raw := BuildDeliveryCard(feedback.CardInput{BodyMD: bodyMD, DeliveryID: 42, State: feedback.CardState{}})
	card := decodeDeliveryCard(t, raw)

	// 零值 state 无状态行：首发卡与 M5 之前的观感一致，只多一排按钮。
	if len(card.Body.Elements) != 2 {
		t.Fatalf("零值 state 的 body.elements 长度 = %d, 期望 2（markdown + column_set，无状态行）",
			len(card.Body.Elements))
	}

	if got := decodeMarkdownElement(t, card.Body.Elements[0], "正文"); got != bodyMD {
		t.Errorf("bodyMD 未原样出现:\n得到 %q\n期望 %q", got, bodyMD)
	}

	btns := decodeButtonColumns(t, card.Body.Elements[1])
	// 顺序即卡片上的左右顺序（契约 §10.2：两个 P0 态度在前，P1 深挖在后）。
	want := []struct {
		label  string
		action types.FeedbackAction
	}{
		{"👍", types.FeedbackActionInterested},
		{"👎", types.FeedbackActionNotInterested},
		{"🔍 深挖", types.FeedbackActionDeepDive},
	}
	for i, w := range want {
		btn := btns[i]
		if btn.Tag != "button" {
			t.Errorf("按钮[%d].tag = %q, 期望 \"button\"", i, btn.Tag)
		}
		if btn.Text.Tag != "plain_text" || btn.Text.Content != w.label {
			t.Errorf("按钮[%d].text = {%q, %q}, 期望 {\"plain_text\", %q}",
				i, btn.Text.Tag, btn.Text.Content, w.label)
		}
		if len(btn.Behaviors) != 1 {
			t.Fatalf("按钮[%d].behaviors 长度 = %d, 期望 1", i, len(btn.Behaviors))
		}
		if btn.Behaviors[0].Type != "callback" {
			t.Errorf("按钮[%d].behaviors[0].type = %q, 期望 \"callback\"", i, btn.Behaviors[0].Type)
		}

		// value 三字段齐全且恰好三个：多字段=协议漂移，少字段=回调解析必失败。
		val := btn.Behaviors[0].Value
		if len(val) != 3 {
			t.Errorf("按钮[%d] value 字段数 = %d, 期望 3（vane_action/fb/delivery_id），实际 %v",
				i, len(val), val)
		}
		assertRawJSONString := func(field, wantVal string) {
			t.Helper()
			got, ok := val[field]
			if !ok {
				t.Errorf("按钮[%d] value 缺字段 %q", i, field)
				return
			}
			// 比对原始字节：期望值带引号，数字/布尔等任何非字符串写法都会在此暴露。
			if want := `"` + wantVal + `"`; string(got) != want {
				t.Errorf("按钮[%d] value.%s 原始字节 = %s, 期望 %s", i, field, got, want)
			}
		}
		assertRawJSONString("vane_action", cardActionFeedback)
		assertRawJSONString("fb", string(w.action))
		// delivery_id 恒为**字符串**（契约 §10.1）：写成 JSON number 会被 SDK
		// 解成 float64，大 id 丢精度且 parseFeedbackValue 的类型断言会静默失败。
		assertRawJSONString("delivery_id", "42")
	}
}

// TestBuildDeliveryCardDeliveryIDPrecision 是 delivery_id 用字符串承载的存在理由：
// 超出 float64 安全整数范围（2^53）的 id 走 JSON number 会被静默改值，走字符串
// 则逐位无损。用 2^53+1 这个"float64 表示不出来"的确切值定向验证。
func TestBuildDeliveryCardDeliveryIDPrecision(t *testing.T) {
	const bigID = int64(9007199254740993) // 2^53+1：float64 会把它舍成 ...992
	raw := BuildDeliveryCard(feedback.CardInput{BodyMD: "正文", DeliveryID: bigID, State: feedback.CardState{}})

	if !strings.Contains(raw, `"delivery_id":"9007199254740993"`) {
		t.Errorf("delivery_id 未以字符串原样承载 %d，卡片 JSON: %s", bigID, raw)
	}
	if strings.Contains(raw, `"delivery_id":9007199254740993`) ||
		strings.Contains(raw, `"delivery_id":9.007199254740992e+15`) {
		t.Errorf("delivery_id 被写成 JSON number（大 id 会丢精度），卡片 JSON: %s", raw)
	}
}

// TestFeedbackStateLine 钉死状态行的文案与分隔符（契约 §10.2）。
// 文案是产品面的确切措辞（深度解读刻意无时态——此行定格后不再变），
// 分隔符恒为 " · "：三段拼接的顺序为 态度 → 误判 → 深度解读。
func TestFeedbackStateLine(t *testing.T) {
	cases := []struct {
		name  string
		state feedback.CardState
		want  string
	}{
		{
			name:  "零值无状态行",
			state: feedback.CardState{},
			want:  "",
		},
		{
			name:  "仅感兴趣",
			state: feedback.CardState{Preference: types.FeedbackActionInterested},
			want:  "✅ 已记录：感兴趣",
		},
		{
			name:  "仅不感兴趣",
			state: feedback.CardState{Preference: types.FeedbackActionNotInterested},
			want:  "🚫 已记录：不感兴趣",
		},
		{
			name:  "仅误判（误判独立于态度，可单独存在）",
			state: feedback.CardState{Misjudged: true},
			want:  "⚠️ 已标记误判",
		},
		{
			name:  "仅深度解读",
			state: feedback.CardState{DeepDiveRequested: true},
			want:  "📖 已请求深度解读（结果以回复消息送达）",
		},
		{
			name:  "感兴趣 + 误判",
			state: feedback.CardState{Preference: types.FeedbackActionInterested, Misjudged: true},
			want:  "✅ 已记录：感兴趣 · ⚠️ 已标记误判",
		},
		{
			name:  "感兴趣 + 深度解读",
			state: feedback.CardState{Preference: types.FeedbackActionInterested, DeepDiveRequested: true},
			want:  "✅ 已记录：感兴趣 · 📖 已请求深度解读（结果以回复消息送达）",
		},
		{
			name:  "误判 + 深度解读（无态度）",
			state: feedback.CardState{Misjudged: true, DeepDiveRequested: true},
			want:  "⚠️ 已标记误判 · 📖 已请求深度解读（结果以回复消息送达）",
		},
		{
			name: "三项齐全（不感兴趣 + 误判 + 深度解读）",
			state: feedback.CardState{
				Preference:        types.FeedbackActionNotInterested,
				Misjudged:         true,
				DeepDiveRequested: true,
			},
			want: "🚫 已记录：不感兴趣 · ⚠️ 已标记误判 · 📖 已请求深度解读（结果以回复消息送达）",
		},
		{
			// 纵深兜底：Preference 的合法取值只有 ""/interested/not_interested
			// （契约 §10.2），库里冒出别的值时不得渲染出"已记录：deep_dive"这类
			// 泄漏内部枚举的文案，只能当作未表态。
			name:  "非态度值落在 Preference 上不渲染态度段",
			state: feedback.CardState{Preference: types.FeedbackActionDeepDive},
			want:  "",
		},
		{
			name: "非态度值落在 Preference 上不影响其余段",
			state: feedback.CardState{
				Preference: types.FeedbackActionQuestion,
				Misjudged:  true,
			},
			want: "⚠️ 已标记误判",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := feedbackStateLine(tc.state); got != tc.want {
				t.Errorf("feedbackStateLine() = %q, 期望 %q", got, tc.want)
			}
		})
	}
}

// TestBuildDeliveryCardStateLinePlacement 验证状态行在卡片里的位置与出现条件：
// 非零值 state 时作为**第三个** element（正文 → 按钮 → 状态行）追加，且按钮与
// 正文一个不少——态度可改是产品语义，按钮不置灰也不消失（契约 §10.2）。
func TestBuildDeliveryCardStateLinePlacement(t *testing.T) {
	const bodyMD = "**标题**\n摘要"

	t.Run("零值 state 不追加状态行", func(t *testing.T) {
		card := decodeDeliveryCard(t, BuildDeliveryCard(feedback.CardInput{BodyMD: bodyMD, DeliveryID: 7, State: feedback.CardState{}}))
		if len(card.Body.Elements) != 2 {
			t.Fatalf("body.elements 长度 = %d, 期望 2（无状态行）", len(card.Body.Elements))
		}
	})

	t.Run("有反馈时状态行排在按钮之后", func(t *testing.T) {
		st := feedback.CardState{
			Preference:        types.FeedbackActionNotInterested,
			Misjudged:         true,
			DeepDiveRequested: true,
		}
		card := decodeDeliveryCard(t, BuildDeliveryCard(feedback.CardInput{BodyMD: bodyMD, DeliveryID: 7, State: st}))
		if len(card.Body.Elements) != 4 {
			t.Fatalf("body.elements 长度 = %d, 期望 4（markdown + column_set + 状态行 + 误判表单）",
				len(card.Body.Elements))
		}
		// 正文原样保留：点按钮后重建的是"同一张卡的新版本"，不是替换成结果文本。
		if got := decodeMarkdownElement(t, card.Body.Elements[0], "正文"); got != bodyMD {
			t.Errorf("状态行版本的正文 = %q, 期望原样 %q", got, bodyMD)
		}
		// 按钮常驻。
		if btns := decodeButtonColumns(t, card.Body.Elements[1]); len(btns) != 3 {
			t.Errorf("状态行版本的按钮数 = %d, 期望 3（按钮常驻）", len(btns))
		}
		got := decodeMarkdownElement(t, card.Body.Elements[2], "状态行")
		if want := feedbackStateLine(st); got != want {
			t.Errorf("状态行 = %q, 期望 %q", got, want)
		}
	})
}

// TestBuildDeliveryCardFormSubmitButton 钉死 👎 后误判表单的提交按钮合法性
// （2026-07-17 实测定位的 200673 根因）：飞书要求 form 容器内至少有一个
// form_action_type=submit 的提交按钮，且 form 内交互组件必须有非空 name
// （缺 name 报 200530）。缺失时整卡非法——发消息被拒（300123 "there is no
// submit button in the form container"），作为回调响应返回则客户端报
// 200673「返回了错误的卡片」，按钮永久转圈、回调被飞书重推。
func TestBuildDeliveryCardFormSubmitButton(t *testing.T) {
	raw := BuildDeliveryCard(feedback.CardInput{
		BodyMD: "正文", DeliveryID: 7,
		State: feedback.CardState{Preference: types.FeedbackActionNotInterested},
	})
	card := decodeDeliveryCard(t, raw)
	if len(card.Body.Elements) != 4 {
		t.Fatalf("body.elements 长度 = %d, 期望 4（markdown + column_set + 状态行 + 误判表单）",
			len(card.Body.Elements))
	}

	var form struct {
		Tag      string `json:"tag"`
		Name     string `json:"name"`
		Elements []struct {
			Tag            string `json:"tag"`
			Name           string `json:"name"`
			FormActionType string `json:"form_action_type"`
		} `json:"elements"`
	}
	if err := json.Unmarshal(card.Body.Elements[3], &form); err != nil {
		t.Fatalf("解析 form 元素失败: %v", err)
	}
	if form.Tag != "form" || form.Name == "" {
		t.Fatalf("form 元素 = {tag:%q, name:%q}, 期望 tag=\"form\" 且 name 非空", form.Tag, form.Name)
	}

	var hasSubmit bool
	for _, el := range form.Elements {
		// form 内所有交互组件（input/button）都必须有非空 name（200530）。
		if el.Name == "" {
			t.Errorf("form 内 %q 组件缺 name", el.Tag)
		}
		if el.Tag == "button" && el.FormActionType == "submit" {
			hasSubmit = true
		}
	}
	if !hasSubmit {
		t.Errorf("form 内没有 form_action_type=\"submit\" 的提交按钮（整卡会被飞书判非法）")
	}
}
