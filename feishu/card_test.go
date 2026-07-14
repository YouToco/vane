package feishu

import (
	"encoding/json"
	"testing"
)

// TestBuildReplyCard 断言卡片符合飞书 JSON 2.0 schema 的结构约定：
// schema/config/header/body 四个顶层字段齐全，markdown 是 body.elements
// 下的一等元素（v1 的 div+lark_md 写法在 2.0 下不生效，结构错了卡片
// 会静默渲染失败，所以用单测钉死结构而非等联调时才发现）。
func TestBuildReplyCard(t *testing.T) {
	// 刻意包含引号、换行与中文：验证 JSON 转义不破坏 markdown 原文。
	markdown := "**你好**，这是 \"见微 Vane\" 的回复\n第二行"
	raw := BuildReplyCard(markdown)

	// 顶层字段齐全性（2.0 schema 缺 schema 字段会被按 v1 解析）。
	var top map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &top); err != nil {
		t.Fatalf("卡片不是合法 JSON: %v\n原文: %s", err, raw)
	}
	for _, key := range []string{"schema", "config", "header", "body"} {
		if _, ok := top[key]; !ok {
			t.Errorf("缺少顶层字段 %q", key)
		}
	}

	var card struct {
		Schema string `json:"schema"`
		Header struct {
			Title struct {
				Tag     string `json:"tag"`
				Content string `json:"content"`
			} `json:"title"`
		} `json:"header"`
		Body struct {
			Elements []struct {
				Tag     string `json:"tag"`
				Content string `json:"content"`
			} `json:"elements"`
		} `json:"body"`
	}
	if err := json.Unmarshal([]byte(raw), &card); err != nil {
		t.Fatalf("卡片结构解析失败: %v", err)
	}

	if card.Schema != "2.0" {
		t.Errorf("schema = %q, 期望 \"2.0\"", card.Schema)
	}
	if card.Header.Title.Tag != "plain_text" {
		t.Errorf("header.title.tag = %q, 期望 \"plain_text\"", card.Header.Title.Tag)
	}
	if card.Header.Title.Content != cardTitle {
		t.Errorf("header.title.content = %q, 期望 %q", card.Header.Title.Content, cardTitle)
	}
	if len(card.Body.Elements) != 1 {
		t.Fatalf("body.elements 长度 = %d, 期望 1", len(card.Body.Elements))
	}
	if card.Body.Elements[0].Tag != "markdown" {
		t.Errorf("body.elements[0].tag = %q, 期望 \"markdown\"", card.Body.Elements[0].Tag)
	}
	if card.Body.Elements[0].Content != markdown {
		t.Errorf("markdown 正文经转义后不一致:\n得到 %q\n期望 %q", card.Body.Elements[0].Content, markdown)
	}
}

// confirmCardButton 是 TestBuildConfirmCard 的按钮解析结构（2.0 schema：
// 交互挂 behaviors，value 只带 vane_action/action_id）。
type confirmCardButton struct {
	Tag  string `json:"tag"`
	Type string `json:"type"`
	Text struct {
		Tag     string `json:"tag"`
		Content string `json:"content"`
	} `json:"text"`
	Behaviors []struct {
		Type  string `json:"type"`
		Value struct {
			VaneAction string `json:"vane_action"`
			ActionID   string `json:"action_id"`
		} `json:"value"`
	} `json:"behaviors"`
}

// TestBuildConfirmCard 钉死确认卡的 JSON 2.0 结构：markdown 摘要 +
// column_set 里确认/取消两个 callback 按钮。按钮结构错了不会报错、
// 只会静默不响应回调（M4 事实基准：v1 的 value 直挂写法在 2.0 下失效），
// 所以逐字段断言 behaviors 的 callback value。
func TestBuildConfirmCard(t *testing.T) {
	summary := "**add_source**\n\n类型：rss\n地址：https://example.com/feed"
	const actionID = "9b8f2a51-0000-4000-8000-123456789abc"
	raw := BuildConfirmCard(summary, actionID)

	var card struct {
		Schema string `json:"schema"`
		Header struct {
			Title struct {
				Content string `json:"content"`
			} `json:"title"`
		} `json:"header"`
		Body struct {
			Elements []json.RawMessage `json:"elements"`
		} `json:"body"`
	}
	if err := json.Unmarshal([]byte(raw), &card); err != nil {
		t.Fatalf("确认卡不是合法 JSON: %v\n原文: %s", err, raw)
	}
	if card.Schema != "2.0" {
		t.Errorf("schema = %q, 期望 \"2.0\"", card.Schema)
	}
	if card.Header.Title.Content != cardTitle {
		t.Errorf("header.title.content = %q, 期望 %q", card.Header.Title.Content, cardTitle)
	}
	if len(card.Body.Elements) != 2 {
		t.Fatalf("body.elements 长度 = %d, 期望 2（markdown + column_set）", len(card.Body.Elements))
	}

	var md struct {
		Tag     string `json:"tag"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(card.Body.Elements[0], &md); err != nil {
		t.Fatalf("markdown 元素解析失败: %v", err)
	}
	if md.Tag != "markdown" || md.Content != summary {
		t.Errorf("markdown 元素 = {%q, %q}, 期望 {\"markdown\", summary 原文}", md.Tag, md.Content)
	}

	var colSet struct {
		Tag     string `json:"tag"`
		Columns []struct {
			Elements []confirmCardButton `json:"elements"`
		} `json:"columns"`
	}
	if err := json.Unmarshal(card.Body.Elements[1], &colSet); err != nil {
		t.Fatalf("column_set 元素解析失败: %v", err)
	}
	if colSet.Tag != "column_set" {
		t.Fatalf("elements[1].tag = %q, 期望 \"column_set\"", colSet.Tag)
	}
	if len(colSet.Columns) != 2 {
		t.Fatalf("columns 长度 = %d, 期望 2", len(colSet.Columns))
	}

	assertButton := func(name string, col int, wantType, wantText, wantAction string) {
		t.Helper()
		if len(colSet.Columns[col].Elements) != 1 {
			t.Fatalf("%s 所在列 elements 长度 = %d, 期望 1", name, len(colSet.Columns[col].Elements))
		}
		btn := colSet.Columns[col].Elements[0]
		if btn.Tag != "button" {
			t.Errorf("%s.tag = %q, 期望 \"button\"", name, btn.Tag)
		}
		if btn.Type != wantType {
			t.Errorf("%s.type = %q, 期望 %q", name, btn.Type, wantType)
		}
		if btn.Text.Tag != "plain_text" || btn.Text.Content != wantText {
			t.Errorf("%s.text = {%q, %q}, 期望 {\"plain_text\", %q}", name, btn.Text.Tag, btn.Text.Content, wantText)
		}
		if len(btn.Behaviors) != 1 {
			t.Fatalf("%s.behaviors 长度 = %d, 期望 1", name, len(btn.Behaviors))
		}
		if btn.Behaviors[0].Type != "callback" {
			t.Errorf("%s.behaviors[0].type = %q, 期望 \"callback\"", name, btn.Behaviors[0].Type)
		}
		if got := btn.Behaviors[0].Value.VaneAction; got != wantAction {
			t.Errorf("%s value.vane_action = %q, 期望 %q", name, got, wantAction)
		}
		if got := btn.Behaviors[0].Value.ActionID; got != actionID {
			t.Errorf("%s value.action_id = %q, 期望 %q", name, got, actionID)
		}
	}
	assertButton("确认按钮", 0, "primary", "确认", cardActionConfirm)
	assertButton("取消按钮", 1, "default", "取消", cardActionCancel)
}
