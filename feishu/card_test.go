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
