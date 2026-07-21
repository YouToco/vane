package feishu

import (
	"encoding/json"
	"testing"
	"time"
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
		Config struct {
			UpdateMulti bool `json:"update_multi"`
		} `json:"config"`
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
	if !card.Config.UpdateMulti {
		t.Error("config.update_multi = false，终态回执必须允许同资源 Patch 重试")
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
		Config struct {
			UpdateMulti bool `json:"update_multi"`
		} `json:"config"`
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
	if !card.Config.UpdateMulti {
		t.Error("config.update_multi = false，确认卡必须允许后续原地更新")
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

// TestRelativeTimeAt 钉死日历日语义（bug 狩猎 2026-07-19 MEDIUM 修复）：
// 判据是用户时区（Asia/Shanghai）的日界，不是流逝时长/24——跨日两小时也是"昨天"，
// 同日二十小时也是"今天"。
func TestRelativeTimeAt(t *testing.T) {
	sh := func(y int, m time.Month, d, hh, mm int) time.Time {
		return time.Date(y, m, d, hh, mm, 0, 0, cardTZ)
	}
	now := sh(2026, 7, 19, 1, 0) // 北京 2026-07-19 01:00
	tests := []struct {
		name string
		t    time.Time
		want string
	}{
		{"昨晚 23:00（流逝仅 2h）→ 昨天", sh(2026, 7, 18, 23, 0), "昨天"},
		{"当日 00:30（同日）→ 今天", sh(2026, 7, 19, 0, 30), "今天"},
		{"未来时间戳（脏数据）→ 今天", sh(2026, 7, 20, 12, 0), "今天"},
		{"前天 → 2 天前", sh(2026, 7, 17, 23, 59), "2 天前"},
		{"UTC 表示的昨天下午（北京昨晚）→ 昨天", time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC), "昨天"},
		{"35 天前 → 1 月前", sh(2026, 6, 14, 1, 0), "1 月前"},
		{"400 天前 → 1 年前", sh(2025, 6, 14, 1, 0), "1 年前"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := relativeTimeAt(tt.t, now); got != tt.want {
				t.Errorf("relativeTimeAt(%v, %v) = %q, want %q", tt.t, now, got, tt.want)
			}
		})
	}
}

// TestPermanentRejection 钉死确定性拒收清单（bug 狩猎 2026-07-19 MAJOR 修复）：
// 卡片结构非法/收件人非法重试必然同败，不得烧 Temporal 重试预算；未知码保持可重试。
func TestPermanentRejection(t *testing.T) {
	for code, want := range map[int]bool{
		200673: true,  // 卡片结构非法（2026-07-17 生产实锤）
		230002: true,  // 收件人 open_id 非法
		230020: false, // 限流类：可重试
		0:      false, // 未知：可重试
	} {
		if got := permanentRejection(code); got != want {
			t.Errorf("permanentRejection(%d) = %v, want %v", code, got, want)
		}
	}
}
