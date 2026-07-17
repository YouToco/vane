package fetcher

import (
	"strings"
	"testing"
)

func TestWatchKey(t *testing.T) {
	cases := []struct {
		name, url, prev, next, want string
	}{
		{
			name: "首次建基线",
			url:  "https://example.com/pricing",
			prev: "",
			next: "abc123",
			want: "watch://https://example.com/pricing#->abc123",
		},
		{
			name: "A到B变化",
			url:  "https://example.com/pricing",
			prev: "aaa",
			next: "bbb",
			want: "watch://https://example.com/pricing#aaa->bbb",
		},
		{
			name: "B到A回滚与A到B不同",
			url:  "https://example.com/pricing",
			prev: "bbb",
			next: "aaa",
			want: "watch://https://example.com/pricing#bbb->aaa",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := watchKey(tc.url, tc.prev, tc.next)
			if got != tc.want {
				t.Errorf("watchKey(%q,%q,%q) = %q, 期望 %q", tc.url, tc.prev, tc.next, got, tc.want)
			}
		})
	}
	a2b := watchKey("https://x.com", "a", "b")
	b2a := watchKey("https://x.com", "b", "a")
	if a2b == b2a {
		t.Errorf("A→B 和 B→A 应产生不同 watchKey：%q vs %q", a2b, b2a)
	}
}

func TestExtractTableText(t *testing.T) {
	html := []byte(`<html><body>
	<table>
		<tr><th>Plan</th><th>Price</th></tr>
		<tr><td>Free</td><td>$0</td></tr>
		<tr><td>Pro</td><td>$20</td></tr>
	</table>
	</body></html>`)
	got, err := extractTableText(html)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if got != "Plan | Price\nFree | $0\nPro | $20" {
		t.Errorf("抽取结果不符: %q", got)
	}
}

func TestExtractTableText_NoTable(t *testing.T) {
	html := []byte(`<html><body><p>Hello World</p></body></html>`)
	got, err := extractTableText(html)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if got != "Hello World" {
		t.Errorf("无表格应退回 body 纯文本: %q", got)
	}
}

func TestSimpleDiff(t *testing.T) {
	old := "Plan | Price\nFree | $0\nPro | $20"
	new := "Plan | Price\nFree | $0\nPro | $25"
	got := simpleDiff(old, new)
	if got == "" || got == "(no visible line-level changes)" {
		t.Fatalf("应该检测到 Pro 价格变化，实得: %q", got)
	}
	if !strings.Contains(got, "- Pro | $20") || !strings.Contains(got, "+ Pro | $25") {
		t.Errorf("diff 应包含旧行(-)和新行(+):\n%s", got)
	}
}

func TestSimpleDiff_NoChange(t *testing.T) {
	text := "line1\nline2"
	got := simpleDiff(text, text)
	if got != "(no visible line-level changes)" {
		t.Errorf("相同文本应无变化: %q", got)
	}
}

func TestCountRows(t *testing.T) {
	cases := []struct{ in string; want int }{
		{"", 0},
		{"single line", 1},
		{"a\nb\nc", 3},
	}
	for _, tc := range cases {
		if got := countRows(tc.in); got != tc.want {
			t.Errorf("countRows(%q) = %d, 期望 %d", tc.in, got, tc.want)
		}
	}
}
