package profilehint

import (
	"strings"
	"testing"

	"github.com/YouToco/vane/types"
)

func TestBuild_Empty(t *testing.T) {
	if got := Build(nil); got != "" {
		t.Errorf("nil 画像应返回空串，实际 %q", got)
	}
	if got := Build(&types.Profile{}); got != "" {
		t.Errorf("全空画像应返回空串，实际 %q", got)
	}
	// 只有空白字符的字段视同空：单行化后不产出任何字段。
	p := &types.Profile{Industry: "  \n ", Tags: []string{"", "  "}, Summary: "\t"}
	if got := Build(p); got != "" {
		t.Errorf("纯空白画像应返回空串，实际 %q", got)
	}
}

func TestBuild_GoldenFull(t *testing.T) {
	p := &types.Profile{
		Industry:   "软件",
		Occupation: "后端工程师",
		Tags:       []string{"Go", "AI", "数据库"},
		Summary:    "关注云原生与分布式系统。",
	}
	want := "行业：软件；职业：后端工程师；关注标签：Go、AI、数据库；摘要：关注云原生与分布式系统。"
	if got := Build(p); got != want {
		t.Errorf("黄金输出不匹配\n期望 %q\n实际 %q", want, got)
	}
}

func TestBuild_SkipEmptyFields(t *testing.T) {
	p := &types.Profile{Tags: []string{"Go", "AI"}}
	if got, want := Build(p), "关注标签：Go、AI"; got != want {
		t.Errorf("空字段应跳过，期望 %q，实际 %q", want, got)
	}
	p = &types.Profile{Industry: "金融", Summary: "关注宏观。"}
	if got, want := Build(p), "行业：金融；摘要：关注宏观。"; got != want {
		t.Errorf("空字段应跳过，期望 %q，实际 %q", want, got)
	}
}

func TestBuild_SingleLine(t *testing.T) {
	p := &types.Profile{
		Industry: "软件\n互联网",
		Tags:     []string{"G\no"},
		Summary:  "第一行\n第二行\r\n  第三行",
	}
	want := "行业：软件 互联网；关注标签：G o；摘要：第一行 第二行 第三行"
	got := Build(p)
	if got != want {
		t.Errorf("单行化黄金输出不匹配\n期望 %q\n实际 %q", want, got)
	}
	if strings.ContainsAny(got, "\r\n") {
		t.Errorf("输出必须是单行（硬约束），实际 %q", got)
	}
}

func TestBuild_TagsCappedAtTen(t *testing.T) {
	tags := make([]string, 12)
	for i := range tags {
		tags[i] = string(rune('a' + i))
	}
	want := "关注标签：a、b、c、d、e、f、g、h、i、j"
	if got := Build(&types.Profile{Tags: tags}); got != want {
		t.Errorf("标签应展示前 10 个，期望 %q，实际 %q", want, got)
	}
}

func TestBuild_TagsSkipBlank(t *testing.T) {
	p := &types.Profile{Tags: []string{"", "  ", "Go"}}
	if got, want := Build(p), "关注标签：Go"; got != want {
		t.Errorf("空白标签应跳过，期望 %q，实际 %q", want, got)
	}
}

func TestBuild_SummaryTruncatedNoNeg(t *testing.T) {
	p := &types.Profile{Summary: strings.Repeat("乙", 350)}
	want := "摘要：" + strings.Repeat("乙", summaryMaxRunes) + ellipsis
	if got := Build(p); got != want {
		t.Errorf("无负面句的截断黄金输出不匹配\n期望 %q\n实际 %q", want, got)
	}
}

func TestBuild_SummaryTruncatedKeepsNegTail(t *testing.T) {
	neg := "不感兴趣：股市。"
	p := &types.Profile{Summary: strings.Repeat("甲", 310) + neg}
	want := "摘要：" + strings.Repeat("甲", summaryMaxRunes) + ellipsis + neg
	if got := Build(p); got != want {
		t.Errorf("负面句保尾黄金输出不匹配\n期望 %q\n实际 %q", want, got)
	}
}

func TestBuild_SummaryOnlyNegSentence(t *testing.T) {
	p := &types.Profile{Summary: "不感兴趣：加密货币。"}
	if got, want := Build(p), "摘要：不感兴趣：加密货币。"; got != want {
		t.Errorf("纯负面句摘要应原样保留，期望 %q，实际 %q", want, got)
	}
}

// TestBuild_F1_NegTailSurvivesFullBudget 是审查 F1 的定向黄金用例：
// 500 rune summary（演化上限）+ 满 12 个 20 字标签把整串顶破 hintMaxRunes
// （各字段合计 566 rune），输出仍必须以完整「不感兴趣：…」句结尾——
// 自家截断常量不得剪掉慢通道负偏好。
func TestBuild_F1_NegTailSurvivesFullBudget(t *testing.T) {
	neg := "不感兴趣：加密货币、明星八卦、体育赛事。"
	front := strings.Repeat("研", 500-runeLen(neg))
	tags := make([]string, 12)
	for i := range tags {
		tags[i] = strings.Repeat("签", 20)
	}
	p := &types.Profile{
		Industry:   "软件与信息服务",
		Occupation: "后端工程师兼技术负责人",
		Tags:       tags,
		Summary:    front + neg,
	}
	if runeLen(p.Summary) != 500 {
		t.Fatalf("用例前提：summary 应为 500 rune，实际 %d", runeLen(p.Summary))
	}

	got := Build(p)
	if !strings.HasSuffix(got, neg) {
		t.Errorf("输出必须以完整负面句结尾，实际尾部 %q", tail(got, 40))
	}
	// 恰好 hintMaxRunes 证明整串护栏确实触发且走了保尾路径（前段预算 + 省略号 + 负面句 = 护栏）。
	if n := runeLen(got); n != hintMaxRunes {
		t.Errorf("超长保尾应恰好压到护栏，期望 %d，实际 %d", hintMaxRunes, n)
	}
	if strings.ContainsAny(got, "\r\n") {
		t.Errorf("输出必须是单行，实际 %q", got)
	}
}

func TestBuild_HintCapNoNeg(t *testing.T) {
	tags := make([]string, 10)
	for i := range tags {
		tags[i] = strings.Repeat("签", 20)
	}
	p := &types.Profile{
		Industry:   strings.Repeat("行", 200),
		Occupation: strings.Repeat("职", 200),
		Tags:       tags,
		Summary:    strings.Repeat("丙", 400),
	}
	got := Build(p)
	if n := runeLen(got); n != hintMaxRunes {
		t.Errorf("无负面句超长时应恰好截到护栏，期望 %d，实际 %d", hintMaxRunes, n)
	}
	if !strings.HasSuffix(got, ellipsis) {
		t.Errorf("截断应以省略号结尾，实际尾部 %q", tail(got, 10))
	}
}

// tail 取字符串末尾 n 个 rune，供断言失败时打印。
func tail(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[len(r)-n:])
}
