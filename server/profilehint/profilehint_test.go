package profilehint

import (
	"strings"
	"testing"

	"github.com/YouToco/vane/server/types"
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

// ---------- NegTail（Gate 探针 ⑤ 的期望值来源，契约 §16.5） ----------

func TestNegTail_None(t *testing.T) {
	tests := []struct {
		name string
		p    *types.Profile
	}{
		{"nil 画像", nil},
		{"空画像", &types.Profile{}},
		{"摘要无负面句", &types.Profile{Summary: "关注云原生与分布式系统。"}},
		// 「不感兴趣」缺冒号不算负面句：演化 prompt 规则 2 锁定的句式带冒号，
		// 而 scorer 的快通道区块头【近期不感兴趣·…】恰好是不带冒号的那种——
		// 这个边界就是探针 ⑤ 不被区块头误命中的第一道保险。
		{"缺冒号不算负面句", &types.Profile{Summary: "近期不感兴趣股市。"}},
		{"其它字段有值但摘要无负面句",
			&types.Profile{Industry: "软件", Tags: []string{"Go"}, Summary: "关注 AI。"}},
		{"摘要为空但有别的字段", &types.Profile{Industry: "软件", Tags: []string{"Go"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := NegTail(tc.p); got != "" {
				t.Errorf("应返回空串，实际 %q", got)
			}
		})
	}
}

func TestNegTail_Extracts(t *testing.T) {
	tests := []struct {
		name    string
		summary string
		want    string
	}{
		{"摘要末尾的负面句",
			"关注云原生。不感兴趣：A、B。", "不感兴趣：A、B。"},
		{"整个摘要就是负面句",
			"不感兴趣：加密货币。", "不感兴趣：加密货币。"},
		// 空白折叠：NegTail 内部先 singleLine，返回值是折叠**之后**的版本。
		// 必须如此——库里 summary 的原文可能带换行，而 hint 侧的负面句是
		// singleLine 之后的产物，两边不都折叠就没法逐字比对（探针 ⑤ 会假红）。
		{"负面句前有换行",
			"关注云原生。\n不感兴趣：A、B。", "不感兴趣：A、B。"},
		{"负面句内含换行与多空格",
			"关注云原生。\n不感兴趣：A、\n  B、\tC。", "不感兴趣：A、 B、 C。"},
		{"负面句后有尾随空白",
			"关注云原生。不感兴趣：A、B。\n\n  ", "不感兴趣：A、B。"},
		{"前缀前后被空白包夹",
			"前段。  \n  不感兴趣：  A、B。  ", "不感兴趣： A、B。"},
		// LastIndex 语义：多次出现取**最后一个**。尾段只会更短、不会丢——
		// 而"更短"对探针是安全方向（比对的是子串是否完整出现，短了只会更容易命中，
		// 不会把已被截断的尾巴判成完整）。
		{"多次出现取最后一个",
			"不感兴趣：A。补充：不感兴趣：B、C。", "不感兴趣：B、C。"},
		{"负面句自身再次出现前缀",
			"关注 AI。不感兴趣：股市。另外不感兴趣：八卦。", "不感兴趣：八卦。"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := NegTail(&types.Profile{Summary: tc.summary})
			if got != tc.want {
				t.Errorf("期望 %q，实际 %q", tc.want, got)
			}
		})
	}
}

// TestNegTail_IsSuffixOfBuild 是本函数存在的**全部理由**，也是探针 ⑤ 正确性的依据。
//
// 探针 ⑤ 拿 NegTail 的返回值去 llm_calls.user_prompt 的画像行里找完整匹配：
// 找不到即判"保尾逻辑（审查 F1）失效"。这个判定只有在
// 「NegTail(p) 必然是 Build(p) 的后缀」成立时才有意义——否则探针找的是一个
// 谁都不会写出来的串，恒 0 命中，恒红（或在别的写法下恒绿）。
//
// 故这条性质必须对**所有会触发截断的路径**成立，逐条覆盖：
//   - buildSummary:95   summary 超 summaryMaxRunes → truncateEllipsis(front) + neg
//   - capHint:114       整串超 hintMaxRunes → front[:headBudget] + ellipsis + neg
//   - 两条同时触发
//   - 负面句自身逼近护栏（headBudget 归零，允许略超 hintMaxRunes——F1 的优先级：
//     不得剪负面句 > 长度上限）
func TestNegTail_IsSuffixOfBuild(t *testing.T) {
	neg := "不感兴趣：加密货币、明星八卦、体育赛事。"
	tenTags := func() []string {
		tags := make([]string, 12)
		for i := range tags {
			tags[i] = strings.Repeat("签", 20)
		}
		return tags
	}

	tests := []struct {
		name string
		p    *types.Profile
	}{
		{"短摘要不触发任何截断",
			&types.Profile{Industry: "软件", Summary: "关注 AI。" + neg}},
		{"仅摘要截断（超 summaryMaxRunes）",
			&types.Profile{Summary: strings.Repeat("研", 400) + neg}},
		{"仅整串截断（摘要不超但字段合计超 hintMaxRunes）",
			&types.Profile{
				Industry:   strings.Repeat("行", 200),
				Occupation: strings.Repeat("职", 200),
				Tags:       tenTags(),
				Summary:    "关注 AI。" + neg,
			}},
		{"摘要截断与整串截断同时触发",
			&types.Profile{
				Industry:   strings.Repeat("行", 100),
				Occupation: strings.Repeat("职", 100),
				Tags:       tenTags(),
				Summary:    strings.Repeat("研", 480-runeLen(neg)) + neg,
			}},
		{"负面句逼近护栏，前段预算归零",
			&types.Profile{
				Industry: strings.Repeat("行", 100),
				Summary:  strings.Repeat("研", 100) + "不感兴趣：" + strings.Repeat("厌", 560) + "。",
			}},
		{"摘要原文带换行（Build 与 NegTail 都先单行化）",
			&types.Profile{Summary: strings.Repeat("研", 400) + "\n" + neg}},
		{"多个负面句前缀 + 截断",
			&types.Profile{Summary: strings.Repeat("研", 400) + "不感兴趣：A。" + neg}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotNeg := NegTail(tc.p)
			if gotNeg == "" {
				t.Fatal("用例前提：本画像应算得出负面句，否则整条断言是空转")
			}
			built := Build(tc.p)
			if built == "" {
				t.Fatal("用例前提：Build 不应为空")
			}
			if !strings.HasSuffix(built, gotNeg) {
				t.Errorf("NegTail 必须是 Build 产物的后缀（探针 ⑤ 的正确性依据）\n"+
					"NegTail = %q\nBuild 尾部 = %q", gotNeg, tail(built, runeLen(gotNeg)+20))
			}
		})
	}
}

// 无负面句时 NegTail 返回空串，而空串是任何串的后缀——上面那条性质会**平凡成立**。
// 探针 ⑤ 正因如此才必须把 ExpectedTail=="" 判成 yellow（不适用）而不是绿：
// 拿空串去 position() 恒命中，绿得毫无信息量。本用例把这个前提钉在这里。
func TestNegTail_EmptyIsVacuousSuffix(t *testing.T) {
	p := &types.Profile{Summary: strings.Repeat("研", 400)}
	if got := NegTail(p); got != "" {
		t.Fatalf("无负面句应返回空串，实际 %q", got)
	}
	if !strings.HasSuffix(Build(p), "") {
		t.Fatal("空串应是任何串的后缀——探针 ⑤ 不能用它当比对目标")
	}
}

// 负面句在 Build 里必须**逐字**存活：NegTail 的返回值不含省略号，
// 也不该在 Build 里被省略号切开。这条是上面后缀性质的加强版——
// 后缀成立但中间被塞了省略号的话，探针 ⑤ 的 position() 仍会失配。
func TestNegTail_NoEllipsisInsideNeg(t *testing.T) {
	neg := "不感兴趣：加密货币、明星八卦。"
	p := &types.Profile{
		Industry:   strings.Repeat("行", 200),
		Occupation: strings.Repeat("职", 200),
		Summary:    strings.Repeat("研", 400) + neg,
	}
	gotNeg := NegTail(p)
	if gotNeg != neg {
		t.Fatalf("NegTail 应原样返回负面句，期望 %q，实际 %q", neg, gotNeg)
	}
	if strings.Contains(gotNeg, ellipsis) {
		t.Errorf("负面句内不应出现省略号，实际 %q", gotNeg)
	}
	if n := strings.Count(Build(p), neg); n != 1 {
		t.Errorf("Build 产物里负面句应完整出现恰好 1 次，实际 %d 次", n)
	}
}
