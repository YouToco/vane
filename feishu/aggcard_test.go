package feishu

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/feedback"
	"github.com/YouToco/vane/types"
)

// xhsTokenURL 带 xsec_token 访问票据的真实形状小红书 URL。规格 A.3 硬约束：
// **测这类 bug 必须用带票据的 URL**——RSS/文档链接丢掉 query 照样能打开，
// href 被截断污染也测不出来（恒真）；只有票据丢了就打不开的链接才能证伪。
const xhsTokenURL = "https://www.xiaohongshu.com/explore/6a5258e9000000020803b20b?xsec_token=YBtt4_e0N5QbjAdXWnOfWUVNUcvmI0IcVFRQ2xeWTMOmA%3D&xsec_source=pc_search"

func TestDisplayURL_结构截断(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"带 query 截成问号省略", xhsTokenURL, "https://www.xiaohongshu.com/explore/6a5258e9000000020803b20b?…"},
		{"无 query 一字不动", "https://platform.claude.com/docs/en/about-claude/pricing",
			"https://platform.claude.com/docs/en/about-claude/pricing"},
		{"空串", "", ""},
		{"路径超硬上限按字符截", "https://e.com/" + strings.Repeat("p/", 60),
			("https://e.com/" + strings.Repeat("p/", 60))[:100] + "…"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := DisplayURL(c.in); got != c.want {
				t.Errorf("DisplayURL(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// TestAggregateCard_href逐字符保留 规格 A.3 的第一条硬约束：显示截断、href 完整。
// 断言构出的卡 JSON 里 markdown 链接的 () 内是逐字符的原始 URL（含票据）。
func TestAggregateCard_href逐字符保留(t *testing.T) {
	card := BuildAggregateCard(feedback.AggregateCardInput{Items: []feedback.CardInput{
		{DeliveryID: 1, Title: "小红书笔记", BodyMD: "正文", URL: xhsTokenURL},
	}})
	// 断言必须打在 JSON **解码后**的字符串上：json.Marshal 会把 & 转义成 \u0026
	//（JSON 层合法转义，飞书解码后还原），生码字符串比对会假阴性。
	contents := markdownContents(t, card)
	joined := strings.Join(contents, "\n")
	if !strings.Contains(joined, "("+xhsTokenURL+")") {
		t.Fatalf("解码后的 markdown 里 href 必须逐字符保留原始 URL（票据不可丢）：%s", joined)
	}
	// 显示文本是截断形（票据不出现在显示段）。
	if !strings.Contains(joined, "[https://www.xiaohongshu.com/explore/6a5258e9000000020803b20b?…]") {
		t.Errorf("显示文本应为结构截断形：%s", joined)
	}
	// 卡内不得出现裸的截断 URL 文本（不在 [] 里的 `?…` 结尾串会被飞书识别成无效链接）。
	if strings.Contains(joined, "：https://") {
		t.Errorf("疑似出现裸 URL 文本（v3 原型点不动的真因）：%s", joined)
	}
}

func TestAggregateCardEventEvidenceUsesSafeBoundedOrderedPrefix(t *testing.T) {
	published := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	sources := []feedback.CanonicalEvidenceSourceV1{
		{
			Ref: "source-1", SourceTitle: "[伪链接](https://evil.example)",
			Title: "first", SourceURL: "https://one.example/release",
			PublishedAt: &published, DiscoveredAt: published,
		},
		{
			Ref: "source-2", SourceTitle: "Second",
			Title: "second", SourceURL: "https://two.example/a(b)",
			DiscoveredAt: published.Add(time.Hour),
		},
		{
			Ref: "source-3", SourceTitle: "Third",
			Title: "third", SourceURL: "https://three.example/item",
			DiscoveredAt: published.Add(2 * time.Hour),
		},
		{
			Ref: "source-4", SourceTitle: "Fourth",
			Title: "fourth", SourceURL: "https://four.example/item",
			DiscoveredAt: published.Add(3 * time.Hour),
		},
	}
	card := BuildAggregateCard(feedback.AggregateCardInput{
		Items: []feedback.CardInput{{
			DeliveryID: 1, Title: "event", BodyMD: "body",
			URL:             "https://legacy.example/item",
			EvidenceSources: sources,
		}},
		CanonicalBrief: &feedback.CanonicalBriefCardV1{
			BatchID: 1, TotalItems: 1, VisibleItems: 1,
			WebURL: "https://vane.example/#/tasks/task-1",
		},
	})
	joined := strings.Join(markdownContents(t, card), "\n")
	for _, want := range []string{
		"**证据与原文**",
		"1. [［伪链接］（https：／／evil.example） · first](https://one.example/release) · 2026-07-28",
		"2. [Second · second](https://two.example/a%28b%29) · 2026-07-28",
		"3. [Third · third](https://three.example/item) · 2026-07-28",
		"另有 1 个证据，请在 Web 查看",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("evidence projection missing %q: %s", want, joined)
		}
	}
	for _, forbidden := range []string{
		"https://legacy.example/item",
		"https://four.example/item",
		"[伪链接](https://evil.example)",
		"原文链接：",
	} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("evidence projection leaked %q: %s", forbidden, joined)
		}
	}
}

func TestCanonicalEvidenceMarkdownOversizedOrInvalidFirstSourceFallsBack(
	t *testing.T,
) {
	now := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	for _, source := range []feedback.CanonicalEvidenceSourceV1{
		{
			Ref: "source-1", Title: "oversized",
			SourceURL: "https://example.com/" +
				strings.Repeat("x", aggEvidenceMarkdownMaxBytes),
			DiscoveredAt: now,
		},
		{
			Ref: "source-1", Title: "unsafe",
			SourceURL: "javascript:alert(1)", DiscoveredAt: now,
		},
	} {
		got := canonicalEvidenceMarkdownV1(
			[]feedback.CanonicalEvidenceSourceV1{source})
		if !strings.Contains(got, "多来源证据已冻结，请在 Web 查看") ||
			len(got) > aggEvidenceMarkdownMaxBytes ||
			strings.Contains(got, source.SourceURL) {
			t.Fatalf("fallback evidence markdown = %q", got)
		}
	}
}

func TestCanonicalEvidenceMarkdownAlwaysReservesOmissionNotice(t *testing.T) {
	now := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	got := canonicalEvidenceMarkdownV1(
		[]feedback.CanonicalEvidenceSourceV1{
			{
				Ref: "source-1", Title: "first",
				SourceURL: "https://example.com/" +
					strings.Repeat("x", 5400),
				DiscoveredAt: now,
			},
			{
				Ref: "source-2", Title: "second",
				SourceURL: "https://example.com/" +
					strings.Repeat("y", 1000),
				DiscoveredAt: now,
			},
		},
	)
	if len(got) > aggEvidenceMarkdownMaxBytes ||
		(!strings.Contains(got, "另有") &&
			!strings.Contains(got, "多来源证据已冻结")) {
		t.Fatalf("bounded evidence omitted its Web fallback: %q", got)
	}
}

func TestCanonicalEvidenceMarkdownNormalizesValidatedURLAndUntrustedLabel(
	t *testing.T,
) {
	published := time.Date(
		2026, 7, 27, 23, 30, 0, 0, time.UTC)
	source := types.StructuredEvidenceSourceV1{
		Ref:         "source-1",
		Title:       "safe\u202eelpmaxe.live `code` ~~strike~~",
		SourceTitle: "Vendor",
		Platform:    "web",
		SourceURL: "https://trusted.example/path with space" +
			"?ticket=SECRET",
		PublishedAt:  &published,
		DiscoveredAt: published,
	}
	if err := source.Validate(); err != nil {
		t.Fatalf("fixture must cross the valid Brief boundary: %v", err)
	}
	got := canonicalEvidenceMarkdownV1(
		[]feedback.CanonicalEvidenceSourceV1{{
			Ref: source.Ref, Title: source.Title,
			SourceTitle: source.SourceTitle, Platform: source.Platform,
			SourceURL: source.SourceURL, PublishedAt: source.PublishedAt,
			DiscoveredAt: source.DiscoveredAt,
		}},
	)
	for _, want := range []string{
		"https://trusted.example/path%20with%20space?ticket=SECRET",
		"2026-07-28",
		"｀code｀", "～～strike～～",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("safe evidence projection missing %q: %q", want, got)
		}
	}
	for _, forbidden := range []string{
		"\u202e", "path with space", "`code`", "~~strike~~",
		"2026-07-27",
	} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("unsafe evidence projection kept %q: %q",
				forbidden, got)
		}
	}
}

func TestCanonicalEvidenceMarkdownFallsBackAfterInvisibleLabelRemoval(
	t *testing.T,
) {
	got := canonicalEvidenceMarkdownV1(
		[]feedback.CanonicalEvidenceSourceV1{{
			Ref: "source-1", Title: "\u202e\u2066",
			SourceTitle: "\u200b\u200c", Platform: "\ufeff",
			SourceURL: "https://example.com/item",
			DiscoveredAt: time.Date(
				2026, 7, 28, 0, 0, 0, 0, time.UTC),
		}},
	)
	if !strings.Contains(got, "[source-1]") ||
		strings.Contains(got, "[]") {
		t.Fatalf("invisible evidence label fallback = %q", got)
	}
}

// TestAggregateCard_双form名互异 规格 A.4：两条同时待填时 form/input/submit 的 name
// 必须按 delivery_id 互异——单条卡的硬编码 name 在 N 条 form 并存时必然重名，
// 正是"对 B 条说推错、记到 A 条"的物理路径。
func TestAggregateCard_双form名互异(t *testing.T) {
	nin := feedback.CardState{BadFeedbackOpen: true}
	card := BuildAggregateCard(feedback.AggregateCardInput{Items: []feedback.CardInput{
		{DeliveryID: 101, Title: "甲", State: nin},
		{DeliveryID: 102, Title: "乙", State: nin},
	}})
	for _, want := range []string{
		`"name":"fbr_101"`, `"name":"detail_101"`, `"name":"submit_101_outdated_or_out_of_window"`,
		`"name":"fbr_102"`, `"name":"detail_102"`, `"name":"submit_102_outdated_or_out_of_window"`,
	} {
		if !strings.Contains(card, want) {
			t.Errorf("双 form 卡应含唯一化 name %s，卡：%s", want, card)
		}
	}
	// 旧世界的硬编码 name 绝不能出现在聚合卡里。
	for _, bad := range []string{`"name":"feedback_reason"`, `"name":"reason"`, `"name":"submit_reason_outdated_or_out_of_window"`} {
		if strings.Contains(card, bad) {
			t.Errorf("聚合卡不得出现硬编码 form name %s（串条物理路径），卡：%s", bad, card)
		}
	}
}

// TestAggregateCard_form硬约束 历史事故 200530/300123/200673：form 内交互组件必须有
// name，且必须含 form_action_type=submit 的提交按钮——结构性校验防整卡被拒。
func TestAggregateCard_form硬约束(t *testing.T) {
	card := BuildAggregateCard(feedback.AggregateCardInput{Items: []feedback.CardInput{
		{DeliveryID: 7, Title: "x", State: feedback.CardState{BadFeedbackOpen: true}},
	}})
	var c struct {
		Body struct {
			Elements []map[string]any `json:"elements"`
		} `json:"body"`
	}
	if err := json.Unmarshal([]byte(card), &c); err != nil {
		t.Fatalf("卡不是合法 JSON: %v", err)
	}
	var form map[string]any
	for _, el := range c.Body.Elements {
		if el["tag"] == "form" {
			form = el
			break
		}
	}
	if form == nil {
		t.Fatal("打开问题面板时应渲染 form")
	}
	els, _ := form["elements"].([]any)
	hasSubmit := false
	for _, e := range els {
		m, _ := e.(map[string]any)
		if m["tag"] == "button" && m["form_action_type"] == "submit" && m["name"] != "" {
			hasSubmit = true
		}
		if m["tag"] == "input" && (m["name"] == nil || m["name"] == "") {
			t.Error("form 内 input 必须有 name（200530）")
		}
	}
	if !hasSubmit {
		t.Error("form 必须含 form_action_type=submit 且带 name 的提交按钮（300123/200673）")
	}
}

// TestAggregateCard_状态门控 form 只在该条暂态打开且未 misjudged 时出现；
// 兄弟条目的状态互不串染。深挖按钮不得出现在聚合卡。
func TestAggregateCardEffectMarkerCoversEveryCallback(t *testing.T) {
	const effectID = "019f9824-39b6-7e13-b247-b5ee5713c52b"
	card := BuildAggregateCard(feedback.AggregateCardInput{
		EffectID: effectID,
		Items: []feedback.CardInput{{
			DeliveryID: 7,
			Title:      "marker",
			State: feedback.CardState{
				BadFeedbackOpen: true,
			},
		}},
	})
	callbacks := strings.Count(card, `"type":"callback"`)
	if got := strings.Count(card, `"effect_id":"`+effectID+`"`); got != callbacks {
		t.Fatalf("effect marker count = %d, want every one of %d callbacks: %s",
			got, callbacks, card)
	}
	legacy := BuildAggregateCard(feedback.AggregateCardInput{
		Items: []feedback.CardInput{{DeliveryID: 7, Title: "legacy"}},
	})
	if strings.Contains(legacy, `"effect_id"`) {
		t.Fatalf("empty marker changed historical card shape: %s", legacy)
	}
}

func TestAggregateCardCanonicalBriefPrefixCarriesDeepLinkMetadata(t *testing.T) {
	items := []feedback.CardInput{
		{DeliveryID: 11, Title: "one", BodyMD: "body one"},
		{DeliveryID: 12, Title: "two", BodyMD: "body two"},
		{DeliveryID: 13, Title: "three", BodyMD: "body three"},
	}
	card := BuildAggregateCard(feedback.AggregateCardInput{
		HeaderTitle: "canonical · 今日 5 条",
		Items:       items,
		CanonicalBrief: &feedback.CanonicalBriefCardV1{
			BatchID: 91, TotalItems: 5, VisibleItems: 3,
			WebURL: "https://vane.example/#/tasks/task-1",
		},
	})
	if !strings.Contains(card, "另有 2 条，在 Web 查看完整简报") ||
		!strings.Contains(card, "https://vane.example/#/tasks/task-1") {
		t.Fatalf("canonical footer missing: %s", card)
	}
	for _, marker := range []string{
		`"brief_batch_id":"91"`,
		`"brief_total":"5"`,
		`"brief_visible":"3"`,
		`"brief_url":"https://vane.example/#/tasks/task-1"`,
	} {
		if !strings.Contains(card, marker) {
			t.Fatalf("canonical callback metadata %s missing: %s", marker, card)
		}
	}
	if strings.Contains(card, "four") || strings.Contains(card, "five") {
		t.Fatalf("card rendered content outside the supplied prefix: %s", card)
	}
}

func TestAggregateCardInvalidCanonicalMetadataStaysLegacy(t *testing.T) {
	card := BuildAggregateCard(feedback.AggregateCardInput{
		Items: []feedback.CardInput{{
			DeliveryID: 11, Title: "one",
		}},
		CanonicalBrief: &feedback.CanonicalBriefCardV1{
			BatchID: 91, TotalItems: 5, VisibleItems: 3,
			WebURL: "javascript:alert(1)",
		},
	})
	if strings.Contains(card, "brief_batch_id") ||
		strings.Contains(card, "查看完整简报") ||
		strings.Contains(card, "javascript:") {
		t.Fatalf("invalid canonical metadata reached card: %s", card)
	}
}

func TestAggregateCard_状态门控(t *testing.T) {
	card := BuildAggregateCard(feedback.AggregateCardInput{Items: []feedback.CardInput{
		{DeliveryID: 1, Title: "未表态"},
		{DeliveryID: 2, Title: "已误判", State: feedback.CardState{BadFeedbackOpen: true, Misjudged: true}},
		{DeliveryID: 3, Title: "待填原因", State: feedback.CardState{BadFeedbackOpen: true}},
	}})
	if strings.Contains(card, `"name":"fbr_1"`) || strings.Contains(card, `"name":"fbr_2"`) {
		t.Error("未表态/已误判的条目不该渲染 form")
	}
	if !strings.Contains(card, `"name":"fbr_3"`) {
		t.Error("打开问题面板且未提交的条目应渲染 form")
	}
	if strings.Contains(card, "深挖") {
		t.Error("深挖按钮不得出现在聚合卡面（附录 A.2）")
	}
}

// TestAggHeaderForTask_确定性 同名恒同色同 emoji；空名落兜底。
func TestAggHeaderForTask_确定性(t *testing.T) {
	t1, c1 := AggHeaderForTask("Anthropic 动态", 3)
	t2, c2 := AggHeaderForTask("Anthropic 动态", 3)
	if t1 != t2 || c1 != c2 {
		t.Errorf("同任务名应恒定: %q/%q vs %q/%q", t1, c1, t2, c2)
	}
	if !strings.Contains(t1, "Anthropic 动态") || !strings.Contains(t1, "今日 3 条") {
		t.Errorf("标题应含任务名与条数: %q", t1)
	}
	te, ce := AggHeaderForTask("", 2)
	if !strings.Contains(te, "今日推送") || ce == "" {
		t.Errorf("空任务名应落兜底: %q/%q", te, ce)
	}
}

// TestExtractReasonFromForm_三重对齐 附录 A.4 的串条闸门。
func TestExtractReasonFromForm_三重对齐(t *testing.T) {
	cases := []struct {
		name       string
		actionName string
		fv         map[string]interface{}
		deliveryID int64
		want       string
		wantErr    bool
	}{
		{"历史卡", "submit_reason", map[string]interface{}{"reason": "太水"}, 5, "太水", false},
		{"聚合卡对齐", "submit_101", map[string]interface{}{"reason_101": "不相关"}, 101, "不相关", false},
		{"聚合卡对齐但原因跳过", "submit_101", map[string]interface{}{}, 101, "", false},
		{"对齐失败_name指向别的条目", "submit_102", map[string]interface{}{"reason_102": "x"}, 101, "", true},
		{"对齐失败_乱name", "submit_evil", map[string]interface{}{"reason_101": "x"}, 101, "", true},
		{"空name_纯历史形状", "", map[string]interface{}{"reason": "旧"}, 5, "旧", false},
		{"空name_但含聚合键_拒绝", "", map[string]interface{}{"reason_9": "x"}, 9, "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := extractReasonFromForm(c.actionName, c.fv, c.deliveryID, "")
			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, c.wantErr)
			}
			if got != c.want {
				t.Errorf("reason = %q, want %q", got, c.want)
			}
		})
	}
}

// markdownContents 解码卡 JSON，收集全部 markdown 元素的 content（含 form 内嵌套）。
func markdownContents(t *testing.T, card string) []string {
	t.Helper()
	var c struct {
		Body struct {
			Elements []json.RawMessage `json:"elements"`
		} `json:"body"`
	}
	if err := json.Unmarshal([]byte(card), &c); err != nil {
		t.Fatalf("卡不是合法 JSON: %v", err)
	}
	var out []string
	var walk func(raw json.RawMessage)
	walk = func(raw json.RawMessage) {
		var el struct {
			Tag      string            `json:"tag"`
			Content  string            `json:"content"`
			Elements []json.RawMessage `json:"elements"`
		}
		if json.Unmarshal(raw, &el) != nil {
			return
		}
		if el.Tag == "markdown" {
			out = append(out, el.Content)
		}
		for _, sub := range el.Elements {
			walk(sub)
		}
	}
	for _, e := range c.Body.Elements {
		walk(e)
	}
	return out
}

func TestBuildAggregateCardExecutiveSummaryPrecedesCanonicalTopItems(
	t *testing.T,
) {
	content := types.ExecutiveBriefContentV1{
		Headline:         "需要关注的竞争变化",
		ExecutiveSummary: "两条证据共同显示市场进入加速阶段。",
		DecisionState:    types.ExecutiveDecisionWatch,
		WhyForYou:        "这会影响你的产品发布时间。",
		Signals: []types.ExecutiveSignalV1{{
			Kind:  types.ExecutiveSignalTrend,
			Title: "加速", Summary: "发布频率增加",
			EvidenceRefs: []types.ExecutiveEvidenceRefV1{{
				InsightID: 10, ClaimIndexes: []int{0},
			}},
		}},
	}
	card := BuildAggregateCard(feedback.AggregateCardInput{
		Executive:        &content,
		ExecutivePartial: true,
		Items: []feedback.CardInput{{
			DeliveryID: 10, Title: "第一条", BodyMD: "证据正文",
		}},
	})
	if strings.Index(card, "需要关注的竞争变化") < 0 ||
		strings.Index(card, "需要关注的竞争变化") >
			strings.Index(card, "第一条") ||
		!strings.Contains(card, "覆盖不完整") {
		t.Fatalf("executive summary is not the card prefix: %s", card)
	}
}

func TestBuildAggregateCardRendersClaimlessExecutiveFallbackOnlyWhenMarked(
	t *testing.T,
) {
	content := types.ExecutiveBriefContentV1{
		Headline:         "最高优先级内容证据不足",
		ExecutiveSummary: "本期仍可阅读逐条情报，但暂不形成行动判断。",
		DecisionState:    types.ExecutiveDecisionInsufficientEvidence,
		WhyForYou:        "等待更多可验证证据后再判断。",
		Signals:          []types.ExecutiveSignalV1{},
		NextSteps:        []types.ExecutiveNextStepV1{},
	}
	input := feedback.AggregateCardInput{
		Executive: &content,
		Items: []feedback.CardInput{{
			DeliveryID: 10, Title: "第一条", BodyMD: "证据正文",
		}},
	}
	if card := BuildAggregateCard(input); strings.Contains(
		card, "最高优先级内容证据不足",
	) {
		t.Fatalf("claimless model content was rendered: %s", card)
	}
	input.ExecutiveFallback = true
	input.ExecutivePartial = true
	card := BuildAggregateCard(input)
	if !strings.Contains(card, "最高优先级内容证据不足") ||
		!strings.Contains(card, "覆盖不完整") ||
		strings.Index(card, "最高优先级内容证据不足") >
			strings.Index(card, "第一条") {
		t.Fatalf("claimless fallback was not rendered as prefix: %s", card)
	}
}
