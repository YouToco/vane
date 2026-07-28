// 聚合推送卡（card-redesign-spec.md 附录 A，2026-07-18 定稿）：一个任务一张卡，
// 卡内 N 条情报，每条挂各自的 [👍][👎] 与条件 form。与单条卡（card.go）的三处实质差异：
//   - 🔍 深挖不上卡面（生产 21 条反馈里仅 2 次点击，却是最重的动作；能力保留在 agent 侧）；
//   - 「阅读原文」按钮 → 可见的原文链接文本（Boss：「看不到 url 从哪来会没有安全感」）；
//   - form/input/submit 的 name 按 delivery_id 唯一化——单条卡的硬编码 name 在
//     N 条 form 并存时必然重名，正是"对 B 条说推错、记到 A 条"的物理路径。
//
// 历史单条卡（聊天里已发出的）不受影响：BuildDeliveryCard 原样保留，
// 重建路径按"同 feishu_message_id 的 delivery 数量"分流（见 feedback.rebuilt）。
package feishu

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/url"
	"strconv"
	"strings"

	"github.com/YouToco/vane/feedback"
)

const (
	// aggDefaultTitle 无任务名时的 header 兜底（存量调度的 PushParams 没有 NLDesc）。
	aggDefaultTitle = "📮 今日推送"
	// aggURLPathHardLimit 原文链接展示的路径硬上限：截断规则是"路径全留、query 省略成 ?…"
	// （附录 A.2 实测定稿——域名与路径是判断来源的唯一依据必须完整；被省的 query 要么是
	// 追踪垃圾要么是读不了的票据）。路径本身超长才走这个硬上限。
	aggURLPathHardLimit = 100
	// P2-C evidence is a compact prefix of the full Web projection. Both caps
	// are deterministic so initial delivery and callback rebuild stay equal,
	// while one adversarial-but-valid URL cannot consume the provider limit.
	aggEvidenceMaxVisible       = 3
	aggEvidenceMarkdownMaxBytes = 6 << 10
	aggEvidenceLabelMaxRunes    = 120
)

// aggHeaderTemplates 任务名哈希取色的调色板。蓝在首位：兜底与单任务期的默认色。
var aggHeaderTemplates = []string{"blue", "wathet", "turquoise", "green", "orange", "purple", "carmine"}

// aggTaskEmojis 任务名哈希取 emoji 的候选集（人工挑选，避免任意 emoji 观感随机）。
var aggTaskEmojis = []string{"🔮", "📮", "🗞️", "📡", "🧭", "🛰️"}

// AggHeaderForTask 由任务名派生聚合卡 header（Push 首发路径用）。
// 任务名为空返回兜底标题与默认色。同名恒同色同 emoji（fnv 哈希，确定性）。
func AggHeaderForTask(taskName string, n int) (title, template string) {
	taskName = strings.TrimSpace(taskName)
	if taskName == "" {
		return fmt.Sprintf("%s · 今日 %d 条", aggDefaultTitle, n), aggHeaderTemplates[0]
	}
	h := fnv.New32a()
	h.Write([]byte(taskName))
	sum := h.Sum32()
	emoji := aggTaskEmojis[sum%uint32(len(aggTaskEmojis))]
	tmpl := aggHeaderTemplates[(sum/7)%uint32(len(aggHeaderTemplates))]
	return fmt.Sprintf("%s %s · 今日 %d 条", emoji, taskName, n), tmpl
}

// DisplayURL 按结构截断 URL 用于展示：路径全留、query 省略成 "?…"；
// 无 query 的干净 URL 一字不动；路径本身超硬上限才按字符截。
// 点击跳转恒用完整原 URL（href 逐字符保留）——展示与跳转分离是本函数存在的全部意义。
func DisplayURL(raw string) string {
	if raw == "" {
		return ""
	}
	base, _, hasQ := strings.Cut(raw, "?")
	if len(base) > aggURLPathHardLimit {
		return base[:aggURLPathHardLimit] + "…"
	}
	if hasQ {
		return base + "?…"
	}
	return base
}

// BuildAggregateCard 构造聚合推送卡 JSON（schema 2.0）。首发与点击重建共用：
// 同一构卡函数 + 各条各自的 CardState = 同一张卡的不同版本，正文永远原样保留。
func BuildAggregateCard(in feedback.AggregateCardInput) string {
	title := strings.TrimSpace(in.HeaderTitle)
	if title == "" {
		title, _ = AggHeaderForTask("", len(in.Items))
	}
	tmpl := in.HeaderTemplate
	if tmpl == "" {
		tmpl = aggHeaderTemplates[0]
	}

	elements := make([]any, 0, len(in.Items)*6)
	for i, item := range in.Items {
		if i > 0 {
			elements = append(elements, map[string]any{"tag": "hr"})
		}
		elements = append(elements,
			aggItemElements(item, in.EffectID, in.CanonicalBrief)...)
	}
	if brief := in.CanonicalBrief; validCanonicalBriefCardV1(
		brief, len(in.Items),
	) {
		label := "在 Web 查看完整简报"
		if remaining := brief.TotalItems - brief.VisibleItems; remaining > 0 {
			label = fmt.Sprintf("另有 %d 条，在 Web 查看完整简报", remaining)
		}
		elements = append(elements,
			map[string]any{"tag": "hr"},
			map[string]any{
				"tag": "markdown",
				"content": fmt.Sprintf(
					"<font color='grey'>[%s](%s)</font>",
					label, brief.WebURL,
				),
			},
		)
	}

	card := map[string]any{
		"schema": "2.0",
		"config": map[string]any{},
		"header": map[string]any{
			"title":    map[string]any{"tag": "plain_text", "content": title},
			"subtitle": map[string]any{"tag": "plain_text", "content": "见微 Vane · 按相关度排序"},
			"template": tmpl,
		},
		"body": map[string]any{"elements": elements},
	}
	raw, _ := json.Marshal(card)
	return string(raw)
}

// aggItemElements 渲染卡内单条情报：标题行 / 来源行 / 解读 / 原文链接 / 按钮 / 状态行 / 条件 form。
func aggItemElements(
	input feedback.CardInput,
	effectID string,
	brief *feedback.CanonicalBriefCardV1,
) []any {
	idStr := strconv.FormatInt(input.DeliveryID, 10)
	els := make([]any, 0, 7)

	// 标题 + ⚡分数（header 已被任务名占用，标题降级为条内首行加粗）。
	titleLine := "**" + escapeMarkdown(input.Title) + "**"
	if input.Title == "" {
		titleLine = "**（无标题）**"
	}
	if input.Score > 0 {
		titleLine += fmt.Sprintf("　<font color='orange'>⚡%d</font>", input.Score)
	}
	els = append(els, map[string]any{"tag": "markdown", "content": titleLine})

	// 来源行：{平台emoji} {栏目} · {域名} · {相对时间}（复用单条卡 subtitle 逻辑）。
	if sub := buildSubtitle(input); sub != "" {
		els = append(els, map[string]any{"tag": "markdown", "content": "<font color='grey'>" + sub + "</font>"})
	}

	if input.BodyMD != "" {
		els = append(els, map[string]any{"tag": "markdown", "content": input.BodyMD})
	}

	// P2-C uses the immutable ordered evidence inventory and does not duplicate
	// the legacy single-source link. The full source set remains available at
	// the canonical Web link appended to the card.
	if len(input.EvidenceSources) > 0 {
		els = append(els, map[string]any{
			"tag": "markdown", "content": canonicalEvidenceMarkdownV1(
				input.EvidenceSources),
		})
	} else if u := strings.TrimSpace(input.URL); u != "" {
		// 原文链接：截断显示 + 完整 href。附录 A.3 硬约束——绝不能出现裸的截断 URL 文本
		//（飞书会把它自动识别成无效链接），必须包在 markdown 链接语法里。
		els = append(els, map[string]any{"tag": "markdown",
			"content": fmt.Sprintf("<font color='grey'>原文链接：</font>[%s](%s)", DisplayURL(u), u)})
	}

	// 反馈按钮：仅 👍👎（深挖不上聚合卡面）。value 结构与单条卡完全一致
	//（vane_action=fb + fb + delivery_id），handler 的按钮路由零改动。
	btnColumns := make([]any, 0, 2)
	for _, b := range feedbackButtons[:2] {
		btnColumns = append(btnColumns, map[string]any{
			"tag": "column", "width": "auto",
			"elements": []any{map[string]any{
				"tag": "button", "type": "default", "width": "default",
				"text": map[string]any{"tag": "plain_text", "content": b.label},
				"behaviors": []any{map[string]any{
					"type": "callback",
					"value": aggregateCallbackValue(
						cardActionFeedback, string(b.action), idStr, effectID,
						brief,
					),
				}},
			}},
		})
	}
	els = append(els, map[string]any{"tag": "column_set", "columns": btnColumns})

	if line := feedbackStateLine(input.State); line != "" {
		els = append(els, map[string]any{"tag": "markdown", "content": line})
	}

	// 条件 form：仅点击 👎 的瞬态响应渲染，且**渲染在该条自己的
	// 元素块内**（需求 (b)）。name 三件套按 delivery_id 唯一化——这是三重对齐断言
	//（handler 侧）的产生端：form=fbr_{id} / input=reason_{id} / submit=submit_{id}，
	// 提交回调靠 Action.Name 后缀与 value.delivery_id 互验。
	if input.State.BadFeedbackOpen && !input.State.Misjudged {
		els = append(els, aggReasonForm(idStr, effectID))
	}
	return els
}

func canonicalEvidenceMarkdownV1(
	sources []feedback.CanonicalEvidenceSourceV1,
) string {
	const heading = "**证据与原文**\n"
	if len(sources) == 0 {
		return ""
	}
	var body strings.Builder
	body.WriteString(heading)
	visible := 0
	for index, source := range sources {
		if visible >= aggEvidenceMaxVisible {
			break
		}
		if source.Ref != "source-"+strconv.Itoa(index+1) {
			break
		}
		label := strings.TrimSpace(source.SourceTitle)
		if label == "" {
			label = strings.TrimSpace(source.Platform)
		}
		title := strings.TrimSpace(source.Title)
		if label != "" && title != "" {
			label += " · " + title
		} else if title != "" {
			label = title
		}
		if label == "" {
			label = source.Ref
		}
		label = escapeEvidenceLabelV1(truncateRunesV1(
			label, aggEvidenceLabelMaxRunes))
		sourceURL, valid := canonicalEvidenceURLV1(source.SourceURL)
		if !valid {
			break
		}
		observedAt := source.DiscoveredAt
		if source.PublishedAt != nil {
			observedAt = source.PublishedAt.UTC()
		}
		if observedAt.IsZero() {
			break
		}
		line := fmt.Sprintf(
			"%d. [%s](%s) · %s\n",
			index+1, label, sourceURL,
			observedAt.UTC().Format("2006-01-02"),
		)
		if body.Len()+len(line) > aggEvidenceMarkdownMaxBytes {
			break
		}
		body.WriteString(line)
		visible++
	}
	if visible == 0 {
		return heading + "<font color='grey'>多来源证据已冻结，请在 Web 查看</font>"
	}
	if remaining := len(sources) - visible; remaining > 0 {
		suffix := fmt.Sprintf(
			"<font color='grey'>另有 %d 个证据，请在 Web 查看</font>",
			remaining,
		)
		if body.Len()+len(suffix) <= aggEvidenceMarkdownMaxBytes {
			body.WriteString(suffix)
		}
	}
	return strings.TrimSuffix(body.String(), "\n")
}

func canonicalEvidenceURLV1(raw string) (string, bool) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return "", false
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed == nil ||
		(parsed.Scheme != "https" && parsed.Scheme != "http") ||
		parsed.Host == "" || parsed.User != nil {
		return "", false
	}
	return strings.NewReplacer(
		"(", "%28", ")", "%29",
	).Replace(raw), true
}

func escapeEvidenceLabelV1(value string) string {
	return strings.NewReplacer(
		"(", "（", ")", "）", "://", "：／／",
	).Replace(escapeMarkdown(value))
}

func truncateRunesV1(value string, max int) string {
	runes := []rune(value)
	if len(runes) <= max {
		return value
	}
	return string(runes[:max]) + "…"
}

// aggReasonForm 该条专属的误判原因 form。结构与单条卡 form 相同，仅 name 唯一化。
// form 硬约束（历史事故 200530/300123/200673）：form 内交互组件必须有 name；
// 必须含 form_action_type=submit 的提交按钮，缺失整卡非法。
func aggReasonForm(idStr, effectID string) map[string]any {
	return feedbackProblemForm(idStr, true, effectID)
}

func aggregateCallbackValue(
	action string,
	feedbackAction string,
	deliveryID string,
	effectID string,
	brief *feedback.CanonicalBriefCardV1,
) map[string]any {
	value := map[string]any{
		"vane_action": action,
		"delivery_id": deliveryID,
	}
	if feedbackAction != "" {
		value["fb"] = feedbackAction
	}
	if effectID != "" {
		value["effect_id"] = effectID
	}
	if brief != nil &&
		validCanonicalBriefCardV1(brief, brief.VisibleItems) {
		value["brief_batch_id"] = strconv.FormatInt(brief.BatchID, 10)
		value["brief_total"] = strconv.Itoa(brief.TotalItems)
		value["brief_visible"] = strconv.Itoa(brief.VisibleItems)
		value["brief_url"] = brief.WebURL
	}
	return value
}

func validCanonicalBriefCardV1(
	brief *feedback.CanonicalBriefCardV1,
	renderedItems int,
) bool {
	return brief != nil && brief.Validate(renderedItems) == nil
}

// escapeMarkdown 中和标题里能改变 markdown/HTML 结构的字符（对抗审查：外部标题
// 含 `[x](恶意url)` 或 `<font>` 会在卡上渲染成假链接/样式注入）。方括号与尖括号
// 替换为全角同形字符——飞书 markdown 不认反斜杠转义，全角在视觉上近似且语法惰性。
func escapeMarkdown(s string) string {
	return strings.NewReplacer(
		"**", "＊＊", "\n", " ",
		"[", "［", "]", "］",
		"<", "＜", ">", "＞",
	).Replace(s)
}
