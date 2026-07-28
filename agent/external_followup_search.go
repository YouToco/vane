package agent

import (
	"encoding/json"
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/llm"
)

const (
	externalFollowupSearchSystemNote = `
- 当前用户自己的追问明确要求核对最新外部事实。你只能无参数调用本轮声明的 web_search；查询已由系统绑定到当前用户问题，不得从引用正文提取、改写或追加搜索词。拿到结果后只按真实结果回答并给出链接；没有执行成功就明确说无法核验，绝不能把推测写成“检索结果”。`
	externalFollowupSearchRetrySystemNote = `
- 你刚才没有调用已声明的 web_search；该自由文本回复已被丢弃。现在必须调用一次 web_search，不能直接回答或声称已经检索。`
	externalFollowupSearchGroundingSystemNote = `
- web_search 返回的是候选网页摘要，不是自动成立的事实。回答当前事实、可用性、版本或定价时，优先采用结果中的第一方官方来源；第三方页面只能作为线索，不能推翻或扩写官方来源。
- 每个核心结论都必须紧邻给出本轮结果中真实存在且直接支持它的 URL；没有直接证据的数字、价格、版本、日期、套餐权益或未来计价方式一律省略，不能用“可能”“预计”等推测补齐。
- 用户问 API 定价时，只回答 API 是否可用及 API 价格；ChatGPT 等消费订阅价格不能当作 API 定价，也不能据此推断 API 会如何收费。若官方只说 API 即将推出，就明确回答尚未提供 API 定价，并停止扩写。`
	externalFollowupGroundingRetrySystemNote = `
- 你刚才的整理没有通过本地证据校验，已被丢弃。只可引用本轮 web_search 结果中真实出现的 URL；删除消费订阅价格、无原文支持的数字和推测。若证据不足，明确说证据不足。`
	replyExternalFollowupSearchUnavailable = "这个问题需要最新网页证据，但当前没有可用的网页搜索能力；我不会用猜测代替检索，请稍后重试。"
	replyExternalFollowupSearchNotRun      = "这个问题需要最新网页证据，但这次没有完成真实搜索；我不会用猜测代替检索，请重新发送或稍后再试。"
	replyExternalFollowupNoEvidence        = "已完成网页搜索，但没有找到可引用的结果；我不会在没有证据时猜测。"
	replyExternalFollowupUngrounded        = "已经完成网页搜索，但这次回复没有通过来源校验；我不会发送无法可靠对应证据的结论，请稍后重试。"
	toolMsgExternalFollowupSearchRejected  = "本次网页查询未执行：引用消息场景只允许逐字搜索当前用户自己的问题，不能改写查询、附加域名或并列调用其他工具。"
)

var (
	externalFollowupURLPattern            = regexp.MustCompile(`https?://[^\s<>"'）)\]]+`)
	externalFollowupAuthorityTokenPattern = regexp.MustCompile(
		`(?i)\b(?:openai|chatgpt|anthropic|claude|google|gemini|microsoft|azure|kimi|neox|deepseek|cohere|xai|mistral)\b`,
	)
	externalFollowupOpenAIProductPattern = regexp.MustCompile(
		`(?i)\b(?:codex|sora)\b|\bgpt (?:4(?:o)?|5(?:\.\d+)?)\b`,
	)
	externalFollowupConcretePricePattern = regexp.MustCompile(
		`(?i)(?:[$€£¥￥]\s*\d+(?:[.,]\d+)?|\d+(?:[.,]\d+)?\s*(?:usd|eur|gbp|cny|rmb|美元|欧元|英镑|人民币|元))`,
	)
	externalFollowupOtherModelPattern = regexp.MustCompile(
		`(?i)\b(?:gpt|o[1-9]|embedding|image|dall e|realtime|whisper|tts|claude|gemini)\b`,
	)
)

type externalFollowupSearchEvidence struct {
	URL           string
	Title         string
	Text          string
	PublishedDate string
}

// externalFollowupSearchQuery conservatively recognizes explicit requests for
// current external facts. It receives only the user-authored suffix extracted
// from the typed Feishu wrapper; quoted/card text never participates.
func externalFollowupSearchQuery(text string) (string, bool) {
	query := strings.TrimSpace(text)
	if query == "" {
		return "", false
	}
	lower := strings.ToLower(query)
	if explicitExternalFollowupLookup(lower) {
		return boundedExternalFollowupQuery(query)
	}
	if followupContainsAny(lower, "后来", "后续") &&
		followupContainsAny(lower, "更新", "变化", "发布", "上线", "开放", "提供",
			"可用", "支持", "价格", "定价", "进展", "消息") {
		return boundedExternalFollowupQuery(query)
	}
	if followupContainsAny(lower, "最新", "目前", "现在", "当前", "截至", "今天", "刚刚") &&
		followupContainsAny(lower, "价格", "定价", "收费", "费率", "状态", "版本", "发布",
			"上线", "开放", "提供", "可用", "支持", "更新", "变化", "进展",
			"消息", "多少", "什么时候", "是否", "有没有", "负责人", "ceo") {
		if !externalFollowupDocumentIntent(lower) ||
			followupContainsAny(lower, "最新", "截至", "刚刚") {
			return boundedExternalFollowupQuery(query)
		}
	}
	if followupContainsAny(lower, "latest") &&
		followupContainsAny(lower, "price", "pricing", "cost", "availability", "status",
			"version", "release", "launch", "supported", "quota", "rate",
			"who is", "when") {
		return boundedExternalFollowupQuery(query)
	}
	if followupContainsAny(lower, "现在", "今天") &&
		followupContainsAny(lower, "价格", "定价", "收费", "费率") &&
		followupContainsAny(lower, "多少", "多少钱") {
		return boundedExternalFollowupQuery(query)
	}
	// Deictic document questions ask what the already-visible quote says. They
	// stay in the zero-tool lane unless one of the strong external lookup/update
	// branches above matched first.
	if externalFollowupDocumentIntent(lower) {
		return "", false
	}
	for _, marker := range []string{
		"是否已", "有没有发布", "有没有提供", "更新了吗",
		"available yet", "released yet", "launched yet",
	} {
		if strings.Contains(lower, marker) {
			return boundedExternalFollowupQuery(query)
		}
	}
	if followupContainsAny(lower, "有更新", "有变化") &&
		followupContainsAny(lower, "吗", "么", "？", "?") {
		return boundedExternalFollowupQuery(query)
	}
	if followupContainsAny(lower, "latest", "current", "currently", "as of", "today", " now",
		"updated", "update") &&
		followupContainsAny(lower, "price", "pricing", "cost", "availability", "status",
			"version", "release", "launch", "supported", "quota", "rate",
			"who is", "when") {
		return boundedExternalFollowupQuery(query)
	}
	return "", false
}

func explicitExternalFollowupLookup(text string) bool {
	return followupContainsAny(text,
		"查一下", "查查", "帮我查一下", "帮我查查",
		"请搜索", "帮我搜索", "搜索一下", "上网搜索", "网页搜索",
		"请检索", "帮我检索", "检索一下",
		"请查证", "帮我查证", "查证一下",
		"请核实", "帮我核实", "核实一下",
		"请核对", "帮我核对", "核对一下",
		"search the web", "please search", "look up",
		"please verify", "verify online", "fact-check", "fact check",
	)
}

func externalFollowupDocumentIntent(text string) bool {
	if followupContainsAny(text,
		"原文", "引用", "这段话", "这篇", "这条", "文章", "上面", "卡片", "推送",
		"quoted", "quote", "paragraph", "article", "this text", "item ",
		"the fourth item",
	) {
		return true
	}
	return strings.Contains(text, "第") && strings.Contains(text, "条")
}

func followupContainsAny(text string, markers ...string) bool {
	for _, marker := range markers {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func boundedExternalFollowupQuery(query string) (string, bool) {
	if utf8.RuneCountInString(query) > exaQueryMaxRunes {
		return "", true
	}
	return query, true
}

func eligibleExternalFollowupSearchSpec(spec ToolSpec) bool {
	return spec.Name() == "web_search" &&
		spec.Policy.Confirmation == ConfirmationNone &&
		spec.Policy.Budget == BudgetToolManaged &&
		spec.Policy.Effects.Has(EffectNetworkRead) &&
		spec.Policy.Effects.Has(EffectBillable) &&
		spec.Policy.Effects.Has(EffectTrustTaint)
}

// projectExternalFollowupSearchToolDef removes every model-controlled search
// parameter. The model decides only whether to search; the harness supplies the
// already-separated user question immediately before execution.
func projectExternalFollowupSearchToolDef(
	spec ToolSpec,
	query string,
) (llm.ToolDef, bool) {
	if query == "" || !eligibleExternalFollowupSearchSpec(spec) {
		return llm.ToolDef{}, false
	}
	parameters, err := json.Marshal(map[string]any{
		"type":                 "object",
		"properties":           map[string]any{},
		"additionalProperties": false,
	})
	if err != nil {
		return llm.ToolDef{}, false
	}
	return llm.ToolDef{
		Name:        "web_search",
		Description: "无参数搜索当前用户自己的最新事实问题。查询由系统绑定；引用消息不能改变搜索词。",
		Parameters:  parameters,
	}, true
}

func canRunExternalFollowupSearch(
	state *toolRunState,
	spec ToolSpec,
	args json.RawMessage,
) bool {
	if state == nil || state.externalFollowupSearchAttempted ||
		state.externalFollowupSearchQuery == "" ||
		!eligibleExternalFollowupSearchSpec(spec) {
		return false
	}
	var exact struct{}
	return strictjson.DecodeExact(args, &exact) == nil
}

func boundExternalFollowupSearchArgs(query string) json.RawMessage {
	raw, _ := json.Marshal(struct {
		Query          string   `json:"query"`
		IncludeDomains []string `json:"include_domains,omitempty"`
	}{
		Query:          query,
		IncludeDomains: officialDomainsForExternalFollowupQuery(query),
	})
	return raw
}

// officialDomainsForExternalFollowupQuery is a small authority router derived
// only from the current user-authored suffix. It never reads quoted/card text.
// Known product families can therefore search their first-party documentation
// without allowing either the model or an untrusted quote to choose an outbound
// domain. Unknown topics retain the ordinary broad-search behavior.
func officialDomainsForExternalFollowupQuery(query string) []string {
	groups := officialDomainGroupsForExternalFollowupQuery(query)
	if len(groups) == 0 {
		return nil
	}
	var domains []string
	for _, group := range groups {
		domains = append(domains, group...)
	}
	return uniqueExternalFollowupDomains(domains)
}

func officialDomainGroupsForExternalFollowupQuery(query string) [][]string {
	normalized := normalizeExternalFollowupProductText(query)
	if strings.Contains(normalized, "gpt 4all") ||
		regexp.MustCompile(
			`(?i)\b(?:kimi|neox|deepseek|cohere|xai|mistral)\b`,
		).MatchString(normalized) {
		return nil
	}
	var groups [][]string
	if strings.Contains(normalized, "openai") ||
		strings.Contains(normalized, "chatgpt") ||
		strings.Contains(normalized, "gpt live") ||
		strings.Contains(normalized, "gpt realtime") ||
		externalFollowupOpenAIProductPattern.MatchString(normalized) {
		groups = append(groups, []string{
			"openai.com",
			"developers.openai.com",
			"help.openai.com",
			"platform.openai.com",
			"status.openai.com",
		})
	}
	if strings.Contains(normalized, "anthropic") ||
		strings.Contains(normalized, "claude") {
		groups = append(groups, []string{"anthropic.com", "docs.anthropic.com"})
	}
	if strings.Contains(normalized, "google") ||
		strings.Contains(normalized, "gemini") {
		groups = append(groups, []string{"ai.google.dev", "cloud.google.com"})
	}
	if strings.Contains(normalized, "microsoft") ||
		strings.Contains(normalized, "azure") {
		groups = append(groups, []string{"microsoft.com", "learn.microsoft.com"})
	}
	if externalFollowupComparisonIntent(normalized) && len(groups) < 2 {
		// A comparison naming an authority outside the bounded registry must
		// stay broad; restricting to the one known side would guarantee an
		// incomplete answer.
		return nil
	}
	// An explicitly named authority that is not in the bounded registry must
	// not be silently dropped from a multi-party query. Keep broad search.
	recognizedTokens := externalFollowupAuthorityTokenPattern.FindAllString(
		normalized, -1,
	)
	for _, token := range recognizedTokens {
		if followupContainsAny(token,
			"kimi", "neox", "deepseek", "cohere", "xai", "mistral",
		) {
			return nil
		}
	}
	return groups
}

func externalFollowupComparisonIntent(normalized string) bool {
	return followupContainsAny(normalized,
		" vs ", " vs. ", " versus ", " and ", " & ",
		"对比", "比较", "分别", "和", "与",
	)
}

func normalizeExternalFollowupProductText(text string) string {
	replacer := strings.NewReplacer(
		"-", " ", "‑", " ", "–", " ", "—", " ", "_", " ",
	)
	return strings.Join(strings.Fields(strings.ToLower(replacer.Replace(text))), " ")
}

func uniqueExternalFollowupDomains(domains []string) []string {
	seen := make(map[string]struct{}, len(domains))
	out := make([]string, 0, len(domains))
	for _, domain := range domains {
		if _, ok := seen[domain]; ok {
			continue
		}
		seen[domain] = struct{}{}
		out = append(out, domain)
	}
	return out
}

// externalFollowupReplyGrounded is the generic citation gate. Only the
// structured URL field returned by Search may become a citation; URLs injected
// into an untrusted title or summary are never admitted as evidence.
func externalFollowupReplyGrounded(
	query string,
	evidence []externalFollowupSearchEvidence,
	reply string,
) bool {
	reply = strings.TrimSpace(reply)
	if reply == "" || len(evidence) == 0 {
		return false
	}
	replyURLs := externalFollowupURLs(reply)
	if len(replyURLs) == 0 {
		return false
	}
	resultURLSet := make(map[string]externalFollowupSearchEvidence, len(evidence))
	for _, item := range evidence {
		normalized := normalizeExternalFollowupURL(item.URL)
		if normalized != "" {
			resultURLSet[normalized] = item
		}
	}
	for _, cited := range replyURLs {
		if _, ok := resultURLSet[normalizeExternalFollowupURL(cited)]; !ok {
			return false
		}
	}
	for _, group := range officialDomainGroupsForExternalFollowupQuery(query) {
		groupCited := false
		for _, cited := range replyURLs {
			parsed, err := url.Parse(normalizeExternalFollowupURL(cited))
			if err == nil {
				host := strings.ToLower(parsed.Hostname())
				for _, domain := range group {
					if externalFollowupHostMatchesDomain(host, domain) {
						groupCited = true
						break
					}
				}
			}
			if groupCited {
				break
			}
		}
		if !groupCited {
			return false
		}
	}
	return true
}

func externalFollowupURLs(text string) []string {
	raw := externalFollowupURLPattern.FindAllString(text, -1)
	urls := make([]string, 0, len(raw))
	for _, found := range raw {
		normalized := strings.TrimRight(
			found,
			".,;:!?，。；：！？",
		)
		if normalized != "" {
			urls = append(urls, normalized)
		}
	}
	return urls
}

func normalizeExternalFollowupURL(raw string) string {
	return strings.TrimRight(
		strings.TrimSpace(raw),
		".,;:!?，。；：！？",
	)
}

func externalFollowupHostMatchesDomain(host, domain string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	domain = strings.ToLower(strings.TrimSpace(domain))
	return host == domain || strings.HasSuffix(host, "."+domain)
}

func isGPTLiveAPIPricingQuery(query string) bool {
	normalized := normalizeExternalFollowupProductText(query)
	return strings.Contains(normalized, "gpt live") &&
		followupContainsAny(normalized, "api", "接口") &&
		followupContainsAny(normalized,
			"价格", "定价", "收费", "费率", "多少", "多少钱",
			"计费", "资费", "费用",
			"price", "pricing", "cost", "how much",
			"billing", "rate", "rates", "charged",
		)
}

func isSingleGPTLiveAPIPricingQuery(query string) bool {
	if !isGPTLiveAPIPricingQuery(query) {
		return false
	}
	normalized := normalizeExternalFollowupProductText(query)
	if externalFollowupComparisonIntent(normalized) ||
		followupContainsAny(normalized,
			"地区", "区域", "国家", "可用地区", "覆盖范围",
			"region", "regions", "country", "countries", "geography",
		) {
		return false
	}
	return len(officialDomainGroupsForExternalFollowupQuery(query)) == 1
}

func deterministicGPTLiveAPIPricingReply(
	evidence []externalFollowupSearchEvidence,
) string {
	var soonURL string
	hasSoonEvidence := false
	var directPricingURL string
	var directPricingDate string
	for _, item := range evidence {
		parsed, err := url.Parse(item.URL)
		if err != nil {
			continue
		}
		host := strings.ToLower(parsed.Hostname())
		if !externalFollowupHostMatchesDomain(host, "openai.com") {
			continue
		}
		body := normalizeExternalFollowupProductText(
			item.Title + "\n" + item.Text,
		)
		if !strings.Contains(body, "gpt live") {
			continue
		}
		if directGPTLiveAPIPricingEvidence(item, body) &&
			(directPricingURL == "" ||
				item.PublishedDate > directPricingDate) {
			directPricingURL = normalizeExternalFollowupURL(item.URL)
			directPricingDate = item.PublishedDate
		}
		if followupContainsAny(body,
			"bring them to the api soon",
			"bring gpt live to the api soon",
			"api version is coming soon",
			"api 版本即将推出",
			"计划很快提供 api",
			"即将推出 api",
		) {
			hasSoonEvidence = true
			soonURL = normalizeExternalFollowupURL(item.URL)
		}
	}
	if directPricingURL != "" {
		return "OpenAI 官方结果已出现 GPT-Live API 定价信息；为避免转述金额出错，" +
			"请以官方页面为准：" + directPricingURL
	}
	if hasSoonEvidence && soonURL != "" {
		return "本次 OpenAI 官方搜索未找到当前 GPT-Live API 定价页；能确认的是该发布页" +
			"当时只写明计划很快提供 API，并未给出价格。历史公告不足以证明之后绝对没有更新：" +
			soonURL
	}
	return "已完成 OpenAI 官方网页搜索，但现有结果不足以可靠确认 GPT-Live API 定价；" +
		"我不会用 ChatGPT 订阅价格或推测代替 API 价格。"
}

func directGPTLiveAPIPricingEvidence(
	item externalFollowupSearchEvidence,
	normalizedBody string,
) bool {
	if !strings.Contains(normalizedBody, "gpt live") ||
		!strings.Contains(normalizedBody, "api") {
		return false
	}
	if followupContainsAny(normalizedBody,
		"not yet available", "unavailable", "coming soon",
		"尚未", "暂未", "即将", "计划", "soon",
	) {
		return false
	}
	lines := strings.Split(item.Title+"\n"+item.Text, "\n")
	for i, line := range lines {
		normalizedLine := normalizeExternalFollowupProductText(line)
		if !externalFollowupConcretePricePattern.MatchString(line) ||
			!followupContainsAny(normalizedLine,
				"per 1m", "per million", "/ 1m", "每百万",
				"per minute", "/minute", "每分钟",
				"input tokens", "output tokens", "audio tokens",
			) {
			continue
		}
		window := normalizedLine
		if i > 0 {
			window = normalizeExternalFollowupProductText(lines[i-1]) +
				" " + window
		}
		nonTargetWindow := strings.ReplaceAll(window, "gpt live", "")
		if !strings.Contains(window, "gpt live") ||
			externalFollowupOtherModelPattern.MatchString(nonTargetWindow) ||
			followupContainsAny(window,
				"chatgpt", "subscription", "per month", "/month",
				"会员", "订阅", "月费", "套餐",
				"gpt 4", "gpt 5", "claude", "gemini",
				"gpt realtime", "realtime api",
				"whisper", "tts", "text to speech", "speech to text",
			) {
			continue
		}
		return true
	}
	return false
}

func containsExternalFollowupSearch(calls []llm.ToolCall) bool {
	for _, call := range calls {
		if call.Name == "web_search" {
			return true
		}
	}
	return false
}
