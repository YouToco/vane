package agent

import (
	"encoding/json"
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
	replyExternalFollowupSearchUnavailable = "这个问题需要最新网页证据，但当前没有可用的网页搜索能力；我不会用猜测代替检索，请稍后重试。"
	replyExternalFollowupSearchNotRun      = "这个问题需要最新网页证据，但这次没有完成真实搜索；我不会用猜测代替检索，请重新发送或稍后再试。"
	toolMsgExternalFollowupSearchRejected  = "本次网页查询未执行：引用消息场景只允许逐字搜索当前用户自己的问题，不能改写查询、附加域名或并列调用其他工具。"
)

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
		Query string `json:"query"`
	}{Query: query})
	return raw
}

func containsExternalFollowupSearch(calls []llm.ToolCall) bool {
	for _, call := range calls {
		if call.Name == "web_search" {
			return true
		}
	}
	return false
}
