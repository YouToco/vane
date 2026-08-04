// web_search / read_page：Exa 两个 ad-hoc 只读工具（M4 契约 §8 扩展）。
//
// 解决的形态（2026-07-20 Boss 生产实测撞见）：用户临时问「这个页面写了什么」
// 「帮我查一下 X 的价格」时，Agent 用本文件的一次性读取能力，不创建长期任务。
// 信源再抓——一次性需求被迫走订阅设施（信源成了"固定点"）。两个工具把 Exa /search
// 与 /contents 接成即时能力：**不建信源、不写内容库、结果只回给当前对话**。
//
// 成本纪律：
//   - 单条消息统一由 Agent 20 次隐藏熔断器保护；
//   - 滚动 24h 上限（ExaDailyCap，默认 100）：从 tool_calls 表 COUNT（排除
//     invalid_args/budget_exceeded——没打上游的拒绝不把限额越顶越死），
//     判定失败 fail-closed 拒绝（护栏失效即放开计费面，宁可少查）；
//   - 每次真实上游调用经 fetcher 层 recordCall 落 tool_calls（SourceID=0 无源口径），
//     agent 层 ToolCallRecorder 另记一行工具调用（契约 §6 全量工具口径）。
//   - 参数校验失败不打上游、不计费、不吃限额。
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/YouToco/vane/fetcher"
	"github.com/YouToco/vane/types"
)

// webSearcher / pageReader 收窄两工具依赖的 fetcher 能力（*fetcher.ExaFetcher /
// *fetcher.ExaContentsFetcher 已实现）。收窄理由同 profileStore：Execute 各分支
// 可用内存假实现覆盖，不依赖真实 Exa 上游。
type webSearcher interface {
	Search(ctx context.Context, query string, numResults int, includeDomains []string) ([]fetcher.SearchResult, error)
}

type pageReader interface {
	ReadPage(ctx context.Context, pageURL string) (title, text string, cached bool, err error)
}

// exaCallCounter 是每日限额判定依赖的计数面（*store.Store 已实现）。
// 与 endpointCallCounter 同模式：限额判定读记账表，拦截口径与账本天然同源。
type exaCallCounter interface {
	CountExaAdHocCallsSince(ctx context.Context, since time.Time) (int, error)
}

// ExaTools 是 web_search / read_page 的装配句柄（与 EndpointTools 同风格）：
// key 未配置时不装配（nil），agent 工具面与上线前一致，而不是装两个恒报「缺 key」的工具。
// counter 为 nil 时不查滚动 24h 限额；单条消息统一由 Agent 隐藏熔断器管理。
// dailyCap ≤0 视为不设限。
type ExaTools struct {
	searcher webSearcher
	reader   pageReader
	counter  exaCallCounter
	dailyCap int
}

// NewExaTools 构造 Exa ad-hoc 工具对。searcher/reader 生产传 *fetcher.ExaFetcher /
// *fetcher.ExaContentsFetcher（经 Multi.Exa()/Multi.ExaContents() 取出，与信源抓取
// 共享同一实例与记账通道）；counter 生产传 *store.Store。
func NewExaTools(
	searcher webSearcher,
	reader pageReader,
	counter exaCallCounter,
	dailyCap int,
) *ExaTools {
	return &ExaTools{
		searcher: searcher, reader: reader,
		counter: counter, dailyCap: dailyCap,
	}
}

// SearchTool 返回 web_search（进静态白名单）。
func (e *ExaTools) SearchTool() ToolSpec {
	return newToolSpec(&webSearchTool{et: e}, withToolSurface(ownerPolicy(
		Effects(EffectNetworkRead, EffectBillable, EffectTrustTaint),
		BudgetToolManaged),
		ExposureAlways, IntentWebResearch, ResultTrustExternal, false))
}

// ReadPageTool 返回 read_page（进静态白名单）。
func (e *ExaTools) ReadPageTool() ToolSpec {
	return newToolSpec(&readPageTool{et: e}, withToolSurface(ownerPolicy(
		Effects(EffectNetworkRead, EffectBillable, EffectTrustTaint),
		BudgetToolManaged),
		ExposureAlways, IntentWebResearch, ResultTrustExternal, false))
}

// checkBudget 只判定滚动 24h 限额；单消息成本由统一工具熔断器负责。
// 返回 "" = 放行；非空 = 拒绝文案（记 budget_exceeded——没打上游，不计入日限额 COUNT，
// 否则被拒绝的调用会把限额越顶越死）。
func (e *ExaTools) checkBudget(ctx context.Context) string {
	rec := recFrom(ctx)
	if e.counter != nil && e.dailyCap > 0 {
		n, err := e.counter.CountExaAdHocCallsSince(ctx, time.Now().Add(-dailyCapWindow))
		if err != nil {
			// fail-closed（同端点工具三纪律）：回固定文案不泄错误链、记 budget_exceeded
			// 不计入 COUNT、错误链只进日志。
			slog.Warn("agent: Exa ad-hoc 每日限额判定失败，fail-closed 拒绝本次调用", "err", err)
			if rec != nil {
				rec.ErrorType = types.ToolErrBudgetExceeded
				rec.Error = err.Error()
			}
			return "网页查询限额检查暂时不可用，本次调用已跳过，请稍后再试。"
		}
		if n >= e.dailyCap {
			if rec != nil {
				rec.ErrorType = types.ToolErrBudgetExceeded
			}
			return fmt.Sprintf("过去 24 小时网页查询已达上限（%d 次），为控制成本暂停调用。请明天再试或让用户调整限额。", e.dailyCap)
		}
	}
	return ""
}

func (e *ExaTools) countCall(ctx context.Context) {
	if state := runStateFrom(ctx); state != nil {
		state.exaCalls++
	}
}

// markInvalidArgs 标记参数校验失败（没打上游）：日限额 COUNT 排除此项，
// 与端点工具的 invalid_args 口径一致。
func markInvalidArgs(ctx context.Context) {
	if rec := recFrom(ctx); rec != nil {
		rec.ErrorType = types.ToolErrInvalidArgs
	}
}

// exaToolError 把 fetcher 层错误翻译成给模型的工具结果文案（返回 nil error 走
// 「模型可读、可自纠」通道，同包错误分层约定）。AppError.Message 是本包自己拼的中文
// （红线 3：无上游原文）；非 AppError 才是真基础设施失败，向上抛。
func exaToolError(err error) (string, error) {
	var ae *types.AppError
	if errors.As(err, &ae) {
		if ae.Retryable {
			return "", err
		}
		return ae.Message, nil
	}
	return "", err
}

// ============================================================
// web_search：一次性语义搜索（不建信源）。
// ============================================================

const (
	// webSearchMaxResults 是工具层的条数上限（低于 Exa 硬上限 100）：每条结果带正文
	// 摘要，条数越多回给模型的文本越长，20 条已到对话可读性边界。
	webSearchMaxResults = 20
	// webSearchTextMaxRunes 是单条结果正文在工具输出里的截断长度。fetcher 层已按
	// exaMaxTextBytes=4000 截断（那是给打分管道的预算）；对话场景模型只需要判断
	// 「这条相关吗」，1200 rune 足够且不挤爆上下文。
	webSearchTextMaxRunes = 1200
	// exaOutMetaMaxRunes 是标题/作者等上游可控短字段的截断上限（对抗审查 F3）：
	// 恶意页面可构造数百 KB 的 <title>，20 条叠加足以撑爆对话上下文——
	// 上游可控文本进模型上下文一律要截断，不只正文。
	exaOutMetaMaxRunes = 200
	// exaOutURLMaxRunes 是 URL 的截断上限（同上）。
	exaOutURLMaxRunes = 500
	// exaQueryMaxRunes / exaMaxIncludeDomains / exaURLMaxRunes 是入参上限
	//（对抗审查 F4）：原样进 Exa 请求体的字段必须有界——超限走与空参相同的
	// 自拼中文报错（不打上游、不计费）。
	exaQueryMaxRunes     = 2000
	exaMaxIncludeDomains = 50
	exaURLMaxRunes       = 2048
)

const webSearchSchema = `{
  "type": "object",
  "properties": {
    "query": {"type": "string", "description": "搜索词（自然语言即可）"},
    "num_results": {"type": "integer", "description": "返回条数，默认 5，最多 20。先少后多：不够再加，别一次拉满"},
    "include_domains": {"type": "array", "items": {"type": "string"},
      "description": "可选：只搜这些域名（如 [\"openai.com\"]）。查某个特定网站的最新信息时用"}
  },
  "required": ["query"]
}`

type webSearchTool struct {
	et *ExaTools
}

func (t *webSearchTool) Name() string { return "web_search" }
func (t *webSearchTool) Description() string {
	return "一次性搜索互联网并返回标题、链接、发布日期和正文摘要（可能计费）。用于临时查资料、查最新信息——" +
		"不需要把搜索词加成信源。返回标题/链接/发布日期/正文摘要。要读某个具体页面的完整正文用 read_page。"
}
func (t *webSearchTool) Parameters() json.RawMessage { return json.RawMessage(webSearchSchema) }
func (t *webSearchTool) untrustedResult() bool       { return true }
func (t *webSearchTool) toolKind() types.ToolCallKind {
	return types.ToolCallKindExaFetch
}

type webSearchArgs struct {
	Query          string   `json:"query"`
	NumResults     int      `json:"num_results"`
	IncludeDomains []string `json:"include_domains"`
}

func (t *webSearchTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	// Daily 限额判定先于一切上游动作（见文件头成本纪律）。
	if msg := t.et.checkBudget(ctx); msg != "" {
		return msg, nil
	}
	var a webSearchArgs
	if err := json.Unmarshal(args, &a); err != nil {
		markInvalidArgs(ctx)
		return "参数不是合法 JSON，请按 schema 重试。", nil
	}
	if strings.TrimSpace(a.Query) == "" {
		markInvalidArgs(ctx)
		return "query 不能为空。", nil
	}
	if len([]rune(a.Query)) > exaQueryMaxRunes {
		markInvalidArgs(ctx)
		return fmt.Sprintf("query 过长（上限 %d 字符），请精简后重试。", exaQueryMaxRunes), nil
	}
	if len(a.IncludeDomains) > exaMaxIncludeDomains {
		markInvalidArgs(ctx)
		return fmt.Sprintf("include_domains 过多（上限 %d 个），请精简后重试。", exaMaxIncludeDomains), nil
	}
	num := a.NumResults
	if num <= 0 {
		num = 5
	}
	if num > webSearchMaxResults {
		num = webSearchMaxResults
	}
	t.et.countCall(ctx)
	ctx = withExaAttribution(ctx, userID)
	results, err := t.et.searcher.Search(ctx, a.Query, num, a.IncludeDomains)
	if err != nil {
		return exaToolError(err)
	}
	if len(results) == 0 {
		result := "没有搜到相关结果。可以换个说法、放宽 include_domains，或改用 read_page 直接读已知页面。"
		markExternalFollowupSearchSuccess(
			ctx, strings.TrimSpace(a.Query), result, nil,
		)
		return result, nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "搜到 %d 条结果：\n", len(results))
	for i, r := range results {
		fmt.Fprintf(&b, "\n[%d] %s\n%s\n", i+1,
			oneLine(truncateRunes(r.Title, exaOutMetaMaxRunes), truncateRunes(r.URL, exaOutURLMaxRunes)),
			truncateRunes(r.URL, exaOutURLMaxRunes))
		var meta []string
		if r.PublishedDate != "" {
			meta = append(meta, "发布: "+r.PublishedDate)
		}
		if r.Author != "" {
			meta = append(meta, "作者: "+truncateRunes(r.Author, exaOutMetaMaxRunes))
		}
		if len(meta) > 0 {
			fmt.Fprintf(&b, "%s\n", strings.Join(meta, " · "))
		}
		if text := truncateRunes(strings.TrimSpace(r.Text), webSearchTextMaxRunes); text != "" {
			fmt.Fprintf(&b, "%s\n", text)
		}
	}
	result := strings.TrimSuffix(b.String(), "\n")
	markExternalFollowupSearchSuccess(
		ctx, strings.TrimSpace(a.Query), result, results,
	)
	return result, nil
}

func (t *webSearchTool) Summarize(json.RawMessage) string { return "" }

func markExternalFollowupSearchSuccess(
	ctx context.Context,
	query string,
	result string,
	results []fetcher.SearchResult,
) {
	invocationURLs := make([]string, 0, len(results))
	for _, item := range results {
		invocationURLs = append(invocationURLs,
			truncateRunes(item.URL, exaOutURLMaxRunes))
	}
	rememberInvocationPublicEvidenceURLs(ctx, invocationURLs)
	if state := runStateFrom(ctx); state != nil {
		state.webResearchSucceeded = true
		state.webSearchSucceeded = true
		// A read only grounds searches that happened before it. Every new
		// successful search creates a fresh read debt, even when this message
		// already opened another page.
		state.webPageReadSucceeded = false
		state.webPageReadResponseRejected = false
		for _, item := range results {
			appendExternalFollowupEvidence(state, externalFollowupSearchEvidence{
				URL:           truncateRunes(item.URL, exaOutURLMaxRunes),
				Title:         truncateRunes(item.Title, exaOutMetaMaxRunes),
				Text:          truncateRunes(item.Text, webSearchTextMaxRunes*4),
				PublishedDate: truncateRunes(item.PublishedDate, exaOutMetaMaxRunes),
			})
		}
	}
	if state := runStateFrom(ctx); state != nil &&
		state.externalFollowupSearchAttempted &&
		state.externalFollowupSearchQuery == query {
		// 只有真实上游无错返回才能授权本轮模型总结为检索结果。预算拒绝、
		// 日限额检查失败、AppError 和基础设施错误都在此前返回，保持 false。
		state.externalFollowupSearchSucceeded = true
		state.externalFollowupSearchResult = result
	}
}

func appendExternalFollowupEvidence(
	state *toolRunState,
	item externalFollowupSearchEvidence,
) {
	if state == nil || strings.TrimSpace(item.URL) == "" {
		return
	}
	for i, existing := range state.externalFollowupSearchEvidence {
		if existing.URL == item.URL {
			state.externalFollowupSearchEvidence[i] = item
			return
		}
	}
	state.externalFollowupSearchEvidence = append(
		state.externalFollowupSearchEvidence, item,
	)
}

// oneLine 把标题压成一行（防空标题时输出裸行）。
func oneLine(title, fallback string) string {
	title = strings.Join(strings.Fields(title), " ")
	if title == "" {
		return fallback
	}
	return title
}

// ============================================================
// read_page：一次性读取指定 URL 正文（maxAgeHours:0 活抓，不建信源）。
// ============================================================

const readPageSchema = `{
  "type": "object",
  "properties": {
    "url": {"type": "string", "description": "要读取的页面地址（http/https），如 https://example.com/pricing"}
  },
  "required": ["url"]
}`

type readPageTool struct {
	et *ExaTools
}

func (t *readPageTool) Name() string { return "read_page" }
func (t *readPageTool) Description() string {
	return "一次性读取指定公开网页的最新正文（可能计费）。" +
		"用于临时查看某个页面写了什么（价格、在售情况、公告等）——不需要把页面加成信源。" +
		"正文过长会截断；要持续监控页面变化则创建带 web_contents 取材目标的任务。"
}
func (t *readPageTool) Parameters() json.RawMessage { return json.RawMessage(readPageSchema) }
func (t *readPageTool) untrustedResult() bool       { return true }
func (t *readPageTool) toolKind() types.ToolCallKind {
	return types.ToolCallKindExaFetch
}

type readPageArgs struct {
	URL string `json:"url"`
}

func (t *readPageTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	// Daily 限额判定先于一切上游动作（见文件头成本纪律）。
	if msg := t.et.checkBudget(ctx); msg != "" {
		return msg, nil
	}
	var a readPageArgs
	if err := json.Unmarshal(args, &a); err != nil {
		markInvalidArgs(ctx)
		return "参数不是合法 JSON，请按 schema 重试。", nil
	}
	u := strings.TrimSpace(a.URL)
	if u == "" {
		markInvalidArgs(ctx)
		return "url 不能为空。", nil
	}
	if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
		markInvalidArgs(ctx)
		return "url 必须是 http(s) 地址（如 https://example.com/page）。", nil
	}
	if len([]rune(u)) > exaURLMaxRunes {
		markInvalidArgs(ctx)
		return fmt.Sprintf("url 过长（上限 %d 字符）。", exaURLMaxRunes), nil
	}
	t.et.countCall(ctx)
	ctx = withExaAttribution(ctx, userID)
	title, text, cached, err := t.et.reader.ReadPage(ctx, u)
	if err != nil {
		// 页面抓不到（ErrPageUnreachable）：URL 打错/需登录/拦抓取是主流原因，
		// 给「检查 URL」话术（对齐 probe 准入的翻译，不只说「稍后再试」）。
		if errors.Is(err, fetcher.ErrPageUnreachable) {
			return "无法抓取该页面（可能不存在、需要登录或阻止了抓取）。请检查 URL 是否正确、" +
				"页面是否公开可访问；确认无误可稍后重试。", nil
		}
		return exaToolError(err)
	}
	var b strings.Builder
	if strings.TrimSpace(title) != "" {
		fmt.Fprintf(&b, "页面标题：%s\n", oneLine(truncateRunes(title, exaOutMetaMaxRunes), u))
	}
	if cached {
		b.WriteString("（注意：上游返回的是缓存副本，可能不是页面最新状态）\n")
	}
	b.WriteString(text)
	result := b.String()
	if state := runStateFrom(ctx); state != nil {
		state.webResearchSucceeded = true
		state.webPageReadSucceeded = true
		appendExternalFollowupEvidence(state, externalFollowupSearchEvidence{
			URL:   truncateRunes(u, exaOutURLMaxRunes),
			Title: truncateRunes(title, exaOutMetaMaxRunes),
			Text:  truncateRunes(text, webSearchTextMaxRunes*4),
		})
	}
	return result, nil
}

func (t *readPageTool) Summarize(json.RawMessage) string { return "" }

// withExaAttribution 让 fetcher 层的真实上游行与 Agent/LLM 行共享 trace/user，
// store 据 user 推导 tenant。上下文只承载本地记账元数据；Exa 请求体仍只有显式
// query/url 参数，不会携带 system prompt、画像或会话历史。
func withExaAttribution(ctx context.Context, userID int64) context.Context {
	var traceID string
	if m, ok := ctx.Value(chatMetaKey{}).(chatMeta); ok {
		traceID = m.traceID
	}
	return fetcher.WithBindingAttribution(ctx, traceID, userID)
}
