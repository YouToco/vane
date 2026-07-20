// web_search / read_page：Exa 两个 ad-hoc 只读工具（M4 契约 §8 扩展）。
//
// 解决的形态（2026-07-20 Boss 生产实测撞见）：用户临时问「这个页面写了什么」
// 「帮我查一下 X 的价格」时，agent 此前没有即时读取能力，只能把页面 add_source 加成
// 信源再抓——一次性需求被迫走订阅设施（信源成了"固定点"）。两个工具把 Exa /search
// 与 /contents 接成即时能力：**不建信源、不写内容库、结果只回给当前对话**。
//
// 成本纪律：按次计费（Exa 上游），每次真实调用经 fetcher 层 recordCall 落 tool_calls
// （SourceID=0 无源口径，与 enrich 补全一致），agent 层 ToolCallRecorder 另记一行
// 工具调用（契约 §6 全量工具口径）。参数校验失败不打上游、不计费。
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

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

// ExaTools 是 web_search / read_page 的装配句柄（与 EndpointTools 同风格）：
// key 未配置时不装配（nil），agent 工具面与上线前一致，而不是装两个恒报「缺 key」的工具。
type ExaTools struct {
	searcher webSearcher
	reader   pageReader
}

// NewExaTools 构造 Exa ad-hoc 工具对。searcher/reader 生产传 *fetcher.ExaFetcher /
// *fetcher.ExaContentsFetcher（经 Multi.Exa()/Multi.ExaContents() 取出，与信源抓取
// 共享同一实例与记账通道）。
func NewExaTools(searcher webSearcher, reader pageReader) *ExaTools {
	return &ExaTools{searcher: searcher, reader: reader}
}

// SearchTool 返回 web_search（进静态白名单，BuildTools 装配）。
func (e *ExaTools) SearchTool() Tool { return &webSearchTool{searcher: e.searcher} }

// ReadPageTool 返回 read_page（进静态白名单，BuildTools 装配）。
func (e *ExaTools) ReadPageTool() Tool { return &readPageTool{reader: e.reader} }

// exaToolError 把 fetcher 层错误翻译成给模型的工具结果文案（返回 nil error 走
// 「模型可读、可自纠」通道，同包错误分层约定）。AppError.Message 是本包自己拼的中文
// （红线 3：无上游原文）；非 AppError 才是真基础设施失败，向上抛。
func exaToolError(err error) (string, error) {
	var ae *types.AppError
	if errors.As(err, &ae) {
		return ae.Message, nil
	}
	return "", err
}

// ============================================================
// web_search：一次性语义搜索（不建信源）。
// ============================================================

// webSearchMaxResults 是工具层的条数上限（低于 Exa 硬上限 100）：每条结果带正文
// 摘要，条数越多回给模型的文本越长，20 条已到对话可读性边界。
const webSearchMaxResults = 20

// webSearchTextMaxRunes 是单条结果正文在工具输出里的截断长度。fetcher 层已按
// exaMaxTextBytes=4000 截断（那是给打分管道的预算）；对话场景模型只需要判断
// 「这条相关吗」，1200 rune 足够且不挤爆上下文。
const webSearchTextMaxRunes = 1200

const webSearchSchema = `{
  "type": "object",
  "properties": {
    "query": {"type": "string", "description": "搜索词（自然语言即可，Exa 是语义搜索）"},
    "num_results": {"type": "integer", "description": "返回条数，默认 5，最多 20。先少后多：不够再加，别一次拉满"},
    "include_domains": {"type": "array", "items": {"type": "string"},
      "description": "可选：只搜这些域名（如 [\"openai.com\"]）。查某个特定网站的最新信息时用"}
  },
  "required": ["query"]
}`

type webSearchTool struct {
	searcher webSearcher
}

func (t *webSearchTool) Name() string { return "web_search" }
func (t *webSearchTool) Description() string {
	return "一次性搜索网页（Exa 语义搜索，按次计费）。用于临时查资料、查最新信息——" +
		"不需要把搜索词加成信源。返回标题/链接/发布日期/正文摘要。要读某个具体页面的完整正文用 read_page。"
}
func (t *webSearchTool) Parameters() json.RawMessage { return json.RawMessage(webSearchSchema) }
func (t *webSearchTool) Mutating() bool              { return false }

type webSearchArgs struct {
	Query          string   `json:"query"`
	NumResults     int      `json:"num_results"`
	IncludeDomains []string `json:"include_domains"`
}

func (t *webSearchTool) Execute(ctx context.Context, _ int64, args json.RawMessage) (string, error) {
	var a webSearchArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "参数不是合法 JSON，请按 schema 重试。", nil
	}
	if strings.TrimSpace(a.Query) == "" {
		return "query 不能为空。", nil
	}
	num := a.NumResults
	if num <= 0 {
		num = 5
	}
	if num > webSearchMaxResults {
		num = webSearchMaxResults
	}
	results, err := t.searcher.Search(ctx, a.Query, num, a.IncludeDomains)
	if err != nil {
		return exaToolError(err)
	}
	if len(results) == 0 {
		return "没有搜到相关结果。可以换个说法、放宽 include_domains，或改用 read_page 直接读已知页面。", nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "搜到 %d 条结果：\n", len(results))
	for i, r := range results {
		fmt.Fprintf(&b, "\n[%d] %s\n%s\n", i+1, oneLine(r.Title, r.URL), r.URL)
		var meta []string
		if r.PublishedDate != "" {
			meta = append(meta, "发布: "+r.PublishedDate)
		}
		if r.Author != "" {
			meta = append(meta, "作者: "+r.Author)
		}
		if len(meta) > 0 {
			fmt.Fprintf(&b, "%s\n", strings.Join(meta, " · "))
		}
		if text := truncateRunes(strings.TrimSpace(r.Text), webSearchTextMaxRunes); text != "" {
			fmt.Fprintf(&b, "%s\n", text)
		}
	}
	return strings.TrimSuffix(b.String(), "\n"), nil
}

func (t *webSearchTool) Summarize(json.RawMessage) string { return "" }

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
	reader pageReader
}

func (t *readPageTool) Name() string { return "read_page" }
func (t *readPageTool) Description() string {
	return "一次性读取指定网页的正文（Exa /contents 强制活抓最新内容，按次计费）。" +
		"用于临时查看某个页面写了什么（价格、在售情况、公告等）——不需要把页面加成信源。" +
		"正文过长会截断；要持续监控页面变化才用 add_source（web/contents）。"
}
func (t *readPageTool) Parameters() json.RawMessage { return json.RawMessage(readPageSchema) }
func (t *readPageTool) Mutating() bool              { return false }

type readPageArgs struct {
	URL string `json:"url"`
}

func (t *readPageTool) Execute(ctx context.Context, _ int64, args json.RawMessage) (string, error) {
	var a readPageArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "参数不是合法 JSON，请按 schema 重试。", nil
	}
	u := strings.TrimSpace(a.URL)
	if u == "" {
		return "url 不能为空。", nil
	}
	if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
		return "url 必须是 http(s) 地址（如 https://example.com/page）。", nil
	}
	title, text, cached, err := t.reader.ReadPage(ctx, u)
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
		fmt.Fprintf(&b, "页面标题：%s\n", oneLine(title, u))
	}
	if cached {
		b.WriteString("（注意：上游返回的是缓存副本，可能不是页面最新状态）\n")
	}
	b.WriteString(text)
	return b.String(), nil
}

func (t *readPageTool) Summarize(json.RawMessage) string { return "" }
