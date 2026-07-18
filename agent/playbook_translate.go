// 任务手册 P1（编译层）：把自然语言手册翻译成结构化抓取计划（fetch_plan）。
//
// 落点（RFC docs/task-playbook-design.md §6/§8，决策 D1「先 A 后 B」）：
// create_schedule / edit_task_playbook 在存下手册正文后，各发**一次** LLM 调用把正文
// 翻译成计划，经 sourcespec 校验后落库 schedule_playbooks.fetch_plan。运行时"按计划抓"
// （Fetch 消费 fetch_plan）本轮**不接线**——编译层只做「翻译 + 校验 + 存 + 看」。
// 打分/出卡的 M5 注入（§4）也不在本轮。
//
// best-effort 铁律：翻译/落库任何环节失败都只 slog、绝不影响主效果（调度已建、手册已存）。
// 翻译是低频（仅建/改任务时各一次，决策 §10#6）且非功能性的增益，不值得为它回滚主效果。
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/google/uuid"

	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/sourcespec"
	"github.com/YouToco/vane/types"
)

// planTranslateMaxTokens 是翻译调用的输出上限：一份计划 JSON 通常只几个源、几百 token，
// 2000 足够且防跑飞。DisableThinking 下预算全给 content（不会被思维链吃光，见 llm.Request）。
const planTranslateMaxTokens = 2000

// errPlanUnparsable：模型输出取不出合法 JSON。
// errAllSourcesDropped：模型**给了源**但无一通过 sourcespec 校验（拼错平台名、乱造字段等
// 翻译质量失败）——与"模型本就返回零源"截然不同，后者是正当的"手册无抓取意图"。
//
// 两者都当**软失败**处理（调用方保留既有计划不动、不用空计划把已编译好的好计划冲掉）；
// 唯有模型确实返回零源（len(wire.Sources)==0）才落空计划、执行正当清空。这条区分修掉了
// 「一次编辑抖动就静默擦除既有抓取计划」的非对称擦除（对抗审查 correctness 项）。
var (
	errPlanUnparsable    = errors.New("翻译输出无法解析为抓取计划 JSON")
	errAllSourcesDropped = errors.New("翻译产出的源无一通过校验（疑翻译质量失败，非无抓取意图）")
)

// FetchPlan 是手册编译出的结构化抓取计划（schedule_playbooks.fetch_plan 的 JSONB 形态）。
// 存的是**已过 sourcespec.Build 校验、归一化后**的源——URL 即幂等键（vane://… 或真实
// http(s)），Config 即 sourcespec 产出的配置。这正是将来运行时"按计划抓"要消费的形态，
// 故编译层就按最终形态落库，而非回显模型原始意图。
type FetchPlan struct {
	Sources []PlannedSource `json:"sources"`
}

// PlannedSource 是计划里的一个已校验源。字段取自 sourcespec.Build 产出的 types.Source，
// 只留运行时抓取真正需要的：platform/capability 定路由，URL 定幂等键，Config 带参数。
type PlannedSource struct {
	Platform   string          `json:"platform"`
	Capability string          `json:"capability"`
	Title      string          `json:"title,omitempty"`
	URL        string          `json:"url"`
	Config     json.RawMessage `json:"config,omitempty"`
}

// wireFetchPlan 是模型**输出**的线形态：Sources 复用 addSourceArgs——计划里"一个源"的
// 参数词汇与 add_source 工具、与 sourcespec 三者刻意同源（同一套 platform/capability/params），
// 从根上杜绝模型产出 sourcespec 消费不了的字段。校验后转成 PlannedSource 落库（见 compilePlan）。
type wireFetchPlan struct {
	Sources []addSourceArgs `json:"sources"`
}

// playbookTranslator 把自然语言手册翻译成结构化抓取计划（P1 编译层）。收窄成接口：
// create_schedule / edit_task_playbook 依赖它，工具单测注入假实现即可，无需真 LLM/HTTP。
type playbookTranslator interface {
	// Translate 返回**已校验、可直接落库**的 fetch_plan JSON（形如 {"sources":[…]}）。
	// 合法但零源 → 返回空计划（{"sources":[]}）+ nil err。仅"LLM 调用失败"或"输出取不出
	// 合法 JSON"才返回非 nil err（调用方据此保留既有计划不动）。
	Translate(ctx context.Context, userID int64, content string) (json.RawMessage, error)
}

// llmPlaybookTranslator 是 playbookTranslator 的生产实现：一次 llm.Do（system=编译规则、
// user=手册正文），输出交 compilePlan 解析 + 逐源 sourcespec 校验。装配见 cmd/server。
// 用 client 默认模型（cfg.LLM.Model），与 scorer/cardgen/evolver 等一次性结构化调用一致。
type llmPlaybookTranslator struct {
	cli *llm.Client
	rec *llm.Recorder
}

// NewPlaybookTranslator 构造生产翻译器。
func NewPlaybookTranslator(cli *llm.Client, rec *llm.Recorder) playbookTranslator {
	return &llmPlaybookTranslator{cli: cli, rec: rec}
}

func (t *llmPlaybookTranslator) Translate(ctx context.Context, userID int64, content string) (json.RawMessage, error) {
	content = strings.TrimSpace(content)
	if content == "" {
		return emptyPlanJSON(), nil // 空手册无从翻译：等价零源计划，不算失败。
	}
	zero := float32(0)
	maxTok := planTranslateMaxTokens
	req := llm.Request{
		System:      translateSystemPrompt,
		User:        content,
		Temperature: &zero, // 编译要稳定可复现，温度取 0。
		MaxTokens:   &maxTok,
		// 结构化 JSON 输出必须关思维链：V4 默认 reasoning 会吃光预算致 content 恒空
		// （llm.Request.DisableThinking 注释里的 2026-07-14 打分事故同因）。
		DisableThinking: true,
	}
	meta := llm.CallMeta{
		TraceID:  uuid.NewString(),
		SpanName: "playbook_translate",
		UserID:   &userID,
	}
	resp, err := llm.Do(ctx, t.cli, t.rec, meta, req)
	if err != nil {
		return nil, err // LLM 调用失败（超时/限流/上游）：上抛，调用方保留既有计划。
	}
	return compilePlan(resp.Content)
}

// compilePlan 是编译层的**纯函数核心**（不碰 LLM/DB，据此单测全部分支）：
//  1. 从模型输出里抽出 JSON（容忍 ```json 代码块围栏与前后散文）；
//  2. 按 wire 形态（addSourceArgs 词汇）反序列化；
//  3. 逐源经 specFromArgs → sourcespec.Build 校验，**校验不过的单源丢弃并 warn**
//     （宁可少给几个源，也不落系统消费不了的坏源）；
//  4. 归一化后的合法源转 PlannedSource，整体 marshal 回 fetch_plan JSON。
//
// 返回约定（区分正当清空 vs 翻译失败，避免非对称静默擦除既有计划）：
//   - 顶层 JSON 取不出/反序列化失败 → (nil, errPlanUnparsable)：软失败，保留既有计划。
//   - 模型给了源但**无一通过校验** → (nil, errAllSourcesDropped)：软失败，保留既有计划。
//   - 模型**本就返回零源**（len(wire.Sources)==0）→ (空计划, nil)：正当清空（手册无抓取意图）。
//   - 至少一个源通过 → (含有效源的计划, nil)：落库有效子集。
func compilePlan(raw string) (json.RawMessage, error) {
	js := extractJSONObject(raw)
	if js == "" {
		return nil, errPlanUnparsable
	}
	var wire wireFetchPlan
	if err := json.Unmarshal([]byte(js), &wire); err != nil {
		return nil, fmt.Errorf("%w: %v", errPlanUnparsable, err)
	}
	plan := FetchPlan{Sources: make([]PlannedSource, 0, len(wire.Sources))}
	for _, a := range wire.Sources {
		if strings.TrimSpace(a.Platform) == "" || strings.TrimSpace(a.Capability) == "" {
			continue // 缺 platform/capability 无从路由，直接跳过（不算致命）。
		}
		src, msg := sourcespec.Build(specFromArgs(a))
		if msg != "" || src == nil {
			slog.Warn("playbook: 抓取计划里的源校验不过，丢弃",
				"platform", a.Platform, "capability", a.Capability, "reason", msg)
			continue
		}
		plan.Sources = append(plan.Sources, plannedFromSource(src))
	}
	// 模型给了源却一个都没通过校验：翻译质量失败（≠"手册无抓取意图"）。软失败保留既有计划，
	// 不用空计划把已编译好的好计划静默冲掉（对抗审查 correctness：非对称擦除）。
	if len(plan.Sources) == 0 && len(wire.Sources) > 0 {
		return nil, errAllSourcesDropped
	}
	out, err := json.Marshal(plan)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errPlanUnparsable, err)
	}
	return out, nil
}

// plannedFromSource 从校验后的 types.Source 抽出落库要留的字段（幂等键 URL + config）。
func plannedFromSource(s *types.Source) PlannedSource {
	return PlannedSource{
		Platform:   string(s.Platform),
		Capability: string(s.Capability),
		Title:      s.Title,
		URL:        s.URL,
		Config:     s.Config,
	}
}

// extractJSONObject 从模型输出里截出最外层 JSON 对象：容忍 ```json 围栏、前后解释性散文。
// 取第一个 '{' 到最后一个 '}'——编译输出约定就是单个对象，不会有并列多对象的歧义。
func extractJSONObject(s string) string {
	s = strings.TrimSpace(s)
	i := strings.IndexByte(s, '{')
	j := strings.LastIndexByte(s, '}')
	if i < 0 || j < i {
		return ""
	}
	return s[i : j+1]
}

// emptyPlanJSON 返回规范的零源计划，供"空手册"与需要显式清空计划的路径复用。
func emptyPlanJSON() json.RawMessage { return json.RawMessage(`{"sources":[]}`) }

// countPlanSources 数一份 fetch_plan JSON 里的源数（渲染/回执用）；不合法或空 → 0。
func countPlanSources(raw json.RawMessage) int {
	if len(raw) == 0 {
		return 0
	}
	var p FetchPlan
	if json.Unmarshal(raw, &p) != nil {
		return 0
	}
	return len(p.Sources)
}

// translateSystemPrompt 是编译规则。锚定三件事：可用的 platform/capability 词汇（严格对齐
// sourcespec 消费面）、D-5 追新用 include_domains 不用日期过滤的铁律、宁缺毋滥不编造。
const translateSystemPrompt = `你是「见微 Vane」情报任务的抓取计划编译器。用户给你一份自然语言的情报任务手册，描述这个定时任务要抓什么信息、关注哪些主题、偏好哪些来源。你的唯一职责是把它翻译成一份结构化的抓取计划 JSON，交给系统据此配置信源。

只输出一个 JSON 对象，不要任何解释、不要 markdown 代码块，形如：
{"sources":[{"platform":"web","capability":"search","query":"Anthropic 最新发布","include_domains":["anthropic.com"]}]}

每个 source 的字段规则（platform + capability 决定必填项，务必严格遵守，规则外的字段一律不填）：
- web + feed：RSS/Atom 订阅。必填 url（RSS 源地址，http/https）。
- web + search：Exa 网页搜索。必填 query（搜索词）。可选 include_domains（域名白名单数组，如 ["anthropic.com","openai.com"]）。可选 category（如 "news"）。
- web + contents：监控某个网页内容变化（如产品定价页）。必填 url（页面地址）。
- x + user_posts：订阅某 X(Twitter) 账号的新发布。必填 screen_name（如 "OpenAI"，不带 @）。
- xhs + search：小红书关键词搜索。必填 keyword。
- xhs + user_posts：订阅某小红书博主。必填 user_id（24 位十六进制）或 profile_url（用户主页链接）。

关键规则：
1. 【追新用 include_domains，绝不用日期过滤】想要某个官方/权威来源的最新内容时，用 web+search 并把 include_domains 限定到该域名（例：只要 Anthropic 官方最新动态 → query 写主题、include_domains 填 ["anthropic.com"]）。绝不要试图靠"发布日期"来"只要最近的"——Exa 的发布日期是从网页猜的，官方站常常猜不出、按日期过滤会把它们连带删光。
2. 只输出手册里明确要的源。手册只说了主题、没点名具体来源时，用 web+search + query 表达主题即可，不要臆造域名。
3. 拿不准某个平台/能力就不要给。宁可少给几个源，也不要给系统消费不了的配置。
4. 如果手册内容与"抓什么"无关（纯粹在讲呈现格式、篇幅偏好等），返回 {"sources":[]}。`
