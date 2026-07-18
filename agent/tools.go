// agent 工具集（M4 契约 §8）：七个工具 + BuildTools 装配。
// Tool 接口本身在 loop.go（契约 §7）定义，本文件只实现具体工具类型。
//
// 错误分层约定（贯穿全部 Execute）：
//   - 模型可自纠的确定性失败（参数不是合法 JSON、字段校验不过）→ 返回中文文案 + nil error，
//     loop 会把文案作为 role=tool 结果回给模型继续多轮（或经确认卡展示给用户）；
//   - 基础设施失败（DB / Temporal）→ 返回 error 向上抛，由 feishu 层 humanize。
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/sourcecatalog"
	"github.com/YouToco/vane/sourcespec"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

// PushTrigger 是 push_now 工具依赖的窄接口（契约 §8）：只暴露 userID 一个入参，
// 不把 workflow.PushScope 泄进工具面。*scheduler.Scheduler 已实现（TriggerPushNow）。
// agent 禁止 import api，即时触发经此接口而非 HTTP handler 复用。
type PushTrigger interface {
	TriggerPushNow(ctx context.Context, userID int64) (runID string, err error)
}

// scheduleUpdater / scheduleDeleter 收窄 update/remove_schedule 依赖的 scheduler 能力
// （与 scheduleCreator 同风格，*scheduler.Scheduler 都已实现）。
//
// 收窄的理由不只是"能起假实现"：Execute 把工具入参翻译成 scheduler.ScheduleSpec 的
// 那几行是**纯接线**，漏传一个字段不报错、只让该能力静默失效——本 PR 的对抗审查实测，
// 删掉 `AnchorAt: a.Spec.AnchorAt` 后全仓测试照样绿。有了接口，替身才能捕获真正传下去
// 的 spec，把"工具面广告的字段真的到达了 scheduler"钉进单测，而不是只断言 schema 有这个 key。
type scheduleUpdater interface {
	UpdatePush(ctx context.Context, schedID string, spec scheduler.ScheduleSpec, nlDesc *string) error
}

type scheduleDeleter interface {
	DeletePush(ctx context.Context, schedID string) error
}

// profileStore 是画像两工具依赖的窄接口（M5 契约 §12.3，*store.Store 已实现），
// 收窄后 Execute 分支可用内存假实现覆盖，不依赖数据库。
type profileStore interface {
	GetProfile(ctx context.Context, userID int64) (*types.Profile, error)
	UpsertProfileFields(ctx context.Context, userID int64, industry, occupation *string, tags []string) (*types.Profile, error)
}

// playbookStore 是任务手册三条路径（view/edit_task_playbook + create_schedule 初始化）
// 依赖的窄接口（*store.Store 已实现）。收窄理由同 profileStore：Execute 分支（rune 截断、
// 归属未命中处理）可用内存假实现覆盖；归属校验的 SQL WHERE 本身只由 store 集成测试覆盖
// （enable_source 先例）。Upsert 返回 ok bool（同 EnableSource 的 enabled bool）：
// false=任务不存在/非本人，未写任何行。
type playbookStore interface {
	GetSchedulePlaybook(ctx context.Context, userID int64, scheduleID string) (*types.SchedulePlaybook, error)
	UpsertSchedulePlaybook(ctx context.Context, userID int64, scheduleID, content string) (ok bool, err error)
	// SetFetchPlan 只改 fetch_plan（P1 编译层），归属同 Upsert 进 SQL；ok=false=任务不存在/非本人/无手册行。
	SetFetchPlan(ctx context.Context, userID int64, scheduleID string, plan json.RawMessage) (ok bool, err error)
}

// scheduleCreator 收窄 create_schedule 依赖的 scheduler 能力（*scheduler.Scheduler 已实现），
// 使 Execute（含决策 D2 的手册初始化胶水）可用假实现单测，不必起真 Temporal。
type scheduleCreator interface {
	CreatePush(ctx context.Context, userID int64, spec scheduler.ScheduleSpec, scope workflow.PushScope, nlDesc string) (string, error)
}

// BuildTools 装配 agent 全部可用工具。返回的切片即工具白名单的静态部分（契约 §10）：
// loop 只认这里注册的名字 + 会话已激活的 TikHub 端点（端点注册表契约 §4），
// 模型编造的其余工具名一律拒绝。
// endpoints 为 nil（TikHub key 未配置）时不装配 search_endpoints，工具面与
// 该特性上线前完全一致。
// tr 是任务手册翻译器（P1 编译层）：create/edit 手册后据此把正文编译成 fetch_plan。
// 允许为 nil（未装配 LLM 的路径/测试）——此时手册仍可存取，只是不编译计划（best-effort）。
func BuildTools(st *store.Store, sched *scheduler.Scheduler, pusher PushTrigger, tr playbookTranslator, endpoints *EndpointTools) []Tool {
	tools := []Tool{
		&listSourcesTool{st: st},
		&addSourceTool{st: st},
		&removeSourceTool{st: st},
		&enableSourceTool{st: st},
		&listSchedulesTool{st: st},
		&createScheduleTool{sched: sched, st: st, tr: tr}, // st：创建即初始化手册（D2）；tr：并编译计划（P1）
		&updateScheduleTool{sched: sched},
		&removeScheduleTool{sched: sched},
		&pushNowTool{pusher: pusher},
		&viewProfileTool{st: st},
		&updateProfileTool{st: st},
		&viewTaskPlaybookTool{st: st}, // 情报任务手册（Task Playbook）
		&editTaskPlaybookTool{st: st, tr: tr},
	}
	if endpoints != nil {
		tools = append(tools, endpoints.SearchTool())
	}
	return tools
}

// emptyParamsSchema 是无参工具的 JSON schema：仍须是合法 object schema，
// DeepSeek FC 对空 properties 已实测可用（M4 spike）。
const emptyParamsSchema = `{"type":"object","properties":{}}`

// summarizeFallback 在确认卡摘要解析 args 失败时兜底：展示截断后的原始参数，
// 保证卡片恒有内容可读（Summarize 无 error 通道，不能失败）。
func summarizeFallback(action string, args json.RawMessage) string {
	raw := []rune(string(args))
	if len(raw) > 200 {
		raw = append(raw[:200], '…')
	}
	return action + "（参数未能解析）：" + string(raw)
}

// ============================================================
// list_sources：读工具，列出当前用户订阅的全部信源。
// ============================================================

type listSourcesTool struct {
	st *store.Store
}

func (t *listSourcesTool) Name() string { return "list_sources" }
func (t *listSourcesTool) Description() string {
	return "列出用户当前订阅的全部信源（含 id、类型、标题、状态）。"
}
func (t *listSourcesTool) Parameters() json.RawMessage { return json.RawMessage(emptyParamsSchema) }
func (t *listSourcesTool) Mutating() bool              { return false }

// Execute 用 ListSubscribedSourcesByUser（不过滤 source.status）而非 active-only：
// 与 GET /api/subscriptions 同语义——被自动 disabled 的源也要让用户看见并知道原因。
func (t *listSourcesTool) Execute(ctx context.Context, userID int64, _ json.RawMessage) (string, error) {
	sources, err := t.st.ListSubscribedSourcesByUser(ctx, userID)
	if err != nil {
		return "", err
	}
	if len(sources) == 0 {
		return "当前没有任何订阅信源。", nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "共 %d 个订阅信源：\n", len(sources))
	for _, src := range sources {
		title := src.Title
		if title == "" {
			title = src.URL
		}
		fmt.Fprintf(&b, "- id=%d [%s/%s] %s（状态: %s）\n", src.ID, src.Platform, src.Capability, title, src.Status)
	}
	return strings.TrimSuffix(b.String(), "\n"), nil
}

func (t *listSourcesTool) Summarize(json.RawMessage) string { return "" }

// ============================================================
// add_source：写工具，校验/构造复用 sourcespec（与 API 加订阅同一份规则）。
// ============================================================

// addSourceArgs 与工具 schema 对应。只认新 schema（platform + capability）——
// M6 契约 §13.1【硬约束】：legacy type 垫片只服务 HTTP api（vane-web 兼容窗口），
// 绝不进 agent 面，否则等于给模型两条重叠表达路（曾让 Boss 听见「已添加 tikhub_xhs 源」）。
type addSourceArgs struct {
	Platform       string   `json:"platform"`
	Capability     string   `json:"capability"`
	URL            string   `json:"url"`
	Query          string   `json:"query"`
	Keyword        string   `json:"keyword"`
	ScreenName     string   `json:"screen_name"`
	UserID         string   `json:"user_id"`     // xhs/user_posts：小红书用户 ID
	ProfileURL     string   `json:"profile_url"` // xhs/user_posts：小红书用户主页链接（可替代 user_id）
	Title          string   `json:"title"`
	Category       string   `json:"category"`
	Categories     []string `json:"categories"`
	IncludeDomains []string `json:"include_domains"` // 仅 web/search：Exa 域名白名单（追新解药）
}

// addSourceSchema 对齐 M6 信源插件化契约：只有新 schema（platform + capability + params）。
// M6 §13.1【硬约束】：legacy type 不进 agent 面（那是 HTTP api 给 vane-web 的兼容窗口）。
// url/query/keyword 的条件必填（随 platform/capability 而定）写进 description 由模型遵循，
// 权威校验在 sourcespec.Build（与 POST /api/subscriptions 完全一致）。
const addSourceSchema = `{
  "type": "object",
  "properties": {
    "platform": {
      "type": "string",
      "enum": ["web", "x", "xhs"],
      "description": "内容平台：web=开放网页；x=X(Twitter)；xhs=小红书"
    },
    "capability": {
      "type": "string",
      "enum": ["feed", "search", "user_posts", "contents"],
      "description": "能力：feed=RSS/Atom 订阅（仅 web）；search=关键词/语义搜索（web=Exa 网页搜索，xhs=小红书关键词）；user_posts=订阅某账号的新发布（x=Twitter 账号，xhs=小红书博主）；contents=监控指定网页内容变化（仅 web，如产品定价页——内容变了才推送）。当前不支持的能力及原因见本工具说明（Description）。"
    },
    "url": {"type": "string", "description": "网页地址（http/https）：platform=web capability=feed 时是 RSS 源地址，capability=contents 时是要监控变化的页面地址，均必填"},
    "query": {"type": "string", "description": "Exa 搜索词，platform=web capability=search 时必填"},
    "keyword": {"type": "string", "description": "小红书搜索关键词，platform=xhs capability=search 时必填"},
    "screen_name": {"type": "string", "description": "X 用户名（如 OpenAI），platform=x capability=user_posts 时必填"},
    "user_id": {"type": "string", "description": "小红书用户 ID（24 位十六进制），platform=xhs capability=user_posts 时必填（或改用 profile_url）"},
    "profile_url": {"type": "string", "description": "小红书用户主页链接（如 https://www.xiaohongshu.com/user/profile/<id>），platform=xhs capability=user_posts 时可替代 user_id"},
    "title": {"type": "string", "description": "可选：展示名，缺省按类型自动生成"},
    "category": {"type": "string", "description": "可选：Exa 结果类别（如 news），仅 web/search 生效"},
    "categories": {"type": "array", "items": {"type": "string"}, "description": "可选：RSS 分类过滤（如 [\"Product\",\"Research\"]），仅 web/feed 生效；不传=不限"},
    "include_domains": {"type": "array", "items": {"type": "string"}, "description": "可选：限定 Exa 搜索只返回这些域名的结果（如 [\"anthropic.com\",\"claude.com\"]），仅 web/search 生效；追新优先用它、避免日期过滤把无发布日期的官方页删光；不传=不限"}
  }
}`

type addSourceTool struct {
	st *store.Store
}

func (t *addSourceTool) Name() string { return "add_source" }
func (t *addSourceTool) Description() string {
	return "添加一个信源并建立订阅。指定 platform（web/xhs/x）和 capability（feed/search/user_posts/contents）。" +
		unavailableCapabilitiesNote()
}

// unavailableCapabilitiesNote 从 sourcecatalog 派生「当前不支持的能力及原因」附到工具说明里。
//
// 存在的理由（契约 §2.2 + 本次审计缺陷）：让模型能主动回答"X 关键词搜索为何不支持"，
// 而不是静默改用别的能力。关键是**原因取自注册表单一事实来源**（sourcecatalog.List），
// 不再是手抄进 schema 的副本——审计发现旧写法把 x/search 的 Reason 硬编码复制到 schema
// 里，注册表一改这里就漂移。改成派生后，注册表与 agent 工具面自动同步，
// 「注册表被三处共用」这句话（fetcher 分发 / sourcespec 构造 / agent 描述）才真正成立。
func unavailableCapabilitiesNote() string {
	var lines []string
	for _, e := range sourcecatalog.List() {
		if e.Available() {
			continue
		}
		lines = append(lines, fmt.Sprintf("%s/%s：%s", e.Platform, e.Capability, e.Reason))
	}
	if len(lines) == 0 {
		return ""
	}
	return "\n\n当前不支持的能力（请勿尝试添加，会被拒绝）：\n- " + strings.Join(lines, "\n- ")
}
func (t *addSourceTool) Parameters() json.RawMessage { return json.RawMessage(addSourceSchema) }
func (t *addSourceTool) Mutating() bool              { return true }

// specFromArgs 把模型给的扁平入参映射为 sourcespec.Spec 的 params map。
// 抽成纯函数是为了让「哪个字段进哪个 param 键」这层 **agent 独有**的映射可被单测：
// Execute 持具体 *store.Store（不可 fake），而拼错键名（如 screen_name→screenname）
// 不会被 sourcespec 自己的用例发现——只会在生产里产出错误的确认卡预填（审计 M-3）。
func specFromArgs(a addSourceArgs) sourcespec.Spec {
	params := make(map[string]string)
	set := func(k, v string) {
		if v != "" {
			params[k] = v
		}
	}
	set("url", a.URL)
	set("query", a.Query)
	set("keyword", a.Keyword)
	set("category", a.Category)
	set("screen_name", a.ScreenName)
	set("user_id", a.UserID)
	set("profile_url", a.ProfileURL)
	if len(a.Categories) > 0 {
		catJSON, _ := json.Marshal(a.Categories)
		params["categories"] = string(catJSON)
	}
	// include_domains：[]string → JSON 字符串数组进 params（与 categories 同一序列化路径），
	// sourcespec 反序列化后归一化 + 入 config/幂等键（D-2 修复）。
	if len(a.IncludeDomains) > 0 {
		domJSON, _ := json.Marshal(a.IncludeDomains)
		params["include_domains"] = string(domJSON)
	}
	return sourcespec.Spec{
		Platform:   a.Platform,
		Capability: a.Capability,
		Params:     params,
		Title:      a.Title,
	}
}

func (t *addSourceTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	var a addSourceArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "参数不是合法 JSON，请修正后重试", nil
	}
	if a.Platform == "" {
		return "请指定 platform（web/xhs/x）与 capability（feed/search/user_posts）", nil
	}
	src, msg := sourcespec.Build(specFromArgs(a))
	if msg != "" {
		return msg, nil
	}
	sourceID, updated, err := t.st.UpsertSource(ctx, src)
	if err != nil {
		return "", err
	}
	if err := t.st.AddSubscription(ctx, userID, sourceID); err != nil {
		return "", err
	}
	title := src.Title
	if title == "" {
		title = src.URL
	}
	verb := "已添加并订阅"
	if updated {
		verb = "已更新既有"
	}
	return fmt.Sprintf("%s信源（id=%d）：[%s/%s] %s", verb, sourceID, src.Platform, src.Capability, title), nil
}

func (t *addSourceTool) Summarize(args json.RawMessage) string {
	var a addSourceArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return summarizeFallback("添加信源", args)
	}
	var b strings.Builder
	p, c := a.Platform, a.Capability
	switch p + "/" + c {
	case "web/feed":
		fmt.Fprintf(&b, "添加 RSS 信源：%s", a.URL)
	case "web/search":
		fmt.Fprintf(&b, "添加搜索信源：搜索词「%s」", strings.TrimSpace(a.Query))
		if a.Category != "" {
			fmt.Fprintf(&b, "，类别「%s」", a.Category)
		}
		if len(a.IncludeDomains) > 0 {
			fmt.Fprintf(&b, "，限定域名 %s", strings.Join(a.IncludeDomains, "、"))
		}
	case "xhs/search":
		fmt.Fprintf(&b, "添加小红书关键词信源：「%s」", strings.TrimSpace(a.Keyword))
	case "xhs/user_posts":
		who := strings.TrimSpace(a.UserID)
		if who == "" {
			who = strings.TrimSpace(a.ProfileURL)
		}
		fmt.Fprintf(&b, "添加小红书博主信源：%s", who)
	case "x/user_posts":
		fmt.Fprintf(&b, "添加 X 用户时间线信源：@%s", strings.TrimSpace(a.ScreenName))
	default:
		fmt.Fprintf(&b, "添加信源（%s/%s，确认后校验）", p, c)
	}
	if title := strings.TrimSpace(a.Title); title != "" {
		fmt.Fprintf(&b, "，展示名「%s」", title)
	}
	return b.String()
}

// ============================================================
// remove_source：写工具，取消订阅（信源本身与历史内容保留，与 DELETE /api/subscriptions 同语义）。
// ============================================================

const removeSourceSchema = `{
  "type": "object",
  "properties": {
    "source_id": {"type": "integer", "description": "要取消订阅的信源 id（可先用 list_sources 查询）"}
  },
  "required": ["source_id"]
}`

type removeSourceTool struct {
	st *store.Store
}

func (t *removeSourceTool) Name() string { return "remove_source" }
func (t *removeSourceTool) Description() string {
	return "取消订阅指定信源（信源本身与历史内容保留）。source_id 可先用 list_sources 查询。"
}
func (t *removeSourceTool) Parameters() json.RawMessage { return json.RawMessage(removeSourceSchema) }
func (t *removeSourceTool) Mutating() bool              { return true }

// Execute 复用 store.RemoveSubscription：按 (user_id, source_id) 删行，天然只能
// 删自己的订阅，无需再校验信源归属；删不存在的订阅静默成功（与 API 幂等语义一致）。
func (t *removeSourceTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	var a struct {
		SourceID int64 `json:"source_id"`
	}
	if err := json.Unmarshal(args, &a); err != nil || a.SourceID <= 0 {
		return "source_id 必须是正整数", nil
	}
	if err := t.st.RemoveSubscription(ctx, userID, a.SourceID); err != nil {
		return "", err
	}
	return fmt.Sprintf("已取消订阅信源（id=%d），信源与历史内容保留。", a.SourceID), nil
}

func (t *removeSourceTool) Summarize(args json.RawMessage) string {
	var a struct {
		SourceID int64 `json:"source_id"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return summarizeFallback("取消订阅信源", args)
	}
	return fmt.Sprintf("取消订阅信源（id=%d）", a.SourceID)
}

const enableSourceSchema = `{
  "type": "object",
  "properties": {
    "source_id": {"type": "integer", "description": "要重新启用的信源 id（连续抓取失败被自动暂停的源，可先用 list_sources 查看状态）"}
  },
  "required": ["source_id"]
}`

type enableSourceTool struct {
	st *store.Store
}

func (t *enableSourceTool) Name() string { return "enable_source" }
func (t *enableSourceTool) Description() string {
	return "重新启用一个因连续抓取失败被自动暂停的信源：置回正常、清零失败计数、立即恢复抓取。source_id 可先用 list_sources 查看状态。"
}
func (t *enableSourceTool) Parameters() json.RawMessage { return json.RawMessage(enableSourceSchema) }
func (t *enableSourceTool) Mutating() bool              { return true }

// Execute 复用 store.EnableSource：归属校验（本人 active 订阅）进 SQL 的 WHERE，
// 启用未订阅的源 enabled=false，回自纠文案而不上抛（与 remove_source 的越权处理一致）。
func (t *enableSourceTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	var a struct {
		SourceID int64 `json:"source_id"`
	}
	if err := json.Unmarshal(args, &a); err != nil || a.SourceID <= 0 {
		return "source_id 必须是正整数", nil
	}
	enabled, err := t.st.EnableSource(ctx, userID, a.SourceID)
	if err != nil {
		return "", err
	}
	if !enabled {
		return fmt.Sprintf("没找到你订阅的信源（id=%d），可能已取消订阅或 id 有误。用 list_sources 查一下。", a.SourceID), nil
	}
	return fmt.Sprintf("已重新启用信源（id=%d），失败计数已清零，将在下次抓取时恢复。", a.SourceID), nil
}

func (t *enableSourceTool) Summarize(args json.RawMessage) string {
	var a struct {
		SourceID int64 `json:"source_id"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return summarizeFallback("重新启用信源", args)
	}
	return fmt.Sprintf("重新启用信源（id=%d）", a.SourceID)
}

// ============================================================
// list_schedules：读工具，列出当前用户的全部定时推送任务。
// ============================================================

type listSchedulesTool struct {
	st *store.Store
}

func (t *listSchedulesTool) Name() string { return "list_schedules" }
func (t *listSchedulesTool) Description() string {
	return "列出用户当前的全部定时推送任务（含 id、触发频率、状态、描述）。"
}
func (t *listSchedulesTool) Parameters() json.RawMessage { return json.RawMessage(emptyParamsSchema) }
func (t *listSchedulesTool) Mutating() bool              { return false }

func (t *listSchedulesTool) Execute(ctx context.Context, userID int64, _ json.RawMessage) (string, error) {
	list, err := t.st.ListSchedulesByUser(ctx, userID)
	if err != nil {
		return "", err
	}
	if len(list) == 0 {
		return "当前没有任何定时推送任务。", nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "共 %d 个定时推送任务：\n", len(list))
	for _, sc := range list {
		desc := sc.NLDescription
		if desc == "" {
			desc = "（无描述）"
		}
		fmt.Fprintf(&b, "- id=%s %s（状态: %s，描述: %s）\n", sc.ID, formatSpecJSON(sc.SpecJSON), sc.Status, desc)
	}
	return strings.TrimSuffix(b.String(), "\n"), nil
}

func (t *listSchedulesTool) Summarize(json.RawMessage) string { return "" }

// formatSpecJSON 把镜像表里的 spec JSONB 渲染成中文频率描述；解析失败时
// 原样展示 JSON（列表工具不应因单条脏数据整体失败）。
func formatSpecJSON(raw json.RawMessage) string {
	var spec scheduler.ScheduleSpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return "频率: " + string(raw)
	}
	return formatScheduleSpec(spec)
}

// formatScheduleSpec 渲染中立 spec 的中文频率描述。默认时区与 scheduler.defaultTZ 一致。
func formatScheduleSpec(spec scheduler.ScheduleSpec) string {
	tz := spec.TZ
	if tz == "" {
		tz = "Asia/Shanghai"
	}
	if spec.EverySeconds > 0 {
		// 有锚点时必须说出来：同样是"每 259200 秒"，有没有锚点决定了它落在
		// epoch 对齐点还是用户指定的时刻，卡面不提等于让用户无从判断对不对。
		if anchor := strings.TrimSpace(spec.AnchorAt); anchor != "" {
			return fmt.Sprintf("从 %s 起每 %d 秒触发一次（时区 %s）", anchor, spec.EverySeconds, tz)
		}
		return fmt.Sprintf("每 %d 秒触发一次（时区 %s，按 epoch 对齐）", spec.EverySeconds, tz)
	}
	return fmt.Sprintf("按 cron「%s」触发（时区 %s）", spec.Cron, tz)
}

// ============================================================
// create_schedule：写工具，前置校验对齐 api/schedules.go，权威校验在 scheduler。
// ============================================================

// minEverySeconds 与 api/schedules.go / scheduler 的 1 小时硬地板一致；
// 这里前置拦截只为尽早给模型清晰文案，scheduler.validateSpec 仍是权威校验方。
const minEverySeconds = 3600

const createScheduleSchema = `{
  "type": "object",
  "properties": {
    "spec": {
      "type": "object",
      "description": "触发频率：cron 与 every_seconds 必须且只能提供一个",
      "properties": {
        "cron": {"type": "string", "description": "5 段 cron（分 时 日 月 周），分钟字段必须是 0-59 的整数，如 \"0 8 * * *\""},
        "every_seconds": {"type": "integer", "description": "固定间隔秒数，不低于 3600（1 小时）"},
        "anchor_at": {"type": "string", "description": "可选：RFC3339 绝对时刻（如 \"2026-07-19T20:00:00+08:00\"），只能与 every_seconds 搭配。给了它触发点就从该时刻起按间隔推进（每 3 天的晚上 8 点 = every_seconds:259200 + 该时刻）；不给则按 Unix epoch 对齐（21600 会落在 00/06/12/18 点整，通常不是用户想要的）"},
        "tz": {"type": "string", "description": "可选：IANA 时区名，缺省 Asia/Shanghai"}
      }
    },
    "nl_description": {"type": "string", "description": "可选：该任务的自然语言描述（如\"每天早上 8 点推送\"），用于列表展示"}
  },
  "required": ["spec"]
}`

// createScheduleArgs 与工具 schema 对应；spec 结构与 api 的 scheduleSpecDTO 一致。
type createScheduleArgs struct {
	Spec struct {
		Cron         string `json:"cron"`
		EverySeconds int    `json:"every_seconds"`
		AnchorAt     string `json:"anchor_at"`
		TZ           string `json:"tz"`
	} `json:"spec"`
	NLDescription string `json:"nl_description"`
}

type createScheduleTool struct {
	sched scheduleCreator
	st    playbookStore      // 创建即初始化任务手册（决策 D2）
	tr    playbookTranslator // 并把手册意图编译成 fetch_plan（P1 编译层；nil=跳过）
}

func (t *createScheduleTool) Name() string { return "create_schedule" }
func (t *createScheduleTool) Description() string {
	return "创建定时推送任务。触发频率用 cron（5 段，分钟字段须为整数）或 every_seconds（固定间隔秒数，不低于 3600）二选一，频率不得高于每小时一次。"
}
func (t *createScheduleTool) Parameters() json.RawMessage {
	return json.RawMessage(createScheduleSchema)
}
func (t *createScheduleTool) Mutating() bool { return true }

// Execute scope 固定为零值（该用户全部 active 订阅）：契约 §8 的工具参数只有
// spec 与 nl_description，按源筛选的定向推送不在 agent 工具面。
func (t *createScheduleTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	var a createScheduleArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "参数不是合法 JSON，请修正后重试", nil
	}
	if msg := validateScheduleArgs(a); msg != "" {
		return msg, nil
	}
	spec := scheduler.ScheduleSpec{
		Cron:         a.Spec.Cron,
		EverySeconds: a.Spec.EverySeconds,
		AnchorAt:     a.Spec.AnchorAt,
		TZ:           a.Spec.TZ,
	}
	schedID, err := t.sched.CreatePush(ctx, userID, spec, workflow.PushScope{}, a.NLDescription)
	if err != nil {
		// CreatePush 的 CodeValidation（cron 分钟字段过细、活跃上限等）是确定性、
		// 用户可修正的失败：转成文案返回而非向上抛，模型/卡片能给出可读提示；
		// 其余（Temporal/DB）仍作为基础设施错误上抛。
		var ae *types.AppError
		if errors.As(err, &ae) && ae.Code == types.CodeValidation {
			return ae.Message, nil
		}
		return "", err
	}
	// 决策 D2：一个定时任务 = 一份自带手册的情报简报。调度建成后随即用用户的自然语言
	// 意图原文初始化手册（P0 只存、不翻译成 fetch_plan、不影响抓取）。best-effort：手册写
	// 失败不回滚已建成的调度——调度是主效果、P0 手册尚不参与抓取，为非功能性的手册写失败去
	// 补偿删 Temporal 调度得不偿失；仅 slog 供对账（与 DeletePush 镜像删除失败只 slog 同原则）。
	content := capPlaybookContent(a.NLDescription)
	if ok, perr := t.st.UpsertSchedulePlaybook(ctx, userID, schedID, content); perr != nil {
		slog.Error("agent: create_schedule 初始化任务手册失败（调度已创建）", "schedule_id", schedID, "err", perr)
	} else if !ok {
		slog.Warn("agent: create_schedule 初始化任务手册未命中刚建的调度（异常）", "schedule_id", schedID)
	} else {
		// 手册已初始化 → 据此编译抓取计划（P1 编译层，best-effort：失败只 slog、不影响调度）。
		compilePlaybookPlan(ctx, t.st, t.tr, userID, schedID, content)
	}
	return fmt.Sprintf("已创建定时推送任务（id=%s）：%s", schedID, formatScheduleSpec(spec)), nil
}

func (t *createScheduleTool) Summarize(args json.RawMessage) string {
	var a createScheduleArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return summarizeFallback("创建定时推送任务", args)
	}
	s := "创建定时推送任务：" + formatScheduleSpec(scheduler.ScheduleSpec{
		Cron:         a.Spec.Cron,
		EverySeconds: a.Spec.EverySeconds,
		AnchorAt:     a.Spec.AnchorAt,
		TZ:           a.Spec.TZ,
	})
	if desc := strings.TrimSpace(a.NLDescription); desc != "" {
		s += fmt.Sprintf("，描述「%s」", desc)
	}
	return s
}

// validateScheduleSpecFields 是 create_schedule / update_schedule 共用的 spec 前置校验，
// 与 api/schedules.go 的 toScheduleSpec 逐条对齐：cron 与 every_seconds 恰好提供其一；
// every_seconds 不低于 1h 地板。文案同 api。
//
// 抽成共用函数而不是两处各写一遍：两个工具对同一个 spec 结构给出不同的拒绝理由，
// 就等于同一条规则在系统里有两个版本，改地板时必漏一处。cron 频率的权威校验仍在
// scheduler.validateCronMinInterval（这里只做能尽早给出清晰文案的那部分）。
func validateScheduleSpecFields(cron string, everySeconds int) string {
	hasCron := strings.TrimSpace(cron) != ""
	hasEvery := everySeconds > 0
	if hasCron == hasEvery {
		return "spec 必须且只能提供 cron 或 every_seconds 之一"
	}
	if hasEvery && everySeconds < minEverySeconds {
		return "推送间隔不得小于 1 小时"
	}
	return ""
}

// validateScheduleArgs 是 create_schedule 的入口（保留原名，语义不变）。
func validateScheduleArgs(a createScheduleArgs) string {
	return validateScheduleSpecFields(a.Spec.Cron, a.Spec.EverySeconds)
}

// ============================================================
// update_schedule：写工具，原地改已有调度的触发频率（不删重建，schedule_id 不变）。
// ============================================================

const updateScheduleSchema = `{
  "type": "object",
  "properties": {
    "schedule_id": {"type": "string", "description": "要修改的定时任务 id（先用 list_schedules 查）"},
    "spec": {
      "type": "object",
      "description": "新的触发频率：cron 与 every_seconds 必须且只能提供一个",
      "properties": {
        "cron": {"type": "string", "description": "5 段 cron（分 时 日 月 周），按时区的墙上时间触发，分钟字段必须是 0-59 的整数，如 \"30 8 * * *\"=每天 8:30。要「每天某个具体时刻」一律用 cron"},
        "every_seconds": {"type": "integer", "description": "固定间隔秒数，不低于 3600。不配 anchor_at 时触发点按 Unix epoch 对齐（21600=6 小时落在 00/06/12/18 点整），要指定从哪个时刻起请配 anchor_at"},
        "anchor_at": {"type": "string", "description": "可选：RFC3339 绝对时刻（如 \"2026-07-19T20:00:00+08:00\"），只能与 every_seconds 搭配。给了它触发点就从该时刻起按间隔推进（每 3 天的晚上 8 点 = every_seconds:259200 + 该时刻）；不给则按 Unix epoch 对齐（21600 会落在 00/06/12/18 点整，通常不是用户想要的）"},
        "tz": {"type": "string", "description": "可选：IANA 时区名，缺省 Asia/Shanghai"}
      }
    },
    "nl_description": {"type": "string", "description": "可选：同时更新该任务的自然语言描述（如\"每天早上 8 点半推送\"）；省略则保持原描述不变"}
  },
  "required": ["schedule_id", "spec"]
}`

type updateScheduleArgs struct {
	ScheduleID string `json:"schedule_id"`
	Spec       struct {
		Cron         string `json:"cron"`
		EverySeconds int    `json:"every_seconds"`
		AnchorAt     string `json:"anchor_at"`
		TZ           string `json:"tz"`
	} `json:"spec"`
	// 指针区分「省略=不改描述」与「显式置空」，与 store.UpdateScheduleSpec 的 nil 语义对应。
	NLDescription *string `json:"nl_description"`
}

type updateScheduleTool struct {
	sched scheduleUpdater
}

func (t *updateScheduleTool) Name() string { return "update_schedule" }
func (t *updateScheduleTool) Description() string {
	return "修改已有定时推送任务的触发频率（原地改，schedule_id 不变，不会中断调度）。" +
		"要改推送时间用本工具，不要用「删除再新建」——那会换掉 id 且中间有空窗。" +
		"频率用 cron（5 段，按墙上时间）或 every_seconds（固定间隔；配 anchor_at 可指定从哪个时刻起，" +
		"不配则按 epoch 对齐）二选一，不得高于每小时一次。"
}
func (t *updateScheduleTool) Parameters() json.RawMessage {
	return json.RawMessage(updateScheduleSchema)
}
func (t *updateScheduleTool) Mutating() bool { return true }

func (t *updateScheduleTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	var a updateScheduleArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "参数不是合法 JSON，请修正后重试", nil
	}
	schedID := strings.TrimSpace(a.ScheduleID)
	if schedID == "" {
		return "schedule_id 必须是非空字符串（可先用 list_schedules 查询）", nil
	}
	if msg := validateScheduleSpecFields(a.Spec.Cron, a.Spec.EverySeconds); msg != "" {
		return msg, nil
	}
	spec := scheduler.ScheduleSpec{
		Cron:         a.Spec.Cron,
		EverySeconds: a.Spec.EverySeconds,
		AnchorAt:     a.Spec.AnchorAt,
		TZ:           a.Spec.TZ,
	}
	if err := t.sched.UpdatePush(ctx, schedID, userID, spec, a.NLDescription); err != nil {
		// 与 create_schedule 同约定：确定性、用户可修正的失败（cron 过细、任务不存在）
		// 转成文案回给模型自纠；基础设施错误上抛。
		var ae *types.AppError
		if errors.As(err, &ae) && (ae.Code == types.CodeValidation || ae.Code == types.CodeNotFound) {
			return ae.Message, nil
		}
		return "", err
	}
	return fmt.Sprintf("已修改定时推送任务（id=%s）：%s", schedID, formatScheduleSpec(spec)), nil
}

func (t *updateScheduleTool) Summarize(args json.RawMessage) string {
	var a updateScheduleArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return summarizeFallback("修改定时推送任务", args)
	}
	s := fmt.Sprintf("修改定时推送任务（id=%s）：触发频率改为 %s",
		strings.TrimSpace(a.ScheduleID),
		formatScheduleSpec(scheduler.ScheduleSpec{
			Cron:         a.Spec.Cron,
			EverySeconds: a.Spec.EverySeconds,
			AnchorAt:     a.Spec.AnchorAt, // 漏了它，卡面会说"按 epoch 对齐"而实际锚定——主动说反话
			TZ:           a.Spec.TZ,
		}))
	// 描述只在显式提供时上卡：省略=不改，卡面不能让人以为描述会被动。
	if a.NLDescription != nil {
		if desc := strings.TrimSpace(*a.NLDescription); desc != "" {
			s += fmt.Sprintf("，描述改为「%s」", desc)
		} else {
			s += "，并清空描述"
		}
	}
	return s
}

// ============================================================
// remove_schedule：写工具，删除 Temporal 调度 + 镜像。
// ============================================================

const removeScheduleSchema = `{
  "type": "object",
  "properties": {
    "schedule_id": {"type": "string", "description": "要删除的定时任务 id（可先用 list_schedules 查询）"}
  },
  "required": ["schedule_id"]
}`

type removeScheduleTool struct {
	sched scheduleDeleter
}

func (t *removeScheduleTool) Name() string { return "remove_schedule" }
func (t *removeScheduleTool) Description() string {
	return "删除指定定时推送任务。schedule_id 可先用 list_schedules 查询。"
}
func (t *removeScheduleTool) Parameters() json.RawMessage {
	return json.RawMessage(removeScheduleSchema)
}
func (t *removeScheduleTool) Mutating() bool { return true }

// Execute 不逐条校验调度归属：单 owner MVP 所有调度同属一人，与
// DELETE /api/schedules/{id} 的既有策略一致（api/schedules.go 注释）。
func (t *removeScheduleTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	var a struct {
		ScheduleID string `json:"schedule_id"`
	}
	if err := json.Unmarshal(args, &a); err != nil || strings.TrimSpace(a.ScheduleID) == "" {
		return "schedule_id 必须是非空字符串", nil
	}
	schedID := strings.TrimSpace(a.ScheduleID)
	if err := t.sched.DeletePush(ctx, schedID, userID); err != nil {
		return "", err
	}
	return fmt.Sprintf("已删除定时推送任务（id=%s）。", schedID), nil
}

func (t *removeScheduleTool) Summarize(args json.RawMessage) string {
	var a struct {
		ScheduleID string `json:"schedule_id"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return summarizeFallback("删除定时推送任务", args)
	}
	return fmt.Sprintf("删除定时推送任务（id=%s）", strings.TrimSpace(a.ScheduleID))
}

// ============================================================
// push_now：读工具（低危，契约 §8）：立即触发一次推送，不建调度、不改任何配置，
// 故不走确认卡。经 PushTrigger 窄接口触发（与 POST /api/push/now 同款底层）。
// 免确认的扇出护栏在 scheduler.TriggerPushNow：确定性 workflow ID 把同一用户的
// 并发管道钉死为 1（审查 #push_now 扇出），模型多次调用只会收到"已在进行"文案。
// ============================================================

type pushNowTool struct {
	pusher PushTrigger
}

func (t *pushNowTool) Name() string { return "push_now" }
func (t *pushNowTool) Description() string {
	return "立即触发一次推送（推送用户全部订阅的最新内容，不创建定时任务）。"
}
func (t *pushNowTool) Parameters() json.RawMessage { return json.RawMessage(emptyParamsSchema) }
func (t *pushNowTool) Mutating() bool              { return false }

func (t *pushNowTool) Execute(ctx context.Context, userID int64, _ json.RawMessage) (string, error) {
	runID, err := t.pusher.TriggerPushNow(ctx, userID)
	if err != nil {
		// 确定性拒绝（并发护栏"已有推送在进行"）走自纠通道：文案回给模型，
		// 由它向用户解释而不是重复触发；基础设施错误仍上抛。
		// 只回 AppError.Message：Error() 会拼上 Cause（Temporal 服务端原文
		// "Workflow execution is already running" 之类），错误链不进模型上下文。
		if errors.Is(err, types.ErrValidation) {
			var ae *types.AppError
			if errors.As(err, &ae) && ae.Message != "" {
				return ae.Message, nil
			}
			return "本次触发被拒绝，请稍后再试。", nil
		}
		return "", err
	}
	return fmt.Sprintf("已触发一次立即推送（run_id=%s），推送卡片稍后送达飞书。", runID), nil
}

func (t *pushNowTool) Summarize(json.RawMessage) string { return "" }

// ============================================================
// view_profile：读工具，查看当前用户画像（M5 契约 §12.3）。
// ============================================================

type viewProfileTool struct {
	st profileStore
}

func (t *viewProfileTool) Name() string { return "view_profile" }
func (t *viewProfileTool) Description() string {
	return "查看用户当前画像（行业、职业、关注标签、摘要）。修改画像前应先调用本工具取现有标签，合并后再提交。"
}
func (t *viewProfileTool) Parameters() json.RawMessage { return json.RawMessage(emptyParamsSchema) }
func (t *viewProfileTool) Mutating() bool              { return false }

// Execute NotFound 回固定引导文案（契约 §12.3 锁死文本）而非报错：画像为空是
// 常态起点，systemPrompt 会驱动模型据此自然引导首采。
func (t *viewProfileTool) Execute(ctx context.Context, userID int64, _ json.RawMessage) (string, error) {
	p, err := t.st.GetProfile(ctx, userID)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return "画像为空：还不了解你。可以告诉我你的行业、职业和关注的主题。", nil
		}
		return "", err
	}
	return "当前画像——" + renderProfile(p), nil
}

func (t *viewProfileTool) Summarize(json.RawMessage) string { return "" }

// ============================================================
// update_profile：写工具，画像首采（2.1）与人工修正（2.3）共用，走 M4 标准确认卡
// （首采不特例：AI 出预填、人点执行的不变式对写画像同样成立）。
// summary 刻意不在工具面：归演化独有；标签删除只在这里发生（演化只增不减）。
// ============================================================

// maxProfileTags 人工标签上限（契约 §2：与库内/演化上限统一为 12，超 12 截前 12——
// 截断而非报错，人工整体替换不得静默丢演化标签，也不得因超限整次作废）。
const maxProfileTags = 12

const updateProfileSchema = `{
  "type": "object",
  "properties": {
    "industry": {"type": "string", "description": "所在行业，省略表示不修改"},
    "occupation": {"type": "string", "description": "职业/岗位，省略表示不修改"},
    "tags": {
      "type": "array",
      "items": {"type": "string"},
      "description": "关注标签列表，整体替换现有标签：先用 view_profile 取现有标签、合并后完整提供（缺了的会被删除），最多 12 个；省略表示不修改"
    }
  }
}`

// updateProfileArgs 以指针/nil 切片区分「省略=不改」与「显式置空」，
// 与 store.UpsertProfileFields 的 nil 语义一一对应。
type updateProfileArgs struct {
	Industry   *string  `json:"industry"`
	Occupation *string  `json:"occupation"`
	Tags       []string `json:"tags"`
}

type updateProfileTool struct {
	st profileStore
}

func (t *updateProfileTool) Name() string { return "update_profile" }
func (t *updateProfileTool) Description() string {
	return "更新用户画像：行业、职业、关注标签。tags 是整体替换，提交前先用 view_profile 查看现有标签并合并；省略的字段保持不变。"
}
func (t *updateProfileTool) Parameters() json.RawMessage {
	return json.RawMessage(updateProfileSchema)
}
func (t *updateProfileTool) Mutating() bool { return true }

// Execute（确认后执行）：全缺省是确定性可自纠失败，回文案不触库；
// UpsertProfileFields 部分更新（nil 不改），不触碰 summary/游标/token 三件套。
func (t *updateProfileTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	var a updateProfileArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "参数不是合法 JSON，请修正后重试", nil
	}
	if a.Industry == nil && a.Occupation == nil && a.Tags == nil {
		return "没有提供任何要修改的字段，请至少提供 industry、occupation、tags 之一", nil
	}
	p, err := t.st.UpsertProfileFields(ctx, userID, a.Industry, a.Occupation, capProfileTags(a.Tags))
	if err != nil {
		return "", err
	}
	return "画像已更新。当前画像——" + renderProfile(p), nil
}

// Summarize 只列提供的字段（契约 §12.3）：确认卡如实展示本次会改什么，
// 未提供的字段绝不出现——「不改」与「清空」在卡面上必须可区分。
func (t *updateProfileTool) Summarize(args json.RawMessage) string {
	var a updateProfileArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return summarizeFallback("更新画像", args)
	}
	var parts []string
	if a.Industry != nil {
		parts = append(parts, profileFieldChange("行业", *a.Industry))
	}
	if a.Occupation != nil {
		parts = append(parts, profileFieldChange("职业", *a.Occupation))
	}
	if a.Tags != nil {
		if tags := capProfileTags(a.Tags); len(tags) == 0 {
			parts = append(parts, "清空关注标签")
		} else {
			// 展示截断后的实际生效值：卡面列 13 个而只落 12 个是对用户撒谎。
			parts = append(parts, "关注标签整体替换为「"+strings.Join(tags, "、")+"」")
		}
	}
	if len(parts) == 0 {
		return "更新画像（未提供任何字段，确认后不会有变更）"
	}
	return "更新画像：" + strings.Join(parts, "；")
}

// capProfileTags 超 12 截前 12。nil 原样透传：nil=不改，截断不得把它变成非 nil 置空。
func capProfileTags(tags []string) []string {
	if len(tags) > maxProfileTags {
		return tags[:maxProfileTags]
	}
	return tags
}

// profileFieldChange 渲染单字段变更描述；空串是显式清空，卡面要说人话。
func profileFieldChange(name, v string) string {
	if strings.TrimSpace(v) == "" {
		return "清空" + name
	}
	return name + "改为「" + v + "」"
}

// renderProfile 把画像渲染成给模型/用户看的中文单段（view_profile 与 update_profile
// 结果共用）。行业/职业/标签为空时显式标注「未设置」：模型据此知道缺什么、该引导
// 采集什么；summary 归演化独有，为空整段省略，不引导模型对它下手。
func renderProfile(p *types.Profile) string {
	var b strings.Builder
	b.WriteString("行业：" + orUnset(p.Industry))
	b.WriteString("；职业：" + orUnset(p.Occupation))
	if len(p.Tags) > 0 {
		b.WriteString("；关注标签：" + strings.Join(p.Tags, "、"))
	} else {
		b.WriteString("；关注标签：（未设置）")
	}
	if s := strings.TrimSpace(p.Summary); s != "" {
		b.WriteString("；摘要：" + s)
	}
	return b.String()
}

func orUnset(s string) string {
	if strings.TrimSpace(s) == "" {
		return "（未设置）"
	}
	return s
}

// ============================================================
// 情报任务手册（Task Playbook P0）：每个定时任务 1:1 绑一份自然语言手册。
// P0 只做存取——view（读）/ edit（写，走确认卡）/ create_schedule 创建即初始化。
// 手册不影响任何抓取/打分/出卡（那是 P1）。
// ============================================================

// maxPlaybookContentRunes 手册内容 rune 上限：超出截断（不报错，同 capProfileTags 策略），
// 防超长手册撑爆确认卡与 DB 行。playbookSummaryPreviewRunes 是确认卡摘要预览的更短上限。
const (
	maxPlaybookContentRunes     = 4000
	playbookSummaryPreviewRunes = 80
)

// capPlaybookContent 超上限截前 N rune（不加省略号——落库内容要干净）。
func capPlaybookContent(s string) string {
	r := []rune(s)
	if len(r) > maxPlaybookContentRunes {
		return string(r[:maxPlaybookContentRunes])
	}
	return s
}

const viewTaskPlaybookSchema = `{
  "type": "object",
  "properties": {
    "schedule_id": {"type": "string", "description": "要查看手册的定时任务 id（先用 list_schedules 查询）"}
  },
  "required": ["schedule_id"]
}`

type viewTaskPlaybookTool struct {
	st playbookStore
}

func (t *viewTaskPlaybookTool) Name() string { return "view_task_playbook" }
func (t *viewTaskPlaybookTool) Description() string {
	return "查看某个定时推送任务的情报手册（描述这个任务要抓什么、关注哪些主题、偏好哪些来源）。schedule_id 可先用 list_schedules 查询。修改手册前应先调用本工具取现有内容。"
}
func (t *viewTaskPlaybookTool) Parameters() json.RawMessage {
	return json.RawMessage(viewTaskPlaybookSchema)
}
func (t *viewTaskPlaybookTool) Mutating() bool { return false }

// Execute NotFound 回引导文案而非报错：手册不存在（老任务未迁移）或任务非本人，
// 对模型都是"没有可看的手册"，回自纠文案让它引导用户；基础设施错误上抛。
func (t *viewTaskPlaybookTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	var a struct {
		ScheduleID string `json:"schedule_id"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return "参数不是合法 JSON，请修正后重试", nil
	}
	schedID := strings.TrimSpace(a.ScheduleID)
	if schedID == "" {
		return "schedule_id 必须是非空字符串（可先用 list_schedules 查询）", nil
	}
	pb, err := t.st.GetSchedulePlaybook(ctx, userID, schedID)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return fmt.Sprintf("没找到你的定时任务（id=%s），或它还没有手册。用 list_schedules 查一下。", schedID), nil
		}
		return "", err
	}
	return renderPlaybook(pb), nil
}

func (t *viewTaskPlaybookTool) Summarize(json.RawMessage) string { return "" }

// renderPlaybook 把手册渲染成给模型/用户看的中文文本：手册正文 +（P1 编译层）据此编译出的
// 抓取计划摘要。content 为空时给引导文案，让模型知道该采集什么；fetch_plan 无源时不赘述。
func renderPlaybook(pb *types.SchedulePlaybook) string {
	content := strings.TrimSpace(pb.Content)
	if content == "" {
		content = "（手册为空——可以告诉我这个任务要抓什么、关注哪些主题、偏好哪些来源。）"
	}
	out := fmt.Sprintf("任务手册（id=%s）：\n%s", pb.ScheduleID, content)
	if plan := renderFetchPlan(pb.FetchPlan); plan != "" {
		out += "\n\n" + plan
	}
	return out
}

// renderFetchPlan 渲染 fetch_plan 里已编译的源；无源/不合法时返回空串（渲染方不赘述）。
func renderFetchPlan(raw json.RawMessage) string {
	var plan FetchPlan
	if len(raw) == 0 || json.Unmarshal(raw, &plan) != nil || len(plan.Sources) == 0 {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "据此编译出的抓取计划（%d 个源）：", len(plan.Sources))
	for _, s := range plan.Sources {
		title := s.Title
		if title == "" {
			title = s.URL
		}
		fmt.Fprintf(&b, "\n- [%s/%s] %s", s.Platform, s.Capability, title)
	}
	return b.String()
}

// compilePlaybookPlan 把手册正文翻译成 fetch_plan 并落库（P1 编译层的工具侧胶水）。
// **best-effort**：翻译器未装配（tr==nil）或任何一步失败都只 slog、绝不影响主效果
// （调度已建 / 手册正文已存）。返回成功落库的计划源数（>=0），供 edit 回执展示。
func compilePlaybookPlan(ctx context.Context, st playbookStore, tr playbookTranslator, userID int64, scheduleID, content string) int {
	if tr == nil {
		return 0 // 未装配 LLM 翻译器（部分测试/降级路径）：手册仍可用，只是不编译计划。
	}
	plan, err := tr.Translate(ctx, userID, content)
	if err != nil {
		// 翻译失败（LLM 调用错误 / 输出取不出 JSON）：保留既有计划不动，只记日志。
		slog.Warn("agent: 手册编译成抓取计划失败（手册已存，本次不更新计划）", "schedule_id", scheduleID, "err", err)
		return 0
	}
	ok, serr := st.SetFetchPlan(ctx, userID, scheduleID, plan)
	if serr != nil {
		slog.Error("agent: 抓取计划落库失败", "schedule_id", scheduleID, "err", serr)
		return 0
	}
	if !ok {
		slog.Warn("agent: 抓取计划落库未命中手册行（任务不存在/非本人/无手册行）", "schedule_id", scheduleID)
		return 0
	}
	return countPlanSources(plan)
}

const editTaskPlaybookSchema = `{
  "type": "object",
  "properties": {
    "schedule_id": {"type": "string", "description": "要修改手册的定时任务 id（先用 list_schedules 查询）"},
    "content": {"type": "string", "description": "手册全文（自然语言，整体替换现有手册）：描述这个情报任务要抓什么、关注哪些主题、偏好哪些来源。提交前先用 view_task_playbook 取现有内容并合并。最多约 4000 字，超出自动截断。"}
  },
  "required": ["schedule_id", "content"]
}`

type editTaskPlaybookArgs struct {
	ScheduleID string `json:"schedule_id"`
	Content    string `json:"content"`
}

type editTaskPlaybookTool struct {
	st playbookStore
	tr playbookTranslator // 改手册后据此重新编译 fetch_plan（P1 编译层；nil=跳过）
}

func (t *editTaskPlaybookTool) Name() string { return "edit_task_playbook" }
func (t *editTaskPlaybookTool) Description() string {
	return "修改某个定时推送任务的情报手册（整体替换现有内容）：用自然语言描述这个任务要抓什么、关注哪些主题、偏好哪些来源。提交前先用 view_task_playbook 查看现有内容并合并；省略会丢掉现有内容。schedule_id 可先用 list_schedules 查询。"
}
func (t *editTaskPlaybookTool) Parameters() json.RawMessage {
	return json.RawMessage(editTaskPlaybookSchema)
}
func (t *editTaskPlaybookTool) Mutating() bool { return true }

// Execute（确认后执行）：空 schedule_id / 空 content 是确定性可自纠失败，回文案不触库；
// content 超上限截断后落库；UpsertSchedulePlaybook 归属未命中（ok=false）回自纠文案不上抛
// （与 enable_source 越权处理一致），基础设施错误上抛。
func (t *editTaskPlaybookTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	var a editTaskPlaybookArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "参数不是合法 JSON，请修正后重试", nil
	}
	schedID := strings.TrimSpace(a.ScheduleID)
	if schedID == "" {
		return "schedule_id 必须是非空字符串（可先用 list_schedules 查询）", nil
	}
	if strings.TrimSpace(a.Content) == "" {
		return "手册内容不能为空。请描述这个任务要抓什么、关注哪些主题、偏好哪些来源。", nil
	}
	content := capPlaybookContent(a.Content)
	ok, err := t.st.UpsertSchedulePlaybook(ctx, userID, schedID, content)
	if err != nil {
		return "", err
	}
	if !ok {
		return fmt.Sprintf("没找到你的定时任务（id=%s），可能 id 有误或已删除。用 list_schedules 查一下。", schedID), nil
	}
	// 手册已更新 → 据此重新编译抓取计划（P1 编译层，best-effort）。回执带上编译出的源数，
	// 让用户/模型看见手册确实转成了可执行的抓取计划；编译失败/零源则回执不提计划，静默降级。
	if n := compilePlaybookPlan(ctx, t.st, t.tr, userID, schedID, content); n > 0 {
		return fmt.Sprintf("已更新定时任务（id=%s）的情报手册，并据此编译出 %d 个抓取源的计划。", schedID, n), nil
	}
	return fmt.Sprintf("已更新定时任务（id=%s）的情报手册。", schedID), nil
}

// Summarize 展示截断后的实际生效内容预览（卡面列超上限文本而只落截断值是对用户撒谎）。
func (t *editTaskPlaybookTool) Summarize(args json.RawMessage) string {
	var a editTaskPlaybookArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return summarizeFallback("修改任务手册", args)
	}
	preview := truncateRunes(strings.TrimSpace(capPlaybookContent(a.Content)), playbookSummaryPreviewRunes)
	return fmt.Sprintf("修改定时任务手册（id=%s）：新内容「%s」", strings.TrimSpace(a.ScheduleID), preview)
}
