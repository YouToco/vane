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

// profileStore 是画像两工具依赖的窄接口（M5 契约 §12.3，*store.Store 已实现），
// 收窄后 Execute 分支可用内存假实现覆盖，不依赖数据库。
type profileStore interface {
	GetProfile(ctx context.Context, userID int64) (*types.Profile, error)
	UpsertProfileFields(ctx context.Context, userID int64, industry, occupation *string, tags []string) (*types.Profile, error)
}

// BuildTools 装配 agent 全部可用工具。返回的切片即工具白名单的静态部分（契约 §10）：
// loop 只认这里注册的名字 + 会话已激活的 TikHub 端点（端点注册表契约 §4），
// 模型编造的其余工具名一律拒绝。
// endpoints 为 nil（TikHub key 未配置）时不装配 search_endpoints，工具面与
// 该特性上线前完全一致。
func BuildTools(st *store.Store, sched *scheduler.Scheduler, pusher PushTrigger, endpoints *EndpointTools) []Tool {
	tools := []Tool{
		&listSourcesTool{st: st},
		&addSourceTool{st: st},
		&removeSourceTool{st: st},
		&listSchedulesTool{st: st},
		&createScheduleTool{sched: sched},
		&removeScheduleTool{sched: sched},
		&pushNowTool{pusher: pusher},
		&viewProfileTool{st: st},
		&updateProfileTool{st: st},
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

// addSourceArgs 与工具 schema 对应。优先用 Platform+Capability（M6 新），
// 若 Type 非空则走 BuildLegacy 兼容旧前端。
type addSourceArgs struct {
	Platform   string   `json:"platform"`
	Capability string   `json:"capability"`
	Type       string   `json:"type"` // 旧字段，兼容 M6 前调用方（走 BuildLegacy）
	URL        string   `json:"url"`
	Query      string   `json:"query"`
	Keyword    string   `json:"keyword"`
	ScreenName string   `json:"screen_name"`
	UserID     string   `json:"user_id"`     // xhs/user_posts：小红书用户 ID
	ProfileURL string   `json:"profile_url"` // xhs/user_posts：小红书用户主页链接（可替代 user_id）
	Title      string   `json:"title"`
	Category   string   `json:"category"`
	Categories []string `json:"categories"`
}

// addSourceSchema 对齐 M6 信源插件化契约：优先 platform + capability，
// 同时保留旧 type 字段走 BuildLegacy 兼容窗口。
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
      "enum": ["feed", "search", "user_posts"],
      "description": "能力：feed=RSS/Atom 订阅（仅 web）；search=关键词/语义搜索（web=Exa 网页搜索，xhs=小红书关键词）；user_posts=订阅某账号的新发布（x=Twitter 账号，xhs=小红书博主）。当前不支持的能力及原因见本工具说明（Description）。"
    },
    "type": {
      "type": "string",
      "enum": ["rss", "exa", "tikhub_xhs"],
      "description": "（兼容旧版）信源类型：rss→web/feed；exa→web/search；tikhub_xhs→xhs/search。优先用 platform+capability"
    },
    "url": {"type": "string", "description": "RSS 源地址（http/https），platform=web capability=feed 时必填"},
    "query": {"type": "string", "description": "Exa 搜索词，platform=web capability=search 时必填"},
    "keyword": {"type": "string", "description": "小红书搜索关键词，platform=xhs capability=search 时必填"},
    "screen_name": {"type": "string", "description": "X 用户名（如 OpenAI），platform=x capability=user_posts 时必填"},
    "user_id": {"type": "string", "description": "小红书用户 ID（24 位十六进制），platform=xhs capability=user_posts 时必填（或改用 profile_url）"},
    "profile_url": {"type": "string", "description": "小红书用户主页链接（如 https://www.xiaohongshu.com/user/profile/<id>），platform=xhs capability=user_posts 时可替代 user_id"},
    "title": {"type": "string", "description": "可选：展示名，缺省按类型自动生成"},
    "category": {"type": "string", "description": "可选：Exa 结果类别（如 news），仅 web/search 生效"},
    "categories": {"type": "array", "items": {"type": "string"}, "description": "可选：RSS 分类过滤（如 [\"Product\",\"Research\"]），仅 web/feed 生效；不传=不限"}
  }
}`

type addSourceTool struct {
	st *store.Store
}

func (t *addSourceTool) Name() string { return "add_source" }
func (t *addSourceTool) Description() string {
	return "添加一个信源并建立订阅。指定 platform（web/xhs/x）和 capability（feed/search/user_posts），或传旧版 type 字段兼容。" +
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

func (t *addSourceTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	var a addSourceArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "参数不是合法 JSON，请修正后重试", nil
	}
	var src *types.Source
	var msg string
	if a.Platform != "" {
		params := make(map[string]string)
		if a.URL != "" {
			params["url"] = a.URL
		}
		if a.Query != "" {
			params["query"] = a.Query
		}
		if a.Keyword != "" {
			params["keyword"] = a.Keyword
		}
		if a.Category != "" {
			params["category"] = a.Category
		}
		if a.ScreenName != "" {
			params["screen_name"] = a.ScreenName
		}
		if a.UserID != "" {
			params["user_id"] = a.UserID
		}
		if a.ProfileURL != "" {
			params["profile_url"] = a.ProfileURL
		}
		if len(a.Categories) > 0 {
			catJSON, _ := json.Marshal(a.Categories)
			params["categories"] = string(catJSON)
		}
		src, msg = sourcespec.Build(sourcespec.Spec{
			Platform:   a.Platform,
			Capability: a.Capability,
			Params:     params,
			Title:      a.Title,
		})
	} else {
		src, msg = sourcespec.BuildLegacy(a.Type, a.URL, a.Query, a.Keyword, a.Title, a.Category)
	}
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
	if p == "" {
		switch types.SourceType(a.Type) {
		case types.SourceTypeRSS, "":
			p, c = "web", "feed"
		case types.SourceTypeExa:
			p, c = "web", "search"
		case types.SourceTypeTikHubXHS:
			p, c = "xhs", "search"
		}
	}
	switch p + "/" + c {
	case "web/feed":
		fmt.Fprintf(&b, "添加 RSS 信源：%s", a.URL)
	case "web/search":
		fmt.Fprintf(&b, "添加搜索信源：搜索词「%s」", strings.TrimSpace(a.Query))
		if a.Category != "" {
			fmt.Fprintf(&b, "，类别「%s」", a.Category)
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
		return fmt.Sprintf("每 %d 秒触发一次（时区 %s）", spec.EverySeconds, tz)
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
		TZ           string `json:"tz"`
	} `json:"spec"`
	NLDescription string `json:"nl_description"`
}

type createScheduleTool struct {
	sched *scheduler.Scheduler
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
		TZ:           a.Spec.TZ,
	})
	if desc := strings.TrimSpace(a.NLDescription); desc != "" {
		s += fmt.Sprintf("，描述「%s」", desc)
	}
	return s
}

// validateScheduleArgs 前置校验，与 api/schedules.go 的 toScheduleSpec 逐条对齐：
// cron 与 every_seconds 恰好提供其一；every_seconds 不低于 1h 地板。文案同 api。
func validateScheduleArgs(a createScheduleArgs) string {
	hasCron := a.Spec.Cron != ""
	hasEvery := a.Spec.EverySeconds > 0
	if hasCron == hasEvery {
		return "spec 必须且只能提供 cron 或 every_seconds 之一"
	}
	if hasEvery && a.Spec.EverySeconds < minEverySeconds {
		return "推送间隔不得小于 1 小时"
	}
	return ""
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
	sched *scheduler.Scheduler
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
	if err := t.sched.DeletePush(ctx, schedID); err != nil {
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
