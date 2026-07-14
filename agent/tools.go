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

// BuildTools 装配 agent 全部可用工具。返回的切片即工具白名单（契约 §10）：
// loop 只认这里注册的名字，模型编造的工具名一律拒绝。
func BuildTools(st *store.Store, sched *scheduler.Scheduler, pusher PushTrigger) []Tool {
	return []Tool{
		&listSourcesTool{st: st},
		&addSourceTool{st: st},
		&removeSourceTool{st: st},
		&listSchedulesTool{st: st},
		&createScheduleTool{sched: sched},
		&removeScheduleTool{sched: sched},
		&pushNowTool{pusher: pusher},
	}
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
		fmt.Fprintf(&b, "- id=%d [%s] %s（状态: %s）\n", src.ID, src.Type, title, src.Status)
	}
	return strings.TrimSuffix(b.String(), "\n"), nil
}

func (t *listSourcesTool) Summarize(json.RawMessage) string { return "" }

// ============================================================
// add_source：写工具，校验/构造复用 sourcespec（与 API 加订阅同一份规则）。
// ============================================================

// addSourceArgs 与工具 schema 对应；字段语义与 sourcespec.Spec 一致。
type addSourceArgs struct {
	Type     string `json:"type"`
	URL      string `json:"url"`
	Query    string `json:"query"`
	Keyword  string `json:"keyword"`
	Title    string `json:"title"`
	Category string `json:"category"`
}

// addSourceSchema 对齐契约 §8 与 M4 spike 验证过的形态：
// {type(enum rss/exa/tikhub_xhs), url, query, keyword, title?, category?}。
// url/query/keyword 的条件必填（随 type 而定）写进 description 由模型遵循，
// 权威校验在 sourcespec.Build（与 POST /api/subscriptions 完全一致）。
const addSourceSchema = `{
  "type": "object",
  "properties": {
    "type": {
      "type": "string",
      "enum": ["rss", "exa", "tikhub_xhs"],
      "description": "信源类型：rss=RSS/Atom 订阅源；exa=Exa 语义搜索；tikhub_xhs=小红书关键词"
    },
    "url": {"type": "string", "description": "RSS 源地址（http/https），type=rss 时必填"},
    "query": {"type": "string", "description": "Exa 搜索词，type=exa 时必填"},
    "keyword": {"type": "string", "description": "小红书搜索关键词，type=tikhub_xhs 时必填"},
    "title": {"type": "string", "description": "可选：展示名，缺省按类型自动生成"},
    "category": {"type": "string", "description": "可选：Exa 结果类别（如 news），仅 type=exa 生效"}
  },
  "required": ["type"]
}`

type addSourceTool struct {
	st *store.Store
}

func (t *addSourceTool) Name() string { return "add_source" }
func (t *addSourceTool) Description() string {
	return "添加一个信源并建立订阅。支持三种类型：rss（提供 url）、exa（提供 query 搜索词）、tikhub_xhs（提供 keyword 小红书关键词）。"
}
func (t *addSourceTool) Parameters() json.RawMessage { return json.RawMessage(addSourceSchema) }
func (t *addSourceTool) Mutating() bool              { return true }

func (t *addSourceTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	var a addSourceArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "参数不是合法 JSON，请修正后重试", nil
	}
	src, msg := sourcespec.Build(sourcespec.Spec{
		Type:     a.Type,
		URL:      a.URL,
		Query:    a.Query,
		Keyword:  a.Keyword,
		Title:    a.Title,
		Category: a.Category,
	})
	if msg != "" {
		return msg, nil
	}
	sourceID, err := t.st.UpsertSource(ctx, src)
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
	return fmt.Sprintf("已添加并订阅信源（id=%d）：[%s] %s", sourceID, src.Type, title), nil
}

func (t *addSourceTool) Summarize(args json.RawMessage) string {
	var a addSourceArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return summarizeFallback("添加信源", args)
	}
	var b strings.Builder
	switch types.SourceType(a.Type) {
	case types.SourceTypeRSS, "": // 缺省同 Build：向后兼容为 rss
		fmt.Fprintf(&b, "添加 RSS 信源：%s", a.URL)
	case types.SourceTypeExa:
		fmt.Fprintf(&b, "添加 Exa 搜索信源：搜索词「%s」", strings.TrimSpace(a.Query))
		if a.Category != "" {
			fmt.Fprintf(&b, "，类别「%s」", a.Category)
		}
	case types.SourceTypeTikHubXHS:
		fmt.Fprintf(&b, "添加小红书关键词信源：「%s」", strings.TrimSpace(a.Keyword))
	default:
		fmt.Fprintf(&b, "添加信源（未知类型 %s，确认后会被拒绝）", a.Type)
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
		if errors.Is(err, types.ErrValidation) {
			return err.Error(), nil
		}
		return "", err
	}
	return fmt.Sprintf("已触发一次立即推送（run_id=%s），推送卡片稍后送达飞书。", runID), nil
}

func (t *pushNowTool) Summarize(json.RawMessage) string { return "" }
