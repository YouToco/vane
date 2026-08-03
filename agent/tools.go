// agent 工具集（M4 契约 §8）：七个工具 + BuildTools 装配。
// Tool 接口本身在 loop.go（契约 §7）定义，本文件只实现具体工具类型。
//
// 错误分层约定（贯穿全部 Execute）：
//   - 模型可自纠的确定性失败（参数不是合法 JSON、字段校验不过）→ 返回中文文案 + nil error，
//     loop 会把文案作为 role=tool 结果回给模型继续多轮；
//   - 基础设施失败（DB / Temporal）→ 返回 error 向上抛，由 feishu 层 humanize。
package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"

	"github.com/YouToco/vane/acquisitiontool"
	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

// TaskRunTrigger is the narrow task-scoped run-now control plane. The Agent
// resolves human descriptions through list_schedules and never exposes the
// internal id to the user. Every execution carries the Agent turn plus the
// provider tool-call id into a durable schedule-command idempotency key.
// Provider call ids are only unique inside one model response and may repeat
// across user turns.
type TaskRunTrigger interface {
	TriggerScheduleNowIdempotent(
		ctx context.Context,
		scheduleID string,
		userID int64,
		idempotencyKey string,
	) error
}

type toolInvocationIDKey struct{}
type providerToolCallIDKey struct{}

func withToolInvocationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, toolInvocationIDKey{}, id)
}

func toolInvocationIDFrom(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(toolInvocationIDKey{}).(string)
	return id, ok && strings.TrimSpace(id) != ""
}

func withProviderToolCallID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, providerToolCallIDKey{}, id)
}

func providerToolCallIDFrom(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(providerToolCallIDKey{}).(string)
	return id, ok && strings.TrimSpace(id) == id && id != ""
}

func scopedToolInvocationID(ctx context.Context, providerCallID string) string {
	providerCallID = strings.TrimSpace(providerCallID)
	meta, ok := ctx.Value(chatMetaKey{}).(chatMeta)
	if !ok || strings.TrimSpace(meta.traceID) == "" {
		return providerCallID
	}
	return strings.TrimSpace(meta.traceID) + "\x00" + providerCallID
}

type scheduleDeleter interface {
	DeletePush(ctx context.Context, schedID string, userID int64) error
}

type idempotentScheduleDeleter interface {
	DeletePushIdempotent(
		ctx context.Context,
		schedID string,
		userID int64,
		idempotencyKey string,
	) error
}

// profileStore 是画像两工具依赖的窄接口（M5 契约 §12.3，*store.Store 已实现），
// 收窄后 Execute 分支可用内存假实现覆盖，不依赖数据库。
type profileStore interface {
	GetProfile(ctx context.Context, userID int64) (*types.Profile, error)
	UpsertProfileFields(ctx context.Context, userID int64, industry, occupation *string, tags []string) (*types.Profile, error)
}

type scheduleListStore interface {
	ListSchedulesByUser(ctx context.Context, userID int64) ([]types.Schedule, error)
}

// playbookStore is deliberately read-only. The task manual is the user-facing
// task definition; internal execution calls are inspected through run
// provenance, not exposed as a second editable product object.
type playbookStore interface {
	GetSchedulePlaybook(ctx context.Context, userID int64, scheduleID string) (*types.SchedulePlaybook, error)
}

type taskRunEvidenceStore interface {
	GetLatestTaskRunEvidenceV1(
		ctx context.Context,
		userID int64,
		taskID string,
	) (*store.TaskLatestRunEvidenceV1, error)
}

type intelligenceQueryStore interface {
	QueryMyIntelligence(
		ctx context.Context,
		scope store.IntelligenceScope,
		query store.IntelligenceQuery,
	) (*store.IntelligenceQueryResult, error)
}

// BuildTools 装配 agent 全部可用工具。返回的切片即工具白名单的静态部分（契约 §10）：
// loop 只认这里注册的名字 + 会话已激活的 TikHub 端点（端点注册表契约 §4），
// 模型编造的其余工具名一律拒绝。
// endpoints 为 nil（TikHub key 未配置）时不装配 search_endpoints，工具面与
// 该特性上线前完全一致。
// exa 是 Exa ad-hoc 工具对（web_search/read_page）；nil（Exa key 未配置）时不装配，
// 工具面与该特性上线前完全一致（同 endpoints 的 nil 语义）。
func BuildTools(st *store.Store, sched *scheduler.Scheduler, runner TaskRunTrigger, endpoints *EndpointTools, exa *ExaTools, definitionEdits ...DefinitionEditController) []ToolSpec {
	tools := []ToolSpec{
		newToolSpec(&listSchedulesTool{st: st}, withToolSurface(
			a2aReadPolicy(Effects(EffectInternalRead)),
			ExposureIntent, IntentTasks, ResultTrustLocal, false)),
		newToolSpec(&createScheduleTool{}, withToolSurface(ownerPolicy(
			Effects(EffectDurableProposal, EffectStateWrite, EffectDirectOwnerWrite),
			BudgetNone),
			ExposureIntent, IntentTasks, ResultTrustLocal, true)),
		newToolSpec(&removeScheduleTool{sched: sched}, withToolSurface(ownerPolicy(
			Effects(EffectStateWrite, EffectDirectOwnerWrite),
			BudgetNone),
			ExposureIntent, IntentTasks, ResultTrustLocal, true)),
		newToolSpec(&runTaskNowTool{runner: runner}, withToolSurface(ownerPolicy(
			Effects(EffectDelivery), BudgetDownstreamManaged),
			ExposureIntent, IntentTasks, ResultTrustLocal, true)),
		newToolSpec(&viewProfileTool{st: st}, withToolSurface(ownerPolicy(
			Effects(EffectInternalRead), BudgetNone),
			ExposureIntent, IntentProfile, ResultTrustLocal, false)),
		newToolSpec(&updateProfileTool{st: st}, withToolSurface(ownerPolicy(
			Effects(EffectStateWrite, EffectDirectOwnerWrite), BudgetNone),
			ExposureIntent, IntentProfile, ResultTrustLocal, true)),
		newToolSpec(&viewTaskPlaybookTool{st: st}, withToolSurface(ownerPolicy(
			Effects(EffectInternalRead), BudgetNone),
			ExposureIntent, IntentTasks, ResultTrustLocal, false)),
		newToolSpec(&viewTaskLatestRunTool{st: st}, withToolSurface(ownerPolicy(
			Effects(EffectInternalRead), BudgetNone),
			ExposureIntent, IntentTasks, ResultTrustLocal, false)),
	}
	if len(definitionEdits) == 1 && definitionEdits[0] != nil {
		tools = append(tools, newToolSpec(&editTaskDefinitionTool{}, withToolSurface(ownerPolicy(
			Effects(EffectDurableProposal, EffectStateWrite, EffectDirectOwnerWrite),
			BudgetNone),
			ExposureIntent, IntentTasks, ResultTrustLocal, true)))
	}
	if endpoints != nil {
		tools = append(tools, endpoints.SearchTool(), endpoints.ReadResultTool())
	}
	if exa != nil {
		tools = append(tools, exa.SearchTool(), exa.ReadPageTool())
	}
	return tools
}

func NewQueryMyIntelligenceTool(st intelligenceQueryStore) ToolSpec {
	return newToolSpec(&queryMyIntelligenceTool{st: st}, withToolSurface(
		ownerPolicy(Effects(EffectInternalRead), BudgetNone),
		ExposureAlways, knownToolIntents, ResultTrustLocal, false))
}

const queryMyIntelligenceSchema = `{
  "type": "object",
  "properties": {
    "dataset": {
      "type": "string",
      "enum": ["tasks","runs","observations","briefs","agent_turns","tool_calls","profile"],
      "description": "一次只查询一个用户情报数据集"
    },
    "select": {
      "type": "array",
      "maxItems": 32,
      "items": {"type":"string"},
      "description": "要返回的目录字段；省略使用该数据集的专业默认列"
    },
    "filters": {
      "type": "array",
      "maxItems": 16,
      "items": {
        "type": "object",
        "properties": {
          "field": {"type":"string"},
          "op": {"type":"string","enum":["eq","neq","gt","gte","lt","lte","contains","in","within"]},
          "value": {"description":"与字段类型匹配的 JSON 值；within 使用 today、yesterday 或 last_7_days"}
        },
        "required": ["field","op","value"],
        "additionalProperties": false
      }
    },
    "group_by": {"type":"array","maxItems":8,"items":{"type":"string"}},
    "metrics": {
      "type":"array","maxItems":8,
      "items": {
        "type":"object",
        "properties": {
          "function":{"type":"string","enum":["count","sum","avg","min","max"]},
          "field":{"type":"string"},
          "as":{"type":"string"}
        },
        "required":["function","as"],
        "additionalProperties":false
      }
    },
    "order_by": {
      "type":"array","maxItems":8,
      "items": {
        "type":"object",
        "properties": {
          "field":{"type":"string"},
          "direction":{"type":"string","enum":["asc","desc"]}
        },
        "required":["field"],
        "additionalProperties":false
      }
    },
    "limit":{"type":"integer","minimum":1,"maximum":100},
    "cursor":{"type":"string","description":"上一页返回的 next_cursor；只能原样继续同一查询"}
  },
  "required":["dataset"],
  "additionalProperties":false
}`

type queryMyIntelligenceTool struct {
	st intelligenceQueryStore
}

func (*queryMyIntelligenceTool) Name() string { return "query_my_intelligence" }
func (*queryMyIntelligenceTool) Description() string {
	return "查询当前用户自己的任务、历史运行、Observation、Brief、历史 Agent 回答、模型实际看到的工具证据或画像。按名称、主题、用途和自然时间定位，不要求用户提供内部 ID；跨数据集问题连续调用本工具后再综合。"
}
func (*queryMyIntelligenceTool) Parameters() json.RawMessage {
	return json.RawMessage(queryMyIntelligenceSchema)
}
func (t *queryMyIntelligenceTool) Execute(
	ctx context.Context,
	userID int64,
	raw json.RawMessage,
) (string, error) {
	if t.st == nil {
		return "", types.NewAppError(types.CodeInternal,
			"用户情报查询未装配", nil)
	}
	var query store.IntelligenceQuery
	if err := strictjson.DecodeExact(raw, &query); err != nil {
		return "query_my_intelligence 参数不是合法关系查询，或包含未知字段", nil
	}
	meta, ok := ctx.Value(chatMetaKey{}).(chatMeta)
	if !ok || meta.scope.TenantID <= 0 || meta.scope.UserID != userID ||
		meta.scope.SessionID <= 0 {
		return "", types.NewAppError(types.CodeValidation,
			"用户情报查询缺少认证会话范围", types.ErrValidation)
	}
	sessionID := meta.scope.SessionID
	storeQuery, projectionErr := prepareIntelligenceToolCallQuery(query)
	if projectionErr != nil {
		return "query_my_intelligence 查询被拒绝：" + projectionErr.Error(), nil
	}
	storeQuery, projectionErr = prepareIntelligenceObservationQuery(storeQuery)
	if projectionErr != nil {
		return "query_my_intelligence 查询被拒绝：" + projectionErr.Error(), nil
	}
	result, err := t.st.QueryMyIntelligence(ctx, store.IntelligenceScope{
		TenantID: meta.scope.TenantID, UserID: userID, SessionID: &sessionID,
	}, storeQuery)
	if err != nil {
		if errors.Is(err, types.ErrValidation) {
			return "query_my_intelligence 查询被拒绝：" + err.Error(), nil
		}
		return "", err
	}
	if err := projectIntelligenceResultForAgent(ctx, result); err != nil {
		return "", err
	}
	if err := projectObservationResultForAgent(ctx, result); err != nil {
		return "", err
	}
	rememberIntelligenceResultReferences(ctx, result)
	encoded, err := json.Marshal(result)
	if err != nil {
		return "", types.NewAppError(types.CodeInternal,
			"编码用户情报查询结果", err)
	}
	return string(encoded), nil
}
func (*queryMyIntelligenceTool) Summarize(args json.RawMessage) string {
	var query store.IntelligenceQuery
	if err := json.Unmarshal(args, &query); err != nil {
		return summarizeFallback("查询我的历史情报", args)
	}
	return "查询我的 " + string(query.Dataset) + " 情报"
}

// edit_task_definition is a schema-only model tool. Loop delegates it directly
// to the durable definition-edit controller.
const editTaskDefinitionSchema = `{
  "type": "object",
  "properties": {
    "task_id": {
      "type": "string",
      "description": "要编辑的定时任务 id（先用 list_schedules 查询）"
    },
    "spec": {
      "type": "object",
      "description": "可选：替换触发频率；cron 与 every_seconds 必须且只能提供一个",
      "properties": {
        "cron": {"type": "string", "description": "5 段 cron，如 \"30 8 * * *\""},
        "every_seconds": {"type": "integer", "description": "固定间隔秒数，不低于 3600"},
        "anchor_at": {"type": "string", "description": "可选 RFC3339 锚点，只与 every_seconds 搭配"},
        "tz": {"type": "string", "description": "可选 IANA 时区，缺省 Asia/Shanghai"}
      },
      "additionalProperties": false
    },
    "intent": {
      "type": "string",
      "minLength": 1,
      "description": "可选：完整替换持续监控意图与任务手册；必须自包含，不能只写增量片段"
    },
    "nl_description": {
      "type": "string",
      "description": "可选：替换任务列表中的自然语言描述；空串表示清空"
    },
    "strictness": {
      "type": "string",
      "enum": ["loose", "normal", "strict"],
      "description": "可选：替换推送门槛"
    },
    "observation_policy": {
      "type": "object",
      "description": "可选：完整替换新鲜度/事件判定策略；修改成功时起生效，不补推更早内容",
      "properties": {
        "schema": {"type":"string","enum":["vane.observation-policy/v1"]},
        "mode": {"type":"string","enum":["content","event"]},
        "window": {
          "type":"object",
          "properties":{
            "kind":{"type":"string","enum":["schedule_interval","rolling_duration","calendar_period"]},
            "rolling_duration_seconds":{"type":"integer","minimum":3600,"maximum":31622400},
            "calendar_period":{"type":"string","enum":["day","week","month"]}
          },
          "required":["kind"],
          "additionalProperties":false
        },
        "late_policy":{"type":"string","enum":["strict","bounded"]},
        "allowed_lateness_seconds":{"type":"integer","minimum":1,"maximum":2592000},
        "evidence":{
          "type":"object",
          "properties":{
            "requirement":{"type":"string","enum":["official_required","trusted_allowed"]},
            "official_domains":{"type":"array","uniqueItems":true,"items":{"type":"string"}}
          },
          "required":["requirement"],
          "additionalProperties":false
        },
        "unknown_time":{"type":"string","enum":["reject","deprioritize","allow"]},
        "event":{
          "type":"object",
          "properties":{
            "subject":{"type":"string","minLength":1},
            "event_kind":{"type":"string","minLength":1},
            "qualification":{"type":"string","enum":["official_announcement","general_availability","either"]}
          },
          "required":["subject","event_kind","qualification"],
          "additionalProperties":false
        },
        "qualifier_prompt":{"type":"string","enum":["vane.qualify-events/v1"]}
      },
      "required":["schema","mode","window","late_policy","evidence","unknown_time"],
      "additionalProperties":false
    }
  },
  "required": ["task_id"],
  "additionalProperties": false
}`

type editTaskDefinitionTool struct{}

func (*editTaskDefinitionTool) Name() string { return "edit_task_definition" }
func (*editTaskDefinitionTool) Description() string {
	return "直接编辑已有定时任务的完整定义。可一次修改触发频率、完整监控意图/手册、列表描述、推送门槛和新鲜度/事件判定策略；未提供的字段保持不变。系统会冻结当前 definition head 与目标定义，再立即交给唯一可恢复协调器执行。"
}
func (*editTaskDefinitionTool) Parameters() json.RawMessage {
	return json.RawMessage(editTaskDefinitionSchema)
}
func (*editTaskDefinitionTool) Execute(
	context.Context,
	int64,
	json.RawMessage,
) (string, error) {
	return "任务编辑必须通过当前可恢复执行器处理，请重新发起编辑。", nil
}
func (*editTaskDefinitionTool) Summarize(args json.RawMessage) string {
	return summarizeFallback("编辑定时推送任务", args)
}

// emptyParamsSchema 是无参工具的 JSON schema：仍须是合法 object schema，
// DeepSeek FC 对空 properties 已实测可用（M4 spike）。
const emptyParamsSchema = `{"type":"object","properties":{}}`

// summarizeFallback 在摘要解析 args 失败时展示截断后的原始参数。
func summarizeFallback(action string, args json.RawMessage) string {
	raw := []rune(string(args))
	if len(raw) > 200 {
		raw = append(raw[:200], '…')
	}
	return action + "（参数未能解析）：" + string(raw)
}

// Task inventory is the only persistent user-facing collection.

type listSchedulesTool struct {
	st scheduleListStore
}

const listSchedulesSchema = `{
  "type": "object",
  "properties": {
    "query": {
      "type": "string",
      "description": "可选：按用户记得的连续原话筛选任务名称、描述或频率；省略时列出全部任务"
    },
    "status": {
      "type": "string",
      "enum": ["active", "paused", "all"],
      "description": "可选：用户问正在运行/生效中的任务时传 active，问暂停任务时传 paused，明确问所有状态时传 all；省略等同 all"
    }
  },
  "additionalProperties": false
}`

func (t *listSchedulesTool) Name() string { return "list_schedules" }
func (t *listSchedulesTool) Description() string {
	return "列出用户当前的定时推送任务（含 id、触发频率、状态、描述）。可按用户原话和 active/paused 状态筛选；用户只问正在运行的任务时必须传 status=active。无唯一结果时按可读名称追问，不能要求用户提供 id。"
}
func (t *listSchedulesTool) Parameters() json.RawMessage {
	return json.RawMessage(listSchedulesSchema)
}

func (t *listSchedulesTool) Execute(ctx context.Context, userID int64, raw json.RawMessage) (string, error) {
	var args struct {
		Query  string `json:"query,omitempty"`
		Status string `json:"status,omitempty"`
	}
	if err := strictjson.DecodeExact(raw, &args); err != nil {
		return "list_schedules 参数不是合法 JSON，或包含未知字段", nil
	}
	list, err := t.st.ListSchedulesByUser(ctx, userID)
	if err != nil {
		return "", err
	}
	status := strings.TrimSpace(strings.ToLower(args.Status))
	switch status {
	case "", "all":
	case "active", "paused":
		filtered := make([]types.Schedule, 0, len(list))
		for _, sc := range list {
			if string(sc.Status) == status {
				filtered = append(filtered, sc)
			}
		}
		list = filtered
	default:
		return "status 只能是 active、paused 或 all", nil
	}
	query := normalizeScheduleLookupText(args.Query)
	if query != "" {
		list = filterSchedulesByQuery(list, query)
	}
	if len(list) == 0 {
		if query != "" {
			return "没有找到与该描述匹配的定时推送任务。", nil
		}
		if status == "active" {
			return "当前没有正在运行的定时推送任务。", nil
		}
		if status == "paused" {
			return "当前没有已暂停的定时推送任务。", nil
		}
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

func normalizeScheduleLookupText(value string) string {
	var out strings.Builder
	for _, r := range strings.ToLower(value) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			out.WriteRune(r)
		}
	}
	return out.String()
}

func filterSchedulesByQuery(
	list []types.Schedule,
	normalizedQuery string,
) []types.Schedule {
	filtered := make([]types.Schedule, 0, len(list))
	for _, sc := range list {
		haystack := normalizeScheduleLookupText(
			sc.NLDescription + " " + formatSpecJSON(sc.SpecJSON),
		)
		if scheduleLookupMatches(haystack, normalizedQuery) {
			filtered = append(filtered, sc)
		}
	}
	return filtered
}

func scheduleLookupMatches(haystack, query string) bool {
	if strings.Contains(haystack, query) {
		return true
	}
	// Users commonly remember a product/person name plus wording that differs
	// from the saved description (for example “Kimi 套餐” versus “Kimi 会员定价”).
	// A distinctive Latin/digit token is a useful fallback and remains much
	// narrower than fuzzy matching the whole Chinese sentence.
	for _, token := range scheduleLookupLatinTokens(query) {
		if len([]rune(token)) >= 3 && strings.Contains(haystack, token) {
			return true
		}
	}
	return false
}

func scheduleLookupLatinTokens(value string) []string {
	var tokens []string
	var current strings.Builder
	flush := func() {
		if current.Len() > 0 {
			tokens = append(tokens, current.String())
			current.Reset()
		}
	}
	for _, r := range strings.ToLower(value) {
		if r <= unicode.MaxASCII && (unicode.IsLetter(r) || unicode.IsDigit(r)) {
			current.WriteRune(r)
		} else {
			flush()
		}
	}
	flush()
	return tokens
}

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

const createScheduleSchemaTemplate = `{
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
    "intent": {"type": "string", "minLength": 1, "description": "用户已经明确表达的持续监控目标与筛选范围。必须完整、自包含；它会成为任务手册，冻结后不得由系统自行扩大主题或范围。"},
    "tool_calls": {
      "type": "array",
      "minItems": 1,
      "maxItems": 64,
      "description": "根据任务手册选择的长期取材 Tool 调用。每项直接填写 Tool 名称和参数；不要构造信源、计划、内部 URL 或内部 id。",
      "items": __ACQUISITION_TOOL_CALL_SCHEMA_V1__
    },
    "observation_policy": {
      "type": "object",
      "description": "可选：任务的新鲜度/事件判定策略。用户说“上新才推”“没有就不推”“只看今天/最近N天”时必须填写。模型从用户明确需求中提取；存在实质歧义时先自然追问，运行时代码拥有最终放行权。",
      "properties": {
        "schema": {"type":"string","enum":["vane.observation-policy/v1"]},
        "mode": {"type":"string","enum":["content","event"],"description":"content=时间范围内的普通内容；event=只有满足证据与资格条件的事件发生才推"},
        "window": {
          "type":"object",
          "properties":{
            "kind":{"type":"string","enum":["schedule_interval","rolling_duration","calendar_period"]},
            "rolling_duration_seconds":{"type":"integer","minimum":3600,"maximum":31622400},
            "calendar_period":{"type":"string","enum":["day","week","month"]}
          },
          "required":["kind"],
          "additionalProperties":false
        },
        "late_policy":{"type":"string","enum":["strict","bounded"]},
        "allowed_lateness_seconds":{"type":"integer","minimum":1,"maximum":2592000},
        "evidence":{
          "type":"object",
          "properties":{
            "requirement":{"type":"string","enum":["official_required","trusted_allowed"]},
            "official_domains":{"type":"array","uniqueItems":true,"items":{"type":"string"}}
          },
          "required":["requirement"],
          "additionalProperties":false
        },
        "unknown_time":{"type":"string","enum":["reject","deprioritize","allow"]},
        "event":{
          "type":"object",
          "properties":{
            "subject":{"type":"string","minLength":1},
            "event_kind":{"type":"string","minLength":1},
            "qualification":{"type":"string","enum":["official_announcement","general_availability","either"]}
          },
          "required":["subject","event_kind","qualification"],
          "additionalProperties":false
        },
        "qualifier_prompt":{"type":"string","enum":["vane.qualify-events/v1"]}
      },
      "required":["schema","mode","window","late_policy","evidence","unknown_time"],
      "additionalProperties":false
    },
    "nl_description": {"type": "string", "description": "可选：该任务的自然语言描述（如\"每天早上 8 点推送\"），用于列表展示"},
    "strictness": {"type": "string", "enum": ["loose", "normal", "strict"], "description": "可选：推送门槛档位，从用户对相关度的要求推断——「只要非常相关的/重大更新才推」→ strict（仅 ≥60 分高相关才推）；「一般相关就行」→ normal（≥40 分）；「都推来看看/宽松点」→ loose（只过滤与画像无关的内容）。用户没表态就不传（按系统兜底，等价 loose）"}
  },
  "required": ["spec", "intent", "tool_calls"],
  "additionalProperties": false
}`

var createScheduleSchema = buildCreateScheduleSchema()

func buildCreateScheduleSchema() string {
	toolCallSchema, err := acquisitiontool.ToolCallSchemaV1()
	if err != nil {
		panic(err)
	}
	schema := strings.Replace(
		createScheduleSchemaTemplate,
		"__ACQUISITION_TOOL_CALL_SCHEMA_V1__",
		string(toolCallSchema),
		1,
	)
	if !json.Valid([]byte(schema)) {
		panic("agent: generated create_schedule schema is invalid")
	}
	return schema
}

// createScheduleArgs 与工具 schema 对应；spec 结构与 api 的 scheduleSpecDTO 一致。
type createScheduleArgs struct {
	Spec struct {
		Cron         string `json:"cron"`
		EverySeconds int    `json:"every_seconds"`
		AnchorAt     string `json:"anchor_at"`
		TZ           string `json:"tz"`
	} `json:"spec"`
	Intent            string                    `json:"intent"`
	ToolCalls         []json.RawMessage         `json:"tool_calls"`
	ObservationPolicy *observation.PolicySpecV1 `json:"observation_policy"`
	NLDescription     string                    `json:"nl_description"`
	Strictness        string                    `json:"strictness"`
}

type createScheduleTool struct{}

func (t *createScheduleTool) Name() string { return "create_schedule" }
func (t *createScheduleTool) Description() string {
	return "直接创建定时情报任务。提交任务手册，并从可用取材 Tool 中选择调用；系统冻结 Tool 调用后立即推进可恢复的创建流程。不要构造信源、抓取计划、内部 URL 或内部 id。触发频率用 cron 或 every_seconds 二选一，频率不得高于每小时一次。"
}
func (t *createScheduleTool) Parameters() json.RawMessage {
	return json.RawMessage(createScheduleSchema)
}

func (t *createScheduleTool) Execute(
	context.Context,
	int64,
	json.RawMessage,
) (string, error) {
	return "任务创建必须通过当前可恢复执行器处理，请重新发起创建。", nil
}

// strictnessLabel / strictnessDesc 档位的人话名与一句话说明（工具回执/摘要共用）。
func strictnessLabel(v types.PushStrictness) string {
	switch v {
	case types.StrictnessStrict:
		return "严格"
	case types.StrictnessNormal:
		return "标准"
	default:
		return "宽松"
	}
}

func strictnessDesc(v types.PushStrictness) string {
	switch v {
	case types.StrictnessStrict:
		return fmt.Sprintf("仅 ≥%d 分的高相关内容才推送", v.MinKeepScore())
	case types.StrictnessNormal:
		return fmt.Sprintf("≥%d 分才推送，弱相关不打扰", v.MinKeepScore())
	default:
		return "只过滤与你画像无关的内容"
	}
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
	if intent := strings.TrimSpace(a.Intent); intent != "" {
		s += "\n监控意图：" + intent
	}
	strictness := types.PushStrictness(a.Strictness)
	if strictness == "" {
		strictness = types.StrictnessLoose
	}
	s += fmt.Sprintf("\n推送门槛：%s（%s）", strictnessLabel(strictness), strictnessDesc(strictness))
	if policy := a.ObservationPolicy; policy != nil {
		s += "\n新鲜度策略：" + summarizeObservationPolicy(*policy)
	}
	return s
}

func summarizeObservationPolicy(policy observation.PolicySpecV1) string {
	var window string
	switch policy.Window.Kind {
	case observation.WindowScheduleInterval:
		window = "相邻两次计划触发之间"
	case observation.WindowRollingDuration:
		window = fmt.Sprintf("最近 %d 小时",
			policy.Window.RollingDurationSeconds/3600)
	case observation.WindowCalendarPeriod:
		window = map[observation.CalendarPeriod]string{
			observation.CalendarDay:   "本日",
			observation.CalendarWeek:  "本周",
			observation.CalendarMonth: "本月",
		}[policy.Window.CalendarPeriod]
	}
	late := "窗口外不补推"
	if policy.LatePolicy == observation.LateBounded {
		late = fmt.Sprintf("允许迟到 %d 秒", policy.AllowedLatenessSecs)
	}
	unknown := map[observation.UnknownTimePolicy]string{
		observation.UnknownTimeReject:       "日期未知拒绝",
		observation.UnknownTimeDeprioritize: "日期未知降权",
		observation.UnknownTimeAllow:        "日期未知允许",
	}[policy.UnknownTime]
	evidence := "允许可信媒体证据"
	if policy.Evidence.Requirement == observation.EvidenceOfficialRequired {
		evidence = "仅官方证据：" + strings.Join(
			policy.Evidence.OfficialDomains, "、")
	}
	if policy.Mode != observation.ModeEvent || policy.Event == nil {
		return "普通内容模式；窗口 " + window + "；" + late + "；" +
			unknown + "；" + evidence
	}
	qualification := map[observation.Qualification]string{
		observation.QualificationAnnouncement:        "官方宣布即算",
		observation.QualificationGeneralAvailability: "正式可用才算",
		observation.QualificationEither:              "官方宣布或正式可用均算",
	}[policy.Event.Qualification]
	return fmt.Sprintf(
		"仅事件发生时推送；%s（%s）；窗口 %s；%s；%s；%s；无匹配事件不发消息",
		policy.Event.Subject, qualification, window, late, unknown, evidence)
}

// validateScheduleSpecFields 是 create_schedule 的 spec 前置校验，
// 与 api/schedules.go 的 toScheduleSpec 逐条对齐：cron 与 every_seconds 恰好提供其一；
// every_seconds 不低于 1h 地板。文案同 api。
//
// cron 频率的权威校验仍在 scheduler.validateCronMinInterval；这里只做
// 能尽早给出清晰文案的结构与地板校验。
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
	if message := validateScheduleSpecFields(a.Spec.Cron, a.Spec.EverySeconds); message != "" {
		return message
	}
	if a.ObservationPolicy != nil {
		if err := a.ObservationPolicy.Validate(); err != nil {
			return "observation_policy 无效：" + err.Error()
		}
	}
	return ""
}

const removeScheduleSchema = `{
  "type": "object",
  "properties": {
    "schedule_ids": {
      "type": "array",
      "items": {"type": "string"},
      "minItems": 1,
      "maxItems": 20,
      "description": "内部任务 id 列表，一次最多 20 个。用户不需要知道 id：根据用户记得的任务描述、时间、主题等先用 list_schedules 定位；每个名称唯一匹配后合并到同一次调用，只有某个名称存在多个合理候选时才按人类可读名称追问。"
    }
  },
  "required": ["schedule_ids"],
  "additionalProperties": false
}`

const removeScheduleBatchMax = 20

type removeScheduleTool struct {
	sched scheduleDeleter
}

func (t *removeScheduleTool) Name() string { return "remove_schedule" }
func (t *removeScheduleTool) Description() string {
	return "直接删除一个或多个定时推送任务，不再二次确认。用户可一次说出多个记得的任务名称、内容、时间或主题；先用 list_schedules 分别解析为内部 schedule_ids，唯一匹配就合并执行，只有某个描述存在多个合理候选时才用名称追问，绝不能要求用户查 ID。"
}
func (t *removeScheduleTool) Parameters() json.RawMessage {
	return json.RawMessage(removeScheduleSchema)
}

// Execute 的归属校验由 Scheduler.DeletePush 内的 GetSchedule(id, userID) 承担：
// 「不存在」与「不属于你」归一为 NotFound，agent 拿到伪造的 schedule_id 也删不动别人的。
//
// 原注释写着「不逐条校验调度归属：单 owner MVP 所有调度同属一人」——契约 §2.8 曾据此
// 把本处列为已知越权洞。校验后来补上了，注释却留在原地。**这条路径尤其危险**：
// schedule_id 直接来自模型生成的工具入参，而模型的输入里混着不可信的外部内容
// （抓来的正文、用户消息），提示注入完全可能让它去删一个别人的 id。
// 守卫见 store/schedule_ownership_test.go。
func (t *removeScheduleTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	ids, errText, _ := removeScheduleIDs(args)
	if errText != "" {
		return errText, nil
	}
	for _, schedID := range ids {
		var err error
		if durable, ok := t.sched.(idempotentScheduleDeleter); ok {
			err = durable.DeletePushIdempotent(
				ctx, schedID, userID,
				removeScheduleIdempotencyKey(userID, schedID),
			)
		} else {
			// Narrow test doubles and pre-command adapters retain the historical
			// interface; production *scheduler.Scheduler always takes the
			// idempotent durable-command branch above.
			err = t.sched.DeletePush(ctx, schedID, userID)
		}
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("已删除 %d 个定时推送任务。", len(ids)), nil
}

func removeScheduleIdempotencyKey(userID int64, scheduleID string) string {
	sum := sha256.Sum256([]byte(
		strconv.FormatInt(userID, 10) + "\x00" + scheduleID,
	))
	return fmt.Sprintf("agent.remove_schedule.v1:%x", sum[:16])
}

func (t *removeScheduleTool) Summarize(args json.RawMessage) string {
	ids, errText, malformed := removeScheduleIDs(args)
	if malformed {
		return summarizeFallback("删除定时推送任务", args)
	}
	if errText != "" {
		return fmt.Sprintf("删除定时推送任务（参数无效：%s；不会执行）", errText)
	}
	return fmt.Sprintf("删除 %d 个定时推送任务（id=%s）",
		len(ids), strings.Join(ids, "、"))
}

func removeScheduleIDs(args json.RawMessage) (ids []string, errText string, malformed bool) {
	var a struct {
		ScheduleIDs []string `json:"schedule_ids"`
	}
	decoder := json.NewDecoder(bytes.NewReader(args))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&a); err != nil {
		return nil, "schedule_ids 必须是非空字符串数组", true
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, "schedule_ids 必须是非空字符串数组", true
	}
	in := a.ScheduleIDs
	if len(in) == 0 {
		return nil, "schedule_ids 必须是非空字符串数组", false
	}
	if len(in) > removeScheduleBatchMax {
		return nil, fmt.Sprintf("一次最多删除 %d 个定时任务", removeScheduleBatchMax), false
	}
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		id := strings.TrimSpace(raw)
		if id == "" {
			return nil, "schedule_ids 必须是非空字符串数组", false
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out, "", false
}

// ============================================================
// run_task_now：立即运行一个或多个已有任务。不存在账号级“立即推送”；
// 每个调用只执行任务已经冻结的定义和版本化 Tool 调用。
// ============================================================

const runTaskNowSchema = `{
  "type": "object",
  "properties": {
    "schedule_ids": {
      "type": "array",
      "items": {"type": "string"},
      "minItems": 1,
      "maxItems": 20,
      "description": "要立即运行的内部任务 id 列表。用户不需要知道 id：先按用户记得的任务名称、主题、时间或用途调用 list_schedules 定位；唯一匹配就执行，多个合理候选才按可读名称追问。"
    }
  },
  "required": ["schedule_ids"],
  "additionalProperties": false
}`

type runTaskNowTool struct {
	runner TaskRunTrigger
}

func (t *runTaskNowTool) Name() string { return "run_task_now" }
func (t *runTaskNowTool) Description() string {
	return "立即运行一个或多个已有任务，只使用各任务手册已经批准并冻结的范围。一次性手动执行独立于周期调度开关：active 或 paused 任务都能运行，paused 只暂停自动周期触发；绝不能要求先恢复任务或再次确认，也不能改变周期调度状态。先用 list_schedules 按用户记得的名称、主题、时间或用途定位；唯一匹配直接运行，真歧义才按可读名称追问，绝不能要求用户提供内部 id。"
}
func (t *runTaskNowTool) Parameters() json.RawMessage {
	return json.RawMessage(runTaskNowSchema)
}

func (t *runTaskNowTool) Execute(
	ctx context.Context,
	userID int64,
	args json.RawMessage,
) (string, error) {
	ids, errText, malformed := removeScheduleIDs(args)
	if malformed || errText != "" {
		if errText == "" {
			errText = "schedule_ids 必须是非空字符串数组"
		}
		return errText, nil
	}
	invocationID, ok := toolInvocationIDFrom(ctx)
	if !ok {
		return "", types.NewAppError(
			types.CodeInternal,
			"任务立即运行缺少耐久调用身份",
			nil,
		)
	}
	for _, scheduleID := range ids {
		if err := t.runner.TriggerScheduleNowIdempotent(
			ctx,
			scheduleID,
			userID,
			runTaskNowIdempotencyKey(userID, invocationID, scheduleID),
		); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("已受理 %d 个任务的立即运行请求，结果会按各任务原有渠道送达。", len(ids)), nil
}

func runTaskNowIdempotencyKey(
	userID int64,
	invocationID string,
	scheduleID string,
) string {
	sum := sha256.Sum256([]byte(
		"agent.run_task_now/v1\x00" +
			strconv.FormatInt(userID, 10) + "\x00" +
			invocationID + "\x00" + scheduleID,
	))
	return fmt.Sprintf("agent.run_task_now.v1:%x", sum[:16])
}

func (t *runTaskNowTool) Summarize(json.RawMessage) string { return "" }

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
// update_profile：只用于画像首次采集（2.1），由 owner 明确表达后直接执行。
// 062 来源级 authority 启用后，已有画像只能在 Web「画像依据」逐条纠正；
// Agent 不提供未经设计的多 claim 修改工具。
// ============================================================

// maxProfileTags 人工标签上限（契约 §2：与库内/演化上限统一为 12，超 12 截前 12——
// 截断而非报错，人工整体替换不得静默丢演化标签，也不得因超限整次作废）。
const maxProfileTags = 12

const updateProfileSchema = `{
  "type": "object",
  "properties": {
    "industry": {"type": "string", "description": "首次画像采集的所在行业"},
    "occupation": {"type": "string", "description": "首次画像采集的职业/岗位"},
    "tags": {
      "type": "array",
      "items": {"type": "string"},
      "description": "首次画像采集的关注标签列表，最多 12 个"
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
	st         profileStore
	authorizer OwnerActionAuthorizer
}

func NewAuthorizedUpdateProfileTool(st profileStore, authorizer OwnerActionAuthorizer) ToolSpec {
	return newToolSpec(&updateProfileTool{st: st, authorizer: authorizer}, withToolSurface(ownerPolicy(
		Effects(EffectStateWrite, EffectDirectOwnerWrite), BudgetNone),
		ExposureIntent, IntentProfile, ResultTrustLocal, true))
}

func (t *updateProfileTool) Name() string { return "update_profile" }
func (t *updateProfileTool) Description() string {
	return "仅用于首次创建用户画像（行业、职业、关注标签）。若画像已存在，不要调用；请引导用户到 Web「画像依据」逐条纠正、排除、固定或撤销。"
}
func (t *updateProfileTool) Parameters() json.RawMessage {
	return json.RawMessage(updateProfileSchema)
}

// Execute：全缺省是确定性可自纠失败，回文案不触库；
// UpsertProfileFields 部分更新（nil 不改），不触碰 summary/游标/token 三件套。
func (t *updateProfileTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	var a updateProfileArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "参数不是合法 JSON，请修正后重试", nil
	}
	if a.Industry == nil && a.Occupation == nil && a.Tags == nil {
		return "没有提供任何要修改的字段，请至少提供 industry、occupation、tags 之一", nil
	}
	if state := runStateFrom(ctx); state != nil && state.agentFirstEnabled {
		if t.authorizer == nil {
			return "画像写入授权能力未装配，本次未执行。", nil
		}
		decision, err := t.authorizer.AuthorizeOwnerAction(ctx, OwnerActionAuthorization{
			OwnerRequest: state.ownerRequest, Action: "update_profile",
			Changes: append(json.RawMessage(nil), args...),
			Targets: []OwnerActionTarget{{Name: "当前用户画像", Status: "current"}},
		})
		if err != nil {
			return "", err
		}
		switch decision {
		case OwnerActionAuthorized:
		case OwnerActionAmbiguous:
			return "画像写入要求仍有歧义，本次未执行；请自然追问一次。", nil
		case OwnerActionDenied:
			return "当前原话没有授权画像写入，本次未执行。", nil
		default:
			return "", errors.New("agent: profile authorizer returned an invalid decision")
		}
	}
	p, err := t.st.UpsertProfileFields(ctx, userID, a.Industry, a.Occupation, capProfileTags(a.Tags))
	if err != nil {
		if errors.Is(err, types.ErrConflict) {
			return "画像已经存在，本次未修改。请到 Web 的「画像依据」逐条纠正、排除、固定或撤销。", nil
		}
		return "", err
	}
	return "画像已首次创建。当前画像——" + renderProfile(p), nil
}

// Summarize 只列提供的字段；未提供的字段绝不出现，确保「不改」与
// 「清空」在操作摘要中可区分。
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
		return "更新画像（未提供任何字段，不会产生变更）"
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
	b.WriteString("行业：")
	b.WriteString(orUnset(p.Industry))
	b.WriteString("；职业：")
	b.WriteString(orUnset(p.Occupation))
	if len(p.Tags) > 0 {
		b.WriteString("；关注标签：")
		b.WriteString(strings.Join(p.Tags, "、"))
	} else {
		b.WriteString("；关注标签：（未设置）")
	}
	if s := strings.TrimSpace(p.Summary); s != "" {
		b.WriteString("；摘要：")
		b.WriteString(s)
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
// P0 只做存取——view（读）/ edit（写）/ create_schedule 创建即初始化。
// 手册不影响任何抓取/打分/出卡（那是 P1）。
// ============================================================

// maxPlaybookContentRunes 手册内容 rune 上限：超出截断（不报错，同 capProfileTags 策略），
// 防超长手册撑爆模型上下文与 DB 行。playbookSummaryPreviewRunes 是摘要预览的更短上限。
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

// renderPlaybook only renders the user-owned manual. Legacy fetch_plan data is
// an internal compatibility projection and must not reappear as a second
// product concept.
func renderPlaybook(pb *types.SchedulePlaybook) string {
	content := strings.TrimSpace(pb.Content)
	if content == "" {
		content = "（手册为空——可以告诉我这个任务要抓什么、关注哪些主题、偏好哪些来源。）"
	}
	return fmt.Sprintf("任务手册（id=%s）：\n%s", pb.ScheduleID, content)
}

const viewTaskLatestRunSchema = `{
  "type": "object",
  "properties": {
    "schedule_id": {"type": "string", "description": "要查看最近运行证据的定时任务 id（先用 list_schedules 查询）"}
  },
  "required": ["schedule_id"],
  "additionalProperties": false
}`

type viewTaskLatestRunTool struct {
	st taskRunEvidenceStore
}

func (t *viewTaskLatestRunTool) Name() string { return "view_task_latest_run" }
func (t *viewTaskLatestRunTool) Description() string {
	return "查看某个任务最近一次已完成运行的精确证据：完成时间、运行结局、来源覆盖、实际工具及 HTTP 状态、推送结局和未推送闸门。回答最近是否运行、实际调用了什么或为什么没推送时必须使用本工具，不能从任务手册推断。"
}
func (t *viewTaskLatestRunTool) Parameters() json.RawMessage {
	return json.RawMessage(viewTaskLatestRunSchema)
}
func (t *viewTaskLatestRunTool) Execute(
	ctx context.Context,
	userID int64,
	raw json.RawMessage,
) (string, error) {
	var args struct {
		ScheduleID string `json:"schedule_id"`
	}
	if err := strictjson.DecodeExact(raw, &args); err != nil {
		return "view_task_latest_run 参数不是合法 JSON，或包含未知字段", nil
	}
	taskID := strings.TrimSpace(args.ScheduleID)
	if taskID == "" {
		return "schedule_id 必须是非空字符串（可先用 list_schedules 查询）", nil
	}
	evidence, err := t.st.GetLatestTaskRunEvidenceV1(ctx, userID, taskID)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return "没有找到你的这个定时任务；请先用 list_schedules 按名称重新定位。", nil
		}
		return "", err
	}
	if evidence == nil {
		return "该任务存在，但还没有任何已完成的运行记录。不能据此声称它已经正常运行。", nil
	}
	rawEvidence, err := json.Marshal(evidence)
	if err != nil {
		return "", err
	}
	return "最近运行证据（仅来自系统运行记录，不含任务手册推断）：\n" +
		string(rawEvidence) +
		"\n注意：该证据只含结构化运行元数据，不含网页正文或页面当前业务状态；不能用 quiet、active 或未推送闸门推断‘仍不可购买’等页面事实。", nil
}
func (t *viewTaskLatestRunTool) Summarize(json.RawMessage) string { return "" }
