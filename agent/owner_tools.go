package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

// TaskRunTrigger is the task-scoped run-now control plane used by manage_tasks.
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

type profileStore interface {
	GetProfile(ctx context.Context, userID int64) (*types.Profile, error)
	UpsertProfileFields(ctx context.Context, userID int64, industry, occupation *string, tags []string) (*types.Profile, error)
}

type idempotentScheduleDeleter interface {
	DeletePushIdempotent(context.Context, string, int64, string) error
}

type intelligenceQueryStore interface {
	QueryMyIntelligence(
		ctx context.Context,
		scope store.IntelligenceScope,
		query store.IntelligenceQuery,
	) (*store.IntelligenceQueryResult, error)
}

// BuildPublicResearchTools is the complete non-internal catalog shared by
// production owner chat and the separately authorized A2A projection.
func BuildPublicResearchTools(endpoints *EndpointTools, exa *ExaTools) []ToolSpec {
	var tools []ToolSpec
	if endpoints != nil {
		tools = append(tools, endpoints.SearchTool(), endpoints.ReadResultTool())
	}
	if exa != nil {
		tools = append(tools, exa.SearchTool(), exa.ReadPageTool())
	}
	return tools
}

// BuildOwnerTools is the only production owner Agent catalog.
func BuildOwnerTools(
	st *store.Store,
	manage ManageTasksDeps,
	authorizer OwnerActionAuthorizer,
	endpoints *EndpointTools,
	exa *ExaTools,
) []ToolSpec {
	tools := []ToolSpec{
		NewQueryMyIntelligenceTool(st),
		NewManageTasksTool(manage),
		NewAuthorizedUpdateProfileTool(st, authorizer),
	}
	return append(tools, BuildPublicResearchTools(endpoints, exa)...)
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
      "enum": ["tasks","runs","observations","briefs","agent_turns","tool_calls","profile","feedbacks"],
      "description": "一次只查询一个用户情报数据集。tasks 是当前任务定义，不是历史情报活动；先从 tasks 取得 task_ref，再用 task_ref 关联 runs/briefs/observations。重大更新、昨天查到什么等时间窗问题必须查询 runs/briefs，必要时查询 observations。runs.outcome_status 只接受 pending/finalized/ambiguous/failed/unavailable，不存在 success；finalized 只表示已结算，是否产出情报同时看 result=content/quiet/failed/interrupted。比较最近一次与上一次时按 created_at 倒序取至少两行，不先筛 outcome_status"
    },
    "select": {
      "type": "array", "maxItems": 32, "items": {"type":"string"},
      "description": "要返回的真实目录字段；不完全确定时必须省略 select，默认列足够回答历史更新问题。tasks 默认 task_ref,task_name,playbook,status,schedule,updated_at；runs 默认 task_ref,run_snapshot_id,run_kind,runtime_generation,outcome_status,result,source_coverage,processing,finalized_at,created_at；briefs 默认 lineage,task_ref,run_snapshot_id,brief_preview,brief_digest,status,significance,decision,delivery_required,failure_code,delivery_status,truth_coverage,payload_coverage,payload_offset,payload_total_chars,payload_total_bytes,payload_complete,generated_at,created_at；observations 默认 lineage,task_ref,run_snapshot_id,invocation_ref,tool_name,model_visible_result,result_digest,stored_size,original_size,source_truncated,payload_coverage,evidence_coverage,trust_type,payload_offset,payload_total_chars,payload_complete,content_count,created_at。不存在 run_ref,brief_ref,result_summary,payload,coverage 通用字段"
    },
    "filters": {
      "type": "array", "maxItems": 16,
      "items": {
        "type": "object",
        "properties": {
          "field": {
            "type":"string",
            "description":"真实目录字段名，不得自造 name、run_ref、brief_ref、result_summary、payload、coverage 等字段。任务名称字段是 tasks.task_name，跨历史数据集关联字段是 task_ref。相对时间通常使用：tasks.updated_at（仅定义/状态/计划变化）；runs.created_at 或 finalized_at；observations.created_at；briefs.generated_at；agent_turns.created_at；tool_calls.created_at；profile.updated_at。within 由 Store 按任务时区解析，不要自行计算日期边界"
          },
          "op": {"type":"string","enum":["eq","neq","gt","gte","lt","lte","contains","in","within"]},
          "value": {"description":"与字段类型匹配的 JSON 值；within 使用 today、yesterday 或 last_7_days"}
        },
        "required": ["field","op","value"], "additionalProperties": false
      }
    },
    "group_by": {"type":"array","maxItems":8,"items":{"type":"string"}},
    "metrics": {
      "type":"array","maxItems":8,
      "items": {
        "type":"object",
        "properties": {
          "function":{"type":"string","enum":["count","sum","avg","min","max"]},
          "field":{"type":"string"}, "as":{"type":"string"}
        },
        "required":["function","as"], "additionalProperties":false
      }
    },
    "order_by": {
      "type":"array","maxItems":8,
      "items": {
        "type":"object",
        "properties": {
          "field":{
            "type":"string",
            "description":"目录字段名；时间排序字段与 filters.field 的数据集映射相同。"
          },
          "direction":{"type":"string","enum":["asc","desc"]}
        },
        "required":["field"], "additionalProperties":false
      }
    },
    "limit":{"type":"integer","minimum":1,"maximum":100},
    "cursor":{"type":"string","description":"上一页返回的 next_cursor；只能原样继续同一查询"}
  },
  "required":["dataset"], "additionalProperties":false
}`

type queryMyIntelligenceTool struct{ st intelligenceQueryStore }

func (*queryMyIntelligenceTool) Name() string { return "query_my_intelligence" }
func (*queryMyIntelligenceTool) Description() string {
	return "查询当前用户自己的任务、历史运行、legacy/V3 Observation/Evidence、legacy/V3 Brief、历史 Agent 回答、模型实际看到的工具证据、画像或推送反馈。tasks 只表示当前任务定义，tasks.updated_at 只表示定义、状态或计划变化；‘过去七天有哪些重大更新’等历史情报问题先用 tasks.task_name 定位任务且不要按时间过滤定义，保存返回的 task_ref，再用 task_ref 查询时间窗内的 runs 和 briefs，必要时查询 observations，不能用 tasks 空结果断言没有更新。runs.outcome_status 只接受 pending/finalized/ambiguous/failed/unavailable，不存在 success；finalized 只表示已结算，是否产出情报同时看 result。比较最近一次与上一次时按 created_at 倒序读取至少两条，不先筛 outcome_status。字段不完全确定时省略 select 使用默认列，绝不自造 run_ref、brief_ref、result_summary、payload、coverage 等字段；within 由 Store 按任务时区解析，不自行猜窗口日期。observations/briefs 的 coverage 会区分 exact、unavailable、全文与分窗，payload_complete=false 或 truncated=true 时必须原样携带 next_cursor 连续查询到完整尾部，不能补猜；用户用‘刚才那条’、‘我点的’等方式指代卡片操作时查询 feedbacks，不要求用户提供内部 ID；跨数据集问题连续调用本工具后再综合。"
}
func (*queryMyIntelligenceTool) Parameters() json.RawMessage {
	return json.RawMessage(queryMyIntelligenceSchema)
}
func (t *queryMyIntelligenceTool) Execute(ctx context.Context, userID int64, raw json.RawMessage) (string, error) {
	if t.st == nil {
		return "", types.NewAppError(types.CodeInternal, "用户情报查询未装配", nil)
	}
	var query store.IntelligenceQuery
	if err := strictjson.DecodeExact(raw, &query); err != nil {
		return "query_my_intelligence 参数不是合法关系查询，或包含未知字段", nil
	}
	meta, ok := ctx.Value(chatMetaKey{}).(chatMeta)
	if !ok || meta.scope.TenantID <= 0 || meta.scope.UserID != userID || meta.scope.SessionID <= 0 {
		return "", types.NewAppError(types.CodeValidation, "用户情报查询缺少认证会话范围", types.ErrValidation)
	}
	sessionID := meta.scope.SessionID
	storeQuery, err := prepareIntelligenceToolCallQuery(query)
	if err != nil {
		return "query_my_intelligence 查询被拒绝：" + err.Error(), nil
	}
	storeQuery, err = prepareIntelligenceObservationQuery(storeQuery)
	if err != nil {
		return "query_my_intelligence 查询被拒绝：" + err.Error(), nil
	}
	storeQuery, err = prepareIntelligenceBriefQuery(storeQuery)
	if err != nil {
		return "query_my_intelligence 查询被拒绝：" + err.Error(), nil
	}
	storeQuery, err = prepareIntelligenceFeedbackQuery(storeQuery)
	if err != nil {
		return "query_my_intelligence 查询被拒绝：" + err.Error(), nil
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
	if err := projectBriefResultForAgent(ctx, result); err != nil {
		return "", err
	}
	if err := projectFeedbackResultForAgent(ctx, result); err != nil {
		return "", err
	}
	rememberIntelligenceResultReferences(ctx, result)
	encoded, err := json.Marshal(result)
	if err != nil {
		return "", types.NewAppError(types.CodeInternal, "编码用户情报查询结果", err)
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

const emptyParamsSchema = `{"type":"object","properties":{}}`

func summarizeFallback(action string, args json.RawMessage) string {
	raw := []rune(string(args))
	if len(raw) > 200 {
		raw = append(raw[:200], '…')
	}
	return action + "（参数未能解析）：" + string(raw)
}

const maxProfileTags = 12

const updateProfileSchema = `{
  "type": "object",
  "properties": {
    "industry": {"type": "string", "description": "首次画像采集的所在行业"},
    "occupation": {"type": "string", "description": "首次画像采集的职业/岗位"},
    "tags": {"type":"array","items":{"type":"string"},"description":"首次画像采集的关注标签列表，最多 12 个"}
  },
  "additionalProperties": false
}`

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

func (*updateProfileTool) Name() string { return "update_profile" }
func (*updateProfileTool) Description() string {
	return "仅用于首次创建用户画像（行业、职业、关注标签）。若画像已存在，不要调用；请引导用户到 Web「画像依据」逐条纠正、排除、固定或撤销。"
}
func (*updateProfileTool) Parameters() json.RawMessage { return json.RawMessage(updateProfileSchema) }
func (t *updateProfileTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	var a updateProfileArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "参数不是合法 JSON，请修正后重试", nil
	}
	if a.Industry == nil && a.Occupation == nil && a.Tags == nil {
		return "没有提供任何要修改的字段，请至少提供 industry、occupation、tags 之一", nil
	}
	state := runStateFrom(ctx)
	if state != nil && state.webActionMode != webActionNone {
		return webActionBoundaryReply, nil
	}
	if state != nil && state.agentFirstEnabled {
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
	if t.st == nil {
		return "", types.NewAppError(types.CodeInternal, "用户画像写入未装配", nil)
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
func (*updateProfileTool) Summarize(args json.RawMessage) string {
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
			parts = append(parts, "关注标签整体替换为「"+strings.Join(tags, "、")+"」")
		}
	}
	if len(parts) == 0 {
		return "更新画像（未提供任何字段，不会产生变更）"
	}
	return "更新画像：" + strings.Join(parts, "；")
}

func capProfileTags(tags []string) []string {
	if len(tags) > maxProfileTags {
		return tags[:maxProfileTags]
	}
	return tags
}

func profileFieldChange(name, value string) string {
	if strings.TrimSpace(value) == "" {
		return "清空" + name
	}
	return name + "改为「" + value + "」"
}

func renderProfile(profile *types.Profile) string {
	var b strings.Builder
	b.WriteString("行业：")
	b.WriteString(orUnset(profile.Industry))
	b.WriteString("；职业：")
	b.WriteString(orUnset(profile.Occupation))
	if len(profile.Tags) > 0 {
		b.WriteString("；关注标签：")
		b.WriteString(strings.Join(profile.Tags, "、"))
	} else {
		b.WriteString("；关注标签：（未设置）")
	}
	if summary := strings.TrimSpace(profile.Summary); summary != "" {
		b.WriteString("；摘要：")
		b.WriteString(summary)
	}
	return b.String()
}

func orUnset(value string) string {
	if strings.TrimSpace(value) == "" {
		return "（未设置）"
	}
	return value
}
