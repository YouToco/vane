// TikHub 端点注册表的 agent 工具面（端点注册表契约 §3/§4/§7）：
//
//   - search_endpoints 检索元工具：在 tikhubcatalog 的 Agent 安全子目录上 BM25 检索，
//     命中的端点**激活**进会话（动态注入为一等 FC 工具，业内 Tool Search /
//     retrieve-then-inject 模式，Boss 拍板 2026-07-18 选注入而非通用转发）。
//   - endpointTool 动态端点工具：按 catalog Entry 即时构造，参数 schema 从注册表
//     生成，Execute 走 tikhubinvoke 通用调用器，结果原文（截断）回给模型阅读。
//
// 三条硬边界：
//   - 端点工具按本地 ToolPolicy 声明网络读取、计费与 taint；
//     因此它们永远不进 task_creation_operations，ExecuteAction 路径只需静态白名单。
//   - 白名单语义（M4 契约 §10）扩展为「静态工具 ∪ 会话已激活端点」：模型编造的
//     端点名（哪怕真在注册表里）只要没被本会话 search_endpoints 激活过，一律拒绝——
//     激活集是显式审计过的调用面，跳过检索直呼端点名是绕过检索留痕的旁门。
//   - 免确认的调用受滚动 24h EndpointDailyCap 与 Agent 统一单消息隐藏熔断器保护；
//     EndpointMsgCap 只保留历史配置兼容，不参与日常规划。
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/tikhubcatalog"
	"github.com/YouToco/vane/tikhubinvoke"
	"github.com/YouToco/vane/types"
)

const (
	// maxActivatedEndpoints 会话内同时激活（注入 FC tools 数组）的端点上限。
	// 在场工具数安全线是 30（RAG-MCP 压测 <30 成功率 >90%）：**再加静态工具前
	// 必须先降本上限**（TestToolCountBudget 是账单，别信任何注释里的手算——
	// 上一版注释就把基数写错了一个）。账目沿革：2026-07-18 read_endpoint_result
	// 入列 15→14（13 基础 + 2 端点工具 + 14 激活 = 29 顶线）；2026-07-19
	// set_task_strictness 入列（任务门槛档位拍板）14→13：14 基础 + 2 端点工具
	// + 13 激活 = 29 压线内；2026-07-20 web_search/read_page 入列（Exa ad-hoc
	// 工具对拍板）13→11：16 基础 + 2 端点工具 + 11 激活 = 29 压线内
	// （基础/端点工具均计入 BuildTools 返回值）。
	maxActivatedEndpoints = 11
	// searchTopK 每次检索返回并激活的端点数（Anthropic Tool Search 默认值同为 5）。
	searchTopK = 5
	// 端点结果的内联上限**不再是常量**：由 agent 模型声明的上下文窗口派生
	// （llm.DeriveInlineLimits，OpenClaw 同款分档 + 窗口 30% 封顶），随
	// EndpointTools 注入。写死过一次的教训见 llm/context.go 头注——6000 rune
	// 是 64K 窗口时代拍的值，模型换到 1M 后没人记得改，直到 Boss 在生产撞见截断。
	//
	// endpointResultFallbackRunes 仅用于装配缺失时的兜底（NewEndpointTools 收到
	// 非法窗口值），不参与正常路径。
	endpointResultFallbackRunes = 16000
	// endpointDefDescMaxRunes 动态工具 Description 的截断上限。激活的每个工具
	// 定义每轮都随请求发送，600 rune 全文 ×15 个会持续吃预算；检索结果文本里
	// 已给过更全的说明，注入定义只保留摘要级描述。
	endpointDefDescMaxRunes = 300
	// dailyCapWindow 每日限额的滚动窗口。用滚动 24h 而非自然日：语义简单无时区
	// 争议，且「过去 24h 花了多少」比「今天零点以来」更贴近成本节流的本意。
	dailyCapWindow = 24 * time.Hour
)

// ============================================================
// 会话激活状态与 per-message 运行状态（ctx 旁路，先例见 loop.go chatMetaKey）
// ============================================================

// toolRunKey 经 ctx 把 *toolRunState 传给工具 Execute：Tool 接口签名由契约固定
// （不含会话/激活参数），而工具实例是全局单例、不能携带 per-message 状态。
type toolRunKey struct{}

// toolCallRecKey 经 ctx 把本次调用的 *types.ToolCall 记录传给工具，
// 由工具回填专属字段（端点 path、HTTP 状态、检索词、候选清单）。
type toolCallRecKey struct{}

// toolRunState 是一次 HandleMessage 的工具运行状态。
type toolRunState struct {
	activation *activationState
	// ownerRequest is the trusted current-user suffix only. Quoted cards,
	// previous assistant text and external tool results never participate in
	// intent routing or direct-write authorization.
	ownerRequest string
	intents      ToolIntent
	// Intent toolkit rollout is fixed by the locally assembled Loop, never by
	// model or external content. Shadow records aggregate exposure differences
	// while returning the legacy registry unchanged.
	intentToolkitsEnabled        bool
	intentToolkitsShadow         bool
	intentToolkitsShadowSeen     bool
	intentToolkitsLegacyCount    int
	intentToolkitsCandidateCount int
	intentToolkitsRemoved        []string
	// Unified loop breaker state. Provider-specific message caps are not used
	// for planning; this hidden ceiling only stops repeated or runaway calls.
	toolExecutions     int
	successfulCalls    map[string]struct{}
	failedCalls        map[string]int
	loopBreakReason    string
	clarificationCount int
	candidateSearches  int
	candidateHits      int
	// sideEffectFreeTurn is set when a possible task edit did not receive a
	// valid execute decision from the isolated semantic gate. The main Agent
	// may still answer or use read-only evidence, but every state write,
	// delivery and activation is removed at declaration and execution time.
	sideEffectFreeTurn bool
	// contextStepOffset reserves context step 1 for semantic adjudication so
	// the main Agent's first request is sealed as step 2 on the same trace.
	contextStepOffset int
	// groundedBrief confines a trusted internal Brief/report follow-up to the
	// exact supplied artifact. It has no tool surface at declaration or
	// execution time, so source text can inform an answer but can never trigger
	// network research, a durable proposal, or a write.
	groundedBrief bool
	// directTaskCreation 表示用户已经明确要求按当前消息直接创建任务，
	// 且没有要求先查询/核对。该模式只缩小当前消息的工具面到 create_schedule；
	// durable operation 由 Agent 立即推进。
	directTaskCreation bool
	// directTaskCreationToolRejected 记录模型在缩面后仍幻觉调用了其他工具。
	// 下一轮会丢弃这段未执行的原生 tool protocol，只保留进入本消息时的
	// 安全基线并强化 system 提示，避免再次触发供应商协议兼容问题。
	directTaskCreationToolRejected bool
	// directTaskCreationResponseRejected 记录模型在没有真实 proposal 时给出了
	// 无工具文字。下一轮同样回到安全基线；连续两次不调用 create_schedule
	// 则返回确定性未创建文案。这里不词法区分“追问”和“口头承诺”，避免同义
	// 承诺加问号绕过。
	directTaskCreationResponseRejected bool
	// directTaskCreationValidationFailures 统计 Agent 精确字段门和
	// create_schedule controller 返回的确定性参数校验失败。上限与全局
	// MaxTurns 分离，防模型拿同一份错误反复猜内部格式并消耗 20 次付费调用。
	directTaskCreationValidationFailures int
	// Direct durable writes return a deterministic result immediately after
	// their coordinator accepts the operation, avoiding a second model turn
	// that could duplicate the write or falsely claim completion.
	directTaskCreationResult string
	// directActionID is generated by the authenticated HTTP boundary from the
	// tenant/user/request identity. Empty preserves historical Feishu behavior,
	// which still generates a fresh card action inside the Agent.
	directActionID string
	// directTaskDefinitionEditID is the server-verified task selected by the
	// Web request. Non-empty activates an isolated proposal lane that declares
	// only edit_task_definition and rejects any argument targeting another ID
	// before the durable controller is called.
	directTaskDefinitionEditID string
	// The three edit counters mirror direct creation's bounded self-correction
	// without sharing state: a rejected hidden tool, a tool-free response, and
	// deterministic argument/controller validation each have explicit budgets.
	directTaskDefinitionEditToolRejected     bool
	directTaskDefinitionEditResponseRejected bool
	directTaskDefinitionEditFailures         int
	directTaskDefinitionEditResult           string
	// naturalTaskDefinitionEdit is the Feishu/name-based edit lane. It first
	// resolves the readable task description with list_schedules, then exposes
	// only edit_task_definition. The user never has to provide an internal ID.
	naturalTaskDefinitionEdit                 bool
	naturalTaskDefinitionEditTaskListed       bool
	naturalTaskDefinitionEditResolvedID       string
	naturalTaskDefinitionEditToolRejected     bool
	naturalTaskDefinitionEditResponseRejected bool
	naturalTaskDefinitionEditFailures         int
	// A direct write may return probe detail that is safe
	// to show once but unsafe to feed into another model turn or persist. The
	// loop returns it deterministically and the normal history scrub replaces
	// the tool exchange with a fixed placeholder.
	directUntrustedWriteResult string
	// endpointCalls 本条消息内已发起的端点调用次数（限额判定用）。计数时机在
	// 校验通过之后、发请求之前：打到上游才算（含 HTTP 错误——失败同样计费），
	// 参数校验失败没打上游、不计费，不吃限额。
	// Provider-family counters remain observation-only. They no longer enforce
	// per-message quotas; the unified hidden fuse uses toolExecutions.
	endpointCalls int
	exaCalls      int
	// inlinedRunes 本条消息内已内联进上下文的端点结果字符数（累计预算，
	// 见 llm.InlineLimits.MsgBudget）。只统计真正回给模型的部分，不含缓存全量。
	inlinedRunes int
	// untrustedExternalResult 表示本条用户消息已把外部网页结果放进模型上下文。
	// 此后直到下一条用户消息，全部工具一律 fail-closed：外部内容可影响回答，
	// 不能借提示注入生成耐久写操作，也不能用 URL/query 外带上下文。
	// 状态不持久化，下一条明确用户请求重新开始。
	untrustedExternalResult bool
	// externalFollowupSearchRequired 只由类型化飞书外部输入中拆出的当前用户后缀
	// 决定。引用正文不参与判断，也不能进入 query。
	externalFollowupSearchRequired bool
	// externalFollowupSearchQuery 是允许送往 Exa 的唯一查询字节。非空表示
	// web_search 已真实装配且通过本地效果策略校验。
	externalFollowupSearchQuery string
	// responseRejected 给模型一次从无工具猜答自纠为真实搜索的机会；attempted /
	// succeeded 分开记录，确保参数改写、预算拒绝、上游失败或并列调用后不能再
	// 输出伪“检索结果”。succeeded 只允许 webSearchTool 在上游无错返回后写入。
	externalFollowupSearchResponseRejected bool
	externalFollowupSearchAttempted        bool
	externalFollowupSearchSucceeded        bool
	// result is the exact bounded text returned by web_search. The final-answer
	// harness validates citations against it instead of trusting the model to
	// remember which URLs really appeared. GroundingFailures gives one
	// tool-free correction turn, then fails closed.
	externalFollowupSearchResult      string
	externalFollowupSearchEvidence    []externalFollowupSearchEvidence
	externalFollowupGroundingFailures int
	// webResearchSucceeded generalizes the former one-off product branch:
	// every successful web search/page read must finish with citations that
	// occur in these structured results.
	webResearchSucceeded        bool
	webSearchSucceeded          bool
	webPageReadSucceeded        bool
	webPageReadResponseRejected bool
	// allowedLocalResultHandles 只登记本条消息的动态端点刚生成的截断句柄。
	// 句柄 res-N 可猜且缓存按 user 共享；仅用 bool 会让恶意端点枚举同用户
	// 其他会话的旧结果。taint 后 read_endpoint_result 必须命中这个集合。
	allowedLocalResultHandles map[string]struct{}
}

// inlineBudget 返回本次调用可内联的字符数：预算未尽给满额，尽了给保底
// （保底仍有句柄可取回，故不是数据丢失，只是首屏变窄）。
// s 为 nil（RunOnce/A2A 无状态轨）时按单次满额，不累计。
func (s *toolRunState) inlineBudget(lim llm.InlineLimits) int {
	if s == nil {
		return lim.PerCall
	}
	if left := lim.MsgBudget - s.inlinedRunes; left < lim.PerCall {
		if left < lim.MinPerCall {
			return lim.MinPerCall
		}
		return left
	}
	return lim.PerCall
}

func runStateFrom(ctx context.Context) *toolRunState {
	s, _ := ctx.Value(toolRunKey{}).(*toolRunState)
	return s
}

func recFrom(ctx context.Context) *types.ToolCall {
	r, _ := ctx.Value(toolCallRecKey{}).(*types.ToolCall)
	return r
}

// activationState 会话的激活端点集合。顺序即注入顺序：append-only + FIFO 逐出，
// 存量前缀在会话内恒稳定——DeepSeek 前缀缓存按最长公共前缀命中，尾部追加不作废
// 已缓存的前段（端点注册表契约 §4）。saveSession 每次全量写回（会话行本就每条
// 消息覆盖写，无需增量脏标记）。
type activationState struct {
	names []string
}

// decodeActivation 解析会话行里的激活列表。损坏数据按空集自愈（下次保存即修复），
// 与 decodeMessages 的自愈原则一致。
func decodeActivation(raw json.RawMessage) *activationState {
	a := &activationState{}
	if len(raw) == 0 {
		return a
	}
	if err := json.Unmarshal(raw, &a.names); err != nil {
		a.names = nil
	}
	return a
}

func (a *activationState) encode() json.RawMessage {
	if len(a.names) == 0 {
		return json.RawMessage("[]")
	}
	raw, err := json.Marshal(a.names)
	if err != nil {
		return json.RawMessage("[]")
	}
	return raw
}

func (a *activationState) contains(name string) bool {
	for _, n := range a.names {
		if n == name {
			return true
		}
	}
	return false
}

// activate 把端点加入激活集。已在集合中不动位置（保前缀稳定）；满员时 FIFO
// 逐出最早激活的端点并返回其名（调用方在检索结果里明示，被逐出的端点再调用
// 会收到白名单拒绝 + 重新检索即可恢复）。
func (a *activationState) activate(name string) (evicted string) {
	if a.contains(name) {
		return ""
	}
	if len(a.names) >= maxActivatedEndpoints {
		evicted = a.names[0]
		a.names = append(a.names[:0], a.names[1:]...)
	}
	a.names = append(a.names, name)
	return evicted
}

// ============================================================
// EndpointTools：搜索元工具 + 动态端点工具的装配单元
// ============================================================

// endpointCallCounter 每日限额的窗口计数（生产实现 *store.Store，
// 读的就是 tool_calls 记账表——限额与账本同源）。
type endpointCallCounter interface {
	CountTikHubEndpointCallsSince(ctx context.Context, since time.Time) (int, error)
}

// endpointInvoker 是端点调用的窄接口（生产实现 *tikhubinvoke.Invoker）。
// 收窄后端点工具的 Execute 分支可用假实现覆盖，不打真网络、不烧真计费。
type endpointInvoker interface {
	Invoke(ctx context.Context, entry tikhubcatalog.Entry, params map[string]any) (*tikhubinvoke.Result, error)
}

// EndpointTools 持有端点工具面的共享依赖，随 Deps 注入 Loop；nil 表示该能力
// 未装配（tikhub key 缺失等），agent 退化为纯静态工具面，行为与本特性上线前一致。
type EndpointTools struct {
	inv      endpointInvoker
	counter  endpointCallCounter
	msgCap   int
	dailyCap int
	results  *resultCache // 大结果缓存（契约 §3.5：截断句柄 + read_endpoint_result 取回）
	// limits 由 agent 模型的上下文窗口派生（llm.DeriveInlineLimits）：内联多少
	// 内容是模型属性不是代码常量，模型换代时自动跟随，无需有人记得改这里。
	limits llm.InlineLimits
}

// NewEndpointTools 构造端点工具面。caps ≤0 时兜底为保守默认（装配疏漏不能变成无限额）。
// contextTokens 是 agent 模型声明的上下文窗口（llm.ContextWindowTokens）；
// ≤0 时按保守兜底档派生，绝不因装配疏漏放大内联量。
func NewEndpointTools(inv endpointInvoker, counter endpointCallCounter, msgCap, dailyCap, contextTokens int) *EndpointTools {
	if msgCap <= 0 {
		msgCap = 10
	}
	if dailyCap <= 0 {
		dailyCap = 200
	}
	limits := llm.DeriveInlineLimits(contextTokens)
	if contextTokens <= 0 {
		limits = llm.InlineLimits{
			PerCall:    endpointResultFallbackRunes,
			MsgBudget:  endpointResultFallbackRunes * 3,
			MinPerCall: endpointResultFallbackRunes / 4,
		}
	}
	return &EndpointTools{inv: inv, counter: counter, msgCap: msgCap, dailyCap: dailyCap,
		results: newResultCache(), limits: limits}
}

// SearchTool 返回检索元工具（进静态白名单，BuildTools 装配）。检索会修改
// 本消息 activation，但现有调用与预算仍完全由工具自身管理。
func (e *EndpointTools) SearchTool() ToolSpec {
	return newToolSpec(&searchEndpointsTool{ep: e}, withToolSurface(ownerPolicy(
		Effects(EffectActivationWrite),
		BudgetNone),
		ExposureIntent, IntentSocialResearch, ResultTrustLocal, false))
}

// ReadResultTool 返回大结果取回工具（进静态白名单，BuildTools 装配；契约 §3.5）。
func (e *EndpointTools) ReadResultTool() ToolSpec {
	return newToolSpec(
		&readEndpointResultTool{cache: e.results, perRead: e.limits.PerCall},
		withToolSurface(ownerPolicy(
			Effects(EffectLocalHandleRead, EffectTrustTaint),
			BudgetNone),
			ExposureContext, IntentSocialResearch, ResultTrustExternal, false),
	)
}

// Resolve 按白名单语义解析动态端点工具：必须**已激活**且仍在注册表里。
// 注册表里存在但未激活 → 不解析（见文件头注第二条硬边界）；
// 已激活但注册表已无此端点（re-gen 下线）→ 不解析，模型收到标准"工具不存在"自纠文案。
func (e *EndpointTools) Resolve(name string, act *activationState) (ToolSpec, bool) {
	if act == nil || !act.contains(name) {
		return ToolSpec{}, false
	}
	entry, ok := tikhubcatalog.AgentLookup(name)
	if !ok {
		return ToolSpec{}, false
	}
	return newToolSpec(&endpointTool{ep: e, entry: entry}, withToolSurface(ownerPolicy(
		Effects(EffectNetworkRead, EffectBillable, EffectTrustTaint),
		BudgetToolManaged),
		ExposureContext, IntentSocialResearch, ResultTrustExternal, false)), true
}

func isRegisteredEndpoint(name string) bool {
	_, ok := tikhubcatalog.AgentLookup(name)
	return ok
}

// Defs 渲染激活端点的 FC 工具声明，顺序 = 激活顺序（前缀稳定性见 activationState）。
func (e *EndpointTools) Defs(act *activationState) []llm.ToolDef {
	if act == nil || len(act.names) == 0 {
		return nil
	}
	defs := make([]llm.ToolDef, 0, len(act.names))
	for _, name := range act.names {
		entry, ok := tikhubcatalog.AgentLookup(name)
		if !ok {
			continue // 注册表下线的端点不再注入；Resolve 同步拒绝，两处口径一致
		}
		spec := newToolSpec(&endpointTool{ep: e, entry: entry}, withToolSurface(ownerPolicy(
			Effects(EffectNetworkRead, EffectBillable, EffectTrustTaint),
			BudgetToolManaged),
			ExposureContext, IntentSocialResearch, ResultTrustExternal, false))
		defs = append(defs, spec.Definition)
	}
	return defs
}

// endpointDefDescription 动态工具的注入描述：摘要 + 截断说明 + 计费提醒。
func endpointDefDescription(entry tikhubcatalog.Entry) string {
	desc := publicEndpointText(entry.Summary)
	if entry.Description != "" {
		desc += "\n" + truncateRunes(
			publicEndpointText(entry.Description),
			endpointDefDescMaxRunes,
		)
	}
	return desc + "\n（社媒公开数据查询，可能计费，请按需调用）"
}

func publicEndpointText(value string) string {
	lines := strings.Split(value, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		lower := strings.ToLower(strings.TrimSpace(line))
		if lower == "" {
			continue
		}
		// Catalog examples are transport-oriented and often consume most of the
		// schema budget. They are not needed to choose or call the business tool;
		// stop before the example section so truncation cannot accidentally turn
		// "[Example]" into the provider-looking fragment "[Exa…".
		if strings.Contains(lower, "[example]") ||
			strings.Contains(lower, "### example") ||
			strings.Contains(lower, "### 示例") ||
			strings.Contains(lower, "[示例]") {
			break
		}
		if strings.Contains(lower, "tikhub") ||
			strings.Contains(lower, "cookie") ||
			strings.Contains(lower, "/api/v") ||
			strings.Contains(lower, "api priority") ||
			strings.Contains(lower, "接口优先级") ||
			strings.Contains(lower, "接口推荐") ||
			strings.Contains(lower, "本接口") ||
			strings.Contains(lower, "request method") ||
			strings.Contains(lower, "请求方法") ||
			strings.Contains(lower, "endpoint path") ||
			strings.Contains(lower, "接口路径") {
			continue
		}
		out = append(out, strings.TrimSpace(line))
	}
	return strings.Join(out, "\n")
}

// endpointParamsSchema 从注册表参数生成 FC JSON schema。类型映射保守：注册表的
// 归一化类型（gen/schemaType）能直映射的直映射，未知类型退化 string——schema 只是
// 给模型的提示，权威校验在 Execute 的 validateEndpointArgs + 上游 422。
func endpointParamsSchema(entry tikhubcatalog.Entry) json.RawMessage {
	props := make(map[string]any, len(entry.Params))
	var required []string
	for _, p := range entry.Params {
		prop := map[string]any{}
		switch {
		case p.Type == "integer" || p.Type == "number" || p.Type == "boolean" || p.Type == "object":
			prop["type"] = p.Type
		case strings.HasPrefix(p.Type, "array:"):
			item := strings.TrimPrefix(p.Type, "array:")
			switch item {
			case "integer", "number", "boolean", "object":
			default:
				item = "string"
			}
			prop["type"] = "array"
			prop["items"] = map[string]any{"type": item}
		default:
			prop["type"] = "string"
		}
		if p.Desc != "" {
			prop["description"] = publicEndpointText(p.Desc)
		}
		if len(p.Enum) > 0 {
			publicEnum := make([]any, 0, len(p.Enum))
			for _, value := range p.Enum {
				if text, isString := value.(string); isString {
					if text, ok := publicEndpointLiteral(text); ok {
						publicEnum = append(publicEnum, text)
					}
				} else {
					publicEnum = append(publicEnum, value)
				}
			}
			if len(publicEnum) > 0 {
				prop["enum"] = publicEnum
			}
		}
		if p.Default != nil {
			if value, isString := p.Default.(string); isString {
				if value, ok := publicEndpointLiteral(value); ok {
					prop["default"] = value
				}
			} else {
				prop["default"] = p.Default
			}
		}
		props[p.Name] = prop
		if p.Required {
			required = append(required, p.Name)
		}
	}
	schema := map[string]any{"type": "object", "properties": props}
	if len(required) > 0 {
		schema["required"] = required
	}
	raw, err := json.Marshal(schema)
	if err != nil {
		// props 全部由基本类型拼成，Marshal 不可能失败；防御性兜底为空参 schema。
		return json.RawMessage(emptyParamsSchema)
	}
	return raw
}

func publicEndpointLiteral(value string) (string, bool) {
	lower := strings.ToLower(strings.TrimSpace(value))
	if strings.Contains(lower, "tikhub") ||
		strings.Contains(lower, "/api/v") ||
		strings.Contains(lower, "request method") ||
		strings.Contains(lower, "endpoint path") {
		return "", false
	}
	return value, true
}

// endpointSystemNote 注入 system prompt 的能力说明（仅 Endpoints 装配时，loop.New）。
// 官方最佳实践：system prompt 写明「可搜索的工具类别」，模型才会主动想到去搜。
func endpointSystemNote() string {
	return "\n\n[社媒数据查询]\n用户想查询/调研社媒平台的内容、账号、评论、热榜等一次性问题时，" +
		"先用 search_endpoints 搜索可用的社媒查询工具（覆盖内容、账号、搜索、热榜、评论、趋势和公开分析；平台：" +
		strings.Join(tikhubcatalog.Platforms(), "、") + "），命中的具体能力会成为你可直接调用的工具。" +
		"检索不到就换关键词（中英文都可）或加 platform 过滤再试。端点可能按次计费且受滚动 24 小时成本护栏约束；" +
		"用户要的是**持续追新**时不要用端点查询，改用 create_schedule 创建任务。"
}

// ============================================================
// search_endpoints：检索元工具（静态白名单成员，读工具免确认）
// ============================================================

type searchEndpointsTool struct {
	ep *EndpointTools
}

const searchEndpointsSchema = `{
  "type": "object",
  "properties": {
    "query": {"type": "string", "description": "检索词，中英文均可（如\"抖音 热榜\"、\"tiktok user videos\"）。检索覆盖端点名/描述/参数说明"},
    "platform": {"type": "string", "description": "可选：平台过滤（douyin/tiktok/xiaohongshu/weibo/bilibili/zhihu/kuaishou/youtube/instagram/twitter 等）"}
  },
  "required": ["query"]
}`

func (t *searchEndpointsTool) Name() string { return "search_endpoints" }
func (t *searchEndpointsTool) Description() string {
	return "搜索可用的社媒查询工具。支持 " +
		strings.Join(tikhubcatalog.Platforms(), "、") +
		" 等平台的内容、账号、搜索、热榜、评论、趋势与公开分析；返回最相关的 " +
		fmt.Sprint(searchTopK) +
		" 个具体工具并立即启用。适用于一次性查询；持续追新请创建任务。"
}
func (t *searchEndpointsTool) Parameters() json.RawMessage {
	return json.RawMessage(searchEndpointsSchema)
}
func (t *searchEndpointsTool) Summarize(json.RawMessage) string { return "" }
func (t *searchEndpointsTool) toolKind() types.ToolCallKind     { return types.ToolCallKindTikHubSearch }

func (t *searchEndpointsTool) Execute(ctx context.Context, _ int64, args json.RawMessage) (string, error) {
	var a struct {
		Query    string `json:"query"`
		Platform string `json:"platform"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return "参数不是合法 JSON，请修正后重试", nil
	}
	a.Query = strings.TrimSpace(a.Query)
	if a.Query == "" {
		return "query 不能为空，请提供检索词（中英文均可）", nil
	}

	hits := tikhubcatalog.Search(a.Query, a.Platform, searchTopK)
	if state := runStateFrom(ctx); state != nil {
		state.candidateSearches++
		state.candidateHits += len(hits)
	}

	// 检索留痕（契约 §6）：无论命中与否都记 query 与候选——零命中的 query 正是
	// 之后优化检索（换分词/加同义词/升级 embedding）最需要的样本。
	if rec := recFrom(ctx); rec != nil {
		rec.RetrievalQuery = a.Query
		if a.Platform != "" {
			rec.RetrievalQuery += " [platform=" + a.Platform + "]"
		}
		for _, h := range hits {
			rec.CandidateTools = append(rec.CandidateTools, h.Entry.Name)
		}
	}

	if len(hits) == 0 {
		msg := "没有检索到匹配端点。可尝试：换关键词（中英文均可）、去掉平台过滤"
		if a.Platform != "" && tikhubcatalog.PlatformCount(strings.ToLower(strings.TrimSpace(a.Platform))) == 0 {
			msg = "平台 " + a.Platform + " 不在目录中。可用平台：" + strings.Join(tikhubcatalog.Platforms(), "、")
		}
		return msg, nil
	}

	state := runStateFrom(ctx)
	var b strings.Builder
	fmt.Fprintf(&b, "检索到 %d 个端点", len(hits))
	if state != nil {
		b.WriteString("（已注入，可直接作为工具调用）")
	}
	b.WriteString("：\n")
	var evicted []string
	for _, h := range hits {
		e := h.Entry
		fmt.Fprintf(&b, "\n## %s\n%s %s [%s]\n%s\n", e.Name, e.Method, e.Path, e.Platform, e.Summary)
		// description 全文（gen 已截 600 rune）只在检索结果里给一次（对抗审查 MEDIUM
		// 漂移缺陷）：注入的工具定义只保留 300 rune 摘要以省每轮预算，其"更全的说明已在
		// 检索结果给过"的前提，正靠这里成立。summary 已单独一行，description 与其重复
		// 时省略，不刷屏。
		if d := strings.TrimSpace(e.Description); d != "" && d != strings.TrimSpace(e.Summary) {
			b.WriteString(d + "\n")
		}
		if len(e.Params) > 0 {
			b.WriteString("参数：")
			for i, p := range e.Params {
				if i > 0 {
					b.WriteString("；")
				}
				b.WriteString(p.Name)
				if p.Required {
					b.WriteString("(必填)")
				}
				if p.Desc != "" {
					b.WriteString(" " + truncateRunes(p.Desc, 80))
				}
			}
			b.WriteString("\n")
		}
		if state != nil {
			if ev := state.activation.activate(e.Name); ev != "" {
				evicted = append(evicted, ev)
			}
		}
	}
	if len(evicted) > 0 {
		fmt.Fprintf(&b, "\n（注入槽位已满，移除了较早注入的：%s——需要时重新检索即可恢复）",
			strings.Join(evicted, "、"))
	}
	return b.String(), nil
}

// ============================================================
// endpointTool：动态注入的端点工具（按次计费面 + daily/统一熔断护栏）
// ============================================================

type endpointTool struct {
	ep    *EndpointTools
	entry tikhubcatalog.Entry
}

func (t *endpointTool) Name() string        { return t.entry.Name }
func (t *endpointTool) Description() string { return endpointDefDescription(t.entry) }
func (t *endpointTool) Parameters() json.RawMessage {
	return endpointParamsSchema(t.entry)
}
func (t *endpointTool) untrustedResult() bool            { return true }
func (t *endpointTool) Summarize(json.RawMessage) string { return "" }
func (t *endpointTool) toolKind() types.ToolCallKind     { return types.ToolCallKindTikHubEndpoint }

func (t *endpointTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	rec := recFrom(ctx)
	if rec != nil {
		rec.EndpointPath = t.entry.Path
	}

	// 滚动 24h 限额判定先于一切上游动作；单消息由统一熔断器负责。
	state := runStateFrom(ctx)
	if t.ep.counter != nil {
		n, err := t.ep.counter.CountTikHubEndpointCallsSince(ctx, time.Now().Add(-dailyCapWindow))
		if err != nil {
			// 限额判定不可用时**拒绝调用**而不是放行：护栏失效即放开计费面，
			// 故障期间宁可少查（fail-closed），与免确认的前提互为代价。
			// 三条纪律（对抗审查 MEDIUM/LOW 缺陷）：
			//  ① 回固定文案而非裸 err——err.Error() 含 AppError.Cause（pgx/DB 内部
			//     细节），经 runToolCalls 的「工具执行失败：」进模型上下文，与本文件
			//     Invoke 分支「只透 Message」的纪律相悖；
			//  ② 记为 budget_exceeded 而非 internal——这次拒绝没打上游、零计费，
			//     计入每日限额 COUNT 会把限额越顶越死（契约 §7 排除条款正为此设）；
			//  ③ 记日志留排查线索（错误链只进日志不进模型）。
			slog.Warn("agent: TikHub 端点每日限额判定失败，fail-closed 拒绝本次调用", "err", err)
			if rec != nil {
				rec.ErrorType = types.ToolErrBudgetExceeded
				rec.Error = err.Error()
			}
			return "端点调用限额检查暂时不可用，本次调用已跳过，请稍后再试。", nil
		}
		if n >= t.ep.dailyCap {
			if rec != nil {
				rec.ErrorType = types.ToolErrBudgetExceeded
			}
			return fmt.Sprintf("过去 24 小时端点调用已达上限（%d 次），为控制成本暂停调用。请明天再试或让用户调整限额。", t.ep.dailyCap), nil
		}
	}

	params, msg := validateEndpointArgs(t.entry, args)
	if msg != "" {
		if rec != nil {
			rec.ErrorType = types.ToolErrInvalidArgs
		}
		return msg, nil
	}

	state.countEndpointCall()
	res, err := t.ep.inv.Invoke(ctx, t.entry, params)
	if err != nil {
		// 上游失败以文案回给模型（可换端点/参数自纠或向用户解释），不上抛——
		// 只透出 AppError.Message，错误链（含内部细节）不进模型上下文。
		if rec != nil {
			rec.ErrorType = errorTypeOf(err)
			rec.Error = err.Error()
		}
		msg := "端点调用失败"
		var ae *types.AppError
		if errors.As(err, &ae) && ae.Message != "" {
			if ae.Retryable {
				return "", err
			}
			msg = "端点调用失败：" + ae.Message
		}
		return msg, nil
	}

	if rec != nil {
		rec.HTTPStatus = &res.Status
		rec.ResultSize = len(res.Body)
	}
	if res.Status < 200 || res.Status > 299 {
		if rec != nil {
			rec.ErrorType = types.ToolErrHTTP
		}
		message := fmt.Sprintf(
			"端点返回 HTTP %d：%s",
			res.Status, truncateRunes(string(res.Body), 500),
		)
		if res.Status == 429 {
			return "", types.NewAppError(
				types.CodeFetchRateLimit, message, nil,
			)
		}
		if res.Status >= 500 {
			return "", types.NewAppError(
				types.CodeFetchTimeout, message, nil,
			)
		}
		return message + "\n（4xx 多为参数问题，可修正后重试）", nil
	}

	raw := string(res.Body)
	limit := state.inlineBudget(t.ep.limits) // 每消息累计预算下的本次额度（由窗口派生）
	body := truncateRunes(raw, limit)
	if state != nil {
		state.inlinedRunes += utf8.RuneCountInString(body)
	}
	if body != raw {
		// 超限：全量进缓存、给句柄——被截部分从此有取回通道（契约 §3.5，
		// 此前只截不给取回是 Boss 实测撞上的死路，也是 Codex 至今被诟病的形态）。
		handle := t.ep.results.put(userID, t.entry.Name, res.Body)
		if state != nil {
			state.allowLocalResultHandle(handle)
		}
		body += buildTruncationNote(handle, len(res.Body), res.Body, res.Truncated, limit)
	}
	return body, nil
}

func (s *toolRunState) countEndpointCall() {
	if s != nil {
		s.endpointCalls++
	}
}

func (s *toolRunState) allowLocalResultHandle(handle string) {
	if s == nil || handle == "" {
		return
	}
	if s.allowedLocalResultHandles == nil {
		s.allowedLocalResultHandles = make(map[string]struct{})
	}
	s.allowedLocalResultHandles[handle] = struct{}{}
}

func (s *toolRunState) allowsLocalResultHandle(handle string) bool {
	if s == nil {
		return false
	}
	_, ok := s.allowedLocalResultHandles[handle]
	return ok
}

func (s *toolRunState) hasLocalResultHandles() bool {
	return s != nil && len(s.allowedLocalResultHandles) > 0
}

// validateEndpointArgs 校验模型产出的参数：未知参数与必填缺失都给出确定性
// 自纠文案（列出合法参数集），类型宽松放行（invoker 字符串化，语义校验交上游 422）。
//
// UseNumber（对抗审查 HIGH 缺陷）：社媒 ID 多为雪花级大整数（TikTok uid ~6.8e18 >
// 2^53），普通 json.Unmarshal 落 float64 会静默丢精度 → 向上游查错对象。用 UseNumber
// 让数字保持 json.Number（原始十进制串），invoker 的 toString 原样透传、body 侧
// json.Marshal 天然保真。
func validateEndpointArgs(entry tikhubcatalog.Entry, args json.RawMessage) (map[string]any, string) {
	params := map[string]any{}
	if len(args) > 0 {
		dec := json.NewDecoder(bytes.NewReader(args))
		dec.UseNumber()
		if err := dec.Decode(&params); err != nil {
			return nil, "参数不是合法 JSON，请修正后重试"
		}
	}
	known := make(map[string]bool, len(entry.Params))
	var names []string
	for _, p := range entry.Params {
		known[p.Name] = true
		names = append(names, p.Name)
	}
	for k := range params {
		if !known[k] {
			return nil, fmt.Sprintf("未知参数 %q。%s 的合法参数：%s", k, entry.Name, strings.Join(names, "、"))
		}
	}
	for _, p := range entry.Params {
		if !p.Required {
			continue
		}
		if v, ok := params[p.Name]; !ok || v == nil || v == "" {
			return nil, fmt.Sprintf("缺少必填参数 %q（%s）", p.Name, p.Desc)
		}
	}
	return params, ""
}

// errorTypeOf 把调用错误映射为低基数分类（tool_calls.error_type）。
func errorTypeOf(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return types.ToolErrTimeout
	}
	var ae *types.AppError
	if errors.As(err, &ae) {
		switch ae.Code {
		case types.CodeFetchTimeout:
			return types.ToolErrTimeout
		case types.CodeValidation:
			return types.ToolErrInvalidArgs
		}
	}
	return types.ToolErrInternal
}

// truncateRunes 按 rune 截断（绝不产生非法 UTF-8），超限加省略号。
func truncateRunes(s string, max int) string {
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	return string([]rune(s)[:max]) + "…"
}
