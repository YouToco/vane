// TikHub 端点注册表的 agent 工具面（端点注册表契约 §3/§4/§7）：
//
//   - tool_search 检索元工具：在已授权的通用 toolsearch 目录上检索，
//     命中的端点**激活**进会话（动态注入为一等 FC 工具，业内 Tool Search /
//     retrieve-then-inject 模式，Boss 拍板 2026-07-18 选注入而非通用转发）。
//   - endpointTool 动态端点工具：模型声明原字节来自 AgentDefinition，
//     Execute 按同名 AgentLookup 路由到现有 tikhubinvoke，结果原文（截断）回给模型。
//
// 三条硬边界：
//   - 端点工具按本地 ToolPolicy 声明网络读取、计费与 taint；
//     因此它们永远不进 task_creation_operations，也不能进入 owner 写操作边界。
//   - 白名单语义（M4 契约 §10）扩展为「静态工具 ∪ 会话已激活端点」：模型编造的
//     端点名（哪怕真在注册表里）只要没被本会话 tool_search 激活过，一律拒绝——
//     激活集是显式审计过的调用面，跳过检索直呼端点名是绕过检索留痕的旁门。
//   - 免确认的调用受滚动 24h EndpointDailyCap 与 Agent 统一单消息隐藏熔断器保护。
package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/YouToco/vane/server/internal/strictjson"
	"github.com/YouToco/vane/server/llm"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/tikhubcatalog"
	"github.com/YouToco/vane/server/tikhubinvoke"
	"github.com/YouToco/vane/server/toolsearch"
	"github.com/YouToco/vane/server/types"
)

const (
	// 动态工具是单会话权限：名额和 schema 字节必须同时满足。
	// 新命中批次以全或无方式加入，超限不逐出旧工具也不部分激活。
	maxActivatedEndpoints      = 16
	maxDynamicSchemaBytes      = 64 << 10
	defaultToolSearchLimit     = 8
	maxToolSearchQueryBytes    = 512
	maxToolSearchPlatformBytes = 64
	// 端点结果的内联上限**不再是常量**：由 agent 模型声明的上下文窗口派生
	// （llm.DeriveInlineLimits，OpenClaw 同款分档 + 窗口 30% 封顶），随
	// EndpointTools 注入。写死过一次的教训见 llm/context.go 头注——6000 rune
	// 是 64K 窗口时代拍的值，模型换到 1M 后没人记得改，直到 Boss 在生产撞见截断。
	//
	// endpointResultFallbackRunes 仅用于装配缺失时的兜底（NewEndpointTools 收到
	// 非法窗口值），不参与正常路径。
	endpointResultFallbackRunes = 16000
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
	ownerRequest         string
	clarifiedOwnerAction string
	manageTasksResult    string
	intents              ToolIntent
	// memoryMutationRequired is derived only from the trusted current owner
	// request. It never authorizes a write; it only prevents the model from
	// claiming an explicit remember/correct/forget request completed before a
	// real manage_memory attempt produced a tool receipt.
	memoryMutationRequired         bool
	memoryMutationResponseRejected bool
	manageMemoryResult             string
	agentFirstEnabled              bool
	// Unified loop breaker state. Provider-specific message caps are not used
	// for planning; this hidden ceiling only stops repeated or runaway calls.
	toolExecutions     int
	successfulCalls    map[string]struct{}
	failedCalls        map[string]int
	toolEvidence       []store.AgentToolEvidenceV1
	actionReceipts     []json.RawMessage
	internalReferences map[string]struct{}
	// compartmentedResearch freezes exact, authenticated internal query
	// evidence before the first untrusted public read. Raw public content stays
	// in the isolated continuation only; the final no-tool synthesis receives
	// this frozen evidence plus a strict public summary.
	compartmentedResearch *compartmentedResearchState
	// publicEvidence is a per-turn, immutable sidecar. Historical external
	// tool rows are projected to opaque refs before the main Agent sees them;
	// current external invocations use the same ref format. Raw bytes are only
	// materialized by the compartmented public-evidence stage.
	publicEvidence      map[string]publicEvidenceRecord
	publicEvidenceOrder []string
	// historicalPublicPending records that this turn projected authenticated
	// historical public bytes into the sidecar. The next current public read
	// consumes it through the normal compartment, while direct text convergence
	// is intercepted and summarized without tools.
	historicalPublicPending bool
	// publicEvidenceDisplayURLs is keyed by the provider tool-call ID and is
	// overwritten by that exact web_search invocation. It must never reuse the
	// turn-wide accumulated grounding list, because refs are invocation-local.
	publicEvidenceDisplayURLs map[string][]string
	deferEvidenceCommit       bool
	// webActionMode is derived only by the authenticated Web task-action
	// ingress. It is a hard execution capability, not model context: create can
	// only create one task; edit can only edit webSelectedTaskRef.
	webActionMode    webActionMode
	webActionClaimed bool
	// webSelectedTaskRef comes from the authenticated Dashboard edit route.
	// It is internal model context only and is always redacted from replies.
	webSelectedTaskRef string
	loopBreakReason    string
	clarificationCount int
	candidateSearches  int
	candidateHits      int
	candidateSlots     int
	// groundedBrief confines a trusted internal Brief/report follow-up to the
	// exact supplied artifact. It has no tool surface at declaration or
	// execution time, so source text can inform an answer but can never trigger
	// network research, a durable proposal, or a write.
	groundedBrief bool
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

type webActionMode uint8

const (
	webActionNone webActionMode = iota
	webActionCreate
	webActionEdit
)

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

// activationState 会话的激活端点集合。顺序即注入顺序：只在尾部追加，
// 存量前缀在会话内恒稳定。超过名额/schema 预算时整批拒绝，不逐出已激活工具；
// 否则模型可通过反复检索无声改写旧轮工具权限。saveSession 每次全量写回。
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
		return a
	}
	seen := make(map[string]struct{}, len(a.names))
	for _, name := range a.names {
		if name == "" || strings.TrimSpace(name) != name || len(name) > 255 {
			a.names = nil
			return a
		}
		if _, duplicate := seen[name]; duplicate {
			a.names = nil
			return a
		}
		seen[name] = struct{}{}
	}
	if len(a.names) > maxActivatedEndpoints {
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

// activateBatch 把命中工具去重后原子加入激活集。验证基于将在下一轮
// 真正注入的 AgentDefinition，因此计数、字节预算与模型可见 schema 同源。
func (a *activationState) activateBatch(c endpointCatalog, names []string) error {
	if a == nil {
		return errors.New("tool_search: activation state is unavailable")
	}
	prospective := append([]string(nil), a.names...)
	seen := make(map[string]struct{}, len(prospective)+len(names))
	for _, name := range prospective {
		seen[name] = struct{}{}
	}
	for _, name := range names {
		if _, duplicate := seen[name]; duplicate {
			continue
		}
		seen[name] = struct{}{}
		prospective = append(prospective, name)
	}
	if _, err := validatedActivationDefinitions(c, prospective); err != nil {
		return err
	}
	a.names = prospective
	return nil
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

// endpointCatalog 把通用模型目录与供应商路由表收窄成 Agent 所需的边界。
// 生产实现始终是 tikhubcatalog；测试可用小目录精确覆盖 64KiB 门槛。
type endpointCatalog interface {
	SearchTools(query, platform string, limit int) ([]toolsearch.Match, error)
	AgentDefinition(name string) (toolsearch.Entry, bool)
	AgentLookup(name string) (tikhubcatalog.Entry, bool)
	Platforms() []string
	PlatformCount(platform string) int
	Digest() string
}

type productionEndpointCatalog struct{}

func (productionEndpointCatalog) SearchTools(query, platform string, limit int) ([]toolsearch.Match, error) {
	return tikhubcatalog.SearchTools(query, platform, limit)
}
func (productionEndpointCatalog) AgentDefinition(name string) (toolsearch.Entry, bool) {
	return tikhubcatalog.AgentDefinition(name)
}
func (productionEndpointCatalog) AgentLookup(name string) (tikhubcatalog.Entry, bool) {
	return tikhubcatalog.AgentLookup(name)
}
func (productionEndpointCatalog) Platforms() []string { return tikhubcatalog.Platforms() }
func (productionEndpointCatalog) PlatformCount(platform string) int {
	return tikhubcatalog.PlatformCount(platform)
}
func (productionEndpointCatalog) Digest() string { return tikhubcatalog.AgentCatalogDigest() }

// EndpointTools 持有端点工具面的共享依赖，随 Deps 注入 Loop；nil 表示该能力
// 未装配（tikhub key 缺失等），agent 退化为纯静态工具面，行为与本特性上线前一致。
type EndpointTools struct {
	inv      endpointInvoker
	catalog  endpointCatalog
	counter  endpointCallCounter
	dailyCap int
	results  *resultCache // 大结果缓存（契约 §3.5：截断句柄 + read_endpoint_result 取回）
	// limits 由 agent 模型的上下文窗口派生（llm.DeriveInlineLimits）：内联多少
	// 内容是模型属性不是代码常量，模型换代时自动跟随，无需有人记得改这里。
	limits llm.InlineLimits
}

// NewEndpointTools 构造端点工具面。dailyCap ≤0 时兜底为保守默认
// （装配疏漏不能变成无限额）。
// contextTokens 是 agent 模型声明的上下文窗口（llm.ContextWindowTokens）；
// ≤0 时按保守兜底档派生，绝不因装配疏漏放大内联量。
func NewEndpointTools(
	inv endpointInvoker,
	counter endpointCallCounter,
	dailyCap, contextTokens int,
) *EndpointTools {
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
	return &EndpointTools{inv: inv, catalog: productionEndpointCatalog{}, counter: counter, dailyCap: dailyCap,
		results: newResultCache(), limits: limits}
}

// SearchTool 返回检索元工具（进静态白名单）。检索会修改
// 本消息 activation，但现有调用与预算仍完全由工具自身管理。
func (e *EndpointTools) SearchTool() ToolSpec {
	return newToolSpec(&toolSearchTool{ep: e}, withToolSurface(ownerPolicy(
		Effects(EffectActivationWrite),
		BudgetNone),
		ExposureAlways, IntentSocialResearch, ResultTrustLocal, false))
}

// ReadResultTool 返回大结果取回工具（进静态白名单；契约 §3.5）。
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
	if e == nil || e.inv == nil || act == nil || !act.contains(name) {
		return ToolSpec{}, false
	}
	e.pruneActivation(act)
	if !act.contains(name) {
		return ToolSpec{}, false
	}
	definitions, err := validatedActivationDefinitions(e.catalog, act.names)
	if err != nil {
		return ToolSpec{}, false
	}
	var definition toolsearch.Entry
	for _, candidate := range definitions {
		if candidate.Name == name {
			definition = candidate
			break
		}
	}
	if definition.Name == "" {
		return ToolSpec{}, false
	}
	entry, ok := e.catalog.AgentLookup(name)
	if !ok {
		return ToolSpec{}, false
	}
	return newToolSpec(&endpointTool{ep: e, entry: entry, definition: definition}, withToolSurface(ownerPolicy(
		Effects(EffectNetworkRead, EffectBillable, EffectTrustTaint),
		BudgetToolManaged),
		ExposureContext, IntentSocialResearch, ResultTrustExternal, false)), true
}

// Defs 渲染激活端点的 FC 工具声明，顺序 = 激活顺序（前缀稳定性见 activationState）。
func (e *EndpointTools) Defs(act *activationState) []llm.ToolDef {
	if e == nil || e.inv == nil || act == nil || len(act.names) == 0 {
		return nil
	}
	e.pruneActivation(act)
	definitions, err := validatedActivationDefinitions(e.catalog, act.names)
	if err != nil {
		return nil
	}
	defs := make([]llm.ToolDef, 0, len(definitions))
	for _, definition := range definitions {
		defs = append(defs, llm.ToolDef{
			Name:        definition.Name,
			Description: definition.Description,
			Parameters:  append(json.RawMessage(nil), definition.Parameters...),
		})
	}
	return defs
}

// pruneActivation removes names whose current model definition or invocation
// route has disappeared. It runs before every declaration and resolution, so
// a catalog/policy revocation takes effect without trusting persisted session
// names. An aggregate budget violation clears the whole set rather than
// choosing an attacker-controlled subset.
func (e *EndpointTools) pruneActivation(act *activationState) {
	if e == nil || e.catalog == nil || act == nil || len(act.names) == 0 {
		return
	}
	retained := make([]string, 0, len(act.names))
	totalSchemaBytes := 0
	for _, name := range act.names {
		definitions, err := validatedActivationDefinitions(e.catalog, []string{name})
		if err != nil {
			continue
		}
		totalSchemaBytes += len(definitions[0].Parameters)
		if len(retained)+1 > maxActivatedEndpoints || totalSchemaBytes > maxDynamicSchemaBytes {
			act.names = nil
			return
		}
		retained = append(retained, name)
	}
	act.names = retained
}

func validatedActivationDefinitions(c endpointCatalog, names []string) ([]toolsearch.Entry, error) {
	if c == nil {
		return nil, errors.New("tool_search: catalog is unavailable")
	}
	if len(names) > maxActivatedEndpoints {
		return nil, fmt.Errorf("tool_search: dynamic tool count exceeds %d", maxActivatedEndpoints)
	}
	definitions := make([]toolsearch.Entry, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	totalSchemaBytes := 0
	for _, name := range names {
		if name == "search_endpoints" {
			return nil, errors.New("tool_search: retired search_endpoints cannot be activated")
		}
		if name == "" || strings.TrimSpace(name) != name {
			return nil, errors.New("tool_search: invalid activated tool name")
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, errors.New("tool_search: duplicate activated tool name")
		}
		seen[name] = struct{}{}
		definition, ok := c.AgentDefinition(name)
		if !ok || definition.Name != name || definition.Description == "" ||
			len(definition.Parameters) == 0 || !json.Valid(definition.Parameters) {
			return nil, fmt.Errorf("tool_search: invalid model definition for %q", name)
		}
		providerEntry, ok := c.AgentLookup(name)
		if !ok || providerEntry.Name != name {
			return nil, fmt.Errorf("tool_search: invocation route missing for %q", name)
		}
		totalSchemaBytes += len(definition.Parameters)
		if totalSchemaBytes > maxDynamicSchemaBytes {
			return nil, fmt.Errorf("tool_search: dynamic schemas exceed %d bytes", maxDynamicSchemaBytes)
		}
		definitions = append(definitions, definition)
	}
	return definitions, nil
}

// endpointSystemNote 注入 system prompt 的能力说明（仅 Endpoints 装配时，loop.New）。
// 官方最佳实践：system prompt 写明「可搜索的工具类别」，模型才会主动想到去搜。
func endpointSystemNote(e *EndpointTools) string {
	platforms := tikhubcatalog.Platforms()
	if e != nil && e.catalog != nil {
		platforms = e.catalog.Platforms()
	}
	return "\n\n[社媒数据查询]\n用户想查询/调研社媒平台的内容、账号、评论、热榜等一次性问题时，" +
		"先用 tool_search 搜索可用的社媒查询工具（覆盖内容、账号、搜索、热榜、评论、趋势和公开分析；平台：" +
		strings.Join(platforms, "、") + "），命中的具体能力会在下一轮成为可直接调用的工具。" +
		"检索不到就换关键词（中英文都可）或加 platform 过滤再试。端点可能按次计费且受滚动 24 小时成本护栏约束；" +
		"用户要的是**持续追新**时不要用端点查询，改用 manage_tasks 创建任务。"
}

// ============================================================
// tool_search：检索元工具（静态白名单成员，读工具免确认）
// ============================================================

type toolSearchTool struct {
	ep *EndpointTools
}

const toolSearchSchema = `{
  "type": "object",
  "properties": {
    "query": {"type": "string", "description": "检索词，中英文均可，最多 512 UTF-8 字节"},
    "platform": {"type": "string", "description": "可选的平台硬过滤（如 douyin/tiktok/xiaohongshu/weibo/youtube）"},
    "limit": {"type": "integer", "minimum": 1, "maximum": 8, "default": 8, "description": "返回并激活的最大工具数"}
  },
  "required": ["query"],
  "additionalProperties": false
}`

func (t *toolSearchTool) Name() string { return "tool_search" }
func (t *toolSearchTool) Description() string {
	return "搜索已授权的可调用工具。当前支持 " +
		strings.Join(t.ep.catalog.Platforms(), "、") +
		" 等社媒平台；命中后完整参数 schema 会在下一轮注入。适用于一次性查询；持续追新请创建任务。"
}
func (t *toolSearchTool) Parameters() json.RawMessage {
	return json.RawMessage(toolSearchSchema)
}
func (t *toolSearchTool) Summarize(json.RawMessage) string { return "" }
func (t *toolSearchTool) toolKind() types.ToolCallKind     { return types.ToolCallKindTikHubSearch }

const toolSearchAuditSchema = "tool_search.audit/v1"

type toolSearchAuditV1 struct {
	SchemaVersion  string `json:"schema_version"`
	Status         string `json:"status"`
	QuerySHA256    string `json:"query_sha256,omitempty"`
	QueryBytes     int    `json:"query_bytes"`
	ArgumentBytes  int    `json:"argument_bytes"`
	PlatformSHA256 string `json:"platform_sha256,omitempty"`
	PlatformBytes  int    `json:"platform_bytes,omitempty"`
	Limit          int    `json:"limit,omitempty"`
	CatalogDigest  string `json:"catalog_digest,omitempty"`
	CandidateCount int    `json:"candidate_count"`
	Truncated      bool   `json:"truncated"`
}

type toolSearchArgumentSummaryV1 struct {
	SchemaVersion  string `json:"schema_version"`
	QuerySHA256    string `json:"query_sha256,omitempty"`
	QueryBytes     int    `json:"query_bytes"`
	ArgumentBytes  int    `json:"argument_bytes"`
	PlatformSHA256 string `json:"platform_sha256,omitempty"`
	PlatformBytes  int    `json:"platform_bytes,omitempty"`
	Limit          int    `json:"limit,omitempty"`
}

// recordToolSearchAudit uses only the existing tool_calls receipt. Raw query
// and platform text are replaced by SHA-256 summaries before persistence;
// schemas, descriptions and provider routing never enter the audit fields.
// Truncated means the requested rank window was full or at least one
// model-facing description was clipped in the bounded search result.
func (t *toolSearchTool) recordAudit(
	ctx context.Context,
	argumentBytes int,
	query, platform string,
	limit int,
	status string,
	truncated bool,
	hits []toolsearch.Match,
) {
	rec := recFrom(ctx)
	if rec == nil {
		return
	}
	queryDigest := summarizedTextDigest(query)
	platformDigest := summarizedTextDigest(platform)
	catalogDigest := ""
	if t != nil && t.ep != nil && t.ep.catalog != nil {
		catalogDigest = t.ep.catalog.Digest()
	}
	audit := toolSearchAuditV1{
		SchemaVersion: toolSearchAuditSchema,
		Status:        status,
		QuerySHA256:   queryDigest, QueryBytes: len(query),
		ArgumentBytes:  argumentBytes,
		PlatformSHA256: platformDigest, PlatformBytes: len(platform),
		Limit: limit, CatalogDigest: catalogDigest,
		CandidateCount: len(hits), Truncated: truncated,
	}
	auditRaw, err := json.Marshal(audit)
	if err != nil {
		return
	}
	argumentSummary, err := json.Marshal(toolSearchArgumentSummaryV1{
		SchemaVersion: toolSearchAuditSchema,
		QuerySHA256:   queryDigest, QueryBytes: len(query),
		ArgumentBytes:  argumentBytes,
		PlatformSHA256: platformDigest, PlatformBytes: len(platform),
		Limit: limit,
	})
	if err != nil {
		return
	}
	rec.Arguments = argumentSummary
	rec.RetrievalQuery = string(auditRaw)
	rec.CandidateTools = rec.CandidateTools[:0]
	for _, hit := range hits {
		rec.CandidateTools = append(rec.CandidateTools,
			hit.Entry.Name+"\tscore="+strconv.FormatFloat(hit.Score, 'g', -1, 64))
	}
	switch status {
	case "invalid":
		rec.ErrorType = types.ToolErrInvalidArgs
	case "error":
		rec.ErrorType = types.ToolErrInternal
	default:
		rec.ErrorType = ""
	}
}

func summarizedTextDigest(value string) string {
	if value == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", digest)
}

func toolSearchResultTruncated(hits []toolsearch.Match, limit int) bool {
	if limit > 0 && len(hits) == limit {
		return true
	}
	for _, hit := range hits {
		if utf8.RuneCountInString(hit.Entry.Description) > 400 {
			return true
		}
	}
	return false
}

func (t *toolSearchTool) Execute(ctx context.Context, _ int64, args json.RawMessage) (string, error) {
	argumentBytes := len(args)
	query := ""
	platform := ""
	limit := 0
	record := func(status string, hits []toolsearch.Match, truncated bool) {
		t.recordAudit(ctx, argumentBytes, query, platform, limit, status, truncated, hits)
	}
	if t == nil || t.ep == nil || t.ep.inv == nil || t.ep.catalog == nil {
		record("error", nil, false)
		return "工具检索与调用能力未授权，本次已跳过", nil
	}
	var a struct {
		Query    string          `json:"query"`
		Platform string          `json:"platform,omitempty"`
		Limit    json.RawMessage `json:"limit,omitempty"`
	}
	if err := strictjson.DecodeExact(args, &a); err != nil {
		record("invalid", nil, false)
		return "参数不是合法 JSON，请修正后重试", nil
	}
	query = a.Query
	platform = a.Platform
	if len(a.Query) > maxToolSearchQueryBytes {
		record("invalid", nil, false)
		return fmt.Sprintf("query 超过 %d UTF-8 字节，请缩短后重试", maxToolSearchQueryBytes), nil
	}
	if len(a.Platform) > maxToolSearchPlatformBytes {
		record("invalid", nil, false)
		return fmt.Sprintf("platform 超过 %d UTF-8 字节", maxToolSearchPlatformBytes), nil
	}
	if !utf8.ValidString(a.Query) || !utf8.ValidString(a.Platform) {
		record("invalid", nil, false)
		return "query 和 platform 必须是合法 UTF-8 文本", nil
	}
	a.Query = strings.TrimSpace(a.Query)
	query = a.Query
	if a.Query == "" {
		record("invalid", nil, false)
		return "query 不能为空，请提供检索词（中英文均可）", nil
	}
	a.Platform = strings.ToLower(strings.TrimSpace(a.Platform))
	platform = a.Platform
	limit = defaultToolSearchLimit
	if len(a.Limit) != 0 {
		if err := strictjson.DecodeExact(a.Limit, &limit); err != nil {
			record("invalid", nil, false)
			return "limit 必须是 1–8 的整数", nil
		}
	}
	if limit < 1 || limit > defaultToolSearchLimit {
		record("invalid", nil, false)
		return fmt.Sprintf("limit 必须在 1–%d 之间", defaultToolSearchLimit), nil
	}

	hits, err := t.ep.catalog.SearchTools(a.Query, a.Platform, limit)
	if err != nil {
		record("error", nil, false)
		return "工具目录检索失败，请缩短或调整检索词后重试", nil
	}
	if state := runStateFrom(ctx); state != nil {
		state.candidateSearches++
		state.candidateHits += len(hits)
		state.candidateSlots += limit
	}

	if len(hits) == 0 {
		record("zero", nil, false)
		msg := "没有检索到匹配端点。可尝试：换关键词（中英文均可）、去掉平台过滤"
		if a.Platform != "" && t.ep.catalog.PlatformCount(a.Platform) == 0 {
			msg = "指定平台不在目录中。可用平台：" + strings.Join(t.ep.catalog.Platforms(), "、")
		}
		return msg, nil
	}

	state := runStateFrom(ctx)
	if state != nil {
		if state.activation == nil {
			record("error", hits, toolSearchResultTruncated(hits, limit))
			return "工具激活状态不可用，本次命中未注入", nil
		}
		names := make([]string, 0, len(hits))
		for _, hit := range hits {
			names = append(names, hit.Entry.Name)
		}
		t.ep.pruneActivation(state.activation)
		if err := state.activation.activateBatch(t.ep.catalog, names); err != nil {
			record("error", hits, toolSearchResultTruncated(hits, limit))
			return fmt.Sprintf("动态工具上限为 %d 个且 schema 总量不超过 %d 字节；本批已全部拒绝，请新建会话或缩小检索范围。",
				maxActivatedEndpoints, maxDynamicSchemaBytes), nil
		}
	}
	record("success", hits, toolSearchResultTruncated(hits, limit))
	var b strings.Builder
	fmt.Fprintf(&b, "检索到 %d 个工具", len(hits))
	if state != nil {
		b.WriteString("（已注入，下一轮可直接调用）")
	}
	b.WriteString("：\n")
	for _, h := range hits {
		definition := h.Entry
		fmt.Fprintf(&b, "\n## %s [%s]\n%s\n", definition.Name,
			definition.Namespace, truncateRunes(definition.Description, 400))
	}
	return b.String(), nil
}

// ============================================================
// endpointTool：动态注入的端点工具（按次计费面 + daily/统一熔断护栏）
// ============================================================

type endpointTool struct {
	ep         *EndpointTools
	entry      tikhubcatalog.Entry
	definition toolsearch.Entry
}

func (t *endpointTool) Name() string        { return t.definition.Name }
func (t *endpointTool) Description() string { return t.definition.Description }
func (t *endpointTool) Parameters() json.RawMessage {
	return append(json.RawMessage(nil), t.definition.Parameters...)
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
