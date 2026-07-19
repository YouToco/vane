// TikHub 端点注册表的 agent 工具面（端点注册表契约 §3/§4/§7）：
//
//   - search_endpoints 检索元工具：在 tikhubcatalog（1002 端点）上 BM25 检索，
//     命中的端点**激活**进会话（动态注入为一等 FC 工具，业内 Tool Search /
//     retrieve-then-inject 模式，Boss 拍板 2026-07-18 选注入而非通用转发）。
//   - endpointTool 动态端点工具：按 catalog Entry 即时构造，参数 schema 从注册表
//     生成，Execute 走 tikhubinvoke 通用调用器，结果原文（截断）回给模型阅读。
//
// 三条硬边界：
//   - 端点工具**全部只读**（Mutating=false）：查询社媒数据不改系统状态，免确认卡
//     （Boss 拍板）；因此它们永远不进 pending_actions，ExecuteAction 路径只需静态白名单。
//   - 白名单语义（M4 契约 §10）扩展为「静态工具 ∪ 会话已激活端点」：模型编造的
//     端点名（哪怕真在注册表里）只要没被本会话 search_endpoints 激活过，一律拒绝——
//     激活集是显式审计过的调用面，跳过检索直呼端点名是绕过检索留痕的旁门。
//   - 免确认的代价用双重限额兜底（契约 §7）：单条消息 EndpointMsgCap 次 +
//     滚动 24h EndpointDailyCap 次，超限回文案让模型向用户解释，绝不静默熔断。
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
	// 必须先降本上限**（TestToolCountBudget 是账单）。账目沿革：2026-07-18
	// read_endpoint_result 入列 15→14（14 静态 + 2 端点工具 + 14 激活 = 29 顶线）；
	// 2026-07-19 set_task_strictness 入列（任务门槛档位拍板）14→13，
	// 15 静态 + 2 端点工具* + 13 激活 = 29 压线内（*端点工具计入 BuildTools 返回值）。
	maxActivatedEndpoints = 13
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
	// endpointCalls 本条消息内已发起的端点调用次数（限额判定用）。计数时机在
	// 校验通过之后、发请求之前：打到上游才算（含 HTTP 错误——失败同样计费），
	// 参数校验失败没打上游、不计费，不吃限额。
	endpointCalls int
	// inlinedRunes 本条消息内已内联进上下文的端点结果字符数（累计预算，
	// 见 llm.InlineLimits.MsgBudget）。只统计真正回给模型的部分，不含缓存全量。
	inlinedRunes int
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

// SearchTool 返回检索元工具（进静态白名单，BuildTools 装配）。
func (e *EndpointTools) SearchTool() Tool { return &searchEndpointsTool{ep: e} }

// ReadResultTool 返回大结果取回工具（进静态白名单，BuildTools 装配；契约 §3.5）。
func (e *EndpointTools) ReadResultTool() Tool {
	return &readEndpointResultTool{cache: e.results, perRead: e.limits.PerCall}
}

// Resolve 按白名单语义解析动态端点工具：必须**已激活**且仍在注册表里。
// 注册表里存在但未激活 → 不解析（见文件头注第二条硬边界）；
// 已激活但注册表已无此端点（re-gen 下线）→ 不解析，模型收到标准"工具不存在"自纠文案。
func (e *EndpointTools) Resolve(name string, act *activationState) (Tool, bool) {
	if act == nil || !act.contains(name) {
		return nil, false
	}
	entry, ok := tikhubcatalog.Lookup(name)
	if !ok {
		return nil, false
	}
	return &endpointTool{ep: e, entry: entry}, true
}

// Defs 渲染激活端点的 FC 工具声明，顺序 = 激活顺序（前缀稳定性见 activationState）。
func (e *EndpointTools) Defs(act *activationState) []llm.ToolDef {
	if act == nil || len(act.names) == 0 {
		return nil
	}
	defs := make([]llm.ToolDef, 0, len(act.names))
	for _, name := range act.names {
		entry, ok := tikhubcatalog.Lookup(name)
		if !ok {
			continue // 注册表下线的端点不再注入；Resolve 同步拒绝，两处口径一致
		}
		defs = append(defs, llm.ToolDef{
			Name:        entry.Name,
			Description: endpointDefDescription(entry),
			Parameters:  endpointParamsSchema(entry),
		})
	}
	return defs
}

// endpointDefDescription 动态工具的注入描述：摘要 + 截断说明 + 计费提醒。
func endpointDefDescription(entry tikhubcatalog.Entry) string {
	desc := entry.Summary
	if entry.Description != "" {
		desc += "\n" + truncateRunes(entry.Description, endpointDefDescMaxRunes)
	}
	return desc + "\n（TikHub " + entry.Method + " " + entry.Path + "，按次计费，按需调用）"
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
			prop["description"] = p.Desc
		}
		if len(p.Enum) > 0 {
			prop["enum"] = p.Enum
		}
		if p.Default != nil {
			prop["default"] = p.Default
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

// endpointSystemNote 注入 system prompt 的能力说明（仅 Endpoints 装配时，loop.New）。
// 官方最佳实践：system prompt 写明「可搜索的工具类别」，模型才会主动想到去搜。
func endpointSystemNote() string {
	return "\n\n[社媒数据查询]\n用户想查询/调研社媒平台的内容、账号、评论、热榜等一次性问题时，" +
		"先用 search_endpoints 在 TikHub 端点目录（" + fmt.Sprint(tikhubcatalog.Len()) + " 个端点，平台：" +
		strings.Join(tikhubcatalog.Platforms(), "、") + "）中检索数据接口，命中的端点会成为你可直接调用的工具。" +
		"检索不到就换关键词（中英文都可）或加 platform 过滤再试。端点按次计费且有单条消息与每日限额，按需少量调用；" +
		"用户要的是**持续追新**时不要用端点查询，改用 add_source 订阅信源。"
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
	return "在 TikHub 社媒数据端点目录（" + fmt.Sprint(tikhubcatalog.Len()) +
		" 个端点）中检索接口。返回最相关的 " + fmt.Sprint(searchTopK) +
		" 个端点并注入为你可直接调用的工具。适用于一次性查询社媒内容/账号/热榜/评论；持续追新请用 add_source。"
}
func (t *searchEndpointsTool) Parameters() json.RawMessage {
	return json.RawMessage(searchEndpointsSchema)
}
func (t *searchEndpointsTool) Mutating() bool                   { return false }
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
// endpointTool：动态注入的端点工具（按次计费面，双重限额）
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
func (t *endpointTool) Mutating() bool                   { return false }
func (t *endpointTool) Summarize(json.RawMessage) string { return "" }
func (t *endpointTool) toolKind() types.ToolCallKind     { return types.ToolCallKindTikHubEndpoint }

func (t *endpointTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	rec := recFrom(ctx)
	if rec != nil {
		rec.EndpointPath = t.entry.Path
	}

	// 限额判定先于一切上游动作（契约 §7）。
	state := runStateFrom(ctx)
	if state != nil && state.endpointCalls >= t.ep.msgCap {
		if rec != nil {
			rec.ErrorType = types.ToolErrBudgetExceeded
		}
		return fmt.Sprintf("本条消息的端点调用已达上限（%d 次）。请基于已有结果回答，或让用户再发一条消息继续。", t.ep.msgCap), nil
	}
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
		// 只透出 AppError.Message，错误链（含内部细节）不进模型上下文（同 push_now）。
		if rec != nil {
			rec.ErrorType = errorTypeOf(err)
			rec.Error = err.Error()
		}
		msg := "端点调用失败"
		var ae *types.AppError
		if errors.As(err, &ae) && ae.Message != "" {
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
		return fmt.Sprintf("端点返回 HTTP %d：%s\n（4xx 多为参数问题，可修正后重试；429 是限流，请稍后或换端点）",
			res.Status, truncateRunes(string(res.Body), 500)), nil
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
		body += buildTruncationNote(handle, len(res.Body), res.Body, res.Truncated, limit)
	}
	return body, nil
}

// countEndpointCall 计入本条消息的端点调用数。state 为 nil 时静默跳过：
// 端点工具只能经 HandleMessage 的动态解析到达（Resolve 依赖激活集），
// state 恒在；nil 只在未来误用时出现，跳过比 panic 稳。
func (s *toolRunState) countEndpointCall() {
	if s != nil {
		s.endpointCalls++
	}
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
