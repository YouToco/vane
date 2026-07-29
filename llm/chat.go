package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

// 本文件实现多轮 + function calling 的 Chat 调用（M4 契约 §4）。
// 与 Complete 并存而非替代：Complete 面向"单 system+user、纯文本回复"的
// 结构化任务（打分/出卡），Chat 面向 agent loop 的多轮工具调用。
// 两者共用同一信号量、错误映射与空 content 告警语义。

// ChatMessage 多轮对话中的一条消息。json tag 同时是 agent 会话在
// agent_sessions.messages 里的持久化格式（契约 §1/§7），字段增删要
// 考虑历史会话数据的兼容性，不可随意改名。
type ChatMessage struct {
	Role       string     `json:"role"` // system|user|assistant|tool
	Content    string     `json:"content"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`   // role=assistant 且有调用时
	ToolCallID string     `json:"tool_call_id,omitempty"` // role=tool 必填
}

// ToolCall 模型发起的一次工具调用。Arguments 保留原始 JSON 字符串不解析：
// schema 校验与解码是工具执行方的职责，这里只负责透传；且回传历史时
// 上游要求 arguments 原样，提前解析再重序列化反而可能改变字段顺序。
type ToolCall struct {
	ID        string `json:"id"`
	Name      string `json:"name"`      // function.name
	Arguments string `json:"arguments"` // function.arguments 原始 JSON 字符串
}

// ToolDef 声明一个可调用工具（发给上游 tools 列表的语义部分，
// 线协议的 {type:"function",function:{...}} 包装由 Chat 内部补齐）。
type ToolDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"` // JSON schema
}

// ChatRequest 多轮对话请求。与 Request 的差异：显式 messages（含历史）、
// 可带 tools、可按次覆盖模型——agent 用 cfg.LLM.AgentModel（推理型），
// 其余调用仍走 Client 默认 model，避免为一个调用面再建一个 Client。
type ChatRequest struct {
	Model           string // 空串用 Client 默认 model；agent 传 cfg.LLM.AgentModel
	Messages        []ChatMessage
	Tools           []ToolDef
	Temperature     *float32 // nil = 不传该字段
	MaxTokens       *int     // nil = 不传该字段
	DisableThinking bool     // 语义同 Request.DisableThinking（见 client.go 的事故注释）
	// beforeSend is installed only by DoChat after it estimates this request.
	// Keeping it private prevents callers from replacing the quota gate.
	beforeSend func(context.Context) error
}

// ChatResponse 单次 Chat 调用结果。现有调用方 API 保持不变；字段由通过
// provider-neutral 校验的 AssistantTurn 投影而来。
type ChatResponse struct {
	Content          string
	ToolCalls        []ToolCall
	FinishReason     string
	PromptTokens     int
	CompletionTokens int
	CacheHitTokens   int
	CacheMissTokens  int
	Model            string
	LatencyMs        int
}

// wire* 是 OpenAI 兼容线协议的收发结构，仅本文件内部使用。
// 与导出的 ChatMessage/ToolCall 刻意分离：导出形态是扁平的易用/持久化格式，
// 线协议要求 tool_calls 嵌套 function 对象且带 type 字段，Chat 做双向转换。
type wireChatMessage struct {
	Role       string         `json:"role"`
	Content    string         `json:"content"`
	ToolCalls  []wireToolCall `json:"tool_calls,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
}

type wireToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"` // 恒 "function"
	Function wireFunctionCall `json:"function"`
}

type wireFunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type wireToolDef struct {
	Type     string          `json:"type"` // 恒 "function"
	Function wireFunctionDef `json:"function"`
}

type wireFunctionDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
	Strict      *bool           `json:"strict,omitempty"`
}

type wireChatRequest struct {
	Model       string            `json:"model"`
	Messages    []wireChatMessage `json:"messages"`
	Tools       []wireToolDef     `json:"tools,omitempty"`
	Temperature *float32          `json:"temperature,omitempty"`
	MaxTokens   *int              `json:"max_tokens,omitempty"`
	Thinking    *thinkingConfig   `json:"thinking,omitempty"`
}

type wireChatResponse struct {
	Model   string           `json:"model"`
	Choices []wireChatChoice `json:"choices"`
	Usage   struct {
		PromptTokens          int `json:"prompt_tokens"`
		CompletionTokens      int `json:"completion_tokens"`
		PromptCacheHitTokens  int `json:"prompt_cache_hit_tokens"`
		PromptCacheMissTokens int `json:"prompt_cache_miss_tokens"`
	} `json:"usage"`
}

// requestModel 计算本次请求实际使用的模型名：覆盖值非空则替代 Client 默认。
// Chat 发请求与 DoChat 记账共用，保证两处永远一致（契约"记账 call.Model 同步"）。
func (c *Client) requestModel(override string) string {
	if override != "" {
		return override
	}
	return c.model
}

// Chat 发起一次多轮（可带 tools）chat completions 调用。
// 错误映射与 Complete 完全一致（复用 mapHTTPError/wrapCtxErr，见 client.go
// Complete 的映射表注释）；客户端同样不做重试。上游 HTTP 200 但工具协议
// 不合法时，会返回只含计费元数据的 response + error；业务调用必须按 error
// 失败关闭，DoChat 会消费元数据完成记账后再向外隐藏 partial response。
func (c *Client) Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error) {
	// 与 Complete 共用同一信号量：并发上限约束的是对上游的总请求数，
	// 不区分调用形态。排队期间 ctx 取消要能立刻退出。
	select {
	case c.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, wrapChatRequestNotSent(ctx.Err())
	}
	defer func() { <-c.sem }()

	start := time.Now()
	model := c.requestModel(req.Model)

	safeMessages, redactedHistory := redactLeakedDSMLMessages(req.Messages)
	messages := make([]wireChatMessage, 0, len(safeMessages))
	for _, m := range safeMessages {
		wm := wireChatMessage{Role: m.Role, Content: m.Content, ToolCallID: m.ToolCallID}
		// assistant 历史消息的 tool_calls 必须原样回传（协议要求：每个
		// tool 消息都要能对上 assistant 侧的 tool_call_id），丢弃会 400。
		for _, tc := range m.ToolCalls {
			wm.ToolCalls = append(wm.ToolCalls, wireToolCall{
				ID:       tc.ID,
				Type:     "function",
				Function: wireFunctionCall{Name: tc.Name, Arguments: tc.Arguments},
			})
		}
		messages = append(messages, wm)
	}
	if redactedHistory > 0 {
		slog.Warn("llm: 已清洗历史 DSML 协议文本",
			"model", model, "messages", redactedHistory)
	}

	// 无 tools 时不携带该字段（omitempty 靠 nil 切片）：空数组对部分兼容
	// 实现是非法请求，且"收尾文案"调用依赖不带 tools 防止再触发工具调用。
	var tools []wireToolDef
	var strict *bool
	if strings.EqualFold(strings.TrimSpace(c.provider), "kimi") {
		enabled := true
		strict = &enabled
	}
	for _, td := range req.Tools {
		tools = append(tools, wireToolDef{
			Type: "function",
			Function: wireFunctionDef{
				Name:        td.Name,
				Description: td.Description,
				Parameters:  td.Parameters,
				Strict:      strict,
			},
		})
	}

	var thinking *thinkingConfig
	if req.DisableThinking {
		thinking = &thinkingConfig{Type: "disabled"}
	}
	payload, err := json.Marshal(wireChatRequest{
		Model:       model,
		Messages:    messages,
		Tools:       tools,
		Temperature: c.requestTemperature(model, req.Temperature),
		MaxTokens:   req.MaxTokens,
		Thinking:    thinking,
	})
	if err != nil {
		// ToolDef.Parameters 是 RawMessage：调用方传入非法 JSON 会在这里暴露。
		return nil, types.NewAppError(types.CodeLLMBadRequest, "llm: 请求体序列化失败", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return nil, types.NewAppError(types.CodeLLMBadRequest, "llm: 构造 HTTP 请求失败", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)

	if err := ctx.Err(); err != nil {
		return nil, wrapChatRequestNotSent(err)
	}
	if req.beforeSend != nil {
		if err := req.beforeSend(ctx); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		// The quota hook may have reserved successfully just as the owner
		// canceled. Mark this as definitely not sent so DoChat refunds it.
		return nil, wrapChatRequestNotSent(err)
	}

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		// http.Client 把 ctx 取消包成 url.Error，需回查 ctx 才能还原超时语义。
		if ctx.Err() != nil {
			return nil, wrapCtxErr(ctx.Err())
		}
		return nil, types.NewAppError(types.CodeLLMUnavailable, "llm: 请求发送失败", err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(httpResp.Body, maxErrBodyBytes))
		return nil, mapHTTPError(httpResp.StatusCode, body)
	}

	raw, err := io.ReadAll(httpResp.Body)
	if err != nil {
		if ctx.Err() != nil {
			return nil, wrapCtxErr(ctx.Err())
		}
		return nil, types.NewAppError(types.CodeLLMUnavailable, "llm: 读取响应体失败", err)
	}

	var cr wireChatResponse
	if err := json.Unmarshal(raw, &cr); err != nil {
		return nil, types.NewAppError(types.CodeLLMUnavailable, "llm: 响应体不是合法 JSON", err)
	}
	if len(cr.Choices) == 0 {
		return nil, types.NewAppError(types.CodeLLMUnavailable, "llm: 响应缺少 choices", nil)
	}

	respModel := cr.Model
	if respModel == "" {
		respModel = model
	}
	resp := &ChatResponse{
		PromptTokens:     cr.Usage.PromptTokens,
		CompletionTokens: cr.Usage.CompletionTokens,
		CacheHitTokens:   cr.Usage.PromptCacheHitTokens,
		CacheMissTokens:  cr.Usage.PromptCacheMissTokens,
		Model:            respModel,
		LatencyMs:        int(time.Since(start).Milliseconds()),
	}
	turn, err := adaptAssistantTurn(cr.Choices[0], assistantTurnOptions{
		Provider:      c.provider,
		RequestModel:  model,
		ResponseModel: respModel,
		ToolsDeclared: len(req.Tools) > 0,
	})
	if err != nil {
		slog.Warn("llm: 上游工具协议响应不合法",
			"provider", c.provider,
			"model", respModel,
			"finish_reason", cr.Choices[0].FinishReason.Value,
			"request_has_tools", len(req.Tools) > 0,
			"native_tool_calls", len(cr.Choices[0].Message.ToolCalls))
		return resp, err
	}
	resp.Content = turn.Content
	resp.ToolCalls = turn.ToolCalls
	resp.FinishReason = turn.StopReason.String()

	// FC 场景下"空 content + 有 tool_calls"是正常形态；两者皆空才是隐性故障
	// 信号（如思维链吃光预算），沿用 Complete 的 WARN 语义留下可检索告警。
	if turn.Content == "" && len(turn.ToolCalls) == 0 {
		slog.Warn("llm: 上游返回空 content 且无 tool_calls",
			"model", respModel,
			"finish_reason", cr.Choices[0].FinishReason.Value,
			"completion_tokens", cr.Usage.CompletionTokens)
	}
	return resp, nil
}

var errChatRequestNotSent = errors.New("llm: chat request not sent")

func wrapChatRequestNotSent(cause error) error {
	return types.NewAppError(
		types.CodeLLMUnavailable,
		"llm: 请求发送前已取消",
		errors.Join(errChatRequestNotSent, cause),
	)
}

// userPromptMaxBytes DoChat 记账时 UserPrompt（messages 数组 JSON）的截断上限。
// 多轮会话上下文会越滚越大，全量入库会让 llm_calls 无谓膨胀；
// 8KB（契约 §4）足够排障时看清最近几轮对话。
// 截断方向为**保尾部**（审查 #截断方向）：messages JSON 序为 [system, 最旧→最新]，
// 触发本次调用的用户原话与最近几轮在尾部——保头部会把排障最需要的内容恰好切掉。
const userPromptMaxBytes = 8 << 10

// truncateUTF8Tail 保留 s 的最后至多 max 字节，并把起点向后推进到合法的 rune
// 边界（防止把多字节字符切一半，Postgres TEXT 拒收非法 UTF-8）。
// 被截断时前缀固定标记，排障时可一眼识别这不是完整上下文。
func truncateUTF8Tail(s string, max int) string {
	if len(s) <= max {
		return s
	}
	start := len(s) - max
	for start < len(s) && !utf8.RuneStart(s[start]) {
		start++
	}
	return "…(前文截断)" + s[start:]
}

// chatPromptRunes 统计一次 Chat 请求里全部输入文本的字符数——
// 多轮消息、工具定义（它们同样进 prompt 且往往不短）都要算进去，
// 漏算工具定义会让 agent 这条路径的估算系统性偏低。
func chatPromptRunes(req ChatRequest) int {
	n := 0
	for _, m := range req.Messages {
		n += utf8.RuneCountInString(m.Content)
	}
	for _, t := range req.Tools {
		n += utf8.RuneCountInString(t.Name) + utf8.RuneCountInString(t.Description)
	}
	return n
}

// DoChat = Chat + 记账，行为对齐 do.go 的 Do：失败也记账、记账用
// WithoutCancel 剥离取消信号、成本按缓存三段单价计算。
// 与 Do 的差异只在 prompt/completion 的落库形态：UserPrompt 记 messages
// 数组 JSON（截断到 8KB）；Completion 记 Content，若有 ToolCalls 则记
// "tool_calls: <json>"（FC 响应的 content 通常为空，工具调用才是有效输出）。
func DoChat(ctx context.Context, c *Client, rec *Recorder, meta CallMeta, req ChatRequest) (*ChatResponse, error) {
	// Sanitize legacy protocol text from every role before every consumer in this
	// function: quota estimation, outbound wire request, and ledger UserPrompt.
	req.Messages, _ = redactLeakedDSMLMessages(req.Messages)
	// 配额闸门（契约 §2.7）。理由与 Do 相同，见 do.go 的说明。
	//
	// 这条路径**比 Do 更需要它**：多轮对话把历史累积进 prompt，生产 7 天实测
	// agent 的 prompt 均值 4381、峰值 44871，是打分的 10 倍与 42 倍；而且 agent
	// 循环里一次用户消息会触发多轮调用，单次交互的总量还要再乘一个轮数。
	// 第一版把这条漏了，却在 Do 的注释里断言"唯一咽喉"——错得最贵的那种。
	estimate := estimateTokens(chatPromptRunes(req), req.MaxTokens)
	reserved := 0.0
	var gateErr error
	req.beforeSend = func(sendCtx context.Context) error {
		if err := rec.CheckQuota(sendCtx, meta.UserID, estimate); err != nil {
			switch {
			case errors.Is(err, store.ErrQuotaExceeded):
				gateErr = types.NewAppError(types.CodeQuotaExceeded,
					"本租户的 LLM 额度已用尽，稍后会随时间自动恢复", nil)
				return gateErr
			case errors.Is(err, store.ErrAmbiguousTenant):
				// 归属不明 ⇒ 拒绝。此刻根本不知道该记谁的账，而花一笔无法归属的钱
				// 正是这道护栏存在的理由；且它是确定性的，放行等于给该用户无限额度。
				if meta.UserID != nil {
					slog.Error("llm: 用户归属多个租户，无法判定配额归属，拒绝调用",
						"user_id", *meta.UserID)
				}
				gateErr = types.NewAppError(types.CodeInternal,
					"账号归属异常，暂时无法处理，请联系管理员", err)
				return gateErr
			default:
				// 其余（数据库抖动等）放行：让 DB 抖动升级成全局 LLM 停摆，
				// 比超额一点糟糕得多。reserved 保持 0，成功后按实际量补扣。
				slog.Error("llm: 配额查询失败，本次放行（护栏此刻失效）", "err", err)
			}
		} else {
			reserved = estimate
		}
		return nil
	}

	start := time.Now()
	resp, err := c.Chat(ctx, req)
	if gateErr != nil {
		// 保持既有语义：配额闸门在上游零调用时直接返回，不伪造一次 LLM 调用。
		return nil, gateErr
	}

	msgsJSON, mErr := json.Marshal(req.Messages)
	if mErr != nil {
		// ChatMessage 全是纯值字段，正常不可能失败；兜底占位保证记账不缺行。
		msgsJSON = []byte("<messages 序列化失败>")
	}

	call := &types.LLMCall{
		TraceID:     meta.TraceID,
		SpanName:    meta.SpanName,
		UserID:      meta.UserID,
		RefType:     meta.RefType,
		RefID:       meta.RefID,
		Provider:    c.provider,
		Model:       c.requestModel(req.Model), // 成功路径下面会覆盖为上游回报的实际模型名
		UserPrompt:  truncateUTF8Tail(string(msgsJSON), userPromptMaxBytes),
		Temperature: req.Temperature,
		MaxTokens:   req.MaxTokens,
	}

	if resp != nil {
		call.Model = resp.Model
		call.PromptTokens = resp.PromptTokens
		call.CompletionTokens = resp.CompletionTokens
		call.LatencyMs = resp.LatencyMs

		// prefix_cache_hit 三态判定与计价：与 do.go 的 Do 保持一致
		// （借 DeepSeek 不变量 hit+miss == prompt_tokens 推断缓存字段是否
		// 被上游返回；未报告时按全量未命中计价，宁可略高估）。
		hitTokens, missTokens := resp.CacheHitTokens, resp.CacheMissTokens
		cacheReported := resp.PromptTokens > 0 && hitTokens+missTokens == resp.PromptTokens
		if cacheReported {
			hit := hitTokens > 0
			call.PrefixCacheHit = &hit
		} else {
			hitTokens, missTokens = 0, resp.PromptTokens
		}
		call.CostUSD = CostUSD(resp.Model, hitTokens, missTokens, resp.CompletionTokens)
	}
	if err != nil {
		call.Error = err.Error()
		if resp == nil {
			// Transport/JSON failures have no response latency metadata.
			call.LatencyMs = int(time.Since(start).Milliseconds())
		}
	} else {
		call.Completion = resp.Content
		if len(resp.ToolCalls) > 0 {
			if tcJSON, tErr := json.Marshal(resp.ToolCalls); tErr == nil {
				call.Completion = "tool_calls: " + string(tcJSON)
			}
		}
	}

	// 与 Do 共用一个有硬上限的 detached tail：既不能因请求取消漏记，
	// 也不能让同步 DB 写无限拖住 Activity/进程关停。HTTP 429/4xx 是上游
	// 明确拒绝，actual=0 可安全退还；但 timeout、5xx、读取/解析响应失败都可能
	// 发生在供应商已生成并计费之后。用量未知时保留事前预扣，不能让自动重试把
	// 最多三笔真实支出全部记成零成本并退回租户额度。
	actualTokens := call.PromptTokens + call.CompletionTokens
	reconcileQuota := reserved > 0 || actualTokens > 0
	if err != nil && resp == nil &&
		types.CodeOf(err) == types.CodeLLMUnavailable &&
		!errors.Is(err, errChatRequestNotSent) {
		reconcileQuota = false
		slog.Warn("llm: 响应用量未知，保留 Agent 调用的保守预扣",
			"trace_id", meta.TraceID,
			"model", c.requestModel(req.Model),
			"reserved_tokens", reserved,
			"error_code", types.CodeOf(err))
	}
	rec.finishCallAccountingWithReservation(
		ctx, call, nil, meta.UserID, reserved,
		actualTokens, nil, reconcileQuota,
	)
	if err != nil {
		// Chat may return a metadata-only response for accounting. Never expose
		// that partial response to callers which could accidentally use it.
		return nil, err
	}
	return resp, nil
}
