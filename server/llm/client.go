// Package llm 封装 DeepSeek（OpenAI 兼容）的 chat completions 调用与记账。
//
// 只用标准库 net/http（契约要求，不引 openai SDK）：接口面很小（单端点、
// 单轮对话），SDK 带来的抽象成本大于收益，且自控请求体能精确满足
// "Temperature/MaxTokens 为 nil 时不携带字段"的语义。
// 客户端自身不做重试——重试属于上层（Temporal / 调用方）的职责，
// 客户端重试会与上层重试叠加放大流量。
package llm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/YouToco/vane/server/config"
	"github.com/YouToco/vane/server/types"
)

// Client 是 DeepSeek chat completions 客户端。
// 并发上限用 buffered channel 信号量实现：相比 semaphore.Weighted
// 无额外依赖，且天然支持 select ctx 取消。
type Client struct {
	httpClient *http.Client
	baseURL    string
	apiKey     string
	model      string
	provider   string // 记账用（llm_calls.provider），不影响请求
	sem        chan struct{}
}

// New 按配置构造客户端。cfg.MaxConcurrent 作为信号量上限，
// 非法值（<1）兜底为 1，避免 make(chan, 0) 造成所有请求互相死等。
func New(cfg config.LLMConfig) *Client {
	maxConcurrent := cfg.MaxConcurrent
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}
	return &Client{
		// 不设 http.Client 级超时：调用超时统一由调用方 ctx 控制，
		// 避免两套超时叠加后语义不清（LLM 生成耗时波动大，固定值不合适）。
		httpClient: &http.Client{},
		baseURL:    strings.TrimRight(cfg.BaseURL, "/"),
		apiKey:     cfg.APIKey,
		model:      cfg.Model,
		provider:   cfg.Provider,
		sem:        make(chan struct{}, maxConcurrent),
	}
}

// requestTemperature keeps provider-fixed model parameters out of the wire
// request. Kimi K2.6/K2.5 reject any non-fixed value and recommend omitting the
// field so the server applies 1.0 (thinking) or 0.6 (thinking disabled).
func (c *Client) requestTemperature(model string, requested *float32) *float32 {
	if !strings.EqualFold(strings.TrimSpace(c.provider), "kimi") {
		return requested
	}
	switch strings.ToLower(strings.TrimSpace(model)) {
	case "kimi-k2.6", "kimi-k2.5":
		return nil
	default:
		return requested
	}
}

// Request 单轮对话请求。Temperature/MaxTokens 为 nil 时请求体不携带
// 对应字段（交给上游默认值），因此用指针而非零值区分"未设置"。
type Request struct {
	System string
	User   string
	// Model overrides the client's legacy default for an immutable prepared
	// run. Empty preserves the existing caller behavior exactly.
	Model       string
	Temperature *float32 // nil = 不传该字段
	MaxTokens   *int     // nil = 不传该字段
	// DisableThinking 显式关闭 DeepSeek V4 的思维链输出（thinking: disabled）。
	// V4 系列默认开启 reasoning：模型先在 reasoning_content 里思考再写 content，
	// 两者共享 max_tokens 预算——小预算调用（如打分 16 token）会被思维链吃光、
	// content 恒为空（2026-07-14 生产实锤：118/118 次打分 content 空、全部回退中位分）。
	// 格式固定的结构化输出（打分/出卡）应设 true：省 token、快、且杜绝空 content。
	DisableThinking bool
	// BeforeSend is an internal effect gate. Complete invokes it only after the
	// concurrency slot is acquired and the local payload is valid, immediately
	// before constructing/sending the HTTP request. It is never serialized.
	BeforeSend func(context.Context) error `json:"-"`
}

// Response 单次调用结果。CacheHitTokens/CacheMissTokens 对应 DeepSeek
// CacheTokensReported / ReasoningTokensReported preserve whether the provider
// actually returned the corresponding breakdown instead of collapsing
// "unknown" into a real zero.
type Response struct {
	Content string
	// UsageReported distinguishes a real zero from an HTTP 200 response that
	// omitted its usage object. Paid callers must treat omission as unknown.
	UsageReported           bool
	PromptTokens            int
	CompletionTokens        int
	CacheHitTokens          int
	CacheMissTokens         int
	CacheTokensReported     bool
	ReasoningTokens         int
	ReasoningTokensReported bool
	Model                   string
	LatencyMs               int
}

// chatMessage / chatRequest / chatResponse 是 OpenAI 兼容协议的收发结构，
// 只在本包内部使用。指针 + omitempty 实现"nil 不携带字段"。
type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model       string          `json:"model"`
	Messages    []chatMessage   `json:"messages"`
	Temperature *float32        `json:"temperature,omitempty"`
	MaxTokens   *int            `json:"max_tokens,omitempty"`
	Thinking    *thinkingConfig `json:"thinking,omitempty"`
}

// thinkingConfig 是 DeepSeek V4 的思维链开关（实测 type=disabled 生效：
// reasoning_content 为空、content 正常、completion_tokens 大幅下降）。
type thinkingConfig struct {
	Type string `json:"type"` // "disabled" | "enabled"
}

type chatUsage struct {
	PromptTokens          *int `json:"prompt_tokens"`
	CompletionTokens      *int `json:"completion_tokens"`
	PromptCacheHitTokens  *int `json:"prompt_cache_hit_tokens"`
	PromptCacheMissTokens *int `json:"prompt_cache_miss_tokens"`
	PromptTokensDetails   struct {
		CachedTokens *int `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
	CompletionTokensDetails struct {
		ReasoningTokens *int `json:"reasoning_tokens"`
	} `json:"completion_tokens_details"`
}

type chatResponse struct {
	Model   string `json:"model"`
	Choices []struct {
		FinishReason string `json:"finish_reason"`
		Message      struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Usage *chatUsage `json:"usage"`
}

func usageCacheBreakdown(prompt int, topHit, topMiss, nestedHit *int) (hit, miss int, reported bool) {
	if topHit != nil && topMiss != nil && *topHit >= 0 && *topMiss >= 0 &&
		*topHit+*topMiss == prompt {
		return *topHit, *topMiss, true
	}
	if nestedHit != nil && *nestedHit >= 0 && *nestedHit <= prompt {
		return *nestedHit, prompt - *nestedHit, true
	}
	return 0, prompt, false
}

func usageReasoningBreakdown(completion int, reported *int) (int, bool) {
	if reported == nil || *reported < 0 || *reported > completion {
		return 0, false
	}
	return *reported, true
}

// maxErrBodyBytes 错误响应体读取上限：只为生成不可逆诊断指纹，防上游异常大响应。
const maxErrBodyBytes = 4 << 10

// Complete 发起一次 chat completions 调用。
//
// 错误映射（契约 §3；契约中的 CodeLLMTimeout/CodeLLMUpstream 在
// types 码表中不存在且禁止新增，按"用最接近的"原则均取可重试的
// CodeLLMUnavailable；超时场景 Cause 保留 ctx 错误，调用方仍可用
// errors.Is(err, context.DeadlineExceeded) 精确区分）：
//   - HTTP 429      → CodeLLMRateLimit
//   - HTTP 5xx      → CodeLLMUnavailable
//   - 其余 HTTP 4xx → CodeLLMBadRequest
//   - ctx 超时/取消 → CodeLLMUnavailable（Cause = ctx.Err()）
func (c *Client) Complete(ctx context.Context, req Request) (*Response, error) {
	// 先占信号量再发请求；排队期间 ctx 取消要能立刻退出，
	// 否则高并发下请求会在队列里僵死到超时。
	select {
	case c.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, wrapCtxErr(ctx.Err())
	}
	defer func() { <-c.sem }()

	start := time.Now()

	// system 为空时不发 system message：空 system 对上游是无意义约束，
	// 部分兼容实现还会因空 content 报 400。
	messages := make([]chatMessage, 0, 2)
	if req.System != "" {
		messages = append(messages, chatMessage{Role: "system", Content: req.System})
	}
	messages = append(messages, chatMessage{Role: "user", Content: req.User})

	var thinking *thinkingConfig
	if req.DisableThinking {
		thinking = &thinkingConfig{Type: "disabled"}
	}
	requestModel := c.requestModel(req.Model)
	// max_tokens 一律不下发（产品决定）：输出上限交给上游默认。
	// 推理模型会把 completion 预算花在 reasoning_content 上，过小的 wire
	// 上限会饿死正文输出。req.MaxTokens 仍保留为配额预留与 llm_calls 账本口径，
	// 不再代表 wire 上限。
	payload, err := json.Marshal(chatRequest{
		Model:       requestModel,
		Messages:    messages,
		Temperature: c.requestTemperature(requestModel, req.Temperature),
		Thinking:    thinking,
	})
	if err != nil {
		return nil, types.NewAppError(types.CodeLLMBadRequest, "llm: 请求体序列化失败", err)
	}
	if req.BeforeSend != nil {
		if err := req.BeforeSend(ctx); err != nil {
			return nil, err
		}
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return nil, types.NewAppError(types.CodeLLMBadRequest, "llm: 构造 HTTP 请求失败", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)

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

	var cr chatResponse
	if err := json.Unmarshal(raw, &cr); err != nil {
		return nil, types.NewAppError(types.CodeLLMUnavailable, "llm: 响应体不是合法 JSON", err)
	}
	model := cr.Model
	if model == "" {
		model = requestModel
	}
	usage := chatUsage{}
	if cr.Usage != nil {
		usage = *cr.Usage
	}
	usageReported := usage.PromptTokens != nil && usage.CompletionTokens != nil &&
		*usage.PromptTokens >= 0 && *usage.CompletionTokens >= 0 &&
		*usage.PromptTokens+*usage.CompletionTokens > 0
	promptTokens, completionTokens := 0, 0
	if usageReported {
		promptTokens, completionTokens = *usage.PromptTokens, *usage.CompletionTokens
	}
	hit, miss, cacheReported := usageCacheBreakdown(
		promptTokens,
		usage.PromptCacheHitTokens,
		usage.PromptCacheMissTokens,
		usage.PromptTokensDetails.CachedTokens,
	)
	reasoning, reasoningReported := usageReasoningBreakdown(
		completionTokens,
		usage.CompletionTokensDetails.ReasoningTokens,
	)
	resp := &Response{
		UsageReported:           usageReported,
		PromptTokens:            promptTokens,
		CompletionTokens:        completionTokens,
		CacheHitTokens:          hit,
		CacheMissTokens:         miss,
		CacheTokensReported:     cacheReported,
		ReasoningTokens:         reasoning,
		ReasoningTokensReported: reasoningReported,
		Model:                   model,
		LatencyMs:               int(time.Since(start).Milliseconds()),
	}
	if len(cr.Choices) == 0 {
		// Even malformed business responses must return metadata to the caller so
		// any provider-reported usage can be durably accounted.
		return resp, types.NewAppError(types.CodeLLMUnavailable, "llm: 响应缺少 choices", nil)
	}

	resp.Content = cr.Choices[0].Message.Content
	if !usageReported {
		// A successful HTTP status without complete token metadata is not a
		// settleable success. Return metadata only and force the durable V3 path
		// to retain its conservative reservation as an indeterminate attempt.
		resp.Content = ""
		return resp, types.NewAppError(types.CodeLLMUnavailable,
			"llm: 响应用量元数据缺失或无效", nil)
	}
	// content 空是隐性故障信号（如思维链吃光 max_tokens 预算导致 finish_reason=length
	// 且 content=""），静默返回会让上层用兜底值掩盖问题——2026-07-14 打分全回退中位分
	// 三个批次才被发现。这里不报错（语义留给调用方），但必须留下可检索的告警。
	if resp.Content == "" {
		slog.Warn("llm: 上游返回空 content",
			"model", model,
			"finish_reason", cr.Choices[0].FinishReason,
			"completion_tokens", completionTokens)
	}
	return resp, nil
}

// wrapCtxErr 把 ctx 取消/超时包成 AppError，Cause 保留原始 ctx 错误
// 供 errors.Is 下钻（context.DeadlineExceeded / context.Canceled）。
func wrapCtxErr(cause error) error {
	return types.NewAppError(types.CodeLLMUnavailable, "llm: 请求被取消或超时", cause)
}

// mapHTTPError 按状态码映射统一错误码。上游响应体可能回显 prompt、用户内容、
// request id 或供应商内部细节，不能进入会被 API/Temporal/应用日志继续传播的错误链。
// 仅保留截断后字节数和短 SHA-256 指纹，足以关联同类故障而不暴露正文。
func mapHTTPError(status int, body []byte) error {
	digest := sha256.Sum256(body)
	msg := fmt.Sprintf("llm: 上游返回 HTTP %d（捕获响应体 %d 字节，sha256=%x）",
		status, len(body), digest[:8])
	switch {
	case status == http.StatusTooManyRequests:
		return types.NewAppError(types.CodeLLMRateLimit, msg, nil)
	case status >= 500:
		return types.NewAppError(types.CodeLLMUnavailable, msg, nil)
	default:
		return types.NewAppError(types.CodeLLMBadRequest, msg, nil)
	}
}
