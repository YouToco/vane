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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/types"
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

// Request 单轮对话请求。Temperature/MaxTokens 为 nil 时请求体不携带
// 对应字段（交给上游默认值），因此用指针而非零值区分"未设置"。
type Request struct {
	System      string
	User        string
	Temperature *float32 // nil = 不传该字段
	MaxTokens   *int     // nil = 不传该字段
}

// Response 单次调用结果。CacheHitTokens/CacheMissTokens 对应 DeepSeek
// usage 顶层的 prompt_cache_hit_tokens / prompt_cache_miss_tokens，
// 上游未返回时为 0（DeepSeek 恒有 hit+miss == prompt_tokens）。
type Response struct {
	Content          string
	PromptTokens     int
	CompletionTokens int
	CacheHitTokens   int
	CacheMissTokens  int
	Model            string
	LatencyMs        int
}

// chatMessage / chatRequest / chatResponse 是 OpenAI 兼容协议的收发结构，
// 只在本包内部使用。指针 + omitempty 实现"nil 不携带字段"。
type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	Temperature *float32      `json:"temperature,omitempty"`
	MaxTokens   *int          `json:"max_tokens,omitempty"`
}

type chatResponse struct {
	Model   string `json:"model"`
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Usage struct {
		PromptTokens          int `json:"prompt_tokens"`
		CompletionTokens      int `json:"completion_tokens"`
		PromptCacheHitTokens  int `json:"prompt_cache_hit_tokens"`
		PromptCacheMissTokens int `json:"prompt_cache_miss_tokens"`
	} `json:"usage"`
}

// maxErrBodyBytes 错误响应体读取上限：只为拼错误详情，防上游异常大响应。
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

	payload, err := json.Marshal(chatRequest{
		Model:       c.model,
		Messages:    messages,
		Temperature: req.Temperature,
		MaxTokens:   req.MaxTokens,
	})
	if err != nil {
		return nil, types.NewAppError(types.CodeLLMBadRequest, "llm: 请求体序列化失败", err)
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
	if len(cr.Choices) == 0 {
		return nil, types.NewAppError(types.CodeLLMUnavailable, "llm: 响应缺少 choices", nil)
	}

	model := cr.Model
	if model == "" {
		model = c.model
	}
	return &Response{
		Content:          cr.Choices[0].Message.Content,
		PromptTokens:     cr.Usage.PromptTokens,
		CompletionTokens: cr.Usage.CompletionTokens,
		CacheHitTokens:   cr.Usage.PromptCacheHitTokens,
		CacheMissTokens:  cr.Usage.PromptCacheMissTokens,
		Model:            model,
		LatencyMs:        int(time.Since(start).Milliseconds()),
	}, nil
}

// wrapCtxErr 把 ctx 取消/超时包成 AppError，Cause 保留原始 ctx 错误
// 供 errors.Is 下钻（context.DeadlineExceeded / context.Canceled）。
func wrapCtxErr(cause error) error {
	return types.NewAppError(types.CodeLLMUnavailable, "llm: 请求被取消或超时", cause)
}

// mapHTTPError 按状态码映射统一错误码，错误详情附上游响应体片段
// （通常含 DeepSeek 的 error.message，排障时省一次抓包）。
func mapHTTPError(status int, body []byte) error {
	detail := strings.TrimSpace(string(body))
	msg := fmt.Sprintf("llm: 上游返回 HTTP %d: %s", status, detail)
	switch {
	case status == http.StatusTooManyRequests:
		return types.NewAppError(types.CodeLLMRateLimit, msg, nil)
	case status >= 500:
		return types.NewAppError(types.CodeLLMUnavailable, msg, nil)
	default:
		return types.NewAppError(types.CodeLLMBadRequest, msg, nil)
	}
}
