package llm

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/types"
)

// okResponseBody 模拟 DeepSeek 正常响应：usage 顶层带缓存字段，
// 且满足 hit+miss == prompt_tokens 的实测不变量。
const okResponseBody = `{
	"model": "deepseek-v4-flash",
	"choices": [{"message": {"role": "assistant", "content": "你好，我是见微。"}}],
	"usage": {
		"prompt_tokens": 10,
		"completion_tokens": 5,
		"prompt_cache_hit_tokens": 4,
		"prompt_cache_miss_tokens": 6
	}
}`

// newTestClient 指向 httptest.Server 构造客户端，测试共用。
func newTestClient(serverURL string, maxConcurrent int) *Client {
	return New(config.LLMConfig{
		Provider:      "deepseek",
		BaseURL:       serverURL,
		APIKey:        "test-key",
		Model:         "deepseek-v4-flash",
		MaxConcurrent: maxConcurrent,
	})
}

// TestCompleteRoundtrip 正常往返：断言请求形态（路径/头/可选字段缺省不携带）
// 与响应解析（含缓存字段）。
func TestCompleteRoundtrip(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/chat/completions" {
			t.Errorf("请求 = %s %s, 期望 POST /chat/completions", r.Method, r.URL.Path)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer test-key" {
			t.Errorf("Authorization = %q, 期望 Bearer test-key", auth)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, 期望 application/json", ct)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("解析请求体失败: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(okResponseBody))
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, 5)
	resp, err := c.Complete(context.Background(), Request{
		System: "你是测试助手",
		User:   "你好",
		// Temperature / MaxTokens 均为 nil，请求体必须不携带对应字段
	})
	if err != nil {
		t.Fatalf("Complete 返回错误: %v", err)
	}

	// 请求体断言：nil 可选参数不得出现在 JSON 里（契约硬约束）。
	if _, has := gotBody["temperature"]; has {
		t.Error("Temperature 为 nil 时请求体不应携带 temperature 字段")
	}
	if _, has := gotBody["max_tokens"]; has {
		t.Error("MaxTokens 为 nil 时请求体不应携带 max_tokens 字段")
	}
	if gotBody["model"] != "deepseek-v4-flash" {
		t.Errorf("model = %v, 期望 deepseek-v4-flash", gotBody["model"])
	}
	msgs, _ := gotBody["messages"].([]any)
	if len(msgs) != 2 {
		t.Fatalf("messages 长度 = %d, 期望 2 (system+user)", len(msgs))
	}

	// 响应解析断言：含 DeepSeek 缓存字段。
	if resp.Content != "你好，我是见微。" {
		t.Errorf("Content = %q", resp.Content)
	}
	if resp.PromptTokens != 10 || resp.CompletionTokens != 5 {
		t.Errorf("tokens = (%d, %d), 期望 (10, 5)", resp.PromptTokens, resp.CompletionTokens)
	}
	if resp.CacheHitTokens != 4 || resp.CacheMissTokens != 6 {
		t.Errorf("cache tokens = (%d, %d), 期望 (4, 6)", resp.CacheHitTokens, resp.CacheMissTokens)
	}
	if resp.Model != "deepseek-v4-flash" {
		t.Errorf("Model = %q, 期望 deepseek-v4-flash", resp.Model)
	}
	if resp.LatencyMs < 0 {
		t.Errorf("LatencyMs = %d, 不应为负", resp.LatencyMs)
	}
}

// TestCompleteOptionalFields 显式设置 Temperature/MaxTokens 时必须携带。
func TestCompleteOptionalFields(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.Write([]byte(okResponseBody))
	}))
	defer srv.Close()

	temp := float32(0.7)
	maxTok := 512
	c := newTestClient(srv.URL, 5)
	if _, err := c.Complete(context.Background(), Request{
		User:        "hi",
		Temperature: &temp,
		MaxTokens:   &maxTok,
	}); err != nil {
		t.Fatalf("Complete 返回错误: %v", err)
	}

	if got, ok := gotBody["temperature"].(float64); !ok || math.Abs(got-0.7) > 1e-6 {
		t.Errorf("temperature = %v, 期望 0.7", gotBody["temperature"])
	}
	if got, ok := gotBody["max_tokens"].(float64); !ok || got != 512 {
		t.Errorf("max_tokens = %v, 期望 512", gotBody["max_tokens"])
	}
	// 未设 DisableThinking 时不得携带 thinking 字段（交上游默认）。
	if _, present := gotBody["thinking"]; present {
		t.Errorf("未禁思考时不应携带 thinking 字段，实际 %v", gotBody["thinking"])
	}
}

// TestCompleteDisableThinking DisableThinking=true 必须序列化为
// thinking:{type:"disabled"}——V4 默认思维链会吃光小 max_tokens 预算导致
// content 恒空（打分全回退中位分的生产事故回归锚点）。
func TestCompleteDisableThinking(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.Write([]byte(okResponseBody))
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, 5)
	if _, err := c.Complete(context.Background(), Request{
		User:            "hi",
		DisableThinking: true,
	}); err != nil {
		t.Fatalf("Complete 返回错误: %v", err)
	}

	th, ok := gotBody["thinking"].(map[string]any)
	if !ok {
		t.Fatalf("thinking 字段缺失或类型错误: %v", gotBody["thinking"])
	}
	if th["type"] != "disabled" {
		t.Errorf("thinking.type = %v, 期望 disabled", th["type"])
	}
}

// TestComplete429RateLimit HTTP 429 必须映射为 CodeLLMRateLimit（可重试）。
func TestComplete429RateLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":{"message":"rate limit exceeded"}}`, http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, 5)
	_, err := c.Complete(context.Background(), Request{User: "hi"})
	if err == nil {
		t.Fatal("期望错误，实际为 nil")
	}
	if code := types.CodeOf(err); code != types.CodeLLMRateLimit {
		t.Errorf("错误码 = %s, 期望 %s", code, types.CodeLLMRateLimit)
	}
	if !types.IsRetryable(err) {
		t.Error("429 应为可重试错误")
	}
	if !errors.Is(err, types.ErrLLM) {
		t.Error("429 错误应匹配 ErrLLM 哨兵")
	}
}

func TestCompleteHTTPErrorDoesNotExposeUpstreamBody(t *testing.T) {
	const secret = "PLAYBOOK-PROMPT-MUST-NOT-LEAK"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":"` + secret + `"}`))
	}))
	defer srv.Close()

	_, err := newTestClient(srv.URL, 1).Complete(t.Context(), Request{User: secret})
	if err == nil {
		t.Fatal("期望上游 HTTP 错误")
	}
	got := err.Error()
	if strings.Contains(got, secret) {
		t.Fatalf("错误链泄露上游响应体: %q", got)
	}
	for _, want := range []string{"HTTP 502", "捕获响应体", "sha256="} {
		if !strings.Contains(got, want) {
			t.Fatalf("安全错误摘要缺少 %q: %q", want, got)
		}
	}
	if code := types.CodeOf(err); code != types.CodeLLMUnavailable {
		t.Fatalf("安全摘要不得改变错误映射，实际 %s", code)
	}
}

// TestComplete500NoRetry 5xx 映射为 CodeLLMUnavailable，且客户端不自行重试
// （重试是上层的事）：断言上游只收到 1 次请求。
func TestComplete500NoRetry(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, 5)
	_, err := c.Complete(context.Background(), Request{User: "hi"})
	if err == nil {
		t.Fatal("期望错误，实际为 nil")
	}
	if code := types.CodeOf(err); code != types.CodeLLMUnavailable {
		t.Errorf("错误码 = %s, 期望 %s", code, types.CodeLLMUnavailable)
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("上游收到 %d 次请求，客户端不应重试（期望 1 次）", got)
	}
}

// TestCompleteTimeout ctx 超时必须能通过 errors.Is 还原出
// context.DeadlineExceeded（AppError 的 Cause 链）。
func TestCompleteTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // 挂住请求直到测试结束，逼出客户端 ctx 超时
	}))
	defer srv.Close()
	defer close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	c := newTestClient(srv.URL, 5)
	_, err := c.Complete(ctx, Request{User: "hi"})
	if err == nil {
		t.Fatal("期望超时错误，实际为 nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("errors.Is(err, context.DeadlineExceeded) = false, err = %v", err)
	}
	if code := types.CodeOf(err); code != types.CodeLLMUnavailable {
		t.Errorf("错误码 = %s, 期望 %s（码表无 LLM_TIMEOUT，取最接近可重试码）", code, types.CodeLLMUnavailable)
	}
}

// TestCompleteSemaphoreSerial 信号量 = 1 时两并发请求必须串行：
// 上游观测到的最大并发数为 1。
func TestCompleteSemaphoreSerial(t *testing.T) {
	var current, maxSeen atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := current.Add(1)
		// CAS 循环维护观测到的最大并发数。
		for {
			m := maxSeen.Load()
			if n <= m || maxSeen.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(150 * time.Millisecond) // 拉长在途窗口，确保并发能被观测到
		current.Add(-1)
		w.Write([]byte(okResponseBody))
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, 1) // 信号量上限 1

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.Complete(context.Background(), Request{User: "hi"}); err != nil {
				t.Errorf("Complete 返回错误: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := maxSeen.Load(); got != 1 {
		t.Errorf("上游观测到最大并发 = %d, 信号量为 1 时期望 1", got)
	}
}

// TestCostUSD 按模型三段单价核对一组已知值（M4 起 flash/pro 双模型分价）。
func TestCostUSD(t *testing.T) {
	tests := []struct {
		name                  string
		model                 string
		hit, miss, completion int
		want                  float64
	}{
		// 整百万便于人肉核对：1M 命中 + 2M 未命中 + 0.5M 输出
		// flash = 0.0028 + 0.28 + 0.14 = 0.4228
		{"flash整百万", "deepseek-v4-flash", 1_000_000, 2_000_000, 500_000, 0.4228},
		// pro = 0.003625 + 0.87 + 0.435 = 1.308625
		{"pro整百万", "deepseek-v4-pro", 1_000_000, 2_000_000, 500_000, 1.308625},
		// 贴近真实单次调用量级：
		// 100/1e6*0.0028 + 900/1e6*0.14 + 500/1e6*0.28 = 0.00026628
		{"flash真实量级", "deepseek-v4-flash", 100, 900, 500, 0.00026628},
		// 未知模型按最贵档（pro）兜底：宁高估不低估。
		{"未知模型按pro兜底", "some-future-model", 1_000_000, 2_000_000, 500_000, 1.308625},
		{"全零", "deepseek-v4-flash", 0, 0, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CostUSD(tt.model, tt.hit, tt.miss, tt.completion)
			if math.Abs(got-tt.want) > 1e-12 {
				t.Errorf("CostUSD(%s, %d, %d, %d) = %.10f, 期望 %.10f",
					tt.model, tt.hit, tt.miss, tt.completion, got, tt.want)
			}
		})
	}
}
