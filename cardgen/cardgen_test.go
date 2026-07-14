package cardgen

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/types"
)

// newTestCardGen 起一个仿 DeepSeek 的 httptest.Server，固定返回 replyContent。
func newTestCardGen(t *testing.T, status int, replyContent string) *CardGen {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":{"message":"boom"}}`))
			return
		}
		resp := map[string]any{
			"model": "deepseek-chat",
			"choices": []any{
				map[string]any{"message": map[string]any{"content": replyContent}},
			},
			"usage": map[string]any{"prompt_tokens": 20, "completion_tokens": 40},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)

	cli := llm.New(config.LLMConfig{
		Provider:      "deepseek",
		BaseURL:       srv.URL,
		APIKey:        "test-key",
		Model:         "deepseek-chat",
		MaxConcurrent: 1,
	})
	return New(cli, llm.NewRecorder(nil))
}

// parseCard 解析飞书卡片 JSON，取出 body.elements[0].content（markdown 正文）。
// 校验产出确实是合法卡片，并让断言能直接检查正文内容。
func parseCard(t *testing.T, cardJSON string) string {
	t.Helper()
	var card struct {
		Schema string `json:"schema"`
		Body   struct {
			Elements []struct {
				Tag     string `json:"tag"`
				Content string `json:"content"`
			} `json:"elements"`
		} `json:"body"`
	}
	if err := json.Unmarshal([]byte(cardJSON), &card); err != nil {
		t.Fatalf("产出不是合法卡片 JSON: %v\n原文: %s", err, cardJSON)
	}
	if card.Schema != "2.0" {
		t.Fatalf("卡片 schema = %q, 期望 2.0", card.Schema)
	}
	if len(card.Body.Elements) == 0 || card.Body.Elements[0].Tag != "markdown" {
		t.Fatalf("卡片缺少 markdown 元素: %s", cardJSON)
	}
	return card.Body.Elements[0].Content
}

func TestGenerate_ProducesValidCardWithLink(t *testing.T) {
	reply := "**AI 芯片新突破**\n\n某公司发布新一代推理芯片。\n\n为什么与你有关：你关注半导体赛道。"
	cg := newTestCardGen(t, http.StatusOK, reply)

	item := types.ScoredItem{
		Item: types.ContentItem{
			ID:    42,
			Title: "AI 芯片",
			URL:   "https://example.com/ai-chip",
		},
		Score: 88,
	}
	cardJSON, err := cg.Generate(context.Background(), 1, item, "trace-c1")
	if err != nil {
		t.Fatalf("Generate 意外报错: %v", err)
	}

	md := parseCard(t, cardJSON)
	if !strings.Contains(md, "AI 芯片新突破") {
		t.Errorf("卡片正文应包含模型解读，实得: %q", md)
	}
	if !strings.Contains(md, item.Item.URL) {
		t.Errorf("卡片正文必须带原文链接 %q，实得: %q", item.Item.URL, md)
	}
}

func TestGenerate_FallbackWhenModelReturnsEmpty(t *testing.T) {
	cg := newTestCardGen(t, http.StatusOK, "   ") // 模型返回空白

	item := types.ScoredItem{
		Item: types.ContentItem{
			ID:    43,
			Title: "兜底标题",
			URL:   "https://example.com/x",
		},
	}
	cardJSON, err := cg.Generate(context.Background(), 1, item, "trace-c2")
	if err != nil {
		t.Fatalf("Generate 意外报错: %v", err)
	}
	md := parseCard(t, cardJSON)
	if !strings.Contains(md, "兜底标题") {
		t.Errorf("空模型输出时应以标题兜底，实得: %q", md)
	}
	if !strings.Contains(md, item.Item.URL) {
		t.Errorf("兜底卡片仍须带原文链接，实得: %q", md)
	}
}

func TestGenerate_NoLinkWhenURLEmpty(t *testing.T) {
	cg := newTestCardGen(t, http.StatusOK, "**标题**\n\n摘要。")

	item := types.ScoredItem{Item: types.ContentItem{ID: 44, Title: "无链接"}}
	cardJSON, err := cg.Generate(context.Background(), 1, item, "trace-c3")
	if err != nil {
		t.Fatalf("Generate 意外报错: %v", err)
	}
	md := parseCard(t, cardJSON)
	if strings.Contains(md, "阅读原文") {
		t.Errorf("URL 为空时不应拼出空链接行，实得: %q", md)
	}
}

func TestGenerate_ReturnsErrorOnUpstreamFailure(t *testing.T) {
	cg := newTestCardGen(t, http.StatusInternalServerError, "")

	item := types.ScoredItem{Item: types.ContentItem{ID: 45, URL: "https://example.com/y"}}
	_, err := cg.Generate(context.Background(), 1, item, "trace-c4")
	if err == nil {
		t.Fatal("上游 5xx 应向上抛错供 Temporal 重试")
	}
	if !errors.Is(err, types.ErrLLM) {
		t.Fatalf("期望 errors.Is(err, ErrLLM)，实得: %v", err)
	}
}
