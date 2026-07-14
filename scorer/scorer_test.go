package scorer

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/types"
)

// newTestScorer 起一个仿 DeepSeek 的 httptest.Server：不论请求内容，
// 都用 replyContent 作为 choices[0].message.content 返回，status 由入参定。
// 这样测试聚焦于 Score 对"模型说了什么"的解析行为，而非网络细节。
func newTestScorer(t *testing.T, status int, replyContent string) *Scorer {
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
			"usage": map[string]any{"prompt_tokens": 10, "completion_tokens": 3},
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
	// Recorder 传 nil store：Record 对 nil store 是 no-op，测试无需数据库。
	return New(cli, llm.NewRecorder(nil), nil)
}

func TestScore_ParsesNumberFromProse(t *testing.T) {
	cases := []struct {
		name  string
		reply string
		want  float64
	}{
		{"纯数字", "85", 85},
		{"混在话里", "这条我打85分", 85},
		{"小数", "95.5", 95.5},
		{"带单位噪声", "我打85分，满分100", 85}, // 取首个数字，不被满分 100 带偏
		{"越界上夹逼", "150", 100},
		{"越界下夹逼", "-20", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sc := newTestScorer(t, http.StatusOK, tc.reply)
			got, err := sc.Score(context.Background(), 1, types.ContentItem{ID: 7, Title: "t"}, "trace-1")
			if err != nil {
				t.Fatalf("Score 意外报错: %v", err)
			}
			if got != tc.want {
				t.Fatalf("Score = %v, 期望 %v（模型回 %q）", got, tc.want, tc.reply)
			}
		})
	}
}

func TestScore_FallbackToMedianOnUnparseable(t *testing.T) {
	sc := newTestScorer(t, http.StatusOK, "这条内容一般般，没法给分")
	got, err := sc.Score(context.Background(), 1, types.ContentItem{ID: 7, Title: "t"}, "trace-2")
	if err != nil {
		t.Fatalf("解析失败不应报错，而应回退中位分: %v", err)
	}
	if got != medianScore {
		t.Fatalf("Score = %v, 期望回退中位分 %v", got, medianScore)
	}
}

func TestScore_ReturnsErrorOnUpstreamFailure(t *testing.T) {
	sc := newTestScorer(t, http.StatusInternalServerError, "")
	_, err := sc.Score(context.Background(), 1, types.ContentItem{ID: 7}, "trace-3")
	if err == nil {
		t.Fatal("上游 5xx 应向上抛错供 Temporal 重试，而非吞掉")
	}
	// 上游 5xx 映射为可重试的 LLM 错误族。
	if !errors.Is(err, types.ErrLLM) {
		t.Fatalf("期望 errors.Is(err, ErrLLM)，实得: %v", err)
	}
}

func TestParseScore(t *testing.T) {
	cases := []struct {
		raw    string
		want   float64
		wantOK bool
	}{
		{"85", 85, true},
		{"这条我打85分", 85, true},
		{"95.5", 95.5, true},
		{"150", 100, true},
		{"-3", 0, true},
		{"没有数字", 0, false},
		{"", 0, false},
	}
	for _, tc := range cases {
		got, ok := parseScore(tc.raw)
		if ok != tc.wantOK || (ok && got != tc.want) {
			t.Errorf("parseScore(%q) = (%v,%v), 期望 (%v,%v)", tc.raw, got, ok, tc.want, tc.wantOK)
		}
	}
}
