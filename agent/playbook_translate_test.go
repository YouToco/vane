package agent

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
)

// TestLLMPlaybookTranslator 覆盖生产翻译器 Translate 的 llm.Do 胶水：请求装配
// （thinking:disabled / 温度 0 / max_tokens / 默认模型）、空手册短路不打 LLM、上游出错回传。
// 这三条分支此前只有 fakeTranslator、真胶水零覆盖——尤其 DisableThinking 一旦回归，V4 思维链
// 会吃光预算致 content 恒空、每次编译静默零源，而无一测试变红（对抗审查 test-gap 项）。
func TestLLMPlaybookTranslator(t *testing.T) {
	newTranslator := func(serverURL string) *llmPlaybookTranslator {
		cli := llm.New(config.LLMConfig{
			Provider: "deepseek", BaseURL: serverURL, APIKey: "test-key",
			Model: "deepseek-v4-flash", MaxConcurrent: 2,
		})
		return &llmPlaybookTranslator{cli: cli, rec: llm.NewRecorder(nil)}
	}
	cannedPlan := func(w http.ResponseWriter, content string) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model":   "deepseek-v4-flash",
			"choices": []map[string]any{{"message": map[string]any{"role": "assistant", "content": content}}},
			"usage":   map[string]any{"prompt_tokens": 10, "completion_tokens": 5, "prompt_cache_hit_tokens": 4, "prompt_cache_miss_tokens": 6},
		})
	}

	t.Run("请求装配正确 + 计划回传", func(t *testing.T) {
		var gotBody map[string]any
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewDecoder(r.Body).Decode(&gotBody)
			cannedPlan(w, `{"sources":[{"platform":"web","capability":"search","query":"Anthropic","include_domains":["anthropic.com"]}]}`)
		}))
		defer srv.Close()

		raw, err := newTranslator(srv.URL).Translate(context.Background(), 7, "只要 Anthropic 官方")
		if err != nil {
			t.Fatalf("Translate 失败: %v", err)
		}
		if n := countPlanSources(raw); n != 1 {
			t.Fatalf("应编译出 1 个源, 实得 %d: %s", n, raw)
		}
		// 关键回归守卫：thinking 必须 disabled（否则 V4 思维链吃光预算、content 恒空）。
		th, ok := gotBody["thinking"].(map[string]any)
		if !ok || th["type"] != "disabled" {
			t.Fatalf("必须携带 thinking:{type:disabled}, 实得 %v", gotBody["thinking"])
		}
		if tmp, _ := gotBody["temperature"].(float64); tmp != 0 {
			t.Errorf("temperature 应为 0, 实得 %v", gotBody["temperature"])
		}
		if mt, _ := gotBody["max_tokens"].(float64); int(mt) != planTranslateMaxTokens {
			t.Errorf("max_tokens 应为 %d, 实得 %v", planTranslateMaxTokens, gotBody["max_tokens"])
		}
		if m, _ := gotBody["model"].(string); m != "deepseek-v4-flash" {
			t.Errorf("model 应为 client 默认 deepseek-v4-flash, 实得 %v", gotBody["model"])
		}
		msgs, _ := gotBody["messages"].([]any)
		if len(msgs) == 0 {
			t.Fatal("请求应带 messages")
		}
		last, _ := msgs[len(msgs)-1].(map[string]any)
		if last["role"] != "user" || last["content"] != "只要 Anthropic 官方" {
			t.Errorf("末条消息应是用户手册正文, 实得 %v", last)
		}
	})

	t.Run("空手册短路：不打 LLM、直接得零源计划", func(t *testing.T) {
		var hits int
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits++
			cannedPlan(w, `{"sources":[]}`)
		}))
		defer srv.Close()

		raw, err := newTranslator(srv.URL).Translate(context.Background(), 7, "   ")
		if err != nil {
			t.Fatalf("空手册不应报错: %v", err)
		}
		if hits != 0 {
			t.Fatalf("空手册不该打 LLM, 实际调用 %d 次", hits)
		}
		if countPlanSources(raw) != 0 {
			t.Fatalf("空手册应得零源计划: %s", raw)
		}
	})

	t.Run("上游出错 → Translate 回非 nil err（保留既有计划）", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		}))
		defer srv.Close()

		if _, err := newTranslator(srv.URL).Translate(context.Background(), 7, "只要官方源"); err == nil {
			t.Fatal("上游 500 应让 Translate 回非 nil err（调用方据此保留既有计划）")
		}
	})
}

// compilePlan 是编译层纯函数核心：解析模型输出 + 逐源 sourcespec 校验。此处覆盖全部分支，
// 无需真 LLM（真 Translate 只是"发一次 llm.Do 再把 Content 交给它"的薄胶水）。
func TestCompilePlan(t *testing.T) {
	parse := func(t *testing.T, raw json.RawMessage) FetchPlan {
		t.Helper()
		var p FetchPlan
		if err := json.Unmarshal(raw, &p); err != nil {
			t.Fatalf("产出的计划不是合法 JSON: %v (%s)", err, raw)
		}
		return p
	}

	t.Run("单个 web/search 源正常编译", func(t *testing.T) {
		raw, err := compilePlan(`{"sources":[{"platform":"web","capability":"search","query":"AI 新闻","include_domains":["anthropic.com"]}]}`)
		if err != nil {
			t.Fatalf("不应报错: %v", err)
		}
		p := parse(t, raw)
		if len(p.Sources) != 1 {
			t.Fatalf("应编译出 1 个源, 实得 %d", len(p.Sources))
		}
		s := p.Sources[0]
		if s.Platform != "web" || s.Capability != "search" {
			t.Fatalf("平台/能力不符: %+v", s)
		}
		// URL 即幂等键：web/search 合成 vane:// 且带归一化后的 include_domains。
		if !strings.HasPrefix(s.URL, "vane://web/search?") || !strings.Contains(s.URL, "include_domains=anthropic.com") {
			t.Fatalf("幂等键不符: %q", s.URL)
		}
		if !strings.Contains(string(s.Config), "anthropic.com") {
			t.Fatalf("config 应含 include_domains: %s", s.Config)
		}
	})

	t.Run("多源全部有效", func(t *testing.T) {
		raw, err := compilePlan(`{"sources":[
			{"platform":"web","capability":"feed","url":"https://openai.com/blog/rss.xml"},
			{"platform":"web","capability":"search","query":"发布"}
		]}`)
		if err != nil {
			t.Fatalf("不应报错: %v", err)
		}
		if p := parse(t, raw); len(p.Sources) != 2 {
			t.Fatalf("应编译出 2 个源, 实得 %d", len(p.Sources))
		}
	})

	t.Run("校验不过的单源被丢弃、其余保留", func(t *testing.T) {
		// 第一个缺 query（web/search 必填）→ sourcespec.Build 拒绝 → 丢弃；第二个有效。
		raw, err := compilePlan(`{"sources":[
			{"platform":"web","capability":"search"},
			{"platform":"web","capability":"feed","url":"https://a.com/rss"}
		]}`)
		if err != nil {
			t.Fatalf("坏源丢弃不算致命, 不应报错: %v", err)
		}
		p := parse(t, raw)
		if len(p.Sources) != 1 || p.Sources[0].Capability != "feed" {
			t.Fatalf("应只留下有效的 feed 源, 实得 %+v", p.Sources)
		}
	})

	t.Run("缺 platform/capability 跳过", func(t *testing.T) {
		raw, err := compilePlan(`{"sources":[{"query":"x"},{"platform":"web","capability":"search","query":"y"}]}`)
		if err != nil {
			t.Fatalf("不应报错: %v", err)
		}
		if p := parse(t, raw); len(p.Sources) != 1 {
			t.Fatalf("缺路由字段的源应跳过, 实得 %d", len(p.Sources))
		}
	})

	t.Run("模型给了源但全部无效 → errAllSourcesDropped（软失败保留既有计划）", func(t *testing.T) {
		// 缺 query 的 web/search 被 Build 丢弃 → 有源输入但零有效输出：这是翻译质量失败，
		// 不是"无抓取意图"，必须软失败（让调用方保留既有计划），绝不用空计划把好计划冲掉。
		if _, err := compilePlan(`{"sources":[{"platform":"web","capability":"search"}]}`); !errors.Is(err, errAllSourcesDropped) {
			t.Fatalf("有源输入但全被丢应报 errAllSourcesDropped, 实得 %v", err)
		}
		// 全缺路由字段（被跳过而非 Build 失败）同样算翻译失败。
		if _, err := compilePlan(`{"sources":[{"query":"x"}]}`); !errors.Is(err, errAllSourcesDropped) {
			t.Fatalf("全缺 platform/capability 也应报 errAllSourcesDropped, 实得 %v", err)
		}
	})

	t.Run("模型本就返回零源 → 正当清空（空计划非错误）", func(t *testing.T) {
		raw, err := compilePlan(`{"sources":[]}`)
		if err != nil {
			t.Fatalf("模型无抓取意图返回零源是合法结果: %v", err)
		}
		if p := parse(t, raw); len(p.Sources) != 0 {
			t.Fatalf("应为空计划")
		}
	})

	t.Run("容忍 markdown 代码块围栏", func(t *testing.T) {
		raw, err := compilePlan("```json\n{\"sources\":[{\"platform\":\"xhs\",\"capability\":\"search\",\"keyword\":\"美妆\"}]}\n```")
		if err != nil {
			t.Fatalf("围栏应被剥离: %v", err)
		}
		if p := parse(t, raw); len(p.Sources) != 1 || p.Sources[0].Platform != "xhs" {
			t.Fatalf("围栏内 JSON 未正确解析: %+v", p.Sources)
		}
	})

	t.Run("容忍前后散文", func(t *testing.T) {
		raw, err := compilePlan(`好的，这是计划：{"sources":[{"platform":"x","capability":"user_posts","screen_name":"OpenAI"}]} 完成。`)
		if err != nil {
			t.Fatalf("散文包裹应被截取: %v", err)
		}
		if p := parse(t, raw); len(p.Sources) != 1 || p.Sources[0].Capability != "user_posts" {
			t.Fatalf("散文中 JSON 未正确解析: %+v", p.Sources)
		}
	})

	t.Run("取不出 JSON → errPlanUnparsable", func(t *testing.T) {
		if _, err := compilePlan("我没法完成这个任务"); !errors.Is(err, errPlanUnparsable) {
			t.Fatalf("无 JSON 应报 errPlanUnparsable, 实得 %v", err)
		}
	})

	t.Run("JSON 非法 → errPlanUnparsable", func(t *testing.T) {
		if _, err := compilePlan(`{"sources": [不是合法}`); !errors.Is(err, errPlanUnparsable) {
			t.Fatalf("非法 JSON 应报 errPlanUnparsable, 实得 %v", err)
		}
	})
}

func TestExtractJSONObject(t *testing.T) {
	cases := []struct{ in, want string }{
		{`{"a":1}`, `{"a":1}`},
		{"```json\n{\"a\":1}\n```", `{"a":1}`},
		{`前缀 {"a":{"b":2}} 后缀`, `{"a":{"b":2}}`},
		{`没有对象`, ``},
		{`}{`, ``}, // 第一个 { 在最后一个 } 之后 → 取不出
	}
	for _, c := range cases {
		if got := extractJSONObject(c.in); got != c.want {
			t.Errorf("extractJSONObject(%q)=%q, 期望 %q", c.in, got, c.want)
		}
	}
}

func TestCountPlanSources(t *testing.T) {
	if n := countPlanSources(nil); n != 0 {
		t.Errorf("nil 计划应为 0, 实得 %d", n)
	}
	if n := countPlanSources(json.RawMessage(`{"sources":[]}`)); n != 0 {
		t.Errorf("空计划应为 0, 实得 %d", n)
	}
	if n := countPlanSources(json.RawMessage(`{"sources":[{"platform":"web"},{"platform":"x"}]}`)); n != 2 {
		t.Errorf("两源计划应为 2, 实得 %d", n)
	}
	if n := countPlanSources(json.RawMessage(`不是JSON`)); n != 0 {
		t.Errorf("坏 JSON 应为 0, 实得 %d", n)
	}
}
