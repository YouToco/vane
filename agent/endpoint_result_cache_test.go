package agent

// 大结果缓存句柄 + read_endpoint_result 的行为锁定（端点注册表契约 §3.5）。

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func execRead(t *testing.T, tool Tool, userID int64, args string) string {
	t.Helper()
	out, err := tool.Execute(context.Background(), userID, json.RawMessage(args))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	return out
}

func TestResultCache_PathExtraction(t *testing.T) {
	body := []byte(`{"code":200,"data":{"items":[{"id":"a","text":"第一条"},{"id":"b","text":"第二条"},{"id":"c","text":"第三条"}],"total":707}}`)
	c := newResultCache()
	h := c.put(1, "ep", body)
	tool := &readEndpointResultTool{cache: c}

	out := execRead(t, tool, 1, fmt.Sprintf(`{"handle":%q,"path":"data.items[1].text"}`, h))
	if out != `"第二条"` {
		t.Errorf("子树取数漂移: %q", out)
	}
	out = execRead(t, tool, 1, fmt.Sprintf(`{"handle":%q,"path":"data.items[0:2]"}`, h))
	if !strings.Contains(out, `"a"`) || !strings.Contains(out, `"b"`) || strings.Contains(out, `"c"`) {
		t.Errorf("切片取数漂移: %q", out)
	}
	out = execRead(t, tool, 1, fmt.Sprintf(`{"handle":%q,"path":"data.total"}`, h))
	if out != "707" {
		t.Errorf("json.Number 应保原始十进制串: %q", out)
	}
	// 路径错误面向模型自纠。
	out = execRead(t, tool, 1, fmt.Sprintf(`{"handle":%q,"path":"data.nope"}`, h))
	if !strings.Contains(out, "不存在") {
		t.Errorf("键不存在应给自纠提示: %q", out)
	}
	out = execRead(t, tool, 1, fmt.Sprintf(`{"handle":%q,"path":"data.items[9]"}`, h))
	if !strings.Contains(out, "越界") {
		t.Errorf("下标越界应给自纠提示: %q", out)
	}
}

func TestResultCache_OffsetPaging(t *testing.T) {
	long := strings.Repeat("字", endpointResultMaxRunes+500)
	c := newResultCache()
	h := c.put(1, "ep", []byte(`"`+long+`"`)) // 合法 JSON 字符串
	tool := &readEndpointResultTool{cache: c}

	first := execRead(t, tool, 1, fmt.Sprintf(`{"handle":%q}`, h))
	if !strings.Contains(first, `"offset":6000`) {
		t.Errorf("超限续读应给下一步 offset: %s", first[len(first)-160:])
	}
	rest := execRead(t, tool, 1, fmt.Sprintf(`{"handle":%q,"offset":%d}`, h, endpointResultMaxRunes))
	if strings.Contains(rest, `"offset"`) && strings.Contains(rest, "续读") {
		t.Errorf("第二页应已读完，不该再给续读提示: %s", rest[len(rest)-160:])
	}
	beyond := execRead(t, tool, 1, fmt.Sprintf(`{"handle":%q,"offset":999999}`, h))
	if !strings.Contains(beyond, "没有更多内容") {
		t.Errorf("越界 offset 应明确终止: %q", beyond)
	}
}

func TestResultCache_OwnerAndExpiry(t *testing.T) {
	c := newResultCache()
	h := c.put(1, "ep", []byte(`{}`))
	tool := &readEndpointResultTool{cache: c}

	// 换个 userID 读不到（句柄可猜，多租户下不绑定就是跨用户读取面）。
	if out := execRead(t, tool, 2, fmt.Sprintf(`{"handle":%q}`, h)); !strings.Contains(out, "不存在或已过期") {
		t.Errorf("跨用户读取应 miss: %q", out)
	}
	// 过期读不到。
	c.mu.Lock()
	c.entries[h].at = time.Now().Add(-resultCacheTTL - time.Minute)
	c.mu.Unlock()
	if out := execRead(t, tool, 1, fmt.Sprintf(`{"handle":%q}`, h)); !strings.Contains(out, "不存在或已过期") {
		t.Errorf("过期句柄应 miss: %q", out)
	}
}

func TestResultCache_LRUEviction(t *testing.T) {
	c := newResultCache()
	first := c.put(1, "ep", []byte(`{}`))
	for i := 0; i < resultCacheMaxEntries; i++ {
		c.put(1, "ep", []byte(`{}`))
	}
	if _, ok := c.get(1, first); ok {
		t.Error("超量后最旧条目应被逐出")
	}
	if len(c.entries) > resultCacheMaxEntries {
		t.Errorf("缓存条目 %d 超上限 %d", len(c.entries), resultCacheMaxEntries)
	}
}

func TestSummarizeJSONStructure(t *testing.T) {
	shape := summarizeJSONStructure([]byte(`{"code":200,"data":{"items":[{"id":"a","user":{"n":1}}],"cursor":"x"}}`))
	for _, want := range []string{"code:num", "array[1]", "cursor:str"} {
		if !strings.Contains(shape, want) {
			t.Errorf("结构摘要缺 %q: %s", want, shape)
		}
	}
	if summarizeJSONStructure([]byte("not json")) != "" {
		t.Error("非 JSON 不该给摘要")
	}
}

// TestToolCountBudget 钉死在场工具数安全线（契约 §4.1）：静态面 + 激活上限 < 30。
// read_endpoint_result 入列时把 maxActivatedEndpoints 15→14 换来的预算，谁再加
// 静态工具谁负责重算这笔账——本测试就是那张账单。
func TestToolCountBudget(t *testing.T) {
	ep := newTestEndpointTools(&fakeInvoker{}, &fakeCounter{}, 10, 200)
	static := len(BuildTools(nil, nil, nil, nil, ep, nil))
	if got := static + maxActivatedEndpoints; got >= 30 {
		t.Errorf("在场工具数 %d（静态 %d + 激活上限 %d）触及 30 工具退化线（RAG-MCP 证据，契约 §4.1）",
			got, static, maxActivatedEndpoints)
	}
}
