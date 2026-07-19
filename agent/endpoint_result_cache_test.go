package agent

// 大结果缓存句柄 + read_endpoint_result 的行为锁定（端点注册表契约 §3.5）。

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/llm"
)

// testLimits 与生产同源：按 agent 模型（deepseek-v4-pro，1M 窗口）派生。
var testLimits = llm.DeriveInlineLimits(1_000_000)

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
	tool := &readEndpointResultTool{cache: c, perRead: testLimits.PerCall}

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
	long := strings.Repeat("字", testLimits.PerCall+500)
	c := newResultCache()
	h := c.put(1, "ep", []byte(`"`+long+`"`)) // 合法 JSON 字符串
	tool := &readEndpointResultTool{cache: c, perRead: testLimits.PerCall}

	first := execRead(t, tool, 1, fmt.Sprintf(`{"handle":%q}`, h))
	if !strings.Contains(first, fmt.Sprintf(`"offset":%d`, testLimits.PerCall)) {
		t.Errorf("超限续读应给下一步 offset: %s", first[len(first)-160:])
	}
	rest := execRead(t, tool, 1, fmt.Sprintf(`{"handle":%q,"offset":%d}`, h, testLimits.PerCall))
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
	tool := &readEndpointResultTool{cache: c, perRead: testLimits.PerCall}

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

// ────────── 对抗审查修复的回归锁定（2026-07-18）──────────

func TestTruncationNote_HonestAboutIncompleteCache(t *testing.T) {
	// MEDIUM：上游响应本身被 2MiB 上限截断时，缓存的也只是前缀——提示绝不能
	// 宣称「完整数据已缓存」，否则模型读完前缀会以为拿到了全量。
	body := []byte(`{"data":{"items":[1,2,3]}}`)
	full := buildTruncationNote("res-1", len(body), body, false, testLimits.PerCall)
	if !strings.Contains(full, "完整数据已缓存") {
		t.Errorf("未截断时应声明完整: %s", full)
	}
	partial := buildTruncationNote("res-1", len(body), body, true, testLimits.PerCall)
	if strings.Contains(partial, "完整数据已缓存") || !strings.Contains(partial, "数据不完整") {
		t.Errorf("上游超限时必须声明不完整: %s", partial)
	}
	if !strings.Contains(partial, "缩小查询范围") {
		t.Error("不完整时应给出下一步（缩小范围/分页重查）")
	}
	// LOW：TTL 措辞不能把 LRU 逐出说成保证。
	if !strings.Contains(full, "最长 30 分钟") || !strings.Contains(full, "提前失效") {
		t.Errorf("TTL 措辞应承认可能提前失效: %s", full)
	}
}

func TestSliceRunes(t *testing.T) {
	s := "一二三四五"
	for _, tc := range []struct {
		offset, limit int
		want          string
		wantMore      bool
	}{
		{0, 2, "一二", true},
		{2, 2, "三四", true},
		{4, 2, "五", false},
		{0, 99, "一二三四五", false},
	} {
		got, total, more := sliceRunes(s, tc.offset, tc.limit)
		if got != tc.want || more != tc.wantMore || total != 5 {
			t.Errorf("sliceRunes(%d,%d) = %q,%d,%v；want %q,5,%v",
				tc.offset, tc.limit, got, total, more, tc.want, tc.wantMore)
		}
	}
	// 越界：空片段 + 正确总数，调用方据此给终止文案。
	if got, total, more := sliceRunes(s, 99, 10); got != "" || total != 5 || more {
		t.Errorf("越界应返回空片段: %q,%d,%v", got, total, more)
	}
}

func TestRenderShape_ArrayConsumesDepth(t *testing.T) {
	// LOW：嵌套数组不得穿透深度预算。
	shape := summarizeJSONStructure([]byte(`[[[["deep"]]]]`))
	if strings.Count(shape, "array[") > 3 {
		t.Errorf("嵌套数组穿透了深度限制: %s", shape)
	}
}

// TestInlineBudget_PerMessageCurve 钉死每消息累计预算曲线（契约 §3.5）：
// 前几次给满额、预算耗尽后降到保底，绝不给 0（模型看不见任何内容会瞎猜）。
// 这是单次上限从 6000 放大到 40000 的安全网——没有它，msgCap=10 次调用
// 最坏能内联 400k 字符，且 FC 多轮历史重发会把这笔账乘以轮数。
func TestInlineBudget_PerMessageCurve(t *testing.T) {
	s := &toolRunState{}
	var total, calls int
	for i := 0; i < 10; i++ {
		got := s.inlineBudget(testLimits)
		if got < testLimits.MinPerCall {
			t.Fatalf("第 %d 次预算 %d 低于保底 %d", i+1, got, testLimits.MinPerCall)
		}
		if i < 3 && got != testLimits.PerCall {
			t.Errorf("第 %d 次应给满额 %d，实得 %d", i+1, testLimits.PerCall, got)
		}
		s.inlinedRunes += got
		total += got
		calls++
	}
	// 10 次（msgCap 上限）累计不得失控：预算 + 保底×剩余次数是硬上界。
	if max := testLimits.MsgBudget + testLimits.MinPerCall*calls; total > max {
		t.Errorf("累计内联 %d 超硬上界 %d", total, max)
	}
	// nil state（RunOnce/A2A 等无状态轨）不 panic 且给满额。
	if (*toolRunState)(nil).inlineBudget(testLimits) != testLimits.PerCall {
		t.Error("nil state 应给满额")
	}
}
