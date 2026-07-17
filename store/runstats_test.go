package store

import (
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestListSpanRunStats 是 DATABASE_URL 门控的集成测试（无则跳过，仓库惯例）：
// 按 span 聚合、错误计数、token 求和、p95 分位、缓存命中三态（true/false/NULL）、
// 窗口过滤。
func TestListSpanRunStats(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 ListSpanRunStats 集成测试")
	}
	ctx := t.Context()

	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 执行失败: %v", err)
	}
	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 建池失败: %v", err)
	}
	registerStoreClose(t, st)

	// span 名带 uuid 后缀：llm_calls 是共享表，唯一标记保证聚合只命中本测试的行。
	span := "runstat_" + uuid.NewString()[:8]
	now := time.Now().UTC()

	// 5 行：1 行错误、latency 10/20/30/40/1000（p95 落在长尾附近）、
	// 缓存命中 true/false/NULL 三态、token 与成本可加和。
	type row struct {
		latency int
		errStr  string
		cache   *bool
		prompt  int
		compl   int
		cost    float64
		age     time.Duration
	}
	bt, bf := true, false
	rowsIn := []row{
		{10, "", &bt, 100, 10, 0.001, time.Minute},
		{20, "", &bt, 100, 10, 0.001, time.Minute},
		{30, "", &bf, 100, 10, 0.001, time.Minute},
		{40, "", nil, 100, 10, 0.001, time.Minute},
		{1000, "上游超时", nil, 0, 0, 0, time.Minute},
	}
	// 窗口外 1 行：不应计入。
	outsider := row{99999, "", nil, 999999, 999999, 9.9, 48 * time.Hour}

	insert := func(r row) {
		_, err := st.pool.Exec(ctx,
			`INSERT INTO llm_calls (trace_id, span_name, provider, model,
			     prompt_tokens, completion_tokens, latency_ms, cost_usd,
			     prefix_cache_hit, error, created_at)
			 VALUES ($1, $2, 'test', 'test-model', $3, $4, $5, $6, $7, $8, $9)`,
			uuid.NewString(), span, r.prompt, r.compl, r.latency, r.cost,
			r.cache, r.errStr, now.Add(-r.age))
		if err != nil {
			t.Fatalf("插入 llm_calls 测试行失败: %v", err)
		}
	}
	for _, r := range rowsIn {
		insert(r)
	}
	insert(outsider)

	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, st, `DELETE FROM llm_calls WHERE span_name = $1`, span)
	})

	stats, err := st.ListSpanRunStats(ctx, now.Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("ListSpanRunStats() 失败: %v", err)
	}
	var got *SpanRunStat
	for i := range stats {
		if stats[i].SpanName == span {
			got = &stats[i]
			break
		}
	}
	if got == nil {
		t.Fatalf("结果中没有本测试的 span %q", span)
	}

	if got.Calls != 5 {
		t.Errorf("Calls = %d，期望 5（窗口外行不计入）", got.Calls)
	}
	if got.Errors != 1 {
		t.Errorf("Errors = %d，期望 1", got.Errors)
	}
	if got.PromptTokens != 400 || got.CompletionTokens != 40 {
		t.Errorf("tokens = (%d, %d)，期望 (400, 40)", got.PromptTokens, got.CompletionTokens)
	}
	if want := 0.004; got.CostUSD < want-1e-9 || got.CostUSD > want+1e-9 {
		t.Errorf("CostUSD = %v，期望 %v", got.CostUSD, want)
	}
	// 浮点聚合按容差比较（percentile_cont 的线性插值实测带 ~1e-13 浮点尾巴）。
	closeTo := func(got, want float64) bool { return got > want-1e-6 && got < want+1e-6 }
	if want := 220.0; !closeTo(got.AvgLatencyMs, want) { // (10+20+30+40+1000)/5
		t.Errorf("AvgLatencyMs = %v，期望 %v", got.AvgLatencyMs, want)
	}
	// percentile_cont(0.95) 对 [10,20,30,40,1000] 线性插值 = 40 + 0.8*(1000-40) = 808。
	if want := 808.0; !closeTo(got.P95LatencyMs, want) {
		t.Errorf("P95LatencyMs = %v，期望 %v", got.P95LatencyMs, want)
	}
	if got.CacheHits != 2 || got.CacheKnown != 3 {
		t.Errorf("cache = (%d 命中 / %d 已知)，期望 (2 / 3)——NULL 不进分母", got.CacheHits, got.CacheKnown)
	}
}
