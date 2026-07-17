package probe

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/profilehint"
	"github.com/YouToco/vane/types"
)

// 本文件锁定 7 条判定的边界，**不需要 DB**：Store 是定义在消费方的窄接口
// （probe.go:51），分层的全部意义就在于此——阈值判定能在纯内存里逐个边界钉死。
//
// 为什么值得这么细：探针写反的代价不是"少报一个 bug"，而是**假绿**——
// 一个恒绿的探针比没有探针更危险，它让 Gate 复跑变成走过场（契约 §16 要求
// "部署后当天与次日复跑"，假绿会让人以为验过了）。故这里逐条把
// 「数据不足 ≠ 通过」「严格大于 vs 大于等于」「谁进分母」钉死。
//
// SQL 语义（RE2 与 PG ARE 的 \d 差异、width_bucket 折回、LIKE 开头锚定）另由
// store/observability_store_test.go 用真 PG 验：那些只有真库能验，替身验不出来。

// fakeStore 是 probe.Store 的测试替身，兼作接线回声位。
type fakeStore struct {
	traces  []types.ScoreTraceStat
	quality types.ScoreQualityStat
	dist    []types.ScoreBucket
	inj     types.ProfileInjectionStat
	neg     types.NegTailStat
	costs   []types.SpanDayCost
	models  []types.ModelUsage
	evolve  types.EvolveCallStat
	batches []types.PushBatchSummary
	profile *types.Profile

	// 回声位：记录 Run 实际传下来的参数，用于断言窗口与期望串没接反。
	gotSince      time.Time
	gotInjSince   time.Time
	gotNegSince   time.Time
	gotMinN       int
	gotTail       string
	gotBatchSince time.Time
	gotBatchLimit int
}

func (f *fakeStore) ListScoreTraceStats(_ context.Context, since time.Time, minN int) ([]types.ScoreTraceStat, error) {
	f.gotSince, f.gotMinN = since, minN
	return f.traces, nil
}

func (f *fakeStore) GetScoreQualityStat(context.Context, time.Time) (types.ScoreQualityStat, error) {
	return f.quality, nil
}

func (f *fakeStore) ListScoreDistribution(context.Context, time.Time) ([]types.ScoreBucket, error) {
	return f.dist, nil
}

func (f *fakeStore) GetProfileInjectionStat(_ context.Context, since time.Time) (types.ProfileInjectionStat, error) {
	f.gotInjSince = since
	return f.inj, nil
}

func (f *fakeStore) GetNegTailStat(_ context.Context, since time.Time, expectedTail string) (types.NegTailStat, error) {
	f.gotNegSince = since
	f.gotTail = expectedTail
	n := f.neg
	n.ExpectedTail = expectedTail
	return n, nil
}

func (f *fakeStore) ListSpanDayCosts(context.Context, time.Time) ([]types.SpanDayCost, error) {
	return f.costs, nil
}

func (f *fakeStore) ListModelUsage(context.Context, time.Time) ([]types.ModelUsage, error) {
	return f.models, nil
}

func (f *fakeStore) GetEvolveCallStat(context.Context, int64, time.Time) (types.EvolveCallStat, error) {
	return f.evolve, nil
}

func (f *fakeStore) ListPushBatchSummaries(_ context.Context, _ int64, since time.Time, limit int) ([]types.PushBatchSummary, error) {
	f.gotBatchSince, f.gotBatchLimit = since, limit
	return f.batches, nil
}

// GetProfile 以 ErrNotFound 表达"尚未首采"——不是错误（probe.go:145 同此语义）。
func (f *fakeStore) GetProfile(context.Context, int64) (*types.Profile, error) {
	if f.profile == nil {
		return nil, types.ErrNotFound
	}
	return f.profile, nil
}

// checkStatus 断言判定状态，失败时连 Summary 一起打印——Summary 是给 Boss 看的人话，
// 判定错了通常人话也错了，一起看比只看状态码省一轮排查。
func checkStatus(t *testing.T, got Result, want Status) {
	t.Helper()
	if got.Status != want {
		t.Errorf("期望 %s，实际 %s；Summary=%q", want, got.Status, got.Summary)
	}
}

// ---------- 探针 ① 分数区分度（§16.1） ----------

// 注意 n≥5 的 minN 门槛由 SQL 的 HAVING 实现（store/observability.go:65），
// judge 只看 DistinctCompletions——传进来的批次已经都是"够大的批"。
// minN 过滤本身由 store/observability_store_test.go 的真 PG 用例锁定。
func TestJudgeDiscrimination(t *testing.T) {
	tr := func(id string, n, distinct int) types.ScoreTraceStat {
		return types.ScoreTraceStat{TraceID: id, N: n, DistinctCompletions: distinct}
	}
	tests := []struct {
		name string
		in   []types.ScoreTraceStat
		want Status
	}{
		// 零批次必须是 yellow：vacuously green 正是本设计要防的假绿——
		// "窗口内没跑过批次"和"跑了且有区分度"是两件事，前者什么都没证明。
		{"零批次是数据不足而非通过", nil, StatusYellow},
		{"空切片同样是数据不足", []types.ScoreTraceStat{}, StatusYellow},
		{"整批同分即 M3 事故形状", []types.ScoreTraceStat{tr("t1", 5, 1)}, StatusRed},
		{"大批次整批同分", []types.ScoreTraceStat{tr("t1", 50, 1)}, StatusRed},
		{"distinct=2 即有区分度", []types.ScoreTraceStat{tr("t1", 5, 2)}, StatusGreen},
		{"多批次全部有区分度", []types.ScoreTraceStat{tr("t1", 50, 12), tr("t2", 8, 3)}, StatusGreen},
		{"混入任一同分批次即红", []types.ScoreTraceStat{tr("t1", 50, 12), tr("t2", 5, 1)}, StatusRed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			checkStatus(t, judgeDiscrimination(tc.in), tc.want)
		})
	}
}

// 红灯的 Detail 必须点名出事的那个 trace 并给出可直接复制的排查 SQL：
// 探针的产出是给人接着查的，只报"红了"等于把排查成本原样留给 Boss。
func TestJudgeDiscrimination_RedDetailNamesTrace(t *testing.T) {
	got := judgeDiscrimination([]types.ScoreTraceStat{
		{TraceID: "ok-trace", N: 50, DistinctCompletions: 9},
		{TraceID: "flat-trace", N: 5, DistinctCompletions: 1},
	})
	checkStatus(t, got, StatusRed)
	if !strings.Contains(got.Detail, "flat-trace") {
		t.Errorf("Detail 应点名同分的 trace，实际 %q", got.Detail)
	}
	// 表名纪律：真实表是 llm_calls，不是 llm_traces（CLAUDE.md:19 的笔误，本 PR 订正）。
	// 照笔误写的 SQL 报 relation does not exist，读起来像"没数据"（红线 6 的失败模式）。
	if !strings.Contains(got.Detail, "llm_calls") {
		t.Errorf("Detail 的排查 SQL 应查 llm_calls 表，实际 %q", got.Detail)
	}
}

// ---------- 探针 ② 中位分回退率（§16.2） ----------

func TestJudgeFallbackRate(t *testing.T) {
	tests := []struct {
		name string
		in   types.ScoreQualityStat
		want Status
	}{
		{"零成功调用是数据不足而非 0% 通过",
			types.ScoreQualityStat{OKTotal: 0}, StatusYellow},
		{"全部调用失败时仍是数据不足——一分未发，谈不上回退",
			types.ScoreQualityStat{OKTotal: 0, Errored: 100}, StatusYellow},
		// 红线是 24h **>10%**（严格大于）。恰好 10% 不红——阈值写成 >= 会让
		// 一个刚好压线的正常窗口报红，红灯喊多了就没人信了。
		{"恰好 10% 不红（红线是严格大于）",
			types.ScoreQualityStat{OKTotal: 100, NoDigit: 10}, StatusGreen},
		{"10.1% 击穿红线",
			types.ScoreQualityStat{OKTotal: 1000, NoDigit: 101}, StatusRed},
		{"全部回退",
			types.ScoreQualityStat{OKTotal: 10, NoDigit: 10}, StatusRed},
		{"零回退",
			types.ScoreQualityStat{OKTotal: 50, NoDigit: 0}, StatusGreen},
		// 分母只含成功调用：Errored 的条目被 Score 活动的"单条打分失败，跳过"分支直接跳过，
		// **没有发出任何分数**。把它算进回退率的话，一次上游 429 抖动就能冲爆红线。
		{"90 次调用失败不进分母：0/10 是 0% 而不是 90%",
			types.ScoreQualityStat{OKTotal: 10, NoDigit: 0, Errored: 90}, StatusGreen},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			checkStatus(t, judgeFallbackRate(tc.in), tc.want)
		})
	}
}

// 恰好 10% 这条边界完全压在浮点比较上，值得单独钉一次：
// FallbackRate 的除法结果必须与 fallbackRedRate 逐位相等，否则 `>` 的语义就悬了。
func TestJudgeFallbackRate_TenPercentIsExact(t *testing.T) {
	q := types.ScoreQualityStat{OKTotal: 100, NoDigit: 10}
	if got := q.FallbackRate(); got != fallbackRedRate {
		t.Fatalf("10/100 应与红线常量逐位相等，实际 %v vs %v", got, fallbackRedRate)
	}
}

// Errored 不进分母这件事必须在人话里说清楚：Boss 看到"回退率 0%"而后台有 90 次失败，
// 得能从 Detail 里读到失败去哪了，否则会以为探针瞎了。
func TestJudgeFallbackRate_DetailAccountsForErrored(t *testing.T) {
	got := judgeFallbackRate(types.ScoreQualityStat{OKTotal: 10, NoDigit: 0, Errored: 90})
	checkStatus(t, got, StatusGreen)
	if !strings.Contains(got.Detail, "90") {
		t.Errorf("Detail 应交代被排除的 90 次失败，实际 %q", got.Detail)
	}
	// 零成功但有失败时，yellow 的 Detail 同样要交代。
	got = judgeFallbackRate(types.ScoreQualityStat{OKTotal: 0, Errored: 7})
	checkStatus(t, got, StatusYellow)
	if !strings.Contains(got.Detail, "7") {
		t.Errorf("零成功时 Detail 应交代 7 次失败，实际 %q", got.Detail)
	}
}

// ---------- 探针 ③ 空输出零容忍（§16.3） ----------

func TestJudgeEmptyOutput(t *testing.T) {
	tests := []struct {
		name string
		in   types.ScoreQualityStat
		want Status
	}{
		// 全零且无调用 → yellow：没跑过打分不等于"0 次空输出"。
		{"窗口内无任何打分调用是数据不足",
			types.ScoreQualityStat{}, StatusYellow},
		// 一次都不容忍：EmptyNoError ⊂ NoDigit ⊂ OKTotal，出现 1 次就是 M3 形状。
		{"1 次空输出即红（零容忍）",
			types.ScoreQualityStat{OKTotal: 50, NoDigit: 1, EmptyNoError: 1}, StatusRed},
		{"整批空输出即红",
			types.ScoreQualityStat{OKTotal: 50, NoDigit: 50, EmptyNoError: 50}, StatusRed},
		{"有空输出时即便同窗口有失败也照红",
			types.ScoreQualityStat{OKTotal: 10, NoDigit: 3, EmptyNoError: 2, Errored: 5}, StatusRed},
		{"有成功调用且零空输出",
			types.ScoreQualityStat{OKTotal: 50, NoDigit: 2, EmptyNoError: 0}, StatusGreen},
		// 只有失败调用时不算"无调用"：确实跑过，且确实 0 次空输出。
		{"只有失败调用时零空输出成立",
			types.ScoreQualityStat{OKTotal: 0, Errored: 9}, StatusGreen},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			checkStatus(t, judgeEmptyOutput(tc.in), tc.want)
		})
	}
}

// M3 事故的成因是 DisableThinking 回归（CLAUDE.md 红线 1），红灯必须直接把这条指过去。
func TestJudgeEmptyOutput_RedDetailPointsAtDisableThinking(t *testing.T) {
	got := judgeEmptyOutput(types.ScoreQualityStat{OKTotal: 50, NoDigit: 50, EmptyNoError: 50})
	checkStatus(t, got, StatusRed)
	if !strings.Contains(got.Detail, "DisableThinking") {
		t.Errorf("Detail 应指向 DisableThinking（红线 1），实际 %q", got.Detail)
	}
}

// ---------- 探针 ④ 画像注入生效性（§16.4） ----------

func TestJudgeInjection(t *testing.T) {
	tests := []struct {
		name       string
		in         types.ProfileInjectionStat
		hasProfile bool
		want       Status
	}{
		// 自检位优先级最高：恒等式 Present+Absent==Total 破了，说明 scorer 的 prompt
		// 结构变了而探针字面量没跟上。此时下面任何判定都不可信——包括"无画像所以不适用"
		// 这个 yellow：先修探针再谈判定。
		{"字面量漂移即红（无画像时也照红，自检位优先）",
			types.ProfileInjectionStat{Total: 50, Absent: 0, Present: 0, Unrecognized: 50}, false, StatusRed},
		{"字面量漂移优先于有画像的绿",
			types.ProfileInjectionStat{Total: 50, Absent: 0, Present: 49, Unrecognized: 1}, true, StatusRed},
		{"字面量漂移优先于有画像的红",
			types.ProfileInjectionStat{Total: 50, Absent: 40, Present: 9, Unrecognized: 1}, true, StatusRed},
		// 无画像时写「暂无」是**正确行为**，不是失效——Absent>0 反而是对的。
		{"无画像时 Absent>0 是正确行为，探针不适用",
			types.ProfileInjectionStat{Total: 50, Absent: 50}, false, StatusYellow},
		{"无画像且无调用同样不适用",
			types.ProfileInjectionStat{}, false, StatusYellow},
		{"有画像但窗口内无调用是数据不足",
			types.ProfileInjectionStat{Total: 0}, true, StatusYellow},
		{"有画像却拿到 1 条「暂无」即红",
			types.ProfileInjectionStat{Total: 50, Absent: 1, Present: 49}, true, StatusRed},
		{"有画像却整窗口「暂无」",
			types.ProfileInjectionStat{Total: 50, Absent: 50}, true, StatusRed},
		{"有画像且 Present=Total",
			types.ProfileInjectionStat{Total: 50, Present: 50}, true, StatusGreen},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			checkStatus(t, judgeInjection(tc.in, tc.hasProfile), tc.want)
		})
	}
}

// 字面量漂移的红灯要说清"这是探针坏了不是画像坏了"，否则会有人去查画像链路白忙一天。
func TestJudgeInjection_DriftRedBlamesProbe(t *testing.T) {
	got := judgeInjection(types.ProfileInjectionStat{Total: 10, Unrecognized: 10}, true)
	checkStatus(t, got, StatusRed)
	if !strings.Contains(got.Detail, "profileHintPrefix") {
		t.Errorf("Detail 应点名要修的探针常量，实际 %q", got.Detail)
	}
}

// ---------- 探针 ⑤ 负面清单保尾（§16.5） ----------

func TestJudgeNegTail(t *testing.T) {
	tests := []struct {
		name string
		in   types.NegTailStat
		want Status
	}{
		// 无负面句 → 探针不适用。这里绝不能绿：慢通道还没产出负面清单，
		// F1 保尾逻辑一次都没被真正验过。
		{"当前画像无负面句则不适用",
			types.NegTailStat{ExpectedTail: ""}, StatusYellow},
		{"无负面句时即便有调用也不适用",
			types.NegTailStat{ExpectedTail: "", Total: 50, Intact: 0}, StatusYellow},
		{"有负面句但窗口内无调用是数据不足",
			types.NegTailStat{ExpectedTail: "不感兴趣：股市。", Total: 0}, StatusYellow},
		{"1 条尾巴丢失即红",
			types.NegTailStat{ExpectedTail: "不感兴趣：股市。", Total: 50, Intact: 49}, StatusRed},
		{"整窗口尾巴全丢",
			types.NegTailStat{ExpectedTail: "不感兴趣：股市。", Total: 50, Intact: 0}, StatusRed},
		{"全部完整",
			types.NegTailStat{ExpectedTail: "不感兴趣：股市。", Total: 50, Intact: 50}, StatusGreen},
		{"单条完整",
			types.NegTailStat{ExpectedTail: "不感兴趣：股市。", Total: 1, Intact: 1}, StatusGreen},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			checkStatus(t, judgeNegTail(tc.in), tc.want)
		})
	}
}

// 红/绿两侧的 Detail 都要带上期望串：比对失败时人得能一眼看出探针在找什么，
// 否则无从判断是"尾巴真丢了"还是"期望串本身算错了"。
func TestJudgeNegTail_DetailCarriesExpectedTail(t *testing.T) {
	tail := "不感兴趣：加密货币、明星八卦。"
	for _, in := range []types.NegTailStat{
		{ExpectedTail: tail, Total: 50, Intact: 3},
		{ExpectedTail: tail, Total: 50, Intact: 50},
	} {
		got := judgeNegTail(in)
		if !strings.Contains(got.Detail, tail) {
			t.Errorf("Intact=%d 时 Detail 应带期望串，实际 %q", in.Intact, got.Detail)
		}
	}
}

// ---------- 探针 ⑥ 日成本环比（§16.6） ----------

// costDay 造一条 (UTC 日, span) 成本行。d 是相对锚点的天偏移。
func costDay(d int, span string, usd float64) types.SpanDayCost {
	anchor := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	return types.SpanDayCost{Day: anchor.AddDate(0, 0, d), SpanName: span, CostUSD: usd, Calls: 1}
}

func TestJudgeCost(t *testing.T) {
	tests := []struct {
		name string
		in   []types.SpanDayCost
		want Status
	}{
		{"完全无成本行", nil, StatusYellow},
		{"只有一个 UTC 日算不出环比",
			[]types.SpanDayCost{costDay(0, "score", 0.01)}, StatusYellow},
		// 红线只卡 score span（Boss 拍板 2026-07-16）：M5 新增的 profile_evolve/deep_dive
		// 是全新 span，全 span 环比测的是"上了新功能"而非"注入变贵"。
		{"其它 span 跨两天不能替 score 凑出环比",
			[]types.SpanDayCost{costDay(-1, "profile_evolve", 5), costDay(0, "profile_evolve", 9)},
			StatusYellow},
		{"score 只有一天而 evolve 有两天，仍算不出",
			[]types.SpanDayCost{costDay(-1, "profile_evolve", 5), costDay(0, "score", 0.5)},
			StatusYellow},
		// 红线是 delta **>= $0.01**（大于等于）。恰好 0.01 必须红——契约写的是
		// "涨幅 < $0.01"，达到即出界。
		{"环比恰好 +$0.01 即红（红线是大于等于）",
			[]types.SpanDayCost{costDay(-1, "score", 0), costDay(0, "score", 0.01)}, StatusRed},
		{"环比 +$0.009 未达红线",
			[]types.SpanDayCost{costDay(-1, "score", 0), costDay(0, "score", 0.009)}, StatusGreen},
		{"环比大涨",
			[]types.SpanDayCost{costDay(-1, "score", 0.01), costDay(0, "score", 0.5)}, StatusRed},
		{"环比下降",
			[]types.SpanDayCost{costDay(-1, "score", 0.5), costDay(0, "score", 0.01)}, StatusGreen},
		// score 是最便宜的 span（MaxTokens=16），cost_usd 逐行舍入后整批为 0 是正常的
		// （types/observability.go:93-96）——两天都 0 必须绿，不能借机断言它该 >0。
		{"两天都是 0 成本是正常的",
			[]types.SpanDayCost{costDay(-1, "score", 0), costDay(0, "score", 0)}, StatusGreen},
		{"同一天多行按天求和后再比",
			[]types.SpanDayCost{
				costDay(-1, "score", 0), costDay(-1, "score", 0),
				costDay(0, "score", 0.004), costDay(0, "score", 0.004),
			}, StatusGreen},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			checkStatus(t, judgeCost(tc.in), tc.want)
		})
	}
}

// 只统计 span_name="score" 的行：本用例在绿色的 score 环比上叠一条天价 profile_evolve，
// 判定必须纹丝不动。若哪天有人把 SpanName 过滤删了，这里立刻红。
func TestJudgeCost_IgnoresNonScoreSpans(t *testing.T) {
	got := judgeCost([]types.SpanDayCost{
		costDay(-1, "score", 0),
		costDay(0, "score", 0.001),
		// M5 首次上线时 profile_evolve 从 0 跳到 $9.99——全 span 环比会在这里假红。
		costDay(-1, "profile_evolve", 0),
		costDay(0, "profile_evolve", 9.99),
		costDay(0, "deep_dive", 3.5),
		costDay(0, "cardgen", 1.2),
	})
	checkStatus(t, got, StatusGreen)
	if !strings.Contains(got.Name, "score") {
		t.Errorf("判定名必须写明只卡 score，否则看板上读起来像全站成本，实际 %q", got.Name)
	}
}

// 环比取的是**最近两个** UTC 日，不是首尾两日：中间有安静日时容易写成排序反了。
func TestJudgeCost_ComparesTwoMostRecentDays(t *testing.T) {
	// 三天：-7 天 $9（远古高峰）、-1 天 $0、0 天 $0.02。
	// 取最近两天 → delta = +$0.02 → 红。若错拿了 -7 天当 prev，delta 为负 → 绿。
	got := judgeCost([]types.SpanDayCost{
		costDay(-7, "score", 9),
		costDay(-1, "score", 0),
		costDay(0, "score", 0.02),
	})
	checkStatus(t, got, StatusRed)
}

// ---------- 探针 ⑦ 演化健康（§16.7） ----------

func tm(t time.Time) *time.Time { return &t }

func TestJudgeEvolve(t *testing.T) {
	t0 := time.Date(2026, 7, 16, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		in   EvolveView
		want Status
	}{
		{"无画像时演化无对象",
			EvolveView{HasProfile: false}, StatusYellow},
		{"无画像时即便有调用也不适用",
			EvolveView{EvolveCallStat: types.EvolveCallStat{Calls: 3}, HasProfile: false}, StatusYellow},
		// Calls=0 → yellow：三种成因（无新反馈短路 / evolver 未装配 / 没跑过 pipeline）
		// 从 DB 无法区分，都不是"演化健康"的证据。
		{"窗口内无演化调用是数据不足",
			EvolveView{HasProfile: true, EvolveCallStat: types.EvolveCallStat{Calls: 0}}, StatusYellow},
		{"全部演化调用失败",
			EvolveView{HasProfile: true,
				EvolveCallStat:   types.EvolveCallStat{Calls: 3, Errored: 3, LastCallAt: tm(t0)},
				ProfileUpdatedAt: tm(t0.Add(time.Minute))}, StatusRed},
		{"单次调用且失败",
			EvolveView{HasProfile: true,
				EvolveCallStat: types.EvolveCallStat{Calls: 1, Errored: 1, LastCallAt: tm(t0)}}, StatusRed},
		{"部分失败不算全失败",
			EvolveView{HasProfile: true,
				EvolveCallStat:   types.EvolveCallStat{Calls: 3, Errored: 2, LastCallAt: tm(t0)},
				ProfileUpdatedAt: tm(t0.Add(time.Minute))}, StatusGreen},
		// updated_at 早于最近一次演化调用 = 这次演化没写回画像。
		// 成因（语义失败丢批 / CAS 退让）均属设计内路径，故 yellow 不 red。
		{"画像 updated_at 早于最近一次调用即未写回",
			EvolveView{HasProfile: true,
				EvolveCallStat:   types.EvolveCallStat{Calls: 2, LastCallAt: tm(t0)},
				ProfileUpdatedAt: tm(t0.Add(-time.Minute))}, StatusYellow},
		{"晚于最近一次调用即已写回",
			EvolveView{HasProfile: true,
				EvolveCallStat:   types.EvolveCallStat{Calls: 2, LastCallAt: tm(t0)},
				ProfileUpdatedAt: tm(t0.Add(time.Minute))}, StatusGreen},
		// 同刻不算"早于"（Before 是严格小于）：写回与调用记账在同一事务外，
		// 时间戳撞秒是常态，撞秒判黄会让绿灯变成抽奖。
		{"同刻不算未写回",
			EvolveView{HasProfile: true,
				EvolveCallStat:   types.EvolveCallStat{Calls: 2, LastCallAt: tm(t0)},
				ProfileUpdatedAt: tm(t0)}, StatusGreen},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			checkStatus(t, judgeEvolve(tc.in), tc.want)
		})
	}
}

// 「tags 恒为旧集合超期」这条腿本探针不覆盖（profiles 无历史表），
// 每条 Detail 都必须把这个盲区说出来——不说的话，绿灯读起来像三条腿全过了。
func TestJudgeEvolve_AllDetailsDiscloseTagBlindSpot(t *testing.T) {
	t0 := time.Date(2026, 7, 16, 8, 0, 0, 0, time.UTC)
	views := []EvolveView{
		{HasProfile: false},
		{HasProfile: true},
		{HasProfile: true, EvolveCallStat: types.EvolveCallStat{Calls: 2, Errored: 2, LastCallAt: tm(t0)}},
		{HasProfile: true, EvolveCallStat: types.EvolveCallStat{Calls: 2, LastCallAt: tm(t0)},
			ProfileUpdatedAt: tm(t0.Add(-time.Minute))},
		{HasProfile: true, EvolveCallStat: types.EvolveCallStat{Calls: 2, LastCallAt: tm(t0)},
			ProfileUpdatedAt: tm(t0.Add(time.Minute))},
	}
	for i, v := range views {
		got := judgeEvolve(v)
		if !strings.Contains(got.Detail, "profile_snapshots") {
			t.Errorf("用例 %d（%s）的 Detail 未披露 tags 盲区：%q", i, got.Status, got.Detail)
		}
	}
}

// ---------- Report.Worst ----------

func TestReportWorst(t *testing.T) {
	res := func(ss ...Status) []Result {
		out := make([]Result, len(ss))
		for i, s := range ss {
			out[i] = Result{Status: s}
		}
		return out
	}
	tests := []struct {
		name string
		in   []Result
		want Status
	}{
		{"全绿", res(StatusGreen, StatusGreen, StatusGreen), StatusGreen},
		{"混一个黄", res(StatusGreen, StatusYellow, StatusGreen), StatusYellow},
		{"全黄", res(StatusYellow, StatusYellow), StatusYellow},
		{"混一个红", res(StatusGreen, StatusYellow, StatusRed, StatusGreen), StatusRed},
		// 红在最后：短路 return 不能让它被前面的黄盖掉。
		{"红在末位", res(StatusYellow, StatusGreen, StatusRed), StatusRed},
		{"红在首位", res(StatusRed, StatusYellow, StatusGreen), StatusRed},
		{"全红", res(StatusRed, StatusRed), StatusRed},
		// 空报告绿：Run 恒产出 7 条，走不到这里；但 Worst 的零值语义得是确定的。
		{"空结果", nil, StatusGreen},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := (Report{Results: tc.in}).Worst(); got != tc.want {
				t.Errorf("期望 %s，实际 %s", tc.want, got)
			}
		})
	}
}

// ---------- Run 接线 ----------

// Run 的接线错误（窗口算错、期望串传错）不会让任何 judge 报错，只会让它们
// 在错误的数据上给出漂亮的绿灯——故窗口与期望串必须单独断言。
func TestRun_WiringAndWindows(t *testing.T) {
	now := time.Date(2026, 7, 16, 8, 30, 0, 0, time.UTC)
	f := &fakeStore{
		profile: &types.Profile{
			UserID:                7,
			Industry:              "软件",
			Summary:               "关注 AI 基础设施。不感兴趣：股市、明星八卦。",
			Tags:                  []string{"Go", "AI"},
			UpdatedAt:             now.Add(-time.Hour),
			LastEvolvedFeedbackID: 42,
		},
	}
	rep, err := Run(t.Context(), f, 7, now, 0)
	if err != nil {
		t.Fatalf("Run() 失败: %v", err)
	}

	// window<=0 → DefaultWindow(24h)，且全部查询共用同一 since。
	if rep.WindowHours != 24 {
		t.Errorf("window<=0 应回落 DefaultWindow=24h，实际 %d", rep.WindowHours)
	}
	if want := now.Add(-DefaultWindow); !f.gotSince.Equal(want) {
		t.Errorf("since 应为 now-24h=%v，实际 %v", want, f.gotSince)
	}
	// 画像创建早于窗口起点（本用例 CreatedAt 为零值）→ 注入统计不钳窗，仍用公共 since。
	if want := now.Add(-DefaultWindow); !f.gotInjSince.Equal(want) {
		t.Errorf("画像早于窗口时注入统计 since 应保持 %v，实际 %v", want, f.gotInjSince)
	}
	if f.gotMinN != minTraceN {
		t.Errorf("minN 应为契约的 %d，实际 %d", minTraceN, f.gotMinN)
	}
	// 批次历史用自己的 14 天长窗口：它是趋势展示，不参与红线判定。
	if want := now.Add(-batchHistoryWindow); !f.gotBatchSince.Equal(want) {
		t.Errorf("批次历史 since 应为 now-14d=%v，实际 %v", want, f.gotBatchSince)
	}
	if f.gotBatchLimit != batchHistoryLimit {
		t.Errorf("批次历史条数应为 %d，实际 %d", batchHistoryLimit, f.gotBatchLimit)
	}

	// 期望负面句必须由 profilehint 亲自算——探针自己 reimplement 必漂，而漂了是**假绿**。
	if want := profilehint.NegTail(f.profile); f.gotTail != want {
		t.Errorf("expectedTail 应来自 profilehint.NegTail，期望 %q，实际 %q", want, f.gotTail)
	}
	if f.gotTail == "" {
		t.Fatal("用例前提：本画像应算得出负面句，否则这条断言是空转")
	}

	// GeneratedAt 一律 UTC（红线 6：内部认 DB 原生时区，换算只在前端）。
	if rep.GeneratedAt.Location() != time.UTC {
		t.Errorf("GeneratedAt 必须是 UTC，实际 %v", rep.GeneratedAt.Location())
	}
	if rep.UserID != 7 {
		t.Errorf("UserID 应回填 7，实际 %d", rep.UserID)
	}

	// 7 条判定齐全且 ID 唯一——看板与 cmd/gate 都按 ID 索引。
	if len(rep.Results) != 7 {
		t.Fatalf("应产出 7 条判定，实际 %d", len(rep.Results))
	}
	seen := map[string]bool{}
	for _, r := range rep.Results {
		if r.ID == "" || r.Name == "" || r.ContractRef == "" || r.Status == "" || r.Summary == "" {
			t.Errorf("判定字段不全: %+v", r)
		}
		if seen[r.ID] {
			t.Errorf("判定 ID 重复: %s", r.ID)
		}
		seen[r.ID] = true
	}

	// EvolveView 的画像侧字段来自 profile 而非 llm_calls。
	if !rep.Evolve.HasProfile {
		t.Error("有画像时 HasProfile 应为 true")
	}
	if rep.Evolve.Cursor != 42 {
		t.Errorf("Cursor 应取 last_evolved_feedback_id=42，实际 %d", rep.Evolve.Cursor)
	}
	if rep.Evolve.TagCount != 2 {
		t.Errorf("TagCount 应为 2，实际 %d", rep.Evolve.TagCount)
	}
	// SummaryRunes 按 rune 计——按 byte 计的话中文摘要会虚高 3 倍。
	if want := len([]rune(f.profile.Summary)); rep.Evolve.SummaryRunes != want {
		t.Errorf("SummaryRunes 应按 rune 计 = %d，实际 %d", want, rep.Evolve.SummaryRunes)
	}
	if rep.Evolve.ProfileUpdatedAt == nil || !rep.Evolve.ProfileUpdatedAt.Equal(f.profile.UpdatedAt) {
		t.Errorf("ProfileUpdatedAt 应取自画像，实际 %v", rep.Evolve.ProfileUpdatedAt)
	}
}

// 显式 window 不被 DefaultWindow 顶掉。
func TestRun_ExplicitWindow(t *testing.T) {
	now := time.Date(2026, 7, 16, 8, 30, 0, 0, time.UTC)
	f := &fakeStore{}
	rep, err := Run(t.Context(), f, 1, now, 72*time.Hour)
	if err != nil {
		t.Fatalf("Run() 失败: %v", err)
	}
	if rep.WindowHours != 72 {
		t.Errorf("显式 window 应为 72h，实际 %d", rep.WindowHours)
	}
	if want := now.Add(-72 * time.Hour); !f.gotSince.Equal(want) {
		t.Errorf("since 应为 now-72h=%v，实际 %v", want, f.gotSince)
	}
}

// 首采前画像不存在是**正常态**（profilehint/cache.go:35 同此语义）：
// GetProfile 的 ErrNotFound 不得让整个体检失败——那会让 Gate 在新用户上直接炸。
func TestRun_ProfileNotFoundIsNotAnError(t *testing.T) {
	now := time.Date(2026, 7, 16, 8, 30, 0, 0, time.UTC)
	f := &fakeStore{
		profile: nil, // → ErrNotFound
		inj:     types.ProfileInjectionStat{Total: 50, Absent: 50},
	}
	rep, err := Run(t.Context(), f, 1, now, 0)
	if err != nil {
		t.Fatalf("画像 NotFound 不应让 Run 失败: %v", err)
	}
	if rep.Evolve.HasProfile {
		t.Error("无画像时 HasProfile 应为 false")
	}
	if rep.Evolve.ProfileUpdatedAt != nil {
		t.Error("无画像时 ProfileUpdatedAt 应为 nil")
	}
	// 无画像 → 期望串为空 → 探针 ⑤ 不适用；探针 ④ 也不该因为整窗口「暂无」而报红。
	if f.gotTail != "" {
		t.Errorf("无画像时期望串应为空，实际 %q", f.gotTail)
	}
	byID := map[string]Result{}
	for _, r := range rep.Results {
		byID[r.ID] = r
	}
	checkStatus(t, byID["profile_injection"], StatusYellow)
	checkStatus(t, byID["neg_tail_intact"], StatusYellow)
	checkStatus(t, byID["evolve_health"], StatusYellow)
	// 一份"什么都没验到"的报告绝不能是绿的。
	if got := rep.Worst(); got != StatusYellow {
		t.Errorf("无画像的空报告应是 yellow 而非 %s——vacuously green 正是本设计要防的", got)
	}
}

// 画像创建时刻落在探针窗口**之内**时，注入统计的起点必须钳到创建时刻：
// 画像存在之前的打分写「暂无」是正确行为，算进 Absent 会让首采后的头 24h 恒报
// 假击穿（2026-07-17 生产实锤：画像 02:37 UTC 建立，24h 窗口把 18:53–00:30 的
// 142 条历史「暂无」判成 §16.4 红线击穿，而画像建立后 52 条全部注入正常）。
func TestRun_InjectionWindowClampedToProfileCreation(t *testing.T) {
	now := time.Date(2026, 7, 17, 14, 30, 0, 0, time.UTC)
	created := time.Date(2026, 7, 17, 2, 37, 48, 0, time.UTC) // 落在 now-24h 之后
	f := &fakeStore{
		profile: &types.Profile{
			UserID:    7,
			Summary:   "关注 AI 基础设施。不感兴趣：股市。",
			Tags:      []string{"AI"},
			CreatedAt: created,
			UpdatedAt: created,
		},
	}
	if _, err := Run(t.Context(), f, 7, now, 0); err != nil {
		t.Fatalf("Run() 失败: %v", err)
	}
	if !f.gotInjSince.Equal(created) {
		t.Errorf("注入统计 since 应钳到画像创建时刻 %v，实际 %v", created, f.gotInjSince)
	}
	// 保尾统计（⑤）与注入同病同治：画像创建前的调用不含负面句同样是正确行为
	//（2026-07-17 同一批 142 条历史调用先后击穿 §16.4 与 §16.5）。
	if !f.gotNegSince.Equal(created) {
		t.Errorf("保尾统计 since 应钳到画像创建时刻 %v，实际 %v", created, f.gotNegSince)
	}
	// 其余统计不受钳窗影响，仍用公共 since。
	if want := now.Add(-DefaultWindow); !f.gotSince.Equal(want) {
		t.Errorf("公共 since 应保持 %v，实际 %v", want, f.gotSince)
	}
}
