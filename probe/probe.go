// Package probe 把 M5 契约 §16 的 Gate 服务端探针固化成代码。
//
// 为什么固化：这 7 条判定是**固定逻辑**，此前每次部署后靠人工 SSH + psql 手敲，
// 敲错一个 WHERE 的代价是"绿灯照推"——M3 那次三批全 50 分静默照推正是这类事故。
//
// 分层：SQL 在 store（数据访问层的既有职责），判定阈值与人话结论在这里，
// 出口有两个且共用本包——/api/admin/observability（看板）与 cmd/gate（CI/上线后一键跑）。
// 单一实现是刻意的：探针 SQL 依赖 scorer 源码里的字面量，一旦有第二份必然漂。
//
// 本包只读，不写任何表、不调用任何模型。用模型去查"模型有没有静默骗人"是循环论证：
// 出问题时它自己也是坏的。这也与 vane-web/docs/ui-interaction-principles.md 的
// 「状态/成本监控 = 纯固化组件」定调一致。
package probe

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/YouToco/vane/profilehint"
	"github.com/YouToco/vane/types"
)

// Status 是单条探针的判定结果。
//
// 三态而非布尔：探针经常处在"没数据所以说不了话"的状态（窗口内没跑过批次、
// owner 还没画像），把它算成绿是**危险的假绿**——契约 §16 要求的是"部署后当天与
// 次日复跑"，一个 vacuously green 的探针会让人以为验过了。Yellow 逼人去看一眼。
type Status string

const (
	StatusGreen  Status = "green"  // 通过
	StatusYellow Status = "yellow" // 数据不足 / 不适用 / 需人工确认，不阻断但不算过
	StatusRed    Status = "red"    // 红线击穿，按契约应回滚排查
)

// Result 是单条探针的结论。Summary 是人话，直接给 Boss 看。
type Result struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	ContractRef string `json:"contract_ref"` // 契约条款号，便于回查原文
	Status      Status `json:"status"`
	Summary     string `json:"summary"`
	Detail      string `json:"detail,omitempty"` // 展开说明 / 下一步怎么查
}

// Store 是探针所需的数据面窄接口，生产实现为 *store.Store。
// 定义在消费方（本包）是仓库既有约定：便于单测替身，无需起 DB。
type Store interface {
	ListScoreTraceStats(ctx context.Context, since time.Time, minN int) ([]types.ScoreTraceStat, error)
	GetScoreQualityStat(ctx context.Context, since time.Time) (types.ScoreQualityStat, error)
	ListScoreDistribution(ctx context.Context, since time.Time) ([]types.ScoreBucket, error)
	GetProfileInjectionStat(ctx context.Context, since time.Time) (types.ProfileInjectionStat, error)
	GetNegTailStat(ctx context.Context, since time.Time, expectedTail string) (types.NegTailStat, error)
	ListSpanDayCosts(ctx context.Context, since time.Time) ([]types.SpanDayCost, error)
	ListModelUsage(ctx context.Context, since time.Time) ([]types.ModelUsage, error)
	GetEvolveCallStat(ctx context.Context, userID int64, since time.Time) (types.EvolveCallStat, error)
	ListPushBatchSummaries(ctx context.Context, userID int64, since time.Time, limit int) ([]types.PushBatchSummary, error)
	GetProfile(ctx context.Context, userID int64) (*types.Profile, error)
}

// 契约 §16 固化的阈值。改这里等于改契约——改之前先改文档。
const (
	// minTraceN 是"批"的最小规模：契约 §16.1 的 n≥5。
	minTraceN = 5
	// fallbackRedRate 是中位分回退率红线：契约 §16.2 的 24h >10%。
	fallbackRedRate = 0.10
	// costDeltaRedUSD 是日成本环比涨幅红线：契约 §16.6 的 <$0.01。
	// Boss 拍板（2026-07-16）：只卡 score span——M5 新增 profile_evolve/deep_dive
	// 两个全新 span，全 span 环比测的是"上了新功能"而非"注入变贵"。
	costDeltaRedUSD = 0.01
	// batchHistoryLimit 是批次历史展示条数。
	batchHistoryLimit = 30
	// DefaultWindow 是默认统计窗口：契约 §16.2 的 24h 红线以此为准。
	DefaultWindow = 24 * time.Hour
	// batchHistoryWindow 是批次历史的回溯窗口，比探针窗口长——历史是给人看趋势的。
	batchHistoryWindow = 14 * 24 * time.Hour
)

// EvolveView 是演化健康的展示数据（探针 ⑦）。
type EvolveView struct {
	types.EvolveCallStat
	HasProfile       bool       `json:"has_profile"`
	ProfileUpdatedAt *time.Time `json:"profile_updated_at,omitempty"`
	Cursor           int64      `json:"cursor"` // profiles.last_evolved_feedback_id
	TagCount         int        `json:"tag_count"`
	SummaryRunes     int        `json:"summary_runes"`
}

// Report 是一次完整体检的产物：7 条判定 + 支撑它们的原始指标（供看板画图）。
type Report struct {
	GeneratedAt time.Time `json:"generated_at"` // UTC
	WindowHours int       `json:"window_hours"`
	UserID      int64     `json:"user_id"`

	Results []Result `json:"results"`

	ScoreDistribution []types.ScoreBucket        `json:"score_distribution"`
	ScoreTraces       []types.ScoreTraceStat     `json:"score_traces"`
	Quality           types.ScoreQualityStat     `json:"quality"`
	Injection         types.ProfileInjectionStat `json:"injection"`
	NegTail           types.NegTailStat          `json:"neg_tail"`
	Costs             []types.SpanDayCost        `json:"costs"`
	Models            []types.ModelUsage         `json:"models"`
	Evolve            EvolveView                 `json:"evolve"`
	Batches           []types.PushBatchSummary   `json:"batches"`
}

// Worst 返回全部判定里最严重的状态，供 CLI 决定退出码。
func (r Report) Worst() Status {
	worst := StatusGreen
	for _, res := range r.Results {
		switch res.Status {
		case StatusRed:
			return StatusRed
		case StatusYellow:
			worst = StatusYellow
		}
	}
	return worst
}

// Run 跑完全部探针并汇总。window<=0 时用 DefaultWindow。
//
// now 由调用方注入而非内部 time.Now()：单测要能钉死时间窗，
// 且 CLI/API 两个出口共用同一个"现在"避免跨查询漂移。
func Run(ctx context.Context, st Store, userID int64, now time.Time, window time.Duration) (Report, error) {
	if window <= 0 {
		window = DefaultWindow
	}
	// 全部查询共用同一 since：分开算会让"24h"随查询耗时漂几十毫秒，
	// 虽无实害但会让复跑结果对不上，排查时徒增噪声。
	since := now.Add(-window)

	rep := Report{
		GeneratedAt: now.UTC(),
		WindowHours: int(window / time.Hour),
		UserID:      userID,
	}

	// 画像先取：探针 ④⑤⑦ 的判定都以"owner 到底有没有画像"为前提。
	// NotFound 不是错误——首采前画像本就不存在（profilehint/cache.go:35 同此语义）。
	prof, err := st.GetProfile(ctx, userID)
	if err != nil && !errors.Is(err, types.ErrNotFound) {
		return rep, err
	}
	if err != nil {
		prof = nil // NotFound：GetProfile 已返回 nil，这里显式归零免得后续误用
	}

	if rep.ScoreTraces, err = st.ListScoreTraceStats(ctx, since, minTraceN); err != nil {
		return rep, err
	}
	if rep.Quality, err = st.GetScoreQualityStat(ctx, since); err != nil {
		return rep, err
	}
	if rep.ScoreDistribution, err = st.ListScoreDistribution(ctx, since); err != nil {
		return rep, err
	}
	// 注入统计的窗口起点钳到画像创建时刻：画像存在**之前**的打分写「暂无」是
	// 正确行为，计入 Absent 会让首采后的头 24h 恒报假击穿（2026-07-17 生产实锤：
	// 画像 02:37 UTC 建立，探针把 18:53–00:30 的 142 条历史「暂无」判成红线击穿，
	// 而画像建立后 52 条全部注入正常、profilehint WARN 零条）。
	injSince := since
	if prof != nil && prof.CreatedAt.After(injSince) {
		injSince = prof.CreatedAt
	}
	if rep.Injection, err = st.GetProfileInjectionStat(ctx, injSince); err != nil {
		return rep, err
	}
	// 期望负面句必须由 profilehint 亲自算（见 profilehint.NegTail 的注释）。
	if rep.NegTail, err = st.GetNegTailStat(ctx, since, profilehint.NegTail(prof)); err != nil {
		return rep, err
	}
	if rep.Costs, err = st.ListSpanDayCosts(ctx, since); err != nil {
		return rep, err
	}
	if rep.Models, err = st.ListModelUsage(ctx, since); err != nil {
		return rep, err
	}
	evolveStat, err := st.GetEvolveCallStat(ctx, userID, since)
	if err != nil {
		return rep, err
	}
	rep.Evolve = EvolveView{EvolveCallStat: evolveStat, HasProfile: prof != nil}
	if prof != nil {
		rep.Evolve.ProfileUpdatedAt = &prof.UpdatedAt
		rep.Evolve.Cursor = prof.LastEvolvedFeedbackID
		rep.Evolve.TagCount = len(prof.Tags)
		rep.Evolve.SummaryRunes = len([]rune(prof.Summary))
	}
	// 批次历史用自己的长窗口：它是给人看趋势的展示数据，不参与任何红线判定。
	if rep.Batches, err = st.ListPushBatchSummaries(ctx, userID,
		now.Add(-batchHistoryWindow), batchHistoryLimit); err != nil {
		return rep, err
	}

	rep.Results = []Result{
		judgeDiscrimination(rep.ScoreTraces),
		judgeFallbackRate(rep.Quality),
		judgeEmptyOutput(rep.Quality),
		judgeInjection(rep.Injection, prof != nil),
		judgeNegTail(rep.NegTail),
		judgeCost(rep.Costs),
		judgeEvolve(rep.Evolve),
	}
	return rep, nil
}

// judgeDiscrimination 探针 ①：任一批 n≥5 且 distinct=1 → 立即回滚排查。
func judgeDiscrimination(traces []types.ScoreTraceStat) Result {
	r := Result{ID: "score_discrimination", Name: "分数区分度", ContractRef: "§16.1"}
	if len(traces) == 0 {
		r.Status = StatusYellow
		r.Summary = fmt.Sprintf("窗口内没有规模 ≥%d 的打分批次，无从判定", minTraceN)
		r.Detail = "不是通过：没跑过批次而已。等一次定时推送后复跑。"
		return r
	}
	var flat []types.ScoreTraceStat
	for _, t := range traces {
		if t.DistinctCompletions == 1 {
			flat = append(flat, t)
		}
	}
	if len(flat) > 0 {
		r.Status = StatusRed
		r.Summary = fmt.Sprintf("%d/%d 个批次整批同分——M3 事故形状", len(flat), len(traces))
		r.Detail = fmt.Sprintf("首个：trace %s（%d 次打分，输出只有 1 种）。"+
			"按契约立即回滚排查。先查 DisableThinking 是否仍生效（CLAUDE.md 红线 1）："+
			"SELECT completion, completion_tokens FROM llm_calls WHERE trace_id='%s' AND span_name='score' LIMIT 5;",
			flat[0].TraceID, flat[0].N, flat[0].TraceID)
		return r
	}
	minD := traces[0].DistinctCompletions
	for _, t := range traces {
		if t.DistinctCompletions < minD {
			minD = t.DistinctCompletions
		}
	}
	r.Status = StatusGreen
	r.Summary = fmt.Sprintf("%d 个批次全部有区分度（最低 %d 种输出）", len(traces), minD)
	r.Detail = "契约要求区分度不低于基线——基线需人工对照部署前快照，本探针只保证不为 1。"
	return r
}

// judgeFallbackRate 探针 ②：中位分回退率 24h >10% 红线。
func judgeFallbackRate(q types.ScoreQualityStat) Result {
	r := Result{ID: "median_fallback_rate", Name: "中位分回退率", ContractRef: "§16.2"}
	if q.OKTotal == 0 {
		r.Status = StatusYellow
		r.Summary = "窗口内没有成功的打分调用，无从判定"
		if q.Errored > 0 {
			r.Detail = fmt.Sprintf("但有 %d 次调用失败（error 非空）——这些条目被直接跳过，"+
				"没有发出任何分数，不算回退。若持续，查上游可用性。", q.Errored)
		}
		return r
	}
	rate := q.FallbackRate()
	r.Detail = fmt.Sprintf("分母只含成功调用（%d 次）；另有 %d 次调用失败被排除——"+
		"失败的条目被 pipeline 直接跳过，一分未发，算进回退率会让上游抖动冲爆红线"+
		"（契约 §16.2 原文未限定 error，本探针按修订版实现）。", q.OKTotal, q.Errored)
	if rate > fallbackRedRate {
		r.Status = StatusRed
		r.Summary = fmt.Sprintf("回退率 %.1f%%（%d/%d）击穿 %.0f%% 红线",
			rate*100, q.NoDigit, q.OKTotal, fallbackRedRate*100)
		return r
	}
	r.Status = StatusGreen
	r.Summary = fmt.Sprintf("回退率 %.1f%%（%d/%d），红线 %.0f%%",
		rate*100, q.NoDigit, q.OKTotal, fallbackRedRate*100)
	return r
}

// judgeEmptyOutput 探针 ③：score 空 completion 且无 error 必须 = 0（DisableThinking 回归）。
// 这是 7 条里唯一按契约原文即可实现、无需修订的一条。
func judgeEmptyOutput(q types.ScoreQualityStat) Result {
	r := Result{ID: "empty_completion", Name: "空输出零容忍", ContractRef: "§16.3"}
	if q.OKTotal == 0 && q.Errored == 0 {
		r.Status = StatusYellow
		r.Summary = "窗口内无打分调用，无从判定"
		return r
	}
	if q.EmptyNoError > 0 {
		r.Status = StatusRed
		r.Summary = fmt.Sprintf("%d 次打分返回空内容却无报错——M3 事故的精确形状", q.EmptyNoError)
		r.Detail = "零容忍红线。这些调用每次都静默回退中位分 50 并照常推送。" +
			"立即查 scorer 的 DisableThinking 是否被改动（CLAUDE.md 红线 1：V4 默认 reasoning " +
			"会吃光 16 token 预算致 content 恒空）。佐证：这些行的 completion_tokens 应非 0。"
		return r
	}
	r.Status = StatusGreen
	r.Summary = fmt.Sprintf("0 次空输出（%d 次成功调用）", q.OKTotal)
	return r
}

// judgeInjection 探针 ④：owner 有画像时"用户画像：暂无"计数必须为 0。
//
// 契约的第二条腿（avg(prompt_tokens) 应上浮）已按 Boss 拍板改为正面字面量断言：
// 原方法无量级、无容差、无基线，且正文截断 800 rune 的抖动比画像本身大一个量级，
// 判不出结果。正面断言精确且无混淆因子，与本探针的反面断言恰好闭合。
func judgeInjection(in types.ProfileInjectionStat, hasProfile bool) Result {
	r := Result{ID: "profile_injection", Name: "画像注入生效性", ContractRef: "§16.4"}
	// 自检位优先：恒等式破了说明 scorer 的 prompt 结构变了而探针字面量没跟上，
	// 此时下面任何判定都不可信，必须先修探针。
	if in.Unrecognized > 0 {
		r.Status = StatusRed
		r.Summary = fmt.Sprintf("%d 条打分 prompt 不以「用户画像：」开头——探针字面量已漂移", in.Unrecognized)
		r.Detail = "这不是画像的问题，是探针的问题：scorer.buildScoreUser 的 prompt 结构变了，" +
			"而 store/observability.go 的 profileHintPrefix 没跟上。先修探针再谈判定。"
		return r
	}
	if !hasProfile {
		r.Status = StatusYellow
		r.Summary = "owner 尚无画像，探针不适用"
		r.Detail = fmt.Sprintf("此时打分 prompt 写「用户画像：暂无」是**正确行为**（当前 %d 条）。"+
			"画像读取失败降级为空画像时也是这个形状，两者从 DB 无法区分——"+
			"profilehint 对 NotFound 刻意不告警（cache.go:35：首采前的正常态）。"+
			"先让 owner 在飞书完成画像首采，本探针才有意义。", in.Absent)
		return r
	}
	if in.Total == 0 {
		r.Status = StatusYellow
		r.Summary = "窗口内无打分调用，无从判定"
		return r
	}
	if in.Absent > 0 {
		r.Status = StatusRed
		r.Summary = fmt.Sprintf("owner 有画像，却有 %d/%d 条打分拿到「暂无」", in.Absent, in.Total)
		r.Detail = "画像注入在这些调用上失效了——打分退化成通用资讯价值判断。" +
			"（统计窗口已钳到画像创建时刻之后，这些「暂无」不是首采前的历史遗留。）" +
			"配套查降级 WARN：journalctl -u vane --since … | grep 'profilehint: 画像读取失败'。" +
			"注意 WARN 每 trace 最多一条（画像按 traceID 锁内只取一次），" +
			"50 条降级的打分只会产生 1 行日志，别指望条数对得上。" +
			"另注意 journalctl 按 VPS 本地 EDT 解释时间，而 llm_calls.created_at 是 UTC（红线 6）。"
		return r
	}
	r.Status = StatusGreen
	r.Summary = fmt.Sprintf("%d/%d 条打分均注入了真实画像", in.Present, in.Total)
	return r
}

// judgeNegTail 探针 ⑤：演化产生「不感兴趣：…」后，负面句须完整出现在打分的画像行里。
func judgeNegTail(n types.NegTailStat) Result {
	r := Result{ID: "neg_tail_intact", Name: "负面清单保尾", ContractRef: "§16.5"}
	if n.ExpectedTail == "" {
		r.Status = StatusYellow
		r.Summary = "当前画像没有「不感兴趣：」句，探针不适用"
		r.Detail = "慢通道演化尚未产出负面清单。Gate 清单 ⑦（push_now 触发演化）跑过之后再复跑。"
		return r
	}
	if n.Total == 0 {
		r.Status = StatusYellow
		r.Summary = "窗口内无打分调用，无从判定"
		return r
	}
	if n.Intact < n.Total {
		r.Status = StatusRed
		r.Summary = fmt.Sprintf("%d/%d 条打分的画像行里负面句不完整", n.Total-n.Intact, n.Total)
		r.Detail = fmt.Sprintf("保尾逻辑（审查 F1）失效：negTail 应原样穿过 buildSummary 与 capHint 的截断。"+
			"期望串：%q", n.ExpectedTail)
		return r
	}
	r.Status = StatusGreen
	r.Summary = fmt.Sprintf("%d/%d 条打分均完整含负面句", n.Intact, n.Total)
	r.Detail = fmt.Sprintf("期望串：%q（比对锚定 user_prompt 第一行——画像 hint 是硬约束单行，"+
		"故可证明它就是整个第一行；全文通配会被快通道区块头「近期不感兴趣」误命中而假绿）。", n.ExpectedTail)
	return r
}

// judgeCost 探针 ⑥：日成本环比涨幅 < $0.01。
//
// Boss 拍板（2026-07-16）：红线只卡 score span。M5 新增 profile_evolve/deep_dive
// 两个全新 span，全 span 环比的首次比较必然因"新功能上线"而超标，那测的不是注入变贵。
// 全 span 总额仍然展示，只是不卡。
func judgeCost(costs []types.SpanDayCost) Result {
	r := Result{ID: "daily_cost", Name: "日成本环比（score）", ContractRef: "§16.6"}

	byDay := map[time.Time]float64{}
	for _, c := range costs {
		if c.SpanName == "score" {
			byDay[c.Day] += c.CostUSD
		}
	}
	if len(byDay) < 2 {
		r.Status = StatusYellow
		r.Summary = "score 成本不足两个 UTC 日，无法算环比"
		r.Detail = "契约要求部署前存基线、部署后当天与次日复跑——至少要跨两天才有环比。"
		return r
	}
	days := make([]time.Time, 0, len(byDay))
	for d := range byDay {
		days = append(days, d)
	}
	sort.Slice(days, func(i, j int) bool { return days[i].After(days[j]) })
	today, prev := byDay[days[0]], byDay[days[1]]
	delta := today - prev

	r.Detail = fmt.Sprintf("最近两个 UTC 日：%s $%.6f → %s $%.6f。"+
		"日界固定 UTC（DB 原生）；北京时间只在看板换算——红线 6 的三时区陷阱。"+
		"注意 cost_usd 是 NUMERIC(10,6) 逐行舍入后求和，score 是最便宜的 span"+
		"（MaxTokens=16），整批舍成 0 是正常的，不要据此断言它必须 >0。",
		days[1].Format("01-02"), prev, days[0].Format("01-02"), today)

	if delta >= costDeltaRedUSD {
		r.Status = StatusRed
		r.Summary = fmt.Sprintf("score 日成本环比 +$%.6f，达到 $%.2f 红线", delta, costDeltaRedUSD)
		return r
	}
	r.Status = StatusGreen
	r.Summary = fmt.Sprintf("score 日成本环比 %+.6f 美元，红线 $%.2f", delta, costDeltaRedUSD)
	return r
}

// judgeEvolve 探针 ⑦：演化健康。
//
// 契约原文三条腿，本期只实现能从 DB 判定的那条（Boss 拍板 2026-07-16）：
//   - 「summary/tags/游标变更」→ 用 profiles.updated_at 与最近一次 profile_evolve
//     调用时刻的先后关系判定。够用但不精确，见下方 Detail。
//   - 「语义失败 WARN 有 raw」→ 那是 journalctl 的活，不在 DB 里，探针只给出查法。
//   - 「tags 集合恒为旧集合超集」→ **无历史表则不可验证**：profiles 每用户仅一行、
//     演化就地覆盖，旧集合在写入那一刻就没了。已移交单测（evolver 的 checkTagGuard）；
//     PR2 加 profile_snapshots 历史表后本探针再补齐。
func judgeEvolve(e EvolveView) Result {
	r := Result{ID: "evolve_health", Name: "演化健康", ContractRef: "§16.7"}
	tagsNote := "「tags 恒为旧集合超集」本探针不覆盖：profiles 无历史表、演化就地覆盖，" +
		"旧集合从 DB 无法取得。该不变量由 evolver.checkTagGuard 保证并由单测锁定；" +
		"PR2 的 profile_snapshots 历史表落地后再补。"

	if !e.HasProfile {
		r.Status = StatusYellow
		r.Summary = "owner 尚无画像，演化无对象"
		r.Detail = tagsNote
		return r
	}
	if e.Calls == 0 {
		r.Status = StatusYellow
		r.Summary = "窗口内无演化调用"
		r.Detail = "三种可能且从 DB 无法区分：无新反馈（evolver 在调模型前就短路，不留任何行）、" +
			"evolver 未装配（nil 时整步 no-op）、窗口内没跑过 pipeline。" +
			"想验证请先点几条反馈再 push_now。" + tagsNote
		return r
	}
	if e.Errored == e.Calls {
		r.Status = StatusRed
		r.Summary = fmt.Sprintf("%d 次演化调用全部失败", e.Calls)
		r.Detail = "演化失败不阻断推送（workflow 的 EvolveProfile 步骤只 Warn），所以链路看起来是绿的——" +
			"但画像已经停止吸收反馈，「越用越准」这条线断了。" + tagsNote
		return r
	}
	r.Detail = fmt.Sprintf("游标 last_evolved_feedback_id=%d；画像 %d 个标签、summary %d 字。%s",
		e.Cursor, e.TagCount, e.SummaryRunes, tagsNote)

	if e.LastCallAt != nil && e.ProfileUpdatedAt != nil && e.ProfileUpdatedAt.Before(*e.LastCallAt) {
		r.Status = StatusYellow
		r.Summary = "最近一次演化未写回画像"
		r.Detail = "判定依据：profiles.updated_at 早于最近一次 profile_evolve 调用时刻。" +
			"成因二选一，均属设计内路径：语义失败丢弃本批（推进游标防死循环，契约 §17）、" +
			"或 CAS 退让（人工修正与演化并发时人工恒赢）。偶发正常，连续出现才是问题。" +
			"查 raw：journalctl -u vane --since … | grep 'evolver: 演化语义失败'（注意 EDT/UTC 差，红线 6）。" +
			r.Detail
		return r
	}
	r.Status = StatusGreen
	r.Summary = fmt.Sprintf("%d 次演化调用，最近一次已写回画像", e.Calls)
	r.Detail = "注意 updated_at 无法归因写入者：人工改画像（UpsertProfileFields）也无条件刷它。" +
		"若 Gate 清单 ⑧（手动修正画像）与本探针时间窗交叠，绿灯可能来自人工写入而非演化——" +
		"两项请错开执行。" + r.Detail
	return r
}
