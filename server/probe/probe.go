// Package probe 把 M5 契约 §16 的 Gate 服务端探针固化成代码。
//
// 为什么固化：这 7 条判定是**固定逻辑**，此前每次部署后靠人工 SSH + psql 手敲，
// 敲错一个 WHERE 的代价是"绿灯照推"——M3 那次三批全 50 分静默照推正是这类事故。
//
// 分层：SQL 在 store（数据访问层的既有职责），判定阈值与人话结论在这里，
// 出口有三个且共用本包——/api/admin/observability（看板）、cmd/gate（部署验证/
// 故障深查的 CLI）与 cmd/server/probewatch（服务内每日巡检+红灯飞书告警）。
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
	"strconv"
	"strings"
	"time"

	"github.com/YouToco/vane/server/profilehint"
	"github.com/YouToco/vane/server/types"
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
	ListScoreTraceStats(ctx context.Context, tenantID int64, since time.Time, minN int) ([]types.ScoreTraceStat, error)
	GetScoreQualityStat(ctx context.Context, tenantID int64, since time.Time) (types.ScoreQualityStat, error)
	ListScoreDistribution(ctx context.Context, tenantID int64, since time.Time) ([]types.ScoreBucket, error)
	GetScoreLivenessStat(ctx context.Context, tenantID int64, since time.Time, floor int) (types.ScoreLivenessStat, error)
	GetProfileInjectionStat(ctx context.Context, tenantID int64, since time.Time) (types.ProfileInjectionStat, error)
	GetNegTailStat(ctx context.Context, tenantID int64, since time.Time, expectedTail string) (types.NegTailStat, error)
	ListSpanDayCosts(ctx context.Context, tenantID int64, since time.Time) ([]types.SpanDayCost, error)
	ListModelUsage(ctx context.Context, tenantID int64, since time.Time) ([]types.ModelUsage, error)
	GetEvolveCallStat(ctx context.Context, tenantID, userID int64, since time.Time) (types.EvolveCallStat, error)
	ListPushBatchSummaries(ctx context.Context, tenantID, userID int64, since time.Time, limit int) ([]types.PushBatchSummary, error)
	GetProfileForTenant(ctx context.Context, tenantID, userID int64) (*types.Profile, error)
	CountA2ATasks(ctx context.Context) (int64, error)
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

	// benignMaxCompletionTokens 是"良性整批同分"判别（§16.1 修订）的 tokens 上界。
	// 干净的单数字回答（"0"~"100"）是 1-2 token；M3 形状是思维链吃满预算（≥16）。
	// 取 4 让两者之间隔着一条宽沟，不会骑墙——超过它的同分批一律按可疑处理。
	benignMaxCompletionTokens = 4
	// livenessMinN 是 §16.8 高分存在性红线的最小样本数：窗口内可解析打分不足
	// 此数时"全是低分"只给黄——一两批真不相关的内容就能凑出全低分，
	// 样本太小时按红告警等于把探针训练成噪声。
	livenessMinN = 10
)

// lowBandCeil 返回"不该推"语义档的上界（含），即 20。
// 从 types.DefaultStrictness.MinKeepScore()（=21）推出而非第二次写死：
// 档位语义的唯一事实源在 types.PushStrictness（migration 025 / 契约 §6.1），
// 探针再抄一个 20 就是第二份字面量——防漂移原则（本包头注释）同样适用于对内引用。
func lowBandCeil() int {
	return types.DefaultStrictness.MinKeepScore() - 1
}

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
	Liveness          types.ScoreLivenessStat    `json:"liveness"`
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
func Run(ctx context.Context, st Store, tenantID, userID int64, now time.Time, window time.Duration) (Report, error) {
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

	// 画像先取：探针 ④⑤⑦ 的判定都以"owner 到底有没有可用画像"为前提。
	// NotFound 不是错误——首采前画像本就不存在（profilehint/cache.go:35 同此语义）。
	prof, err := st.GetProfileForTenant(ctx, tenantID, userID)
	if err != nil && !errors.Is(err, types.ErrNotFound) {
		return rep, err
	}
	if err != nil {
		prof = nil // NotFound：GetProfile 已返回 nil，这里显式归零免得后续误用
	}
	// 画像重置会保留 append-only 审计与 profiles 行，但清空所有可渲染字段。
	// 运行时 profilehint.Build 对这种行返回空串，打分 prompt 正确写「暂无」；
	// Gate 必须复用同一个定义，不能把“数据库有一行”误报成“注入失效”。
	hasProfile := profilehint.Build(prof) != ""

	if rep.ScoreTraces, err = st.ListScoreTraceStats(ctx, tenantID, since, minTraceN); err != nil {
		return rep, err
	}
	if rep.Quality, err = st.GetScoreQualityStat(ctx, tenantID, since); err != nil {
		return rep, err
	}
	if rep.ScoreDistribution, err = st.ListScoreDistribution(ctx, tenantID, since); err != nil {
		return rep, err
	}
	if rep.Liveness, err = st.GetScoreLivenessStat(ctx, tenantID, since, lowBandCeil()); err != nil {
		return rep, err
	}
	// 注入（④）与保尾（⑤）统计的窗口起点钳到画像创建时刻：画像存在**之前**的
	// 打分既写「暂无」也不含负面句，都是正确行为，计入失败会让首采后的头 24h 恒报
	// 假击穿（2026-07-17 生产实锤，两针先后被同一批数据击穿：画像 02:37 UTC 建立，
	// 24h 窗口把 18:53–00:30 的 142 条历史调用分别判成 §16.4 与 §16.5 红线，
	// 而画像建立后 52 条注入与保尾全部正常）。
	//
	// 已知限制：⑤ 的严格基准是"当前负面句进入画像的时刻"——演化改写负面句后，
	// 改写前的调用含旧句、期望串是新句，钳 CreatedAt 挡不住这档假红。profiles
	// 无历史表无法定位改写时刻（同 judgeEvolve 的超集不可验证限制），出现时以
	// Detail 里的期望串人工比对窗口内 prompt 即可辨认。
	injSince := since
	if hasProfile && prof.CreatedAt.After(injSince) {
		injSince = prof.CreatedAt
	}
	if rep.Injection, err = st.GetProfileInjectionStat(ctx, tenantID, injSince); err != nil {
		return rep, err
	}
	// 期望负面句必须由 profilehint 亲自算（见 profilehint.NegTail 的注释）。
	if rep.NegTail, err = st.GetNegTailStat(ctx, tenantID, injSince, profilehint.NegTail(prof)); err != nil {
		return rep, err
	}
	if rep.Costs, err = st.ListSpanDayCosts(ctx, tenantID, since); err != nil {
		return rep, err
	}
	if rep.Models, err = st.ListModelUsage(ctx, tenantID, since); err != nil {
		return rep, err
	}
	evolveStat, err := st.GetEvolveCallStat(ctx, tenantID, userID, since)
	if err != nil {
		return rep, err
	}
	rep.Evolve = EvolveView{EvolveCallStat: evolveStat, HasProfile: hasProfile}
	if hasProfile {
		rep.Evolve.ProfileUpdatedAt = &prof.UpdatedAt
		rep.Evolve.Cursor = prof.LastEvolvedFeedbackID
		rep.Evolve.TagCount = len(prof.Tags)
		rep.Evolve.SummaryRunes = len([]rune(prof.Summary))
	}
	// 批次历史用自己的长窗口：它是给人看趋势的展示数据，不参与任何红线判定。
	if rep.Batches, err = st.ListPushBatchSummaries(ctx, tenantID, userID,
		now.Add(-batchHistoryWindow), batchHistoryLimit); err != nil {
		return rep, err
	}

	rep.Results = []Result{
		judgeDiscrimination(rep.ScoreTraces),
		judgeFallbackRate(rep.Quality),
		judgeEmptyOutput(rep.Quality),
		judgeInjection(rep.Injection, hasProfile),
		judgeNegTail(rep.NegTail),
		judgeCost(rep.Costs),
		judgeEvolve(rep.Evolve),
		judgeLiveness(rep.Liveness),
		judgeA2ATasks(ctx, st),
	}
	return rep, nil
}

// judgeA2ATasks 探针 P-A2A（a2a-contract §10）：CountA2ATasks 查询成功（含 0 行）
// = green（migration 013 落位、表可读）；查询报错 = red。无 yellow——表存在性与
// 数据量无关。**刻意偏离既有模式**：其余探针的 Store 报错会 return (rep, err) 中断
// 整轮（cmd/gate 计 exit 2 = "探针坏了"）；本探针的报错就地记 StatusRed（exit 1）——
// 表缺失/不可读正是本探针要报告的产品事实，不是探针自身故障。
func judgeA2ATasks(ctx context.Context, st Store) Result {
	res := Result{
		ID:          "P-A2A",
		Name:        "A2A 任务表可读",
		ContractRef: "a2a-contract §10",
	}
	n, err := st.CountA2ATasks(ctx)
	if err != nil {
		res.Status = StatusRed
		res.Summary = "a2a_tasks 查询失败"
		res.Detail = fmt.Sprintf("CountA2ATasks 报错：%v——检查 migration 013 是否落位、表是否可读", err)
		return res
	}
	res.Status = StatusGreen
	res.Summary = fmt.Sprintf("a2a_tasks 可读，现存 %d 条任务", n)
	return res
}

// judgeDiscrimination 探针 ①：任一批 n≥5 且 distinct=1 → 按形状分流（2026-07-20 修订）。
//
// 「整批同分」有两种成因，账本上可区分（2026-07-19 生产实锤后修订，契约 §16.1）：
//   - M3 形状：completion 空、tokens 反而高（思维链吃光预算）→ 红，立即回滚排查；
//   - 良性形状：completion 全为同一个 0-20 整数分且 tokens 是单数字回答量级
//     ——整批内容与画像全不相关时模型正确地全打低分就长这样（5 篇 HN 长文全 0 分），
//     打分器没坏 → 黄（机判良性，可见但不告警；probewatch 只对红发卡）。
//
// 修订前两种成因同判红，每逢"整批真不相关"就误发回滚告警；且红灯在 24h 窗口内
// 存续期间每次部署重启都重发一张卡（2026-07-19 一天 6 次部署 5 张同卡）。
// 良性判定依赖"打分器仍能打出高分"这一前提，由 §16.8 高分存在性兜底——
// 若 prompt 坏掉致模型恒输出低分，本探针会把每批都判良性，而 §16.8 会红。
func judgeDiscrimination(traces []types.ScoreTraceStat) Result {
	r := Result{ID: "score_discrimination", Name: "分数区分度", ContractRef: "§16.1"}
	if len(traces) == 0 {
		r.Status = StatusYellow
		r.Summary = fmt.Sprintf("窗口内没有规模 ≥%d 的打分批次，无从判定（0 批）", minTraceN)
		r.Detail = "不是通过：没跑过批次而已。等一次定时推送后复跑。"
		return r
	}
	var benign, suspect []types.ScoreTraceStat
	for _, t := range traces {
		if t.DistinctCompletions != 1 {
			continue
		}
		if isBenignFlatBatch(t) {
			benign = append(benign, t)
		} else {
			suspect = append(suspect, t)
		}
	}
	if len(suspect) > 0 {
		r.Status = StatusRed
		r.Summary = fmt.Sprintf("%d/%d 个批次整批同分且非良性形状——M3 事故形状", len(suspect), len(traces))
		r.Detail = fmt.Sprintf("首个：trace %s（%d 次打分，输出只有 1 种，tokens 峰值 %d）。"+
			"按契约立即回滚排查。先查 DisableThinking 是否仍生效（AGENTS.md 红线 1）："+
			"SELECT completion, completion_tokens FROM llm_calls WHERE trace_id='%s' AND span_name='score' LIMIT 5;"+
			"「整批同一 0-20 整数分且 tokens≤%d」的良性形状（内容真不相关）已被本探针自动分流为黄，"+
			"走到红意味着：输出为空（M3 精确形状）、或同分卡在 21+ 段（M3 事故正是整批「50」）、"+
			"或 tokens 异常高（思维链泄漏）。若人工复核确认实为良性而机器误判，"+
			"先查 store 层 min(completion)/max(completion_tokens) 两列取数是否正确——那是判别的输入。",
			suspect[0].TraceID, suspect[0].N, suspect[0].MaxCompletionTokens,
			suspect[0].TraceID, benignMaxCompletionTokens)
		if len(benign) > 0 {
			r.Detail += fmt.Sprintf("另有 %d 个良性同分批（同一低分、tokens 正常）未计入红线。", len(benign))
		}
		return r
	}
	if len(benign) > 0 {
		r.Status = StatusYellow
		r.Summary = fmt.Sprintf("%d/%d 个批次整批同一低分——机判「整批不相关」良性形状，非 M3",
			len(benign), len(traces))
		r.Detail = fmt.Sprintf("首个：trace %s（%d 次打分全部输出 %q）。判别依据（契约 §16.1 修订）："+
			"completion 为同一个 0-%d 整数（打分 prompt 的「不该推」语义档）且 completion_tokens≤%d"+
			"（单数字回答量级）；M3 形状是 completion 空、tokens 反而高，账本上可区分。"+
			"这类批次已被任务门槛（契约 §6.1）拦住不出卡，系统行为正确，无需回滚。"+
			"复核：SELECT completion, completion_tokens FROM llm_calls WHERE trace_id='%s' AND span_name='score';"+
			"前提校验看 §16.8 高分存在性——它若同时红，打分器可能坏成恒低分，本条的良性判定不可信。",
			benign[0].TraceID, benign[0].N, benign[0].MinCompletion,
			lowBandCeil(), benignMaxCompletionTokens, benign[0].TraceID)
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

// isBenignFlatBatch 判断一个整批同分（distinct=1）的批次是否为良性形状：
// 唯一输出是干净的整数、落在"不该推"语义档（0-20）、tokens 是单数字回答量级。
// 三条有一条不满足就按可疑处理——探针宁可误红也不误绿，绿的方向必须证据齐全。
func isBenignFlatBatch(t types.ScoreTraceStat) bool {
	sc, err := strconv.Atoi(strings.TrimSpace(t.MinCompletion))
	if err != nil {
		return false // 空串（M3 形状）、带解释文字、非整数——都不是干净的低分回答
	}
	if sc < 0 || sc > lowBandCeil() {
		return false // 21+ 段同分（M3 事故正是全"50"）不良性
	}
	// tokens=0 说明记账不完整，证据不齐按可疑处理；上界见 benignMaxCompletionTokens 注释。
	return t.MaxCompletionTokens >= 1 && t.MaxCompletionTokens <= benignMaxCompletionTokens
}

// judgeLiveness 探针 §16.8（2026-07-20 新增）：窗口内必须存在高于"不该推"档的打分输出。
//
// 这是 §16.1 良性同分判别的 cover property（vacuous pass 的规定解法：给"所有同分批
// 都可解释"配一条"至少存在一条高分"）。单独成条而不并进 §16.1，是因为它红的成因
// 与处置完全不同：§16.1 红 = 打分器输出形状坏了，回滚；本条红 = 打分器恒低分或
// 信源全面失效，先查内容再定——混成一条会让告警卡说不清让人干什么。
func judgeLiveness(lv types.ScoreLivenessStat) Result {
	r := Result{ID: "score_liveness", Name: "高分存在性", ContractRef: "§16.8"}
	floor := lowBandCeil()
	if lv.Parsable == 0 {
		r.Status = StatusYellow
		r.Summary = "窗口内没有可解析的打分输出，无从判定（0 条）"
		r.Detail = "不是通过：没有样本而已（窗口内没跑过批次，或输出全部无数字——后者看 §16.2）。" +
			"等一次定时推送后复跑。"
		return r
	}
	if lv.AboveFloor > 0 {
		r.Status = StatusGreen
		r.Summary = fmt.Sprintf("%d/%d 条打分高于「不该推」档（>%d 分）——打分器保有区分能力",
			lv.AboveFloor, lv.Parsable, floor)
		r.Detail = "本探针是 §16.1 良性同分判别的前提校验：那边把「整批同一低分」判为良性，" +
			"依赖打分器仍能对相关内容打出高分——本条绿灯就是这个前提的存在性证明。"
		return r
	}
	if lv.Parsable >= livenessMinN {
		r.Status = StatusRed
		r.Summary = fmt.Sprintf("窗口内 %d 条打分全部 ≤%d 分，没有一条高于「不该推」档", lv.Parsable, floor)
		r.Detail = "两种成因都需要人来定：打分器坏成恒低分（此时 §16.1 的「良性同分」判定不可信，" +
			"M3 类事故可能正躲在它背后——先人工抽几条打分对象看内容与分数是否相称）、" +
			"或信源全面失效（抓回来的东西没有一条与画像相关，信源配置该体检了）。" +
			"排查：SELECT completion, count(*) FROM llm_calls WHERE span_name='score' AND error='' " +
			"AND created_at >= now() - interval '24 hours' GROUP BY 1 ORDER BY 2 DESC;"
		return r
	}
	r.Status = StatusYellow
	r.Summary = fmt.Sprintf("窗口内仅 %d 条打分（红线要求 ≥%d）且全部 ≤%d 分——样本不足，先观察",
		lv.Parsable, livenessMinN, floor)
	r.Detail = "一两批真不相关的内容就能凑出「全低分」，样本这么小时按红告警只会制造噪声。" +
		"次日复跑（契约 §16 部署后复跑要求）样本够了自然会转红或转绿。"
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
			"立即查 scorer 的 DisableThinking 是否被改动（AGENTS.md 红线 1：V4 默认 reasoning " +
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
		r.Detail = "慢通道演化尚未产出负面清单。运行一次具体任务并产生反馈后再复跑。"
		return r
	}
	if n.Total == 0 {
		r.Status = StatusYellow
		r.Summary = "窗口内无打分调用，无从判定"
		return r
	}
	if n.WithTail == 0 {
		r.Status = StatusYellow
		r.Summary = fmt.Sprintf("当前画像有负面句，但窗口内 %d 条打分都没注入它", n.Total)
		r.Detail = "不是保尾失效（没有可剪的东西），更像画像注入本身出了问题——" +
			"先看探针 ④「画像注入生效性」，那边红了这里才有意义。" +
			"也可能是窗口整个落在负面句首次产生之前。"
		return r
	}
	if n.Intact < n.WithTail {
		r.Status = StatusRed
		r.Summary = fmt.Sprintf("%d/%d 条打分的负面句被截断了", n.WithTail-n.Intact, n.WithTail)
		r.Detail = fmt.Sprintf("保尾逻辑（审查 F1）失效：negTail 应原样穿过 buildSummary 与 capHint 的截断，"+
			"故「%s」之后到行尾不该出现省略号「%s」，现在出现了。"+
			"排查从 profilehint.go 的 buildSummary:74 / capHint:93 两条路径入手。"+
			"（当前画像的负面句是 %q，仅供参照——判据不比对它，见下条说明。）",
			profilehint.NegPrefix, profilehint.EllipsisRune, n.ExpectedTail)
		return r
	}
	r.Status = StatusGreen
	r.Summary = fmt.Sprintf("%d/%d 条打分的负面句均完整未截断", n.Intact, n.WithTail)
	r.Detail = fmt.Sprintf("判据是每条自包含的：画像行（=user_prompt 第一行）里「%s」之后到行尾无省略号。"+
		"**不**与当前画像逐字比对——画像会演化，2026-07-19 那次把负面句从 2 项加到 3 项，"+
		"字面比对让演化前写的 70 条全部假红，而它们一个字都没被剪。"+
		"锚定第一行同样关键：全文通配会被快通道区块头「近期不感兴趣」误命中而假绿。"+
		"当前画像的负面句：%q（参照用）。", profilehint.NegPrefix, n.ExpectedTail)
	if n.WithTail < n.Total {
		r.Detail += fmt.Sprintf("另：%d/%d 条打分注入时画像还没有负面句（演化前的历史），未计入判定。",
			n.Total-n.WithTail, n.Total)
	}
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
			"想验证请先点几条反馈，再手动运行对应任务。" + tagsNote
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
