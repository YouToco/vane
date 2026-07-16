package types

import "time"

// 可观测性指标结构（Gate 服务端探针的数据面，M5 契约 §16）。
//
// 为什么单独成文件而非并入 entities.go：这些不是表实体，是聚合查询的产物——
// store 层按实体拆文件的约定对它们不适用，probe 层又只该管判定不该拼 SQL。
// 归属 types 是为了让 store（产出）与 probe（消费）都不反向依赖对方。
//
// 时区纪律（CLAUDE.md 红线 6）：所有时间字段一律 UTC（DB 原生），
// 北京时间只在前端展示层换算——探针内部出现本地时区即是 bug。

// ScoreTraceStat 是单个 trace 的打分区分度统计（探针 ①）。
//
// 为什么按 trace 分组：一次 pipeline = 一个 trace_id，也就是"一批"。
// M3 事故的形状是"整批同分"，只有按批聚合才看得见——全局 distinct 会被
// 其它正常批次稀释掉。注意同一 trace_id 还覆盖 profile_evolve/cardgen
// （workflow.go:45/74/100 传同一个 traceID），故 SQL 必须限定 span_name。
type ScoreTraceStat struct {
	TraceID string `json:"trace_id"`
	// N 是该批次成功返回的打分次数（error='' 的行）。
	N int `json:"n"`
	// DistinctCompletions 是原始 completion 文本的去重计数。
	// 刻意统计原始文本而非夹逼后的分数：parseScore 的 clamp 发生在记账之后
	// （scorer.go:256-261），库里存的是模型原话。故 "85" 与 "85分" 算两种。
	// 这对探针是安全的方向——它只会让 distinct 偏高（更不容易误报），
	// 而 M3 的真实形状（整批逐字节相同的 "50"）依然 distinct=1。
	DistinctCompletions int       `json:"distinct_completions"`
	StartedAt           time.Time `json:"started_at"`
}

// ScoreQualityStat 是打分质量的窗口统计（探针 ②③）。
//
// 四个计数的关系（llm/do.go 的真值表，缺一不可）：
//   - OKTotal   = error='' 的行，即"模型确实答了"。它是回退率的分母。
//   - NoDigit   ⊂ OKTotal：答了但没数字 → parseScore 失败 → 静默回退中位分 50。
//   - EmptyNoError ⊂ NoDigit：答了但完全为空 → M3 事故的精确形状（thinking 吃光预算）。
//   - Errored   = error<>'' 的行，与上面三者互斥：调用本身失败，
//     该条目被 activities.go:325-329 直接跳过，**没有发出任何分数**。
//
// 契约 §16 原文"completion 无数字占比"未限定 error，会把 Errored 也算成回退——
// 一次上游 429 抖动就能冲爆 10% 红线，而它其实一分都没发。故分母必须是 OKTotal。
type ScoreQualityStat struct {
	OKTotal      int `json:"ok_total"`
	NoDigit      int `json:"no_digit"`
	EmptyNoError int `json:"empty_no_error"`
	Errored      int `json:"errored"`
}

// FallbackRate 是中位分回退率（探针 ②）。无成功调用时返回 0：
// 零调用不是"回退率 100%"，判定交给 probe 层按 OKTotal==0 走"数据不足"。
func (s ScoreQualityStat) FallbackRate() float64 {
	if s.OKTotal == 0 {
		return 0
	}
	return float64(s.NoDigit) / float64(s.OKTotal)
}

// ScoreBucket 是分数分布直方图的一个桶，区间为 [Lo, Hi)，最后一桶闭合到 100。
type ScoreBucket struct {
	Lo    int `json:"lo"`
	Hi    int `json:"hi"`
	Count int `json:"count"`
}

// ProfileInjectionStat 是画像注入生效性统计（探针 ④）。
//
// buildScoreUser（scorer.go:202-210）两个分支都以 "用户画像：" 开头，故恒有
// Present + Absent == Total。Unrecognized 是这个恒等式的余数：它 >0 意味着
// scorer 的 prompt 结构变了而探针字面量没跟上——探针的自检位，
// 让"字面量漂移"表现为响亮的红灯而不是永远的绿灯。
type ProfileInjectionStat struct {
	Total   int `json:"total"`
	Absent  int `json:"absent"`
	Present int `json:"present"`
	// Unrecognized = Total - Present - Absent，恒应为 0。
	Unrecognized int `json:"unrecognized"`
}

// NegTailStat 是负面清单保尾统计（探针 ⑤，F1 的线上验证）。
// Intact 统计"画像行里完整出现了负面句"的打分次数。
type NegTailStat struct {
	Total  int `json:"total"`
	Intact int `json:"intact"`
	// ExpectedTail 是比对用的期望负面句（取自 profiles.summary，已折叠空白）。
	// 为空表示当前画像没有负面句，探针不适用。
	ExpectedTail string `json:"expected_tail"`
}

// SpanDayCost 是按 (UTC 日, span) 聚合的成本（探针 ⑥）。
//
// 量化注意：cost_usd 是 NUMERIC(10,6)，每行**独立**四舍五入后才求和，
// 故本值与真实总额有系统性偏差。score 是最便宜的 span（MaxTokens=16），
// 单次全缓存命中可低至 ~$0.0000008 → 逐行舍入后可能整批为 0。
// 因此任何"score 成本必须 > 0"的断言都是错的，不要写。
type SpanDayCost struct {
	Day      time.Time `json:"day"` // UTC 日界
	SpanName string    `json:"span_name"`
	Calls    int       `json:"calls"`
	CostUSD  float64   `json:"cost_usd"`
}

// ModelUsage 是按 model 聚合的调用量（探针 ⑥ 的伴生：计价漂移探测）。
//
// 为什么需要它：CostUSD 按 resp.Model（**上游报回的名字**）查价，未知 key
// 静默回落 v4-pro 价（约 flash 的 3 倍）。上游一次改名就能无声烧穿预算，
// 且不产生任何 error。按 model 分组是唯一能提前看见它的角度。
type ModelUsage struct {
	Model   string  `json:"model"`
	Calls   int     `json:"calls"`
	CostUSD float64 `json:"cost_usd"`
}

// EvolveCallStat 是演化调用的窗口统计（探针 ⑦ 的 llm_calls 一侧）。
type EvolveCallStat struct {
	Calls   int `json:"calls"`
	Errored int `json:"errored"`
	// LastCallAt 是**不限窗口**的最近一次演化调用时刻，nil = 从未演化。
	// 不限窗口是刻意的：判定"最近这次演化写没写成"需要拿它与 profiles.updated_at
	// 比较，而上次演化可能落在窗口之外。
	LastCallAt *time.Time `json:"last_call_at,omitempty"`
}

// PushBatchSummary 是推送批次历史的一行（含投递计数）。
//
// 已知盲区（M5 契约 §16 修订记录 / PR2 待办）：本结构只能覆盖**真的建了行**的
// 批次。pipeline 有五处提前退出（workflow.go:55/66/77/92/103）在 Push 之前
// return nil，push_batches 零行——"今早无新内容"这类空批次在库里根本不存在，
// 不是查询查不到。补齐它需要写入路径变更，见 PR2。
type PushBatchSummary struct {
	ID     int64       `json:"id"`
	Status BatchStatus `json:"status"`
	// CreatedAt 是批次时间锚点（UTC）。刻意不用 scheduled_at：该列从无代码写入，
	// 恒为 NULL（store/push_batches.go:16/36 两处 INSERT 都不含它）。
	CreatedAt      time.Time `json:"created_at"`
	IdempotencyKey string    `json:"idempotency_key"` // = workflow 的 traceID，可据此关联 llm_calls
	DeliveryCount  int       `json:"delivery_count"`
	SentCount      int       `json:"sent_count"`
	// MaxScore/MinScore 是本批 deliveries.score 的极值，nil = 本批无投递。
	// 注意这是 LLM **原始相关分**，不是排序用的有效分——有效分含时新度衰减、
	// 只在推送时刻内存中存在，从不落库（selector/selector.go:49-51）。
	// 故看板上"分数高的排在后面"是正常的，不是 bug。
	MaxScore *float64 `json:"max_score,omitempty"`
	MinScore *float64 `json:"min_score,omitempty"`
}
