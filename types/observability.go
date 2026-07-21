package types

import "time"

// 可观测性指标结构（Gate 服务端探针的数据面，M5 契约 §16）。
//
// 为什么单独成文件而非并入 entities.go：这些不是表实体，是聚合查询的产物——
// store 层按实体拆文件的约定对它们不适用，probe 层又只该管判定不该拼 SQL。
// 归属 types 是为了让 store（产出）与 probe（消费）都不反向依赖对方。
//
// 时区纪律（AGENTS.md 红线 6）：所有时间字段一律 UTC（DB 原生），
// 北京时间只在前端展示层换算——探针内部出现本地时区即是 bug。

// ScoreTraceStat 是单个 trace 的打分区分度统计（探针 ①）。
//
// 为什么按 trace 分组：一次 pipeline = 一个 trace_id，也就是"一批"。
// M3 事故的形状是"整批同分"，只有按批聚合才看得见——全局 distinct 会被
// 其它正常批次稀释掉。注意同一 trace_id 还覆盖 profile_evolve/cardgen
// （PushPipelineWorkflow 给这三步传同一个 traceID），故 SQL 必须限定 span_name。
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
	// MinCompletion 是该批 completion 的字典序最小值（原始文本）。
	// DistinctCompletions==1 时它就是整批唯一的输出——§16.1 良性同分判别
	// （2026-07-20 修订）只在该前提下消费它，其余场景仅供展示。
	MinCompletion string `json:"min_completion"`
	// MaxCompletionTokens 是该批 completion_tokens 的最大值。用途同上：
	// 区分"干净的单数字回答"（1-2 token）与 M3 形状（思维链吃满预算，tokens 反而高）。
	MaxCompletionTokens int `json:"max_completion_tokens"`
}

// ScoreLivenessStat 是"高分存在性"统计（探针 §16.8，2026-07-20 新增）。
//
// 它是 §16.1 良性同分判别的 cover property：§16.1 把「整批同一低分」判为良性
// 依赖一个假设——打分器仍能对相关内容给出高分。若 prompt 坏掉致模型恒输出低分，
// §16.1 会把每一批都判成"良性"，而本探针在窗口内找不到任何一条高于"不该推"档
// 的输出，就会把这个假设的破裂变成红灯（vacuous pass 的规定解法：
// 给"所有 X 满足 P"配一条"至少存在一个 X"）。
type ScoreLivenessStat struct {
	// Parsable 是窗口内 completion 含数字的成功打分数（与 ListScoreDistribution 同口径）。
	Parsable int `json:"parsable"`
	// AboveFloor 是解析分高于"不该推"语义档（>20）的条数。
	AboveFloor int `json:"above_floor"`
}

// ScoreQualityStat 是打分质量的窗口统计（探针 ②③）。
//
// 四个计数的关系（llm/do.go 的真值表，缺一不可）：
//   - OKTotal   = error=” 的行，即"模型确实答了"。它是回退率的分母。
//   - NoDigit   ⊂ OKTotal：答了但没数字 → parseScore 失败 → 静默回退中位分 50。
//   - EmptyNoError ⊂ NoDigit：答了但完全为空 → M3 事故的精确形状（thinking 吃光预算）。
//   - Errored   = error<>” 的行，与上面三者互斥：调用本身失败，
//     该条目被 Score 活动的"单条打分失败，跳过"分支直接跳过，**没有发出任何分数**。
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
//
// 判据是**每条调用自包含**的：看它自己那条负面句有没有被截断，而不是拿它跟
// 「当前画像」的负面句字面比对。后者曾让本探针在 2026-07-19 假红——画像演化
// 把负面句从 2 项改成 3 项后，演化前写的 70 条全都"不匹配"，可它们一个字都没被剪。
// 期望值取自会随时间变的外部状态，判据就不可能只反映被测性质。
type NegTailStat struct {
	// Total 是窗口内的打分调用总数。
	Total int `json:"total"`
	// WithTail 是画像行里带「不感兴趣：」句的调用数。
	// 小于 Total 说明有些调用注入时画像还没有负面句（演化前的历史），
	// 这不是 F1 失效——F1 管的是"有负面句时不许剪断它"。
	WithTail int `json:"with_tail"`
	// Intact 是这些负面句里**完整收尾**的条数（到行尾无省略号且以句号结束）。
	// Intact < WithTail 才是保尾真失效。
	Intact int `json:"intact"`
	// ExpectedTail 是当前画像的负面句，**仅供报告里作参照展示**，不参与判定。
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
// 空批次盲区已由 009 补上（PR2）：五处提前退出（workflow.go 的 fetch/dedup/score/
// select/cardgen 闸门）现在各自留一行 Status=empty 的批次，ExitGate 说明从哪退的、
// StageCounts 说明各阶段还剩几条。故"今早无新内容"现在**在库里有行**。
//
// 仍存在的盲区，读这张表时必须知道（别把它当成 pipeline 的完整流水账）：
// **pipeline 中途报错的运行仍然没有行**。Fetch/Score 等活动重试耗尽后 workflow
// 直接失败返回，压根走不到任何闸门。这是刻意的边界，不是遗漏——"跑崩了"本就
// 有记录（Temporal 里该 workflow 是 Failed，加 journalctl 的错误日志），而"跑完了
// 但没东西推"此前才是真的无记录（五处闸门全 return nil，Temporal 显示 Completed，
// 库里零行，两边都说不出话）。把 Temporal 的执行史再往 Postgres 抄一遍，是用更差的
// 实现重造一个 Temporal。故本表的语义是"推送决策的产物"，不是"每次触发的日志"。
type PushBatchSummary struct {
	ID     int64       `json:"id"`
	Status BatchStatus `json:"status"`
	// ExitGate 提前退出的闸门，空串 = 跑到了 Push（Status=done|failed|pending）。
	// 与 Status=empty 恒同时出现：有 gate ⇔ 是空批次。
	ExitGate BatchExitGate `json:"exit_gate"`
	// StageCounts 各阶段跑完后剩余条数；字段为 nil = 那一步没跑（不是"跑了得 0"）。
	// 009 之前的历史行、以及**成功推送的批次**，这里全 nil。
	//
	// done 批次没有漏斗是刻意的范围控制，但别把补它想成免费的——代价不比现在小：
	// 计数得经 PushIn 传进 Push 活动，而 PushIn 是 in-flight 敏感的 Temporal 载荷
	// （改它 = 停在 Push 前的 workflow 重放时解不出新字段，契约 §8.2），且写入点在
	// CreatePushBatchIdempotent 那条 #1 CRITICAL 幂等路径上——正是本 PR 通篇刻意
	// 绕开的两样东西。schema 确实不用再动（JSONB 列已就位），但 schema 从来不是
	// 这件事的难点，别让"列都建好了"读起来像"接上就行"。
	//
	// 真要做，先想清楚它值不值：done 批次的漏斗信息，DeliveryCount 已经给了末端，
	// 中间各级的价值目前只是"好看"。
	StageCounts PipelineCounts `json:"stage_counts"`
	// CreatedAt 是批次时间锚点（UTC）。刻意不用 scheduled_at：该列从无代码写入，
	// 恒为 NULL（store/push_batches.go 里三处 INSERT——CreatePushBatch /
	// CreatePushBatchIdempotent / RecordEmptyPushBatch——的列清单都不含它）。
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
