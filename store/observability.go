package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/profilehint"
	"github.com/YouToco/vane/types"
)

// withTenantObservabilityRead installs the tenant RLS identity on the same
// transaction that performs an observability query. A SQL tenant predicate is
// not a substitute: under the non-owner server runtime, an unset GUC makes RLS
// silently return zero rows and can turn a red Gate into a vacuous yellow pass.
func withTenantObservabilityRead[T any](
	ctx context.Context,
	s *Store,
	tenantID int64,
	read func(pgx.Tx) (T, error),
) (T, error) {
	var zero T
	if tenantID <= 0 {
		return zero, types.NewAppError(
			types.CodeValidation, "观测数据租户范围无效", types.ErrValidation)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return zero, types.NewAppError(types.CodeDatabase, "开启租户观测读取事务", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `SET LOCAL search_path = pg_catalog, public, pg_temp`); err != nil {
		return zero, types.NewAppError(types.CodeDatabase, "固定租户观测读取 search_path", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		strconv.FormatInt(tenantID, 10)); err != nil {
		return zero, types.NewAppError(types.CodeDatabase, "设置租户观测读取范围", err)
	}
	value, err := read(tx)
	if err != nil {
		return zero, err
	}
	if err := tx.Commit(ctx); err != nil {
		return zero, types.NewAppError(types.CodeDatabase, "提交租户观测读取事务", err)
	}
	return value, nil
}

// Gate 服务端探针的数据面查询（M5 契约 §16）。只读、只聚合，不写任何表。
//
// 表名纪律：LLM 明细表是 **llm_calls**（001_init.sql:172），不是 llm_traces。
// 当时的协作入口曾误记为后者（本 PR 已订正）——照那个名字写的 SQL 会报
// relation does not exist，而那读起来像"没数据"，正是红线 6 警告的失败模式。
//
// 正则纪律：Go 侧 parseScore 用 `-?\d+(?:\.\d+)?`（scorer.go:81），RE2 的 \d
// 恒为 ASCII [0-9]。而 Postgres ARE 的 \d 是 [[:digit:]]、受 locale 影响，
// 可能把全角数字也算进去。故本文件一律显式写 [0-9]，不用 \d。

// scoreSpan 等是探针关心的 span_name 字面量。
//
// 为什么在这里复述而不 import：写入方（scorer.go:132 / evolver.go:116 等）把它们
// 硬编码在各自的 CallMeta 里，全仓没有共享常量。本 PR 是只读功能，刻意不去重构
// 打分/演化链路（核心路径，改动需全流程对抗审查）——故这里是**副本**。
//
// 副本会漂吗？会，但漂了会响：ProfileInjectionStat.Unrecognized 与
// probe 层的零调用判定共同构成自检位，字面量对不上时探针变红而非变绿。
// 另有 probe/literals_test.go 在 CI 阶段直接比对源码，把它挡在上线之前。
//
// 注意 001_init.sql:175 的 span 注释清单是错的（列了三个从未写过的
// summarize/feedback_interpret/quality_check，漏了四个真实存在的）——
// 本 PR 已一并订正。真实 span 恰为六个：
// score / cardgen / profile_evolve / deep_dive / chat_reply / agent。
const (
	scoreSpan   = "score"
	evolveSpan  = "profile_evolve"
	cardgenSpan = "cardgen"
)

// profileHintPrefix 是 buildScoreUser 恒定写在 user_prompt **开头**的画像行前缀
// （scorer.go:205 与 :207 两个分支都写它）。
// profileAbsentPrefix 是"无画像"分支的完整前缀（scorer.go:205）。
//
// 锚定在开头（LIKE '前缀%'）而非契约 §16:633 原文的 '%…%' 全文通配：正文是全系统
// 最大的攻击面且 promptguard.Sanitize 不剥这串字，一篇正文里恰好含
// "用户画像：暂无" 的 RSS 就能让全文通配版误判成"注入失效"。开头锚定不可伪造。
const (
	profileHintPrefix   = "用户画像："
	profileAbsentPrefix = "用户画像：暂无"
)

// ListScoreTraceStats 返回窗口内每个 trace 的打分区分度（探针 ①）。
// minN 是"批"的最小规模（契约取 5）：样本太小时同分不能说明问题。
//
// error=” 过滤是必须的：失败行 completion 恒为 ”（llm/do.go 只在成功分支赋值），
// 混进来会让 distinct 既可能虚高（多一个 ”）也可能虚低（整批失败时 distinct=1），
// 两个方向都是误判。
func (s *Store) ListScoreTraceStats(ctx context.Context, tenantID int64, since time.Time, minN int) ([]types.ScoreTraceStat, error) {
	return withTenantObservabilityRead(ctx, s, tenantID, func(tx pgx.Tx) ([]types.ScoreTraceStat, error) {
		rows, err := tx.Query(ctx,
			`SELECT trace_id, count(*)::int, count(DISTINCT completion)::int, min(created_at),
		        min(completion), max(completion_tokens)::int
			 FROM llm_calls
			 WHERE span_name = $1 AND error = '' AND created_at >= $2
			   AND tenant_id = $4
		 GROUP BY trace_id
		 HAVING count(*) >= $3
		 ORDER BY min(created_at) DESC`,
			scoreSpan, since, minN, tenantID)
		if err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "查询打分批次区分度", err)
		}
		defer rows.Close()

		var out []types.ScoreTraceStat
		for rows.Next() {
			var st types.ScoreTraceStat
			if err := rows.Scan(&st.TraceID, &st.N, &st.DistinctCompletions, &st.StartedAt,
				&st.MinCompletion, &st.MaxCompletionTokens); err != nil {
				return nil, types.NewAppError(types.CodeDatabase, "扫描打分批次统计行", err)
			}
			out = append(out, st)
		}
		if err := rows.Err(); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "遍历打分批次统计结果集", err)
		}
		return out, nil
	})
}

// GetScoreQualityStat 返回窗口内的打分质量四联计数（探针 ②③）。
//
// 四个 FILTER 的边界即 llm/do.go 的真值表，不可随意增删：
//   - error<>”            → 调用失败，条目被跳过，**没发分**（不是回退）
//   - error=” 且无数字    → 静默回退中位分 50（含空 completion）
//   - error=” 且 completion=”→ M3 事故的精确形状（DisableThinking 回归）
func (s *Store) GetScoreQualityStat(ctx context.Context, tenantID int64, since time.Time) (types.ScoreQualityStat, error) {
	return withTenantObservabilityRead(ctx, s, tenantID, func(tx pgx.Tx) (types.ScoreQualityStat, error) {
		var st types.ScoreQualityStat
		err := tx.QueryRow(ctx,
			`SELECT
		     count(*) FILTER (WHERE error = '')::int,
		     count(*) FILTER (WHERE error = '' AND completion !~ '[0-9]')::int,
		     count(*) FILTER (WHERE error = '' AND completion = '')::int,
		     count(*) FILTER (WHERE error <> '')::int
			 FROM llm_calls
			 WHERE span_name = $1 AND created_at >= $2 AND tenant_id = $3`,
			scoreSpan, since, tenantID).Scan(&st.OKTotal, &st.NoDigit, &st.EmptyNoError, &st.Errored)
		if err != nil {
			return st, types.NewAppError(types.CodeDatabase, "查询打分质量统计", err)
		}
		return st, nil
	})
}

// ListScoreDistribution 返回窗口内的分数分布直方图（10 个宽度为 10 的桶）。
//
// 提数逻辑与 parseScore 逐字对齐（scorer.go:247-263）：取首个数字 → 夹逼 [0,100]。
// 正则写作 `-?[0-9]+\.?[0-9]*` 而非 Go 的 `-?[0-9]+(?:\.[0-9]+)?`，是为了**一个括号都不出现**：
// Postgres 的 substring(x from pattern) 在 pattern 含括号时返回第一个括号子表达式
// 而非整个匹配，`(?:)` 是否算"括号"依实现而定——绕开比赌它安全。
// 两者对所有实际输入取到同一数值（差异仅在 "85." 这类尾点串上，PG 与 Go 都得 85）。
//
// width_bucket(x,0,100,10) 对 x=100 返回 11（右开区间之外），故 LEAST 折回第 10 桶，
// 使末桶语义为闭区间 [90,100]。
func (s *Store) ListScoreDistribution(ctx context.Context, tenantID int64, since time.Time) ([]types.ScoreBucket, error) {
	return withTenantObservabilityRead(ctx, s, tenantID, func(tx pgx.Tx) ([]types.ScoreBucket, error) {
		rows, err := tx.Query(ctx,
			`SELECT b, count(*)::int FROM (
		     SELECT LEAST(10, width_bucket(
		         LEAST(100, GREATEST(0,
		             substring(completion from '-?[0-9]+\.?[0-9]*')::numeric)),
		         0, 100, 10)) AS b
		     FROM llm_calls
			     WHERE span_name = $1 AND error = '' AND created_at >= $2
			       AND tenant_id = $3
		       AND completion ~ '[0-9]'
		 ) t
		 GROUP BY b ORDER BY b`,
			scoreSpan, since, tenantID)
		if err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "查询分数分布", err)
		}
		defer rows.Close()

		// 预置 10 个空桶：直方图要的是完整轮廓，缺桶应显示为 0 而不是消失。
		buckets := make([]types.ScoreBucket, 10)
		for i := range buckets {
			buckets[i] = types.ScoreBucket{Lo: i * 10, Hi: i*10 + 10}
		}
		for rows.Next() {
			var b, n int
			if err := rows.Scan(&b, &n); err != nil {
				return nil, types.NewAppError(types.CodeDatabase, "扫描分数分布行", err)
			}
			if b >= 1 && b <= 10 {
				buckets[b-1].Count = n
			}
		}
		if err := rows.Err(); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "遍历分数分布结果集", err)
		}
		return buckets, nil
	})
}

// GetScoreLivenessStat 返回窗口内"高分存在性"计数（探针 §16.8）。
//
// floor 是"不该推"语义档的上界（由 probe 层从 types.DefaultStrictness.MinKeepScore()
// 推出并传入，不在这里第二次定义档位）。AboveFloor 统计解析分**严格大于** floor 的条数。
//
// 提数表达式与 ListScoreDistribution 逐字相同（含"正则零括号"的取舍，见其注释）：
// 两个查询消费同一个 parseScore 语义，表达式分叉等于探针之间口径漂移。
func (s *Store) GetScoreLivenessStat(ctx context.Context, tenantID int64, since time.Time, floor int) (types.ScoreLivenessStat, error) {
	return withTenantObservabilityRead(ctx, s, tenantID, func(tx pgx.Tx) (types.ScoreLivenessStat, error) {
		var st types.ScoreLivenessStat
		err := tx.QueryRow(ctx,
			`SELECT count(*)::int,
		        count(*) FILTER (WHERE sc > $3)::int
		 FROM (
		     SELECT LEAST(100, GREATEST(0,
		         substring(completion from '-?[0-9]+\.?[0-9]*')::numeric)) AS sc
		     FROM llm_calls
			     WHERE span_name = $1 AND error = '' AND created_at >= $2
			       AND tenant_id = $4
		       AND completion ~ '[0-9]'
		 ) t`,
			scoreSpan, since, floor, tenantID).Scan(&st.Parsable, &st.AboveFloor)
		if err != nil {
			return st, types.NewAppError(types.CodeDatabase, "查询高分存在性统计", err)
		}
		return st, nil
	})
}

// GetProfileInjectionStat 返回窗口内画像注入生效性统计（探针 ④）。
//
// 只统计 score span：契约 §16:633 原文未限定 span，而 cardgen 在**运行时**拼出
// 一模一样的 "用户画像：暂无"（cardgen.go:131 的 "用户画像：" + :133 的 "暂无"），
// 源码 grep 看不见但库里在。每 trace 约 50 条 score + 5 条 cardgen，不限定就把两者混算。
//
// 注意 deep_dive span 对本探针天然免疫：它在无画像时整行省略而非写"暂无"
// （deepdive.go:252），故永远不会命中。那条链路的降级要靠 deepdive.go:271 的 WARN。
func (s *Store) GetProfileInjectionStat(ctx context.Context, tenantID int64, since time.Time) (types.ProfileInjectionStat, error) {
	return withTenantObservabilityRead(ctx, s, tenantID, func(tx pgx.Tx) (types.ProfileInjectionStat, error) {
		var st types.ProfileInjectionStat
		err := tx.QueryRow(ctx,
			`SELECT
		     count(*)::int,
		     count(*) FILTER (WHERE user_prompt LIKE $3)::int,
		     count(*) FILTER (WHERE user_prompt LIKE $4 AND user_prompt NOT LIKE $3)::int
			 FROM llm_calls
			 WHERE span_name = $1 AND created_at >= $2 AND tenant_id = $5`,
			scoreSpan, since, profileAbsentPrefix+"%", profileHintPrefix+"%", tenantID).
			Scan(&st.Total, &st.Absent, &st.Present)
		if err != nil {
			return st, types.NewAppError(types.CodeDatabase, "查询画像注入统计", err)
		}
		st.Unrecognized = st.Total - st.Absent - st.Present
		return st, nil
	})
}

// GetNegTailStat 统计窗口内打分调用的负面句保尾情况（探针 ⑤）。
//
// 锚定第一行是本探针的关键：画像 hint 是**硬约束单行**（profilehint.go 的 singleLine
// 把换行也折成空格），而 buildScoreUser 写完画像行紧跟一个 '\n'（scorer.go:209）——
// 故 hint 可证明就是 user_prompt 的**整个第一行**。
//
// 为什么不能图省事用全文通配：scorer.go:223 的快通道区块头【近期不感兴趣·…】里
// 也有"不感兴趣"，且区块内嵌的是用户内容标题——一条标题里恰好含"不感兴趣："
// 就能让探针在**尾巴已被切掉**时照样 PASS，正好废掉 F1 验证。第一行锚定挡住了它。
//
// 判据为什么不比对 expectedTail（2026-07-19 改）：原实现拿**当前画像**的负面句
// 去逐字比对窗口内所有调用，而画像会演化。07-19 15:11 演化把负面句从 2 项加到
// 3 项，之前写的 70 条立刻全部"不匹配"→ 报红，可生产库实证那 70 条的负面句
// 完整收尾、一个字没被剪。期望值取自会漂移的外部状态，判据就测不准被测性质。
//
// 现在验的是每条自包含的不变量：负面句从「不感兴趣：」到行尾**不含省略号**。
// 这正是 F1 承诺的形状——buildSummary/capHint 只在 neg 之前放省略号，neg 整段原样
// 附加（profilehint.go:74/93）。保尾一旦失效，截断必然在 neg 里或其后留下省略号标记
// （truncateEllipsis 恒追加），故本判据既充分又与画像版本无关。
//
// 刻意**不**要求以句号收尾：句号来自演化 prompt 规则 2 的格式约定，是模型的合规行为，
// 模型偶尔漏写就会让探针假红——而那是格式问题，不是 F1 要管的截断。判据只该反映被测性质。
func (s *Store) GetNegTailStat(ctx context.Context, tenantID int64, since time.Time, expectedTail string) (types.NegTailStat, error) {
	st := types.NegTailStat{ExpectedTail: expectedTail}
	if tenantID <= 0 {
		return st, types.NewAppError(
			types.CodeValidation, "观测数据租户范围无效", types.ErrValidation)
	}
	if expectedTail == "" {
		return st, nil // 当前画像无负面句，探针不适用；不打 DB
	}
	return withTenantObservabilityRead(ctx, s, tenantID, func(tx pgx.Tx) (types.NegTailStat, error) {
		err := tx.QueryRow(ctx,
			`SELECT
		     count(*)::int,
		     count(*) FILTER (WHERE split_part(user_prompt, E'\n', 1) LIKE '%' || $3 || '%')::int,
		     count(*) FILTER (WHERE split_part(user_prompt, E'\n', 1) ~ ($3 || '[^' || $4 || ']*$'))::int
			 FROM llm_calls
			 WHERE span_name = $1 AND created_at >= $2 AND tenant_id = $5`,
			scoreSpan, since, profilehint.NegPrefix, profilehint.EllipsisRune, tenantID).
			Scan(&st.Total, &st.WithTail, &st.Intact)
		if err != nil {
			return st, types.NewAppError(types.CodeDatabase, "查询负面清单保尾统计", err)
		}
		return st, nil
	})
}

// ListSpanDayCosts 返回窗口内按 (UTC 日, span) 聚合的成本（探针 ⑥）。
//
// 日界固定 UTC：created_at 是 TIMESTAMPTZ（UTC），而 VPS 本地是 EDT、Boss 读的是
// 北京时间，三个时区（红线 6）。探针内部认 DB 原生时区，换算只在前端做——
// 内部一出现本地时区，"哪天"就会随执行环境漂。
func (s *Store) ListSpanDayCosts(ctx context.Context, tenantID int64, since time.Time) ([]types.SpanDayCost, error) {
	return withTenantObservabilityRead(ctx, s, tenantID, func(tx pgx.Tx) ([]types.SpanDayCost, error) {
		rows, err := tx.Query(ctx,
			`SELECT date_trunc('day', created_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC',
		        span_name, count(*)::int, sum(cost_usd)::float8
			 FROM llm_calls
			 WHERE created_at >= $1 AND tenant_id = $2
		 GROUP BY 1, 2
		 ORDER BY 1 DESC, 2`,
			since, tenantID)
		if err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "查询按 span 分日成本", err)
		}
		defer rows.Close()

		var out []types.SpanDayCost
		for rows.Next() {
			var c types.SpanDayCost
			if err := rows.Scan(&c.Day, &c.SpanName, &c.Calls, &c.CostUSD); err != nil {
				return nil, types.NewAppError(types.CodeDatabase, "扫描 span 成本行", err)
			}
			out = append(out, c)
		}
		if err := rows.Err(); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "遍历 span 成本结果集", err)
		}
		return out, nil
	})
}

// ListModelUsage 返回窗口内按 model 聚合的调用量与成本（探针 ⑥ 伴生：计价漂移）。
func (s *Store) ListModelUsage(ctx context.Context, tenantID int64, since time.Time) ([]types.ModelUsage, error) {
	return withTenantObservabilityRead(ctx, s, tenantID, func(tx pgx.Tx) ([]types.ModelUsage, error) {
		rows, err := tx.Query(ctx,
			`SELECT model, count(*)::int, sum(cost_usd)::float8
			 FROM llm_calls
			 WHERE created_at >= $1 AND tenant_id = $2
		 GROUP BY model
		 ORDER BY count(*) DESC, model`,
			since, tenantID)
		if err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "查询 model 用量", err)
		}
		defer rows.Close()

		var out []types.ModelUsage
		for rows.Next() {
			var m types.ModelUsage
			if err := rows.Scan(&m.Model, &m.Calls, &m.CostUSD); err != nil {
				return nil, types.NewAppError(types.CodeDatabase, "扫描 model 用量行", err)
			}
			out = append(out, m)
		}
		if err := rows.Err(); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "遍历 model 用量结果集", err)
		}
		return out, nil
	})
}

// GetEvolveCallStat 返回该用户的演化调用统计（探针 ⑦ 的 llm_calls 一侧）。
// LastCallAt 刻意不受 since 约束——见 types.EvolveCallStat 的字段注释。
func (s *Store) GetEvolveCallStat(ctx context.Context, tenantID, userID int64, since time.Time) (types.EvolveCallStat, error) {
	return withTenantObservabilityRead(ctx, s, tenantID, func(tx pgx.Tx) (types.EvolveCallStat, error) {
		var st types.EvolveCallStat
		err := tx.QueryRow(ctx,
			`SELECT
		     count(*) FILTER (WHERE created_at >= $3)::int,
		     count(*) FILTER (WHERE created_at >= $3 AND error <> '')::int,
		     max(created_at)
			 FROM llm_calls
			 WHERE span_name = $1 AND user_id = $2 AND tenant_id = $4`,
			evolveSpan, userID, since, tenantID).Scan(&st.Calls, &st.Errored, &st.LastCallAt)
		if err != nil {
			return st, types.NewAppError(types.CodeDatabase,
				fmt.Sprintf("查询用户 %d 的演化调用统计", userID), err)
		}
		return st, nil
	})
}

// ListPushBatchSummaries 返回该用户最近的推送批次（含投递计数与原始分极值）。
//
// LEFT JOIN 而非 JOIN：投递数为 0 的批次必须显示。009 起这里有**两种**零投递批次，
// 读的人（和看板）必须靠 status 分开它们，别再一律当异常：
//   - status=empty：正常终态，本就没东西可推，exit_gate 说明停在哪一步。
//   - status=done|failed 而投递数为 0：真异常——Push 建了批次却一条都没插成
//     （activities.go 的插入失败只 Warn 后继续）。
//
// 009 之前"今早无新内容"连行都没有，现在有了（见 types.PushBatchSummary 的注释，
// 那里也写明了剩下的盲区：中途报错的运行仍无行，那是 Temporal 的账）。
//
// GROUP BY b.id 即可带出 b 的其余列：id 是主键，Postgres 认函数依赖，
// 不必把 exit_gate/stage_counts 也堆进 GROUP BY。
//
// 无 ORDER BY 稳定性问题：created_at 有 DEFAULT now() 且同批次 id 单调，
// 用 (created_at DESC, id DESC) 保证并列时序稳定。
func (s *Store) ListPushBatchSummaries(ctx context.Context, tenantID, userID int64, since time.Time, limit int) ([]types.PushBatchSummary, error) {
	return withTenantObservabilityRead(ctx, s, tenantID, func(tx pgx.Tx) ([]types.PushBatchSummary, error) {
		rows, err := tx.Query(ctx,
			`SELECT b.id, b.status, b.exit_gate, b.stage_counts, b.created_at, b.idempotency_key,
		        b.run_snapshot_id,
		        count(d.id)::int,
		        count(d.id) FILTER (WHERE d.status = $4)::int,
		        max(d.score)::float8, min(d.score)::float8
			 FROM push_batches b
			 LEFT JOIN deliveries d
			   ON d.batch_id = b.id AND d.tenant_id = $5 AND d.user_id = $1
			 WHERE b.user_id = $1 AND b.created_at >= $2 AND b.tenant_id = $5
		 GROUP BY b.id
		 ORDER BY b.created_at DESC, b.id DESC
		 LIMIT $3`,
			userID, since, limit, types.DeliveryStatusSent, tenantID)
		if err != nil {
			return nil, types.NewAppError(types.CodeDatabase,
				fmt.Sprintf("查询用户 %d 的推送批次历史", userID), err)
		}
		defer rows.Close()

		var out []types.PushBatchSummary
		for rows.Next() {
			var b types.PushBatchSummary
			// stage_counts 先落 []byte 再解：JSONB NOT NULL DEFAULT '{}'（009），
			// 空对象解出全 nil 的 PipelineCounts，恰是"这些阶段没记录"的正确语义。
			var countsJSON []byte
			var runSnapshotID *int64
			if err := rows.Scan(&b.ID, &b.Status, &b.ExitGate, &countsJSON, &b.CreatedAt, &b.IdempotencyKey,
				&runSnapshotID, &b.DeliveryCount, &b.SentCount, &b.MaxScore, &b.MinScore); err != nil {
				return nil, types.NewAppError(types.CodeDatabase, "扫描推送批次统计行", err)
			}
			if runSnapshotID != nil {
				logicalKey, ok := compiledPushBatchLogicalKey(*runSnapshotID, b.IdempotencyKey)
				if !ok {
					return nil, types.NewAppError(types.CodeDatabase,
						fmt.Sprintf("解析批次 %d 的运行幂等键", b.ID), nil)
				}
				b.IdempotencyKey = logicalKey
			}
			if err := json.Unmarshal(countsJSON, &b.StageCounts); err != nil {
				return nil, types.NewAppError(types.CodeDatabase,
					fmt.Sprintf("解析批次 %d 的漏斗计数", b.ID), err)
			}
			out = append(out, b)
		}
		if err := rows.Err(); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "遍历推送批次结果集", err)
		}
		return out, nil
	})
}
