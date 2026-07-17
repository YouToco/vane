// 运行统计查询（M7 功能 6.5 的数据面）：llm_calls 按 span 聚合的完整运行指标
// （调用量/错误/成本/token/延迟/缓存命中）。只读——本文件不写任何表。
//
// 与 observability.go 的分工：那边是 Gate 探针的红线判定输入（窄口径，服务判定），
// 这边是运行监控页的展示数据（富口径，服务人看趋势）。SpanDayCost/ModelUsage
// 两个既有聚合被 6.5 页面复用，不在此重复实现。
package store

import (
	"context"
	"time"

	"github.com/YouToco/vane/types"
)

// SpanRunStat 是一个 span 的窗口运行统计。
//
// 延迟给 avg + p95 双值：avg 被长尾拉高时看不出"多数请求其实很快"，
// p95 单独给才知道尾巴多长。CacheHits 只统计 prefix_cache_hit IS TRUE——
// 该列可空（旧行/不支持缓存的调用为 NULL），NULL 既不算命中也不算未命中，
// 命中率的分母是 CacheKnown 而非 Calls，前端算比例必须用 CacheKnown。
type SpanRunStat struct {
	SpanName         string  `json:"span_name"`
	Calls            int     `json:"calls"`
	Errors           int     `json:"errors"` // error 列非空串的行数
	CostUSD          float64 `json:"cost_usd"`
	PromptTokens     int64   `json:"prompt_tokens"`
	CompletionTokens int64   `json:"completion_tokens"`
	AvgLatencyMs     float64 `json:"avg_latency_ms"`
	P95LatencyMs     float64 `json:"p95_latency_ms"`
	CacheHits        int     `json:"cache_hits"`
	CacheKnown       int     `json:"cache_known"` // prefix_cache_hit 非 NULL 的行数（命中率分母）
}

// ListSpanRunStats 返回窗口内按 span 聚合的运行统计，按调用量降序。
func (s *Store) ListSpanRunStats(ctx context.Context, since time.Time) ([]SpanRunStat, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT span_name,
		        count(*)::int,
		        count(*) FILTER (WHERE error <> '')::int,
		        sum(cost_usd)::float8,
		        coalesce(sum(prompt_tokens), 0)::bigint,
		        coalesce(sum(completion_tokens), 0)::bigint,
		        coalesce(avg(latency_ms), 0)::float8,
		        coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms), 0)::float8,
		        count(*) FILTER (WHERE prefix_cache_hit IS TRUE)::int,
		        count(prefix_cache_hit)::int
		 FROM llm_calls
		 WHERE created_at >= $1
		 GROUP BY span_name
		 ORDER BY count(*) DESC, span_name`,
		since)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "查询 span 运行统计", err)
	}
	defer rows.Close()

	var out []SpanRunStat
	for rows.Next() {
		var r SpanRunStat
		if err := rows.Scan(&r.SpanName, &r.Calls, &r.Errors, &r.CostUSD,
			&r.PromptTokens, &r.CompletionTokens, &r.AvgLatencyMs, &r.P95LatencyMs,
			&r.CacheHits, &r.CacheKnown); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描 span 运行统计行", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历 span 运行统计结果集", err)
	}
	return out, nil
}
