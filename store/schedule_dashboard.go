// 任务数据面查询（M7 功能 6.6/6.7）：任务详情页与任务列表页的任务级聚合读面。
// 只读——本文件不写任何表；写路径在 push_batches.go / deliveries.go / task_fetch_targets.go。
//
// 归集口径（成本）：推送管道以 workflow 确定性 traceID 同时作为
// push_batches.idempotency_key（CreatePushBatchIdempotent / RecordEmptyPushBatch）与
// llm_calls.trace_id（Score/CardGen/EvolveProfile 等 Activity 记账），
// 故「任务运行 LLM 成本」= 经 idempotency_key ↔ trace_id 关联到该任务批次的调用之和。
// 这**只覆盖推送管道运行**：agent 会话、深挖、A2A 的调用不挂任务批次，不在此口径内；
// 020 之前的历史批次 schedule_id 为 NULL，同样不计入。宁可少算、口径清晰，不编大数。
//
// 抓取侧 tool_calls 的 trace 锚是 Temporal workflow ID。task_run_snapshots 已把该 ID
// 与 exact tenant/user/task 不可变绑定，因此工具成本通过 snapshot EXISTS 精确归集；
// 不按时间、字符串前缀或当前任务手册猜测。没有 snapshot 的 legacy 调用保持未归因。
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// ScheduleBatchItem 任务的一次运行（push_batches 行 + 投递计数）。
// Status/ExitGate 语义见 types.BatchStatus / types.BatchExitGate：status 是结局，
// exit_gate 是空批时“为什么”（跑到 Push 的批次为空串）。
type ScheduleBatchItem struct {
	ID          int64           `json:"id"`
	Status      string          `json:"status"`
	ExitGate    string          `json:"exit_gate"`
	StageCounts json.RawMessage `json:"stage_counts"`
	Deliveries  int64           `json:"deliveries"` // 本批投递行数（含未发成的）
	Sent        int64           `json:"sent"`       // 其中已发送成功的
	CreatedAt   time.Time       `json:"created_at"`
}

// BatchHistoryQuery 是 ListScheduleBatches 的过滤条件（与 DeliveryHistoryQuery 同构）。
type BatchHistoryQuery struct {
	PageSize  int    // <=0 → 20；钳上限 100
	PageToken string // (created_at,id) 键集游标，本包编解码，调用方视为不透明串
}

// ScheduleRunSummary 每任务运行概览：详情页统计行与列表页（6.7）共用一份口径。
// 7 天窗口按 push_batches/deliveries.created_at 计。
type ScheduleRunSummary struct {
	ScheduleID     string     `json:"schedule_id"`
	LastRunAt      *time.Time `json:"last_run_at,omitempty"` // 无批次历史时缺省
	LastStatus     string     `json:"last_status"`           // 最近一批的结局；无批次为空串
	LastExitGate   string     `json:"last_exit_gate"`
	Batches7d      int64      `json:"batches_7d"`
	EmptyBatches7d int64      `json:"empty_batches_7d"`
	SentPushes7d   int64      `json:"sent_pushes_7d"`
}

// ScheduleRunCost 是任务运行的已知成本与最近一次取材调用摘要。
// ToolPricedCalls 小于 ToolCalls 表示部分上游回执没有可信 cost_usd；调用仍计数，
// 已知金额仍保留为下限，但上层不得宣称完整成本。
type ScheduleRunCost struct {
	LLMCostUSD                float64 `json:"llm_cost_usd"`
	LLMCalls                  int64   `json:"llm_calls"`
	ToolCostUSD               float64 `json:"tool_cost_usd"`
	ToolCalls                 int64   `json:"tool_calls"`
	ToolPricedCalls           int64   `json:"tool_priced_calls"`
	LatestAcquisitionCalls    int     `json:"latest_acquisition_calls"`
	LatestAcquisitionFailures int     `json:"latest_acquisition_failures"`
	// Internal low-cardinality fact consumed only by taskhealth sanitization.
	// Never serialize it through the sibling schedule-detail response.
	LatestAcquisitionErrorType string `json:"-"`
}

// ListScheduleBatches 按任务倒序返回运行历史（push_batches + 每批投递计数）。
//
//	排序/翻页 = (created_at,id) 键集游标，与 ListDeliveryHistory 同构
//	total = 该任务的批次总数
//
// 归属校验用 push_batches.user_id 谓词：批次建行时即带归属，无需再 JOIN schedules。
// 传他人任务 id → total=0 空页（不泄露存在性；404 语义由调用方先 GetSchedule 把关）。
func (s *Store) ListScheduleBatches(ctx context.Context, userID int64, scheduleID string, q BatchHistoryQuery) (items []ScheduleBatchItem, total int64, next string, err error) {
	pageSize := clampHistoryPageSize(q.PageSize)

	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM push_batches WHERE user_id = $1 AND schedule_id = $2`,
		userID, scheduleID).Scan(&total); err != nil {
		return nil, 0, "", types.NewAppError(types.CodeDatabase, "统计任务批次总数", err)
	}

	args := []any{userID, scheduleID, types.DeliveryStatusSent}
	cursorCond := ""
	if q.PageToken != "" {
		cursorAt, cursorID, derr := decodeHistoryCursor(q.PageToken)
		if derr != nil {
			return nil, 0, "", derr
		}
		args = append(args, cursorAt, cursorID)
		cursorCond = fmt.Sprintf(" AND (pb.created_at, pb.id) < ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, pageSize)

	rows, err := s.pool.Query(ctx,
		`SELECT pb.id, pb.status, pb.exit_gate, pb.stage_counts, pb.created_at,
		        count(d.id), count(d.id) FILTER (WHERE d.status = $3)
		 FROM push_batches pb
		 LEFT JOIN deliveries d ON d.batch_id = pb.id
		 WHERE pb.user_id = $1 AND pb.schedule_id = $2`+cursorCond+
			fmt.Sprintf(` GROUP BY pb.id, pb.status, pb.exit_gate, pb.stage_counts, pb.created_at
		 ORDER BY pb.created_at DESC, pb.id DESC LIMIT $%d`, len(args)),
		args...)
	if err != nil {
		return nil, 0, "", types.NewAppError(types.CodeDatabase, "查询任务运行历史", err)
	}
	defer rows.Close()

	for rows.Next() {
		var it ScheduleBatchItem
		var stageCounts []byte
		if err := rows.Scan(&it.ID, &it.Status, &it.ExitGate, &stageCounts,
			&it.CreatedAt, &it.Deliveries, &it.Sent); err != nil {
			return nil, 0, "", types.NewAppError(types.CodeDatabase, "扫描任务批次行", err)
		}
		if len(stageCounts) == 0 {
			stageCounts = []byte("{}")
		}
		it.StageCounts = json.RawMessage(stageCounts)
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, "", types.NewAppError(types.CodeDatabase, "遍历任务批次结果集", err)
	}

	if len(items) == pageSize {
		last := items[len(items)-1]
		next = encodeHistoryCursor(last.CreatedAt, last.ID)
	}
	return items, total, next, nil
}

// scheduleRunSummarySQL 是概览查询本体：schedules 锚定归属与成熟谓词，LATERAL 取最近
// 一批，标量子查询各数各的（每用户活跃任务有上限，行数个位数，不值得为它建物化）。
// extraCond 只允许本文件内两个调用方注入 “AND s.id = $4”。
const scheduleRunSummarySQL = `
SELECT s.id,
       lb.created_at, COALESCE(lb.status, ''), COALESCE(lb.exit_gate, ''),
       (SELECT count(*) FROM push_batches pb
         WHERE pb.schedule_id = s.id AND pb.created_at >= now() - interval '7 days'),
       (SELECT count(*) FROM push_batches pb
         WHERE pb.schedule_id = s.id AND pb.status = $2
           AND pb.created_at >= now() - interval '7 days'),
       (SELECT count(*) FROM deliveries d
          JOIN push_batches pb ON pb.id = d.batch_id
         WHERE pb.schedule_id = s.id AND d.status = $3
           AND d.created_at >= now() - interval '7 days')
  FROM schedules s
  LEFT JOIN LATERAL (
       SELECT pb.created_at, pb.status, pb.exit_gate
         FROM push_batches pb
        WHERE pb.schedule_id = s.id
        ORDER BY pb.created_at DESC, pb.id DESC
        LIMIT 1
  ) lb ON true
 WHERE s.user_id = $1
   AND ` + matureSchedulePredicate

func scanScheduleRunSummary(row pgx.Row, sum *ScheduleRunSummary) error {
	return row.Scan(&sum.ScheduleID, &sum.LastRunAt, &sum.LastStatus, &sum.LastExitGate,
		&sum.Batches7d, &sum.EmptyBatches7d, &sum.SentPushes7d)
}

// ListScheduleRunSummaries 返回该用户全部可见任务的运行概览（列表页 6.7 的数据面），
// 与 ListSchedulesByUser 同序（created_at 倒序），前端可按 schedule_id 装配。
func (s *Store) ListScheduleRunSummaries(ctx context.Context, userID int64) ([]ScheduleRunSummary, error) {
	rows, err := s.pool.Query(ctx,
		scheduleRunSummarySQL+` ORDER BY s.created_at DESC`,
		userID, types.BatchStatusEmpty, types.DeliveryStatusSent)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "查询任务运行概览", err)
	}
	defer rows.Close()

	var out []ScheduleRunSummary
	for rows.Next() {
		var sum ScheduleRunSummary
		if err := scanScheduleRunSummary(rows, &sum); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描任务运行概览行", err)
		}
		out = append(out, sum)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历任务运行概览结果集", err)
	}
	return out, nil
}

// GetScheduleRunSummary 返回单任务运行概览（详情页统计行）。
// 不存在或不属于该用户 → CodeNotFound（与 GetSchedule 同口径，不泄露他人任务存在性）。
func (s *Store) GetScheduleRunSummary(ctx context.Context, userID int64, scheduleID string) (*ScheduleRunSummary, error) {
	var sum ScheduleRunSummary
	err := scanScheduleRunSummary(s.pool.QueryRow(ctx,
		scheduleRunSummarySQL+` AND s.id = $4`,
		userID, types.BatchStatusEmpty, types.DeliveryStatusSent, scheduleID), &sum)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("调度 id=%s 不存在", scheduleID), err)
		}
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询任务运行概览（schedule_id=%s）", scheduleID), err)
	}
	return &sum, nil
}

// GetScheduleRunCost 按两个不可变锚归集任务成本：LLM 走 push batch trace，
// acquisition Tool 走 task run snapshot workflow identity。无可归集调用返回全零；
// 他人任务也返回全零，不泄露任务或调用是否存在。
func (s *Store) GetScheduleRunCost(ctx context.Context, userID int64, scheduleID string) (*ScheduleRunCost, error) {
	var out ScheduleRunCost
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(sum(lc.cost_usd), 0), count(*)
		   FROM llm_calls lc
		   JOIN push_batches pb ON pb.idempotency_key = lc.trace_id
		  WHERE pb.user_id = $1 AND pb.schedule_id = $2
		    AND lc.user_id = $1
		    AND lc.tenant_id IS NOT DISTINCT FROM pb.tenant_id
		    AND pb.idempotency_key <> ''`,
		userID, scheduleID).Scan(&out.LLMCostUSD, &out.LLMCalls); err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("归集任务 LLM 成本（schedule_id=%s）", scheduleID), err)
	}
	if err := s.pool.QueryRow(ctx,
		`WITH authorized_task AS (
		     SELECT tenant_id
		       FROM schedules
		      WHERE id = $2 AND user_id = $1
		   )
		 SELECT COALESCE(sum(tc.cost_usd), 0),
		        count(*),
		        count(tc.cost_usd)
		   FROM tool_calls tc
		   JOIN authorized_task task
		     ON tc.tenant_id IS NOT DISTINCT FROM task.tenant_id
		  WHERE tc.user_id = $1
		    AND tc.tool_kind IN ($3, $4)
		    AND EXISTS (
		         SELECT 1
		           FROM task_run_snapshots rs
		          WHERE rs.tenant_id = task.tenant_id
		            AND rs.user_id = $1
		            AND rs.task_id = $2
		            AND rs.temporal_workflow_id = tc.trace_id
		    )`,
		userID, scheduleID,
		types.ToolCallKindBindingFetch, types.ToolCallKindExaFetch,
	).Scan(&out.ToolCostUSD, &out.ToolCalls, &out.ToolPricedCalls); err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("归集任务取材成本（schedule_id=%s）", scheduleID), err)
	}
	if err := s.pool.QueryRow(ctx,
		`WITH authorized_task AS (
		     SELECT tenant_id, user_id, id
		       FROM schedules
		      WHERE id = $2 AND user_id = $1
		   ),
		   latest_run AS (
		     SELECT latest.tenant_id, latest.user_id, latest.task_id,
		            latest.temporal_workflow_id,
		            (
		              SELECT count(*)
		                FROM task_run_snapshots same_workflow
		               WHERE same_workflow.tenant_id = latest.tenant_id
		                 AND same_workflow.user_id = latest.user_id
		                 AND same_workflow.task_id = latest.task_id
		                 AND same_workflow.temporal_workflow_id =
		                     latest.temporal_workflow_id
		            ) AS workflow_run_count
		       FROM task_run_snapshots latest
		       JOIN authorized_task task
		         ON latest.tenant_id = task.tenant_id
		        AND latest.user_id = task.user_id
		        AND latest.task_id = task.id
		      ORDER BY latest.created_at DESC, latest.id DESC
		      LIMIT 1
		   ),
		   latest_calls AS (
		     SELECT tc.error_type
		       FROM tool_calls tc
		       JOIN latest_run run
		         ON tc.tenant_id IS NOT DISTINCT FROM run.tenant_id
		        AND tc.user_id = run.user_id
		        AND tc.trace_id = run.temporal_workflow_id
		      WHERE tc.tool_kind IN ($3, $4)
		        AND run.workflow_run_count = 1
		   )
		 SELECT count(*),
		        count(*) FILTER (WHERE error_type <> ''),
		        COALESCE((
		          SELECT error_type
		            FROM latest_calls
		           WHERE error_type <> ''
		           ORDER BY CASE error_type
		             WHEN 'budget_exceeded' THEN 1
		             WHEN 'invalid_args' THEN 2
		             WHEN 'timeout' THEN 3
		             WHEN 'http_error' THEN 4
		             ELSE 5
		           END
		           LIMIT 1
		        ), '')
		   FROM latest_calls`,
		userID, scheduleID,
		types.ToolCallKindBindingFetch, types.ToolCallKindExaFetch,
	).Scan(
		&out.LatestAcquisitionCalls,
		&out.LatestAcquisitionFailures,
		&out.LatestAcquisitionErrorType,
	); err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("归集最近任务取材状态（schedule_id=%s）", scheduleID), err)
	}
	return &out, nil
}
