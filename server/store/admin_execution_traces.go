package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// AdminTraceUser is a platform-owner navigation item. TenantID and UserID are
// both required because a user may have memberships in more than one tenant.
type AdminTraceUser struct {
	TenantID  int64  `json:"tenant_id"`
	UserID    int64  `json:"user_id"`
	Name      string `json:"name"`
	Email     string `json:"email"`
	TaskCount int64  `json:"task_count"`
}

type AdminTraceTask struct {
	TaskID    string     `json:"task_id"`
	Title     string     `json:"title"`
	Status    string     `json:"status"`
	RunCount  int64      `json:"run_count"`
	LastRunAt *time.Time `json:"last_run_at,omitempty"`
}

type AdminTraceRun struct {
	SnapshotID     int64      `json:"snapshot_id"`
	SchemaVersion  string     `json:"schema_version"`
	Status         string     `json:"status"`
	Result         string     `json:"result"`
	SourceCoverage string     `json:"source_coverage"`
	Processing     string     `json:"processing"`
	FailureCode    string     `json:"failure_code"`
	FailureMessage string     `json:"failure_message"`
	CreatedAt      time.Time  `json:"created_at"`
	FinalizedAt    *time.Time `json:"finalized_at,omitempty"`
	ModelCalls     int64      `json:"model_calls"`
	ToolCalls      int64      `json:"tool_calls"`
}

// AdminTraceEvent is the merged chronological wire shape. Model-only and
// tool-only fields remain omitted for the other kind so the UI can render a
// professional typed timeline without exposing an unstructured database row.
type AdminTraceEvent struct {
	Kind      string    `json:"kind"`
	CreatedAt time.Time `json:"created_at"`

	SpanName         string   `json:"span_name,omitempty"`
	Provider         string   `json:"provider,omitempty"`
	Model            string   `json:"model,omitempty"`
	SystemPrompt     string   `json:"system_prompt,omitempty"`
	UserPrompt       string   `json:"user_prompt,omitempty"`
	Completion       string   `json:"completion,omitempty"`
	PromptTokens     int      `json:"prompt_tokens,omitempty"`
	CompletionTokens int      `json:"completion_tokens,omitempty"`
	LatencyMS        int      `json:"latency_ms,omitempty"`
	Temperature      *float32 `json:"temperature,omitempty"`
	MaxTokens        *int     `json:"max_tokens,omitempty"`

	ToolName        string          `json:"tool_name,omitempty"`
	ToolKind        string          `json:"tool_kind,omitempty"`
	EndpointPath    string          `json:"endpoint_path,omitempty"`
	Arguments       json.RawMessage `json:"arguments,omitempty"`
	ResultPreview   string          `json:"result_preview,omitempty"`
	ResultSize      int             `json:"result_size,omitempty"`
	ResultTruncated bool            `json:"result_truncated,omitempty"`
	HTTPStatus      *int            `json:"http_status,omitempty"`
	DurationMS      int             `json:"duration_ms,omitempty"`
	ErrorType       string          `json:"error_type,omitempty"`

	Error         string   `json:"error,omitempty"`
	PricingStatus string   `json:"pricing_status,omitempty"`
	CostAmount    *float64 `json:"cost_amount,omitempty"`
	CostCurrency  string   `json:"cost_currency,omitempty"`
}

type AdminExecutionTrace struct {
	Run    AdminTraceRun     `json:"run"`
	Events []AdminTraceEvent `json:"events"`
}

func (s *Store) ListAdminTraceUsers(ctx context.Context) ([]AdminTraceUser, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT m.tenant_id, u.id, u.name, COALESCE(u.email, ''),
		       count(DISTINCT sc.id)
		  FROM memberships m
		  JOIN users u ON u.id = m.user_id
		  LEFT JOIN schedules sc
		    ON sc.tenant_id = m.tenant_id AND sc.user_id = m.user_id
		 GROUP BY m.tenant_id, u.id, u.name, u.email
		 ORDER BY count(DISTINCT sc.id) DESC, u.name, u.id, m.tenant_id`)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "查询执行轨迹用户", err)
	}
	defer rows.Close()

	out := make([]AdminTraceUser, 0)
	for rows.Next() {
		var item AdminTraceUser
		if err := rows.Scan(&item.TenantID, &item.UserID, &item.Name,
			&item.Email, &item.TaskCount); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描执行轨迹用户", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历执行轨迹用户", err)
	}
	return out, nil
}

func (s *Store) ListAdminTraceTasks(
	ctx context.Context, tenantID, userID int64,
) ([]AdminTraceTask, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT sc.id, sc.nl_description, sc.status,
		       count(rs.id), max(rs.created_at)
		  FROM schedules sc
		  JOIN memberships m
		    ON m.tenant_id = sc.tenant_id AND m.user_id = sc.user_id
		  LEFT JOIN task_run_snapshots rs
		    ON rs.tenant_id = sc.tenant_id
		   AND rs.user_id = sc.user_id
		   AND rs.task_id = sc.id
		 WHERE sc.tenant_id = $1 AND sc.user_id = $2
		 GROUP BY sc.id, sc.nl_description, sc.status, sc.created_at
		 ORDER BY max(rs.created_at) DESC NULLS LAST, sc.created_at DESC`,
		tenantID, userID)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "查询执行轨迹任务", err)
	}
	defer rows.Close()

	out := make([]AdminTraceTask, 0)
	for rows.Next() {
		var item AdminTraceTask
		if err := rows.Scan(&item.TaskID, &item.Title, &item.Status,
			&item.RunCount, &item.LastRunAt); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描执行轨迹任务", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历执行轨迹任务", err)
	}
	return out, nil
}

func (s *Store) ListAdminTraceRuns(
	ctx context.Context, tenantID, userID int64, taskID string, limit int,
) ([]AdminTraceRun, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT rs.id, COALESCE(o.schema_version, ''),
		       COALESCE(o.status, 'pending'), COALESCE(o.result, ''),
		       COALESCE(o.source_coverage, ''), COALESCE(o.processing, ''),
		       COALESCE(o.failure_code, ''), COALESCE(o.failure_message, ''),
		       rs.created_at, o.finalized_at,
		       (
		         SELECT count(*)
		           FROM llm_calls lc
		          WHERE lc.tenant_id = rs.tenant_id
		            AND lc.user_id = rs.user_id
		            AND (
		              lc.run_snapshot_id = rs.id
		              OR (lc.run_snapshot_id IS NULL AND EXISTS (
		              SELECT 1 FROM push_batches pb
		               WHERE pb.tenant_id = rs.tenant_id
		                 AND pb.user_id = rs.user_id
		                 AND pb.schedule_id = rs.task_id
		                 AND pb.run_snapshot_id = rs.id
		                 AND pb.idempotency_key <> ''
		                 AND pb.idempotency_key = lc.trace_id
		              ))
		            )
		       ),
		       (
		         SELECT count(*)
		           FROM tool_calls tc
		          WHERE tc.tenant_id = rs.tenant_id
		            AND tc.user_id = rs.user_id
		            AND (
		              tc.run_snapshot_id = rs.id
		              OR (
		                tc.run_snapshot_id IS NULL
		                AND tc.trace_id = rs.temporal_workflow_id
		                AND 1 = (
		                  SELECT count(*) FROM task_run_snapshots same_run
		                   WHERE same_run.tenant_id = rs.tenant_id
		                     AND same_run.user_id = rs.user_id
		                     AND same_run.temporal_workflow_id =
		                         rs.temporal_workflow_id
		                )
		              )
		            )
		       )
		  FROM task_run_snapshots rs
		  LEFT JOIN task_run_outcomes o
		    ON o.tenant_id = rs.tenant_id
		   AND o.user_id = rs.user_id
		   AND o.task_id = rs.task_id
		   AND o.run_snapshot_id = rs.id
		 WHERE rs.tenant_id = $1 AND rs.user_id = $2 AND rs.task_id = $3
		 ORDER BY rs.created_at DESC, rs.id DESC
		 LIMIT $4`, tenantID, userID, taskID, limit)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "查询执行轨迹运行", err)
	}
	defer rows.Close()

	out := make([]AdminTraceRun, 0)
	for rows.Next() {
		var item AdminTraceRun
		if err := scanAdminTraceRun(rows, &item); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描执行轨迹运行", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历执行轨迹运行", err)
	}
	return out, nil
}

func scanAdminTraceRun(row pgx.Row, out *AdminTraceRun) error {
	return row.Scan(
		&out.SnapshotID, &out.SchemaVersion, &out.Status, &out.Result,
		&out.SourceCoverage, &out.Processing, &out.FailureCode,
		&out.FailureMessage, &out.CreatedAt, &out.FinalizedAt,
		&out.ModelCalls, &out.ToolCalls,
	)
}

// GetAdminExecutionTrace reads the exact immutable run and appends the access
// audit in one transaction. Callers must not return any accumulated response
// until this method has committed successfully.
func (s *Store) GetAdminExecutionTrace(
	ctx context.Context,
	actorTenantID, actorUserID, targetTenantID, targetUserID int64,
	taskID string,
	snapshotID int64,
) (*AdminExecutionTrace, error) {
	tx, err := s.beginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	})
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "开始执行轨迹审计事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var out AdminExecutionTrace
	var workflowID string
	err = tx.QueryRow(ctx, `
		SELECT rs.id, COALESCE(o.schema_version, ''),
		       COALESCE(o.status, 'pending'), COALESCE(o.result, ''),
		       COALESCE(o.source_coverage, ''), COALESCE(o.processing, ''),
		       COALESCE(o.failure_code, ''), COALESCE(o.failure_message, ''),
		       rs.created_at, o.finalized_at,
		       (
		         SELECT count(*)
		           FROM llm_calls lc
		          WHERE lc.tenant_id = rs.tenant_id
		            AND lc.user_id = rs.user_id
		            AND (
		              lc.run_snapshot_id = rs.id
		              OR (lc.run_snapshot_id IS NULL AND EXISTS (
		              SELECT 1 FROM push_batches pb
		               WHERE pb.tenant_id = rs.tenant_id
		                 AND pb.user_id = rs.user_id
		                 AND pb.schedule_id = rs.task_id
		                 AND pb.run_snapshot_id = rs.id
		                 AND pb.idempotency_key <> ''
		                 AND pb.idempotency_key = lc.trace_id
		              ))
		            )
		       ),
		       (
		         SELECT count(*) FROM tool_calls tc
		          WHERE tc.tenant_id = rs.tenant_id
		            AND tc.user_id = rs.user_id
		            AND (
		              tc.run_snapshot_id = rs.id
		              OR (
		                tc.run_snapshot_id IS NULL
		                AND tc.trace_id = rs.temporal_workflow_id
		                AND 1 = (
		                  SELECT count(*) FROM task_run_snapshots same_run
		                   WHERE same_run.tenant_id = rs.tenant_id
		                     AND same_run.user_id = rs.user_id
		                     AND same_run.temporal_workflow_id =
		                         rs.temporal_workflow_id
		                )
		              )
		            )
		       ),
		       rs.temporal_workflow_id
		  FROM task_run_snapshots rs
		  JOIN schedules sc
		    ON sc.tenant_id = rs.tenant_id
		   AND sc.user_id = rs.user_id
		   AND sc.id = rs.task_id
		  JOIN memberships m
		    ON m.tenant_id = rs.tenant_id AND m.user_id = rs.user_id
		  LEFT JOIN task_run_outcomes o
		    ON o.tenant_id = rs.tenant_id
		   AND o.user_id = rs.user_id
		   AND o.task_id = rs.task_id
		   AND o.run_snapshot_id = rs.id
		 WHERE rs.tenant_id = $1 AND rs.user_id = $2
		   AND rs.task_id = $3 AND rs.id = $4`,
		targetTenantID, targetUserID, taskID, snapshotID,
	).Scan(
		&out.Run.SnapshotID, &out.Run.SchemaVersion, &out.Run.Status,
		&out.Run.Result, &out.Run.SourceCoverage, &out.Run.Processing,
		&out.Run.FailureCode, &out.Run.FailureMessage, &out.Run.CreatedAt,
		&out.Run.FinalizedAt, &out.Run.ModelCalls, &out.Run.ToolCalls,
		&workflowID,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound, "运行轨迹不存在", err)
		}
		return nil, types.NewAppError(types.CodeDatabase, "读取运行轨迹身份", err)
	}

	modelRows, err := tx.Query(ctx, `
		SELECT lc.span_name, lc.provider, lc.model,
		       lc.system_prompt, lc.user_prompt, lc.completion,
		       lc.prompt_tokens, lc.completion_tokens, lc.latency_ms,
		       lc.temperature, lc.max_tokens, lc.error, lc.created_at,
		       lc.pricing_status, lc.cost_amount::double precision,
		       COALESCE(lc.cost_currency, '')
		  FROM llm_calls lc
		 WHERE lc.tenant_id = $1 AND lc.user_id = $2
		   AND (
		     lc.run_snapshot_id = $4
		     OR (lc.run_snapshot_id IS NULL AND EXISTS (
		     SELECT 1 FROM push_batches pb
		      WHERE pb.tenant_id = $1 AND pb.user_id = $2
		        AND pb.schedule_id = $3 AND pb.run_snapshot_id = $4
		        AND pb.idempotency_key <> ''
		        AND pb.idempotency_key = lc.trace_id
		     ))
		   )
		 ORDER BY lc.created_at, lc.id`,
		targetTenantID, targetUserID, taskID, snapshotID)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "读取运行模型调用", err)
	}
	for modelRows.Next() {
		event := AdminTraceEvent{Kind: "model"}
		if err := modelRows.Scan(
			&event.SpanName, &event.Provider, &event.Model,
			&event.SystemPrompt, &event.UserPrompt, &event.Completion,
			&event.PromptTokens, &event.CompletionTokens, &event.LatencyMS,
			&event.Temperature, &event.MaxTokens, &event.Error, &event.CreatedAt,
			&event.PricingStatus, &event.CostAmount, &event.CostCurrency,
		); err != nil {
			modelRows.Close()
			return nil, types.NewAppError(types.CodeDatabase, "扫描运行模型调用", err)
		}
		out.Events = append(out.Events, event)
	}
	if err := modelRows.Err(); err != nil {
		modelRows.Close()
		return nil, types.NewAppError(types.CodeDatabase, "遍历运行模型调用", err)
	}
	modelRows.Close()

	toolRows, err := tx.Query(ctx, `
		SELECT tc.tool_name, tc.tool_kind, tc.provider, tc.endpoint_path,
		       COALESCE(tc.arguments, '{}'::jsonb), tc.result_preview,
		       tc.result_size, tc.http_status, tc.duration_ms,
		       tc.error_type, tc.error, tc.created_at, tc.pricing_status,
		       tc.cost_amount::double precision, COALESCE(tc.cost_currency, '')
		  FROM tool_calls tc
		 WHERE tc.tenant_id = $1 AND tc.user_id = $2
		   AND (
		     tc.run_snapshot_id = $4
		     OR (
		       tc.run_snapshot_id IS NULL AND tc.trace_id = $3
		       AND 1 = (
		         SELECT count(*) FROM task_run_snapshots same_run
		          WHERE same_run.tenant_id = $1 AND same_run.user_id = $2
		            AND same_run.temporal_workflow_id = $3
		       )
		     )
		   )
		 ORDER BY tc.created_at, tc.id`,
		targetTenantID, targetUserID, workflowID, snapshotID)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "读取运行工具调用", err)
	}
	for toolRows.Next() {
		event := AdminTraceEvent{Kind: "tool"}
		var args []byte
		if err := toolRows.Scan(
			&event.ToolName, &event.ToolKind, &event.Provider, &event.EndpointPath,
			&args, &event.ResultPreview, &event.ResultSize, &event.HTTPStatus,
			&event.DurationMS, &event.ErrorType, &event.Error, &event.CreatedAt,
			&event.PricingStatus, &event.CostAmount, &event.CostCurrency,
		); err != nil {
			toolRows.Close()
			return nil, types.NewAppError(types.CodeDatabase, "扫描运行工具调用", err)
		}
		event.Arguments = json.RawMessage(args)
		event.ResultTruncated = event.ResultSize > len([]byte(event.ResultPreview))
		out.Events = append(out.Events, event)
	}
	if err := toolRows.Err(); err != nil {
		toolRows.Close()
		return nil, types.NewAppError(types.CodeDatabase, "遍历运行工具调用", err)
	}
	toolRows.Close()

	sort.SliceStable(out.Events, func(i, j int) bool {
		return out.Events[i].CreatedAt.Before(out.Events[j].CreatedAt)
	})

	tag, err := tx.Exec(ctx, `
		INSERT INTO admin_trace_access_events (
		    actor_tenant_id, actor_user_id, target_tenant_id, target_user_id,
		    task_id, run_snapshot_id
		) VALUES ($1,$2,$3,$4,$5,$6)`,
		actorTenantID, actorUserID, targetTenantID, targetUserID,
		taskID, snapshotID)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "记录执行轨迹访问审计", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, types.NewAppError(types.CodeDatabase,
			"记录执行轨迹访问审计", fmt.Errorf("inserted %d rows", tag.RowsAffected()))
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "提交执行轨迹访问审计", err)
	}
	return &out, nil
}
