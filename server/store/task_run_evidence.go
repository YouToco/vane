package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// TaskRunToolEvidenceV1 is deliberately metadata-only. Tool result bodies can
// contain untrusted page text; the conversational read surface only needs the
// exact acquisition mechanism, status and error evidence to explain a run.
type TaskRunToolEvidenceV1 struct {
	Name         string `json:"name"`
	Kind         string `json:"kind"`
	Provider     string `json:"provider,omitempty"`
	EndpointPath string `json:"endpoint_path,omitempty"`
	HTTPStatus   *int   `json:"http_status,omitempty"`
	DurationMS   int    `json:"duration_ms"`
	ErrorType    string `json:"error_type,omitempty"`
}

type TaskRunPushEvidenceV1 struct {
	Status      string               `json:"status"`
	ExitGate    string               `json:"exit_gate,omitempty"`
	StageCounts types.PipelineCounts `json:"stage_counts"`
	CreatedAt   time.Time            `json:"created_at"`
}

// TaskLatestRunEvidenceV1 is an owner-scoped projection of immutable run facts.
// It never asks the model to infer execution from the task definition.
type TaskLatestRunEvidenceV1 struct {
	RunSnapshotID  int64                   `json:"run_snapshot_id"`
	FinalizedAt    time.Time               `json:"finalized_at"`
	Result         string                  `json:"result"`
	SourceCoverage string                  `json:"source_coverage"`
	Processing     string                  `json:"processing"`
	FailureCode    string                  `json:"failure_code,omitempty"`
	Push           *TaskRunPushEvidenceV1  `json:"push,omitempty"`
	Tools          []TaskRunToolEvidenceV1 `json:"tools"`
}

// GetLatestTaskRunEvidenceV1 returns the newest finalized run for one task.
// GetSchedule performs the same mature-task and owner boundary used by the
// product task list; the subsequent reads retain the exact tenant/user/task
// scope and the immutable snapshot id.
func (s *Store) GetLatestTaskRunEvidenceV1(
	ctx context.Context,
	userID int64,
	taskID string,
) (*TaskLatestRunEvidenceV1, error) {
	sc, err := s.GetSchedule(ctx, taskID, userID)
	if err != nil {
		return nil, err
	}

	var out TaskLatestRunEvidenceV1
	err = s.pool.QueryRow(ctx, `
		SELECT run_snapshot_id, finalized_at, result, source_coverage,
		       processing, failure_code
		  FROM task_run_outcomes
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		   AND status='finalized'
		 ORDER BY finalized_at DESC, id DESC
		 LIMIT 1`, sc.TenantID, userID, taskID).Scan(
		&out.RunSnapshotID, &out.FinalizedAt, &out.Result,
		&out.SourceCoverage, &out.Processing, &out.FailureCode,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			"读取任务最近运行结局", err)
	}

	var push TaskRunPushEvidenceV1
	var stageCounts []byte
	err = s.pool.QueryRow(ctx, `
		SELECT status, exit_gate, stage_counts, created_at
		  FROM push_batches
		 WHERE tenant_id=$1 AND user_id=$2 AND schedule_id=$3
		   AND run_snapshot_id=$4
		 ORDER BY created_at DESC, id DESC
		 LIMIT 1`, sc.TenantID, userID, taskID, out.RunSnapshotID).Scan(
		&push.Status, &push.ExitGate, &stageCounts, &push.CreatedAt,
	)
	if err == nil {
		if len(stageCounts) != 0 {
			if decodeErr := json.Unmarshal(stageCounts, &push.StageCounts); decodeErr != nil {
				return nil, types.NewAppError(types.CodeDatabase,
					"解析任务最近运行漏斗", decodeErr)
			}
		}
		out.Push = &push
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, types.NewAppError(types.CodeDatabase,
			"读取任务最近推送结局", err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT tool_name, tool_kind, provider, endpoint_path, http_status,
		       duration_ms, error_type
		  FROM tool_calls
		 WHERE tenant_id=$1 AND user_id=$2 AND run_snapshot_id=$3
		 ORDER BY created_at, id`, sc.TenantID, userID, out.RunSnapshotID)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			"读取任务最近运行工具", err)
	}
	defer rows.Close()
	for rows.Next() {
		var tool TaskRunToolEvidenceV1
		if err := rows.Scan(
			&tool.Name, &tool.Kind, &tool.Provider, &tool.EndpointPath,
			&tool.HTTPStatus, &tool.DurationMS, &tool.ErrorType,
		); err != nil {
			return nil, types.NewAppError(types.CodeDatabase,
				"扫描任务最近运行工具", err)
		}
		out.Tools = append(out.Tools, tool)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			"遍历任务最近运行工具", err)
	}
	if out.Tools == nil {
		out.Tools = []TaskRunToolEvidenceV1{}
	}
	return &out, nil
}
