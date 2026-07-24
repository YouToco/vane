package store

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

const maxTaskRunSnapshotShadowAuditPageSize = 1000

type TaskRunSnapshotShadowAuditItem struct {
	TaskID              string                      `json:"task_id"`
	SnapshotID          int64                       `json:"snapshot_id"`
	V1PayloadDigest     string                      `json:"v1_payload_digest"`
	ShadowPayloadDigest string                      `json:"shadow_payload_digest,omitempty"`
	Status              TaskRunSnapshotShadowStatus `json:"status"`
}

type TaskRunSnapshotShadowAuditPage struct {
	Items []TaskRunSnapshotShadowAuditItem `json:"items"`
	Next  *int64                           `json:"next,omitempty"`
}

// AuditTaskRunSnapshotShadowsV2 strictly verifies one canary's v1 rows and
// sidecars without returning any persisted raw payload.
func (s *Store) AuditTaskRunSnapshotShadowsV2(
	ctx context.Context,
	taskID string,
	since time.Time,
	afterID int64,
	limit int,
) (TaskRunSnapshotShadowAuditPage, error) {
	if !validTaskRunTaskID(taskID) || since.IsZero() || afterID < 0 ||
		limit <= 0 || limit > maxTaskRunSnapshotShadowAuditPageSize {
		return TaskRunSnapshotShadowAuditPage{},
			taskRunValidationError("task run snapshot v2 audit request is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return TaskRunSnapshotShadowAuditPage{},
			taskRunDatabaseError("begin task run snapshot v2 audit", err)
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	rows, err := tx.Query(ctx,
		`SELECT id, tenant_id, user_id, task_id, temporal_workflow_id, temporal_run_id
		   FROM task_run_snapshots
		  WHERE task_id=$1 AND created_at >= $2 AND id > $3
		  ORDER BY id
		  LIMIT $4`,
		taskID, since, afterID, limit+1)
	if err != nil {
		return TaskRunSnapshotShadowAuditPage{},
			taskRunDatabaseError("list task run snapshot v2 audit page", err)
	}
	defer rows.Close()
	type scope struct {
		id, tenantID, userID      int64
		taskID, workflowID, runID string
	}
	scopes := make([]scope, 0, limit+1)
	for rows.Next() {
		var item scope
		if err := rows.Scan(&item.id, &item.tenantID, &item.userID,
			&item.taskID, &item.workflowID, &item.runID); err != nil {
			return TaskRunSnapshotShadowAuditPage{},
				taskRunDatabaseError("scan task run snapshot v2 audit scope", err)
		}
		scopes = append(scopes, item)
	}
	if err := rows.Err(); err != nil {
		return TaskRunSnapshotShadowAuditPage{},
			taskRunDatabaseError("iterate task run snapshot v2 audit scopes", err)
	}
	hasMore := len(scopes) > limit
	if hasMore {
		scopes = scopes[:limit]
	}
	page := TaskRunSnapshotShadowAuditPage{
		Items: make([]TaskRunSnapshotShadowAuditItem, 0, len(scopes)),
	}
	for _, item := range scopes {
		lookup := CreateOrGetTaskRunSnapshotParams{
			TenantID: item.tenantID, UserID: item.userID, TaskID: item.taskID,
			TemporalWorkflowID: item.workflowID, TemporalRunID: item.runID,
		}
		parent, found, err := loadTaskRunSnapshot(ctx, tx, lookup)
		if err != nil || !found {
			if err == nil {
				err = taskRunIntegrityError()
			}
			return TaskRunSnapshotShadowAuditPage{}, err
		}
		status, shadowDigest, shadowFound, err :=
			loadAndValidateTaskRunSnapshotShadowAuditV2(ctx, tx, parent)
		if err != nil {
			return TaskRunSnapshotShadowAuditPage{}, err
		}
		if !shadowFound {
			status = "missing"
		}
		page.Items = append(page.Items, TaskRunSnapshotShadowAuditItem{
			TaskID: parent.TaskID, SnapshotID: parent.ID,
			V1PayloadDigest:     parent.PayloadDigest,
			ShadowPayloadDigest: shadowDigest,
			Status:              status,
		})
	}
	if hasMore {
		next := scopes[len(scopes)-1].id
		page.Next = &next
	}
	if err := tx.Commit(ctx); err != nil {
		return TaskRunSnapshotShadowAuditPage{},
			taskRunDatabaseError("commit task run snapshot v2 audit", err)
	}
	return page, nil
}

type taskRunSnapshotShadowAuditQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func loadAndValidateTaskRunSnapshotShadowAuditV2(
	ctx context.Context,
	q taskRunSnapshotShadowAuditQueryer,
	parent *taskRunSnapshot,
) (TaskRunSnapshotShadowStatus, string, bool, error) {
	var status string
	var payload []byte
	var payloadDigest string
	err := q.QueryRow(ctx,
		`SELECT status, payload, payload_digest
		   FROM task_run_snapshot_v2_shadows
		  WHERE run_snapshot_id=$1`,
		parent.ID).Scan(&status, &payload, &payloadDigest)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, taskRunDatabaseError(
			"load task run snapshot v2 audit row", err)
	}
	decoded, canonical, err := readTaskRunSnapshotShadowPayloadV2(payload)
	if err != nil || !constantTimeDigestEqual(sha256Hex(payload), payloadDigest) ||
		!bytes.Equal(canonical, payload) ||
		status != string(decoded.Status) ||
		decoded.Legacy.SnapshotID != parent.ID ||
		decoded.Legacy.PayloadDigest != parent.PayloadDigest ||
		!bytes.Equal(decoded.Legacy.Payload, parent.Payload) {
		return "", "", false, taskRunIntegrityError()
	}
	return decoded.Status, payloadDigest, true, nil
}
