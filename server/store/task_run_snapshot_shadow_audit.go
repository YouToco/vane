package store

import (
	"bytes"
	"context"
	"errors"
	"math"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

const maxTaskRunSnapshotShadowAuditPageSize = 1000

type TaskRunSnapshotShadowAuditItem struct {
	TaskID              string                           `json:"task_id"`
	SnapshotID          int64                            `json:"snapshot_id"`
	V1PayloadDigest     string                           `json:"v1_payload_digest"`
	ShadowPayloadDigest string                           `json:"shadow_payload_digest,omitempty"`
	Status              TaskRunSnapshotShadowStatus      `json:"status"`
	TypedAuditStatus    CompiledRunSnapshotV2AuditStatus `json:"typed_audit_status"`
	TypedEqual          bool                             `json:"typed_equal"`
}

type TaskRunSnapshotShadowAuditPage struct {
	Items []TaskRunSnapshotShadowAuditItem `json:"items"`
	Next  *int64                           `json:"next,omitempty"`
}

type TaskRunSnapshotShadowAuditScope struct {
	ThroughID int64 `json:"through_id"`
	Count     int64 `json:"count"`
}

// FreezeTaskRunSnapshotShadowAuditScope takes one repeatable-read observation
// of the exact task's upper id and row count. A later page scan uses ThroughID
// as its immutable ceiling; rows created after this call cannot enter the
// operator Gate, while Count detects deletion or skipped-prefix drift.
func (s *Store) FreezeTaskRunSnapshotShadowAuditScope(
	ctx context.Context,
	taskID string,
	since time.Time,
) (TaskRunSnapshotShadowAuditScope, error) {
	if !validTaskRunTaskID(taskID) || since.IsZero() {
		return TaskRunSnapshotShadowAuditScope{},
			taskRunValidationError("task run snapshot v2 audit scope is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return TaskRunSnapshotShadowAuditScope{},
			taskRunDatabaseError("begin task run snapshot v2 audit scope", err)
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	var scope TaskRunSnapshotShadowAuditScope
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(id), 0), COUNT(*)
		   FROM task_run_snapshots
		  WHERE task_id=$1 AND created_at >= $2
		    AND reference_schema_version=$3`,
		taskID, since, taskRunReferenceSchemaVersionV1,
	).Scan(&scope.ThroughID, &scope.Count); err != nil {
		return TaskRunSnapshotShadowAuditScope{},
			taskRunDatabaseError("freeze task run snapshot v2 audit scope", err)
	}
	if (scope.Count == 0) != (scope.ThroughID == 0) ||
		scope.Count < 0 || scope.ThroughID < 0 {
		return TaskRunSnapshotShadowAuditScope{}, taskRunIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return TaskRunSnapshotShadowAuditScope{},
			taskRunDatabaseError("commit task run snapshot v2 audit scope", err)
	}
	return scope, nil
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
	return s.auditTaskRunSnapshotShadowsV2(
		ctx, taskID, since, afterID, math.MaxInt64, limit, false)
}

// AuditTaskRunSnapshotShadowsV2Through freezes the upper snapshot boundary so
// a strict operator Gate cannot silently admit rows created between pages.
func (s *Store) AuditTaskRunSnapshotShadowsV2Through(
	ctx context.Context,
	taskID string,
	since time.Time,
	afterID int64,
	throughID int64,
	limit int,
) (TaskRunSnapshotShadowAuditPage, error) {
	return s.auditTaskRunSnapshotShadowsV2(
		ctx, taskID, since, afterID, throughID, limit, true)
}

func (s *Store) auditTaskRunSnapshotShadowsV2(
	ctx context.Context,
	taskID string,
	since time.Time,
	afterID int64,
	throughID int64,
	limit int,
	typed bool,
) (TaskRunSnapshotShadowAuditPage, error) {
	if !validTaskRunTaskID(taskID) || since.IsZero() || afterID < 0 ||
		throughID <= 0 || afterID >= throughID ||
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
		  WHERE task_id=$1 AND created_at >= $2 AND id > $3 AND id <= $4
		    AND reference_schema_version=$6
		  ORDER BY id
		  LIMIT $5`,
		taskID, since, afterID, throughID, limit+1,
		taskRunReferenceSchemaVersionV1)
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
		auditItem := TaskRunSnapshotShadowAuditItem{
			TaskID: parent.TaskID, SnapshotID: parent.ID,
			V1PayloadDigest: parent.PayloadDigest,
		}
		if typed {
			ref, err := parent.safeRef()
			if err != nil {
				return TaskRunSnapshotShadowAuditPage{}, taskRunIntegrityError()
			}
			expected := types.RunIdentity{
				TemporalWorkflowID: parent.TemporalWorkflowID,
				TemporalRunID:      parent.TemporalRunID,
				RunKind:            types.RunSnapshotKindScheduled,
				TenantID:           parent.TenantID,
				UserID:             parent.UserID,
				TaskID:             parent.TaskID,
			}
			_, audit, err := auditCompiledTaskRunSnapshotV2(ctx, tx, expected, ref)
			if err != nil {
				return TaskRunSnapshotShadowAuditPage{}, err
			}
			auditItem.Status = audit.ShadowStatus
			if audit.Status == CompiledRunSnapshotV2AuditMissing {
				auditItem.Status = "missing"
			}
			auditItem.ShadowPayloadDigest = audit.ShadowPayloadDigest
			auditItem.TypedAuditStatus = audit.Status
			auditItem.TypedEqual = audit.TypedEqual
		} else {
			status, shadowDigest, shadowFound, err :=
				loadAndValidateTaskRunSnapshotShadowAuditV2(ctx, tx, parent)
			if err != nil {
				return TaskRunSnapshotShadowAuditPage{}, err
			}
			auditItem.Status = status
			if !shadowFound {
				auditItem.Status = "missing"
			}
			auditItem.ShadowPayloadDigest = shadowDigest
		}
		page.Items = append(page.Items, auditItem)
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
