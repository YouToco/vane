package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/types"
)

// CompiledRunSnapshotAuthority is the immutable body selected for one exact
// Temporal run. It is diagnostic metadata, not an authorization input.
type CompiledRunSnapshotAuthority string

const (
	CompiledRunSnapshotAuthorityV1 CompiledRunSnapshotAuthority = "retained_v1"
	CompiledRunSnapshotAuthorityV2 CompiledRunSnapshotAuthority = "retained_v2"
)

// LoadAuthoritativeCompiledTaskRunSnapshot selects the body bound to the
// immutable parent marker. A NULL marker always means retained v1, even when a
// 3a audit sidecar exists. A non-NULL marker must materialize one exact v2
// sidecar or fail closed; current schedule/head/latest-event state is never
// consulted, so later edits and rollback cannot flip an in-flight run.
func (s *Store) LoadAuthoritativeCompiledTaskRunSnapshot(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
) (
	runcontext.CompiledSnapshotV1,
	CompiledRunSnapshotAuthority,
	error,
) {
	tx, err := s.beginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return runcontext.CompiledSnapshotV1{}, "",
			taskRunDatabaseError("begin authoritative task run snapshot read", err)
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	_, snapshot, authority, err := loadAuthoritativeCompiledTaskRunSnapshot(
		ctx, tx, expected, ref)
	if err != nil {
		return runcontext.CompiledSnapshotV1{}, "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return runcontext.CompiledSnapshotV1{}, "",
			taskRunDatabaseError("commit authoritative task run snapshot read", err)
	}
	return snapshot, authority, nil
}

func loadAuthoritativeCompiledTaskRunSnapshot(
	ctx context.Context,
	q taskRunSnapshotQueryer,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
) (
	*taskRunSnapshot,
	runcontext.CompiledSnapshotV1,
	CompiledRunSnapshotAuthority,
	error,
) {
	parent, retainedV1, err := loadCompiledTaskRunSnapshotV1(
		ctx, q, expected, ref)
	if err != nil {
		return nil, runcontext.CompiledSnapshotV1{}, "", err
	}
	if parent.V2CutoverEventID == nil {
		return parent, retainedV1, CompiledRunSnapshotAuthorityV1, nil
	}
	if *parent.V2CutoverEventID <= 0 {
		return nil, runcontext.CompiledSnapshotV1{}, "",
			taskRunIntegrityError()
	}

	var (
		eventID                   int64
		eventTenantID             int64
		eventUserID               int64
		eventTaskID               string
		action                    string
		approvedDefinitionVersion int64
		approvedDefinitionDigest  string
		highWatermark             int64
		shadowDefinitionVersion   *int64
		shadowDefinitionDigest    *string
	)
	if err := q.QueryRow(ctx,
		`SELECT e.id, e.tenant_id, e.user_id, e.task_id, e.action,
		        e.approved_definition_version, e.approved_definition_digest,
		        e.snapshot_high_watermark,
		        sh.approved_definition_version, sh.approved_definition_digest
		   FROM task_run_snapshot_v2_cutover_events e
		   LEFT JOIN task_run_snapshot_v2_shadows sh
		     ON sh.run_snapshot_id=$2
		    AND sh.tenant_id=e.tenant_id
		    AND sh.user_id=e.user_id
		    AND sh.task_id=e.task_id
		  WHERE e.id=$1`,
		*parent.V2CutoverEventID, parent.ID,
	).Scan(
		&eventID, &eventTenantID, &eventUserID, &eventTaskID, &action,
		&approvedDefinitionVersion, &approvedDefinitionDigest, &highWatermark,
		&shadowDefinitionVersion, &shadowDefinitionDigest,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, runcontext.CompiledSnapshotV1{}, "",
				taskRunIntegrityError()
		}
		return nil, runcontext.CompiledSnapshotV1{}, "",
			taskRunDatabaseError("load task run snapshot authority marker", err)
	}
	if eventID != *parent.V2CutoverEventID ||
		eventTenantID != parent.TenantID ||
		eventUserID != parent.UserID ||
		eventTaskID != parent.TaskID ||
		action != "activate" ||
		highWatermark <= 0 || parent.ID <= highWatermark ||
		shadowDefinitionVersion == nil ||
		*shadowDefinitionVersion != approvedDefinitionVersion ||
		shadowDefinitionDigest == nil ||
		!constantTimeDigestEqual(
			*shadowDefinitionDigest, approvedDefinitionDigest) {
		return nil, runcontext.CompiledSnapshotV1{}, "",
			taskRunIntegrityError()
	}
	materialized, audit, err := auditCompiledTaskRunSnapshotV2(
		ctx, q, expected, ref)
	if err != nil {
		return nil, runcontext.CompiledSnapshotV1{}, "", err
	}
	if audit.Status != CompiledRunSnapshotV2AuditMatch ||
		audit.ShadowStatus != TaskRunSnapshotShadowMatch ||
		!audit.TypedEqual {
		return nil, runcontext.CompiledSnapshotV1{}, "",
			taskRunIntegrityError()
	}
	return parent, materialized, CompiledRunSnapshotAuthorityV2, nil
}
