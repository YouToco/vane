package store

import (
	"bytes"
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

type TaskRunSnapshotShadowStatus string

const (
	TaskRunSnapshotShadowMatch                 TaskRunSnapshotShadowStatus = "match"
	TaskRunSnapshotShadowLegacyCompatible      TaskRunSnapshotShadowStatus = "legacy_compatible"
	TaskRunSnapshotShadowHeadless              TaskRunSnapshotShadowStatus = "headless"
	TaskRunSnapshotShadowProjectionMismatch    TaskRunSnapshotShadowStatus = "projection_mismatch"
	TaskRunSnapshotShadowAdaptivePresent       TaskRunSnapshotShadowStatus = "adaptive_present"
	TaskRunSnapshotShadowAdaptiveBasisMismatch TaskRunSnapshotShadowStatus = "adaptive_basis_mismatch"
	TaskRunSnapshotShadowAdaptiveForLegacy     TaskRunSnapshotShadowStatus = "adaptive_for_legacy"
)

func (s TaskRunSnapshotShadowStatus) valid() bool {
	switch s {
	case TaskRunSnapshotShadowMatch,
		TaskRunSnapshotShadowLegacyCompatible,
		TaskRunSnapshotShadowHeadless,
		TaskRunSnapshotShadowProjectionMismatch,
		TaskRunSnapshotShadowAdaptivePresent,
		TaskRunSnapshotShadowAdaptiveBasisMismatch,
		TaskRunSnapshotShadowAdaptiveForLegacy:
		return true
	default:
		return false
	}
}

type taskRunSnapshotShadowV2 struct {
	RunSnapshotID             int64
	TenantID                  int64
	UserID                    int64
	TaskID                    string
	TemporalWorkflowID        string
	TemporalRunID             string
	Status                    TaskRunSnapshotShadowStatus
	ApprovedDefinitionVersion *int64
	ApprovedDefinitionDigest  *string
	AdaptiveVersion           int64
	AdaptiveDigest            *string
	Payload                   []byte
	PayloadDigest             string
}

func buildTaskRunSnapshotShadowV2(
	ctx context.Context,
	tx pgx.Tx,
	snapshot *taskRunSnapshot,
) (taskRunSnapshotShadowV2, error) {
	decoded, err := readTaskRunSnapshotPayload(snapshot.Payload)
	if err != nil || decoded.Payload == nil || decoded.Payload.Definition == nil {
		return taskRunSnapshotShadowV2{}, taskRunIntegrityError()
	}
	var rawMode string
	var headVersion *int64
	var headDigest *string
	if err := tx.QueryRow(ctx,
		`SELECT execution_mode, approved_definition_version, approved_definition_digest
		   FROM schedules
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		snapshot.TenantID, snapshot.UserID, snapshot.TaskID,
	).Scan(&rawMode, &headVersion, &headDigest); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return taskRunSnapshotShadowV2{}, taskRunNotFound()
		}
		return taskRunSnapshotShadowV2{},
			taskRunDatabaseError("load task run snapshot v2 head", err)
	}
	mode, err := types.ParseExecutionMode(rawMode)
	if err != nil || (headVersion == nil) != (headDigest == nil) {
		return taskRunSnapshotShadowV2{}, taskRunIntegrityError()
	}

	payload := taskRunSnapshotShadowPayloadV2{
		SchemaVersion: taskRunSnapshotShadowPayloadSchemaV2,
		Identity: taskRunSnapshotShadowIdentityV2{
			TenantID: snapshot.TenantID, UserID: snapshot.UserID, TaskID: snapshot.TaskID,
			TemporalWorkflowID: snapshot.TemporalWorkflowID,
			TemporalRunID:      snapshot.TemporalRunID,
		},
		Legacy: taskRunSnapshotShadowLegacyV2{
			SnapshotID: snapshot.ID, PayloadDigest: snapshot.PayloadDigest,
			Payload: bytes.Clone(snapshot.Payload),
		},
	}
	shadow := taskRunSnapshotShadowV2{
		RunSnapshotID: snapshot.ID, TenantID: snapshot.TenantID,
		UserID: snapshot.UserID, TaskID: snapshot.TaskID,
		TemporalWorkflowID: snapshot.TemporalWorkflowID,
		TemporalRunID:      snapshot.TemporalRunID,
	}
	if headVersion == nil {
		payload.Status = TaskRunSnapshotShadowHeadless
		shadow.Status = payload.Status
		return sealTaskRunSnapshotShadowV2(shadow, payload)
	}
	approved, err := loadApprovedDefinitionVersionTx(ctx, tx,
		snapshot.TenantID, snapshot.UserID, snapshot.TaskID, *headVersion)
	if err != nil {
		return taskRunSnapshotShadowV2{}, err
	}
	if !constantTimeTaskStateDigestEqual(approved.Digest, *headDigest) ||
		approved.Definition.ExecutionMode != mode {
		return taskRunSnapshotShadowV2{}, taskRunIntegrityError()
	}
	payload.Approved = &taskRunSnapshotShadowApprovedV2{
		Version: approved.Version, Digest: approved.Digest,
		SchemaVersion: approved.Definition.SchemaVersion,
		Payload:       bytes.Clone(approved.Payload),
	}
	shadow.ApprovedDefinitionVersion = cloneInt64Pointer(headVersion)
	shadow.ApprovedDefinitionDigest = cloneStringPointer(headDigest)

	adaptive, found, err := loadAdaptiveStateTx(ctx, tx,
		snapshot.TenantID, snapshot.UserID, snapshot.TaskID, false)
	if err != nil {
		return taskRunSnapshotShadowV2{}, err
	}
	if found {
		payload.Adaptive = &taskRunSnapshotShadowAdaptiveV2{
			Version: adaptive.Version, Digest: adaptive.Digest,
			SchemaVersion:          adaptive.State.SchemaVersion,
			Payload:                bytes.Clone(adaptive.Payload),
			BasisDefinitionVersion: adaptive.BasisDefinitionVersion,
			BasisDefinitionDigest:  adaptive.BasisDefinitionDigest,
			LastKnownGoodDefinitionVersion: cloneOptionalVersion(
				adaptive.LastKnownGoodDefinitionVersion),
		}
		shadow.AdaptiveVersion = adaptive.Version
		shadow.AdaptiveDigest = cloneStringValue(adaptive.Digest)
	}

	status := compareTaskRunSnapshotShadowV2(decoded.Payload, approved.Definition)
	if found && (adaptive.BasisDefinitionVersion != approved.Version ||
		!constantTimeTaskStateDigestEqual(
			adaptive.BasisDefinitionDigest, approved.Digest)) {
		status = TaskRunSnapshotShadowAdaptiveBasisMismatch
	} else if found &&
		approved.Definition.SourceScope == taskstate.SourceScopeLegacySubscriptions {
		status = TaskRunSnapshotShadowAdaptiveForLegacy
	} else if found {
		status = TaskRunSnapshotShadowAdaptivePresent
	}
	payload.Status = status
	shadow.Status = status
	return sealTaskRunSnapshotShadowV2(shadow, payload)
}

func compareTaskRunSnapshotShadowV2(
	legacy *taskRunSnapshotPayloadV1,
	approved taskstate.ApprovedDefinitionV1,
) TaskRunSnapshotShadowStatus {
	if legacy == nil || legacy.Definition == nil ||
		legacy.Mode != string(approved.ExecutionMode) ||
		approved.Intent != approved.PlaybookContent {
		return TaskRunSnapshotShadowProjectionMismatch
	}
	got := legacy.Definition
	if got.TaskID != approved.TaskID || got.TenantID != approved.TenantID ||
		got.UserID != approved.UserID || got.NLDescription != approved.NLDescription ||
		!bytes.Equal(got.SpecJSON, approved.SpecJSON) ||
		!bytes.Equal(got.ScopeJSON, approved.ScopeJSON) ||
		got.PlaybookContent != approved.PlaybookContent ||
		got.Strictness != string(approved.Strictness) ||
		got.SourceScope != string(approved.SourceScope) ||
		!bytes.Equal(got.FetchPlan, approved.FetchPlan) {
		return TaskRunSnapshotShadowProjectionMismatch
	}
	if approved.SourceScope == taskstate.SourceScopeLegacySubscriptions {
		return TaskRunSnapshotShadowLegacyCompatible
	}
	if approved.SourceScope != taskstate.SourceScopeApprovedPlan ||
		len(got.Sources) != len(approved.Sources) {
		return TaskRunSnapshotShadowProjectionMismatch
	}
	for i := range approved.Sources {
		left, right := got.Sources[i], approved.Sources[i]
		if left.SourceID != right.SourceID ||
			left.Platform != string(right.Platform) ||
			left.Capability != string(right.Capability) ||
			left.Title != right.Title || left.URL != right.URL ||
			!bytes.Equal(left.Config, right.Config) {
			return TaskRunSnapshotShadowProjectionMismatch
		}
	}
	return TaskRunSnapshotShadowMatch
}

func validateExistingTaskRunSnapshotShadowV2(
	ctx context.Context,
	tx pgx.Tx,
	snapshot *taskRunSnapshot,
) error {
	var status string
	var tenantID, userID int64
	var taskID, workflowID, runID string
	var approvedVersion *int64
	var approvedDigest *string
	var adaptiveVersion int64
	var adaptiveDigest *string
	var payload []byte
	var payloadDigest string
	err := tx.QueryRow(ctx,
		`SELECT tenant_id, user_id, task_id, temporal_workflow_id, temporal_run_id,
		        status, approved_definition_version, approved_definition_digest,
		        adaptive_version, adaptive_digest, payload, payload_digest
		   FROM task_run_snapshot_v2_shadows
		  WHERE run_snapshot_id=$1`,
		snapshot.ID).Scan(
		&tenantID, &userID, &taskID, &workflowID, &runID, &status,
		&approvedVersion, &approvedDigest, &adaptiveVersion, &adaptiveDigest,
		&payload, &payloadDigest)
	if errors.Is(err, pgx.ErrNoRows) {
		// A committed v1 from before shadow enablement is intentionally not
		// backfilled from a newer movable head.
		return nil
	}
	if err != nil {
		return taskRunDatabaseError("load task run snapshot v2 shadow", err)
	}
	decoded, canonical, err := readTaskRunSnapshotShadowPayloadV2(payload)
	if err != nil || !bytes.Equal(canonical, payload) ||
		!constantTimeDigestEqual(sha256Hex(payload), payloadDigest) ||
		tenantID != snapshot.TenantID || userID != snapshot.UserID ||
		taskID != snapshot.TaskID || workflowID != snapshot.TemporalWorkflowID ||
		runID != snapshot.TemporalRunID || decoded.Legacy.SnapshotID != snapshot.ID ||
		decoded.Legacy.PayloadDigest != snapshot.PayloadDigest ||
		!bytes.Equal(decoded.Legacy.Payload, snapshot.Payload) ||
		status != string(decoded.Status) ||
		approvedVersionValue(decoded.Approved) != pointerValue(approvedVersion) ||
		approvedDigestValue(decoded.Approved) != pointerStringValue(approvedDigest) ||
		adaptiveVersionValue(decoded.Adaptive) != adaptiveVersion ||
		adaptiveDigestValue(decoded.Adaptive) != pointerStringValue(adaptiveDigest) {
		return taskRunIntegrityError()
	}
	return nil
}

func approvedVersionValue(value *taskRunSnapshotShadowApprovedV2) int64 {
	if value == nil {
		return 0
	}
	return value.Version
}

func approvedDigestValue(value *taskRunSnapshotShadowApprovedV2) string {
	if value == nil {
		return ""
	}
	return value.Digest
}

func adaptiveVersionValue(value *taskRunSnapshotShadowAdaptiveV2) int64 {
	if value == nil {
		return 0
	}
	return value.Version
}

func adaptiveDigestValue(value *taskRunSnapshotShadowAdaptiveV2) string {
	if value == nil {
		return ""
	}
	return value.Digest
}

func pointerValue(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func pointerStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func sealTaskRunSnapshotShadowV2(
	shadow taskRunSnapshotShadowV2,
	payload taskRunSnapshotShadowPayloadV2,
) (taskRunSnapshotShadowV2, error) {
	canonical, digest, err := encodeTaskRunSnapshotShadowPayloadV2(payload)
	if err != nil {
		return taskRunSnapshotShadowV2{}, taskRunIntegrityError()
	}
	shadow.Payload = canonical
	shadow.PayloadDigest = digest
	return shadow, nil
}

func insertTaskRunSnapshotShadowV2(
	ctx context.Context,
	tx pgx.Tx,
	shadow taskRunSnapshotShadowV2,
) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO task_run_snapshot_v2_shadows (
			run_snapshot_id, tenant_id, user_id, task_id,
			temporal_workflow_id, temporal_run_id, status,
			approved_definition_version, approved_definition_digest,
			adaptive_version, adaptive_digest, payload, payload_digest
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		shadow.RunSnapshotID, shadow.TenantID, shadow.UserID, shadow.TaskID,
		shadow.TemporalWorkflowID, shadow.TemporalRunID, shadow.Status,
		shadow.ApprovedDefinitionVersion, shadow.ApprovedDefinitionDigest,
		shadow.AdaptiveVersion, shadow.AdaptiveDigest,
		shadow.Payload, shadow.PayloadDigest)
	if err != nil {
		return taskRunDatabaseError("insert task run snapshot v2 shadow", err)
	}
	return nil
}

func cloneInt64Pointer(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	return cloneStringValue(*value)
}

func cloneStringValue(value string) *string {
	cloned := value
	return &cloned
}
