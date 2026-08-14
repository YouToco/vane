package store

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/taskstate"
)

// ToolAdaptiveStateRecord is the V2 invocation-digest-scoped view stored in
// the existing task_adaptive_states aggregate. V1 has a separate typed reader
// so neither protocol can reinterpret the other's payload.
type ToolAdaptiveStateRecord struct {
	State                          taskstate.AdaptiveStateV2
	Version                        int64
	Digest                         string
	Payload                        []byte
	BasisDefinitionVersion         int64
	BasisDefinitionDigest          string
	LastKnownGoodDefinitionVersion *int64
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
}

func buildInitialToolAdaptiveState(
	definition taskstate.ApprovedDefinitionV2,
) (taskstate.AdaptiveStateV2, error) {
	invocations := make([]taskstate.InvocationAdaptiveStateV1,
		0, len(definition.ToolCalls))
	for _, call := range definition.ToolCalls {
		invocations = append(invocations, taskstate.InvocationAdaptiveStateV1{
			InvocationDigest: call.Digest,
			Cursor:           []byte(`{}`),
			Status:           taskstate.InvocationStatusActive,
		})
	}
	return taskstate.BuildAdaptiveStateV2(taskstate.AdaptiveStateInputV2{
		TenantID: definition.TenantID, UserID: definition.UserID,
		TaskID: definition.TaskID, InvocationStates: invocations,
		RunStats: taskstate.RunStatsV1{},
	})
}

func insertInitialToolAdaptiveStateTx(
	ctx context.Context,
	tx pgx.Tx,
	approved ToolApprovedDefinitionVersionRecord,
) (ToolAdaptiveStateRecord, error) {
	state, err := buildInitialToolAdaptiveState(approved.Definition)
	if err != nil {
		return ToolAdaptiveStateRecord{}, taskStateValidation(
			"initial Tool adaptive state is invalid")
	}
	payload, err := taskstate.EncodeAdaptiveStateV2(state)
	if err != nil {
		return ToolAdaptiveStateRecord{}, taskStateValidation(
			"initial Tool adaptive state is invalid")
	}
	record := ToolAdaptiveStateRecord{
		State: state, Version: 1, Digest: digestTaskStatePayload(payload),
		Payload:                bytes.Clone(payload),
		BasisDefinitionVersion: approved.Version,
		BasisDefinitionDigest:  approved.Digest,
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO task_adaptive_states (
			tenant_id, user_id, task_id, version, schema_version,
			payload_digest, payload, basis_definition_version,
			basis_definition_digest, last_known_good_definition_version
		 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL)
		 RETURNING created_at, updated_at`,
		state.TenantID, state.UserID, state.TaskID, record.Version,
		state.SchemaVersion, record.Digest, record.Payload,
		record.BasisDefinitionVersion, record.BasisDefinitionDigest,
	).Scan(&record.CreatedAt, &record.UpdatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return ToolAdaptiveStateRecord{}, taskStateConflict(
				"initial Tool adaptive state already exists")
		}
		return ToolAdaptiveStateRecord{}, taskStateDatabaseError(
			"insert initial Tool adaptive state", err)
	}
	return record, nil
}

func taskCreationInitialToolAdaptiveStateMatchesTx(
	ctx context.Context,
	tx pgx.Tx,
	approved ToolApprovedDefinitionVersionRecord,
) (bool, error) {
	want, err := buildInitialToolAdaptiveState(approved.Definition)
	if err != nil {
		return false, err
	}
	wantPayload, err := taskstate.EncodeAdaptiveStateV2(want)
	if err != nil {
		return false, err
	}
	got, err := scanToolAdaptiveState(tx.QueryRow(ctx,
		`SELECT version, schema_version, payload_digest, payload,
		        basis_definition_version, basis_definition_digest,
		        last_known_good_definition_version, created_at, updated_at
		   FROM task_adaptive_states
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		approved.Definition.TenantID, approved.Definition.UserID,
		approved.Definition.TaskID),
		approved.Definition.TenantID, approved.Definition.UserID,
		approved.Definition.TaskID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return got.Version == 1 &&
		got.BasisDefinitionVersion == approved.Version &&
		constantTimeTaskStateDigestEqual(
			got.BasisDefinitionDigest, approved.Digest) &&
		got.LastKnownGoodDefinitionVersion == nil &&
		bytes.Equal(got.Payload, wantPayload), nil
}

// GetToolAdaptiveStateForDefinition loads V2 state only when the exact
// Approved V2 head fence still matches.
func (s *Store) GetToolAdaptiveStateForDefinition(
	ctx context.Context,
	tenantID, userID int64,
	taskID string,
	basis ApprovedDefinitionFence,
) (ToolAdaptiveStateRecord, error) {
	if err := validateTaskStateScope(tenantID, userID, taskID); err != nil {
		return ToolAdaptiveStateRecord{}, err
	}
	if basis.Version <= 0 || !validTaskStateDigest(basis.Digest) {
		return ToolAdaptiveStateRecord{}, taskStateValidation(
			"Tool adaptive approved-definition fence is invalid")
	}
	return scanToolAdaptiveState(s.pool.QueryRow(ctx,
		`SELECT a.version, a.schema_version, a.payload_digest, a.payload,
		        a.basis_definition_version, a.basis_definition_digest,
		        a.last_known_good_definition_version, a.created_at, a.updated_at
		   FROM task_adaptive_states a
		   JOIN schedules s
		     ON s.tenant_id=a.tenant_id AND s.user_id=a.user_id AND s.id=a.task_id
		  WHERE a.tenant_id=$1 AND a.user_id=$2 AND a.task_id=$3
		    AND a.schema_version=$4
		    AND a.basis_definition_version=$5
		    AND a.basis_definition_digest=$6
		    AND s.approved_definition_version=$5
		    AND s.approved_definition_digest=$6`,
		tenantID, userID, taskID, taskstate.AdaptiveStateSchemaVersionV2,
		basis.Version, basis.Digest), tenantID, userID, taskID)
}

func scanToolAdaptiveState(
	row pgx.Row,
	tenantID, userID int64,
	taskID string,
) (ToolAdaptiveStateRecord, error) {
	var record ToolAdaptiveStateRecord
	var schemaVersion string
	if err := row.Scan(&record.Version, &schemaVersion, &record.Digest,
		&record.Payload, &record.BasisDefinitionVersion,
		&record.BasisDefinitionDigest, &record.LastKnownGoodDefinitionVersion,
		&record.CreatedAt, &record.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ToolAdaptiveStateRecord{}, err
		}
		return ToolAdaptiveStateRecord{}, taskStateDatabaseError(
			"load Tool adaptive state", err)
	}
	state, err := taskstate.DecodeAdaptiveStateV2(record.Payload)
	if err != nil {
		return ToolAdaptiveStateRecord{}, taskStateIntegrity()
	}
	canonical, err := taskstate.EncodeAdaptiveStateV2(state)
	if err != nil || !bytes.Equal(canonical, record.Payload) ||
		schemaVersion != taskstate.AdaptiveStateSchemaVersionV2 ||
		state.SchemaVersion != schemaVersion ||
		state.TenantID != tenantID || state.UserID != userID ||
		state.TaskID != taskID || record.Version <= 0 ||
		record.BasisDefinitionVersion <= 0 ||
		!validTaskStateDigest(record.BasisDefinitionDigest) ||
		record.LastKnownGoodDefinitionVersion != nil ||
		!constantTimeDigestMatches(record.Digest, record.Payload) {
		return ToolAdaptiveStateRecord{}, taskStateIntegrity()
	}
	record.State = state
	record.Payload = bytes.Clone(record.Payload)
	return record, nil
}
