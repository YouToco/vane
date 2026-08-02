package store

import (
	"bytes"
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

// HasCurrentResearchApprovedDefinitionV3 is the shadow/authority preflight.
// It verifies the current immutable head and canonical V3 payload without
// exposing the task manual across the scheduler boundary.
func (s *Store) HasCurrentResearchApprovedDefinitionV3(
	ctx context.Context,
	tenantID, userID int64,
	taskID string,
) (bool, error) {
	if err := validateTaskStateScope(tenantID, userID, taskID); err != nil {
		return false, err
	}
	var schemaVersion, rawMode, digest string
	var payload []byte
	err := s.pool.QueryRow(ctx,
		`SELECT d.schema_version,d.execution_mode,d.definition_digest,d.payload
		   FROM schedules schedule
		   JOIN tenants tenant ON tenant.id=schedule.tenant_id
		   JOIN memberships membership
		     ON membership.tenant_id=schedule.tenant_id AND membership.user_id=schedule.user_id
		   JOIN task_approved_definition_versions d
		     ON d.tenant_id=schedule.tenant_id AND d.user_id=schedule.user_id
		    AND d.task_id=schedule.id AND d.version=schedule.approved_definition_version
		    AND d.definition_digest=schedule.approved_definition_digest
		    AND d.execution_mode=schedule.execution_mode
		  WHERE schedule.tenant_id=$1 AND schedule.user_id=$2 AND schedule.id=$3
		    AND schedule.status='active' AND tenant.status='active'
		    AND tenant.deleted_at IS NULL AND d.schema_version=$4`,
		tenantID, userID, taskID, taskstate.ApprovedDefinitionSchemaVersionV3,
	).Scan(&schemaVersion, &rawMode, &digest, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, taskStateDatabaseError("load Research V3 approved definition", err)
	}
	definition, err := taskstate.DecodeApprovedDefinitionV3(payload)
	canonical, canonicalErr := taskstate.EncodeApprovedDefinitionV3(definition)
	if err != nil || canonicalErr != nil || !bytes.Equal(canonical, payload) ||
		schemaVersion != taskstate.ApprovedDefinitionSchemaVersionV3 ||
		rawMode != string(types.ExecutionModeDiscoverAtRun) ||
		definition.TenantID != tenantID || definition.UserID != userID ||
		definition.TaskID != taskID ||
		definition.ExecutionMode != types.ExecutionModeDiscoverAtRun ||
		!constantTimeDigestMatches(digest, payload) {
		return false, taskStateIntegrity()
	}
	return true, nil
}
