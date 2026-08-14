package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/taskstate"
	"github.com/YouToco/vane/server/types"
)

// LoadTaskDefinitionEditProposalBasis returns the exact immutable head and the
// completed create-schedule provenance needed to prepare one edit proposal.
// The user predicate and active membership are in the query, so a guessed
// TaskID cannot reveal or prepare another owner's task.
func (s *Store) LoadTaskDefinitionEditProposalBasis(
	ctx context.Context,
	userID int64,
	taskID string,
) (
	int64,
	types.ScheduleStatus,
	int64,
	string,
	taskstate.ApprovedDefinitionV1,
	[]byte,
	error,
) {
	if userID <= 0 || strings.TrimSpace(taskID) == "" ||
		taskID != strings.TrimSpace(taskID) || len(taskID) > 255 {
		return 0, "", 0, "", taskstate.ApprovedDefinitionV1{},
			nil, taskDefinitionEditValidation(
				"proposal basis scope is invalid",
			)
	}
	var (
		tenantID      int64
		status        types.ScheduleStatus
		version       int64
		digest        string
		payload       []byte
		preparedBytes []byte
	)
	err := s.pool.QueryRow(ctx,
		`SELECT s.tenant_id, s.status, d.version, d.definition_digest,
		        d.payload, creation.prepared_schedule
		   FROM schedules s
		   JOIN tenants t ON t.id=s.tenant_id
		   JOIN memberships m
		     ON m.tenant_id=s.tenant_id AND m.user_id=s.user_id
		   JOIN task_approved_definition_versions d
		     ON d.tenant_id=s.tenant_id AND d.user_id=s.user_id
		    AND d.task_id=s.id
		    AND d.version=s.approved_definition_version
		    AND d.definition_digest=s.approved_definition_digest
		    AND d.execution_mode=s.execution_mode
		   JOIN LATERAL (
		     SELECT p.prepared_schedule
		       FROM task_creation_operations p
		      WHERE p.tenant_id=s.tenant_id AND p.user_id=s.user_id
		        AND p.task_id=s.id AND p.tool_name='create_schedule'
		        AND p.execution_version=$3 AND p.status=$4 AND p.phase=$5
		        AND p.tombstoned_at IS NOT NULL
		      ORDER BY p.id
		      LIMIT 1
		   ) creation ON true
		  WHERE s.user_id=$1 AND s.id=$2
		    AND t.status='active' AND t.deleted_at IS NULL
		    AND `+matureSchedulePredicate,
		userID, taskID, types.TaskCreationExecutionVersionV1,
		types.TaskOperationStatusExecuted, types.TaskCreationPhaseCompleted,
	).Scan(&tenantID, &status, &version, &digest, &payload, &preparedBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, "", 0, "", taskstate.ApprovedDefinitionV1{},
				nil, taskDefinitionEditNotFound()
		}
		return 0, "", 0, "", taskstate.ApprovedDefinitionV1{},
			nil, taskDefinitionEditDatabaseError(
				"load proposal basis", err,
			)
	}
	definition, err := taskstate.DecodeApprovedDefinitionV1(payload)
	if err != nil {
		return 0, "", 0, "", taskstate.ApprovedDefinitionV1{},
			nil, taskDefinitionEditIntegrity()
	}
	canonicalDefinition, err := taskstate.EncodeApprovedDefinitionV1(definition)
	if err != nil || !bytes.Equal(canonicalDefinition, payload) ||
		definition.TenantID != tenantID || definition.UserID != userID ||
		definition.TaskID != taskID || version <= 0 ||
		!constantTimeDigestMatches(digest, payload) {
		return 0, "", 0, "", taskstate.ApprovedDefinitionV1{},
			nil, taskDefinitionEditIntegrity()
	}
	if len(preparedBytes) == 0 || !json.Valid(preparedBytes) {
		return 0, "", 0, "", taskstate.ApprovedDefinitionV1{},
			nil, taskDefinitionEditIntegrity()
	}
	return tenantID, status, version, digest, definition,
		bytes.Clone(preparedBytes), nil
}

// LoadTaskDefinitionEditOperationByActor restores the complete frozen scope
// from an action ID without trusting task identity from the card callback.
func (s *Store) LoadTaskDefinitionEditOperationByActor(
	ctx context.Context,
	actionID string,
	userID int64,
) (*types.TaskDefinitionEditOperation, error) {
	if strings.TrimSpace(actionID) == "" ||
		actionID != strings.TrimSpace(actionID) || userID <= 0 {
		return nil, taskDefinitionEditValidation(
			"operation actor lookup is invalid",
		)
	}
	var operation types.TaskDefinitionEditOperation
	err := scanTaskDefinitionEditOperation(
		s.pool.QueryRow(ctx,
			`SELECT `+taskDefinitionEditOperationColumns+`
			   FROM task_definition_edit_operations o
			  WHERE o.id=$1 AND o.user_id=$2
			    AND EXISTS (
			      SELECT 1
			        FROM tenants t
			        JOIN memberships m
			          ON m.tenant_id=t.id AND m.user_id=o.user_id
			       WHERE t.id=o.tenant_id
			         AND t.status='active' AND t.deleted_at IS NULL
			    )`,
			actionID, userID,
		),
		&operation,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, taskDefinitionEditNotFound()
		}
		return nil, taskDefinitionEditDatabaseError(
			"load operation by actor", err,
		)
	}
	return cloneTaskDefinitionEditOperation(&operation), nil
}
