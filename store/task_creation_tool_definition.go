package store

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

const (
	compiledTaskDefinitionProtocolV1 = "vane.compiled-task-definition/v1"
	compiledTaskDefinitionProtocolV2 = "vane.compiled-task-definition/v2"
)

// ToolApprovedDefinitionVersionRecord is the Source-free immutable head used
// by newly compiled tasks. The legacy ApprovedDefinitionVersionRecord remains
// the dedicated V1 recovery view.
type ToolApprovedDefinitionVersionRecord struct {
	Definition   taskstate.ApprovedDefinitionV2
	Version      int64
	Digest       string
	Payload      []byte
	OperationRef string
	CreatedAt    time.Time
}

func compiledPlanUsesToolInvocations(plan *compiledFetchPlan) (bool, error) {
	if plan == nil || len(plan.Targets) == 0 {
		return false, taskCreationValidation("compiled Tool plan is empty")
	}
	withTool := 0
	for _, target := range plan.Targets {
		if target.ToolName != "" {
			withTool++
		}
	}
	return withTool == len(plan.Targets), nil
}

func buildTaskCreationToolApprovedDefinition(
	legacy types.PausedCompiledTaskDefinition,
	plan *compiledFetchPlan,
) (taskstate.ApprovedDefinitionV2, error) {
	usesTools, err := compiledPlanUsesToolInvocations(plan)
	if err != nil {
		return taskstate.ApprovedDefinitionV2{}, err
	}
	if !usesTools {
		return taskstate.ApprovedDefinitionV2{}, taskCreationValidation(
			"Source-free task creation requires frozen Tool calls")
	}
	calls := make([]taskstate.ToolInvocationV1, 0, len(plan.Targets))
	for _, target := range plan.Targets {
		call, err := taskstate.BuildToolInvocationV1(
			target.ToolName, "v1", target.ToolArgs)
		if err != nil {
			return taskstate.ApprovedDefinitionV2{}, taskCreationValidation(
				"compiled acquisition Tool call is invalid")
		}
		calls = append(calls, call)
	}
	definition, err := taskstate.BuildApprovedDefinitionV2(
		taskstate.ApprovedDefinitionInputV2{
			TenantID: legacy.TenantID, UserID: legacy.UserID, TaskID: legacy.TaskID,
			NLDescription: legacy.NLDescription,
			SpecJSON:      legacy.SpecJSON, ScopeJSON: legacy.ScopeJSON,
			TaskManual: legacy.PlaybookContent, Strictness: legacy.Strictness,
			ToolCalls: calls, ExecutionMode: types.ExecutionModeCompiled,
			DeliveryPolicy: taskstate.DeliveryPolicyOwnerFeishu,
			BudgetPolicy:   taskstate.BudgetPolicyInheritTenantQuota,
		})
	if err != nil {
		return taskstate.ApprovedDefinitionV2{}, taskCreationValidation(
			"task creation Tool-approved definition is invalid")
	}
	return definition, nil
}

func encodeToolApprovedDefinitionForStore(
	definition taskstate.ApprovedDefinitionV2,
) ([]byte, string, taskstate.ApprovedDefinitionV2, error) {
	if err := taskstate.ValidateApprovedDefinitionV2ForWrite(definition); err != nil {
		return nil, "", taskstate.ApprovedDefinitionV2{},
			taskStateValidation("Tool-approved definition is not writable")
	}
	payload, err := taskstate.EncodeApprovedDefinitionV2(definition)
	if err != nil {
		return nil, "", taskstate.ApprovedDefinitionV2{},
			taskStateValidation("Tool-approved definition is invalid")
	}
	normalized, err := taskstate.DecodeApprovedDefinitionV2(payload)
	if err != nil {
		return nil, "", taskstate.ApprovedDefinitionV2{},
			taskStateValidation("Tool-approved definition is invalid")
	}
	return payload, digestTaskStatePayload(payload), normalized, nil
}

func insertPausedToolTaskDefinitionTx(
	ctx context.Context,
	tx pgx.Tx,
	def types.PausedCompiledTaskDefinition,
) error {
	if _, err := tx.Exec(ctx,
		`INSERT INTO schedules
			(id, tenant_id, user_id, nl_description, spec_json, scope_json, status,
			 push_strictness, execution_mode)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		def.TaskID, def.TenantID, def.UserID, def.NLDescription,
		[]byte(def.SpecJSON), []byte(def.ScopeJSON),
		types.ScheduleStatusPaused, nullableStrictness(def.Strictness),
		types.ExecutionModeCompiled); err != nil {
		if isUniqueViolation(err) {
			return taskCreationConflict("task id already exists")
		}
		return taskCreationDatabaseError("insert Source-free paused task", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO schedule_playbooks (schedule_id, content, fetch_plan)
		 VALUES ($1, $2, $3)`,
		def.TaskID, def.PlaybookContent, []byte(`{}`)); err != nil {
		return taskCreationDatabaseError("insert task manual", err)
	}
	return nil
}

func insertTaskCreationToolApprovedDefinitionTx(
	ctx context.Context,
	tx pgx.Tx,
	legacy types.PausedCompiledTaskDefinition,
	plan *compiledFetchPlan,
	operationID string,
) (ToolApprovedDefinitionVersionRecord, error) {
	definition, err := buildTaskCreationToolApprovedDefinition(legacy, plan)
	if err != nil {
		return ToolApprovedDefinitionVersionRecord{}, err
	}
	payload, digest, definition, err := encodeToolApprovedDefinitionForStore(definition)
	if err != nil {
		return ToolApprovedDefinitionVersionRecord{}, err
	}
	operationRef := taskCreationOperationRefPrefix + operationID
	if !validTaskStateReference(operationRef, 1024) {
		return ToolApprovedDefinitionVersionRecord{}, taskStateValidation(
			"task creation operation reference is invalid")
	}
	record := ToolApprovedDefinitionVersionRecord{
		Definition: definition, Version: initialApprovedDefinitionVersion,
		Digest: digest, Payload: bytes.Clone(payload), OperationRef: operationRef,
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO task_approved_definition_versions (
			tenant_id, user_id, task_id, version, schema_version,
			execution_mode, definition_digest, payload, operation_ref
		 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 RETURNING created_at`,
		definition.TenantID, definition.UserID, definition.TaskID,
		record.Version, definition.SchemaVersion, definition.ExecutionMode,
		record.Digest, record.Payload, record.OperationRef,
	).Scan(&record.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return ToolApprovedDefinitionVersionRecord{}, taskStateConflict(
				"task creation Tool-approved definition already exists")
		}
		return ToolApprovedDefinitionVersionRecord{}, taskStateDatabaseError(
			"insert task creation Tool-approved definition", err)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE schedules
		    SET approved_definition_version=$4,
		        approved_definition_digest=$5
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3
		    AND execution_mode=$6
		    AND approved_definition_version IS NULL
		    AND approved_definition_digest IS NULL`,
		definition.TenantID, definition.UserID, definition.TaskID,
		record.Version, record.Digest, types.ExecutionModeCompiled)
	if err != nil {
		return ToolApprovedDefinitionVersionRecord{}, taskStateDatabaseError(
			"advance task creation Tool-approved definition head", err)
	}
	if tag.RowsAffected() != 1 {
		return ToolApprovedDefinitionVersionRecord{}, taskStateConflict(
			"task creation Tool-approved definition head changed")
	}
	return record, nil
}

func taskCreationToolApprovedDefinitionMatchesTx(
	ctx context.Context,
	tx pgx.Tx,
	legacy types.PausedCompiledTaskDefinition,
	plan *compiledFetchPlan,
	operationID string,
) (bool, error) {
	want, err := buildTaskCreationToolApprovedDefinition(legacy, plan)
	if err != nil {
		return false, err
	}
	payload, digest, _, err := encodeToolApprovedDefinitionForStore(want)
	if err != nil {
		return false, err
	}
	operationRef := taskCreationOperationRefPrefix + operationID
	record, err := scanToolApprovedDefinitionVersion(tx.QueryRow(ctx,
		`SELECT d.version, d.schema_version, d.execution_mode,
		        d.definition_digest, d.payload, d.operation_ref, d.created_at
		   FROM task_approved_definition_versions d
		  WHERE d.tenant_id=$1 AND d.user_id=$2 AND d.task_id=$3
		    AND d.operation_ref=$4`,
		legacy.TenantID, legacy.UserID, legacy.TaskID, operationRef),
		legacy.TenantID, legacy.UserID, legacy.TaskID)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if record.Version != initialApprovedDefinitionVersion ||
		record.Digest != digest || !bytes.Equal(record.Payload, payload) {
		return false, nil
	}
	return taskCreationInitialToolAdaptiveStateMatchesTx(ctx, tx, record)
}

// GetCurrentToolApprovedDefinition loads only the Source-free V2 head. A V1
// task returns NotFound here and must use the retained recovery API.
func (s *Store) GetCurrentToolApprovedDefinition(
	ctx context.Context,
	tenantID, userID int64,
	taskID string,
) (ToolApprovedDefinitionVersionRecord, error) {
	if err := validateTaskStateScope(tenantID, userID, taskID); err != nil {
		return ToolApprovedDefinitionVersionRecord{}, err
	}
	return scanToolApprovedDefinitionVersion(s.pool.QueryRow(ctx,
		`SELECT d.version, d.schema_version, d.execution_mode,
		        d.definition_digest, d.payload, d.operation_ref, d.created_at
		   FROM schedules s
		   JOIN tenants t ON t.id=s.tenant_id
		   JOIN memberships m ON m.tenant_id=s.tenant_id AND m.user_id=s.user_id
		   JOIN task_approved_definition_versions d
		     ON d.tenant_id=s.tenant_id AND d.user_id=s.user_id AND d.task_id=s.id
		    AND d.version=s.approved_definition_version
		    AND d.definition_digest=s.approved_definition_digest
		    AND d.execution_mode=s.execution_mode
		  WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3
		    AND d.schema_version=$4
		    AND t.status='active' AND t.deleted_at IS NULL`,
		tenantID, userID, taskID, taskstate.ApprovedDefinitionSchemaVersionV2),
		tenantID, userID, taskID)
}

func scanToolApprovedDefinitionVersion(
	row pgx.Row,
	tenantID, userID int64,
	taskID string,
) (ToolApprovedDefinitionVersionRecord, error) {
	var record ToolApprovedDefinitionVersionRecord
	var schemaVersion, rawMode string
	if err := row.Scan(&record.Version, &schemaVersion, &rawMode, &record.Digest,
		&record.Payload, &record.OperationRef, &record.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ToolApprovedDefinitionVersionRecord{}, taskStateNotFound()
		}
		return ToolApprovedDefinitionVersionRecord{}, taskStateDatabaseError(
			"load Tool-approved definition", err)
	}
	definition, err := taskstate.DecodeApprovedDefinitionV2(record.Payload)
	if err != nil {
		return ToolApprovedDefinitionVersionRecord{}, taskStateIntegrity()
	}
	canonical, err := taskstate.EncodeApprovedDefinitionV2(definition)
	if err != nil || !bytes.Equal(canonical, record.Payload) ||
		schemaVersion != taskstate.ApprovedDefinitionSchemaVersionV2 ||
		definition.SchemaVersion != schemaVersion ||
		definition.TenantID != tenantID || definition.UserID != userID ||
		definition.TaskID != taskID || definition.ExecutionMode != types.ExecutionMode(rawMode) ||
		record.Version <= 0 || !validTaskStateReference(record.OperationRef, 1024) ||
		!constantTimeDigestMatches(record.Digest, record.Payload) {
		return ToolApprovedDefinitionVersionRecord{}, taskStateIntegrity()
	}
	record.Definition = definition
	record.Payload = bytes.Clone(record.Payload)
	return record, nil
}

func pausedToolTaskDefinitionMatches(
	ctx context.Context,
	tx pgx.Tx,
	def types.PausedCompiledTaskDefinition,
	expectedStatus types.ScheduleStatus,
) (bool, error) {
	var (
		tenantID, userID             int64
		nlDescription, playbook      string
		specJSON, scopeJSON, planRaw []byte
		status                       types.ScheduleStatus
		executionMode                string
		strictness                   *string
	)
	err := tx.QueryRow(ctx,
		`SELECT s.tenant_id, s.user_id, s.nl_description, s.spec_json, s.scope_json,
		        s.status, s.execution_mode, s.push_strictness, p.content, p.fetch_plan
		   FROM schedules s
		   JOIN schedule_playbooks p ON p.schedule_id=s.id
		  WHERE s.id=$1
		  FOR UPDATE OF s, p`, def.TaskID,
	).Scan(&tenantID, &userID, &nlDescription, &specJSON, &scopeJSON,
		&status, &executionMode, &strictness, &playbook, &planRaw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, taskCreationDatabaseError("load committed Tool task", err)
	}
	if tenantID != def.TenantID || userID != def.UserID ||
		nlDescription != def.NLDescription || status != expectedStatus ||
		executionMode != string(types.ExecutionModeCompiled) ||
		playbook != def.PlaybookContent ||
		!nullableStringsEqual(strictness, nullableStrictness(def.Strictness)) ||
		!taskCreationJSONEqual(specJSON, def.SpecJSON) ||
		!taskCreationJSONEqual(scopeJSON, def.ScopeJSON) ||
		!taskCreationJSONEqual(planRaw, []byte(`{}`)) {
		return false, nil
	}
	var linked int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM task_fetch_targets WHERE schedule_id=$1`,
		def.TaskID).Scan(&linked); err != nil {
		return false, taskCreationDatabaseError(
			"verify Source-free task has no fetch-target links", err)
	}
	return linked == 0, nil
}
