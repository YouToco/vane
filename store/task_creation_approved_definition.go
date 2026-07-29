package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

const taskCreationOperationRefPrefix = "task-creation-operation:"

// insertTaskCreationApprovedDefinitionTx installs the immutable compiled
// definition which was approved by one durable create_schedule action. The
// caller owns the transaction and must already have materialized the legacy
// schedule/playbook/source aggregate in it. Keeping this helper transaction-
// local prevents the legacy aggregate and its exact immutable head from ever
// becoming independently visible.
func insertTaskCreationApprovedDefinitionTx(
	ctx context.Context,
	tx pgx.Tx,
	legacy types.PausedCompiledTaskDefinition,
	plan *compiledFetchPlan,
	operationID string,
) (ApprovedDefinitionVersionRecord, error) {
	definition, err := buildTaskCreationApprovedDefinitionTx(ctx, tx, legacy, plan)
	if err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}
	payload, digest, definition, err := encodeApprovedDefinitionForStore(definition)
	if err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}
	operationRef := taskCreationOperationRefPrefix + operationID
	if !validTaskStateReference(operationRef, 1024) {
		return ApprovedDefinitionVersionRecord{}, taskStateValidation(
			"task creation operation reference is invalid")
	}
	record := ApprovedDefinitionVersionRecord{
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
			return ApprovedDefinitionVersionRecord{}, taskStateConflict(
				"task creation approved definition already exists")
		}
		return ApprovedDefinitionVersionRecord{}, taskStateDatabaseError(
			"insert task creation approved definition", err)
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
		return ApprovedDefinitionVersionRecord{}, taskStateDatabaseError(
			"advance task creation approved definition head", err)
	}
	if tag.RowsAffected() != 1 {
		return ApprovedDefinitionVersionRecord{}, taskStateConflict(
			"task creation approved definition head changed")
	}
	return record, nil
}

// taskCreationApprovedDefinitionMatchesTx verifies response-loss replay against
// both the materialized legacy projection and the exact immutable head. A
// replay can therefore never adopt a headless, differently-approved, or
// dynamically-routed task merely because its legacy fields still match.
func taskCreationApprovedDefinitionMatchesTx(
	ctx context.Context,
	tx pgx.Tx,
	legacy types.PausedCompiledTaskDefinition,
	plan *compiledFetchPlan,
	operationID string,
) (bool, error) {
	definition, err := buildTaskCreationApprovedDefinitionTx(ctx, tx, legacy, plan)
	if err != nil {
		return false, err
	}
	payload, digest, _, err := encodeApprovedDefinitionForStore(definition)
	if err != nil {
		return false, err
	}
	operationRef := taskCreationOperationRefPrefix + operationID
	var headVersion *int64
	var headDigest *string
	var rawMode string
	if err := tx.QueryRow(ctx,
		`SELECT approved_definition_version, approved_definition_digest, execution_mode
		   FROM schedules
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3
		  FOR UPDATE`,
		legacy.TenantID, legacy.UserID, legacy.TaskID,
	).Scan(&headVersion, &headDigest, &rawMode); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, taskStateDatabaseError(
			"load task creation approved definition head", err)
	}
	if headVersion == nil || headDigest == nil ||
		*headVersion != initialApprovedDefinitionVersion ||
		!constantTimeTaskStateDigestEqual(*headDigest, digest) ||
		rawMode != string(types.ExecutionModeCompiled) {
		return false, nil
	}
	record, err := loadApprovedDefinitionByOperationRefTx(ctx, tx,
		legacy.TenantID, legacy.UserID, legacy.TaskID, operationRef)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return record.Version == initialApprovedDefinitionVersion &&
		constantTimeTaskStateDigestEqual(record.Digest, digest) &&
		bytes.Equal(record.Payload, payload) &&
		record.Definition.ExecutionMode == types.ExecutionModeCompiled, nil
}

func buildTaskCreationApprovedDefinitionTx(
	ctx context.Context,
	tx pgx.Tx,
	legacy types.PausedCompiledTaskDefinition,
	plan *compiledFetchPlan,
) (taskstate.ApprovedDefinitionV1, error) {
	if plan == nil || len(plan.Targets) == 0 {
		return taskstate.ApprovedDefinitionV1{}, taskStateValidation(
			"task creation approved plan is empty")
	}
	linkedIDs := make(map[string]int64, len(plan.Targets))
	rows, err := tx.Query(ctx,
		`SELECT src.url, src.id
		   FROM task_fetch_targets ss
		   JOIN fetch_targets src ON src.id=ss.fetch_target_id
		  WHERE ss.schedule_id=$1
		  ORDER BY src.url, src.id
		  FOR SHARE OF ss, src`, legacy.TaskID)
	if err != nil {
		return taskstate.ApprovedDefinitionV1{}, taskStateDatabaseError(
			"load task creation materialized sources", err)
	}
	defer rows.Close()
	for rows.Next() {
		var sourceURL string
		var sourceID int64
		if err := rows.Scan(&sourceURL, &sourceID); err != nil {
			return taskstate.ApprovedDefinitionV1{}, taskStateDatabaseError(
				"scan task creation materialized source", err)
		}
		if sourceID <= 0 || !validTaskStateReference(sourceURL, 4096) {
			return taskstate.ApprovedDefinitionV1{}, taskStateIntegrity()
		}
		if _, duplicate := linkedIDs[sourceURL]; duplicate {
			return taskstate.ApprovedDefinitionV1{}, taskStateIntegrity()
		}
		linkedIDs[sourceURL] = sourceID
	}
	if err := rows.Err(); err != nil {
		return taskstate.ApprovedDefinitionV1{}, taskStateDatabaseError(
			"iterate task creation materialized sources", err)
	}
	if len(linkedIDs) != len(plan.Targets) {
		return taskstate.ApprovedDefinitionV1{}, taskStateConflict(
			"task creation plan and materialized sources differ")
	}
	approvedSources := make([]taskstate.ApprovedSourceV1, 0, len(plan.Targets))
	legacyPlan := taskstate.FetchPlanV1{
		Sources: make([]taskstate.PlanSourceV1, 0, len(plan.Targets)),
	}
	for _, source := range plan.Targets {
		sourceID, ok := linkedIDs[source.URL]
		if !ok {
			return taskstate.ApprovedDefinitionV1{}, taskStateConflict(
				"task creation plan and materialized sources differ")
		}
		approvedSources = append(approvedSources, taskstate.ApprovedSourceV1{
			SourceID: sourceID, Platform: types.Platform(source.Platform),
			Capability: types.Capability(source.Capability), Title: source.Title,
			URL: source.URL, Config: bytes.Clone(source.Config),
		})
		legacyPlan.Sources = append(legacyPlan.Sources, taskstate.PlanSourceV1{
			Platform:   types.Platform(source.Platform),
			Capability: types.Capability(source.Capability),
			Title:      source.Title, URL: source.URL,
			Config: bytes.Clone(source.Config),
		})
	}
	legacyFetchPlan, err := json.Marshal(legacyPlan)
	if err != nil {
		return taskstate.ApprovedDefinitionV1{}, taskStateIntegrity()
	}
	definition, err := taskstate.BuildApprovedDefinitionV1(
		taskstate.ApprovedDefinitionInputV1{
			TenantID: legacy.TenantID, UserID: legacy.UserID, TaskID: legacy.TaskID,
			Intent: legacy.PlaybookContent, NLDescription: legacy.NLDescription,
			SpecJSON: legacy.SpecJSON, ScopeJSON: legacy.ScopeJSON,
			PlaybookContent: legacy.PlaybookContent,
			SourceScope:     taskstate.SourceScopeApprovedPlan,
			FetchPlan:       legacyFetchPlan, Strictness: legacy.Strictness,
			Sources: approvedSources, ExecutionMode: types.ExecutionModeCompiled,
			DeliveryPolicy: taskstate.DeliveryPolicyOwnerFeishu,
			BudgetPolicy:   taskstate.BudgetPolicyInheritTenantQuota,
		})
	if err != nil {
		return taskstate.ApprovedDefinitionV1{}, taskStateValidation(
			"task creation approved definition is invalid")
	}
	return definition, nil
}
