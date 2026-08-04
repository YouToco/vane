package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

const researchV3SourceBaselineSchemaV1 = "vane.research-v3-source-baseline/v1"

type researchV3PreparedBinding struct {
	Target               types.ResearchV3DefinitionHead
	ScheduleStatus       types.ScheduleStatus
	BaseMode             types.ExecutionMode
	BaseHead             *types.ResearchV3DefinitionHead
	SourceBaselineDigest string
	Definition           taskstate.ApprovedDefinitionV3
}

type researchV3ProductionHeadExpectation uint8

const (
	researchV3ExpectBaseHead researchV3ProductionHeadExpectation = iota
	researchV3ExpectTargetHead
	researchV3ExpectBaseOrTargetHead
)

type researchV3SourceBaselineV1 struct {
	Schema                 string                          `json:"schema"`
	TenantID               int64                           `json:"tenant_id"`
	UserID                 int64                           `json:"user_id"`
	TaskID                 string                          `json:"task_id"`
	TargetDefinitionDigest string                          `json:"target_definition_digest"`
	PushStrictness         types.PushStrictness            `json:"push_strictness"`
	BaseExecutionMode      types.ExecutionMode             `json:"base_execution_mode"`
	BaseDefinition         *types.ResearchV3DefinitionHead `json:"base_definition,omitempty"`
}

func sealResearchV3SourceBaseline(definition taskstate.ApprovedDefinitionV3,
	strictness types.PushStrictness, mode types.ExecutionMode,
	head *types.ResearchV3DefinitionHead,
) (string, error) {
	if !strictness.Valid() {
		strictness = types.DefaultStrictness
	}
	digest, err := taskstate.DigestApprovedDefinitionV3(definition)
	if err != nil || (mode != types.ExecutionModeCompiled && mode != types.ExecutionModeDiscoverAtRun) ||
		(head != nil && (head.Version <= 0 || !validDigestSyntaxV3(head.Digest))) {
		return "", taskStateIntegrity()
	}
	payload, err := json.Marshal(researchV3SourceBaselineV1{
		Schema: researchV3SourceBaselineSchemaV1, TenantID: definition.TenantID,
		UserID: definition.UserID, TaskID: definition.TaskID,
		TargetDefinitionDigest: digest, PushStrictness: strictness,
		BaseExecutionMode: mode, BaseDefinition: head,
	})
	if err != nil {
		return "", taskStateIntegrity()
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

// loadPreparedResearchV3BindingTx proves that the immutable sidecar still
// describes the live task projection that was shadowed. Callers that mutate
// cutover state hold the exact-task advisory lock in the same transaction.
func loadPreparedResearchV3BindingTx(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, tenantID, userID int64, taskID string, requireActive bool,
	expectation researchV3ProductionHeadExpectation,
) (researchV3PreparedBinding, error) {
	var binding researchV3PreparedBinding
	var preparedScheduleStatus types.ScheduleStatus
	var baseVersion *int64
	var baseDigest *string
	var schema, definitionMode, liveMode, name, manual string
	var payload, liveSpec []byte
	var rawStrictness *string
	var liveVersion *int64
	var liveDigest *string
	statusPredicate := "schedule.status IN ('active','paused')"
	if requireActive {
		statusPredicate = "schedule.status='active'"
	}
	err := q.QueryRow(ctx, `SELECT head.definition_version,head.definition_digest,
		       head.base_execution_mode,head.base_definition_version,
		       head.base_definition_digest,head.source_baseline_digest,
		       head.prepared_schedule_status,
		       definition.schema_version,definition.execution_mode,definition.payload,
		       schedule.status,schedule.execution_mode,schedule.approved_definition_version,
		       schedule.approved_definition_digest,schedule.nl_description,
		       playbook.content,schedule.spec_json,schedule.push_strictness
		  FROM research_v3_prepared_definition_heads head
		  JOIN schedules schedule ON schedule.tenant_id=head.tenant_id
		   AND schedule.user_id=head.user_id AND schedule.id=head.task_id
		  JOIN tenants tenant ON tenant.id=schedule.tenant_id
		  JOIN memberships membership ON membership.tenant_id=schedule.tenant_id
		   AND membership.user_id=schedule.user_id
		  JOIN schedule_playbooks playbook ON playbook.schedule_id=schedule.id
		  JOIN task_approved_definition_versions definition
		    ON definition.tenant_id=head.tenant_id AND definition.user_id=head.user_id
		   AND definition.task_id=head.task_id AND definition.version=head.definition_version
		   AND definition.definition_digest=head.definition_digest
		 WHERE head.tenant_id=$1 AND head.user_id=$2 AND head.task_id=$3
		   AND `+statusPredicate+` AND tenant.status='active' AND tenant.deleted_at IS NULL
		   AND membership.role='owner'`, tenantID, userID, taskID).Scan(
		&binding.Target.Version, &binding.Target.Digest, &binding.BaseMode,
		&baseVersion, &baseDigest, &binding.SourceBaselineDigest, &preparedScheduleStatus,
		&schema, &definitionMode, &payload, &binding.ScheduleStatus,
		&liveMode, &liveVersion, &liveDigest,
		&name, &manual, &liveSpec, &rawStrictness)
	if errors.Is(err, pgx.ErrNoRows) {
		return binding, types.NewAppError(types.CodeNotFound,
			"prepared research V3 definition is unavailable", types.ErrNotFound)
	}
	if err != nil {
		return binding, taskStateDatabaseError("load prepared research V3 binding", err)
	}
	if (baseVersion == nil) != (baseDigest == nil) || (liveVersion == nil) != (liveDigest == nil) {
		return binding, taskStateIntegrity()
	}
	if preparedScheduleStatus != binding.ScheduleStatus {
		return binding, types.NewAppError(types.CodeConflict,
			"research V3 schedule status changed after prepare; prepare and shadow it again",
			types.ErrConflict)
	}
	if baseVersion != nil {
		binding.BaseHead = &types.ResearchV3DefinitionHead{Version: *baseVersion, Digest: *baseDigest}
	}
	liveHeadMatches := false
	switch expectation {
	case researchV3ExpectBaseHead:
		liveHeadMatches = types.ExecutionMode(liveMode) == binding.BaseMode &&
			researchV3HeadsEqual(binding.BaseHead, nullableResearchV3Head(liveVersion, liveDigest))
	case researchV3ExpectTargetHead:
		liveHeadMatches = types.ExecutionMode(liveMode) == types.ExecutionModeDiscoverAtRun &&
			researchV3HeadsEqual(&binding.Target, nullableResearchV3Head(liveVersion, liveDigest))
	case researchV3ExpectBaseOrTargetHead:
		liveHead := nullableResearchV3Head(liveVersion, liveDigest)
		liveHeadMatches = (types.ExecutionMode(liveMode) == binding.BaseMode &&
			researchV3HeadsEqual(binding.BaseHead, liveHead)) ||
			(types.ExecutionMode(liveMode) == types.ExecutionModeDiscoverAtRun &&
				researchV3HeadsEqual(&binding.Target, liveHead))
	default:
		return researchV3PreparedBinding{}, taskStateIntegrity()
	}
	definition, decodeErr := taskstate.DecodeApprovedDefinitionV3(payload)
	canonical, encodeErr := taskstate.EncodeApprovedDefinitionV3(definition)
	if decodeErr != nil || encodeErr != nil || !bytes.Equal(canonical, payload) ||
		schema != taskstate.ApprovedDefinitionSchemaVersionV3 ||
		definitionMode != string(types.ExecutionModeDiscoverAtRun) ||
		definition.TenantID != tenantID || definition.UserID != userID || definition.TaskID != taskID ||
		subtle.ConstantTimeCompare([]byte(binding.Target.Digest), []byte(taskstateDigest(payload))) != 1 {
		return researchV3PreparedBinding{}, taskStateIntegrity()
	}
	rebuilt, buildErr := taskstate.BuildApprovedDefinitionV3(taskstate.ApprovedDefinitionInputV3{
		TenantID: tenantID, UserID: userID, TaskID: taskID, TaskName: name,
		TaskManual: manual, SpecJSON: liveSpec, ExecutionMode: types.ExecutionModeDiscoverAtRun,
		Notification: definition.Notification, Output: definition.Output,
		PlannerBudget: definition.PlannerBudget, DeliveryPolicy: definition.DeliveryPolicy,
		TenantBudgetPolicy: definition.TenantBudgetPolicy,
	})
	rebuiltPayload, rebuiltErr := taskstate.EncodeApprovedDefinitionV3(rebuilt)
	strictness := types.DefaultStrictness
	if rawStrictness != nil {
		strictness = types.PushStrictness(*rawStrictness)
	}
	baseline, baselineErr := sealResearchV3SourceBaseline(
		definition, strictness, binding.BaseMode, binding.BaseHead)
	if buildErr != nil || rebuiltErr != nil || baselineErr != nil ||
		!bytes.Equal(rebuiltPayload, payload) || !liveHeadMatches ||
		subtle.ConstantTimeCompare([]byte(baseline),
			[]byte(binding.SourceBaselineDigest)) != 1 {
		return researchV3PreparedBinding{}, types.NewAppError(types.CodeConflict,
			"research V3 task changed after prepare; prepare and shadow it again", types.ErrConflict)
	}
	binding.Definition = definition
	return binding, nil
}

func nullableResearchV3Head(version *int64, digest *string) *types.ResearchV3DefinitionHead {
	if version == nil || digest == nil {
		return nil
	}
	return &types.ResearchV3DefinitionHead{Version: *version, Digest: *digest}
}

func bindResearchV3AppScopeTx(ctx context.Context, tx pgx.Tx, tenantID, userID int64) error {
	if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id',$1,true),
		set_config('app.user_id',$2,true)`, fmt.Sprint(tenantID), fmt.Sprint(userID)); err != nil {
		return taskStateDatabaseError("bind research V3 app scope", err)
	}
	return nil
}
