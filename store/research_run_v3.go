package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

const (
	researchRunPlanSchemaV3 = "vane.research-run-plan/v3"
	researchRunStepSchemaV3 = "vane.research-run-step/v3"
)

type CreateOrGetResearchRunPlanV3Params struct {
	Identity      types.RunIdentity
	RunSnapshotID int64
	Plan          runcontext.ResearchExecutionPlanV3
}

func (s *Store) CreateOrGetResearchRunSnapshotV3(
	ctx context.Context,
	identity types.RunIdentity,
	policy runtimepolicy.BundleV1,
) (types.ResearchRunSnapshotRefV3, error) {
	if identity.RunKind != types.RunSnapshotKindScheduled || identity.TenantID <= 0 ||
		identity.UserID <= 0 || identity.TaskID == "" ||
		identity.TemporalWorkflowID == "" || identity.TemporalRunID == "" {
		return types.ResearchRunSnapshotRefV3{}, researchRunValidationError("research snapshot identity is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return types.ResearchRunSnapshotRefV3{}, researchRunDatabaseError("begin research snapshot transaction", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := setResearchRunScopeV3(ctx, tx, identity.TenantID, identity.UserID); err != nil {
		return types.ResearchRunSnapshotRefV3{}, err
	}
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1,$2))`,
		identity.TemporalRunID, taskRunSnapshotLockSeed); err != nil {
		return types.ResearchRunSnapshotRefV3{}, researchRunDatabaseError("lock research snapshot", err)
	}
	lookup := CreateOrGetTaskRunSnapshotParams{
		TenantID: identity.TenantID, UserID: identity.UserID, TaskID: identity.TaskID,
		TemporalWorkflowID: identity.TemporalWorkflowID,
		TemporalRunID:      identity.TemporalRunID,
	}
	if existing, found, err := loadResearchRunSnapshotRowV3(ctx, tx, lookup); err != nil {
		return types.ResearchRunSnapshotRefV3{}, err
	} else if found {
		ref, err := validateStoredResearchRunSnapshotV3(identity, existing)
		if err != nil {
			return types.ResearchRunSnapshotRefV3{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return types.ResearchRunSnapshotRefV3{}, researchRunDatabaseError("commit research snapshot replay", err)
		}
		return ref, nil
	}
	if err := policy.Validate(); err != nil {
		return types.ResearchRunSnapshotRefV3{}, researchRunValidationError("research snapshot policy is invalid")
	}
	definitionVersion, definitionDigest, definition, err :=
		loadCurrentResearchDefinitionV3(ctx, tx, identity)
	if err != nil {
		return types.ResearchRunSnapshotRefV3{}, err
	}
	var cutoff time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&cutoff); err != nil {
		return types.ResearchRunSnapshotRefV3{}, researchRunDatabaseError("freeze research history cutoff", err)
	}
	cutoffText := cutoff.UTC().Format(time.RFC3339Nano)
	seal, err := runcontext.SealResearchSnapshotV3(runcontext.ResearchSnapshotV3{
		Identity: identity, DefinitionVersion: definitionVersion,
		HistoryThroughUTC: cutoffText, Definition: definition, Policy: policy,
	})
	if err != nil || seal.DefinitionDigest != definitionDigest {
		return types.ResearchRunSnapshotRefV3{}, researchRunIntegrityError()
	}
	var snapshotID int64
	if err := tx.QueryRow(ctx, `SELECT nextval('task_run_snapshots_id_seq')`).Scan(&snapshotID); err != nil {
		return types.ResearchRunSnapshotRefV3{}, researchRunDatabaseError("allocate research snapshot", err)
	}
	ref, err := types.SealResearchRunSnapshotRefV3(types.ResearchRunSnapshotRefV3{
		SnapshotID: snapshotID, TemporalWorkflowID: identity.TemporalWorkflowID,
		TemporalRunID: identity.TemporalRunID, RunKind: identity.RunKind,
		TenantID: identity.TenantID, UserID: identity.UserID, TaskID: identity.TaskID,
		DefinitionVersion: definitionVersion, DefinitionDigest: definitionDigest,
		CapabilityCatalogDigest: seal.PolicyDigests.CapabilityCatalogDigest,
		ToolPolicyDigest:        seal.PolicyDigests.ToolPolicyDigest,
		PromptPolicyDigest:      seal.PolicyDigests.PromptPolicyDigest,
		ModelPolicyDigest:       seal.PolicyDigests.ModelPolicyDigest,
		QuotaPolicyDigest:       seal.PolicyDigests.QuotaPolicyDigest,
		PlannerBudget:           definition.PlannerBudget, HistoryThroughUTC: cutoffText,
		PayloadDigest: seal.PayloadDigest,
	})
	if err != nil {
		return types.ResearchRunSnapshotRefV3{}, researchRunIntegrityError()
	}
	budgetJSON, _ := json.Marshal(definition.PlannerBudget)
	_, err = tx.Exec(ctx,
		`INSERT INTO task_run_snapshots (
		     id,tenant_id,user_id,task_id,temporal_workflow_id,temporal_run_id,
		     run_kind,execution_mode,adaptive_version,capability_catalog_digest,
		     tool_policy_digest,prompt_policy_digest,model_policy_digest,
		     quota_policy_digest,definition_digest,plan_digest,payload_digest,
		     reference_digest,reference_schema_version,payload,budget,created_at
		 ) VALUES ($1,$2,$3,$4,$5,$6,'scheduled','discover_at_run',0,$7,$8,$9,$10,$11,
		           $12,'',$13,$14,$15,$16,$17,$18)`,
		snapshotID, identity.TenantID, identity.UserID, identity.TaskID,
		identity.TemporalWorkflowID, identity.TemporalRunID,
		seal.PolicyDigests.CapabilityCatalogDigest, seal.PolicyDigests.ToolPolicyDigest,
		seal.PolicyDigests.PromptPolicyDigest, seal.PolicyDigests.ModelPolicyDigest,
		seal.PolicyDigests.QuotaPolicyDigest, definitionDigest, seal.PayloadDigest,
		ref.ReferenceDigest, types.ResearchRunSnapshotRefSchemaV3,
		seal.CanonicalPayload, budgetJSON, cutoff,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return types.ResearchRunSnapshotRefV3{}, researchRunConflictError()
		}
		return types.ResearchRunSnapshotRefV3{}, researchRunDatabaseError("insert research snapshot", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.ResearchRunSnapshotRefV3{}, researchRunDatabaseError("commit research snapshot", err)
	}
	return ref, nil
}

// loadResearchRunSnapshotRowV3 intentionally bypasses the legacy V1/V2
// decoder. That decoder is retained unchanged for Temporal replay and rejects
// every future reference schema by design; V3 validates its own exact payload
// immediately after this scope-only read.
func loadResearchRunSnapshotRowV3(
	ctx context.Context, q taskRunSnapshotQueryer, p CreateOrGetTaskRunSnapshotParams,
) (*taskRunSnapshot, bool, error) {
	if err := validateTaskRunLookupInput(p); err != nil {
		return nil, false, err
	}
	snapshot, err := scanTaskRunSnapshot(q.QueryRow(ctx,
		`SELECT `+taskRunSnapshotColumns+`
		   FROM task_run_snapshots
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		    AND temporal_workflow_id=$4 AND temporal_run_id=$5`,
		p.TenantID, p.UserID, p.TaskID, p.TemporalWorkflowID, p.TemporalRunID,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, researchRunDatabaseError("load research snapshot", err)
	}
	return snapshot, true, nil
}

func loadCurrentResearchDefinitionV3(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity,
) (int64, string, taskstate.ApprovedDefinitionV3, error) {
	var version int64
	var digest, schemaVersion string
	var payload, scheduleSpec []byte
	err := tx.QueryRow(ctx,
		`SELECT schedule.approved_definition_version,
		        schedule.approved_definition_digest,definition.schema_version,
		        definition.payload,schedule.spec_json
		   FROM schedules schedule
		   JOIN tenants tenant ON tenant.id=schedule.tenant_id AND tenant.status='active'
		   JOIN memberships membership
		     ON membership.tenant_id=schedule.tenant_id AND membership.user_id=schedule.user_id
		   JOIN task_approved_definition_versions definition
		     ON definition.tenant_id=schedule.tenant_id
		    AND definition.user_id=schedule.user_id AND definition.task_id=schedule.id
		    AND definition.version=schedule.approved_definition_version
		    AND definition.definition_digest=schedule.approved_definition_digest
		  WHERE schedule.id=$1 AND schedule.tenant_id=$2 AND schedule.user_id=$3
		    AND (schedule.status='active' OR (
		        schedule.status='paused' AND public.authorize_manual_task_run_v1(
		            schedule.tenant_id,schedule.user_id,schedule.id,$4
		        )
		    )) AND schedule.execution_mode='discover_at_run'
		  FOR SHARE OF schedule`,
		identity.TaskID, identity.TenantID, identity.UserID, identity.TemporalWorkflowID,
	).Scan(&version, &digest, &schemaVersion, &payload, &scheduleSpec)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, "", taskstate.ApprovedDefinitionV3{}, researchRunValidationError("research task is not active")
	}
	if err != nil {
		return 0, "", taskstate.ApprovedDefinitionV3{}, researchRunDatabaseError("load research definition", err)
	}
	if schemaVersion != taskstate.ApprovedDefinitionSchemaVersionV3 ||
		researchRunSHA256(payload) != digest {
		return 0, "", taskstate.ApprovedDefinitionV3{}, researchRunIntegrityError()
	}
	definition, err := taskstate.DecodeApprovedDefinitionV3(payload)
	canonicalScheduleSpec, scheduleErr := canonicalTaskRunJSONObject(scheduleSpec)
	if err != nil || definition.TenantID != identity.TenantID ||
		definition.UserID != identity.UserID || definition.TaskID != identity.TaskID ||
		definition.ExecutionMode != types.ExecutionModeDiscoverAtRun || scheduleErr != nil ||
		!bytes.Equal(canonicalScheduleSpec, definition.SpecJSON) {
		return 0, "", taskstate.ApprovedDefinitionV3{}, researchRunIntegrityError()
	}
	return version, digest, definition, nil
}

func validateStoredResearchRunSnapshotV3(
	identity types.RunIdentity, snapshot *taskRunSnapshot,
) (types.ResearchRunSnapshotRefV3, error) {
	if snapshot == nil || snapshot.ReferenceSchemaVersion != types.ResearchRunSnapshotRefSchemaV3 ||
		snapshot.Mode != types.ExecutionModeDiscoverAtRun || snapshot.AdaptiveVersion != 0 ||
		snapshot.PlanDigest != "" || snapshot.CreatedAt.IsZero() ||
		snapshot.RunKind != identity.RunKind || snapshot.TenantID != identity.TenantID ||
		snapshot.UserID != identity.UserID || snapshot.TaskID != identity.TaskID ||
		snapshot.TemporalWorkflowID != identity.TemporalWorkflowID ||
		snapshot.TemporalRunID != identity.TemporalRunID {
		return types.ResearchRunSnapshotRefV3{}, researchRunIntegrityError()
	}
	seal, err := runcontext.DecodeResearchSnapshotPayloadV3(snapshot.Payload)
	if err != nil || seal.PayloadDigest != snapshot.PayloadDigest ||
		seal.DefinitionDigest != snapshot.DefinitionDigest ||
		seal.PolicyDigests.CapabilityCatalogDigest != snapshot.CapabilityCatalogDigest ||
		seal.PolicyDigests.ToolPolicyDigest != snapshot.ToolPolicyDigest ||
		seal.PolicyDigests.PromptPolicyDigest != snapshot.PromptPolicyDigest ||
		seal.PolicyDigests.ModelPolicyDigest != snapshot.ModelPolicyDigest ||
		seal.PolicyDigests.QuotaPolicyDigest != snapshot.QuotaPolicyDigest {
		return types.ResearchRunSnapshotRefV3{}, researchRunIntegrityError()
	}
	ref, err := types.SealResearchRunSnapshotRefV3(types.ResearchRunSnapshotRefV3{
		SnapshotID: snapshot.ID, TemporalWorkflowID: identity.TemporalWorkflowID,
		TemporalRunID: identity.TemporalRunID, RunKind: identity.RunKind,
		TenantID: identity.TenantID, UserID: identity.UserID, TaskID: identity.TaskID,
		DefinitionVersion:       seal.Payload.DefinitionVersion,
		DefinitionDigest:        snapshot.DefinitionDigest,
		CapabilityCatalogDigest: snapshot.CapabilityCatalogDigest,
		ToolPolicyDigest:        snapshot.ToolPolicyDigest,
		PromptPolicyDigest:      snapshot.PromptPolicyDigest,
		ModelPolicyDigest:       snapshot.ModelPolicyDigest,
		QuotaPolicyDigest:       snapshot.QuotaPolicyDigest,
		PlannerBudget:           seal.Payload.PlannerBudget,
		HistoryThroughUTC:       seal.Payload.HistoryThroughUTC,
		PayloadDigest:           snapshot.PayloadDigest,
	})
	if err != nil || ref.ReferenceDigest != snapshot.ReferenceDigest {
		return types.ResearchRunSnapshotRefV3{}, researchRunIntegrityError()
	}
	return ref, nil
}

type ResearchRunStepPhaseV3 string

const (
	ResearchRunStepCompletedV3     ResearchRunStepPhaseV3 = "completed"
	ResearchRunStepFailedV3        ResearchRunStepPhaseV3 = "failed"
	ResearchRunStepIndeterminateV3 ResearchRunStepPhaseV3 = "indeterminate"
)

type ResearchRunStepExecutionV3 struct {
	StepID        int64
	FirstWriter   bool
	Ordinal       int
	InvocationID  string
	ToolName      string
	Arguments     json.RawMessage
	RequestDigest string
}

type CommitResearchRunStepV3Params struct {
	Identity      types.RunIdentity
	RunSnapshotID int64
	PlanRef       types.ResearchRunPlanRefV3
	Ordinal       int
	Phase         ResearchRunStepPhaseV3
	ResultDigest  string
	CostMicroUSD  int64
	ErrorCode     string
}

type ResearchRunStepReceiptV3 struct {
	StepID        int64
	Ordinal       int
	Phase         ResearchRunStepPhaseV3
	InvocationID  string
	ToolName      string
	RequestDigest string
	ResultDigest  string
	CostMicroUSD  int64
	ErrorCode     string
}

type researchRunPlanRowV3 struct {
	ID                      int64
	TenantID                int64
	UserID                  int64
	TaskID                  string
	RunSnapshotID           int64
	TemporalWorkflowID      string
	TemporalRunID           string
	DefinitionDigest        string
	CapabilityCatalogDigest string
	PlanDigest              string
	PlanPayload             []byte
}

func (s *Store) CreateOrGetResearchRunPlanV3(
	ctx context.Context,
	params CreateOrGetResearchRunPlanV3Params,
) (types.ResearchRunPlanRefV3, error) {
	if err := validateResearchPlanScopeV3(params); err != nil {
		return types.ResearchRunPlanRefV3{}, err
	}

	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return types.ResearchRunPlanRefV3{}, researchRunDatabaseError("begin research plan transaction", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := setResearchRunScopeV3(ctx, tx, params.Identity.TenantID, params.Identity.UserID); err != nil {
		return types.ResearchRunPlanRefV3{}, err
	}
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1,0))`,
		"research-plan/v3:"+params.Identity.TemporalRunID); err != nil {
		return types.ResearchRunPlanRefV3{}, researchRunDatabaseError("lock research plan", err)
	}
	row, found, err := loadResearchRunPlanV3(ctx, tx, params.Identity, params.RunSnapshotID)
	if err != nil {
		return types.ResearchRunPlanRefV3{}, err
	}
	if !found {
		if err := validateResearchPlanCreateV3(params); err != nil {
			return types.ResearchRunPlanRefV3{}, err
		}
		payload, encodeErr := runcontext.EncodeResearchExecutionPlanV3(params.Plan)
		if encodeErr != nil {
			return types.ResearchRunPlanRefV3{}, researchRunValidationError("research plan payload is invalid")
		}
		planDigest := researchRunSHA256(payload)
		row, err = insertResearchRunPlanV3(ctx, tx, params, payload, planDigest)
		if err != nil {
			return types.ResearchRunPlanRefV3{}, err
		}
	}
	ref, err := researchRunPlanRefV3(row)
	if err != nil {
		return types.ResearchRunPlanRefV3{}, researchRunIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return types.ResearchRunPlanRefV3{}, researchRunDatabaseError("commit research plan transaction", err)
	}
	return ref, nil
}

func (s *Store) BeginResearchRunStepV3(
	ctx context.Context,
	identity types.RunIdentity,
	runSnapshotID int64,
	planRef types.ResearchRunPlanRefV3,
	ordinal int,
) (ResearchRunStepExecutionV3, error) {
	if err := planRef.ValidateFor(identity, runSnapshotID); err != nil || ordinal < 0 || ordinal >= 16 {
		return ResearchRunStepExecutionV3{}, researchRunValidationError("research step scope is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ResearchRunStepExecutionV3{}, researchRunDatabaseError("begin research step transaction", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := setResearchRunScopeV3(ctx, tx, identity.TenantID, identity.UserID); err != nil {
		return ResearchRunStepExecutionV3{}, err
	}
	if err := lockResearchRunStepV3(ctx, tx, identity.TemporalRunID, planRef.PlanDigest, ordinal); err != nil {
		return ResearchRunStepExecutionV3{}, err
	}
	plan, row, err := loadAndValidateResearchRunPlanV3(ctx, tx, identity, runSnapshotID, planRef)
	if err != nil {
		return ResearchRunStepExecutionV3{}, err
	}
	if ordinal >= len(plan.Steps) {
		return ResearchRunStepExecutionV3{}, researchRunValidationError("research step ordinal is outside the plan")
	}
	if err := authorizeResearchRunEffectV3(ctx, tx, identity); err != nil {
		return ResearchRunStepExecutionV3{}, err
	}
	step := plan.Steps[ordinal]
	requestDigest := digestResearchRunStepRequestV3(ordinal, step)
	var stepID int64
	firstWriter := true
	err = tx.QueryRow(ctx,
		`INSERT INTO research_run_steps (
		     tenant_id,user_id,task_id,plan_id,temporal_run_id,plan_digest,
		     step_ordinal,phase,invocation_id,tool_name,request_digest,
		     result_digest,cost_micro_usd,error_code,schema_version
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,'started',$8,$9,$10,NULL,0,NULL,$11)
		 ON CONFLICT (tenant_id,user_id,temporal_run_id,plan_digest,step_ordinal,phase)
		 DO NOTHING RETURNING id`,
		identity.TenantID, identity.UserID, identity.TaskID, row.ID,
		identity.TemporalRunID, planRef.PlanDigest, ordinal,
		step.InvocationID, step.ToolName, requestDigest, researchRunStepSchemaV3,
	).Scan(&stepID)
	if errors.Is(err, pgx.ErrNoRows) {
		firstWriter = false
		err = tx.QueryRow(ctx,
			`SELECT id FROM research_run_steps
			 WHERE tenant_id=$1 AND user_id=$2 AND temporal_run_id=$3
			   AND plan_digest=$4 AND step_ordinal=$5 AND phase='started'
			   AND plan_id=$6 AND invocation_id=$7 AND tool_name=$8
			   AND request_digest=$9`,
			identity.TenantID, identity.UserID, identity.TemporalRunID,
			planRef.PlanDigest, ordinal, row.ID, step.InvocationID,
			step.ToolName, requestDigest).Scan(&stepID)
		if errors.Is(err, pgx.ErrNoRows) {
			return ResearchRunStepExecutionV3{}, researchRunConflictError()
		}
	}
	if err != nil {
		return ResearchRunStepExecutionV3{}, researchRunDatabaseError("seal research step start", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ResearchRunStepExecutionV3{}, researchRunDatabaseError("commit research step start", err)
	}
	return ResearchRunStepExecutionV3{
		StepID: stepID, FirstWriter: firstWriter,
		Ordinal: ordinal, InvocationID: step.InvocationID,
		ToolName: step.ToolName, Arguments: append(json.RawMessage(nil), step.Arguments...),
		RequestDigest: requestDigest,
	}, nil
}

func (s *Store) CommitResearchRunStepV3(
	ctx context.Context,
	params CommitResearchRunStepV3Params,
) (ResearchRunStepReceiptV3, error) {
	if err := validateResearchRunStepCommitV3(params); err != nil {
		return ResearchRunStepReceiptV3{}, err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ResearchRunStepReceiptV3{}, researchRunDatabaseError("begin research step receipt transaction", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := setResearchRunScopeV3(ctx, tx, params.Identity.TenantID, params.Identity.UserID); err != nil {
		return ResearchRunStepReceiptV3{}, err
	}
	if err := lockResearchRunStepV3(ctx, tx, params.Identity.TemporalRunID, params.PlanRef.PlanDigest, params.Ordinal); err != nil {
		return ResearchRunStepReceiptV3{}, err
	}
	plan, row, err := loadAndValidateResearchRunPlanV3(
		ctx, tx, params.Identity, params.RunSnapshotID, params.PlanRef)
	if err != nil {
		return ResearchRunStepReceiptV3{}, err
	}
	if params.Ordinal >= len(plan.Steps) {
		return ResearchRunStepReceiptV3{}, researchRunValidationError("research step ordinal is outside the plan")
	}
	step := plan.Steps[params.Ordinal]
	requestDigest := digestResearchRunStepRequestV3(params.Ordinal, step)
	if receipt, found, err := loadResearchRunTerminalStepV3(
		ctx, tx, params, row.ID, step, requestDigest,
	); err != nil {
		return ResearchRunStepReceiptV3{}, err
	} else if found {
		if err := tx.Commit(ctx); err != nil {
			return ResearchRunStepReceiptV3{}, researchRunDatabaseError("commit research step replay", err)
		}
		return receipt, nil
	}
	var stepID int64
	err = tx.QueryRow(ctx,
		`INSERT INTO research_run_steps (
		     tenant_id,user_id,task_id,plan_id,temporal_run_id,plan_digest,
		     step_ordinal,phase,invocation_id,tool_name,request_digest,
		     result_digest,cost_micro_usd,error_code,schema_version
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		 RETURNING id`,
		params.Identity.TenantID, params.Identity.UserID, params.Identity.TaskID,
		row.ID, params.Identity.TemporalRunID, params.PlanRef.PlanDigest,
		params.Ordinal, params.Phase, step.InvocationID, step.ToolName,
		requestDigest, nullableResearchString(params.ResultDigest),
		params.CostMicroUSD, nullableResearchString(params.ErrorCode),
		researchRunStepSchemaV3,
	).Scan(&stepID)
	if err != nil {
		return ResearchRunStepReceiptV3{}, researchRunDatabaseError("seal research step terminal receipt", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ResearchRunStepReceiptV3{}, researchRunDatabaseError("commit research step terminal receipt", err)
	}
	return researchRunStepReceiptV3(stepID, params, step, requestDigest), nil
}

func validateResearchPlanCreateV3(params CreateOrGetResearchRunPlanV3Params) error {
	if err := validateResearchPlanScopeV3(params); err != nil {
		return err
	}
	if params.Plan.Validate() != nil || params.Plan.DefinitionDigest == "" ||
		params.Plan.CapabilityCatalogDigest == "" {
		return researchRunValidationError("research plan payload is invalid")
	}
	return nil
}

func validateResearchPlanScopeV3(params CreateOrGetResearchRunPlanV3Params) error {
	identity := params.Identity
	if identity.RunKind != types.RunSnapshotKindScheduled || params.RunSnapshotID <= 0 ||
		identity.TenantID <= 0 || identity.UserID <= 0 ||
		strings.TrimSpace(identity.TaskID) == "" || strings.TrimSpace(identity.TaskID) != identity.TaskID ||
		strings.TrimSpace(identity.TemporalWorkflowID) == "" ||
		strings.TrimSpace(identity.TemporalRunID) == "" {
		return researchRunValidationError("research plan scope is invalid")
	}
	return nil
}

func insertResearchRunPlanV3(
	ctx context.Context, tx pgx.Tx,
	params CreateOrGetResearchRunPlanV3Params,
	payload []byte, planDigest string,
) (researchRunPlanRowV3, error) {
	identity := params.Identity
	row, err := scanResearchRunPlanV3(tx.QueryRow(ctx,
		`INSERT INTO research_run_plans (
		     tenant_id,user_id,task_id,run_snapshot_id,temporal_workflow_id,
		     temporal_run_id,definition_digest,capability_catalog_digest,
		     plan_digest,plan_payload,schema_version
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		 RETURNING id,tenant_id,user_id,task_id,run_snapshot_id,
		           temporal_workflow_id,temporal_run_id,definition_digest,
		           capability_catalog_digest,plan_digest,plan_payload`,
		identity.TenantID, identity.UserID, identity.TaskID, params.RunSnapshotID,
		identity.TemporalWorkflowID, identity.TemporalRunID,
		params.Plan.DefinitionDigest, params.Plan.CapabilityCatalogDigest,
		planDigest, payload, researchRunPlanSchemaV3))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return researchRunPlanRowV3{}, researchRunConflictError()
		}
		return researchRunPlanRowV3{}, researchRunDatabaseError("insert research run plan", err)
	}
	return row, nil
}

func loadResearchRunPlanV3(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity, snapshotID int64,
) (researchRunPlanRowV3, bool, error) {
	row, err := scanResearchRunPlanV3(tx.QueryRow(ctx,
		`SELECT plan.id,plan.tenant_id,plan.user_id,plan.task_id,plan.run_snapshot_id,
		        plan.temporal_workflow_id,plan.temporal_run_id,plan.definition_digest,
		        plan.capability_catalog_digest,plan.plan_digest,plan.plan_payload
		   FROM research_run_plans plan
		   JOIN task_run_snapshots snapshot
		     ON snapshot.id=plan.run_snapshot_id
		    AND snapshot.tenant_id=plan.tenant_id
		    AND snapshot.user_id=plan.user_id AND snapshot.task_id=plan.task_id
		    AND snapshot.temporal_workflow_id=plan.temporal_workflow_id
		    AND snapshot.temporal_run_id=plan.temporal_run_id
		  WHERE plan.tenant_id=$1 AND plan.user_id=$2 AND plan.task_id=$3
		    AND plan.run_snapshot_id=$4 AND plan.temporal_workflow_id=$5
		    AND plan.temporal_run_id=$6
		    AND snapshot.reference_schema_version=$7`,
		identity.TenantID, identity.UserID, identity.TaskID, snapshotID,
		identity.TemporalWorkflowID, identity.TemporalRunID,
		types.ResearchRunSnapshotRefSchemaV3))
	if errors.Is(err, pgx.ErrNoRows) {
		return researchRunPlanRowV3{}, false, nil
	}
	if err != nil {
		return researchRunPlanRowV3{}, false, researchRunDatabaseError("load research run plan", err)
	}
	if researchRunSHA256(row.PlanPayload) != row.PlanDigest {
		return researchRunPlanRowV3{}, false, researchRunIntegrityError()
	}
	plan, decodeErr := runcontext.DecodeResearchExecutionPlanV3(row.PlanPayload)
	if decodeErr != nil || plan.DefinitionDigest != row.DefinitionDigest ||
		plan.CapabilityCatalogDigest != row.CapabilityCatalogDigest {
		return researchRunPlanRowV3{}, false, researchRunIntegrityError()
	}
	return row, true, nil
}

type researchRunPlanScannerV3 interface{ Scan(...any) error }

func scanResearchRunPlanV3(scanner researchRunPlanScannerV3) (researchRunPlanRowV3, error) {
	var row researchRunPlanRowV3
	err := scanner.Scan(&row.ID, &row.TenantID, &row.UserID, &row.TaskID,
		&row.RunSnapshotID, &row.TemporalWorkflowID, &row.TemporalRunID,
		&row.DefinitionDigest, &row.CapabilityCatalogDigest,
		&row.PlanDigest, &row.PlanPayload)
	return row, err
}

func researchRunPlanRefV3(row researchRunPlanRowV3) (types.ResearchRunPlanRefV3, error) {
	return types.SealResearchRunPlanRefV3(types.ResearchRunPlanRefV3{
		PlanID: row.ID, RunSnapshotID: row.RunSnapshotID,
		TemporalWorkflowID: row.TemporalWorkflowID, TemporalRunID: row.TemporalRunID,
		TenantID: row.TenantID, UserID: row.UserID, TaskID: row.TaskID,
		DefinitionDigest:        row.DefinitionDigest,
		CapabilityCatalogDigest: row.CapabilityCatalogDigest,
		PlanDigest:              row.PlanDigest,
	})
}

func loadAndValidateResearchRunPlanV3(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity, snapshotID int64,
	ref types.ResearchRunPlanRefV3,
) (runcontext.ResearchExecutionPlanV3, researchRunPlanRowV3, error) {
	row, found, err := loadResearchRunPlanV3(ctx, tx, identity, snapshotID)
	if err != nil {
		return runcontext.ResearchExecutionPlanV3{}, researchRunPlanRowV3{}, err
	}
	if !found || row.ID != ref.PlanID || row.PlanDigest != ref.PlanDigest ||
		row.DefinitionDigest != ref.DefinitionDigest ||
		row.CapabilityCatalogDigest != ref.CapabilityCatalogDigest {
		return runcontext.ResearchExecutionPlanV3{}, researchRunPlanRowV3{}, researchRunConflictError()
	}
	plan, err := runcontext.DecodeResearchExecutionPlanV3(row.PlanPayload)
	if err != nil || plan.DefinitionDigest != row.DefinitionDigest ||
		plan.CapabilityCatalogDigest != row.CapabilityCatalogDigest {
		return runcontext.ResearchExecutionPlanV3{}, researchRunPlanRowV3{}, researchRunIntegrityError()
	}
	return plan, row, nil
}

func authorizeResearchRunEffectV3(ctx context.Context, tx pgx.Tx, identity types.RunIdentity) error {
	var authorized bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
		     SELECT 1 FROM schedules schedule
		     JOIN tenants tenant ON tenant.id=schedule.tenant_id AND tenant.status='active'
		     JOIN memberships membership
		       ON membership.tenant_id=schedule.tenant_id AND membership.user_id=schedule.user_id
		    WHERE schedule.id=$1 AND schedule.tenant_id=$2 AND schedule.user_id=$3
		      AND (schedule.status='active' OR (
		          schedule.status='paused' AND public.authorize_manual_task_run_v1(
		              schedule.tenant_id,schedule.user_id,schedule.id,$4
		          )
		      )) AND schedule.execution_mode='discover_at_run'
		 )`, identity.TaskID, identity.TenantID, identity.UserID,
		identity.TemporalWorkflowID).Scan(&authorized); err != nil {
		return researchRunDatabaseError("authorize research run effect", err)
	}
	if !authorized {
		return types.NewAppError(types.CodeValidation,
			"research run 当前不允许外部调用", types.ErrValidation)
	}
	return nil
}

func validateResearchRunStepCommitV3(params CommitResearchRunStepV3Params) error {
	if err := params.PlanRef.ValidateFor(params.Identity, params.RunSnapshotID); err != nil ||
		params.Ordinal < 0 || params.Ordinal >= 16 || params.CostMicroUSD < 0 ||
		(params.Phase != ResearchRunStepCompletedV3 &&
			params.Phase != ResearchRunStepFailedV3 &&
			params.Phase != ResearchRunStepIndeterminateV3) {
		return researchRunValidationError("research terminal step is invalid")
	}
	switch params.Phase {
	case ResearchRunStepCompletedV3:
		if !validResearchRunDigest(params.ResultDigest) || params.ErrorCode != "" {
			return researchRunValidationError("completed research step receipt is invalid")
		}
	case ResearchRunStepFailedV3, ResearchRunStepIndeterminateV3:
		if params.ResultDigest != "" || !validResearchRunErrorCode(params.ErrorCode) {
			return researchRunValidationError("failed research step receipt is invalid")
		}
	}
	return nil
}

func loadResearchRunTerminalStepV3(
	ctx context.Context, tx pgx.Tx, params CommitResearchRunStepV3Params,
	planID int64, step runcontext.ResearchPlanStepV3, requestDigest string,
) (ResearchRunStepReceiptV3, bool, error) {
	var receipt ResearchRunStepReceiptV3
	var phase string
	var storedPlanID int64
	var resultDigest, errorCode *string
	err := tx.QueryRow(ctx,
		`SELECT id,plan_id,step_ordinal,phase,invocation_id,tool_name,request_digest,
		        result_digest,cost_micro_usd,error_code
		   FROM research_run_steps
		  WHERE tenant_id=$1 AND user_id=$2 AND temporal_run_id=$3
		    AND plan_digest=$4 AND step_ordinal=$5
		    AND phase IN ('completed','failed','indeterminate')`,
		params.Identity.TenantID, params.Identity.UserID,
		params.Identity.TemporalRunID, params.PlanRef.PlanDigest,
		params.Ordinal).Scan(&receipt.StepID, &storedPlanID, &receipt.Ordinal, &phase,
		&receipt.InvocationID, &receipt.ToolName, &receipt.RequestDigest,
		&resultDigest, &receipt.CostMicroUSD, &errorCode)
	if errors.Is(err, pgx.ErrNoRows) {
		return ResearchRunStepReceiptV3{}, false, nil
	}
	if err != nil {
		return ResearchRunStepReceiptV3{}, false, researchRunDatabaseError("load research step receipt", err)
	}
	receipt.Phase = ResearchRunStepPhaseV3(phase)
	if resultDigest != nil {
		receipt.ResultDigest = *resultDigest
	}
	if errorCode != nil {
		receipt.ErrorCode = *errorCode
	}
	expected := researchRunStepReceiptV3(receipt.StepID, params, step, requestDigest)
	if storedPlanID != planID || receipt != expected {
		return ResearchRunStepReceiptV3{}, false, researchRunConflictError()
	}
	return receipt, true, nil
}

func researchRunStepReceiptV3(
	stepID int64, params CommitResearchRunStepV3Params,
	step runcontext.ResearchPlanStepV3, requestDigest string,
) ResearchRunStepReceiptV3 {
	return ResearchRunStepReceiptV3{
		StepID: stepID, Ordinal: params.Ordinal, Phase: params.Phase,
		InvocationID: step.InvocationID, ToolName: step.ToolName,
		RequestDigest: requestDigest, ResultDigest: params.ResultDigest,
		CostMicroUSD: params.CostMicroUSD, ErrorCode: params.ErrorCode,
	}
}

func digestResearchRunStepRequestV3(ordinal int, step runcontext.ResearchPlanStepV3) string {
	payload, _ := json.Marshal(struct {
		Schema       string          `json:"schema"`
		Ordinal      int             `json:"ordinal"`
		InvocationID string          `json:"invocation_id"`
		ToolName     string          `json:"tool_name"`
		Arguments    json.RawMessage `json:"arguments"`
	}{"vane.research-step-request/v3", ordinal, step.InvocationID, step.ToolName, step.Arguments})
	return researchRunSHA256(payload)
}

func lockResearchRunStepV3(
	ctx context.Context, tx pgx.Tx, runID, planDigest string, ordinal int,
) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`,
		fmt.Sprintf("research-step/v3:%s:%s:%d", runID, planDigest, ordinal))
	if err != nil {
		return researchRunDatabaseError("lock research run step", err)
	}
	return nil
}

func setResearchRunScopeV3(ctx context.Context, tx pgx.Tx, tenantID, userID int64) error {
	if tenantID <= 0 || userID <= 0 {
		return researchRunValidationError("research run scope is invalid")
	}
	if _, err := tx.Exec(ctx, `SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		return researchRunDatabaseError("set research run search path", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10), strconv.FormatInt(userID, 10)); err != nil {
		return researchRunDatabaseError("set research run scope", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return researchRunDatabaseError("enter research run role", err)
	}
	return nil
}

func nullableResearchString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func validResearchRunErrorCode(value string) bool {
	return value != "" && len(value) <= 128 && strings.TrimSpace(value) == value
}

func validResearchRunDigest(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func researchRunSHA256(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func researchRunValidationError(message string) error {
	return types.NewAppError(types.CodeValidation, message, types.ErrValidation)
}

func researchRunConflictError() error {
	return types.NewAppError(types.CodeConflict,
		"research run 已存在不一致的不可变记录", types.ErrConflict)
}

func researchRunIntegrityError() error {
	return types.NewAppError(types.CodeConflict,
		"research run 不可变记录完整性校验失败", types.ErrConflict)
}

func researchRunDatabaseError(message string, err error) error {
	return types.NewAppError(types.CodeDatabase, message, err)
}
