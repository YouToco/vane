package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/types"
)

const (
	taskRunSnapshotPayloadVersion  = "vane.task-run-snapshot-payload/v1"
	taskRunLegacyDefinitionVersion = "vane.task-run-legacy-definition/v1"
	taskRunPlanDigestVersion       = "vane.task-run-execution-plan/v1"
	taskRunPolicyDigestVersion     = "vane.runtime-policy-digest/v1"

	taskRunSourceScopeApproved = "approved_plan"

	maxTaskRunReferenceBytes = 512
	maxTaskRunPayloadBytes   = 2 << 20
	maxTaskRunJSONBytes      = 256 << 10

	// taskRunSnapshotLockSeed domains PostgreSQL's stable hashtextextended
	// result for per-Temporal-Run advisory locks. Hash collisions only serialize
	// unrelated runs; they cannot merge identities or change the unique arbiter.
	taskRunSnapshotLockSeed int64 = 0x56414e45
)

// CreateOrGetTaskRunSnapshotParams contains the explicit run scope and the
// live policy references observed at Activity start. The scoped idempotency key
// is Tenant/User/Task/WorkflowID/RunID. Once that key has a committed row,
// every later field is deliberately ignored: response-lost retries must reuse
// the first committed snapshot even after a deployment, task edit, or policy
// change. RunID additionally has a global anti-misrouting unique constraint.
//
// The five policy JSON values must come from C1's typed non-secret policy
// builder. Never pass generic runtime config or credential objects here. C1a has
// zero production call points; strict JSON persistence alone cannot prove that
// an arbitrary caller-provided object is secret-free.
type CreateOrGetTaskRunSnapshotParams struct {
	TenantID              int64
	UserID                int64
	TaskID                string
	TemporalWorkflowID    string
	TemporalRunID         string
	Mode                  types.ExecutionMode
	AdaptiveVersion       int64
	CapabilityCatalogJSON json.RawMessage
	ToolPolicyJSON        json.RawMessage
	PromptPolicyJSON      json.RawMessage
	ModelPolicyJSON       json.RawMessage
	QuotaPolicyJSON       json.RawMessage
	BudgetJSON            json.RawMessage
	ObservationRollout    observation.RolloutMode
}

// taskRunSnapshot is the private immutable database row. It must never cross
// the store boundary because Payload contains playbook, URL/config, prompts,
// model configuration, and quota policy bodies.
type taskRunSnapshot struct {
	ID                      int64
	TenantID                int64
	UserID                  int64
	TaskID                  string
	TemporalWorkflowID      string
	TemporalRunID           string
	RunKind                 types.RunSnapshotKind
	Mode                    types.ExecutionMode
	AdaptiveVersion         int64
	CapabilityCatalogDigest string
	ToolPolicyDigest        string
	PromptPolicyDigest      string
	ModelPolicyDigest       string
	QuotaPolicyDigest       string
	DefinitionDigest        string
	PlanDigest              string
	PayloadDigest           string
	ReferenceDigest         string
	ReferenceSchemaVersion  string
	Payload                 json.RawMessage
	BudgetJSON              json.RawMessage
	V2CutoverEventID        *int64
	CreatedAt               time.Time
}

const taskRunSnapshotColumns = `id, tenant_id, user_id, task_id,
	temporal_workflow_id, temporal_run_id, run_kind, execution_mode, adaptive_version,
	capability_catalog_digest, tool_policy_digest, prompt_policy_digest,
	model_policy_digest, quota_policy_digest, definition_digest, plan_digest,
	payload_digest, reference_digest, reference_schema_version, payload, budget,
	v2_cutover_event_id, created_at`

type taskRunSnapshotQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// createOrGetTaskRunSnapshot is the package-private raw-JSON persistence
// primitive. C1 production code must enter through the typed
// CreateOrGetCompiledTaskRunSnapshotV1 adapter.
func (s *Store) createOrGetTaskRunSnapshot(
	ctx context.Context,
	p CreateOrGetTaskRunSnapshotParams,
) (*taskRunSnapshot, error) {
	return s.createOrGetTaskRunSnapshotWithShadowV2(ctx, p, false)
}

func (s *Store) createOrGetTaskRunSnapshotWithShadowV2(
	ctx context.Context,
	p CreateOrGetTaskRunSnapshotParams,
	shadowV2 bool,
) (*taskRunSnapshot, error) {
	if err := validateTaskRunLookupInput(p); err != nil {
		return nil, err
	}

	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, taskRunDatabaseError("begin task run snapshot transaction", err)
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := setTaskRunTenantContext(ctx, tx, p.TenantID); err != nil {
		return nil, err
	}
	if err := lockTaskRunSnapshotRun(ctx, tx, p.TemporalRunID); err != nil {
		return nil, err
	}

	if existing, found, err := loadTaskRunSnapshot(ctx, tx, p); err != nil {
		return nil, err
	} else if found {
		if shadowV2 {
			if _, err := validateExistingTaskRunSnapshotShadowV2(
				ctx, tx, existing); err != nil {
				return nil, err
			}
		}
		return existing, nil
	}
	v2CutoverEventID, cutoverEnrolled, err := loadTaskRunSnapshotAdmissionV2(
		ctx, tx, p.TenantID, p.UserID, p.TaskID)
	if err != nil {
		return nil, err
	}
	if cutoverEnrolled {
		// Active admission is durable database state, not a rollout flag.
		// Active and rolled-back tasks both need a complete sidecar population:
		// rollback runs stay v1-authoritative but must be auditable before a
		// later reactivation.
		shadowV2 = true
	}
	if err := validateNewTaskRunInput(p); err != nil {
		return nil, err
	}
	policies, _, err := canonicalTaskRunPolicies(p)
	if err != nil {
		return nil, err
	}
	budget, budgetJSON, err := canonicalTaskRunBudget(p.BudgetJSON)
	if err != nil {
		return nil, err
	}
	if err := lockTaskRunMembership(ctx, tx, p.TenantID, p.UserID); err != nil {
		return nil, err
	}

	definition, _, _, err := loadTaskRunDefinition(ctx, tx, p)
	if err != nil {
		return nil, err
	}
	payload := taskRunSnapshotPayload{
		SchemaVersion:          taskRunSnapshotPayloadVersion,
		TenantID:               p.TenantID,
		UserID:                 p.UserID,
		TaskID:                 p.TaskID,
		RunKind:                types.RunSnapshotKindScheduled,
		Mode:                   p.Mode,
		AdaptiveVersion:        p.AdaptiveVersion,
		ObservationRollout:     p.ObservationRollout,
		Policies:               policies,
		Budget:                 budget,
		Definition:             definition,
		ReferenceSchemaVersion: types.RunSnapshotSchemaVersion,
	}
	preparedPayload, err := canonicalizeTaskRunPayloadForWrite(&payload)
	if err != nil {
		return nil, taskRunValidationError("task run snapshot payload is invalid")
	}
	payloadJSON := preparedPayload.Canonical
	definitionDigest := preparedPayload.DefinitionDigest
	planDigest := preparedPayload.PlanDigest
	policyDigests := preparedPayload.PolicyDigests
	payloadDigest := sha256Hex(payloadJSON)
	var snapshotID int64
	if err := tx.QueryRow(ctx,
		`SELECT nextval('task_run_snapshots_id_seq')`).Scan(&snapshotID); err != nil {
		return nil, taskRunDatabaseError("allocate task run snapshot id", err)
	}
	candidate := &taskRunSnapshot{
		ID: snapshotID, TenantID: p.TenantID, UserID: p.UserID, TaskID: p.TaskID,
		TemporalWorkflowID: p.TemporalWorkflowID, TemporalRunID: p.TemporalRunID,
		RunKind: types.RunSnapshotKindScheduled, Mode: p.Mode,
		AdaptiveVersion:         p.AdaptiveVersion,
		CapabilityCatalogDigest: policyDigests.CapabilityCatalog,
		ToolPolicyDigest:        policyDigests.ToolPolicy,
		PromptPolicyDigest:      policyDigests.PromptPolicy,
		ModelPolicyDigest:       policyDigests.ModelPolicy,
		QuotaPolicyDigest:       policyDigests.QuotaPolicy,
		DefinitionDigest:        definitionDigest,
		PlanDigest:              planDigest,
		PayloadDigest:           payloadDigest,
		Payload:                 payloadJSON,
		BudgetJSON:              budgetJSON,
		V2CutoverEventID:        v2CutoverEventID,
		ReferenceSchemaVersion:  types.RunSnapshotSchemaVersion,
	}
	sealedRef, err := sealTaskRunSnapshotReferenceV1(candidate, taskRunBudgetV1{
		MaxPlannerRounds: budget.MaxPlannerRounds,
		MaxToolCalls:     budget.MaxToolCalls,
		MaxTokens:        budget.MaxTokens,
		MaxCostMicroUSD:  budget.MaxCostMicroUSD,
		DurationMs:       budget.DurationMs,
	})
	if err != nil {
		return nil, taskRunIntegrityError()
	}
	candidate.ReferenceDigest = sealedRef.ReferenceDigest
	var shadow taskRunSnapshotShadowV2
	if shadowV2 {
		shadow, err = buildTaskRunSnapshotShadowV2(ctx, tx, candidate)
		if err != nil {
			return nil, err
		}
	}

	// RunID is the conflict arbiter because it is globally unique. Using only
	// the scoped unique here is racy on PostgreSQL: concurrent identical writers
	// may surface the non-arbiter RunID constraint as 23505 instead of DO NOTHING.
	// After an empty RETURNING, serialization failure, or a non-arbiter 23505, a
	// fresh transaction distinguishes the exact scoped winner from a real
	// cross-scope/workflow RunID collision.
	inserted, err := scanTaskRunSnapshot(tx.QueryRow(ctx,
		`INSERT INTO task_run_snapshots (
			id, tenant_id, user_id, task_id, temporal_workflow_id, temporal_run_id,
			run_kind, execution_mode, adaptive_version, capability_catalog_digest,
			tool_policy_digest, prompt_policy_digest, model_policy_digest,
			quota_policy_digest, definition_digest, plan_digest, payload_digest,
			reference_digest, reference_schema_version, payload, budget,
			v2_cutover_event_id
		 ) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
			$15, $16, $17, $18, $19, $20, $21, $22
		 ) ON CONFLICT (temporal_run_id) DO NOTHING
		 RETURNING `+taskRunSnapshotColumns,
		candidate.ID, candidate.TenantID, candidate.UserID, candidate.TaskID,
		candidate.TemporalWorkflowID, candidate.TemporalRunID, string(candidate.RunKind),
		string(candidate.Mode), candidate.AdaptiveVersion, candidate.CapabilityCatalogDigest,
		candidate.ToolPolicyDigest, candidate.PromptPolicyDigest,
		candidate.ModelPolicyDigest, candidate.QuotaPolicyDigest,
		candidate.DefinitionDigest, candidate.PlanDigest, candidate.PayloadDigest,
		candidate.ReferenceDigest, candidate.ReferenceSchemaVersion,
		candidate.Payload, candidate.BudgetJSON, candidate.V2CutoverEventID,
	))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || taskRunSerializationFailure(err) ||
			isUniqueViolation(err) {
			rollbackCompiledTaskTx(ctx, tx)
			return loadTaskRunWinnerOrConflict(ctx, s.pool, p)
		}
		return nil, taskRunDatabaseError("insert task run snapshot", err)
	}
	if err := validateStoredTaskRunSnapshot(inserted, p); err != nil {
		return nil, err
	}
	if shadowV2 {
		if err := insertTaskRunSnapshotShadowV2(ctx, tx, shadow); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		if shadowV2 {
			rollbackCompiledTaskTx(ctx, tx)
			recoveryCtx, cancel := context.WithTimeout(
				context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			winner, found, loadErr := s.loadTaskRunSnapshotShadowBehindFenceV2(
				recoveryCtx, p)
			if loadErr != nil {
				return nil, loadErr
			}
			if found {
				return winner, nil
			}
		}
		return nil, taskRunDatabaseError("commit task run snapshot transaction", err)
	}
	return inserted, nil
}

// loadTaskRunSnapshotAdmissionV2 is called only after exact RunID replay has
// missed. Its schedule lock is the first mutable task lock for a genuinely new
// run and is held through parent+sidecar commit, excluding operator/purge/
// delete transitions. The marker is derived only from the current pointer;
// callers cannot supply or select a historical event.
func loadTaskRunSnapshotAdmissionV2(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID int64,
	taskID string,
) (*int64, bool, error) {
	var pointer *int64
	if err := tx.QueryRow(ctx,
		`SELECT run_snapshot_cutover_event_id
		   FROM schedules
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3
		  FOR SHARE`,
		tenantID, userID, taskID,
	).Scan(&pointer); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, taskRunNotFound()
		}
		return nil, false, taskRunDatabaseError(
			"lock task run snapshot admission", err)
	}
	if pointer == nil {
		return nil, false, nil
	}
	var action string
	if err := tx.QueryRow(ctx,
		`SELECT action
		   FROM task_run_snapshot_v2_cutover_events
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND task_id=$4`,
		*pointer, tenantID, userID, taskID,
	).Scan(&action); err != nil {
		return nil, false, taskRunIntegrityError()
	}
	switch action {
	case "activate":
		marker := *pointer
		return &marker, true, nil
	case "rollback":
		return nil, true, nil
	default:
		return nil, false, taskRunIntegrityError()
	}
}

func (s *Store) loadTaskRunSnapshotShadowBehindFenceV2(
	ctx context.Context,
	p CreateOrGetTaskRunSnapshotParams,
) (*taskRunSnapshot, bool, error) {
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, false, taskRunDatabaseError(
			"begin task run snapshot v2 recovery transaction", err)
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockTaskRunSnapshotRun(ctx, tx, p.TemporalRunID); err != nil {
		return nil, false, err
	}
	winner, found, err := loadTaskRunSnapshot(ctx, tx, p)
	if err != nil || !found {
		return winner, found, err
	}
	if found, err := validateExistingTaskRunSnapshotShadowV2(
		ctx, tx, winner); err != nil {
		return nil, false, err
	} else if !found {
		return nil, false, taskRunIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, taskRunDatabaseError(
			"commit task run snapshot v2 recovery transaction", err)
	}
	return winner, true, nil
}

func setTaskRunTenantContext(
	ctx context.Context,
	tx pgx.Tx,
	tenantID int64,
) error {
	if tenantID <= 0 {
		return taskRunValidationError("task run snapshot tenant is invalid")
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id', $1, true)`,
		fmt.Sprintf("%d", tenantID)); err != nil {
		return taskRunDatabaseError("set task run snapshot tenant context", err)
	}
	return nil
}

func lockTaskRunSnapshotRun(ctx context.Context, tx pgx.Tx, temporalRunID string) error {
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, $2))`,
		temporalRunID, taskRunSnapshotLockSeed); err != nil {
		return taskRunDatabaseError("lock task run snapshot execution", err)
	}
	return nil
}

func loadTaskRunSnapshot(
	ctx context.Context,
	q taskRunSnapshotQueryer,
	p CreateOrGetTaskRunSnapshotParams,
) (*taskRunSnapshot, bool, error) {
	snapshot, err := scanTaskRunSnapshot(q.QueryRow(ctx,
		`SELECT `+taskRunSnapshotColumns+`
		   FROM task_run_snapshots
		  WHERE tenant_id = $1 AND user_id = $2 AND task_id = $3
		    AND temporal_workflow_id = $4 AND temporal_run_id = $5`,
		p.TenantID, p.UserID, p.TaskID, p.TemporalWorkflowID, p.TemporalRunID,
	))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, taskRunDatabaseError("load task run snapshot", err)
	}
	if err := validateStoredTaskRunSnapshot(snapshot, p); err != nil {
		return nil, false, err
	}
	return snapshot, true, nil
}

func loadTaskRunWinnerOrConflict(
	ctx context.Context,
	q taskRunSnapshotQueryer,
	p CreateOrGetTaskRunSnapshotParams,
) (*taskRunSnapshot, error) {
	winner, found, err := loadTaskRunSnapshot(ctx, q, p)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, types.NewAppError(types.CodeConflict,
			"task run snapshot execution identity conflicts with an existing run", nil)
	}
	return winner, nil
}

func scanTaskRunSnapshot(row pgx.Row) (*taskRunSnapshot, error) {
	var snapshot taskRunSnapshot
	var mode string
	if err := row.Scan(
		&snapshot.ID, &snapshot.TenantID, &snapshot.UserID, &snapshot.TaskID,
		&snapshot.TemporalWorkflowID, &snapshot.TemporalRunID, &snapshot.RunKind, &mode,
		&snapshot.AdaptiveVersion, &snapshot.CapabilityCatalogDigest,
		&snapshot.ToolPolicyDigest, &snapshot.PromptPolicyDigest,
		&snapshot.ModelPolicyDigest, &snapshot.QuotaPolicyDigest,
		&snapshot.DefinitionDigest, &snapshot.PlanDigest, &snapshot.PayloadDigest,
		&snapshot.ReferenceDigest, &snapshot.ReferenceSchemaVersion,
		&snapshot.Payload, &snapshot.BudgetJSON,
		&snapshot.V2CutoverEventID,
		&snapshot.CreatedAt,
	); err != nil {
		return nil, err
	}
	snapshot.Mode = types.ExecutionMode(mode)
	return &snapshot, nil
}

func validateTaskRunLookupInput(p CreateOrGetTaskRunSnapshotParams) error {
	if p.TenantID <= 0 || p.UserID <= 0 || !validTaskRunTaskID(p.TaskID) ||
		!validTaskRunReference(p.TemporalWorkflowID) ||
		!validTaskRunReference(p.TemporalRunID) {
		return taskRunValidationError("task run snapshot scope is invalid")
	}
	return nil
}

func validateNewTaskRunInput(p CreateOrGetTaskRunSnapshotParams) error {
	// C0 deliberately has no DiscoverAtRun persistence semantics yet. Unknown,
	// future values, and the future dynamic mode all fail closed.
	if p.Mode != types.ExecutionModeCompiled {
		return taskRunValidationError("task run snapshot mode is not supported")
	}
	if p.AdaptiveVersion != 0 {
		return taskRunValidationError("compiled task run adaptive version must be zero")
	}
	return nil
}

func lockTaskRunMembership(ctx context.Context, tx pgx.Tx, tenantID, userID int64) error {
	var valid bool
	err := tx.QueryRow(ctx,
		`SELECT true
		   FROM memberships m
		   JOIN tenants t ON t.id = m.tenant_id
		  WHERE m.tenant_id = $1 AND m.user_id = $2
		    AND t.status = 'active' AND t.deleted_at IS NULL
		  FOR SHARE OF m, t`,
		tenantID, userID,
	).Scan(&valid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return taskRunNotFound()
		}
		return taskRunDatabaseError("lock task run membership", err)
	}
	if !valid {
		return taskRunNotFound()
	}
	return nil
}

func loadTaskRunDefinition(
	ctx context.Context,
	tx pgx.Tx,
	p CreateOrGetTaskRunSnapshotParams,
) (taskRunDefinitionPayload, string, string, error) {
	var definition taskRunDefinitionPayload
	var strictness *string
	// Pre-A2 schedules may legitimately have no playbook row; the legacy
	// runtime treated that exactly like an empty playbook. Preserve that
	// behavior only when task_fetch_targets is also empty (checked below).
	err := tx.QueryRow(ctx,
		`SELECT s.nl_description, s.spec_json, s.scope_json, s.push_strictness,
		        COALESCE(pb.content, ''), COALESCE(pb.fetch_plan, '{}'::jsonb)
		   FROM schedules s
		   LEFT JOIN schedule_playbooks pb ON pb.schedule_id = s.id
		  WHERE s.id = $1 AND s.tenant_id = $2 AND s.user_id = $3
		    AND (
		      s.status = $4 OR (
		        s.status = $5 AND authorize_manual_task_run_v1(
		          s.tenant_id, s.user_id, s.id, $6
		        )
		      )
		    )
		    AND `+matureSchedulePredicate+`
		  FOR SHARE OF s`,
		p.TaskID, p.TenantID, p.UserID, types.ScheduleStatusActive,
		types.ScheduleStatusPaused, p.TemporalWorkflowID,
	).Scan(
		&definition.NLDescription, &definition.SpecJSON, &definition.ScopeJSON,
		&strictness, &definition.PlaybookContent, &definition.FetchPlan,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return taskRunDefinitionPayload{}, "", "", taskRunNotFound()
		}
		return taskRunDefinitionPayload{}, "", "",
			taskRunDatabaseError("load active task run definition", err)
	}
	definition.TaskID = p.TaskID
	definition.TenantID = p.TenantID
	definition.UserID = p.UserID
	if strictness != nil {
		definition.Strictness = types.PushStrictness(*strictness)
	}

	var errNormalize error
	definition.SpecJSON, errNormalize = canonicalTaskRunJSONObject(definition.SpecJSON)
	if errNormalize != nil {
		return taskRunDefinitionPayload{}, "", "", taskRunIntegrityError()
	}
	definition.ScopeJSON, errNormalize = canonicalTaskRunJSONObject(definition.ScopeJSON)
	if errNormalize != nil {
		return taskRunDefinitionPayload{}, "", "", taskRunIntegrityError()
	}
	if definition.Strictness != "" && !definition.Strictness.Valid() {
		return taskRunDefinitionPayload{}, "", "", taskRunIntegrityError()
	}

	var planObject map[string]json.RawMessage
	if err := strictjson.Decode(definition.FetchPlan, &planObject); err != nil || planObject == nil {
		return taskRunDefinitionPayload{}, "", "", taskRunIntegrityError()
	}
	var definitionDigest string
	if len(planObject) == 0 {
		return taskRunDefinitionPayload{}, "", "",
			taskRunValidationError("task run requires an approved fetch plan")
	} else {
		definition.SourceScope = taskRunSourceScopeApproved
		compiledDef := types.PausedCompiledTaskDefinition{
			TaskID:          definition.TaskID,
			TenantID:        definition.TenantID,
			UserID:          definition.UserID,
			NLDescription:   definition.NLDescription,
			SpecJSON:        definition.SpecJSON,
			ScopeJSON:       definition.ScopeJSON,
			PlaybookContent: definition.PlaybookContent,
			FetchPlan:       definition.FetchPlan,
			Strictness:      definition.Strictness,
		}
		plan, validateErr := validatePausedCompiledTaskDefinition(compiledDef)
		if validateErr != nil {
			return taskRunDefinitionPayload{}, "", "", taskRunIntegrityError()
		}
		_, marshalErr := canonicalTaskRunCompiledPlan(plan)
		if marshalErr != nil {
			return taskRunDefinitionPayload{}, "", "", taskRunIntegrityError()
		}
		// The mutable task model uses fetch_plan.targets. The immutable
		// task-run-snapshot/v1 wire is already deployed with fetch_plan.sources,
		// so adapt explicitly at this boundary instead of reinterpreting v1.
		legacyPlan := taskRunFetchPlanV1{
			Sources: make([]taskRunPlanSourceV1, len(plan.Targets)),
		}
		for index, target := range plan.Targets {
			legacyPlan.Sources[index] = taskRunPlanSourceV1{
				Platform: target.Platform, Capability: target.Capability,
				Title: target.Title, URL: target.URL, Config: target.Config,
			}
		}
		canonicalLegacyPlan, marshalErr :=
			canonicalTaskRunCompiledPlanV1(&legacyPlan)
		if marshalErr != nil {
			return taskRunDefinitionPayload{}, "", "", taskRunIntegrityError()
		}
		definition.FetchPlan = canonicalLegacyPlan
		compiledDef.FetchPlan = canonicalLegacyPlan
		definition.Sources, err = loadApprovedTaskRunSources(
			ctx, tx, p.TaskID, plan.Targets)
		if err != nil {
			return taskRunDefinitionPayload{}, "", "", err
		}
		if len(definition.Sources) != len(plan.Targets) {
			return taskRunDefinitionPayload{}, "", "", taskRunIntegrityError()
		}
		definitionDigest, err = types.DigestPausedCompiledTaskDefinition(compiledDef)
		if err != nil {
			return taskRunDefinitionPayload{}, "", "", taskRunIntegrityError()
		}
	}
	planDigest, err := digestTaskRunPlan(definition)
	if err != nil {
		return taskRunDefinitionPayload{}, "", "", taskRunIntegrityError()
	}
	return definition, definitionDigest, planDigest, nil
}

func loadApprovedTaskRunSources(
	ctx context.Context,
	tx pgx.Tx,
	taskID string,
	planned []compiledPlanTarget,
) ([]taskRunSourceIdentity, error) {
	rows, err := tx.Query(ctx,
		`SELECT s.id, s.url
		   FROM task_fetch_targets ss
		   JOIN fetch_targets s ON s.id = ss.fetch_target_id
		  WHERE ss.schedule_id = $1
		  ORDER BY s.url, s.id
		  FOR SHARE OF ss, s`, taskID)
	if err != nil {
		return nil, taskRunDatabaseError("load approved task run sources", err)
	}
	defer rows.Close()

	linkedIDs := make(map[string]int64, len(planned))
	for rows.Next() {
		var sourceID int64
		var sourceURL string
		if err := rows.Scan(&sourceID, &sourceURL); err != nil {
			return nil, taskRunDatabaseError("scan approved task run source link", err)
		}
		if sourceID <= 0 || !validTaskRunSourceText(sourceURL, maxCompiledTaskTargetURLBytes) {
			return nil, taskRunIntegrityError()
		}
		if _, duplicate := linkedIDs[sourceURL]; duplicate {
			return nil, taskRunIntegrityError()
		}
		linkedIDs[sourceURL] = sourceID
	}
	if err := rows.Err(); err != nil {
		return nil, taskRunDatabaseError("iterate approved task run source links", err)
	}
	if len(linkedIDs) != len(planned) {
		return nil, taskRunIntegrityError()
	}

	sources := make([]taskRunSourceIdentity, 0, len(planned))
	for _, source := range planned {
		sourceID, ok := linkedIDs[source.URL]
		if !ok {
			return nil, taskRunIntegrityError()
		}
		config, err := canonicalTaskRunJSONObject(source.Config)
		if err != nil {
			return nil, taskRunIntegrityError()
		}
		frozen := taskRunSourceIdentity{
			SourceID: sourceID, Platform: source.Platform, Capability: source.Capability,
			Title: source.Title, URL: source.URL, Config: config,
		}
		if !validTaskRunSourceIdentity(frozen) {
			return nil, taskRunIntegrityError()
		}
		sources = append(sources, frozen)
	}
	sortTaskRunSources(sources)
	return sources, nil
}

func collectTaskRunSources(rows pgx.Rows) ([]taskRunSourceIdentity, error) {
	sources := make([]taskRunSourceIdentity, 0)
	for rows.Next() {
		var source taskRunSourceIdentity
		if err := rows.Scan(
			&source.SourceID, &source.Platform, &source.Capability,
			&source.Title, &source.URL, &source.Config,
		); err != nil {
			return nil, taskRunDatabaseError("scan task run source identity", err)
		}
		config, err := canonicalTaskRunJSONObject(source.Config)
		if err != nil || !validTaskRunSourceIdentity(source) {
			return nil, taskRunIntegrityError()
		}
		source.Config = config
		sources = append(sources, source)
	}
	if err := rows.Err(); err != nil {
		return nil, taskRunDatabaseError("iterate task run source identities", err)
	}
	sortTaskRunSources(sources)
	return sources, nil
}

func sortTaskRunSources(sources []taskRunSourceIdentity) {
	sort.Slice(sources, func(i, j int) bool {
		if sources[i].URL == sources[j].URL {
			return sources[i].SourceID < sources[j].SourceID
		}
		return sources[i].URL < sources[j].URL
	})
}

func validateStoredTaskRunSnapshot(
	snapshot *taskRunSnapshot,
	p CreateOrGetTaskRunSnapshotParams,
) error {
	if snapshot == nil {
		return taskRunIntegrityError()
	}
	switch snapshot.ReferenceSchemaVersion {
	case taskRunReferenceSchemaVersionV1:
		return validateStoredTaskRunSnapshotV1(snapshot, p)
	case types.RunSnapshotSchemaVersionV2:
		return validateStoredTaskRunSnapshotV2(snapshot, p)
	default:
		return taskRunIntegrityError()
	}
}

func validateStoredTaskRunSnapshotV1(
	snapshot *taskRunSnapshot,
	p CreateOrGetTaskRunSnapshotParams,
) error {
	if snapshot == nil || snapshot.CreatedAt.IsZero() {
		return taskRunIntegrityError()
	}
	if snapshot.TenantID != p.TenantID || snapshot.UserID != p.UserID ||
		snapshot.TaskID != p.TaskID || snapshot.TemporalRunID != p.TemporalRunID ||
		snapshot.TemporalWorkflowID != p.TemporalWorkflowID {
		return types.NewAppError(types.CodeConflict,
			"task run snapshot identity differs from the committed run", nil)
	}
	ref, err := snapshot.safeRef()
	if err != nil {
		return taskRunIntegrityError()
	}
	if ref.TemporalWorkflowID != p.TemporalWorkflowID ||
		ref.TemporalRunID != p.TemporalRunID || ref.TenantID != p.TenantID ||
		ref.UserID != p.UserID || ref.TaskID != p.TaskID {
		return taskRunIntegrityError()
	}

	decodedPayload, err := readTaskRunSnapshotPayload(snapshot.Payload)
	if err != nil {
		return taskRunIntegrityError()
	}
	payload := decodedPayload.Payload
	canonicalPayload := decodedPayload.Canonical
	definitionDigest := decodedPayload.DefinitionDigest
	planDigest := decodedPayload.PlanDigest
	policyDigests := decodedPayload.PolicyDigests
	storedBudget, budgetJSON, err := readTaskRunBudgetV1(snapshot.BudgetJSON)
	if err != nil || payload.Budget == nil || storedBudget != *payload.Budget {
		return taskRunIntegrityError()
	}
	if payload.SchemaVersion != taskRunSnapshotPayloadSchemaV1 ||
		payload.TenantID != snapshot.TenantID || payload.UserID != snapshot.UserID ||
		payload.TaskID != snapshot.TaskID ||
		payload.RunKind != string(snapshot.RunKind) ||
		payload.ReferenceSchemaVersion != snapshot.ReferenceSchemaVersion ||
		payload.Mode != string(snapshot.Mode) || payload.AdaptiveVersion != snapshot.AdaptiveVersion ||
		!constantTimeDigestEqual(policyDigests.CapabilityCatalog, snapshot.CapabilityCatalogDigest) ||
		!constantTimeDigestEqual(policyDigests.ToolPolicy, snapshot.ToolPolicyDigest) ||
		!constantTimeDigestEqual(policyDigests.PromptPolicy, snapshot.PromptPolicyDigest) ||
		!constantTimeDigestEqual(policyDigests.ModelPolicy, snapshot.ModelPolicyDigest) ||
		!constantTimeDigestEqual(policyDigests.QuotaPolicy, snapshot.QuotaPolicyDigest) ||
		!constantTimeDigestEqual(definitionDigest, snapshot.DefinitionDigest) ||
		!constantTimeDigestEqual(planDigest, snapshot.PlanDigest) ||
		!bytes.Equal(canonicalPayload, snapshot.Payload) ||
		!constantTimeDigestEqual(sha256Hex(snapshot.Payload), snapshot.PayloadDigest) {
		return taskRunIntegrityError()
	}
	snapshot.Payload = canonicalPayload
	snapshot.BudgetJSON = budgetJSON
	return nil
}

func validTaskRunTaskID(value string) bool {
	return value != "" && strings.TrimSpace(value) == value && len(value) <= 255 &&
		utf8.ValidString(value) && !containsUnsafeTaskRunRune(value)
}

func validTaskRunReference(value string) bool {
	return value != "" && strings.TrimSpace(value) == value &&
		len(value) <= maxTaskRunReferenceBytes && utf8.ValidString(value) &&
		!containsUnsafeTaskRunRune(value)
}

func validTaskRunSourceIdentity(source taskRunSourceIdentity) bool {
	return source.SourceID > 0 && validTaskRunSourceText(source.Platform, maxCompiledTaskTargetTextBytes) &&
		validTaskRunSourceText(source.Capability, maxCompiledTaskTargetTextBytes) &&
		validTaskRunSourceText(source.URL, maxCompiledTaskTargetURLBytes) &&
		len(source.Title) <= maxCompiledTaskTargetTextBytes && utf8.ValidString(source.Title)
}

func validTaskRunSourceText(value string, maxBytes int) bool {
	return value != "" && strings.TrimSpace(value) == value && len(value) <= maxBytes &&
		utf8.ValidString(value) && !containsUnsafeTaskRunRune(value)
}

func containsUnsafeTaskRunRune(value string) bool {
	for _, r := range value {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf) {
			return true
		}
	}
	return false
}

func taskRunSerializationFailure(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40001"
}

func taskRunValidationError(message string) error {
	return types.NewAppError(types.CodeValidation, message, nil)
}

func taskRunNotFound() error {
	return types.NewAppError(types.CodeNotFound,
		"task run snapshot source aggregate is unavailable", nil)
}

func taskRunIntegrityError() error {
	return types.NewAppError(types.CodeInternal,
		"task run snapshot integrity check failed", nil)
}

// taskRunDatabaseError deliberately strips PostgreSQL Detail and arbitrary
// driver text: constraint failures may quote the complete JSONB row, which can
// contain playbook content, source URLs, and source config.
func taskRunDatabaseError(action string, cause error) error {
	var safeCause error
	switch {
	case cause == nil:
		safeCause = errors.New("database operation did not converge")
	case errors.Is(cause, context.Canceled), errors.Is(cause, context.DeadlineExceeded):
		safeCause = cause
	default:
		var pgErr *pgconn.PgError
		if errors.As(cause, &pgErr) {
			safeCause = fmt.Errorf("postgres sqlstate %s", pgErr.Code)
		} else {
			safeCause = errors.New("database operation failed")
		}
	}
	return types.NewAppError(types.CodeDatabase, action, safeCause)
}
