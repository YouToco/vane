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
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

const (
	researchRunPlanSchemaV3     = "vane.research-run-plan/v3"
	researchRunStepSchemaV3     = "vane.research-run-step/v3"
	researchRunEvidenceSchemaV3 = "vane.research-run-evidence/v3"
)

type CreateOrGetResearchRunPlanV3Params struct {
	Identity                types.RunIdentity
	RunSnapshotID           int64
	PlannerLLMReservationID int64
	Plan                    runcontext.ResearchExecutionPlanV3
}

func (s *Store) CreateOrGetResearchRunSnapshotV3(
	ctx context.Context,
	identity types.RunIdentity,
	policy runtimepolicy.BundleV1,
	researchTools runtimepolicy.ResearchToolPolicyV3,
	researchModel runtimepolicy.ResearchModelPolicyV3,
) (types.ResearchRunSnapshotRefV3, error) {
	return s.CreateOrGetResearchRunSnapshotWithAuthorityV3(
		ctx, identity, policy, researchTools, researchModel, "")
}

// CreateOrGetResearchRunSnapshotWithAuthorityV3 binds an optional exact
// Schedule Action capability into the immutable snapshot. Empty is the shadow
// path and can never later acquire delivery authority.
func (s *Store) CreateOrGetResearchRunSnapshotWithAuthorityV3(
	ctx context.Context,
	identity types.RunIdentity,
	policy runtimepolicy.BundleV1,
	researchTools runtimepolicy.ResearchToolPolicyV3,
	researchModel runtimepolicy.ResearchModelPolicyV3,
	authorityToken string,
) (types.ResearchRunSnapshotRefV3, error) {
	if identity.RunKind != types.RunSnapshotKindScheduled || identity.TenantID <= 0 ||
		identity.UserID <= 0 || identity.TaskID == "" ||
		identity.TemporalWorkflowID == "" || identity.TemporalRunID == "" {
		return types.ResearchRunSnapshotRefV3{}, researchRunValidationError("research snapshot identity is invalid")
	}
	// Snapshot creation and capability registration are control-plane writes.
	// The restricted executor cannot mint a snapshot and then self-authorize it.
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return types.ResearchRunSnapshotRefV3{}, researchRunDatabaseError("begin research snapshot transaction", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := bindResearchV3AppScopeTx(
		ctx, tx, identity.TenantID, identity.UserID); err != nil {
		return types.ResearchRunSnapshotRefV3{}, err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,101))`,
		fmt.Sprintf("%d/%d/%s", identity.TenantID, identity.UserID, identity.TaskID)); err != nil {
		return types.ResearchRunSnapshotRefV3{}, researchRunDatabaseError(
			"lock research V3 task baseline", err)
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
		if err := validateResearchSnapshotAuthorityTokenV3(ref, authorityToken); err != nil {
			return types.ResearchRunSnapshotRefV3{}, err
		}
		if err := s.registerResearchRunCapabilityInControlTxV1(ctx, tx, ref); err != nil {
			return types.ResearchRunSnapshotRefV3{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return types.ResearchRunSnapshotRefV3{}, researchRunDatabaseError("commit research snapshot replay", err)
		}
		if _, err := s.resolveResearchRunCapabilityV1(ctx, ref); err != nil {
			return types.ResearchRunSnapshotRefV3{}, err
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
	authority, err := loadResearchSnapshotAuthorityV3(
		ctx, tx, identity, definitionVersion, definitionDigest, authorityToken)
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
		ResearchTools: researchTools,
		ResearchModel: researchModel, AuthorityGeneration: authority.Generation,
		TargetActionDigest:        authority.TargetActionDigest,
		ActionAuthorizationDigest: authority.ActionAuthorizationDigest,
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
		AuthorityGeneration:       authority.Generation,
		TargetActionDigest:        authority.TargetActionDigest,
		ActionAuthorizationDigest: authority.ActionAuthorizationDigest,
		CapabilityCatalogDigest:   seal.PolicyDigests.CapabilityCatalogDigest,
		ToolPolicyDigest:          seal.ResearchToolPolicyDigest,
		PromptPolicyDigest:        seal.PolicyDigests.PromptPolicyDigest,
		ModelPolicyDigest:         seal.ResearchModelPolicyDigest,
		QuotaPolicyDigest:         seal.PolicyDigests.QuotaPolicyDigest,
		PlannerBudget:             definition.PlannerBudget, HistoryThroughUTC: cutoffText,
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
		seal.PolicyDigests.CapabilityCatalogDigest, seal.ResearchToolPolicyDigest,
		seal.PolicyDigests.PromptPolicyDigest, seal.ResearchModelPolicyDigest,
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
	if err := s.registerResearchRunCapabilityInControlTxV1(ctx, tx, ref); err != nil {
		return types.ResearchRunSnapshotRefV3{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return types.ResearchRunSnapshotRefV3{}, researchRunDatabaseError("commit research snapshot", err)
	}
	if _, err := s.resolveResearchRunCapabilityV1(ctx, ref); err != nil {
		return types.ResearchRunSnapshotRefV3{}, err
	}
	return ref, nil
}

type researchSnapshotAuthorityV3 struct {
	Generation                int64
	TargetActionDigest        string
	ActionAuthorizationDigest string
}

func loadResearchSnapshotAuthorityV3(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity,
	definitionVersion int64, definitionDigest, token string,
) (researchSnapshotAuthorityV3, error) {
	if token == "" {
		return researchSnapshotAuthorityV3{}, nil
	}
	if strings.TrimSpace(token) != token || len(token) < 32 || len(token) > 512 {
		return researchSnapshotAuthorityV3{}, researchRunValidationError(
			"research snapshot Action authority is invalid")
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,101))`,
		fmt.Sprintf("%d/%d/%s", identity.TenantID, identity.UserID, identity.TaskID)); err != nil {
		return researchSnapshotAuthorityV3{}, researchRunDatabaseError(
			"lock research snapshot Action authority", err)
	}
	var authority researchSnapshotAuthorityV3
	err := tx.QueryRow(ctx, `
		SELECT generation,target_action_digest,action_authorization_digest
		  FROM research_v3_delivery_authorities
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		   AND definition_version=$4 AND definition_digest=$5
		   AND status='enabled'`, identity.TenantID, identity.UserID,
		identity.TaskID, definitionVersion, definitionDigest).Scan(
		&authority.Generation, &authority.TargetActionDigest,
		&authority.ActionAuthorizationDigest)
	if errors.Is(err, pgx.ErrNoRows) {
		return researchSnapshotAuthorityV3{}, researchRunConflictError()
	}
	if err != nil {
		return researchSnapshotAuthorityV3{}, researchRunDatabaseError(
			"load research snapshot Action authority", err)
	}
	tokenSum := sha256.Sum256([]byte(token))
	tokenDigest := hex.EncodeToString(tokenSum[:])
	if subtle.ConstantTimeCompare([]byte(tokenDigest),
		[]byte(authority.ActionAuthorizationDigest)) != 1 {
		return researchSnapshotAuthorityV3{}, researchRunConflictError()
	}
	return authority, nil
}

func validateResearchSnapshotAuthorityTokenV3(
	ref types.ResearchRunSnapshotRefV3, token string,
) error {
	if token == "" {
		if ref.AuthorityGeneration != 0 || ref.TargetActionDigest != "" ||
			ref.ActionAuthorizationDigest != "" {
			return researchRunConflictError()
		}
		return nil
	}
	tokenSum := sha256.Sum256([]byte(token))
	tokenDigest := hex.EncodeToString(tokenSum[:])
	if ref.AuthorityGeneration <= 0 || subtle.ConstantTimeCompare(
		[]byte(tokenDigest), []byte(ref.ActionAuthorizationDigest)) != 1 {
		return researchRunConflictError()
	}
	return nil
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
	if isExactResearchV3ShadowWorkflowID(identity.TemporalWorkflowID) {
		binding, err := loadPreparedResearchV3BindingTx(
			ctx, tx, identity.TenantID, identity.UserID, identity.TaskID, true,
			researchV3ExpectBaseOrTargetHead)
		if err != nil {
			return 0, "", taskstate.ApprovedDefinitionV3{}, err
		}
		return binding.Target.Version, binding.Target.Digest, binding.Definition, nil
	}
	var version int64
	var digest, schemaVersion string
	var payload, scheduleSpec []byte
	query :=
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
		  FOR SHARE OF schedule`
	args := []any{identity.TaskID, identity.TenantID, identity.UserID, identity.TemporalWorkflowID}
	err := tx.QueryRow(ctx, query, args...).Scan(
		&version, &digest, &schemaVersion, &payload, &scheduleSpec)
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

func isExactResearchV3ShadowWorkflowID(value string) bool {
	const prefix = "research-v3-shadow-"
	if !strings.HasPrefix(value, prefix) || len(value) != len(prefix)+64 {
		return false
	}
	for _, r := range value[len(prefix):] {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
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
		seal.ResearchToolPolicyDigest != snapshot.ToolPolicyDigest ||
		seal.PolicyDigests.PromptPolicyDigest != snapshot.PromptPolicyDigest ||
		seal.ResearchModelPolicyDigest != snapshot.ModelPolicyDigest ||
		seal.PolicyDigests.QuotaPolicyDigest != snapshot.QuotaPolicyDigest {
		return types.ResearchRunSnapshotRefV3{}, researchRunIntegrityError()
	}
	ref, err := types.SealResearchRunSnapshotRefV3(types.ResearchRunSnapshotRefV3{
		SnapshotID: snapshot.ID, TemporalWorkflowID: identity.TemporalWorkflowID,
		TemporalRunID: identity.TemporalRunID, RunKind: identity.RunKind,
		TenantID: identity.TenantID, UserID: identity.UserID, TaskID: identity.TaskID,
		DefinitionVersion:         seal.Payload.DefinitionVersion,
		DefinitionDigest:          snapshot.DefinitionDigest,
		AuthorityGeneration:       seal.Payload.AuthorityGeneration,
		TargetActionDigest:        seal.Payload.TargetActionDigest,
		ActionAuthorizationDigest: seal.Payload.ActionAuthorizationDigest,
		CapabilityCatalogDigest:   snapshot.CapabilityCatalogDigest,
		ToolPolicyDigest:          snapshot.ToolPolicyDigest,
		PromptPolicyDigest:        snapshot.PromptPolicyDigest,
		ModelPolicyDigest:         snapshot.ModelPolicyDigest,
		QuotaPolicyDigest:         snapshot.QuotaPolicyDigest,
		PlannerBudget:             seal.Payload.PlannerBudget,
		HistoryThroughUTC:         seal.Payload.HistoryThroughUTC,
		PayloadDigest:             snapshot.PayloadDigest,
	})
	if err != nil || ref.ReferenceDigest != snapshot.ReferenceDigest {
		return types.ResearchRunSnapshotRefV3{}, researchRunIntegrityError()
	}
	return ref, nil
}

// LoadResearchRunSnapshotV3 opens a sealed snapshot only inside an Activity.
// The payload contains the task manual and runtime policies and must never be
// returned through Temporal history; callers keep only the validated ref there.
func (s *Store) LoadResearchRunSnapshotV3(
	ctx context.Context, identity types.RunIdentity,
	ref types.ResearchRunSnapshotRefV3,
) (runcontext.ResearchSnapshotSealV3, error) {
	if err := ref.ValidateFor(identity); err != nil {
		return runcontext.ResearchSnapshotSealV3{}, researchRunValidationError("research snapshot reference is invalid")
	}
	tx, scopedRef, err := s.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{}, identity, ref.SnapshotID)
	if err != nil {
		return runcontext.ResearchSnapshotSealV3{}, researchRunDatabaseError("begin research snapshot read", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if scopedRef != ref {
		return runcontext.ResearchSnapshotSealV3{}, researchRunIntegrityError()
	}
	row, found, err := loadResearchRunSnapshotRowV3(ctx, tx, CreateOrGetTaskRunSnapshotParams{
		TenantID: identity.TenantID, UserID: identity.UserID, TaskID: identity.TaskID,
		TemporalWorkflowID: identity.TemporalWorkflowID, TemporalRunID: identity.TemporalRunID,
	})
	if err != nil {
		return runcontext.ResearchSnapshotSealV3{}, err
	}
	if !found {
		return runcontext.ResearchSnapshotSealV3{}, researchRunValidationError("research snapshot is unavailable")
	}
	storedRef, err := validateStoredResearchRunSnapshotV3(identity, row)
	if err != nil || storedRef != ref {
		return runcontext.ResearchSnapshotSealV3{}, researchRunIntegrityError()
	}
	seal, err := runcontext.DecodeResearchSnapshotPayloadV3(row.Payload)
	if err != nil || seal.PayloadDigest != ref.PayloadDigest {
		return runcontext.ResearchSnapshotSealV3{}, researchRunIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return runcontext.ResearchSnapshotSealV3{}, researchRunDatabaseError("commit research snapshot read", err)
	}
	return seal, nil
}

type ResearchRunStepPhaseV3 string

const (
	ResearchRunStepStartedV3       ResearchRunStepPhaseV3 = "started"
	ResearchRunStepCompletedV3     ResearchRunStepPhaseV3 = "completed"
	ResearchRunStepFailedV3        ResearchRunStepPhaseV3 = "failed"
	ResearchRunStepIndeterminateV3 ResearchRunStepPhaseV3 = "indeterminate"
)

type ResearchRunStepExecutionV3 struct {
	StepID               int64
	SpendReservationID   int64
	FirstWriter          bool
	Ordinal              int
	InvocationID         string
	ToolName             string
	Arguments            json.RawMessage
	RequestDigest        string
	ReservedQuotaUnits   float64
	ReservedCostMicroUSD int64
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
	ProviderCall  ResearchProviderCallV3
}

// CommitResearchRunStepEvidenceV3Params is the only success path for a V3
// Tool step. Result contains exactly the UTF-8 bytes shown to the model after
// runtime truncation; OriginalSize records normalized pre-bound bytes. A
// provider's own size/truncation belongs to its separately typed receipt.
type CommitResearchRunStepEvidenceV3Params struct {
	Identity      types.RunIdentity
	RunSnapshotID int64
	PlanRef       types.ResearchRunPlanRefV3
	Ordinal       int
	Result        []byte
	OriginalSize  int
	TrustType     string
	CostMicroUSD  int64
	ProviderCall  ResearchProviderCallV3
}

type ResearchRunStepEvidenceReceiptV3 struct {
	ResearchRunStepReceiptV3
	EvidenceID   int64
	OriginalSize int
	Truncated    bool
	TrustType    string
}

type ResearchRunEvidenceV3 struct {
	EvidenceID    int64
	StartedStepID int64
	Ordinal       int
	InvocationID  string
	ToolName      string
	RequestDigest string
	ResultDigest  string
	Result        []byte
	OriginalSize  int
	Truncated     bool
	TrustType     string
}

type ResearchRunStepResolutionV3 struct {
	Phase    ResearchRunStepPhaseV3
	Receipt  ResearchRunStepReceiptV3
	Evidence *ResearchRunEvidenceV3
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
	ToolPolicyDigest        string
	PlanDigest              string
	PlanPayload             []byte
	PlannerLLMReservationID *int64
}

func (s *Store) CreateOrGetResearchRunPlanV3(
	ctx context.Context,
	params CreateOrGetResearchRunPlanV3Params,
) (types.ResearchRunPlanRefV3, error) {
	if err := validateResearchPlanScopeV3(params); err != nil {
		return types.ResearchRunPlanRefV3{}, err
	}

	tx, _, err := s.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{}, params.Identity, params.RunSnapshotID)
	if err != nil {
		return types.ResearchRunPlanRefV3{}, researchRunDatabaseError("begin research plan transaction", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
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
		plannerBudget, err := validateResearchPlanSnapshotPolicyV3(ctx, tx, params)
		if err != nil {
			return types.ResearchRunPlanRefV3{}, err
		}
		if len(params.Plan.Steps) > plannerBudget.MaxToolCalls {
			return types.ResearchRunPlanRefV3{}, researchRunValidationError(
				"research plan exceeds its frozen Tool-call budget")
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
	} else {
		storedPlan, decodeErr := runcontext.DecodeResearchExecutionPlanV3(row.PlanPayload)
		if decodeErr != nil {
			return types.ResearchRunPlanRefV3{}, researchRunIntegrityError()
		}
		recoveryParams := params
		recoveryParams.Plan = storedPlan
		plannerBudget, policyErr := validateResearchPlanSnapshotPolicyV3(
			ctx, tx, recoveryParams)
		if policyErr != nil || len(storedPlan.Steps) > plannerBudget.MaxToolCalls {
			return types.ResearchRunPlanRefV3{}, researchRunIntegrityError()
		}
		if params.PlannerLLMReservationID > 0 &&
			(row.PlannerLLMReservationID == nil ||
				*row.PlannerLLMReservationID != params.PlannerLLMReservationID) {
			return types.ResearchRunPlanRefV3{}, researchRunConflictError()
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

// LoadResearchRunPlanRefV3 is the recovery-first planner read. It returns only
// the sealed Temporal-safe reference; a coordinator must call this before any
// paid planning model so Activity retry or response loss cannot spend twice.
func (s *Store) LoadResearchRunPlanRefV3(
	ctx context.Context, identity types.RunIdentity,
	snapshotRef types.ResearchRunSnapshotRefV3,
) (types.ResearchRunPlanRefV3, bool, error) {
	if err := snapshotRef.ValidateFor(identity); err != nil {
		return types.ResearchRunPlanRefV3{}, false,
			researchRunValidationError("research plan recovery snapshot is invalid")
	}
	tx, scopedRef, err := s.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{}, identity, snapshotRef.SnapshotID)
	if err != nil {
		return types.ResearchRunPlanRefV3{}, false,
			researchRunDatabaseError("begin research plan recovery", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if scopedRef != snapshotRef {
		return types.ResearchRunPlanRefV3{}, false, researchRunIntegrityError()
	}
	snapshot, found, err := loadResearchRunSnapshotRowV3(ctx, tx,
		CreateOrGetTaskRunSnapshotParams{
			TenantID: identity.TenantID, UserID: identity.UserID, TaskID: identity.TaskID,
			TemporalWorkflowID: identity.TemporalWorkflowID,
			TemporalRunID:      identity.TemporalRunID,
		})
	if err != nil {
		return types.ResearchRunPlanRefV3{}, false, err
	}
	storedSnapshotRef, err := validateStoredResearchRunSnapshotV3(identity, snapshot)
	if !found || err != nil || storedSnapshotRef != snapshotRef {
		return types.ResearchRunPlanRefV3{}, false, researchRunIntegrityError()
	}
	row, found, err := loadResearchRunPlanV3(ctx, tx, identity, snapshotRef.SnapshotID)
	if err != nil || !found {
		if err != nil {
			return types.ResearchRunPlanRefV3{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return types.ResearchRunPlanRefV3{}, false,
				researchRunDatabaseError("commit empty research plan recovery", err)
		}
		return types.ResearchRunPlanRefV3{}, false, nil
	}
	ref, err := researchRunPlanRefV3(row)
	if err != nil || ref.DefinitionDigest != snapshotRef.DefinitionDigest ||
		ref.CapabilityCatalogDigest != snapshotRef.CapabilityCatalogDigest ||
		ref.ToolPolicyDigest != snapshotRef.ToolPolicyDigest {
		return types.ResearchRunPlanRefV3{}, false, researchRunIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return types.ResearchRunPlanRefV3{}, false,
			researchRunDatabaseError("commit research plan recovery", err)
	}
	return ref, true, nil
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
	tx, scopedRef, err := s.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{}, identity, runSnapshotID)
	if err != nil {
		return ResearchRunStepExecutionV3{}, researchRunDatabaseError("begin research step transaction", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if scopedRef.SnapshotID != runSnapshotID {
		return ResearchRunStepExecutionV3{}, researchRunIntegrityError()
	}
	var execution ResearchRunStepExecutionV3
	var returnedPlanDigest string
	err = tx.QueryRow(ctx,
		`SELECT out_started_step_id,out_reservation_id,out_first_writer,
		        out_plan_digest,out_invocation_id,out_tool_name,out_arguments,
		        out_request_digest,out_reserved_quota_units,
		        out_reserved_cost_micro_usd
		   FROM admit_research_run_tool_step_cap_v1($1,$2,$3)`,
		runSnapshotID, planRef.PlanID, ordinal,
	).Scan(&execution.StepID, &execution.SpendReservationID,
		&execution.FirstWriter, &returnedPlanDigest, &execution.InvocationID,
		&execution.ToolName, &execution.Arguments, &execution.RequestDigest,
		&execution.ReservedQuotaUnits, &execution.ReservedCostMicroUSD)
	if err != nil {
		return ResearchRunStepExecutionV3{}, classifyResearchToolAdmissionErrV3(err)
	}
	execution.Ordinal = ordinal
	var arguments any
	if returnedPlanDigest != planRef.PlanDigest || execution.StepID <= 0 ||
		execution.SpendReservationID <= 0 || execution.InvocationID == "" ||
		execution.ToolName == "" || execution.RequestDigest !=
		digestResearchRunStepRequestV3(planRef.PlanDigest, ordinal) ||
		execution.ReservedQuotaUnits != researchRunQuotaUnitsV3 ||
		execution.ReservedCostMicroUSD <= 0 ||
		json.Unmarshal(execution.Arguments, &arguments) != nil {
		return ResearchRunStepExecutionV3{}, researchRunIntegrityError()
	}
	canonicalArguments, err := json.Marshal(arguments)
	if err != nil {
		return ResearchRunStepExecutionV3{}, researchRunIntegrityError()
	}
	execution.Arguments = canonicalArguments
	if err := tx.Commit(ctx); err != nil {
		return ResearchRunStepExecutionV3{}, researchRunDatabaseError("commit research step start", err)
	}
	if !execution.FirstWriter {
		execution.Arguments = nil
	}
	return execution, nil
}

func classifyResearchToolAdmissionErrV3(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "P0001" {
		return types.NewAppError(types.CodeQuotaExceeded,
			"research Tool budget or quota is exhausted", ErrQuotaExceeded)
	}
	return researchRunDatabaseError("atomically admit research Tool step", err)
}

// LoadResearchRunStepResolutionV3 is the only recovery read after Begin
// returns FirstWriter=false. It never returns Tool arguments. A completed
// resolution is usable only when the exact model-visible evidence is present.
func (s *Store) LoadResearchRunStepResolutionV3(
	ctx context.Context, identity types.RunIdentity, runSnapshotID int64,
	planRef types.ResearchRunPlanRefV3, ordinal int,
) (ResearchRunStepResolutionV3, error) {
	if err := planRef.ValidateFor(identity, runSnapshotID); err != nil ||
		ordinal < 0 || ordinal >= planRef.StepCount {
		return ResearchRunStepResolutionV3{}, researchRunValidationError("research step recovery scope is invalid")
	}
	tx, _, err := s.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{}, identity, runSnapshotID)
	if err != nil {
		return ResearchRunStepResolutionV3{}, researchRunDatabaseError("begin research step recovery", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := lockPushEffectSchemaWriter(ctx, tx); err != nil {
		return ResearchRunStepResolutionV3{}, researchRunDatabaseError("lock research schema admission", err)
	}
	if err := lockResearchRunStepV3(ctx, tx, identity.TemporalRunID,
		planRef.PlanDigest, ordinal); err != nil {
		return ResearchRunStepResolutionV3{}, err
	}
	plan, row, err := loadAndValidateResearchRunPlanV3(
		ctx, tx, identity, runSnapshotID, planRef)
	if err != nil {
		return ResearchRunStepResolutionV3{}, err
	}
	if ordinal >= len(plan.Steps) {
		return ResearchRunStepResolutionV3{}, researchRunIntegrityError()
	}
	step := plan.Steps[ordinal]
	requestDigest := digestResearchRunStepRequestV3(planRef.PlanDigest, ordinal)
	var startedStepID int64
	if err := tx.QueryRow(ctx,
		`SELECT id FROM research_run_steps
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND plan_id=$4
		    AND temporal_run_id=$5 AND plan_digest=$6 AND step_ordinal=$7
		    AND phase='started' AND invocation_id=$8 AND tool_name=$9
		    AND request_digest=$10`,
		identity.TenantID, identity.UserID, identity.TaskID, row.ID,
		identity.TemporalRunID, planRef.PlanDigest, ordinal,
		step.InvocationID, step.ToolName, requestDigest,
	).Scan(&startedStepID); errors.Is(err, pgx.ErrNoRows) {
		return ResearchRunStepResolutionV3{}, researchRunValidationError("research step has no immutable start")
	} else if err != nil {
		return ResearchRunStepResolutionV3{}, researchRunDatabaseError("load research step start", err)
	}
	resolution := ResearchRunStepResolutionV3{
		Phase: ResearchRunStepStartedV3,
		Receipt: ResearchRunStepReceiptV3{
			StepID: startedStepID, Ordinal: ordinal, Phase: ResearchRunStepStartedV3,
			InvocationID: step.InvocationID, ToolName: step.ToolName,
			RequestDigest: requestDigest,
		},
	}
	var phase string
	var resultDigest, errorCode *string
	terminal := ResearchRunStepReceiptV3{
		Ordinal: ordinal, InvocationID: step.InvocationID,
		ToolName: step.ToolName, RequestDigest: requestDigest,
	}
	err = tx.QueryRow(ctx,
		`SELECT id,phase,result_digest,cost_micro_usd,error_code
		   FROM research_run_steps
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND plan_id=$4
		    AND temporal_run_id=$5 AND plan_digest=$6 AND step_ordinal=$7
		    AND phase IN ('completed','failed','indeterminate')
		    AND invocation_id=$8 AND tool_name=$9 AND request_digest=$10`,
		identity.TenantID, identity.UserID, identity.TaskID, row.ID,
		identity.TemporalRunID, planRef.PlanDigest, ordinal,
		step.InvocationID, step.ToolName, requestDigest,
	).Scan(&terminal.StepID, &phase, &resultDigest, &terminal.CostMicroUSD, &errorCode)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return ResearchRunStepResolutionV3{}, researchRunDatabaseError("commit started research recovery", err)
		}
		return resolution, nil
	}
	if err != nil {
		return ResearchRunStepResolutionV3{}, researchRunDatabaseError("load research terminal step", err)
	}
	terminal.Phase = ResearchRunStepPhaseV3(phase)
	if resultDigest != nil {
		terminal.ResultDigest = *resultDigest
	}
	if errorCode != nil {
		terminal.ErrorCode = *errorCode
	}
	resolution.Phase, resolution.Receipt = terminal.Phase, terminal
	if err := validateResearchRunSpendSettlementV3(
		ctx, tx, identity, runSnapshotID, row.ID, planRef.PlanDigest,
		terminal, nil); err != nil {
		return ResearchRunStepResolutionV3{}, err
	}
	if terminal.Phase == ResearchRunStepCompletedV3 {
		var evidence ResearchRunEvidenceV3
		evidence.Ordinal = ordinal
		evidence.InvocationID, evidence.ToolName = step.InvocationID, step.ToolName
		evidence.RequestDigest = requestDigest
		err := tx.QueryRow(ctx,
			`SELECT id,started_step_id,result_bytes,result_digest,original_size,
			        truncated,trust_type
			   FROM research_run_evidence
			  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND plan_id=$4
			    AND temporal_run_id=$5 AND plan_digest=$6 AND step_ordinal=$7
			    AND invocation_id=$8 AND tool_name=$9 AND request_digest=$10`,
			identity.TenantID, identity.UserID, identity.TaskID, row.ID,
			identity.TemporalRunID, planRef.PlanDigest, ordinal,
			step.InvocationID, step.ToolName, requestDigest,
		).Scan(&evidence.EvidenceID, &evidence.StartedStepID, &evidence.Result,
			&evidence.ResultDigest, &evidence.OriginalSize, &evidence.Truncated,
			&evidence.TrustType)
		if errors.Is(err, pgx.ErrNoRows) {
			return ResearchRunStepResolutionV3{}, researchRunIntegrityError()
		}
		if err != nil {
			return ResearchRunStepResolutionV3{}, researchRunDatabaseError("load completed research evidence", err)
		}
		if evidence.EvidenceID <= 0 || evidence.StartedStepID != startedStepID ||
			evidence.ResultDigest != terminal.ResultDigest ||
			researchRunSHA256(evidence.Result) != evidence.ResultDigest ||
			evidence.OriginalSize < len(evidence.Result) ||
			evidence.Truncated != (evidence.OriginalSize > len(evidence.Result)) ||
			(evidence.TrustType != "local" && evidence.TrustType != "external") {
			return ResearchRunStepResolutionV3{}, researchRunIntegrityError()
		}
		resolution.Evidence = &evidence
	} else if terminal.Phase != ResearchRunStepFailedV3 &&
		terminal.Phase != ResearchRunStepIndeterminateV3 {
		return ResearchRunStepResolutionV3{}, researchRunIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return ResearchRunStepResolutionV3{}, researchRunDatabaseError("commit research step recovery", err)
	}
	return resolution, nil
}

func (s *Store) CommitResearchRunStepV3(
	ctx context.Context,
	params CommitResearchRunStepV3Params,
) (ResearchRunStepReceiptV3, error) {
	if err := validateResearchRunStepCommitV3(params); err != nil {
		return ResearchRunStepReceiptV3{}, err
	}
	tx, _, err := s.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{}, params.Identity, params.RunSnapshotID)
	if err != nil {
		return ResearchRunStepReceiptV3{}, researchRunDatabaseError("begin research step receipt transaction", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := lockPushEffectSchemaWriter(ctx, tx); err != nil {
		return ResearchRunStepReceiptV3{}, researchRunDatabaseError("lock research schema admission", err)
	}
	if exists, err := lockTenantAdmissionRootShared(ctx, tx,
		params.Identity.TenantID); err != nil {
		return ResearchRunStepReceiptV3{}, researchRunDatabaseError(
			"lock research settlement tenant admission", err)
	} else if !exists {
		return ResearchRunStepReceiptV3{}, researchRunValidationError(
			"research settlement tenant is unavailable")
	}
	if err := lockResearchRunSpendBudgetV3(ctx, tx,
		params.Identity.TemporalRunID); err != nil {
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
	if err := validateResearchProviderTraceV3(
		params.Identity, params.RunSnapshotID, params.PlanRef.PlanDigest,
		params.Ordinal, step, params.ProviderCall); err != nil {
		return ResearchRunStepReceiptV3{}, err
	}
	requestDigest := digestResearchRunStepRequestV3(params.PlanRef.PlanDigest, params.Ordinal)
	var startedStepID int64
	if err := tx.QueryRow(ctx,
		`SELECT id FROM research_run_steps
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND plan_id=$4
		    AND temporal_run_id=$5 AND plan_digest=$6 AND step_ordinal=$7
		    AND phase='started' AND invocation_id=$8 AND tool_name=$9
		    AND request_digest=$10`,
		params.Identity.TenantID, params.Identity.UserID, params.Identity.TaskID,
		row.ID, params.Identity.TemporalRunID, params.PlanRef.PlanDigest,
		params.Ordinal, step.InvocationID, step.ToolName, requestDigest,
	).Scan(&startedStepID); errors.Is(err, pgx.ErrNoRows) {
		return ResearchRunStepReceiptV3{}, researchRunValidationError(
			"research terminal step has no immutable start")
	} else if err != nil {
		return ResearchRunStepReceiptV3{}, researchRunDatabaseError(
			"load research terminal start", err)
	}
	reservation, err := loadResearchRunSpendReservationV3(
		ctx, tx, params.Identity, row.ID, startedStepID, params.Ordinal, requestDigest)
	if err != nil {
		return ResearchRunStepReceiptV3{}, err
	}
	if err := params.ProviderCall.validateForTerminal(
		params.Phase, params.CostMicroUSD,
		reservation.ReservedCostMicroUSD); err != nil {
		return ResearchRunStepReceiptV3{}, err
	}
	if receipt, found, err := loadResearchRunTerminalStepV3(
		ctx, tx, params, row.ID, step, requestDigest,
	); err != nil {
		return ResearchRunStepReceiptV3{}, err
	} else if found {
		if err := validateResearchRunSpendSettlementV3(
			ctx, tx, params.Identity, params.RunSnapshotID, row.ID,
			params.PlanRef.PlanDigest, receipt, &params.ProviderCall); err != nil {
			return ResearchRunStepReceiptV3{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return ResearchRunStepReceiptV3{}, researchRunDatabaseError("commit research step replay", err)
		}
		return receipt, nil
	}
	var toolCallID *int64
	actualQuotaUnits := researchRunQuotaUnitsV3
	if params.ProviderCall.Attempted {
		sealedToolCallID, err := insertResearchProviderToolCallV3(
			ctx, tx, params.Identity, reservation, params.ProviderCall.TraceID,
			step.Arguments, nil, 0, params.ErrorCode, params.ProviderCall)
		if err != nil {
			return ResearchRunStepReceiptV3{}, err
		}
		toolCallID = &sealedToolCallID
	} else {
		// actual_quota_units=0 records that the Provider was never attempted. The
		// admitted reservation and migration 096's quota_floor_policy_version=1
		// remain a permanent quota and cost floor; no compensation is issued.
		actualQuotaUnits = 0
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
	pricingStatus := "unpriced"
	currency := "USD"
	if params.ProviderCall.Attempted && params.ProviderCall.CostKnown {
		pricingStatus = params.ProviderCall.PricingStatus
		currency = params.ProviderCall.CostCurrency
	} else if params.ProviderCall.Attempted {
		pricingStatus = "estimated"
	}
	if err := insertResearchRunSpendSettlementV3(
		ctx, tx, params.Identity, reservation, stepID, params.Phase, toolCallID,
		params.CostMicroUSD, actualQuotaUnits, pricingStatus, currency); err != nil {
		return ResearchRunStepReceiptV3{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ResearchRunStepReceiptV3{}, researchRunDatabaseError("commit research step terminal receipt", err)
	}
	return researchRunStepReceiptV3(stepID, params, step, requestDigest), nil
}

func (s *Store) CommitResearchRunStepEvidenceV3(
	ctx context.Context,
	params CommitResearchRunStepEvidenceV3Params,
) (ResearchRunStepEvidenceReceiptV3, error) {
	if err := validateResearchRunStepEvidenceCommitV3(params); err != nil {
		return ResearchRunStepEvidenceReceiptV3{}, err
	}
	resultDigest := researchRunSHA256(params.Result)
	terminalParams := CommitResearchRunStepV3Params{
		Identity: params.Identity, RunSnapshotID: params.RunSnapshotID,
		PlanRef: params.PlanRef, Ordinal: params.Ordinal,
		Phase: ResearchRunStepCompletedV3, ResultDigest: resultDigest,
		CostMicroUSD: params.CostMicroUSD,
	}
	tx, _, err := s.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{}, params.Identity, params.RunSnapshotID)
	if err != nil {
		return ResearchRunStepEvidenceReceiptV3{}, researchRunDatabaseError("begin research evidence transaction", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := lockPushEffectSchemaWriter(ctx, tx); err != nil {
		return ResearchRunStepEvidenceReceiptV3{}, researchRunDatabaseError("lock research schema admission", err)
	}
	if exists, err := lockTenantAdmissionRootShared(ctx, tx,
		params.Identity.TenantID); err != nil {
		return ResearchRunStepEvidenceReceiptV3{}, researchRunDatabaseError(
			"lock research evidence tenant admission", err)
	} else if !exists {
		return ResearchRunStepEvidenceReceiptV3{}, researchRunValidationError(
			"research evidence tenant is unavailable")
	}
	if err := lockResearchRunSpendBudgetV3(ctx, tx,
		params.Identity.TemporalRunID); err != nil {
		return ResearchRunStepEvidenceReceiptV3{}, err
	}
	if err := lockResearchRunStepV3(ctx, tx, params.Identity.TemporalRunID,
		params.PlanRef.PlanDigest, params.Ordinal); err != nil {
		return ResearchRunStepEvidenceReceiptV3{}, err
	}
	plan, row, err := loadAndValidateResearchRunPlanV3(
		ctx, tx, params.Identity, params.RunSnapshotID, params.PlanRef)
	if err != nil {
		return ResearchRunStepEvidenceReceiptV3{}, err
	}
	if params.Ordinal >= len(plan.Steps) {
		return ResearchRunStepEvidenceReceiptV3{}, researchRunValidationError("research evidence ordinal is outside the plan")
	}
	step := plan.Steps[params.Ordinal]
	if err := validateResearchProviderTraceV3(
		params.Identity, params.RunSnapshotID, params.PlanRef.PlanDigest,
		params.Ordinal, step, params.ProviderCall); err != nil {
		return ResearchRunStepEvidenceReceiptV3{}, err
	}
	requestDigest := digestResearchRunStepRequestV3(params.PlanRef.PlanDigest, params.Ordinal)
	if terminal, found, err := loadResearchRunTerminalStepV3(
		ctx, tx, terminalParams, row.ID, step, requestDigest,
	); err != nil {
		return ResearchRunStepEvidenceReceiptV3{}, err
	} else if found {
		if err := validateResearchRunSpendSettlementV3(
			ctx, tx, params.Identity, params.RunSnapshotID, row.ID,
			params.PlanRef.PlanDigest, terminal, &params.ProviderCall); err != nil {
			return ResearchRunStepEvidenceReceiptV3{}, err
		}
		receipt, err := loadResearchRunEvidenceReceiptV3(
			ctx, tx, params, terminal, row.ID, step, requestDigest, resultDigest)
		if err != nil {
			return ResearchRunStepEvidenceReceiptV3{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return ResearchRunStepEvidenceReceiptV3{}, researchRunDatabaseError("commit research evidence replay", err)
		}
		return receipt, nil
	}
	var startedStepID int64
	if err := tx.QueryRow(ctx,
		`SELECT id FROM research_run_steps
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND plan_id=$4
		    AND temporal_run_id=$5 AND plan_digest=$6 AND step_ordinal=$7
		    AND phase='started' AND invocation_id=$8 AND tool_name=$9
		    AND request_digest=$10`,
		params.Identity.TenantID, params.Identity.UserID, params.Identity.TaskID,
		row.ID, params.Identity.TemporalRunID, params.PlanRef.PlanDigest,
		params.Ordinal, step.InvocationID, step.ToolName, requestDigest,
	).Scan(&startedStepID); errors.Is(err, pgx.ErrNoRows) {
		return ResearchRunStepEvidenceReceiptV3{}, researchRunValidationError("research evidence has no immutable start")
	} else if err != nil {
		return ResearchRunStepEvidenceReceiptV3{}, researchRunDatabaseError("load research evidence start", err)
	}
	reservation, err := loadResearchRunSpendReservationV3(
		ctx, tx, params.Identity, row.ID, startedStepID, params.Ordinal, requestDigest)
	if err != nil {
		return ResearchRunStepEvidenceReceiptV3{}, err
	}
	if reservation.RunSnapshotID != params.RunSnapshotID ||
		reservation.PlanDigest != params.PlanRef.PlanDigest ||
		reservation.InvocationID != step.InvocationID || reservation.ToolName != step.ToolName {
		return ResearchRunStepEvidenceReceiptV3{}, researchRunIntegrityError()
	}
	if params.CostMicroUSD > reservation.ReservedCostMicroUSD {
		return ResearchRunStepEvidenceReceiptV3{}, researchRunValidationError(
			"completed research spend exceeds its frozen Tool cap")
	}
	toolCallID, err := insertResearchProviderToolCallV3(
		ctx, tx, params.Identity, reservation, params.ProviderCall.TraceID,
		step.Arguments, params.Result, params.OriginalSize, "", params.ProviderCall)
	if err != nil {
		return ResearchRunStepEvidenceReceiptV3{}, err
	}
	truncated := params.OriginalSize > len(params.Result)
	var evidenceID int64
	if err := tx.QueryRow(ctx,
		`INSERT INTO research_run_evidence (
		     tenant_id,user_id,task_id,plan_id,started_step_id,temporal_run_id,
		     plan_digest,step_ordinal,invocation_id,tool_name,request_digest,
		     result_bytes,result_digest,original_size,truncated,trust_type,schema_version
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
		 RETURNING id`,
		params.Identity.TenantID, params.Identity.UserID, params.Identity.TaskID,
		row.ID, startedStepID, params.Identity.TemporalRunID,
		params.PlanRef.PlanDigest, params.Ordinal, step.InvocationID, step.ToolName,
		requestDigest, params.Result, resultDigest, params.OriginalSize, truncated,
		params.TrustType, researchRunEvidenceSchemaV3,
	).Scan(&evidenceID); err != nil {
		return ResearchRunStepEvidenceReceiptV3{}, researchRunDatabaseError("seal research Tool evidence", err)
	}
	var terminalStepID int64
	if err := tx.QueryRow(ctx,
		`INSERT INTO research_run_steps (
		     tenant_id,user_id,task_id,plan_id,temporal_run_id,plan_digest,
		     step_ordinal,phase,invocation_id,tool_name,request_digest,
		     result_digest,cost_micro_usd,error_code,schema_version
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,'completed',$8,$9,$10,$11,$12,NULL,$13)
		 RETURNING id`,
		params.Identity.TenantID, params.Identity.UserID, params.Identity.TaskID,
		row.ID, params.Identity.TemporalRunID, params.PlanRef.PlanDigest,
		params.Ordinal, step.InvocationID, step.ToolName, requestDigest,
		resultDigest, params.CostMicroUSD, researchRunStepSchemaV3,
	).Scan(&terminalStepID); err != nil {
		return ResearchRunStepEvidenceReceiptV3{}, researchRunDatabaseError("seal completed research step", err)
	}
	if err := insertResearchRunSpendSettlementV3(
		ctx, tx, params.Identity, reservation, terminalStepID,
		ResearchRunStepCompletedV3, &toolCallID, params.CostMicroUSD,
		researchRunQuotaUnitsV3, params.ProviderCall.PricingStatus,
		params.ProviderCall.CostCurrency,
	); err != nil {
		return ResearchRunStepEvidenceReceiptV3{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ResearchRunStepEvidenceReceiptV3{}, researchRunDatabaseError("commit research Tool evidence", err)
	}
	return ResearchRunStepEvidenceReceiptV3{
		ResearchRunStepReceiptV3: researchRunStepReceiptV3(
			terminalStepID, terminalParams, step, requestDigest),
		EvidenceID: evidenceID, OriginalSize: params.OriginalSize,
		Truncated: truncated, TrustType: params.TrustType,
	}, nil
}

func validateResearchPlanCreateV3(params CreateOrGetResearchRunPlanV3Params) error {
	if err := validateResearchPlanScopeV3(params); err != nil {
		return err
	}
	if params.PlannerLLMReservationID <= 0 ||
		params.Plan.Validate() != nil || params.Plan.DefinitionDigest == "" ||
		params.Plan.CapabilityCatalogDigest == "" || params.Plan.ToolPolicyDigest == "" {
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
		     plan_digest,plan_payload,planner_llm_spend_reservation_id,schema_version
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		 RETURNING id,tenant_id,user_id,task_id,run_snapshot_id,
		           temporal_workflow_id,temporal_run_id,definition_digest,
		           capability_catalog_digest,plan_digest,plan_payload,
		           planner_llm_spend_reservation_id`,
		identity.TenantID, identity.UserID, identity.TaskID, params.RunSnapshotID,
		identity.TemporalWorkflowID, identity.TemporalRunID,
		params.Plan.DefinitionDigest, params.Plan.CapabilityCatalogDigest,
		planDigest, payload, params.PlannerLLMReservationID, researchRunPlanSchemaV3))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return researchRunPlanRowV3{}, researchRunConflictError()
		}
		return researchRunPlanRowV3{}, researchRunDatabaseError("insert research run plan", err)
	}
	row.ToolPolicyDigest = params.Plan.ToolPolicyDigest
	return row, nil
}

func validateResearchPlanSnapshotPolicyV3(
	ctx context.Context, tx pgx.Tx, params CreateOrGetResearchRunPlanV3Params,
) (types.PlannerBudget, error) {
	row, found, err := loadResearchRunSnapshotRowV3(ctx, tx,
		CreateOrGetTaskRunSnapshotParams{
			TenantID: params.Identity.TenantID, UserID: params.Identity.UserID,
			TaskID:             params.Identity.TaskID,
			TemporalWorkflowID: params.Identity.TemporalWorkflowID,
			TemporalRunID:      params.Identity.TemporalRunID,
		})
	if err != nil {
		return types.PlannerBudget{}, err
	}
	if !found || row.ID != params.RunSnapshotID {
		return types.PlannerBudget{}, researchRunValidationError(
			"research plan snapshot is unavailable")
	}
	ref, err := validateStoredResearchRunSnapshotV3(params.Identity, row)
	if err != nil || ref.SnapshotID != params.RunSnapshotID ||
		ref.CapabilityCatalogDigest != params.Plan.CapabilityCatalogDigest ||
		ref.ToolPolicyDigest != params.Plan.ToolPolicyDigest ||
		ref.DefinitionDigest != params.Plan.DefinitionDigest {
		return types.PlannerBudget{}, researchRunIntegrityError()
	}
	return ref.PlannerBudget, nil
}

func loadResearchRunPlanV3(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity, snapshotID int64,
) (researchRunPlanRowV3, bool, error) {
	row, err := scanResearchRunPlanV3(tx.QueryRow(ctx,
		`SELECT plan.id,plan.tenant_id,plan.user_id,plan.task_id,plan.run_snapshot_id,
		        plan.temporal_workflow_id,plan.temporal_run_id,plan.definition_digest,
		        plan.capability_catalog_digest,plan.plan_digest,plan.plan_payload,
		        plan.planner_llm_spend_reservation_id
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
	var snapshotToolPolicyDigest string
	if err := tx.QueryRow(ctx,
		`SELECT tool_policy_digest FROM task_run_snapshots
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND task_id=$4
		    AND temporal_workflow_id=$5 AND temporal_run_id=$6
		    AND reference_schema_version=$7`,
		snapshotID, identity.TenantID, identity.UserID, identity.TaskID,
		identity.TemporalWorkflowID, identity.TemporalRunID,
		types.ResearchRunSnapshotRefSchemaV3,
	).Scan(&snapshotToolPolicyDigest); err != nil || plan.ToolPolicyDigest != snapshotToolPolicyDigest {
		return researchRunPlanRowV3{}, false, researchRunIntegrityError()
	}
	row.ToolPolicyDigest = plan.ToolPolicyDigest
	return row, true, nil
}

type researchRunPlanScannerV3 interface{ Scan(...any) error }

func scanResearchRunPlanV3(scanner researchRunPlanScannerV3) (researchRunPlanRowV3, error) {
	var row researchRunPlanRowV3
	err := scanner.Scan(&row.ID, &row.TenantID, &row.UserID, &row.TaskID,
		&row.RunSnapshotID, &row.TemporalWorkflowID, &row.TemporalRunID,
		&row.DefinitionDigest, &row.CapabilityCatalogDigest,
		&row.PlanDigest, &row.PlanPayload, &row.PlannerLLMReservationID)
	return row, err
}

func researchRunPlanRefV3(row researchRunPlanRowV3) (types.ResearchRunPlanRefV3, error) {
	plan, err := runcontext.DecodeResearchExecutionPlanV3(row.PlanPayload)
	if err != nil {
		return types.ResearchRunPlanRefV3{}, err
	}
	return types.SealResearchRunPlanRefV3(types.ResearchRunPlanRefV3{
		PlanID: row.ID, RunSnapshotID: row.RunSnapshotID,
		TemporalWorkflowID: row.TemporalWorkflowID, TemporalRunID: row.TemporalRunID,
		TenantID: row.TenantID, UserID: row.UserID, TaskID: row.TaskID,
		DefinitionDigest:        row.DefinitionDigest,
		CapabilityCatalogDigest: row.CapabilityCatalogDigest,
		ToolPolicyDigest:        row.ToolPolicyDigest,
		PlanDigest:              row.PlanDigest,
		StepCount:               len(plan.Steps),
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
		row.CapabilityCatalogDigest != ref.CapabilityCatalogDigest ||
		row.ToolPolicyDigest != ref.ToolPolicyDigest {
		return runcontext.ResearchExecutionPlanV3{}, researchRunPlanRowV3{}, researchRunConflictError()
	}
	plan, err := runcontext.DecodeResearchExecutionPlanV3(row.PlanPayload)
	if err != nil || plan.DefinitionDigest != row.DefinitionDigest ||
		plan.CapabilityCatalogDigest != row.CapabilityCatalogDigest ||
		plan.ToolPolicyDigest != row.ToolPolicyDigest ||
		len(plan.Steps) != ref.StepCount {
		return runcontext.ResearchExecutionPlanV3{}, researchRunPlanRowV3{}, researchRunIntegrityError()
	}
	return plan, row, nil
}

func authorizeResearchRunEffectV3(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity, runSnapshotID int64,
) error {
	if runSnapshotID <= 0 {
		return researchRunValidationError("research run effect snapshot is invalid")
	}
	var capabilityAvailable bool
	if err := tx.QueryRow(ctx, `SELECT to_regprocedure(
		'public.authorize_research_run_effect_cap_v1(bigint)') IS NOT NULL`,
	).Scan(&capabilityAvailable); err != nil {
		return researchRunDatabaseError("inspect research run effect capability", err)
	}
	if !capabilityAvailable {
		return authorizeLegacyResearchRunEffectV3(ctx, tx, identity)
	}
	var authorized int
	if err := tx.QueryRow(ctx,
		`SELECT 1 WHERE public.authorize_research_run_effect_cap_v1($1)`,
		runSnapshotID).Scan(&authorized); err != nil {
		var pgErr *pgconn.PgError
		if errors.Is(err, pgx.ErrNoRows) ||
			(errors.As(err, &pgErr) && pgErr.Code == "42501") {
			return types.NewAppError(types.CodeValidation,
				"research run 当前不允许外部调用", types.ErrValidation)
		}
		return researchRunDatabaseError("authorize research run effect", err)
	}
	return nil
}

// authorizeLegacyResearchRunEffectV3 preserves replay for databases that are
// intentionally held before migration 108. It is the exact formal-only fence
// that preceded the capability function; shadow admission never falls back.
func authorizeLegacyResearchRunEffectV3(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity,
) error {
	var authorized int
	if err := tx.QueryRow(ctx,
		`SELECT 1 FROM schedules schedule
		 JOIN tenants tenant ON tenant.id=schedule.tenant_id AND tenant.status='active'
		 JOIN memberships membership
		   ON membership.tenant_id=schedule.tenant_id AND membership.user_id=schedule.user_id
		WHERE schedule.id=$1 AND schedule.tenant_id=$2 AND schedule.user_id=$3
		  AND (schedule.status='active' OR (
		      schedule.status='paused' AND public.authorize_research_manual_task_run_cap_v1(
		          schedule.tenant_id,schedule.user_id,schedule.id,$4
		      )
		  )) AND schedule.execution_mode='discover_at_run'
		FOR SHARE OF schedule,tenant,membership`,
		identity.TaskID, identity.TenantID, identity.UserID,
		identity.TemporalWorkflowID).Scan(&authorized); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return types.NewAppError(types.CodeValidation,
				"research run 当前不允许外部调用", types.ErrValidation)
		}
		return researchRunDatabaseError("authorize legacy research run effect", err)
	}
	return nil
}

func validateResearchRunStepCommitV3(params CommitResearchRunStepV3Params) error {
	if err := params.PlanRef.ValidateFor(params.Identity, params.RunSnapshotID); err != nil ||
		params.Ordinal < 0 || params.Ordinal >= 16 || params.CostMicroUSD < 0 ||
		(params.Phase != ResearchRunStepFailedV3 &&
			params.Phase != ResearchRunStepIndeterminateV3) {
		return researchRunValidationError("research terminal step is invalid")
	}
	if params.ResultDigest != "" || !validResearchRunErrorCode(params.ErrorCode) {
		return researchRunValidationError("failed research step receipt is invalid")
	}
	return nil
}

func validateResearchRunStepEvidenceCommitV3(
	params CommitResearchRunStepEvidenceV3Params,
) error {
	if err := params.PlanRef.ValidateFor(params.Identity, params.RunSnapshotID); err != nil ||
		params.Ordinal < 0 || params.Ordinal >= params.PlanRef.StepCount ||
		len(params.Result) > types.MaxModelVisibleToolResultBytes || !utf8.Valid(params.Result) ||
		bytes.IndexByte(params.Result, 0) >= 0 ||
		params.OriginalSize < len(params.Result) || params.OriginalSize > 2147483647 ||
		(params.TrustType != "local" && params.TrustType != "external") ||
		params.CostMicroUSD < 0 ||
		params.ProviderCall.validateForCompleted(params.CostMicroUSD) != nil {
		return researchRunValidationError("research Tool evidence is invalid")
	}
	return nil
}

func validateResearchProviderTraceV3(
	identity types.RunIdentity,
	runSnapshotID int64,
	planDigest string,
	ordinal int,
	step runcontext.ResearchPlanStepV3,
	call ResearchProviderCallV3,
) error {
	if !call.Attempted {
		return nil
	}
	expected, err := runcontext.ResearchExecutionTraceV3(
		identity, runSnapshotID, planDigest, ordinal, step.InvocationID)
	if err != nil || call.TraceID != expected {
		return researchRunValidationError(
			"research provider receipt does not match its immutable plan step")
	}
	return nil
}

func loadResearchRunEvidenceReceiptV3(
	ctx context.Context, tx pgx.Tx, params CommitResearchRunStepEvidenceV3Params,
	terminal ResearchRunStepReceiptV3, planID int64,
	step runcontext.ResearchPlanStepV3, requestDigest, resultDigest string,
) (ResearchRunStepEvidenceReceiptV3, error) {
	var (
		evidenceID, storedPlanID  int64
		storedResult              []byte
		storedOriginal            int
		storedTruncated           bool
		storedTrust, storedDigest string
	)
	err := tx.QueryRow(ctx,
		`SELECT id,plan_id,result_bytes,result_digest,original_size,truncated,trust_type
		   FROM research_run_evidence
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND plan_id=$4
		    AND temporal_run_id=$5 AND plan_digest=$6 AND step_ordinal=$7
		    AND invocation_id=$8 AND tool_name=$9 AND request_digest=$10`,
		params.Identity.TenantID, params.Identity.UserID, params.Identity.TaskID,
		planID, params.Identity.TemporalRunID, params.PlanRef.PlanDigest,
		params.Ordinal, step.InvocationID, step.ToolName, requestDigest,
	).Scan(&evidenceID, &storedPlanID, &storedResult, &storedDigest,
		&storedOriginal, &storedTruncated, &storedTrust)
	if errors.Is(err, pgx.ErrNoRows) {
		return ResearchRunStepEvidenceReceiptV3{}, researchRunIntegrityError()
	}
	if err != nil {
		return ResearchRunStepEvidenceReceiptV3{}, researchRunDatabaseError("load research Tool evidence", err)
	}
	if evidenceID <= 0 || storedPlanID != planID ||
		!bytes.Equal(storedResult, params.Result) || storedDigest != resultDigest ||
		storedOriginal != params.OriginalSize ||
		storedTruncated != (params.OriginalSize > len(params.Result)) ||
		storedTrust != params.TrustType {
		return ResearchRunStepEvidenceReceiptV3{}, researchRunConflictError()
	}
	return ResearchRunStepEvidenceReceiptV3{
		ResearchRunStepReceiptV3: terminal, EvidenceID: evidenceID,
		OriginalSize: storedOriginal, Truncated: storedTruncated, TrustType: storedTrust,
	}, nil
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

func digestResearchRunStepRequestV3(planDigest string, ordinal int) string {
	return researchRunSHA256([]byte(fmt.Sprintf(
		"vane.research-step-request/v4:%s:%d", planDigest, ordinal)))
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
