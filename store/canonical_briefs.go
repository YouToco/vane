package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

const canonicalBriefLockNamespaceV1 = "vane/canonical-brief/v1/"
const canonicalBriefLockSeedV1 = int64(0x42524631)

const runOutcomeColumnsV1 = `id,tenant_id,user_id,task_id,run_snapshot_id,
	schema_version,status,result,source_coverage,processing,failure_code,
	failure_message,finalized_at,outcome_digest,created_at`

type runOutcomeRowV1 struct {
	ID             int64
	TenantID       int64
	UserID         int64
	TaskID         string
	RunSnapshotID  int64
	SchemaVersion  string
	Status         string
	Result         sql.NullString
	SourceCoverage sql.NullString
	Processing     sql.NullString
	FailureCode    string
	FailureMessage string
	FinalizedAt    sql.NullTime
	OutcomeDigest  sql.NullString
}

func scanRunOutcomeRowV1(row pgx.Row) (runOutcomeRowV1, error) {
	var stored runOutcomeRowV1
	var createdAt sql.NullTime
	err := row.Scan(
		&stored.ID, &stored.TenantID, &stored.UserID, &stored.TaskID,
		&stored.RunSnapshotID, &stored.SchemaVersion, &stored.Status,
		&stored.Result, &stored.SourceCoverage, &stored.Processing,
		&stored.FailureCode, &stored.FailureMessage, &stored.FinalizedAt,
		&stored.OutcomeDigest, &createdAt,
	)
	return stored, err
}

func (r runOutcomeRowV1) marker() types.RunOutcomeMarkerV1 {
	return types.RunOutcomeMarkerV1{
		ID: r.ID, SchemaVersion: r.SchemaVersion,
		RunSnapshotID: r.RunSnapshotID, TenantID: r.TenantID,
		UserID: r.UserID, TaskID: r.TaskID,
	}
}

func (r runOutcomeRowV1) finalized() (types.RunOutcomeV1, error) {
	if r.Status != "finalized" || !r.Result.Valid ||
		!r.SourceCoverage.Valid || !r.Processing.Valid ||
		!r.FinalizedAt.Valid || !r.OutcomeDigest.Valid {
		return types.RunOutcomeV1{}, canonicalBriefIntegrityError()
	}
	outcome := types.RunOutcomeV1{
		RunOutcomeMarkerV1: r.marker(),
		Result:             types.RunResultV1(r.Result.String),
		SourceCoverage:     types.RunCompletenessV1(r.SourceCoverage.String),
		Processing:         types.RunCompletenessV1(r.Processing.String),
		FailureCode:        r.FailureCode,
		FailureMessage:     r.FailureMessage,
		FinalizedAt:        r.FinalizedAt.Time.Round(0).UTC().Truncate(time.Microsecond),
		Digest:             r.OutcomeDigest.String,
	}
	if err := outcome.Validate(); err != nil {
		return types.RunOutcomeV1{}, canonicalBriefIntegrityError()
	}
	return outcome, nil
}

// CreatePendingRunOutcomeV1 creates or recovers the one pending marker owned
// by an exact immutable run. P1-A intentionally has no production caller.
func (s *Store) CreatePendingRunOutcomeV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
) (types.RunOutcomeMarkerV1, error) {
	tx, err := s.beginCanonicalBriefTxV1(ctx, expected, ref)
	if err != nil {
		return types.RunOutcomeMarkerV1{}, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockCanonicalBriefRunV1(ctx, tx, ref.SnapshotID); err != nil {
		return types.RunOutcomeMarkerV1{}, err
	}
	var recoveryAvailable bool
	if err := tx.QueryRow(ctx, `
		SELECT to_regprocedure(
		    'read_stale_run_outcomes_v1(timestamptz,bigint,integer)'
		) IS NOT NULL`,
	).Scan(&recoveryAvailable); err != nil {
		return types.RunOutcomeMarkerV1{},
			canonicalBriefDatabaseError(
				"check run outcome recovery capability", err)
	}
	if !recoveryAvailable {
		return types.RunOutcomeMarkerV1{},
			canonicalBriefDatabaseError(
				"check run outcome recovery capability",
				errors.New("run outcome recovery capability is unavailable"))
	}

	stored, found, err := loadRunOutcomeForSnapshotV1(
		ctx, tx, ref.SnapshotID, false)
	if err != nil {
		return types.RunOutcomeMarkerV1{}, err
	}
	if found {
		marker := stored.marker()
		if err := marker.Validate(); err != nil ||
			marker.TenantID != expected.TenantID ||
			marker.UserID != expected.UserID ||
			marker.TaskID != expected.TaskID {
			return types.RunOutcomeMarkerV1{}, canonicalBriefIntegrityError()
		}
		if err := commitCanonicalBriefTxV1(ctx, tx, "recover run outcome marker"); err != nil {
			return types.RunOutcomeMarkerV1{}, err
		}
		return marker, nil
	}

	stored, err = scanRunOutcomeRowV1(tx.QueryRow(ctx,
		`INSERT INTO task_run_outcomes (
		    tenant_id,user_id,task_id,run_snapshot_id,schema_version
		 ) VALUES ($1,$2,$3,$4,$5)
		 RETURNING `+runOutcomeColumnsV1,
		expected.TenantID, expected.UserID, expected.TaskID,
		ref.SnapshotID, types.RunOutcomeSchemaVersionV1,
	))
	if err != nil {
		return types.RunOutcomeMarkerV1{},
			canonicalBriefDatabaseError("create run outcome marker", err)
	}
	marker := stored.marker()
	if err := marker.Validate(); err != nil {
		return types.RunOutcomeMarkerV1{}, canonicalBriefIntegrityError()
	}
	if err := commitCanonicalBriefTxV1(ctx, tx, "commit run outcome marker"); err != nil {
		return types.RunOutcomeMarkerV1{}, err
	}
	return marker, nil
}

// FinalizeRunOutcomeV1 performs the pending->finalized CAS. An exact replay
// returns the stored immutable outcome; a different terminal claim conflicts.
func (s *Store) FinalizeRunOutcomeV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	outcome types.RunOutcomeV1,
) (types.RunOutcomeV1, error) {
	if err := outcome.Validate(); err != nil ||
		outcome.RunSnapshotID != ref.SnapshotID ||
		outcome.TenantID != expected.TenantID ||
		outcome.UserID != expected.UserID ||
		outcome.TaskID != expected.TaskID {
		return types.RunOutcomeV1{},
			canonicalBriefValidationError("run outcome finalization is invalid")
	}
	tx, err := s.beginCanonicalBriefTxV1(ctx, expected, ref)
	if err != nil {
		return types.RunOutcomeV1{}, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockCanonicalBriefRunV1(ctx, tx, ref.SnapshotID); err != nil {
		return types.RunOutcomeV1{}, err
	}
	stored, found, err := loadRunOutcomeForSnapshotV1(
		ctx, tx, ref.SnapshotID, true)
	if err != nil {
		return types.RunOutcomeV1{}, err
	}
	if !found || stored.ID != outcome.ID {
		return types.RunOutcomeV1{},
			canonicalBriefNotFoundError("run outcome marker is unavailable")
	}
	if stored.Status == "finalized" {
		replayed, err := stored.finalized()
		if err != nil {
			return types.RunOutcomeV1{}, err
		}
		if replayed.Digest != outcome.Digest {
			return types.RunOutcomeV1{},
				canonicalBriefConflictError("run outcome already finalized differently")
		}
		if err := commitCanonicalBriefTxV1(ctx, tx, "replay run outcome finalization"); err != nil {
			return types.RunOutcomeV1{}, err
		}
		return replayed, nil
	}
	if stored.Status != "pending" {
		return types.RunOutcomeV1{}, canonicalBriefIntegrityError()
	}

	tag, err := tx.Exec(ctx,
		`UPDATE task_run_outcomes
		    SET status='finalized',result=$2,source_coverage=$3,
		        processing=$4,failure_code=$5,failure_message=$6,
		        finalized_at=$7,outcome_digest=$8
		  WHERE id=$1 AND status='pending'`,
		outcome.ID, outcome.Result, outcome.SourceCoverage,
		outcome.Processing, outcome.FailureCode, outcome.FailureMessage,
		outcome.FinalizedAt, outcome.Digest,
	)
	if err != nil {
		return types.RunOutcomeV1{},
			canonicalBriefDatabaseError("finalize run outcome", err)
	}
	if tag.RowsAffected() != 1 {
		return types.RunOutcomeV1{},
			canonicalBriefConflictError("run outcome finalization lost its CAS")
	}
	if err := commitCanonicalBriefTxV1(ctx, tx, "commit run outcome finalization"); err != nil {
		return types.RunOutcomeV1{}, err
	}
	return outcome, nil
}

// FinalizeRunOutcomeClaimV1 performs the production pending->finalized CAS.
// Workflow and recovery callers submit no timestamp or digest: both are bound
// to database time while the exact run marker is locked. A response-lost retry
// of the same semantic claim returns the original immutable result.
func (s *Store) FinalizeRunOutcomeClaimV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	claim types.RunOutcomeClaimV1,
) (types.RunOutcomeV1, error) {
	if err := claim.Validate(); err != nil ||
		claim.RunSnapshotID != ref.SnapshotID ||
		claim.TenantID != expected.TenantID ||
		claim.UserID != expected.UserID ||
		claim.TaskID != expected.TaskID {
		return types.RunOutcomeV1{},
			canonicalBriefValidationError("run outcome claim is invalid")
	}
	tx, err := s.beginCanonicalBriefTxV1(ctx, expected, ref)
	if err != nil {
		return types.RunOutcomeV1{}, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	outcome, err := finalizeRunOutcomeClaimTxV1(ctx, tx, claim)
	if err != nil {
		return types.RunOutcomeV1{}, err
	}
	if err := commitCanonicalBriefTxV1(ctx, tx, "commit run outcome claim"); err != nil {
		return types.RunOutcomeV1{}, err
	}
	return outcome, nil
}

func finalizeRunOutcomeClaimTxV1(
	ctx context.Context,
	tx pgx.Tx,
	claim types.RunOutcomeClaimV1,
) (types.RunOutcomeV1, error) {
	if err := lockCanonicalBriefRunV1(
		ctx, tx, claim.RunSnapshotID); err != nil {
		return types.RunOutcomeV1{}, err
	}
	stored, found, err := loadRunOutcomeForSnapshotV1(
		ctx, tx, claim.RunSnapshotID, true)
	if err != nil {
		return types.RunOutcomeV1{}, err
	}
	if !found || stored.ID != claim.ID {
		return types.RunOutcomeV1{},
			canonicalBriefNotFoundError("run outcome marker is unavailable")
	}
	claim, err = normalizeCanonicalBriefTerminalClaimV1(
		ctx, tx, claim)
	if err != nil {
		return types.RunOutcomeV1{}, err
	}
	if stored.Status == "finalized" {
		replayed, err := stored.finalized()
		if err != nil {
			return types.RunOutcomeV1{}, err
		}
		if !claim.Matches(replayed) {
			return types.RunOutcomeV1{},
				canonicalBriefConflictError("run outcome already finalized differently")
		}
		if err := resolveCanonicalBriefStageTxV1(ctx, tx, replayed); err != nil {
			return types.RunOutcomeV1{}, err
		}
		return replayed, nil
	}
	if stored.Status != "pending" {
		return types.RunOutcomeV1{}, canonicalBriefIntegrityError()
	}

	var finalizedAt time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&finalizedAt); err != nil {
		return types.RunOutcomeV1{},
			canonicalBriefDatabaseError("read run outcome finalization time", err)
	}
	outcome, err := claim.SealAt(finalizedAt)
	if err != nil {
		return types.RunOutcomeV1{}, canonicalBriefIntegrityError()
	}
	tag, err := tx.Exec(ctx,
		`UPDATE task_run_outcomes
		    SET status='finalized',result=$2,source_coverage=$3,
		        processing=$4,failure_code=$5,failure_message=$6,
		        finalized_at=$7,outcome_digest=$8
		  WHERE id=$1 AND status='pending'`,
		outcome.ID, outcome.Result, outcome.SourceCoverage,
		outcome.Processing, outcome.FailureCode, outcome.FailureMessage,
		outcome.FinalizedAt, outcome.Digest,
	)
	if err != nil {
		return types.RunOutcomeV1{},
			canonicalBriefDatabaseError("finalize run outcome claim", err)
	}
	if tag.RowsAffected() != 1 {
		return types.RunOutcomeV1{},
			canonicalBriefConflictError("run outcome claim lost its CAS")
	}
	if err := resolveCanonicalBriefStageTxV1(ctx, tx, outcome); err != nil {
		return types.RunOutcomeV1{}, err
	}
	return outcome, nil
}

func resolveCanonicalBriefStageTxV1(
	ctx context.Context,
	tx pgx.Tx,
	outcome types.RunOutcomeV1,
) error {
	available, err := canonicalBriefStageCapabilityV1(ctx, tx)
	if err != nil {
		return err
	}
	if !available {
		// P1-B histories and safely downgraded deployments have no stage.
		return nil
	}
	stage, found, err := loadCanonicalBriefStageV1(ctx, tx, outcome.ID)
	if err != nil || !found {
		return err
	}
	if outcome.Result == types.RunResultContent {
		switch stage.status {
		case "promoted":
			stored, found, err := loadBriefForOutcomeV1(ctx, tx, outcome.ID)
			if err != nil {
				return err
			}
			if !found || stored.requestDigest != stage.requestDigest ||
				!stage.briefSnapshotID.Valid ||
				stage.briefSnapshotID.Int64 != stored.brief.ID ||
				!stage.resolvedAt.Valid ||
				!stage.resolvedAt.Time.Equal(outcome.FinalizedAt) {
				return canonicalBriefIntegrityError()
			}
			return nil
		case "aborted":
			return canonicalBriefIntegrityError()
		case "staged":
			return promoteCanonicalBriefStageTxV1(
				ctx, tx, stage, outcome.FinalizedAt)
		default:
			return canonicalBriefIntegrityError()
		}
	}
	switch stage.status {
	case "aborted":
		if !stage.resolvedAt.Valid ||
			!stage.resolvedAt.Time.Equal(outcome.FinalizedAt) {
			return canonicalBriefIntegrityError()
		}
		return nil
	case "promoted":
		return canonicalBriefIntegrityError()
	case "staged":
		tag, err := tx.Exec(ctx,
			`UPDATE canonical_brief_stages
			    SET status='aborted',resolved_at=$2
			  WHERE run_outcome_id=$1 AND status='staged'`,
			outcome.ID, outcome.FinalizedAt,
		)
		if err != nil {
			return canonicalBriefDatabaseError(
				"abort canonical Brief stage", err)
		}
		if tag.RowsAffected() != 1 {
			return canonicalBriefIntegrityError()
		}
		return nil
	default:
		return canonicalBriefIntegrityError()
	}
}

func promoteCanonicalBriefStageTxV1(
	ctx context.Context,
	tx pgx.Tx,
	stage canonicalBriefStageV1,
	resolvedAt time.Time,
) error {
	if existing, found, err := loadBriefForOutcomeV1(
		ctx, tx, stage.draft.RunOutcomeID); err != nil {
		return err
	} else if found {
		if existing.requestDigest != stage.requestDigest {
			return canonicalBriefIntegrityError()
		}
		tag, err := tx.Exec(ctx,
			`UPDATE canonical_brief_stages
			    SET status='promoted',brief_snapshot_id=$2,resolved_at=$3
			  WHERE run_outcome_id=$1 AND status='staged'`,
			stage.draft.RunOutcomeID, existing.brief.ID, resolvedAt,
		)
		if err != nil {
			return canonicalBriefDatabaseError(
				"repair canonical Brief stage promotion", err)
		}
		if tag.RowsAffected() != 1 {
			return canonicalBriefIntegrityError()
		}
		return nil
	}

	var briefID int64
	if err := tx.QueryRow(
		ctx, `SELECT nextval('brief_snapshots_id_seq')`).Scan(&briefID); err != nil {
		return canonicalBriefDatabaseError("allocate canonical Brief id", err)
	}
	brief, err := stage.draft.Seal(briefID)
	if err != nil {
		return canonicalBriefIntegrityError()
	}
	payload, err := json.Marshal(brief)
	if err != nil {
		return canonicalBriefIntegrityError()
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO brief_snapshots (
		    id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id,
		    push_batch_id,schema_version,request_digest,payload_digest,
		    payload,insight_count,generated_at
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		brief.ID, brief.TenantID, brief.UserID, brief.TaskID,
		brief.RunOutcomeID, brief.RunSnapshotID, brief.PushBatchID,
		brief.SchemaVersion, stage.requestDigest, brief.Digest, payload,
		len(brief.Insights), brief.GeneratedAt,
	)
	if err != nil {
		return canonicalBriefDatabaseError(
			"promote canonical Brief stage", err)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE canonical_brief_stages
		    SET status='promoted',brief_snapshot_id=$2,resolved_at=$3
		  WHERE run_outcome_id=$1 AND status='staged'`,
		brief.RunOutcomeID, brief.ID, resolvedAt,
	)
	if err != nil {
		return canonicalBriefDatabaseError(
			"complete canonical Brief stage promotion", err)
	}
	if tag.RowsAffected() != 1 {
		return canonicalBriefIntegrityError()
	}
	return nil
}

// LoadPreparedBriefDraftV1 recovers immutable staged bytes before consulting
// mutable task authorization or delivery evidence.
func (s *Store) LoadPreparedBriefDraftV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	marker types.RunOutcomeMarkerV1,
) (types.BriefDraftV1, bool, error) {
	if err := marker.Validate(); err != nil ||
		marker.RunSnapshotID != ref.SnapshotID ||
		marker.TenantID != expected.TenantID ||
		marker.UserID != expected.UserID ||
		marker.TaskID != expected.TaskID {
		return types.BriefDraftV1{}, false,
			canonicalBriefValidationError("brief stage lookup is invalid")
	}
	tx, err := s.beginCanonicalBriefTxV1(ctx, expected, ref)
	if err != nil {
		return types.BriefDraftV1{}, false, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockCanonicalBriefRunV1(ctx, tx, ref.SnapshotID); err != nil {
		return types.BriefDraftV1{}, false, err
	}
	available, err := canonicalBriefStageCapabilityV1(ctx, tx)
	if err != nil {
		return types.BriefDraftV1{}, false, err
	}
	if !available {
		return types.BriefDraftV1{}, false,
			canonicalBriefDatabaseError(
				"check canonical Brief stage capability",
				errors.New("canonical Brief stage capability is unavailable"))
	}
	outcomeRow, found, err := loadRunOutcomeForSnapshotV1(
		ctx, tx, ref.SnapshotID, true)
	if err != nil {
		return types.BriefDraftV1{}, false, err
	}
	if !found || outcomeRow.ID != marker.ID ||
		outcomeRow.marker() != marker {
		return types.BriefDraftV1{}, false,
			canonicalBriefNotFoundError("run outcome marker is unavailable")
	}
	stage, found, err := loadCanonicalBriefStageV1(ctx, tx, marker.ID)
	if err != nil {
		return types.BriefDraftV1{}, false, err
	}
	if found && stage.status != "staged" {
		return types.BriefDraftV1{}, false, canonicalBriefIntegrityError()
	}
	if err := commitCanonicalBriefTxV1(
		ctx, tx, "load canonical Brief stage"); err != nil {
		return types.BriefDraftV1{}, false, err
	}
	if !found {
		return types.BriefDraftV1{}, false, nil
	}
	return stage.draft, true, nil
}

// LoadSealedEmptyBriefBatchV1 recovers the committed empty-plan receipt before
// mutable task or tenant authorization is consulted. It is the nil-draft
// counterpart of LoadPreparedBriefDraftV1.
func (s *Store) LoadSealedEmptyBriefBatchV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	marker types.RunOutcomeMarkerV1,
	traceID string,
) (int64, bool, error) {
	if err := expected.Validate(); err != nil ||
		ref.Validate() != nil || marker.Validate() != nil ||
		traceID == "" || traceID != strings.TrimSpace(traceID) ||
		len(traceID) > 512 ||
		marker.RunSnapshotID != ref.SnapshotID ||
		marker.TenantID != expected.TenantID ||
		marker.UserID != expected.UserID ||
		marker.TaskID != expected.TaskID {
		return 0, false, canonicalBriefValidationError(
			"empty brief receipt lookup is invalid")
	}
	tx, err := s.beginCanonicalBriefTxV1(ctx, expected, ref)
	if err != nil {
		return 0, false, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockCanonicalBriefRunV1(ctx, tx, ref.SnapshotID); err != nil {
		return 0, false, err
	}
	outcomeRow, found, err := loadRunOutcomeForSnapshotV1(
		ctx, tx, ref.SnapshotID, true)
	if err != nil {
		return 0, false, err
	}
	if !found || outcomeRow.ID != marker.ID ||
		outcomeRow.marker() != marker {
		return 0, false, canonicalBriefNotFoundError(
			"run outcome marker is unavailable")
	}
	if _, found, err := loadCanonicalBriefStageV1(
		ctx, tx, marker.ID); err != nil {
		return 0, false, err
	} else if found {
		return 0, false, nil
	}
	var batchID int64
	var briefState, authority string
	physicalKey := compiledPushBatchPhysicalKeyV1(ref.SnapshotID, traceID)
	err = tx.QueryRow(ctx,
		`SELECT id,brief_state,delivery_authority
		   FROM push_batches
		  WHERE tenant_id=$1 AND user_id=$2 AND schedule_id=$3
		    AND run_snapshot_id=$4 AND idempotency_key=$5`,
		expected.TenantID, expected.UserID, expected.TaskID, ref.SnapshotID,
		physicalKey,
	).Scan(&batchID, &briefState, &authority)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := commitCanonicalBriefTxV1(
			ctx, tx, "commit absent empty brief receipt lookup"); err != nil {
			return 0, false, err
		}
		return 0, false, nil
	}
	if err != nil {
		return 0, false, canonicalBriefDatabaseError(
			"load empty canonical Brief batch", err)
	}
	if briefState != "sealed" ||
		authority != string(types.PushBatchDeliveryAuthorityEffect) {
		if err := commitCanonicalBriefTxV1(
			ctx, tx, "commit open empty brief receipt lookup"); err != nil {
			return 0, false, err
		}
		return 0, false, nil
	}
	evidence, err := readBriefEvidenceV1(
		ctx, tx, batchID, ref.SnapshotID)
	if err != nil {
		return 0, false, err
	}
	if len(evidence) != 0 {
		return 0, false, canonicalBriefIntegrityError()
	}
	if err := commitCanonicalBriefTxV1(
		ctx, tx, "commit empty brief receipt lookup"); err != nil {
		return 0, false, err
	}
	return batchID, true, nil
}

// PrepareBriefDraftV1 durably stages the exact post-delivery/pre-render
// payload while the RunOutcome is still pending. It seals the batch in the
// same transaction, so no renderer or sender can observe an unfrozen delivery
// set. Final outcome CAS later promotes the stage for content or aborts it for
// failure/interruption.
func (s *Store) PrepareBriefDraftV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	marker types.RunOutcomeMarkerV1,
	batchID int64,
	generatedAt time.Time,
	orderedDeliveryIDs []int64,
) (types.BriefDraftV1, error) {
	if err := marker.Validate(); err != nil ||
		marker.RunSnapshotID != ref.SnapshotID ||
		marker.TenantID != expected.TenantID ||
		marker.UserID != expected.UserID ||
		marker.TaskID != expected.TaskID ||
		batchID <= 0 || generatedAt.IsZero() ||
		len(orderedDeliveryIDs) == 0 {
		return types.BriefDraftV1{},
			canonicalBriefValidationError("brief preparation input is invalid")
	}
	seen := make(map[int64]struct{}, len(orderedDeliveryIDs))
	for _, deliveryID := range orderedDeliveryIDs {
		if deliveryID <= 0 {
			return types.BriefDraftV1{},
				canonicalBriefValidationError(
					"brief preparation delivery identity is invalid")
		}
		if _, exists := seen[deliveryID]; exists {
			return types.BriefDraftV1{},
				canonicalBriefValidationError(
					"brief preparation delivery identity is duplicated")
		}
		seen[deliveryID] = struct{}{}
	}

	tx, err := s.beginCanonicalBriefTxV1(ctx, expected, ref)
	if err != nil {
		return types.BriefDraftV1{}, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockCanonicalBriefRunV1(ctx, tx, ref.SnapshotID); err != nil {
		return types.BriefDraftV1{}, err
	}
	available, err := canonicalBriefStageCapabilityV1(ctx, tx)
	if err != nil {
		return types.BriefDraftV1{}, err
	}
	if !available {
		return types.BriefDraftV1{},
			canonicalBriefDatabaseError(
				"check canonical Brief stage capability",
				errors.New("canonical Brief stage capability is unavailable"))
	}
	outcomeRow, found, err := loadRunOutcomeForSnapshotV1(
		ctx, tx, ref.SnapshotID, true)
	if err != nil {
		return types.BriefDraftV1{}, err
	}
	if !found || outcomeRow.ID != marker.ID ||
		outcomeRow.marker() != marker {
		return types.BriefDraftV1{},
			canonicalBriefNotFoundError("run outcome marker is unavailable")
	}
	if existing, found, err := loadCanonicalBriefStageV1(
		ctx, tx, marker.ID); err != nil {
		return types.BriefDraftV1{}, err
	} else if found {
		if existing.status != "staged" ||
			existing.draft.PushBatchID != batchID ||
			existing.draft.GeneratedAt !=
				generatedAt.Round(0).UTC().Truncate(time.Microsecond) ||
			len(existing.draft.Insights) != len(orderedDeliveryIDs) {
			return types.BriefDraftV1{},
				canonicalBriefConflictError(
					"canonical Brief stage already differs")
		}
		for index, deliveryID := range orderedDeliveryIDs {
			if existing.draft.Insights[index].ID != deliveryID {
				return types.BriefDraftV1{},
					canonicalBriefConflictError(
						"canonical Brief stage order already differs")
			}
		}
		if err := commitCanonicalBriefTxV1(
			ctx, tx, "replay canonical Brief stage"); err != nil {
			return types.BriefDraftV1{}, err
		}
		return existing.draft, nil
	}
	var briefState string
	err = tx.QueryRow(ctx,
		`SELECT brief_state
		   FROM push_batches
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND schedule_id=$4 AND run_snapshot_id=$5
		  FOR UPDATE`,
		batchID, expected.TenantID, expected.UserID,
		expected.TaskID, ref.SnapshotID,
	).Scan(&briefState)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.BriefDraftV1{},
			canonicalBriefNotFoundError("compiled brief batch is unavailable")
	}
	if err != nil {
		return types.BriefDraftV1{},
			canonicalBriefDatabaseError("lock compiled brief batch", err)
	}
	if briefState != "open" {
		return types.BriefDraftV1{},
			canonicalBriefConflictError("compiled brief batch is already sealed")
	}

	evidenceByID, err := readBriefEvidenceV1(ctx, tx, batchID, ref.SnapshotID)
	if err != nil {
		return types.BriefDraftV1{}, err
	}
	if len(evidenceByID) != len(orderedDeliveryIDs) {
		return types.BriefDraftV1{},
			canonicalBriefConflictError(
				"brief preparation must cover the complete delivery set")
	}
	insights := make([]types.InsightV1, 0, len(orderedDeliveryIDs))
	for index, deliveryID := range orderedDeliveryIDs {
		stored, exists := evidenceByID[deliveryID]
		if !exists || !stored.complete {
			return types.BriefDraftV1{},
				canonicalBriefConflictError(
					"brief preparation evidence is incomplete")
		}
		insights = append(insights, types.InsightV1{
			ID: deliveryID, RankPosition: index + 1,
			Title: stored.title, BodyMD: stored.bodyMD,
			SourceTitle:  stored.sourceTitle,
			SourceURL:    stored.sourceURL,
			PublishedAt:  stored.publishedAt,
			DiscoveredAt: stored.discoveredAt,
		})
	}
	draft, err := (types.BriefDraftV1{
		SchemaVersion: types.BriefSchemaVersionV1,
		RunOutcomeID:  marker.ID,
		RunSnapshotID: marker.RunSnapshotID,
		PushBatchID:   batchID,
		TenantID:      marker.TenantID,
		UserID:        marker.UserID,
		TaskID:        marker.TaskID,
		GeneratedAt:   generatedAt,
		Insights:      insights,
	}).Canonical()
	if err != nil {
		return types.BriefDraftV1{}, canonicalBriefIntegrityError()
	}
	requestDigest, err := draft.RequestDigest()
	if err != nil {
		return types.BriefDraftV1{}, canonicalBriefIntegrityError()
	}
	payload, err := json.Marshal(draft)
	if err != nil {
		return types.BriefDraftV1{}, canonicalBriefIntegrityError()
	}
	tag, err := tx.Exec(ctx,
		`UPDATE push_batches SET brief_state='sealed'
		  WHERE id=$1 AND brief_state='open'`, batchID)
	if err != nil {
		return types.BriefDraftV1{},
			canonicalBriefDatabaseError("seal canonical Brief stage batch", err)
	}
	if tag.RowsAffected() != 1 {
		return types.BriefDraftV1{},
			canonicalBriefConflictError(
				"canonical Brief stage batch seal lost its CAS")
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO canonical_brief_stages (
		    run_outcome_id,tenant_id,user_id,task_id,run_snapshot_id,
		    push_batch_id,schema_version,request_digest,payload,
		    insight_count,generated_at
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		draft.RunOutcomeID, draft.TenantID, draft.UserID, draft.TaskID,
		draft.RunSnapshotID, draft.PushBatchID, draft.SchemaVersion,
		requestDigest, payload, len(draft.Insights), draft.GeneratedAt,
	)
	if err != nil {
		return types.BriefDraftV1{},
			canonicalBriefDatabaseError("stage canonical Brief", err)
	}
	if err := commitCanonicalBriefTxV1(
		ctx, tx, "commit canonical Brief stage"); err != nil {
		return types.BriefDraftV1{}, err
	}
	return draft, nil
}

// SealEmptyBriefBatchV1 closes the canonical namespace for an observation run
// whose complete delivery plan is empty. No stage or Brief is created, but the
// sealed batch makes the quiet result immutable and prevents a later writer
// from attaching content to the same exact run.
func (s *Store) SealEmptyBriefBatchV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	marker types.RunOutcomeMarkerV1,
	batchID int64,
) error {
	if err := expected.Validate(); err != nil ||
		ref.Validate() != nil || marker.Validate() != nil ||
		batchID <= 0 ||
		marker.RunSnapshotID != ref.SnapshotID ||
		marker.TenantID != expected.TenantID ||
		marker.UserID != expected.UserID ||
		marker.TaskID != expected.TaskID {
		return canonicalBriefValidationError(
			"empty brief batch input is invalid")
	}
	tx, err := s.beginCanonicalBriefTxV1(ctx, expected, ref)
	if err != nil {
		return err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockCanonicalBriefRunV1(ctx, tx, ref.SnapshotID); err != nil {
		return err
	}
	available, err := canonicalBriefStageCapabilityV1(ctx, tx)
	if err != nil {
		return err
	}
	if !available {
		return canonicalBriefDatabaseError(
			"check empty canonical Brief capability",
			errors.New("canonical Brief stage capability is unavailable"))
	}
	outcomeRow, found, err := loadRunOutcomeForSnapshotV1(
		ctx, tx, ref.SnapshotID, true)
	if err != nil {
		return err
	}
	if !found || outcomeRow.ID != marker.ID ||
		outcomeRow.marker() != marker || outcomeRow.Status != "pending" {
		return canonicalBriefNotFoundError(
			"pending run outcome marker is unavailable")
	}
	if _, found, err := loadCanonicalBriefStageV1(
		ctx, tx, marker.ID); err != nil {
		return err
	} else if found {
		return canonicalBriefConflictError(
			"empty canonical Brief batch already has a stage")
	}
	var briefState, batchStatus, authority string
	err = tx.QueryRow(ctx,
		`SELECT brief_state,status,delivery_authority
		   FROM push_batches
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND schedule_id=$4 AND run_snapshot_id=$5
		  FOR UPDATE`,
		batchID, expected.TenantID, expected.UserID,
		expected.TaskID, ref.SnapshotID,
	).Scan(&briefState, &batchStatus, &authority)
	if errors.Is(err, pgx.ErrNoRows) {
		return canonicalBriefNotFoundError(
			"empty canonical Brief batch is unavailable")
	}
	if err != nil {
		return canonicalBriefDatabaseError(
			"lock empty canonical Brief batch", err)
	}
	if batchStatus != string(types.BatchStatusPending) ||
		authority != string(types.PushBatchDeliveryAuthorityEffect) {
		return canonicalBriefConflictError(
			"empty canonical Brief batch authority differs")
	}
	var deliveryCount int
	if err := tx.QueryRow(ctx, `
		SELECT count(*)
		  FROM read_canonical_brief_delivery_evidence_v1($1,$2)`,
		batchID, ref.SnapshotID,
	).Scan(&deliveryCount); err != nil {
		return canonicalBriefDatabaseError(
			"inspect empty canonical Brief batch", err)
	}
	if deliveryCount != 0 {
		return canonicalBriefConflictError(
			"empty canonical Brief batch contains delivery evidence")
	}
	switch briefState {
	case "sealed":
		// Exact response-loss replay.
	case "open":
		tag, err := tx.Exec(ctx,
			`UPDATE push_batches SET brief_state='sealed'
			  WHERE id=$1 AND brief_state='open'`, batchID)
		if err != nil {
			return canonicalBriefDatabaseError(
				"seal empty canonical Brief batch", err)
		}
		if tag.RowsAffected() != 1 {
			return canonicalBriefConflictError(
				"empty canonical Brief batch seal lost its CAS")
		}
	default:
		return canonicalBriefIntegrityError()
	}
	return commitCanonicalBriefTxV1(
		ctx, tx, "commit empty canonical Brief batch")
}

// FreezeBriefV1 persists one immutable, channel-neutral whole-Brief snapshot.
// The exact finalized content outcome, compiled batch, and complete set of
// delivery IDs are verified before the payload is admitted.
func (s *Store) FreezeBriefV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	draft types.BriefDraftV1,
) (types.BriefV1, error) {
	canonical, err := draft.Canonical()
	if err != nil ||
		canonical.RunSnapshotID != ref.SnapshotID ||
		canonical.TenantID != expected.TenantID ||
		canonical.UserID != expected.UserID ||
		canonical.TaskID != expected.TaskID {
		return types.BriefV1{},
			canonicalBriefValidationError("brief draft is invalid")
	}
	requestDigest, err := canonical.RequestDigest()
	if err != nil {
		return types.BriefV1{},
			canonicalBriefValidationError("brief request cannot be sealed")
	}
	tx, err := s.beginCanonicalBriefTxV1(ctx, expected, ref)
	if err != nil {
		return types.BriefV1{}, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	brief, err := freezeBriefTxV1(ctx, tx, expected, ref, canonical, requestDigest)
	if err != nil {
		return types.BriefV1{}, err
	}
	if err := commitCanonicalBriefTxV1(ctx, tx, "commit canonical brief"); err != nil {
		return types.BriefV1{}, err
	}
	return brief, nil
}

func freezeBriefTxV1(
	ctx context.Context,
	tx pgx.Tx,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	canonical types.BriefDraftV1,
	requestDigest string,
) (types.BriefV1, error) {
	if err := lockCanonicalBriefRunV1(ctx, tx, ref.SnapshotID); err != nil {
		return types.BriefV1{}, err
	}

	outcomeRow, found, err := loadRunOutcomeForSnapshotV1(
		ctx, tx, ref.SnapshotID, false)
	if err != nil {
		return types.BriefV1{}, err
	}
	if !found || outcomeRow.ID != canonical.RunOutcomeID {
		return types.BriefV1{},
			canonicalBriefNotFoundError("content run outcome is unavailable")
	}
	outcome, err := outcomeRow.finalized()
	if err != nil {
		return types.BriefV1{}, err
	}
	if outcome.Result != types.RunResultContent {
		return types.BriefV1{},
			canonicalBriefConflictError("only a content outcome may own a brief")
	}

	if existing, found, err := loadBriefForOutcomeV1(
		ctx, tx, canonical.RunOutcomeID); err != nil {
		return types.BriefV1{}, err
	} else if found {
		if existing.requestDigest != requestDigest {
			return types.BriefV1{},
				canonicalBriefConflictError("brief already frozen differently")
		}
		return existing.brief, nil
	}

	var briefState string
	err = tx.QueryRow(ctx,
		`SELECT brief_state
		   FROM push_batches
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND schedule_id=$4 AND run_snapshot_id=$5
		  FOR UPDATE`,
		canonical.PushBatchID, expected.TenantID, expected.UserID,
		expected.TaskID, ref.SnapshotID,
	).Scan(&briefState)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.BriefV1{},
			canonicalBriefNotFoundError("compiled brief batch is unavailable")
	}
	if err != nil {
		return types.BriefV1{},
			canonicalBriefDatabaseError("lock compiled brief batch", err)
	}
	if briefState != "open" {
		return types.BriefV1{},
			canonicalBriefConflictError("compiled brief batch is already sealed")
	}
	if err := verifyBriefDeliveriesV1(ctx, tx, canonical); err != nil {
		return types.BriefV1{}, err
	}
	tag, err := tx.Exec(ctx,
		`UPDATE push_batches SET brief_state='sealed'
		  WHERE id=$1 AND brief_state='open'`, canonical.PushBatchID)
	if err != nil {
		return types.BriefV1{},
			canonicalBriefDatabaseError("seal compiled brief batch", err)
	}
	if tag.RowsAffected() != 1 {
		return types.BriefV1{},
			canonicalBriefConflictError("compiled brief batch seal lost its CAS")
	}

	var briefID int64
	if err := tx.QueryRow(
		ctx, `SELECT nextval('brief_snapshots_id_seq')`).Scan(&briefID); err != nil {
		return types.BriefV1{},
			canonicalBriefDatabaseError("allocate brief id", err)
	}
	brief, err := canonical.Seal(briefID)
	if err != nil {
		return types.BriefV1{}, canonicalBriefIntegrityError()
	}
	payload, err := json.Marshal(brief)
	if err != nil {
		return types.BriefV1{}, canonicalBriefIntegrityError()
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO brief_snapshots (
		    id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id,
		    push_batch_id,schema_version,request_digest,payload_digest,
		    payload,insight_count,generated_at
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		brief.ID, brief.TenantID, brief.UserID, brief.TaskID,
		brief.RunOutcomeID, brief.RunSnapshotID, brief.PushBatchID,
		brief.SchemaVersion, requestDigest, brief.Digest, payload,
		len(brief.Insights), brief.GeneratedAt,
	)
	if err != nil {
		return types.BriefV1{},
			canonicalBriefDatabaseError("freeze canonical brief", err)
	}
	return brief, nil
}

// LoadBriefV1 reads the exact immutable Brief behind the same tenant/user/run
// boundary. P1-D may build a separate read model; P1-A keeps this dark.
func (s *Store) LoadBriefV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
) (types.BriefV1, bool, error) {
	tx, err := s.beginCanonicalBriefTxV1(ctx, expected, ref)
	if err != nil {
		return types.BriefV1{}, false, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	stored, found, err := loadBriefForSnapshotV1(ctx, tx, ref.SnapshotID)
	if err != nil {
		return types.BriefV1{}, false, err
	}
	if err := commitCanonicalBriefTxV1(ctx, tx, "load canonical brief"); err != nil {
		return types.BriefV1{}, false, err
	}
	if !found {
		return types.BriefV1{}, false, nil
	}
	return stored.brief, true, nil
}

func (s *Store) beginCanonicalBriefTxV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
) (pgx.Tx, error) {
	if _, err := validateTaskRunSnapshotReferenceForExpectedV1(
		ref, expected); err != nil || ref.SnapshotID <= 0 {
		return nil, canonicalBriefValidationError(
			"canonical brief run reference is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, canonicalBriefDatabaseError(
			"begin canonical brief transaction", err)
	}
	if err := lockPushEffectSchemaWriter(ctx, tx); err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, canonicalBriefDatabaseError(
			"lock canonical brief schema admission", err)
	}
	if _, err := tx.Exec(
		ctx, `SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, canonicalBriefDatabaseError(
			"pin canonical brief search path", err)
	}
	exists, err := lockTenantAdmissionRoot(ctx, tx, expected.TenantID)
	if err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, canonicalBriefDatabaseError(
			"lock canonical brief tenant admission", err)
	}
	if !exists {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, canonicalBriefNotFoundError("canonical brief tenant is unavailable")
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		strconv.FormatInt(expected.TenantID, 10)); err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, canonicalBriefDatabaseError(
			"set canonical brief tenant", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.user_id',$1,true)`,
		strconv.FormatInt(expected.UserID, 10)); err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, canonicalBriefDatabaseError(
			"set canonical brief user", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_brief_writer`); err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, canonicalBriefDatabaseError(
			"enter canonical brief writer role", err)
	}
	var exact bool
	err = tx.QueryRow(ctx,
		`SELECT true
		   FROM read_canonical_brief_run_identity_v1($1)
		  WHERE task_id=$2
		    AND temporal_workflow_id=$3 AND temporal_run_id=$4
		    AND reference_schema_version=$5
		    AND reference_digest=$6 AND payload_digest=$7`,
		ref.SnapshotID, expected.TaskID,
		expected.TemporalWorkflowID, expected.TemporalRunID,
		ref.SchemaVersion, ref.ReferenceDigest, ref.PayloadDigest,
	).Scan(&exact)
	if errors.Is(err, pgx.ErrNoRows) || !exact {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, canonicalBriefNotFoundError(
			"canonical brief run snapshot is unavailable")
	}
	if err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, canonicalBriefDatabaseError(
			"lock canonical brief run snapshot", err)
	}
	return tx, nil
}

func lockCanonicalBriefRunV1(
	ctx context.Context, tx pgx.Tx, snapshotID int64,
) error {
	key := canonicalBriefLockNamespaceV1 + strconv.FormatInt(snapshotID, 10)
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1,$2))`,
		key, canonicalBriefLockSeedV1); err != nil {
		return canonicalBriefDatabaseError("lock canonical brief run", err)
	}
	return nil
}

func loadRunOutcomeForSnapshotV1(
	ctx context.Context, tx pgx.Tx, snapshotID int64, forUpdate bool,
) (runOutcomeRowV1, bool, error) {
	query := `SELECT ` + runOutcomeColumnsV1 + `
	    FROM task_run_outcomes WHERE run_snapshot_id=$1`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	stored, err := scanRunOutcomeRowV1(tx.QueryRow(ctx, query, snapshotID))
	if errors.Is(err, pgx.ErrNoRows) {
		return runOutcomeRowV1{}, false, nil
	}
	if err != nil {
		return runOutcomeRowV1{}, false,
			canonicalBriefDatabaseError("load run outcome", err)
	}
	return stored, true, nil
}

type canonicalBriefStageV1 struct {
	draft           types.BriefDraftV1
	requestDigest   string
	status          string
	briefSnapshotID sql.NullInt64
	resolvedAt      sql.NullTime
}

func canonicalBriefStageCapabilityV1(
	ctx context.Context,
	tx pgx.Tx,
) (bool, error) {
	var available bool
	if err := tx.QueryRow(ctx,
		`SELECT to_regclass('public.canonical_brief_stages') IS NOT NULL`,
	).Scan(&available); err != nil {
		return false, canonicalBriefDatabaseError(
			"check canonical Brief stage capability", err)
	}
	return available, nil
}

func loadCanonicalBriefStageV1(
	ctx context.Context,
	tx pgx.Tx,
	runOutcomeID int64,
) (canonicalBriefStageV1, bool, error) {
	var (
		stage                                 canonicalBriefStageV1
		tenantID, userID, snapshotID, batchID int64
		taskID, schemaVersion                 string
		payload                               []byte
		insightCount                          int
		generatedAt                           time.Time
	)
	err := tx.QueryRow(ctx,
		`SELECT tenant_id,user_id,task_id,run_snapshot_id,push_batch_id,
		        schema_version,request_digest,payload,insight_count,
		        generated_at,status,brief_snapshot_id,resolved_at
		   FROM canonical_brief_stages
		  WHERE run_outcome_id=$1
		  FOR UPDATE`,
		runOutcomeID,
	).Scan(
		&tenantID, &userID, &taskID, &snapshotID, &batchID,
		&schemaVersion, &stage.requestDigest, &payload, &insightCount,
		&generatedAt, &stage.status, &stage.briefSnapshotID,
		&stage.resolvedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return canonicalBriefStageV1{}, false, nil
	}
	if err != nil {
		return canonicalBriefStageV1{}, false,
			canonicalBriefDatabaseError("load canonical Brief stage", err)
	}
	if err := json.Unmarshal(payload, &stage.draft); err != nil {
		return canonicalBriefStageV1{}, false, canonicalBriefIntegrityError()
	}
	canonicalDraft, canonicalErr := stage.draft.Canonical()
	if canonicalErr != nil ||
		stage.draft.RunOutcomeID != runOutcomeID ||
		stage.draft.TenantID != tenantID ||
		stage.draft.UserID != userID ||
		stage.draft.TaskID != taskID ||
		stage.draft.RunSnapshotID != snapshotID ||
		stage.draft.PushBatchID != batchID ||
		stage.draft.SchemaVersion != schemaVersion ||
		len(stage.draft.Insights) != insightCount ||
		!stage.draft.GeneratedAt.Equal(generatedAt) {
		return canonicalBriefStageV1{}, false, canonicalBriefIntegrityError()
	}
	canonicalPayload, err := json.Marshal(canonicalDraft)
	if err != nil || !bytes.Equal(payload, canonicalPayload) {
		return canonicalBriefStageV1{}, false, canonicalBriefIntegrityError()
	}
	stage.draft = canonicalDraft
	requestDigest, err := stage.draft.RequestDigest()
	if err != nil || requestDigest != stage.requestDigest {
		return canonicalBriefStageV1{}, false, canonicalBriefIntegrityError()
	}
	switch stage.status {
	case "staged":
		if stage.briefSnapshotID.Valid || stage.resolvedAt.Valid {
			return canonicalBriefStageV1{}, false,
				canonicalBriefIntegrityError()
		}
	case "promoted":
		if !stage.briefSnapshotID.Valid || !stage.resolvedAt.Valid {
			return canonicalBriefStageV1{}, false,
				canonicalBriefIntegrityError()
		}
	case "aborted":
		if stage.briefSnapshotID.Valid || !stage.resolvedAt.Valid {
			return canonicalBriefStageV1{}, false,
				canonicalBriefIntegrityError()
		}
	default:
		return canonicalBriefStageV1{}, false, canonicalBriefIntegrityError()
	}
	return stage, true, nil
}

type storedBriefV1 struct {
	brief         types.BriefV1
	requestDigest string
}

func loadBriefForOutcomeV1(
	ctx context.Context, tx pgx.Tx, outcomeID int64,
) (storedBriefV1, bool, error) {
	return loadBriefV1(ctx, tx,
		`FROM brief_snapshots WHERE run_outcome_id=$1`, outcomeID)
}

func loadBriefForSnapshotV1(
	ctx context.Context, tx pgx.Tx, snapshotID int64,
) (storedBriefV1, bool, error) {
	return loadBriefV1(ctx, tx,
		`FROM brief_snapshots WHERE run_snapshot_id=$1`, snapshotID)
}

func loadBriefV1(
	ctx context.Context, tx pgx.Tx, predicate string, argument int64,
) (storedBriefV1, bool, error) {
	var (
		id, tenantID, userID, outcomeID, snapshotID, batchID int64
		taskID, schemaVersion, requestDigest, payloadDigest  string
		payload                                              []byte
		insightCount                                         int
		generatedAt                                          sql.NullTime
	)
	err := tx.QueryRow(ctx,
		`SELECT id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id,
		        push_batch_id,schema_version,request_digest,payload_digest,
		        payload,insight_count,generated_at `+predicate,
		argument,
	).Scan(
		&id, &tenantID, &userID, &taskID, &outcomeID, &snapshotID,
		&batchID, &schemaVersion, &requestDigest, &payloadDigest,
		&payload, &insightCount, &generatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return storedBriefV1{}, false, nil
	}
	if err != nil {
		return storedBriefV1{}, false,
			canonicalBriefDatabaseError("load canonical brief", err)
	}
	var brief types.BriefV1
	if err := json.Unmarshal(payload, &brief); err != nil ||
		brief.Validate() != nil || brief.ID != id ||
		brief.TenantID != tenantID || brief.UserID != userID ||
		brief.TaskID != taskID || brief.RunOutcomeID != outcomeID ||
		brief.RunSnapshotID != snapshotID || brief.PushBatchID != batchID ||
		brief.SchemaVersion != schemaVersion ||
		brief.Digest != payloadDigest || len(brief.Insights) != insightCount ||
		!generatedAt.Valid || !brief.GeneratedAt.Equal(generatedAt.Time) {
		return storedBriefV1{}, false, canonicalBriefIntegrityError()
	}
	canonicalPayload, err := json.Marshal(brief)
	if err != nil || !bytes.Equal(canonicalPayload, payload) {
		return storedBriefV1{}, false, canonicalBriefIntegrityError()
	}
	computedRequest, err := brief.BriefDraftV1.RequestDigest()
	if err != nil || computedRequest != requestDigest {
		return storedBriefV1{}, false, canonicalBriefIntegrityError()
	}
	return storedBriefV1{
		brief: brief, requestDigest: requestDigest,
	}, true, nil
}

func verifyBriefDeliveriesV1(
	ctx context.Context, tx pgx.Tx, draft types.BriefDraftV1,
) error {
	deliveries, err := readBriefEvidenceV1(
		ctx, tx, draft.PushBatchID, draft.RunSnapshotID)
	if err != nil {
		return err
	}
	if len(deliveries) != len(draft.Insights) {
		return canonicalBriefConflictError(
			"brief must freeze the complete batch delivery set")
	}
	for _, insight := range draft.Insights {
		stored, exists := deliveries[insight.ID]
		if !exists {
			return canonicalBriefConflictError(
				"brief insight does not belong to the exact batch")
		}
		if !stored.complete ||
			insight.BodyMD != stored.bodyMD ||
			insight.DiscoveredAt != stored.discoveredAt ||
			insight.Title != stored.title ||
			insight.SourceURL != stored.sourceURL ||
			insight.SourceTitle != stored.sourceTitle ||
			!canonicalBriefOptionalTimeEqual(
				insight.PublishedAt, stored.publishedAt) {
			return canonicalBriefConflictError(
				"brief insight does not match durable source evidence")
		}
	}
	return nil
}

type briefEvidenceV1 struct {
	complete     bool
	bodyMD       string
	discoveredAt time.Time
	title        string
	sourceURL    string
	publishedAt  *time.Time
	sourceTitle  string
}

func readBriefEvidenceV1(
	ctx context.Context,
	tx pgx.Tx,
	batchID int64,
	runSnapshotID int64,
) (map[int64]briefEvidenceV1, error) {
	rows, err := tx.Query(ctx,
		`SELECT delivery_id,evidence_complete,body_md,discovered_at,content_title,
		        canonical_url,published_at,source_title
		   FROM read_canonical_brief_delivery_evidence_v1($1,$2)`,
		batchID, runSnapshotID,
	)
	if err != nil {
		return nil,
			canonicalBriefDatabaseError("read brief delivery evidence", err)
	}
	defer rows.Close()
	deliveries := make(map[int64]briefEvidenceV1)
	for rows.Next() {
		var id int64
		var complete bool
		var bodyMD string
		var discoveredAt time.Time
		var title, sourceURL, sourceTitle string
		var publishedAt sql.NullTime
		if err := rows.Scan(
			&id, &complete, &bodyMD, &discoveredAt, &title, &sourceURL,
			&publishedAt, &sourceTitle,
		); err != nil {
			return nil,
				canonicalBriefDatabaseError("scan brief delivery", err)
		}
		var canonicalPublishedAt *time.Time
		if publishedAt.Valid {
			value := publishedAt.Time.Round(0).UTC().Truncate(time.Microsecond)
			canonicalPublishedAt = &value
		}
		deliveries[id] = briefEvidenceV1{
			complete:     complete,
			bodyMD:       bodyMD,
			discoveredAt: discoveredAt.Round(0).UTC().Truncate(time.Microsecond),
			title:        title,
			sourceURL:    sourceURL,
			publishedAt:  canonicalPublishedAt,
			sourceTitle:  sourceTitle,
		}
	}
	if err := rows.Err(); err != nil {
		return nil,
			canonicalBriefDatabaseError("read brief delivery evidence", err)
	}
	return deliveries, nil
}

func canonicalBriefOptionalTimeEqual(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

func commitCanonicalBriefTxV1(
	ctx context.Context, tx pgx.Tx, action string,
) error {
	if err := tx.Commit(ctx); err != nil {
		return canonicalBriefDatabaseError(action, err)
	}
	return nil
}

func canonicalBriefValidationError(message string) error {
	return types.NewAppError(types.CodeValidation, message, nil)
}

func canonicalBriefNotFoundError(message string) error {
	return types.NewAppError(types.CodeNotFound, message, nil)
}

func canonicalBriefConflictError(message string) error {
	return types.NewAppError(types.CodeConflict, message, nil)
}

func canonicalBriefIntegrityError() error {
	return types.NewAppError(
		types.CodeInternal, "canonical outcome/brief integrity check failed", nil)
}

func canonicalBriefDatabaseError(action string, cause error) error {
	return taskRunDatabaseError(fmt.Sprintf("canonical brief: %s", action), cause)
}
