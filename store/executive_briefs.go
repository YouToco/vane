package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

type ExecutiveSynthesisStatusV1 string

const (
	ExecutiveSynthesisPrepared  ExecutiveSynthesisStatusV1 = "prepared"
	ExecutiveSynthesisSpending  ExecutiveSynthesisStatusV1 = "spending"
	ExecutiveSynthesisFinalized ExecutiveSynthesisStatusV1 = "finalized"
	ExecutiveSynthesisAmbiguous ExecutiveSynthesisStatusV1 = "ambiguous"
	ExecutiveSynthesisFallback  ExecutiveSynthesisStatusV1 = "fallback"
)

type ExecutiveSynthesisPrepareV1 struct {
	Marker         types.RunOutcomeMarkerV1
	PushBatchID    int64
	ProfileEpoch   int64
	ProfileVersion int64
	ProfileDigest  string
	InputDigest    string
	RequestDigest  string
}

type ExecutiveSynthesisReceiptV1 struct {
	ExecutiveSynthesisPrepareV1
	Status            ExecutiveSynthesisStatusV1
	GenerationMode    types.ExecutiveGenerationModeV1
	Processing        types.RunCompletenessV1
	Content           *types.ExecutiveBriefContentV1
	SpendingStartedAt *time.Time
	FinalizedAt       *time.Time
}

func (p ExecutiveSynthesisPrepareV1) validate(
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
) error {
	if p.Marker.Validate() != nil ||
		p.Marker.TenantID != expected.TenantID ||
		p.Marker.UserID != expected.UserID ||
		p.Marker.TaskID != expected.TaskID ||
		p.Marker.RunSnapshotID != ref.SnapshotID ||
		p.PushBatchID <= 0 || p.ProfileEpoch < 0 ||
		p.ProfileVersion < 0 ||
		!validStoreDigestV1(p.ProfileDigest) ||
		!validStoreDigestV1(p.InputDigest) ||
		!validStoreDigestV1(p.RequestDigest) {
		return canonicalBriefValidationError(
			"executive synthesis preparation is invalid")
	}
	return nil
}

func validStoreDigestV1(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') &&
			(char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

const executiveSynthesisColumnsV1 = `
	run_outcome_id,tenant_id,user_id,task_id,run_snapshot_id,push_batch_id,
	schema_version,profile_epoch,profile_version,profile_digest,input_digest,
	request_digest,status,generation_mode,processing,content_payload,
	spending_started_at,finalized_at`

func scanExecutiveSynthesisReceiptV1(
	row pgx.Row,
) (ExecutiveSynthesisReceiptV1, error) {
	var (
		receipt                      ExecutiveSynthesisReceiptV1
		schemaVersion                string
		generation, processing       sql.NullString
		payload                      []byte
		spendingStarted, finalizedAt sql.NullTime
	)
	err := row.Scan(
		&receipt.Marker.ID, &receipt.Marker.TenantID,
		&receipt.Marker.UserID, &receipt.Marker.TaskID,
		&receipt.Marker.RunSnapshotID, &receipt.PushBatchID,
		&schemaVersion, &receipt.ProfileEpoch, &receipt.ProfileVersion,
		&receipt.ProfileDigest, &receipt.InputDigest,
		&receipt.RequestDigest, &receipt.Status, &generation,
		&processing, &payload, &spendingStarted, &finalizedAt,
	)
	if err != nil {
		return ExecutiveSynthesisReceiptV1{}, err
	}
	receipt.Marker.SchemaVersion = types.RunOutcomeSchemaVersionV1
	if schemaVersion != types.ExecutiveBriefSchemaVersionV1 ||
		receipt.Marker.Validate() != nil ||
		!validStoreDigestV1(receipt.ProfileDigest) ||
		!validStoreDigestV1(receipt.InputDigest) ||
		!validStoreDigestV1(receipt.RequestDigest) {
		return ExecutiveSynthesisReceiptV1{}, canonicalBriefIntegrityError()
	}
	if generation.Valid {
		receipt.GenerationMode =
			types.ExecutiveGenerationModeV1(generation.String)
	}
	if processing.Valid {
		receipt.Processing = types.RunCompletenessV1(processing.String)
	}
	if len(payload) > 0 {
		var content types.ExecutiveBriefContentV1
		if err := json.Unmarshal(payload, &content); err != nil {
			return ExecutiveSynthesisReceiptV1{},
				canonicalBriefIntegrityError()
		}
		canonical, err := json.Marshal(content)
		if err != nil || !bytes.Equal(canonical, payload) ||
			content.ValidateIssue() != nil {
			return ExecutiveSynthesisReceiptV1{},
				canonicalBriefIntegrityError()
		}
		receipt.Content = &content
	}
	if spendingStarted.Valid {
		value := spendingStarted.Time.Round(0).UTC().Truncate(time.Microsecond)
		receipt.SpendingStartedAt = &value
	}
	if finalizedAt.Valid {
		value := finalizedAt.Time.Round(0).UTC().Truncate(time.Microsecond)
		receipt.FinalizedAt = &value
	}
	if err := validateExecutiveSynthesisReceiptShapeV1(receipt); err != nil {
		return ExecutiveSynthesisReceiptV1{}, err
	}
	return receipt, nil
}

func validateExecutiveSynthesisReceiptShapeV1(
	receipt ExecutiveSynthesisReceiptV1,
) error {
	switch receipt.Status {
	case ExecutiveSynthesisPrepared:
		if receipt.GenerationMode != "" || receipt.Processing != "" ||
			receipt.Content != nil || receipt.SpendingStartedAt != nil ||
			receipt.FinalizedAt != nil {
			return canonicalBriefIntegrityError()
		}
	case ExecutiveSynthesisSpending:
		if receipt.GenerationMode != "" || receipt.Processing != "" ||
			receipt.Content != nil || receipt.SpendingStartedAt == nil ||
			receipt.FinalizedAt != nil {
			return canonicalBriefIntegrityError()
		}
	case ExecutiveSynthesisFinalized:
		if receipt.GenerationMode != types.ExecutiveGenerationModel ||
			receipt.Processing != types.RunCompletenessComplete ||
			receipt.Content == nil || receipt.SpendingStartedAt == nil ||
			receipt.FinalizedAt == nil {
			return canonicalBriefIntegrityError()
		}
	case ExecutiveSynthesisAmbiguous:
		if receipt.GenerationMode != "" || receipt.Processing != "" ||
			receipt.Content != nil || receipt.SpendingStartedAt == nil ||
			receipt.FinalizedAt == nil {
			return canonicalBriefIntegrityError()
		}
	case ExecutiveSynthesisFallback:
		if receipt.GenerationMode != types.ExecutiveGenerationFallback ||
			receipt.Processing != types.RunCompletenessPartial ||
			receipt.Content == nil || receipt.FinalizedAt == nil {
			return canonicalBriefIntegrityError()
		}
	default:
		return canonicalBriefIntegrityError()
	}
	return nil
}

func (s *Store) PrepareExecutiveSynthesisV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	prepare ExecutiveSynthesisPrepareV1,
) (ExecutiveSynthesisReceiptV1, error) {
	return s.prepareExecutiveSynthesisV1(
		ctx, expected, ref, prepare, "vane_brief_synthesis_writer")
}

func (s *Store) PrepareExecutiveSynthesisRecoveryV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	prepare ExecutiveSynthesisPrepareV1,
) (ExecutiveSynthesisReceiptV1, error) {
	return s.prepareExecutiveSynthesisV1(
		ctx, expected, ref, prepare, "vane_brief_synthesis_recovery")
}

func (s *Store) prepareExecutiveSynthesisV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	prepare ExecutiveSynthesisPrepareV1,
	role string,
) (ExecutiveSynthesisReceiptV1, error) {
	if err := prepare.validate(expected, ref); err != nil {
		return ExecutiveSynthesisReceiptV1{}, err
	}
	tx, err := s.beginCanonicalBriefTxV1(ctx, expected, ref)
	if err != nil {
		return ExecutiveSynthesisReceiptV1{}, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockCanonicalBriefRunV1(ctx, tx, ref.SnapshotID); err != nil {
		return ExecutiveSynthesisReceiptV1{}, err
	}
	stage, found, err := loadCanonicalBriefStageV1(
		ctx, tx, prepare.Marker.ID)
	if err != nil {
		return ExecutiveSynthesisReceiptV1{}, err
	}
	if !found ||
		(stage.status != "staged" && stage.status != "promoted") ||
		stage.draft.PushBatchID != prepare.PushBatchID ||
		stage.draft.RunSnapshotID != ref.SnapshotID {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefConflictError(
				"executive synthesis requires the exact staged Brief")
	}
	if _, err := tx.Exec(ctx, `RESET ROLE`); err != nil {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefDatabaseError(
				"leave canonical Brief writer role", err)
	}
	if role == "vane_brief_synthesis_recovery" {
		if err := lockExecutiveRecoveryMembershipV1(
			ctx, tx, expected); err != nil {
			return ExecutiveSynthesisReceiptV1{}, err
		}
	}
	if _, err := tx.Exec(
		ctx, `SET LOCAL ROLE `+role); err != nil {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefDatabaseError(
				"enter executive synthesis writer role", err)
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO executive_brief_synthesis_receipts (
		    run_outcome_id,tenant_id,user_id,task_id,run_snapshot_id,
		    push_batch_id,schema_version,profile_epoch,profile_version,
		    profile_digest,input_digest,request_digest
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		 ON CONFLICT (run_outcome_id) DO NOTHING`,
		prepare.Marker.ID, expected.TenantID, expected.UserID,
		expected.TaskID, ref.SnapshotID, prepare.PushBatchID,
		types.ExecutiveBriefSchemaVersionV1,
		prepare.ProfileEpoch, prepare.ProfileVersion,
		prepare.ProfileDigest, prepare.InputDigest, prepare.RequestDigest,
	)
	if err != nil {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefDatabaseError(
				"prepare executive synthesis receipt", err)
	}
	receipt, err := scanExecutiveSynthesisReceiptV1(tx.QueryRow(ctx,
		`SELECT `+executiveSynthesisColumnsV1+`
		   FROM executive_brief_synthesis_receipts
		  WHERE run_outcome_id=$1
		  FOR UPDATE`,
		prepare.Marker.ID,
	))
	if err != nil {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefDatabaseError(
				"load executive synthesis receipt", err)
	}
	if receipt.ExecutiveSynthesisPrepareV1 != prepare {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefConflictError(
				"executive synthesis receipt differs")
	}
	if err := tx.Commit(ctx); err != nil {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefDatabaseError(
				"commit executive synthesis preparation", err)
	}
	return receipt, nil
}

func validateExecutiveSynthesisMarkerV1(
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	marker types.RunOutcomeMarkerV1,
) error {
	if marker.Validate() != nil ||
		marker.TenantID != expected.TenantID ||
		marker.UserID != expected.UserID ||
		marker.TaskID != expected.TaskID ||
		marker.RunSnapshotID != ref.SnapshotID {
		return canonicalBriefValidationError(
			"executive synthesis marker is invalid")
	}
	return nil
}

func validateExecutiveContentReferencesV1(
	content types.ExecutiveBriefContentV1,
	draft types.BriefDraftV1,
) error {
	claimsByInsight := make(map[int64]int, len(draft.Insights))
	for _, insight := range draft.Insights {
		if insight.Structured == nil {
			continue
		}
		claimsByInsight[insight.ID] = len(insight.Structured.Claims)
	}
	validateRefs := func(refs []types.ExecutiveEvidenceRefV1) bool {
		for _, ref := range refs {
			if ref.BriefID != 0 {
				return false
			}
			claimCount, ok := claimsByInsight[ref.InsightID]
			if !ok {
				return false
			}
			for _, claimIndex := range ref.ClaimIndexes {
				if claimIndex < 0 || claimIndex >= claimCount {
					return false
				}
			}
		}
		return true
	}
	for _, signal := range content.Signals {
		if !validateRefs(signal.EvidenceRefs) {
			return canonicalBriefValidationError(
				"executive synthesis references unavailable evidence")
		}
	}
	for _, step := range content.NextSteps {
		if !validateRefs(step.EvidenceRefs) {
			return canonicalBriefValidationError(
				"executive synthesis references unavailable evidence")
		}
	}
	return nil
}

func (s *Store) ClaimExecutiveSynthesisSpendV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	marker types.RunOutcomeMarkerV1,
) (ExecutiveSynthesisReceiptV1, bool, error) {
	if err := validateExecutiveSynthesisMarkerV1(
		expected, ref, marker); err != nil {
		return ExecutiveSynthesisReceiptV1{}, false, err
	}
	tx, err := s.beginExecutiveSynthesisTxV1(ctx, expected, ref, false)
	if err != nil {
		return ExecutiveSynthesisReceiptV1{}, false, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockCanonicalBriefRunV1(ctx, tx, ref.SnapshotID); err != nil {
		return ExecutiveSynthesisReceiptV1{}, false, err
	}
	if _, err := tx.Exec(
		ctx, `SET LOCAL ROLE vane_brief_synthesis_writer`); err != nil {
		return ExecutiveSynthesisReceiptV1{}, false,
			canonicalBriefDatabaseError(
				"enter executive synthesis writer role", err)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE executive_brief_synthesis_receipts
		    SET status='spending',spending_started_at=clock_timestamp()
		  WHERE run_outcome_id=$1 AND tenant_id=$2 AND user_id=$3
		    AND task_id=$4 AND run_snapshot_id=$5 AND status='prepared'`,
		marker.ID, expected.TenantID, expected.UserID,
		expected.TaskID, ref.SnapshotID,
	)
	if err != nil {
		return ExecutiveSynthesisReceiptV1{}, false,
			canonicalBriefDatabaseError(
				"claim executive synthesis spend", err)
	}
	receipt, err := loadExecutiveSynthesisReceiptTxV1(
		ctx, tx, marker.ID, true)
	if err != nil {
		return ExecutiveSynthesisReceiptV1{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ExecutiveSynthesisReceiptV1{}, false,
			canonicalBriefDatabaseError(
				"commit executive synthesis spend claim", err)
	}
	return receipt, tag.RowsAffected() == 1, nil
}

func (s *Store) FinalizeExecutiveSynthesisV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	marker types.RunOutcomeMarkerV1,
	content types.ExecutiveBriefContentV1,
) (ExecutiveSynthesisReceiptV1, error) {
	if err := validateExecutiveSynthesisMarkerV1(
		expected, ref, marker); err != nil {
		return ExecutiveSynthesisReceiptV1{}, err
	}
	return s.finalizeExecutiveSynthesisV1(
		ctx, expected, ref, marker, content, false)
}

func (s *Store) FinalizeExecutiveSynthesisFallbackV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	marker types.RunOutcomeMarkerV1,
	content types.ExecutiveBriefContentV1,
) (ExecutiveSynthesisReceiptV1, error) {
	if err := validateExecutiveSynthesisMarkerV1(
		expected, ref, marker); err != nil {
		return ExecutiveSynthesisReceiptV1{}, err
	}
	return s.finalizeExecutiveSynthesisV1(
		ctx, expected, ref, marker, content, true)
}

func (s *Store) finalizeExecutiveSynthesisV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	marker types.RunOutcomeMarkerV1,
	content types.ExecutiveBriefContentV1,
	fallback bool,
) (ExecutiveSynthesisReceiptV1, error) {
	if err := content.ValidateIssue(); err != nil {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefValidationError(
				"executive synthesis content is invalid")
	}
	payload, err := json.Marshal(content)
	if err != nil || len(payload) < 2 || len(payload) > 256<<10 {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefValidationError(
				"executive synthesis content is invalid")
	}
	tx, err := s.beginExecutiveSynthesisTxV1(ctx, expected, ref, false)
	if err != nil {
		return ExecutiveSynthesisReceiptV1{}, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockCanonicalBriefRunV1(ctx, tx, ref.SnapshotID); err != nil {
		return ExecutiveSynthesisReceiptV1{}, err
	}
	if _, err := tx.Exec(ctx, `RESET ROLE`); err != nil {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefDatabaseError(
				"leave executive synthesis writer role", err)
	}
	if _, err := tx.Exec(
		ctx, `SET LOCAL ROLE vane_brief_writer`); err != nil {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefDatabaseError(
				"enter canonical Brief writer role", err)
	}
	stage, found, err := loadCanonicalBriefStageV1(
		ctx, tx, marker.ID)
	if err != nil {
		return ExecutiveSynthesisReceiptV1{}, err
	}
	if !found || stage.status != "staged" ||
		stage.draft.RunSnapshotID != ref.SnapshotID {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefConflictError(
				"executive synthesis stage is unavailable")
	}
	if err := validateExecutiveContentReferencesV1(
		content, stage.draft); err != nil {
		return ExecutiveSynthesisReceiptV1{}, err
	}
	if _, err := tx.Exec(ctx, `RESET ROLE`); err != nil {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefDatabaseError(
				"leave canonical Brief writer role", err)
	}
	if _, err := tx.Exec(
		ctx, `SET LOCAL ROLE vane_brief_synthesis_writer`); err != nil {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefDatabaseError(
				"enter executive synthesis writer role", err)
	}
	targetStatus := "finalized"
	generationMode := string(types.ExecutiveGenerationModel)
	processing := string(types.RunCompletenessComplete)
	sourceStatuses := "('spending')"
	if fallback {
		targetStatus = "fallback"
		generationMode = string(types.ExecutiveGenerationFallback)
		processing = string(types.RunCompletenessPartial)
		sourceStatuses = "('prepared','spending','ambiguous')"
	}
	query := fmt.Sprintf(
		`UPDATE executive_brief_synthesis_receipts
		    SET status=$2,generation_mode=$3,processing=$4,
		        content_payload=$5,
		        content_digest=encode(sha256($5),'hex'),
		        finalized_at=clock_timestamp()
		  WHERE run_outcome_id=$1 AND status IN %s`,
		sourceStatuses,
	)
	tag, err := tx.Exec(ctx, query, marker.ID, targetStatus,
		generationMode, processing, payload)
	if err != nil {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefDatabaseError(
				"finalize executive synthesis", err)
	}
	receipt, err := loadExecutiveSynthesisReceiptTxV1(
		ctx, tx, marker.ID, true)
	if err != nil {
		return ExecutiveSynthesisReceiptV1{}, err
	}
	if tag.RowsAffected() == 0 {
		expectedStatus := ExecutiveSynthesisFinalized
		expectedMode := types.ExecutiveGenerationModel
		if fallback {
			expectedStatus = ExecutiveSynthesisFallback
			expectedMode = types.ExecutiveGenerationFallback
		}
		storedPayload, marshalErr := json.Marshal(receipt.Content)
		if receipt.Status != expectedStatus ||
			receipt.GenerationMode != expectedMode ||
			marshalErr != nil || !bytes.Equal(storedPayload, payload) {
			return ExecutiveSynthesisReceiptV1{},
				canonicalBriefConflictError(
					"executive synthesis already finalized differently")
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefDatabaseError(
				"commit executive synthesis finalization", err)
	}
	return receipt, nil
}

func (s *Store) MarkExecutiveSynthesisAmbiguousV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	marker types.RunOutcomeMarkerV1,
) error {
	if err := validateExecutiveSynthesisMarkerV1(
		expected, ref, marker); err != nil {
		return err
	}
	tx, err := s.beginExecutiveSynthesisTxV1(ctx, expected, ref, false)
	if err != nil {
		return err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if _, err := tx.Exec(
		ctx, `SET LOCAL ROLE vane_brief_synthesis_writer`); err != nil {
		return canonicalBriefDatabaseError(
			"enter executive synthesis writer role", err)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE executive_brief_synthesis_receipts
		    SET status='ambiguous',finalized_at=clock_timestamp()
		  WHERE run_outcome_id=$1 AND status='spending'`,
		marker.ID,
	)
	if err != nil {
		return canonicalBriefDatabaseError(
			"mark executive synthesis ambiguous", err)
	}
	if tag.RowsAffected() == 0 {
		receipt, loadErr := loadExecutiveSynthesisReceiptTxV1(
			ctx, tx, marker.ID, true)
		if loadErr != nil {
			return loadErr
		}
		if receipt.Status != ExecutiveSynthesisAmbiguous &&
			receipt.Status != ExecutiveSynthesisFallback &&
			receipt.Status != ExecutiveSynthesisFinalized {
			return canonicalBriefConflictError(
				"executive synthesis ambiguity differs")
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return canonicalBriefDatabaseError(
			"commit executive synthesis ambiguity", err)
	}
	return nil
}

func loadExecutiveSynthesisReceiptTxV1(
	ctx context.Context,
	tx pgx.Tx,
	runOutcomeID int64,
	forUpdate bool,
) (ExecutiveSynthesisReceiptV1, error) {
	query := `SELECT ` + executiveSynthesisColumnsV1 + `
	    FROM executive_brief_synthesis_receipts
	   WHERE run_outcome_id=$1`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	receipt, err := scanExecutiveSynthesisReceiptV1(
		tx.QueryRow(ctx, query, runOutcomeID))
	if errors.Is(err, pgx.ErrNoRows) {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefNotFoundError(
				"executive synthesis receipt is unavailable")
	}
	if err != nil {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefDatabaseError(
				"load executive synthesis receipt", err)
	}
	return receipt, nil
}

func (s *Store) FreezeExecutiveBriefArtifactV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	draft types.ExecutiveBriefArtifactDraftV1,
) (types.ExecutiveBriefArtifactV1, error) {
	return s.freezeExecutiveBriefArtifactV1(
		ctx, expected, ref, draft, false)
}

func (s *Store) FreezeExecutiveBriefArtifactRecoveryV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	draft types.ExecutiveBriefArtifactDraftV1,
) (types.ExecutiveBriefArtifactV1, error) {
	return s.freezeExecutiveBriefArtifactV1(
		ctx, expected, ref, draft, true)
}

func (s *Store) freezeExecutiveBriefArtifactV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	draft types.ExecutiveBriefArtifactDraftV1,
	recovery bool,
) (types.ExecutiveBriefArtifactV1, error) {
	if err := draft.Validate(); err != nil ||
		draft.RunSnapshotID != ref.SnapshotID ||
		draft.TenantID != expected.TenantID ||
		draft.UserID != expected.UserID ||
		draft.TaskID != expected.TaskID {
		return types.ExecutiveBriefArtifactV1{},
			canonicalBriefValidationError(
				"executive Brief artifact is invalid")
	}
	tx, err := s.beginExecutiveSynthesisTxV1(
		ctx, expected, ref, recovery)
	if err != nil {
		return types.ExecutiveBriefArtifactV1{}, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockCanonicalBriefRunV1(ctx, tx, ref.SnapshotID); err != nil {
		return types.ExecutiveBriefArtifactV1{}, err
	}
	receipt, err := loadExecutiveSynthesisReceiptTxV1(
		ctx, tx, draft.RunOutcomeID, true)
	if err != nil {
		return types.ExecutiveBriefArtifactV1{}, err
	}
	if receipt.Status != ExecutiveSynthesisFinalized &&
		receipt.Status != ExecutiveSynthesisFallback {
		return types.ExecutiveBriefArtifactV1{},
			canonicalBriefConflictError(
				"executive synthesis receipt is not terminal")
	}
	if receipt.PushBatchID != draft.PushBatchID ||
		receipt.ProfileEpoch != draft.ProfileEpoch ||
		receipt.ProfileVersion != draft.ProfileVersion ||
		receipt.ProfileDigest != draft.ProfileDigest ||
		receipt.InputDigest != draft.InputDigest ||
		receipt.GenerationMode != draft.GenerationMode ||
		receipt.Processing != draft.Processing ||
		receipt.Content == nil ||
		!reflect.DeepEqual(*receipt.Content, draft.Content) ||
		receipt.FinalizedAt == nil ||
		!receipt.FinalizedAt.Equal(draft.GeneratedAt) {
		return types.ExecutiveBriefArtifactV1{},
			canonicalBriefConflictError(
				"executive Brief artifact differs from synthesis receipt")
	}
	var briefID int64
	err = tx.QueryRow(ctx,
		`SELECT id FROM brief_snapshots
		  WHERE run_outcome_id=$1 AND tenant_id=$2 AND user_id=$3
		    AND task_id=$4 AND run_snapshot_id=$5`,
		draft.RunOutcomeID, expected.TenantID, expected.UserID,
		expected.TaskID, ref.SnapshotID,
	).Scan(&briefID)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.ExecutiveBriefArtifactV1{},
			canonicalBriefNotFoundError(
				"executive Brief canonical snapshot is unavailable")
	}
	if err != nil {
		return types.ExecutiveBriefArtifactV1{},
			canonicalBriefDatabaseError(
				"load executive Brief canonical snapshot", err)
	}
	boundContent, err := draft.Content.BindBriefID(briefID)
	if err != nil {
		return types.ExecutiveBriefArtifactV1{},
			canonicalBriefIntegrityError()
	}
	draft.Content = boundContent
	requestDigest, err := draft.RequestDigest()
	if err != nil {
		return types.ExecutiveBriefArtifactV1{},
			canonicalBriefValidationError(
				"executive Brief artifact cannot be sealed")
	}
	var (
		artifactID    int64
		storedRequest string
		payload       []byte
	)
	err = tx.QueryRow(ctx,
		`SELECT id,request_digest,payload
		   FROM executive_brief_artifacts
		  WHERE run_outcome_id=$1`,
		draft.RunOutcomeID,
	).Scan(&artifactID, &storedRequest, &payload)
	if err == nil {
		var artifact types.ExecutiveBriefArtifactV1
		if json.Unmarshal(payload, &artifact) != nil ||
			artifact.Validate() != nil ||
			storedRequest != requestDigest {
			return types.ExecutiveBriefArtifactV1{},
				canonicalBriefConflictError(
					"executive Brief artifact already differs")
		}
		if err := tx.Commit(ctx); err != nil {
			return types.ExecutiveBriefArtifactV1{},
				canonicalBriefDatabaseError(
					"commit executive Brief artifact replay", err)
		}
		return artifact, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return types.ExecutiveBriefArtifactV1{},
			canonicalBriefDatabaseError(
				"load executive Brief artifact", err)
	}
	if err := tx.QueryRow(ctx,
		`SELECT nextval('executive_brief_artifacts_id_seq')`,
	).Scan(&artifactID); err != nil {
		return types.ExecutiveBriefArtifactV1{},
			canonicalBriefDatabaseError(
				"allocate executive Brief artifact id", err)
	}
	artifact, err := draft.Seal(artifactID, briefID)
	if err != nil {
		return types.ExecutiveBriefArtifactV1{},
			canonicalBriefIntegrityError()
	}
	payload, err = json.Marshal(artifact)
	if err != nil {
		return types.ExecutiveBriefArtifactV1{},
			canonicalBriefIntegrityError()
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO executive_brief_artifacts (
		    id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id,
		    push_batch_id,brief_snapshot_id,schema_version,request_digest,
		    payload_digest,payload,generated_at
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		artifact.ID, artifact.TenantID, artifact.UserID, artifact.TaskID,
		artifact.RunOutcomeID, artifact.RunSnapshotID,
		artifact.PushBatchID, artifact.BriefSnapshotID,
		artifact.SchemaVersion, requestDigest, artifact.Digest,
		payload, artifact.GeneratedAt,
	)
	if err != nil {
		return types.ExecutiveBriefArtifactV1{},
			canonicalBriefDatabaseError(
				"freeze executive Brief artifact", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.ExecutiveBriefArtifactV1{},
			canonicalBriefDatabaseError(
				"commit executive Brief artifact", err)
	}
	return artifact, nil
}

func (s *Store) beginExecutiveSynthesisTxV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	recovery bool,
) (pgx.Tx, error) {
	tx, err := s.beginCanonicalBriefTxV1(ctx, expected, ref)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `RESET ROLE`); err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, canonicalBriefDatabaseError(
			"leave canonical Brief writer role", err)
	}
	if recovery {
		if err := lockExecutiveRecoveryMembershipV1(
			ctx, tx, expected); err != nil {
			rollbackCompiledTaskTx(ctx, tx)
			return nil, err
		}
	}
	role := "vane_brief_synthesis_writer"
	if recovery {
		role = "vane_brief_synthesis_recovery"
	}
	if _, err := tx.Exec(
		ctx, `SET LOCAL ROLE `+role); err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, canonicalBriefDatabaseError(
			"enter executive synthesis role", err)
	}
	return tx, nil
}

func lockExecutiveRecoveryMembershipV1(
	ctx context.Context,
	tx pgx.Tx,
	expected types.RunIdentity,
) error {
	var authorized bool
	err := tx.QueryRow(ctx,
		`SELECT true
		   FROM schedules s
		   JOIN memberships m
		     ON m.tenant_id=s.tenant_id AND m.user_id=s.user_id
		  WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3
		  FOR KEY SHARE OF s,m`,
		expected.TenantID, expected.UserID, expected.TaskID,
	).Scan(&authorized)
	if errors.Is(err, pgx.ErrNoRows) {
		return canonicalBriefNotFoundError(
			"executive synthesis recovery scope is unavailable")
	}
	if err != nil {
		return canonicalBriefDatabaseError(
			"lock executive synthesis recovery scope", err)
	}
	return nil
}

func ExecutiveSynthesisInputDigestV1(
	draft types.BriefDraftV1,
) (string, error) {
	payload, err := json.Marshal(draft)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("%x", sum[:]), nil
}

func ExecutiveSynthesisRequestDigestV1(
	profileDigest string,
	inputDigest string,
	rendererVersion string,
) (string, error) {
	if !validStoreDigestV1(profileDigest) ||
		!validStoreDigestV1(inputDigest) ||
		rendererVersion == "" {
		return "", errors.New("executive synthesis request is invalid")
	}
	payload, err := json.Marshal(struct {
		ProfileDigest   string `json:"profile_digest"`
		InputDigest     string `json:"input_digest"`
		RendererVersion string `json:"renderer_version"`
	}{
		ProfileDigest:   profileDigest,
		InputDigest:     inputDigest,
		RendererVersion: rendererVersion,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("%x", sum[:]), nil
}
