package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

type ExecutiveSynthesisRecoveryCursorV1 struct {
	CandidateAt time.Time
	OutcomeID   int64
}

type ExecutiveSynthesisRecoveryCandidateV1 struct {
	CandidateAt    time.Time
	Kind           string
	Identity       types.RunIdentity
	Ref            types.RunSnapshotRef
	Marker         types.RunOutcomeMarkerV1
	PushBatchID    int64
	Status         ExecutiveSynthesisStatusV1
	ProfileEpoch   int64
	ProfileVersion int64
	ProfileDigest  string
	InputDigest    string
	FinalizedAt    *time.Time
}

func (s *Store) ListExecutiveSynthesisRecoveryCandidatesV1(
	ctx context.Context,
	cursor *ExecutiveSynthesisRecoveryCursorV1,
	limit int,
) ([]ExecutiveSynthesisRecoveryCandidateV1, error) {
	if limit <= 0 || limit > 100 {
		return nil, canonicalBriefValidationError(
			"executive synthesis recovery page is invalid")
	}
	var afterAt any
	var afterID int64
	if cursor != nil {
		if cursor.CandidateAt.IsZero() || cursor.OutcomeID <= 0 {
			return nil, canonicalBriefValidationError(
				"executive synthesis recovery cursor is invalid")
		}
		afterAt = cursor.CandidateAt
		afterID = cursor.OutcomeID
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, canonicalBriefDatabaseError(
			"begin executive synthesis recovery read", err)
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if _, err := tx.Exec(
		ctx, `SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		return nil, canonicalBriefDatabaseError(
			"pin executive synthesis recovery search path", err)
	}
	if _, err := tx.Exec(
		ctx, `SET LOCAL ROLE vane_brief_synthesis_recovery`); err != nil {
		return nil, canonicalBriefDatabaseError(
			"enter executive synthesis recovery role", err)
	}
	rows, err := tx.Query(ctx, `
		SELECT candidate_at,recovery_kind,outcome_id,
		       outcome_schema_version,run_snapshot_id,tenant_id,user_id,
		       task_id,temporal_workflow_id,temporal_run_id,
		       reference_schema_version,reference_digest,
		       snapshot_payload_digest,push_batch_id,receipt_status,
		       profile_epoch,profile_version,profile_digest,input_digest,
		       finalized_at
		  FROM read_executive_synthesis_recovery_v1($1,$2,$3)`,
		afterAt, afterID, limit)
	if err != nil {
		return nil, canonicalBriefDatabaseError(
			"read executive synthesis recovery candidates", err)
	}
	defer rows.Close()
	out := make([]ExecutiveSynthesisRecoveryCandidateV1, 0, limit)
	for rows.Next() {
		var (
			candidate     ExecutiveSynthesisRecoveryCandidateV1
			outcomeSchema string
			finalized     sql.NullTime
		)
		if err := rows.Scan(
			&candidate.CandidateAt, &candidate.Kind,
			&candidate.Marker.ID, &outcomeSchema,
			&candidate.Marker.RunSnapshotID,
			&candidate.Marker.TenantID, &candidate.Marker.UserID,
			&candidate.Marker.TaskID,
			&candidate.Identity.TemporalWorkflowID,
			&candidate.Identity.TemporalRunID,
			&candidate.Ref.SchemaVersion,
			&candidate.Ref.ReferenceDigest,
			&candidate.Ref.PayloadDigest,
			&candidate.PushBatchID, &candidate.Status,
			&candidate.ProfileEpoch, &candidate.ProfileVersion,
			&candidate.ProfileDigest, &candidate.InputDigest,
			&finalized,
		); err != nil {
			return nil, canonicalBriefDatabaseError(
				"scan executive synthesis recovery candidate", err)
		}
		candidate.CandidateAt = candidate.CandidateAt.Round(0).UTC().
			Truncate(time.Microsecond)
		candidate.Marker.SchemaVersion = outcomeSchema
		candidate.Ref.SnapshotID = candidate.Marker.RunSnapshotID
		candidate.Identity = types.RunIdentity{
			TemporalWorkflowID: candidate.Identity.TemporalWorkflowID,
			TemporalRunID:      candidate.Identity.TemporalRunID,
			RunKind:            types.RunSnapshotKindScheduled,
			TenantID:           candidate.Marker.TenantID,
			UserID:             candidate.Marker.UserID,
			TaskID:             candidate.Marker.TaskID,
		}
		if finalized.Valid {
			value := finalized.Time.Round(0).UTC().Truncate(time.Microsecond)
			candidate.FinalizedAt = &value
		}
		if (candidate.Kind != "fallback" &&
			candidate.Kind != "freeze" &&
			candidate.Kind != "prepare") ||
			candidate.CandidateAt.IsZero() ||
			candidate.Marker.Validate() != nil ||
			candidate.Identity.Validate() != nil ||
			candidate.Ref.Validate() != nil ||
			candidate.PushBatchID <= 0 ||
			candidate.ProfileEpoch < 0 ||
			candidate.ProfileVersion < 0 ||
			!validStoreDigestV1(candidate.ProfileDigest) ||
			!validStoreDigestV1(candidate.InputDigest) {
			return nil, canonicalBriefIntegrityError()
		}
		out = append(out, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, canonicalBriefDatabaseError(
			"iterate executive synthesis recovery candidates", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, canonicalBriefDatabaseError(
			"commit executive synthesis recovery read", err)
	}
	return out, nil
}

func (s *Store) LoadExecutiveSynthesisReceiptV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	marker types.RunOutcomeMarkerV1,
) (ExecutiveSynthesisReceiptV1, error) {
	if err := validateExecutiveSynthesisMarkerV1(
		expected, ref, marker); err != nil {
		return ExecutiveSynthesisReceiptV1{}, err
	}
	tx, err := s.beginExecutiveSynthesisTxV1(
		ctx, expected, ref, false)
	if err != nil {
		return ExecutiveSynthesisReceiptV1{}, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	receipt, err := loadExecutiveSynthesisReceiptTxV1(
		ctx, tx, marker.ID, false)
	if err != nil {
		return ExecutiveSynthesisReceiptV1{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefDatabaseError(
				"commit executive synthesis receipt read", err)
	}
	return receipt, nil
}

func (s *Store) RecoverExecutiveSynthesisFallbackV1(
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
	if err := content.ValidateIssueFallback(); err != nil {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefValidationError(
				"executive synthesis recovery content is invalid")
	}
	payload, err := json.Marshal(content)
	if err != nil || len(payload) < 2 || len(payload) > 256<<10 {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefValidationError(
				"executive synthesis recovery payload is invalid")
	}
	tx, err := s.beginExecutiveSynthesisTxV1(
		ctx, expected, ref, true)
	if err != nil {
		return ExecutiveSynthesisReceiptV1{}, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockCanonicalBriefRunV1(
		ctx, tx, ref.SnapshotID); err != nil {
		return ExecutiveSynthesisReceiptV1{}, err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE executive_brief_synthesis_receipts
		   SET status='fallback',
		       generation_mode='deterministic_fallback',
		       processing='partial',content_payload=$2,
		       content_digest=encode(sha256($2),'hex'),
		       finalized_at=clock_timestamp()
		 WHERE run_outcome_id=$1
		   AND status IN ('prepared','spending','ambiguous')`,
		marker.ID, payload)
	if err != nil {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefDatabaseError(
				"recover executive synthesis fallback", err)
	}
	receipt, err := loadExecutiveSynthesisReceiptTxV1(
		ctx, tx, marker.ID, true)
	if err != nil {
		return ExecutiveSynthesisReceiptV1{}, err
	}
	if tag.RowsAffected() == 0 {
		storedPayload, marshalErr := json.Marshal(receipt.Content)
		if receipt.Status != ExecutiveSynthesisFallback ||
			marshalErr != nil || !bytes.Equal(storedPayload, payload) {
			return ExecutiveSynthesisReceiptV1{},
				canonicalBriefConflictError(
					"executive synthesis recovery already differs")
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ExecutiveSynthesisReceiptV1{},
			canonicalBriefDatabaseError(
				"commit executive synthesis recovery", err)
	}
	return receipt, nil
}
