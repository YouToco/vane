package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/types"
)

type CreateOrGetResearchPlannerToolSearchReceiptV1Params struct {
	Identity                types.RunIdentity
	SnapshotRef             types.ResearchRunSnapshotRefV3
	PlannerLLMReservationID int64
	Receipt                 runcontext.ResearchPlannerToolSearchReceiptV1
}

// CreateOrGetResearchPlannerToolSearchReceiptV1 freezes one deterministic
// local catalog search after the exact planner LLM request has settled. The
// database trigger independently binds the request to that completion and
// every returned name/schema digest to the run snapshot's tool policy.
func (s *Store) CreateOrGetResearchPlannerToolSearchReceiptV1(
	ctx context.Context,
	params CreateOrGetResearchPlannerToolSearchReceiptV1Params,
) (runcontext.ResearchPlannerToolSearchReceiptV1, error) {
	if params.SnapshotRef.ValidateFor(params.Identity) != nil ||
		params.PlannerLLMReservationID <= 0 || params.Receipt.Validate() != nil ||
		params.Receipt.CatalogDigest != params.SnapshotRef.ToolPolicyDigest {
		return runcontext.ResearchPlannerToolSearchReceiptV1{},
			researchRunValidationError("research planner tool search receipt scope is invalid")
	}
	payload, err := runcontext.EncodeResearchPlannerToolSearchReceiptV1(params.Receipt)
	if err != nil {
		return runcontext.ResearchPlannerToolSearchReceiptV1{},
			researchRunValidationError("research planner tool search receipt payload is invalid")
	}
	digest := researchRunSHA256(payload)
	tx, scopedRef, err := s.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{}, params.Identity, params.SnapshotRef.SnapshotID)
	if err != nil {
		return runcontext.ResearchPlannerToolSearchReceiptV1{},
			researchRunDatabaseError("begin research planner tool search receipt", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if scopedRef != params.SnapshotRef {
		return runcontext.ResearchPlannerToolSearchReceiptV1{}, researchRunIntegrityError()
	}
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1,0))`,
		fmt.Sprintf("research-planner-tool-search/v1:%d:%d",
			params.SnapshotRef.SnapshotID, params.Receipt.RoundOrdinal)); err != nil {
		return runcontext.ResearchPlannerToolSearchReceiptV1{},
			researchRunDatabaseError("lock research planner tool search receipt", err)
	}
	var storedPayload []byte
	var storedDigest string
	var storedReservationID int64
	err = tx.QueryRow(ctx, `
		SELECT receipt_payload,receipt_digest,planner_llm_spend_reservation_id
		  FROM research_planner_tool_search_receipts
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		   AND run_snapshot_id=$4 AND round_ordinal=$5`,
		params.Identity.TenantID, params.Identity.UserID, params.Identity.TaskID,
		params.SnapshotRef.SnapshotID, params.Receipt.RoundOrdinal,
	).Scan(&storedPayload, &storedDigest, &storedReservationID)
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = tx.Exec(ctx, `
			INSERT INTO research_planner_tool_search_receipts (
			    tenant_id,user_id,task_id,run_snapshot_id,round_ordinal,
			    planner_llm_spend_reservation_id,catalog_digest,
			    receipt_payload,receipt_digest,schema_version
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			params.Identity.TenantID, params.Identity.UserID, params.Identity.TaskID,
			params.SnapshotRef.SnapshotID, params.Receipt.RoundOrdinal,
			params.PlannerLLMReservationID, params.Receipt.CatalogDigest,
			payload, digest, runcontext.ResearchPlannerToolSearchReceiptSchemaV1)
		if err != nil {
			return runcontext.ResearchPlannerToolSearchReceiptV1{},
				researchRunDatabaseError("insert research planner tool search receipt", err)
		}
		storedPayload, storedDigest, storedReservationID = payload, digest,
			params.PlannerLLMReservationID
	} else if err != nil {
		return runcontext.ResearchPlannerToolSearchReceiptV1{},
			researchRunDatabaseError("load research planner tool search receipt", err)
	}
	if storedReservationID != params.PlannerLLMReservationID ||
		storedDigest != digest || !bytes.Equal(storedPayload, payload) {
		return runcontext.ResearchPlannerToolSearchReceiptV1{}, researchRunConflictError()
	}
	stored, err := runcontext.DecodeResearchPlannerToolSearchReceiptV1(storedPayload)
	if err != nil || stored.CatalogDigest != params.SnapshotRef.ToolPolicyDigest {
		return runcontext.ResearchPlannerToolSearchReceiptV1{}, researchRunIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return runcontext.ResearchPlannerToolSearchReceiptV1{},
			researchRunDatabaseError("commit research planner tool search receipt", err)
	}
	return stored, nil
}

func (s *Store) LoadResearchPlannerToolSearchReceiptsV1(
	ctx context.Context, identity types.RunIdentity,
	snapshot types.ResearchRunSnapshotRefV3,
) ([]runcontext.ResearchPlannerToolSearchReceiptV1, error) {
	if snapshot.ValidateFor(identity) != nil {
		return nil, researchRunValidationError("research planner tool search recovery scope is invalid")
	}
	tx, scopedRef, err := s.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly}, identity, snapshot.SnapshotID)
	if err != nil {
		return nil, researchRunDatabaseError("begin research planner tool search recovery", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if scopedRef != snapshot {
		return nil, researchRunIntegrityError()
	}
	rows, err := tx.Query(ctx, `
		SELECT round_ordinal,catalog_digest,receipt_payload,receipt_digest
		  FROM research_planner_tool_search_receipts
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND run_snapshot_id=$4
		 ORDER BY round_ordinal`, identity.TenantID, identity.UserID,
		identity.TaskID, snapshot.SnapshotID)
	if err != nil {
		return nil, researchRunDatabaseError("load research planner tool search recovery", err)
	}
	defer rows.Close()
	receipts := make([]runcontext.ResearchPlannerToolSearchReceiptV1, 0, 4)
	previousRound := -1
	for rows.Next() {
		var round int
		var catalogDigest, receiptDigest string
		var payload []byte
		if err := rows.Scan(&round, &catalogDigest, &payload, &receiptDigest); err != nil {
			return nil, researchRunDatabaseError("scan research planner tool search recovery", err)
		}
		receipt, err := runcontext.DecodeResearchPlannerToolSearchReceiptV1(payload)
		if err != nil || receipt.RoundOrdinal != round || round <= previousRound ||
			receipt.CatalogDigest != catalogDigest || catalogDigest != snapshot.ToolPolicyDigest ||
			receiptDigest != researchRunSHA256(payload) {
			return nil, researchRunIntegrityError()
		}
		previousRound = round
		receipts = append(receipts, receipt)
	}
	if err := rows.Err(); err != nil {
		return nil, researchRunDatabaseError("read research planner tool search recovery", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, researchRunDatabaseError("commit research planner tool search recovery", err)
	}
	return receipts, nil
}
