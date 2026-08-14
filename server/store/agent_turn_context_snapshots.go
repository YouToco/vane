package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/agentcontext"
	"github.com/YouToco/vane/server/agentledger"
)

// SealAgentTurnContextSnapshot writes an immutable shadow candidate only when
// the exact session is currently ledger-authoritative. The Store, not the
// caller, owns the B3 route/evidence check and seal-time durability watermark.
// That watermark is not the candidate's causal input base and must never drive
// automatic replay/resume. Legacy sessions return Skipped without writing.
func (s *Store) SealAgentTurnContextSnapshot(
	ctx context.Context,
	scope agentcontext.Scope,
	candidate agentcontext.CandidateSnapshot,
) (agentcontext.SealResult, error) {
	if scope.TenantID <= 0 || scope.UserID <= 0 || scope.SessionID <= 0 ||
		candidate.Scope != scope {
		return agentcontext.SealResult{},
			agentEventValidationError("agent turn context scope is invalid")
	}
	if err := agentcontext.VerifyCandidate(candidate); err != nil {
		return agentcontext.SealResult{},
			agentEventValidationError("agent turn context candidate is invalid")
	}

	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return agentcontext.SealResult{},
			agentEventDatabaseError("begin agent turn context seal", err)
	}
	defer func() {
		_ = tx.Rollback(context.WithoutCancel(ctx))
	}()
	if err := setAgentEventRuntimeContext(ctx, tx, scope.TenantID); err != nil {
		return agentcontext.SealResult{}, err
	}
	ledgerScope := agentledger.Scope{
		TenantID: scope.TenantID, UserID: scope.UserID,
		SessionID: scope.SessionID,
	}
	projection, events, _, err :=
		loadAuthoritativeAgentSessionProjectionForUpdate(
			ctx, tx, ledgerScope,
		)
	if err != nil {
		return agentcontext.SealResult{}, err
	}
	status, err := loadAgentSessionProjectionAuthorityStatus(
		ctx, tx, ledgerScope,
	)
	if err != nil {
		return agentcontext.SealResult{}, err
	}
	if status.Route != AgentSessionProjectionRouteLedger {
		if err := tx.Commit(ctx); err != nil {
			return agentcontext.SealResult{},
				agentEventDatabaseError(
					"commit skipped agent turn context seal", err)
		}
		return agentcontext.SealResult{Skipped: true}, nil
	}
	if status.Generation <= 0 || len(events) == 0 {
		return agentcontext.SealResult{}, agentEventIntegrityError()
	}
	head := events[len(events)-1]
	if head.Sequence <= 0 || head.ID <= 0 {
		return agentcontext.SealResult{}, agentEventIntegrityError()
	}
	projectionDigest, err := agentledger.ProjectionDigest(projection)
	if err != nil {
		return agentcontext.SealResult{}, agentEventIntegrityError()
	}

	existing, found, err := loadAgentTurnContextSnapshot(
		ctx, tx, scope, candidate.TurnID, candidate.ContextStep,
	)
	if err != nil {
		return agentcontext.SealResult{}, err
	}
	if found {
		if existing.Digest != candidate.Digest ||
			existing.Scope != candidate.Scope {
			return agentcontext.SealResult{},
				agentEventConflict()
		}
		if err := verifyStoredAgentTurnContextSnapshot(
			ctx, tx, scope, existing, status.Generation, events,
		); err != nil {
			return agentcontext.SealResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return agentcontext.SealResult{},
				agentEventDatabaseError(
					"commit replayed agent turn context seal", err)
		}
		return agentcontext.SealResult{Sealed: true, Snapshot: existing}, nil
	}

	snapshot := agentcontext.TurnSnapshot{
		CandidateSnapshot:          candidate,
		SealAuthorityGeneration:    status.Generation,
		SealLedgerHeadSequence:     head.Sequence,
		SealLedgerHeadEventID:      head.ID,
		SealLedgerProjectionDigest: projectionDigest,
	}
	snapshot.SnapshotDigest, err =
		agentcontext.TurnSnapshotDigest(snapshot)
	if err != nil {
		return agentcontext.SealResult{},
			agentEventIntegrityError()
	}
	candidateRaw, err := json.Marshal(candidate)
	if err != nil {
		return agentcontext.SealResult{},
			agentEventValidationError(
				"agent turn context candidate is invalid")
	}
	var id int64
	err = tx.QueryRow(ctx,
		`INSERT INTO public.agent_turn_context_snapshots (
		    tenant_id,user_id,session_id,turn_id,context_step,
		    schema_version,compiler_version,candidate_digest,
		    candidate_snapshot,replayable,seal_authority_generation,
		    seal_ledger_head_sequence,seal_ledger_head_event_id,
		    seal_ledger_projection_digest,snapshot_digest
		 ) VALUES (
		    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15
		 )
		 RETURNING id`,
		scope.TenantID, scope.UserID, scope.SessionID,
		candidate.TurnID, candidate.ContextStep,
		candidate.SchemaVersion, candidate.CompilerVersion,
		candidate.Digest, candidateRaw, candidate.Replayable,
		snapshot.SealAuthorityGeneration, snapshot.SealLedgerHeadSequence,
		snapshot.SealLedgerHeadEventID, snapshot.SealLedgerProjectionDigest,
		snapshot.SnapshotDigest,
	).Scan(&id)
	if err != nil {
		return agentcontext.SealResult{},
			agentEventDatabaseError("insert agent turn context snapshot", err)
	}
	if id <= 0 {
		return agentcontext.SealResult{}, agentEventIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return agentcontext.SealResult{},
			agentEventDatabaseError("commit agent turn context seal", err)
	}
	return agentcontext.SealResult{Sealed: true, Snapshot: snapshot}, nil
}

func loadAgentTurnContextSnapshot(
	ctx context.Context,
	tx pgx.Tx,
	scope agentcontext.Scope,
	turnID string,
	contextStep int,
) (agentcontext.TurnSnapshot, bool, error) {
	var snapshot agentcontext.TurnSnapshot
	var candidateRaw []byte
	var schemaVersion, compilerVersion, candidateDigest string
	var replayable bool
	err := tx.QueryRow(ctx,
		`SELECT candidate_snapshot,schema_version,compiler_version,
		        candidate_digest,replayable,seal_authority_generation,
		        seal_ledger_head_sequence,seal_ledger_head_event_id,
		        seal_ledger_projection_digest,snapshot_digest
		   FROM public.agent_turn_context_snapshots
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
		    AND turn_id=$4 AND context_step=$5`,
		scope.TenantID, scope.UserID, scope.SessionID, turnID, contextStep,
	).Scan(
		&candidateRaw, &schemaVersion, &compilerVersion,
		&candidateDigest, &replayable, &snapshot.SealAuthorityGeneration,
		&snapshot.SealLedgerHeadSequence, &snapshot.SealLedgerHeadEventID,
		&snapshot.SealLedgerProjectionDigest, &snapshot.SnapshotDigest,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return agentcontext.TurnSnapshot{}, false, nil
	}
	if err != nil {
		return agentcontext.TurnSnapshot{}, false,
			agentEventDatabaseError(
				"load agent turn context snapshot", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(candidateRaw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&snapshot.CandidateSnapshot); err != nil {
		return agentcontext.TurnSnapshot{}, false,
			agentEventIntegrityError()
	}
	if schemaVersion != snapshot.SchemaVersion ||
		compilerVersion != snapshot.CompilerVersion ||
		!constantTimeAgentEventDigestEqual(
			candidateDigest, snapshot.Digest,
		) ||
		replayable != snapshot.Replayable {
		return agentcontext.TurnSnapshot{}, false,
			agentEventIntegrityError()
	}
	return snapshot, true, nil
}

func verifyStoredAgentTurnContextSnapshot(
	ctx context.Context,
	tx pgx.Tx,
	scope agentcontext.Scope,
	snapshot agentcontext.TurnSnapshot,
	currentGeneration int64,
	events []agentledger.Event,
) error {
	if err := agentcontext.VerifyCandidate(
		snapshot.CandidateSnapshot,
	); err != nil {
		return agentEventIntegrityError()
	}
	digest, err := agentcontext.TurnSnapshotDigest(snapshot)
	if err != nil || !constantTimeAgentEventDigestEqual(
		digest, snapshot.SnapshotDigest,
	) {
		return agentEventIntegrityError()
	}
	if snapshot.SealAuthorityGeneration <= 0 ||
		snapshot.SealAuthorityGeneration > currentGeneration ||
		snapshot.SealLedgerHeadSequence <= 0 ||
		snapshot.SealLedgerHeadEventID <= 0 ||
		!validAgentEventDigest(snapshot.SealLedgerProjectionDigest) {
		return agentEventIntegrityError()
	}
	if snapshot.SealLedgerHeadSequence > int64(len(events)) {
		return agentEventIntegrityError()
	}
	head := events[snapshot.SealLedgerHeadSequence-1]
	if head.Sequence != snapshot.SealLedgerHeadSequence ||
		head.ID != snapshot.SealLedgerHeadEventID {
		return agentEventIntegrityError()
	}
	projected, err := agentledger.ProjectLatestSessionSnapshot(
		events[:snapshot.SealLedgerHeadSequence],
	)
	if err != nil {
		return agentEventIntegrityError()
	}
	projectionDigest, err := agentledger.ProjectionDigest(projected)
	if err != nil || !constantTimeAgentEventDigestEqual(
		projectionDigest, snapshot.SealLedgerProjectionDigest,
	) {
		return agentEventIntegrityError()
	}
	if err := verifyAgentTurnContextSealAuthority(
		ctx, tx, scope, snapshot, events,
	); err != nil {
		return err
	}
	return nil
}

func verifyAgentTurnContextSealAuthority(
	ctx context.Context,
	tx pgx.Tx,
	scope agentcontext.Scope,
	snapshot agentcontext.TurnSnapshot,
	events []agentledger.Event,
) error {
	var action string
	var authorityHead int64
	var legacyDigest, ledgerDigest string
	err := tx.QueryRow(ctx,
		`SELECT action,ledger_head_sequence,legacy_digest,ledger_digest
		   FROM public.agent_session_projection_authority_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
		    AND generation=$4`,
		scope.TenantID, scope.UserID, scope.SessionID,
		snapshot.SealAuthorityGeneration,
	).Scan(&action, &authorityHead, &legacyDigest, &ledgerDigest)
	if errors.Is(err, pgx.ErrNoRows) {
		return agentEventIntegrityError()
	}
	if err != nil {
		return agentEventDatabaseError(
			"load agent turn context seal authority", err,
		)
	}
	if action != string(AgentSessionProjectionAuthorityActivate) ||
		authorityHead <= 0 ||
		authorityHead > snapshot.SealLedgerHeadSequence ||
		authorityHead > int64(len(events)) ||
		events[authorityHead-1].Sequence != authorityHead {
		return agentEventIntegrityError()
	}
	projected, err := agentledger.ProjectLatestSessionSnapshot(
		events[:authorityHead],
	)
	if err != nil {
		return agentEventIntegrityError()
	}
	digest, err := agentledger.ProjectionDigest(projected)
	if err != nil ||
		!constantTimeAgentEventDigestEqual(digest, legacyDigest) ||
		!constantTimeAgentEventDigestEqual(digest, ledgerDigest) {
		return agentEventIntegrityError()
	}
	return nil
}
