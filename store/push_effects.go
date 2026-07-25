package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/types"
)

const (
	maxPushEffectLease       = 24 * time.Hour
	maxPushEffectRetryWindow = 30 * 24 * time.Hour
	pushEffectTakeoverGrace  = 30 * time.Second
	maxPushEffectFailure     = 128
	maxPushEffectOwner       = 255
	maxPushEffectMessageID   = 512
)

const pushEffectColumns = `
	id, tenant_id, user_id, task_id, run_snapshot_id, run_id, step_id,
	chunk_index, chunk_count, batch_id, delivery_ids, provider, app_identity,
	provider_chat_id, target, card_payload, card_digest, provider_uuid::text,
	idempotency_expires_at, schema_version, canonical_payload, payload_digest,
	status, lease_owner, lease_until,
	takeover_not_before, fence, attempt, next_attempt_at, provider_message_id,
	failure_class, ambiguous_since, sent_at, blocked_at, created_at, updated_at`

type pushEffectScanner interface {
	Scan(...any) error
}

func scanPushEffect(row pushEffectScanner) (*pusheffect.Effect, error) {
	var effect pusheffect.Effect
	err := row.Scan(
		&effect.ID, &effect.TenantID, &effect.UserID, &effect.TaskID,
		&effect.RunSnapshotID, &effect.RunID, &effect.StepID,
		&effect.ChunkIndex, &effect.ChunkCount, &effect.BatchID,
		&effect.DeliveryIDs, &effect.Provider, &effect.AppIdentity,
		&effect.ProviderChatID, &effect.Target, &effect.Card,
		&effect.CardDigest, &effect.ProviderUUID, &effect.IdempotencyExpiresAt,
		&effect.SchemaVersion, &effect.CanonicalPayload, &effect.PayloadDigest,
		&effect.Status, &effect.LeaseOwner, &effect.LeaseUntil,
		&effect.TakeoverNotBefore, &effect.Fence, &effect.Attempt,
		&effect.NextAttemptAt, &effect.ProviderMessageID, &effect.FailureClass,
		&effect.AmbiguousSince, &effect.SentAt, &effect.BlockedAt,
		&effect.CreatedAt, &effect.UpdatedAt,
	)
	return &effect, err
}

// CreatePushEffect freezes one provider request. Exact response-lost replay
// returns the existing row; any identity or byte drift is a conflict.
func (s *Store) CreatePushEffect(
	ctx context.Context,
	prepared pusheffect.Prepared,
) (*pusheffect.Effect, error) {
	canonical, err := pusheffect.Canonicalize(prepared)
	if err != nil {
		return nil, pushEffectValidation("push effect payload is invalid")
	}
	tx, err := s.beginPushEffectCoordinatorTx(ctx, prepared.TenantID)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return nil, pushEffectConflict("push effect tenant is unavailable")
		}
		return nil, pushEffectDatabaseError("begin create transaction", err)
	}
	defer rollbackPushEffectTx(ctx, tx)

	if _, err := lockPushEffectBatchAdmission(
		ctx,
		tx,
		types.PushBatchScope{
			TenantID: prepared.TenantID,
			UserID:   prepared.UserID,
			BatchID:  prepared.BatchID,
		},
		prepared.RunSnapshotID,
		types.PushBatchDeliveryAuthorityEffect,
	); err != nil {
		return nil, err
	}

	existing, err := loadPushEffectForUpdate(ctx, tx, prepared.Scope())
	if err == nil {
		if err := validatePushEffectReplay(existing, canonical); err != nil {
			return nil, err
		}
		if err := validateStoredPushEffect(existing); err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, pushEffectDatabaseError(
				"commit create exact replay", err)
		}
		return existing, nil
	}
	if !errors.Is(err, types.ErrNotFound) {
		return nil, err
	}
	if err := verifyPushEffectAggregate(ctx, tx, prepared); err != nil {
		return nil, err
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO push_effects (
			id, tenant_id, user_id, task_id, run_snapshot_id, run_id, step_id,
			chunk_index, chunk_count, batch_id, delivery_ids, provider,
			app_identity, provider_chat_id, target, card_payload, card_digest,
			provider_uuid, idempotency_expires_at, schema_version,
			canonical_payload, payload_digest
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
			$17,$18::uuid,$19,$20,$21,$22
		) ON CONFLICT (id) DO NOTHING`,
		prepared.ID, prepared.TenantID, prepared.UserID, prepared.TaskID,
		prepared.RunSnapshotID, prepared.RunID, prepared.StepID,
		prepared.ChunkIndex, prepared.ChunkCount, prepared.BatchID,
		prepared.DeliveryIDs, prepared.Provider, prepared.AppIdentity,
		prepared.ProviderChatID, prepared.Target, prepared.Card,
		canonical.CardDigest(), prepared.ProviderUUID,
		prepared.IdempotencyExpiresAt, pusheffect.SchemaVersion,
		canonical.Payload(), canonical.Digest(),
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, pushEffectConflict(
				"push effect immutable identity is already bound")
		}
		return nil, pushEffectDatabaseError("insert push effect", err)
	}
	effect, err := loadPushEffectForUpdate(ctx, tx, prepared.Scope())
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		if err := validatePushEffectReplay(effect, canonical); err != nil {
			return nil, err
		}
	}
	if err := validateStoredPushEffect(effect); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, pushEffectDatabaseError("commit create transaction", err)
	}
	return effect, nil
}

func (s *Store) LoadPushEffect(
	ctx context.Context,
	scope pusheffect.Scope,
) (*pusheffect.Effect, error) {
	if err := validatePushEffectScope(scope); err != nil {
		return nil, err
	}
	tx, err := s.beginPushEffectCoordinatorTx(ctx, scope.TenantID)
	if err != nil {
		return nil, pushEffectDatabaseError("begin load transaction", err)
	}
	defer rollbackPushEffectTx(ctx, tx)
	effect, err := loadPushEffect(ctx, tx, scope, "")
	if err != nil {
		return nil, err
	}
	if err := validateStoredPushEffect(effect); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, pushEffectDatabaseError("commit load transaction", err)
	}
	return effect, nil
}

// ListRecoverablePushEffectTenantIDs is the only cross-tenant discovery
// exception. It is owner/admin READ ONLY and returns tenant IDs only; every
// subsequent row read or mutation re-enters the tenant-scoped coordinator role.
func (s *Store) ListRecoverablePushEffectTenantIDs(
	ctx context.Context,
	before time.Time,
	afterTenantID int64,
	limit int,
) ([]int64, error) {
	if before.IsZero() || afterTenantID < 0 || limit <= 0 || limit > 1000 {
		return nil, pushEffectValidation(
			"recoverable push effect tenant query is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, pushEffectDatabaseError(
			"begin recoverable tenant discovery", err)
	}
	defer rollbackPushEffectTx(ctx, tx)
	rows, err := tx.Query(ctx, `
		SELECT DISTINCT tenant_id
		  FROM push_effects
		 WHERE tenant_id>$2 AND (
		       (status IN ('prepared','definite_failed') AND
		        next_attempt_at<=LEAST($1,clock_timestamp())) OR
		       (status='sending' AND takeover_not_before IS NOT NULL AND
		        takeover_not_before<=LEAST($1,clock_timestamp()))
		   )
		 ORDER BY tenant_id LIMIT $3`,
		before, afterTenantID, limit)
	if err != nil {
		return nil, pushEffectDatabaseError(
			"list recoverable tenant shards", err)
	}
	defer rows.Close()
	tenantIDs := make([]int64, 0)
	for rows.Next() {
		var tenantID int64
		if err := rows.Scan(&tenantID); err != nil {
			return nil, pushEffectDatabaseError(
				"scan recoverable tenant shard", err)
		}
		tenantIDs = append(tenantIDs, tenantID)
	}
	if err := rows.Err(); err != nil {
		return nil, pushEffectDatabaseError(
			"iterate recoverable tenant shards", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, pushEffectDatabaseError(
			"commit recoverable tenant discovery", err)
	}
	return tenantIDs, nil
}

// ListRecoverablePushEffects returns due safe-send rows and stale sending rows
// for one tenant. Callers must Claim the former or TakeOver the latter; listing
// itself grants no provider authority.
func (s *Store) ListRecoverablePushEffects(
	ctx context.Context,
	tenantID int64,
	before time.Time,
	limit int,
) ([]pusheffect.Effect, error) {
	if tenantID <= 0 || before.IsZero() || limit <= 0 || limit > 1000 {
		return nil, pushEffectValidation(
			"recoverable push effect query is invalid")
	}
	tx, err := s.beginPushEffectCoordinatorTx(ctx, tenantID)
	if err != nil {
		return nil, pushEffectDatabaseError(
			"begin recoverable push effect list", err)
	}
	defer rollbackPushEffectTx(ctx, tx)
	rows, err := tx.Query(ctx, `
		SELECT `+pushEffectColumns+`
		  FROM push_effects
		 WHERE tenant_id=$1 AND (
		       (status IN ('prepared','definite_failed') AND
		        next_attempt_at<=LEAST($2,clock_timestamp())) OR
		       (status='sending' AND takeover_not_before IS NOT NULL AND
		        takeover_not_before<=LEAST($2,clock_timestamp()))
		   )
		 ORDER BY
		   CASE WHEN status='sending' THEN takeover_not_before
		        ELSE next_attempt_at END,
		   id
		 LIMIT $3`,
		tenantID, before, limit)
	if err != nil {
		return nil, pushEffectDatabaseError(
			"list recoverable push effects", err)
	}
	defer rows.Close()
	effects := make([]pusheffect.Effect, 0)
	for rows.Next() {
		effect, err := scanPushEffect(rows)
		if err != nil {
			return nil, pushEffectDatabaseError(
				"scan recoverable push effect", err)
		}
		if err := validateStoredPushEffect(effect); err != nil {
			return nil, err
		}
		effects = append(effects, *effect)
	}
	if err := rows.Err(); err != nil {
		return nil, pushEffectDatabaseError(
			"iterate recoverable push effects", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, pushEffectDatabaseError(
			"commit recoverable push effect list", err)
	}
	return effects, nil
}

// ClaimPushEffect authorizes a fresh send only from prepared or a failure
// proven to have happened before the provider accepted the request.
func (s *Store) ClaimPushEffect(
	ctx context.Context,
	params pusheffect.ClaimParams,
) (*pusheffect.Effect, error) {
	if err := validatePushEffectClaim(params); err != nil {
		return nil, err
	}
	tx, err := s.beginPushEffectCoordinatorTx(ctx, params.TenantID)
	if err != nil {
		return nil, pushEffectDatabaseError("begin claim transaction", err)
	}
	defer rollbackPushEffectTx(ctx, tx)
	if err := lockPushEffectBatchForScope(ctx, tx, params.Scope); err != nil {
		return nil, err
	}
	effect, databaseNow, err := loadPushEffectWithClock(ctx, tx, params.Scope)
	if err != nil {
		return nil, err
	}
	if err := validateStoredPushEffect(effect); err != nil {
		return nil, err
	}
	if effect.Status == pusheffect.StatusSending &&
		effect.LeaseOwner == params.LeaseOwner && effect.LeaseUntil != nil &&
		databaseNow.Before(*effect.LeaseUntil) {
		if err := tx.Commit(ctx); err != nil {
			return nil, pushEffectDatabaseError("commit claim replay", err)
		}
		return effect, nil
	}
	if effect.Status != pusheffect.StatusPrepared &&
		effect.Status != pusheffect.StatusDefiniteFailed {
		return nil, pushEffectBusyOrTerminal(effect.Status)
	}
	if databaseNow.Before(effect.NextAttemptAt) {
		return nil, pushEffectBusy()
	}
	tag, err := tx.Exec(ctx, `
		UPDATE push_effects
		   SET status='sending', lease_owner=$4,
		       lease_until=clock_timestamp()+($6*interval '1 microsecond'),
		       takeover_not_before=clock_timestamp()+($7*interval '1 microsecond'),
		       fence=fence+1, attempt=attempt+1,
		       failure_class='', ambiguous_since=NULL,
		       updated_at=clock_timestamp()
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND fence=$5
		   AND status IN ('prepared','definite_failed')
		   AND next_attempt_at<=clock_timestamp()
		   AND lease_owner='' AND lease_until IS NULL`,
		params.ID, params.TenantID, params.UserID, params.LeaseOwner,
		effect.Fence, params.LeaseDuration.Microseconds(),
		(params.LeaseDuration + pushEffectTakeoverGrace).Microseconds(),
	)
	if err != nil {
		return nil, pushEffectDatabaseError("claim push effect", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, pushEffectBusy()
	}
	effect, err = loadPushEffectForUpdate(ctx, tx, params.Scope)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, pushEffectDatabaseError("commit claim transaction", err)
	}
	return effect, nil
}

// ClaimPushEffectReconciliation authorizes an exact retry of the frozen
// target/card/provider UUID only while the provider's idempotency window is
// still open. It never interprets a provider history miss as proof of no send.
func (s *Store) ClaimPushEffectReconciliation(
	ctx context.Context,
	params pusheffect.ClaimParams,
) (*pusheffect.Effect, error) {
	if err := validatePushEffectClaim(params); err != nil {
		return nil, err
	}
	tx, err := s.beginPushEffectCoordinatorTx(ctx, params.TenantID)
	if err != nil {
		return nil, pushEffectDatabaseError(
			"begin reconciliation claim transaction", err)
	}
	defer rollbackPushEffectTx(ctx, tx)
	if err := lockPushEffectBatchForScope(ctx, tx, params.Scope); err != nil {
		return nil, err
	}
	effect, databaseNow, err := loadPushEffectWithClock(ctx, tx, params.Scope)
	if err != nil {
		return nil, err
	}
	if err := validateStoredPushEffect(effect); err != nil {
		return nil, err
	}
	if effect.Status == pusheffect.StatusSending &&
		effect.LeaseOwner == params.LeaseOwner && effect.LeaseUntil != nil &&
		databaseNow.Before(*effect.LeaseUntil) {
		if err := tx.Commit(ctx); err != nil {
			return nil, pushEffectDatabaseError(
				"commit reconciliation claim replay", err)
		}
		return effect, nil
	}
	if effect.Status != pusheffect.StatusAmbiguous ||
		!databaseNow.Before(effect.IdempotencyExpiresAt) ||
		databaseNow.Add(params.LeaseDuration).After(
			effect.IdempotencyExpiresAt) {
		return nil, pushEffectConflict(
			"push effect reconciliation window is unavailable")
	}
	tag, err := tx.Exec(ctx, `
		UPDATE push_effects
		   SET status='sending', lease_owner=$4,
		       lease_until=clock_timestamp()+($6*interval '1 microsecond'),
		       takeover_not_before=clock_timestamp()+($7*interval '1 microsecond'),
		       fence=fence+1, attempt=attempt+1,
		       failure_class='', ambiguous_since=NULL,
		       updated_at=clock_timestamp()
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND fence=$5
		   AND status='ambiguous'
		   AND idempotency_expires_at>=
		       clock_timestamp()+($6*interval '1 microsecond')
		   AND lease_owner='' AND lease_until IS NULL`,
		params.ID, params.TenantID, params.UserID, params.LeaseOwner,
		effect.Fence, params.LeaseDuration.Microseconds(),
		(params.LeaseDuration + pushEffectTakeoverGrace).Microseconds())
	if err != nil {
		return nil, pushEffectDatabaseError(
			"claim push effect reconciliation", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, pushEffectBusy()
	}
	effect, err = loadPushEffectForUpdate(ctx, tx, params.Scope)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, pushEffectDatabaseError(
			"commit reconciliation claim transaction", err)
	}
	return effect, nil
}

// TakeOverStalePushEffect invalidates the old fence and classifies the
// external outcome as unknown. It deliberately returns no sending lease.
func (s *Store) TakeOverStalePushEffect(
	ctx context.Context,
	scope pusheffect.Scope,
) (*pusheffect.Effect, error) {
	if err := validatePushEffectScope(scope); err != nil {
		return nil, err
	}
	tx, err := s.beginPushEffectCoordinatorTx(ctx, scope.TenantID)
	if err != nil {
		return nil, pushEffectDatabaseError("begin takeover transaction", err)
	}
	defer rollbackPushEffectTx(ctx, tx)
	if err := lockPushEffectBatchForScope(ctx, tx, scope); err != nil {
		return nil, err
	}
	effect, databaseNow, err := loadPushEffectWithClock(ctx, tx, scope)
	if err != nil {
		return nil, err
	}
	if err := validateStoredPushEffect(effect); err != nil {
		return nil, err
	}
	if effect.Status == pusheffect.StatusAmbiguous {
		if err := tx.Commit(ctx); err != nil {
			return nil, pushEffectDatabaseError("commit takeover replay", err)
		}
		return effect, nil
	}
	if effect.Status != pusheffect.StatusSending ||
		effect.TakeoverNotBefore == nil ||
		databaseNow.Before(*effect.TakeoverNotBefore) {
		return nil, pushEffectBusyOrTerminal(effect.Status)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE push_effects
		   SET status='ambiguous', lease_owner='', lease_until=NULL,
		       takeover_not_before=NULL, fence=fence+1, attempt=attempt+1,
		       failure_class='response_unknown',
		       ambiguous_since=clock_timestamp(), updated_at=clock_timestamp()
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND fence=$4
		   AND status='sending' AND takeover_not_before<=clock_timestamp()`,
		scope.ID, scope.TenantID, scope.UserID, effect.Fence,
	)
	if err != nil {
		return nil, pushEffectDatabaseError("take over stale push effect", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, pushEffectBusy()
	}
	effect, err = loadPushEffectForUpdate(ctx, tx, scope)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, pushEffectDatabaseError("commit takeover transaction", err)
	}
	return effect, nil
}

func (s *Store) RecordPushEffectDefiniteFailure(
	ctx context.Context,
	params pusheffect.FailureParams,
) error {
	if err := validatePushEffectFailure(params, true); err != nil {
		return err
	}
	return s.recordPushEffectFailure(ctx, params, pusheffect.StatusDefiniteFailed)
}

func (s *Store) RecordPushEffectAmbiguous(
	ctx context.Context,
	params pusheffect.FailureParams,
) error {
	if err := validatePushEffectFailure(params, false); err != nil {
		return err
	}
	return s.recordPushEffectFailure(ctx, params, pusheffect.StatusAmbiguous)
}

func (s *Store) recordPushEffectFailure(
	ctx context.Context,
	params pusheffect.FailureParams,
	status pusheffect.Status,
) error {
	tx, err := s.beginPushEffectCoordinatorTx(ctx, params.TenantID)
	if err != nil {
		return pushEffectDatabaseError("begin failure transaction", err)
	}
	defer rollbackPushEffectTx(ctx, tx)
	if err := lockPushEffectBatchForScope(ctx, tx, params.Scope); err != nil {
		return err
	}
	effect, databaseNow, err := loadPushEffectWithClock(ctx, tx, params.Scope)
	if err != nil {
		return err
	}
	if err := validateStoredPushEffect(effect); err != nil {
		return err
	}
	if effect.Status == status && effect.Fence == params.Fence &&
		effect.FailureClass == params.Class && effect.LeaseOwner == "" {
		if err := tx.Commit(ctx); err != nil {
			return pushEffectDatabaseError("commit failure replay", err)
		}
		return nil
	}
	if err := validateActivePushEffectLease(effect, params.Lease, databaseNow); err != nil {
		return err
	}
	var tag pgconn.CommandTag
	if status == pusheffect.StatusDefiniteFailed {
		tag, err = tx.Exec(ctx, `
			UPDATE push_effects
			   SET status='definite_failed', lease_owner='', lease_until=NULL,
			       takeover_not_before=NULL, failure_class=$6,
			       next_attempt_at=clock_timestamp()+($7*interval '1 microsecond'),
			       updated_at=clock_timestamp()
			 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
			   AND lease_owner=$4 AND fence=$5 AND status='sending'
			   AND lease_until>clock_timestamp()`,
			params.ID, params.TenantID, params.UserID, params.LeaseOwner,
			params.Fence, params.Class, params.RetryAfter.Microseconds())
	} else {
		tag, err = tx.Exec(ctx, `
			UPDATE push_effects
			   SET status='ambiguous', lease_owner='', lease_until=NULL,
			       takeover_not_before=NULL, failure_class=$6,
			       ambiguous_since=clock_timestamp(), updated_at=clock_timestamp()
			 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
			   AND lease_owner=$4 AND fence=$5 AND status='sending'
			   AND lease_until>clock_timestamp()`,
			params.ID, params.TenantID, params.UserID, params.LeaseOwner,
			params.Fence, params.Class)
	}
	if err != nil {
		return pushEffectDatabaseError("record push effect failure", err)
	}
	if tag.RowsAffected() != 1 {
		return pushEffectLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return pushEffectDatabaseError("commit failure transaction", err)
	}
	return nil
}

func (s *Store) RecordPushEffectSent(
	ctx context.Context,
	receipt pusheffect.SentReceipt,
) error {
	if err := validatePushEffectSentReceipt(receipt); err != nil {
		return err
	}
	tx, err := s.beginPushEffectReceiptTx(ctx, receipt.TenantID)
	if err != nil {
		return pushEffectDatabaseError("begin sent receipt transaction", err)
	}
	defer rollbackPushEffectTx(ctx, tx)
	if err := lockPushEffectBatchForScope(ctx, tx, receipt.Scope); err != nil {
		return err
	}
	var (
		status                        pusheffect.Status
		leaseOwner, providerMessageID string
		fence                         int64
		sentAt                        *time.Time
	)
	err = tx.QueryRow(ctx, `
		SELECT status, lease_owner, fence, provider_message_id, sent_at
		  FROM push_effects
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		 FOR UPDATE`,
		receipt.ID, receipt.TenantID, receipt.UserID,
	).Scan(&status, &leaseOwner, &fence, &providerMessageID, &sentAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return pushEffectNotFound()
	}
	if err != nil {
		return pushEffectDatabaseError("load sent receipt target", err)
	}
	if status == pusheffect.StatusSent {
		if fence == receipt.ExpectedFence &&
			providerMessageID == receipt.ProviderMessageID && sentAt != nil {
			if err := tx.Commit(ctx); err != nil {
				return pushEffectDatabaseError("commit sent receipt replay", err)
			}
			return nil
		}
		return pushEffectConflict("sent receipt differs")
	}
	if fence != receipt.ExpectedFence ||
		(status == pusheffect.StatusSending && leaseOwner != receipt.LeaseOwner) ||
		(status == pusheffect.StatusAmbiguous && receipt.LeaseOwner != "") ||
		(status != pusheffect.StatusSending && status != pusheffect.StatusAmbiguous) {
		return pushEffectLeaseLost()
	}
	tag, err := tx.Exec(ctx, `
		UPDATE push_effects
		   SET status='sent', lease_owner='', lease_until=NULL,
		       takeover_not_before=NULL, provider_message_id=$5,
		       failure_class='', ambiguous_since=NULL,
		       sent_at=clock_timestamp(), updated_at=clock_timestamp()
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND fence=$4
		   AND status IN ('sending','ambiguous')`,
		receipt.ID, receipt.TenantID, receipt.UserID,
		receipt.ExpectedFence, receipt.ProviderMessageID)
	if err != nil {
		return pushEffectDatabaseError("write sent receipt", err)
	}
	if tag.RowsAffected() != 1 {
		return pushEffectLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return pushEffectDatabaseError("commit sent receipt transaction", err)
	}
	return nil
}

func (s *Store) BlockPushEffect(
	ctx context.Context,
	resolution pusheffect.Resolution,
) error {
	if err := validatePushEffectResolution(resolution); err != nil {
		return err
	}
	tx, err := s.beginPushEffectOperatorTx(ctx, resolution.TenantID)
	if err != nil {
		return pushEffectDatabaseError("begin block transaction", err)
	}
	defer rollbackPushEffectTx(ctx, tx)
	if err := lockPushEffectBatchForScope(ctx, tx, resolution.Scope); err != nil {
		return err
	}
	var status pusheffect.Status
	var fence int64
	var class string
	var blockedAt *time.Time
	err = tx.QueryRow(ctx, `
		SELECT status, fence, failure_class, blocked_at
		  FROM push_effects
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		 FOR UPDATE`,
		resolution.ID, resolution.TenantID, resolution.UserID,
	).Scan(&status, &fence, &class, &blockedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return pushEffectNotFound()
	}
	if err != nil {
		return pushEffectDatabaseError("load block target", err)
	}
	if status == pusheffect.StatusBlocked {
		if fence == resolution.ExpectedFence && class == resolution.Class &&
			blockedAt != nil {
			if err := tx.Commit(ctx); err != nil {
				return pushEffectDatabaseError("commit block replay", err)
			}
			return nil
		}
		return pushEffectConflict("blocked resolution differs")
	}
	if status != pusheffect.StatusAmbiguous ||
		fence != resolution.ExpectedFence {
		return pushEffectLeaseLost()
	}
	tag, err := tx.Exec(ctx, `
		UPDATE push_effects
		   SET status='blocked', failure_class=$5,
		       blocked_at=clock_timestamp(), updated_at=clock_timestamp()
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		   AND fence=$4 AND status='ambiguous'`,
		resolution.ID, resolution.TenantID, resolution.UserID,
		resolution.ExpectedFence, resolution.Class)
	if err != nil {
		return pushEffectDatabaseError("block push effect", err)
	}
	if tag.RowsAffected() != 1 {
		return pushEffectLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return pushEffectDatabaseError("commit block transaction", err)
	}
	return nil
}

func verifyPushEffectAggregate(
	ctx context.Context,
	tx pgx.Tx,
	prepared pusheffect.Prepared,
) error {
	var snapshotFound bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM task_run_snapshots
			 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
			   AND task_id=$4 AND temporal_run_id=$5
		)`,
		prepared.RunSnapshotID, prepared.TenantID, prepared.UserID,
		prepared.TaskID, prepared.RunID,
	).Scan(&snapshotFound); err != nil {
		return pushEffectDatabaseError("verify run snapshot", err)
	}
	var batchFound bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM push_batches
			 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
			   AND run_snapshot_id=$4
		)`,
		prepared.BatchID, prepared.TenantID, prepared.UserID,
		prepared.RunSnapshotID,
	).Scan(&batchFound); err != nil {
		return pushEffectDatabaseError("verify push batch", err)
	}
	var deliveryCount int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM deliveries
		 WHERE tenant_id=$1 AND user_id=$2 AND batch_id=$3
		   AND id=ANY($4::bigint[])`,
		prepared.TenantID, prepared.UserID, prepared.BatchID,
		prepared.DeliveryIDs,
	).Scan(&deliveryCount); err != nil {
		return pushEffectDatabaseError("verify deliveries", err)
	}
	if !snapshotFound || !batchFound || deliveryCount != len(prepared.DeliveryIDs) {
		return pushEffectConflict("push effect aggregate provenance differs")
	}
	return nil
}

func lockPushEffectBatchForScope(
	ctx context.Context,
	tx pgx.Tx,
	scope pusheffect.Scope,
) error {
	var batchID, snapshotID int64
	err := tx.QueryRow(ctx, `
		SELECT batch_id,run_snapshot_id
		  FROM push_effects
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3`,
		scope.ID, scope.TenantID, scope.UserID,
	).Scan(&batchID, &snapshotID)
	if errors.Is(err, pgx.ErrNoRows) {
		return pushEffectNotFound()
	}
	if err != nil {
		return pushEffectDatabaseError("inspect push effect batch admission", err)
	}
	_, err = lockPushEffectBatchAdmission(
		ctx,
		tx,
		types.PushBatchScope{
			TenantID: scope.TenantID,
			UserID:   scope.UserID,
			BatchID:  batchID,
		},
		snapshotID,
		types.PushBatchDeliveryAuthorityEffect,
	)
	return err
}

func loadPushEffect(
	ctx context.Context,
	tx pgx.Tx,
	scope pusheffect.Scope,
	lock string,
) (*pusheffect.Effect, error) {
	effect, err := scanPushEffect(tx.QueryRow(ctx,
		`SELECT `+pushEffectColumns+`
		   FROM push_effects
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 `+lock,
		scope.ID, scope.TenantID, scope.UserID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, pushEffectNotFound()
	}
	if err != nil {
		return nil, pushEffectDatabaseError("load push effect", err)
	}
	return effect, nil
}

func loadPushEffectForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	scope pusheffect.Scope,
) (*pusheffect.Effect, error) {
	return loadPushEffect(ctx, tx, scope, "FOR UPDATE")
}

func loadPushEffectWithClock(
	ctx context.Context,
	tx pgx.Tx,
	scope pusheffect.Scope,
) (*pusheffect.Effect, time.Time, error) {
	effect, err := loadPushEffectForUpdate(ctx, tx, scope)
	if err != nil {
		return nil, time.Time{}, err
	}
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		return nil, time.Time{}, pushEffectDatabaseError("read database clock", err)
	}
	return effect, databaseNow, nil
}

func validatePushEffectReplay(
	stored *pusheffect.Effect,
	requested pusheffect.Canonical,
) error {
	if stored == nil ||
		!constantPushEffectDigestEqual(stored.PayloadDigest, requested.Digest()) ||
		!bytes.Equal(stored.CanonicalPayload, requested.Payload()) {
		return pushEffectConflict("push effect id has different canonical bytes")
	}
	return nil
}

func validateStoredPushEffect(effect *pusheffect.Effect) error {
	if effect == nil || !effect.Status.Valid() ||
		effect.SchemaVersion != pusheffect.SchemaVersion ||
		effect.Fence < 0 || effect.Attempt < 0 {
		return pushEffectIntegrity()
	}
	canonical, err := pusheffect.Decode(
		effect.CanonicalPayload, effect.PayloadDigest)
	if err != nil {
		return pushEffectIntegrity()
	}
	prepared := canonical.Prepared()
	if prepared.ID != effect.ID || prepared.TenantID != effect.TenantID ||
		prepared.UserID != effect.UserID || prepared.TaskID != effect.TaskID ||
		prepared.RunSnapshotID != effect.RunSnapshotID ||
		prepared.RunID != effect.RunID || prepared.StepID != effect.StepID ||
		prepared.ChunkIndex != effect.ChunkIndex ||
		prepared.ChunkCount != effect.ChunkCount ||
		prepared.BatchID != effect.BatchID ||
		!slices.Equal(prepared.DeliveryIDs, effect.DeliveryIDs) ||
		prepared.Provider != effect.Provider ||
		prepared.AppIdentity != effect.AppIdentity ||
		prepared.ProviderChatID != effect.ProviderChatID ||
		prepared.Target != effect.Target ||
		!bytes.Equal(prepared.Card, effect.Card) ||
		canonical.CardDigest() != effect.CardDigest ||
		prepared.ProviderUUID != effect.ProviderUUID ||
		!prepared.IdempotencyExpiresAt.Equal(effect.IdempotencyExpiresAt) {
		return pushEffectIntegrity()
	}
	return nil
}

func validateActivePushEffectLease(
	effect *pusheffect.Effect,
	lease pusheffect.Lease,
	databaseNow time.Time,
) error {
	if effect.Status != pusheffect.StatusSending ||
		effect.LeaseOwner != lease.LeaseOwner ||
		effect.Fence != lease.Fence || effect.LeaseUntil == nil ||
		!databaseNow.Before(*effect.LeaseUntil) {
		return pushEffectLeaseLost()
	}
	return nil
}

func validatePushEffectScope(scope pusheffect.Scope) error {
	if scope.ID == "" || scope.ID != strings.TrimSpace(scope.ID) ||
		len(scope.ID) > 512 || !utf8.ValidString(scope.ID) ||
		scope.TenantID <= 0 || scope.UserID <= 0 {
		return pushEffectValidation("push effect scope is invalid")
	}
	return nil
}

func validatePushEffectClaim(params pusheffect.ClaimParams) error {
	if err := validatePushEffectScope(params.Scope); err != nil {
		return err
	}
	if params.LeaseOwner == "" ||
		params.LeaseOwner != strings.TrimSpace(params.LeaseOwner) ||
		len(params.LeaseOwner) > maxPushEffectOwner ||
		!utf8.ValidString(params.LeaseOwner) ||
		params.LeaseDuration <= 0 || params.LeaseDuration > maxPushEffectLease ||
		params.LeaseDuration.Microseconds() <= 0 {
		return pushEffectValidation("push effect claim is invalid")
	}
	return nil
}

func validatePushEffectLease(lease pusheffect.Lease) error {
	if err := validatePushEffectScope(lease.Scope); err != nil {
		return err
	}
	if lease.LeaseOwner == "" ||
		lease.LeaseOwner != strings.TrimSpace(lease.LeaseOwner) ||
		len(lease.LeaseOwner) > maxPushEffectOwner ||
		!utf8.ValidString(lease.LeaseOwner) || lease.Fence <= 0 {
		return pushEffectValidation("push effect lease is invalid")
	}
	return nil
}

func validatePushEffectFailure(
	params pusheffect.FailureParams,
	definite bool,
) error {
	if err := validatePushEffectLease(params.Lease); err != nil {
		return err
	}
	if params.Class == "" || params.Class != strings.TrimSpace(params.Class) ||
		len(params.Class) > maxPushEffectFailure ||
		!utf8.ValidString(params.Class) {
		return pushEffectValidation("push effect failure class is invalid")
	}
	if definite {
		if params.RetryAfter <= 0 ||
			params.RetryAfter > maxPushEffectRetryWindow ||
			params.RetryAfter.Microseconds() <= 0 {
			return pushEffectValidation("push effect retry boundary is invalid")
		}
	} else if params.RetryAfter != 0 {
		return pushEffectValidation("ambiguous push effect cannot schedule a retry")
	}
	return nil
}

func validatePushEffectSentReceipt(receipt pusheffect.SentReceipt) error {
	if err := validatePushEffectScope(receipt.Scope); err != nil {
		return err
	}
	if receipt.ExpectedFence <= 0 ||
		(receipt.LeaseOwner != "" &&
			(receipt.LeaseOwner != strings.TrimSpace(receipt.LeaseOwner) ||
				len(receipt.LeaseOwner) > maxPushEffectOwner)) ||
		receipt.ProviderMessageID == "" ||
		receipt.ProviderMessageID != strings.TrimSpace(receipt.ProviderMessageID) ||
		len(receipt.ProviderMessageID) > maxPushEffectMessageID ||
		!utf8.ValidString(receipt.ProviderMessageID) {
		return pushEffectValidation("push effect sent receipt is invalid")
	}
	return nil
}

func validatePushEffectResolution(resolution pusheffect.Resolution) error {
	if err := validatePushEffectScope(resolution.Scope); err != nil {
		return err
	}
	if resolution.ExpectedFence <= 0 ||
		resolution.Class == "" ||
		resolution.Class != strings.TrimSpace(resolution.Class) ||
		len(resolution.Class) > maxPushEffectFailure ||
		!utf8.ValidString(resolution.Class) {
		return pushEffectValidation("push effect resolution is invalid")
	}
	return nil
}

func constantPushEffectDigestEqual(left, right string) bool {
	return len(left) == sha256.Size*2 && len(right) == sha256.Size*2 &&
		subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func pushEffectValidation(message string) error {
	return types.NewAppError(types.CodeValidation, message, nil)
}

func pushEffectConflict(message string) error {
	return types.NewAppError(types.CodeConflict, message, nil)
}

func pushEffectNotFound() error {
	return types.NewAppError(types.CodeNotFound, "push effect is unavailable", nil)
}

func pushEffectBusy() error {
	return types.NewAppError(types.CodeConflict, "push effect has an active owner", nil)
}

func pushEffectBusyOrTerminal(status pusheffect.Status) error {
	if status == pusheffect.StatusSending {
		return pushEffectBusy()
	}
	return pushEffectConflict("push effect state does not authorize sending")
}

func pushEffectLeaseLost() error {
	return types.NewAppError(types.CodeConflict,
		"push effect lease is no longer valid", nil)
}

func pushEffectIntegrity() error {
	return types.NewAppError(types.CodeInternal,
		"push effect integrity check failed", nil)
}

func pushEffectDatabaseError(action string, cause error) error {
	if errors.Is(cause, types.ErrNotFound) {
		return pushEffectNotFound()
	}
	var safeCause error
	switch {
	case cause == nil:
		safeCause = errors.New("database operation did not converge")
	case errors.Is(cause, context.Canceled),
		errors.Is(cause, context.DeadlineExceeded):
		safeCause = cause
	default:
		var pgErr *pgconn.PgError
		if errors.As(cause, &pgErr) {
			safeCause = fmt.Errorf("postgres sqlstate %s", pgErr.Code)
		} else {
			safeCause = errors.New("database operation failed")
		}
	}
	return types.NewAppError(types.CodeDatabase, "push effect: "+action, safeCause)
}
