package store

import (
	"context"
	"crypto/subtle"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/types"
)

// ClaimAuthorizedPushEffect atomically revalidates the exact immutable run
// snapshot plus current tenant/member/task authority while claiming a fresh
// provider-send lease. A live revocation preserves every provider/effect field
// and advances only the database-clock retry schedule.
func (s *Store) ClaimAuthorizedPushEffect(
	ctx context.Context,
	params pusheffect.AuthorizedClaimParams,
) (*pusheffect.Effect, pusheffect.AuthorizedClaimDecision, error) {
	return s.claimAuthorizedPushEffect(ctx, params, false)
}

// ClaimAuthorizedPushEffectReconciliation applies the same live authority gate
// to an exact UUID replay. The database-clock predicate additionally requires
// the complete lease to fit inside the frozen provider idempotency window.
func (s *Store) ClaimAuthorizedPushEffectReconciliation(
	ctx context.Context,
	params pusheffect.AuthorizedClaimParams,
) (*pusheffect.Effect, pusheffect.AuthorizedClaimDecision, error) {
	return s.claimAuthorizedPushEffect(ctx, params, true)
}

func (s *Store) claimAuthorizedPushEffect(
	ctx context.Context,
	params pusheffect.AuthorizedClaimParams,
	reconciliation bool,
) (*pusheffect.Effect, pusheffect.AuthorizedClaimDecision, error) {
	return s.claimAuthorizedPushEffectWithGate(ctx, params, reconciliation, nil)
}

type pushEffectClaimGate func(context.Context, pgx.Tx, *pusheffect.Effect) error

func (s *Store) claimAuthorizedPushEffectWithGate(
	ctx context.Context,
	params pusheffect.AuthorizedClaimParams,
	reconciliation bool,
	gate pushEffectClaimGate,
) (*pusheffect.Effect, pusheffect.AuthorizedClaimDecision, error) {
	if err := validatePushEffectClaim(params.ClaimParams); err != nil {
		return nil, "", err
	}
	if params.ExpectedTaskID == "" {
		return nil, "", pushEffectValidation(
			"authorized push effect task is invalid")
	}
	if params.DenialRetryAfter <= 0 ||
		params.DenialRetryAfter > maxPushEffectRetryWindow ||
		params.DenialRetryAfter.Microseconds() <= 0 {
		return nil, "", pushEffectValidation(
			"authorized push effect denial retry is invalid")
	}
	tx, err := s.beginPushEffectCoordinatorTx(ctx, params.TenantID)
	if err != nil {
		return nil, "", pushEffectDatabaseError(
			"begin authorized claim transaction", err)
	}
	defer rollbackPushEffectTx(ctx, tx)
	if err := lockPushEffectBatchForScope(ctx, tx, params.Scope); err != nil {
		return nil, "", err
	}
	effect, databaseNow, err := loadPushEffectWithClock(ctx, tx, params.Scope)
	if err != nil {
		return nil, "", err
	}
	if err := validateStoredPushEffect(effect); err != nil {
		return nil, "", err
	}
	if effect.TaskID != params.ExpectedTaskID {
		return nil, "", pushEffectConflict(
			"push effect is outside the enabled recovery task")
	}
	v3Identity, v3Snapshot, err := validatePushEffectRunSnapshotForClaim(ctx, tx, effect)
	if err != nil {
		return nil, "", err
	}
	// Generic fresh recovery and ambiguous reconciliation are V3 delivery
	// entry points too. They must cross the same exact cutover fence as the
	// workflow-owned first claim. Sent-receipt settlement never enters claim.
	if v3Identity != nil && v3Snapshot != nil {
		if _, err := RequireResearchV3DeliveryAuthorityForClaimTx(
			ctx, tx, *v3Identity, *v3Snapshot); err != nil {
			return nil, "", err
		}
	}
	if gate != nil {
		if err := gate(ctx, tx, effect); err != nil {
			return nil, "", err
		}
	}
	admitted, err := canonicalBriefPushRecoveryAdmittedV1(
		ctx, tx, effect)
	if err != nil {
		return nil, "", err
	}
	if !admitted {
		if !reconciliation {
			if _, err := tx.Exec(ctx, `
				UPDATE push_effects
				   SET next_attempt_at=clock_timestamp()+
				       ($5*interval '1 microsecond'),
				       updated_at=clock_timestamp()
				 WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND fence=$4
				   AND status IN ('prepared','definite_failed')`,
				effect.ID, effect.TenantID, effect.UserID, effect.Fence,
				params.DenialRetryAfter.Microseconds(),
			); err != nil {
				return nil, "", pushEffectDatabaseError(
					"defer canonical push effect recovery", err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, "", pushEffectDatabaseError(
				"commit canonical push effect recovery denial", err)
		}
		return nil, pusheffect.AuthorizedClaimDenied, nil
	}

	if effect.Status == pusheffect.StatusSending &&
		effect.LeaseOwner == params.LeaseOwner &&
		effect.LeaseUntil != nil &&
		databaseNow.Before(*effect.LeaseUntil) {
		replayed, authorized, err := loadAuthorizedPushEffectClaimReplay(
			ctx, tx, effect, params, reconciliation)
		if err != nil {
			return nil, "", err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, "", pushEffectDatabaseError(
				"commit authorized claim replay", err)
		}
		if !authorized {
			return nil, pusheffect.AuthorizedClaimDenied, nil
		}
		return replayed, pusheffect.AuthorizedClaimed, nil
	}

	if reconciliation {
		if effect.Status != pusheffect.StatusAmbiguous ||
			!databaseNow.Before(effect.IdempotencyExpiresAt) ||
			databaseNow.Add(params.LeaseDuration).After(
				effect.IdempotencyExpiresAt) {
			return nil, "", pushEffectConflict(
				"push effect reconciliation window is unavailable")
		}
		if databaseNow.Before(effect.NextAttemptAt) {
			if err := tx.Commit(ctx); err != nil {
				return nil, "", pushEffectDatabaseError(
					"commit reconciliation not-due decision", err)
			}
			return nil, pusheffect.AuthorizedClaimNotDue, nil
		}
	} else {
		if effect.Status != pusheffect.StatusPrepared &&
			effect.Status != pusheffect.StatusDefiniteFailed {
			return nil, "", pushEffectBusyOrTerminal(effect.Status)
		}
		if !databaseNow.Before(effect.IdempotencyExpiresAt) ||
			databaseNow.Add(params.LeaseDuration).After(
				effect.IdempotencyExpiresAt) {
			return nil, "", pushEffectConflict(
				"push effect provider window cannot contain the complete lease")
		}
		if databaseNow.Before(effect.NextAttemptAt) {
			if err := tx.Commit(ctx); err != nil {
				return nil, "", pushEffectDatabaseError(
					"commit authorized claim not-due decision", err)
			}
			return nil, pusheffect.AuthorizedClaimNotDue, nil
		}
	}

	claimed, authorized, err := updateAuthorizedPushEffectClaim(
		ctx, tx, effect, params, reconciliation)
	if err != nil {
		return nil, "", err
	}
	if !authorized {
		if err := tx.Commit(ctx); err != nil {
			return nil, "", pushEffectDatabaseError(
				"commit unauthorized claim", err)
		}
		return nil, pusheffect.AuthorizedClaimDenied, nil
	}
	if err := validateStoredPushEffect(claimed); err != nil {
		return nil, "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, "", pushEffectDatabaseError(
			"commit authorized claim transaction", err)
	}
	return claimed, pusheffect.AuthorizedClaimed, nil
}

func canonicalBriefPushRecoveryAdmittedV1(
	ctx context.Context,
	tx pgx.Tx,
	effect *pusheffect.Effect,
) (bool, error) {
	var referenceSchema string
	if err := tx.QueryRow(ctx, `
		SELECT reference_schema_version
		  FROM task_run_snapshots
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		   AND task_id=$4 AND temporal_run_id=$5`,
		effect.RunSnapshotID, effect.TenantID, effect.UserID,
		effect.TaskID, effect.RunID,
	).Scan(&referenceSchema); err != nil {
		return false, pushEffectDatabaseError(
			"load canonical recovery snapshot schema", err)
	}
	if referenceSchema == types.RunSnapshotSchemaVersionV2 {
		// Tool V2 does not produce a canonical Brief. Its exact delivery set is
		// frozen by Tool provenance plus the durable effect itself.
		return true, nil
	}
	if referenceSchema == types.ResearchRunSnapshotRefSchemaV3 {
		var briefID, planID int64
		var anchorReferenceDigest, anchorBriefDigest string
		err := tx.QueryRow(ctx, `
			SELECT brief_id,plan_id,brief_reference_digest,brief_digest
			  FROM research_brief_deliveries
			 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
			   AND run_snapshot_id=$4 AND batch_id=$5 AND effect_id=$6
			   AND status='prepared'`,
			effect.TenantID, effect.UserID, effect.TaskID,
			effect.RunSnapshotID, effect.BatchID, effect.ID).Scan(
			&briefID, &planID, &anchorReferenceDigest, &anchorBriefDigest)
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, pushEffectDatabaseError(
				"load research Brief push recovery anchor", err)
		}
		var briefRow ResearchBriefSynthesisV3
		var briefStatus string
		err = tx.QueryRow(ctx, `
			SELECT id,tenant_id,user_id,task_id,run_snapshot_id,plan_id,
			       temporal_workflow_id,temporal_run_id,definition_digest,plan_digest,
			       notification_threshold,request_digest,evidence_digest,history_digest,
			       status,significance,decision,delivery_required,brief_digest,finalized_at
			  FROM research_brief_syntheses
			  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND task_id=$4
			    AND run_snapshot_id=$5 AND plan_id=$6
			    AND temporal_run_id=$7 AND status='finalized'
			    AND delivery_required=true`,
			briefID, effect.TenantID, effect.UserID, effect.TaskID,
			effect.RunSnapshotID, planID, effect.RunID).Scan(
			&briefRow.ID, &briefRow.TenantID, &briefRow.UserID, &briefRow.TaskID,
			&briefRow.RunSnapshotID, &briefRow.PlanID, &briefRow.TemporalWorkflowID,
			&briefRow.TemporalRunID, &briefRow.DefinitionDigest, &briefRow.PlanDigest,
			&briefRow.NotificationThreshold, &briefRow.RequestDigest,
			&briefRow.EvidenceDigest, &briefRow.HistoryDigest, &briefStatus,
			&briefRow.Significance, &briefRow.Decision, &briefRow.DeliveryRequired,
			&briefRow.BriefDigest, &briefRow.FinalizedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, pushEffectDatabaseError(
				"load finalized research Brief push recovery evidence", err)
		}
		briefRow.Status = ResearchBriefSynthesisStatusV3(briefStatus)
		briefRef, err := researchBriefRefFromSynthesisV3(briefRow)
		if err != nil {
			return false, err
		}
		return briefRef.BriefID == briefID && briefRef.PlanID == planID &&
			briefRef.RunSnapshotID == effect.RunSnapshotID &&
			briefRef.BriefDigest == anchorBriefDigest &&
			subtle.ConstantTimeCompare([]byte(briefRef.ReferenceDigest),
				[]byte(anchorReferenceDigest)) == 1, nil
	}
	var available bool
	if err := tx.QueryRow(ctx, `
		SELECT to_regprocedure(
		    'public.canonical_brief_push_recovery_admitted_v1(bigint,bigint,text,bigint,bigint)'
		) IS NOT NULL`,
	).Scan(&available); err != nil {
		return false, pushEffectDatabaseError(
			"check canonical push recovery capability", err)
	}
	if !available {
		return true, nil
	}
	var admitted bool
	if err := tx.QueryRow(ctx, `
		SELECT public.canonical_brief_push_recovery_admitted_v1(
		    $1,$2,$3,$4,$5
		)`,
		effect.TenantID, effect.UserID, effect.TaskID,
		effect.RunSnapshotID, effect.BatchID,
	).Scan(&admitted); err != nil {
		return false, pushEffectDatabaseError(
			"check canonical push recovery admission", err)
	}
	return admitted, nil
}

func validatePushEffectRunSnapshotForClaim(
	ctx context.Context,
	tx pgx.Tx,
	effect *pusheffect.Effect,
) (*types.RunIdentity, *types.ResearchRunSnapshotRefV3, error) {
	var snapshot taskRunSnapshot
	var rawMode string
	err := tx.QueryRow(ctx, `
		SELECT `+pushEffectRunSnapshotReferenceColumns+`
		  FROM task_run_snapshots
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		   AND task_id=$4 AND temporal_run_id=$5`,
		effect.RunSnapshotID, effect.TenantID, effect.UserID,
		effect.TaskID, effect.RunID).Scan(
		&snapshot.ID, &snapshot.TenantID, &snapshot.UserID, &snapshot.TaskID,
		&snapshot.TemporalWorkflowID, &snapshot.TemporalRunID,
		&snapshot.RunKind, &rawMode, &snapshot.AdaptiveVersion,
		&snapshot.CapabilityCatalogDigest, &snapshot.ToolPolicyDigest,
		&snapshot.PromptPolicyDigest, &snapshot.ModelPolicyDigest,
		&snapshot.QuotaPolicyDigest, &snapshot.DefinitionDigest,
		&snapshot.PlanDigest, &snapshot.PayloadDigest,
		&snapshot.ReferenceDigest, &snapshot.ReferenceSchemaVersion,
		&snapshot.Payload, &snapshot.BudgetJSON, &snapshot.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, pushEffectIntegrity()
	}
	if err != nil {
		return nil, nil, pushEffectDatabaseError(
			"load authorized claim run snapshot", err)
	}
	mode, err := types.ParseExecutionMode(rawMode)
	if err != nil {
		return nil, nil, pushEffectIntegrity()
	}
	snapshot.Mode = mode
	var temporalWorkflowID string
	switch snapshot.ReferenceSchemaVersion {
	case types.RunSnapshotSchemaVersionV2:
		ref, err := snapshot.safeRefV2()
		if err != nil ||
			ref.SnapshotID != effect.RunSnapshotID ||
			ref.TenantID != effect.TenantID ||
			ref.UserID != effect.UserID ||
			ref.TaskID != effect.TaskID ||
			ref.TemporalRunID != effect.RunID {
			return nil, nil, pushEffectIntegrity()
		}
		temporalWorkflowID = ref.TemporalWorkflowID
	case types.ResearchRunSnapshotRefSchemaV3:
		identity := types.RunIdentity{
			TemporalWorkflowID: snapshot.TemporalWorkflowID,
			TemporalRunID:      snapshot.TemporalRunID,
			RunKind:            snapshot.RunKind,
			TenantID:           snapshot.TenantID,
			UserID:             snapshot.UserID,
			TaskID:             snapshot.TaskID,
		}
		fullSnapshot, found, err := loadResearchRunSnapshotRowV3(ctx, tx,
			CreateOrGetTaskRunSnapshotParams{
				TenantID: identity.TenantID, UserID: identity.UserID,
				TaskID: identity.TaskID, TemporalWorkflowID: identity.TemporalWorkflowID,
				TemporalRunID: identity.TemporalRunID,
			})
		if err != nil {
			return nil, nil, err
		}
		ref, err := validateStoredResearchRunSnapshotV3(identity, fullSnapshot)
		if !found || err != nil || ref.SnapshotID != effect.RunSnapshotID ||
			ref.TenantID != effect.TenantID || ref.UserID != effect.UserID ||
			ref.TaskID != effect.TaskID || ref.TemporalRunID != effect.RunID {
			return nil, nil, pushEffectIntegrity()
		}
		// V3 Workflow IDs are independently sealed by the research capability
		// and do not use the legacy scheduled/manual naming grammar.
		return &identity, &ref, nil
	default:
		ref, err := snapshot.safeRef()
		if err != nil ||
			ref.SnapshotID != effect.RunSnapshotID ||
			ref.TenantID != effect.TenantID ||
			ref.UserID != effect.UserID ||
			ref.TaskID != effect.TaskID ||
			ref.TemporalRunID != effect.RunID {
			return nil, nil, pushEffectIntegrity()
		}
		temporalWorkflowID = ref.TemporalWorkflowID
	}
	if temporalWorkflowID == scheduledTaskWorkflowID(effect.TaskID) {
		return nil, nil, nil
	}
	if !validTaskRunWorkflowExecutionIDV1(
		effect.TaskID, temporalWorkflowID,
	) {
		return nil, nil, pushEffectIntegrity()
	}
	// Temporal Schedule appends the nominal UTC time to ordinary executions.
	// The sealed snapshot and exact effect coordinates provide the authority;
	// no separate repair-era workflow allowlist is needed.
	return nil, nil, nil
}

const pushEffectRunSnapshotReferenceColumns = `id, tenant_id, user_id, task_id,
	temporal_workflow_id, temporal_run_id, run_kind, execution_mode,
	adaptive_version, capability_catalog_digest, tool_policy_digest,
	prompt_policy_digest, model_policy_digest, quota_policy_digest,
	definition_digest, plan_digest, payload_digest, reference_digest,
	reference_schema_version, payload, budget, created_at`

func loadAuthorizedPushEffectClaimReplay(
	ctx context.Context,
	tx pgx.Tx,
	effect *pusheffect.Effect,
	params pusheffect.AuthorizedClaimParams,
	reconciliation bool,
) (*pusheffect.Effect, bool, error) {
	windowPredicate := ""
	if reconciliation {
		windowPredicate = `
		   AND clock_timestamp()+($6*interval '1 microsecond')
		       <=e.idempotency_expires_at`
	} else {
		// A same-owner replay returns the already-held lease rather than
		// extending it. Reject legacy/invalid sending rows whose complete
		// lease crosses the frozen provider UUID window.
		windowPredicate = `
		   AND $6::bigint>0
		   AND e.lease_until<=e.idempotency_expires_at`
	}
	row := tx.QueryRow(ctx, `
		SELECT `+pushEffectColumns+`
		  FROM push_effects e
		 WHERE e.id=$1 AND e.tenant_id=$2 AND e.user_id=$3
		   AND e.status='sending' AND e.lease_owner=$4 AND e.fence=$5
		   AND e.lease_until>clock_timestamp()`+windowPredicate+`
		   AND $7::bigint>0
		   AND `+authorizedPushEffectRunPredicate(),
		params.ID, params.TenantID, params.UserID, params.LeaseOwner,
		effect.Fence, params.LeaseDuration.Microseconds(),
		params.DenialRetryAfter.Microseconds(),
		types.ScheduleStatusActive, types.TenantStatusActive,
		params.ExpectedTaskID)
	replayed, err := scanPushEffect(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, pushEffectScanError(
			"revalidate authorized claim replay", err)
	}
	if err := validateStoredPushEffect(replayed); err != nil {
		return nil, false, err
	}
	return replayed, true, nil
}

func updateAuthorizedPushEffectClaim(
	ctx context.Context,
	tx pgx.Tx,
	effect *pusheffect.Effect,
	params pusheffect.AuthorizedClaimParams,
	reconciliation bool,
) (*pusheffect.Effect, bool, error) {
	if !reconciliation {
		return updateFreshAuthorizedPushEffectClaim(
			ctx, tx, effect, params)
	}
	statusPredicate := `
		   AND e.status='ambiguous'
		   AND e.next_attempt_at<=clock_timestamp()
		   AND clock_timestamp()+($6*interval '1 microsecond')
		       <=e.idempotency_expires_at`
	row := tx.QueryRow(ctx, `
		UPDATE push_effects e
		   SET status='sending', lease_owner=$4,
		       lease_until=clock_timestamp()+($6*interval '1 microsecond'),
		       takeover_not_before=clock_timestamp()+($7*interval '1 microsecond'),
		       fence=e.fence+1, attempt=e.attempt+1,
		       failure_class='', ambiguous_since=NULL,
		       updated_at=clock_timestamp()
		 WHERE e.id=$1 AND e.tenant_id=$2 AND e.user_id=$3 AND e.fence=$5
		   AND e.lease_owner='' AND e.lease_until IS NULL`+
		statusPredicate+`
		   AND `+authorizedPushEffectRunPredicate()+`
		 RETURNING `+pushEffectColumns,
		params.ID, params.TenantID, params.UserID, params.LeaseOwner,
		effect.Fence, params.LeaseDuration.Microseconds(),
		(params.LeaseDuration + pushEffectTakeoverGrace).Microseconds(),
		types.ScheduleStatusActive, types.TenantStatusActive,
		params.ExpectedTaskID)
	claimed, err := scanPushEffect(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, pushEffectScanError(
			"claim authorized push effect reconciliation", err)
	}
	return claimed, true, nil
}

func updateFreshAuthorizedPushEffectClaim(
	ctx context.Context,
	tx pgx.Tx,
	effect *pusheffect.Effect,
	params pusheffect.AuthorizedClaimParams,
) (*pusheffect.Effect, bool, error) {
	var authorized bool
	err := tx.QueryRow(ctx, `
		WITH database_clock AS MATERIALIZED (
			SELECT clock_timestamp() AS database_now
		), decision AS (
			SELECT (
				`+authorizedPushEffectRunPredicate()+`
				AND database_clock.database_now+
				    ($6*interval '1 microsecond')<=e.idempotency_expires_at
			) AS authorized,
			database_clock.database_now
			  FROM push_effects e
			  CROSS JOIN database_clock
			 WHERE e.id=$1 AND e.tenant_id=$2 AND e.user_id=$3
			   AND e.fence=$5
		)
		UPDATE push_effects e
		   SET status=CASE WHEN decision.authorized THEN 'sending'
		                   ELSE e.status END,
		       lease_owner=CASE WHEN decision.authorized THEN $4
		                        ELSE e.lease_owner END,
		       lease_until=CASE WHEN decision.authorized
		                   THEN decision.database_now+
		                        ($6*interval '1 microsecond')
		                   ELSE NULL END,
		       takeover_not_before=CASE WHEN decision.authorized
		                   THEN decision.database_now+
		                        ($7*interval '1 microsecond')
		                   ELSE NULL END,
		       fence=e.fence+CASE WHEN decision.authorized THEN 1 ELSE 0 END,
		       attempt=e.attempt+CASE WHEN decision.authorized THEN 1 ELSE 0 END,
		       next_attempt_at=CASE WHEN decision.authorized
		                   THEN e.next_attempt_at
		                   ELSE decision.database_now+
		                        ($11*interval '1 microsecond') END,
		       failure_class=CASE WHEN decision.authorized THEN ''
		                          ELSE e.failure_class END,
		       ambiguous_since=CASE WHEN decision.authorized THEN NULL
		                            ELSE e.ambiguous_since END,
		       updated_at=decision.database_now
		  FROM decision
		 WHERE e.id=$1 AND e.tenant_id=$2 AND e.user_id=$3 AND e.fence=$5
		   AND e.task_id=$10
		   AND e.status IN ('prepared','definite_failed')
		   AND e.next_attempt_at<=clock_timestamp()
		   AND e.lease_owner='' AND e.lease_until IS NULL
		 RETURNING decision.authorized`,
		params.ID, params.TenantID, params.UserID, params.LeaseOwner,
		effect.Fence, params.LeaseDuration.Microseconds(),
		(params.LeaseDuration + pushEffectTakeoverGrace).Microseconds(),
		types.ScheduleStatusActive, types.TenantStatusActive,
		params.ExpectedTaskID,
		params.DenialRetryAfter.Microseconds(),
	).Scan(&authorized)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, pushEffectBusy()
	}
	if err != nil {
		return nil, false, pushEffectDatabaseError(
			"claim or defer unauthorized push effect", err)
	}
	if !authorized {
		return nil, false, nil
	}
	claimed, err := loadPushEffectForUpdate(ctx, tx, params.Scope)
	if err != nil {
		return nil, false, err
	}
	return claimed, true, nil
}

// authorizedPushEffectRunPredicate is evaluated by the same SQL statement that
// mutates the effect. It deliberately repeats every immutable coordinate and
// current live-state predicate; a stale preflight cannot authorize the UPDATE.
func authorizedPushEffectRunPredicate() string {
	return `EXISTS (
	SELECT 1
	  FROM task_run_snapshots r
	  JOIN schedules s
	    ON s.id=r.task_id AND s.tenant_id=r.tenant_id AND s.user_id=r.user_id
	  JOIN tenants t ON t.id=r.tenant_id
	  JOIN memberships m
	    ON m.tenant_id=r.tenant_id AND m.user_id=r.user_id
	 WHERE r.id=e.run_snapshot_id
	   AND e.task_id=$10
	   AND r.tenant_id=e.tenant_id
	   AND r.user_id=e.user_id
	   AND r.task_id=e.task_id
	   AND r.temporal_run_id=e.run_id
	   AND (
	     s.status=$8 OR (
	       s.status='paused' AND authorize_manual_task_run_v1(
	         r.tenant_id, r.user_id, r.task_id,
	         r.temporal_workflow_id
	       )
	     )
	   )
	   AND t.status=$9 AND t.deleted_at IS NULL
	   AND NOT EXISTS (
	       SELECT 1
	         FROM task_creation_operations p
	        WHERE p.task_id=s.id
	          AND p.tenant_id=s.tenant_id AND p.user_id=s.user_id
	          AND p.tool_name='create_schedule' AND p.execution_version=1
	          AND NOT (p.status='executed' AND p.phase='completed')
	   )
)`
}
