package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/types"
)

func TestClaimAuthorizedPushEffectHonorsCanonicalTerminalAdmission(t *testing.T) {
	f, effect := authorizedPushEffectFixture(t)
	payload := []byte("{}")
	sum := sha256.Sum256(payload)
	requestDigest := hex.EncodeToString(sum[:])
	var outcomeID int64
	tx, err := f.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if _, err := tx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		fmt.Sprint(effect.TenantID), fmt.Sprint(effect.UserID)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		t.Context(), `SET LOCAL ROLE vane_brief_writer`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(),
		`UPDATE push_batches SET brief_state='sealed' WHERE id=$1`,
		effect.BatchID); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(t.Context(), `
		INSERT INTO task_run_outcomes (
		    tenant_id,user_id,task_id,run_snapshot_id,schema_version
		) VALUES ($1,$2,$3,$4,$5)
		RETURNING id`,
		effect.TenantID, effect.UserID, effect.TaskID,
		effect.RunSnapshotID, types.RunOutcomeSchemaVersionV1,
	).Scan(&outcomeID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), `
		INSERT INTO canonical_brief_stages (
		    run_outcome_id,tenant_id,user_id,task_id,run_snapshot_id,
		    push_batch_id,schema_version,request_digest,payload,
		    insight_count,generated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,1,clock_timestamp())`,
		outcomeID, effect.TenantID, effect.UserID, effect.TaskID,
		effect.RunSnapshotID, effect.BatchID, types.BriefSchemaVersionV1,
		requestDigest, payload,
	); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, f.st,
			`DELETE FROM canonical_brief_stages WHERE run_outcome_id=$1`,
			outcomeID)
		cleanupExec(cleanupCtx, t, f.st,
			`DELETE FROM task_run_outcomes WHERE id=$1`, outcomeID)
	})
	params := pusheffect.AuthorizedClaimParams{
		ClaimParams: pusheffect.ClaimParams{
			Scope: effect.Scope(), LeaseOwner: "canonical-pending-denied",
			LeaseDuration: time.Minute,
		},
		ExpectedTaskID: effect.TaskID, DenialRetryAfter: time.Minute,
	}
	claimed, decision, err := f.st.ClaimAuthorizedPushEffect(
		t.Context(), params)
	if err != nil || claimed != nil ||
		decision != pusheffect.AuthorizedClaimDenied {
		t.Fatalf("staged claim=%+v decision=%q err=%v",
			claimed, decision, err)
	}

	tx, err = f.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if _, err := tx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		fmt.Sprint(effect.TenantID), fmt.Sprint(effect.UserID)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		t.Context(), `SET LOCAL ROLE vane_brief_writer`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), `
		WITH finalized AS (
		    UPDATE task_run_outcomes
		       SET status='finalized',result='interrupted',
		           source_coverage='partial',processing='partial',
		           failure_code='workflow_terminated',
		           failure_message='workflow was terminated',
		           finalized_at=clock_timestamp(),
		           outcome_digest=$2
		     WHERE id=$1
		     RETURNING finalized_at
		)
		UPDATE canonical_brief_stages
		   SET status='aborted',
		       resolved_at=(SELECT finalized_at FROM finalized)
		 WHERE run_outcome_id=$1`,
		outcomeID, strings.Repeat("0", 64),
	); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE push_effects SET next_attempt_at=clock_timestamp()
		  WHERE id=$1`,
		effect.ID); err != nil {
		t.Fatal(err)
	}
	params.LeaseOwner = "canonical-aborted-denied"
	claimed, decision, err = f.st.ClaimAuthorizedPushEffect(
		t.Context(), params)
	if err != nil || claimed != nil ||
		decision != pusheffect.AuthorizedClaimDenied {
		t.Fatalf("aborted claim=%+v decision=%q err=%v",
			claimed, decision, err)
	}
}

func TestClaimAuthorizedPushEffectAllowsPromotedCanonicalContent(t *testing.T) {
	f, effect := authorizedPushEffectFixture(t)
	payload := []byte("{}")
	sum := sha256.Sum256(payload)
	requestDigest := hex.EncodeToString(sum[:])
	generatedAt := time.Date(2026, 7, 27, 10, 11, 12, 0, time.UTC)
	var outcomeID int64
	tx, err := f.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if _, err := tx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		fmt.Sprint(effect.TenantID), fmt.Sprint(effect.UserID)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		t.Context(), `SET LOCAL ROLE vane_brief_writer`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(),
		`UPDATE push_batches SET brief_state='sealed' WHERE id=$1`,
		effect.BatchID); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(t.Context(), `
		INSERT INTO task_run_outcomes (
		    tenant_id,user_id,task_id,run_snapshot_id,schema_version
		) VALUES ($1,$2,$3,$4,$5)
		RETURNING id`,
		effect.TenantID, effect.UserID, effect.TaskID,
		effect.RunSnapshotID, types.RunOutcomeSchemaVersionV1,
	).Scan(&outcomeID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), `
		INSERT INTO canonical_brief_stages (
		    run_outcome_id,tenant_id,user_id,task_id,run_snapshot_id,
		    push_batch_id,schema_version,request_digest,payload,
		    insight_count,generated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,1,$10)`,
		outcomeID, effect.TenantID, effect.UserID, effect.TaskID,
		effect.RunSnapshotID, effect.BatchID, types.BriefSchemaVersionV1,
		requestDigest, payload, generatedAt,
	); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	var briefID int64
	finalizedAt := time.Date(2026, 7, 27, 10, 12, 13, 0, time.UTC)
	tx, err = f.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if _, err := tx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		fmt.Sprint(effect.TenantID), fmt.Sprint(effect.UserID)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		t.Context(), `SET LOCAL ROLE vane_brief_writer`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), `
		UPDATE task_run_outcomes
		   SET status='finalized',result='content',
		       source_coverage='partial',processing='partial',
		       finalized_at=$2,outcome_digest=$3
		 WHERE id=$1`,
		outcomeID, finalizedAt, strings.Repeat("1", 64),
	); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(t.Context(), `
		INSERT INTO brief_snapshots (
		    id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id,
		    push_batch_id,schema_version,request_digest,payload_digest,
		    payload,insight_count,generated_at
		) VALUES (
		    nextval('brief_snapshots_id_seq'),$1,$2,$3,$4,$5,$6,$7,
		    $8,$9,$10,1,$11
		) RETURNING id`,
		effect.TenantID, effect.UserID, effect.TaskID, outcomeID,
		effect.RunSnapshotID, effect.BatchID, types.BriefSchemaVersionV1,
		requestDigest, strings.Repeat("2", 64), payload, generatedAt,
	).Scan(&briefID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), `
		UPDATE canonical_brief_stages
		   SET status='promoted',brief_snapshot_id=$2,resolved_at=$3
		 WHERE run_outcome_id=$1`,
		outcomeID, briefID, finalizedAt,
	); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, f.st,
			`DELETE FROM canonical_brief_stages WHERE run_outcome_id=$1`,
			outcomeID)
		cleanupExec(cleanupCtx, t, f.st,
			`DELETE FROM brief_snapshots WHERE id=$1`, briefID)
		cleanupExec(cleanupCtx, t, f.st,
			`DELETE FROM task_run_outcomes WHERE id=$1`, outcomeID)
	})
	claimed, decision, err := f.st.ClaimAuthorizedPushEffect(
		t.Context(),
		pusheffect.AuthorizedClaimParams{
			ClaimParams: pusheffect.ClaimParams{
				Scope: effect.Scope(), LeaseOwner: "canonical-promoted",
				LeaseDuration: time.Minute,
			},
			ExpectedTaskID: effect.TaskID, DenialRetryAfter: time.Minute,
		},
	)
	if err != nil || decision != pusheffect.AuthorizedClaimed ||
		claimed == nil || claimed.Status != pusheffect.StatusSending {
		t.Fatalf("promoted claim=%+v decision=%q err=%v",
			claimed, decision, err)
	}
}

func TestClaimAuthorizedPushEffectIsExactAndBacksOffLiveDenial(t *testing.T) {
	t.Run("authorized exact task claims", func(t *testing.T) {
		f, effect := authorizedPushEffectFixture(t)
		claimed, decision, err := f.st.ClaimAuthorizedPushEffect(
			t.Context(),
			pusheffect.AuthorizedClaimParams{
				ClaimParams: pusheffect.ClaimParams{
					Scope: effect.Scope(), LeaseOwner: "authorized-test",
					LeaseDuration: time.Minute,
				},
				ExpectedTaskID:   effect.TaskID,
				DenialRetryAfter: time.Minute,
			},
		)
		if err != nil || decision != pusheffect.AuthorizedClaimed ||
			claimed == nil || claimed.Status != pusheffect.StatusSending {
			t.Fatalf("claim=%+v decision=%q err=%v", claimed, decision, err)
		}
		if len(claimed.ObservationEventKeys) != 1 ||
			claimed.ObservationEventKeys[0] != effect.ObservationEventKeys[0] {
			t.Fatalf("claim did not hydrate canonical event keys: %+v", claimed)
		}
	})

	t.Run("membership revocation advances only database retry clock", func(t *testing.T) {
		f, effect := authorizedPushEffectFixture(t)
		if _, err := f.st.pool.Exec(t.Context(), `
			DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`,
			effect.TenantID, effect.UserID,
		); err != nil {
			t.Fatal(err)
		}
		var before time.Time
		if err := f.st.pool.QueryRow(
			t.Context(), `SELECT clock_timestamp()`,
		).Scan(&before); err != nil {
			t.Fatal(err)
		}
		const retryAfter = 2 * time.Minute
		claimed, decision, err := f.st.ClaimAuthorizedPushEffect(
			t.Context(),
			pusheffect.AuthorizedClaimParams{
				ClaimParams: pusheffect.ClaimParams{
					Scope: effect.Scope(), LeaseOwner: "denied-test",
					LeaseDuration: time.Minute,
				},
				ExpectedTaskID: effect.TaskID, DenialRetryAfter: retryAfter,
			},
		)
		if err != nil || claimed != nil ||
			decision != pusheffect.AuthorizedClaimDenied {
			t.Fatalf("denied claim=%+v decision=%q err=%v", claimed, decision, err)
		}
		var (
			status, failure string
			fence           int64
			attempt         int
			next, after     time.Time
		)
		if err := f.st.pool.QueryRow(t.Context(), `
			SELECT status,failure_class,fence,attempt,next_attempt_at,
			       clock_timestamp()
			  FROM push_effects WHERE id=$1`,
			effect.ID,
		).Scan(&status, &failure, &fence, &attempt, &next, &after); err != nil {
			t.Fatal(err)
		}
		if status != string(pusheffect.StatusPrepared) ||
			failure != "" || fence != 0 || attempt != 0 {
			t.Fatalf(
				"denial mutated provider state=%q/%q fence=%d attempt=%d",
				status, failure, fence, attempt,
			)
		}
		if next.Before(before.Add(retryAfter)) ||
			next.After(after.Add(retryAfter)) {
			t.Fatalf("denial retry=%s outside DB bounds", next)
		}
	})

	t.Run("tenant suspension denies with durable backoff", func(t *testing.T) {
		f, effect := authorizedPushEffectFixture(t)
		if _, err := f.st.pool.Exec(t.Context(), `
			UPDATE tenants SET status=$2 WHERE id=$1`,
			effect.TenantID, types.TenantStatusSuspended,
		); err != nil {
			t.Fatal(err)
		}
		assertAuthorizedPushEffectDeniedWithBackoff(t, f, effect)
	})

	t.Run("task pause denies with durable backoff", func(t *testing.T) {
		f, effect := authorizedPushEffectFixture(t)
		if _, err := f.st.pool.Exec(t.Context(), `
			UPDATE schedules SET status='paused' WHERE id=$1`,
			effect.TaskID,
		); err != nil {
			t.Fatal(err)
		}
		assertAuthorizedPushEffectDeniedWithBackoff(t, f, effect)
	})

	t.Run("incomplete creation action denies with durable backoff", func(t *testing.T) {
		f, effect := authorizedPushEffectFixture(t)
		if _, err := f.st.pool.Exec(t.Context(), `
			INSERT INTO pending_actions (
			    id,tenant_id,user_id,tool_name,args,summary,status,expires_at,
			    execution_version,phase,task_id
			) VALUES (
			    $1,$2,$3,'create_schedule','{}','authority fence',
			    'pending',clock_timestamp()+interval '1 hour',1,'prepared',$4
			)`,
			uuid.NewString(), effect.TenantID, effect.UserID, effect.TaskID,
		); err != nil {
			t.Fatal(err)
		}
		assertAuthorizedPushEffectDeniedWithBackoff(t, f, effect)
	})

	t.Run("sub-microsecond denial retry is rejected", func(t *testing.T) {
		f, effect := authorizedPushEffectFixture(t)
		_, _, err := f.st.ClaimAuthorizedPushEffect(
			t.Context(),
			pusheffect.AuthorizedClaimParams{
				ClaimParams: pusheffect.ClaimParams{
					Scope: effect.Scope(), LeaseOwner: "tiny-retry",
					LeaseDuration: time.Minute,
				},
				ExpectedTaskID: effect.TaskID, DenialRetryAfter: time.Nanosecond,
			},
		)
		if err == nil {
			t.Fatal("sub-microsecond denial retry was accepted")
		}
	})

	t.Run("cross scope and wrong exact task never mutate", func(t *testing.T) {
		f, effect := authorizedPushEffectFixture(t)
		params := pusheffect.AuthorizedClaimParams{
			ClaimParams: pusheffect.ClaimParams{
				Scope: effect.Scope(), LeaseOwner: "wrong-scope",
				LeaseDuration: time.Minute,
			},
			ExpectedTaskID: "another-task", DenialRetryAfter: time.Minute,
		}
		if _, _, err := f.st.ClaimAuthorizedPushEffect(
			t.Context(), params); err == nil {
			t.Fatal("wrong exact task was accepted")
		}
		params.ExpectedTaskID = effect.TaskID
		params.TenantID++
		if _, _, err := f.st.ClaimAuthorizedPushEffect(
			t.Context(), params); err == nil {
			t.Fatal("cross-tenant scope was accepted")
		}
		params.TenantID--
		params.UserID++
		if _, _, err := f.st.ClaimAuthorizedPushEffect(
			t.Context(), params); err == nil {
			t.Fatal("cross-user scope was accepted")
		}
		loaded, err := f.st.LoadPushEffect(t.Context(), effect.Scope())
		if err != nil {
			t.Fatal(err)
		}
		if loaded.Status != pusheffect.StatusPrepared ||
			loaded.Fence != 0 || loaded.Attempt != 0 {
			t.Fatalf("rejected scope mutated effect: %+v", loaded)
		}
	})
}

func assertAuthorizedPushEffectDeniedWithBackoff(
	t *testing.T,
	f *taskRunSnapshotFixture,
	effect *pusheffect.Effect,
) {
	t.Helper()
	var before time.Time
	if err := f.st.pool.QueryRow(
		t.Context(), `SELECT clock_timestamp()`,
	).Scan(&before); err != nil {
		t.Fatal(err)
	}
	const retryAfter = 2 * time.Minute
	claimed, decision, err := f.st.ClaimAuthorizedPushEffect(
		t.Context(),
		pusheffect.AuthorizedClaimParams{
			ClaimParams: pusheffect.ClaimParams{
				Scope: effect.Scope(), LeaseOwner: "live-denial",
				LeaseDuration: 10 * time.Minute,
			},
			ExpectedTaskID: effect.TaskID, DenialRetryAfter: retryAfter,
		},
	)
	if err != nil || claimed != nil ||
		decision != pusheffect.AuthorizedClaimDenied {
		t.Fatalf("denied claim=%+v decision=%q err=%v", claimed, decision, err)
	}
	var (
		status, failure string
		fence           int64
		attempt         int
		next, after     time.Time
	)
	if err := f.st.pool.QueryRow(t.Context(), `
		SELECT status,failure_class,fence,attempt,next_attempt_at,
		       clock_timestamp()
		  FROM push_effects WHERE id=$1`,
		effect.ID,
	).Scan(&status, &failure, &fence, &attempt, &next, &after); err != nil {
		t.Fatal(err)
	}
	if status != string(pusheffect.StatusPrepared) ||
		failure != "" || fence != 0 || attempt != 0 {
		t.Fatalf(
			"denial mutated provider state=%q/%q fence=%d attempt=%d",
			status, failure, fence, attempt,
		)
	}
	if next.Before(before.Add(retryAfter)) ||
		next.After(after.Add(retryAfter)) {
		t.Fatalf("denial retry=%s outside DB bounds", next)
	}
}

func TestClaimAuthorizedPushEffectHasOneConcurrentWinner(t *testing.T) {
	f, effect := authorizedPushEffectFixture(t)
	type result struct {
		decision pusheffect.AuthorizedClaimDecision
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var workers sync.WaitGroup
	for worker := range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			_, decision, err := f.st.ClaimAuthorizedPushEffect(
				t.Context(),
				pusheffect.AuthorizedClaimParams{
					ClaimParams: pusheffect.ClaimParams{
						Scope: effect.Scope(),
						LeaseOwner: "concurrent-authority-" +
							string(rune('a'+worker)),
						LeaseDuration: 10 * time.Minute,
					},
					ExpectedTaskID:   effect.TaskID,
					DenialRetryAfter: time.Minute,
				},
			)
			results <- result{decision: decision, err: err}
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	winners := 0
	losers := 0
	for got := range results {
		if got.err == nil && got.decision == pusheffect.AuthorizedClaimed {
			winners++
		} else if got.err != nil {
			losers++
		} else {
			t.Fatalf("unexpected concurrent decision=%q/%v", got.decision, got.err)
		}
	}
	if winners != 1 || losers != 1 {
		t.Fatalf("concurrent winners=%d losers=%d", winners, losers)
	}
}

func TestClaimAuthorizedPushEffectAllowsOnlyFinalizedLegacyBatch63Workflow(
	t *testing.T,
) {
	f := newLegacyBatch63Fixture(t)
	plan, err := f.st.PreviewLegacyBatch63Repair(
		t.Context(), f.evidence, f.expiresAt, f.buildCard)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.FinalizeLegacyBatch63Repair(
		t.Context(), plan.PlanDigest, f.evidence, f.expiresAt, f.buildCard,
	); err != nil {
		t.Fatal(err)
	}
	scope := pusheffect.Scope{
		ID: legacyBatch63EffectID, TenantID: plan.Prepared.TenantID,
		UserID: plan.Prepared.UserID,
	}
	claimed, decision, err := f.st.ClaimAuthorizedPushEffect(
		t.Context(),
		pusheffect.AuthorizedClaimParams{
			ClaimParams: pusheffect.ClaimParams{
				Scope: scope, LeaseOwner: "legacy-batch63-dated-workflow",
				LeaseDuration: time.Minute,
			},
			ExpectedTaskID: legacyBatch63TaskID, DenialRetryAfter: time.Minute,
		},
	)
	if err != nil || decision != pusheffect.AuthorizedClaimed ||
		claimed == nil || claimed.Status != pusheffect.StatusSending ||
		claimed.TaskID != legacyBatch63TaskID ||
		claimed.RunID != legacyBatch63RunID {
		t.Fatalf(
			"legacy dated claim=%+v decision=%q err=%v",
			claimed, decision, err,
		)
	}
}

func TestClaimAuthorizedPushEffectRejectsDatedWorkflowWithoutExactRepairAudit(
	t *testing.T,
) {
	f, effect := authorizedPushEffectFixtureForWorkflow(
		t, "dated")
	_, decision, err := f.st.ClaimAuthorizedPushEffect(
		t.Context(),
		authorizedPushEffectClaimParams(effect, "ordinary-dated-workflow"),
	)
	if err == nil || decision != "" {
		t.Fatalf("ordinary dated workflow decision=%q err=%v", decision, err)
	}
	assertPushEffectProviderStateUnclaimed(t, f, effect)
}

func TestClaimAuthorizedPushEffectRejectsDriftedLegacyBatch63Evidence(
	t *testing.T,
) {
	tests := []struct {
		name   string
		mutate func(*testing.T, legacyBatch63Fixture, LegacyBatch63RepairPlan)
	}{
		{
			name: "task",
			mutate: func(
				t *testing.T,
				f legacyBatch63Fixture,
				_ LegacyBatch63RepairPlan,
			) {
				if _, err := f.st.pool.Exec(t.Context(), `
					UPDATE push_effects SET task_id=task_id||'-drift'
					 WHERE id=$1`,
					legacyBatch63EffectID,
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "run",
			mutate: func(
				t *testing.T,
				f legacyBatch63Fixture,
				_ LegacyBatch63RepairPlan,
			) {
				if _, err := f.st.pool.Exec(t.Context(), `
					UPDATE push_effects SET run_id=run_id||'-drift'
					 WHERE id=$1`,
					legacyBatch63EffectID,
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "workflow",
			mutate: func(
				t *testing.T,
				f legacyBatch63Fixture,
				plan LegacyBatch63RepairPlan,
			) {
				if _, err := f.st.pool.Exec(t.Context(), `
					UPDATE task_run_snapshots
					   SET temporal_workflow_id=$2
					 WHERE id=$1`,
					plan.Prepared.RunSnapshotID,
					scheduledTaskWorkflowID(legacyBatch63TaskID),
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "effect",
			mutate: func(
				t *testing.T,
				f legacyBatch63Fixture,
				_ LegacyBatch63RepairPlan,
			) {
				if _, err := f.st.pool.Exec(t.Context(), `
					UPDATE push_effects SET provider=provider||'-drift'
					 WHERE id=$1`,
					legacyBatch63EffectID,
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "audit",
			mutate: func(
				t *testing.T,
				f legacyBatch63Fixture,
				_ LegacyBatch63RepairPlan,
			) {
				if _, err := f.st.pool.Exec(t.Context(), `
					DELETE FROM legacy_batch63_repair_events
					 WHERE batch_id=63`,
				); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newLegacyBatch63Fixture(t)
			plan, err := f.st.PreviewLegacyBatch63Repair(
				t.Context(), f.evidence, f.expiresAt, f.buildCard)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.st.FinalizeLegacyBatch63Repair(
				t.Context(), plan.PlanDigest, f.evidence, f.expiresAt,
				f.buildCard,
			); err != nil {
				t.Fatal(err)
			}
			test.mutate(t, f, plan)
			effect := &pusheffect.Effect{
				Prepared: pusheffect.Prepared{
					ID: legacyBatch63EffectID, TenantID: plan.Prepared.TenantID,
					UserID: plan.Prepared.UserID, TaskID: legacyBatch63TaskID,
				},
			}
			_, decision, err := f.st.ClaimAuthorizedPushEffect(
				t.Context(),
				authorizedPushEffectClaimParams(
					effect, "legacy-batch63-"+test.name),
			)
			if err == nil || decision != "" {
				t.Fatalf(
					"drifted %s decision=%q err=%v",
					test.name, decision, err,
				)
			}
		})
	}
}

func TestClaimAuthorizedPushEffectRejectsBlockedLegacyBatch63Repair(
	t *testing.T,
) {
	f := newLegacyBatch63Fixture(t)
	plan, err := f.st.PreviewLegacyBatch63Repair(
		t.Context(), f.evidence, f.expiresAt, f.buildCard)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.FinalizeLegacyBatch63Repair(
		t.Context(), plan.PlanDigest, f.evidence, f.expiresAt, f.buildCard,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.AbortLegacyBatch63Repair(
		t.Context(), plan.PlanDigest,
	); err != nil {
		t.Fatal(err)
	}
	var ready bool
	if err := f.st.pool.QueryRow(t.Context(), `
		SELECT public.legacy_push_batch_63_claim_ready_v1(
			$1,$2,$3,$4,$5,$6,$7
		)`,
		legacyBatch63EffectID, plan.Prepared.TenantID, plan.Prepared.UserID,
		legacyBatch63TaskID, plan.Prepared.RunSnapshotID, legacyBatch63RunID,
		plan.PayloadDigest,
	).Scan(&ready); err != nil {
		t.Fatal(err)
	}
	if ready {
		t.Fatal("blocked legacy batch 63 remained claim-ready")
	}
	effect := &pusheffect.Effect{
		Prepared: pusheffect.Prepared{
			ID: legacyBatch63EffectID, TenantID: plan.Prepared.TenantID,
			UserID: plan.Prepared.UserID, TaskID: legacyBatch63TaskID,
		},
	}
	_, decision, err := f.st.ClaimAuthorizedPushEffect(
		t.Context(),
		authorizedPushEffectClaimParams(effect, "legacy-batch63-blocked"),
	)
	if err == nil || decision != "" {
		t.Fatalf("blocked decision=%q err=%v", decision, err)
	}
}

func TestClaimAuthorizedPushEffectRejectsLegacyBatch63AfterEnableBy(
	t *testing.T,
) {
	t.Run("fresh", func(t *testing.T) {
		f, plan := finalizedLegacyBatch63Repair(t)
		expireLegacyBatch63EnableBy(t, f)
		effect := &pusheffect.Effect{Prepared: plan.Prepared}
		claimed, decision, err := f.st.ClaimAuthorizedPushEffect(
			t.Context(),
			authorizedPushEffectClaimParams(effect, "legacy-late-fresh"),
		)
		if err != nil || claimed != nil ||
			decision != pusheffect.AuthorizedClaimDenied {
			t.Fatalf("late fresh claim=%+v decision=%q err=%v",
				claimed, decision, err)
		}
		var status string
		var fence int64
		var attempt int
		if err := f.st.pool.QueryRow(t.Context(), `
			SELECT status,fence,attempt FROM push_effects WHERE id=$1`,
			effect.ID,
		).Scan(&status, &fence, &attempt); err != nil {
			t.Fatal(err)
		}
		if status != string(pusheffect.StatusPrepared) ||
			fence != 0 || attempt != 0 {
			t.Fatalf("late fresh mutated effect=%s/%d/%d",
				status, fence, attempt)
		}
	})

	t.Run("same owner replay", func(t *testing.T) {
		f, plan := finalizedLegacyBatch63Repair(t)
		effect := &pusheffect.Effect{Prepared: plan.Prepared}
		params := authorizedPushEffectClaimParams(effect, "legacy-late-replay")
		if _, decision, err := f.st.ClaimAuthorizedPushEffect(
			t.Context(), params,
		); err != nil || decision != pusheffect.AuthorizedClaimed {
			t.Fatalf("initial claim decision=%q err=%v", decision, err)
		}
		if _, err := f.st.pool.Exec(t.Context(), `
			UPDATE legacy_batch63_repair_events
			   SET enable_by=clock_timestamp()-interval '1 second'
			 WHERE batch_id=63 AND phase='finalized'`); err != nil {
			t.Fatal(err)
		}
		claimed, decision, err := f.st.ClaimAuthorizedPushEffect(
			t.Context(), params)
		if err != nil || claimed == nil ||
			decision != pusheffect.AuthorizedClaimed {
			t.Fatalf("late replay claim=%+v decision=%q err=%v",
				claimed, decision, err)
		}
	})

	t.Run("reconciliation", func(t *testing.T) {
		f, plan := finalizedLegacyBatch63Repair(t)
		effect := &pusheffect.Effect{Prepared: plan.Prepared}
		params := authorizedPushEffectClaimParams(
			effect, "legacy-late-reconciliation")
		if _, decision, err := f.st.ClaimAuthorizedPushEffect(
			t.Context(), params,
		); err != nil || decision != pusheffect.AuthorizedClaimed {
			t.Fatalf("initial claim decision=%q err=%v", decision, err)
		}
		if _, err := f.st.pool.Exec(t.Context(), `
			UPDATE push_effects
			   SET status='ambiguous',lease_owner='',lease_until=NULL,
			       takeover_not_before=NULL,
			       failure_class='provider_outcome_unknown',
			       ambiguous_since=clock_timestamp(),
			       next_attempt_at=clock_timestamp()-interval '1 second'
			 WHERE id=$1`,
			legacyBatch63EffectID,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := f.st.pool.Exec(t.Context(), `
			UPDATE legacy_batch63_repair_events
			   SET enable_by=clock_timestamp()-interval '1 second'
			 WHERE batch_id=63 AND phase='finalized'`); err != nil {
			t.Fatal(err)
		}
		claimed, decision, err := f.st.ClaimAuthorizedPushEffectReconciliation(
			t.Context(), params)
		if err != nil || claimed == nil ||
			decision != pusheffect.AuthorizedClaimed {
			t.Fatalf("late reconciliation claim=%+v decision=%q err=%v",
				claimed, decision, err)
		}
	})

	t.Run("definite failure retry and terminal budget", func(t *testing.T) {
		f, plan := finalizedLegacyBatch63Repair(t)
		effect := &pusheffect.Effect{Prepared: plan.Prepared}
		params := authorizedPushEffectClaimParams(
			effect, "legacy-definite-1")
		claimed, decision, err := f.st.ClaimAuthorizedPushEffect(
			t.Context(), params)
		if err != nil || decision != pusheffect.AuthorizedClaimed {
			t.Fatalf("initial claim=%+v decision=%q err=%v",
				claimed, decision, err)
		}
		if _, err := f.st.pool.Exec(t.Context(), `
			UPDATE legacy_batch63_repair_events
			   SET enable_by=clock_timestamp()-interval '1 second'
			 WHERE batch_id=63 AND phase='finalized'`); err != nil {
			t.Fatal(err)
		}
		for {
			lease := pusheffect.Lease{
				Scope: claimed.Scope(), LeaseOwner: claimed.LeaseOwner,
				Fence: claimed.Fence,
			}
			if err := f.st.RecordPushEffectDefiniteFailure(
				t.Context(),
				pusheffect.FailureParams{
					Lease: lease, Class: "provider_definite_failure",
					RetryAfter: time.Second,
				},
			); err != nil {
				t.Fatal(err)
			}
			if claimed.Attempt >= pusheffect.RecoveryMaxAttempts {
				break
			}
			if _, err := f.st.pool.Exec(t.Context(), `
				UPDATE push_effects
				   SET next_attempt_at=clock_timestamp()-interval '1 second'
				 WHERE id=$1`, legacyBatch63EffectID); err != nil {
				t.Fatal(err)
			}
			params.LeaseOwner = "legacy-definite-" +
				string(rune('1'+claimed.Attempt))
			wantAttempt := claimed.Attempt + 1
			claimed, decision, err = f.st.ClaimAuthorizedPushEffect(
				t.Context(), params)
			if err != nil || decision != pusheffect.AuthorizedClaimed {
				t.Fatalf("retry attempt=%d claim=%+v decision=%q err=%v",
					wantAttempt, claimed, decision, err)
			}
		}
		if claimed.Attempt != pusheffect.RecoveryMaxAttempts {
			t.Fatalf("terminal attempt=%d", claimed.Attempt)
		}
		if err := f.st.BlockExhaustedPushEffectAttempts(
			t.Context(),
			pusheffect.ExhaustedResolution{
				Scope: claimed.Scope(), ExpectedFence: claimed.Fence,
				ExpectedTaskID: legacyBatch63TaskID,
			},
		); err != nil {
			t.Fatal(err)
		}
		terminal, err := f.st.LoadPushEffect(
			t.Context(), claimed.Scope())
		if err != nil {
			t.Fatal(err)
		}
		if terminal.Status != pusheffect.StatusBlocked ||
			terminal.FailureClass != "attempt_budget_exhausted" {
			t.Fatalf("terminal effect=%+v", terminal)
		}
		status, err := f.st.VerifyLegacyBatch63Repair(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if status.Phase != "finalized" ||
			status.EffectStatus != string(pusheffect.StatusBlocked) {
			t.Fatalf("terminal repair status=%+v", status)
		}
	})
}

func TestLegacyBatch63AbortWinsAgainstLateClaim(t *testing.T) {
	f, plan := finalizedLegacyBatch63Repair(t)
	expireLegacyBatch63EnableBy(t, f)
	effect := &pusheffect.Effect{Prepared: plan.Prepared}
	type result struct {
		operation string
		decision  pusheffect.AuthorizedClaimDecision
		err       error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		_, decision, err := f.st.ClaimAuthorizedPushEffect(
			t.Context(),
			authorizedPushEffectClaimParams(effect, "legacy-late-race"),
		)
		results <- result{
			operation: "claim", decision: decision, err: err,
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		_, err := f.st.AbortLegacyBatch63Repair(
			t.Context(), plan.PlanDigest)
		results <- result{operation: "abort", err: err}
	}()
	close(start)
	workers.Wait()
	close(results)
	for got := range results {
		switch got.operation {
		case "claim":
			if got.err == nil &&
				got.decision != pusheffect.AuthorizedClaimDenied {
				t.Fatalf("late claim decision=%q err=%v",
					got.decision, got.err)
			}
			if got.err != nil && got.decision != "" {
				t.Fatalf("late claim decision=%q err=%v",
					got.decision, got.err)
			}
		case "abort":
			if got.err != nil {
				t.Fatalf("abort lost to late claim: %v", got.err)
			}
		}
	}
	status, err := f.st.VerifyLegacyBatch63Repair(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase != "blocked" || status.EffectStatus != "blocked" ||
		status.BatchStatus != "failed" {
		t.Fatalf("late race status=%+v", status)
	}
}

func TestLegacyBatch63FreshClaimAndAbortHaveOneWinner(t *testing.T) {
	f, plan := finalizedLegacyBatch63Repair(t)
	effect := &pusheffect.Effect{Prepared: plan.Prepared}
	type result struct {
		operation string
		decision  pusheffect.AuthorizedClaimDecision
		err       error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		_, decision, err := f.st.ClaimAuthorizedPushEffect(
			t.Context(),
			authorizedPushEffectClaimParams(effect, "legacy-fresh-race"),
		)
		results <- result{
			operation: "claim", decision: decision, err: err,
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		_, err := f.st.AbortLegacyBatch63Repair(
			t.Context(), plan.PlanDigest)
		results <- result{operation: "abort", err: err}
	}()
	close(start)
	workers.Wait()
	close(results)
	winners := 0
	for got := range results {
		switch got.operation {
		case "claim":
			if got.err == nil &&
				got.decision == pusheffect.AuthorizedClaimed {
				winners++
			} else if got.err == nil || got.decision != "" {
				t.Fatalf("fresh race claim decision=%q err=%v",
					got.decision, got.err)
			}
		case "abort":
			if got.err == nil {
				winners++
			}
		}
	}
	if winners != 1 {
		t.Fatalf("fresh claim-vs-abort winners=%d", winners)
	}
	status, err := f.st.VerifyLegacyBatch63Repair(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	switch status.Phase {
	case "finalized":
		if status.EffectStatus != "sending" {
			t.Fatalf("claim winner status=%+v", status)
		}
	case "blocked":
		if status.EffectStatus != "blocked" ||
			status.BatchStatus != "failed" {
			t.Fatalf("abort winner status=%+v", status)
		}
	default:
		t.Fatalf("nonterminal race status=%+v", status)
	}
}

func finalizedLegacyBatch63Repair(
	t *testing.T,
) (legacyBatch63Fixture, LegacyBatch63RepairPlan) {
	t.Helper()
	f := newLegacyBatch63Fixture(t)
	plan, err := f.st.PreviewLegacyBatch63Repair(
		t.Context(), f.evidence, f.expiresAt, f.buildCard)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.FinalizeLegacyBatch63Repair(
		t.Context(), plan.PlanDigest, f.evidence, f.expiresAt, f.buildCard,
	); err != nil {
		t.Fatal(err)
	}
	return f, plan
}

func expireLegacyBatch63EnableBy(t *testing.T, f legacyBatch63Fixture) {
	t.Helper()
	if _, err := f.st.pool.Exec(t.Context(), `
		UPDATE legacy_batch63_repair_events
		   SET enable_by=clock_timestamp()-interval '1 second'
		 WHERE batch_id=63 AND phase='finalized'`); err != nil {
		t.Fatal(err)
	}
	var payloadDigest string
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT payload_digest FROM push_effects WHERE id=$1`,
		legacyBatch63EffectID,
	).Scan(&payloadDigest); err != nil {
		t.Fatal(err)
	}
	var identityReady, freshReady bool
	if err := f.st.pool.QueryRow(t.Context(), `
		SELECT
		  legacy_push_batch_63_claim_ready_v1(
		    $1,1,1,$2,3,$3,$4
		  ),
		  legacy_push_batch_63_fresh_claim_ready_v1(
		    $1,1,1,$2,3,$3,$4
		  )`,
		legacyBatch63EffectID, legacyBatch63TaskID, legacyBatch63RunID,
		payloadDigest,
	).Scan(&identityReady, &freshReady); err != nil {
		t.Fatal(err)
	}
	if !identityReady || freshReady {
		t.Fatalf("late readiness identity/fresh=%v/%v",
			identityReady, freshReady)
	}
}

func authorizedPushEffectClaimParams(
	effect *pusheffect.Effect,
	leaseOwner string,
) pusheffect.AuthorizedClaimParams {
	return pusheffect.AuthorizedClaimParams{
		ClaimParams: pusheffect.ClaimParams{
			Scope: effect.Scope(), LeaseOwner: leaseOwner,
			LeaseDuration: time.Minute,
		},
		ExpectedTaskID: effect.TaskID, DenialRetryAfter: time.Minute,
	}
}

func TestClaimAuthorizedPushEffectSchema49DoesNotResolveLegacyHelper(
	t *testing.T,
) {
	f := newPushEffectFixtureAt(t, 49)
	var helperAbsent bool
	if err := f.db.QueryRowContext(t.Context(), `
		SELECT to_regprocedure(
		  'public.legacy_push_batch_63_claim_ready_v1(text,bigint,bigint,text,bigint,text,text)'
		) IS NULL`,
	).Scan(&helperAbsent); err != nil {
		t.Fatal(err)
	}
	if !helperAbsent {
		t.Fatal("schema 49 unexpectedly contains migration 050 helper")
	}
	digest := strings.Repeat("a", 64)
	budget := []byte(
		`{"max_planner_rounds":0,"max_tool_calls":0,"max_tokens":0,` +
			`"max_cost_micro_usd":0,"duration_ms":0}`)
	snapshot := taskRunSnapshot{
		ID: f.prepared.RunSnapshotID, TenantID: f.prepared.TenantID,
		UserID: f.prepared.UserID, TaskID: f.prepared.TaskID,
		TemporalWorkflowID:      scheduledTaskWorkflowID(f.prepared.TaskID),
		TemporalRunID:           f.prepared.RunID,
		RunKind:                 types.RunSnapshotKindScheduled,
		Mode:                    types.ExecutionModeCompiled,
		CapabilityCatalogDigest: digest, ToolPolicyDigest: digest,
		PromptPolicyDigest: digest, ModelPolicyDigest: digest,
		QuotaPolicyDigest: digest, DefinitionDigest: digest,
		PlanDigest: digest, PayloadDigest: digest,
		ReferenceSchemaVersion: taskRunReferenceSchemaVersionV1,
		BudgetJSON:             budget,
	}
	ref, err := sealTaskRunSnapshotReferenceV1(
		&snapshot, taskRunBudgetV1{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.pool.Exec(t.Context(), `
		UPDATE task_run_snapshots
		   SET temporal_workflow_id=$2,reference_digest=$3,
		       reference_schema_version=$4,budget=$5
		 WHERE id=$1`,
		snapshot.ID, snapshot.TemporalWorkflowID, ref.ReferenceDigest,
		taskRunReferenceSchemaVersionV1, budget,
	); err != nil {
		t.Fatal(err)
	}
	effect, err := f.store.CreatePushEffect(t.Context(), f.prepared)
	if err != nil {
		t.Fatal(err)
	}
	claimed, decision, err := f.store.ClaimAuthorizedPushEffect(
		t.Context(),
		authorizedPushEffectClaimParams(effect, "schema49-normal"),
	)
	if err != nil || decision != pusheffect.AuthorizedClaimed ||
		claimed == nil || claimed.Status != pusheffect.StatusSending {
		t.Fatalf("schema49 claim=%+v decision=%q err=%v",
			claimed, decision, err)
	}
}

func TestClaimAuthorizedPushEffectLeaseFitsProviderWindow(t *testing.T) {
	t.Run("fresh claim rejects an incomplete provider window", func(t *testing.T) {
		f, effect := authorizedPushEffectFixture(t)
		claimed, decision, err := f.st.ClaimAuthorizedPushEffect(
			t.Context(),
			pusheffect.AuthorizedClaimParams{
				ClaimParams: pusheffect.ClaimParams{
					Scope: effect.Scope(), LeaseOwner: "window-too-short",
					LeaseDuration: 2 * time.Hour,
				},
				ExpectedTaskID:   effect.TaskID,
				DenialRetryAfter: time.Minute,
			},
		)
		if err == nil || !errors.Is(err, types.ErrConflict) ||
			claimed != nil || decision != "" {
			t.Fatalf(
				"incomplete-window claim=%+v decision=%q err=%v",
				claimed, decision, err,
			)
		}
		assertPushEffectProviderStateUnclaimed(t, f, effect)
	})

	t.Run("fresh lease is bounded by one database clock sample", func(t *testing.T) {
		f, effect := authorizedPushEffectFixture(t)
		claimed, decision, err := f.st.ClaimAuthorizedPushEffect(
			t.Context(),
			pusheffect.AuthorizedClaimParams{
				ClaimParams: pusheffect.ClaimParams{
					Scope: effect.Scope(), LeaseOwner: "database-clock-window",
					LeaseDuration: 59 * time.Minute,
				},
				ExpectedTaskID:   effect.TaskID,
				DenialRetryAfter: time.Minute,
			},
		)
		if err != nil || decision != pusheffect.AuthorizedClaimed ||
			claimed == nil || claimed.LeaseUntil == nil {
			t.Fatalf("bounded claim=%+v decision=%q err=%v", claimed, decision, err)
		}
		if claimed.LeaseUntil.After(claimed.IdempotencyExpiresAt) {
			t.Fatalf(
				"lease_until=%s exceeds provider expiry=%s",
				claimed.LeaseUntil, claimed.IdempotencyExpiresAt,
			)
		}
		var (
			leaseUntil, updatedAt time.Time
			expiresAt             time.Time
		)
		if err := f.st.pool.QueryRow(t.Context(), `
			SELECT lease_until,idempotency_expires_at,updated_at
			  FROM push_effects WHERE id=$1`,
			effect.ID,
		).Scan(&leaseUntil, &expiresAt, &updatedAt); err != nil {
			t.Fatal(err)
		}
		if !leaseUntil.Equal(updatedAt.Add(59*time.Minute)) ||
			leaseUntil.After(expiresAt) {
			t.Fatalf(
				"DB-clock lease=%s updated=%s expiry=%s",
				leaseUntil, updatedAt, expiresAt,
			)
		}
	})

	t.Run("same owner replay rejects a lease beyond provider expiry", func(t *testing.T) {
		f, effect := authorizedPushEffectFixture(t)
		params := pusheffect.AuthorizedClaimParams{
			ClaimParams: pusheffect.ClaimParams{
				Scope: effect.Scope(), LeaseOwner: "same-owner-window",
				LeaseDuration: time.Minute,
			},
			ExpectedTaskID: effect.TaskID, DenialRetryAfter: time.Minute,
		}
		claimed, decision, err := f.st.ClaimAuthorizedPushEffect(
			t.Context(), params)
		if err != nil || decision != pusheffect.AuthorizedClaimed ||
			claimed == nil || claimed.LeaseUntil == nil {
			t.Fatalf("initial claim=%+v decision=%q err=%v", claimed, decision, err)
		}
		if _, err := f.st.pool.Exec(t.Context(), `
			UPDATE push_effects
			   SET lease_until=idempotency_expires_at+interval '1 second',
			       takeover_not_before=idempotency_expires_at+
			           interval '31 seconds'
			 WHERE id=$1`,
			effect.ID,
		); err != nil {
			t.Fatal(err)
		}
		replayed, replayDecision, err :=
			f.st.ClaimAuthorizedPushEffect(t.Context(), params)
		if err != nil || replayed != nil ||
			replayDecision != pusheffect.AuthorizedClaimDenied {
			t.Fatalf(
				"out-of-window replay=%+v decision=%q err=%v",
				replayed, replayDecision, err,
			)
		}
		var (
			status                string
			fence, attempt        int64
			leaseUntil, expiresAt time.Time
		)
		if err := f.st.pool.QueryRow(t.Context(), `
			SELECT status,fence,attempt,lease_until,idempotency_expires_at
			  FROM push_effects WHERE id=$1`,
			effect.ID,
		).Scan(
			&status, &fence, &attempt, &leaseUntil, &expiresAt,
		); err != nil {
			t.Fatal(err)
		}
		if status != string(pusheffect.StatusSending) ||
			fence != claimed.Fence || attempt != int64(claimed.Attempt) ||
			!leaseUntil.After(expiresAt) {
			t.Fatalf(
				"replay mutated provider state=%q fence=%d attempt=%d lease=%s expiry=%s",
				status, fence, attempt, leaseUntil, expiresAt,
			)
		}
	})
}

func TestClaimAuthorizedPushEffectExpiredWindowHasNoConcurrentWinner(
	t *testing.T,
) {
	f, effect := authorizedPushEffectFixture(t)
	type result struct {
		decision pusheffect.AuthorizedClaimDecision
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var workers sync.WaitGroup
	for worker := range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			_, decision, err := f.st.ClaimAuthorizedPushEffect(
				t.Context(),
				pusheffect.AuthorizedClaimParams{
					ClaimParams: pusheffect.ClaimParams{
						Scope: effect.Scope(),
						LeaseOwner: "expired-window-" +
							string(rune('a'+worker)),
						LeaseDuration: 2 * time.Hour,
					},
					ExpectedTaskID:   effect.TaskID,
					DenialRetryAfter: time.Minute,
				},
			)
			results <- result{decision: decision, err: err}
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	for got := range results {
		if got.err == nil || !errors.Is(got.err, types.ErrConflict) ||
			got.decision != "" {
			t.Fatalf(
				"concurrent expired-window decision=%q err=%v",
				got.decision, got.err,
			)
		}
	}
	assertPushEffectProviderStateUnclaimed(t, f, effect)
}

func assertPushEffectProviderStateUnclaimed(
	t *testing.T,
	f *taskRunSnapshotFixture,
	effect *pusheffect.Effect,
) {
	t.Helper()
	var (
		status, owner string
		fence         int64
		attempt       int
		leaseUntil    *time.Time
	)
	if err := f.st.pool.QueryRow(t.Context(), `
		SELECT status,lease_owner,fence,attempt,lease_until
		  FROM push_effects WHERE id=$1`,
		effect.ID,
	).Scan(&status, &owner, &fence, &attempt, &leaseUntil); err != nil {
		t.Fatal(err)
	}
	if status != string(pusheffect.StatusPrepared) || owner != "" ||
		fence != 0 || attempt != 0 || leaseUntil != nil {
		t.Fatalf(
			"provider state changed=%q owner=%q fence=%d attempt=%d lease=%v",
			status, owner, fence, attempt, leaseUntil,
		)
	}
}

func TestClaimAuthorizedPushEffectReconciliationHonorsDatabaseDueTime(
	t *testing.T,
) {
	f, effect := authorizedPushEffectFixture(t)
	claimed, decision, err := f.st.ClaimAuthorizedPushEffect(
		t.Context(),
		pusheffect.AuthorizedClaimParams{
			ClaimParams: pusheffect.ClaimParams{
				Scope: effect.Scope(), LeaseOwner: "first-attempt",
				LeaseDuration: 10 * time.Minute,
			},
			ExpectedTaskID: effect.TaskID, DenialRetryAfter: time.Minute,
		},
	)
	if err != nil || decision != pusheffect.AuthorizedClaimed {
		t.Fatalf("fresh claim=%q/%v", decision, err)
	}
	if err := f.st.RecordPushEffectAmbiguous(
		t.Context(),
		pusheffect.FailureParams{
			Lease: pusheffect.Lease{
				Scope: claimed.Scope(), LeaseOwner: claimed.LeaseOwner,
				Fence: claimed.Fence,
			},
			Class: "provider_response_unknown",
		},
	); err != nil {
		t.Fatal(err)
	}
	ambiguous, err := f.st.LoadPushEffect(t.Context(), effect.Scope())
	if err != nil {
		t.Fatal(err)
	}
	if ambiguous.Status != pusheffect.StatusAmbiguous ||
		ambiguous.LeaseOwner != "" || ambiguous.LeaseUntil != nil {
		t.Fatalf("ambiguous checkpoint=%+v", ambiguous)
	}
	if _, err := f.st.DeferOrBlockPushEffectReconciliation(
		t.Context(),
		pusheffect.ReconciliationSchedule{
			Scope: ambiguous.Scope(), ExpectedFence: ambiguous.Fence,
			RetryAfter: time.Hour,
		},
	); err != nil {
		var (
			status, owner string
			fence         int64
			expires       time.Time
		)
		queryErr := f.st.pool.QueryRow(t.Context(), `
			SELECT status,lease_owner,fence,idempotency_expires_at
			  FROM push_effects WHERE id=$1`,
			effect.ID,
		).Scan(&status, &owner, &fence, &expires)
		t.Fatalf(
			"defer error=%v row=%q/%q/%d expires=%s query=%v",
			err, status, owner, fence, expires, queryErr,
		)
	}
	params := pusheffect.AuthorizedClaimParams{
		ClaimParams: pusheffect.ClaimParams{
			Scope: ambiguous.Scope(), LeaseOwner: "reconciliation",
			LeaseDuration: 10 * time.Minute,
		},
		ExpectedTaskID: ambiguous.TaskID, DenialRetryAfter: time.Minute,
	}
	if replay, got, err := f.st.ClaimAuthorizedPushEffectReconciliation(
		t.Context(), params,
	); err != nil || replay != nil || got != pusheffect.AuthorizedClaimNotDue {
		t.Fatalf("not-due reconciliation=%+v/%q/%v", replay, got, err)
	}
	if _, err := f.st.pool.Exec(t.Context(), `
		UPDATE push_effects
		   SET next_attempt_at=clock_timestamp()-interval '1 microsecond'
		 WHERE id=$1`,
		effect.ID,
	); err != nil {
		t.Fatal(err)
	}
	replayed, got, err := f.st.ClaimAuthorizedPushEffectReconciliation(
		t.Context(), params)
	if err != nil || got != pusheffect.AuthorizedClaimed ||
		replayed == nil || replayed.Fence != ambiguous.Fence+1 {
		t.Fatalf("due reconciliation=%+v/%q/%v", replayed, got, err)
	}
}

func authorizedPushEffectFixture(
	t *testing.T,
) (*taskRunSnapshotFixture, *pusheffect.Effect) {
	return authorizedPushEffectFixtureWithExpiry(t, "", time.Hour)
}

func authorizedPushEffectFixtureForWorkflow(
	t *testing.T,
	workflowID string,
) (*taskRunSnapshotFixture, *pusheffect.Effect) {
	return authorizedPushEffectFixtureWithExpiry(t, workflowID, time.Hour)
}

func authorizedPushEffectFixtureWithExpiry(
	t *testing.T,
	workflowID string,
	expiresAfter time.Duration,
) (*taskRunSnapshotFixture, *pusheffect.Effect) {
	t.Helper()
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	identity := scheduledRunIdentity(
		taskID, f.tenantID, f.userID, "push-effect-auth-"+uuid.NewString())
	if workflowID == "dated" {
		identity.TemporalWorkflowID = scheduledTaskWorkflowID(taskID) +
			"-2026-07-24T20:28:32Z"
	} else if workflowID != "" {
		identity.TemporalWorkflowID = workflowID
	}
	ref, err := f.st.CreateOrGetCompiledTaskRunSnapshotV1(
		t.Context(),
		CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: identity, Policy: testCompiledRunPolicyV1(t),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	var batchID, deliveryID int64
	if err := f.st.pool.QueryRow(t.Context(), `
		INSERT INTO push_batches (
			tenant_id,user_id,status,idempotency_key,schedule_id,run_snapshot_id
		) VALUES ($1,$2,'pending',$3,$4,$5) RETURNING id`,
		f.tenantID, f.userID, "auth-batch-"+uuid.NewString(),
		taskID, ref.SnapshotID,
	).Scan(&batchID); err != nil {
		t.Fatal(err)
	}
	winner, err := f.st.ClaimPushBatchDeliveryAuthority(
		t.Context(),
		types.PushBatchScope{
			TenantID: f.tenantID, UserID: f.userID, BatchID: batchID,
		},
		types.PushBatchDeliveryAuthorityEffect,
	)
	if err != nil || winner != types.PushBatchDeliveryAuthorityEffect {
		t.Fatalf("batch authority=%q err=%v", winner, err)
	}
	if err := f.st.pool.QueryRow(t.Context(), `
		INSERT INTO deliveries (
			tenant_id,batch_id,user_id,score,card_json,status
		) VALUES ($1,$2,$3,80,'{}'::jsonb,'pending') RETURNING id`,
		f.tenantID, batchID, f.userID,
	).Scan(&deliveryID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, f.st,
			`DELETE FROM push_effects WHERE tenant_id=$1`, f.tenantID)
		cleanupExec(cleanupCtx, t, f.st,
			`DELETE FROM task_observed_events WHERE tenant_id=$1`, f.tenantID)
		cleanupExec(cleanupCtx, t, f.st,
			`DELETE FROM pending_actions WHERE tenant_id=$1`, f.tenantID)
		cleanupExec(cleanupCtx, t, f.st,
			`DELETE FROM deliveries WHERE tenant_id=$1`, f.tenantID)
		cleanupExec(cleanupCtx, t, f.st,
			`DELETE FROM push_batches WHERE tenant_id=$1`, f.tenantID)
	})
	eventKey :=
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := f.st.pool.Exec(t.Context(), `
		INSERT INTO task_observed_events (
		    tenant_id,user_id,task_id,policy_digest,event_key,event_type,
		    subject,occurred_at,evidence_json,run_snapshot_id,
		    temporal_run_id,delivery_id,status
		) VALUES (
		    $1,$2,$3,$4,$5,'release','authorization fixture',
		    clock_timestamp(),'{"source":"authorization-fixture"}'::jsonb,
		    $6,$7,$8,'qualified'
		)`,
		f.tenantID, f.userID, taskID,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		eventKey, ref.SnapshotID, identity.TemporalRunID, deliveryID,
	); err != nil {
		t.Fatal(err)
	}
	// The CHECK constraint compares this value with a database-generated
	// created_at. An exact one-hour Go-clock value flakes if the test runner
	// clock is even slightly ahead of the PostgreSQL container.
	var idempotencyExpiresAt time.Time
	if err := f.st.pool.QueryRow(t.Context(), `
		SELECT clock_timestamp() +
		       $1::double precision * interval '1 microsecond'`,
		expiresAfter.Microseconds(),
	).Scan(&idempotencyExpiresAt); err != nil {
		t.Fatal(err)
	}
	idempotencyExpiresAt = idempotencyExpiresAt.UTC()
	effect, err := f.st.CreatePushEffect(
		t.Context(),
		pusheffect.Prepared{
			ID: uuid.NewString(), TenantID: f.tenantID, UserID: f.userID,
			TaskID: taskID, RunSnapshotID: ref.SnapshotID,
			RunID: identity.TemporalRunID, StepID: "push",
			ChunkIndex: 0, ChunkCount: 1, BatchID: batchID,
			DeliveryIDs: []int64{deliveryID}, Provider: "feishu",
			AppIdentity:          "cli_push_effect_authorization",
			ProviderChatID:       "oc_push_effect_authorization",
			Target:               "ou_push_effect_authorization",
			Card:                 []byte(`{"card":"authorization"}`),
			ProviderUUID:         uuid.NewString(),
			ObservationEventKeys: []string{eventKey},
			IdempotencyExpiresAt: idempotencyExpiresAt,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return f, effect
}
