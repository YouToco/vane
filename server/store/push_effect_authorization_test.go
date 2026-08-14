package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/pusheffect"
	"github.com/YouToco/vane/server/types"
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
			INSERT INTO task_creation_operations (
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

func TestClaimAuthorizedPushEffectAcceptsExactTemporalScheduleExecution(
	t *testing.T,
) {
	f, effect := authorizedPushEffectFixtureForWorkflow(
		t, "dated")
	claimed, decision, err := f.st.ClaimAuthorizedPushEffect(
		t.Context(),
		authorizedPushEffectClaimParams(effect, "ordinary-dated-workflow"),
	)
	if err != nil || decision != pusheffect.AuthorizedClaimed ||
		claimed == nil || claimed.Status != pusheffect.StatusSending {
		t.Fatalf("ordinary dated workflow decision=%q err=%v", decision, err)
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

func TestUpdateFreshAuthorizedPushEffectClaimUsesFrozenDatabaseClock(t *testing.T) {
	f, effect := authorizedPushEffectFixture(t)
	var databaseNow time.Time
	// Deliberately place the frozen sample beyond the live database clock. A
	// regression that re-reads clock_timestamp() inside the UPDATE sees the
	// effect as not due and returns the false competing-owner error.
	if err := f.st.pool.QueryRow(t.Context(), `
		SELECT clock_timestamp()+interval '30 minutes'`,
	).Scan(&databaseNow); err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.pool.Exec(t.Context(), `
		UPDATE push_effects
		   SET next_attempt_at=$2
		 WHERE id=$1`,
		effect.ID, databaseNow,
	); err != nil {
		t.Fatal(err)
	}

	tx, err := f.st.beginPushEffectCoordinatorTx(t.Context(), effect.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackPushEffectTx(t.Context(), tx)
	if err := lockPushEffectBatchForScope(t.Context(), tx, effect.Scope()); err != nil {
		t.Fatal(err)
	}
	stored, err := loadPushEffectForUpdate(t.Context(), tx, effect.Scope())
	if err != nil {
		t.Fatal(err)
	}
	params := pusheffect.AuthorizedClaimParams{
		ClaimParams: pusheffect.ClaimParams{
			Scope: stored.Scope(), LeaseOwner: "frozen-clock-test",
			LeaseDuration: time.Minute,
		},
		ExpectedTaskID: stored.TaskID, DenialRetryAfter: time.Minute,
	}
	claimed, authorized, err := updateFreshAuthorizedPushEffectClaim(
		t.Context(), tx, stored, params, databaseNow)
	if err != nil || !authorized || claimed == nil {
		t.Fatalf("claim=%+v authorized=%v err=%v", claimed, authorized, err)
	}
	wantLeaseUntil := databaseNow.Add(params.LeaseDuration)
	wantTakeover := wantLeaseUntil.Add(pushEffectTakeoverGrace)
	if claimed.LeaseUntil == nil || !claimed.LeaseUntil.Equal(wantLeaseUntil) ||
		claimed.TakeoverNotBefore == nil ||
		!claimed.TakeoverNotBefore.Equal(wantTakeover) ||
		!claimed.NextAttemptAt.Equal(databaseNow) ||
		!claimed.UpdatedAt.Equal(databaseNow) {
		t.Fatalf(
			"claim clock lease=%v takeover=%v next=%s updated=%s "+
				"want lease=%s takeover=%s next/updated=%s",
			claimed.LeaseUntil, claimed.TakeoverNotBefore,
			claimed.NextAttemptAt, claimed.UpdatedAt,
			wantLeaseUntil, wantTakeover, databaseNow,
		)
	}
}

func TestClaimAuthorizedPushEffectSamplesClockAfterAdmission(t *testing.T) {
	f, effect := authorizedPushEffectFixture(t)
	const leaseDuration = time.Minute
	var gateCompleted time.Time
	claimed, decision, err := f.st.claimAuthorizedPushEffectWithGate(
		t.Context(),
		pusheffect.AuthorizedClaimParams{
			ClaimParams: pusheffect.ClaimParams{
				Scope: effect.Scope(), LeaseOwner: "late-clock-test",
				LeaseDuration: leaseDuration,
			},
			ExpectedTaskID: effect.TaskID, DenialRetryAfter: time.Minute,
		},
		false,
		func(ctx context.Context, tx pgx.Tx, _ *pusheffect.Effect) error {
			return tx.QueryRow(ctx, `
				SELECT clock_timestamp() FROM (SELECT pg_sleep(0.05)) AS delayed`,
			).Scan(&gateCompleted)
		},
	)
	if err != nil || decision != pusheffect.AuthorizedClaimed || claimed == nil {
		t.Fatalf("claim=%+v decision=%q err=%v", claimed, decision, err)
	}
	if claimed.LeaseUntil == nil ||
		claimed.LeaseUntil.Before(gateCompleted.Add(leaseDuration)) {
		t.Fatalf(
			"lease=%v does not contain a complete post-admission window from %s",
			claimed.LeaseUntil, gateCompleted,
		)
	}
}

func TestAuthorizedFreshPushEffectDueDistinguishesInitialFromBackoff(t *testing.T) {
	created := time.Date(2026, time.August, 12, 10, 0, 0, 0, time.UTC)
	databaseNow := created.Add(-time.Second)
	initial := &pusheffect.Effect{
		Status: pusheffect.StatusPrepared, NextAttemptAt: created,
		CreatedAt: created, UpdatedAt: created.Add(time.Microsecond),
	}
	if !authorizedFreshPushEffectDue(initial, databaseNow) {
		t.Fatal("never-attempted effect was delayed by a database clock rollback")
	}
	backoff := *initial
	backoff.UpdatedAt = databaseNow
	backoff.NextAttemptAt = databaseNow.Add(time.Minute)
	if authorizedFreshPushEffectDue(&backoff, databaseNow) {
		t.Fatal("authorization denial backoff was bypassed as an initial claim")
	}
}

func TestClaimAuthorizedPushEffectHandlesInitialClockRollbackWithoutBypassingBackoff(
	t *testing.T,
) {
	t.Run("pristine public claim", func(t *testing.T) {
		f, effect := authorizedPushEffectFixture(t)
		if _, err := f.st.pool.Exec(t.Context(), `
			UPDATE push_effects
			   SET next_attempt_at=clock_timestamp()+interval '1 minute',
			       created_at=clock_timestamp()+interval '1 minute',
			       updated_at=clock_timestamp()+interval '1 minute'
			 WHERE id=$1`, effect.ID,
		); err != nil {
			t.Fatal(err)
		}
		claimed, decision, err := f.st.ClaimAuthorizedPushEffect(
			t.Context(), authorizedPushEffectClaimParams(effect, "rollback-public"))
		if err != nil || decision != pusheffect.AuthorizedClaimed || claimed == nil {
			t.Fatalf("rollback claim=%+v decision=%q err=%v", claimed, decision, err)
		}
	})

	t.Run("committed denial remains not due", func(t *testing.T) {
		f, effect := authorizedPushEffectFixture(t)
		if _, err := f.st.pool.Exec(t.Context(), `
			DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`,
			effect.TenantID, effect.UserID,
		); err != nil {
			t.Fatal(err)
		}
		params := authorizedPushEffectClaimParams(effect, "denied-backoff")
		params.DenialRetryAfter = 2 * time.Minute
		if claimed, decision, err := f.st.ClaimAuthorizedPushEffect(
			t.Context(), params,
		); err != nil || claimed != nil || decision != pusheffect.AuthorizedClaimDenied {
			t.Fatalf("denial=%+v/%q/%v", claimed, decision, err)
		}
		if _, err := f.st.pool.Exec(t.Context(), `
			INSERT INTO memberships (tenant_id,user_id,role) VALUES ($1,$2,'owner')`,
			effect.TenantID, effect.UserID,
		); err != nil {
			t.Fatal(err)
		}
		if claimed, decision, err := f.st.ClaimAuthorizedPushEffect(
			t.Context(), params,
		); err != nil || claimed != nil || decision != pusheffect.AuthorizedClaimNotDue {
			t.Fatalf("backoff replay=%+v/%q/%v", claimed, decision, err)
		}
	})
}

func TestAuthorizedPushEffectClockSampleBindsReplayReconciliationAndDenials(
	t *testing.T,
) {
	t.Run("same-owner replay", func(t *testing.T) {
		f, effect := authorizedPushEffectFixture(t)
		params := pusheffect.AuthorizedClaimParams{
			ClaimParams: pusheffect.ClaimParams{
				Scope: effect.Scope(), LeaseOwner: "replay-clock-test",
				LeaseDuration: time.Minute,
			},
			ExpectedTaskID: effect.TaskID, DenialRetryAfter: time.Minute,
		}
		claimed, decision, err := f.st.ClaimAuthorizedPushEffect(t.Context(), params)
		if err != nil || decision != pusheffect.AuthorizedClaimed || claimed == nil {
			t.Fatalf("initial claim=%+v/%q/%v", claimed, decision, err)
		}
		var leaseUntil time.Time
		if err := f.st.pool.QueryRow(t.Context(), `
			UPDATE push_effects
			   SET lease_until=clock_timestamp()-interval '1 second',
			       takeover_not_before=clock_timestamp()+interval '29 seconds'
			 WHERE id=$1
			 RETURNING lease_until`, effect.ID,
		).Scan(&leaseUntil); err != nil {
			t.Fatal(err)
		}
		tx, stored := lockedAuthorizedPushEffectForClockTest(t, f, effect)
		defer rollbackPushEffectTx(t.Context(), tx)
		replayed, authorized, err := loadAuthorizedPushEffectClaimReplay(
			t.Context(), tx, stored, params, false, leaseUntil.Add(-time.Second))
		if err != nil || !authorized || replayed == nil {
			t.Fatalf("replay=%+v authorized=%v err=%v", replayed, authorized, err)
		}
	})

	t.Run("reconciliation", func(t *testing.T) {
		f, effect := authorizedPushEffectFixture(t)
		params := pusheffect.AuthorizedClaimParams{
			ClaimParams: pusheffect.ClaimParams{
				Scope: effect.Scope(), LeaseOwner: "initial-clock-test",
				LeaseDuration: time.Minute,
			},
			ExpectedTaskID: effect.TaskID, DenialRetryAfter: time.Minute,
		}
		claimed, decision, err := f.st.ClaimAuthorizedPushEffect(t.Context(), params)
		if err != nil || decision != pusheffect.AuthorizedClaimed || claimed == nil {
			t.Fatalf("initial claim=%+v/%q/%v", claimed, decision, err)
		}
		if err := f.st.RecordPushEffectAmbiguous(t.Context(), pusheffect.FailureParams{
			Lease: pusheffect.Lease{
				Scope: claimed.Scope(), LeaseOwner: claimed.LeaseOwner,
				Fence: claimed.Fence,
			},
			Class: "provider_response_unknown",
		}); err != nil {
			t.Fatal(err)
		}
		var databaseNow time.Time
		if err := f.st.pool.QueryRow(t.Context(), `
			SELECT clock_timestamp()+interval '10 minutes'`,
		).Scan(&databaseNow); err != nil {
			t.Fatal(err)
		}
		if _, err := f.st.pool.Exec(t.Context(), `
			UPDATE push_effects SET next_attempt_at=$2 WHERE id=$1`,
			effect.ID, databaseNow,
		); err != nil {
			t.Fatal(err)
		}
		tx, stored := lockedAuthorizedPushEffectForClockTest(t, f, effect)
		defer rollbackPushEffectTx(t.Context(), tx)
		params.LeaseOwner = "reconciliation-clock-test"
		reconciled, authorized, err := updateAuthorizedPushEffectClaim(
			t.Context(), tx, stored, params, true, databaseNow)
		if err != nil || !authorized || reconciled == nil ||
			reconciled.LeaseUntil == nil ||
			!reconciled.LeaseUntil.Equal(databaseNow.Add(params.LeaseDuration)) ||
			!reconciled.UpdatedAt.Equal(databaseNow) {
			t.Fatalf("reconciliation=%+v authorized=%v err=%v", reconciled, authorized, err)
		}
	})

	t.Run("live authority denial", func(t *testing.T) {
		f, effect := authorizedPushEffectFixture(t)
		if _, err := f.st.pool.Exec(t.Context(), `
			UPDATE tenants SET status=$2 WHERE id=$1`,
			effect.TenantID, types.TenantStatusSuspended,
		); err != nil {
			t.Fatal(err)
		}
		tx, stored := lockedAuthorizedPushEffectForClockTest(t, f, effect)
		defer rollbackPushEffectTx(t.Context(), tx)
		databaseNow := stored.CreatedAt.Add(10 * time.Minute)
		params := pusheffect.AuthorizedClaimParams{
			ClaimParams: pusheffect.ClaimParams{
				Scope: stored.Scope(), LeaseOwner: "denial-clock-test",
				LeaseDuration: time.Minute,
			},
			ExpectedTaskID: stored.TaskID, DenialRetryAfter: 2 * time.Minute,
		}
		claimed, authorized, err := updateFreshAuthorizedPushEffectClaim(
			t.Context(), tx, stored, params, databaseNow)
		if err != nil || authorized || claimed != nil {
			t.Fatalf("denial=%+v authorized=%v err=%v", claimed, authorized, err)
		}
		denied, err := loadPushEffectForUpdate(t.Context(), tx, effect.Scope())
		if err != nil {
			t.Fatal(err)
		}
		if !denied.NextAttemptAt.Equal(databaseNow.Add(params.DenialRetryAfter)) ||
			!denied.UpdatedAt.Equal(databaseNow) {
			t.Fatalf("denial clock next=%s updated=%s", denied.NextAttemptAt, denied.UpdatedAt)
		}
	})

	t.Run("canonical admission denial", func(t *testing.T) {
		f, effect := authorizedPushEffectFixture(t)
		tx, stored := lockedAuthorizedPushEffectForClockTest(t, f, effect)
		defer rollbackPushEffectTx(t.Context(), tx)
		databaseNow := stored.CreatedAt.Add(10 * time.Minute)
		const retryAfter = 2 * time.Minute
		if err := deferCanonicalPushEffectRecovery(
			t.Context(), tx, stored, retryAfter, databaseNow,
		); err != nil {
			t.Fatal(err)
		}
		denied, err := loadPushEffectForUpdate(t.Context(), tx, effect.Scope())
		if err != nil {
			t.Fatal(err)
		}
		if !denied.NextAttemptAt.Equal(databaseNow.Add(retryAfter)) ||
			!denied.UpdatedAt.Equal(databaseNow) {
			t.Fatalf("canonical denial clock next=%s updated=%s",
				denied.NextAttemptAt, denied.UpdatedAt)
		}
	})
}

func lockedAuthorizedPushEffectForClockTest(
	t *testing.T,
	f *taskRunSnapshotFixture,
	effect *pusheffect.Effect,
) (pgx.Tx, *pusheffect.Effect) {
	t.Helper()
	tx, err := f.st.beginPushEffectCoordinatorTx(t.Context(), effect.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := lockPushEffectBatchForScope(t.Context(), tx, effect.Scope()); err != nil {
		rollbackPushEffectTx(t.Context(), tx)
		t.Fatal(err)
	}
	stored, err := loadPushEffectForUpdate(t.Context(), tx, effect.Scope())
	if err != nil {
		rollbackPushEffectTx(t.Context(), tx)
		t.Fatal(err)
	}
	return tx, stored
}

func authorizedPushEffectFixture(
	t *testing.T,
) (*taskRunSnapshotFixture, *pusheffect.Effect) {
	return authorizedPushEffectFixtureWithExpiry(t, "", 59*time.Minute+30*time.Second)
}

func authorizedPushEffectFixtureForWorkflow(
	t *testing.T,
	workflowID string,
) (*taskRunSnapshotFixture, *pusheffect.Effect) {
	return authorizedPushEffectFixtureWithExpiry(
		t, workflowID, 59*time.Minute+30*time.Second)
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
			`DELETE FROM task_creation_operations WHERE tenant_id=$1`, f.tenantID)
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
