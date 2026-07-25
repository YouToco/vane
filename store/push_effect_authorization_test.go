package store

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/types"
)

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
	t.Helper()
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	identity := scheduledRunIdentity(
		taskID, f.tenantID, f.userID, "push-effect-auth-"+uuid.NewString())
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
			IdempotencyExpiresAt: time.Now().UTC().
				Truncate(time.Microsecond).Add(time.Hour),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return f, effect
}
