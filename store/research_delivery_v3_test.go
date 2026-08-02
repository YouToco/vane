package store

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
	"github.com/google/uuid"
)

func TestResearchBriefDeliveryV3PostgresReceiptRecoveryAndIsolation(t *testing.T) {
	f := newResearchBriefDeliveryFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
	brief, _ := finalizeResearchBriefFixtureV3(t, f, types.ResearchBriefSignificanceMajorV3)
	params := PrepareResearchBriefDeliveryV3Params{
		Identity: f.identity, SnapshotRef: f.snapshotRef, PlanRef: f.planRef,
		BriefRef: brief, Provider: "feishu", AppIdentity: "cli_a:gen_1",
		ProviderChatID: "oc_owner_chat", Target: "ou_owner",
		Card: []byte(`{"schema":"2.0","body":{"elements":[{"tag":"markdown","content":"major update"}]}}`),
	}
	anchor, effect, err := f.st.PrepareOrGetResearchBriefDeliveryV3(t.Context(), params)
	if err != nil {
		t.Fatalf("prepare delivery: %v", err)
	}
	if anchor.Status != "prepared" || anchor.ID <= 0 || anchor.BatchID <= 0 ||
		anchor.DeliveryID <= 0 || effect == nil || effect.ID != anchor.EffectID ||
		effect.Status != pusheffect.StatusPrepared {
		t.Fatalf("unexpected prepared anchor/effect: %+v %+v", anchor, effect)
	}
	replayed, replayedEffect, err := f.st.PrepareOrGetResearchBriefDeliveryV3(t.Context(), params)
	if err != nil || replayed.ID != anchor.ID || replayedEffect.ID != effect.ID {
		t.Fatalf("exact prepare replay: %+v %+v %v", replayed, replayedEffect, err)
	}
	drift := params
	drift.Card = []byte(`{"schema":"2.0","body":{"elements":[{"tag":"markdown","content":"changed"}]}}`)
	if _, _, err := f.st.PrepareOrGetResearchBriefDeliveryV3(t.Context(), drift); err == nil {
		t.Fatal("card drift replay unexpectedly succeeded")
	}

	if _, err := f.st.ClaimResearchBriefDeliveryV3(t.Context(), f.identity,
		f.snapshotRef, f.planRef, brief, "test/research-delivery", 2*time.Minute); err == nil {
		t.Fatal("delivery claim unexpectedly succeeded without durable cutover authority")
	}
	if _, err := f.st.pool.Exec(t.Context(), `
		INSERT INTO research_v3_delivery_authorities (
		 tenant_id,user_id,task_id,generation,definition_version,
		 definition_digest,target_action_digest,action_authorization_digest,
		 status,enabled_at
		)
		SELECT $1,$2,$3,$4,$5,$6,$7,$8,'enabled',snapshot.created_at-interval '1 second'
		  FROM task_run_snapshots snapshot WHERE snapshot.id=$9`,
		f.identity.TenantID, f.identity.UserID, f.identity.TaskID,
		f.snapshotRef.AuthorityGeneration, f.snapshotRef.DefinitionVersion,
		f.snapshotRef.DefinitionDigest, f.snapshotRef.TargetActionDigest,
		f.snapshotRef.ActionAuthorizationDigest, f.snapshotRef.SnapshotID); err != nil {
		t.Fatalf("enable exact test delivery authority: %v", err)
	}
	// Exercise admission through the real non-owner coordinator role. Missing
	// or wrong user scope must be invisible under RLS; the exact user is visible
	// without granting mutation authority or BYPASSRLS.
	tx, err := f.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(t.Context())
	if _, err := tx.Exec(t.Context(), `
		SELECT set_config('app.tenant_id',$1,true),
		       set_config('app.user_id',$2,true)`,
		fmt.Sprint(f.identity.TenantID), fmt.Sprint(f.identity.UserID+999999)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), `SET LOCAL ROLE vane_push_effect_coordinator`); err != nil {
		t.Fatal(err)
	}
	var role string
	var wrongCount int
	if err := tx.QueryRow(t.Context(), `
		SELECT current_user,(SELECT count(*) FROM research_v3_delivery_authorities
		 WHERE tenant_id=$1 AND task_id=$2)`,
		f.identity.TenantID, f.identity.TaskID).Scan(&role, &wrongCount); err != nil {
		t.Fatal(err)
	}
	if role != "vane_push_effect_coordinator" || wrongCount != 0 {
		t.Fatalf("coordinator user RLS failed: role=%q rows=%d", role, wrongCount)
	}
	if _, err := tx.Exec(t.Context(), `SELECT set_config('app.user_id',$1,true)`,
		fmt.Sprint(f.identity.UserID)); err != nil {
		t.Fatal(err)
	}
	var exactCount int
	var canUpdate, bypassRLS, hasScheduleColumns bool
	if err := tx.QueryRow(t.Context(), `
		SELECT (SELECT count(*) FROM research_v3_delivery_authorities
		         WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3),
		       has_column_privilege('vane_push_effect_coordinator',
		         'research_v3_delivery_authorities','status','UPDATE'),
		       (SELECT rolbypassrls FROM pg_roles
		         WHERE rolname='vane_push_effect_coordinator'),
		       has_column_privilege('vane_push_effect_coordinator',
		         'schedules','approved_definition_version','SELECT') AND
		       has_column_privilege('vane_push_effect_coordinator',
		         'schedules','approved_definition_digest','SELECT') AND
		       has_column_privilege('vane_push_effect_coordinator',
		         'schedules','execution_mode','SELECT')`,
		f.identity.TenantID, f.identity.UserID, f.identity.TaskID).Scan(
		&exactCount, &canUpdate, &bypassRLS, &hasScheduleColumns); err != nil {
		t.Fatal(err)
	}
	if exactCount != 1 || canUpdate || bypassRLS || !hasScheduleColumns {
		t.Fatalf("coordinator admission ACL drift: rows=%d update=%v bypass=%v schedule=%v",
			exactCount, canUpdate, bypassRLS, hasScheduleColumns)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	claimed, err := f.st.ClaimResearchBriefDeliveryV3(t.Context(), f.identity,
		f.snapshotRef, f.planRef, brief, "test/research-delivery", 2*time.Minute)
	if err != nil {
		t.Fatalf("claim delivery: %v", err)
	}
	if claimed.Status != pusheffect.StatusSending || claimed.Fence <= effect.Fence {
		t.Fatalf("unexpected claim: %+v", claimed)
	}
	receipt := pusheffect.SentReceipt{
		Scope: claimed.Scope(), ExpectedFence: claimed.Fence,
		LeaseOwner: claimed.LeaseOwner, ProviderMessageID: "om_v3_delivery_1",
	}
	if err := f.st.RecordPushEffectSentWithDeliveries(t.Context(), receipt); err != nil {
		t.Fatalf("settle delivery receipt: %v", err)
	}
	settled, err := f.st.LoadResearchBriefDeliveryV3(t.Context(), f.identity,
		f.snapshotRef, f.planRef, brief)
	if err != nil {
		t.Fatalf("load settled delivery: %v", err)
	}
	if settled.Status != "sent" || settled.ProviderMessageID != receipt.ProviderMessageID ||
		settled.SentAt == nil || !validResearchRunDigest(settled.ReceiptDigest) {
		t.Fatalf("unexpected settled receipt: %+v", settled)
	}
	// Provider success + local commit followed by response loss: both receipt
	// replay and full prepare replay converge without creating another effect.
	if err := f.st.RecordPushEffectSentWithDeliveries(t.Context(), receipt); err != nil {
		t.Fatalf("sent receipt replay: %v", err)
	}
	afterLoss, afterLossEffect, err := f.st.PrepareOrGetResearchBriefDeliveryV3(t.Context(), params)
	if err != nil || afterLoss.Status != "sent" ||
		afterLoss.ReceiptDigest != settled.ReceiptDigest ||
		afterLossEffect.Status != pusheffect.StatusSent {
		t.Fatalf("response-lost recovery differs: %+v %+v %v", afterLoss, afterLossEffect, err)
	}
	wrongMessage := receipt
	wrongMessage.LeaseOwner = ""
	wrongMessage.ProviderMessageID = "om_v3_delivery_other"
	if err := f.st.RecordPushEffectSentWithDeliveries(t.Context(), wrongMessage); err == nil {
		t.Fatal("different provider receipt replay unexpectedly succeeded")
	}

	var effectCount, anchorCount int
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM push_effects WHERE id=$1`, effect.ID).Scan(&effectCount); err != nil {
		t.Fatal(err)
	}
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM research_brief_deliveries WHERE brief_id=$1`, brief.BriefID).Scan(&anchorCount); err != nil {
		t.Fatal(err)
	}
	if effectCount != 1 || anchorCount != 1 {
		t.Fatalf("replay duplicated durable rows: effects=%d anchors=%d", effectCount, anchorCount)
	}

	foreign := newResearchBriefDeliveryFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
	foreignBrief, _ := finalizeResearchBriefFixtureV3(t, foreign, types.ResearchBriefSignificanceMajorV3)
	if _, err := f.st.LoadResearchBriefDeliveryV3(t.Context(), foreign.identity,
		foreign.snapshotRef, foreign.planRef, foreignBrief); err == nil {
		t.Fatal("foreign owner unexpectedly read delivery")
	}
}

func TestResearchBriefDeliveryV3QuietBriefCreatesNoEffectPostgres(t *testing.T) {
	f := newResearchBriefDeliveryFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
	brief, _ := finalizeResearchBriefFixtureV3(t, f, types.ResearchBriefSignificanceQualifiedV3)
	if brief.DeliveryRequired {
		t.Fatal("fixture should be quiet under major-only threshold")
	}
	params := PrepareResearchBriefDeliveryV3Params{
		Identity: f.identity, SnapshotRef: f.snapshotRef, PlanRef: f.planRef,
		BriefRef: brief, Provider: "feishu", AppIdentity: "cli_a:gen_1",
		ProviderChatID: "oc_owner_chat", Target: "ou_owner",
		Card: []byte(`{"schema":"2.0"}`),
	}
	if _, _, err := f.st.PrepareOrGetResearchBriefDeliveryV3(t.Context(), params); err == nil {
		t.Fatal("quiet Brief unexpectedly prepared a provider effect")
	}
	var count int
	if err := f.st.pool.QueryRow(t.Context(), `
		SELECT count(*) FROM push_effects
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND step_id=$4`,
		f.identity.TenantID, f.identity.UserID, f.identity.TaskID,
		"research-brief-delivery/v3").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("quiet Brief created %d effects", count)
	}
}

func TestResearchBriefDeliveryV3RecoveryRequiresLiveExactAuthorityPostgres(t *testing.T) {
	t.Run("shadow_run_cannot_borrow_task_authority", func(t *testing.T) {
		f := newResearchBriefFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
		brief, _ := finalizeResearchBriefFixtureV3(t, f, types.ResearchBriefSignificanceMajorV3)
		_, effect, err := f.st.PrepareOrGetResearchBriefDeliveryV3(t.Context(),
			researchDeliveryPrepareParamsV3(f, brief))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.st.pool.Exec(t.Context(), `
			INSERT INTO research_v3_delivery_authorities (
			 tenant_id,user_id,task_id,generation,definition_version,
			 definition_digest,target_action_digest,action_authorization_digest,
			 status,enabled_at
			)
			SELECT $1,$2,$3,1,$4,$5,$6,$7,'enabled',snapshot.created_at-interval '1 second'
			  FROM task_run_snapshots snapshot WHERE snapshot.id=$8`,
			f.identity.TenantID, f.identity.UserID, f.identity.TaskID,
			f.snapshotRef.DefinitionVersion, f.snapshotRef.DefinitionDigest,
			strings.Repeat("a", 64), strings.Repeat("b", 64),
			f.snapshotRef.SnapshotID); err != nil {
			t.Fatal(err)
		}
		if _, _, err := f.st.ClaimAuthorizedPushEffect(t.Context(),
			researchDeliveryRecoveryClaimV3(effect, "shadow-borrow")); err == nil {
			t.Fatal("shadow V3 run borrowed the task's live delivery authority")
		}
		assertResearchDeliveryEffectStatusV3(t, f, effect.Scope(),
			pusheffect.StatusPrepared, 0)
	})

	t.Run("prepared_after_revoke", func(t *testing.T) {
		f := newResearchBriefDeliveryFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
		brief, _ := finalizeResearchBriefFixtureV3(t, f, types.ResearchBriefSignificanceMajorV3)
		_, effect, err := f.st.PrepareOrGetResearchBriefDeliveryV3(t.Context(),
			researchDeliveryPrepareParamsV3(f, brief))
		if err != nil {
			t.Fatal(err)
		}
		enableTestResearchDeliveryAuthorityV3(t, f)
		revokeTestResearchDeliveryAuthorityV3(t, f)
		if _, _, err := f.st.ClaimAuthorizedPushEffect(t.Context(),
			researchDeliveryRecoveryClaimV3(effect, "recovery-after-revoke")); err == nil {
			t.Fatal("revoked V3 prepared effect was claimed by generic recovery")
		}
		assertResearchDeliveryEffectStatusV3(t, f, effect.Scope(),
			pusheffect.StatusPrepared, 0)
	})

	t.Run("ambiguous_after_revoke", func(t *testing.T) {
		f := newResearchBriefDeliveryFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
		brief, _ := finalizeResearchBriefFixtureV3(t, f, types.ResearchBriefSignificanceMajorV3)
		_, effect, err := f.st.PrepareOrGetResearchBriefDeliveryV3(t.Context(),
			researchDeliveryPrepareParamsV3(f, brief))
		if err != nil {
			t.Fatal(err)
		}
		enableTestResearchDeliveryAuthorityV3(t, f)
		claimed, decision, err := f.st.ClaimAuthorizedPushEffect(t.Context(),
			researchDeliveryRecoveryClaimV3(effect, "initial-provider-attempt"))
		if err != nil || decision != pusheffect.AuthorizedClaimed || claimed == nil {
			t.Fatalf("initial claim=%+v/%q/%v", claimed, decision, err)
		}
		if err := f.st.RecordPushEffectAmbiguous(t.Context(), pusheffect.FailureParams{
			Lease: pusheffect.Lease{Scope: claimed.Scope(), LeaseOwner: claimed.LeaseOwner,
				Fence: claimed.Fence},
			Class: "provider_response_unknown",
		}); err != nil {
			t.Fatal(err)
		}
		revokeTestResearchDeliveryAuthorityV3(t, f)
		if _, _, err := f.st.ClaimAuthorizedPushEffectReconciliation(t.Context(),
			researchDeliveryRecoveryClaimV3(effect, "reconcile-after-revoke")); err == nil {
			t.Fatal("revoked V3 ambiguous effect was claimed for reconciliation")
		}
		assertResearchDeliveryEffectStatusV3(t, f, effect.Scope(),
			pusheffect.StatusAmbiguous, 1)
	})

	t.Run("receipt_after_revoke", func(t *testing.T) {
		f := newResearchBriefDeliveryFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
		brief, _ := finalizeResearchBriefFixtureV3(t, f, types.ResearchBriefSignificanceMajorV3)
		_, effect, err := f.st.PrepareOrGetResearchBriefDeliveryV3(t.Context(),
			researchDeliveryPrepareParamsV3(f, brief))
		if err != nil {
			t.Fatal(err)
		}
		enableTestResearchDeliveryAuthorityV3(t, f)
		claimed, decision, err := f.st.ClaimAuthorizedPushEffect(t.Context(),
			researchDeliveryRecoveryClaimV3(effect, "provider-success"))
		if err != nil || decision != pusheffect.AuthorizedClaimed || claimed == nil {
			t.Fatalf("claim=%+v/%q/%v", claimed, decision, err)
		}
		revokeTestResearchDeliveryAuthorityV3(t, f)
		if err := f.st.RecordPushEffectSentWithDeliveries(t.Context(), pusheffect.SentReceipt{
			Scope: claimed.Scope(), ExpectedFence: claimed.Fence,
			LeaseOwner: claimed.LeaseOwner, ProviderMessageID: "om_after_revoke",
		}); err != nil {
			t.Fatalf("positive provider receipt was blocked by revoke: %v", err)
		}
		assertResearchDeliveryEffectStatusV3(t, f, effect.Scope(),
			pusheffect.StatusSent, 1)
	})
}

func TestResearchBriefDeliveryV3CorruptionFailsClosedPostgres(t *testing.T) {
	t.Run("tampered_brief_reference", func(t *testing.T) {
		f := newResearchBriefDeliveryFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
		brief, _ := finalizeResearchBriefFixtureV3(t, f, types.ResearchBriefSignificanceMajorV3)
		params := researchDeliveryPrepareParamsV3(f, brief)
		anchor, effect, err := f.st.PrepareOrGetResearchBriefDeliveryV3(t.Context(), params)
		if err != nil {
			t.Fatal(err)
		}
		enableTestResearchDeliveryAuthorityV3(t, f)
		if _, err := f.st.pool.Exec(t.Context(), `DELETE FROM research_brief_deliveries WHERE id=$1`,
			anchor.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := f.st.pool.Exec(t.Context(), `
			INSERT INTO research_brief_deliveries (
			 tenant_id,user_id,task_id,run_snapshot_id,plan_id,brief_id,
			 temporal_workflow_id,temporal_run_id,brief_reference_digest,brief_digest,
			 card_digest,batch_id,delivery_id,effect_id,schema_version
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
			f.identity.TenantID, f.identity.UserID, f.identity.TaskID,
			anchor.RunSnapshotID, anchor.PlanID, anchor.BriefID,
			f.identity.TemporalWorkflowID, f.identity.TemporalRunID,
			strings.Repeat("b", 64), anchor.BriefDigest, anchor.CardDigest,
			anchor.BatchID, anchor.DeliveryID, anchor.EffectID,
			researchBriefDeliverySchemaV3); err != nil {
			t.Fatal(err)
		}
		claimed, decision, err := f.st.ClaimAuthorizedPushEffect(t.Context(),
			researchDeliveryRecoveryClaimV3(effect, "tampered-recovery"))
		if err != nil || claimed != nil || decision != pusheffect.AuthorizedClaimDenied {
			t.Fatalf("tampered ref recovery=%+v/%q/%v", claimed, decision, err)
		}
		assertResearchDeliveryEffectStatusV3(t, f, effect.Scope(),
			pusheffect.StatusPrepared, 0)
	})

	t.Run("missing_anchor_settlement", func(t *testing.T) {
		f := newResearchBriefDeliveryFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
		brief, _ := finalizeResearchBriefFixtureV3(t, f, types.ResearchBriefSignificanceMajorV3)
		anchor, effect, err := f.st.PrepareOrGetResearchBriefDeliveryV3(t.Context(),
			researchDeliveryPrepareParamsV3(f, brief))
		if err != nil {
			t.Fatal(err)
		}
		enableTestResearchDeliveryAuthorityV3(t, f)
		claimed, err := f.st.ClaimResearchBriefDeliveryV3(t.Context(), f.identity,
			f.snapshotRef, f.planRef, brief, "provider-missing-anchor", 2*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.st.pool.Exec(t.Context(), `DELETE FROM research_brief_deliveries WHERE id=$1`,
			anchor.ID); err != nil {
			t.Fatal(err)
		}
		if err := f.st.RecordPushEffectSentWithDeliveries(t.Context(), pusheffect.SentReceipt{
			Scope: claimed.Scope(), ExpectedFence: claimed.Fence,
			LeaseOwner: claimed.LeaseOwner, ProviderMessageID: "om_missing_anchor",
		}); err == nil {
			t.Fatal("V3 settlement treated a missing anchor as legacy")
		}
		assertResearchDeliveryEffectStatusV3(t, f, effect.Scope(),
			pusheffect.StatusSending, 1)
	})
}

func researchDeliveryPrepareParamsV3(
	f researchBriefFixtureV3, brief types.ResearchBriefRefV3,
) PrepareResearchBriefDeliveryV3Params {
	return PrepareResearchBriefDeliveryV3Params{
		Identity: f.identity, SnapshotRef: f.snapshotRef, PlanRef: f.planRef,
		BriefRef: brief, Provider: "feishu", AppIdentity: "cli_a:gen_1",
		ProviderChatID: "oc_owner_chat", Target: "ou_owner",
		Card: []byte(`{"schema":"2.0","body":{"elements":[]}}`),
	}
}

func newResearchBriefDeliveryFixtureV3(
	t *testing.T, threshold taskstate.NotificationThresholdV3, completeEvidence bool,
) researchBriefFixtureV3 {
	t.Helper()
	return newResearchBriefFixtureWithAuthorityV3(t, threshold, completeEvidence,
		nil, "research-v3-delivery-"+uuid.NewString())
}

func enableTestResearchDeliveryAuthorityV3(t *testing.T, f researchBriefFixtureV3) {
	t.Helper()
	if _, err := f.st.pool.Exec(t.Context(), `
		INSERT INTO research_v3_delivery_authorities (
		 tenant_id,user_id,task_id,generation,definition_version,
		 definition_digest,target_action_digest,action_authorization_digest,
		 status,enabled_at
		)
		SELECT $1,$2,$3,$4,$5,$6,$7,$8,'enabled',snapshot.created_at-interval '1 second'
		  FROM task_run_snapshots snapshot WHERE snapshot.id=$9`,
		f.identity.TenantID, f.identity.UserID, f.identity.TaskID,
		f.snapshotRef.AuthorityGeneration, f.snapshotRef.DefinitionVersion,
		f.snapshotRef.DefinitionDigest, f.snapshotRef.TargetActionDigest,
		f.snapshotRef.ActionAuthorizationDigest, f.snapshotRef.SnapshotID); err != nil {
		t.Fatalf("enable exact test delivery authority: %v", err)
	}
}

func revokeTestResearchDeliveryAuthorityV3(t *testing.T, f researchBriefFixtureV3) {
	t.Helper()
	if _, err := f.st.pool.Exec(t.Context(), `
		UPDATE research_v3_delivery_authorities
		   SET status='revoked',revoked_at=clock_timestamp()
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND status='enabled'`,
		f.identity.TenantID, f.identity.UserID, f.identity.TaskID); err != nil {
		t.Fatalf("revoke exact test delivery authority: %v", err)
	}
}

func researchDeliveryRecoveryClaimV3(
	effect *pusheffect.Effect, owner string,
) pusheffect.AuthorizedClaimParams {
	return pusheffect.AuthorizedClaimParams{
		ClaimParams: pusheffect.ClaimParams{Scope: effect.Scope(), LeaseOwner: owner,
			LeaseDuration: 2 * time.Minute},
		ExpectedTaskID: effect.TaskID, DenialRetryAfter: time.Minute,
	}
}

func assertResearchDeliveryEffectStatusV3(
	t *testing.T, f researchBriefFixtureV3, scope pusheffect.Scope,
	wantStatus pusheffect.Status, wantAttempt int,
) {
	t.Helper()
	effect, err := f.st.LoadPushEffect(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	if effect.Status != wantStatus || effect.Attempt != wantAttempt {
		t.Fatalf("effect status=%q attempt=%d want=%q/%d",
			effect.Status, effect.Attempt, wantStatus, wantAttempt)
	}
}

func TestResearchBriefDeliveryReceiptDigestBindsProviderEvidencePostgres(t *testing.T) {
	// A small structural guard makes the required digest fields explicit in
	// production SQL; mutation of any listed coordinate must change the hash.
	source := productionStoreFunction(t, "settleResearchBriefDeliveryReceiptV3")
	for _, required := range []string{
		"vane.research-brief-delivery-receipt/v3", "effect_id",
		"brief_digest", "card_digest", "$4", "status='sent'",
	} {
		if !functionContainsString(source, required) {
			t.Fatalf("receipt settlement no longer binds %q", required)
		}
	}
}
