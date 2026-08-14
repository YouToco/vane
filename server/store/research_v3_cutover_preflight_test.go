package store

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/YouToco/vane/server/taskstate"
	"github.com/YouToco/vane/server/types"
)

func TestVerifyEnabledResearchV3ActionAuthorizationPostgres(t *testing.T) {
	f := newResearchBriefFixtureWithWorkflowV3(t,
		taskstate.NotificationThresholdMajorV3, true, nil, "",
		"research-v3-shadow-"+strings.Repeat("f", 64))
	token := strings.Repeat("a", 64)
	tokenSum := sha256.Sum256([]byte(token))
	if _, err := f.st.pool.Exec(t.Context(), `
		INSERT INTO research_v3_delivery_authorities (
		 tenant_id,user_id,task_id,generation,definition_version,
		 definition_digest,target_action_digest,action_authorization_digest,
		 status,enabled_at
		) VALUES ($1,$2,$3,1,$4,$5,$6,$7,'enabled',clock_timestamp())`,
		f.tenantID, f.userID, f.taskID, f.snapshotRef.DefinitionVersion,
		f.snapshotRef.DefinitionDigest, strings.Repeat("b", 64),
		hex.EncodeToString(tokenSum[:])); err != nil {
		t.Fatal(err)
	}
	if err := f.st.VerifyEnabledResearchV3ActionAuthorization(
		t.Context(), f.tenantID, f.userID, f.taskID, token); err != nil {
		t.Fatalf("matching enabled authority: %v", err)
	}
	if err := f.st.VerifyEnabledResearchV3ActionEvidence(
		t.Context(), f.tenantID, f.userID, f.taskID, token,
		strings.Repeat("b", 64)); err != nil {
		t.Fatalf("matching enabled Action evidence: %v", err)
	}
	if err := f.st.VerifyEnabledResearchV3ActionEvidence(
		t.Context(), f.tenantID, f.userID, f.taskID, token,
		strings.Repeat("d", 64)); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("tampered Action digest error=%v", err)
	}
	if err := f.st.VerifyEnabledResearchV3ActionAuthorization(
		t.Context(), f.tenantID, f.userID, f.taskID,
		strings.Repeat("c", 64)); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("tampered token error=%v", err)
	}
	if _, err := f.st.pool.Exec(t.Context(), `
		UPDATE research_v3_delivery_authorities
		   SET status='revoked',revoked_at=clock_timestamp()
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		f.tenantID, f.userID, f.taskID); err != nil {
		t.Fatal(err)
	}
	if err := f.st.VerifyEnabledResearchV3ActionAuthorization(
		t.Context(), f.tenantID, f.userID, f.taskID, token); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("revoked authority error=%v", err)
	}
}

func TestResearchV3CutoverShadowPreflightPostgres(t *testing.T) {
	t.Run("current finalized delivery-dark shadow is eligible", func(t *testing.T) {
		f := newResearchBriefFixtureWithWorkflowV3(t,
			taskstate.NotificationThresholdMajorV3, true, nil, "",
			"research-v3-shadow-"+strings.Repeat("a", 64))
		brief, _ := finalizeResearchBriefFixtureV3(t, f, types.ResearchBriefSignificanceMajorV3)
		head, err := f.st.LoadCurrentResearchApprovedDefinitionV3Head(
			t.Context(), f.tenantID, f.userID, f.taskID)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.st.RequireSuccessfulResearchV3ShadowPreflight(
			t.Context(), f.tenantID, f.userID, f.taskID, head); err != nil {
			t.Fatalf("eligible shadow preflight: %v", err)
		}
		stale := head
		stale.Version++
		if err := f.st.RequireSuccessfulResearchV3ShadowPreflight(
			t.Context(), f.tenantID, f.userID, f.taskID, stale); types.CodeOf(err) != types.CodeConflict {
			t.Fatalf("stale head preflight err=%v", err)
		}
		if _, _, err := f.st.PrepareOrGetResearchBriefDeliveryV3(
			t.Context(), researchDeliveryPrepareParamsV3(f, brief)); err != nil {
			t.Fatal(err)
		}
		if err := f.st.RequireSuccessfulResearchV3ShadowPreflight(
			t.Context(), f.tenantID, f.userID, f.taskID, head); types.CodeOf(err) != types.CodeConflict {
			t.Fatalf("shadow with delivery anchor preflight err=%v", err)
		}
	})

	t.Run("formal authority snapshot cannot create shadow Tool evidence", func(t *testing.T) {
		f := newResearchBriefFixtureWithWorkflowV3(t,
			taskstate.NotificationThresholdMajorV3, false, nil,
			strings.Repeat("b", 64),
			"research-v3-shadow-"+strings.Repeat("c", 64))
		if f.beginErr == nil || types.CodeOf(f.beginErr) != types.CodeDatabase {
			t.Fatalf("authority-bearing shadow Tool admission err=%v", f.beginErr)
		}
		var steps, reservations int
		if err := f.st.pool.QueryRow(t.Context(), `SELECT
			(SELECT count(*) FROM research_run_steps WHERE plan_id=$1),
			(SELECT count(*) FROM research_run_step_spend_reservations WHERE plan_id=$1)`,
			f.planRef.PlanID).Scan(&steps, &reservations); err != nil {
			t.Fatal(err)
		}
		if steps != 0 || reservations != 0 {
			t.Fatalf("denied authority-bearing shadow spent steps=%d reservations=%d",
				steps, reservations)
		}
		var tokens, burst float64
		if err := f.st.pool.QueryRow(t.Context(), `SELECT tokens,burst
			FROM tenant_quota WHERE tenant_id=$1 AND bucket='exa_calls'`,
			f.tenantID).Scan(&tokens, &burst); err != nil {
			t.Fatal(err)
		}
		if tokens != burst {
			t.Fatalf("denied authority-bearing shadow debited quota tokens=%v burst=%v",
				tokens, burst)
		}
	})

	t.Run("non-shadow workflow is ineligible", func(t *testing.T) {
		f := newResearchBriefFixtureV3(t,
			taskstate.NotificationThresholdMajorV3, true)
		finalizeResearchBriefFixtureV3(t, f, types.ResearchBriefSignificanceMajorV3)
		head, err := f.st.LoadCurrentResearchApprovedDefinitionV3Head(
			t.Context(), f.tenantID, f.userID, f.taskID)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.st.RequireSuccessfulResearchV3ShadowPreflight(
			t.Context(), f.tenantID, f.userID, f.taskID, head); types.CodeOf(err) != types.CodeConflict {
			t.Fatalf("non-shadow preflight err=%v", err)
		}
	})
}
