package store

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/types"
)

func TestProfileClaimAuthorityAndEvolverRegression(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		requireDatabaseCapability(t)
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	u, err := st.UpsertUserByOpenID(
		t.Context(), "profile_claim_"+uuid.NewString(), "claim")
	if err != nil {
		t.Fatal(err)
	}
	attachTenant(t, st, u.ID)
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		for _, table := range []string{
			"profile_claim_receipts", "profile_claim_events", "profile_claims",
			"profile_claim_states", "profile_edit_receipts",
			"profile_edit_revisions", "profiles",
		} {
			cleanupExec(ctx, t, st, "DELETE FROM "+table+" WHERE user_id=$1", u.ID)
		}
		cleanupExec(ctx, t, st, `DELETE FROM memberships WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id=$1`, u.ID)
	})

	industry, occupation := "AI", "Engineer"
	initial, err := st.PatchProfile(
		t.Context(), 1, u.ID, nil,
		types.ProfileEditPatch{
			Industry: &industry, Occupation: &occupation,
			Tags: ptrStrings([]string{"A", "B"}),
		},
		"claim-create", strings.Repeat("1", 64))
	if err != nil {
		t.Fatal(err)
	}
	list, err := st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if list.Version != 0 {
		t.Fatalf("initial version=%d", list.Version)
	}
	for _, claim := range list.Claims {
		if claim.Source.State != "manual" {
			t.Fatalf("explicit profile intake not marked manual: %+v", claim)
		}
	}
	industryClaim := findClaim(t, list.Claims, "industry", "AI", true)
	correctInput := types.ProfileClaimAction{
		ExpectedVersion: 0, Action: "correct",
		ClaimID: parseTestID(t, industryClaim.ID), Value: "Biotech",
	}
	type actionOutcome struct {
		result *types.ProfileClaimActionResult
		err    error
	}
	firstAttempts := make(chan actionOutcome, 2)
	for range 2 {
		go func() {
			result, err := st.ApplyProfileClaimAction(
				t.Context(), 1, u.ID, correctInput,
				"claim-correct", strings.Repeat("2", 64))
			firstAttempts <- actionOutcome{result: result, err: err}
		}()
	}
	var correct *types.ProfileClaimActionResult
	for range 2 {
		outcome := <-firstAttempts
		if outcome.err != nil {
			t.Fatalf("concurrent same-key attempt: %v", outcome.err)
		}
		if correct == nil {
			correct = outcome.result
		} else if outcome.result.EventID != correct.EventID ||
			outcome.result.Version != correct.Version {
			t.Fatalf("same-key attempts diverged: %+v / %+v", correct, outcome.result)
		}
	}
	if correct.Version != 1 || correct.Profile.Industry != "Biotech" ||
		!strings.Contains(correct.Profile.Summary, "人工纠正：行业=Biotech") {
		t.Fatalf("correct projection=%+v", correct)
	}
	// 响应丢失重放必须返回原响应，不能第二次递增 version。
	replay, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedVersion: 0, Action: "correct",
			ClaimID: parseTestID(t, industryClaim.ID), Value: "Biotech",
		},
		"claim-correct", strings.Repeat("2", 64))
	if err != nil || replay.Version != 1 || replay.EventID != correct.EventID {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	// 另一个 tenant/user 即使猜中 claim/event id，也不能读取或补偿。
	var tenantB int64
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO tenants DEFAULT VALUES RETURNING id`).Scan(&tenantB); err != nil {
		t.Fatal(err)
	}
	userB, err := st.UpsertUserByOpenID(
		t.Context(), "profile_claim_b_"+uuid.NewString(), "claim-b")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`,
		tenantB, userB.ID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		for _, table := range []string{
			"profile_claim_receipts", "profile_claim_events", "profile_claims",
			"profile_claim_states", "profile_edit_receipts",
			"profile_edit_revisions", "profiles",
		} {
			cleanupExec(ctx, t, st, "DELETE FROM "+table+" WHERE user_id=$1", userB.ID)
		}
		cleanupExec(ctx, t, st, `DELETE FROM memberships WHERE user_id=$1`, userB.ID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id=$1`, userB.ID)
		cleanupExec(ctx, t, st, `DELETE FROM tenants WHERE id=$1`, tenantB)
	})
	bIndustry := "Finance"
	if _, err := st.PatchProfile(
		t.Context(), tenantB, userB.ID, nil,
		types.ProfileEditPatch{Industry: &bIndustry},
		"claim-b-create", strings.Repeat("a", 64),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ListProfileClaims(t.Context(), tenantB, userB.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.GetProfileEvolutionBase(
		t.Context(), tenantB, u.ID,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("wrong-tenant evolution base leaked: %v", err)
	}
	if _, err := st.ApplyProfileClaimAction(
		t.Context(), tenantB, userB.ID,
		types.ProfileClaimAction{
			ExpectedVersion: 0, Action: "pin",
			ClaimID: parseTestID(t, industryClaim.ID),
		},
		"claim-cross-claim", strings.Repeat("b", 64),
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("cross-tenant claim target accepted: %v", err)
	}
	if _, err := st.ApplyProfileClaimAction(
		t.Context(), tenantB, userB.ID,
		types.ProfileClaimAction{
			ExpectedVersion: 0, Action: "revoke",
			EventID: parseTestID(t, correct.EventID),
		},
		"claim-cross-event", strings.Repeat("c", 64),
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("cross-user revoke target accepted: %v", err)
	}

	if err := st.EvolveProfile(
		t.Context(), u.ID, "模型摘要", []string{"A", "C"}, 10,
		correct.Profile.UpdatedAt, 0,
		0, correct.Version,
	); err != nil {
		t.Fatalf("evolve after correction: %v", err)
	}
	p, err := st.GetProfileForTenant(t.Context(), 1, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if p.Industry != "Biotech" ||
		!strings.Contains(p.Summary, "模型摘要") ||
		!strings.Contains(p.Summary, "人工纠正：行业=Biotech") {
		t.Fatalf("evolver overwrote active correction: %+v", p)
	}
	baseSummary, baseTags, err := st.GetProfileEvolutionBase(
		t.Context(), 1, u.ID)
	if err != nil || baseSummary != "模型摘要" ||
		strings.Join(baseTags, ",") != "C" {
		t.Fatalf("pure evolution base summary=%q tags=%v err=%v",
			baseSummary, baseTags, err)
	}
	list, err = st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil || list.Version != 2 {
		t.Fatalf("after evolve version=%v err=%v", list, err)
	}
	if len(list.Events) == 0 || list.Events[0].ID != correct.EventID ||
		!list.Events[0].Revocable || list.Events[0].Revoked {
		t.Fatalf("GET did not expose active revocable event: %+v", list.Events)
	}
	if claim := findClaim(t, list.Claims, "tag", "A", true); claim.Source.State != "manual" {
		t.Fatalf("manual intake authority did not win duplicate evidence: %+v", claim)
	}
	cClaimEvidence := findClaim(t, list.Claims, "tag", "C", true)
	if cClaimEvidence.Source.State != "evidence" ||
		cClaimEvidence.Source.RefType != "feedback_range" {
		t.Fatalf("evolved source missing batch provenance: %+v", cClaimEvidence)
	}

	aClaim := findClaim(t, list.Claims, "tag", "A", true)
	pinned, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedVersion: 2, Action: "pin",
			ClaimID: parseTestID(t, aClaim.ID),
		},
		"claim-pin", strings.Repeat("3", 64))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.EvolveProfile(
		t.Context(), u.ID, "模型摘要 2\n人工纠正：不应沉淀",
		[]string{"C", "D"}, 20, pinned.Profile.UpdatedAt, 10,
		0, pinned.Version,
	); err != nil {
		t.Fatalf("evolve after pin: %v", err)
	}
	p, err = st.GetProfileForTenant(t.Context(), 1, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(p.Tags, ",") != "A,B,C,D" {
		t.Fatalf("pin must retain and order tag first: %v", p.Tags)
	}
	if strings.Count(p.Summary, "人工纠正：") != 1 ||
		strings.Contains(p.Summary, "不应沉淀") {
		t.Fatalf("derived manual segment was promoted into evidence: %q", p.Summary)
	}

	list, err = st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil || list.Version != 4 {
		t.Fatalf("after second evolve version=%+v err=%v", list, err)
	}
	cClaim := findClaim(t, list.Claims, "tag", "C", true)
	suppressed, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedVersion: 4, Action: "suppress",
			ClaimID: parseTestID(t, cClaim.ID),
		},
		"claim-suppress", strings.Repeat("4", 64))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(suppressed.Profile.Tags, ","), "C") {
		t.Fatalf("suppressed C remains: %v", suppressed.Profile.Tags)
	}
	suppressEventID := parseTestID(t, suppressed.EventID)
	revoked, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedVersion: 5, Action: "revoke", EventID: suppressEventID,
		},
		"claim-revoke", strings.Repeat("5", 64))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(revoked.Profile.Tags, ","), "C") {
		t.Fatalf("revoke did not compensate suppress: %v", revoked.Profile.Tags)
	}
	listAfterRevoke, err := st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	var sawRevoked bool
	for _, event := range listAfterRevoke.Events {
		if event.ID == suppressed.EventID {
			sawRevoked = event.Revoked && !event.Revocable
		}
	}
	if !sawRevoked {
		t.Fatalf("GET history did not expose revoked event: %+v", listAfterRevoke.Events)
	}
	if _, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedVersion: 6, Action: "revoke", EventID: suppressEventID,
		},
		"claim-revoke-again", strings.Repeat("6", 64),
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("already revoked event accepted: %v", err)
	}

	// 两个相同 expected_version 的写只能有一个提交。
	list, err = st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	occupationClaim := findClaim(t, list.Claims, "occupation", "Engineer", true)
	occupationClaimID := parseTestID(t, occupationClaim.ID)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i, value := range []string{"Researcher", "Founder"} {
		wg.Add(1)
		go func(i int, value string) {
			defer wg.Done()
			_, err := st.ApplyProfileClaimAction(
				t.Context(), 1, u.ID,
				types.ProfileClaimAction{
					ExpectedVersion: list.Version, Action: "correct",
					ClaimID: occupationClaimID, Value: value,
				},
				"claim-race-"+value, strings.Repeat(string(rune('7'+i)), 64))
			errs <- err
		}(i, value)
	}
	wg.Wait()
	close(errs)
	var success, conflict int
	for err := range errs {
		switch {
		case err == nil:
			success++
		case errors.Is(err, types.ErrConflict):
			conflict++
		default:
			t.Fatalf("unexpected concurrent error: %v", err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("CAS race success/conflict=%d/%d", success, conflict)
	}

	_ = initial
}

func TestSummaryClaimSentenceCorrectionsSurviveEvolution(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		requireDatabaseCapability(t)
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	u, err := st.UpsertUserByOpenID(
		t.Context(), "summary_claim_"+uuid.NewString(), "summary-claim")
	if err != nil {
		t.Fatal(err)
	}
	attachTenant(t, st, u.ID)
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		for _, table := range []string{
			"profile_claim_receipts", "profile_claim_events", "profile_claims",
			"profile_claim_states", "profile_edit_receipts",
			"profile_edit_revisions", "profiles",
		} {
			cleanupExec(ctx, t, st, "DELETE FROM "+table+" WHERE user_id=$1", u.ID)
		}
		cleanupExec(ctx, t, st, `DELETE FROM memberships WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id=$1`, u.ID)
	})
	industry := "AI"
	created, err := st.PatchProfile(
		t.Context(), 1, u.ID, nil,
		types.ProfileEditPatch{Industry: &industry},
		"summary-create", strings.Repeat("1", 64))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.EvolveProfile(
		t.Context(), u.ID, "安全事实。污染画像！另一个事实。",
		[]string{"A"}, 10, created.UpdatedAt, 0,
		0, 0,
	); err != nil {
		t.Fatal(err)
	}
	list, err := st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil || list.Version != 1 {
		t.Fatalf("first summary claims=%+v err=%v", list, err)
	}
	if findClaim(t, list.Claims, "summary", "安全事实。", true).Source.State != "evidence" {
		t.Fatal("summary sentence source is not evidence")
	}
	polluted := findClaim(t, list.Claims, "summary", "污染画像！", true)
	corrected, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedVersion: 1, Action: "correct",
			ClaimID: parseTestID(t, polluted.ID), Value: "已确认的新事实！",
		},
		"summary-correct", strings.Repeat("2", 64))
	if err != nil {
		t.Fatal(err)
	}
	other := findClaim(t, corrected.Claims, "summary", "另一个事实。", true)
	suppressed, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedVersion: 2, Action: "suppress",
			ClaimID: parseTestID(t, other.ID),
		},
		"summary-suppress", strings.Repeat("3", 64))
	if err != nil {
		t.Fatal(err)
	}
	replacement := findClaim(t, suppressed.Claims, "summary", "已确认的新事实！", true)
	pinned, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedVersion: 3, Action: "pin",
			ClaimID: parseTestID(t, replacement.ID),
		},
		"summary-pin", strings.Repeat("4", 64))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(pinned.Profile.Summary, "summary=") ||
		strings.Contains(pinned.Profile.Summary, "污染画像") ||
		strings.Contains(pinned.Profile.Summary, "另一个事实") {
		t.Fatalf("sentence correction leaked metadata/pollution: %q", pinned.Profile.Summary)
	}
	if err := st.EvolveProfile(
		t.Context(), u.ID,
		"安全事实更新。污染画像！另一个事实。新增事实。",
		[]string{"A", "B"}, 20, pinned.Profile.UpdatedAt, 10,
		0, pinned.Version,
	); err != nil {
		t.Fatal(err)
	}
	p, err := st.GetProfileForTenant(t.Context(), 1, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"安全事实更新。", "已确认的新事实！", "新增事实。"} {
		if !strings.Contains(p.Summary, want) {
			t.Fatalf("summary lost %q: %q", want, p.Summary)
		}
	}
	for _, forbidden := range []string{"污染画像", "另一个事实", "排除摘要"} {
		if strings.Contains(p.Summary, forbidden) {
			t.Fatalf("summary leaked %q: %q", forbidden, p.Summary)
		}
	}
}

func TestProfileClaimRevokeDependencyChain(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		requireDatabaseCapability(t)
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	u, err := st.UpsertUserByOpenID(
		t.Context(), "claim_revoke_chain_"+uuid.NewString(), "claim-chain")
	if err != nil {
		t.Fatal(err)
	}
	attachTenant(t, st, u.ID)
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		for _, table := range []string{
			"profile_claim_receipts", "profile_claim_events", "profile_claims",
			"profile_claim_states", "profile_edit_receipts",
			"profile_edit_revisions", "profiles",
		} {
			cleanupExec(ctx, t, st, "DELETE FROM "+table+" WHERE user_id=$1", u.ID)
		}
		cleanupExec(ctx, t, st, `DELETE FROM memberships WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id=$1`, u.ID)
	})

	industry := "AI"
	if _, err := st.PatchProfile(
		t.Context(), 1, u.ID, nil,
		types.ProfileEditPatch{Industry: &industry},
		"chain-create", strings.Repeat("1", 64),
	); err != nil {
		t.Fatal(err)
	}
	list, err := st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	original := findClaim(t, list.Claims, "industry", "AI", true)
	corrected, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedVersion: 0, Action: "correct",
			ClaimID: parseTestID(t, original.ID), Value: "Biotech",
		},
		"chain-correct", strings.Repeat("2", 64),
	)
	if err != nil {
		t.Fatal(err)
	}
	replacement := findClaim(t, corrected.Claims, "industry", "Biotech", true)
	pinned, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedVersion: 1, Action: "pin",
			ClaimID: parseTestID(t, replacement.ID),
		},
		"chain-pin", strings.Repeat("3", 64),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedVersion: 2, Action: "revoke",
			EventID: parseTestID(t, corrected.EventID),
		},
		"chain-revoke-blocked", strings.Repeat("4", 64),
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("correction with effective dependent pin was revocable: %v", err)
	}
	if _, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedVersion: 2, Action: "revoke",
			EventID: parseTestID(t, pinned.EventID),
		},
		"chain-revoke-pin", strings.Repeat("5", 64),
	); err != nil {
		t.Fatal(err)
	}
	list, err = st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	var correctionRevocable bool
	for _, event := range list.Events {
		if event.ID == corrected.EventID {
			correctionRevocable = event.Revocable && !event.Revoked
		}
	}
	if !correctionRevocable {
		t.Fatalf("GET did not release correction after dependent revoke: %+v", list.Events)
	}
	recovered, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedVersion: 3, Action: "revoke",
			EventID: parseTestID(t, corrected.EventID),
		},
		"chain-revoke-correct", strings.Repeat("6", 64),
	)
	if err != nil {
		t.Fatalf("POST disagreed with GET revocable state: %v", err)
	}
	if recovered.Version != 4 || recovered.Profile.Industry != "AI" {
		t.Fatalf("compensating chain recovery=%+v", recovered)
	}
}

func TestProfileClaimLedgerSurvivesMembershipRevocation(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		requireDatabaseCapability(t)
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	u, err := st.UpsertUserByOpenID(
		t.Context(), "claim_membership_"+uuid.NewString(), "claim-membership")
	if err != nil {
		t.Fatal(err)
	}
	attachTenant(t, st, u.ID)
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		for _, table := range []string{
			"profile_claim_receipts", "profile_claim_events", "profile_claims",
			"profile_claim_states", "profile_edit_receipts",
			"profile_edit_revisions", "profiles",
		} {
			cleanupExec(ctx, t, st, "DELETE FROM "+table+" WHERE user_id=$1", u.ID)
		}
		cleanupExec(ctx, t, st, `DELETE FROM memberships WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id=$1`, u.ID)
	})
	industry := "AI"
	if _, err := st.PatchProfile(
		t.Context(), 1, u.ID, nil,
		types.ProfileEditPatch{Industry: &industry},
		"membership-create", strings.Repeat("1", 64),
	); err != nil {
		t.Fatal(err)
	}
	list, err := st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	claim := findClaim(t, list.Claims, "industry", "AI", true)
	pinned, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedVersion: 0, Action: "pin",
			ClaimID: parseTestID(t, claim.ID),
		},
		"membership-pin", strings.Repeat("2", 64),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`DELETE FROM memberships WHERE tenant_id=1 AND user_id=$1`, u.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ListProfileClaims(
		t.Context(), 1, u.ID); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("revoked direct claim read remained authorized: %v", err)
	}
	if _, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedVersion: 1, Action: "suppress",
			ClaimID: parseTestID(t, claim.ID),
		},
		"membership-revoked", strings.Repeat("3", 64),
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("revoked direct claim write remained authorized: %v", err)
	}
	var profiles, states, claims, events, receipts int
	if err := st.pool.QueryRow(t.Context(), `
		SELECT
		  (SELECT count(*) FROM profiles WHERE user_id=$1),
		  (SELECT count(*) FROM profile_claim_states WHERE user_id=$1),
		  (SELECT count(*) FROM profile_claims WHERE user_id=$1),
		  (SELECT count(*) FROM profile_claim_events WHERE user_id=$1),
		  (SELECT count(*) FROM profile_claim_receipts WHERE user_id=$1)`,
		u.ID,
	).Scan(&profiles, &states, &claims, &events, &receipts); err != nil {
		t.Fatal(err)
	}
	if profiles != 1 || states != 1 || claims == 0 || events != 1 || receipts != 1 {
		t.Fatalf("membership revoke deleted audit profile/state/claims/events/receipts=%d/%d/%d/%d/%d",
			profiles, states, claims, events, receipts)
	}
	if _, err := st.pool.Exec(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role) VALUES(1,$1,'owner')`,
		u.ID); err != nil {
		t.Fatal(err)
	}
	list, err = st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil || list.Version != 1 || len(list.Events) != 1 ||
		list.Events[0].ID != pinned.EventID {
		t.Fatalf("re-added member lost claim audit: %+v err=%v", list, err)
	}
}

func TestProfileClaimMembershipRevokeSerializesWithScopedTransaction(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		requireDatabaseCapability(t)
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	u, err := st.UpsertUserByOpenID(
		t.Context(), "claim_revoke_race_"+uuid.NewString(), "claim-revoke-race")
	if err != nil {
		t.Fatal(err)
	}
	attachTenant(t, st, u.ID)
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, st, `DELETE FROM memberships WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id=$1`, u.ID)
	})

	tx, err := st.beginProfileClaimScopedTx(t.Context(), 1, false, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	deleted := make(chan error, 1)
	go func() {
		_, deleteErr := st.pool.Exec(context.Background(),
			`DELETE FROM memberships WHERE tenant_id=1 AND user_id=$1`, u.ID)
		deleted <- deleteErr
	}()
	select {
	case err := <-deleted:
		_ = tx.Rollback(t.Context())
		t.Fatalf("membership revoke did not wait for authorized tx: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-deleted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("membership revoke stayed blocked after scoped tx commit")
	}
	if _, err := st.beginProfileClaimScopedTx(
		t.Context(), 1, false, u.ID,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("post-revoke scoped transaction remained authorized: %v", err)
	}
}

func TestProfileClaimMutationSummaryBoundAndDuplicatePin(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		requireDatabaseCapability(t)
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	u, err := st.UpsertUserByOpenID(
		t.Context(), "claim_bound_"+uuid.NewString(), "claim-bound")
	if err != nil {
		t.Fatal(err)
	}
	attachTenant(t, st, u.ID)
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		for _, table := range []string{
			"profile_claim_receipts", "profile_claim_events", "profile_claims",
			"profile_claim_states", "profile_edit_receipts",
			"profile_edit_revisions", "profiles",
		} {
			cleanupExec(ctx, t, st, "DELETE FROM "+table+" WHERE user_id=$1", u.ID)
		}
		cleanupExec(ctx, t, st, `DELETE FROM memberships WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id=$1`, u.ID)
	})
	industry := "AI"
	created, err := st.PatchProfile(
		t.Context(), 1, u.ID, nil,
		types.ProfileEditPatch{Industry: &industry},
		"bound-create", strings.Repeat("1", 64),
	)
	if err != nil {
		t.Fatal(err)
	}
	baseSummary := strings.Repeat("甲", 240) +
		strings.Repeat("乙", 240) + strings.Repeat("丙", 20)
	if err := st.EvolveProfile(
		t.Context(), u.ID, baseSummary, nil,
		10, created.UpdatedAt, 0,
		0, 0,
	); err != nil {
		t.Fatal(err)
	}
	list, err := st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil || list.Version != 1 {
		t.Fatalf("500-rune generation=%+v err=%v", list, err)
	}
	short := findClaim(t, list.Claims, "summary", strings.Repeat("丙", 20), true)
	if _, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedVersion: 1, Action: "correct",
			ClaimID: parseTestID(t, short.ID), Value: strings.Repeat("戊", 240),
		},
		"bound-overflow", strings.Repeat("2", 64),
	); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("summary overflow accepted: %v", err)
	}
	list, err = st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil || list.Version != 1 {
		t.Fatalf("overflow was not atomic: %+v err=%v", list, err)
	}
	first := findClaim(t, list.Claims, "summary", strings.Repeat("甲", 240), true)
	pinned, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedVersion: 1, Action: "pin",
			ClaimID: parseTestID(t, first.ID),
		},
		"bound-pin", strings.Repeat("3", 64),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedVersion: 2, Action: "pin",
			ClaimID: parseTestID(t, first.ID),
		},
		"bound-pin-again", strings.Repeat("4", 64),
	); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("duplicate pin accepted: %v", err)
	}
	// Simulate a later generation created by a pre-fit producer: it has a new
	// physical claim ID but the same semantic value as the retained pin.
	claimTx, err := st.beginProfileClaimScopedTx(t.Context(), 1, true, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := bindProfileEpochTx(t.Context(), claimTx, 0); err != nil {
		t.Fatal(err)
	}
	var duplicateID int64
	if err := claimTx.QueryRow(t.Context(), `
		INSERT INTO profile_claims
		    (tenant_id,user_id,profile_epoch,field_name,claim_value,source_state,
		     source_ref_type,source_ref,generation)
		VALUES(1,$1,0,'summary',$2,'evidence',
		       'feedback_range','feedbacks:(10,20]',20)
		RETURNING id`,
		u.ID, strings.Repeat("甲", 240)).Scan(&duplicateID); err != nil {
		t.Fatal(err)
	}
	if err := claimTx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID,
		types.ProfileClaimAction{
			ExpectedVersion: 2, Action: "pin", ClaimID: duplicateID,
		},
		"bound-semantic-pin", strings.Repeat("5", 64),
	); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("cross-generation semantic duplicate pin accepted: %v", err)
	}
	if err := st.EvolveProfile(
		t.Context(), u.ID, strings.Repeat("丁", 500), nil,
		20, pinned.Profile.UpdatedAt, 10,
		0, pinned.Version,
	); err != nil {
		t.Fatalf("budget-fit evolution failed: %v", err)
	}
	list, err = st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil || list.Version != 3 {
		t.Fatalf("fit evolution did not advance version: %+v err=%v", list, err)
	}
	var semanticActive int
	for _, claim := range list.Claims {
		if claim.Field == "summary" &&
			claim.Value == strings.Repeat("甲", 240) && claim.Active {
			semanticActive++
		}
	}
	if semanticActive != 1 {
		t.Fatalf("duplicate summary representatives active=%d", semanticActive)
	}
	p, err := st.GetProfileForTenant(t.Context(), 1, u.ID)
	if err != nil || utf8.RuneCountInString(stripDerivedManualSegment(p.Summary)) != 500 ||
		p.LastEvolvedFeedbackID != 20 ||
		!strings.Contains(p.Summary, strings.Repeat("甲", 240)) ||
		!strings.Contains(p.Summary, strings.Repeat("丁", 260)) {
		t.Fatalf("fit evolution lost authority/budget/cursor: %+v err=%v", p, err)
	}
}

func TestLegacyProfileTenantResolutionFailsClosed(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		requireDatabaseCapability(t)
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	u, err := st.UpsertUserByOpenID(
		t.Context(), "claim_multi_member_"+uuid.NewString(), "multi-member")
	if err != nil {
		t.Fatal(err)
	}
	attachTenant(t, st, u.ID)
	var tenant2 int64
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO tenants DEFAULT VALUES RETURNING id`).Scan(&tenant2); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'member')`,
		tenant2, u.ID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		for _, table := range []string{
			"profile_claim_receipts", "profile_claim_events", "profile_claims",
			"profile_claim_states", "profile_epochs", "profiles",
		} {
			cleanupExec(ctx, t, st, "DELETE FROM "+table+" WHERE user_id=$1", u.ID)
		}
		cleanupExec(ctx, t, st, `DELETE FROM memberships WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM tenants WHERE id=$1`, tenant2)
	})
	industry := "AI"
	if _, err := st.UpsertProfileFields(
		t.Context(), u.ID, &industry, nil, nil,
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("multi-membership first intake guessed a tenant: %v", err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`, tenant2, u.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertProfileFields(
		t.Context(), u.ID, &industry, nil, nil,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'member')`,
		tenant2, u.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.GetProfileEvolutionBase(
		t.Context(), 0, u.ID,
	); err != nil {
		t.Fatalf("profile tenant did not win multi-membership resolution: %v", err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`DELETE FROM memberships WHERE tenant_id=1 AND user_id=$1`, u.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.GetProfileEvolutionBase(
		t.Context(), 0, u.ID,
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("mismatched profile membership fell through to another tenant: %v", err)
	}
}

func TestInitialProfileCreateConcurrentSameKeyExactReplay(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		requireDatabaseCapability(t)
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	u, err := st.UpsertUserByOpenID(
		t.Context(), "profile_create_replay_"+uuid.NewString(), "create-replay")
	if err != nil {
		t.Fatal(err)
	}
	attachTenant(t, st, u.ID)
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		for _, table := range []string{
			"profile_claims", "profile_claim_states", "profile_edit_receipts",
			"profile_edit_revisions", "profiles",
		} {
			cleanupExec(ctx, t, st, "DELETE FROM "+table+" WHERE user_id=$1", u.ID)
		}
		cleanupExec(ctx, t, st, `DELETE FROM memberships WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id=$1`, u.ID)
	})
	industry := "AI"
	patch := types.ProfileEditPatch{
		Industry: &industry, Tags: ptrStrings([]string{"A"}),
	}
	type outcome struct {
		view *types.ProfileView
		err  error
	}
	out := make(chan outcome, 2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			<-start
			view, err := st.PatchProfile(
				t.Context(), 1, u.ID, nil, patch,
				"same-create-key", strings.Repeat("d", 64))
			out <- outcome{view: view, err: err}
		}()
	}
	close(start)
	first, second := <-out, <-out
	if first.err != nil || second.err != nil {
		t.Fatalf("same-key create errors=%v/%v", first.err, second.err)
	}
	if !first.view.UpdatedAt.Equal(second.view.UpdatedAt) ||
		first.view.Industry != second.view.Industry {
		t.Fatalf("same-key create did not exact replay: %+v / %+v", first.view, second.view)
	}
	list, err := st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil || list.Version != 0 || len(list.Claims) != 2 {
		t.Fatalf("duplicate initial claim state: %+v err=%v", list, err)
	}
}

func TestProfileClaimEventPaginationAndBoundedActionReplay(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		requireDatabaseCapability(t)
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	u, err := st.UpsertUserByOpenID(
		t.Context(), "claim_page_"+uuid.NewString(), "claim-page")
	if err != nil {
		t.Fatal(err)
	}
	attachTenant(t, st, u.ID)
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		for _, table := range []string{
			"profile_claim_receipts", "profile_claim_events", "profile_claims",
			"profile_claim_states", "profiles",
		} {
			cleanupExec(ctx, t, st, "DELETE FROM "+table+" WHERE user_id=$1", u.ID)
		}
		cleanupExec(ctx, t, st, `DELETE FROM memberships WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id=$1`, u.ID)
	})

	seedTx, err := st.beginProfileClaimScopedTx(t.Context(), 1, true, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = seedTx.Rollback(context.Background()) }()
	if _, err := seedTx.Exec(t.Context(), `
		INSERT INTO profiles(tenant_id,user_id,industry)
		VALUES(1,$1,'legacy-source')`, u.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := seedTx.Exec(t.Context(), `
		INSERT INTO profile_epochs(tenant_id,user_id,profile_epoch)
		VALUES(1,$1,0)`, u.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := seedTx.Exec(t.Context(), `
		INSERT INTO profile_claim_states(tenant_id,user_id)
		VALUES(1,$1)`, u.ID); err != nil {
		t.Fatal(err)
	}
	if err := bindProfileEpochTx(t.Context(), seedTx, 0); err != nil {
		t.Fatal(err)
	}
	var originalClaimID, resultClaimID, dependentResultClaimID int64
	if err := seedTx.QueryRow(t.Context(), `
		INSERT INTO profile_claims
		    (tenant_id,user_id,profile_epoch,field_name,claim_value,source_state)
		VALUES(1,$1,0,'industry','legacy-source','source_unavailable')
		RETURNING id`, u.ID).Scan(&originalClaimID); err != nil {
		t.Fatal(err)
	}
	if err := seedTx.QueryRow(t.Context(), `
		INSERT INTO profile_claims
		    (tenant_id,user_id,profile_epoch,field_name,claim_value,source_state,
		     supersedes_claim_id)
		VALUES(1,$1,0,'industry','corrected-source','manual',$2)
		RETURNING id`, u.ID, originalClaimID).Scan(&resultClaimID); err != nil {
		t.Fatal(err)
	}
	if err := seedTx.QueryRow(t.Context(), `
		INSERT INTO profile_claims
		    (tenant_id,user_id,profile_epoch,field_name,claim_value,source_state,
		     supersedes_claim_id)
		VALUES(1,$1,0,'industry','dependent-source','manual',$2)
		RETURNING id`,
		u.ID, originalClaimID).Scan(&dependentResultClaimID); err != nil {
		t.Fatal(err)
	}
	var correctEventID, dependentPinID, revokePinID int64
	if err := seedTx.QueryRow(t.Context(), `
		INSERT INTO profile_claim_events
		    (tenant_id,user_id,profile_epoch,actor_user_id,event_kind,target_claim_id,
		     result_claim_id,expected_version,result_version)
		VALUES(1,$1,0,$1,'correct',$2,$3,0,1)
		RETURNING id`,
		u.ID, originalClaimID, resultClaimID).Scan(&correctEventID); err != nil {
		t.Fatal(err)
	}
	if err := seedTx.QueryRow(t.Context(), `
		INSERT INTO profile_claim_events
		    (tenant_id,user_id,profile_epoch,actor_user_id,event_kind,target_claim_id,
		     expected_version,result_version)
		VALUES(1,$1,0,$1,'pin',$2,1,2)
		RETURNING id`,
		u.ID, resultClaimID).Scan(&dependentPinID); err != nil {
		t.Fatal(err)
	}
	if err := seedTx.QueryRow(t.Context(), `
		INSERT INTO profile_claim_events
		    (tenant_id,user_id,profile_epoch,actor_user_id,event_kind,target_event_id,
		     expected_version,result_version)
		VALUES(1,$1,0,$1,'revoke',$2,2,3)
		RETURNING id`,
		u.ID, dependentPinID).Scan(&revokePinID); err != nil {
		t.Fatal(err)
	}
	var dependentCorrectID, activeDependentPinID int64
	if err := seedTx.QueryRow(t.Context(), `
		INSERT INTO profile_claim_events
		    (tenant_id,user_id,profile_epoch,actor_user_id,event_kind,target_claim_id,
		     result_claim_id,expected_version,result_version)
		VALUES(1,$1,0,$1,'correct',$2,$3,3,4)
		RETURNING id`,
		u.ID, originalClaimID, dependentResultClaimID,
	).Scan(&dependentCorrectID); err != nil {
		t.Fatal(err)
	}
	if err := seedTx.QueryRow(t.Context(), `
		INSERT INTO profile_claim_events
		    (tenant_id,user_id,profile_epoch,actor_user_id,event_kind,target_claim_id,
		     expected_version,result_version)
		VALUES(1,$1,0,$1,'pin',$2,4,5)
		RETURNING id`,
		u.ID, dependentResultClaimID).Scan(&activeDependentPinID); err != nil {
		t.Fatal(err)
	}
	if _, err := seedTx.Exec(t.Context(), `
		INSERT INTO profile_claim_events
		    (tenant_id,user_id,profile_epoch,actor_user_id,event_kind,target_claim_id,
		     expected_version,result_version)
		SELECT 1,$1,0,$1,'pin',$2,n-1,n
		  FROM generate_series(6,1000) n`,
		u.ID, originalClaimID); err != nil {
		t.Fatal(err)
	}
	if _, err := seedTx.Exec(t.Context(), `
		UPDATE profile_claim_states SET version=1000
		 WHERE tenant_id=1 AND user_id=$1`, u.ID); err != nil {
		t.Fatal(err)
	}
	if err := seedTx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}

	defaultPage, err := st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(defaultPage.Events) != defaultProfileClaimEventLimit ||
		!defaultPage.EventsHasMore || defaultPage.EventsNextCursor == "" {
		t.Fatalf("no-parameter default page events/more/cursor=%d/%t/%q",
			len(defaultPage.Events), defaultPage.EventsHasMore,
			defaultPage.EventsNextCursor)
	}
	if len(defaultPage.Claims) > maxPublicFirstProfileClaims {
		t.Fatalf("no-parameter default claims=%d", len(defaultPage.Claims))
	}
	for i := 1; i < len(defaultPage.Events); i++ {
		if parseTestID(t, defaultPage.Events[i-1].ID) <=
			parseTestID(t, defaultPage.Events[i].ID) {
			t.Fatalf("no-parameter default events not id DESC: %s then %s",
				defaultPage.Events[i-1].ID, defaultPage.Events[i].ID)
		}
	}
	defaultCursor, err := decodeProfileClaimEventCursor(
		defaultPage.EventsNextCursor)
	if err != nil {
		t.Fatal(err)
	}
	if defaultCursor.Limit != defaultProfileClaimEventLimit {
		t.Fatalf("no-parameter cursor limit=%d", defaultCursor.Limit)
	}
	defaultContinuation, err := st.ListProfileClaimsPage(
		t.Context(), 1, u.ID,
		ProfileClaimEventPageOptions{
			Limit:  defaultProfileClaimEventLimit,
			Cursor: defaultPage.EventsNextCursor,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(defaultContinuation.Events) != defaultProfileClaimEventLimit ||
		len(defaultContinuation.Claims) > maxPublicEventContextClaims {
		t.Fatalf("default continuation events/claims=%d/%d",
			len(defaultContinuation.Events), len(defaultContinuation.Claims))
	}
	if parseTestID(t, defaultContinuation.Events[0].ID) >=
		parseTestID(t, defaultPage.Events[len(defaultPage.Events)-1].ID) {
		t.Fatalf("default continuation overlapped first page: %s after %s",
			defaultContinuation.Events[0].ID,
			defaultPage.Events[len(defaultPage.Events)-1].ID)
	}

	options := ProfileClaimEventPageOptions{Limit: 37}
	seenEvents := make(map[string]bool, 1000)
	lastID := int64(^uint64(0) >> 1)
	firstCursor := ""
	pageNumber := 0
	var oldest types.ProfileClaimEvent
	statusByID := make(map[string]types.ProfileClaimEvent)
	for {
		page, err := st.ListProfileClaimsPage(
			t.Context(), 1, u.ID, options)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Events) == 0 || len(page.Events) > options.Limit {
			t.Fatalf("page %d events=%d", pageNumber, len(page.Events))
		}
		if pageNumber == 0 {
			if len(page.Claims) > maxPublicFirstProfileClaims {
				t.Fatalf("first claims=%d", len(page.Claims))
			}
			source := findClaim(
				t, page.Claims, "industry", "legacy-source", true)
			if source.Source.State != "source_unavailable" {
				t.Fatalf("source_unavailable lost on first page: %+v", source)
			}
			firstCursor = page.EventsNextCursor
		} else {
			if len(page.Claims) > maxPublicEventContextClaims {
				t.Fatalf("continuation claims=%d", len(page.Claims))
			}
			contextIDs := make(map[string]bool)
			for _, event := range page.Events {
				if event.TargetClaimID != "" {
					contextIDs[event.TargetClaimID] = true
				}
				if event.ResultClaimID != "" {
					contextIDs[event.ResultClaimID] = true
				}
			}
			for _, claim := range page.Claims {
				if !contextIDs[claim.ID] {
					t.Fatalf("continuation leaked non-context claim %+v", claim)
				}
			}
		}
		for _, event := range page.Events {
			id := parseTestID(t, event.ID)
			if id >= lastID {
				t.Fatalf("events not strict id DESC: %d after %d", id, lastID)
			}
			if seenEvents[event.ID] {
				t.Fatalf("duplicate paged event %s", event.ID)
			}
			seenEvents[event.ID] = true
			lastID = id
			oldest = event
			statusByID[event.ID] = event
		}
		if !page.EventsHasMore {
			if page.EventsNextCursor != "" {
				t.Fatal("terminal page exposed next cursor")
			}
			break
		}
		if page.EventsNextCursor == "" {
			t.Fatal("non-terminal page omitted cursor")
		}
		options.Cursor = page.EventsNextCursor
		pageNumber++
	}
	if len(seenEvents) != 1000 {
		t.Fatalf("paged events=%d want 1000", len(seenEvents))
	}
	if oldest.ID != strconv.FormatInt(correctEventID, 10) || !oldest.Revocable {
		t.Fatalf("oldest correction not revocable: %+v", oldest)
	}
	dependentPin := statusByID[strconv.FormatInt(dependentPinID, 10)]
	if !dependentPin.Revoked || dependentPin.Revocable {
		t.Fatalf("revoked dependent pin status=%+v", dependentPin)
	}
	if revoked := statusByID[strconv.FormatInt(revokePinID, 10)]; revoked.Revocable {
		t.Fatalf("revoke event advertised revocable: %+v", revoked)
	}
	dependentCorrect := statusByID[strconv.FormatInt(dependentCorrectID, 10)]
	if dependentCorrect.Revoked || dependentCorrect.Revocable {
		t.Fatalf("cross-page dependent correction status=%+v (pin=%d)",
			dependentCorrect, activeDependentPinID)
	}

	decoded, err := decodeProfileClaimEventCursor(firstCursor)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Kind != profileClaimEventCursorKind ||
		decoded.Schema != profileClaimEventCursorSchema {
		t.Fatalf("cursor kind/schema=%+v", decoded)
	}
	if _, err := st.ListProfileClaimsPage(
		t.Context(), 1, u.ID+1,
		ProfileClaimEventPageOptions{Limit: 37, Cursor: firstCursor},
	); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("cross-user cursor accepted: %v", err)
	}
	if _, err := st.ListProfileClaimsPage(
		t.Context(), 1, u.ID,
		ProfileClaimEventPageOptions{Limit: 20, Cursor: firstCursor},
	); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("cursor limit rebinding accepted: %v", err)
	}
	tamperedSnapshot := decoded
	tamperedSnapshot.SnapshotMaxEventID--
	tamperedCursor, err := encodeProfileClaimEventCursor(tamperedSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ListProfileClaimsPage(
		t.Context(), 1, u.ID,
		ProfileClaimEventPageOptions{Limit: 37, Cursor: tamperedCursor},
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("cursor snapshot max rebinding did not conflict: %v", err)
	}
	wrongKind := decoded
	wrongKind.Kind = "other_events"
	wrongKindCursor, err := encodeProfileClaimEventCursor(wrongKind)
	if err != nil {
		t.Fatal(err)
	}
	wrongSchema := decoded
	wrongSchema.Schema = "vane.profile-claim-event-cursor/v1"
	wrongSchemaCursor, err := encodeProfileClaimEventCursor(wrongSchema)
	if err != nil {
		t.Fatal(err)
	}
	invalidBefore := decoded
	invalidBefore.BeforeEventID = invalidBefore.SnapshotMaxEventID + 1
	invalidBeforeCursor, err := encodeProfileClaimEventCursor(invalidBefore)
	if err != nil {
		t.Fatal(err)
	}
	malformed := []string{
		strings.Repeat("x", maxProfileClaimEventCursorLen+1),
		base64.RawURLEncoding.EncodeToString([]byte(`{}`)),
		base64.RawURLEncoding.EncodeToString(
			[]byte(`{"schema":"x","schema":"y"}`)),
		base64.RawURLEncoding.EncodeToString(
			[]byte(`{"unknown":true}`)),
		base64.RawURLEncoding.EncodeToString(
			[]byte(`{"schema":"x"} trailing`)),
		wrongKindCursor,
		wrongSchemaCursor,
		invalidBeforeCursor,
	}
	for _, cursor := range malformed {
		if _, err := st.ListProfileClaimsPage(
			t.Context(), 1, u.ID,
			ProfileClaimEventPageOptions{Limit: 37, Cursor: cursor},
		); !errors.Is(err, types.ErrValidation) {
			t.Fatalf("malformed cursor accepted: %v", err)
		}
	}

	concurrentTx, err := st.beginProfileClaimScopedTx(
		t.Context(), 1, true, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := bindProfileEpochTx(t.Context(), concurrentTx, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := concurrentTx.Exec(t.Context(), `
		INSERT INTO profile_claim_events
		    (tenant_id,user_id,profile_epoch,actor_user_id,event_kind,target_claim_id,
		     expected_version,result_version)
		VALUES(1,$1,0,$1,'pin',$2,1000,1001)`,
		u.ID, originalClaimID); err != nil {
		t.Fatal(err)
	}
	if _, err := concurrentTx.Exec(t.Context(), `
		UPDATE profile_claim_states SET version=1001
		 WHERE tenant_id=1 AND user_id=$1 AND version=1000`, u.ID); err != nil {
		t.Fatal(err)
	}
	if err := concurrentTx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ListProfileClaimsPage(
		t.Context(), 1, u.ID,
		ProfileClaimEventPageOptions{Limit: 37, Cursor: firstCursor},
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("stale cursor version did not return conflict: %v", err)
	}

	action := types.ProfileClaimAction{
		ExpectedVersion: 1001, Action: "suppress", ClaimID: originalClaimID,
	}
	firstAction, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID, action,
		"page-action", strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	if firstAction.ClaimsComplete ||
		len(firstAction.Claims) > maxPublicActionProfileClaims {
		t.Fatalf("action claim bound/completeness=%d/%t",
			len(firstAction.Claims), firstAction.ClaimsComplete)
	}
	if findClaim(
		t, firstAction.Claims, "industry", "legacy-source", false,
	).Source.State != "source_unavailable" {
		t.Fatal("action target context lost source state")
	}
	replay, err := st.ApplyProfileClaimAction(
		t.Context(), 1, u.ID, action,
		"page-action", strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	firstPayload, err := json.Marshal(firstAction)
	if err != nil {
		t.Fatal(err)
	}
	replayPayload, err := json.Marshal(replay)
	if err != nil {
		t.Fatal(err)
	}
	// Receipt replay is a wire-level exactness contract. time.Time values
	// scanned by pgx and decoded from the stored JSON can carry different
	// internal *time.Location pointers while encoding to identical RFC3339
	// bytes. reflect.DeepEqual therefore makes this test runner-timezone
	// dependent even though the API response is unchanged.
	if !bytes.Equal(firstPayload, replayPayload) {
		t.Fatalf("bounded action receipt replay drifted:\n%+v\n%+v",
			string(firstPayload), string(replayPayload))
	}
}

func TestPublicProfileClaimPageFailsClosedAboveActiveBound(t *testing.T) {
	claims := make([]profileClaimRow, 0, maxPublicActiveProfileClaims+1)
	projection := profileClaimProjection{
		active: make(map[int64]bool, maxPublicActiveProfileClaims+1),
		pinned: map[int64]bool{},
	}
	for id := int64(1); id <= maxPublicActiveProfileClaims+1; id++ {
		claims = append(claims, profileClaimRow{
			ID: id, Field: "summary", Value: strconv.FormatInt(id, 10),
			SourceState: "manual",
		})
		projection.active[id] = true
	}
	if _, err := publicProfileClaimsForPage(
		claims, projection, nil, true,
	); !errors.Is(err, types.ErrInternal) {
		t.Fatalf("first page silently truncated active overflow: %v", err)
	}
	if _, err := publicProfileClaimsForAction(
		claims, projection, nil,
	); !errors.Is(err, types.ErrInternal) {
		t.Fatalf("action silently truncated active overflow: %v", err)
	}
}

func TestSplitSummaryClaimsUnicodeAndBound(t *testing.T) {
	long := strings.Repeat("界", maxSummaryClaimRunes+5) + "。第二句！third."
	got := splitSummaryClaims(long)
	if len(got) != 4 {
		t.Fatalf("split=%q", got)
	}
	for _, claim := range got {
		if utf8.RuneCountInString(claim) > maxSummaryClaimRunes {
			t.Fatalf("oversize claim runes=%d", utf8.RuneCountInString(claim))
		}
	}
	if strings.Join(got, "") != long {
		t.Fatalf("split did not preserve text: %q", strings.Join(got, ""))
	}
}

func TestPublicProfileClaimEventsKeepsOldRevocableAuthority(t *testing.T) {
	events := make([]profileClaimEventRow, 0, 60)
	for i := int64(1); i <= 60; i++ {
		target := i
		events = append(events, profileClaimEventRow{
			ID: i, Kind: "pin", TargetClaimID: &target,
			CreatedAt: time.Unix(i, 0),
		})
	}
	got := publicProfileClaimEvents(events)
	if len(got) != 60 {
		t.Fatalf("revocable events were truncated: %d", len(got))
	}
	if got[len(got)-1].ID != "1" || !got[len(got)-1].Revocable {
		t.Fatalf("oldest active authority missing: %+v", got[len(got)-1])
	}
}

func TestPublicProfileClaimsBoundsHistoryButKeepsAuthority(t *testing.T) {
	claims := make([]profileClaimRow, 0, 80)
	for id := int64(1); id <= 80; id++ {
		claims = append(claims, profileClaimRow{
			ID: id, Field: "tag", Value: fmt.Sprintf("tag-%d", id),
			SourceState: "evidence", CreatedAt: time.Unix(id, 0),
		})
	}
	target := int64(2)
	projection := profileClaimProjection{
		active: map[int64]bool{1: true},
		pinned: map[int64]bool{},
	}
	got := publicProfileClaims(
		claims, projection,
		[]profileClaimEventRow{{ID: 1, Kind: "suppress", TargetClaimID: &target}},
	)
	if len(got) != 50 {
		t.Fatalf("bounded claim history=%d want 50", len(got))
	}
	seen := make(map[string]bool, len(got))
	for _, claim := range got {
		seen[claim.ID] = true
	}
	for _, mandatory := range []string{"1", "2", "80"} {
		if !seen[mandatory] {
			t.Fatalf("mandatory/recent claim %s missing: %+v", mandatory, got)
		}
	}
	if seen["3"] {
		t.Fatalf("old inactive non-authority claim escaped history bound")
	}

	for id := int64(1); id <= 60; id++ {
		projection.active[id] = true
	}
	got = publicProfileClaims(claims, projection, nil)
	if len(got) != 60 {
		t.Fatalf("active claims were truncated: %d", len(got))
	}
}

func TestProjectProfileClaimsSummaryShowsOnlyFinalOverride(t *testing.T) {
	one, two := int64(1), int64(2)
	claims := []profileClaimRow{
		{ID: 1, Field: "industry", Value: "AI", SourceState: "source_unavailable"},
		{ID: 2, Field: "industry", Value: "Biotech", SourceState: "manual", SupersedesID: &one},
		{ID: 3, Field: "industry", Value: "Health", SourceState: "manual", SupersedesID: &two},
	}
	events := []profileClaimEventRow{
		{ID: 1, Kind: "correct", TargetClaimID: &one, ResultClaimID: &two},
		{ID: 2, Kind: "correct", TargetClaimID: &two, ResultClaimID: ptrInt64(3)},
	}
	got := projectProfileClaims(claims, events, 0)
	if got.industry != "Health" ||
		!strings.Contains(got.summary, "行业=Health") ||
		strings.Contains(got.summary, "Biotech") {
		t.Fatalf("stale manual history leaked into summary: %+v", got)
	}
}

func ptrInt64(v int64) *int64 { return &v }

func ptrStrings(v []string) *[]string { return &v }

func findClaim(
	t *testing.T, claims []types.ProfileClaim, field, value string, active bool,
) types.ProfileClaim {
	t.Helper()
	for _, claim := range claims {
		if claim.Field == field && claim.Value == value && claim.Active == active {
			return claim
		}
	}
	t.Fatalf("claim not found field=%s value=%s active=%t in %+v", field, value, active, claims)
	return types.ProfileClaim{}
}

func parseTestID(t *testing.T, raw string) int64 {
	t.Helper()
	var id int64
	if _, err := fmt.Sscan(raw, &id); err != nil || id <= 0 {
		t.Fatalf("invalid id %q", raw)
	}
	return id
}
