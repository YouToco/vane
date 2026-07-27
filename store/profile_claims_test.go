package store

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

func TestProfileClaimAuthorityAndEvolverRegression(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过画像 claim authority 真 PostgreSQL 测试")
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
		t.Skip("未设置 DATABASE_URL")
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
		t.Skip("未设置 DATABASE_URL")
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
		t.Skip("未设置 DATABASE_URL")
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

func TestProfileClaimMutationSummaryBoundAndDuplicatePin(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL")
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
	if err := st.EvolveProfile(
		t.Context(), u.ID, strings.Repeat("丁", 300), nil,
		20, pinned.Profile.UpdatedAt, 10,
	); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("multi-generation pinned summary overflow accepted: %v", err)
	}
	list, err = st.ListProfileClaims(t.Context(), 1, u.ID)
	if err != nil || list.Version != 2 {
		t.Fatalf("rejected evolution changed version: %+v err=%v", list, err)
	}
	p, err := st.GetProfileForTenant(t.Context(), 1, u.ID)
	if err != nil || utf8.RuneCountInString(stripDerivedManualSegment(p.Summary)) != 500 ||
		p.LastEvolvedFeedbackID != 10 {
		t.Fatalf("rejected evolution changed projection: %+v err=%v", p, err)
	}
}

func TestLegacyProfileTenantResolutionFailsClosed(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL")
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
			"profile_claim_states", "profiles",
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
		t.Skip("未设置 DATABASE_URL")
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
