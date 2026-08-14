package exploration

import (
	"reflect"
	"testing"
	"time"
)

func TestSelectV1ExcludesCanonicalRecentAndInvalidCandidates(t *testing.T) {
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	candidates := []CandidateV1{
		candidateForTest(1, 90, ReasonChallengesJudgmentV1, now),
		candidateForTest(2, 89, ReasonAdjacentOpportunityV1, now.Add(-time.Minute)),
		candidateForTest(3, 88, ReasonNewSourceV1, now.Add(-2*time.Minute)),
		candidateForTest(4, 54, ReasonNewSourceV1, now.Add(-3*time.Minute)),
		candidateForTest(5, 87, ReasonNewSourceV1, now.Add(-4*time.Minute)),
	}
	candidates[4].SourceURL = "javascript:alert(1)"
	options := optionsForTest()
	options.Scope.CanonicalContentItemIDs[1] = struct{}{}
	options.Scope.RecentlyShownContentItemIDs[2] = struct{}{}

	feed, err := SelectV1(candidates, options)
	if err != nil {
		t.Fatal(err)
	}
	if feed.SchemaVersion != SchemaVersionV1 || feed.Channel != ChannelWebV1 {
		t.Fatalf("feed envelope = %#v", feed)
	}
	if len(feed.Items) != 1 || feed.Items[0].ContentItemID != 3 {
		t.Fatalf("selected items = %#v, want only evidence-safe item 3", feed.Items)
	}
}

func TestSelectV1DiversityFirstThenQualityFill(t *testing.T) {
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	candidates := []CandidateV1{
		candidateForTest(1, 100, ReasonNewSourceV1, now),
		candidateForTest(2, 99, ReasonNewSourceV1, now.Add(-time.Minute)),
		candidateForTest(3, 70, ReasonAdjacentOpportunityV1, now.Add(-2*time.Minute)),
		candidateForTest(4, 60, ReasonChallengesJudgmentV1, now.Add(-3*time.Minute)),
	}
	options := optionsForTest()
	options.Limit = 99

	feed, err := SelectV1(candidates, options)
	if err != nil {
		t.Fatal(err)
	}
	if len(feed.Items) != 3 {
		t.Fatalf("items = %d, want hard Web cap 3", len(feed.Items))
	}
	want := []int64{1, 3, 4}
	for index, id := range want {
		if feed.Items[index].ContentItemID != id {
			t.Fatalf("item[%d] = %d, want %d; feed=%#v",
				index, feed.Items[index].ContentItemID, id, feed.Items)
		}
	}
}

func TestSelectV1DuplicatePermutationIsStableAndConflictsFailClosed(t *testing.T) {
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	low := candidateForTest(7, 55, ReasonNewSourceV1, now)
	low.Title = "low"
	high := candidateForTest(7, 100, ReasonChallengesJudgmentV1, now)
	high.Title = "high"
	options := optionsForTest()
	options.Limit = 2
	a, err := SelectV1([]CandidateV1{low, high}, options)
	if err != nil {
		t.Fatal(err)
	}
	b, err := SelectV1([]CandidateV1{high, low}, options)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) || len(a.Items) != 0 {
		t.Fatalf("conflicting duplicate was order-sensitive: a=%#v b=%#v", a, b)
	}

	same := candidateForTest(8, 80, ReasonNewSourceV1, now)
	identical, err := SelectV1([]CandidateV1{same, same}, options)
	if err != nil {
		t.Fatal(err)
	}
	if len(identical.Items) != 1 || identical.Items[0].ContentItemID != 8 {
		t.Fatalf("identical replay did not deduplicate: %#v", identical.Items)
	}
}

func TestSelectV1RequiresBoundedReasonAndEvidence(t *testing.T) {
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	noReason := candidateForTest(1, 80, ReasonNewSourceV1, now)
	noReason.NewSource = false
	noEvidence := candidateForTest(2, 80, ReasonNewSourceV1, now)
	noEvidence.EvidenceSources = nil
	duplicateEvidence := candidateForTest(3, 80, ReasonNewSourceV1, now)
	duplicateEvidence.EvidenceSources = append(
		duplicateEvidence.EvidenceSources,
		duplicateEvidence.EvidenceSources[0],
	)

	feed, err := SelectV1(
		[]CandidateV1{noReason, noEvidence, duplicateEvidence},
		optionsForTest(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(feed.Items) != 0 {
		t.Fatalf("invalid candidates escaped fail-closed validation: %#v", feed.Items)
	}
}

func TestSelectV1ReasonPrecedenceIsFixed(t *testing.T) {
	candidate := candidateForTest(
		1, 80, ReasonNewSourceV1, time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC),
	)
	candidate.AdjacentOpportunity = true
	candidate.ChallengesJudgment = true
	feed, err := SelectV1([]CandidateV1{candidate}, optionsForTest())
	if err != nil {
		t.Fatal(err)
	}
	if len(feed.Items) != 1 ||
		feed.Items[0].Reason != ReasonChallengesJudgmentV1 {
		t.Fatalf("reason precedence = %#v", feed.Items)
	}
}

func TestSelectV1RequiresCompleteScopeAndVerifiedReceipt(t *testing.T) {
	candidate := candidateForTest(
		1, 80, ReasonNewSourceV1, time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC),
	)
	for _, options := range []OptionsV1{
		{},
		{ExpectedScope: scopeIdentityForTest(), Scope: &ExclusionSnapshotV1{}},
		{ExpectedScope: scopeIdentityForTest(), Scope: completeScopeForTest()},
	} {
		feed, err := SelectV1([]CandidateV1{candidate}, options)
		if err == nil || len(feed.Items) != 0 {
			t.Fatalf("incomplete authority did not fail closed: feed=%#v err=%v", feed, err)
		}
	}
	options := optionsForTest()
	options.Verifier = evidenceRejectingVerifierForTest{}
	feed, err := SelectV1([]CandidateV1{candidate}, options)
	if err != nil {
		t.Fatal(err)
	}
	if len(feed.Items) != 0 {
		t.Fatalf("unverified candidate escaped: %#v", feed.Items)
	}
}

func TestSelectV1RejectsCrossScopeAndUnverifiedSnapshot(t *testing.T) {
	candidate := candidateForTest(
		1, 80, ReasonNewSourceV1, time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC),
	)
	crossScope := optionsForTest()
	crossScope.ExpectedScope.UserID++
	feed, err := SelectV1([]CandidateV1{candidate}, crossScope)
	if err == nil || len(feed.Items) != 0 {
		t.Fatalf("cross-scope snapshot escaped: feed=%#v err=%v", feed, err)
	}
	unverified := optionsForTest()
	unverified.Verifier = rejectingVerifierForTest{}
	feed, err = SelectV1([]CandidateV1{candidate}, unverified)
	if err == nil || len(feed.Items) != 0 {
		t.Fatalf("unverified snapshot escaped: feed=%#v err=%v", feed, err)
	}
}

func TestSelectV1MutedDirectionIsExcluded(t *testing.T) {
	candidate := candidateForTest(
		1, 80, ReasonNewSourceV1, time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC),
	)
	options := optionsForTest()
	options.Scope.MutedDirectionKeys[candidate.DirectionKey] = struct{}{}
	feed, err := SelectV1([]CandidateV1{candidate}, options)
	if err != nil {
		t.Fatal(err)
	}
	if len(feed.Items) != 0 {
		t.Fatalf("muted direction escaped: %#v", feed.Items)
	}
}

func TestSelectV1RejectsURLQueryFragmentAndDuplicateEvidenceURL(t *testing.T) {
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	for _, rawURL := range []string{
		"https://provider.example/private?api_key=secret",
		"https://provider.example/private#token",
		"https://user:secret@provider.example/private",
		"https://:443/private",
		"http://localhost:8080/private",
		"http://127.0.0.1/private",
		"http://10.0.0.1/private",
		"http://169.254.1.1/private",
	} {
		candidate := candidateForTest(1, 80, ReasonNewSourceV1, now)
		candidate.SourceURL = rawURL
		feed, err := SelectV1([]CandidateV1{candidate}, optionsForTest())
		if err != nil {
			t.Fatal(err)
		}
		if len(feed.Items) != 0 {
			t.Fatalf("credential-bearing URL escaped: %s", feed.Items[0].SourceURL)
		}
	}

	candidate := candidateForTest(2, 80, ReasonNewSourceV1, now)
	duplicate := candidate.EvidenceSources[0]
	duplicate.Ref = "source-2"
	candidate.EvidenceSources = append(candidate.EvidenceSources, duplicate)
	feed, err := SelectV1([]CandidateV1{candidate}, optionsForTest())
	if err != nil {
		t.Fatal(err)
	}
	if len(feed.Items) != 0 {
		t.Fatalf("duplicate evidence URL inflated source count: %#v", feed.Items)
	}

	candidate = candidateForTest(3, 80, ReasonNewSourceV1, now)
	duplicate = candidate.EvidenceSources[0]
	duplicate.Ref = "source-2"
	duplicate.SourceURL = "https://example.com:443/evidence"
	candidate.EvidenceSources = append(candidate.EvidenceSources, duplicate)
	feed, err = SelectV1([]CandidateV1{candidate}, optionsForTest())
	if err != nil {
		t.Fatal(err)
	}
	if len(feed.Items) != 0 {
		t.Fatalf("default-port equivalent evidence inflated source count: %#v", feed.Items)
	}

	candidate = candidateForTest(4, 80, ReasonNewSourceV1, now)
	candidate.EvidenceSources[0].SourceURL = "https://example.com/%65vidence"
	feed, err = SelectV1([]CandidateV1{candidate}, optionsForTest())
	if err != nil {
		t.Fatal(err)
	}
	if len(feed.Items) != 0 {
		t.Fatalf("encoded-path evidence escaped canonical boundary: %#v", feed.Items)
	}
}

func TestFeedbackV1Validate(t *testing.T) {
	for _, feedback := range []FeedbackV1{
		FeedbackInspiringV1,
		FeedbackOffTargetV1,
		FeedbackMuteDirectionV1,
	} {
		if err := feedback.Validate(); err != nil {
			t.Fatalf("%q rejected: %v", feedback, err)
		}
	}
	if err := FeedbackV1("interested").Validate(); err == nil {
		t.Fatal("ordinary Brief feedback escaped exploration feedback boundary")
	}
}

func candidateForTest(
	id int64,
	score int,
	reason BoundaryReasonV1,
	when time.Time,
) CandidateV1 {
	candidate := CandidateV1{
		ContentItemID:         id,
		DirectionKey:          "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		EvidenceReceiptID:     id + 100,
		EvidenceReceiptDigest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Title:                 "A useful boundary signal",
		Summary:               "Evidence-backed context outside the ordinary Top-N.",
		SourceTitle:           "Example",
		SourceURL:             "https://example.com/article",
		PublishedAt:           &when,
		DiscoveredAt:          when.Add(time.Minute),
		RelevanceScore:        score,
		EvidenceSources: []EvidenceSourceV1{{
			Ref:       "source-1",
			Title:     "Primary evidence",
			SourceURL: "https://example.com/evidence",
		}},
	}
	switch reason {
	case ReasonChallengesJudgmentV1:
		candidate.ChallengesJudgment = true
	case ReasonAdjacentOpportunityV1:
		candidate.AdjacentOpportunity = true
	case ReasonNewSourceV1:
		candidate.NewSource = true
	}
	return candidate
}

type acceptingVerifierForTest struct{}

func (acceptingVerifierForTest) VerifyExplorationScopeV1(
	expected ScopeIdentityV1,
	snapshot ExclusionSnapshotV1,
) bool {
	return expected == snapshot.Scope
}

func (acceptingVerifierForTest) VerifyExplorationEvidenceV1(
	ScopeIdentityV1,
	CandidateV1,
) bool {
	return true
}

type rejectingVerifierForTest struct{}

func (rejectingVerifierForTest) VerifyExplorationScopeV1(
	ScopeIdentityV1,
	ExclusionSnapshotV1,
) bool {
	return false
}

func (rejectingVerifierForTest) VerifyExplorationEvidenceV1(
	ScopeIdentityV1,
	CandidateV1,
) bool {
	return false
}

type evidenceRejectingVerifierForTest struct{}

func (evidenceRejectingVerifierForTest) VerifyExplorationScopeV1(
	expected ScopeIdentityV1,
	snapshot ExclusionSnapshotV1,
) bool {
	return expected == snapshot.Scope
}

func (evidenceRejectingVerifierForTest) VerifyExplorationEvidenceV1(
	ScopeIdentityV1,
	CandidateV1,
) bool {
	return false
}

func completeScopeForTest() *ExclusionSnapshotV1 {
	return &ExclusionSnapshotV1{
		Scope:                       scopeIdentityForTest(),
		SnapshotReceiptID:           77,
		SnapshotDigest:              "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		CanonicalContentItemIDs:     map[int64]struct{}{},
		RecentlyShownContentItemIDs: map[int64]struct{}{},
		MutedDirectionKeys:          map[string]struct{}{},
	}
}

func scopeIdentityForTest() ScopeIdentityV1 {
	return ScopeIdentityV1{
		TenantID: 7,
		UserID:   11,
		TaskID:   "task-exploration",
	}
}

func optionsForTest() OptionsV1 {
	return OptionsV1{
		ExpectedScope: scopeIdentityForTest(),
		Scope:         completeScopeForTest(),
		Verifier:      acceptingVerifierForTest{},
	}
}
