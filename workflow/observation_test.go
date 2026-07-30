package workflow

import (
	"testing"
	"time"

	"github.com/YouToco/vane/eventqualifier"
	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/types"
)

func TestValidateQualifiedEventsRejectsForgedTimeAndEvidence(t *testing.T) {
	policy := workflowEventPolicy(t)
	digest, err := observation.PolicyDigest(policy)
	if err != nil {
		t.Fatal(err)
	}
	published := time.Date(2026, 7, 24, 1, 0, 0, 0, time.UTC)
	window := observation.Window{Start: published.Add(-24 * time.Hour), End: published}
	item := types.ContentItem{
		ID: 11, URL: "https://openai.com/index/model",
		PublishedAt: &published, ContentHash: "hash",
	}
	base := eventqualifier.Result{Outcome: "match", Events: []eventqualifier.Event{{
		EventType: "model_release", Subject: "OpenAI models",
		ReleaseIdentifier: "gpt-test", OccurredAt: published.Format(time.RFC3339),
		Qualification:      string(observation.QualificationGeneralAvailability),
		EvidenceContentIDs: []int64{11},
	}}}
	activities := &Activities{}
	got, outcome, err := activities.validateQualifiedEvents(
		policy, digest, window, []types.ContentItem{item}, "", base)
	if err != nil || outcome != "match" || len(got) != 1 {
		t.Fatalf("valid result got=%v outcome=%q err=%v", got, outcome, err)
	}

	cases := []struct {
		name      string
		mutate    func(*eventqualifier.Result)
		candidate types.ContentItem
	}{
		{
			name: "outside window", candidate: item,
			mutate: func(result *eventqualifier.Result) {
				result.Events[0].OccurredAt = window.Start.Format(time.RFC3339)
			},
		},
		{
			name: "unknown evidence", candidate: item,
			mutate: func(result *eventqualifier.Result) {
				result.Events[0].EvidenceContentIDs = []int64{99}
			},
		},
		{
			name: "non official evidence",
			candidate: func() types.ContentItem {
				copy := item
				copy.URL = "https://example.com/model"
				return copy
			}(),
			mutate: func(*eventqualifier.Result) {},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidate := base
			candidate.Events = append([]eventqualifier.Event(nil), base.Events...)
			tc.mutate(&candidate)
			_, _, err := activities.validateQualifiedEvents(
				policy, digest, window,
				[]types.ContentItem{tc.candidate}, "", candidate)
			if err == nil || types.CodeOf(err) != types.CodeValidation {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestCanonicalReleaseIdentityNormalizesPresentationVariants(t *testing.T) {
	for _, value := range []string{
		"GPT-5.5",
		"GPT 5.5",
		"Introducing GPT-5.5",
		"GPT-5.5 official model release",
	} {
		if got := canonicalReleaseIdentity(value); got != "gpt55" {
			t.Fatalf("canonicalReleaseIdentity(%q)=%q, want gpt55", value, got)
		}
	}
}

func TestQualifyContentWindowDeprioritizesUnknownDates(t *testing.T) {
	policy := workflowEventPolicy(t)
	policy.Mode = observation.ModeContent
	policy.Event = nil
	policy.QualifierPrompt = ""
	policy.Evidence.Requirement = observation.EvidenceTrustedAllowed
	policy.Evidence.OfficialDomains = nil
	policy.UnknownTime = observation.UnknownTimeDeprioritize
	window := observation.Window{
		Start: time.Date(2026, 7, 23, 1, 0, 0, 0, time.UTC),
		End:   time.Date(2026, 7, 24, 1, 0, 0, 0, time.UTC),
	}
	got := qualifyContentWindow(policy, window, []types.ContentItem{{ID: 1}})
	if len(got) != 1 || got[0].ObservationScorePenalty >= 0 {
		t.Fatalf("got=%+v", got)
	}
}

func TestAdmissibleEventEvidenceCandidatesKeepsDatedCrossEvidence(t *testing.T) {
	policy := workflowEventPolicy(t)
	window := observation.Window{
		Start: time.Date(2026, 7, 23, 1, 0, 0, 0, time.UTC),
		End:   time.Date(2026, 7, 24, 1, 0, 0, 0, time.UTC),
	}
	inWindow := window.Start.Add(time.Hour)
	beforeWindow := window.Start
	got := admissibleEventEvidenceCandidates(policy, window, []types.ContentItem{
		{ID: 1, URL: "https://openai.com/index/unknown-date"},
		{ID: 2, URL: "https://openai.com/index/too-old", PublishedAt: &beforeWindow},
		{ID: 3, URL: "https://example.com/index/model", PublishedAt: &inWindow},
		{ID: 4, URL: "https://news.openai.com/index/model", PublishedAt: &inWindow},
	})
	if len(got) != 2 || got[0].ID != 3 || got[1].ID != 4 {
		t.Fatalf("got=%+v", got)
	}
}

func TestValidateQualifiedEventsEnforcesManualEvidencePair(t *testing.T) {
	policy := workflowEventPolicy(t)
	policy.Evidence.Requirement = observation.EvidenceTrustedAllowed
	policy.Evidence.OfficialDomains = nil
	digest, err := observation.PolicyDigest(policy)
	if err != nil {
		t.Fatal(err)
	}
	officialAt := time.Date(2026, 7, 24, 1, 0, 0, 0, time.UTC)
	crossAt := officialAt.Add(time.Hour)
	window := observation.Window{
		Start: officialAt.Add(-time.Hour), End: crossAt.Add(time.Hour),
	}
	items := []types.ContentItem{
		{
			ID: 21, URL: "https://official.example/release",
			Content: "official release body", PublishedAt: &officialAt,
		},
		{
			ID: 22, URL: "https://media.example/report",
			Content: "independent report", PublishedAt: &crossAt,
		},
	}
	result := eventqualifier.Result{
		Outcome: "match",
		Events: []eventqualifier.Event{{
			EventType: "model_release", Subject: "OpenAI models",
			ReleaseIdentifier: "gpt-test",
			OccurredAt:        officialAt.Format(time.RFC3339),
			Qualification: string(
				observation.QualificationGeneralAvailability),
			EvidenceContentIDs: []int64{21, 22},
		}},
	}
	activities := &Activities{}
	got, outcome, err := activities.validateQualifiedEvents(
		policy, digest, window, items,
		"必须有官方原文交叉核验；输出交叉证据。", result)
	if err != nil || outcome != "match" || len(got) != 1 {
		t.Fatalf("manual evidence pair got=%+v outcome=%q err=%v",
			got, outcome, err)
	}
	result.Events[0].EvidenceContentIDs = []int64{21}
	if _, _, err := activities.validateQualifiedEvents(
		policy, digest, window, items,
		"必须有官方原文交叉核验；输出交叉证据。", result,
	); err == nil {
		t.Fatal("manual cross-evidence requirement admitted one source")
	}
}

func TestValidateQualifiedEventsRejectsReusedPrimary(t *testing.T) {
	policy := workflowEventPolicy(t)
	digest, err := observation.PolicyDigest(policy)
	if err != nil {
		t.Fatal(err)
	}
	published := time.Date(2026, 7, 24, 1, 0, 0, 0, time.UTC)
	window := observation.Window{
		Start: published.Add(-time.Hour), End: published.Add(time.Hour),
	}
	item := types.ContentItem{
		ID: 31, URL: "https://openai.com/release",
		Content: "official", PublishedAt: &published,
	}
	result := eventqualifier.Result{
		Outcome: "match",
		Events: []eventqualifier.Event{
			{
				EventType: "model_release", Subject: "OpenAI models",
				ReleaseIdentifier: "gpt-one",
				OccurredAt:        published.Format(time.RFC3339),
				Qualification: string(
					observation.QualificationGeneralAvailability),
				EvidenceContentIDs: []int64{31},
			},
			{
				EventType: "model_release", Subject: "OpenAI models",
				ReleaseIdentifier: "gpt-two",
				OccurredAt:        published.Format(time.RFC3339),
				Qualification: string(
					observation.QualificationGeneralAvailability),
				EvidenceContentIDs: []int64{31},
			},
		},
	}
	if _, _, err := (&Activities{}).validateQualifiedEvents(
		policy, digest, window, []types.ContentItem{item}, "", result,
	); err == nil {
		t.Fatal("one primary candidate was reused for two events")
	}
}

func TestAdmissibleEventEvidenceCandidatesHonorBoundedLateness(t *testing.T) {
	policy := workflowEventPolicy(t)
	policy.LatePolicy = observation.LateBounded
	policy.AllowedLatenessSecs = int64((2 * time.Hour) / time.Second)
	window := observation.Window{
		Start: time.Date(2026, 7, 23, 1, 0, 0, 0, time.UTC),
		End:   time.Date(2026, 7, 24, 1, 0, 0, 0, time.UTC),
	}
	withinLateness := window.Start.Add(-time.Hour)
	beyondLateness := window.Start.Add(-2 * time.Hour)
	got := admissibleEventEvidenceCandidates(policy, window, []types.ContentItem{
		{ID: 1, URL: "https://openai.com/index/accepted", PublishedAt: &withinLateness},
		{ID: 2, URL: "https://openai.com/index/rejected", PublishedAt: &beyondLateness},
	})
	if len(got) != 1 || got[0].ID != 1 {
		t.Fatalf("got=%+v", got)
	}
}

func workflowEventPolicy(t *testing.T) observation.PolicyV1 {
	t.Helper()
	policy, err := observation.Compile(observation.PolicySpecV1{
		Schema: observation.SchemaV1, Mode: observation.ModeEvent,
		Window:     observation.WindowSpecV1{Kind: observation.WindowScheduleInterval},
		LatePolicy: observation.LateStrict,
		Evidence: observation.EvidencePolicyV1{
			Requirement:     observation.EvidenceOfficialRequired,
			OfficialDomains: []string{"openai.com"},
		},
		UnknownTime: observation.UnknownTimeReject,
		Event: &observation.EventPolicyV1{
			Subject: "OpenAI models", EventKind: "model_release",
			Qualification: observation.QualificationGeneralAvailability,
		},
		QualifierPrompt: observation.QualifierPromptV1,
	}, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	return policy
}
