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
		Title: "GPT Test", Content: "GPT Test is generally available.",
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
			ID: 21, URL: "https://openai.com/release",
			Title: "GPT Test", Content: "GPT Test official release body",
			PublishedAt: &officialAt,
		},
		{
			ID: 22, URL: "https://media.example/report",
			Title: "GPT Test ships", Content: "Independent GPT Test report",
			PublishedAt: &crossAt,
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
		"监控 OpenAI；必须有官方原文交叉核验；输出交叉证据。", result)
	if err != nil || outcome != "match" || len(got) != 1 {
		t.Fatalf("manual evidence pair got=%+v outcome=%q err=%v",
			got, outcome, err)
	}
	result.Events[0].EvidenceContentIDs = []int64{21}
	if _, _, err := activities.validateQualifiedEvents(
		policy, digest, window, items,
		"监控 OpenAI；必须有官方原文交叉核验；输出交叉证据。", result,
	); err == nil {
		t.Fatal("manual cross-evidence requirement admitted one source")
	}
}

func TestValidateQualifiedEventsKeepsValidEventFromMixedBatch(t *testing.T) {
	policy := workflowEventPolicy(t)
	policy.Event.Subject = "Frontier AI releases"
	policy.Evidence.Requirement = observation.EvidenceTrustedAllowed
	policy.Evidence.OfficialDomains = nil
	digest, err := observation.PolicyDigest(policy)
	if err != nil {
		t.Fatal(err)
	}
	releasedAt := time.Date(2026, 7, 24, 17, 0, 0, 0, time.UTC)
	crossAt := releasedAt.Add(time.Hour)
	window := observation.Window{
		Start: releasedAt.Add(-time.Hour), End: crossAt.Add(time.Hour),
	}
	items := []types.ContentItem{
		{
			ID: 1, URL: "https://releasebot.io/updates/anthropic/claude-opus-5",
			Title: "Claude Opus 5", Content: "Claude Opus 5 release tracker",
			PublishedAt: &releasedAt,
		},
		{
			ID: 2, URL: "https://blog.google/products/google-cloud/managed-agents/",
			Title:       "Google Managed Agents",
			Content:     "Google Managed Agents are generally available",
			PublishedAt: &releasedAt,
		},
		{
			ID: 3, URL: "https://gcn.com/cloud/google-managed-agents",
			Title:       "Google Managed Agents launch",
			Content:     "Google Managed Agents reach general availability",
			PublishedAt: &crossAt,
		},
	}
	result := eventqualifier.Result{Outcome: "match", Events: []eventqualifier.Event{
		{
			EventType: "model_release", Subject: "Frontier AI releases",
			ReleaseIdentifier: "Claude Opus 5",
			OccurredAt:        releasedAt.Format(time.RFC3339),
			Qualification: string(
				observation.QualificationGeneralAvailability),
			EvidenceContentIDs: []int64{1, 3},
		},
		{
			EventType: "model_release", Subject: "Frontier AI releases",
			ReleaseIdentifier: "Google Managed Agents",
			OccurredAt:        releasedAt.Format(time.RFC3339),
			Qualification: string(
				observation.QualificationGeneralAvailability),
			EvidenceContentIDs: []int64{2, 3},
		},
	}}
	got, outcome, err := (&Activities{}).validateQualifiedEvents(
		policy, digest, window, items,
		"监控 Anthropic 和 Google 官方原文；必须交叉核验。", result)
	if err != nil || outcome != "match" || len(got) != 1 || got[0].ID != 2 {
		t.Fatalf("mixed batch got=%+v outcome=%q err=%v", got, outcome, err)
	}
}

func TestValidateQualifiedEventsRejectsUnrelatedCrossEvidence(t *testing.T) {
	policy := workflowEventPolicy(t)
	policy.Evidence.Requirement = observation.EvidenceTrustedAllowed
	policy.Evidence.OfficialDomains = nil
	digest, err := observation.PolicyDigest(policy)
	if err != nil {
		t.Fatal(err)
	}
	releasedAt := time.Date(2026, 7, 24, 17, 0, 0, 0, time.UTC)
	crossAt := releasedAt.Add(time.Hour)
	window := observation.Window{
		Start: releasedAt.Add(-time.Hour), End: crossAt.Add(time.Hour),
	}
	items := []types.ContentItem{
		{
			ID: 1, URL: "https://openai.com/index/gpt-live-transcribe",
			Title:       "GPT Live Transcribe",
			Content:     "GPT Live Transcribe adds transcription",
			PublishedAt: &releasedAt,
		},
		{
			ID: 2, URL: "https://media.example/gpt-5-6-arc-agi",
			Title:       "GPT 5.6 ARC-AGI settings",
			Content:     "Testing GPT 5.6 on ARC-AGI",
			PublishedAt: &crossAt,
		},
	}
	result := eventqualifier.Result{Outcome: "match", Events: []eventqualifier.Event{{
		EventType: "model_release", Subject: "OpenAI models",
		ReleaseIdentifier: "GPT Live Transcribe transcription",
		OccurredAt:        releasedAt.Format(time.RFC3339),
		Qualification: string(
			observation.QualificationGeneralAvailability),
		EvidenceContentIDs: []int64{1, 2},
	}}}
	if _, _, err := (&Activities{}).validateQualifiedEvents(
		policy, digest, window, items,
		"监控 OpenAI 官方原文；必须交叉核验。", result,
	); err == nil || types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("unrelated cross evidence err=%v", err)
	}
}

func TestOfficialHostGroundedInTaskManualRejectsPathImpersonation(t *testing.T) {
	manual := "监控 OpenAI、Anthropic 和 Google DeepMind 的官方原文。"
	for rawURL, want := range map[string]bool{
		"https://openai.com/index/model":                   true,
		"https://www.anthropic.com/news/model":             true,
		"https://ai.google.dev/gemini-api/docs/changelog":  true,
		"https://blog.google/technology/ai/update":         true,
		"https://cloud.google.com/blog/products/ai/update": true,
		"https://deepmind.google/discover/blog/model":      true,
		"https://releasebot.io/updates/anthropic/model":    false,
		"https://media.example/openai/model":               false,
		"http://openai.com/insecure":                       false,
	} {
		if got := officialHostGroundedInTaskManual(rawURL, manual); got != want {
			t.Errorf("officialHostGroundedInTaskManual(%q)=%t want %t",
				rawURL, got, want)
		}
	}
}

func TestOfficialContentIDsForTaskUsesHostNotBranding(t *testing.T) {
	items := []types.ContentItem{
		{ID: 1, URL: "https://www.anthropic.com/news/claude-opus-5"},
		{ID: 2, URL: "https://blog.google/technology/ai/update"},
		{ID: 3, URL: "https://releasebot.io/updates/anthropic/claude-opus-5"},
	}
	got := officialContentIDsForTask(
		items,
		"监控 Anthropic 和 Google DeepMind；必须有官方原文交叉核验。",
	)
	if _, ok := got[1]; !ok {
		t.Fatal("Anthropic official host was not marked")
	}
	if _, ok := got[2]; !ok {
		t.Fatal("Google official host was not marked")
	}
	if _, ok := got[3]; ok {
		t.Fatal("third-party branding was marked official")
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
