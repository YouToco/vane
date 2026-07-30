package task

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/observation"
)

func TestAlignOfficialEventFetchPlanUsesApprovedDomainsAsAuthority(t *testing.T) {
	policy, err := observation.Compile(observation.PolicySpecV1{
		Schema: observation.SchemaV1, Mode: observation.ModeEvent,
		Window:     observation.WindowSpecV1{Kind: observation.WindowScheduleInterval},
		LatePolicy: observation.LateStrict,
		Evidence: observation.EvidencePolicyV1{
			Requirement: observation.EvidenceOfficialRequired,
			OfficialDomains: []string{
				"openai.com", "anthropic.com", "deepmind.google",
			},
		},
		UnknownTime: observation.UnknownTimeReject,
		Event: &observation.EventPolicyV1{
			Subject: "OpenAI、Anthropic、Google DeepMind",
			EventKind: "新模型发布、API定价变化、功能弃用、重大能力更新",
			Qualification: observation.QualificationAnnouncement,
		},
		QualifierPrompt: observation.QualifierPromptV1,
	}, time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	plan := compiledFetchPlan{Targets: []compiledFetchTarget{
		mustDefinitionEditSearchTarget(t,
			"OpenAI new model release", []string{"openai.com"}),
		mustDefinitionEditSearchTarget(t,
			"Anthropic Claude release", []string{"anthropic.com"}),
		mustDefinitionEditSearchTarget(t,
			"Google Gemini release",
			[]string{"ai.google", "blog.google", "developers.google.com"}),
	}}
	got, err := alignOfficialEventFetchPlan(policy, plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Targets) != 3 {
		t.Fatalf("targets=%+v", got.Targets)
	}
	var domains []string
	for _, target := range got.Targets {
		var config definitionEditSearchConfig
		if err := strictjson.DecodeExact(target.Config, &config); err != nil {
			t.Fatal(err)
		}
		domains = append(domains, config.IncludeDomains...)
		if config.Query == "Google Gemini release" &&
			!slices.Equal(config.IncludeDomains, []string{"deepmind.google"}) {
			t.Fatalf("Google target domains=%v", config.IncludeDomains)
		}
	}
	slices.Sort(domains)
	if !slices.Equal(
		domains,
		[]string{"anthropic.com", "deepmind.google", "openai.com"},
	) {
		t.Fatalf("compiled official domain coverage=%v", domains)
	}
}

func mustDefinitionEditSearchTarget(
	t *testing.T,
	query string,
	domains []string,
) compiledFetchTarget {
	t.Helper()
	target, err := rebuildDefinitionEditSearchTarget(
		compiledFetchTarget{}, query, "", domains,
	)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(target)
	if err != nil || !json.Valid(canonical) {
		t.Fatalf("target=%+v err=%v", target, err)
	}
	return target
}
