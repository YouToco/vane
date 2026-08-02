package types

import (
	"strings"
	"testing"
)

func TestResearchBriefPayloadV3CanonicalSignificanceAndCitations(t *testing.T) {
	payload := ResearchBriefPayloadV3{
		SchemaVersion: ResearchBriefPayloadSchemaV3,
		Headline:      "Kimi 套餐状态",
		Summary:       "仍需预约，没有达到推送门槛。",
		Significance:  ResearchBriefSignificanceQualifiedV3,
		Citations: []ResearchBriefCitationV3{{
			Kind: ResearchBriefCitationCurrentEvidenceV3, Ref: "17",
		}},
	}
	encoded, err := EncodeResearchBriefPayloadV3(payload)
	if err != nil {
		t.Fatal(err)
	}
	decoded, canonical, err := DecodeResearchBriefPayloadV3(encoded)
	if err != nil || decoded.Significance != payload.Significance || string(canonical) != string(encoded) {
		t.Fatalf("decoded=%+v canonical=%s err=%v", decoded, canonical, err)
	}

	forged := payload
	forged.Significance = ResearchBriefSignificanceMajorV3
	forgedBytes, err := EncodeResearchBriefPayloadV3(forged)
	if err != nil {
		t.Fatal(err)
	}
	changed, _, err := DecodeResearchBriefPayloadV3(forgedBytes)
	if err != nil || changed.Significance != ResearchBriefSignificanceMajorV3 {
		t.Fatalf("significance must be derived from the exact payload: %+v err=%v", changed, err)
	}
}

func TestResearchBriefPayloadV3RejectsUngroundedOrNoncanonicalContent(t *testing.T) {
	base := ResearchBriefPayloadV3{
		SchemaVersion: ResearchBriefPayloadSchemaV3,
		Headline:      "Headline", Summary: "Summary",
		Significance: ResearchBriefSignificanceNoneV3,
	}
	invalid := []ResearchBriefPayloadV3{
		base,
		func() ResearchBriefPayloadV3 {
			value := base
			value.Citations = []ResearchBriefCitationV3{{Kind: ResearchBriefCitationHistoryV3, Ref: "brief:1"}}
			return value
		}(),
		func() ResearchBriefPayloadV3 {
			value := base
			value.Citations = []ResearchBriefCitationV3{{Kind: ResearchBriefCitationCurrentEvidenceV3, Ref: "0"}}
			return value
		}(),
	}
	for index, payload := range invalid {
		if _, err := EncodeResearchBriefPayloadV3(payload); err == nil {
			t.Fatalf("invalid payload %d passed", index)
		}
	}
	valid := base
	valid.Citations = []ResearchBriefCitationV3{{Kind: ResearchBriefCitationCurrentEvidenceV3, Ref: "1"}}
	encoded, err := EncodeResearchBriefPayloadV3(valid)
	if err != nil {
		t.Fatal(err)
	}
	noncanonical := append([]byte(" "), encoded...)
	if _, _, err := DecodeResearchBriefPayloadV3(noncanonical); err == nil {
		t.Fatal("non-canonical Brief payload passed")
	}
}

func researchBriefRefFixtureV3(t *testing.T) (RunIdentity, ResearchBriefRefV3) {
	t.Helper()
	digest := strings.Repeat("a", 64)
	identity := RunIdentity{
		TemporalWorkflowID: "workflow-v3", TemporalRunID: "run-v3",
		RunKind: RunSnapshotKindScheduled, TenantID: 7, UserID: 9, TaskID: "task-v3",
	}
	ref, err := SealResearchBriefRefV3(ResearchBriefRefV3{
		BriefID: 11, RunSnapshotID: 12, PlanID: 13,
		TemporalWorkflowID: identity.TemporalWorkflowID, TemporalRunID: identity.TemporalRunID,
		TenantID: identity.TenantID, UserID: identity.UserID, TaskID: identity.TaskID,
		DefinitionDigest: digest, PlanDigest: digest, RequestDigest: digest,
		BriefDigest: digest, EvidenceDigest: digest, HistoryDigest: digest,
		NotificationThreshold: "all_qualified_updates",
		Significance:          ResearchBriefSignificanceQualifiedV3,
		Decision:              ResearchBriefDecisionDeliverV3, DeliveryRequired: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return identity, ref
}

func TestResearchBriefRefV3SealAndValidate(t *testing.T) {
	identity, ref := researchBriefRefFixtureV3(t)
	if err := ref.ValidateFor(identity, ref.RunSnapshotID, ref.PlanID); err != nil {
		t.Fatal(err)
	}
	mutations := []func(*ResearchBriefRefV3){
		func(r *ResearchBriefRefV3) { r.UserID++ },
		func(r *ResearchBriefRefV3) { r.BriefDigest = strings.Repeat("b", 64) },
		func(r *ResearchBriefRefV3) { r.DeliveryRequired = false },
		func(r *ResearchBriefRefV3) { r.Decision = ResearchBriefDecisionQuietV3 },
		func(r *ResearchBriefRefV3) { r.NotificationThreshold = "unknown" },
	}
	for index, mutate := range mutations {
		invalid := ref
		mutate(&invalid)
		if err := invalid.ValidateFor(identity, ref.RunSnapshotID, ref.PlanID); err == nil {
			t.Fatalf("mutation %d passed", index)
		}
	}
}

func TestResearchBriefRefV3NotificationMatrix(t *testing.T) {
	identity, base := researchBriefRefFixtureV3(t)
	tests := []struct {
		threshold    string
		significance ResearchBriefSignificanceV3
		decision     ResearchBriefDecisionV3
		deliver      bool
	}{
		{"major_updates_only", ResearchBriefSignificanceNoneV3, ResearchBriefDecisionQuietV3, false},
		{"major_updates_only", ResearchBriefSignificanceQualifiedV3, ResearchBriefDecisionQuietV3, false},
		{"major_updates_only", ResearchBriefSignificanceMajorV3, ResearchBriefDecisionDeliverV3, true},
		{"all_qualified_updates", ResearchBriefSignificanceNoneV3, ResearchBriefDecisionQuietV3, false},
		{"all_qualified_updates", ResearchBriefSignificanceQualifiedV3, ResearchBriefDecisionDeliverV3, true},
		{"all_qualified_updates", ResearchBriefSignificanceMajorV3, ResearchBriefDecisionDeliverV3, true},
	}
	for _, test := range tests {
		candidate := base
		candidate.NotificationThreshold = test.threshold
		candidate.Significance = test.significance
		candidate.Decision = test.decision
		candidate.DeliveryRequired = test.deliver
		sealed, err := SealResearchBriefRefV3(candidate)
		if err != nil {
			t.Fatalf("%s/%s: %v", test.threshold, test.significance, err)
		}
		if err := sealed.ValidateFor(identity, sealed.RunSnapshotID, sealed.PlanID); err != nil {
			t.Fatalf("%s/%s validate: %v", test.threshold, test.significance, err)
		}
	}
}
