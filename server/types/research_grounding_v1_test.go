package types

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestResearchGroundingVerdictV1CanonicalContract(t *testing.T) {
	digest := strings.Repeat("a", 64)
	grounded := ResearchGroundingVerdictPayloadV1{
		SchemaVersion:   ResearchGroundingVerdictSchemaV1,
		CandidateDigest: digest, Verdict: ResearchGroundingGroundedV1,
		Issues: []ResearchGroundingIssueV1{},
	}
	payload, err := json.Marshal(grounded)
	if err != nil {
		t.Fatal(err)
	}
	decoded, canonical, err := DecodeResearchGroundingVerdictV1(payload)
	if err != nil || decoded.Verdict != ResearchGroundingGroundedV1 ||
		string(canonical) != string(payload) {
		t.Fatalf("decoded=%+v canonical=%s err=%v", decoded, canonical, err)
	}
	formatted := append([]byte("\n  "), payload...)
	formatted = append(formatted, '\n')
	if normalized, gotCanonical, err := NormalizeResearchGroundingVerdictV1(formatted); err != nil ||
		normalized.Verdict != ResearchGroundingGroundedV1 || string(gotCanonical) != string(payload) {
		t.Fatalf("normalized=%+v canonical=%s err=%v", normalized, gotCanonical, err)
	}

	unsupported := grounded
	unsupported.Verdict = ResearchGroundingUnsupportedV1
	unsupported.Issues = []ResearchGroundingIssueV1{{
		Field: "summary", Claim: "Claude Opus 5 was released.",
		Refs: []ResearchBriefCitationV3{{
			Kind: ResearchBriefCitationCurrentEvidenceV3, Ref: "428",
		}},
		Reason: "Evidence 428 only describes OpenAI.",
	}}
	payload, _ = json.Marshal(unsupported)
	if _, _, err := DecodeResearchGroundingVerdictV1(payload); err != nil {
		t.Fatal(err)
	}
}

func TestResearchGroundingVerdictV1RejectsUnsafeShapes(t *testing.T) {
	digest := strings.Repeat("b", 64)
	for name, raw := range map[string]string{
		"grounded with issue": `{"schema_version":"vane.research-grounding-verdict/v1","candidate_digest":"` + digest + `","verdict":"grounded","issues":[{"field":"summary","claim":"x","refs":[],"reason":"y"}]}`,
		"unsupported empty":   `{"schema_version":"vane.research-grounding-verdict/v1","candidate_digest":"` + digest + `","verdict":"unsupported","issues":[]}`,
		"unknown field":       `{"schema_version":"vane.research-grounding-verdict/v1","candidate_digest":"` + digest + `","verdict":"grounded","issues":[],"write_action":"delete"}`,
		"noncanonical":        "{\n\"schema_version\":\"vane.research-grounding-verdict/v1\",\"candidate_digest\":\"" + digest + "\",\"verdict\":\"grounded\",\"issues\":[]}",
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := DecodeResearchGroundingVerdictV1([]byte(raw)); err == nil {
				t.Fatal("unsafe grounding verdict accepted")
			}
		})
	}
}
