package runcontext

import (
	"bytes"
	"strings"
	"testing"
)

func TestResearchPlannerToolSearchReceiptV1CanonicalRoundTrip(t *testing.T) {
	digest := strings.Repeat("a", 64)
	score, err := CanonicalResearchPlannerSearchScoreV1(1.25)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := BuildResearchPlannerToolSearchReceiptV1(1, digest,
		"official release search", 8, []ResearchPlannerToolSearchMatchV1{{
			Name: "web_search", SchemaDigest: strings.Repeat("b", 64), Score: score,
		}})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := EncodeResearchPlannerToolSearchReceiptV1(receipt)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeResearchPlannerToolSearchReceiptV1(payload)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, _ := EncodeResearchPlannerToolSearchReceiptV1(decoded)
	if !bytes.Equal(payload, reencoded) {
		t.Fatalf("receipt bytes drifted: %s != %s", payload, reencoded)
	}
	if _, err := DigestResearchPlannerToolSearchReceiptV1(decoded); err != nil {
		t.Fatal(err)
	}
}

func TestResearchPlannerToolSearchReceiptV1RejectsAuthorityMutations(t *testing.T) {
	digest := strings.Repeat("a", 64)
	base, err := BuildResearchPlannerToolSearchReceiptV1(0, digest, "official status", 2,
		[]ResearchPlannerToolSearchMatchV1{{
			Name: "web_search", SchemaDigest: strings.Repeat("b", 64), Score: "1.250000000",
		}})
	if err != nil {
		t.Fatal(err)
	}
	mutations := []ResearchPlannerToolSearchReceiptV1{base, base, base, base, base, base}
	mutations[0].CatalogDigest = "bad"
	mutations[1].Query = " official status"
	mutations[2].Limit = 9
	mutations[3].Matches = append(mutations[3].Matches, mutations[3].Matches[0])
	mutations[4].Matches[0].SchemaDigest = strings.Repeat("c", 63)
	mutations[5].Matches[0].Score = "1.25000000"
	for index, mutation := range mutations {
		if mutation.Validate() == nil {
			t.Fatalf("mutation %d crossed receipt validation", index)
		}
	}
	validPayload, _ := EncodeResearchPlannerToolSearchReceiptV1(base)
	duplicate := bytes.Replace(validPayload,
		[]byte(`"query":"official status"`),
		[]byte(`"query":"attacker","query":"official status"`), 1)
	if _, err := DecodeResearchPlannerToolSearchReceiptV1(duplicate); err == nil {
		t.Fatal("duplicate-key receipt crossed strict decoder")
	}
}
