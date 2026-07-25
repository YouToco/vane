package feedback

import "testing"

func TestParseAggMetadataPreservesOnlyConsistentEffectMarker(t *testing.T) {
	const effectID = "019f9824-39b6-7e13-b247-b5ee5713c52b"
	card := []byte(`{
		"header":{"title":{"content":"title"},"template":"blue"},
		"body":{"elements":[
			{"behaviors":[{"value":{"effect_id":"` + effectID + `"}}]},
			{"behaviors":[{"value":{"effect_id":"` + effectID + `"}}]}
		]}
	}`)
	title, template, gotEffectID := parseAggMetadata(card)
	if title != "title" || template != "blue" || gotEffectID != effectID {
		t.Fatalf("metadata = %q/%q/%q", title, template, gotEffectID)
	}

	drifted := []byte(`{
		"header":{"title":{"content":"title"},"template":"blue"},
		"body":{"elements":[
			{"value":{"effect_id":"one"}},
			{"value":{"effect_id":"two"}}
		]}
	}`)
	title, template, gotEffectID = parseAggMetadata(drifted)
	if title != "title" || template != "blue" || gotEffectID != "" {
		t.Fatalf("drifted metadata = %q/%q/%q, want marker rejected",
			title, template, gotEffectID)
	}
}
