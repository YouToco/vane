package strictjson

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestValidateRejectsDuplicateKeysAndTrailingValues(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{
		`{"a":1,"a":2}`,
		`{"nested":{"a":1,"a":2}}`,
		`[{"a":1,"a":2}]`,
		`{"a":1} {"b":2}`,
	} {
		if err := Validate([]byte(raw)); err == nil {
			t.Fatalf("Validate(%s) unexpectedly succeeded", raw)
		}
	}
	if err := Validate([]byte(`{"a":[1,{"b":2}]}`)); err != nil {
		t.Fatalf("valid JSON rejected: %v", err)
	}
}

func TestDecodeRejectsUnknownFieldsAndPreservesWideNumbers(t *testing.T) {
	t.Parallel()
	type envelope struct {
		Value any `json:"value"`
	}
	var decoded envelope
	if err := Decode([]byte(`{"value":9007199254740993}`), &decoded); err != nil {
		t.Fatal(err)
	}
	number, ok := decoded.Value.(json.Number)
	if !ok || number.String() != "9007199254740993" {
		t.Fatalf("wide number changed: %#v", decoded.Value)
	}
	if err := Decode([]byte(`{"value":1,"extra":true}`), &decoded); err == nil ||
		!strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field was not rejected: %v", err)
	}
}
