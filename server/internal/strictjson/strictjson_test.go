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

func TestValidateRejectsInvalidUTF8BeforeDecoderReplacement(t *testing.T) {
	t.Parallel()
	raw := append([]byte(`{"value":"`), 0xff)
	raw = append(raw, []byte(`"}`)...)
	if err := Validate(raw); err == nil {
		t.Fatal("Validate accepted invalid UTF-8 that encoding/json would replace")
	}
}

func TestValidateRejectsUnpairedSurrogateEscapes(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{
		`{"value":"\ud800"}`,
		`{"value":"\ud801"}`,
		`{"value":"\udfff"}`,
		`{"value":"\ud800\u0041"}`,
	} {
		if err := Validate([]byte(raw)); err == nil {
			t.Fatalf("Validate(%s) accepted an unpaired surrogate", raw)
		}
	}
	for _, raw := range []string{
		`{"value":"\ud83d\ude80"}`,
		`{"value":"literal \\ud800"}`,
	} {
		if err := Validate([]byte(raw)); err != nil {
			t.Fatalf("Validate(%s) rejected valid JSON: %v", raw, err)
		}
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
