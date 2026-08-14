package strictjson

import (
	"encoding/json"
	"testing"
)

type exactDecodeFixture struct {
	Name   string `json:"name"`
	Nested struct {
		Count int `json:"count"`
	} `json:"nested"`
	Opaque   json.RawMessage `json:"opaque"`
	Optional string          `json:"optional,omitempty"`
}

func TestDecodeExact_RejectsCaseFoldedAndEscapedSchemaKeys(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		ok   bool
	}{
		{
			name: "exact keys and opaque schema island",
			raw:  `{"name":"ok","nested":{"count":1},"opaque":{"ANY_Key":true}}`,
			ok:   true,
		},
		{
			name: "case alias",
			raw:  `{"NAME":"override","nested":{"count":1},"opaque":{}}`,
		},
		{
			name: "case fold duplicate",
			raw:  `{"name":"first","NAME":"override","nested":{"count":1},"opaque":{}}`,
		},
		{
			name: "escaped exact key",
			raw:  `{"\u006eame":"override","nested":{"count":1},"opaque":{}}`,
		},
		{
			name: "nested case alias",
			raw:  `{"name":"ok","nested":{"COUNT":1},"opaque":{}}`,
		},
		{
			name: "unknown field",
			raw:  `{"name":"ok","nested":{"count":1},"opaque":{},"future":true}`,
		},
		{
			name: "missing required root field",
			raw:  `{"nested":{"count":1},"opaque":{}}`,
		},
		{
			name: "missing required nested field",
			raw:  `{"name":"ok","nested":{},"opaque":{}}`,
		},
		{
			name: "missing required opaque field",
			raw:  `{"name":"ok","nested":{"count":1}}`,
		},
		{
			name: "null required scalar",
			raw:  `{"name":null,"nested":{"count":1},"opaque":{}}`,
		},
		{
			name: "null required nested scalar",
			raw:  `{"name":"ok","nested":{"count":null},"opaque":{}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got exactDecodeFixture
			err := DecodeExact([]byte(tt.raw), &got)
			if tt.ok && err != nil {
				t.Fatalf("DecodeExact() error = %v", err)
			}
			if !tt.ok && err == nil {
				t.Fatalf("DecodeExact() accepted %s", tt.raw)
			}
		})
	}
}
