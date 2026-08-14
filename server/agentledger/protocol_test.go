package agentledger

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestCanonicalize(t *testing.T) {
	t.Parallel()

	got, err := Canonicalize(Input{
		Kind: KindUserMessage,
		Body: []byte(`{"z":1,"nested":{"b":2,"a":1},"a":"hello"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []byte(
		`{"schema_version":"vane.agent-event/v1","kind":"user_message",` +
			`"body":{"a":"hello","nested":{"a":1,"b":2},"z":1}}`,
	)
	if !bytes.Equal(got.Payload(), want) {
		t.Fatalf("canonical payload=%s, want %s", got.Payload(), want)
	}
	decoded, err := Decode(got.Payload(), got.Digest())
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Kind() != KindUserMessage ||
		!bytes.Equal(decoded.Payload(), got.Payload()) {
		t.Fatalf("decoded event drifted: kind=%q payload=%s",
			decoded.Kind(), decoded.Payload())
	}

	// Accessors must not let a caller mutate the sealed bytes.
	mutated := got.Payload()
	mutated[0] = '['
	if got.Payload()[0] != '{' {
		t.Fatal("canonical payload accessor leaked its backing array")
	}
}

func TestCanonicalizeRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input Input
	}{
		{name: "unknown kind", input: Input{Kind: "future", Body: []byte(`{}`)}},
		{name: "null body", input: Input{Kind: KindUserMessage, Body: []byte(`null`)}},
		{name: "array body", input: Input{Kind: KindUserMessage, Body: []byte(`[]`)}},
		{name: "duplicate key", input: Input{
			Kind: KindUserMessage, Body: []byte(`{"text":"a","text":"b"}`),
		}},
		{name: "multiple values", input: Input{
			Kind: KindUserMessage, Body: []byte(`{} {}`),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := Canonicalize(tt.input); !errors.Is(err, ErrInvalidEvent) {
				t.Fatalf("Canonicalize() error=%v, want ErrInvalidEvent", err)
			}
		})
	}
}

func TestDecodeFailsClosed(t *testing.T) {
	t.Parallel()

	event, err := Canonicalize(Input{
		Kind: KindToolCall,
		Body: []byte(`{"call_id":"call-1","name":"view_profile","arguments":{}}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		payload []byte
		digest  string
	}{
		{
			name: "noncanonical bytes",
			payload: []byte(
				`{"kind":"tool_call","schema_version":"vane.agent-event/v1",` +
					`"body":{"arguments":{},"call_id":"call-1","name":"view_profile"}}`,
			),
			digest: event.Digest(),
		},
		{name: "digest corruption", payload: event.Payload(), digest: "0" + event.Digest()[1:]},
		{
			name: "unknown outer field",
			payload: []byte(
				`{"schema_version":"vane.agent-event/v1","kind":"tool_call",` +
					`"body":{},"extra":true}`,
			),
			digest: event.Digest(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := Decode(tt.payload, tt.digest); !errors.Is(err, ErrInvalidEvent) {
				t.Fatalf("Decode() error=%v, want ErrInvalidEvent", err)
			}
		})
	}
}

func TestBatchDigestIsOrderSensitive(t *testing.T) {
	t.Parallel()

	first, err := Canonicalize(Input{Kind: KindTurnStarted, Body: []byte(`{"turn_id":"t1"}`)})
	if err != nil {
		t.Fatal(err)
	}
	second, err := Canonicalize(Input{Kind: KindUserMessage, Body: []byte(`{"text":"hello"}`)})
	if err != nil {
		t.Fatal(err)
	}
	forward, err := BatchDigest([]CanonicalEvent{first, second})
	if err != nil {
		t.Fatal(err)
	}
	reverse, err := BatchDigest([]CanonicalEvent{second, first})
	if err != nil {
		t.Fatal(err)
	}
	if forward == reverse {
		t.Fatal("batch digest must bind event order")
	}
}

func TestCanonicalizeV1PreservesNumberLexemes(t *testing.T) {
	t.Parallel()

	digests := make(map[string]struct{})
	for _, body := range []string{`{"n":1}`, `{"n":1.0}`, `{"n":1e0}`} {
		event, err := Canonicalize(Input{
			Kind: KindToolResult, Body: []byte(body),
		})
		if err != nil {
			t.Fatal(err)
		}
		digests[event.Digest()] = struct{}{}
	}
	if len(digests) != 3 {
		t.Fatalf("v1 numeric lexical forms collapsed: %v", digests)
	}
}

func TestCanonicalizeBodySizeBoundaryAndDigestCase(t *testing.T) {
	t.Parallel()

	body := `{"v":"` + strings.Repeat("x", maxEventBodyBytes-8) + `"}`
	event, err := Canonicalize(Input{
		Kind: KindAssistantMessage, Body: []byte(body),
	})
	if err != nil {
		t.Fatalf("exact body limit rejected: %v", err)
	}
	tooLarge := `{"v":"` + strings.Repeat("x", maxEventBodyBytes-7) + `"}`
	if _, err := Canonicalize(Input{
		Kind: KindAssistantMessage, Body: []byte(tooLarge),
	}); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("max+1 body error=%v, want ErrInvalidEvent", err)
	}
	if _, err := Decode(event.Payload(), strings.ToUpper(event.Digest())); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("uppercase digest error=%v, want ErrInvalidEvent", err)
	}
}
