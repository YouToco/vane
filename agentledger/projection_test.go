package agentledger

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestBuildAndProjectLatestSessionSnapshot(t *testing.T) {
	first := mustProjectionBatch(t, ProjectionSnapshotInput{
		Scope:  Scope{TenantID: 1, UserID: 2, SessionID: 3},
		TurnID: "turn-1",
		Messages: json.RawMessage(`[
			{"role":"user","content":"hello"},
			{"role":"assistant","content":"","tool_calls":[
				{"id":"call-1","name":"list_sources","arguments":"{}"}
			]},
			{"role":"tool","content":"none","tool_call_id":"call-1"},
			{"role":"assistant","content":"done"}
		]`),
		TurnCount:      2,
		ActivatedTools: json.RawMessage(`["endpoint_a"]`),
	})
	second := mustProjectionBatch(t, ProjectionSnapshotInput{
		Scope:  first.Scope,
		TurnID: "turn-2",
		Messages: json.RawMessage(`[
			{"role":"user","content":"new"},
			{"role":"assistant","content":"done"}
		]`),
		TurnCount:      3,
		ActivatedTools: json.RawMessage(`["endpoint_a","endpoint_b"]`),
	})

	events := append(materializeProjectionBatch(t, first, 1),
		materializeProjectionBatch(t, second, int64(len(first.Events)+1))...)
	projected, err := ProjectLatestSessionSnapshot(events)
	if err != nil {
		t.Fatal(err)
	}
	want := SessionProjection{
		Messages: json.RawMessage(
			`[{"role":"user","content":"new"},{"role":"assistant","content":"done"}]`,
		),
		TurnCount:      3,
		ActivatedTools: json.RawMessage(`["endpoint_a","endpoint_b"]`),
	}
	gotDigest, err := ProjectionDigest(projected)
	if err != nil {
		t.Fatal(err)
	}
	wantDigest, err := ProjectionDigest(want)
	if err != nil {
		t.Fatal(err)
	}
	if gotDigest != wantDigest {
		t.Fatalf("latest snapshot digest=%s want=%s", gotDigest, wantDigest)
	}
}

func TestBuildProjectionSnapshotBatchBounds(t *testing.T) {
	baseDigest, err := ProjectionDigest(SessionProjection{
		Messages:       json.RawMessage("[]"),
		ActivatedTools: json.RawMessage("[]"),
	})
	if err != nil {
		t.Fatal(err)
	}
	messages := make([]map[string]string, 61)
	for i := range messages {
		messages[i] = map[string]string{"role": "user", "content": "x"}
	}
	raw, err := json.Marshal(messages)
	if err != nil {
		t.Fatal(err)
	}
	_, err = BuildProjectionSnapshotBatch(ProjectionSnapshotInput{
		Scope:                Scope{TenantID: 1, UserID: 2, SessionID: 3},
		TurnID:               "turn-bounds",
		BaseProjectionDigest: baseDigest,
		Messages:             raw,
	})
	if !errors.Is(err, ErrInvalidProjection) {
		t.Fatalf("61 messages error=%v, want ErrInvalidProjection", err)
	}

	sixty := messages[:60]
	raw, err = json.Marshal(sixty)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := BuildProjectionSnapshotBatch(ProjectionSnapshotInput{
		Scope:                Scope{TenantID: 1, UserID: 2, SessionID: 3},
		TurnID:               "turn-at-limit",
		BaseProjectionDigest: baseDigest,
		Messages:             raw,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Events) != 62 {
		t.Fatalf("event count=%d want=62", len(batch.Events))
	}
}

func TestProjectLatestSessionSnapshotRejectsIncompleteGeneration(t *testing.T) {
	batch := mustProjectionBatch(t, ProjectionSnapshotInput{
		Scope:    Scope{TenantID: 1, UserID: 2, SessionID: 3},
		TurnID:   "turn-incomplete",
		Messages: json.RawMessage(`[{"role":"user","content":"hello"}]`),
	})
	events := materializeProjectionBatch(t, batch, 1)
	events[len(events)-1].Kind = KindAssistantMessage
	if _, err := ProjectLatestSessionSnapshot(events); !errors.Is(
		err, ErrInvalidProjection,
	) {
		t.Fatalf("incomplete generation error=%v, want ErrInvalidProjection", err)
	}
}

func TestProjectionDigestDoesNotExposeBodiesInErrors(t *testing.T) {
	secret := "secret-token-must-not-escape"
	_, err := ProjectionDigest(SessionProjection{
		Messages: json.RawMessage(
			`[{"role":"assistant","content":"","tool_calls":[` +
				`{"id":"call","name":"tool","arguments":"not-json-` + secret + `"}]}]`,
		),
	})
	if err == nil {
		t.Fatal("invalid arguments unexpectedly accepted")
	}
	if got := err.Error(); contains(got, secret) {
		t.Fatalf("error leaked message body: %q", got)
	}
}

func TestAppendProjectionMessagesTruncatesAtUserBoundary(t *testing.T) {
	messages := make([]map[string]string, 60)
	for i := range messages {
		role := "assistant"
		if i%3 == 0 {
			role = "user"
		}
		messages[i] = map[string]string{
			"role": role, "content": string(rune('a' + i%26)),
		}
	}
	current, err := json.Marshal(messages)
	if err != nil {
		t.Fatal(err)
	}
	got, err := AppendProjectionMessages(
		current,
		json.RawMessage(`[{"role":"user","content":"callback"}]`),
	)
	if err != nil {
		t.Fatal(err)
	}
	var projected []map[string]any
	if err := json.Unmarshal(got, &projected); err != nil {
		t.Fatal(err)
	}
	if len(projected) > 41 {
		t.Fatalf("truncated message count=%d want <=41", len(projected))
	}
	if projected[0]["role"] != "user" ||
		projected[0]["content"] != messages[0]["content"] {
		t.Fatalf("first user intent not preserved: %#v", projected[0])
	}
	if projected[1]["role"] != "user" {
		t.Fatalf("recent boundary role=%v want user", projected[1]["role"])
	}
	if projected[len(projected)-1]["content"] != "callback" {
		t.Fatalf("callback not retained: %#v", projected[len(projected)-1])
	}
}

func TestProjectionSnapshotTurnID(t *testing.T) {
	batch := mustProjectionBatch(t, ProjectionSnapshotInput{
		Scope:    Scope{TenantID: 1, UserID: 2, SessionID: 3},
		TurnID:   "side-request-1",
		Messages: json.RawMessage(`[{"role":"user","content":"callback"}]`),
	})
	canonical, err := Canonicalize(batch.Events[0])
	if err != nil {
		t.Fatal(err)
	}
	got, err := ProjectionSnapshotTurnID(canonical)
	if err != nil {
		t.Fatal(err)
	}
	if got != "side-request-1" {
		t.Fatalf("turn id=%q want side-request-1", got)
	}
}

func TestAppendProjectionMessagesRejectsEmptyAppend(t *testing.T) {
	if _, err := AppendProjectionMessages(
		json.RawMessage(`[{"role":"user","content":"existing"}]`),
		json.RawMessage(`[]`),
	); !errors.Is(err, ErrInvalidProjection) {
		t.Fatalf("empty append error=%v, want ErrInvalidProjection", err)
	}
}

func mustProjectionBatch(
	t *testing.T,
	input ProjectionSnapshotInput,
) AppendBatch {
	t.Helper()
	if input.BaseProjectionDigest == "" {
		digest, err := ProjectionDigest(SessionProjection{
			Messages:       json.RawMessage("[]"),
			ActivatedTools: json.RawMessage("[]"),
		})
		if err != nil {
			t.Fatal(err)
		}
		input.BaseProjectionDigest = digest
	}
	batch, err := BuildProjectionSnapshotBatch(input)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func materializeProjectionBatch(
	t *testing.T,
	batch AppendBatch,
	firstSequence int64,
) []Event {
	t.Helper()
	canonical := make([]CanonicalEvent, len(batch.Events))
	for i := range batch.Events {
		event, err := Canonicalize(batch.Events[i])
		if err != nil {
			t.Fatal(err)
		}
		canonical[i] = event
	}
	batchDigest, err := BatchDigest(canonical)
	if err != nil {
		t.Fatal(err)
	}
	events := make([]Event, len(canonical))
	for i := range canonical {
		events[i] = Event{
			ID:             firstSequence + int64(i),
			Scope:          batch.Scope,
			Sequence:       firstSequence + int64(i),
			IdempotencyKey: batch.IdempotencyKey,
			BatchIndex:     i,
			BatchSize:      len(canonical),
			Kind:           canonical[i].Kind(),
			SchemaVersion:  SchemaVersion,
			Payload:        canonical[i].Payload(),
			PayloadDigest:  canonical[i].Digest(),
			BatchDigest:    batchDigest,
		}
	}
	return events
}

func contains(value, fragment string) bool {
	for i := 0; i+len(fragment) <= len(value); i++ {
		if value[i:i+len(fragment)] == fragment {
			return true
		}
	}
	return false
}
