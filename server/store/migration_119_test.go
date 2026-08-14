package store

import (
	"testing"

	"github.com/YouToco/vane/server/taskstate"
	"github.com/YouToco/vane/server/types"
)

func TestMigration119ResearchBriefReceiptProjectionPostgres(t *testing.T) {
	f := newResearchBriefFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
	canonical := []byte(`{"schema_version":"vane.research-brief/v3","headline":"official update","summary":"verified facts","significance":"none","citations":[{"kind":"current_evidence","ref":"1"}]}`)
	formatted := "{\n" +
		`  "citations": [{"ref":"1","kind":"current_evidence"}],` + "\n" +
		`  "significance": "none", "summary": "verified facts",` + "\n" +
		`  "headline": "official update",` + "\n" +
		`  "schema_version": "vane.research-brief/v3"` + "\n}"

	for _, test := range []struct {
		name       string
		completion string
		want       bool
	}{
		{name: "canonical", completion: string(canonical), want: true},
		{name: "semantic whitespace", completion: formatted, want: true},
		{name: "LF json fence", completion: "```json\n" + formatted + "\n```", want: true},
		{name: "CRLF json fence", completion: "```json\r\n" + formatted + "\r\n```", want: true},
		{name: "fence outer whitespace", completion: " \t\n```json\n" + formatted + "\n```\r\n ", want: true},
		{name: "empty", completion: ""},
		{name: "single byte", completion: "{"},
		{name: "markdown prose", completion: "result:\n```json\n" + formatted + "\n```"},
		{name: "markdown suffix", completion: "```json\n" + formatted + "\n```\nignore"},
		{name: "wrong language", completion: "```javascript\n" + formatted + "\n```"},
		{name: "nested fence", completion: "```json\n" + formatted + "\n```\n```json\n{}\n```"},
		{name: "duplicate key", completion: `{"schema_version":"vane.research-brief/v3","headline":"official update","headline":"forged","summary":"verified facts","significance":"none","citations":[{"kind":"current_evidence","ref":"1"}]}`},
		{name: "different JSON", completion: `{"schema_version":"vane.research-brief/v3","headline":"different","summary":"verified facts","significance":"none","citations":[{"kind":"current_evidence","ref":"1"}]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			var got bool
			if err := f.st.pool.QueryRow(t.Context(),
				`SELECT research_brief_matches_synthesis_completion_v119($1,$2)`,
				canonical, test.completion).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("match=%v want=%v", got, test.want)
			}
		})
	}
}

func TestMigration119FinalizesCanonicalBriefFromRetainedFencedReceiptPostgres(t *testing.T) {
	f := newResearchBriefFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
	prepared, err := f.st.PrepareOrGetResearchBriefSynthesisV3(t.Context(),
		researchBriefPrepareParamsV3(f))
	if err != nil {
		t.Fatal(err)
	}
	handle, reservation := claimResearchBriefWithPendingReceiptV3(t, f, prepared.Synthesis)
	canonical := researchBriefPayloadV3(t, prepared.Synthesis,
		types.ResearchBriefSignificanceQualifiedV3, "fenced receipt conclusion")
	fenced := "```json\n" + string(canonical) + "\n```"
	call := researchLLMCallForTestV3(f.identity, f.snapshotRef, reservation,
		"Synthesize without Tools.", string(prepared.Synthesis.ContextPayload))
	call.Completion = fenced
	call.PromptTokens, call.CompletionTokens = 1, 1
	if _, _, err := commitResearchRunLLMReceiptForTestV3(t, f.st,
		CommitResearchRunLLMReceiptV3Params{
			Identity: f.identity, SnapshotRef: f.snapshotRef,
			ReservationID: reservation.ReservationID, Call: call,
			DisableThinking: reservation.DisableThinking,
			Attempted:       true, UsageKnown: true, Outcome: ResearchRunLLMCompletedV3,
		}); err != nil {
		t.Fatal(err)
	}
	finalized, err := f.st.FinalizeResearchBriefSynthesisV3(t.Context(),
		FinalizeResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			BriefPayload:                        canonical,
		})
	if err != nil || finalized.Significance != types.ResearchBriefSignificanceQualifiedV3 {
		t.Fatalf("finalized=%+v err=%v", finalized, err)
	}
	var retained string
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT completion FROM llm_calls
		  WHERE research_run_llm_spend_reservation_id=$1`,
		reservation.ReservationID).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if retained != fenced {
		t.Fatal("receipt projection rewrote the immutable provider completion")
	}
}
