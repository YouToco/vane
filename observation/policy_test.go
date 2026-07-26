package observation

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestDecodePolicyV1Exact_FlattenedEmbeddedWire(t *testing.T) {
	want := mustScheduleIntervalPolicy(
		t, time.Date(2026, 7, 25, 1, 0, 0, 0, time.UTC),
	)
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodePolicyV1Exact(raw)
	if err != nil {
		t.Fatalf("DecodePolicyV1Exact() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded policy = %+v, want %+v", got, want)
	}

	tests := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{
			name: "unknown field",
			mutate: func(value []byte) []byte {
				return bytes.Replace(value, []byte(`"schema":`),
					[]byte(`"future":true,"schema":`), 1)
			},
		},
		{
			name: "case folded field",
			mutate: func(value []byte) []byte {
				return bytes.Replace(value, []byte(`"schema"`),
					[]byte(`"SCHEMA"`), 1)
			},
		},
		{
			name: "missing effective at",
			mutate: func(value []byte) []byte {
				return bytes.Replace(value,
					[]byte(`,"effective_at":"2026-07-25T01:00:00Z"`), nil, 1)
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := DecodePolicyV1Exact(
				testCase.mutate(bytes.Clone(raw)),
			); err == nil {
				t.Fatal("DecodePolicyV1Exact() accepted invalid wire")
			}
		})
	}
}

func TestScheduleIntervalUsesNominalAndOpenClosedBoundary(t *testing.T) {
	effective := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	policy, err := Compile(PolicySpecV1{
		Schema: SchemaV1, Mode: ModeEvent,
		Window:     WindowSpecV1{Kind: WindowScheduleInterval},
		LatePolicy: LateStrict,
		Evidence: EvidencePolicyV1{
			Requirement:     EvidenceOfficialRequired,
			OfficialDomains: []string{"openai.com"},
		},
		UnknownTime: UnknownTimeReject,
		Event: &EventPolicyV1{
			Subject: "OpenAI models", EventKind: "model_release",
			Qualification: QualificationGeneralAvailability,
		},
		QualifierPrompt: QualifierPromptV1,
	}, effective)
	if err != nil {
		t.Fatal(err)
	}
	nominal := time.Date(2026, 7, 24, 1, 0, 0, 0, time.UTC)
	window, err := WindowForNominal(policy, Schedule{
		Cron: "0 9 * * *", TimeZone: "Asia/Shanghai",
	}, nominal)
	if err != nil {
		t.Fatal(err)
	}
	wantStart := time.Date(2026, 7, 23, 1, 0, 0, 0, time.UTC)
	if !window.Start.Equal(wantStart) || !window.End.Equal(nominal) {
		t.Fatalf("window=%+v want (%s,%s]", window, wantStart, nominal)
	}
	if window.Contains(window.Start) {
		t.Fatal("start boundary must be excluded")
	}
	if !window.Contains(window.End) {
		t.Fatal("end boundary must be included")
	}
}

func TestFirstRunClampsToPolicyEffectiveAt(t *testing.T) {
	nominal := time.Date(2026, 7, 24, 1, 0, 0, 0, time.UTC)
	effective := nominal.Add(-2 * time.Hour)
	policy, err := Compile(PolicySpecV1{
		Schema: SchemaV1, Mode: ModeContent,
		Window: WindowSpecV1{
			Kind: WindowRollingDuration, RollingDurationSeconds: 30 * 24 * 3600,
		},
		LatePolicy:  LateStrict,
		Evidence:    EvidencePolicyV1{Requirement: EvidenceTrustedAllowed},
		UnknownTime: UnknownTimeDeprioritize,
	}, effective)
	if err != nil {
		t.Fatal(err)
	}
	window, err := WindowForNominal(policy, Schedule{TimeZone: "Asia/Shanghai"}, nominal)
	if err != nil {
		t.Fatal(err)
	}
	if !window.Start.Equal(effective) {
		t.Fatalf("start=%s want effective_at=%s", window.Start, effective)
	}
}

func TestScheduleIntervalAcceptsSundaySevenAlias(t *testing.T) {
	policy := mustScheduleIntervalPolicy(t, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	nominal := time.Date(2026, 7, 12, 9, 0, 0, 0, time.UTC)
	window, err := WindowForNominal(policy, Schedule{
		Cron: "0 9 * * 7", TimeZone: "UTC",
	}, nominal)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 7, 5, 9, 0, 0, 0, time.UTC)
	if !window.Start.Equal(want) {
		t.Fatalf("start=%s want=%s", window.Start, want)
	}
}

func TestScheduleIntervalRespectsDSTCalendarPredecessor(t *testing.T) {
	policy := mustScheduleIntervalPolicy(t, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	cases := []struct {
		name    string
		nominal time.Time
		want    time.Time
	}{
		{
			name:    "spring forward is twenty three hours",
			nominal: time.Date(2026, 3, 8, 13, 0, 0, 0, time.UTC),
			want:    time.Date(2026, 3, 7, 14, 0, 0, 0, time.UTC),
		},
		{
			name:    "fall back is twenty five hours",
			nominal: time.Date(2026, 11, 1, 14, 0, 0, 0, time.UTC),
			want:    time.Date(2026, 10, 31, 13, 0, 0, 0, time.UTC),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			window, err := WindowForNominal(policy, Schedule{
				Cron: "0 9 * * *", TimeZone: "America/New_York",
			}, tc.nominal)
			if err != nil {
				t.Fatal(err)
			}
			if !window.Start.Equal(tc.want) {
				t.Fatalf("start=%s want=%s", window.Start, tc.want)
			}
		})
	}
}

func TestScheduleIntervalMatchesTemporalCalendarDOMAndDOW(t *testing.T) {
	policy := mustScheduleIntervalPolicy(t, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	nominal := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC) // Monday and day 1.
	window, err := WindowForNominal(policy, Schedule{
		Cron: "0 9 1 * 1", TimeZone: "UTC",
	}, nominal)
	if err != nil {
		t.Fatal(err)
	}
	// Temporal calendar fields are conjunctive. Unix cron OR semantics would
	// incorrectly choose a recent Monday or recent first-of-month.
	want := time.Date(2025, 12, 1, 9, 0, 0, 0, time.UTC)
	if !window.Start.Equal(want) {
		t.Fatalf("start=%s want=%s", window.Start, want)
	}
}

func mustScheduleIntervalPolicy(t *testing.T, effective time.Time) PolicyV1 {
	t.Helper()
	policy, err := Compile(PolicySpecV1{
		Schema: SchemaV1, Mode: ModeContent,
		Window:      WindowSpecV1{Kind: WindowScheduleInterval},
		LatePolicy:  LateStrict,
		Evidence:    EvidencePolicyV1{Requirement: EvidenceTrustedAllowed},
		UnknownTime: UnknownTimeReject,
	}, effective)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}
