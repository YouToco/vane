package taskstate

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/YouToco/vane/types"
)

const goldenAdaptiveStateV1 = `{"schema_version":"vane.task-adaptive-state/v1","tenant_id":7,"user_id":11,"task_id":"task-adaptive-v1","query_variants":[{"source_id":1,"query":"AI \"新品\" 🚀"},{"source_id":1,"query":"AI official releases"}],"capability_order":[{"platform":"web","capability":"search"},{"platform":"web","capability":"feed"}],"source_order":[1],"run_stats":{"attempted_runs":8,"successful_runs":5,"empty_runs":1,"failed_runs":2,"consecutive_failures":1}}`

const goldenAdaptiveStateV1Digest = "759015e118d9d2ee877a25e18e6a320a0e0124ab2663fb7b82a22b91ff61f084"

func TestAdaptiveStateV1_ExactCanonicalGolden(t *testing.T) {
	t.Parallel()
	input := validAdaptiveStateInputV1()
	input.QueryVariants[0].Query = `AI "新品" 🚀`
	state, err := BuildAdaptiveStateV1(input)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := EncodeAdaptiveStateV1(state)
	if err != nil {
		t.Fatal(err)
	}
	if string(canonical) != goldenAdaptiveStateV1 {
		t.Fatalf("adaptive V1 canonical wire drifted:\n got: %s\nwant: %s",
			canonical, goldenAdaptiveStateV1)
	}
	digest, err := DigestAdaptiveStateV1(state)
	if err != nil || digest != goldenAdaptiveStateV1Digest {
		t.Fatalf("adaptive V1 digest=%q err=%v want=%q",
			digest, err, goldenAdaptiveStateV1Digest)
	}
	decoded, err := DecodeAdaptiveStateV1([]byte(goldenAdaptiveStateV1))
	if err != nil {
		t.Fatalf("decode retained adaptive V1 golden: %v", err)
	}
	reencoded, err := EncodeAdaptiveStateV1(decoded)
	if err != nil || string(reencoded) != goldenAdaptiveStateV1 {
		t.Fatalf("retained adaptive V1 reencode=%s err=%v", reencoded, err)
	}
}

func TestAdaptiveStateV1_CanonicalRoundTripDigestAndCopies(t *testing.T) {
	t.Parallel()
	input := validAdaptiveStateInputV1()
	state, err := BuildAdaptiveStateV1(input)
	if err != nil {
		t.Fatalf("BuildAdaptiveStateV1() error = %v", err)
	}
	canonical, err := EncodeAdaptiveStateV1(state)
	if err != nil {
		t.Fatalf("EncodeAdaptiveStateV1() error = %v", err)
	}
	decoded, err := DecodeAdaptiveStateV1(canonical)
	if err != nil {
		t.Fatalf("DecodeAdaptiveStateV1() error = %v", err)
	}
	reencoded, err := EncodeAdaptiveStateV1(decoded)
	if err != nil || !bytes.Equal(canonical, reencoded) {
		t.Fatalf("canonical round trip: encoded=%s reencoded=%s err=%v", canonical, reencoded, err)
	}
	digest, err := DigestAdaptiveStateV1(state)
	if err != nil || len(digest) != 64 || strings.ToLower(digest) != digest {
		t.Fatalf("DigestAdaptiveStateV1() = %q, %v", digest, err)
	}

	input.QueryVariants[0].Query = "mutated"
	input.CapabilityOrder[0].Capability = types.CapContents
	input.SourceOrder[0] = 99
	if state.QueryVariants[0].Query == "mutated" ||
		state.CapabilityOrder[0].Capability == types.CapContents || state.SourceOrder[0] == 99 {
		t.Fatal("built adaptive state aliases caller-owned slices")
	}
}

func TestAdaptiveStateV1_StrictReader(t *testing.T) {
	t.Parallel()
	state, err := BuildAdaptiveStateV1(validAdaptiveStateInputV1())
	if err != nil {
		t.Fatal(err)
	}
	valid, err := EncodeAdaptiveStateV1(state)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "unknown root field", payload: insertBeforeLastObjectByte(valid, `,"mode":"compiled"`)},
		{name: "null root", payload: []byte(`null`)},
		{name: "duplicate root field", payload: insertBeforeLastObjectByte(valid, `,"task_id":"other"`)},
		{name: "case folded field", payload: bytes.Replace(valid, []byte(`"source_order"`), []byte(`"Source_Order"`), 1)},
		{name: "missing root field", payload: bytes.Replace(valid, []byte(`"source_order":[1],`), nil, 1)},
		{name: "null query variants", payload: replaceJSONFieldValue(valid, "query_variants", `null`)},
		{name: "null capability order", payload: replaceJSONFieldValue(valid, "capability_order", `null`)},
		{name: "null source order", payload: replaceJSONFieldValue(valid, "source_order", `null`)},
		{name: "null run stats", payload: replaceJSONFieldValue(valid, "run_stats", `null`)},
		{name: "unpaired surrogate", payload: bytes.Replace(valid, []byte(`"query":"AI latest"`), []byte(`"query":"\ud801"`), 1)},
		{name: "escaped paragraph separator", payload: bytes.Replace(valid, []byte(`"query":"AI latest"`), []byte(`"query":"AI\u2029latest"`), 1)},
		{name: "unknown nested query field", payload: bytes.Replace(valid, []byte(`"query":"AI latest"`), []byte(`"query":"AI latest","url":"https://evil.example"`), 1)},
		{name: "missing nested query field", payload: bytes.Replace(valid, []byte(`"source_id":1,`), nil, 1)},
		{name: "duplicate nested stat", payload: bytes.Replace(valid, []byte(`"attempted_runs":8`), []byte(`"attempted_runs":8,"attempted_runs":9`), 1)},
		{name: "trailing value", payload: append(slices.Clone(valid), []byte(` []`)...)},
		{name: "invalid utf8", payload: append(slices.Clone(valid[:len(valid)-1]), 0xff, '}')},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if _, err := DecodeAdaptiveStateV1(testCase.payload); !errors.Is(err, ErrInvalidState) {
				t.Fatalf("DecodeAdaptiveStateV1() error = %v", err)
			}
		})
	}
}

func TestAdaptiveStateV1_InitialStateUsesNonNullArrays(t *testing.T) {
	t.Parallel()
	input := validAdaptiveStateInputV1()
	input.QueryVariants = []QueryVariantV1{}
	input.CapabilityOrder = []ReadCapabilityV1{}
	input.SourceOrder = []int64{}
	input.RunStats = RunStatsV1{}
	state, err := BuildAdaptiveStateV1(input)
	if err != nil {
		t.Fatalf("BuildAdaptiveStateV1() error = %v", err)
	}
	payload, err := EncodeAdaptiveStateV1(state)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"query_variants", "capability_order", "source_order"} {
		if !bytes.Contains(payload, []byte(`"`+field+`":[]`)) {
			t.Fatalf("%s is not an explicit empty array: %s", field, payload)
		}
	}
}

func TestAdaptiveStateV1_RejectsInvalidBoundsAndDuplicates(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*AdaptiveStateInputV1)
	}{
		{name: "tenant missing", mutate: func(i *AdaptiveStateInputV1) { i.TenantID = 0 }},
		{name: "task id invalid utf8", mutate: func(i *AdaptiveStateInputV1) { i.TaskID = string([]byte{0xff}) }},
		{name: "nil query variants", mutate: func(i *AdaptiveStateInputV1) { i.QueryVariants = nil }},
		{name: "nil capability order", mutate: func(i *AdaptiveStateInputV1) { i.CapabilityOrder = nil }},
		{name: "nil source order", mutate: func(i *AdaptiveStateInputV1) { i.SourceOrder = nil }},
		{name: "query source id missing", mutate: func(i *AdaptiveStateInputV1) { i.QueryVariants[0].SourceID = 0 }},
		{name: "query empty", mutate: func(i *AdaptiveStateInputV1) { i.QueryVariants[0].Query = "" }},
		{name: "query control rune", mutate: func(i *AdaptiveStateInputV1) { i.QueryVariants[0].Query = "AI\nlatest" }},
		{name: "query line separator", mutate: func(i *AdaptiveStateInputV1) { i.QueryVariants[0].Query = "AI\u2028latest" }},
		{name: "query too long", mutate: func(i *AdaptiveStateInputV1) { i.QueryVariants[0].Query = strings.Repeat("q", maxQueryBytes+1) }},
		{name: "too many queries", mutate: func(i *AdaptiveStateInputV1) {
			i.QueryVariants = make([]QueryVariantV1, maxQueryVariantCount+1)
		}},
		{name: "duplicate query", mutate: func(i *AdaptiveStateInputV1) { i.QueryVariants = append(i.QueryVariants, i.QueryVariants[0]) }},
		{name: "invalid capability pair", mutate: func(i *AdaptiveStateInputV1) {
			i.CapabilityOrder[0] = ReadCapabilityV1{Platform: types.PlatformX, Capability: types.CapSearch}
		}},
		{name: "too many capabilities", mutate: func(i *AdaptiveStateInputV1) {
			i.CapabilityOrder = make([]ReadCapabilityV1, maxCapabilityCount+1)
		}},
		{name: "duplicate capability", mutate: func(i *AdaptiveStateInputV1) { i.CapabilityOrder = append(i.CapabilityOrder, i.CapabilityOrder[0]) }},
		{name: "source id missing", mutate: func(i *AdaptiveStateInputV1) { i.SourceOrder[0] = 0 }},
		{name: "too many sources", mutate: func(i *AdaptiveStateInputV1) {
			i.SourceOrder = make([]int64, maxSourceCount+1)
		}},
		{name: "duplicate source id", mutate: func(i *AdaptiveStateInputV1) { i.SourceOrder = append(i.SourceOrder, i.SourceOrder[0]) }},
		{name: "negative stats", mutate: func(i *AdaptiveStateInputV1) { i.RunStats.FailedRuns = -1 }},
		{name: "stats exceed attempts", mutate: func(i *AdaptiveStateInputV1) { i.RunStats.SuccessfulRuns = 9 }},
		{name: "stats do not sum", mutate: func(i *AdaptiveStateInputV1) { i.RunStats.EmptyRuns = 2 }},
		{name: "consecutive exceeds failures", mutate: func(i *AdaptiveStateInputV1) { i.RunStats.ConsecutiveFailures = 3 }},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			input := validAdaptiveStateInputV1()
			testCase.mutate(&input)
			if _, err := BuildAdaptiveStateV1(input); !errors.Is(err, ErrInvalidState) {
				t.Fatalf("BuildAdaptiveStateV1() error = %v", err)
			}
		})
	}
}

func TestAdaptiveStateV1_WireHasNoApprovedOrSideEffectFields(t *testing.T) {
	t.Parallel()
	state, err := BuildAdaptiveStateV1(validAdaptiveStateInputV1())
	if err != nil {
		t.Fatal(err)
	}
	payload, err := EncodeAdaptiveStateV1(state)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(payload, &object); err != nil {
		t.Fatal(err)
	}
	wantFields := []string{
		"schema_version", "tenant_id", "user_id", "task_id", "query_variants",
		"capability_order", "source_order", "run_stats",
	}
	gotFields := make([]string, 0, len(object))
	for field := range object {
		gotFields = append(gotFields, field)
	}
	slices.Sort(gotFields)
	slices.Sort(wantFields)
	if !slices.Equal(gotFields, wantFields) {
		t.Fatalf("adaptive wire fields = %v", gotFields)
	}

	forbidden := []string{
		"url", "topic", "schedule", "mode", "channel", "delivery", "budget",
		"account", "secret", "credential", "token", "code", "sql", "source_config",
		"last_known_good",
	}
	var document any
	if err := json.Unmarshal(payload, &document); err != nil {
		t.Fatal(err)
	}
	assertNoForbiddenJSONKeys(t, document, forbidden)

	// Keep an independent type-level check so a zero-length slice cannot make a
	// newly added nested DTO field vacuously disappear from the payload probe.
	for _, schema := range []reflect.Type{
		reflect.TypeOf(AdaptiveStateV1{}), reflect.TypeOf(QueryVariantV1{}),
		reflect.TypeOf(ReadCapabilityV1{}), reflect.TypeOf(RunStatsV1{}),
	} {
		for i := 0; i < schema.NumField(); i++ {
			jsonName := strings.Split(schema.Field(i).Tag.Get("json"), ",")[0]
			for _, fragment := range forbidden {
				if strings.Contains(jsonName, fragment) {
					t.Fatalf("adaptive field %q contains forbidden fragment %q", jsonName, fragment)
				}
			}
		}
	}
}

func TestAdaptiveStateV1_RejectsAggregatePayloadOverLimit(t *testing.T) {
	t.Parallel()
	input := validAdaptiveStateInputV1()
	input.QueryVariants = make([]QueryVariantV1, maxQueryVariantCount)
	for i := range input.QueryVariants {
		input.QueryVariants[i] = QueryVariantV1{
			SourceID: 1,
			Query:    strings.Repeat("q", maxQueryBytes-3) + strconv.Itoa(i),
		}
	}
	if _, err := BuildAdaptiveStateV1(input); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("oversize adaptive state error = %v", err)
	}
}

func assertNoForbiddenJSONKeys(t *testing.T, value any, forbidden []string) {
	t.Helper()
	switch typed := value.(type) {
	case map[string]any:
		for key, nested := range typed {
			for _, fragment := range forbidden {
				if strings.Contains(key, fragment) {
					t.Fatalf("adaptive json key %q contains forbidden fragment %q", key, fragment)
				}
			}
			assertNoForbiddenJSONKeys(t, nested, forbidden)
		}
	case []any:
		for _, nested := range typed {
			assertNoForbiddenJSONKeys(t, nested, forbidden)
		}
	}
}

func validAdaptiveStateInputV1() AdaptiveStateInputV1 {
	return AdaptiveStateInputV1{
		TenantID: 7,
		UserID:   11,
		TaskID:   "task-adaptive-v1",
		QueryVariants: []QueryVariantV1{
			{SourceID: 1, Query: "AI latest"},
			{SourceID: 1, Query: "AI official releases"},
		},
		CapabilityOrder: []ReadCapabilityV1{
			{Platform: types.PlatformWeb, Capability: types.CapSearch},
			{Platform: types.PlatformWeb, Capability: types.CapFeed},
		},
		SourceOrder: []int64{1},
		RunStats: RunStatsV1{
			AttemptedRuns: 8, SuccessfulRuns: 5, EmptyRuns: 1,
			FailedRuns: 2, ConsecutiveFailures: 1,
		},
	}
}

func FuzzDecodeAdaptiveStateV1(f *testing.F) {
	state, err := BuildAdaptiveStateV1(validAdaptiveStateInputV1())
	if err != nil {
		f.Fatal(err)
	}
	canonical, err := EncodeAdaptiveStateV1(state)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(canonical)
	f.Add([]byte(`null`))
	f.Add([]byte(`{"schema_version":"vane.task-adaptive-state/v1"}`))
	f.Fuzz(func(t *testing.T, payload []byte) {
		decoded, err := DecodeAdaptiveStateV1(payload)
		if err != nil {
			return
		}
		if !utf8.Valid(payload) {
			t.Fatal("decoder accepted invalid utf8")
		}
		if err := decoded.Validate(); err != nil {
			t.Fatalf("decoded value failed validation: %v", err)
		}
	})
}
