package taskstate

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/server/types"
)

func TestToolInvocationV1DigestCanonicalAndVersioned(t *testing.T) {
	first, err := BuildToolInvocationV1("weibo_user_posts", "v1",
		json.RawMessage(`{"uid":"2803301701","options":{"page":1,"mode":"latest"}}`))
	if err != nil {
		t.Fatal(err)
	}
	equivalent, err := BuildToolInvocationV1("weibo_user_posts", "v1",
		json.RawMessage(`{"options":{"mode":"latest","page":1},"uid":"2803301701"}`))
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest != equivalent.Digest ||
		!bytes.Equal(first.Arguments, equivalent.Arguments) {
		t.Fatal("equivalent JSON arguments must produce one canonical invocation digest")
	}
	changedVersion, err := BuildToolInvocationV1("weibo_user_posts", "v2",
		first.Arguments)
	if err != nil {
		t.Fatal(err)
	}
	if changedVersion.Digest == first.Digest {
		t.Fatal("tool contract version must participate in invocation digest")
	}
	changedArgs, err := BuildToolInvocationV1("weibo_user_posts", "v1",
		json.RawMessage(`{"uid":"2803301702","options":{"page":1,"mode":"latest"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if changedArgs.Digest == first.Digest {
		t.Fatal("arguments must participate in invocation digest")
	}
}

func TestToolInvocationV1RejectsTamperingAndNonObjectArguments(t *testing.T) {
	invocation, err := BuildToolInvocationV1("x_user_posts", "v1",
		json.RawMessage(`{"screen_name":"openai"}`))
	if err != nil {
		t.Fatal(err)
	}
	invocation.Arguments = json.RawMessage(`{"screen_name":"anthropic"}`)
	if _, err := EncodeToolInvocationV1(invocation); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("tampered invocation error = %v", err)
	}
	if _, err := BuildToolInvocationV1("x_user_posts", "v1",
		json.RawMessage(`["openai"]`)); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("array arguments error = %v", err)
	}
}

func TestApprovedDefinitionV2ContainsToolCallsWithoutSourceModel(t *testing.T) {
	weibo, err := BuildToolInvocationV1("weibo_user_posts", "v1",
		json.RawMessage(`{"uid":"2803301701"}`))
	if err != nil {
		t.Fatal(err)
	}
	wechat, err := BuildToolInvocationV1("wechat_mp_user_posts", "v1",
		json.RawMessage(`{"username":"gh_363b924965e9"}`))
	if err != nil {
		t.Fatal(err)
	}
	definition, err := BuildApprovedDefinitionV2(ApprovedDefinitionInputV2{
		TenantID: 7, UserID: 11, TaskID: "boss-watch",
		NLDescription:  "每天检查并在有更新时推送",
		SpecJSON:       json.RawMessage(`{"tz":"Asia/Shanghai","cron":"0 8 * * *"}`),
		ScopeJSON:      json.RawMessage(`{"max_items":10}`),
		TaskManual:     "关注微博账号 2803301701 与公众号 gh_363b924965e9 的新内容。",
		Strictness:     types.PushStrictness("normal"),
		ToolCalls:      []ToolInvocationV1{weibo, wechat},
		ExecutionMode:  types.ExecutionModeCompiled,
		DeliveryPolicy: DeliveryPolicyOwnerFeishu,
		BudgetPolicy:   BudgetPolicyInheritTenantQuota,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := EncodeApprovedDefinitionV2(definition)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		`"source_id"`, `"sources"`, `"source_scope"`, `"fetch_plan"`,
		`"platform"`, `"capability"`, `"url"`, `"config"`,
	} {
		if strings.Contains(string(payload), forbidden) {
			t.Fatalf("Source-era field leaked into V2 definition: %s", forbidden)
		}
	}
	decoded, err := DecodeApprovedDefinitionV2(payload)
	if err != nil || len(decoded.ToolCalls) != 2 ||
		decoded.ToolCalls[0].Digest != weibo.Digest {
		t.Fatalf("V2 definition round trip failed: decoded=%+v err=%v", decoded, err)
	}
}

func TestApprovedDefinitionV2ReaderRetainsRetiredTools(t *testing.T) {
	current, err := BuildToolInvocationV1("web_search", "v1",
		json.RawMessage(`{"query":"history"}`))
	if err != nil {
		t.Fatal(err)
	}
	definition, err := BuildApprovedDefinitionV2(ApprovedDefinitionInputV2{
		TenantID: 7, UserID: 11, TaskID: "history",
		NLDescription: "retain history",
		SpecJSON:      json.RawMessage(`{}`), ScopeJSON: json.RawMessage(`{}`),
		TaskManual: "retain the approved historical call", ToolCalls: []ToolInvocationV1{current},
		ExecutionMode:  types.ExecutionModeCompiled,
		DeliveryPolicy: DeliveryPolicyOwnerFeishu,
		BudgetPolicy:   BudgetPolicyInheritTenantQuota,
	})
	if err != nil {
		t.Fatal(err)
	}
	retired, err := BuildToolInvocationV1("retired_acquisition_tool", "v7",
		json.RawMessage(`{"historic":true}`))
	if err != nil {
		t.Fatal(err)
	}
	definition.ToolCalls = []ToolInvocationV1{retired}
	payload, err := EncodeApprovedDefinitionV2(definition)
	if err != nil {
		t.Fatalf("frozen reader must encode a structurally valid retired call: %v", err)
	}
	if _, err := DecodeApprovedDefinitionV2(payload); err != nil {
		t.Fatalf("frozen reader must retain retired calls: %v", err)
	}
	if err := ValidateApprovedDefinitionV2ForWrite(definition); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("current writer must reject retired calls: %v", err)
	}
}

func TestApprovedDefinitionV2WriteRejectsSecretToolArguments(t *testing.T) {
	call, err := BuildToolInvocationV1(
		"web_search", "v1",
		json.RawMessage(`{"query":"AI","api_key":"SECRET-CANARY"}`))
	if err != nil {
		t.Fatal(err)
	}
	_, err = BuildApprovedDefinitionV2(ApprovedDefinitionInputV2{
		TenantID: 1, UserID: 2, TaskID: "secret-arguments",
		SpecJSON: json.RawMessage(`{}`), ScopeJSON: json.RawMessage(`{}`),
		NLDescription: "monitor AI",
		TaskManual:    "monitor AI", Strictness: types.StrictnessNormal,
		ToolCalls:      []ToolInvocationV1{call},
		ExecutionMode:  types.ExecutionModeCompiled,
		DeliveryPolicy: DeliveryPolicyOwnerFeishu,
		BudgetPolicy:   BudgetPolicyInheritTenantQuota,
	})
	if !errors.Is(err, ErrInvalidState) {
		t.Fatalf("secret Tool arguments were accepted: %v", err)
	}
}

func TestAdaptiveStateV2IsInvocationDigestScopedAndCanonical(t *testing.T) {
	left, err := BuildToolInvocationV1("x_user_posts", "v1",
		json.RawMessage(`{"screen_name":"openai"}`))
	if err != nil {
		t.Fatal(err)
	}
	right, err := BuildToolInvocationV1("weibo_user_posts", "v1",
		json.RawMessage(`{"uid":"2803301701"}`))
	if err != nil {
		t.Fatal(err)
	}
	last := time.Date(2026, 7, 29, 12, 0, 0, 0, time.FixedZone("offset", 8*60*60))
	state, err := BuildAdaptiveStateV2(AdaptiveStateInputV2{
		TenantID: 7, UserID: 11, TaskID: "boss-watch",
		InvocationStates: []InvocationAdaptiveStateV1{
			{
				InvocationDigest: right.Digest, Cursor: json.RawMessage(`{"page":2,"token":"b"}`),
				Status: InvocationStatusBackoff, LastFetchedAt: &last, FailCount: 2,
			},
			{
				InvocationDigest: left.Digest, Cursor: json.RawMessage(`{}`),
				Status: InvocationStatusActive,
			},
		},
		RunStats: RunStatsV1{AttemptedRuns: 2, SuccessfulRuns: 1, FailedRuns: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.InvocationStates[0].InvocationDigest > state.InvocationStates[1].InvocationDigest {
		t.Fatal("invocation states must be canonically ordered by digest")
	}
	payload, err := EncodeAdaptiveStateV2(state)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte(`source_id`)) {
		t.Fatal("adaptive V2 must not contain a Source ID")
	}
	decoded, err := DecodeAdaptiveStateV2(payload)
	if err != nil {
		t.Fatal(err)
	}
	for _, current := range decoded.InvocationStates {
		if current.InvocationDigest == right.Digest &&
			(current.LastFetchedAt == nil ||
				current.LastFetchedAt.Location() != time.UTC ||
				current.LastFetchedAt.Hour() != 4) {
			t.Fatalf("timestamp was not normalized to UTC: %+v", current.LastFetchedAt)
		}
	}
}

func TestAdaptiveStateV2RejectsInvalidInvocationDigest(t *testing.T) {
	_, err := BuildAdaptiveStateV2(AdaptiveStateInputV2{
		TenantID: 7, UserID: 11, TaskID: "boss-watch",
		InvocationStates: []InvocationAdaptiveStateV1{{
			InvocationDigest: strings.Repeat("g", 64),
			Cursor:           json.RawMessage(`{}`), Status: InvocationStatusActive,
		}},
		RunStats: RunStatsV1{},
	})
	if !errors.Is(err, ErrInvalidState) {
		t.Fatalf("invalid digest error = %v", err)
	}
}
