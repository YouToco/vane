package acquisitiontool

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestMaterializeApprovedToolCallV1MatchesApprovedV1Writes(t *testing.T) {
	tests := []struct {
		name string
		args string
	}{
		{"web_search", `{"query":"AI","category":"news","include_domains":["openai.com"]}`},
		{"web_feed", `{"feed_url":"https://openai.com/rss.xml","categories":["AI"]}`},
		{"web_contents", `{"page_url":"https://openai.com/pricing"}`},
		{"web_product_status", `{"page_url":"https://www.kimi.com/membership/pricing"}`},
		{"x_user_posts", `{"screen_name":"@OpenAI"}`},
		{"xhs_search", `{"keyword":"AI 创业"}`},
		{"xhs_user_posts", `{"user_id":"6a5578b3000000000e03cc00"}`},
		{"xhs_hot_list", `{}`},
		{"xhs_topic_feed", `{"page_id":"6301c499df9bea0001dc6f47"}`},
		{"xhs_faved_notes", `{"user_id":"6a5578b3000000000e03cc00"}`},
		{"weibo_user_posts", `{"uid":"2803301701"}`},
		{"weibo_hot_list", `{}`},
		{"wechat_mp_user_posts", `{"username":"gh_openai"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw := json.RawMessage(test.args)
			canonical, err := CanonicalizeToolArgumentsV1(test.name, raw)
			if err != nil {
				t.Fatalf("canonicalize current write: %v", err)
			}
			frozen, err := MaterializeApprovedToolCallV1(
				test.name, "v1", canonical)
			if err != nil {
				t.Fatalf("materialize frozen call: %v", err)
			}
			requirement, err := decodeApprovedToolArgumentsV1(test.name, canonical)
			if err != nil {
				t.Fatal(err)
			}
			current, message := BuildTarget(requirement)
			if message != "" || current == nil {
				t.Fatalf("materialize current write: %s", message)
			}
			current.ID = 0
			current.FetchIntervalSeconds = 0
			current.NextFetchAt = current.NextFetchAt.UTC()
			current.LastFetchedAt = nil
			current.FailCount = 0
			left, _ := json.Marshal(frozen)
			right, _ := json.Marshal(current)
			if !bytes.Equal(left, right) {
				t.Fatalf("frozen V1 drifted:\n%s\n%s", left, right)
			}
		})
	}
}

func TestMaterializeApprovedToolCallV1RejectsVersionAndArgumentDrift(
	t *testing.T,
) {
	if _, err := MaterializeApprovedToolCallV1(
		"web_search", "v2", json.RawMessage(`{"query":"AI"}`)); err == nil {
		t.Fatal("accepted an unsupported frozen Tool contract version")
	}
	if _, err := MaterializeApprovedToolCallV1(
		"web_search", "v1",
		json.RawMessage(`{"query":"AI","api_key":"secret"}`)); err == nil {
		t.Fatal("accepted a secret/unknown frozen Tool argument")
	}
}
