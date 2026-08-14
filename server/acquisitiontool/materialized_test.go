package acquisitiontool

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/YouToco/vane/types"
)

func TestValidateMaterializedRoundTripsEveryAvailableCapability(t *testing.T) {
	t.Parallel()
	tests := []Requirement{
		{Platform: "web", Capability: "feed", Params: map[string]string{
			"url": "https://example.com/feed.xml", "categories": `["AI","news"]`,
		}},
		{Platform: "web", Capability: "search", Params: map[string]string{
			"query": "AI", "category": "news", "include_domains": `["openai.com","anthropic.com"]`,
		}},
		{Platform: "web", Capability: "contents", Params: map[string]string{
			"url": "https://example.com/pricing", "title": "Pricing",
		}},
		{Platform: "x", Capability: "user_posts", Params: map[string]string{"screen_name": "OpenAI"}},
		{Platform: "xhs", Capability: "search", Params: map[string]string{"keyword": "人工智能"}},
		{Platform: "xhs", Capability: "user_posts", Params: map[string]string{"user_id": "6a5578b3000000000e03cc00"}},
		{Platform: "xhs", Capability: "hot_list", Params: map[string]string{}},
		{Platform: "xhs", Capability: "topic_feed", Params: map[string]string{"page_id": "6a5578b3000000000e03cc00"}},
		{Platform: "xhs", Capability: "faved_notes", Params: map[string]string{"user_id": "6a5578b3000000000e03cc00"}},
		{Platform: "weibo", Capability: "user_posts", Params: map[string]string{"uid": "2803301701"}},
		{Platform: "weibo", Capability: "hot_list", Params: map[string]string{}},
		{Platform: "wechat_mp", Capability: "user_posts", Params: map[string]string{"username": "gh_363b924965e9"}},
	}
	for _, spec := range tests {
		spec := spec
		t.Run(spec.Platform+"/"+spec.Capability, func(t *testing.T) {
			source, message := BuildTarget(spec)
			if message != "" || source == nil {
				t.Fatalf("BuildTarget: source=%+v message=%q", source, message)
			}
			if message := ValidateMaterialized(source); message != "" {
				t.Fatalf("ValidateMaterialized: %s; source=%+v config=%s",
					message, source, source.Config)
			}
		})
	}
}

func TestValidateMaterializedRejectsIdentityConfigDrift(t *testing.T) {
	t.Parallel()
	source, message := BuildTarget(Requirement{
		Platform: "web", Capability: "search",
		Params: map[string]string{"query": "approved", "include_domains": `["example.com"]`},
	})
	if message != "" {
		t.Fatal(message)
	}

	tests := []struct {
		name   string
		mutate func(*types.FetchTarget)
	}{
		{
			name: "runtime query differs",
			mutate: func(source *types.FetchTarget) {
				source.Config = json.RawMessage(`{"query":"different","include_domains":["example.com"]}`)
			},
		},
		{
			name: "required config missing",
			mutate: func(source *types.FetchTarget) {
				source.Config = json.RawMessage(`{}`)
			},
		},
		{
			name: "URL query reordered",
			mutate: func(source *types.FetchTarget) {
				source.URL = "vane://web/search?include_domains=example.com&q=approved"
			},
		},
		{
			name: "unknown runtime config",
			mutate: func(source *types.FetchTarget) {
				source.Config = append(bytes.TrimSuffix(source.Config, []byte("}")), []byte(`,"write":true}`)...)
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			clone := *source
			clone.Config = bytes.Clone(source.Config)
			testCase.mutate(&clone)
			if message := ValidateMaterialized(&clone); message == "" {
				t.Fatalf("drift unexpectedly accepted: %+v config=%s", clone, clone.Config)
			}
		})
	}
}

func TestValidateMaterializedRejectsReservedFeedMarkerWithoutConfig(t *testing.T) {
	t.Parallel()
	categorized, message := BuildTarget(Requirement{
		Platform: "web", Capability: "feed", Title: "AI feed",
		Params: map[string]string{
			"url":        "https://example.com/feed",
			"categories": `["AI"]`,
		},
	})
	if message != "" || categorized == nil {
		t.Fatalf("fixture BuildTarget failed: %q", message)
	}
	spoofed := *categorized
	spoofed.Config = json.RawMessage(`{}`)
	if message := ValidateMaterialized(&spoofed); message == "" {
		t.Fatalf("reserved category URL accepted with empty config: %+v", spoofed)
	}
	if source, message := BuildTarget(Requirement{
		Platform: "web", Capability: "feed", Title: "spoofed feed",
		Params: map[string]string{"url": categorized.URL},
	}); message == "" || source != nil {
		t.Fatalf("BuildTarget accepted reserved category URL without config: source=%+v message=%q",
			source, message)
	}

	queryMarker, message := BuildTarget(Requirement{
		Platform: "web", Capability: "feed", Title: "remote query",
		Params: map[string]string{"url": "https://example.com/feed?vane-categories=AI"},
	})
	if message != "" || queryMarker == nil {
		t.Fatalf("query marker fixture BuildTarget failed: %q", message)
	}
	if message := ValidateMaterialized(queryMarker); message != "" {
		t.Fatalf("remote query parameter was mistaken for reserved fragment: %s", message)
	}
}
