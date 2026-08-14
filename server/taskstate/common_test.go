package taskstate

import (
	"testing"

	"github.com/YouToco/vane/types"
)

// This table is intentionally independent of application enum validators.
// Removing a retained V1 literal must fail here even if current capabilities
// or defaults changed at the same time.
func TestFrozenV1LiteralAllowlists(t *testing.T) {
	t.Parallel()
	capabilities := [][2]string{
		{"web", "feed"},
		{"web", "search"},
		{"web", "contents"},
		{"x", "user_posts"},
		{"xhs", "search"},
		{"xhs", "user_posts"},
		{"xhs", "hot_list"},
		{"xhs", "topic_feed"},
		{"xhs", "faved_notes"},
		{"weibo", "user_posts"},
		{"weibo", "hot_list"},
		{"wechat_mp", "user_posts"},
	}
	for _, pair := range capabilities {
		if !validReadCapability(types.Platform(pair[0]), types.Capability(pair[1])) {
			t.Errorf("retained V1 capability %s/%s is no longer readable", pair[0], pair[1])
		}
	}
	if validReadCapability(types.Platform("web"), types.Capability("write")) {
		t.Fatal("V1 capability allowlist accepted a write capability")
	}

	for _, strictness := range []string{"loose", "normal", "strict"} {
		if !validStrictnessV1(types.PushStrictness(strictness)) {
			t.Errorf("retained V1 strictness %q is no longer readable", strictness)
		}
	}
	if validStrictnessV1(types.PushStrictness("extreme")) {
		t.Fatal("V1 strictness allowlist accepted an unknown value")
	}

	for _, mode := range []string{"compiled", "discover_at_run"} {
		if !validExecutionModeV1(types.ExecutionMode(mode)) {
			t.Errorf("retained V1 mode %q is no longer readable", mode)
		}
	}
	if validExecutionModeV1(types.ExecutionMode("unknown")) {
		t.Fatal("V1 mode allowlist accepted unknown")
	}
}
