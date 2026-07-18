package fetcher

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/types"
)

// TestXHSUser_Live 是打真实 TikHub API 的端到端验证，默认跳过（不进 CI，不烧配额）。
// 运行：VANE_FETCH_TIKHUB_API_KEY=<key> VANE_LIVE_XHS_USER_ID=<24hex> go test -run TestXHSUser_Live -v ./fetcher/
func TestXHSUser_Live(t *testing.T) {
	key := os.Getenv("VANE_FETCH_TIKHUB_API_KEY")
	userID := os.Getenv("VANE_LIVE_XHS_USER_ID")
	if key == "" || userID == "" {
		t.Skip("需 VANE_FETCH_TIKHUB_API_KEY + VANE_LIVE_XHS_USER_ID 才跑真实 API")
	}

	f := NewXHSUser(config.FetchConfig{TimeoutSeconds: 25, MaxResponseMB: 5, TikhubAPIKey: key})
	cfg, _ := json.Marshal(map[string]string{"user_id": userID})
	src := types.Source{ID: 1, Platform: types.PlatformXHS, Capability: types.CapUserPosts, Config: cfg}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	items, err := f.Fetch(ctx, src)
	if err != nil {
		t.Fatalf("真实抓取失败: %v", err)
	}
	if len(items) == 0 {
		t.Fatal("真实抓取返回 0 条（该用户应有已发布笔记）")
	}
	for i, it := range items {
		// 硬约束：身份、种类、URL、发布时间必须成立，否则下游管线会静默出错。
		if it.CanonicalKey == "" || it.CanonicalKey != it.ExternalID {
			t.Errorf("[%d] canonical_key 应为裸 note_id：key=%q ext=%q", i, it.CanonicalKey, it.ExternalID)
		}
		if it.Kind != types.KindArticle {
			t.Errorf("[%d] Kind 应为 article，实际 %q", i, it.Kind)
		}
		if it.Simhash == nil {
			t.Errorf("[%d] simhash 未算", i)
		}
		if it.PublishedAt == nil {
			t.Errorf("[%d] 发布时间缺失（create_time 未解析？）", i)
		}
		t.Logf("[%d] note=%s pub=%v title=%q author=%q desc_runes=%d url=%s",
			i, it.ExternalID, it.PublishedAt, it.Title, it.Author, utf8.RuneCountInString(it.Content), it.URL)
	}
}
