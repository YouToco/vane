package fetcher

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/sourcecatalog"
	"github.com/YouToco/vane/types"
)

func TestMulti_DispatchesByType(t *testing.T) {
	// RSS 假服务端。
	rssSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/rss+xml")
		_, _ = w.Write([]byte(sampleRSS))
	}))
	defer rssSrv.Close()
	// Exa 假服务端。
	exaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(sampleExaResponse))
	}))
	defer exaSrv.Close()
	// TikHub 小红书搜索假服务端。
	xhsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(sampleTikhubResponse))
	}))
	defer xhsSrv.Close()
	// TikHub 小红书用户笔记假服务端。
	xhsUserSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(sampleXHSUserResponse))
	}))
	defer xhsUserSrv.Close()

	// seen 传 nil：本用例只验证按类型分发，不涉及详情补全（nil 会跳过补全）。
	m := NewMulti(config.FetchConfig{
		TimeoutSeconds: 10, MaxResponseMB: 1,
		ExaAPIKey: "k", TikhubAPIKey: "k",
	}, nil)
	// 子抓取器分别指向对应假服务端；RSS 放行环回（httptest 监听 127.0.0.1）。
	m.rss = newTestFetcher()
	m.exa.searchURL = exaSrv.URL
	m.tikhub.baseURL = xhsSrv.URL
	m.xhsUser.baseURL = xhsUserSrv.URL

	cases := []struct {
		name string
		src  types.Source
		want int // 期望条数
	}{
		{"rss", types.Source{ID: 1, Platform: types.PlatformWeb, Capability: types.CapFeed, URL: rssSrv.URL}, 2},
		{"exa", types.Source{ID: 2, Platform: types.PlatformWeb, Capability: types.CapSearch, Config: json.RawMessage(`{"query":"x"}`)}, 2},
		{"tikhub", types.Source{ID: 3, Platform: types.PlatformXHS, Capability: types.CapSearch, Config: json.RawMessage(`{"keyword":"x"}`)}, 1},
		{"xhs_user", types.Source{ID: 4, Platform: types.PlatformXHS, Capability: types.CapUserPosts, Config: json.RawMessage(`{"user_id":"6a5578b3000000000e03cc00"}`)}, 2},
	}
	for _, tc := range cases {
		items, err := m.Fetch(context.Background(), tc.src)
		if err != nil {
			t.Errorf("%s: Fetch 意外失败: %v", tc.name, err)
			continue
		}
		if len(items) != tc.want {
			t.Errorf("%s: 期望 %d 条，实际 %d", tc.name, tc.want, len(items))
		}
	}
}

func TestMulti_UnknownPlatform(t *testing.T) {
	m := NewMulti(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1}, nil)
	_, err := m.Fetch(context.Background(), types.Source{ID: 1, Platform: "carrier_pigeon"})
	if !errors.Is(err, types.ErrValidation) {
		t.Errorf("未知平台应判 ErrValidation，实际 %v", err)
	}
	if types.IsRetryable(err) {
		t.Error("未知平台不应可重试")
	}
}

// TestMulti_UnavailableCapabilityCarriesReason 验证：注册为 Unavailable 的能力（x/search）
// 被抓取时返回 CodeValidation，且**报错里带得出 sourcecatalog 的 Reason**——这正是契约 §2.2
// 要的"机器可读的不可用原因"，让 agent 能解释为何不支持，而不是静默改道。
func TestMulti_UnavailableCapabilityCarriesReason(t *testing.T) {
	m := NewMulti(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1}, nil)
	_, err := m.Fetch(context.Background(), types.Source{
		ID: 9, Platform: types.PlatformX, Capability: types.CapSearch,
	})
	if !errors.Is(err, types.ErrValidation) {
		t.Fatalf("Unavailable 能力应判 ErrValidation，实际 %v", err)
	}
	if types.IsRetryable(err) {
		t.Error("Unavailable 能力不应可重试")
	}
	entry, _ := sourcecatalog.Lookup(types.PlatformX, types.CapSearch)
	if entry.Reason == "" || !strings.Contains(err.Error(), entry.Reason) {
		t.Errorf("报错未携带 sourcecatalog Reason；err=%q，reason=%q", err.Error(), entry.Reason)
	}
}

// TestMulti_EveryAvailableCapabilityIsWired 是**注册表↔装配的漂移守护**：
// 遍历 sourcecatalog 里每个 Available 条目，用空 config 调 Multi.Fetch，断言绝不返回
// CodeInternal（那是"注册为可用却没接抓取器"的漂移信号）。有了它，任何人将来在
// sourcecatalog 加一行 Available 却忘了在 multi.go 接 case，CI 立刻红。
//
// 不触网的保证分两层，别只靠"provider 恰好先校验"这条隐性约定（对抗审查 CONFIRMED：
// 那条约定不成文，将来某个无 key、空配置即打固定端点的 provider 会让本测试真发外网请求）：
//  1. 空 config：现有各 provider 在此以 CodeValidation 早退（缺 key/缺 user_id/空 URL）；
//  2. 兜底离线：把所有**有 baseURL/searchURL 字段的联网 provider**指到一个**已关闭**的
//     本地服务端——即便某 provider 跳过校验直接发请求，也是本地"连接被拒"秒失败，绝不打外网。
//
// rss 无远端 baseURL 字段（走 URL 参数 + 私网拦截），空 URL 在拨号前即校验失败。
// 将来新增一个自带远端 baseURL 的 provider，请在下方 offline 覆盖里加一行。
func TestMulti_EveryAvailableCapabilityIsWired(t *testing.T) {
	// 已关闭的本地服务端：拿到 URL 后立即 Close，后续连接立刻 connection refused（本地、无外网）。
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	dead.Close()

	m := NewMulti(config.FetchConfig{TimeoutSeconds: 5, MaxResponseMB: 1, ExaAPIKey: "k", TikhubAPIKey: "k"}, nil)
	// 兜底把联网 provider 全指向死服务端（防将来 provider 跳过校验直接发请求时打外网）。
	m.exa.searchURL = dead.URL
	m.tikhub.baseURL = dead.URL
	m.xhsUser.baseURL = dead.URL
	m.twitter.baseURL = dead.URL

	for _, e := range sourcecatalog.List() {
		if !e.Available() {
			continue
		}
		_, err := m.Fetch(context.Background(), types.Source{
			ID: 1, Platform: e.Platform, Capability: e.Capability,
		})
		if err == nil {
			continue // 空 config 仍成功也无妨（只是不该是 CodeInternal）。
		}
		if errors.Is(err, types.ErrInternal) {
			t.Errorf("能力 %s/%s 在 sourcecatalog 标记 Available，但 multi.go 未接抓取器（漂移）：%v",
				e.Platform, e.Capability, err)
		}
	}
}
