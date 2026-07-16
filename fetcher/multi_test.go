package fetcher

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/YouToco/vane/config"
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
	// TikHub 假服务端。
	xhsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(sampleTikhubResponse))
	}))
	defer xhsSrv.Close()

	// seen 传 nil：本用例只验证按类型分发，不涉及详情补全（nil 会跳过补全）。
	m := NewMulti(config.FetchConfig{
		TimeoutSeconds: 10, MaxResponseMB: 1,
		ExaAPIKey: "k", TikhubAPIKey: "k",
	}, nil)
	// 三个子抓取器分别指向对应假服务端；RSS 放行环回（httptest 监听 127.0.0.1）。
	m.rss = newTestFetcher()
	m.exa.searchURL = exaSrv.URL
	m.tikhub.baseURL = xhsSrv.URL

	cases := []struct {
		name string
		src  types.Source
		want int // 期望条数
	}{
		{"rss", types.Source{ID: 1, Platform: types.PlatformWeb, Capability: types.CapFeed, URL: rssSrv.URL}, 2},
		{"exa", types.Source{ID: 2, Platform: types.PlatformWeb, Capability: types.CapSearch, Config: json.RawMessage(`{"query":"x"}`)}, 2},
		{"tikhub", types.Source{ID: 3, Platform: types.PlatformXHS, Capability: types.CapSearch, Config: json.RawMessage(`{"keyword":"x"}`)}, 1},
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
