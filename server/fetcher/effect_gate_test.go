package fetcher

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/YouToco/vane/types"
)

func TestEffectGateDeniesEveryPrimaryUpstreamCall(t *testing.T) {
	errRevoked := errors.New("compiled task revoked")
	deny := func(context.Context) error { return errRevoked }

	tests := []struct {
		name string
		call func(context.Context, string) error
	}{
		{
			name: "rss",
			call: func(ctx context.Context, upstreamURL string) error {
				_, err := newTestFetcher().fetchRSSWithEffectGate(
					ctx,
					types.FetchTarget{ID: 1, Platform: types.PlatformWeb, Capability: types.CapFeed, URL: upstreamURL},
					enrichMaxPerRound,
					deny,
				)
				return err
			},
		},
		{
			name: "exa search",
			call: func(ctx context.Context, upstreamURL string) error {
				_, err := newTestExa(upstreamURL).fetchWithEffectGate(
					ctx, exaSrc(1, `{"query":"ai"}`), deny)
				return err
			},
		},
		{
			name: "exa contents",
			call: func(ctx context.Context, upstreamURL string) error {
				_, err := newTestExaContents(upstreamURL).fetchWithEffectGate(
					ctx, contentsSource(`{"url":"https://example.com/page"}`), deny)
				return err
			},
		},
		{
			name: "binding",
			call: func(ctx context.Context, upstreamURL string) error {
				_, err := newTestBinding(upstreamURL, nil, nil).fetchWithEffectGate(
					ctx, bindingSrc(types.CapHotList, `{}`), deny)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var hits atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				hits.Add(1)
			}))
			t.Cleanup(server.Close)

			err := tt.call(t.Context(), server.URL)
			if !errors.Is(err, errRevoked) {
				t.Fatalf("call error = %v, want revocation", err)
			}
			if got := hits.Load(); got != 0 {
				t.Fatalf("upstream calls after revocation = %d, want 0", got)
			}
		})
	}
}
