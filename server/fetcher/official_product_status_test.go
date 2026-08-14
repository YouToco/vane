package fetcher

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/YouToco/vane/server/config"
	"github.com/YouToco/vane/server/types"
)

func TestProductStatusFetcher_KimiReservationSnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Connect-Protocol-Version") != "1" ||
			r.Header.Get("x-msh-platform") != "web" {
			t.Fatalf("unexpected request: method=%s headers=%v", r.Method, r.Header)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"goods":[
			{"id":"free","title":"Adagio","membershipLevel":"LEVEL_FREE","amounts":[{"currency":"USD","priceInCents":"0"}]},
			{"id":"paid","title":"Moderato","membershipLevel":"LEVEL_BASIC","amounts":[{"currency":"USD","priceInCents":"1900"}],"billingCycle":{"duration":1,"timeUnit":"TIME_UNIT_MONTH"},"transitionSummary":{"reason":"REASON_SUBSCRIPTION_NEED_APPLY"}}
		]}`))
	}))
	defer server.Close()

	fetcher := NewProductStatus(config.FetchConfig{}, nil)
	fetcher.kimiURL = server.URL
	items, err := fetcher.Fetch(t.Context(), types.FetchTarget{
		Platform: types.PlatformWeb, Capability: types.CapProductStatus,
		Config: json.RawMessage(`{"url":"https://www.kimi.com/membership/pricing"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("items=%d, want 1", len(items))
	}
	item := items[0]
	if item.Kind != types.KindPageContent ||
		!strings.Contains(item.Content, "仅可预约（尚不可直接购买）") ||
		!strings.Contains(item.Content, "REASON_SUBSCRIPTION_NEED_APPLY") ||
		strings.Contains(item.Content, "Adagio") {
		t.Fatalf("unexpected snapshot: %+v", item)
	}
	if !strings.HasPrefix(item.CanonicalKey, "product-status://https://www.kimi.com/membership/pricing#") {
		t.Fatalf("canonical key=%q", item.CanonicalKey)
	}
}

func TestNormalizeKimiProductStatus_PurchasableWhenPaidPlanHasNoGate(t *testing.T) {
	response := kimiGoodsResponse{}
	var good kimiGood
	good.Title = "Moderato"
	good.MembershipLevel = "LEVEL_BASIC"
	good.Amounts = append(good.Amounts, struct {
		Currency     string `json:"currency"`
		PriceInCents string `json:"priceInCents"`
	}{Currency: "USD", PriceInCents: "1900"})
	response.Goods = append(response.Goods, good)
	content, status, err := normalizeKimiProductStatus(response)
	if err != nil {
		t.Fatal(err)
	}
	if status != "可直接购买" || !strings.Contains(content, "reason=NONE") {
		t.Fatalf("status=%q content=%q", status, content)
	}
}

func TestSupportedKimiPricingURLRejectsLookalike(t *testing.T) {
	for _, raw := range []string{
		"http://www.kimi.com/membership/pricing",
		"https://www.kimi.com.evil.example/membership/pricing",
		"https://www.kimi.com/other",
	} {
		if _, err := supportedKimiPricingURL(raw); err == nil {
			t.Fatalf("accepted unsupported URL %q", raw)
		}
	}
}
