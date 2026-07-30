package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/auth"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

func TestProviderPriceEndpointsOwner(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("未设置 DATABASE_URL，跳过供应商价格 API 集成测试")
	}
	st := inviteAPIStore(t)
	ctx := t.Context()
	tag := uuid.NewString()
	user, err := st.UpsertUserByOpenID(ctx, "pricing-api-"+tag, "pricing api owner")
	if err != nil {
		t.Fatalf("准备平台 owner 用户: %v", err)
	}
	provider := "api-pricing-" + tag

	mux := http.NewServeMux()
	fake := newFakeAuthStore()
	token, hash, err := auth.NewSessionToken()
	if err != nil {
		t.Fatal(err)
	}
	fake.sessions[string(hash)] = &types.Session{
		TokenHash: hash, UserID: user.ID, TenantID: 1,
		ExpiresAt: time.Now().Add(time.Hour),
	}
	fake.members[user.ID] = []types.Membership{{TenantID: 1, UserID: user.ID}}
	deps := Deps{Store: st, Auth: fake, Principal: auth.NewContextResolver()}
	cookie := &http.Cookie{Name: sessionCookieName, Value: token}
	Mount(mux, deps)

	get := httptest.NewRequest(http.MethodGet, "/api/admin/provider-prices", nil)
	get.AddCookie(cookie)
	getRec := httptest.NewRecorder()
	mux.ServeHTTP(getRec, get)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GET provider prices=%d body=%s", getRec.Code, getRec.Body.String())
	}
	var list struct {
		Rules []store.ProviderPriceRule `json:"rules"`
	}
	if err := json.Unmarshal(getRec.Body.Bytes(), &list); err != nil || list.Rules == nil {
		t.Fatalf("价格列表 JSON=%s err=%v", getRec.Body.String(), err)
	}

	body := map[string]any{
		"provider": provider, "resource": "/request", "meter": "request",
		"currency": "USD", "request_unit_price": 0.0123,
		"request_included_quantity": 1, "request_additional_unit_price": 0.0123,
		"source_url": "https://example.com/official-pricing", "note": "API test",
	}
	raw, _ := json.Marshal(body)
	changeID := "provider-price-" + tag
	post := func(payload []byte, key string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/admin/provider-prices", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		req.AddCookie(cookie)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}
	first := post(raw, changeID)
	if first.Code != http.StatusCreated {
		t.Fatalf("POST provider price=%d body=%s", first.Code, first.Body.String())
	}
	var firstRule store.ProviderPriceRule
	if err := json.Unmarshal(first.Body.Bytes(), &firstRule); err != nil ||
		firstRule.Provider != provider || firstRule.RequestUnitPrice == nil {
		t.Fatalf("价格写入响应=%s err=%v", first.Body.String(), err)
	}
	replay := post(raw, changeID)
	if replay.Code != http.StatusCreated {
		t.Fatalf("幂等重放=%d body=%s", replay.Code, replay.Body.String())
	}
	var replayRule store.ProviderPriceRule
	if err := json.Unmarshal(replay.Body.Bytes(), &replayRule); err != nil ||
		replayRule.ID != firstRule.ID {
		t.Fatalf("幂等重放没有复用 exact rule: %s err=%v", replay.Body.String(), err)
	}
	if rec := post(raw, ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("缺少幂等键=%d body=%s", rec.Code, rec.Body.String())
	}
	invalid := append(raw[:len(raw)-1], []byte(`,"unknown":true}`)...)
	if rec := post(invalid, "provider-price-invalid-"+tag); rec.Code != http.StatusBadRequest {
		t.Fatalf("未知字段应拒绝=%d body=%s", rec.Code, rec.Body.String())
	}
}
