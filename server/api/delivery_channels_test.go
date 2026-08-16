package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

func deliveryPreferenceRequest(method, body string, tenantID, userID int64) *http.Request {
	req := httptest.NewRequest(method, "/api/channels/delivery-preference", strings.NewReader(body))
	ctx := auth.WithPrincipal(req.Context(), auth.Principal{
		TenantID: types.TenantID(tenantID), UserID: userID,
	})
	return req.WithContext(ctx)
}

func TestDeliveryChannelPreferenceHandlersPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		requireDatabaseCapability(t)
	}
	st := inviteAPIStore(t)
	ctx := t.Context()
	user, err := st.UpsertUserByOpenID(ctx,
		fmt.Sprintf("delivery-preference-api-%d", time.Now().UnixNano()), "member")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.AddMembership(ctx, 1, user.ID, types.MembershipRoleMember); err != nil {
		t.Fatal(err)
	}
	cleanup, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = cleanup.Exec(t.Context(), `DELETE FROM memberships WHERE tenant_id=1 AND user_id=$1`, user.ID)
		_, _ = cleanup.Exec(t.Context(), `DELETE FROM users WHERE id=$1`, user.ID)
		cleanup.Close()
	})

	s := &server{deps: Deps{Store: st}}
	get := httptest.NewRecorder()
	s.handleGetDeliveryChannelPreference(get,
		deliveryPreferenceRequest(http.MethodGet, "", 1, user.ID))
	if get.Code != http.StatusOK {
		t.Fatalf("default GET status=%d body=%s", get.Code, get.Body.String())
	}
	var defaultPreference store.DeliveryChannelPreference
	if err := json.Unmarshal(get.Body.Bytes(), &defaultPreference); err != nil {
		t.Fatal(err)
	}
	if defaultPreference.Selection != store.DeliveryChannelFeishu || defaultPreference.Explicit {
		t.Fatalf("default preference=%+v", defaultPreference)
	}

	patch := httptest.NewRecorder()
	s.handlePatchDeliveryChannelPreference(patch,
		deliveryPreferenceRequest(http.MethodPatch, `{"selection":"both"}`, 1, user.ID))
	if patch.Code != http.StatusOK {
		t.Fatalf("PATCH status=%d body=%s", patch.Code, patch.Body.String())
	}
	var saved store.DeliveryChannelPreference
	if err := json.Unmarshal(patch.Body.Bytes(), &saved); err != nil {
		t.Fatal(err)
	}
	if saved.Selection != store.DeliveryChannelBoth || !saved.Explicit {
		t.Fatalf("saved preference=%+v", saved)
	}

	get = httptest.NewRecorder()
	s.handleGetDeliveryChannelPreference(get,
		deliveryPreferenceRequest(http.MethodGet, "", 1, user.ID))
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"selection":"both"`) {
		t.Fatalf("stored GET status=%d body=%s", get.Code, get.Body.String())
	}
}

func TestDeliveryChannelPreferencePatchRejectsUnsafeInput(t *testing.T) {
	s := &server{deps: Deps{Origin: "https://vane.example"}}
	tests := []struct {
		name string
		body string
	}{
		{name: "malformed", body: `{"selection":`},
		{name: "unknown field", body: `{"selection":"feishu","tenant_id":999}`},
		{name: "trailing object", body: `{"selection":"feishu"}{}`},
		{name: "invalid selection", body: `{"selection":"email"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			s.handlePatchDeliveryChannelPreference(rr,
				deliveryPreferenceRequest(http.MethodPatch, test.body, 1, 1))
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
			}
		})
	}

	missingPrincipal := httptest.NewRecorder()
	s.handleGetDeliveryChannelPreference(missingPrincipal,
		httptest.NewRequest(http.MethodGet, "/api/channels/delivery-preference", nil))
	if missingPrincipal.Code != http.StatusBadRequest {
		t.Fatalf("missing principal status=%d body=%s",
			missingPrincipal.Code, missingPrincipal.Body.String())
	}

	missingPatchPrincipal := httptest.NewRecorder()
	s.handlePatchDeliveryChannelPreference(missingPatchPrincipal,
		httptest.NewRequest(http.MethodPatch, "/api/channels/delivery-preference",
			strings.NewReader(`{"selection":"feishu"}`)))
	if missingPatchPrincipal.Code != http.StatusBadRequest {
		t.Fatalf("missing PATCH principal status=%d body=%s",
			missingPatchPrincipal.Code, missingPatchPrincipal.Body.String())
	}

	badOriginRequest := deliveryPreferenceRequest(
		http.MethodPatch, `{"selection":"feishu"}`, 1, 1)
	badOriginRequest.Header.Set("Origin", "https://attacker.example")
	badOrigin := httptest.NewRecorder()
	s.handlePatchDeliveryChannelPreference(badOrigin, badOriginRequest)
	if badOrigin.Code != http.StatusForbidden {
		t.Fatalf("bad origin status=%d body=%s", badOrigin.Code, badOrigin.Body.String())
	}

	invalidScopeGet := httptest.NewRecorder()
	s.handleGetDeliveryChannelPreference(invalidScopeGet,
		deliveryPreferenceRequest(http.MethodGet, "", 0, 1))
	if invalidScopeGet.Code != http.StatusBadRequest {
		t.Fatalf("invalid-scope GET status=%d body=%s",
			invalidScopeGet.Code, invalidScopeGet.Body.String())
	}
	invalidScopePatch := httptest.NewRecorder()
	s.handlePatchDeliveryChannelPreference(invalidScopePatch,
		deliveryPreferenceRequest(http.MethodPatch, `{"selection":"both"}`, 0, 1))
	if invalidScopePatch.Code != http.StatusBadRequest {
		t.Fatalf("invalid-scope PATCH status=%d body=%s",
			invalidScopePatch.Code, invalidScopePatch.Body.String())
	}
}
