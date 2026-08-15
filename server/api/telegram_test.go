package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/telegram"
	"github.com/YouToco/vane/server/types"
)

type fakeTelegramManager struct {
	status     telegram.Status
	tenantID   int64
	userID     int64
	issued     bool
	unlinked   bool
	tested     bool
	identity   store.ChannelIdentity
	bindingErr error
	blocked    store.ChannelDeliveryBlockStats
	blockedErr error
	issueErr   error
	unlinkErr  error
	testErr    error
}

func (f *fakeTelegramManager) Status() telegram.Status { return f.status }
func (f *fakeTelegramManager) IssueLink(_ context.Context, tenantID, userID int64) (telegram.Link, error) {
	f.tenantID, f.userID, f.issued = tenantID, userID, true
	if f.issueErr != nil {
		return telegram.Link{}, f.issueErr
	}
	return telegram.Link{
		DeepLink:  "https://t.me/vane_bot?start=opaque",
		ExpiresAt: time.Now().Add(time.Minute),
	}, nil
}
func (f *fakeTelegramManager) Binding(context.Context, int64, int64) (store.ChannelIdentity, error) {
	return f.identity, f.bindingErr
}
func (f *fakeTelegramManager) BlockedReplies(context.Context, int64, int64) (store.ChannelDeliveryBlockStats, error) {
	return f.blocked, f.blockedErr
}
func (f *fakeTelegramManager) Unlink(_ context.Context, tenantID, userID int64) error {
	f.tenantID, f.userID, f.unlinked = tenantID, userID, true
	return f.unlinkErr
}
func (f *fakeTelegramManager) SendTest(_ context.Context, tenantID, userID int64) error {
	f.tenantID, f.userID, f.tested = tenantID, userID, true
	return f.testErr
}

func telegramPrincipalRequest(method, path string) *http.Request {
	req := httptest.NewRequest(method, path, nil)
	ctx := auth.WithPrincipal(req.Context(), auth.Principal{
		TenantID: types.TenantID(7), UserID: 9,
	})
	return req.WithContext(ctx)
}

func TestTelegramLinkUsesOnlySessionPrincipal(t *testing.T) {
	fake := &fakeTelegramManager{status: telegram.Status{Enabled: true, Ready: true}}
	s := &server{deps: Deps{Telegram: fake}}
	rr := httptest.NewRecorder()
	s.handleTelegramLink(rr, telegramPrincipalRequest(
		http.MethodPost, "/api/telegram/link?tenant_id=999&user_id=999"))
	if rr.Code != http.StatusOK || !fake.issued ||
		fake.tenantID != 7 || fake.userID != 9 {
		t.Fatalf("status=%d issued=%t scope=(%d,%d)",
			rr.Code, fake.issued, fake.tenantID, fake.userID)
	}
}

func TestTelegramStatusDoesNotExposeExternalActorOrChat(t *testing.T) {
	fake := &fakeTelegramManager{
		status: telegram.Status{Enabled: true, Ready: true, BotID: 123,
			BotUsername: "vane_bot"},
		identity: store.ChannelIdentity{
			TenantID: 7, UserID: 9, ExternalUserID: "sensitive-actor",
			ProviderChatID: "sensitive-chat", BoundAt: time.Now(),
		},
	}
	s := &server{deps: Deps{Telegram: fake}}
	rr := httptest.NewRecorder()
	s.handleTelegramStatus(rr, telegramPrincipalRequest(
		http.MethodGet, "/api/telegram/status"))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !containsAll(body, `"bound":true`, `"bot_username":"vane_bot"`) ||
		containsAll(body, "sensitive-actor") || containsAll(body, "sensitive-chat") {
		t.Fatalf("unsafe status body=%s", body)
	}
}

func TestTelegramHandlersCoverDisabledUnboundAndFailureStates(t *testing.T) {
	appErr := types.NewAppError(types.CodeDatabase, "telegram unavailable", context.DeadlineExceeded)

	t.Run("disabled status", func(t *testing.T) {
		s := &server{}
		rr := httptest.NewRecorder()
		s.handleTelegramStatus(rr, telegramPrincipalRequest(http.MethodGet, "/api/telegram/status"))
		if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"enabled":false`) {
			t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
		}
	})

	for _, tc := range []struct {
		name string
		fake *fakeTelegramManager
		want int
	}{
		{name: "unbound", fake: &fakeTelegramManager{
			status:     telegram.Status{Enabled: true, Ready: true},
			bindingErr: types.NewAppError(types.CodeNotFound, "not bound", types.ErrNotFound),
		}, want: http.StatusOK},
		{name: "blocked observation failure", fake: &fakeTelegramManager{
			status: telegram.Status{Enabled: true, Ready: true}, blockedErr: appErr,
		}, want: http.StatusInternalServerError},
		{name: "binding failure", fake: &fakeTelegramManager{
			status: telegram.Status{Enabled: true, Ready: true}, bindingErr: appErr,
		}, want: http.StatusInternalServerError},
	} {
		t.Run("status "+tc.name, func(t *testing.T) {
			s := &server{deps: Deps{Telegram: tc.fake}}
			rr := httptest.NewRecorder()
			s.handleTelegramStatus(rr, telegramPrincipalRequest(http.MethodGet, "/api/telegram/status"))
			if rr.Code != tc.want {
				t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
			}
		})
	}

	t.Run("missing principal", func(t *testing.T) {
		s := &server{deps: Deps{Telegram: &fakeTelegramManager{status: telegram.Status{Ready: true}}}}
		for _, call := range []func(http.ResponseWriter, *http.Request){
			s.handleTelegramStatus, s.handleTelegramLink, s.handleTelegramUnlink, s.handleTelegramTest,
		} {
			rr := httptest.NewRecorder()
			call(rr, httptest.NewRequest(http.MethodPost, "/api/telegram", nil))
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
			}
		}
	})

	t.Run("disabled mutations", func(t *testing.T) {
		s := &server{}
		for _, call := range []func(http.ResponseWriter, *http.Request){
			s.handleTelegramLink, s.handleTelegramUnlink, s.handleTelegramTest,
		} {
			rr := httptest.NewRecorder()
			call(rr, telegramPrincipalRequest(http.MethodPost, "/api/telegram"))
			if rr.Code != http.StatusConflict {
				t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
			}
		}
	})

	t.Run("cross origin mutations", func(t *testing.T) {
		s := &server{deps: Deps{Origin: "https://vane.test", Telegram: &fakeTelegramManager{
			status: telegram.Status{Ready: true},
		}}}
		for _, call := range []func(http.ResponseWriter, *http.Request){
			s.handleTelegramLink, s.handleTelegramUnlink, s.handleTelegramTest,
		} {
			rr := httptest.NewRecorder()
			req := telegramPrincipalRequest(http.MethodPost, "/api/telegram")
			req.Header.Set("Origin", "https://attacker.test")
			call(rr, req)
			if rr.Code != http.StatusForbidden {
				t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
			}
		}
	})

	for _, tc := range []struct {
		name string
		call func(*server, http.ResponseWriter, *http.Request)
		fake *fakeTelegramManager
		flag func(*fakeTelegramManager) bool
	}{
		{name: "link failure", call: (*server).handleTelegramLink,
			fake: &fakeTelegramManager{status: telegram.Status{Ready: true}, issueErr: appErr},
			flag: func(f *fakeTelegramManager) bool { return f.issued }},
		{name: "unlink failure", call: (*server).handleTelegramUnlink,
			fake: &fakeTelegramManager{status: telegram.Status{Ready: true}, unlinkErr: appErr},
			flag: func(f *fakeTelegramManager) bool { return f.unlinked }},
		{name: "test failure", call: (*server).handleTelegramTest,
			fake: &fakeTelegramManager{status: telegram.Status{Ready: true}, testErr: appErr},
			flag: func(f *fakeTelegramManager) bool { return f.tested }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &server{deps: Deps{Telegram: tc.fake}}
			rr := httptest.NewRecorder()
			tc.call(s, rr, telegramPrincipalRequest(http.MethodPost, "/api/telegram"))
			if rr.Code != http.StatusInternalServerError || !tc.flag(tc.fake) {
				t.Fatalf("status=%d called=%t body=%s", rr.Code, tc.flag(tc.fake), rr.Body.String())
			}
		})
	}

	t.Run("successful unlink and test", func(t *testing.T) {
		fake := &fakeTelegramManager{status: telegram.Status{Ready: true}}
		s := &server{deps: Deps{Telegram: fake}}
		for _, call := range []func(http.ResponseWriter, *http.Request){
			s.handleTelegramUnlink, s.handleTelegramTest,
		} {
			rr := httptest.NewRecorder()
			call(rr, telegramPrincipalRequest(http.MethodPost, "/api/telegram"))
			if rr.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
			}
		}
		if !fake.unlinked || !fake.tested || fake.tenantID != 7 || fake.userID != 9 {
			t.Fatalf("fake=%+v", fake)
		}
	})
}

func containsAll(value string, wants ...string) bool {
	for _, want := range wants {
		if !strings.Contains(value, want) {
			return false
		}
	}
	return true
}
