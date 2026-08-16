package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

type fakeFleetStore struct {
	*fakeIngressStore
	mu      sync.Mutex
	active  map[userScope]store.CredentialMetadata
	secrets map[string][]byte
}

func newFakeFleetStore() *fakeFleetStore {
	notFound := types.NewAppError(types.CodeNotFound, "empty", types.ErrNotFound)
	return &fakeFleetStore{
		fakeIngressStore: &fakeIngressStore{claimErr: notFound, readyErr: notFound},
		active:           make(map[userScope]store.CredentialMetadata),
		secrets:          make(map[string][]byte),
	}
}

func (f *fakeFleetStore) setCredential(tenantID, userID, generation, botID int64, token, secret string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	metadata, _ := json.Marshal(map[string]any{"bot_id": botID,
		"bot_username": fmt.Sprintf("bot_%d", botID)})
	scope := telegramScope(tenantID, userID)
	f.active[userScope{tenantID: tenantID, userID: userID}] = store.CredentialMetadata{CredentialScope: scope,
		Generation: generation, Metadata: metadata, Status: "active"}
	payload, _ := json.Marshal(storedTelegramSecret{
		BotToken: token, WebhookSecret: secret})
	f.secrets[fmt.Sprintf("%d/%d/%d", tenantID, userID, generation)] = payload
}

func (f *fakeFleetStore) ListActiveUserCredentialMetadata(
	_ context.Context, provider, purpose string,
) ([]store.CredentialMetadata, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]store.CredentialMetadata, 0, len(f.active))
	for _, item := range f.active {
		if item.Provider == provider && item.Purpose == purpose {
			result = append(result, item)
		}
	}
	return result, nil
}

func (f *fakeFleetStore) ActiveCredentialMetadata(
	_ context.Context, scope store.CredentialScope,
) (store.CredentialMetadata, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	item, ok := f.active[userScope{tenantID: scope.TenantID, userID: scope.UserID}]
	if !ok {
		return store.CredentialMetadata{}, types.NewAppError(
			types.CodeNotFound, "missing", types.ErrNotFound)
	}
	return item, nil
}

func (f *fakeFleetStore) UseCredential(
	_ context.Context, scope store.CredentialScope, generation int64,
	use func([]byte, store.CredentialMetadata) error,
) error {
	f.mu.Lock()
	secret := append([]byte(nil), f.secrets[fmt.Sprintf("%d/%d/%d", scope.TenantID, scope.UserID, generation)]...)
	metadata := f.active[userScope{tenantID: scope.TenantID, userID: scope.UserID}]
	f.mu.Unlock()
	if len(secret) == 0 {
		return types.NewAppError(types.CodeNotFound, "missing", types.ErrNotFound)
	}
	return use(secret, metadata)
}

func telegramFleetProvider(t *testing.T) (*httptest.Server, *sync.Map) {
	t.Helper()
	webhooks := &sync.Map{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		tokenPath := strings.TrimPrefix(strings.TrimSuffix(r.URL.Path, "/"+method), "/bot")
		idText, _, _ := strings.Cut(tokenPath, ":")
		botID, _ := strconv.ParseInt(idText, 10, 64)
		w.Header().Set("Content-Type", "application/json")
		switch method {
		case "getMe":
			_, _ = fmt.Fprintf(w, `{"ok":true,"result":{"id":%d,"username":"bot_%d"}}`, botID, botID)
		case "setWebhook":
			body, _ := io.ReadAll(r.Body)
			var request struct {
				URL string `json:"url"`
			}
			_ = json.Unmarshal(body, &request)
			webhooks.Store(botID, request.URL)
			_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
		case "setMyCommands", "setChatMenuButton":
			_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
		case "getWebhookInfo":
			value, _ := webhooks.Load(botID)
			_, _ = fmt.Fprintf(w, `{"ok":true,"result":{"url":%q,"max_connections":1,"allowed_updates":["message","callback_query","my_chat_member"]}}`, value)
		default:
			http.Error(w, "unknown", http.StatusNotFound)
		}
	}))
	return server, webhooks
}

func TestFleetIsolatesTenantBotsAndHotRotates(t *testing.T) {
	provider, _ := telegramFleetProvider(t)
	defer provider.Close()
	st := newFakeFleetStore()
	st.setCredential(7, 70, 1, 111, "111:user-seven", "secret-seven")
	st.setCredential(7, 80, 1, 222, "222:user-eight", "secret-eight")
	fleet, err := NewFleet(FleetConfig{
		WebhookURL: "https://api.vane.test/telegram/webhook",
		APIBaseURL: provider.URL, Workers: 1, Dynamic: true,
		ShutdownGrace: time.Second,
	}, st, &fakeChannelAgent{}, provider.Client(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := fleet.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fleet.Shutdown(context.Background()) })
	if got := fleet.PrincipalStatus(t.Context(), 7, 70); !got.Ready || got.BotID != 111 {
		t.Fatalf("user 70 status=%+v", got)
	}
	if got := fleet.PrincipalStatus(t.Context(), 7, 80); !got.Ready || got.BotID != 222 {
		t.Fatalf("user 80 status=%+v", got)
	}

	request := func(path, secret string) int {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"update_id":-1}`))
		req.SetPathValue("bot_id", strings.TrimPrefix(path, "/telegram/webhook/"))
		req.Header.Set(webhookSecretHeader, secret)
		rr := httptest.NewRecorder()
		fleet.Handler().ServeHTTP(rr, req)
		return rr.Code
	}
	if got := request("/telegram/webhook/111", "secret-seven"); got != http.StatusOK {
		t.Fatalf("tenant 7 webhook status=%d", got)
	}
	if got := request("/telegram/webhook/111", "secret-eight"); got != http.StatusUnauthorized {
		t.Fatalf("cross-tenant secret status=%d", got)
	}
	if got := request("/telegram/webhook/999", "secret-seven"); got != http.StatusNotFound {
		t.Fatalf("unknown bot status=%d", got)
	}

	st.setCredential(7, 70, 2, 333, "333:user-seven-new", "secret-seven-new")
	if err := fleet.ActivateUser(t.Context(), 7, 70); err != nil {
		t.Fatal(err)
	}
	if got := request("/telegram/webhook/111", "secret-seven"); got != http.StatusNotFound {
		t.Fatalf("retired bot route status=%d", got)
	}
	if got := request("/telegram/webhook/333", "secret-seven-new"); got != http.StatusOK {
		t.Fatalf("rotated bot route status=%d", got)
	}
	if got := fleet.PrincipalStatus(t.Context(), 7, 80); !got.Ready || got.BotID != 222 {
		t.Fatalf("user 80 changed during user 70 rotation: %+v", got)
	}
}

func TestFleetRejectsOneBotAcrossTenantsWithoutReplacingOwner(t *testing.T) {
	provider, _ := telegramFleetProvider(t)
	defer provider.Close()
	st := newFakeFleetStore()
	st.setCredential(7, 70, 1, 111, "111:user-seven", "secret-seven")
	fleet, err := NewFleet(FleetConfig{WebhookURL: "https://api.vane.test/telegram/webhook",
		APIBaseURL: provider.URL, Workers: 1, Dynamic: true,
		ShutdownGrace: time.Second}, st, &fakeChannelAgent{}, provider.Client(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := fleet.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fleet.Shutdown(context.Background()) })
	st.setCredential(7, 80, 1, 111, "111:copied", "secret-eight")
	if err := fleet.ActivateUser(t.Context(), 7, 80); err == nil {
		t.Fatal("duplicate bot identity activated for another tenant")
	}
	if got := fleet.PrincipalStatus(t.Context(), 7, 70); !got.Ready || got.BotID != 111 {
		t.Fatalf("original tenant manager was replaced: %+v", got)
	}
}

func TestTenantWebhookURLRejectsAuthorityDrift(t *testing.T) {
	if got, err := tenantWebhookURL("https://api.vane.test/telegram/webhook", "123"); err != nil || got != "https://api.vane.test/telegram/webhook/123" {
		t.Fatalf("url=%q err=%v", got, err)
	}
	for _, value := range []string{
		"http://api.vane.test/telegram/webhook",
		"https://api.vane.test/other",
		"https://api.vane.test/telegram/webhook?tenant=7",
	} {
		if _, err := tenantWebhookURL(value, "123"); err == nil {
			t.Fatalf("unsafe base accepted: %s", value)
		}
	}
}
