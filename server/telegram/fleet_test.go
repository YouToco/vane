package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

type fakeFleetStore struct {
	*fakeIngressStore
	mu        sync.Mutex
	active    map[userScope]store.CredentialMetadata
	secrets   map[string][]byte
	listErr   error
	activeErr error
	useErr    error
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
	if f.listErr != nil {
		return nil, f.listErr
	}
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
	if f.activeErr != nil {
		return store.CredentialMetadata{}, f.activeErr
	}
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
	if f.useErr != nil {
		return f.useErr
	}
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
	if _, err := fleet.IssueLink(t.Context(), 7, 70); err != nil {
		t.Fatal(err)
	}
	if _, err := fleet.IssueRouteLink(t.Context(), 7, 70); err != nil {
		t.Fatal(err)
	}
	if _, err := fleet.Binding(t.Context(), 7, 70); err != nil {
		t.Fatal(err)
	}
	if _, err := fleet.Routes(t.Context(), 7, 70); err != nil {
		t.Fatal(err)
	}
	if _, err := fleet.BlockedReplies(t.Context(), 7, 70); err != nil {
		t.Fatal(err)
	}
	if err := fleet.UnlinkRoute(t.Context(), 7, 70, 91); err != nil {
		t.Fatal(err)
	}
	// The provider fake deliberately rejects sendMessage. These calls still
	// prove that the fleet delegates through the exact user's manager.
	if err := fleet.SendTest(t.Context(), 7, 70); err == nil {
		t.Fatal("provider rejection was hidden by SendTest")
	}
	if err := fleet.SendTextEffect(t.Context(), 7, 70, 91,
		uuid.NewString(), "periodic_report", "body"); err == nil {
		t.Fatal("provider rejection was hidden by SendTextEffect")
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
	if err := fleet.Unlink(t.Context(), 7, 70); err != nil {
		t.Fatal(err)
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

func TestFleetLegacyBridgeStartsRoutesAndDelegates(t *testing.T) {
	provider, _ := telegramFleetProvider(t)
	defer provider.Close()
	st := newFakeFleetStore()
	fleet, err := NewFleet(FleetConfig{Legacy: Config{
		Enabled: true, Token: "444:legacy", WebhookSecret: "legacy-secret",
		WebhookURL: "https://api.vane.test/telegram/webhook",
		APIBaseURL: provider.URL, Workers: 1,
	}, ShutdownGrace: time.Second}, st, &fakeChannelAgent{}, provider.Client(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := fleet.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fleet.Shutdown(context.Background()) })
	if got := fleet.Status(); !got.Enabled || !got.Ready || got.BotID != 444 {
		t.Fatalf("status=%+v", got)
	}
	if got := fleet.PrincipalStatus(t.Context(), 7, 70); !got.Ready || got.BotID != 444 {
		t.Fatalf("principal status=%+v", got)
	}
	req := httptest.NewRequest(http.MethodPost, "/telegram/webhook", strings.NewReader(`{"update_id":-1}`))
	req.Header.Set(webhookSecretHeader, "legacy-secret")
	rr := httptest.NewRecorder()
	fleet.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("legacy webhook status=%d body=%s", rr.Code, rr.Body.String())
	}
	if _, err := fleet.IssueLink(t.Context(), 7, 70); err != nil {
		t.Fatal(err)
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
		"https://user@api.vane.test/telegram/webhook",
		"https://api.vane.test/telegram/webhook#fragment",
	} {
		if _, err := tenantWebhookURL(value, "123"); err == nil {
			t.Fatalf("unsafe base accepted: %s", value)
		}
	}
	if _, err := tenantWebhookURL("https://api.vane.test/telegram/webhook", "not-a-bot"); err == nil {
		t.Fatal("invalid bot identity accepted")
	}
}

func TestFleetEmptyLifecycleAndDelegationBoundaries(t *testing.T) {
	if _, err := NewFleet(FleetConfig{Dynamic: true}, nil, nil, nil, nil); err == nil {
		t.Fatal("dynamic fleet accepted incomplete dependencies")
	}
	fleet, err := NewFleet(FleetConfig{}, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := fleet.Ping(t.Context()); err == nil {
		t.Fatal("unstarted fleet ping succeeded")
	}
	if err := fleet.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := fleet.Start(t.Context()); err == nil {
		t.Fatal("second fleet start succeeded")
	}
	if err := fleet.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := fleet.Status(); got.Enabled || got.Ready {
		t.Fatalf("empty fleet status=%+v", got)
	}
	if got := fleet.PrincipalStatus(t.Context(), 1, 2); got.Enabled || got.Ready {
		t.Fatalf("empty principal status=%+v", got)
	}
	req := httptest.NewRequest(http.MethodPost, "/telegram/webhook/404", nil)
	req.SetPathValue("bot_id", "404")
	rr := httptest.NewRecorder()
	fleet.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("empty handler status=%d", rr.Code)
	}
	if err := fleet.DeactivateUser(t.Context(), 1, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := fleet.IssueLink(t.Context(), 1, 2); err == nil {
		t.Fatal("IssueLink without manager succeeded")
	}
	if _, err := fleet.IssueRouteLink(t.Context(), 1, 2); err == nil {
		t.Fatal("IssueRouteLink without manager succeeded")
	}
	if _, err := fleet.Binding(t.Context(), 1, 2); err == nil {
		t.Fatal("Binding without manager succeeded")
	}
	if _, err := fleet.Routes(t.Context(), 1, 2); err == nil {
		t.Fatal("Routes without manager succeeded")
	}
	if got, err := fleet.BlockedReplies(t.Context(), 1, 2); err != nil || got != (store.ChannelDeliveryBlockStats{}) {
		t.Fatalf("blocked replies=%+v err=%v", got, err)
	}
	if err := fleet.Unlink(t.Context(), 1, 2); err == nil {
		t.Fatal("Unlink without manager succeeded")
	}
	if err := fleet.UnlinkRoute(t.Context(), 1, 2, 3); err == nil {
		t.Fatal("UnlinkRoute without manager succeeded")
	}
	if err := fleet.SendTest(t.Context(), 1, 2); err == nil {
		t.Fatal("SendTest without manager succeeded")
	}
	if err := fleet.SendTextEffect(t.Context(), 1, 2, 3,
		"00000000-0000-0000-0000-000000000001", "brief", "body"); err == nil {
		t.Fatal("SendTextEffect without manager succeeded")
	}
	if err := fleet.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := fleet.Ping(t.Context()); err == nil {
		t.Fatal("shutdown fleet ping succeeded")
	}
}

func TestFleetCredentialInventoryAndActivationFailures(t *testing.T) {
	sentinel := types.NewAppError(types.CodeDatabase, "sentinel", errors.New("database unavailable"))
	st := newFakeFleetStore()
	st.listErr = sentinel
	fleet, err := NewFleet(FleetConfig{Dynamic: true}, st, &fakeChannelAgent{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := fleet.Start(t.Context()); !errors.Is(err, sentinel) {
		t.Fatalf("start err=%v", err)
	}

	st = newFakeFleetStore()
	fleet, err = NewFleet(FleetConfig{Dynamic: true,
		WebhookURL: "https://api.vane.test/telegram/webhook"}, st, &fakeChannelAgent{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := fleet.ActivateUser(t.Context(), 0, 1); err == nil {
		t.Fatal("invalid principal activated")
	}
	st.activeErr = sentinel
	if err := fleet.ActivateUser(t.Context(), 1, 2); !errors.Is(err, sentinel) {
		t.Fatalf("metadata err=%v", err)
	}
	st.activeErr = nil

	bad := store.CredentialMetadata{CredentialScope: telegramScope(1, 2),
		Generation: 1, Metadata: json.RawMessage(`{"bot_id":0}`)}
	if err := fleet.activateMetadata(t.Context(), bad); err == nil {
		t.Fatal("zero bot metadata activated")
	}
	bad.Metadata = json.RawMessage(`not-json`)
	if err := fleet.activateMetadata(t.Context(), bad); err == nil {
		t.Fatal("invalid bot metadata activated")
	}
	metadata, _ := json.Marshal(storedTelegramMetadata{BotID: 123})
	bad.Metadata = metadata
	st.active[userScope{tenantID: 1, userID: 2}] = bad
	st.useErr = sentinel
	if err := fleet.activateMetadata(t.Context(), bad); !errors.Is(err, sentinel) {
		t.Fatalf("credential use err=%v", err)
	}
	st.useErr = nil
	st.secrets["1/2/1"] = []byte(`not-json`)
	if err := fleet.activateMetadata(t.Context(), bad); err == nil {
		t.Fatal("invalid stored secret activated")
	}
	secret, _ := json.Marshal(storedTelegramSecret{BotToken: "123:token", WebhookSecret: "secret"})
	st.secrets["1/2/1"] = secret
	fleet.cfg.WebhookURL = "http://api.vane.test/telegram/webhook"
	if err := fleet.activateMetadata(t.Context(), bad); err == nil {
		t.Fatal("unsafe webhook URL activated")
	}
	fleet.cfg.WebhookURL = "https://api.vane.test/telegram/webhook"
	if err := fleet.activateMetadata(t.Context(), bad); err == nil || !strings.Contains(err.Error(), "not started") {
		t.Fatalf("unstarted activation err=%v", err)
	}
}
