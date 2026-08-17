package telegram

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/agent"
	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

type fakeIngressStore struct {
	identity          store.ChannelIdentity
	route             store.ChannelRoute
	resolveErr        error
	acceptCalls       int
	acceptedText      string
	acceptedKind      string
	acceptedThread    string
	acceptedMedia     []byte
	accepted          bool
	acceptErr         error
	claim             store.ChannelIngress
	claimErr          error
	replyReady        string
	replyErr          error
	sending           store.ChannelIngress
	sendingErr        error
	readyClaim        store.ChannelIngress
	readyErr          error
	completed         []string
	completeErr       error
	rejected          bool
	rejectErr         error
	ambiguous         bool
	ambiguousErr      error
	consumed          bool
	consumeErr        error
	linkHash          []byte
	issueErr          error
	revokeErr         error
	authorityErr      error
	blocked           store.ChannelDeliveryBlockStats
	blockedErr        error
	outbound          store.ChannelOutboundEffect
	prepareErr        error
	claimOutboundErr  error
	routes            []store.ChannelRoute
	routesErr         error
	routeReplay       bool
	migratedFrom      string
	migratedTo        string
	invalidatedChat   string
	invalidatedThread string
	invalidatedReason string
	deferredReply     bool
	deferredOutbound  bool
}

func (f *fakeIngressStore) IssueTelegramLinkRequest(_ context.Context, _, _ int64, _ string, hash []byte, _ time.Time) error {
	f.linkHash = append([]byte(nil), hash...)
	return f.issueErr
}
func (f *fakeIngressStore) ConsumeTelegramLinkRequest(_ context.Context, tokenHash []byte, _, _, _, _ string) (store.ChannelIdentity, bool, error) {
	f.consumed = true
	f.linkHash = append([]byte(nil), tokenHash...)
	return f.identity, true, f.consumeErr
}
func (f *fakeIngressStore) ResolveTelegramIdentity(context.Context, string, string, string) (store.ChannelIdentity, error) {
	return f.identity, f.resolveErr
}
func (f *fakeIngressStore) GetTelegramIdentityForUser(context.Context, int64, int64, string) (store.ChannelIdentity, error) {
	return f.identity, f.resolveErr
}
func (f *fakeIngressStore) RevokeTelegramIdentity(context.Context, int64, int64, string) error {
	return f.revokeErr
}
func (f *fakeIngressStore) RevokeTelegramIdentityAuthorized(context.Context, store.TelegramRuntimeAuthority) error {
	return f.revokeErr
}
func (f *fakeIngressStore) VerifyTelegramRuntimeAuthority(context.Context, store.TelegramRuntimeAuthority) error {
	return f.authorityErr
}
func (f *fakeIngressStore) AcceptTelegramIngress(context.Context, store.ChannelIdentity, string, string, string, string) (bool, error) {
	f.acceptCalls++
	return f.accepted, f.acceptErr
}
func (f *fakeIngressStore) IssueTelegramRouteLinkRequest(_ context.Context, _, _ int64, _ string, hash []byte, _ time.Time) error {
	f.linkHash = append([]byte(nil), hash...)
	return f.issueErr
}
func (f *fakeIngressStore) ConsumeTelegramRouteLinkRequest(context.Context, []byte, string, string, string, string, string, string) (store.ChannelRoute, bool, error) {
	f.consumed = true
	return f.route, !f.routeReplay, f.consumeErr
}
func (f *fakeIngressStore) ResolveTelegramRoute(context.Context, string, string, string, string) (store.ChannelIdentity, store.ChannelRoute, error) {
	return f.identity, f.route, f.resolveErr
}
func (f *fakeIngressStore) AcceptTelegramRoutedIngress(_ context.Context, _ store.ChannelIdentity, route store.ChannelRoute, _, _, text, _, _, kind, _ string, media []byte) (bool, error) {
	f.acceptCalls++
	f.acceptedText = text
	f.acceptedKind = kind
	f.acceptedThread = route.ProviderThreadID
	f.acceptedMedia = append([]byte(nil), media...)
	return f.accepted, f.acceptErr
}
func (f *fakeIngressStore) ListTelegramRoutesForUser(context.Context, int64, int64, string) ([]store.ChannelRoute, error) {
	if f.routes != nil || f.routesErr != nil {
		return f.routes, f.routesErr
	}
	route := f.route
	if route.ID == 0 {
		route = store.ChannelRoute{ID: 1, RouteKind: "private",
			ProviderChatID: f.identity.ProviderChatID, ProviderThreadID: "0"}
	}
	return []store.ChannelRoute{route}, f.resolveErr
}
func (f *fakeIngressStore) RevokeTelegramRoute(context.Context, int64, int64, int64, string) error {
	return f.revokeErr
}
func (f *fakeIngressStore) MigrateTelegramRoutes(_ context.Context, _ string, oldChatID, newChatID string) error {
	f.migratedFrom, f.migratedTo = oldChatID, newChatID
	return f.revokeErr
}
func (f *fakeIngressStore) InvalidateTelegramDestination(_ context.Context, _, chatID, threadID, reason string) error {
	f.invalidatedChat, f.invalidatedThread, f.invalidatedReason = chatID, threadID, reason
	return f.revokeErr
}
func (f *fakeIngressStore) PrepareTelegramOutbound(_ context.Context, tenantID, userID, routeID int64, effectID, kind, text string) (store.ChannelOutboundEffect, error) {
	if f.prepareErr != nil {
		return store.ChannelOutboundEffect{}, f.prepareErr
	}
	if f.outbound.Status != "" && f.outbound.Status != "prepared" {
		return f.outbound, nil
	}
	chatID := f.route.ProviderChatID
	threadID := f.route.ProviderThreadID
	if chatID == "" {
		chatID = f.identity.ProviderChatID
	}
	if chatID == "" {
		chatID = "42"
	}
	if threadID == "" {
		threadID = "0"
	}
	f.outbound = store.ChannelOutboundEffect{EffectID: effectID, TenantID: tenantID,
		UserID: userID, RouteID: routeID, Provider: "telegram",
		ProviderChatID: chatID, ProviderThreadID: threadID,
		EffectKind: kind, PayloadText: text, Status: "prepared"}
	return f.outbound, nil
}
func (f *fakeIngressStore) ClaimTelegramOutbound(context.Context, string) (store.ChannelOutboundEffect, error) {
	if f.claimOutboundErr != nil {
		return store.ChannelOutboundEffect{}, f.claimOutboundErr
	}
	f.outbound.Status = "sending"
	return f.outbound, nil
}
func (f *fakeIngressStore) ClaimNextTelegramOutbound(context.Context, string) (store.ChannelOutboundEffect, error) {
	return store.ChannelOutboundEffect{}, types.NewAppError(
		types.CodeNotFound, "empty", types.ErrNotFound)
}
func (f *fakeIngressStore) CompleteTelegramOutbound(context.Context, store.ChannelOutboundEffect, []string) error {
	f.outbound.Status = "sent"
	return f.completeErr
}
func (f *fakeIngressStore) MarkTelegramOutboundRejected(context.Context, store.ChannelOutboundEffect, string) error {
	f.outbound.Status = "failed"
	return f.rejectErr
}
func (f *fakeIngressStore) DeferTelegramOutbound(context.Context, store.ChannelOutboundEffect, time.Duration, int) (bool, error) {
	f.deferredOutbound = true
	return true, f.rejectErr
}
func (f *fakeIngressStore) MarkTelegramOutboundAmbiguous(context.Context, store.ChannelOutboundEffect, []string, string) error {
	f.outbound.Status = "ambiguous"
	return f.ambiguousErr
}
func (f *fakeIngressStore) ClaimNextTelegramIngress(context.Context, string, time.Duration) (store.ChannelIngress, error) {
	return f.claim, f.claimErr
}
func (f *fakeIngressStore) MarkTelegramIngressReplyReady(_ context.Context, _ store.ChannelIngress, reply string) error {
	f.replyReady = reply
	return f.replyErr
}
func (*fakeIngressStore) MarkTelegramIngressFailed(context.Context, store.ChannelIngress, string) error {
	return nil
}
func (f *fakeIngressStore) ClaimTelegramReplySend(context.Context, string, string, string) (store.ChannelIngress, error) {
	return f.sending, f.sendingErr
}
func (f *fakeIngressStore) ClaimNextTelegramReplySend(context.Context, string) (store.ChannelIngress, error) {
	return f.readyClaim, f.readyErr
}
func (f *fakeIngressStore) CompleteTelegramReply(_ context.Context, _ store.ChannelIngress, ids []string) error {
	f.completed = append([]string(nil), ids...)
	return f.completeErr
}
func (f *fakeIngressStore) MarkTelegramReplyRejected(context.Context, store.ChannelIngress, string) error {
	f.rejected = true
	return f.rejectErr
}
func (f *fakeIngressStore) DeferTelegramReply(context.Context, store.ChannelIngress, time.Duration, int) (bool, error) {
	f.deferredReply = true
	return true, f.rejectErr
}
func (f *fakeIngressStore) MarkTelegramReplyAmbiguous(context.Context, store.ChannelIngress, []string, string) error {
	f.ambiguous = true
	return f.ambiguousErr
}
func (f *fakeIngressStore) TelegramBlockedReplyStats(context.Context, string) (store.ChannelDeliveryBlockStats, error) {
	return f.blocked, f.blockedErr
}
func (f *fakeIngressStore) TelegramBlockedReplyStatsForUser(context.Context, string, int64, int64) (store.ChannelDeliveryBlockStats, error) {
	return f.blocked, f.blockedErr
}

type fakeChannelAgent struct {
	calls  int
	scope  string
	turnID string
	text   string
	reply  string
	err    error
}

func (f *fakeChannelAgent) HandleChannelMessage(
	_ context.Context, _ auth.Principal, scope, turnID, text string,
) (agent.Outcome, error) {
	f.calls++
	f.scope, f.turnID, f.text = scope, turnID, text
	return agent.Outcome{Reply: f.reply}, f.err
}

func newWebhookTestManager(t *testing.T, st *fakeIngressStore) *Manager {
	t.Helper()
	m, err := NewManager(Config{
		Enabled: true, Token: "123:token", WebhookSecret: "hook_secret",
		WebhookURL: "https://api.vane.test/telegram/webhook", Workers: 1,
	}, st, &fakeChannelAgent{}, &http.Client{Timeout: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	m.bot = Bot{ID: 123, Username: "vane_test_bot"}
	m.status.Ready = true
	m.status.BotID = 123
	return m
}

func webhookRequest(t *testing.T, secret, body string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/telegram/webhook", strings.NewReader(body))
	if secret != "" {
		req.Header.Set(webhookSecretHeader, secret)
	}
	return req
}

func TestWebhookRejectsMissingOrWrongSecretBeforeStore(t *testing.T) {
	for _, secret := range []string{"", "wrong", " hook_secret", "hook_secret "} {
		st := &fakeIngressStore{}
		m := newWebhookTestManager(t, st)
		rr := httptest.NewRecorder()
		m.Handler().ServeHTTP(rr, webhookRequest(t, secret,
			`{"update_id":1,"message":{"message_id":1,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"hello"}}`))
		if rr.Code != http.StatusUnauthorized || st.acceptCalls != 0 {
			t.Fatalf("secret=%q status=%d store_calls=%d", secret, rr.Code, st.acceptCalls)
		}
	}
}

func TestWebhookRejectsOversizedBodyBeforeStore(t *testing.T) {
	st := &fakeIngressStore{}
	m := newWebhookTestManager(t, st)
	rr := httptest.NewRecorder()
	body := `{"update_id":1,"message":{"message_id":1,"from":{"id":42},` +
		`"chat":{"id":42,"type":"private"},"text":"` +
		strings.Repeat("x", webhookBodyLimit+1) + `"}}`
	m.Handler().ServeHTTP(rr, webhookRequest(t, "hook_secret", body))
	if rr.Code != http.StatusBadRequest || st.acceptCalls != 0 {
		t.Fatalf("status=%d calls=%d", rr.Code, st.acceptCalls)
	}
}

func TestWebhookRejectsUnboundAndGroupBeforeAgentIngress(t *testing.T) {
	t.Run("unbound private", func(t *testing.T) {
		st := &fakeIngressStore{resolveErr: types.NewAppError(
			types.CodeNotFound, "unbound", types.ErrNotFound)}
		m := newWebhookTestManager(t, st)
		rr := httptest.NewRecorder()
		m.Handler().ServeHTTP(rr, webhookRequest(t, "hook_secret",
			`{"update_id":1,"message":{"message_id":1,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"hello"}}`))
		if rr.Code != http.StatusOK || st.acceptCalls != 0 {
			t.Fatalf("status=%d calls=%d", rr.Code, st.acceptCalls)
		}
	})
	t.Run("group", func(t *testing.T) {
		st := &fakeIngressStore{}
		m := newWebhookTestManager(t, st)
		rr := httptest.NewRecorder()
		m.Handler().ServeHTTP(rr, webhookRequest(t, "hook_secret",
			`{"update_id":2,"message":{"message_id":1,"from":{"id":42},"chat":{"id":-99,"type":"group"},"text":"hello"}}`))
		if rr.Code != http.StatusOK || st.acceptCalls != 0 {
			t.Fatalf("status=%d calls=%d", rr.Code, st.acceptCalls)
		}
	})
	t.Run("identity store unavailable", func(t *testing.T) {
		st := &fakeIngressStore{resolveErr: types.NewAppError(
			types.CodeDatabase, "database unavailable", context.DeadlineExceeded)}
		m := newWebhookTestManager(t, st)
		rr := httptest.NewRecorder()
		m.Handler().ServeHTTP(rr, webhookRequest(t, "hook_secret",
			`{"update_id":3,"message":{"message_id":1,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"hello"}}`))
		if rr.Code != http.StatusServiceUnavailable || st.acceptCalls != 0 {
			t.Fatalf("status=%d calls=%d", rr.Code, st.acceptCalls)
		}
	})
}

func TestWebhookAcceptsOnlyAddressedAuthorizedGroupAndTopicMessages(t *testing.T) {
	st := &fakeIngressStore{
		identity: store.ChannelIdentity{ID: 1, TenantID: 7, UserID: 9,
			Provider: "telegram", AppIdentity: "123", ExternalUserID: "42",
			Status: "active"},
		route: store.ChannelRoute{ID: 2, TenantID: 7, UserID: 9,
			IdentityID: 1, Provider: "telegram", AppIdentity: "123",
			ProviderChatID: "-1007", ProviderThreadID: "88",
			ChatType: "supergroup", RouteKind: "topic", Status: "active"},
	}
	m := newWebhookTestManager(t, st)
	ordinary := `{"update_id":20,"message":{"message_id":3,"message_thread_id":88,"from":{"id":42},"chat":{"id":-1007,"type":"supergroup"},"text":"ambient"}}`
	rr := httptest.NewRecorder()
	m.Handler().ServeHTTP(rr, webhookRequest(t, "hook_secret", ordinary))
	if rr.Code != http.StatusOK || st.acceptCalls != 0 {
		t.Fatalf("ordinary status=%d calls=%d", rr.Code, st.acceptCalls)
	}
	mentioned := `{"update_id":21,"message":{"message_id":4,"message_thread_id":88,"from":{"id":42},"chat":{"id":-1007,"type":"supergroup"},"text":"@vane_test_bot 列出任务"}}`
	rr = httptest.NewRecorder()
	m.Handler().ServeHTTP(rr, webhookRequest(t, "hook_secret", mentioned))
	if rr.Code != http.StatusOK || st.acceptCalls != 1 ||
		st.acceptedText != "列出任务" || st.acceptedThread != "88" {
		t.Fatalf("mentioned status=%d calls=%d text=%q thread=%q",
			rr.Code, st.acceptCalls, st.acceptedText, st.acceptedThread)
	}
	command := `{"update_id":22,"message":{"message_id":5,"message_thread_id":88,"from":{"id":42},"chat":{"id":-1007,"type":"supergroup"},"text":"/tasks@vane_test_bot"}}`
	rr = httptest.NewRecorder()
	m.Handler().ServeHTTP(rr, webhookRequest(t, "hook_secret", command))
	if rr.Code != http.StatusOK || st.acceptCalls != 2 ||
		st.acceptedText != "列出我的情报任务" || st.acceptedKind != "command" {
		t.Fatalf("command status=%d calls=%d text=%q kind=%q",
			rr.Code, st.acceptCalls, st.acceptedText, st.acceptedKind)
	}
	ambientMedia := `{"update_id":23,"message":{"message_id":6,"message_thread_id":88,"from":{"id":42},"chat":{"id":-1007,"type":"supergroup"},"photo":[{"file_id":"ambient","file_unique_id":"ambient-u","width":640,"height":360,"file_size":1000}]}}`
	rr = httptest.NewRecorder()
	m.Handler().ServeHTTP(rr, webhookRequest(t, "hook_secret", ambientMedia))
	if rr.Code != http.StatusOK || st.acceptCalls != 2 {
		t.Fatalf("ambient media status=%d calls=%d", rr.Code, st.acceptCalls)
	}
	mentionedMedia := `{"update_id":24,"message":{"message_id":7,"message_thread_id":88,"from":{"id":42},"chat":{"id":-1007,"type":"supergroup"},"caption":"@vane_test_bot 分析这张图","photo":[{"file_id":"photo-file","file_unique_id":"photo-u","width":1280,"height":720,"file_size":2000}]}}`
	rr = httptest.NewRecorder()
	m.Handler().ServeHTTP(rr, webhookRequest(t, "hook_secret", mentionedMedia))
	media, err := types.DecodeChannelMessageEnvelopeV1(st.acceptedMedia)
	if rr.Code != http.StatusOK || st.acceptCalls != 3 || err != nil ||
		st.acceptedText != "telegram:media-help" || st.acceptedKind != "message" ||
		len(media.Items) != 1 || media.Items[0].Kind != "image" ||
		media.Caption != "分析这张图" {
		t.Fatalf("mentioned media status=%d calls=%d text=%q kind=%q media=%+v err=%v",
			rr.Code, st.acceptCalls, st.acceptedText, st.acceptedKind, media, err)
	}
}

func TestWebhookConsumesTelegramLifecycleBeforeAgentIngress(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		assert func(*testing.T, *fakeIngressStore)
	}{
		{
			name: "group migration",
			body: `{"update_id":40,"message":{"message_id":9,"chat":{"id":-99,"type":"group"},"migrate_to_chat_id":-10099}}`,
			assert: func(t *testing.T, st *fakeIngressStore) {
				if st.migratedFrom != "-99" || st.migratedTo != "-10099" {
					t.Fatalf("migration=%q -> %q", st.migratedFrom, st.migratedTo)
				}
			},
		},
		{
			name: "bot removed",
			body: `{"update_id":41,"my_chat_member":{"chat":{"id":-10099,"type":"supergroup"},"from":{"id":42},"old_chat_member":{"user":{"id":123,"is_bot":true},"status":"administrator"},"new_chat_member":{"user":{"id":123,"is_bot":true},"status":"kicked"}}}`,
			assert: func(t *testing.T, st *fakeIngressStore) {
				if st.invalidatedChat != "-10099" || st.invalidatedThread != "" ||
					st.invalidatedReason != "bot_membership_lost" {
					t.Fatalf("invalidation=%q/%q reason=%q", st.invalidatedChat,
						st.invalidatedThread, st.invalidatedReason)
				}
			},
		},
		{
			name: "topic closed",
			body: `{"update_id":42,"message":{"message_id":10,"message_thread_id":88,"chat":{"id":-10099,"type":"supergroup"},"forum_topic_closed":{}}}`,
			assert: func(t *testing.T, st *fakeIngressStore) {
				if st.invalidatedChat != "-10099" || st.invalidatedThread != "88" ||
					st.invalidatedReason != "topic_closed" {
					t.Fatalf("invalidation=%q/%q reason=%q", st.invalidatedChat,
						st.invalidatedThread, st.invalidatedReason)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			st := &fakeIngressStore{}
			m := newWebhookTestManager(t, st)
			rr := httptest.NewRecorder()
			m.Handler().ServeHTTP(rr, webhookRequest(t, "hook_secret", test.body))
			if rr.Code != http.StatusOK || st.acceptCalls != 0 {
				t.Fatalf("status=%d ingress_calls=%d", rr.Code, st.acceptCalls)
			}
			test.assert(t, st)
		})
	}
}

func TestWebhookLifecycleDatabaseFailureIsRetryable(t *testing.T) {
	st := &fakeIngressStore{revokeErr: types.NewAppError(
		types.CodeDatabase, "database unavailable", context.DeadlineExceeded)}
	m := newWebhookTestManager(t, st)
	rr := httptest.NewRecorder()
	m.Handler().ServeHTTP(rr, webhookRequest(t, "hook_secret",
		`{"update_id":43,"message":{"message_id":10,"message_thread_id":88,"chat":{"id":-10099,"type":"supergroup"},"forum_topic_closed":{}}}`))
	if rr.Code != http.StatusServiceUnavailable || st.acceptCalls != 0 {
		t.Fatalf("status=%d ingress_calls=%d", rr.Code, st.acceptCalls)
	}
}

func TestWebhookSignedCallbackIsScopedAndDurablyAccepted(t *testing.T) {
	st := &fakeIngressStore{
		identity: store.ChannelIdentity{ID: 1},
		route:    store.ChannelRoute{ID: 2, ProviderThreadID: "0"},
	}
	m := newWebhookTestManager(t, st)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/answerCallbackQuery") {
			t.Fatalf("path=%s", r.URL.Path)
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer provider.Close()
	client, err := NewClient("123:token", provider.URL, &http.Client{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	m.client = client
	data := m.callbackData(123, "tasks")
	body := `{"update_id":30,"callback_query":{"id":"cb-1","from":{"id":42},"message":{"message_id":9,"chat":{"id":42,"type":"private"}},"data":"` + data + `"}}`
	rr := httptest.NewRecorder()
	m.Handler().ServeHTTP(rr, webhookRequest(t, "hook_secret", body))
	if rr.Code != http.StatusOK || st.acceptCalls != 1 ||
		st.acceptedKind != "callback" || st.acceptedText != "列出我的情报任务" {
		t.Fatalf("status=%d calls=%d kind=%q text=%q",
			rr.Code, st.acceptCalls, st.acceptedKind, st.acceptedText)
	}
	tampered := strings.Replace(data, "tasks", "status", 1)
	body = `{"update_id":31,"callback_query":{"id":"cb-2","from":{"id":42},"message":{"message_id":9,"chat":{"id":42,"type":"private"}},"data":"` + tampered + `"}}`
	rr = httptest.NewRecorder()
	m.Handler().ServeHTTP(rr, webhookRequest(t, "hook_secret", body))
	if rr.Code != http.StatusOK || st.acceptCalls != 1 {
		t.Fatalf("tampered status=%d calls=%d", rr.Code, st.acceptCalls)
	}
}

func TestGroupRouteInstallRequiresTelegramAdmin(t *testing.T) {
	raw := bytes.Repeat([]byte{'r'}, 32)
	token := base64.RawURLEncoding.EncodeToString(raw)
	for _, tc := range []struct {
		status   string
		consumed bool
	}{
		{status: "member", consumed: false},
		{status: "administrator", consumed: true},
	} {
		t.Run(tc.status, func(t *testing.T) {
			st := &fakeIngressStore{route: store.ChannelRoute{ID: 3}}
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/getChatMember"):
					_, _ = w.Write([]byte(`{"ok":true,"result":{"status":"` + tc.status + `"}}`))
				case strings.HasSuffix(r.URL.Path, "/sendMessage"):
					_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":10,"chat":{"id":-1007}}}`))
				default:
					t.Fatalf("path=%s", r.URL.Path)
				}
			}))
			defer provider.Close()
			m := newWebhookTestManager(t, st)
			client, err := NewClient("123:token", provider.URL, &http.Client{Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			m.client = client
			body := `{"update_id":40,"message":{"message_id":11,"message_thread_id":88,"from":{"id":42},"chat":{"id":-1007,"type":"supergroup"},"text":"/connect ` + token + `"}}`
			rr := httptest.NewRecorder()
			m.Handler().ServeHTTP(rr, webhookRequest(t, "hook_secret", body))
			if rr.Code != http.StatusOK || st.consumed != tc.consumed {
				t.Fatalf("status=%d consumed=%t", rr.Code, st.consumed)
			}
		})
	}
}

func TestGroupRouteInstallFailureAndReplayBranches(t *testing.T) {
	m := newWebhookTestManager(t, &fakeIngressStore{})
	if err := m.consumeRouteLink(t.Context(), m.bot,
		normalizedUpdate{ActorID: 42, ChatID: -1007, ChatType: "supergroup"},
		"not-base64"); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("invalid token err=%v", err)
	}
	raw := bytes.Repeat([]byte{'g'}, 32)
	token := base64.RawURLEncoding.EncodeToString(raw)
	for _, tc := range []struct {
		name       string
		statusCode int
		storeErr   error
		replay     bool
	}{
		{name: "provider failure", statusCode: http.StatusInternalServerError},
		{name: "store failure", statusCode: http.StatusOK, storeErr: context.DeadlineExceeded},
		{name: "exact replay", statusCode: http.StatusOK, replay: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &fakeIngressStore{consumeErr: tc.storeErr, routeReplay: tc.replay}
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/getChatMember") && tc.statusCode == http.StatusOK {
					_, _ = w.Write([]byte(`{"ok":true,"result":{"status":"administrator"}}`))
					return
				}
				w.WriteHeader(tc.statusCode)
			}))
			defer provider.Close()
			manager := newWebhookTestManager(t, st)
			client, err := NewClient("123:token", provider.URL, &http.Client{Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			manager.client = client
			err = manager.consumeRouteLink(t.Context(), manager.bot,
				normalizedUpdate{ActorID: 42, ChatID: -1007, ThreadID: 88, ChatType: "supergroup"}, token)
			if tc.replay && err != nil {
				t.Fatalf("replay err=%v", err)
			}
			if !tc.replay && err == nil {
				t.Fatal("failure branch succeeded")
			}
		})
	}
	t.Run("confirmation ambiguous", func(t *testing.T) {
		st := &fakeIngressStore{}
		provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/getChatMember") {
				_, _ = w.Write([]byte(`{"ok":true,"result":{"status":"creator"}}`))
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer provider.Close()
		manager := newWebhookTestManager(t, st)
		client, err := NewClient("123:token", provider.URL, &http.Client{Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		manager.client = client
		if err := manager.consumeRouteLink(t.Context(), manager.bot,
			normalizedUpdate{ActorID: 42, ChatID: -1007, ThreadID: 88, ChatType: "supergroup"}, token); err != nil {
			t.Fatalf("provider-crossed confirmation changed route result: %v", err)
		}
	})
}

func TestWebhookAcceptsBoundPrivateTextWithStableIdentity(t *testing.T) {
	st := &fakeIngressStore{identity: store.ChannelIdentity{
		ID: 3, TenantID: 7, UserID: 9, Provider: "telegram",
		AppIdentity: "123", ExternalUserID: "42", ProviderChatID: "42",
		Status: "active",
	}}
	m := newWebhookTestManager(t, st)
	rr := httptest.NewRecorder()
	m.Handler().ServeHTTP(rr, webhookRequest(t, "hook_secret",
		`{"update_id":77,"message":{"message_id":1,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"列出任务"}}`))
	if rr.Code != http.StatusOK || st.acceptCalls != 1 {
		t.Fatalf("status=%d calls=%d", rr.Code, st.acceptCalls)
	}
	if got := stableTelegramTurnID(123, 77); got != stableTelegramTurnID(123, 77) ||
		got == stableTelegramTurnID(124, 77) {
		t.Fatalf("unstable/cross-bot turn id=%q", got)
	}
}

func TestSplitMessageBoundariesPreserveUnicode(t *testing.T) {
	for _, count := range []int{4095, 4096, 4097, 8193} {
		input := strings.Repeat("见", count-1) + "🙂"
		chunks := SplitMessage(input)
		if strings.Join(chunks, "") != input {
			t.Fatalf("count=%d content changed", count)
		}
		for _, chunk := range chunks {
			if len([]rune(chunk)) > maxMessageRunes {
				t.Fatalf("count=%d oversized chunk", count)
			}
		}
	}
}

func TestStripBotMentionRequiresExactUsernameBoundary(t *testing.T) {
	for _, test := range []struct {
		name string
		text string
		want string
		ok   bool
	}{
		{name: "standalone", text: "请 @Vane_Bot 列出任务", want: "请   列出任务", ok: true},
		{name: "punctuation", text: "@vane_bot，列出任务", want: "，列出任务", ok: true},
		{name: "username suffix", text: "@vane_bot_extra 列出任务", want: "@vane_bot_extra 列出任务"},
		{name: "email local part", text: "owner@vane_bot 列出任务", want: "owner@vane_bot 列出任务"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := stripBotMention(test.text, "vane_bot")
			if got != test.want || ok != test.ok {
				t.Fatalf("got=(%q,%t), want=(%q,%t)", got, ok, test.want, test.ok)
			}
		})
	}
}

func TestTelegramUpdateNormalizationAndCommandMatrix(t *testing.T) {
	m := newWebhookTestManager(t, &fakeIngressStore{})
	bot := Bot{ID: 123, Username: "vane_test_bot"}
	validMessage := func() *Message {
		return &Message{MessageID: 1, From: &User{ID: 42},
			Chat: Chat{ID: 42, Type: "private"}, Text: "hello"}
	}
	invalid := []Update{
		{},
		{CallbackQuery: &CallbackQuery{ID: "cb", From: User{ID: 42}}},
		{CallbackQuery: &CallbackQuery{ID: strings.Repeat("x", 129), From: User{ID: 42}, Message: validMessage()}},
		{Message: &Message{MessageID: 1, Chat: Chat{ID: 42, Type: "private"}, Text: "x"}},
		{Message: &Message{MessageID: 1, From: &User{ID: 42, IsBot: true}, Chat: Chat{ID: 42, Type: "private"}, Text: "x"}},
		{Message: &Message{MessageID: 1, From: &User{ID: 42}, Chat: Chat{ID: 42, Type: "channel"}, Text: "x"}},
		{Message: &Message{MessageID: 1, From: &User{ID: 42}, Chat: Chat{ID: 43, Type: "private"}, Text: "x"}},
		{Message: &Message{MessageID: 1, From: &User{ID: 42}, Chat: Chat{ID: 42, Type: "private"}}},
		{Message: &Message{MessageID: 1, MessageThreadID: -1, From: &User{ID: 42}, Chat: Chat{ID: 42, Type: "private"}, Text: "x"}},
	}
	for i, update := range invalid {
		if got, ok := m.normalizeUpdate(bot, update); ok {
			t.Fatalf("invalid[%d] normalized=%+v", i, got)
		}
	}

	caption := validMessage()
	caption.Text, caption.Caption = "", " caption "
	mediaVariants := []*Message{caption}
	wantMediaKinds := []string{"", "image", "document", "audio", "video",
		"voice", "animation", "video_note", "sticker"}
	for _, configure := range []func(*Message){
		func(message *Message) {
			message.Caption = " compare "
			message.MediaGroupID = "album-1"
			message.Photo = []FileRef{
				{FileID: "small", Width: 320, Height: 180, FileSize: 10},
				{FileID: "p", FileUniqueID: "pu", Width: 1280, Height: 720, FileSize: 20},
			}
		},
		func(message *Message) { message.Document = &FileRef{FileID: "d"} },
		func(message *Message) { message.Audio = &FileRef{FileID: "a"} },
		func(message *Message) { message.Video = &FileRef{FileID: "v"} },
		func(message *Message) { message.Voice = &FileRef{FileID: "o"} },
		func(message *Message) {
			message.Animation = &FileRef{FileID: "g"}
			message.Document = &FileRef{FileID: "compat-duplicate"}
		},
		func(message *Message) { message.VideoNote = &FileRef{FileID: "n", Length: 240} },
		func(message *Message) { message.Sticker = &FileRef{FileID: "s"} },
	} {
		message := validMessage()
		configure(message)
		mediaVariants = append(mediaVariants, message)
	}
	for i, message := range mediaVariants {
		got, ok := m.normalizeUpdate(bot, Update{Message: message})
		if !ok {
			t.Fatalf("variant[%d] rejected", i)
		}
		if i == 0 && got.Text != "caption" {
			t.Fatalf("caption=%q", got.Text)
		}
		if i > 0 && got.Text != "telegram:media-help" {
			t.Fatalf("media[%d]=%q", i, got.Text)
		}
		if i == 0 && len(got.MediaEnvelope) != 0 {
			t.Fatalf("caption-only envelope=%s", got.MediaEnvelope)
		}
		if i > 0 {
			envelope, err := types.DecodeChannelMessageEnvelopeV1(got.MediaEnvelope)
			if err != nil || len(envelope.Items) != 1 ||
				envelope.Items[0].Kind != wantMediaKinds[i] {
				t.Fatalf("media[%d] envelope=%+v err=%v", i, envelope, err)
			}
			if i == 1 && (envelope.Items[0].ProviderFileID != "p" ||
				envelope.Caption != "compare" || envelope.MediaGroupID != "album-1") {
				t.Fatalf("photo envelope=%+v", envelope)
			}
			if i == 6 && envelope.Items[0].ProviderFileID != "g" {
				t.Fatalf("animation duplicate not collapsed: %+v", envelope)
			}
			if i == 7 && (envelope.Items[0].Width != 240 || envelope.Items[0].Height != 240) {
				t.Fatalf("video note dimensions=%+v", envelope.Items[0])
			}
		}
	}

	reply := validMessage()
	reply.Chat = Chat{ID: -1007, Type: "supergroup"}
	reply.ReplyToMessage = &Message{From: &User{ID: bot.ID}}
	if got, ok := m.normalizeUpdate(bot, Update{Message: reply}); !ok || got.Text != "hello" {
		t.Fatalf("reply normalized=%+v ok=%t", got, ok)
	}
	for _, tc := range []struct {
		input string
		want  string
	}{
		{input: "/help", want: "telegram:help"},
		{input: "/status", want: "telegram:status"},
		{input: "/new", want: "telegram:new-help"},
		{input: "/new watch OpenAI", want: "创建情报任务：watch OpenAI"},
		{input: "/unknown", want: "telegram:unknown-command"},
		{input: "/start token", want: "/start token"},
		{input: "/connect token", want: "/connect token"},
	} {
		message := validMessage()
		message.Text = tc.input
		got, ok := m.normalizeUpdate(bot, Update{Message: message})
		if !ok || got.Text != tc.want || got.Kind != "command" {
			t.Fatalf("input=%q got=%+v ok=%t", tc.input, got, ok)
		}
	}
	if _, _, ok := parseCommand("/tasks@foreign_bot", bot.Username); ok {
		t.Fatal("foreign bot command accepted")
	}
	if _, ok := startToken("/start", bot.Username); ok {
		t.Fatal("empty start token accepted")
	}
	if _, ok := routeToken("/help token", bot.Username); ok {
		t.Fatal("non-route token accepted")
	}
	if commandPrompt("bogus", "") != "" || callbackPrompt("bogus") != "" {
		t.Fatal("unknown action received a prompt")
	}
	for _, input := range []string{"telegram:help", "telegram:status", "telegram:new-help", "telegram:unknown-command", "telegram:media-help"} {
		if reply, ok := staticCommandReply(input); !ok || reply == "" {
			t.Fatalf("static reply input=%s reply=%q ok=%t", input, reply, ok)
		}
	}
	mediaReply, ok := staticCommandReply("telegram:media-help")
	if !ok || !strings.Contains(mediaReply, "不会通过转写、抽帧、OCR 或描述生成绕行处理") ||
		!strings.Contains(mediaReply, "后续原生能力升级") {
		t.Fatalf("media reply permits an implicit conversion fallback: %q", mediaReply)
	}
	if reply, ok := staticCommandReply("ordinary"); ok || reply != "" {
		t.Fatalf("ordinary static reply=%q ok=%t", reply, ok)
	}
	if action, ok := m.verifyCallback(bot.ID, "invalid"); ok || action != "" {
		t.Fatal("malformed callback accepted")
	}
	if action, ok := m.verifyCallback(bot.ID, m.callbackData(bot.ID, "delete")); ok || action != "" {
		t.Fatal("unsupported callback accepted")
	}
	if m.commandKeyboard() == nil {
		t.Fatal("ready manager has no command keyboard")
	}
	m.status.Ready = false
	if m.commandKeyboard() != nil {
		t.Fatal("unready manager exposed command keyboard")
	}
}

func TestProcessOneUsesStableAgentTurnAndSettlesSend(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/sendMessage") {
			t.Fatalf("path=%s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":88,"chat":{"id":42,"type":"private"}}}`))
	}))
	defer api.Close()
	turnID := stableTelegramTurnID(123, 77)
	item := store.ChannelIngress{
		Provider: "telegram", AppIdentity: "123", ProviderUpdateID: "77",
		TenantID: 7, UserID: 9, RouteID: 31, ProviderChatID: "42", InputText: "列出任务",
		StableTurnID: turnID, ProcessingLease: "lease",
	}
	st := &fakeIngressStore{claim: item, sending: item}
	st.sending.ReplyText = "这是回复"
	agentFake := &fakeChannelAgent{reply: "这是回复"}
	m, err := NewManager(Config{
		Enabled: true, Token: "123:token", WebhookSecret: "secret",
		WebhookURL: "https://api.vane.test/telegram/webhook", APIBaseURL: api.URL,
		Workers: 1,
	}, st, agentFake, &http.Client{Timeout: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	m.bot = Bot{ID: 123, Username: "bot"}
	m.status.Ready = true
	if !m.processOne(t.Context()) {
		t.Fatal("processOne returned false")
	}
	if agentFake.calls != 1 || agentFake.scope != "channel-route:31" ||
		agentFake.turnID != turnID ||
		st.replyReady != "这是回复" || len(st.completed) != 1 ||
		st.completed[0] != "88" || st.ambiguous {
		t.Fatalf("agent=%+v store=%+v", agentFake, st)
	}
}

func TestRateLimitedReplyUsesDurableRetryAfterWithoutAmbiguity(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"ok":false,"error_code":429,"parameters":{"retry_after":9}}`))
	}))
	defer api.Close()
	item := store.ChannelIngress{Provider: "telegram", AppIdentity: "123",
		ProviderUpdateID: "81", ProviderChatID: "42", ReplyText: "reply"}
	st := &fakeIngressStore{}
	m, err := NewManager(Config{Enabled: true, Token: "123:token",
		WebhookSecret: "secret", WebhookURL: "https://vane.test/telegram/webhook",
		APIBaseURL: api.URL, Workers: 1}, st, &fakeChannelAgent{},
		&http.Client{Timeout: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	m.status.Ready = true
	m.deliverClaimedReply(t.Context(), item)
	if !st.deferredReply || st.rejected || st.ambiguous || !m.Status().Ready {
		t.Fatalf("deferred=%t rejected=%t ambiguous=%t status=%+v",
			st.deferredReply, st.rejected, st.ambiguous, m.Status())
	}
}

func TestRateLimitedOutboundUsesStableEffectDeferral(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"ok":false,"error_code":429,"parameters":{"retry_after":11}}`))
	}))
	defer api.Close()
	st := &fakeIngressStore{route: store.ChannelRoute{
		ID: 31, ProviderChatID: "42", ProviderThreadID: "0",
	}}
	m, err := NewManager(Config{Enabled: true, Token: "123:token",
		WebhookSecret: "secret", WebhookURL: "https://vane.test/telegram/webhook",
		APIBaseURL: api.URL, Workers: 1}, st, &fakeChannelAgent{},
		&http.Client{Timeout: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = m.SendTextEffect(t.Context(), 7, 9, 31, uuid.NewString(),
		"test", "rate limited")
	if !errors.Is(err, types.ErrPush) || !st.deferredOutbound ||
		st.outbound.Status == "ambiguous" || st.outbound.Status == "failed" {
		t.Fatalf("err=%v deferred=%t outbound=%+v",
			err, st.deferredOutbound, st.outbound)
	}
}

func TestProcessOneTurnsAgentFailureIntoDurableUserReply(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":89,"chat":{"id":42}}}`))
	}))
	defer api.Close()
	item := store.ChannelIngress{
		Provider: "telegram", AppIdentity: "123", ProviderUpdateID: "78",
		UserID: 9, ProviderChatID: "42", InputText: "执行任务",
		StableTurnID: stableTelegramTurnID(123, 78), ProcessingLease: "lease",
	}
	st := &fakeIngressStore{claim: item, sending: item}
	st.sending.ReplyText = "这次处理未能确认完成。请先在 Vane 网页检查任务状态，再决定是否重试。"
	m, err := NewManager(Config{
		Enabled: true, Token: "123:token", WebhookSecret: "secret",
		WebhookURL: "https://api.vane.test/telegram/webhook", APIBaseURL: api.URL,
		Workers: 1,
	}, st, &fakeChannelAgent{err: context.DeadlineExceeded},
		&http.Client{Timeout: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	m.bot = Bot{ID: 123, Username: "bot"}
	m.status.Ready = true
	if !m.processOne(t.Context()) || !strings.Contains(st.replyReady, "网页检查任务状态") ||
		len(st.completed) != 1 {
		t.Fatalf("reply=%q completed=%v", st.replyReady, st.completed)
	}
}

func TestDefiniteProviderRejectIsNotMarkedAmbiguous(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"ok":false,"error_code":401}`))
	}))
	defer api.Close()
	item := store.ChannelIngress{Provider: "telegram", AppIdentity: "123",
		ProviderUpdateID: "79", ProviderChatID: "42", ReplyText: "reply"}
	st := &fakeIngressStore{}
	m, err := NewManager(Config{
		Enabled: true, Token: "123:token", WebhookSecret: "secret",
		WebhookURL: "https://api.vane.test/telegram/webhook", APIBaseURL: api.URL,
		Workers: 1,
	}, st, &fakeChannelAgent{}, &http.Client{Timeout: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	m.status.Ready = true
	m.deliverClaimedReply(t.Context(), item)
	if !st.rejected || st.ambiguous || m.Status().Ready ||
		m.Status().LastErrorCode != "provider_auth_rejected" {
		t.Fatalf("rejected=%t ambiguous=%t status=%+v",
			st.rejected, st.ambiguous, m.Status())
	}
}

func TestForbiddenProviderRejectDoesNotTakeAdapterOffline(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"ok":false,"error_code":403,"description":"Forbidden: bot was blocked by the user"}`))
	}))
	defer api.Close()
	item := store.ChannelIngress{Provider: "telegram", AppIdentity: "123",
		ProviderUpdateID: "80", ProviderChatID: "42", ReplyText: "reply"}
	st := &fakeIngressStore{}
	m, err := NewManager(Config{
		Enabled: true, Token: "123:token", WebhookSecret: "secret",
		WebhookURL: "https://api.vane.test/telegram/webhook", APIBaseURL: api.URL,
		Workers: 1,
	}, st, &fakeChannelAgent{}, &http.Client{Timeout: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	m.bot = Bot{ID: 123, Username: "bot"}
	m.status.Ready = true
	m.deliverClaimedReply(t.Context(), item)
	if !st.rejected || st.ambiguous || !m.Status().Ready ||
		m.Status().LastErrorCode != "" {
		t.Fatalf("rejected=%t ambiguous=%t status=%+v",
			st.rejected, st.ambiguous, m.Status())
	}
}

func TestManagerStartRequiresExactVerifiedWebhookState(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/getMe"):
			_, _ = w.Write([]byte(`{"ok":true,"result":{"id":123,"username":"vane_bot"}}`))
		case strings.HasSuffix(r.URL.Path, "/setWebhook"):
			_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
		case strings.HasSuffix(r.URL.Path, "/setMyCommands"),
			strings.HasSuffix(r.URL.Path, "/setChatMenuButton"):
			_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
		case strings.HasSuffix(r.URL.Path, "/getWebhookInfo"):
			// The last error may be from Telegram racing this process before its
			// HTTP listener is live. Exact URL state, not historical delivery
			// text, is the startup authority.
			_, _ = w.Write([]byte(`{"ok":true,"result":{"url":"https://api.vane.test/telegram/webhook","pending_update_count":2,"last_error_message":"Connection refused","max_connections":1,"allowed_updates":["message","callback_query","my_chat_member"]}}`))
		default:
			t.Fatalf("unexpected method path=%s", r.URL.Path)
		}
	}))
	defer api.Close()
	st := &fakeIngressStore{claimErr: types.NewAppError(
		types.CodeNotFound, "none", types.ErrNotFound),
		readyErr: types.NewAppError(
			types.CodeNotFound, "none", types.ErrNotFound)}
	m, err := NewManager(Config{
		Enabled: true, Token: "123:token", WebhookSecret: "secret",
		WebhookURL: "https://api.vane.test/telegram/webhook",
		APIBaseURL: api.URL, Workers: 1,
	}, st, &fakeChannelAgent{}, &http.Client{Timeout: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	if err := m.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	status := m.Status()
	if !status.Ready || status.BotID != 123 || status.BotUsername != "vane_bot" ||
		status.PendingUpdateCount != 2 {
		cancel()
		t.Fatalf("status=%+v", status)
	}
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
	defer shutdownCancel()
	if err := m.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
}

func TestReadyReplyRecoverySendsWithoutRepeatingAgent(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":91,"chat":{"id":42}}}`))
	}))
	defer api.Close()
	ready := store.ChannelIngress{
		Provider: "telegram", AppIdentity: "123", ProviderUpdateID: "77",
		ProviderChatID: "42", ReplyText: "恢复的耐久回复", Status: "sending",
	}
	st := &fakeIngressStore{readyClaim: ready}
	agentFake := &fakeChannelAgent{reply: "should not run"}
	m, err := NewManager(Config{
		Enabled: true, Token: "123:token", WebhookSecret: "secret",
		WebhookURL: "https://api.vane.test/telegram/webhook", APIBaseURL: api.URL,
		Workers: 1,
	}, st, agentFake, &http.Client{Timeout: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	m.bot = Bot{ID: 123, Username: "bot"}
	m.status.Ready = true
	if !m.processReadyReply(t.Context()) || agentFake.calls != 0 ||
		len(st.completed) != 1 || st.completed[0] != "91" {
		t.Fatalf("agent_calls=%d completed=%v", agentFake.calls, st.completed)
	}
}

func TestStartTokenHashIsNotRawToken(t *testing.T) {
	raw := bytes.Repeat([]byte{'a'}, 32)
	token := base64.RawURLEncoding.EncodeToString(raw)
	wantHash := sha256.Sum256(raw)
	st := &fakeIngressStore{}
	m := newWebhookTestManager(t, st)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":1,"chat":{"id":42}}}`))
	}))
	defer api.Close()
	var err error
	m.client, err = NewClient("123:token", api.URL,
		&http.Client{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	body := `{"update_id":9,"message":{"message_id":1,"from":{"id":42},` +
		`"chat":{"id":42,"type":"private"},"text":"/start ` + token + `"}}`
	m.Handler().ServeHTTP(rr, webhookRequest(t, "hook_secret", body))
	if rr.Code != http.StatusOK || !st.consumed ||
		!bytes.Equal(st.linkHash, wantHash[:]) || bytes.Equal(st.linkHash, raw) {
		t.Fatalf("status=%d consumed=%t hash=%x", rr.Code, st.consumed, st.linkHash)
	}
	if _, ok := startToken("/start abc extra", "vane_test_bot"); ok {
		t.Fatal("ambiguous start command accepted")
	}
}

func TestStartPairingStoreFailureRequestsProviderRetry(t *testing.T) {
	raw := bytes.Repeat([]byte{'b'}, 32)
	token := base64.RawURLEncoding.EncodeToString(raw)
	st := &fakeIngressStore{consumeErr: types.NewAppError(
		types.CodeDatabase, "database unavailable", context.DeadlineExceeded)}
	m := newWebhookTestManager(t, st)
	rr := httptest.NewRecorder()
	body := `{"update_id":10,"message":{"message_id":1,"from":{"id":42},` +
		`"chat":{"id":42,"type":"private"},"text":"/start ` + token + `"}}`
	m.Handler().ServeHTTP(rr, webhookRequest(t, "hook_secret", body))
	if rr.Code != http.StatusServiceUnavailable || !st.consumed {
		t.Fatalf("status=%d consumed=%t", rr.Code, st.consumed)
	}
}

func TestManagerConfigurationAndDisabledLifecycle(t *testing.T) {
	disabled, err := NewManager(Config{}, nil, nil, nil, nil)
	if err != nil || disabled.Status().Enabled {
		t.Fatalf("disabled=%+v err=%v", disabled, err)
	}
	if err := disabled.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := disabled.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := disabled.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, cfg := range []Config{
		{Enabled: true, Token: "123:token", WebhookURL: "https://vane.test/telegram/webhook", Workers: 1},
		{Enabled: true, Token: "123:token", WebhookSecret: "secret", WebhookURL: "https://vane.test/telegram/webhook", Workers: 2},
	} {
		if _, err := NewManager(cfg, &fakeIngressStore{}, &fakeChannelAgent{}, nil, nil); err == nil {
			t.Fatalf("invalid config accepted: %+v", cfg)
		}
	}
	if _, err := NewManager(Config{
		Enabled: true, Token: "bad-token", WebhookSecret: "secret",
		WebhookURL: "https://vane.test/telegram/webhook", Workers: 1,
	}, &fakeIngressStore{}, &fakeChannelAgent{}, nil, nil); err == nil {
		t.Fatal("invalid token accepted")
	}
}

func TestManagerOwnerOperationsAndPing(t *testing.T) {
	now := time.Now()
	st := &fakeIngressStore{
		identity: store.ChannelIdentity{ID: 3, TenantID: 7, UserID: 9,
			ProviderChatID: "42", BoundAt: now},
		blocked: store.ChannelDeliveryBlockStats{Count: 2, OldestAt: &now},
	}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/sendMessage") {
			t.Fatalf("path=%s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":7,"chat":{"id":42}}}`))
	}))
	defer api.Close()
	m, err := NewManager(Config{
		Enabled: true, Token: "123:token", WebhookSecret: "secret",
		WebhookURL: "https://vane.test/telegram/webhook", APIBaseURL: api.URL,
		Workers: 1,
	}, st, &fakeChannelAgent{}, &http.Client{Timeout: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.IssueLink(t.Context(), 7, 9); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("not-ready issue link=%v", err)
	}
	if _, err := m.Binding(t.Context(), 7, 9); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("not-ready binding=%v", err)
	}
	if err := m.Unlink(t.Context(), 7, 9); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("not-ready unlink=%v", err)
	}
	if got, err := m.BlockedReplies(t.Context(), 7, 9); err != nil || got.Count != 0 {
		t.Fatalf("not-ready blocked=%+v err=%v", got, err)
	}
	if err := m.Ping(t.Context()); err == nil {
		t.Fatal("unready manager ping succeeded")
	}
	m.bot = Bot{ID: 123, Username: "vane_bot"}
	m.status.Ready = true
	link, err := m.IssueLink(t.Context(), 7, 9)
	if err != nil || !strings.HasPrefix(link.DeepLink, "https://t.me/vane_bot?start=") ||
		len(st.linkHash) != sha256.Size {
		t.Fatalf("link=%+v hash=%x err=%v", link, st.linkHash, err)
	}
	if got, err := m.Binding(t.Context(), 7, 9); err != nil || got.ID != 3 {
		t.Fatalf("binding=%+v err=%v", got, err)
	}
	if got, err := m.BlockedReplies(t.Context(), 7, 9); err != nil || got.Count != 2 {
		t.Fatalf("blocked=%+v err=%v", got, err)
	}
	if err := m.Unlink(t.Context(), 7, 9); err != nil {
		t.Fatal(err)
	}
	if err := m.SendTest(t.Context(), 7, 9); err != nil {
		t.Fatal(err)
	}
	if err := m.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := m.Status(); got.BlockedReplyCount != 2 || got.OldestBlockedAt == nil {
		t.Fatalf("status=%+v", got)
	}
	st.blockedErr = context.DeadlineExceeded
	if err := m.Ping(t.Context()); err == nil {
		t.Fatal("blocked observation failure was ignored")
	}
}

func TestManagerRouteOwnerOperations(t *testing.T) {
	now := time.Now()
	st := &fakeIngressStore{routes: []store.ChannelRoute{
		{ID: 1, RouteKind: "private", ChatType: "private", BoundAt: now},
		{ID: 2, RouteKind: "topic", ChatType: "supergroup", BoundAt: now},
	}}
	m := newWebhookTestManager(t, st)
	m.status.Ready = false
	if _, err := m.IssueRouteLink(t.Context(), 7, 9); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("unready route link err=%v", err)
	}
	if _, err := m.Routes(t.Context(), 7, 9); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("unready routes err=%v", err)
	}
	if err := m.UnlinkRoute(t.Context(), 7, 9, 2); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("unready unlink route err=%v", err)
	}
	if err := m.SendTest(t.Context(), 7, 9); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("unready test err=%v", err)
	}
	m.status.Ready = true
	link, err := m.IssueRouteLink(t.Context(), 7, 9)
	if err != nil || !strings.Contains(link.DeepLink, "?startgroup=") ||
		!strings.HasPrefix(link.Command, "/connect ") {
		t.Fatalf("route link=%+v err=%v", link, err)
	}
	routes, err := m.Routes(t.Context(), 7, 9)
	if err != nil || len(routes) != 2 || routes[1].Kind != "topic" {
		t.Fatalf("routes=%+v err=%v", routes, err)
	}
	if err := m.UnlinkRoute(t.Context(), 7, 9, 2); err != nil {
		t.Fatal(err)
	}
	st.routesErr = context.DeadlineExceeded
	if _, err := m.Routes(t.Context(), 7, 9); err == nil {
		t.Fatal("route store failure ignored")
	}
	st.routesErr = nil
	st.routes = []store.ChannelRoute{{ID: 2, RouteKind: "group"}}
	if err := m.SendTest(t.Context(), 7, 9); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("test without private route err=%v", err)
	}
	st.issueErr = context.DeadlineExceeded
	if _, err := m.IssueRouteLink(t.Context(), 7, 9); err == nil {
		t.Fatal("route link store failure ignored")
	}
	st.revokeErr = context.DeadlineExceeded
	if err := m.UnlinkRoute(t.Context(), 7, 9, 2); err == nil {
		t.Fatal("route unlink store failure ignored")
	}
}

func TestSendTextEffectSettlementMatrix(t *testing.T) {
	newManager := func(t *testing.T, st *fakeIngressStore, handler http.HandlerFunc) *Manager {
		t.Helper()
		provider := httptest.NewServer(handler)
		t.Cleanup(provider.Close)
		m, err := NewManager(Config{Enabled: true, Token: "123:token",
			WebhookSecret: "secret", WebhookURL: "https://vane.test/telegram/webhook",
			APIBaseURL: provider.URL, Workers: 1}, st, &fakeChannelAgent{},
			&http.Client{Timeout: time.Second}, nil)
		if err != nil {
			t.Fatal(err)
		}
		m.bot = Bot{ID: 123, Username: "bot"}
		m.status.Ready = true
		return m
	}
	success := func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":7,"chat":{"id":42}}}`))
	}
	t.Run("prepare failure", func(t *testing.T) {
		st := &fakeIngressStore{prepareErr: context.DeadlineExceeded}
		m := newManager(t, st, success)
		if err := m.SendTextEffect(t.Context(), 7, 9, 1, uuid.NewString(), "test", "x"); err == nil {
			t.Fatal("prepare failure ignored")
		}
	})
	t.Run("already sent", func(t *testing.T) {
		st := &fakeIngressStore{outbound: store.ChannelOutboundEffect{Status: "sent"}}
		m := newManager(t, st, success)
		if err := m.SendTextEffect(t.Context(), 7, 9, 1, uuid.NewString(), "test", "x"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("blocked state", func(t *testing.T) {
		st := &fakeIngressStore{outbound: store.ChannelOutboundEffect{Status: "ambiguous"}}
		m := newManager(t, st, success)
		if err := m.SendTextEffect(t.Context(), 7, 9, 1, uuid.NewString(), "test", "x"); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("claim failure", func(t *testing.T) {
		st := &fakeIngressStore{claimOutboundErr: context.DeadlineExceeded}
		m := newManager(t, st, success)
		if err := m.SendTextEffect(t.Context(), 7, 9, 1, uuid.NewString(), "test", "x"); err == nil {
			t.Fatal("claim failure ignored")
		}
	})
	t.Run("definite reject", func(t *testing.T) {
		st := &fakeIngressStore{}
		m := newManager(t, st, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"ok":false,"error_code":403}`))
		})
		if err := m.SendTextEffect(t.Context(), 7, 9, 1, uuid.NewString(), "test", "x"); err == nil || st.outbound.Status != "failed" {
			t.Fatalf("status=%s err=%v", st.outbound.Status, err)
		}
	})
	t.Run("reject settlement failure", func(t *testing.T) {
		st := &fakeIngressStore{rejectErr: context.DeadlineExceeded}
		m := newManager(t, st, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
		})
		if err := m.SendTextEffect(t.Context(), 7, 9, 1, uuid.NewString(), "test", "x"); err == nil || m.Status().Ready {
			t.Fatalf("err=%v ready=%t", err, m.Status().Ready)
		}
	})
	t.Run("ambiguous settlement failure", func(t *testing.T) {
		st := &fakeIngressStore{ambiguousErr: context.DeadlineExceeded}
		m := newManager(t, st, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		})
		if err := m.SendTextEffect(t.Context(), 7, 9, 1, uuid.NewString(), "test", "x"); err == nil {
			t.Fatal("ambiguous settlement failure ignored")
		}
	})
	t.Run("completion failure", func(t *testing.T) {
		st := &fakeIngressStore{completeErr: context.DeadlineExceeded}
		m := newManager(t, st, success)
		if err := m.SendTextEffect(t.Context(), 7, 9, 1, uuid.NewString(), "test", "x"); err == nil {
			t.Fatal("completion failure ignored")
		}
	})
	t.Run("success", func(t *testing.T) {
		st := &fakeIngressStore{}
		m := newManager(t, st, success)
		if err := m.SendTextEffect(t.Context(), 7, 9, 1, uuid.NewString(), "test", "x"); err != nil || st.outbound.Status != "sent" {
			t.Fatalf("status=%s err=%v", st.outbound.Status, err)
		}
	})
}

func TestManagerOwnerOperationStoreAndProviderFailures(t *testing.T) {
	st := &fakeIngressStore{identity: store.ChannelIdentity{ProviderChatID: "42"}}
	m := newWebhookTestManager(t, st)
	st.issueErr = context.DeadlineExceeded
	if _, err := m.IssueLink(t.Context(), 7, 9); err == nil {
		t.Fatal("issue store failure ignored")
	}
	st.issueErr = nil
	st.resolveErr = context.DeadlineExceeded
	if _, err := m.Binding(t.Context(), 7, 9); err == nil {
		t.Fatal("binding store failure ignored")
	}
	st.resolveErr = nil
	st.revokeErr = context.DeadlineExceeded
	if err := m.Unlink(t.Context(), 7, 9); err == nil {
		t.Fatal("unlink store failure ignored")
	}
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"ok":false,"error_code":403}`))
	}))
	defer provider.Close()
	client, err := NewClient("123:token", provider.URL, &http.Client{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	m.client = client
	if err := m.SendTest(t.Context(), 7, 9); err == nil || !m.Status().Ready {
		t.Fatalf("send test err=%v status=%+v", err, m.Status())
	}
}

func TestManagerStartFailureStages(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
		code    string
	}{
		{name: "get me", code: "get_me", handler: func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"ok":false,"error_code":401}`))
		}},
		{name: "set webhook", code: "set_webhook", handler: func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/getMe") {
				_, _ = w.Write([]byte(`{"ok":true,"result":{"id":123,"username":"bot"}}`))
				return
			}
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"ok":false,"error_code":400}`))
		}},
		{name: "set commands", code: "set_commands", handler: func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/getMe") {
				_, _ = w.Write([]byte(`{"ok":true,"result":{"id":123,"username":"bot"}}`))
				return
			}
			if strings.HasSuffix(r.URL.Path, "/setWebhook") {
				_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
				return
			}
			w.WriteHeader(http.StatusBadRequest)
		}},
		{name: "set menu", code: "set_menu", handler: func(w http.ResponseWriter, r *http.Request) {
			switch {
			case strings.HasSuffix(r.URL.Path, "/getMe"):
				_, _ = w.Write([]byte(`{"ok":true,"result":{"id":123,"username":"bot"}}`))
			case strings.HasSuffix(r.URL.Path, "/setWebhook"), strings.HasSuffix(r.URL.Path, "/setMyCommands"):
				_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
			default:
				w.WriteHeader(http.StatusBadRequest)
			}
		}},
		{name: "get webhook info", code: "get_webhook_info", handler: func(w http.ResponseWriter, r *http.Request) {
			switch {
			case strings.HasSuffix(r.URL.Path, "/getMe"):
				_, _ = w.Write([]byte(`{"ok":true,"result":{"id":123,"username":"bot"}}`))
			case strings.HasSuffix(r.URL.Path, "/setWebhook"):
				_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
			case strings.HasSuffix(r.URL.Path, "/setMyCommands"),
				strings.HasSuffix(r.URL.Path, "/setChatMenuButton"):
				_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
			default:
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"ok":false,"error_code":500}`))
			}
		}},
		{name: "state mismatch", code: "webhook_state_mismatch", handler: func(w http.ResponseWriter, r *http.Request) {
			switch {
			case strings.HasSuffix(r.URL.Path, "/getMe"):
				_, _ = w.Write([]byte(`{"ok":true,"result":{"id":123,"username":"bot"}}`))
			case strings.HasSuffix(r.URL.Path, "/setWebhook"):
				_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
			case strings.HasSuffix(r.URL.Path, "/setMyCommands"),
				strings.HasSuffix(r.URL.Path, "/setChatMenuButton"):
				_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
			default:
				_, _ = w.Write([]byte(`{"ok":true,"result":{"url":"https://wrong.test","max_connections":40,"allowed_updates":[]}}`))
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := httptest.NewServer(tc.handler)
			defer api.Close()
			m, err := NewManager(Config{Enabled: true, Token: "123:token",
				WebhookSecret: "secret", WebhookURL: "https://vane.test/telegram/webhook",
				APIBaseURL: api.URL, Workers: 1}, &fakeIngressStore{},
				&fakeChannelAgent{}, &http.Client{Timeout: time.Second}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := m.Start(t.Context()); err == nil || m.Status().LastErrorCode != tc.code {
				t.Fatalf("err=%v status=%+v", err, m.Status())
			}
		})
	}
}

func TestWebhookProtocolAndPersistenceFailureBranches(t *testing.T) {
	validBody := `{"update_id":81,"message":{"message_id":1,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"hello"}}`
	t.Run("wrong method", func(t *testing.T) {
		m := newWebhookTestManager(t, &fakeIngressStore{})
		rr := httptest.NewRecorder()
		m.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/telegram/webhook", nil))
		if rr.Code != http.StatusNotFound {
			t.Fatalf("status=%d", rr.Code)
		}
	})
	t.Run("not ready", func(t *testing.T) {
		m := newWebhookTestManager(t, &fakeIngressStore{})
		m.status.Ready = false
		rr := httptest.NewRecorder()
		m.Handler().ServeHTTP(rr, webhookRequest(t, "hook_secret", validBody))
		if rr.Code != http.StatusServiceUnavailable {
			t.Fatalf("status=%d", rr.Code)
		}
	})
	t.Run("trailing payload", func(t *testing.T) {
		m := newWebhookTestManager(t, &fakeIngressStore{})
		rr := httptest.NewRecorder()
		m.Handler().ServeHTTP(rr, webhookRequest(t, "hook_secret", validBody+` {}`))
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("status=%d", rr.Code)
		}
	})
	t.Run("invalid update", func(t *testing.T) {
		m := newWebhookTestManager(t, &fakeIngressStore{})
		rr := httptest.NewRecorder()
		m.Handler().ServeHTTP(rr, webhookRequest(t, "hook_secret", `{"update_id":-1}`))
		if rr.Code != http.StatusOK {
			t.Fatalf("status=%d", rr.Code)
		}
	})
	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{name: "conflict", err: types.NewAppError(types.CodeConflict, "conflict", types.ErrConflict), want: http.StatusConflict},
		{name: "database", err: context.DeadlineExceeded, want: http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &fakeIngressStore{identity: store.ChannelIdentity{ID: 1}, acceptErr: tc.err}
			m := newWebhookTestManager(t, st)
			rr := httptest.NewRecorder()
			m.Handler().ServeHTTP(rr, webhookRequest(t, "hook_secret", validBody))
			if rr.Code != tc.want || st.acceptCalls != 1 {
				t.Fatalf("status=%d calls=%d", rr.Code, st.acceptCalls)
			}
		})
	}
}

func TestWorkerErrorBranchesRemainDurable(t *testing.T) {
	m := newWebhookTestManager(t, &fakeIngressStore{})
	m.status.Ready = false
	if m.processReadyReply(t.Context()) || m.processOne(t.Context()) {
		t.Fatal("unready manager made progress")
	}
	m.status.Ready = true
	st := m.store.(*fakeIngressStore)
	st.readyErr = context.DeadlineExceeded
	st.claimErr = context.DeadlineExceeded
	if m.processReadyReply(t.Context()) || m.processOne(t.Context()) {
		t.Fatal("claim failures made progress")
	}
	item := store.ChannelIngress{Provider: "telegram", AppIdentity: "123",
		ProviderUpdateID: "90", UserID: 9, ProviderChatID: "42",
		StableTurnID: stableTelegramTurnID(123, 90), InputText: "hello"}
	st.claimErr = nil
	st.claim = item
	st.replyErr = context.DeadlineExceeded
	if !m.processOne(t.Context()) {
		t.Fatal("persist failure did not count as claimed progress")
	}
	st.replyErr = nil
	st.sendingErr = context.DeadlineExceeded
	if !m.processOne(t.Context()) {
		t.Fatal("send claim failure did not count as claimed progress")
	}
}

func TestDeliverySettlementFailureBranches(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":92,"chat":{"id":42}}}`))
	}))
	defer server.Close()
	item := store.ChannelIngress{Provider: "telegram", AppIdentity: "123",
		ProviderUpdateID: "92", ProviderChatID: "42", ReplyText: "reply"}
	for _, tc := range []struct {
		name      string
		configure func(*fakeIngressStore)
	}{
		{name: "completion", configure: func(st *fakeIngressStore) { st.completeErr = context.DeadlineExceeded }},
		{name: "rejection", configure: func(st *fakeIngressStore) { st.rejectErr = context.DeadlineExceeded }},
		{name: "ambiguity", configure: func(st *fakeIngressStore) { st.ambiguousErr = context.DeadlineExceeded }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &fakeIngressStore{}
			tc.configure(st)
			apiURL := server.URL
			if tc.name == "rejection" {
				rejected := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusForbidden)
					_, _ = w.Write([]byte(`{"ok":false,"error_code":403}`))
				}))
				defer rejected.Close()
				apiURL = rejected.URL
			}
			if tc.name == "ambiguity" {
				ambiguous := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				}))
				defer ambiguous.Close()
				apiURL = ambiguous.URL
			}
			m, err := NewManager(Config{Enabled: true, Token: "123:token",
				WebhookSecret: "secret", WebhookURL: "https://vane.test/telegram/webhook",
				APIBaseURL: apiURL, Workers: 1}, st, &fakeChannelAgent{},
				&http.Client{Timeout: time.Second}, nil)
			if err != nil {
				t.Fatal(err)
			}
			m.status.Ready = true
			m.deliverClaimedReply(t.Context(), item)
		})
	}
}

func TestDeliverClaimedReplyPreservesTopicAndAddsCommandKeyboard(t *testing.T) {
	var body string
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, _ := io.ReadAll(r.Body)
		body = string(payload)
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":92,"chat":{"id":-1007}}}`))
	}))
	defer provider.Close()
	st := &fakeIngressStore{}
	m, err := NewManager(Config{Enabled: true, Token: "123:token",
		WebhookSecret: "secret", WebhookURL: "https://vane.test/telegram/webhook",
		APIBaseURL: provider.URL, Workers: 1}, st, &fakeChannelAgent{},
		&http.Client{Timeout: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	m.bot = Bot{ID: 123, Username: "bot"}
	m.status.Ready = true
	m.deliverClaimedReply(t.Context(), store.ChannelIngress{
		Provider: "telegram", AppIdentity: "123", ProviderUpdateID: "92",
		ProviderChatID: "-1007", ProviderThreadID: "88", ReplyText: "help",
		InputText: "telegram:help", IngressKind: "command",
	})
	if len(st.completed) != 1 || !strings.Contains(body, `"message_thread_id":88`) ||
		!strings.Contains(body, `"reply_markup"`) {
		t.Fatalf("completed=%v body=%s", st.completed, body)
	}
}

func TestOutboundEffectPreservesTopicAndBlocksAmbiguousRetry(t *testing.T) {
	var body string
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, _ := io.ReadAll(r.Body)
		body = string(payload)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"ok":false,"error_code":500}`))
	}))
	defer provider.Close()
	st := &fakeIngressStore{route: store.ChannelRoute{ID: 7,
		ProviderChatID: "-1007", ProviderThreadID: "88", RouteKind: "topic"}}
	m, err := NewManager(Config{Enabled: true, Token: "123:token",
		WebhookSecret: "secret", WebhookURL: "https://vane.test/telegram/webhook",
		APIBaseURL: provider.URL, Workers: 1}, st, &fakeChannelAgent{},
		&http.Client{Timeout: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	m.bot = Bot{ID: 123, Username: "bot"}
	m.status.Ready = true
	err = m.SendTextEffect(t.Context(), 7, 9, 7, uuid.NewString(),
		"periodic_report", "report")
	if err == nil || st.outbound.Status != "ambiguous" ||
		!strings.Contains(body, `"message_thread_id":88`) {
		t.Fatalf("err=%v status=%s body=%s", err, st.outbound.Status, body)
	}
}

func TestManagerStartBlockedObservationAndShutdownDeadline(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/getMe"):
			_, _ = w.Write([]byte(`{"ok":true,"result":{"id":123,"username":"bot"}}`))
		case strings.HasSuffix(r.URL.Path, "/setWebhook"):
			_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
		case strings.HasSuffix(r.URL.Path, "/setMyCommands"),
			strings.HasSuffix(r.URL.Path, "/setChatMenuButton"):
			_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
		default:
			_, _ = w.Write([]byte(`{"ok":true,"result":{"url":"https://vane.test/telegram/webhook","max_connections":1,"allowed_updates":["message","callback_query","my_chat_member"]}}`))
		}
	}))
	defer api.Close()
	st := &fakeIngressStore{blockedErr: context.DeadlineExceeded}
	m, err := NewManager(Config{Enabled: true, Token: "123:token",
		WebhookSecret: "secret", WebhookURL: "https://vane.test/telegram/webhook",
		APIBaseURL: api.URL, Workers: 0}, st, &fakeChannelAgent{},
		&http.Client{Timeout: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(t.Context()); err == nil || m.Status().LastErrorCode != "blocked_reply_observation" {
		t.Fatalf("err=%v status=%+v", err, m.Status())
	}
	m.cfg.Enabled = true
	m.wg.Add(1)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := m.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown err=%v", err)
	}
	m.wg.Done()
}
