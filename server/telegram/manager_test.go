package telegram

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/server/agent"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

type fakeIngressStore struct {
	identity     store.ChannelIdentity
	resolveErr   error
	acceptCalls  int
	accepted     bool
	acceptErr    error
	claim        store.ChannelIngress
	claimErr     error
	replyReady   string
	replyErr     error
	sending      store.ChannelIngress
	sendingErr   error
	readyClaim   store.ChannelIngress
	readyErr     error
	completed    []string
	completeErr  error
	rejected     bool
	rejectErr    error
	ambiguous    bool
	ambiguousErr error
	consumed     bool
	consumeErr   error
	linkHash     []byte
	issueErr     error
	revokeErr    error
	blocked      store.ChannelDeliveryBlockStats
	blockedErr   error
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
func (f *fakeIngressStore) AcceptTelegramIngress(context.Context, store.ChannelIdentity, string, string, string, string) (bool, error) {
	f.acceptCalls++
	return f.accepted, f.acceptErr
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
	turnID string
	text   string
	reply  string
	err    error
}

func (f *fakeChannelAgent) HandleChannelMessage(_ context.Context, _ int64, turnID, text string) (agent.Outcome, error) {
	f.calls++
	f.turnID, f.text = turnID, text
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
		TenantID: 7, UserID: 9, ProviderChatID: "42", InputText: "列出任务",
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
	if agentFake.calls != 1 || agentFake.turnID != turnID ||
		st.replyReady != "这是回复" || len(st.completed) != 1 ||
		st.completed[0] != "88" || st.ambiguous {
		t.Fatalf("agent=%+v store=%+v", agentFake, st)
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
		case strings.HasSuffix(r.URL.Path, "/getWebhookInfo"):
			// The last error may be from Telegram racing this process before its
			// HTTP listener is live. Exact URL state, not historical delivery
			// text, is the startup authority.
			_, _ = w.Write([]byte(`{"ok":true,"result":{"url":"https://api.vane.test/telegram/webhook","pending_update_count":2,"last_error_message":"Connection refused","max_connections":1,"allowed_updates":["message"]}}`))
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
	if _, ok := startToken("/start abc extra"); ok {
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
		{name: "get webhook info", code: "get_webhook_info", handler: func(w http.ResponseWriter, r *http.Request) {
			switch {
			case strings.HasSuffix(r.URL.Path, "/getMe"):
				_, _ = w.Write([]byte(`{"ok":true,"result":{"id":123,"username":"bot"}}`))
			case strings.HasSuffix(r.URL.Path, "/setWebhook"):
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

func TestManagerStartBlockedObservationAndShutdownDeadline(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/getMe"):
			_, _ = w.Write([]byte(`{"ok":true,"result":{"id":123,"username":"bot"}}`))
		case strings.HasSuffix(r.URL.Path, "/setWebhook"):
			_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
		default:
			_, _ = w.Write([]byte(`{"ok":true,"result":{"url":"https://vane.test/telegram/webhook","max_connections":1,"allowed_updates":["message"]}}`))
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
