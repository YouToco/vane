package telegram

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/agent"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

const (
	webhookSecretHeader = "X-Telegram-Bot-Api-Secret-Token"
	webhookBodyLimit    = 1 << 20
	linkTTL             = 10 * time.Minute
	workerLease         = 3 * time.Minute
	workerPoll          = 2 * time.Second
	agentBudget         = 2 * time.Minute
	sendBudget          = 15 * time.Second
	maxMessageRunes     = 4096
)

var telegramTurnNamespace = uuid.MustParse("24f09e0f-c35c-5d84-884e-31b6d44cf610")

type Config struct {
	Enabled       bool
	Token         string
	WebhookSecret string
	WebhookURL    string
	APIBaseURL    string
	Workers       int
}

type Status struct {
	Enabled            bool       `json:"enabled"`
	Ready              bool       `json:"ready"`
	BotID              int64      `json:"bot_id,omitempty"`
	BotUsername        string     `json:"bot_username,omitempty"`
	WebhookURL         string     `json:"webhook_url,omitempty"`
	PendingUpdateCount int        `json:"pending_update_count,omitempty"`
	LastErrorCode      string     `json:"last_error_code,omitempty"`
	BlockedReplyCount  int        `json:"blocked_reply_count,omitempty"`
	OldestBlockedAt    *time.Time `json:"oldest_blocked_at,omitempty"`
}

type Link struct {
	DeepLink  string    `json:"deep_link"`
	ExpiresAt time.Time `json:"expires_at"`
}

type IngressStore interface {
	IssueTelegramLinkRequest(context.Context, int64, int64, string, []byte, time.Time) error
	ConsumeTelegramLinkRequest(context.Context, []byte, string, string, string, string) (store.ChannelIdentity, bool, error)
	ResolveTelegramIdentity(context.Context, string, string, string) (store.ChannelIdentity, error)
	GetTelegramIdentityForUser(context.Context, int64, int64, string) (store.ChannelIdentity, error)
	RevokeTelegramIdentity(context.Context, int64, int64, string) error
	AcceptTelegramIngress(context.Context, store.ChannelIdentity, string, string, string, string) (bool, error)
	ClaimNextTelegramIngress(context.Context, string, time.Duration) (store.ChannelIngress, error)
	MarkTelegramIngressReplyReady(context.Context, store.ChannelIngress, string) error
	MarkTelegramIngressFailed(context.Context, store.ChannelIngress, string) error
	ClaimTelegramReplySend(context.Context, string, string, string) (store.ChannelIngress, error)
	ClaimNextTelegramReplySend(context.Context, string) (store.ChannelIngress, error)
	CompleteTelegramReply(context.Context, store.ChannelIngress, []string) error
	MarkTelegramReplyRejected(context.Context, store.ChannelIngress, string) error
	MarkTelegramReplyAmbiguous(context.Context, store.ChannelIngress, []string, string) error
	TelegramBlockedReplyStats(context.Context, string) (store.ChannelDeliveryBlockStats, error)
	TelegramBlockedReplyStatsForUser(context.Context, string, int64, int64) (store.ChannelDeliveryBlockStats, error)
}

type ChannelAgent interface {
	HandleChannelMessage(context.Context, int64, string, string) (agent.Outcome, error)
}

type Manager struct {
	cfg    Config
	store  IngressStore
	agent  ChannelAgent
	client *Client
	logger *slog.Logger

	mu     sync.RWMutex
	status Status
	bot    Bot

	runCtx context.Context
	cancel context.CancelFunc
	wake   chan struct{}
	wg     sync.WaitGroup
}

func NewManager(
	cfg Config,
	st IngressStore,
	agentLoop ChannelAgent,
	httpClient *http.Client,
	logger *slog.Logger,
) (*Manager, error) {
	if !cfg.Enabled {
		return &Manager{cfg: cfg, store: st, agent: agentLoop,
			logger: logger, status: Status{Enabled: false}}, nil
	}
	if st == nil || agentLoop == nil || cfg.WebhookSecret == "" ||
		cfg.WebhookURL == "" {
		return nil, errors.New("telegram: enabled adapter dependencies are incomplete")
	}
	client, err := NewClient(cfg.Token, cfg.APIBaseURL, httpClient)
	if err != nil {
		return nil, err
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.Workers != 1 {
		return nil, errors.New("telegram: ingress v1 requires exactly one worker")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		cfg: cfg, store: st, agent: agentLoop, client: client, logger: logger,
		status: Status{Enabled: true, WebhookURL: cfg.WebhookURL},
		wake:   make(chan struct{}, 1),
	}, nil
}

// Start verifies the token, installs the exact webhook with the minimal update
// set, verifies provider state, then opens background processing admission.
func (m *Manager) Start(ctx context.Context) error {
	if !m.cfg.Enabled {
		return nil
	}
	startupCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	bot, err := m.client.GetMe(startupCtx)
	if err != nil {
		m.setError("get_me")
		return fmt.Errorf("telegram: verify bot identity: %w", err)
	}
	if err := m.client.SetWebhook(
		startupCtx, m.cfg.WebhookURL, m.cfg.WebhookSecret); err != nil {
		m.setError("set_webhook")
		return fmt.Errorf("telegram: install webhook: %w", err)
	}
	info, err := m.client.GetWebhookInfo(startupCtx)
	if err != nil {
		m.setError("get_webhook_info")
		return fmt.Errorf("telegram: verify webhook: %w", err)
	}
	// last_error_message describes the most recent delivery failure, not the
	// current webhook installation. Telegram may attempt a pending update in
	// the short interval before this process starts listening, so treating that
	// historical field as startup authority can create a permanent restart
	// loop. The exact provider URL is the authoritative installation check;
	// /readyz starts serving immediately after run() finishes wiring the mux.
	if info.URL != m.cfg.WebhookURL || info.MaxConnections != 1 ||
		len(info.AllowedUpdates) != 1 || info.AllowedUpdates[0] != "message" {
		m.setError("webhook_state_mismatch")
		return errors.New("telegram: provider webhook state is not ready")
	}
	blocked, err := m.store.TelegramBlockedReplyStats(
		startupCtx, strconv.FormatInt(bot.ID, 10))
	if err != nil {
		m.setError("blocked_reply_observation")
		return fmt.Errorf("telegram: inspect blocked replies: %w", err)
	}

	m.mu.Lock()
	m.bot = bot
	m.status.Ready = true
	m.status.BotID = bot.ID
	m.status.BotUsername = bot.Username
	m.status.PendingUpdateCount = info.PendingUpdateCount
	m.status.LastErrorCode = ""
	m.status.BlockedReplyCount = blocked.Count
	m.status.OldestBlockedAt = blocked.OldestAt
	m.runCtx, m.cancel = context.WithCancel(ctx)
	m.mu.Unlock()
	if blocked.Count > 0 {
		m.logger.Error("telegram: provider-crossed replies require operator review",
			"blocked_count", blocked.Count, "oldest_at", blocked.OldestAt)
	}
	for range m.cfg.Workers {
		m.wg.Add(1)
		go m.runWorker()
	}
	m.signal()
	return nil
}

func (m *Manager) Shutdown(ctx context.Context) error {
	if !m.cfg.Enabled {
		return nil
	}
	m.mu.Lock()
	if m.cancel != nil {
		m.cancel()
	}
	m.status.Ready = false
	m.mu.Unlock()
	done := make(chan struct{})
	go func() { m.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) Status() Status {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.status
}

// Ping joins the process readiness contract only when Telegram is enabled.
// Startup does not become ready until getMe, setWebhook and getWebhookInfo have
// all succeeded, so deployment cannot mistake a listening HTTP socket for a
// usable Bot ingress.
func (m *Manager) Ping(ctx context.Context) error {
	status := m.Status()
	if status.Enabled && !status.Ready {
		return errors.New("telegram adapter is not ready")
	}
	if status.Enabled {
		bot, ready := m.botIdentity()
		if !ready {
			return errors.New("telegram adapter is not ready")
		}
		stats, err := m.store.TelegramBlockedReplyStats(
			ctx, strconv.FormatInt(bot.ID, 10))
		if err != nil {
			return err
		}
		m.mu.Lock()
		m.status.BlockedReplyCount = stats.Count
		m.status.OldestBlockedAt = stats.OldestAt
		m.mu.Unlock()
	}
	return nil
}

func (m *Manager) setError(code string) {
	m.mu.Lock()
	m.status.Ready = false
	m.status.LastErrorCode = code
	m.mu.Unlock()
}

func (m *Manager) observeProviderError(err error) {
	var deliveryErr *DeliveryError
	// Telegram 401 proves that this bot credential is rejected. A 403 is only
	// recipient/chat scoped (for example, the user blocked the bot), so it must
	// remain a terminal failure for that delivery without taking the adapter
	// globally offline.
	if errors.As(err, &deliveryErr) &&
		deliveryErr.HTTPStatus == http.StatusUnauthorized {
		m.setError("provider_auth_rejected")
	}
}

func (m *Manager) botIdentity() (Bot, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.bot, m.status.Ready
}

func (m *Manager) Handler() http.Handler {
	return http.HandlerFunc(m.handleWebhook)
}

func (m *Manager) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if !m.cfg.Enabled || r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	provided := r.Header.Get(webhookSecretHeader)
	if len(provided) != len(m.cfg.WebhookSecret) ||
		subtle.ConstantTimeCompare(
			[]byte(provided), []byte(m.cfg.WebhookSecret)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	bot, ready := m.botIdentity()
	if !ready {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, webhookBodyLimit)
	decoder := json.NewDecoder(r.Body)
	var update Update
	if err := decoder.Decode(&update); err != nil {
		http.Error(w, "invalid update", http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		http.Error(w, "invalid update", http.StatusBadRequest)
		return
	}
	if update.UpdateID < 0 || update.Message == nil ||
		update.Message.From == nil || update.Message.Chat.Type != "private" ||
		update.Message.Chat.ID != update.Message.From.ID ||
		strings.TrimSpace(update.Message.Text) == "" ||
		len(update.Message.Text) > 65536 {
		w.WriteHeader(http.StatusOK)
		return
	}
	actorID := strconv.FormatInt(update.Message.From.ID, 10)
	chatID := strconv.FormatInt(update.Message.Chat.ID, 10)
	appIdentity := strconv.FormatInt(bot.ID, 10)
	text := update.Message.Text
	if token, ok := startToken(text); ok {
		if err := m.consumeLink(r.Context(), bot, actorID, chatID, token); err != nil {
			if !errors.Is(err, types.ErrNotFound) &&
				!errors.Is(err, types.ErrConflict) &&
				!errors.Is(err, types.ErrValidation) {
				http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	identity, err := m.store.ResolveTelegramIdentity(
		r.Context(), appIdentity, actorID, chatID)
	if err != nil {
		// Unknown actors are ACKed without a model, Tool, Temporal or business
		// write. Static rejection replies would make the public bot a spam relay.
		if errors.Is(err, types.ErrNotFound) {
			w.WriteHeader(http.StatusOK)
			return
		}
		// Provider retries non-2xx updates. A database or context failure is not
		// evidence that the actor is unbound and must never become message loss.
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	digest := semanticUpdateDigest(update.UpdateID, actorID, chatID, text)
	turnID := stableTelegramTurnID(bot.ID, update.UpdateID)
	_, err = m.store.AcceptTelegramIngress(
		r.Context(), identity, strconv.FormatInt(update.UpdateID, 10),
		digest, text, turnID)
	if err != nil {
		if errors.Is(err, types.ErrConflict) {
			http.Error(w, "conflicting update", http.StatusConflict)
			return
		}
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	m.signal()
	w.WriteHeader(http.StatusOK)
}

func startToken(text string) (string, bool) {
	parts := strings.Fields(text)
	if len(parts) != 2 || parts[0] != "/start" || len(parts[1]) > 64 {
		return "", false
	}
	return parts[1], true
}

func (m *Manager) consumeLink(
	ctx context.Context, bot Bot, actorID, chatID, token string,
) error {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) != 32 {
		return types.NewAppError(types.CodeValidation,
			"Telegram 配对码格式无效", types.ErrValidation)
	}
	hash := sha256.Sum256(raw)
	digest := sha256.Sum256([]byte(strings.Join(
		[]string{strconv.FormatInt(bot.ID, 10), actorID, chatID}, "\x00")))
	_, firstConsumption, err := m.store.ConsumeTelegramLinkRequest(
		ctx, hash[:], strconv.FormatInt(bot.ID, 10), actorID, chatID,
		hex.EncodeToString(digest[:]))
	if err != nil {
		return err
	}
	if !firstConsumption {
		return nil
	}
	sendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sendBudget)
	defer cancel()
	if _, err := m.client.SendMessage(sendCtx, chatID,
		"Telegram 已与 Vane 账号安全绑定。现在可以直接向我查询或管理任务。"); err != nil {
		m.observeProviderError(err)
		m.logger.Warn("telegram: pairing confirmation not proven delivered",
			"error_code", sanitizeDeliveryCode(err))
	}
	return nil
}

func semanticUpdateDigest(updateID int64, actorID, chatID, text string) string {
	payload := strings.Join([]string{
		strconv.FormatInt(updateID, 10), actorID, chatID, text,
	}, "\x00")
	digest := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(digest[:])
}

func stableTelegramTurnID(botID, updateID int64) string {
	key := fmt.Sprintf("telegram:v1:%d:%d", botID, updateID)
	return uuid.NewSHA1(telegramTurnNamespace, []byte(key)).String()
}

func (m *Manager) signal() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *Manager) runWorker() {
	defer m.wg.Done()
	ticker := time.NewTicker(workerPoll)
	defer ticker.Stop()
	for {
		select {
		case <-m.runCtx.Done():
			return
		case <-m.wake:
		case <-ticker.C:
		}
		for {
			progress := m.processReadyReply(m.runCtx)
			if m.processOne(m.runCtx) {
				progress = true
			}
			if !progress {
				break
			}
		}
	}
}

func (m *Manager) processReadyReply(ctx context.Context) bool {
	bot, ready := m.botIdentity()
	if !ready {
		return false
	}
	item, err := m.store.ClaimNextTelegramReplySend(
		ctx, strconv.FormatInt(bot.ID, 10))
	if errors.Is(err, types.ErrNotFound) {
		return false
	}
	if err != nil {
		m.logger.Error("telegram: recover ready reply failed",
			"error_code", types.CodeOf(err))
		return false
	}
	m.deliverClaimedReply(ctx, item)
	return true
}

func (m *Manager) processOne(ctx context.Context) bool {
	bot, ready := m.botIdentity()
	if !ready {
		return false
	}
	item, err := m.store.ClaimNextTelegramIngress(
		ctx, strconv.FormatInt(bot.ID, 10), workerLease)
	if errors.Is(err, types.ErrNotFound) {
		return false
	}
	if err != nil {
		m.logger.Error("telegram: claim ingress failed", "error_code", types.CodeOf(err))
		return false
	}
	agentCtx, cancelAgent := context.WithTimeout(ctx, agentBudget)
	outcome, agentErr := m.agent.HandleChannelMessage(
		agentCtx, item.UserID, item.StableTurnID, item.InputText)
	cancelAgent()
	reply := outcome.Reply
	if agentErr != nil || strings.TrimSpace(outcome.Reply) == "" {
		// The webhook was already durably accepted, so a terminal silent failure
		// would strand the user. Persist a content-free, actionable reply through
		// the same outbox; it does not claim whether a prior Tool committed.
		reply = "这次处理未能确认完成。请先在 Vane 网页检查任务状态，再决定是否重试。"
	}
	if err := m.store.MarkTelegramIngressReplyReady(
		ctx, item, reply); err != nil {
		m.logger.Error("telegram: persist reply failed", "error_code", types.CodeOf(err))
		return true
	}
	sending, err := m.store.ClaimTelegramReplySend(
		ctx, item.Provider, item.AppIdentity, item.ProviderUpdateID)
	if err != nil {
		m.logger.Error("telegram: claim send failed", "error_code", types.CodeOf(err))
		return true
	}
	m.deliverClaimedReply(ctx, sending)
	return true
}

func (m *Manager) deliverClaimedReply(
	ctx context.Context, sending store.ChannelIngress,
) {
	chunks := SplitMessage(sending.ReplyText)
	messageIDs := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		sendCtx, cancelSend := context.WithTimeout(ctx, sendBudget)
		messageID, sendErr := m.client.SendMessage(
			sendCtx, sending.ProviderChatID, chunk)
		cancelSend()
		if sendErr != nil {
			// A provider-declared 4xx before any chunk is definite failure, not
			// ambiguity. Once a prior chunk exists, even a later explicit reject is
			// partial delivery and remains blocked from automatic retry.
			var deliveryErr *DeliveryError
			if len(messageIDs) == 0 && errors.As(sendErr, &deliveryErr) &&
				deliveryErr.DefinitelyNotSent {
				m.observeProviderError(sendErr)
				if err := m.store.MarkTelegramReplyRejected(
					ctx, sending, sanitizeDeliveryCode(sendErr)); err != nil {
					m.logger.Error("telegram: persist provider rejection failed",
						"error_code", types.CodeOf(err))
				} else {
					m.logger.Warn("telegram: provider definitely rejected reply",
						"error_code", sanitizeDeliveryCode(sendErr))
				}
				return
			}
			if err := m.store.MarkTelegramReplyAmbiguous(
				ctx, sending, messageIDs, sanitizeDeliveryCode(sendErr)); err != nil {
				m.logger.Error("telegram: persist ambiguous reply failed",
					"error_code", types.CodeOf(err))
			}
			return
		}
		messageIDs = append(messageIDs, messageID)
	}
	if err := m.store.CompleteTelegramReply(ctx, sending, messageIDs); err != nil {
		// Provider success with lost local settlement is intentionally left in
		// sending. Recovery never retries that state.
		m.logger.Error("telegram: reply sent but settlement failed",
			"error_code", types.CodeOf(err))
	}
}

// SplitMessage splits on rune boundaries and preserves exact order. Telegram's
// sendMessage limit is 4096 characters; plain text avoids Markdown/HTML drift.
func SplitMessage(text string) []string {
	if text == "" {
		return nil
	}
	if utf8.RuneCountInString(text) <= maxMessageRunes {
		return []string{text}
	}
	runes := []rune(text)
	chunks := make([]string, 0, (len(runes)+maxMessageRunes-1)/maxMessageRunes)
	for len(runes) > 0 {
		n := min(len(runes), maxMessageRunes)
		chunks = append(chunks, string(runes[:n]))
		runes = runes[n:]
	}
	return chunks
}

func (m *Manager) IssueLink(
	ctx context.Context, tenantID, userID int64,
) (Link, error) {
	bot, ready := m.botIdentity()
	if !ready {
		return Link{}, types.NewAppError(types.CodeConflict,
			"Telegram Bot 尚未就绪", types.ErrConflict)
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return Link{}, types.NewAppError(types.CodeInternal,
			"生成 Telegram 配对码", err)
	}
	hash := sha256.Sum256(raw)
	expiresAt := time.Now().Add(linkTTL)
	if err := m.store.IssueTelegramLinkRequest(
		ctx, tenantID, userID, strconv.FormatInt(bot.ID, 10),
		hash[:], expiresAt); err != nil {
		return Link{}, err
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	return Link{
		DeepLink:  "https://t.me/" + bot.Username + "?start=" + token,
		ExpiresAt: expiresAt,
	}, nil
}

func (m *Manager) Binding(
	ctx context.Context, tenantID, userID int64,
) (store.ChannelIdentity, error) {
	bot, ready := m.botIdentity()
	if !ready {
		return store.ChannelIdentity{}, types.NewAppError(types.CodeConflict,
			"Telegram Bot 尚未就绪", types.ErrConflict)
	}
	return m.store.GetTelegramIdentityForUser(
		ctx, tenantID, userID, strconv.FormatInt(bot.ID, 10))
}

func (m *Manager) BlockedReplies(
	ctx context.Context, tenantID, userID int64,
) (store.ChannelDeliveryBlockStats, error) {
	m.mu.RLock()
	botID := m.bot.ID
	m.mu.RUnlock()
	if botID <= 0 {
		return store.ChannelDeliveryBlockStats{}, nil
	}
	return m.store.TelegramBlockedReplyStatsForUser(
		ctx, strconv.FormatInt(botID, 10), tenantID, userID)
}

func (m *Manager) Unlink(ctx context.Context, tenantID, userID int64) error {
	bot, ready := m.botIdentity()
	if !ready {
		return types.NewAppError(types.CodeConflict,
			"Telegram Bot 尚未就绪", types.ErrConflict)
	}
	return m.store.RevokeTelegramIdentity(
		ctx, tenantID, userID, strconv.FormatInt(bot.ID, 10))
}

func (m *Manager) SendTest(
	ctx context.Context, tenantID, userID int64,
) error {
	identity, err := m.Binding(ctx, tenantID, userID)
	if err != nil {
		return err
	}
	sendCtx, cancel := context.WithTimeout(ctx, sendBudget)
	defer cancel()
	_, err = m.client.SendMessage(sendCtx, identity.ProviderChatID,
		"Vane Telegram 连接测试成功。")
	if err != nil {
		m.observeProviderError(err)
		return types.NewAppError(types.CodePushFailed,
			"Telegram 测试消息未确认送达", nil)
	}
	return nil
}
