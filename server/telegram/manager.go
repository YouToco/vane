package telegram

import (
	"context"
	"crypto/hmac"
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
	maxRateLimitRetries = 5
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
	Command   string    `json:"command,omitempty"`
	ExpiresAt time.Time `json:"expires_at"`
}

type RouteSummary struct {
	ID       int64     `json:"id"`
	Kind     string    `json:"kind"`
	ChatType string    `json:"chat_type"`
	BoundAt  time.Time `json:"bound_at"`
}

type IngressStore interface {
	IssueTelegramLinkRequest(context.Context, int64, int64, string, []byte, time.Time) error
	ConsumeTelegramLinkRequest(context.Context, []byte, string, string, string, string) (store.ChannelIdentity, bool, error)
	ResolveTelegramIdentity(context.Context, string, string, string) (store.ChannelIdentity, error)
	GetTelegramIdentityForUser(context.Context, int64, int64, string) (store.ChannelIdentity, error)
	RevokeTelegramIdentity(context.Context, int64, int64, string) error
	AcceptTelegramIngress(context.Context, store.ChannelIdentity, string, string, string, string) (bool, error)
	IssueTelegramRouteLinkRequest(context.Context, int64, int64, string, []byte, time.Time) error
	ConsumeTelegramRouteLinkRequest(context.Context, []byte, string, string, string, string, string, string) (store.ChannelRoute, bool, error)
	ResolveTelegramRoute(context.Context, string, string, string, string) (store.ChannelIdentity, store.ChannelRoute, error)
	AcceptTelegramRoutedIngress(context.Context, store.ChannelIdentity, store.ChannelRoute, string, string, string, string, string, string, string, []byte) (bool, error)
	ListTelegramRoutesForUser(context.Context, int64, int64, string) ([]store.ChannelRoute, error)
	RevokeTelegramRoute(context.Context, int64, int64, int64, string) error
	MigrateTelegramRoutes(context.Context, string, string, string) error
	InvalidateTelegramDestination(context.Context, string, string, string, string) error
	PrepareTelegramOutbound(context.Context, int64, int64, int64, string, string, string) (store.ChannelOutboundEffect, error)
	ClaimTelegramOutbound(context.Context, string) (store.ChannelOutboundEffect, error)
	CompleteTelegramOutbound(context.Context, store.ChannelOutboundEffect, []string) error
	MarkTelegramOutboundRejected(context.Context, store.ChannelOutboundEffect, string) error
	DeferTelegramOutbound(context.Context, store.ChannelOutboundEffect, time.Duration, int) (bool, error)
	MarkTelegramOutboundAmbiguous(context.Context, store.ChannelOutboundEffect, []string, string) error
	ClaimNextTelegramIngress(context.Context, string, time.Duration) (store.ChannelIngress, error)
	MarkTelegramIngressReplyReady(context.Context, store.ChannelIngress, string) error
	MarkTelegramIngressFailed(context.Context, store.ChannelIngress, string) error
	ClaimTelegramReplySend(context.Context, string, string, string) (store.ChannelIngress, error)
	ClaimNextTelegramReplySend(context.Context, string) (store.ChannelIngress, error)
	CompleteTelegramReply(context.Context, store.ChannelIngress, []string) error
	MarkTelegramReplyRejected(context.Context, store.ChannelIngress, string) error
	DeferTelegramReply(context.Context, store.ChannelIngress, time.Duration, int) (bool, error)
	MarkTelegramReplyAmbiguous(context.Context, store.ChannelIngress, []string, string) error
	TelegramBlockedReplyStats(context.Context, string) (store.ChannelDeliveryBlockStats, error)
	TelegramBlockedReplyStatsForUser(context.Context, string, int64, int64) (store.ChannelDeliveryBlockStats, error)
}

type ChannelAgent interface {
	HandleChannelMessage(context.Context, int64, string, string, string) (agent.Outcome, error)
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
	commands := []BotCommand{
		{Command: "help", Description: "查看 Vane Bot 使用说明"},
		{Command: "status", Description: "查看当前连接状态"},
		{Command: "tasks", Description: "列出我的情报任务"},
		{Command: "new", Description: "用自然语言创建任务"},
		{Command: "connect", Description: "连接当前群组或话题"},
	}
	if err := m.client.SetCommands(startupCtx, commands); err != nil {
		m.setError("set_commands")
		return fmt.Errorf("telegram: install command menu: %w", err)
	}
	if err := m.client.SetCommandsMenu(startupCtx); err != nil {
		m.setError("set_menu")
		return fmt.Errorf("telegram: install command menu button: %w", err)
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
		len(info.AllowedUpdates) != 3 || info.AllowedUpdates[0] != "message" ||
		info.AllowedUpdates[1] != "callback_query" ||
		info.AllowedUpdates[2] != "my_chat_member" {
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
	if update.UpdateID < 0 {
		w.WriteHeader(http.StatusOK)
		return
	}
	if handled, err := m.handleLifecycleUpdate(r.Context(), bot, update); handled {
		if err != nil {
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	inbound, ok := m.normalizeUpdate(bot, update)
	if !ok {
		w.WriteHeader(http.StatusOK)
		return
	}
	actorID := strconv.FormatInt(inbound.ActorID, 10)
	chatID := strconv.FormatInt(inbound.ChatID, 10)
	threadID := strconv.FormatInt(inbound.ThreadID, 10)
	appIdentity := strconv.FormatInt(bot.ID, 10)
	text := inbound.Text
	if inbound.ChatType == "private" {
		if token, start := startToken(text, bot.Username); start {
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
	} else if token, connect := routeToken(text, bot.Username); connect {
		if err := m.consumeRouteLink(r.Context(), bot, inbound, token); err != nil {
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
	identity, route, err := m.store.ResolveTelegramRoute(
		r.Context(), appIdentity, actorID, chatID, threadID)
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
	digest := semanticUpdateDigest(update.UpdateID, actorID, chatID, threadID,
		inbound.MessageID, inbound.Kind, inbound.CallbackID, text,
		inbound.MediaEnvelope)
	turnID := stableTelegramTurnID(bot.ID, update.UpdateID)
	_, err = m.store.AcceptTelegramRoutedIngress(
		r.Context(), identity, route, strconv.FormatInt(update.UpdateID, 10),
		digest, text, turnID, inbound.MessageID, inbound.Kind,
		inbound.CallbackID, inbound.MediaEnvelope)
	if err != nil {
		if errors.Is(err, types.ErrConflict) {
			http.Error(w, "conflicting update", http.StatusConflict)
			return
		}
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	if inbound.CallbackID != "" {
		ackCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 3*time.Second)
		if err := m.client.AnswerCallbackQuery(ackCtx, inbound.CallbackID, "Vane 已接收"); err != nil {
			m.logger.Warn("telegram: callback acknowledgement not proven",
				"error_code", sanitizeDeliveryCode(err))
		}
		cancel()
	}
	m.signal()
	w.WriteHeader(http.StatusOK)
}

func (m *Manager) handleLifecycleUpdate(
	ctx context.Context, bot Bot, update Update,
) (bool, error) {
	appIdentity := strconv.FormatInt(bot.ID, 10)
	if membership := update.MyChatMember; membership != nil {
		if membership.Chat.ID == 0 || membership.NewChatMember.User.ID != bot.ID ||
			!membership.NewChatMember.User.IsBot {
			return true, nil
		}
		if membership.NewChatMember.Status != "left" &&
			membership.NewChatMember.Status != "kicked" {
			return true, nil
		}
		return true, m.store.InvalidateTelegramDestination(
			ctx, appIdentity, strconv.FormatInt(membership.Chat.ID, 10), "",
			"bot_membership_lost")
	}
	message := update.Message
	if message == nil || message.Chat.ID == 0 {
		return false, nil
	}
	oldChatID, newChatID := message.Chat.ID, message.MigrateToChatID
	if message.MigrateFromChatID != 0 {
		oldChatID, newChatID = message.MigrateFromChatID, message.Chat.ID
	}
	if newChatID != 0 {
		if oldChatID == 0 || oldChatID == newChatID {
			return true, nil
		}
		return true, m.store.MigrateTelegramRoutes(ctx, appIdentity,
			strconv.FormatInt(oldChatID, 10), strconv.FormatInt(newChatID, 10))
	}
	if (message.ForumTopicClosed != nil ||
		message.GeneralForumTopicHidden != nil) && message.MessageThreadID > 0 {
		return true, m.store.InvalidateTelegramDestination(
			ctx, appIdentity, strconv.FormatInt(message.Chat.ID, 10),
			strconv.FormatInt(message.MessageThreadID, 10), "topic_closed")
	}
	return false, nil
}

type normalizedUpdate struct {
	ActorID, ChatID, ThreadID int64
	MessageID                 string
	ChatType                  string
	Text                      string
	Kind                      string
	CallbackID                string
	MediaEnvelope             []byte
}

func (m *Manager) normalizeUpdate(bot Bot, update Update) (normalizedUpdate, bool) {
	if update.CallbackQuery != nil {
		query := update.CallbackQuery
		if query.Message == nil || query.From.ID <= 0 || query.ID == "" ||
			len(query.ID) > 128 || query.Message.Chat.ID == 0 {
			return normalizedUpdate{}, false
		}
		action, ok := m.verifyCallback(bot.ID, query.Data)
		if !ok {
			return normalizedUpdate{}, false
		}
		text := callbackPrompt(action)
		return normalizedUpdate{
			ActorID: query.From.ID, ChatID: query.Message.Chat.ID,
			ThreadID:  query.Message.MessageThreadID,
			MessageID: strconv.FormatInt(query.Message.MessageID, 10),
			ChatType:  query.Message.Chat.Type, Text: text, Kind: "callback",
			CallbackID: query.ID,
		}, true
	}
	message := update.Message
	if message == nil || message.From == nil || message.From.ID <= 0 ||
		message.From.IsBot || message.Chat.ID == 0 || message.MessageID <= 0 {
		return normalizedUpdate{}, false
	}
	if message.Chat.Type != "private" && message.Chat.Type != "group" &&
		message.Chat.Type != "supergroup" {
		return normalizedUpdate{}, false
	}
	if message.Chat.Type == "private" && message.Chat.ID != message.From.ID {
		return normalizedUpdate{}, false
	}
	hasMedia := telegramMessageHasMedia(message)
	text := strings.TrimSpace(message.Text)
	if hasMedia || text == "" {
		// Telegram media semantics use Caption. If a malformed provider payload
		// carries both text and media, do not let the text bypass the durable
		// media path or diverge from what Telegram clients display as caption.
		text = strings.TrimSpace(message.Caption)
	}
	if (text == "" && !hasMedia) || len(text) > 65536 {
		return normalizedUpdate{}, false
	}
	kind := "message"
	command, args, isCommand := parseCommand(text, bot.Username)
	if message.Chat.Type != "private" && !isCommand &&
		!replyTargetsBot(message, bot.ID) {
		var mentioned bool
		text, mentioned = stripBotMention(text, bot.Username)
		if !mentioned {
			return normalizedUpdate{}, false
		}
	}
	var mediaEnvelope []byte
	if hasMedia {
		var err error
		mediaEnvelope, err = telegramMediaEnvelope(message, text)
		if err != nil {
			return normalizedUpdate{}, false
		}
		// The durable envelope preserves the caption and provider file authority,
		// but the current worker intentionally remains text-only. A future media
		// processor may consume it only through an exact native same-modality
		// capability; conversion fallbacks are forbidden.
		text = "telegram:media-help"
	} else if isCommand {
		kind = "command"
		if command != "start" && command != "connect" {
			text = commandPrompt(command, args)
			if text == "" {
				text = "telegram:unknown-command"
			}
		}
	}
	threadID := message.MessageThreadID
	if threadID < 0 {
		return normalizedUpdate{}, false
	}
	return normalizedUpdate{
		ActorID: message.From.ID, ChatID: message.Chat.ID, ThreadID: threadID,
		MessageID: strconv.FormatInt(message.MessageID, 10),
		ChatType:  message.Chat.Type, Text: text, Kind: kind,
		MediaEnvelope: mediaEnvelope,
	}, true
}

func telegramMessageHasMedia(message *Message) bool {
	return len(message.Photo) > 0 || message.Document != nil ||
		message.Audio != nil || message.Video != nil || message.Voice != nil ||
		message.Animation != nil || message.VideoNote != nil || message.Sticker != nil
}

func telegramMediaEnvelope(message *Message, caption string) ([]byte, error) {
	var kind string
	var file *FileRef
	// Telegram sets document as a compatibility duplicate for animation. Pick
	// one semantic item so a future worker cannot download the same bytes twice.
	switch {
	case message.Animation != nil:
		kind, file = "animation", message.Animation
	case len(message.Photo) > 0:
		kind = "image"
		file = largestTelegramPhoto(message.Photo)
	case message.Video != nil:
		kind, file = "video", message.Video
	case message.VideoNote != nil:
		kind, file = "video_note", message.VideoNote
	case message.Voice != nil:
		kind, file = "voice", message.Voice
	case message.Audio != nil:
		kind, file = "audio", message.Audio
	case message.Document != nil:
		kind, file = "document", message.Document
	case message.Sticker != nil:
		kind, file = "sticker", message.Sticker
	default:
		return nil, types.NewAppError(
			types.CodeValidation, "Telegram 媒体类型无效", types.ErrValidation)
	}
	width, height := file.Width, file.Height
	if kind == "video_note" && file.Length > 0 {
		width, height = file.Length, file.Length
	}
	envelope := types.ChannelMessageEnvelopeV1{
		Schema:       types.ChannelMessageEnvelopeV1Schema,
		Caption:      caption,
		MediaGroupID: strings.TrimSpace(message.MediaGroupID),
		Items: []types.ChannelMessageMediaItemV1{{
			Kind: kind, ProviderFileID: file.FileID,
			ProviderUniqueID: file.FileUniqueID, MIMEType: file.MIMEType,
			FileName: file.FileName, SizeBytes: file.FileSize,
			Width: width, Height: height, DurationSeconds: file.Duration,
		}},
	}
	return types.MarshalChannelMessageEnvelopeV1(envelope)
}

func largestTelegramPhoto(photos []FileRef) *FileRef {
	best := &photos[0]
	for index := 1; index < len(photos); index++ {
		candidate := &photos[index]
		if candidate.Width > best.Width ||
			(candidate.Width == best.Width && candidate.Height > best.Height) ||
			(candidate.Width == best.Width && candidate.Height == best.Height &&
				candidate.FileSize > best.FileSize) {
			best = candidate
		}
	}
	return best
}

func parseCommand(text, username string) (string, string, bool) {
	parts := strings.Fields(text)
	if len(parts) == 0 || !strings.HasPrefix(parts[0], "/") {
		return "", "", false
	}
	command := strings.TrimPrefix(parts[0], "/")
	if at := strings.IndexByte(command, '@'); at >= 0 {
		if !strings.EqualFold(command[at+1:], username) {
			return "", "", false
		}
		command = command[:at]
	}
	args := strings.TrimSpace(strings.TrimPrefix(text, parts[0]))
	return strings.ToLower(command), args, true
}

func startToken(text, username string) (string, bool) {
	command, args, ok := parseCommand(text, username)
	if !ok || command != "start" || strings.Contains(args, " ") ||
		len(args) == 0 || len(args) > 64 {
		return "", false
	}
	return args, true
}

func routeToken(text, username string) (string, bool) {
	command, args, ok := parseCommand(text, username)
	if !ok || (command != "connect" && command != "start") ||
		strings.Contains(args, " ") || len(args) == 0 || len(args) > 64 {
		return "", false
	}
	return args, true
}

func replyTargetsBot(message *Message, botID int64) bool {
	return message.ReplyToMessage != nil && message.ReplyToMessage.From != nil &&
		message.ReplyToMessage.From.ID == botID
}

func stripBotMention(text, username string) (string, bool) {
	needle := "@" + strings.ToLower(username)
	lower := strings.ToLower(text)
	for offset := 0; offset <= len(lower)-len(needle); {
		index := strings.Index(lower[offset:], needle)
		if index < 0 {
			break
		}
		index += offset
		beforeOK := index == 0 || !telegramUsernameChar(lower[index-1])
		after := index + len(needle)
		afterOK := after == len(lower) || !telegramUsernameChar(lower[after])
		if beforeOK && afterOK {
			cleaned := strings.TrimSpace(text[:index] + " " + text[after:])
			return cleaned, cleaned != ""
		}
		offset = index + len(needle)
	}
	return text, false
}

func telegramUsernameChar(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= '0' && value <= '9' ||
		value == '_'
}

func commandPrompt(command, args string) string {
	switch command {
	case "help":
		return "telegram:help"
	case "status":
		return "telegram:status"
	case "tasks":
		return "列出我的情报任务"
	case "new":
		if strings.TrimSpace(args) == "" {
			return "telegram:new-help"
		}
		return "创建情报任务：" + strings.TrimSpace(args)
	default:
		return ""
	}
}

func callbackPrompt(action string) string {
	return commandPrompt(action, "")
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

func (m *Manager) consumeRouteLink(
	ctx context.Context, bot Bot, inbound normalizedUpdate, token string,
) error {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) != 32 {
		return types.NewAppError(types.CodeValidation,
			"Telegram 群组连接码格式无效", types.ErrValidation)
	}
	chatID := strconv.FormatInt(inbound.ChatID, 10)
	threadID := strconv.FormatInt(inbound.ThreadID, 10)
	memberCtx, cancelMember := context.WithTimeout(ctx, sendBudget)
	member, err := m.client.GetChatMember(memberCtx, chatID, inbound.ActorID)
	cancelMember()
	if err != nil {
		m.observeProviderError(err)
		return err
	}
	if member.Status != "creator" && member.Status != "administrator" {
		return types.NewAppError(types.CodeConflict,
			"只有 Telegram 群管理员可以连接 Vane", types.ErrConflict)
	}
	hash := sha256.Sum256(raw)
	digest := sha256.Sum256([]byte(strings.Join([]string{
		strconv.FormatInt(bot.ID, 10), strconv.FormatInt(inbound.ActorID, 10),
		chatID, threadID, inbound.ChatType,
	}, "\x00")))
	_, first, err := m.store.ConsumeTelegramRouteLinkRequest(
		ctx, hash[:], strconv.FormatInt(bot.ID, 10),
		strconv.FormatInt(inbound.ActorID, 10), chatID, threadID,
		inbound.ChatType, hex.EncodeToString(digest[:]))
	if err != nil || !first {
		return err
	}
	sendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sendBudget)
	defer cancel()
	_, err = m.client.SendMessageTo(sendCtx, chatID,
		"当前群组或话题已安全连接 Vane。只有完成绑定的 owner 发出的命令、@提及或对 Bot 的回复会进入 Agent。",
		SendMessageOptions{MessageThreadID: inbound.ThreadID})
	if err != nil {
		m.observeProviderError(err)
		m.logger.Warn("telegram: route confirmation not proven delivered",
			"error_code", sanitizeDeliveryCode(err))
	}
	return nil
}

func semanticUpdateDigest(
	updateID int64, actorID, chatID, threadID, messageID, kind,
	callbackID, text string, mediaEnvelope []byte,
) string {
	payload := strings.Join([]string{
		strconv.FormatInt(updateID, 10), actorID, chatID, threadID,
		messageID, kind, callbackID, text, string(mediaEnvelope),
	}, "\x00")
	digest := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(digest[:])
}

func (m *Manager) callbackData(botID int64, action string) string {
	body := "v1:" + action
	mac := hmac.New(sha256.New, []byte(m.cfg.WebhookSecret))
	_, _ = mac.Write([]byte(strconv.FormatInt(botID, 10) + "\x00" + body))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil)[:12])
	return body + ":" + signature
}

func (m *Manager) verifyCallback(botID int64, value string) (string, bool) {
	parts := strings.Split(value, ":")
	if len(parts) != 3 || parts[0] != "v1" || len(value) > 64 {
		return "", false
	}
	action := parts[1]
	if action != "help" && action != "status" && action != "tasks" && action != "new" {
		return "", false
	}
	expected := m.callbackData(botID, action)
	if len(expected) != len(value) ||
		subtle.ConstantTimeCompare([]byte(expected), []byte(value)) != 1 {
		return "", false
	}
	return action, true
}

func (m *Manager) commandKeyboard() *InlineKeyboardMarkup {
	bot, ready := m.botIdentity()
	if !ready {
		return nil
	}
	return &InlineKeyboardMarkup{InlineKeyboard: [][]InlineKeyboardButton{
		{{Text: "查看任务", CallbackData: m.callbackData(bot.ID, "tasks")},
			{Text: "连接状态", CallbackData: m.callbackData(bot.ID, "status")}},
		{{Text: "创建任务", CallbackData: m.callbackData(bot.ID, "new")},
			{Text: "帮助", CallbackData: m.callbackData(bot.ID, "help")}},
	}}
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
	reply, handled := staticCommandReply(item.InputText)
	var agentErr error
	if !handled {
		agentCtx, cancelAgent := context.WithTimeout(ctx, agentBudget)
		var outcome agent.Outcome
		outcome, agentErr = m.agent.HandleChannelMessage(
			agentCtx, item.UserID, channelConversationScope(item.RouteID),
			item.StableTurnID, item.InputText)
		cancelAgent()
		reply = outcome.Reply
	}
	if agentErr != nil || strings.TrimSpace(reply) == "" {
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

func channelConversationScope(routeID int64) string {
	return "channel-route:" + strconv.FormatInt(routeID, 10)
}

func staticCommandReply(input string) (string, bool) {
	switch input {
	case "telegram:help":
		return "Vane Telegram 助手\n\n/tasks 查看任务\n/new <描述> 创建任务\n/status 查看连接状态\n/connect <连接码> 在群组或话题中完成连接\n\n私聊可直接提问；群组中请使用命令、@提及 Bot，或回复 Bot 消息。", true
	case "telegram:status":
		return "当前 Telegram 身份与此聊天路由均已通过 Vane 授权。", true
	case "telegram:new-help":
		return "请在 /new 后直接描述任务，例如：/new 每周一整理 OpenAI 官方产品更新。", true
	case "telegram:unknown-command":
		return "暂不支持这个命令。发送 /help 查看可用功能。", true
	case "telegram:media-help":
		return "已收到媒体消息，但当前 Vane 尚未启用对应模态的原生理解，文件内容没有被下载或交给模型，也不会通过转写、抽帧、OCR 或描述生成绕行处理。请改用文字描述。媒体类型和文件引用已在你的绑定范围内安全记录，只用于后续原生能力升级，不会触发后台处理。", true
	default:
		return "", false
	}
}

func (m *Manager) deliverClaimedReply(
	ctx context.Context, sending store.ChannelIngress,
) {
	chunks := SplitMessage(sending.ReplyText)
	messageIDs := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		sendCtx, cancelSend := context.WithTimeout(ctx, sendBudget)
		options := SendMessageOptions{}
		if sending.ProviderThreadID != "" && sending.ProviderThreadID != "0" {
			options.MessageThreadID, _ = strconv.ParseInt(sending.ProviderThreadID, 10, 64)
		}
		if len(messageIDs) == 0 &&
			(strings.HasPrefix(sending.InputText, "telegram:") ||
				sending.IngressKind == "callback") {
			options.ReplyMarkup = m.commandKeyboard()
		}
		messageID, sendErr := m.client.SendMessageTo(
			sendCtx, sending.ProviderChatID, chunk, options)
		cancelSend()
		if sendErr != nil {
			// A provider-declared 4xx before any chunk is definite failure, not
			// ambiguity. Once a prior chunk exists, even a later explicit reject is
			// partial delivery and remains blocked from automatic retry.
			var deliveryErr *DeliveryError
			if len(messageIDs) == 0 && errors.As(sendErr, &deliveryErr) &&
				deliveryErr.DefinitelyNotSent &&
				deliveryErr.HTTPStatus == http.StatusTooManyRequests &&
				deliveryErr.RetryAfter > 0 {
				scheduled, deferErr := m.store.DeferTelegramReply(
					ctx, sending, deliveryErr.RetryAfter, maxRateLimitRetries)
				if deferErr != nil {
					m.logger.Error("telegram: persist rate-limit deferral failed",
						"error_code", types.CodeOf(deferErr))
				} else if scheduled {
					m.logger.Warn("telegram: reply rate limited; retry scheduled",
						"retry_after", deliveryErr.RetryAfter)
				} else {
					m.logger.Error("telegram: reply rate-limit retry budget exhausted")
				}
				return
			}
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

func (m *Manager) IssueRouteLink(
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
			"生成 Telegram 群组连接码", err)
	}
	hash := sha256.Sum256(raw)
	expiresAt := time.Now().Add(linkTTL)
	if err := m.store.IssueTelegramRouteLinkRequest(ctx, tenantID, userID,
		strconv.FormatInt(bot.ID, 10), hash[:], expiresAt); err != nil {
		return Link{}, err
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	return Link{
		DeepLink:  "https://t.me/" + bot.Username + "?startgroup=" + token,
		Command:   "/connect " + token,
		ExpiresAt: expiresAt,
	}, nil
}

func (m *Manager) Routes(
	ctx context.Context, tenantID, userID int64,
) ([]RouteSummary, error) {
	bot, ready := m.botIdentity()
	if !ready {
		return nil, types.NewAppError(types.CodeConflict,
			"Telegram Bot 尚未就绪", types.ErrConflict)
	}
	routes, err := m.store.ListTelegramRoutesForUser(
		ctx, tenantID, userID, strconv.FormatInt(bot.ID, 10))
	if err != nil {
		return nil, err
	}
	out := make([]RouteSummary, 0, len(routes))
	for _, route := range routes {
		out = append(out, RouteSummary{ID: route.ID, Kind: route.RouteKind,
			ChatType: route.ChatType, BoundAt: route.BoundAt})
	}
	return out, nil
}

func (m *Manager) UnlinkRoute(
	ctx context.Context, tenantID, userID, routeID int64,
) error {
	bot, ready := m.botIdentity()
	if !ready {
		return types.NewAppError(types.CodeConflict,
			"Telegram Bot 尚未就绪", types.ErrConflict)
	}
	return m.store.RevokeTelegramRoute(ctx, tenantID, userID, routeID,
		strconv.FormatInt(bot.ID, 10))
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
	bot, ready := m.botIdentity()
	if !ready {
		return types.NewAppError(types.CodeConflict,
			"Telegram Bot 尚未就绪", types.ErrConflict)
	}
	routes, err := m.store.ListTelegramRoutesForUser(
		ctx, tenantID, userID, strconv.FormatInt(bot.ID, 10))
	if err != nil {
		return err
	}
	for _, route := range routes {
		if route.RouteKind == "private" {
			return m.SendTextEffect(ctx, tenantID, userID, route.ID,
				uuid.NewString(), "test", "Vane Telegram 连接测试成功。")
		}
	}
	return types.NewAppError(types.CodeNotFound,
		"Telegram 私聊路由不可用", types.ErrNotFound)
}

// SendTextEffect is the outbound extension point for future Brief, periodic
// report, feedback and operational notifications. Callers must derive a stable
// effectID from their durable business fact. It never retries a provider-crossed
// request; sending/ambiguous remain operator-visible terminal states.
func (m *Manager) SendTextEffect(
	ctx context.Context, tenantID, userID, routeID int64,
	effectID, effectKind, text string,
) error {
	prepared, err := m.store.PrepareTelegramOutbound(
		ctx, tenantID, userID, routeID, effectID, effectKind, text)
	if err != nil {
		return err
	}
	switch prepared.Status {
	case "sent":
		return nil
	case "prepared":
	default:
		return types.NewAppError(types.CodeConflict,
			"Telegram outbound effect 已结算或等待人工核对", types.ErrConflict)
	}
	claimed, err := m.store.ClaimTelegramOutbound(ctx, effectID)
	if err != nil {
		return err
	}
	threadID, _ := strconv.ParseInt(claimed.ProviderThreadID, 10, 64)
	chunks := SplitMessage(claimed.PayloadText)
	messageIDs := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		sendCtx, cancel := context.WithTimeout(ctx, sendBudget)
		messageID, sendErr := m.client.SendMessageTo(sendCtx,
			claimed.ProviderChatID, chunk,
			SendMessageOptions{MessageThreadID: threadID})
		cancel()
		if sendErr != nil {
			var deliveryErr *DeliveryError
			if len(messageIDs) == 0 && errors.As(sendErr, &deliveryErr) &&
				deliveryErr.DefinitelyNotSent &&
				deliveryErr.HTTPStatus == http.StatusTooManyRequests &&
				deliveryErr.RetryAfter > 0 {
				scheduled, deferErr := m.store.DeferTelegramOutbound(
					ctx, claimed, deliveryErr.RetryAfter, maxRateLimitRetries)
				if deferErr != nil {
					return deferErr
				}
				if scheduled {
					return types.NewAppError(types.CodePushFailed,
						"Telegram 限流，已按 provider retry_after 耐久延后", nil)
				}
				return types.NewAppError(types.CodePushFailed,
					"Telegram 限流重试预算已耗尽", nil)
			}
			if len(messageIDs) == 0 && errors.As(sendErr, &deliveryErr) &&
				deliveryErr.DefinitelyNotSent {
				m.observeProviderError(sendErr)
				if settleErr := m.store.MarkTelegramOutboundRejected(
					ctx, claimed, sanitizeDeliveryCode(sendErr)); settleErr != nil {
					return settleErr
				}
				return types.NewAppError(types.CodePushFailed,
					"Telegram 明确拒绝发送", nil)
			}
			if settleErr := m.store.MarkTelegramOutboundAmbiguous(
				ctx, claimed, messageIDs, sanitizeDeliveryCode(sendErr)); settleErr != nil {
				return settleErr
			}
			return types.NewAppError(types.CodePushFailed,
				"Telegram 发送结果不确定，已阻止自动重发", nil)
		}
		messageIDs = append(messageIDs, messageID)
	}
	if err := m.store.CompleteTelegramOutbound(ctx, claimed, messageIDs); err != nil {
		return types.NewAppError(types.CodePushFailed,
			"Telegram 已发送但本地结算失败，已阻止自动重发", err)
	}
	return nil
}
