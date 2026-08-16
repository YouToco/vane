package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

const telegramCredentialPurpose = "bot_api"

// CredentialStore deliberately separates metadata inventory from exact-
// generation secret use. Fleet startup can discover user managers without
// ever materializing a cross-user plaintext secret list.
type CredentialStore interface {
	IngressStore
	ListActiveUserCredentialMetadata(context.Context, string, string) ([]store.CredentialMetadata, error)
	ActiveCredentialMetadata(context.Context, store.CredentialScope) (store.CredentialMetadata, error)
	UseCredential(context.Context, store.CredentialScope, int64, func([]byte, store.CredentialMetadata) error) error
}

type FleetConfig struct {
	Legacy        Config
	WebhookURL    string
	APIBaseURL    string
	Workers       int
	Dynamic       bool
	ShutdownGrace time.Duration
}

type fleetEntry struct {
	tenantID   int64
	userID     int64
	botID      string
	generation int64
	manager    *Manager
}

// Fleet owns one isolated Manager per user-owned Bot. A webhook URL contains
// the verified Telegram bot id, and routing happens before update decoding, so
// an update can never select a tenant from attacker-controlled JSON fields.
type Fleet struct {
	cfg        FleetConfig
	store      CredentialStore
	agent      ChannelAgent
	httpClient *http.Client
	logger     *slog.Logger

	mu          sync.RWMutex
	byUser      map[userScope]*fleetEntry
	byBot       map[string]*fleetEntry
	legacy      *Manager
	started     bool
	runCtx      context.Context
	reconfigure sync.Mutex
}

type userScope struct {
	tenantID int64
	userID   int64
}

type storedTelegramSecret struct {
	BotToken      string `json:"bot_token"`
	WebhookSecret string `json:"webhook_secret"`
}

type storedTelegramMetadata struct {
	BotID int64 `json:"bot_id"`
}

func NewFleet(
	cfg FleetConfig,
	st CredentialStore,
	agentLoop ChannelAgent,
	httpClient *http.Client,
	logger *slog.Logger,
) (*Fleet, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 20 * time.Second}
	}
	if cfg.Workers == 0 {
		cfg.Workers = 1
	}
	if cfg.ShutdownGrace == 0 {
		cfg.ShutdownGrace = 2*time.Minute + 10*time.Second
	}
	if cfg.Dynamic && (st == nil || agentLoop == nil) {
		return nil, errors.New("telegram: dynamic fleet dependencies are incomplete")
	}
	return &Fleet{cfg: cfg, store: st, agent: agentLoop,
		httpClient: httpClient, logger: logger,
		byUser: make(map[userScope]*fleetEntry), byBot: make(map[string]*fleetEntry)}, nil
}

func (f *Fleet) Start(ctx context.Context) error {
	f.reconfigure.Lock()
	defer f.reconfigure.Unlock()
	f.mu.Lock()
	if f.started {
		f.mu.Unlock()
		return errors.New("telegram: fleet already started")
	}
	f.started = true
	f.runCtx = context.WithoutCancel(ctx)
	f.mu.Unlock()

	var active []store.CredentialMetadata
	var err error
	if f.cfg.Dynamic {
		active, err = f.store.ListActiveUserCredentialMetadata(
			ctx, "telegram", telegramCredentialPurpose)
		if err != nil {
			return fmt.Errorf("telegram: list user credentials: %w", err)
		}
	}
	for _, metadata := range active {
		if err := f.activateMetadata(ctx, metadata); err != nil {
			_ = f.shutdownAll(context.Background())
			return fmt.Errorf("telegram: activate user %d/%d generation %d: %w",
				metadata.TenantID, metadata.UserID, metadata.Generation, err)
		}
	}

	// The legacy environment adapter remains a migration bridge for users that
	// have not configured their own database-backed Bot.
	if f.cfg.Legacy.Enabled {
		legacy, err := NewManager(f.cfg.Legacy, f.store, f.agent,
			f.httpClient, f.logger.With("credential_source", "legacy"))
		if err != nil {
			_ = f.shutdownAll(context.Background())
			return err
		}
		if err := legacy.Start(ctx); err != nil {
			_ = f.shutdownAll(context.Background())
			return err
		}
		f.mu.Lock()
		f.legacy = legacy
		f.mu.Unlock()
	}
	return nil
}

func (f *Fleet) ActivateUser(ctx context.Context, tenantID, userID int64) error {
	if !f.cfg.Dynamic || tenantID <= 0 || userID <= 0 {
		return types.NewAppError(types.CodeConflict,
			"Telegram 用户凭证运行时尚未启用", types.ErrConflict)
	}
	f.reconfigure.Lock()
	defer f.reconfigure.Unlock()
	metadata, err := f.store.ActiveCredentialMetadata(ctx, telegramScope(tenantID, userID))
	if err != nil {
		return err
	}
	return f.activateMetadata(ctx, metadata)
}

func (f *Fleet) activateMetadata(ctx context.Context, metadata store.CredentialMetadata) error {
	var secret storedTelegramSecret
	var declared storedTelegramMetadata
	if err := json.Unmarshal(metadata.Metadata, &declared); err != nil || declared.BotID <= 0 {
		return errors.New("telegram: stored bot metadata is invalid")
	}
	if err := f.store.UseCredential(ctx, metadata.CredentialScope,
		metadata.Generation, func(raw []byte, _ store.CredentialMetadata) error {
			if err := json.Unmarshal(raw, &secret); err != nil {
				return errors.New("telegram: stored bot credential is invalid")
			}
			return nil
		}); err != nil {
		return err
	}
	botID := strconv.FormatInt(declared.BotID, 10)
	webhookURL, err := tenantWebhookURL(f.cfg.WebhookURL, botID)
	if err != nil {
		return err
	}
	manager, err := NewManager(Config{
		Enabled: true, Token: secret.BotToken, WebhookSecret: secret.WebhookSecret,
		WebhookURL: webhookURL, APIBaseURL: f.cfg.APIBaseURL,
		Workers: f.cfg.Workers,
	}, f.store, f.agent, f.httpClient,
		f.logger.With("tenant_id", metadata.TenantID, "bot_id", botID,
			"credential_generation", metadata.Generation))
	if err != nil {
		return err
	}
	entry := &fleetEntry{tenantID: metadata.TenantID, userID: metadata.UserID, botID: botID,
		generation: metadata.Generation, manager: manager}

	f.mu.Lock()
	if other := f.byBot[botID]; other != nil &&
		(other.tenantID != metadata.TenantID || other.userID != metadata.UserID) {
		f.mu.Unlock()
		return types.NewAppError(types.CodeConflict,
			"该 Telegram Bot 已由其他用户配置", types.ErrConflict)
	}
	key := userScope{tenantID: metadata.TenantID, userID: metadata.UserID}
	old := f.byUser[key]
	// Provisional route makes setWebhook safe: Telegram receives 503 (and
	// retries) until Manager.Start has proved getMe/webhook state and Ready.
	f.byBot[botID] = entry
	f.mu.Unlock()

	f.mu.RLock()
	runCtx := f.runCtx
	started := f.started
	f.mu.RUnlock()
	if !started || runCtx == nil {
		f.restoreProvisional(entry, old)
		return errors.New("telegram: fleet is not started")
	}
	if err := manager.Start(runCtx); err != nil {
		f.restoreProvisional(entry, old)
		return err
	}

	f.mu.Lock()
	f.byUser[key] = entry
	if old != nil && old.botID != botID && f.byBot[old.botID] == old {
		delete(f.byBot, old.botID)
	}
	f.mu.Unlock()
	if old != nil && old.manager != manager {
		_ = f.shutdownManager(old.manager)
	}
	return nil
}

func (f *Fleet) restoreProvisional(current, old *fleetEntry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.byBot[current.botID] == current {
		if old != nil && old.botID == current.botID {
			f.byBot[current.botID] = old
		} else {
			delete(f.byBot, current.botID)
		}
	}
}

func (f *Fleet) DeactivateUser(_ context.Context, tenantID, userID int64) error {
	f.reconfigure.Lock()
	defer f.reconfigure.Unlock()
	f.mu.Lock()
	key := userScope{tenantID: tenantID, userID: userID}
	entry := f.byUser[key]
	if entry != nil {
		delete(f.byUser, key)
		if f.byBot[entry.botID] == entry {
			delete(f.byBot, entry.botID)
		}
	}
	f.mu.Unlock()
	if entry == nil {
		return nil
	}
	return f.shutdownManager(entry.manager)
}

func (f *Fleet) shutdownManager(manager *Manager) error {
	ctx, cancel := context.WithTimeout(context.Background(), f.cfg.ShutdownGrace)
	defer cancel()
	return manager.Shutdown(ctx)
}

func (f *Fleet) Shutdown(ctx context.Context) error {
	f.reconfigure.Lock()
	defer f.reconfigure.Unlock()
	return f.shutdownAll(ctx)
}

func (f *Fleet) shutdownAll(ctx context.Context) error {
	f.mu.Lock()
	entries := make([]*fleetEntry, 0, len(f.byUser))
	for _, entry := range f.byUser {
		entries = append(entries, entry)
	}
	legacy := f.legacy
	f.byUser = make(map[userScope]*fleetEntry)
	f.byBot = make(map[string]*fleetEntry)
	f.legacy = nil
	f.started = false
	f.mu.Unlock()
	var errs []error
	for _, entry := range entries {
		if err := entry.manager.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if legacy != nil {
		if err := legacy.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (f *Fleet) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		botID := r.PathValue("bot_id")
		f.mu.RLock()
		entry := f.byBot[botID]
		legacy := f.legacy
		f.mu.RUnlock()
		if botID != "" && entry != nil {
			entry.manager.Handler().ServeHTTP(w, r)
			return
		}
		if botID == "" && legacy != nil {
			legacy.Handler().ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
	})
}

func (f *Fleet) Status() Status {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.legacy != nil {
		return f.legacy.Status()
	}
	return Status{Enabled: len(f.byUser) > 0, Ready: len(f.byUser) > 0}
}

func (f *Fleet) PrincipalStatus(_ context.Context, tenantID, userID int64) Status {
	f.mu.RLock()
	entry := f.byUser[userScope{tenantID: tenantID, userID: userID}]
	legacy := f.legacy
	f.mu.RUnlock()
	if entry != nil {
		return entry.manager.Status()
	}
	if legacy != nil {
		return legacy.Status()
	}
	return Status{}
}

// Ping proves the fleet router and inventory loop are alive. Per-user Bot
// credential rejection is exposed in that user's status and must not make
// every other SaaS user fail process readiness.
func (f *Fleet) Ping(context.Context) error {
	f.mu.RLock()
	started := f.started
	f.mu.RUnlock()
	if !started {
		return errors.New("telegram fleet is not started")
	}
	return nil
}

func (f *Fleet) managerForUser(tenantID, userID int64) (*Manager, error) {
	f.mu.RLock()
	entry := f.byUser[userScope{tenantID: tenantID, userID: userID}]
	legacy := f.legacy
	f.mu.RUnlock()
	if entry != nil {
		return entry.manager, nil
	}
	if legacy != nil {
		return legacy, nil
	}
	return nil, types.NewAppError(types.CodeConflict,
		"当前用户尚未配置 Telegram Bot", types.ErrConflict)
}

func (f *Fleet) IssueLink(ctx context.Context, tenantID, userID int64) (Link, error) {
	m, err := f.managerForUser(tenantID, userID)
	if err != nil {
		return Link{}, err
	}
	return m.IssueLink(ctx, tenantID, userID)
}
func (f *Fleet) IssueRouteLink(ctx context.Context, tenantID, userID int64) (Link, error) {
	m, err := f.managerForUser(tenantID, userID)
	if err != nil {
		return Link{}, err
	}
	return m.IssueRouteLink(ctx, tenantID, userID)
}
func (f *Fleet) Binding(ctx context.Context, tenantID, userID int64) (store.ChannelIdentity, error) {
	m, err := f.managerForUser(tenantID, userID)
	if err != nil {
		return store.ChannelIdentity{}, err
	}
	return m.Binding(ctx, tenantID, userID)
}
func (f *Fleet) Routes(ctx context.Context, tenantID, userID int64) ([]RouteSummary, error) {
	m, err := f.managerForUser(tenantID, userID)
	if err != nil {
		return nil, err
	}
	return m.Routes(ctx, tenantID, userID)
}
func (f *Fleet) BlockedReplies(ctx context.Context, tenantID, userID int64) (store.ChannelDeliveryBlockStats, error) {
	m, err := f.managerForUser(tenantID, userID)
	if err != nil {
		return store.ChannelDeliveryBlockStats{}, nil
	}
	return m.BlockedReplies(ctx, tenantID, userID)
}
func (f *Fleet) Unlink(ctx context.Context, tenantID, userID int64) error {
	m, err := f.managerForUser(tenantID, userID)
	if err != nil {
		return err
	}
	return m.Unlink(ctx, tenantID, userID)
}
func (f *Fleet) UnlinkRoute(ctx context.Context, tenantID, userID, routeID int64) error {
	m, err := f.managerForUser(tenantID, userID)
	if err != nil {
		return err
	}
	return m.UnlinkRoute(ctx, tenantID, userID, routeID)
}
func (f *Fleet) SendTest(ctx context.Context, tenantID, userID int64) error {
	m, err := f.managerForUser(tenantID, userID)
	if err != nil {
		return err
	}
	return m.SendTest(ctx, tenantID, userID)
}

// SendTextEffect routes a durable outbound effect through the exact Bot owned
// by this Vane principal. The route is re-proved again by Manager/Store before
// any provider call.
func (f *Fleet) SendTextEffect(
	ctx context.Context, tenantID, userID, routeID int64,
	effectID, effectKind, body string,
) error {
	m, err := f.managerForUser(tenantID, userID)
	if err != nil {
		return err
	}
	return m.SendTextEffect(ctx, tenantID, userID, routeID,
		effectID, effectKind, body)
}

func telegramScope(tenantID, userID int64) store.CredentialScope {
	return store.CredentialScope{Kind: "user", TenantID: tenantID, UserID: userID,
		Provider: "telegram", Purpose: telegramCredentialPurpose}
}

func tenantWebhookURL(base, botID string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(base))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		strings.TrimRight(parsed.Path, "/") != "/telegram/webhook" {
		return "", errors.New("telegram: fleet webhook base URL is invalid")
	}
	if _, err := strconv.ParseInt(botID, 10, 64); err != nil {
		return "", errors.New("telegram: bot identity is invalid")
	}
	parsed.Path = "/telegram/webhook/" + botID
	return parsed.String(), nil
}
