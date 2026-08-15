package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/YouToco/vane/server/config"
	"github.com/YouToco/vane/server/telegram"
)

type telegramShutdowner interface {
	Shutdown(context.Context) error
}

func buildTelegramManager(
	cfg config.TelegramConfig,
	st telegram.CredentialStore,
	agentLoop telegram.ChannelAgent,
) (*telegram.Fleet, error) {
	legacy := telegram.Config{
		Enabled: cfg.Enabled, Token: cfg.BotToken,
		WebhookSecret: cfg.WebhookSecret, WebhookURL: cfg.WebhookURL,
		APIBaseURL: cfg.APIBaseURL, Workers: cfg.Workers,
	}
	return telegram.NewFleet(telegram.FleetConfig{
		Legacy: legacy, WebhookURL: cfg.WebhookURL,
		APIBaseURL: cfg.APIBaseURL, Workers: cfg.Workers,
		Dynamic: st != nil,
	}, st, agentLoop, &http.Client{Timeout: 20 * time.Second}, slog.Default())
}

func shutdownTelegramIngress(manager telegramShutdowner, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := manager.Shutdown(ctx); err != nil {
		return fmt.Errorf("排空 Telegram update: %w", err)
	}
	return nil
}

func appendTelegramReadiness(
	stores []readyzStore, enabled bool, manager readyzStore,
) []readyzStore {
	if enabled {
		return append(stores, manager)
	}
	return stores
}

func mountTelegramWebhook(
	mux *http.ServeMux, enabled bool, handler http.Handler,
) {
	if enabled {
		mux.Handle("POST /telegram/webhook", handler)
		mux.Handle("POST /telegram/webhook/{bot_id}", handler)
	}
}
