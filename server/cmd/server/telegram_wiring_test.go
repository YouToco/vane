package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/YouToco/vane/server/config"
)

type telegramWiringProbe struct {
	pingErr     error
	shutdownErr error
}

func (p telegramWiringProbe) Ping(context.Context) error     { return p.pingErr }
func (p telegramWiringProbe) Shutdown(context.Context) error { return p.shutdownErr }

func TestTelegramWiringDisabledConstructionAndReadiness(t *testing.T) {
	manager, err := buildTelegramManager(config.TelegramConfig{}, nil, nil)
	if err != nil || manager.Status().Enabled {
		t.Fatalf("manager=%+v err=%v", manager, err)
	}
	base := []readyzStore{telegramWiringProbe{}}
	if got := appendTelegramReadiness(base, false, manager); len(got) != 1 {
		t.Fatalf("disabled readiness len=%d", len(got))
	}
	if got := appendTelegramReadiness(base, true, manager); len(got) != 2 {
		t.Fatalf("enabled readiness len=%d", len(got))
	}
}

func TestTelegramWiringMountAndShutdown(t *testing.T) {
	mux := http.NewServeMux()
	mountTelegramWebhook(mux, false, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("disabled webhook invoked")
	}))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/telegram/webhook", nil))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("disabled status=%d", rr.Code)
	}
	mountTelegramWebhook(mux, true, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/telegram/webhook", nil))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("enabled status=%d", rr.Code)
	}
	if err := shutdownTelegramIngress(telegramWiringProbe{}, time.Second); err != nil {
		t.Fatal(err)
	}
	probeErr := errors.New("drain failed")
	if err := shutdownTelegramIngress(
		telegramWiringProbe{shutdownErr: probeErr}, time.Second,
	); !errors.Is(err, probeErr) {
		t.Fatalf("shutdown err=%v", err)
	}
}
