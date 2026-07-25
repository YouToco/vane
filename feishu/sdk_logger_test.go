package feishu

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestFeishuSDKLoggerDropsAllProviderValues(t *testing.T) {
	var output bytes.Buffer
	logger := &feishuSDKLogger{logger: slog.New(slog.NewTextHandler(
		&output, &slog.HandlerOptions{Level: slog.LevelDebug},
	))}
	credentialURL := "wss://msg-frontier.feishu.cn/ws/v2?" +
		"access_key=secret%zz&ticket=also-secret"

	logger.Debug(context.Background(), credentialURL)
	logger.Info(context.Background(), credentialURL)
	logger.Warn(context.Background(), credentialURL)
	logger.Error(context.Background(), "connect failed", errors.New(credentialURL))

	logged := output.String()
	if strings.Contains(logged, "secret") ||
		strings.Contains(logged, "access_key") ||
		strings.Contains(logged, "ticket") {
		t.Fatalf("SDK credential escaped value firewall: %s", logged)
	}
	if !strings.Contains(logged, "SDK error") {
		t.Fatalf("credential-free SDK error event missing: %s", logged)
	}
}

func TestManagerWSFailureDropsProviderErrorValues(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(
		&output, &slog.HandlerOptions{Level: slog.LevelDebug},
	))
	credentialURL := "wss://msg-frontier.feishu.cn/ws/v2?" +
		"access_key=secret%zz&ticket=also-secret"
	m := NewManager(nil, nil, nil)

	m.mu.Lock()
	m.recordWSFailureLocked(logger, errors.New(credentialURL))
	m.mu.Unlock()

	status := m.Status()
	combined := output.String() + status.LastError
	if strings.Contains(combined, "secret") ||
		strings.Contains(combined, "access_key") ||
		strings.Contains(combined, "ticket") {
		t.Fatalf("manager retained provider credential: %s", combined)
	}
	if status.LastError == "" || !strings.Contains(output.String(), "error_type") {
		t.Fatalf("credential-free failure observability missing: status=%q log=%s",
			status.LastError, output.String())
	}
}
