package feishu

import (
	"context"
	"errors"
	"testing"
)

func TestFeishuSDKLoggerDropsAllProviderValues(t *testing.T) {
	calls := 0
	logger := &feishuSDKLogger{
		errorEvent: func(context.Context) {
			calls++
		},
	}
	credentialURL := "wss://msg-frontier.feishu.cn/ws/v2?" +
		"access_key=secret%zz&ticket=also-secret"

	logger.Debug(context.Background(), credentialURL)
	logger.Info(context.Background(), credentialURL)
	logger.Warn(context.Background(), credentialURL)
	logger.Error(context.Background(), "connect failed", errors.New(credentialURL))

	if calls != 1 {
		t.Fatalf("fixed SDK error event calls = %d, want 1", calls)
	}
}
