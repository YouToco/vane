package feishu

import (
	"context"
	"log/slog"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

// feishuSDKLogger is a value firewall around the Feishu WebSocket SDK.
//
// The SDK logs complete connection URLs at INFO and can also embed those URLs
// in errors. Their query contains short-lived access_key and ticket
// credentials. No SDK-provided value is therefore safe to forward verbatim.
// Vane retains a fixed error event here and emits a credential-free error type
// when Start returns.
type feishuSDKLogger struct {
	errorEvent func(context.Context)
}

var _ larkcore.Logger = (*feishuSDKLogger)(nil)

func newFeishuSDKLogger() *feishuSDKLogger {
	return &feishuSDKLogger{
		errorEvent: func(ctx context.Context) {
			slog.ErrorContext(ctx, "feishu: SDK error（详情已脱敏）")
		},
	}
}

func (*feishuSDKLogger) Debug(context.Context, ...interface{}) {}
func (*feishuSDKLogger) Info(context.Context, ...interface{})  {}
func (*feishuSDKLogger) Warn(context.Context, ...interface{})  {}

func (l *feishuSDKLogger) Error(ctx context.Context, _ ...interface{}) {
	if l != nil && l.errorEvent != nil {
		l.errorEvent(ctx)
	}
}
