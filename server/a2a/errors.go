// 错误卫生（契约 §5.6/§8.1）：sanitize 是 executor 全部 error 的唯一对外翻译点。
package a2a

import (
	"errors"

	"github.com/YouToco/vane/types"
)

// internalErrorText 是非 AppError 的固定对外文案。
const internalErrorText = "内部错误，请稍后重试"

// sanitize 把内部错误翻译成可对外的文案（先例：api.writeAppError）：
// AppError → 其 Message（人话，可对外）；非 AppError → 固定文案。
// 只有本函数的返回值能进 yield 的事件 / FAILED 状态 message / Artifact 文案——
// SDK 会把 executor 产出的内容写进协议响应，原始错误链（pgx/SQL/路径）一个字节
// 不得外泄（突变测试钉死，executor_test.go）。原始错误由调用方带 taskId/contextId
// 落 slog（结构化，多跳排查用）。
func sanitize(err error) string {
	var appErr *types.AppError
	if errors.As(err, &appErr) && appErr.Message != "" {
		return appErr.Message
	}
	return internalErrorText
}
