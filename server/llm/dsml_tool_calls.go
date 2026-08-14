package llm

import (
	"errors"
	"strings"

	"github.com/YouToco/vane/types"
)

const dsmlSafeContent = "模型返回了无法安全解析的内部协议；原文已移除，且没有据此执行任何操作。"

// ErrToolProtocolResponse classifies an upstream response that exposed an
// internal tool protocol instead of returning a valid native tool call or
// ordinary assistant content. Callers may recover only from durable state they
// already own; they must never parse or execute the leaked text.
var ErrToolProtocolResponse = errors.New("llm: unsafe tool protocol response")

type toolProtocolResponseError struct {
	app *types.AppError
}

func (e *toolProtocolResponseError) Error() string {
	return e.app.Error()
}

func (e *toolProtocolResponseError) Unwrap() error {
	return e.app
}

func (e *toolProtocolResponseError) Is(target error) bool {
	return target == ErrToolProtocolResponse
}

func newToolProtocolResponseError() error {
	app := types.NewAppError(types.CodeLLMUnavailable,
		"llm: 上游工具调用格式异常", nil)
	app.Retryable = false
	return &toolProtocolResponseError{app: app}
}

// containsLeakedDSMLToolCalls recognizes DeepSeek V4's internal tool-call
// protocol markers. It deliberately does not parse the envelope: model text
// must never be promoted into executable ToolCalls by this compatibility path.
func containsLeakedDSMLToolCalls(content string) bool {
	return strings.Contains(content, "｜｜DSML｜｜") ||
		strings.Contains(content, "｜DSML｜")
}

// RedactLeakedDSMLContent removes exact fullwidth DSML protocol markers from
// persisted/history content. Stored rows predate reliable provider/model
// metadata, so this narrow marker-based scrub is intentionally provider-agnostic.
func RedactLeakedDSMLContent(content string) (string, bool) {
	if !containsLeakedDSMLToolCalls(content) {
		return content, false
	}
	return dsmlSafeContent, true
}

// redactLeakedDSMLMessages returns a by-value sanitized copy only when needed,
// leaving the caller's slice and structs untouched. Historical production
// leaks have appeared under user, assistant, and tool roles, so every role's
// Content is scrubbed while ToolCalls and ToolCallID remain intact.
func redactLeakedDSMLMessages(messages []ChatMessage) ([]ChatMessage, int) {
	redacted := messages
	count := 0
	for i, message := range messages {
		content, ok := RedactLeakedDSMLContent(message.Content)
		if !ok {
			continue
		}
		if count == 0 {
			redacted = append([]ChatMessage(nil), messages...)
		}
		redacted[i].Content = content
		count++
	}
	return redacted, count
}

func isDeepSeekV4(provider string, models ...string) bool {
	if !strings.EqualFold(provider, "deepseek") {
		return false
	}
	for _, model := range models {
		if strings.HasPrefix(strings.ToLower(model), "deepseek-v4-") {
			return true
		}
	}
	return false
}

func isDeepSeekV4DSML(provider, content string, models ...string) bool {
	return containsLeakedDSMLToolCalls(content) && isDeepSeekV4(provider, models...)
}
