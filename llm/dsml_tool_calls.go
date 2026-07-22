package llm

import "strings"

const dsmlSafeContent = "请查看当前会话中的最新状态。"

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
