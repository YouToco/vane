// Package credentialguard contains the deliberately conservative credential
// patterns shared by model-facing admission and the final Store boundary.
package credentialguard

import (
	"regexp"
	"strings"
)

var highConfidencePatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)-----BEGIN[[:space:]]+(?:[A-Z0-9]+[[:space:]]+)*PRIVATE KEY-----`),
	regexp.MustCompile(`(?i)(?:postgres(?:ql)?|mysql|mongodb(?:\+srv)?|redis|rediss|amqp|amqps)://[^[:space:]/:@]+:[^[:space:]/@]+@`),
	regexp.MustCompile(`(?i)\bsk-[a-z0-9_-]{16,}\b`),
	regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{20,}\b`),
	regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`),
	regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b`),
	regexp.MustCompile(`(?i)(?:api[ _-]?key|access[ _-]?token|refresh[ _-]?token|client[ _-]?secret|token|secret|password|passwd|pwd|密码|密钥|令牌)[[:space:]]*(?:=|:|：|is[[:space:]]+|是[[:space:]]+)[[:space:]]*["']?[^[:space:]"']{8,}`),
}

// ContainsCredential reports only high-confidence credential material. It is
// a rejection gate rather than a redactor: accepted memories must remain
// byte-exact owner facts, while rejected input is never echoed or persisted.
func ContainsCredential(text string) bool {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return false
	}
	for _, pattern := range highConfidencePatterns {
		if pattern.MatchString(trimmed) {
			return true
		}
	}
	return false
}
