package types

import "unicode/utf8"

const MaxModelVisibleToolResultBytes = 256 * 1024

// BoundModelVisibleToolResultUTF8 keeps the exact bytes shown to a model
// within the evidence contract while preserving valid UTF-8. OriginalSize is
// measured before truncation; the ellipsis makes truncation explicit.
func BoundModelVisibleToolResultUTF8(result string) (visible string, originalSize int) {
	originalSize = len(result)
	if originalSize <= MaxModelVisibleToolResultBytes {
		return result, originalSize
	}
	const suffix = "…"
	cut := MaxModelVisibleToolResultBytes - len(suffix)
	for cut > 0 && !utf8.ValidString(result[:cut]) {
		cut--
	}
	return result[:cut] + suffix, originalSize
}
