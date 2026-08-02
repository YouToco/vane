package types

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode/utf8"
)

const MaxModelVisibleToolResultBytes = 256 * 1024

type ModelVisibleToolResult struct {
	Visible        []byte
	NormalizedSize int
	Truncated      bool
	Digest         string
}

// NormalizeModelVisibleToolResult creates the one exact byte slice that may
// be shown to a model and persisted as evidence. NormalizedSize is the size
// after invalid UTF-8 and NUL normalization but before the model-visible cap;
// a provider's own size/truncation belongs in a separate provider receipt.
func NormalizeModelVisibleToolResult(raw []byte) ModelVisibleToolResult {
	normalized := strings.ReplaceAll(strings.ToValidUTF8(string(raw), "�"), "\x00", "")
	visible, normalizedSize := boundNormalizedModelVisibleToolResult(normalized)
	sum := sha256.Sum256([]byte(visible))
	return ModelVisibleToolResult{
		Visible:        []byte(visible),
		NormalizedSize: normalizedSize,
		Truncated:      normalizedSize > len(visible),
		Digest:         hex.EncodeToString(sum[:]),
	}
}

// BoundModelVisibleToolResultUTF8 keeps the exact bytes shown to a model
// within the evidence contract while preserving valid UTF-8. OriginalSize is
// measured before truncation; the ellipsis makes truncation explicit.
func BoundModelVisibleToolResultUTF8(result string) (visible string, originalSize int) {
	return boundNormalizedModelVisibleToolResult(result)
}

func boundNormalizedModelVisibleToolResult(result string) (visible string, originalSize int) {
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
