package types

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestBoundModelVisibleToolResultUTF8(t *testing.T) {
	short := "官方结果"
	if visible, size := BoundModelVisibleToolResultUTF8(short); visible != short || size != len(short) {
		t.Fatalf("short visible=%q size=%d", visible, size)
	}
	long := strings.Repeat("a", MaxModelVisibleToolResultBytes-1) + "界"
	visible, size := BoundModelVisibleToolResultUTF8(long)
	if size != len(long) || len(visible) > MaxModelVisibleToolResultBytes ||
		!utf8.ValidString(visible) || !strings.HasSuffix(visible, "…") {
		t.Fatalf("bounded bytes=%d original=%d valid=%v", len(visible), size, utf8.ValidString(visible))
	}
}

func TestNormalizeModelVisibleToolResultExactBytes(t *testing.T) {
	raw := append([]byte("官方\x00结果"), 0xff, 0xfe)
	result := NormalizeModelVisibleToolResult(raw)
	if string(result.Visible) != "官方结果�" || result.NormalizedSize != len(result.Visible) ||
		result.Truncated || len(result.Digest) != 64 {
		t.Fatalf("result=%+v visible=%q", result, result.Visible)
	}
	result.Visible[0] = 'x'
	again := NormalizeModelVisibleToolResult(raw)
	if string(again.Visible) != "官方结果�" {
		t.Fatalf("normalizer retained caller mutation: %q", again.Visible)
	}
}

func TestNormalizeModelVisibleToolResultTruncatesAfterNormalization(t *testing.T) {
	raw := append([]byte(strings.Repeat("a", MaxModelVisibleToolResultBytes)), 0xff)
	result := NormalizeModelVisibleToolResult(raw)
	if !result.Truncated || result.NormalizedSize <= len(result.Visible) ||
		len(result.Visible) > MaxModelVisibleToolResultBytes ||
		!utf8.Valid(result.Visible) || !strings.HasSuffix(string(result.Visible), "…") {
		t.Fatalf("result=%+v", result)
	}
}
