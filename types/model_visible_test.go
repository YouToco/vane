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
