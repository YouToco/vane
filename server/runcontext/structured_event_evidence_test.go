package runcontext

import (
	"strings"
	"testing"
)

func TestStructuredEventEvidenceTextV1TrimsAfterTruncation(t *testing.T) {
	content := strings.Repeat("证", structuredEventEvidenceMaxRunesV1-1) +
		" " + "截断后不可见"

	got := StructuredEventEvidenceTextV1(content)
	if len([]rune(got)) != structuredEventEvidenceMaxRunesV1-1 {
		t.Fatalf("rune count = %d", len([]rune(got)))
	}
	if strings.TrimSpace(got) != got {
		t.Fatalf("truncated evidence retains boundary whitespace: %q", got)
	}
}

func TestStructuredEventEvidenceTextV1TrimsOriginalBoundaries(t *testing.T) {
	got := StructuredEventEvidenceTextV1("\n  正文证据  \t")
	if got != "正文证据" {
		t.Fatalf("got %q", got)
	}
}

func TestStructuredEventEvidenceTextV1CanNormalizeToEmpty(t *testing.T) {
	for _, content := range []string{
		" \n\t ",
		strings.Repeat(" ", structuredEventEvidenceMaxRunesV1) + "正文",
	} {
		if got := StructuredEventEvidenceTextV1(content); got != "" {
			t.Fatalf("got %q", got)
		}
	}
}
