package runcontext

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/YouToco/vane/promptguard"
	"github.com/YouToco/vane/types"
)

const structuredEventEvidenceMaxRunesV1 = 800

// StructuredEventEvidenceTextV1 is the single normalization boundary shared
// by CardGen and the later durable Brief validator.
func StructuredEventEvidenceTextV1(content string) string {
	runes := []rune(content)
	if len(runes) > structuredEventEvidenceMaxRunesV1 {
		runes = runes[:structuredEventEvidenceMaxRunesV1]
	}
	return strings.TrimSpace(promptguard.Sanitize(string(runes)))
}

// StructuredEventEvidenceMetadataV1 derives only inventory-owned presentation
// fields. Mutable source rows never participate: source is the frozen run
// snapshot entry selected for this content item.
func StructuredEventEvidenceMetadataV1(
	index int,
	item types.ContentItem,
	source SourceV1,
) (types.StructuredEvidenceSourceV1, error) {
	publishedAt := item.PublishedAt
	if publishedAt != nil {
		published := publishedAt.Round(0).UTC().Truncate(time.Microsecond)
		publishedAt = &published
	}
	metadata := types.StructuredEvidenceSourceV1{
		Ref:         "source-" + strconv.Itoa(index+1),
		Title:       strings.TrimSpace(item.Title),
		SourceTitle: strings.TrimSpace(source.Title),
		Platform:    string(source.Platform),
		SourceURL:   strings.TrimSpace(item.URL),
		PublishedAt: publishedAt,
		DiscoveredAt: item.CreatedAt.Round(0).UTC().
			Truncate(time.Microsecond),
	}
	if index < 0 || metadata.Validate() != nil {
		return types.StructuredEvidenceSourceV1{},
			errors.New("structured event evidence metadata is invalid")
	}
	return metadata, nil
}
