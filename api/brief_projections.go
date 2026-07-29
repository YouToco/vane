package api

import (
	"time"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

// Public brief projections deliberately exclude tenant/user identities,
// snapshot/outcome IDs, profile epochs, provenance digests, and frozen input
// identities. Those fields remain durable integrity metadata, not API data.
type executiveBriefResponseV1 struct {
	GenerationMode types.ExecutiveGenerationModeV1 `json:"generation_mode"`
	Processing     types.RunCompletenessV1         `json:"processing"`
	GeneratedAt    time.Time                       `json:"generated_at"`
	Content        types.ExecutiveBriefContentV1   `json:"content"`
}

type taskBriefItemResponseV1 struct {
	ID             int64                      `json:"id"`
	PushBatchID    int64                      `json:"push_batch_id"`
	GeneratedAt    time.Time                  `json:"generated_at"`
	SourceCoverage types.RunCompletenessV1    `json:"source_coverage"`
	Processing     types.RunCompletenessV1    `json:"processing"`
	Insights       []store.TaskBriefInsightV1 `json:"insights"`
	Executive      *executiveBriefResponseV1  `json:"executive,omitempty"`
}

type taskBriefPageResponseV1 struct {
	Items         []taskBriefItemResponseV1 `json:"items"`
	Total         int64                     `json:"total"`
	NextPageToken string                    `json:"next_page_token,omitempty"`
	LatestCheck   *store.TaskLatestCheckV1  `json:"latest_check,omitempty"`
}

func publicTaskBriefPageV1(
	page store.TaskBriefPageV1,
	includeExecutive bool,
) taskBriefPageResponseV1 {
	out := taskBriefPageResponseV1{
		Items: make([]taskBriefItemResponseV1, len(page.Items)),
		Total: page.Total, NextPageToken: page.NextPageToken,
		LatestCheck: page.LatestCheck,
	}
	for index, item := range page.Items {
		public := taskBriefItemResponseV1{
			ID: item.ID, PushBatchID: item.PushBatchID,
			GeneratedAt:    item.GeneratedAt,
			SourceCoverage: item.SourceCoverage,
			Processing:     item.Processing,
			Insights:       item.Insights,
		}
		if includeExecutive && item.Executive != nil {
			public.Executive = &executiveBriefResponseV1{
				GenerationMode: item.Executive.GenerationMode,
				Processing:     item.Executive.Processing,
				GeneratedAt:    item.Executive.GeneratedAt,
				Content: publicExecutiveBriefContentV1(
					item.Executive.Content),
			}
		}
		out.Items[index] = public
	}
	return out
}

type periodicBriefReportResponseV1 struct {
	ID             int64                           `json:"id"`
	Cadence        string                          `json:"cadence"`
	Timezone       string                          `json:"timezone"`
	PeriodStart    time.Time                       `json:"period_start"`
	PeriodEnd      time.Time                       `json:"period_end"`
	GeneratedAt    time.Time                       `json:"generated_at"`
	GenerationMode types.ExecutiveGenerationModeV1 `json:"generation_mode"`
	SourceCoverage types.RunCompletenessV1         `json:"source_coverage"`
	Processing     types.RunCompletenessV1         `json:"processing"`
	Content        types.ExecutiveBriefContentV1   `json:"content"`
}

type periodicBriefReportPageResponseV1 struct {
	Items      []periodicBriefReportResponseV1 `json:"items"`
	NextCursor string                          `json:"next_cursor,omitempty"`
}

func publicPeriodicBriefReportPageV1(
	page store.PeriodicBriefReportPageV1,
) periodicBriefReportPageResponseV1 {
	out := periodicBriefReportPageResponseV1{
		Items:      make([]periodicBriefReportResponseV1, len(page.Items)),
		NextCursor: page.NextCursor,
	}
	for index, report := range page.Items {
		out.Items[index] = periodicBriefReportResponseV1{
			ID: report.ID, Cadence: report.Cadence,
			Timezone:    report.Timezone,
			PeriodStart: report.PeriodStart, PeriodEnd: report.PeriodEnd,
			GeneratedAt:    report.GeneratedAt,
			GenerationMode: report.GenerationMode,
			SourceCoverage: report.SourceCoverage,
			Processing:     report.Processing,
			Content:        publicExecutiveBriefContentV1(report.Content),
		}
	}
	return out
}

// publicExecutiveBriefContentV1 keeps the JSON contract array-shaped even for
// artifacts written before empty fallback slices were canonicalized. Storage
// remains immutable; only the user-facing projection normalizes nil to [].
func publicExecutiveBriefContentV1(
	content types.ExecutiveBriefContentV1,
) types.ExecutiveBriefContentV1 {
	content.Signals = append(
		[]types.ExecutiveSignalV1{}, content.Signals...)
	content.NextSteps = append(
		[]types.ExecutiveNextStepV1{}, content.NextSteps...)
	return content
}
