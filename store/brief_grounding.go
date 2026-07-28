package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

type GroundedBriefKindV1 string

const (
	GroundedBriefIssue  GroundedBriefKindV1 = "brief"
	GroundedBriefReport GroundedBriefKindV1 = "report"
)

type GroundedEvidenceBriefV1 struct {
	BriefID  int64                `json:"brief_id"`
	Insights []TaskBriefInsightV1 `json:"insights"`
}

type GroundedBriefContextV1 struct {
	Kind           GroundedBriefKindV1             `json:"kind"`
	ID             int64                           `json:"id"`
	Cadence        string                          `json:"cadence,omitempty"`
	PeriodStart    string                          `json:"period_start,omitempty"`
	PeriodEnd      string                          `json:"period_end,omitempty"`
	SourceCoverage types.RunCompletenessV1         `json:"source_coverage"`
	Processing     types.RunCompletenessV1         `json:"processing"`
	GenerationMode types.ExecutiveGenerationModeV1 `json:"generation_mode"`
	Content        types.ExecutiveBriefContentV1   `json:"content"`
	Evidence       []GroundedEvidenceBriefV1       `json:"evidence"`
}

func projectGroundedEvidenceBriefV1(
	brief types.BriefV1,
) (GroundedEvidenceBriefV1, error) {
	out := GroundedEvidenceBriefV1{
		BriefID:  brief.ID,
		Insights: make([]TaskBriefInsightV1, len(brief.Insights)),
	}
	for index, frozen := range brief.Insights {
		projected := TaskBriefInsightV1{
			ID: frozen.ID, RankPosition: frozen.RankPosition,
			Title: frozen.Title, BodyMD: frozen.BodyMD,
			SourceTitle: frozen.SourceTitle, SourceURL: frozen.SourceURL,
			PublishedAt:  frozen.PublishedAt,
			DiscoveredAt: frozen.DiscoveredAt,
		}
		if frozen.Structured != nil {
			projected.Structured =
				projectTaskBriefStructuredInsightV1(frozen.Structured)
		}
		eventEvidence, err := projectTaskBriefEventEvidenceV1(frozen)
		if err != nil {
			return GroundedEvidenceBriefV1{}, err
		}
		projected.EventEvidence = eventEvidence
		out.Insights[index] = projected
	}
	return out, nil
}

func decodeGroundedBriefV1(
	payload []byte,
	tenantID, userID int64,
	taskID string,
	briefID int64,
) (types.BriefV1, error) {
	var brief types.BriefV1
	if json.Unmarshal(payload, &brief) != nil {
		return types.BriefV1{}, periodicIntegrityErrorV1()
	}
	canonical, err := json.Marshal(brief)
	if err != nil || !bytes.Equal(canonical, payload) ||
		brief.Validate() != nil || brief.ID != briefID ||
		brief.TenantID != tenantID || brief.UserID != userID ||
		brief.TaskID != taskID {
		return types.BriefV1{}, periodicIntegrityErrorV1()
	}
	return brief, nil
}

func (s *Store) LoadGroundedBriefContextV1(
	ctx context.Context,
	tenantID, userID int64,
	taskID string,
	kind GroundedBriefKindV1,
	id int64,
) (GroundedBriefContextV1, error) {
	if id <= 0 || (kind != GroundedBriefIssue &&
		kind != GroundedBriefReport) {
		return GroundedBriefContextV1{}, types.NewAppError(
			types.CodeValidation, "简报追问范围无效", nil)
	}
	tx, _, err := authorizeReportTaskV1(
		ctx, s, tenantID, userID, taskID, "vane_brief_reader")
	if err != nil {
		return GroundedBriefContextV1{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var out GroundedBriefContextV1
	if kind == GroundedBriefIssue {
		var briefPayload, artifactPayload []byte
		var sourceCoverage, processing types.RunCompletenessV1
		err = tx.QueryRow(ctx,
			`SELECT b.payload,a.payload,o.source_coverage,o.processing
			   FROM brief_snapshots b
			   JOIN task_run_outcomes o
			     ON o.id=b.run_outcome_id
			    AND o.tenant_id=b.tenant_id
			    AND o.user_id=b.user_id
			    AND o.task_id=b.task_id
			   JOIN executive_brief_artifacts a
			     ON a.brief_snapshot_id=b.id
			    AND a.tenant_id=b.tenant_id
			    AND a.user_id=b.user_id
			    AND a.task_id=b.task_id
			  WHERE b.id=$4 AND b.tenant_id=$1 AND b.user_id=$2
			    AND b.task_id=$3`,
			tenantID, userID, taskID, id,
		).Scan(&briefPayload, &artifactPayload,
			&sourceCoverage, &processing)
		if errors.Is(err, pgx.ErrNoRows) {
			return GroundedBriefContextV1{}, types.NewAppError(
				types.CodeNotFound, "执行简报不存在", nil)
		}
		if err != nil {
			return GroundedBriefContextV1{},
				briefFeedDBError("读取执行简报追问依据", err)
		}
		brief, err := decodeGroundedBriefV1(
			briefPayload, tenantID, userID, taskID, id)
		if err != nil {
			return GroundedBriefContextV1{}, err
		}
		var artifact types.ExecutiveBriefArtifactV1
		if json.Unmarshal(artifactPayload, &artifact) != nil ||
			artifact.Validate() != nil ||
			artifact.BriefSnapshotID != id ||
			artifact.TenantID != tenantID || artifact.UserID != userID ||
			artifact.TaskID != taskID {
			return GroundedBriefContextV1{},
				periodicIntegrityErrorV1()
		}
		evidence, err := projectGroundedEvidenceBriefV1(brief)
		if err != nil {
			return GroundedBriefContextV1{}, err
		}
		out = GroundedBriefContextV1{
			Kind: kind, ID: id, SourceCoverage: sourceCoverage,
			Processing:     processing,
			GenerationMode: artifact.GenerationMode,
			Content:        artifact.Content,
			Evidence:       []GroundedEvidenceBriefV1{evidence},
		}
	} else {
		var reportPayload []byte
		err = tx.QueryRow(ctx,
			`SELECT payload FROM periodic_brief_reports
			  WHERE id=$4 AND tenant_id=$1 AND user_id=$2 AND task_id=$3`,
			tenantID, userID, taskID, id).Scan(&reportPayload)
		if errors.Is(err, pgx.ErrNoRows) {
			return GroundedBriefContextV1{}, types.NewAppError(
				types.CodeNotFound, "周期报告不存在", nil)
		}
		if err != nil {
			return GroundedBriefContextV1{},
				briefFeedDBError("读取周期报告追问依据", err)
		}
		var report types.PeriodicBriefReportV1
		if json.Unmarshal(reportPayload, &report) != nil ||
			report.Validate() != nil || report.ID != id ||
			report.TenantID != tenantID || report.UserID != userID ||
			report.TaskID != taskID {
			return GroundedBriefContextV1{},
				periodicIntegrityErrorV1()
		}
		out = GroundedBriefContextV1{
			Kind: kind, ID: id, Cadence: report.Cadence,
			PeriodStart:    report.PeriodStart.Format(time.RFC3339Nano),
			PeriodEnd:      report.PeriodEnd.Format(time.RFC3339Nano),
			SourceCoverage: report.SourceCoverage,
			Processing:     report.Processing,
			GenerationMode: report.GenerationMode,
			Content:        report.Content,
			Evidence:       make([]GroundedEvidenceBriefV1, len(report.Inputs)),
		}
		for index, input := range report.Inputs {
			var briefPayload []byte
			var payloadDigest string
			err := tx.QueryRow(ctx,
				`SELECT payload,payload_digest FROM brief_snapshots
				  WHERE id=$4 AND tenant_id=$1 AND user_id=$2
				    AND task_id=$3`,
				tenantID, userID, taskID, input.BriefID,
			).Scan(&briefPayload, &payloadDigest)
			if err != nil || payloadDigest != input.Digest {
				return GroundedBriefContextV1{},
					periodicIntegrityErrorV1()
			}
			brief, err := decodeGroundedBriefV1(
				briefPayload, tenantID, userID, taskID, input.BriefID)
			if err != nil {
				return GroundedBriefContextV1{}, err
			}
			out.Evidence[index], err =
				projectGroundedEvidenceBriefV1(brief)
			if err != nil {
				return GroundedBriefContextV1{}, err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return GroundedBriefContextV1{},
			briefFeedDBError("提交简报追问依据读取", err)
	}
	return out, nil
}
