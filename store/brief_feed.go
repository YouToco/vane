package store

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

const briefFeedCursorVersionV1 = 1
const maxBriefFeedPageSizeV1 = 20
const maxBriefFeedPayloadBytesV1 = 40 << 20

// TaskBriefQuery pages immutable whole Briefs. A Brief is the pagination atom:
// no page can split one run's ranked insight set.
type TaskBriefQuery struct {
	PageSize  int
	PageToken string
}

// TaskBriefInsightV1 is the frozen insight plus the same current feedback
// state projected onto Feishu cards. Feedback is intentionally not part of the
// immutable Brief digest.
type TaskBriefInsightV1 struct {
	ID            int64                         `json:"id"`
	RankPosition  int                           `json:"rank_position"`
	Title         string                        `json:"title"`
	BodyMD        string                        `json:"body_md"`
	SourceTitle   string                        `json:"source_title"`
	SourceURL     string                        `json:"source_url"`
	PublishedAt   *time.Time                    `json:"published_at,omitempty"`
	DiscoveredAt  time.Time                     `json:"discovered_at"`
	Structured    *TaskBriefStructuredInsightV1 `json:"structured,omitempty"`
	EventEvidence *TaskBriefEventEvidenceV1     `json:"event_evidence,omitempty"`
	Feedback      TaskBriefFeedbackStateV1      `json:"feedback"`
}

// TaskBriefStructuredInsightV1 deliberately omits the internal evidence
// digest. Readers receive the verified claims and excerpts, not a fingerprint
// of private source body bytes.
type TaskBriefStructuredInsightV1 struct {
	SchemaVersion    string                    `json:"schema_version"`
	BodyMD           string                    `json:"body_md"`
	WhatChanged      string                    `json:"what_changed"`
	WhyItMatters     string                    `json:"why_it_matters"`
	ImportanceReason string                    `json:"importance_reason"`
	Claims           []types.StructuredClaimV1 `json:"claims"`
}

// TaskBriefEventEvidenceV1 is the public, channel-neutral projection of the
// inventory metadata already frozen in the immutable Brief. It deliberately
// omits provenance, digests, database IDs and raw evidence bodies.
type TaskBriefEventEvidenceV1 struct {
	SchemaVersion string                      `json:"schema_version"`
	Sources       []TaskBriefEvidenceSourceV1 `json:"sources"`
}

type TaskBriefEvidenceSourceV1 struct {
	Ref          string     `json:"ref"`
	Title        string     `json:"title"`
	SourceTitle  string     `json:"source_title"`
	Platform     string     `json:"platform"`
	SourceURL    string     `json:"source_url"`
	PublishedAt  *time.Time `json:"published_at,omitempty"`
	DiscoveredAt time.Time  `json:"discovered_at"`
}

type TaskBriefFeedbackStateV1 struct {
	Preference        string `json:"preference,omitempty"`
	Misjudged         bool   `json:"misjudged"`
	DeepDiveRequested bool   `json:"deep_dive_requested"`
}

// TaskBriefItemV1 is the reader-facing projection of one canonical content run.
type TaskBriefItemV1 struct {
	ID             int64                   `json:"id"`
	PushBatchID    int64                   `json:"push_batch_id"`
	GeneratedAt    time.Time               `json:"generated_at"`
	SourceCoverage types.RunCompletenessV1 `json:"source_coverage"`
	Processing     types.RunCompletenessV1 `json:"processing"`
	Insights       []TaskBriefInsightV1    `json:"insights"`
}

// TaskLatestCheckV1 is independent from the newest non-empty Brief. Quiet,
// failed, and interrupted runs advance this projection without erasing the
// previous useful Brief.
type TaskLatestCheckV1 struct {
	FinalizedAt    time.Time               `json:"finalized_at"`
	Result         types.RunResultV1       `json:"result"`
	SourceCoverage types.RunCompletenessV1 `json:"source_coverage"`
	Processing     types.RunCompletenessV1 `json:"processing"`
	FailureCode    string                  `json:"failure_code,omitempty"`
}

// TaskBriefPageV1 is one task-scoped Brief page plus the latest terminal check.
type TaskBriefPageV1 struct {
	Items         []TaskBriefItemV1  `json:"items"`
	Total         int64              `json:"total"`
	NextPageToken string             `json:"next_page_token,omitempty"`
	LatestCheck   *TaskLatestCheckV1 `json:"latest_check,omitempty"`
}

type briefFeedCursorV1 struct {
	Version     int       `json:"v"`
	TaskID      string    `json:"task_id"`
	GeneratedAt time.Time `json:"generated_at"`
	BriefID     int64     `json:"brief_id"`
}

// ListTaskBriefsV1 reads canonical Briefs under an exact tenant/user/task
// boundary. It re-validates immutable payload digests before returning data and
// attaches feedback only after the page's delivery IDs have been established.
func (s *Store) ListTaskBriefsV1(
	ctx context.Context,
	tenantID int64,
	userID int64,
	taskID string,
	q TaskBriefQuery,
) (TaskBriefPageV1, error) {
	if tenantID <= 0 || userID <= 0 || taskID == "" ||
		taskID != strings.TrimSpace(taskID) || len(taskID) > 255 {
		return TaskBriefPageV1{}, types.NewAppError(
			types.CodeValidation, "任务简报范围无效", nil)
	}
	pageSize := clampBriefFeedPageSizeV1(q.PageSize)
	var cursor *briefFeedCursorV1
	if q.PageToken != "" {
		decoded, err := decodeBriefFeedCursorV1(q.PageToken, taskID)
		if err != nil {
			return TaskBriefPageV1{}, err
		}
		cursor = &decoded
	}

	tx, err := s.beginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	})
	if err != nil {
		return TaskBriefPageV1{}, briefFeedDBError("开启任务简报读取事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(
		ctx, `SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		return TaskBriefPageV1{}, briefFeedDBError("固定任务简报读取路径", err)
	}

	tenantExists, err := lockTenantAdmissionRootShared(ctx, tx, tenantID)
	if err != nil {
		return TaskBriefPageV1{}, briefFeedDBError(
			"锁定任务简报租户读取准入", err)
	}
	if !tenantExists {
		return TaskBriefPageV1{}, types.NewAppError(
			types.CodeNotFound, "任务不存在", nil)
	}

	// The shared tenant-admission root keeps purge outside this section.
	// Membership then schedule matches task-creation's established row-lock
	// order. Together, both locks make authorization and the page read one
	// operation: revocation/deletion either wins before these checks or waits
	// for this already-authorized read to finish.
	var admitted bool
	err = tx.QueryRow(ctx,
		`SELECT true
		   FROM memberships
		  WHERE tenant_id=$1 AND user_id=$2
		  FOR KEY SHARE`,
		tenantID, userID,
	).Scan(&admitted)
	if errors.Is(err, pgx.ErrNoRows) {
		return TaskBriefPageV1{}, types.NewAppError(
			types.CodeNotFound, "任务不存在", nil)
	}
	if err != nil {
		return TaskBriefPageV1{}, briefFeedDBError("校验任务简报成员关系", err)
	}
	if !admitted {
		return TaskBriefPageV1{}, types.NewAppError(
			types.CodeNotFound, "任务不存在", nil)
	}
	admitted = false
	err = tx.QueryRow(ctx,
		`SELECT true
		   FROM schedules
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3
		  FOR KEY SHARE`,
		tenantID, userID, taskID,
	).Scan(&admitted)
	if errors.Is(err, pgx.ErrNoRows) {
		return TaskBriefPageV1{}, types.NewAppError(
			types.CodeNotFound, "任务不存在", nil)
	}
	if err != nil {
		return TaskBriefPageV1{}, briefFeedDBError("校验任务简报归属", err)
	}
	if !admitted {
		return TaskBriefPageV1{}, types.NewAppError(
			types.CodeNotFound, "任务不存在", nil)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10),
		strconv.FormatInt(userID, 10),
	); err != nil {
		return TaskBriefPageV1{}, briefFeedDBError("设置任务简报读取范围", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_brief_reader`); err != nil {
		return TaskBriefPageV1{}, briefFeedDBError("进入任务简报读取角色", err)
	}

	page := TaskBriefPageV1{Items: []TaskBriefItemV1{}}
	if err := tx.QueryRow(ctx,
		`SELECT count(*)
		   FROM brief_snapshots
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		tenantID, userID, taskID,
	).Scan(&page.Total); err != nil {
		return TaskBriefPageV1{}, briefFeedDBError("统计任务简报", err)
	}

	var latest TaskLatestCheckV1
	err = tx.QueryRow(ctx,
		`SELECT finalized_at,result,source_coverage,processing,failure_code
		   FROM task_run_outcomes
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		    AND status='finalized'
		  ORDER BY finalized_at DESC,id DESC
		  LIMIT 1`,
		tenantID, userID, taskID,
	).Scan(
		&latest.FinalizedAt, &latest.Result, &latest.SourceCoverage,
		&latest.Processing, &latest.FailureCode,
	)
	if err == nil {
		latest.FinalizedAt = latest.FinalizedAt.Round(0).UTC().Truncate(time.Microsecond)
		page.LatestCheck = &latest
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return TaskBriefPageV1{}, briefFeedDBError("读取任务最近检查", err)
	}

	args := []any{tenantID, userID, taskID}
	cursorSQL := ""
	if cursor != nil {
		args = append(args, cursor.GeneratedAt, cursor.BriefID)
		cursorSQL = fmt.Sprintf(
			" AND (bs.generated_at,bs.id)<($%d,$%d)",
			len(args)-1, len(args),
		)
	}
	args = append(args, pageSize+1)
	rows, err := tx.Query(ctx,
		`SELECT bs.id,bs.run_outcome_id,bs.run_snapshot_id,bs.push_batch_id,
		        bs.schema_version,bs.request_digest,bs.payload,
		        bs.payload_digest,bs.insight_count,bs.generated_at,
		        o.status,o.result,o.source_coverage,o.processing
		   FROM brief_snapshots bs
		   JOIN task_run_outcomes o
		     ON o.id=bs.run_outcome_id
		    AND o.tenant_id=bs.tenant_id
		    AND o.user_id=bs.user_id
		    AND o.task_id=bs.task_id
		  WHERE bs.tenant_id=$1 AND bs.user_id=$2 AND bs.task_id=$3`+
			cursorSQL+
			fmt.Sprintf(
				` ORDER BY bs.generated_at DESC,bs.id DESC LIMIT $%d`,
				len(args),
			),
		args...,
	)
	if err != nil {
		return TaskBriefPageV1{}, briefFeedDBError("读取任务简报页", err)
	}
	defer rows.Close()
	totalPayloadBytes := 0
	hasMoreByBytes := false
	for rows.Next() {
		var (
			briefID, outcomeID, snapshotID, batchID int64
			schemaVersion, requestDigest            string
			payload, canonicalPayload               []byte
			payloadDigest                           string
			insightCount                            int
			generatedAt                             time.Time
			outcomeStatus                           string
			outcomeResult                           types.RunResultV1
			sourceCoverage                          types.RunCompletenessV1
			processing                              types.RunCompletenessV1
		)
		if err := rows.Scan(
			&briefID, &outcomeID, &snapshotID, &batchID,
			&schemaVersion, &requestDigest, &payload, &payloadDigest,
			&insightCount, &generatedAt, &outcomeStatus, &outcomeResult,
			&sourceCoverage, &processing,
		); err != nil {
			return TaskBriefPageV1{}, briefFeedDBError("扫描任务简报页", err)
		}
		if len(page.Items) > 0 &&
			totalPayloadBytes+len(payload) > maxBriefFeedPayloadBytesV1 {
			hasMoreByBytes = true
			break
		}
		var brief types.BriefV1
		if err := json.Unmarshal(payload, &brief); err != nil {
			return TaskBriefPageV1{}, types.NewAppError(
				types.CodeInternal, "任务简报完整性校验失败", nil)
		}
		canonicalPayload, err = json.Marshal(brief)
		computedRequest, requestErr := brief.BriefDraftV1.RequestDigest()
		if err != nil || requestErr != nil ||
			!bytes.Equal(canonicalPayload, payload) ||
			brief.Validate() != nil || brief.ID != briefID ||
			brief.RunOutcomeID != outcomeID ||
			brief.RunSnapshotID != snapshotID ||
			brief.PushBatchID != batchID ||
			brief.SchemaVersion != schemaVersion ||
			computedRequest != requestDigest ||
			brief.Digest != payloadDigest ||
			len(brief.Insights) != insightCount ||
			!brief.GeneratedAt.Equal(generatedAt) ||
			brief.TenantID != tenantID || brief.UserID != userID ||
			brief.TaskID != taskID ||
			outcomeStatus != "finalized" ||
			outcomeResult != types.RunResultContent {
			return TaskBriefPageV1{}, types.NewAppError(
				types.CodeInternal, "任务简报完整性校验失败", nil)
		}
		totalPayloadBytes += len(payload)
		page.Items = append(page.Items, TaskBriefItemV1{
			ID: brief.ID, PushBatchID: brief.PushBatchID,
			GeneratedAt:    brief.GeneratedAt,
			SourceCoverage: sourceCoverage, Processing: processing,
			Insights: make([]TaskBriefInsightV1, len(brief.Insights)),
		})
		for i := range brief.Insights {
			frozen := brief.Insights[i]
			projected := TaskBriefInsightV1{
				ID: frozen.ID, RankPosition: frozen.RankPosition,
				Title: frozen.Title, BodyMD: frozen.BodyMD,
				SourceTitle: frozen.SourceTitle, SourceURL: frozen.SourceURL,
				PublishedAt:  frozen.PublishedAt,
				DiscoveredAt: frozen.DiscoveredAt,
			}
			if frozen.Structured != nil {
				projected.Structured = projectTaskBriefStructuredInsightV1(
					frozen.Structured)
			}
			eventEvidence, projectErr :=
				projectTaskBriefEventEvidenceV1(frozen)
			if projectErr != nil {
				return TaskBriefPageV1{}, types.NewAppError(
					types.CodeInternal,
					"任务简报证据映射完整性校验失败", nil)
			}
			projected.EventEvidence = eventEvidence
			page.Items[len(page.Items)-1].Insights[i] = projected
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return TaskBriefPageV1{}, briefFeedDBError("遍历任务简报页", err)
	}
	rows.Close()

	trimmedItems, hasMore := trimBriefFeedPageV1(
		page.Items, pageSize, hasMoreByBytes)
	page.Items = trimmedItems
	if err := attachTaskBriefFeedbacksV1(
		ctx, tx, tenantID, userID, page.Items); err != nil {
		return TaskBriefPageV1{}, err
	}
	if hasMore {
		last := page.Items[len(page.Items)-1]
		token, err := encodeBriefFeedCursorV1(taskID, last.GeneratedAt, last.ID)
		if err != nil {
			return TaskBriefPageV1{}, types.NewAppError(
				types.CodeInternal, "生成任务简报分页游标失败", err)
		}
		page.NextPageToken = token
	}
	if err := tx.Commit(ctx); err != nil {
		return TaskBriefPageV1{}, briefFeedDBError("提交任务简报读取", err)
	}
	return page, nil
}

func projectTaskBriefEventEvidenceV1(
	insight types.InsightV1,
) (*TaskBriefEventEvidenceV1, error) {
	if insight.EventEvidence == nil {
		return nil, nil
	}
	sources, err := types.ProjectInsightEvidenceSourcesV1(insight)
	if err != nil {
		return nil, err
	}
	projected := &TaskBriefEventEvidenceV1{
		SchemaVersion: insight.EventEvidence.SchemaVersion,
		Sources: make(
			[]TaskBriefEvidenceSourceV1,
			len(sources),
		),
	}
	for index, source := range sources {
		projected.Sources[index] = TaskBriefEvidenceSourceV1{
			Ref: source.Ref, Title: source.Title,
			SourceTitle: source.SourceTitle, Platform: source.Platform,
			SourceURL: source.SourceURL, PublishedAt: source.PublishedAt,
			DiscoveredAt: source.DiscoveredAt,
		}
	}
	return projected, nil
}

func projectTaskBriefStructuredInsightV1(
	frozen *types.StructuredInsightV1,
) *TaskBriefStructuredInsightV1 {
	if frozen == nil {
		return nil
	}
	return &TaskBriefStructuredInsightV1{
		SchemaVersion:    frozen.SchemaVersion,
		BodyMD:           frozen.BodyMD,
		WhatChanged:      frozen.WhatChanged,
		WhyItMatters:     frozen.WhyItMatters,
		ImportanceReason: frozen.ImportanceReason,
		// Reader JSON always uses [] for zero claims. Older frozen payloads
		// and body-only fallback legitimately carry a nil slice.
		Claims: append([]types.StructuredClaimV1{}, frozen.Claims...),
	}
}

func trimBriefFeedPageV1(
	items []TaskBriefItemV1,
	pageSize int,
	hasMoreByBytes bool,
) ([]TaskBriefItemV1, bool) {
	hasMore := hasMoreByBytes || len(items) > pageSize
	if len(items) > pageSize {
		items = items[:pageSize]
	}
	return items, hasMore
}

func clampBriefFeedPageSizeV1(value int) int {
	switch {
	case value <= 0:
		return 10
	case value > maxBriefFeedPageSizeV1:
		return maxBriefFeedPageSizeV1
	default:
		return value
	}
}

func attachTaskBriefFeedbacksV1(
	ctx context.Context,
	tx pgx.Tx,
	tenantID int64,
	userID int64,
	items []TaskBriefItemV1,
) error {
	ids := make([]int64, 0)
	locations := make(map[int64][][2]int)
	for briefIndex := range items {
		for insightIndex := range items[briefIndex].Insights {
			id := items[briefIndex].Insights[insightIndex].ID
			if _, exists := locations[id]; !exists {
				ids = append(ids, id)
			}
			locations[id] = append(
				locations[id], [2]int{briefIndex, insightIndex})
		}
	}
	if len(ids) == 0 {
		return nil
	}
	rows, err := tx.Query(ctx,
		`WITH subject_epoch AS (
		   SELECT CASE WHEN EXISTS (
		     SELECT 1 FROM profiles WHERE tenant_id=$1 AND user_id=$2
		   ) THEN COALESCE((
		     SELECT active_epoch FROM profile_claim_states
		      WHERE tenant_id=$1 AND user_id=$2
		   ),-1) ELSE 0 END AS profile_epoch
		 )
		 SELECT target.delivery_id,COALESCE(preference.action,''),
		        EXISTS (
		            SELECT 1 FROM feedbacks f
		            CROSS JOIN subject_epoch e
		             WHERE f.tenant_id=$1 AND f.user_id=$2
		               AND f.delivery_id=target.delivery_id
		               AND f.action='misjudged'
		               AND f.profile_epoch=e.profile_epoch
		        ),
		        EXISTS (
		            SELECT 1 FROM feedbacks f
		            CROSS JOIN subject_epoch e
		             WHERE f.tenant_id=$1 AND f.user_id=$2
		               AND f.delivery_id=target.delivery_id
		               AND f.action='deep_dive'
		               AND f.profile_epoch=e.profile_epoch
		        )
		   FROM unnest($3::bigint[]) AS target(delivery_id)
		   LEFT JOIN LATERAL (
		       SELECT f.action
		         FROM feedbacks f
		         CROSS JOIN subject_epoch e
		        WHERE f.tenant_id=$1 AND f.user_id=$2
		          AND f.delivery_id=target.delivery_id
		          AND f.action IN ('interested','not_interested')
		          AND f.profile_epoch=e.profile_epoch
		        ORDER BY f.created_at DESC,f.id DESC
		        LIMIT 1
		   ) preference ON true`,
		tenantID, userID, ids,
	)
	if err != nil {
		return briefFeedDBError("读取任务简报反馈", err)
	}
	defer rows.Close()
	for rows.Next() {
		var deliveryID int64
		var feedback TaskBriefFeedbackStateV1
		if err := rows.Scan(
			&deliveryID, &feedback.Preference, &feedback.Misjudged,
			&feedback.DeepDiveRequested,
		); err != nil {
			return briefFeedDBError("扫描任务简报反馈", err)
		}
		for _, location := range locations[deliveryID] {
			briefIndex, insightIndex := location[0], location[1]
			items[briefIndex].Insights[insightIndex].Feedback = feedback
		}
	}
	if err := rows.Err(); err != nil {
		return briefFeedDBError("遍历任务简报反馈", err)
	}
	return nil
}

func encodeBriefFeedCursorV1(
	taskID string, generatedAt time.Time, briefID int64,
) (string, error) {
	cursor := briefFeedCursorV1{
		Version: briefFeedCursorVersionV1, TaskID: taskID,
		GeneratedAt: generatedAt.Round(0).UTC().Truncate(time.Microsecond),
		BriefID:     briefID,
	}
	raw, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeBriefFeedCursorV1(
	token string, taskID string,
) (briefFeedCursorV1, error) {
	if len(token) > 2048 {
		return briefFeedCursorV1{}, briefFeedCursorErrorV1()
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return briefFeedCursorV1{}, briefFeedCursorErrorV1()
	}
	var cursor briefFeedCursorV1
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil ||
		cursor.Version != briefFeedCursorVersionV1 ||
		cursor.TaskID != taskID || cursor.BriefID <= 0 ||
		cursor.GeneratedAt.IsZero() ||
		cursor.GeneratedAt != cursor.GeneratedAt.Round(0).UTC().Truncate(time.Microsecond) {
		return briefFeedCursorV1{}, briefFeedCursorErrorV1()
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return briefFeedCursorV1{}, briefFeedCursorErrorV1()
	}
	return cursor, nil
}

func briefFeedCursorErrorV1() error {
	return types.NewAppError(
		types.CodeValidation, "无效的任务简报分页游标", nil)
}

func briefFeedDBError(message string, err error) error {
	return types.NewAppError(types.CodeDatabase, message, err)
}
