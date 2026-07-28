package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

type BriefReportModeV1 string
type BriefReportCadenceV1 string
type BriefReportDeliveryV1 string

const (
	BriefReportModeAuto   BriefReportModeV1 = "auto"
	BriefReportModeManual BriefReportModeV1 = "manual"

	BriefReportCadenceDaily   BriefReportCadenceV1 = "daily"
	BriefReportCadenceWeekly  BriefReportCadenceV1 = "weekly"
	BriefReportCadenceMonthly BriefReportCadenceV1 = "monthly"

	BriefReportDeliveryImportant BriefReportDeliveryV1 = "important"
	BriefReportDeliveryAlways    BriefReportDeliveryV1 = "always"
	BriefReportDeliveryWebOnly   BriefReportDeliveryV1 = "web_only"
)

type BriefReportSettingsV1 struct {
	Mode      BriefReportModeV1     `json:"mode"`
	Cadence   BriefReportCadenceV1  `json:"cadence"`
	Delivery  BriefReportDeliveryV1 `json:"delivery"`
	Timezone  string                `json:"timezone"`
	UpdatedAt *time.Time            `json:"updated_at,omitempty"`
}

type BriefReportSettingsPatchV1 struct {
	Mode     *BriefReportModeV1     `json:"mode,omitempty"`
	Cadence  *BriefReportCadenceV1  `json:"cadence,omitempty"`
	Delivery *BriefReportDeliveryV1 `json:"delivery,omitempty"`
}

func (s BriefReportSettingsV1) valid() bool {
	return (s.Mode == BriefReportModeAuto || s.Mode == BriefReportModeManual) &&
		(s.Cadence == BriefReportCadenceDaily ||
			s.Cadence == BriefReportCadenceWeekly ||
			s.Cadence == BriefReportCadenceMonthly) &&
		(s.Delivery == BriefReportDeliveryImportant ||
			s.Delivery == BriefReportDeliveryAlways ||
			s.Delivery == BriefReportDeliveryWebOnly) &&
		s.Timezone != "" && len(s.Timezone) <= 255
}

func (p BriefReportSettingsPatchV1) valid() bool {
	if p.Mode == nil && p.Cadence == nil && p.Delivery == nil {
		return false
	}
	if p.Mode != nil && *p.Mode != BriefReportModeAuto &&
		*p.Mode != BriefReportModeManual {
		return false
	}
	if p.Cadence != nil && *p.Cadence != BriefReportCadenceDaily &&
		*p.Cadence != BriefReportCadenceWeekly &&
		*p.Cadence != BriefReportCadenceMonthly {
		return false
	}
	if p.Delivery != nil && *p.Delivery != BriefReportDeliveryImportant &&
		*p.Delivery != BriefReportDeliveryAlways &&
		*p.Delivery != BriefReportDeliveryWebOnly {
		return false
	}
	return true
}

type reportTaskScopeV1 struct {
	SpecJSON json.RawMessage
	Timezone string
	Cadence  BriefReportCadenceV1
}

func authorizeReportTaskV1(
	ctx context.Context,
	s *Store,
	tenantID, userID int64,
	taskID string,
	role string,
) (pgx.Tx, reportTaskScopeV1, error) {
	if tenantID <= 0 || userID <= 0 || taskID == "" ||
		taskID != strings.TrimSpace(taskID) || len(taskID) > 255 {
		return nil, reportTaskScopeV1{}, types.NewAppError(
			types.CodeValidation, "周期报告范围无效", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return nil, reportTaskScopeV1{}, briefFeedDBError(
			"开启周期报告事务", err)
	}
	fail := func(message string, cause error) (pgx.Tx, reportTaskScopeV1, error) {
		_ = tx.Rollback(ctx)
		return nil, reportTaskScopeV1{}, briefFeedDBError(message, cause)
	}
	if _, err := tx.Exec(ctx,
		`SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		return fail("固定周期报告读取路径", err)
	}
	var specJSON []byte
	err = tx.QueryRow(ctx,
		`SELECT s.spec_json
		   FROM memberships m
		   JOIN schedules s
		     ON s.tenant_id=m.tenant_id AND s.user_id=m.user_id
		  WHERE m.tenant_id=$1 AND m.user_id=$2 AND s.id=$3
		  FOR KEY SHARE OF m,s`,
		tenantID, userID, taskID,
	).Scan(&specJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		_ = tx.Rollback(ctx)
		return nil, reportTaskScopeV1{}, types.NewAppError(
			types.CodeNotFound, "任务不存在", nil)
	}
	if err != nil {
		return fail("校验周期报告任务归属", err)
	}
	scope, err := reportScopeFromSpecV1(specJSON)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, reportTaskScopeV1{}, err
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10),
		strconv.FormatInt(userID, 10),
	); err != nil {
		return fail("设置周期报告读取范围", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+role); err != nil {
		return fail("进入周期报告角色", err)
	}
	return tx, scope, nil
}

func reportScopeFromSpecV1(raw []byte) (reportTaskScopeV1, error) {
	var spec struct {
		TZ           string `json:"tz"`
		EverySeconds int64  `json:"every_seconds"`
		Cron         string `json:"cron"`
	}
	if json.Unmarshal(raw, &spec) != nil || spec.TZ == "" {
		return reportTaskScopeV1{}, types.NewAppError(
			types.CodeInternal, "任务时区无效", nil)
	}
	if _, err := time.LoadLocation(spec.TZ); err != nil {
		return reportTaskScopeV1{}, types.NewAppError(
			types.CodeInternal, "任务时区无效", nil)
	}
	cadence := BriefReportCadenceWeekly
	switch {
	case spec.EverySeconds > 0 && spec.EverySeconds <= 12*60*60:
		cadence = BriefReportCadenceDaily
	case spec.EverySeconds > 0 && spec.EverySeconds <= 4*24*60*60:
		cadence = BriefReportCadenceWeekly
	case spec.EverySeconds > 0:
		cadence = BriefReportCadenceMonthly
	case strings.Contains(spec.Cron, "* * *"):
		cadence = BriefReportCadenceDaily
	}
	return reportTaskScopeV1{
		SpecJSON: append(json.RawMessage(nil), raw...),
		Timezone: spec.TZ,
		Cadence:  cadence,
	}, nil
}

func (s *Store) GetBriefReportSettingsV1(
	ctx context.Context,
	tenantID, userID int64,
	taskID string,
) (BriefReportSettingsV1, error) {
	tx, scope, err := authorizeReportTaskV1(
		ctx, s, tenantID, userID, taskID, "vane_brief_reader")
	if err != nil {
		return BriefReportSettingsV1{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	settings := BriefReportSettingsV1{
		Mode: BriefReportModeAuto, Cadence: scope.Cadence,
		Delivery: BriefReportDeliveryImportant, Timezone: scope.Timezone,
	}
	var updatedAt time.Time
	err = tx.QueryRow(ctx,
		`SELECT mode,cadence,delivery,updated_at
		   FROM brief_report_settings
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		tenantID, userID, taskID,
	).Scan(&settings.Mode, &settings.Cadence, &settings.Delivery, &updatedAt)
	if err == nil {
		value := updatedAt.Round(0).UTC().Truncate(time.Microsecond)
		settings.UpdatedAt = &value
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return BriefReportSettingsV1{}, briefFeedDBError(
			"读取周期报告设置", err)
	}
	if !settings.valid() {
		return BriefReportSettingsV1{}, types.NewAppError(
			types.CodeInternal, "周期报告设置完整性校验失败", nil)
	}
	if err := tx.Commit(ctx); err != nil {
		return BriefReportSettingsV1{}, briefFeedDBError(
			"提交周期报告设置读取", err)
	}
	return settings, nil
}

func (s *Store) PatchBriefReportSettingsV1(
	ctx context.Context,
	tenantID, userID int64,
	taskID string,
	patch BriefReportSettingsPatchV1,
) (BriefReportSettingsV1, error) {
	if !patch.valid() {
		return BriefReportSettingsV1{}, types.NewAppError(
			types.CodeValidation, "周期报告设置无效", nil)
	}
	tx, scope, err := authorizeReportTaskV1(
		ctx, s, tenantID, userID, taskID, "vane_periodic_brief_writer")
	if err != nil {
		return BriefReportSettingsV1{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	mode, cadence, delivery := BriefReportModeAuto, scope.Cadence,
		BriefReportDeliveryImportant
	var storedUpdated time.Time
	err = tx.QueryRow(ctx,
		`SELECT mode,cadence,delivery,updated_at
		   FROM brief_report_settings
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		  FOR UPDATE`,
		tenantID, userID, taskID,
	).Scan(&mode, &cadence, &delivery, &storedUpdated)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return BriefReportSettingsV1{}, briefFeedDBError(
			"锁定周期报告设置", err)
	}
	if patch.Mode != nil {
		mode = *patch.Mode
	}
	if patch.Cadence != nil {
		cadence = *patch.Cadence
	}
	if patch.Delivery != nil {
		delivery = *patch.Delivery
	}
	// Explicit cadence is a manual override even if the caller omitted mode.
	if patch.Cadence != nil && patch.Mode == nil {
		mode = BriefReportModeManual
	}
	var updatedAt time.Time
	err = tx.QueryRow(ctx,
		`INSERT INTO brief_report_settings (
		    tenant_id,user_id,task_id,mode,cadence,delivery,
		    cadence_changed_at,updated_at
		 ) VALUES ($1,$2,$3,$4,$5,$6,clock_timestamp(),clock_timestamp())
		 ON CONFLICT (tenant_id,user_id,task_id) DO UPDATE SET
		    mode=EXCLUDED.mode,cadence=EXCLUDED.cadence,
		    delivery=EXCLUDED.delivery,
		    auto_candidate=NULL,auto_candidate_streak=0,
		    cadence_changed_at=CASE
		        WHEN brief_report_settings.cadence IS DISTINCT FROM
		             EXCLUDED.cadence THEN clock_timestamp()
		        ELSE brief_report_settings.cadence_changed_at
		    END,
		    updated_at=clock_timestamp()
		 RETURNING updated_at`,
		tenantID, userID, taskID, mode, cadence, delivery,
	).Scan(&updatedAt)
	if err != nil {
		return BriefReportSettingsV1{}, briefFeedDBError(
			"保存周期报告设置", err)
	}
	settings := BriefReportSettingsV1{
		Mode: mode, Cadence: cadence, Delivery: delivery,
		Timezone: scope.Timezone,
	}
	value := updatedAt.Round(0).UTC().Truncate(time.Microsecond)
	settings.UpdatedAt = &value
	if !settings.valid() {
		return BriefReportSettingsV1{}, types.NewAppError(
			types.CodeInternal, "周期报告设置完整性校验失败", nil)
	}
	if err := tx.Commit(ctx); err != nil {
		return BriefReportSettingsV1{}, briefFeedDBError(
			"提交周期报告设置修改", err)
	}
	return settings, nil
}

type PeriodicBriefReportQueryV1 struct {
	Cadence  BriefReportCadenceV1
	PageSize int
	Cursor   string
}

type PeriodicBriefReportPageV1 struct {
	Items      []types.PeriodicBriefReportV1 `json:"items"`
	NextCursor string                        `json:"next_cursor,omitempty"`
}

type PeriodicBriefIntentStatusV1 string

const (
	PeriodicBriefIntentPrepared  PeriodicBriefIntentStatusV1 = "prepared"
	PeriodicBriefIntentRunning   PeriodicBriefIntentStatusV1 = "running"
	PeriodicBriefIntentFinalized PeriodicBriefIntentStatusV1 = "finalized"
	PeriodicBriefIntentFallback  PeriodicBriefIntentStatusV1 = "fallback"
)

type PeriodicBriefIntentV1 struct {
	ID             int64                       `json:"id"`
	TenantID       int64                       `json:"tenant_id"`
	UserID         int64                       `json:"user_id"`
	TaskID         string                      `json:"task_id"`
	Cadence        BriefReportCadenceV1        `json:"cadence"`
	Timezone       string                      `json:"timezone"`
	PeriodStart    time.Time                   `json:"period_start"`
	PeriodEnd      time.Time                   `json:"period_end"`
	WorkflowID     string                      `json:"workflow_id"`
	TemporalRunID  string                      `json:"temporal_run_id,omitempty"`
	InputBriefIDs  []int64                     `json:"input_brief_ids"`
	InputDigest    string                      `json:"input_digest"`
	RunOutcomeIDs  []int64                     `json:"run_outcome_ids"`
	OutcomeDigest  string                      `json:"outcome_digest"`
	SourceCoverage types.RunCompletenessV1     `json:"source_coverage"`
	Processing     types.RunCompletenessV1     `json:"processing"`
	PartialReason  string                      `json:"partial_reason,omitempty"`
	Status         PeriodicBriefIntentStatusV1 `json:"status"`
	CreatedAt      time.Time                   `json:"created_at"`
	StartedAt      *time.Time                  `json:"started_at,omitempty"`
	FinalizedAt    *time.Time                  `json:"finalized_at,omitempty"`
}

type PeriodicBriefIntentInputsV1 struct {
	Intent PeriodicBriefIntentV1 `json:"intent"`
	Briefs []types.BriefV1       `json:"briefs"`
}

type PeriodicReportScheduleV1 struct {
	TenantID int64
	UserID   int64
	TaskID   string
	Settings BriefReportSettingsV1
}

func (s *Store) GetPeriodicReportScheduleV1(
	ctx context.Context,
	taskID string,
) (PeriodicReportScheduleV1, error) {
	if taskID == "" || taskID != strings.TrimSpace(taskID) ||
		len(taskID) > 255 {
		return PeriodicReportScheduleV1{}, types.NewAppError(
			types.CodeValidation, "周期报告任务无效", nil)
	}
	var candidate PeriodicReportScheduleV1
	var specJSON []byte
	err := s.pool.QueryRow(ctx,
		`SELECT s.tenant_id,s.user_id,s.id,s.spec_json
		   FROM schedules s
		   JOIN memberships m
		     ON m.tenant_id=s.tenant_id AND m.user_id=s.user_id
		  WHERE s.id=$1 AND s.execution_mode='compiled'
		    AND s.status IN ('active','paused')`,
		taskID,
	).Scan(
		&candidate.TenantID, &candidate.UserID,
		&candidate.TaskID, &specJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return PeriodicReportScheduleV1{}, types.NewAppError(
			types.CodeNotFound, "周期报告任务不存在", nil)
	}
	if err != nil {
		return PeriodicReportScheduleV1{}, briefFeedDBError(
			"读取周期报告任务", err)
	}
	scope, err := reportScopeFromSpecV1(specJSON)
	if err != nil {
		return PeriodicReportScheduleV1{}, err
	}
	candidate.Settings = BriefReportSettingsV1{
		Mode: BriefReportModeAuto, Cadence: scope.Cadence,
		Delivery: BriefReportDeliveryImportant, Timezone: scope.Timezone,
	}
	var updatedAt time.Time
	err = s.pool.QueryRow(ctx,
		`SELECT mode,cadence,delivery,updated_at
		   FROM brief_report_settings
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		candidate.TenantID, candidate.UserID, candidate.TaskID,
	).Scan(
		&candidate.Settings.Mode, &candidate.Settings.Cadence,
		&candidate.Settings.Delivery, &updatedAt)
	if err == nil {
		value := updatedAt.Round(0).UTC().Truncate(time.Microsecond)
		candidate.Settings.UpdatedAt = &value
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return PeriodicReportScheduleV1{}, briefFeedDBError(
			"读取周期报告任务设置", err)
	}
	return candidate, nil
}

func (s *Store) BindPeriodicBriefIntentRunV1(
	ctx context.Context,
	tenantID, userID, intentID int64,
	temporalRunID string,
) error {
	if tenantID <= 0 || userID <= 0 || intentID <= 0 ||
		temporalRunID == "" || len(temporalRunID) > 255 {
		return types.NewAppError(types.CodeValidation,
			"周期报告运行身份无效", nil)
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE periodic_brief_intents
		    SET temporal_run_id=$4
		  WHERE id=$3 AND tenant_id=$1 AND user_id=$2
		    AND (temporal_run_id IS NULL OR temporal_run_id=$4)`,
		tenantID, userID, intentID, temporalRunID)
	if err != nil {
		return briefFeedDBError("绑定周期报告运行身份", err)
	}
	if tag.RowsAffected() != 1 {
		return types.NewAppError(types.CodeConflict,
			"周期报告运行身份已不同", nil)
	}
	return nil
}

func PeriodicBriefWorkflowIDV1(
	tenantID, userID int64,
	taskID string,
	cadence BriefReportCadenceV1,
	periodStart time.Time,
) string {
	scope := fmt.Sprintf("%d:%d:%s:%s:%s", tenantID, userID, taskID,
		cadence, periodStart.Round(0).UTC().Format(time.RFC3339Nano))
	sum := sha256.Sum256([]byte(scope))
	return fmt.Sprintf("periodic-brief-v1-%x", sum[:])
}

func digestPeriodicIdentityV1(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("%x", sum[:]), nil
}

// PreparePeriodicBriefIntentV1 creates the durable period identity before a
// Temporal workflow may be started. Repeated triggers return the same row.
func (s *Store) PreparePeriodicBriefIntentV1(
	ctx context.Context,
	tenantID, userID int64,
	taskID string,
	cadence BriefReportCadenceV1,
	periodStart, periodEnd time.Time,
) (PeriodicBriefIntentV1, error) {
	if cadence != BriefReportCadenceDaily &&
		cadence != BriefReportCadenceWeekly &&
		cadence != BriefReportCadenceMonthly ||
		periodStart.IsZero() || periodEnd.IsZero() ||
		!periodStart.Before(periodEnd) {
		return PeriodicBriefIntentV1{}, types.NewAppError(
			types.CodeValidation, "周期报告区间无效", nil)
	}
	tx, scope, err := authorizeReportTaskV1(
		ctx, s, tenantID, userID, taskID, "vane_periodic_brief_writer")
	if err != nil {
		return PeriodicBriefIntentV1{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	periodStart = periodStart.Round(0).UTC().Truncate(time.Microsecond)
	periodEnd = periodEnd.Round(0).UTC().Truncate(time.Microsecond)
	type outcomeIdentity struct {
		ID             int64                   `json:"id"`
		Result         types.RunResultV1       `json:"result"`
		SourceCoverage types.RunCompletenessV1 `json:"source_coverage"`
		Processing     types.RunCompletenessV1 `json:"processing"`
	}
	outcomes := make([]outcomeIdentity, 0)
	rows, err := tx.Query(ctx,
		`SELECT id,result,source_coverage,processing
		   FROM task_run_outcomes
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		    AND status='finalized'
		    AND finalized_at >= $4 AND finalized_at < $5
		  ORDER BY finalized_at,id
		  LIMIT 2049`,
		tenantID, userID, taskID, periodStart, periodEnd)
	if err != nil {
		return PeriodicBriefIntentV1{}, briefFeedDBError(
			"读取周期运行覆盖", err)
	}
	for rows.Next() {
		var value outcomeIdentity
		if err := rows.Scan(
			&value.ID, &value.Result, &value.SourceCoverage,
			&value.Processing); err != nil {
			rows.Close()
			return PeriodicBriefIntentV1{}, briefFeedDBError(
				"扫描周期运行覆盖", err)
		}
		outcomes = append(outcomes, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return PeriodicBriefIntentV1{}, briefFeedDBError(
			"遍历周期运行覆盖", err)
	}
	rows.Close()
	partialReasons := make([]string, 0, 2)
	if len(outcomes) > 2048 {
		outcomes = outcomes[:2048]
		partialReasons = append(partialReasons, "运行覆盖超过上限")
	}
	sourceCoverage, processing := types.RunCompletenessComplete,
		types.RunCompletenessComplete
	outcomeIDs := make([]int64, len(outcomes))
	for index, outcome := range outcomes {
		outcomeIDs[index] = outcome.ID
		if outcome.SourceCoverage == types.RunCompletenessPartial {
			sourceCoverage = types.RunCompletenessPartial
		}
		if outcome.Processing == types.RunCompletenessPartial ||
			outcome.Result == types.RunResultFailed ||
			outcome.Result == types.RunResultInterrupted {
			processing = types.RunCompletenessPartial
		}
	}
	type briefIdentity struct {
		ID     int64  `json:"id"`
		Digest string `json:"digest"`
	}
	briefs := make([]briefIdentity, 0)
	rows, err = tx.Query(ctx,
		`SELECT id,payload_digest
		   FROM brief_snapshots
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		    AND generated_at >= $4 AND generated_at < $5
		  ORDER BY generated_at DESC,id DESC
		  LIMIT 21`,
		tenantID, userID, taskID, periodStart, periodEnd)
	if err != nil {
		return PeriodicBriefIntentV1{}, briefFeedDBError(
			"读取周期 Brief 输入", err)
	}
	for rows.Next() {
		var value briefIdentity
		if err := rows.Scan(&value.ID, &value.Digest); err != nil {
			rows.Close()
			return PeriodicBriefIntentV1{}, briefFeedDBError(
				"扫描周期 Brief 输入", err)
		}
		briefs = append(briefs, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return PeriodicBriefIntentV1{}, briefFeedDBError(
			"遍历周期 Brief 输入", err)
	}
	rows.Close()
	if len(briefs) > 20 {
		briefs = briefs[:20]
		processing = types.RunCompletenessPartial
		partialReasons = append(partialReasons, "简报输入超过 20 期")
	}
	briefIDs := make([]int64, len(briefs))
	for index, brief := range briefs {
		briefIDs[index] = brief.ID
	}
	inputDigest, err := digestPeriodicIdentityV1(briefs)
	if err != nil {
		return PeriodicBriefIntentV1{}, err
	}
	outcomeDigest, err := digestPeriodicIdentityV1(outcomes)
	if err != nil {
		return PeriodicBriefIntentV1{}, err
	}
	workflowID := PeriodicBriefWorkflowIDV1(
		tenantID, userID, taskID, cadence, periodStart)
	_, err = tx.Exec(ctx,
		`INSERT INTO periodic_brief_intents (
		    tenant_id,user_id,task_id,cadence,timezone,
		    period_start,period_end,workflow_id,input_brief_ids,input_digest,
		    run_outcome_ids,outcome_digest,source_coverage,processing,
		    partial_reason
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		 ON CONFLICT (
		    tenant_id,user_id,task_id,cadence,period_start,period_end
		 ) DO NOTHING`,
		tenantID, userID, taskID, cadence, scope.Timezone,
		periodStart, periodEnd, workflowID, briefIDs, inputDigest,
		outcomeIDs, outcomeDigest, sourceCoverage, processing,
		strings.Join(partialReasons, "；"))
	if err != nil {
		return PeriodicBriefIntentV1{}, briefFeedDBError(
			"创建周期报告意图", err)
	}
	intent, err := scanPeriodicBriefIntentV1(tx.QueryRow(ctx,
		`SELECT id,tenant_id,user_id,task_id,cadence,timezone,
		        period_start,period_end,workflow_id,temporal_run_id,
		        input_brief_ids,input_digest,run_outcome_ids,outcome_digest,
		        source_coverage,processing,partial_reason,status,created_at,
		        started_at,finalized_at
		   FROM periodic_brief_intents
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		    AND cadence=$4 AND period_start=$5 AND period_end=$6
		  FOR UPDATE`,
		tenantID, userID, taskID, cadence, periodStart, periodEnd))
	if err != nil {
		return PeriodicBriefIntentV1{}, briefFeedDBError(
			"读取周期报告意图", err)
	}
	if intent.WorkflowID != workflowID ||
		intent.InputDigest != inputDigest ||
		intent.OutcomeDigest != outcomeDigest {
		return PeriodicBriefIntentV1{}, types.NewAppError(
			types.CodeConflict, "周期报告输入已封存且不同", nil)
	}
	if err := tx.Commit(ctx); err != nil {
		return PeriodicBriefIntentV1{}, briefFeedDBError(
			"提交周期报告意图", err)
	}
	return intent, nil
}

func scanPeriodicBriefIntentV1(row pgx.Row) (PeriodicBriefIntentV1, error) {
	var intent PeriodicBriefIntentV1
	var temporalRunID, partialReason *string
	err := row.Scan(
		&intent.ID, &intent.TenantID, &intent.UserID, &intent.TaskID,
		&intent.Cadence, &intent.Timezone, &intent.PeriodStart,
		&intent.PeriodEnd, &intent.WorkflowID, &temporalRunID,
		&intent.InputBriefIDs, &intent.InputDigest,
		&intent.RunOutcomeIDs, &intent.OutcomeDigest,
		&intent.SourceCoverage, &intent.Processing, &partialReason,
		&intent.Status, &intent.CreatedAt, &intent.StartedAt,
		&intent.FinalizedAt)
	if err != nil {
		return PeriodicBriefIntentV1{}, err
	}
	if temporalRunID != nil {
		intent.TemporalRunID = *temporalRunID
	}
	if partialReason != nil {
		intent.PartialReason = *partialReason
	}
	intent.PeriodStart = intent.PeriodStart.Round(0).UTC().Truncate(time.Microsecond)
	intent.PeriodEnd = intent.PeriodEnd.Round(0).UTC().Truncate(time.Microsecond)
	intent.CreatedAt = intent.CreatedAt.Round(0).UTC().Truncate(time.Microsecond)
	return intent, nil
}

func (s *Store) LoadPeriodicBriefIntentInputsV1(
	ctx context.Context,
	tenantID, userID, intentID int64,
) (PeriodicBriefIntentInputsV1, error) {
	if intentID <= 0 {
		return PeriodicBriefIntentInputsV1{}, types.NewAppError(
			types.CodeValidation, "周期报告意图无效", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return PeriodicBriefIntentInputsV1{}, briefFeedDBError(
			"开启周期报告输入事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		return PeriodicBriefIntentInputsV1{}, briefFeedDBError(
			"固定周期报告输入路径", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10),
		strconv.FormatInt(userID, 10)); err != nil {
		return PeriodicBriefIntentInputsV1{}, briefFeedDBError(
			"设置周期报告输入范围", err)
	}
	if _, err := tx.Exec(ctx,
		`SET LOCAL ROLE vane_periodic_brief_writer`); err != nil {
		return PeriodicBriefIntentInputsV1{}, briefFeedDBError(
			"进入周期报告输入角色", err)
	}
	intent, err := scanPeriodicBriefIntentV1(tx.QueryRow(ctx,
		`SELECT id,tenant_id,user_id,task_id,cadence,timezone,
		        period_start,period_end,workflow_id,temporal_run_id,
		        input_brief_ids,input_digest,run_outcome_ids,outcome_digest,
		        source_coverage,processing,partial_reason,status,created_at,
		        started_at,finalized_at
		   FROM periodic_brief_intents
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		  FOR KEY SHARE`,
		intentID, tenantID, userID))
	if errors.Is(err, pgx.ErrNoRows) {
		return PeriodicBriefIntentInputsV1{}, types.NewAppError(
			types.CodeNotFound, "周期报告不存在", nil)
	}
	if err != nil {
		return PeriodicBriefIntentInputsV1{}, briefFeedDBError(
			"读取周期报告意图", err)
	}
	inputs := PeriodicBriefIntentInputsV1{
		Intent: intent, Briefs: make([]types.BriefV1, 0, len(intent.InputBriefIDs))}
	for _, briefID := range intent.InputBriefIDs {
		var payload, payloadDigest []byte
		var digest string
		err := tx.QueryRow(ctx,
			`SELECT payload,payload_digest
			   FROM brief_snapshots
			  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND task_id=$4`,
			briefID, tenantID, userID, intent.TaskID,
		).Scan(&payload, &digest)
		if errors.Is(err, pgx.ErrNoRows) {
			return PeriodicBriefIntentInputsV1{}, periodicIntegrityErrorV1()
		}
		if err != nil {
			return PeriodicBriefIntentInputsV1{}, briefFeedDBError(
				"读取周期 Brief", err)
		}
		payloadDigest = []byte(digest)
		var brief types.BriefV1
		if json.Unmarshal(payload, &brief) != nil ||
			brief.Validate() != nil || brief.ID != briefID ||
			brief.TenantID != tenantID || brief.UserID != userID ||
			brief.TaskID != intent.TaskID ||
			brief.Digest != string(payloadDigest) {
			return PeriodicBriefIntentInputsV1{}, periodicIntegrityErrorV1()
		}
		inputs.Briefs = append(inputs.Briefs, brief)
	}
	identities := make([]struct {
		ID     int64  `json:"id"`
		Digest string `json:"digest"`
	}, len(inputs.Briefs))
	for index, brief := range inputs.Briefs {
		identities[index].ID = brief.ID
		identities[index].Digest = brief.Digest
	}
	computedInput, err := digestPeriodicIdentityV1(identities)
	if err != nil || computedInput != intent.InputDigest {
		return PeriodicBriefIntentInputsV1{}, periodicIntegrityErrorV1()
	}
	if err := tx.Commit(ctx); err != nil {
		return PeriodicBriefIntentInputsV1{}, briefFeedDBError(
			"提交周期报告输入读取", err)
	}
	return inputs, nil
}

type PeriodicSynthesisReceiptV1 struct {
	IntentID          int64
	RequestDigest     string
	Status            ExecutiveSynthesisStatusV1
	GenerationMode    types.ExecutiveGenerationModeV1
	Content           *types.ExecutiveBriefContentV1
	SpendingStartedAt *time.Time
	FinalizedAt       *time.Time
}

// ClaimPeriodicSynthesisSpendV1 atomically prepares and claims the one
// provider-call authority for an exact durable intent.
func (s *Store) ClaimPeriodicSynthesisSpendV1(
	ctx context.Context,
	tenantID, userID, intentID int64,
	requestDigest string,
) (PeriodicSynthesisReceiptV1, bool, error) {
	if intentID <= 0 || !validStoreDigestV1(requestDigest) {
		return PeriodicSynthesisReceiptV1{}, false,
			types.NewAppError(types.CodeValidation,
				"周期综合付费请求无效", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return PeriodicSynthesisReceiptV1{}, false,
			briefFeedDBError("开启周期综合事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		return PeriodicSynthesisReceiptV1{}, false,
			briefFeedDBError("固定周期综合路径", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10),
		strconv.FormatInt(userID, 10)); err != nil {
		return PeriodicSynthesisReceiptV1{}, false,
			briefFeedDBError("设置周期综合范围", err)
	}
	if _, err := tx.Exec(ctx,
		`SET LOCAL ROLE vane_periodic_brief_writer`); err != nil {
		return PeriodicSynthesisReceiptV1{}, false,
			briefFeedDBError("进入周期综合角色", err)
	}
	var taskID string
	err = tx.QueryRow(ctx,
		`SELECT task_id FROM periodic_brief_intents
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		  FOR UPDATE`,
		intentID, tenantID, userID).Scan(&taskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return PeriodicSynthesisReceiptV1{}, false,
			types.NewAppError(types.CodeNotFound,
				"周期报告不存在", nil)
	}
	if err != nil {
		return PeriodicSynthesisReceiptV1{}, false,
			briefFeedDBError("锁定周期报告意图", err)
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO periodic_synthesis_receipts (
		    intent_id,tenant_id,user_id,task_id,request_digest
		 ) VALUES ($1,$2,$3,$4,$5)
		 ON CONFLICT (intent_id) DO NOTHING`,
		intentID, tenantID, userID, taskID, requestDigest)
	if err != nil {
		return PeriodicSynthesisReceiptV1{}, false,
			briefFeedDBError("准备周期综合回执", err)
	}
	receipt, err := scanPeriodicSynthesisReceiptV1(tx.QueryRow(ctx,
		`SELECT intent_id,request_digest,status,generation_mode,
		        content_payload,spending_started_at,finalized_at
		   FROM periodic_synthesis_receipts
		  WHERE intent_id=$1 FOR UPDATE`, intentID))
	if err != nil {
		return PeriodicSynthesisReceiptV1{}, false,
			briefFeedDBError("读取周期综合回执", err)
	}
	if receipt.RequestDigest != requestDigest {
		return PeriodicSynthesisReceiptV1{}, false,
			types.NewAppError(types.CodeConflict,
				"周期综合请求已封存且不同", nil)
	}
	claimed := false
	if receipt.Status == ExecutiveSynthesisPrepared {
		tag, updateErr := tx.Exec(ctx,
			`UPDATE periodic_synthesis_receipts
			    SET status='spending',
			        spending_started_at=clock_timestamp()
			  WHERE intent_id=$1 AND status='prepared'`, intentID)
		if updateErr != nil {
			return PeriodicSynthesisReceiptV1{}, false,
				briefFeedDBError("领取周期综合付费权限", updateErr)
		}
		claimed = tag.RowsAffected() == 1
		_, updateErr = tx.Exec(ctx,
			`UPDATE periodic_brief_intents
			    SET status='running',started_at=clock_timestamp()
			  WHERE id=$1 AND status='prepared'`, intentID)
		if updateErr != nil {
			return PeriodicSynthesisReceiptV1{}, false,
				briefFeedDBError("标记周期报告运行中", updateErr)
		}
		receipt, err = scanPeriodicSynthesisReceiptV1(tx.QueryRow(ctx,
			`SELECT intent_id,request_digest,status,generation_mode,
			        content_payload,spending_started_at,finalized_at
			   FROM periodic_synthesis_receipts
			  WHERE intent_id=$1`, intentID))
		if err != nil {
			return PeriodicSynthesisReceiptV1{}, false,
				briefFeedDBError("重读周期综合回执", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return PeriodicSynthesisReceiptV1{}, false,
			briefFeedDBError("提交周期综合付费权限", err)
	}
	return receipt, claimed, nil
}

func scanPeriodicSynthesisReceiptV1(
	row pgx.Row,
) (PeriodicSynthesisReceiptV1, error) {
	var receipt PeriodicSynthesisReceiptV1
	var generation *string
	var payload []byte
	err := row.Scan(
		&receipt.IntentID, &receipt.RequestDigest, &receipt.Status,
		&generation, &payload, &receipt.SpendingStartedAt,
		&receipt.FinalizedAt)
	if err != nil {
		return PeriodicSynthesisReceiptV1{}, err
	}
	if generation != nil {
		receipt.GenerationMode =
			types.ExecutiveGenerationModeV1(*generation)
	}
	if len(payload) > 0 {
		var content types.ExecutiveBriefContentV1
		if json.Unmarshal(payload, &content) != nil ||
			content.ValidatePeriodic() != nil {
			return PeriodicSynthesisReceiptV1{}, periodicIntegrityErrorV1()
		}
		receipt.Content = &content
	}
	return receipt, nil
}

func (s *Store) FinalizePeriodicBriefReportV1(
	ctx context.Context,
	tenantID, userID, intentID int64,
	requestDigest string,
	draft types.PeriodicBriefReportDraftV1,
	fallback bool,
) (types.PeriodicBriefReportV1, error) {
	if intentID <= 0 || !validStoreDigestV1(requestDigest) ||
		draft.Validate() != nil || draft.TenantID != tenantID ||
		draft.UserID != userID {
		return types.PeriodicBriefReportV1{}, types.NewAppError(
			types.CodeValidation, "周期报告终态无效", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return types.PeriodicBriefReportV1{},
			briefFeedDBError("开启周期报告终态事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		return types.PeriodicBriefReportV1{},
			briefFeedDBError("固定周期报告终态路径", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10),
		strconv.FormatInt(userID, 10)); err != nil {
		return types.PeriodicBriefReportV1{},
			briefFeedDBError("设置周期报告终态范围", err)
	}
	if _, err := tx.Exec(ctx,
		`SET LOCAL ROLE vane_periodic_brief_writer`); err != nil {
		return types.PeriodicBriefReportV1{},
			briefFeedDBError("进入周期报告终态角色", err)
	}
	intent, err := scanPeriodicBriefIntentV1(tx.QueryRow(ctx,
		`SELECT id,tenant_id,user_id,task_id,cadence,timezone,
		        period_start,period_end,workflow_id,temporal_run_id,
		        input_brief_ids,input_digest,run_outcome_ids,outcome_digest,
		        source_coverage,processing,partial_reason,status,created_at,
		        started_at,finalized_at
		   FROM periodic_brief_intents
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 FOR UPDATE`,
		intentID, tenantID, userID))
	if err != nil {
		return types.PeriodicBriefReportV1{},
			briefFeedDBError("锁定周期报告意图", err)
	}
	if intent.TaskID != draft.TaskID || intent.Cadence !=
		BriefReportCadenceV1(draft.Cadence) ||
		!intent.PeriodStart.Equal(draft.PeriodStart) ||
		!intent.PeriodEnd.Equal(draft.PeriodEnd) ||
		intent.InputDigest != draft.InputDigest {
		return types.PeriodicBriefReportV1{},
			types.NewAppError(types.CodeConflict,
				"周期报告终态与意图不同", nil)
	}
	payload, err := json.Marshal(draft.Content)
	if err != nil {
		return types.PeriodicBriefReportV1{}, err
	}
	generation := types.ExecutiveGenerationModel
	receiptStatus := string(ExecutiveSynthesisFinalized)
	intentStatus := string(PeriodicBriefIntentFinalized)
	if fallback {
		generation = types.ExecutiveGenerationFallback
		receiptStatus = string(ExecutiveSynthesisFallback)
		intentStatus = string(PeriodicBriefIntentFallback)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE periodic_synthesis_receipts
		    SET status=$2,generation_mode=$3,content_payload=$4,
		        content_digest=encode(sha256($4),'hex'),
		        finalized_at=clock_timestamp()
		  WHERE intent_id=$1 AND request_digest=$5 AND status='spending'`,
		intentID, receiptStatus, generation, payload, requestDigest)
	if err != nil {
		return types.PeriodicBriefReportV1{},
			briefFeedDBError("完成周期综合回执", err)
	}
	if tag.RowsAffected() == 0 {
		var existing types.PeriodicBriefReportV1
		var existingPayload []byte
		err := tx.QueryRow(ctx,
			`SELECT payload FROM periodic_brief_reports
			  WHERE intent_id=$1`, intentID).Scan(&existingPayload)
		if err == nil && json.Unmarshal(existingPayload, &existing) == nil &&
			existing.Validate() == nil {
			if commitErr := tx.Commit(ctx); commitErr != nil {
				return types.PeriodicBriefReportV1{}, commitErr
			}
			return existing, nil
		}
		return types.PeriodicBriefReportV1{},
			types.NewAppError(types.CodeConflict,
				"周期综合终态不可覆盖", nil)
	}
	var reportID int64
	err = tx.QueryRow(ctx,
		`SELECT nextval('periodic_brief_reports_id_seq')`).Scan(&reportID)
	if err != nil {
		return types.PeriodicBriefReportV1{},
			briefFeedDBError("预留周期报告身份", err)
	}
	report, err := draft.Seal(reportID)
	if err != nil {
		return types.PeriodicBriefReportV1{}, err
	}
	reportRequestDigest, err := draft.RequestDigest()
	if err != nil {
		return types.PeriodicBriefReportV1{}, err
	}
	reportPayload, err := json.Marshal(report)
	if err != nil {
		return types.PeriodicBriefReportV1{}, err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO periodic_brief_reports (
		    id,intent_id,tenant_id,user_id,task_id,cadence,
		    period_start,period_end,schema_version,request_digest,
		    payload_digest,payload,generated_at
		 ) VALUES (
		    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13
		 )`,
		reportID, intentID, tenantID, userID, draft.TaskID, draft.Cadence,
		draft.PeriodStart, draft.PeriodEnd,
		types.PeriodicBriefSchemaVersionV1, reportRequestDigest,
		report.Digest, reportPayload, draft.GeneratedAt)
	if err != nil {
		return types.PeriodicBriefReportV1{},
			briefFeedDBError("封存周期报告", err)
	}
	_, err = tx.Exec(ctx,
		`UPDATE periodic_brief_intents
		    SET status=$2,finalized_at=clock_timestamp()
		  WHERE id=$1 AND status='running'`,
		intentID, intentStatus)
	if err != nil {
		return types.PeriodicBriefReportV1{},
			briefFeedDBError("完成周期报告意图", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.PeriodicBriefReportV1{},
			briefFeedDBError("提交周期报告终态", err)
	}
	return report, nil
}

func (s *Store) AuthorizeAndConsumePeriodicSynthesisQuotaV1(
	ctx context.Context,
	tenantID, userID, intentID int64,
	amount float64,
) error {
	if tenantID <= 0 || userID <= 0 || intentID <= 0 || amount <= 0 {
		return types.NewAppError(types.CodeValidation,
			"周期综合配额请求无效", nil)
	}
	var remaining float64
	err := s.pool.QueryRow(ctx,
		`UPDATE tenant_quota q
		    SET tokens=LEAST(
		            q.burst,
		            q.tokens+q.rate*EXTRACT(EPOCH FROM
		                (clock_timestamp()-q.updated_at))
		        )-$4,
		        updated_at=clock_timestamp()
		   FROM periodic_brief_intents i
		   JOIN schedules s
		     ON s.tenant_id=i.tenant_id AND s.user_id=i.user_id
		    AND s.id=i.task_id
		   JOIN memberships m
		     ON m.tenant_id=i.tenant_id AND m.user_id=i.user_id
		  WHERE i.id=$3 AND i.tenant_id=$1 AND i.user_id=$2
		    AND i.status='running'
		    AND q.tenant_id=i.tenant_id
		    AND q.bucket=$5
		    AND LEAST(
		            q.burst,
		            q.tokens+q.rate*EXTRACT(EPOCH FROM
		                (clock_timestamp()-q.updated_at))
		        ) >= $4
		  RETURNING q.tokens`,
		tenantID, userID, intentID, amount, string(QuotaLLMTokens),
	).Scan(&remaining)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrQuotaExceeded
	}
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"扣减周期综合配额", err)
	}
	return nil
}

type periodicBriefCursorV1 struct {
	Version   int                  `json:"v"`
	TaskID    string               `json:"task_id"`
	Cadence   BriefReportCadenceV1 `json:"cadence,omitempty"`
	PeriodEnd time.Time            `json:"period_end"`
	ReportID  int64                `json:"report_id"`
}

func (s *Store) ListPeriodicBriefReportsV1(
	ctx context.Context,
	tenantID, userID int64,
	taskID string,
	query PeriodicBriefReportQueryV1,
) (PeriodicBriefReportPageV1, error) {
	if query.Cadence != "" && query.Cadence != BriefReportCadenceDaily &&
		query.Cadence != BriefReportCadenceWeekly &&
		query.Cadence != BriefReportCadenceMonthly {
		return PeriodicBriefReportPageV1{}, types.NewAppError(
			types.CodeValidation, "报告周期无效", nil)
	}
	pageSize := query.PageSize
	if pageSize == 0 {
		pageSize = 10
	}
	if pageSize < 1 || pageSize > 20 {
		return PeriodicBriefReportPageV1{}, types.NewAppError(
			types.CodeValidation, "page_size 必须是 1 到 20 之间的整数", nil)
	}
	var cursor *periodicBriefCursorV1
	if query.Cursor != "" {
		decoded, err := decodePeriodicBriefCursorV1(
			query.Cursor, taskID, query.Cadence)
		if err != nil {
			return PeriodicBriefReportPageV1{}, err
		}
		cursor = &decoded
	}
	tx, _, err := authorizeReportTaskV1(
		ctx, s, tenantID, userID, taskID, "vane_brief_reader")
	if err != nil {
		return PeriodicBriefReportPageV1{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	args := []any{tenantID, userID, taskID}
	where := ""
	if query.Cadence != "" {
		args = append(args, query.Cadence)
		where += fmt.Sprintf(" AND cadence=$%d", len(args))
	}
	if cursor != nil {
		args = append(args, cursor.PeriodEnd, cursor.ReportID)
		where += fmt.Sprintf(
			" AND (period_end,id)<($%d,$%d)", len(args)-1, len(args))
	}
	args = append(args, pageSize+1)
	rows, err := tx.Query(ctx,
		`SELECT id,request_digest,payload_digest,payload
		   FROM periodic_brief_reports
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`+where+
			fmt.Sprintf(
				` ORDER BY period_end DESC,id DESC LIMIT $%d`, len(args)),
		args...)
	if err != nil {
		return PeriodicBriefReportPageV1{}, briefFeedDBError(
			"读取周期报告页", err)
	}
	defer rows.Close()
	page := PeriodicBriefReportPageV1{
		Items: []types.PeriodicBriefReportV1{}}
	for rows.Next() {
		var id int64
		var requestDigest, payloadDigest string
		var payload []byte
		if err := rows.Scan(
			&id, &requestDigest, &payloadDigest, &payload); err != nil {
			return PeriodicBriefReportPageV1{}, briefFeedDBError(
				"扫描周期报告", err)
		}
		var report types.PeriodicBriefReportV1
		canonical, marshalErr := json.Marshal(report)
		if json.Unmarshal(payload, &report) != nil {
			return PeriodicBriefReportPageV1{}, periodicIntegrityErrorV1()
		}
		canonical, marshalErr = json.Marshal(report)
		computedRequest, requestErr :=
			report.PeriodicBriefReportDraftV1.RequestDigest()
		if marshalErr != nil || requestErr != nil ||
			!bytes.Equal(canonical, payload) || report.Validate() != nil ||
			report.ID != id || report.Digest != payloadDigest ||
			computedRequest != requestDigest ||
			report.TenantID != tenantID || report.UserID != userID ||
			report.TaskID != taskID {
			return PeriodicBriefReportPageV1{}, periodicIntegrityErrorV1()
		}
		page.Items = append(page.Items, report)
	}
	if err := rows.Err(); err != nil {
		return PeriodicBriefReportPageV1{}, briefFeedDBError(
			"遍历周期报告", err)
	}
	if len(page.Items) > pageSize {
		page.Items = page.Items[:pageSize]
		last := page.Items[len(page.Items)-1]
		page.NextCursor, err = encodePeriodicBriefCursorV1(
			taskID, query.Cadence, last.PeriodEnd, last.ID)
		if err != nil {
			return PeriodicBriefReportPageV1{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return PeriodicBriefReportPageV1{}, briefFeedDBError(
			"提交周期报告读取", err)
	}
	return page, nil
}

func periodicIntegrityErrorV1() error {
	return types.NewAppError(
		types.CodeInternal, "周期报告完整性校验失败", nil)
}

func encodePeriodicBriefCursorV1(
	taskID string,
	cadence BriefReportCadenceV1,
	periodEnd time.Time,
	reportID int64,
) (string, error) {
	payload, err := json.Marshal(periodicBriefCursorV1{
		Version: 1, TaskID: taskID, Cadence: cadence,
		PeriodEnd: periodEnd.Round(0).UTC().Truncate(time.Microsecond),
		ReportID:  reportID,
	})
	if err != nil {
		return "", types.NewAppError(
			types.CodeInternal, "生成周期报告游标失败", err)
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodePeriodicBriefCursorV1(
	value, taskID string,
	cadence BriefReportCadenceV1,
) (periodicBriefCursorV1, error) {
	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(payload) > 2048 {
		return periodicBriefCursorV1{}, types.NewAppError(
			types.CodeValidation, "周期报告游标无效", nil)
	}
	var cursor periodicBriefCursorV1
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&cursor) != nil || cursor.Version != 1 ||
		cursor.TaskID != taskID || cursor.Cadence != cadence ||
		cursor.PeriodEnd.IsZero() || cursor.ReportID <= 0 {
		return periodicBriefCursorV1{}, types.NewAppError(
			types.CodeValidation, "周期报告游标无效", nil)
	}
	return cursor, nil
}
