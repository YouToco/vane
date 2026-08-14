package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

type PeriodicReportDeliveryStatusV1 string

const (
	PeriodicReportDeliveryPrepared  PeriodicReportDeliveryStatusV1 = "prepared"
	PeriodicReportDeliverySending   PeriodicReportDeliveryStatusV1 = "sending"
	PeriodicReportDeliverySent      PeriodicReportDeliveryStatusV1 = "sent"
	PeriodicReportDeliveryAmbiguous PeriodicReportDeliveryStatusV1 = "ambiguous"
	PeriodicReportDeliverySkipped   PeriodicReportDeliveryStatusV1 = "skipped"
)

type PeriodicReportDeliveryV1 struct {
	ReportID          int64
	TenantID          int64
	UserID            int64
	TaskID            string
	DeliveryMode      BriefReportDeliveryV1
	DecisionState     types.ExecutiveDecisionStateV1
	CardPayload       []byte
	CardDigest        string
	ProviderUUID      string
	AppIdentity       string
	TargetOpenID      string
	ProviderChatID    string
	Status            PeriodicReportDeliveryStatusV1
	Attempt           int
	AttemptStartedAt  *time.Time
	ProviderMessageID string
	FinalizedAt       *time.Time
}

type PeriodicDeliveryRecoveryCursorV1 struct {
	UpdatedAt time.Time
	ReportID  int64
}

type PeriodicDeliveryRecoveryCandidateV1 struct {
	UpdatedAt time.Time
	PeriodicReportDeliveryV1
}

func (s *Store) ListPeriodicMissingDeliveryReportsV1(
	ctx context.Context,
	afterReportID int64,
	limit int,
) ([]types.PeriodicBriefReportV1, error) {
	if afterReportID < 0 || limit < 1 || limit > 100 {
		return nil, types.NewAppError(
			types.CodeValidation, "周期报告缺失推送恢复页无效", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx,
		`SET LOCAL ROLE vane_brief_synthesis_recovery`); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx,
		`SELECT report_id,payload
		   FROM read_periodic_missing_delivery_recovery_v1($1,$2)`,
		afterReportID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]types.PeriodicBriefReportV1, 0, limit)
	for rows.Next() {
		var reportID int64
		var payload []byte
		if err := rows.Scan(&reportID, &payload); err != nil {
			return nil, err
		}
		var report types.PeriodicBriefReportV1
		if json.Unmarshal(payload, &report) != nil ||
			report.ID != reportID || report.Validate() != nil {
			return nil, periodicIntegrityErrorV1()
		}
		out = append(out, report)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func validPeriodicDeliveryV1(value PeriodicReportDeliveryV1) bool {
	parsed, uuidErr := uuid.Parse(value.ProviderUUID)
	return value.ReportID > 0 && value.TenantID > 0 && value.UserID > 0 &&
		value.TaskID != "" &&
		(value.DeliveryMode == BriefReportDeliveryImportant ||
			value.DeliveryMode == BriefReportDeliveryAlways ||
			value.DeliveryMode == BriefReportDeliveryWebOnly) &&
		value.DecisionState.Valid() &&
		len(value.CardPayload) >= 2 && len(value.CardPayload) <= 6144 &&
		validStoreDigestV1(value.CardDigest) &&
		parsed != uuid.Nil && uuidErr == nil &&
		parsed.String() == value.ProviderUUID &&
		value.AppIdentity != "" && value.TargetOpenID != ""
}

func scanPeriodicReportDeliveryV1(
	row pgx.Row,
) (PeriodicReportDeliveryV1, error) {
	var out PeriodicReportDeliveryV1
	var providerUUID uuid.UUID
	var messageID *string
	err := row.Scan(
		&out.ReportID, &out.TenantID, &out.UserID, &out.TaskID,
		&out.DeliveryMode, &out.DecisionState, &out.CardPayload,
		&out.CardDigest, &providerUUID, &out.AppIdentity,
		&out.TargetOpenID, &out.ProviderChatID, &out.Status,
		&out.Attempt, &out.AttemptStartedAt, &messageID, &out.FinalizedAt)
	if err != nil {
		return PeriodicReportDeliveryV1{}, err
	}
	out.ProviderUUID = providerUUID.String()
	if messageID != nil {
		out.ProviderMessageID = *messageID
	}
	sum := sha256.Sum256(out.CardPayload)
	if !validPeriodicDeliveryV1(out) ||
		hex.EncodeToString(sum[:]) != out.CardDigest {
		return PeriodicReportDeliveryV1{}, periodicIntegrityErrorV1()
	}
	return out, nil
}

const periodicDeliveryColumnsV1 = `
	report_id,tenant_id,user_id,task_id,delivery_mode,decision_state,
	card_payload,card_digest,provider_uuid,app_identity,target_open_id,
	provider_chat_id,status,attempt,attempt_started_at,
	provider_message_id,finalized_at`

func lockPeriodicReportMembershipV1(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID, reportID int64,
) (string, error) {
	var taskID string
	err := tx.QueryRow(ctx,
		`SELECT p.task_id
		   FROM periodic_brief_reports p
		   JOIN schedules s
		     ON s.tenant_id=p.tenant_id AND s.user_id=p.user_id
		    AND s.id=p.task_id
		   JOIN memberships m
		     ON m.tenant_id=p.tenant_id AND m.user_id=p.user_id
		  WHERE p.id=$3 AND p.tenant_id=$1 AND p.user_id=$2
		  FOR KEY SHARE OF m,s`,
		tenantID, userID, reportID).Scan(&taskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", types.NewAppError(
			types.CodeNotFound, "周期报告推送范围不存在", nil)
	}
	if err != nil {
		return "", briefFeedDBError("校验周期报告推送权限", err)
	}
	return taskID, nil
}

func (s *Store) PreparePeriodicReportDeliveryV1(
	ctx context.Context,
	report types.PeriodicBriefReportV1,
	mode BriefReportDeliveryV1,
	card []byte,
	providerUUID, appIdentity, targetOpenID, providerChatID string,
	shouldSend bool,
) (PeriodicReportDeliveryV1, error) {
	sum := sha256.Sum256(card)
	prepared := PeriodicReportDeliveryV1{
		ReportID: report.ID, TenantID: report.TenantID,
		UserID: report.UserID, TaskID: report.TaskID,
		DeliveryMode: mode, DecisionState: report.Content.DecisionState,
		CardPayload:  append([]byte(nil), card...),
		CardDigest:   hex.EncodeToString(sum[:]),
		ProviderUUID: providerUUID, AppIdentity: appIdentity,
		TargetOpenID: targetOpenID, ProviderChatID: providerChatID,
		Status: PeriodicReportDeliveryPrepared,
	}
	if !shouldSend {
		prepared.Status = PeriodicReportDeliverySkipped
	}
	if report.Validate() != nil || !validPeriodicDeliveryV1(prepared) {
		return PeriodicReportDeliveryV1{}, types.NewAppError(
			types.CodeValidation, "周期报告推送准备无效", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return PeriodicReportDeliveryV1{},
			briefFeedDBError("开启周期报告推送准备", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		return PeriodicReportDeliveryV1{}, err
	}
	taskID, err := lockPeriodicReportMembershipV1(
		ctx, tx, report.TenantID, report.UserID, report.ID)
	if err != nil {
		return PeriodicReportDeliveryV1{}, err
	}
	if taskID != report.TaskID {
		return PeriodicReportDeliveryV1{}, types.NewAppError(
			types.CodeConflict, "周期报告推送任务已不同", nil)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(report.TenantID, 10),
		strconv.FormatInt(report.UserID, 10)); err != nil {
		return PeriodicReportDeliveryV1{}, err
	}
	if _, err := tx.Exec(ctx,
		`SET LOCAL ROLE vane_periodic_brief_writer`); err != nil {
		return PeriodicReportDeliveryV1{}, err
	}
	status := string(prepared.Status)
	_, err = tx.Exec(ctx,
		`INSERT INTO periodic_report_deliveries (
		    report_id,tenant_id,user_id,task_id,delivery_mode,
		    decision_state,card_payload,card_digest,provider_uuid,
		    app_identity,target_open_id,provider_chat_id,status,finalized_at
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
		           CASE WHEN $13='skipped' THEN clock_timestamp() END)
		 ON CONFLICT (report_id) DO NOTHING`,
		prepared.ReportID, prepared.TenantID, prepared.UserID,
		prepared.TaskID, prepared.DeliveryMode, prepared.DecisionState,
		prepared.CardPayload, prepared.CardDigest, prepared.ProviderUUID,
		prepared.AppIdentity, prepared.TargetOpenID,
		prepared.ProviderChatID, status)
	if err != nil {
		return PeriodicReportDeliveryV1{},
			briefFeedDBError("准备周期报告推送回执", err)
	}
	stored, err := scanPeriodicReportDeliveryV1(tx.QueryRow(ctx,
		`SELECT `+periodicDeliveryColumnsV1+`
		   FROM periodic_report_deliveries WHERE report_id=$1 FOR UPDATE`,
		report.ID))
	if err != nil {
		return PeriodicReportDeliveryV1{}, err
	}
	expected := prepared
	expected.Attempt = stored.Attempt
	expected.AttemptStartedAt = stored.AttemptStartedAt
	expected.FinalizedAt = stored.FinalizedAt
	statusCompatible := false
	if shouldSend {
		switch stored.Status {
		case PeriodicReportDeliveryPrepared,
			PeriodicReportDeliverySending,
			PeriodicReportDeliverySent,
			PeriodicReportDeliveryAmbiguous:
			statusCompatible = true
		}
	} else {
		statusCompatible = stored.Status == PeriodicReportDeliverySkipped
	}
	if !bytes.Equal(stored.CardPayload, expected.CardPayload) ||
		stored.ReportID != expected.ReportID ||
		stored.TenantID != expected.TenantID ||
		stored.UserID != expected.UserID ||
		stored.TaskID != expected.TaskID ||
		stored.DeliveryMode != expected.DeliveryMode ||
		stored.DecisionState != expected.DecisionState ||
		stored.CardDigest != expected.CardDigest ||
		stored.ProviderUUID != expected.ProviderUUID ||
		stored.AppIdentity != expected.AppIdentity ||
		stored.TargetOpenID != expected.TargetOpenID ||
		stored.ProviderChatID != expected.ProviderChatID ||
		!statusCompatible {
		return PeriodicReportDeliveryV1{}, types.NewAppError(
			types.CodeConflict, "周期报告推送准备已不同", nil)
	}
	if err := tx.Commit(ctx); err != nil {
		return PeriodicReportDeliveryV1{}, err
	}
	return stored, nil
}

func (s *Store) ClaimPeriodicReportDeliveryV1(
	ctx context.Context,
	tenantID, userID, reportID int64,
) (PeriodicReportDeliveryV1, bool, error) {
	// The status predicate is the compare-and-swap. Read committed lets a
	// competing recovery worker wait for the winner and then observe zero
	// affected rows instead of surfacing a serialization failure.
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return PeriodicReportDeliveryV1{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		return PeriodicReportDeliveryV1{}, false, err
	}
	if _, err := lockPeriodicReportMembershipV1(
		ctx, tx, tenantID, userID, reportID); err != nil {
		return PeriodicReportDeliveryV1{}, false, err
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10),
		strconv.FormatInt(userID, 10)); err != nil {
		return PeriodicReportDeliveryV1{}, false, err
	}
	if _, err := tx.Exec(ctx,
		`SET LOCAL ROLE vane_periodic_brief_writer`); err != nil {
		return PeriodicReportDeliveryV1{}, false, err
	}
	tag, err := tx.Exec(ctx,
		`UPDATE periodic_report_deliveries
		    SET status='sending',attempt=attempt+1,
		        attempt_started_at=clock_timestamp(),
		        finalized_at=NULL,updated_at=clock_timestamp()
		  WHERE report_id=$1 AND tenant_id=$2 AND user_id=$3
		    AND status='prepared'`,
		reportID, tenantID, userID)
	if err != nil {
		return PeriodicReportDeliveryV1{}, false, err
	}
	out, err := scanPeriodicReportDeliveryV1(tx.QueryRow(ctx,
		`SELECT `+periodicDeliveryColumnsV1+`
		   FROM periodic_report_deliveries WHERE report_id=$1`,
		reportID))
	if err != nil {
		return PeriodicReportDeliveryV1{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return PeriodicReportDeliveryV1{}, false, err
	}
	return out, tag.RowsAffected() == 1, nil
}

func (s *Store) FinalizePeriodicReportDeliveryV1(
	ctx context.Context,
	tenantID, userID, reportID int64,
	status PeriodicReportDeliveryStatusV1,
	messageID string,
) error {
	if status != PeriodicReportDeliverySent &&
		status != PeriodicReportDeliveryAmbiguous &&
		status != PeriodicReportDeliveryPrepared {
		return types.NewAppError(
			types.CodeValidation, "周期报告推送终态无效", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		return err
	}
	if _, err := lockPeriodicReportMembershipV1(
		ctx, tx, tenantID, userID, reportID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10),
		strconv.FormatInt(userID, 10)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`SET LOCAL ROLE vane_periodic_brief_writer`); err != nil {
		return err
	}
	finalized := status != PeriodicReportDeliveryPrepared
	tag, err := tx.Exec(ctx,
		`UPDATE periodic_report_deliveries
		    SET status=$2,
		        attempt_started_at=CASE WHEN $2='prepared'
		            THEN NULL ELSE attempt_started_at END,
		        provider_message_id=NULLIF($3,''),
		        finalized_at=CASE WHEN $4 THEN clock_timestamp() END,
		        updated_at=clock_timestamp()
		  WHERE report_id=$1 AND status='sending'`,
		reportID, status, messageID, finalized)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return types.NewAppError(
			types.CodeConflict, "周期报告推送终态不可覆盖", nil)
	}
	return tx.Commit(ctx)
}

func (s *Store) ListPeriodicDeliveryRecoveryCandidatesV1(
	ctx context.Context,
	cursor *PeriodicDeliveryRecoveryCursorV1,
	limit int,
) ([]PeriodicDeliveryRecoveryCandidateV1, error) {
	if limit < 1 || limit > 100 {
		return nil, types.NewAppError(
			types.CodeValidation, "周期报告推送恢复页无效", nil)
	}
	var afterAt any
	var afterID int64
	if cursor != nil {
		afterAt, afterID = cursor.UpdatedAt, cursor.ReportID
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx,
		`SET LOCAL ROLE vane_brief_synthesis_recovery`); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx,
		`SELECT updated_at,report_id,tenant_id,user_id,task_id,
		        delivery_mode,decision_state,card_payload,card_digest,
		        provider_uuid,app_identity,target_open_id,provider_chat_id,
		        status,attempt,attempt_started_at,provider_message_id,
		        finalized_at
		   FROM read_periodic_delivery_recovery_v1($1,$2,$3)`,
		afterAt, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]PeriodicDeliveryRecoveryCandidateV1, 0, limit)
	for rows.Next() {
		var candidate PeriodicDeliveryRecoveryCandidateV1
		var providerUUID uuid.UUID
		var messageID *string
		err := rows.Scan(
			&candidate.UpdatedAt, &candidate.ReportID,
			&candidate.TenantID, &candidate.UserID, &candidate.TaskID,
			&candidate.DeliveryMode, &candidate.DecisionState,
			&candidate.CardPayload, &candidate.CardDigest, &providerUUID,
			&candidate.AppIdentity, &candidate.TargetOpenID,
			&candidate.ProviderChatID, &candidate.Status,
			&candidate.Attempt, &candidate.AttemptStartedAt,
			&messageID, &candidate.FinalizedAt)
		if err != nil {
			return nil, err
		}
		candidate.ProviderUUID = providerUUID.String()
		if messageID != nil {
			candidate.ProviderMessageID = *messageID
		}
		if !validPeriodicDeliveryV1(candidate.PeriodicReportDeliveryV1) {
			return nil, periodicIntegrityErrorV1()
		}
		out = append(out, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}
