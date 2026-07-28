package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

func (s *Store) LoadExecutiveBriefForFeedbackV1(
	ctx context.Context,
	userID, deliveryID, batchID int64,
) (types.ExecutiveBriefArtifactV1, bool, error) {
	brief, found, err := s.LoadCanonicalBriefForFeedbackV1(
		ctx, userID, deliveryID, batchID)
	if err != nil || !found {
		return types.ExecutiveBriefArtifactV1{}, false, err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return types.ExecutiveBriefArtifactV1{}, false,
			briefFeedDBError("开启执行简报反馈读取事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		return types.ExecutiveBriefArtifactV1{}, false,
			briefFeedDBError("固定执行简报反馈读取路径", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(brief.TenantID, 10),
		strconv.FormatInt(userID, 10)); err != nil {
		return types.ExecutiveBriefArtifactV1{}, false,
			briefFeedDBError("设置执行简报反馈读取范围", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_brief_reader`); err != nil {
		return types.ExecutiveBriefArtifactV1{}, false,
			briefFeedDBError("进入执行简报反馈读取角色", err)
	}
	var payload []byte
	var digest string
	err = tx.QueryRow(ctx,
		`SELECT payload,payload_digest
		   FROM executive_brief_artifacts
		  WHERE brief_snapshot_id=$1 AND tenant_id=$2 AND user_id=$3
		    AND task_id=$4`,
		brief.ID, brief.TenantID, userID, brief.TaskID,
	).Scan(&payload, &digest)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.ExecutiveBriefArtifactV1{}, false, nil
	}
	if err != nil {
		return types.ExecutiveBriefArtifactV1{}, false,
			briefFeedDBError("读取执行简报反馈 artifact", err)
	}
	var artifact types.ExecutiveBriefArtifactV1
	canonical, marshalErr := json.Marshal(artifact)
	if json.Unmarshal(payload, &artifact) != nil {
		return types.ExecutiveBriefArtifactV1{}, false,
			canonicalBriefIntegrityError()
	}
	canonical, marshalErr = json.Marshal(artifact)
	if marshalErr != nil || !bytes.Equal(canonical, payload) ||
		artifact.Validate() != nil || artifact.Digest != digest ||
		artifact.BriefSnapshotID != brief.ID ||
		artifact.PushBatchID != batchID ||
		artifact.UserID != userID {
		return types.ExecutiveBriefArtifactV1{}, false,
			canonicalBriefIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return types.ExecutiveBriefArtifactV1{}, false,
			briefFeedDBError("提交执行简报反馈读取", err)
	}
	return artifact, true, nil
}

// LoadCanonicalBriefForFeedbackV1 resolves one already-authorized delivery
// callback to its immutable whole Brief. The initial delivery lookup is exact
// user/batch scoped; payload access then enters the same least-privilege reader
// role used by the Web feed.
func (s *Store) LoadCanonicalBriefForFeedbackV1(
	ctx context.Context,
	userID int64,
	deliveryID int64,
	batchID int64,
) (types.BriefV1, bool, error) {
	if userID <= 0 || deliveryID <= 0 || batchID <= 0 {
		return types.BriefV1{}, false, types.NewAppError(
			types.CodeValidation, "canonical Brief 反馈范围无效", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return types.BriefV1{}, false,
			briefFeedDBError("开启 canonical Brief 反馈读取事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(
		ctx, `SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		return types.BriefV1{}, false,
			briefFeedDBError("固定 canonical Brief 反馈读取路径", err)
	}

	var tenantID int64
	err = tx.QueryRow(ctx, `
		SELECT d.tenant_id
		  FROM deliveries d
		  JOIN push_batches b
		    ON b.id=d.batch_id
		   AND b.tenant_id=d.tenant_id
		   AND b.user_id=d.user_id
		 WHERE d.id=$1 AND d.user_id=$2 AND d.batch_id=$3`,
		deliveryID, userID, batchID,
	).Scan(&tenantID)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.BriefV1{}, false, nil
	}
	if err != nil {
		return types.BriefV1{}, false,
			briefFeedDBError("校验 canonical Brief 反馈投递", err)
	}
	if tenantID <= 0 {
		return types.BriefV1{}, false, nil
	}
	tenantExists, err := lockTenantAdmissionRootShared(ctx, tx, tenantID)
	if err != nil {
		return types.BriefV1{}, false,
			briefFeedDBError("锁定 canonical Brief 反馈租户准入", err)
	}
	if !tenantExists {
		return types.BriefV1{}, false, nil
	}
	var lockedTenantID int64
	var taskID string
	err = tx.QueryRow(ctx, `
		SELECT d.tenant_id,COALESCE(b.schedule_id,'')
		  FROM deliveries d
		  JOIN push_batches b
		    ON b.id=d.batch_id
		   AND b.tenant_id=d.tenant_id
		   AND b.user_id=d.user_id
		 WHERE d.id=$1 AND d.user_id=$2 AND d.batch_id=$3
		 FOR KEY SHARE OF d,b`,
		deliveryID, userID, batchID,
	).Scan(&lockedTenantID, &taskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.BriefV1{}, false, nil
	}
	if err != nil {
		return types.BriefV1{}, false,
			briefFeedDBError("重校验 canonical Brief 反馈投递", err)
	}
	if lockedTenantID != tenantID || taskID == "" {
		return types.BriefV1{}, false, canonicalBriefIntegrityError()
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10),
		strconv.FormatInt(userID, 10),
	); err != nil {
		return types.BriefV1{}, false,
			briefFeedDBError("设置 canonical Brief 反馈读取范围", err)
	}
	if _, err := tx.Exec(
		ctx, `SET LOCAL ROLE vane_brief_reader`); err != nil {
		return types.BriefV1{}, false,
			briefFeedDBError("进入 canonical Brief 反馈读取角色", err)
	}
	stored, found, err := loadBriefV1(
		ctx, tx,
		`FROM brief_snapshots WHERE push_batch_id=$1`,
		batchID,
	)
	if err != nil {
		return types.BriefV1{}, false, err
	}
	if !found {
		return types.BriefV1{}, false, nil
	}
	brief := stored.brief
	if brief.TenantID != tenantID || brief.UserID != userID ||
		brief.TaskID != taskID || brief.PushBatchID != batchID {
		return types.BriefV1{}, false, canonicalBriefIntegrityError()
	}
	deliveryFound := false
	for _, insight := range brief.Insights {
		if insight.ID == deliveryID {
			deliveryFound = true
			break
		}
	}
	if !deliveryFound {
		return types.BriefV1{}, false, canonicalBriefIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return types.BriefV1{}, false,
			briefFeedDBError("提交 canonical Brief 反馈读取", err)
	}
	return brief, true, nil
}
