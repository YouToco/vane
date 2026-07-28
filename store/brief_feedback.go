package store

import (
	"context"
	"errors"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

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
	tx, err := s.beginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	})
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
	).Scan(&tenantID, &taskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.BriefV1{}, false, nil
	}
	if err != nil {
		return types.BriefV1{}, false,
			briefFeedDBError("校验 canonical Brief 反馈投递", err)
	}
	if tenantID <= 0 || taskID == "" {
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
