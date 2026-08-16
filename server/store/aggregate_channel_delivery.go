package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

// PrepareAggregateTelegramOutbound atomically freezes the Telegram payload,
// exact route and exact aggregate delivery IDs.  The Telegram worker cannot
// observe the prepared channel effect before its business mapping commits.
func (s *Store) PrepareAggregateTelegramOutbound(
	ctx context.Context,
	planID string,
	tenantID, userID int64,
	taskID string,
	batchID int64,
	chunkIndex, chunkCount int,
	deliveryIDs []int64,
	routeID int64,
	effectID, text string,
) (ChannelOutboundEffect, error) {
	parsedPlan, planErr := uuid.Parse(planID)
	parsedEffect, effectErr := uuid.Parse(effectID)
	if planErr != nil || parsedPlan.String() != planID ||
		effectErr != nil || parsedEffect.String() != effectID ||
		tenantID <= 0 || userID <= 0 || batchID <= 0 || routeID <= 0 ||
		strings.TrimSpace(taskID) != taskID || taskID == "" ||
		chunkIndex < 0 || chunkCount <= 0 || chunkIndex >= chunkCount ||
		len(deliveryIDs) == 0 || !slices.IsSorted(deliveryIDs) ||
		len(slices.Compact(append([]int64(nil), deliveryIDs...))) != len(deliveryIDs) ||
		!validOutboundKind("aggregate_brief") || strings.TrimSpace(text) == "" ||
		len(text) > 262144 {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeValidation,
			"Telegram 聚合简报 effect 无效", types.ErrValidation)
	}
	for _, id := range deliveryIDs {
		if id <= 0 {
			return ChannelOutboundEffect{}, types.NewAppError(types.CodeValidation,
				"Telegram 聚合简报 delivery 无效", types.ErrValidation)
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"开启 Telegram 聚合简报准备", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	// Replay takes the outbound row before the plan/mapping rows, matching the
	// provider-completion transaction. A first writer sees no row here and no
	// sender can observe its later insert until this transaction commits.
	if _, lockErr := scanChannelOutbound(tx.QueryRow(ctx,
		`SELECT `+channelOutboundColumns+` FROM channel_outbound_effects
		  WHERE effect_id=$1 FOR UPDATE`, parsedEffect)); lockErr != nil &&
		!errors.Is(lockErr, pgx.ErrNoRows) {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"锁定 Telegram 聚合简报重放", lockErr)
	}
	var selection DeliveryChannelSelection
	var frozenRouteID *int64
	err = tx.QueryRow(ctx,
		`SELECT selection,telegram_route_id
		   FROM artifact_delivery_plans
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND task_id=$4 AND
		        artifact_kind=$5 FOR UPDATE`,
		parsedPlan, tenantID, userID, taskID, ArtifactDeliveryAggregateBrief,
	).Scan(&selection, &frozenRouteID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeNotFound,
			"聚合简报投递计划不可用", types.ErrNotFound)
	}
	if err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"锁定聚合简报投递计划", err)
	}
	if !selection.Includes("telegram") || frozenRouteID == nil ||
		*frozenRouteID != routeID {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeConflict,
			"Telegram 聚合简报路由与冻结计划不同", types.ErrConflict)
	}
	var batchOK bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
		 SELECT 1 FROM push_batches
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND schedule_id=$4 AND
		        delivery_authority='effect' AND status='pending'
		)`, batchID, tenantID, userID, taskID).Scan(&batchOK); err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"验证聚合简报批次", err)
	}
	if !batchOK {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeConflict,
			"聚合简报批次不允许多渠道发送", types.ErrConflict)
	}
	lockedIDs, err := lockAggregateDeliveryIDs(
		ctx, tx, tenantID, userID, batchID, deliveryIDs)
	if err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"锁定 Telegram 聚合简报内容", err)
	}
	if !slices.Equal(lockedIDs, deliveryIDs) {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeConflict,
			"Telegram 聚合简报内容范围不同", types.ErrConflict)
	}
	var route ChannelRoute
	route, err = scanChannelRoute(tx.QueryRow(ctx,
		`SELECT cr.id,cr.tenant_id,cr.user_id,cr.identity_id,cr.provider,
		        cr.app_identity,cr.provider_chat_id,cr.provider_thread_id,
		        cr.chat_type,cr.route_kind,cr.status,cr.bound_at,cr.revoked_at
		   FROM channel_routes cr
		 JOIN channel_identities ci ON ci.id=cr.identity_id
		 WHERE cr.id=$1 AND cr.tenant_id=$2 AND cr.user_id=$3 AND
		       cr.provider='telegram' AND cr.status='active' AND ci.status='active'
		 FOR UPDATE OF cr`, routeID, tenantID, userID))
	if err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeNotFound,
			"Telegram 聚合简报路由不可用", err)
	}
	digestBytes := sha256.Sum256([]byte(text))
	digest := hex.EncodeToString(digestBytes[:])
	if _, err := tx.Exec(ctx,
		`INSERT INTO channel_outbound_effects
		 (effect_id,tenant_id,user_id,route_id,provider,app_identity,
		  provider_chat_id,provider_thread_id,effect_kind,payload_text,payload_digest)
		 VALUES ($1,$2,$3,$4,'telegram',$5,$6,$7,'aggregate_brief',$8,$9)
		 ON CONFLICT (effect_id) DO NOTHING`, parsedEffect, tenantID, userID,
		routeID, route.AppIdentity, route.ProviderChatID, route.ProviderThreadID,
		text, digest); err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"准备 Telegram 聚合简报消息", err)
	}
	effect, err := scanChannelOutbound(tx.QueryRow(ctx,
		`SELECT `+channelOutboundColumns+` FROM channel_outbound_effects
		  WHERE effect_id=$1 FOR UPDATE`, parsedEffect))
	if err != nil || effect.TenantID != tenantID || effect.UserID != userID ||
		effect.RouteID != routeID || effect.EffectKind != "aggregate_brief" ||
		effect.PayloadText != text || effect.PayloadDigest != digest {
		if err == nil {
			err = types.ErrConflict
		}
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeConflict,
			"Telegram 聚合简报消息重放不同", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO aggregate_channel_delivery_effects
		 (channel_effect_id,plan_id,tenant_id,user_id,task_id,batch_id,
		  provider,chunk_index,chunk_count,delivery_ids)
		 VALUES ($1,$2,$3,$4,$5,$6,'telegram',$7,$8,$9)
		 ON CONFLICT (channel_effect_id) DO NOTHING`, parsedEffect, parsedPlan,
		tenantID, userID, taskID, batchID, chunkIndex, chunkCount, deliveryIDs); err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"冻结 Telegram 聚合简报内容集", err)
	}
	var storedPlan uuid.UUID
	var storedTask, provider, status string
	var storedBatch int64
	var storedChunk, storedCount int
	var storedIDs []int64
	if err := tx.QueryRow(ctx,
		`SELECT plan_id,task_id,batch_id,provider,chunk_index,chunk_count,
		        delivery_ids,status
		   FROM aggregate_channel_delivery_effects
		  WHERE channel_effect_id=$1 FOR UPDATE`, parsedEffect).Scan(
		&storedPlan, &storedTask, &storedBatch, &provider, &storedChunk,
		&storedCount, &storedIDs, &status); err != nil ||
		storedPlan != parsedPlan || storedTask != taskID || storedBatch != batchID ||
		provider != "telegram" || storedChunk != chunkIndex ||
		storedCount != chunkCount || !slices.Equal(storedIDs, deliveryIDs) ||
		(status != "prepared" && status != "sent") {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeConflict,
			"Telegram 聚合简报内容集重放不同", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"提交 Telegram 聚合简报准备", err)
	}
	return effect, nil
}

// SettleAggregateTelegramOutbound projects a sent Telegram child receipt into
// the channel-neutral batch. For "both", Feishu remains responsible for the
// final delivery/batch projection; for Telegram-only this method closes it
// without forging a Feishu message ID.
func (s *Store) SettleAggregateTelegramOutbound(
	ctx context.Context, tenantID, userID int64, planID, effectID string,
) error {
	parsedPlan, planErr := uuid.Parse(planID)
	parsedEffect, effectErr := uuid.Parse(effectID)
	if planErr != nil || effectErr != nil || parsedPlan.String() != planID ||
		parsedEffect.String() != effectID || tenantID <= 0 || userID <= 0 {
		return types.NewAppError(types.CodeValidation,
			"Telegram 聚合简报结算无效", types.ErrValidation)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"开启 Telegram 聚合简报结算", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	effect, err := scanChannelOutbound(tx.QueryRow(ctx,
		`SELECT `+channelOutboundColumns+` FROM channel_outbound_effects
		  WHERE effect_id=$1 AND tenant_id=$2 AND user_id=$3 FOR UPDATE`,
		parsedEffect, tenantID, userID))
	if err != nil || effect.Status != "sent" ||
		len(effect.ProviderMessageIDs) == 0 {
		return types.NewAppError(types.CodeConflict,
			"Telegram 聚合简报尚未可结算", err)
	}
	var storedPlan uuid.UUID
	var mappedStatus string
	err = tx.QueryRow(ctx,
		`SELECT plan_id,status FROM aggregate_channel_delivery_effects
		  WHERE channel_effect_id=$1 AND tenant_id=$2 AND user_id=$3
		  FOR UPDATE`, parsedEffect, tenantID, userID).Scan(
		&storedPlan, &mappedStatus)
	if err != nil || storedPlan != parsedPlan {
		return types.NewAppError(types.CodeNotFound,
			"Telegram 聚合简报结算映射不可用", err)
	}
	if mappedStatus != "prepared" && mappedStatus != "sent" {
		return types.NewAppError(types.CodeConflict,
			"Telegram 聚合简报结算状态不同", types.ErrConflict)
	}
	if err := settleAggregateTelegramOutboundTx(
		ctx, tx, effect, effect.ProviderMessageIDs); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"提交 Telegram 聚合简报结算", err)
	}
	return nil
}

// settleAggregateTelegramOutboundTx is invoked by CompleteTelegramOutbound in
// the same transaction as the provider receipt. That closes the crash window
// in which Telegram had accepted a message but a later task revoke could make
// the Activity unable to project the already-sent aggregate batch. Retained
// pre-142 schemas have no mapping table and deliberately remain a no-op.
func settleAggregateTelegramOutboundTx(
	ctx context.Context, tx pgx.Tx, effect ChannelOutboundEffect,
	messageIDs []string,
) error {
	var tableExists bool
	if err := tx.QueryRow(ctx,
		`SELECT to_regclass('public.aggregate_channel_delivery_effects') IS NOT NULL`,
	).Scan(&tableExists); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"检查 Telegram 聚合简报结算能力", err)
	}
	if !tableExists {
		return nil
	}
	parsedEffect, err := uuid.Parse(effect.EffectID)
	if err != nil || parsedEffect.String() != effect.EffectID ||
		len(messageIDs) == 0 {
		return types.NewAppError(types.CodeValidation,
			"Telegram 聚合简报子回执无效", types.ErrValidation)
	}
	var planID uuid.UUID
	var taskID, mappedStatus string
	var batchID int64
	var deliveryIDs []int64
	err = tx.QueryRow(ctx,
		`SELECT plan_id,task_id,batch_id,delivery_ids,status
		   FROM aggregate_channel_delivery_effects
		  WHERE channel_effect_id=$1 AND tenant_id=$2 AND user_id=$3
		  FOR UPDATE`, parsedEffect, effect.TenantID, effect.UserID).Scan(
		&planID, &taskID, &batchID, &deliveryIDs, &mappedStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"读取 Telegram 聚合简报子回执", err)
	}
	var selection DeliveryChannelSelection
	if err := tx.QueryRow(ctx,
		`SELECT selection FROM artifact_delivery_plans
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND task_id=$4
		  FOR UPDATE`, planID, effect.TenantID, effect.UserID, taskID).Scan(
		&selection); err != nil || !selection.Includes("telegram") {
		return types.NewAppError(types.CodeConflict,
			"Telegram 聚合简报结算计划不同", err)
	}
	messageJSON, _ := json.Marshal(messageIDs)
	if mappedStatus == "prepared" {
		if _, err := tx.Exec(ctx,
			`UPDATE aggregate_channel_delivery_effects
			    SET status='sent',provider_message_ids=$2,
			        sent_at=clock_timestamp(),updated_at=clock_timestamp()
			  WHERE channel_effect_id=$1 AND status='prepared'`,
			parsedEffect, string(messageJSON)); err != nil {
			return types.NewAppError(types.CodeDatabase,
				"记录 Telegram 聚合简报子回执", err)
		}
	} else if mappedStatus != "sent" {
		return types.NewAppError(types.CodeConflict,
			"Telegram 聚合简报子回执状态不同", types.ErrConflict)
	}
	if selection != DeliveryChannelTelegram {
		return nil
	}
	lockedIDs, lockErr := lockAggregateDeliveryIDs(
		ctx, tx, effect.TenantID, effect.UserID, batchID, deliveryIDs)
	if lockErr != nil || !slices.Equal(lockedIDs, deliveryIDs) {
		return types.NewAppError(types.CodeConflict,
			"Telegram 聚合简报结算内容不同", lockErr)
	}
	now := time.Now().UTC()
	if _, err := tx.Exec(ctx,
		`UPDATE deliveries SET status=$5,sent_at=COALESCE(sent_at,$6)
		  WHERE tenant_id=$1 AND user_id=$2 AND batch_id=$3 AND id=ANY($4) AND
		        status IN ($7,$5)`, effect.TenantID, effect.UserID, batchID,
		deliveryIDs, types.DeliveryStatusSent, now,
		types.DeliveryStatusPending); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"结算 Telegram 聚合简报内容", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE task_observed_events
		    SET status='delivered',delivered_at=COALESCE(delivered_at,$5)
		  WHERE tenant_id=$1 AND user_id=$2 AND delivery_id=ANY($3) AND
		        status IN ('qualified','delivered') AND task_id=$4`,
		effect.TenantID, effect.UserID, deliveryIDs, taskID, now); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"结算 Telegram 聚合简报观测事件", err)
	}
	var pending bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM deliveries
		  WHERE tenant_id=$1 AND user_id=$2 AND batch_id=$3 AND status<>$4)`,
		effect.TenantID, effect.UserID, batchID,
		types.DeliveryStatusSent).Scan(&pending); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"检查 Telegram 聚合简报批次", err)
	}
	if !pending {
		if _, err := tx.Exec(ctx,
			`UPDATE push_batches SET status=$4
			  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND
			        delivery_authority='effect'`, batchID, effect.TenantID,
			effect.UserID, types.BatchStatusDone); err != nil {
			return types.NewAppError(types.CodeDatabase,
				"结算 Telegram 聚合简报批次", err)
		}
	}
	return nil
}

func lockAggregateDeliveryIDs(
	ctx context.Context, tx pgx.Tx, tenantID, userID, batchID int64,
	deliveryIDs []int64,
) ([]int64, error) {
	rows, err := tx.Query(ctx,
		`SELECT id FROM deliveries
		  WHERE tenant_id=$1 AND user_id=$2 AND batch_id=$3 AND id=ANY($4)
		  ORDER BY id FOR UPDATE`, tenantID, userID, batchID, deliveryIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	locked := make([]int64, 0, len(deliveryIDs))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		locked = append(locked, id)
	}
	return locked, rows.Err()
}
