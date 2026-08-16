package store

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

type DeliveryChannelSelection string

const (
	DeliveryChannelFeishu   DeliveryChannelSelection = "feishu"
	DeliveryChannelTelegram DeliveryChannelSelection = "telegram"
	DeliveryChannelBoth     DeliveryChannelSelection = "both"
)

func (s DeliveryChannelSelection) Valid() bool {
	return s == DeliveryChannelFeishu || s == DeliveryChannelTelegram ||
		s == DeliveryChannelBoth
}

func (s DeliveryChannelSelection) Includes(provider string) bool {
	switch provider {
	case "feishu":
		return s == DeliveryChannelFeishu || s == DeliveryChannelBoth
	case "telegram":
		return s == DeliveryChannelTelegram || s == DeliveryChannelBoth
	default:
		return false
	}
}

type DeliveryChannelPreference struct {
	Selection       DeliveryChannelSelection `json:"selection"`
	Scope           string                   `json:"scope"`
	TaskID          string                   `json:"task_id,omitempty"`
	TelegramRouteID *int64                   `json:"telegram_route_id,omitempty"`
	Explicit        bool                     `json:"explicit"`
	UpdatedAt       *time.Time               `json:"updated_at,omitempty"`
}

func validateDeliveryPreferenceScope(tenantID, userID int64, taskID string) error {
	if tenantID <= 0 || userID <= 0 || strings.TrimSpace(taskID) != taskID ||
		len(taskID) > 255 {
		return types.NewAppError(types.CodeValidation,
			"投递渠道偏好范围无效", types.ErrValidation)
	}
	return nil
}

// ResolveDeliveryChannelPreference applies task override -> account default ->
// legacy Feishu default.  Missing configuration therefore preserves existing
// users and historical behavior.
func (s *Store) ResolveDeliveryChannelPreference(
	ctx context.Context, tenantID, userID int64, taskID string,
) (DeliveryChannelPreference, error) {
	if err := validateDeliveryPreferenceScope(tenantID, userID, taskID); err != nil {
		return DeliveryChannelPreference{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return DeliveryChannelPreference{}, types.NewAppError(
			types.CodeDatabase, "开启投递渠道偏好读取", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10), strconv.FormatInt(userID, 10)); err != nil {
		return DeliveryChannelPreference{}, types.NewAppError(
			types.CodeDatabase, "绑定投递渠道偏好范围", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return DeliveryChannelPreference{}, types.NewAppError(
			types.CodeDatabase, "进入投递渠道偏好角色", err)
	}
	var selection DeliveryChannelSelection
	var storedTaskID *string
	var resultRouteID *int64
	var updatedAt time.Time
	err = tx.QueryRow(ctx,
		`SELECT selection,task_id,telegram_route_id,updated_at
		   FROM delivery_channel_preferences
		  WHERE tenant_id=$1 AND user_id=$2 AND
		        (task_id=$3 OR task_id IS NULL)
		  ORDER BY task_id IS NULL
		  LIMIT 1`, tenantID, userID, taskID).Scan(
		&selection, &storedTaskID, &resultRouteID, &updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return DeliveryChannelPreference{}, types.NewAppError(
				types.CodeDatabase, "提交默认投递渠道偏好", err)
		}
		return DeliveryChannelPreference{
			Selection: DeliveryChannelFeishu, Scope: "default", Explicit: false,
		}, nil
	}
	if err != nil {
		return DeliveryChannelPreference{}, types.NewAppError(
			types.CodeDatabase, "读取投递渠道偏好", err)
	}
	if !selection.Valid() {
		return DeliveryChannelPreference{}, types.NewAppError(
			types.CodeConflict, "投递渠道偏好完整性校验失败", types.ErrConflict)
	}
	result := DeliveryChannelPreference{
		Selection: selection, Scope: "account", TelegramRouteID: resultRouteID,
		Explicit: true, UpdatedAt: &updatedAt,
	}
	if storedTaskID != nil {
		result.Scope, result.TaskID = "task", *storedTaskID
	}
	if err := tx.Commit(ctx); err != nil {
		return DeliveryChannelPreference{}, types.NewAppError(
			types.CodeDatabase, "提交投递渠道偏好读取", err)
	}
	return result, nil
}

func (s *Store) PutAccountDeliveryChannelPreference(
	ctx context.Context, tenantID, userID int64, selection DeliveryChannelSelection,
	telegramRouteID *int64,
) (DeliveryChannelPreference, error) {
	if selection == DeliveryChannelFeishu {
		telegramRouteID = nil
	}
	if err := validateDeliveryPreferenceScope(tenantID, userID, ""); err != nil ||
		!selection.Valid() || (telegramRouteID != nil && *telegramRouteID <= 0) {
		return DeliveryChannelPreference{}, types.NewAppError(
			types.CodeValidation, "投递渠道偏好无效", types.ErrValidation)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return DeliveryChannelPreference{}, types.NewAppError(
			types.CodeDatabase, "开启投递渠道偏好写入", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10), strconv.FormatInt(userID, 10)); err != nil {
		return DeliveryChannelPreference{}, types.NewAppError(
			types.CodeDatabase, "绑定投递渠道偏好范围", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return DeliveryChannelPreference{}, types.NewAppError(
			types.CodeDatabase, "进入投递渠道偏好角色", err)
	}
	if telegramRouteID != nil {
		var routeActive bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (
			 SELECT 1 FROM channel_routes
			  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND
			        provider='telegram' AND status='active'
			)`, *telegramRouteID, tenantID, userID).Scan(&routeActive); err != nil {
			return DeliveryChannelPreference{}, types.NewAppError(
				types.CodeDatabase, "验证 Telegram 投递目的地", err)
		}
		if !routeActive {
			return DeliveryChannelPreference{}, types.NewAppError(
				types.CodeNotFound, "Telegram 投递目的地不可用", types.ErrNotFound)
		}
	}
	var updatedAt time.Time
	err = tx.QueryRow(ctx,
		`INSERT INTO delivery_channel_preferences
		     (tenant_id,user_id,task_id,selection,telegram_route_id)
		 VALUES ($1,$2,NULL,$3,$4)
		 ON CONFLICT ON CONSTRAINT uq_delivery_channel_preference_scope
		 DO UPDATE SET selection=EXCLUDED.selection,
		               telegram_route_id=EXCLUDED.telegram_route_id
		 RETURNING updated_at`, tenantID, userID, selection, telegramRouteID).Scan(&updatedAt)
	if err != nil {
		return DeliveryChannelPreference{}, types.NewAppError(
			types.CodeDatabase, "保存投递渠道偏好", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return DeliveryChannelPreference{}, types.NewAppError(
			types.CodeDatabase, "提交投递渠道偏好", err)
	}
	return DeliveryChannelPreference{
		Selection: selection, Scope: "account", TelegramRouteID: telegramRouteID,
		Explicit: true, UpdatedAt: &updatedAt,
	}, nil
}
