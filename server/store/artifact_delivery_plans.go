package store

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

const (
	ArtifactDeliveryAggregateBrief = "aggregate_brief"
	ArtifactDeliveryPeriodicReport = "periodic_report"
	ArtifactDeliveryResearchV3     = "research_v3"
)

var artifactDeliveryPlanNamespace = uuid.MustParse("cf1231bd-b214-51d7-ad85-00666842f3ec")

type ArtifactDeliveryPlan struct {
	ID              string                   `json:"id"`
	TenantID        int64                    `json:"tenant_id"`
	UserID          int64                    `json:"user_id"`
	TaskID          string                   `json:"task_id"`
	ArtifactKind    string                   `json:"artifact_kind"`
	ArtifactKey     string                   `json:"artifact_key"`
	Selection       DeliveryChannelSelection `json:"selection"`
	TelegramRouteID *int64                   `json:"telegram_route_id,omitempty"`
	CreatedAt       time.Time                `json:"created_at"`
}

func validArtifactDeliveryKind(kind string) bool {
	return kind == ArtifactDeliveryAggregateBrief ||
		kind == ArtifactDeliveryPeriodicReport || kind == ArtifactDeliveryResearchV3
}

func scanArtifactDeliveryPlan(row pgx.Row) (ArtifactDeliveryPlan, error) {
	var plan ArtifactDeliveryPlan
	var id uuid.UUID
	err := row.Scan(&id, &plan.TenantID, &plan.UserID, &plan.TaskID,
		&plan.ArtifactKind, &plan.ArtifactKey, &plan.Selection,
		&plan.TelegramRouteID, &plan.CreatedAt)
	plan.ID = id.String()
	if err == nil && (plan.TenantID <= 0 || plan.UserID <= 0 ||
		strings.TrimSpace(plan.TaskID) != plan.TaskID || plan.TaskID == "" ||
		!validArtifactDeliveryKind(plan.ArtifactKind) ||
		strings.TrimSpace(plan.ArtifactKey) != plan.ArtifactKey ||
		plan.ArtifactKey == "" || !plan.Selection.Valid() ||
		(plan.Selection.Includes("telegram") != (plan.TelegramRouteID != nil))) {
		return ArtifactDeliveryPlan{}, errors.New("artifact delivery plan integrity mismatch")
	}
	return plan, err
}

// PrepareArtifactDeliveryPlan is first-writer-wins for one immutable business
// artifact. It resolves an implicit Telegram private route only before the
// insert; replay returns the stored provider set even when mutable preferences
// have since changed.
func (s *Store) PrepareArtifactDeliveryPlan(
	ctx context.Context, tenantID, userID int64, taskID, artifactKind,
	artifactKey string, preference DeliveryChannelPreference,
) (ArtifactDeliveryPlan, error) {
	if err := validateDeliveryPreferenceScope(tenantID, userID, taskID); err != nil ||
		taskID == "" || !validArtifactDeliveryKind(artifactKind) ||
		strings.TrimSpace(artifactKey) != artifactKey || artifactKey == "" ||
		len(artifactKey) > 512 || !preference.Selection.Valid() ||
		(preference.TelegramRouteID != nil && *preference.TelegramRouteID <= 0) {
		return ArtifactDeliveryPlan{}, types.NewAppError(types.CodeValidation,
			"产物投递计划无效", types.ErrValidation)
	}
	planID := uuid.NewSHA1(artifactDeliveryPlanNamespace, []byte(strings.Join(
		[]string{strconv.FormatInt(tenantID, 10), strconv.FormatInt(userID, 10),
			taskID, artifactKind, artifactKey}, "\x00")))
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ArtifactDeliveryPlan{}, types.NewAppError(types.CodeDatabase,
			"开启产物投递计划", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10), strconv.FormatInt(userID, 10)); err != nil {
		return ArtifactDeliveryPlan{}, types.NewAppError(types.CodeDatabase,
			"绑定产物投递计划范围", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return ArtifactDeliveryPlan{}, types.NewAppError(types.CodeDatabase,
			"进入产物投递计划角色", err)
	}
	const columns = `id,tenant_id,user_id,task_id,artifact_kind,artifact_key,
		selection,telegram_route_id,created_at`
	existing, err := scanArtifactDeliveryPlan(tx.QueryRow(ctx,
		`SELECT `+columns+` FROM artifact_delivery_plans
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND
		        artifact_kind=$4 AND artifact_key=$5`,
		tenantID, userID, taskID, artifactKind, artifactKey))
	if err == nil {
		if existing.ID != planID.String() {
			return ArtifactDeliveryPlan{}, types.NewAppError(types.CodeConflict,
				"产物投递计划身份冲突", types.ErrConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return ArtifactDeliveryPlan{}, types.NewAppError(types.CodeDatabase,
				"提交产物投递计划重放", err)
		}
		return existing, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ArtifactDeliveryPlan{}, types.NewAppError(types.CodeDatabase,
			"读取产物投递计划", err)
	}
	routeID := preference.TelegramRouteID
	if preference.Selection.Includes("telegram") {
		var resolved int64
		if err := tx.QueryRow(ctx,
			`SELECT * FROM lock_artifact_delivery_telegram_route_v1($1,$2,$3)`,
			tenantID, userID, routeID).Scan(&resolved); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ArtifactDeliveryPlan{}, types.NewAppError(types.CodeNotFound,
					"Telegram 投递目的地不可用", types.ErrNotFound)
			}
			return ArtifactDeliveryPlan{}, types.NewAppError(types.CodeDatabase,
				"冻结 Telegram 投递目的地", err)
		}
		routeID = &resolved
	} else {
		routeID = nil
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO artifact_delivery_plans
		 (id,tenant_id,user_id,task_id,artifact_kind,artifact_key,selection,telegram_route_id)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, planID, tenantID, userID,
		taskID, artifactKind, artifactKey, preference.Selection, routeID); err != nil {
		return ArtifactDeliveryPlan{}, types.NewAppError(types.CodeDatabase,
			"写入产物投递计划", err)
	}
	plan, err := scanArtifactDeliveryPlan(tx.QueryRow(ctx,
		`SELECT `+columns+` FROM artifact_delivery_plans WHERE id=$1`, planID))
	if err != nil {
		return ArtifactDeliveryPlan{}, types.NewAppError(types.CodeDatabase,
			"读取已冻结产物投递计划", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ArtifactDeliveryPlan{}, types.NewAppError(types.CodeDatabase,
			"提交产物投递计划", err)
	}
	return plan, nil
}

func (s *Store) LoadArtifactDeliveryPlan(
	ctx context.Context, tenantID, userID int64, taskID, artifactKind,
	artifactKey string,
) (ArtifactDeliveryPlan, error) {
	if err := validateDeliveryPreferenceScope(tenantID, userID, taskID); err != nil ||
		taskID == "" || !validArtifactDeliveryKind(artifactKind) ||
		strings.TrimSpace(artifactKey) != artifactKey || artifactKey == "" {
		return ArtifactDeliveryPlan{}, types.NewAppError(types.CodeValidation,
			"产物投递计划范围无效", types.ErrValidation)
	}
	const columns = `id,tenant_id,user_id,task_id,artifact_kind,artifact_key,
		selection,telegram_route_id,created_at`
	plan, err := scanArtifactDeliveryPlan(s.pool.QueryRow(ctx,
		`SELECT `+columns+` FROM artifact_delivery_plans
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND
		        artifact_kind=$4 AND artifact_key=$5`,
		tenantID, userID, taskID, artifactKind, artifactKey))
	if errors.Is(err, pgx.ErrNoRows) {
		return ArtifactDeliveryPlan{}, types.NewAppError(types.CodeNotFound,
			"产物投递计划不存在", types.ErrNotFound)
	}
	if err != nil {
		return ArtifactDeliveryPlan{}, types.NewAppError(types.CodeDatabase,
			"读取产物投递计划", err)
	}
	return plan, nil
}
