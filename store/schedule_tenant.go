package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// ResolveActiveTenantForUser is the legacy scheduler bridge: its public API
// predates explicit tenant scope, but a durable Schedule Action may no longer
// omit TenantID. Missing and ambiguous memberships both fail closed before any
// Temporal mutation.
func (s *Store) ResolveActiveTenantForUser(ctx context.Context, userID int64) (int64, error) {
	if userID <= 0 {
		return 0, types.NewAppError(types.CodeValidation, "用户 ID 无效", nil)
	}
	rows, err := s.pool.Query(ctx,
		`SELECT m.tenant_id
		   FROM memberships m
		   JOIN tenants t ON t.id = m.tenant_id
		  WHERE m.user_id = $1 AND t.status = $2 AND t.deleted_at IS NULL
		  ORDER BY m.tenant_id
		  LIMIT 2`,
		userID, types.TenantStatusActive)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "解析用户租户", err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return 0, types.NewAppError(types.CodeDatabase, "扫描用户租户", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "遍历用户租户", err)
	}
	switch len(ids) {
	case 0:
		return 0, types.NewAppError(types.CodeNotFound, "用户没有有效租户", pgx.ErrNoRows)
	case 1:
		return ids[0], nil
	default:
		return 0, types.NewAppError(types.CodeInternal,
			fmt.Sprintf("用户 %d 同时属于多个有效租户", userID), errors.New("ambiguous tenant"))
	}
}
