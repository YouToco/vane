package store

import (
	"context"

	"github.com/YouToco/vane/types"
)

const maxTaskCreationSourceReferences = 64

// ResolveTaskCreationSources returns an ordered, by-value snapshot of existing
// sources that the exact tenant/user may use in a new task proposal. Entitlement
// v1 is deliberately narrow: the user must currently have an active subscription
// and the shared source itself must be active. A task-only schedule_sources link
// is not sufficient.
//
// Every unavailable case intentionally has the same observable result. Sources
// are globally shared, so distinguishing a missing id from another user's,
// another tenant's, inactive, or disabled source would create an enumeration
// oracle. The query carries tenant_id and user_id all the way through instead of
// loading a global source and authorizing it afterwards.
func (s *Store) ResolveTaskCreationSources(
	ctx context.Context,
	tenantID, userID int64,
	sourceIDs []int64,
) ([]types.Source, error) {
	if tenantID <= 0 || userID <= 0 || len(sourceIDs) == 0 ||
		len(sourceIDs) > maxTaskCreationSourceReferences {
		return nil, types.NewAppError(
			types.CodeValidation, "已有信源引用参数无效", nil,
		)
	}
	seen := make(map[int64]struct{}, len(sourceIDs))
	for _, sourceID := range sourceIDs {
		if sourceID <= 0 {
			return nil, types.NewAppError(
				types.CodeValidation, "已有信源引用参数无效", nil,
			)
		}
		if _, duplicate := seen[sourceID]; duplicate {
			return nil, types.NewAppError(
				types.CodeValidation, "已有信源引用参数无效", nil,
			)
		}
		seen[sourceID] = struct{}{}
	}

	rows, err := s.pool.Query(ctx,
		`SELECT `+sourceColumns+`
		   FROM unnest($3::bigint[]) WITH ORDINALITY AS requested(id, ord)
		   JOIN memberships m
		     ON m.tenant_id = $1 AND m.user_id = $2
		   JOIN tenants t
		     ON t.id = m.tenant_id
		    AND t.status = $4 AND t.deleted_at IS NULL
		   JOIN subscriptions sub
		     ON sub.tenant_id = m.tenant_id
		    AND sub.user_id = m.user_id
		    AND sub.source_id = requested.id
		    AND sub.status = $5
		   JOIN sources s
		     ON s.id = requested.id AND s.status = $6
		  ORDER BY requested.ord`,
		tenantID, userID, sourceIDs,
		types.TenantStatusActive,
		types.SubscriptionStatusActive,
		types.SourceStatusActive,
	)
	if err != nil {
		return nil, types.NewAppError(
			types.CodeDatabase, "解析任务已有信源", err,
		)
	}
	defer rows.Close()

	resolved := make([]types.Source, 0, len(sourceIDs))
	for rows.Next() {
		var source types.Source
		if err := scanSource(rows, &source); err != nil {
			return nil, types.NewAppError(
				types.CodeDatabase, "扫描任务已有信源", err,
			)
		}
		resolved = append(resolved, source)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(
			types.CodeDatabase, "遍历任务已有信源", err,
		)
	}
	if len(resolved) != len(sourceIDs) {
		return nil, unavailableTaskCreationSource()
	}
	for i, source := range resolved {
		if source.ID != sourceIDs[i] {
			return nil, types.NewAppError(
				types.CodeInternal, "任务已有信源顺序损坏", types.ErrInternal,
			)
		}
	}
	return resolved, nil
}

func unavailableTaskCreationSource() error {
	return types.NewAppError(
		types.CodeNotFound, "一个或多个已有信源当前不可用", nil,
	)
}
