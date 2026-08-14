package store

import (
	"context"
	"time"

	"github.com/YouToco/vane/server/types"
)

// ListRecentSimhashesForTaskRunV2 reads user-level delivered/content history
// under the exact live V2 run. It does not require or consult Source
// appearances.
func (s *Store) ListRecentSimhashesForTaskRunV2(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRefV2,
	since time.Time,
	excludeIDs []int64,
) ([]int64, error) {
	if since.IsZero() {
		return nil, taskRunValidationError(
			"compiled Tool simhash window is invalid")
	}
	if excludeIDs == nil {
		excludeIDs = []int64{}
	}
	tx, _, err := s.beginAuthorizedCompiledToolRunWriteV2(
		ctx, expected, ref)
	if err != nil {
		return nil, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)

	rows, err := tx.Query(ctx,
		`SELECT ci.simhash
		   FROM content_items ci
		  WHERE (
		      EXISTS (
		          SELECT 1
		            FROM deliveries d
		           WHERE d.content_item_id = ci.id
		             AND d.tenant_id = $1 AND d.user_id = $2
		      )
		      OR EXISTS (
		          SELECT 1
		            FROM task_run_content_provenance p
		           WHERE p.tenant_id = $1 AND p.user_id = $2
		             AND ci.id = ANY(p.content_item_ids)
		      )
		  )
		    AND ci.fetched_at >= $3 AND ci.simhash IS NOT NULL
		    AND ci.id <> ALL($4)
		  ORDER BY ci.fetched_at DESC, ci.id DESC`,
		expected.TenantID, expected.UserID, since, excludeIDs)
	if err != nil {
		return nil, taskRunDatabaseError(
			"query compiled Tool recent simhashes", err)
	}
	defer rows.Close()
	out := make([]int64, 0)
	for rows.Next() {
		var simhash int64
		if err := rows.Scan(&simhash); err != nil {
			return nil, taskRunDatabaseError(
				"scan compiled Tool simhash", err)
		}
		out = append(out, simhash)
	}
	if err := rows.Err(); err != nil {
		return nil, taskRunDatabaseError(
			"iterate compiled Tool simhashes", err)
	}
	rows.Close()
	if err := commitCompiledRunWriteV1(
		ctx, tx, "commit compiled Tool simhash read"); err != nil {
		return nil, err
	}
	return out, nil
}
