package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// ListDueSourcesByIDs returns the live health rows for the exact frozen source
// set that are still active and due. The caller overlays the frozen execution
// identity (URL, capability, title, and config) before fetching; this method
// deliberately supplies only the current source row and never expands scope
// through mutable task or user relationships.
func (s *Store) ListDueSourcesByIDs(ctx context.Context, sourceIDs []int64) ([]types.Source, error) {
	if err := validateCompiledRunSourceIDs(sourceIDs); err != nil {
		return nil, err
	}

	rows, err := s.pool.Query(ctx,
		`SELECT `+sourceColumns+`
		   FROM sources s
		  WHERE s.id = ANY($1::bigint[])
		    AND s.status = $2
		    AND s.next_fetch_at <= now()
		  ORDER BY s.id`,
		sourceIDs, types.SourceStatusActive)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			"查询编译运行的到期信源", err)
	}
	defer rows.Close()

	out := make([]types.Source, 0, len(sourceIDs))
	for rows.Next() {
		var src types.Source
		if err := scanSource(rows, &src); err != nil {
			return nil, types.NewAppError(types.CodeDatabase,
				"扫描编译运行 source 行", err)
		}
		out = append(out, src)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			"遍历编译运行 source 结果集", err)
	}
	return out, nil
}

// ListUnpushedForTaskRunV1 returns content observed by an allowed subset of the
// immutable source set and not yet delivered to the exact tenant+user. A source
// ID is usable only while its live semantic identity/config still exactly
// matches the frozen source. Drifted or deleted sources are omitted rather than
// failing stable siblings; status is deliberately excluded because disabling
// future fetches must not hide content already observed under the same identity.
// Delivery de-duplication stays cross-task inside that tenant, but a delivery in
// another tenant never suppresses this run. The sealed snapshot, live task,
// membership, exact-source locks, and candidate SELECT share one transaction.
func (s *Store) ListUnpushedForTaskRunV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	sourceIDs []int64,
	limit int,
	perSourceCap int,
) ([]types.ContentItem, error) {
	if limit <= 0 || perSourceCap <= 0 {
		return nil, types.NewAppError(types.CodeValidation,
			"编译运行候选参数必须为正数", nil)
	}
	if err := validateCompiledRunSourceIDs(sourceIDs); err != nil {
		return nil, err
	}
	tx, err := s.beginAuthorizedCompiledRunWriteV1(ctx, expected, ref)
	if err != nil {
		return nil, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	exactSourceIDs := make([]int64, 0, len(sourceIDs))
	for _, sourceID := range sourceIDs {
		frozen, err := loadAuthoritativeTaskRunSource(
			ctx, tx, expected, ref, sourceID)
		if err != nil {
			return nil, err
		}
		var exactID int64
		err = tx.QueryRow(ctx,
			`SELECT s.id
			   FROM sources s
			  WHERE s.id = $1
			    AND s.platform = $2 AND s.capability = $3
			    AND s.title = $4 AND s.url = $5
			    AND s.config = $6::jsonb
			  FOR SHARE OF s`,
			sourceID, frozen.Platform, frozen.Capability, frozen.Title,
			frozen.URL, frozen.Config,
		).Scan(&exactID)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, types.NewAppError(types.CodeDatabase,
				"锁定编译运行候选信源", err)
		}
		exactSourceIDs = append(exactSourceIDs, exactID)
	}
	if len(exactSourceIDs) == 0 {
		if err := commitCompiledRunWriteV1(ctx, tx,
			"commit empty compiled candidate read"); err != nil {
			return nil, err
		}
		return []types.ContentItem{}, nil
	}

	rows, err := tx.Query(ctx,
		`SELECT id, source_id, external_id, canonical_key, url, title, content, author,
		        published_at, content_hash, simhash, fetched_at, created_at, kind
		   FROM (
		       SELECT ci.id, ci.source_id, ci.external_id, ci.canonical_key, ci.url, ci.title,
		              ci.content, ci.author, ci.published_at, ci.content_hash, ci.simhash,
		              ci.fetched_at, ci.created_at, ci.kind,
		              ROW_NUMBER() OVER (
		                  PARTITION BY matched.source_id
		                  ORDER BY ci.fetched_at DESC, ci.id DESC
		              ) AS rn
		         FROM content_items ci
		         JOIN LATERAL (
		            SELECT MIN(cs.source_id) AS source_id
		              FROM content_sources cs
		             WHERE cs.content_item_id = ci.id
			       AND cs.source_id = ANY($3::bigint[])
		         ) matched ON matched.source_id IS NOT NULL
		        WHERE NOT EXISTS (
		              SELECT 1
		                FROM deliveries d
		               WHERE d.tenant_id = $1 AND d.user_id = $2
		                 AND d.content_item_id = ci.id
		                 AND d.status <> 'failed'
		                 AND NOT EXISTS (
		                     SELECT 1
		                       FROM task_observed_events e
		                       JOIN push_batches b ON b.id=d.batch_id
		                      WHERE e.delivery_id=d.id
		                        AND e.tenant_id=$1 AND e.user_id=$2
		                        AND e.status='qualified'
		                        AND e.created_at <=
		                            clock_timestamp() - interval '10 minutes'
		                        AND b.status IN ('failed','pending')
		                 )
		          )
		   ) candidates
		  WHERE candidates.rn <= $5
		  ORDER BY fetched_at DESC, id DESC
		  LIMIT $4`,
		expected.TenantID, expected.UserID, exactSourceIDs, limit, perSourceCap)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询租户 %d 用户 %d 的编译运行候选",
				expected.TenantID, expected.UserID), err)
	}
	defer rows.Close()

	out := make([]types.ContentItem, 0)
	for rows.Next() {
		var item types.ContentItem
		if err := rows.Scan(
			&item.ID, &item.SourceID, &item.ExternalID, &item.CanonicalKey, &item.URL,
			&item.Title, &item.Content, &item.Author, &item.PublishedAt, &item.ContentHash,
			&item.Simhash, &item.FetchedAt, &item.CreatedAt, &item.Kind,
		); err != nil {
			return nil, types.NewAppError(types.CodeDatabase,
				"扫描编译运行 content_item 行", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			"遍历编译运行 content_item 结果集", err)
	}
	rows.Close()
	if err := commitCompiledRunWriteV1(ctx, tx,
		"commit compiled candidate read"); err != nil {
		return nil, err
	}
	return out, nil
}

// SourceForContentFromIDs returns the lowest source ID in the intersection of
// an item's observed sources and the frozen source set. It is used for display
// attribution because ContentItem.SourceID is the global first observer and may
// not be part of this compiled run. An empty intersection is not an error.
func (s *Store) SourceForContentFromIDs(
	ctx context.Context,
	contentItemID int64,
	sourceIDs []int64,
) (int64, bool, error) {
	if contentItemID <= 0 {
		return 0, false, types.NewAppError(types.CodeValidation,
			"编译运行内容 id 必须为正数", nil)
	}
	if err := validateCompiledRunSourceIDs(sourceIDs); err != nil {
		return 0, false, err
	}

	var sourceID int64
	err := s.pool.QueryRow(ctx,
		`SELECT cs.source_id
		   FROM content_sources cs
		  WHERE cs.content_item_id = $1
		    AND cs.source_id = ANY($2::bigint[])
		  ORDER BY cs.source_id
		  LIMIT 1`,
		contentItemID, sourceIDs).Scan(&sourceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询内容 %d 的编译运行命中源", contentItemID), err)
	}
	return sourceID, true, nil
}

func validateCompiledRunSourceIDs(sourceIDs []int64) error {
	if len(sourceIDs) == 0 {
		return types.NewAppError(types.CodeValidation,
			"编译运行信源集合不能为空", nil)
	}

	seen := make(map[int64]struct{}, len(sourceIDs))
	for _, sourceID := range sourceIDs {
		if sourceID <= 0 {
			return types.NewAppError(types.CodeValidation,
				"编译运行信源 id 必须为正数", nil)
		}
		if _, exists := seen[sourceID]; exists {
			return types.NewAppError(types.CodeValidation,
				"编译运行信源集合不能包含重复 id", nil)
		}
		seen[sourceID] = struct{}{}
	}
	return nil
}
