package store

import (
	"context"

	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/types"
)

// StructuredEventEvidenceContentV1 is an Activity-only view of one content row
// and the deterministic frozen source identity through which the exact run
// admitted it. It must not cross an API or renderer boundary.
type StructuredEventEvidenceContentV1 struct {
	Item   types.ContentItem
	Source runcontext.SourceV1
}

// LoadStructuredEventEvidenceForTaskRunV1 loads the ordered, bounded content
// IDs already sealed by the qualifier. Every ID must still belong to at least
// one source in the exact immutable run snapshot. The lowest matching frozen
// source ID is deterministic display attribution when content appeared in
// multiple admitted sources.
func (s *Store) LoadStructuredEventEvidenceForTaskRunV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	contentIDs []int64,
) ([]StructuredEventEvidenceContentV1, error) {
	if len(contentIDs) == 0 || len(contentIDs) > 8 {
		return nil, taskRunValidationError(
			"structured event evidence content set is invalid")
	}
	seen := make(map[int64]struct{}, len(contentIDs))
	for _, contentID := range contentIDs {
		if contentID <= 0 {
			return nil, taskRunValidationError(
				"structured event evidence content id is invalid")
		}
		if _, exists := seen[contentID]; exists {
			return nil, taskRunValidationError(
				"structured event evidence content id is duplicated")
		}
		seen[contentID] = struct{}{}
	}

	tx, err := s.beginAuthorizedCompiledRunWriteV1(ctx, expected, ref)
	if err != nil {
		return nil, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	_, snapshot, _, err := loadAuthoritativeCompiledTaskRunSnapshot(
		ctx, tx, expected, ref)
	if err != nil {
		return nil, err
	}
	frozenSources := make(
		map[int64]runcontext.SourceV1, len(snapshot.Definition.Sources))
	sourceIDs := make([]int64, len(snapshot.Definition.Sources))
	for index, source := range snapshot.Definition.Sources {
		sourceIDs[index] = source.SourceID
		frozenSources[source.SourceID] = source
	}
	if len(sourceIDs) == 0 {
		return nil, taskRunIntegrityError()
	}

	rows, err := tx.Query(ctx,
		`WITH requested AS (
		     SELECT content_item_id, ordinal
		       FROM unnest($1::bigint[]) WITH ORDINALITY
		            AS input(content_item_id, ordinal)
		 )
		 SELECT ci.id, ci.source_id, ci.external_id, ci.canonical_key,
		        ci.url, ci.title, ci.content, ci.author, ci.published_at,
		        ci.content_hash, ci.simhash, ci.fetched_at, ci.created_at,
		        ci.kind, matched.source_id
		   FROM requested r
		   JOIN content_items ci ON ci.id=r.content_item_id
		   JOIN LATERAL (
		        SELECT MIN(cs.source_id) AS source_id
		          FROM content_sources cs
		         WHERE cs.content_item_id=ci.id
		           AND cs.source_id=ANY($2::bigint[])
		   ) matched ON matched.source_id IS NOT NULL
		  ORDER BY r.ordinal
		  FOR SHARE OF ci`,
		contentIDs, sourceIDs,
	)
	if err != nil {
		return nil, taskRunDatabaseError(
			"load structured event evidence content", err)
	}
	defer rows.Close()

	out := make([]StructuredEventEvidenceContentV1, 0, len(contentIDs))
	for rows.Next() {
		var (
			item     types.ContentItem
			sourceID int64
		)
		if err := rows.Scan(
			&item.ID, &item.SourceID, &item.ExternalID, &item.CanonicalKey,
			&item.URL, &item.Title, &item.Content, &item.Author,
			&item.PublishedAt, &item.ContentHash, &item.Simhash,
			&item.FetchedAt, &item.CreatedAt, &item.Kind, &sourceID,
		); err != nil {
			return nil, taskRunDatabaseError(
				"scan structured event evidence content", err)
		}
		source, ok := frozenSources[sourceID]
		if !ok {
			return nil, taskRunIntegrityError()
		}
		out = append(out, StructuredEventEvidenceContentV1{
			Item: item, Source: source,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, taskRunDatabaseError(
			"iterate structured event evidence content", err)
	}
	rows.Close()
	if len(out) != len(contentIDs) {
		return nil, types.NewAppError(
			types.CodeConflict,
			"structured event evidence is outside the frozen run snapshot", nil)
	}
	if err := commitCompiledRunWriteV1(
		ctx, tx, "commit structured event evidence read"); err != nil {
		return nil, err
	}
	return out, nil
}
