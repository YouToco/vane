package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// UpsertContentItemForTaskRunV1 persists globally shared content only while
// the exact compiled run is still live. The authorization locks and all three
// content writes share one transaction, so a concurrent task/tenant/member
// revocation cannot commit in the gap between authorization and persistence.
// sourceID must be one of the immutable snapshot sources; mutable task links
// and subscriptions are never consulted.
func (s *Store) UpsertContentItemForTaskRunV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	sourceID int64,
	item *types.ContentItem,
) (id int64, isNew bool, err error) {
	if item == nil || sourceID <= 0 || item.SourceID != sourceID ||
		item.CanonicalKey == "" {
		return 0, false, taskRunValidationError("compiled content input is invalid")
	}
	tx, err := s.beginAuthorizedCompiledRunWriteV1(ctx, expected, ref)
	if err != nil {
		return 0, false, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)

	frozen, err := loadAuthoritativeTaskRunSource(
		ctx, tx, expected, ref, sourceID)
	if err != nil {
		return 0, false, err
	}
	// Hold the exact global source identity stable until its appearance is
	// committed. If it changed while the network call was in flight, linking the
	// old response to the reused source_id would make future tasks see source A's
	// content as source B's. Fail closed before either the global content row or
	// its appearance is written.
	var sourceExact bool
	if err := tx.QueryRow(ctx,
		`SELECT true
		   FROM sources s
		  WHERE s.id = $1
		    AND s.platform = $2 AND s.capability = $3
		    AND s.title = $4 AND s.url = $5
		    AND s.config = $6::jsonb
		    AND s.status = $7
		  FOR SHARE OF s`,
		sourceID, frozen.Platform, frozen.Capability, frozen.Title, frozen.URL,
		frozen.Config, types.SourceStatusActive,
	).Scan(&sourceExact); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false, taskRunNotFound()
		}
		return 0, false, taskRunDatabaseError("lock compiled content source", err)
	}
	if !sourceExact {
		return 0, false, taskRunNotFound()
	}

	isNew = true
	err = tx.QueryRow(ctx,
		`INSERT INTO content_items (
		    source_id, external_id, canonical_key, url, title, content, author,
		    published_at, content_hash, simhash, kind
		 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		 ON CONFLICT (canonical_key) DO NOTHING
		 RETURNING id`,
		item.SourceID, item.ExternalID, item.CanonicalKey, item.URL, item.Title,
		item.Content, item.Author, item.PublishedAt, item.ContentHash, item.Simhash,
		item.Kind,
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		isNew = false
		err = tx.QueryRow(ctx,
			`SELECT id FROM content_items WHERE canonical_key = $1`,
			item.CanonicalKey,
		).Scan(&id)
	}
	if err != nil {
		return 0, false, taskRunDatabaseError("upsert compiled content item", err)
	}

	if !isNew {
		if _, err := tx.Exec(ctx,
			`UPDATE content_items
			    SET content = $2, content_hash = $3, simhash = $4
			  WHERE id = $1 AND char_length(content) < char_length($2)`,
			id, item.Content, item.ContentHash, item.Simhash,
		); err != nil {
			return 0, false, taskRunDatabaseError("update compiled content body", err)
		}
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO content_sources (content_item_id, source_id, external_id, url)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (content_item_id, source_id) DO NOTHING`,
		id, sourceID, item.ExternalID, item.URL,
	); err != nil {
		return 0, false, taskRunDatabaseError("link compiled content source", err)
	}
	if err := commitCompiledRunWriteV1(ctx, tx, "commit compiled content item"); err != nil {
		return 0, false, err
	}
	return id, isNew, nil
}

// UpdateSourceFetchStateForTaskRunV1 advances shared source health only when
// the live source still has the exact identity/config frozen in this run. A
// historical snapshot therefore cannot advance health for a reconfigured
// global source. updated=false is the safe, expected stale-snapshot no-op.
func (s *Store) UpdateSourceFetchStateForTaskRunV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	sourceID int64,
	lastFetched time.Time,
	nextFetch time.Time,
	failCount int,
) (updated bool, err error) {
	if sourceID <= 0 || nextFetch.IsZero() || failCount < 0 {
		return false, taskRunValidationError("compiled source fetch state input is invalid")
	}
	tx, err := s.beginAuthorizedCompiledRunWriteV1(ctx, expected, ref)
	if err != nil {
		return false, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	frozen, err := loadAuthoritativeTaskRunSource(
		ctx, tx, expected, ref, sourceID)
	if err != nil {
		return false, err
	}

	tag, err := tx.Exec(ctx,
		`UPDATE sources s
		    SET last_fetched_at = $2, next_fetch_at = $3,
		        fail_count = $4, updated_at = now()
		  WHERE s.id = $1
		    AND s.platform = $5 AND s.capability = $6
		    AND s.title = $7 AND s.url = $8
		    AND s.config = $9::jsonb
		    AND s.status = $10`,
		sourceID, lastFetched, nextFetch, failCount,
		frozen.Platform, frozen.Capability, frozen.Title, frozen.URL, frozen.Config,
		types.SourceStatusActive,
	)
	if err != nil {
		return false, taskRunDatabaseError("update compiled source fetch state", err)
	}
	if err := commitCompiledRunWriteV1(ctx, tx,
		"commit compiled source fetch state"); err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// DisableSourceIfActiveForTaskRunV1 is the exact stale-snapshot-safe variant
// of DisableSourceIfActive. The immutable source must belong to the run and its
// current global identity/config must still match before status can change.
func (s *Store) DisableSourceIfActiveForTaskRunV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	sourceID int64,
) (disabled bool, err error) {
	if sourceID <= 0 {
		return false, taskRunValidationError("compiled source disable input is invalid")
	}
	tx, err := s.beginAuthorizedCompiledRunWriteV1(ctx, expected, ref)
	if err != nil {
		return false, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	frozen, err := loadAuthoritativeTaskRunSource(
		ctx, tx, expected, ref, sourceID)
	if err != nil {
		return false, err
	}

	tag, err := tx.Exec(ctx,
		`UPDATE sources s
		    SET status = $2, updated_at = now()
		  WHERE s.id = $1 AND s.status = $3
		    AND s.platform = $4 AND s.capability = $5
		    AND s.title = $6 AND s.url = $7
		    AND s.config = $8::jsonb`,
		sourceID, types.SourceStatusDisabled, types.SourceStatusActive,
		frozen.Platform, frozen.Capability, frozen.Title, frozen.URL, frozen.Config,
	)
	if err != nil {
		return false, taskRunDatabaseError("disable compiled source", err)
	}
	if err := commitCompiledRunWriteV1(ctx, tx,
		"commit compiled source disable"); err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// ListRecentSimhashesForTaskRunV1 preserves user-wide, cross-task near-dup
// behavior inside one tenant. History is visible when the content belongs to
// an active subscription, any task-private source owned by that tenant+user,
// or an existing delivery. The delivery arm deliberately survives task/source
// unlinking so previously pushed content cannot be reintroduced as new. The
// explicit tenant predicates prevent a user with memberships in two tenants
// from carrying one tenant's history into the other. The live run and sealed
// snapshot are revalidated in the same read transaction.
func (s *Store) ListRecentSimhashesForTaskRunV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	since time.Time,
	excludeIDs []int64,
) ([]int64, error) {
	if since.IsZero() {
		return nil, taskRunValidationError("compiled simhash window is invalid")
	}
	if excludeIDs == nil {
		excludeIDs = []int64{}
	}
	tx, err := s.beginAuthorizedCompiledRunWriteV1(ctx, expected, ref)
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
		            FROM content_sources cs
		            JOIN subscriptions sub ON sub.source_id = cs.source_id
		           WHERE cs.content_item_id = ci.id
		             AND sub.tenant_id = $1 AND sub.user_id = $2
		             AND sub.status = $5
		      )
		      OR EXISTS (
		          SELECT 1
		            FROM content_sources cs
		            JOIN schedule_sources ss ON ss.source_id = cs.source_id
		            JOIN schedules s ON s.id = ss.schedule_id
		           WHERE cs.content_item_id = ci.id
		             AND s.tenant_id = $1 AND s.user_id = $2
		      )
		      OR EXISTS (
		          SELECT 1
		            FROM deliveries d
		           WHERE d.content_item_id = ci.id
		             AND d.tenant_id = $1 AND d.user_id = $2
		      )
		  )
		    AND ci.fetched_at >= $3 AND ci.simhash IS NOT NULL
		    AND ci.id <> ALL($4)
		  ORDER BY ci.fetched_at DESC, ci.id DESC`,
		expected.TenantID, expected.UserID, since, excludeIDs,
		types.SubscriptionStatusActive,
	)
	if err != nil {
		return nil, taskRunDatabaseError("query compiled recent simhashes", err)
	}
	defer rows.Close()

	out := make([]int64, 0)
	for rows.Next() {
		var simhash int64
		if err := rows.Scan(&simhash); err != nil {
			return nil, taskRunDatabaseError("scan compiled simhash", err)
		}
		out = append(out, simhash)
	}
	if err := rows.Err(); err != nil {
		return nil, taskRunDatabaseError("iterate compiled simhashes", err)
	}
	rows.Close()
	if err := commitCompiledRunWriteV1(ctx, tx,
		"commit compiled simhash read"); err != nil {
		return nil, err
	}
	return out, nil
}

func loadAuthoritativeTaskRunSource(
	ctx context.Context,
	tx pgx.Tx,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	sourceID int64,
) (taskRunSourceIdentityV1, error) {
	_, snapshot, _, err := loadAuthoritativeCompiledTaskRunSnapshot(
		ctx, tx, expected, ref)
	if err != nil {
		return taskRunSourceIdentityV1{}, err
	}
	for _, source := range snapshot.Definition.Sources {
		if source.SourceID == sourceID {
			return taskRunSourceIdentityV1{
				SourceID: source.SourceID, Platform: string(source.Platform),
				Capability: string(source.Capability), Title: source.Title,
				URL: source.URL, Config: source.Config,
			}, nil
		}
	}
	return taskRunSourceIdentityV1{}, taskRunValidationError(
		"compiled source is outside the frozen task scope")
}
