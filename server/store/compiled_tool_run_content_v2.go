package store

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/runcontext"
	"github.com/YouToco/vane/server/types"
)

// LoadContentObservationForTaskRunV2 recovers an already-committed atomic
// result before any provider call. It validates the exact V2 snapshot and
// invocation but deliberately does not require the task to remain live:
// returning immutable evidence is not a new external or database side effect.
func (s *Store) LoadContentObservationForTaskRunV2(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRefV2,
	invocationDigest string,
) ([]types.ContentItem, bool, error) {
	tx, snapshot, err := s.beginCompiledToolRunReadV2(ctx, expected, ref)
	if err != nil {
		return nil, false, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if !snapshotContainsInvocationV2(snapshot, invocationDigest) {
		return nil, false, taskRunValidationError(
			"Tool invocation is outside the frozen run")
	}
	items, found, err := loadContentObservationV2(
		ctx, tx, expected, ref, invocationDigest)
	if err != nil {
		return nil, false, err
	}
	if err := commitCompiledRunWriteV1(
		ctx, tx, "commit Tool observation recovery"); err != nil {
		return nil, false, err
	}
	return items, found, nil
}

// ListContentCandidatesForTaskRunV2 reads the exact immutable observation
// sets in frozen invocation order and removes content already delivered to the
// same user. It never routes through Source/content_sources candidate readers.
func (s *Store) ListContentCandidatesForTaskRunV2(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRefV2,
	limit int,
) ([]runcontext.ToolCandidateV1, error) {
	if limit <= 0 || limit > runcontext.MaxToolObservationItemsV1 {
		return nil, taskRunValidationError(
			"Tool candidate limit is invalid")
	}
	tx, snapshot, err := s.beginCompiledToolRunReadV2(ctx, expected, ref)
	if err != nil {
		return nil, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)

	ordered := make([]runcontext.ToolCandidateV1, 0)
	seenContentIDs := make(map[int64]struct{})
	for _, call := range snapshot.Definition.ToolCalls {
		items, found, err := loadContentObservationV2(
			ctx, tx, expected, ref, call.Digest)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, types.NewAppError(types.CodeConflict,
				"Tool run observation set is incomplete", nil)
		}
		for _, item := range items {
			if _, duplicate := seenContentIDs[item.ID]; duplicate {
				continue
			}
			seenContentIDs[item.ID] = struct{}{}
			ordered = append(ordered, runcontext.ToolCandidateV1{
				InvocationDigest: call.Digest,
				Item:             item,
			})
		}
	}
	if len(ordered) == 0 {
		if err := commitCompiledRunWriteV1(
			ctx, tx, "commit empty Tool candidate read"); err != nil {
			return nil, err
		}
		return []runcontext.ToolCandidateV1{}, nil
	}
	contentIDs := make([]int64, len(ordered))
	for i := range ordered {
		contentIDs[i] = ordered[i].Item.ID
	}
	rows, err := tx.Query(ctx,
		`SELECT DISTINCT content_item_id
		   FROM deliveries
		  WHERE tenant_id=$1 AND user_id=$2
		    AND content_item_id=ANY($3::bigint[])
		    AND status<>'failed'`,
		expected.TenantID, expected.UserID, contentIDs)
	if err != nil {
		return nil, taskRunDatabaseError(
			"query delivered Tool content", err)
	}
	delivered := make(map[int64]struct{})
	for rows.Next() {
		var contentID int64
		if err := rows.Scan(&contentID); err != nil {
			rows.Close()
			return nil, taskRunDatabaseError(
				"scan delivered Tool content", err)
		}
		delivered[contentID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, taskRunDatabaseError(
			"iterate delivered Tool content", err)
	}
	rows.Close()
	candidates := make(
		[]runcontext.ToolCandidateV1, 0, min(limit, len(ordered)))
	for _, candidate := range ordered {
		if _, alreadyDelivered :=
			delivered[candidate.Item.ID]; alreadyDelivered {
			continue
		}
		candidates = append(candidates, candidate)
		if len(candidates) == limit {
			break
		}
	}
	if err := commitCompiledRunWriteV1(
		ctx, tx, "commit Tool candidate read"); err != nil {
		return nil, err
	}
	return candidates, nil
}

// CommitContentObservationForTaskRunV2 atomically upserts globally shared
// content with source_id=NULL and commits one immutable result set keyed by
// (run_snapshot_id, invocation_digest). It never writes content_sources.
func (s *Store) CommitContentObservationForTaskRunV2(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRefV2,
	invocationDigest string,
	items []types.ContentItem,
) ([]types.ContentItem, error) {
	if items == nil || len(items) > runcontext.MaxToolObservationItemsV1 {
		return nil, taskRunValidationError(
			"Tool observation result set is invalid")
	}
	for i := range items {
		if items[i].SourceID != 0 ||
			strings.TrimSpace(items[i].ExternalID) == "" ||
			strings.TrimSpace(items[i].CanonicalKey) == "" ||
			strings.TrimSpace(items[i].ContentHash) == "" {
			return nil, taskRunValidationError(
				"Source-free Tool content is invalid")
		}
	}
	tx, snapshot, err := s.beginAuthorizedCompiledToolRunWriteV2(
		ctx, expected, ref)
	if err != nil {
		return nil, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if !snapshotContainsInvocationV2(snapshot, invocationDigest) {
		return nil, taskRunValidationError(
			"Tool invocation is outside the frozen run")
	}
	if recovered, found, err := loadContentObservationV2(
		ctx, tx, expected, ref, invocationDigest,
	); err != nil {
		return nil, err
	} else if found {
		if err := commitCompiledRunWriteV1(
			ctx, tx, "commit Tool observation recovery"); err != nil {
			return nil, err
		}
		return recovered, nil
	}

	persisted := make([]types.ContentItem, len(items))
	contentIDs := make([]int64, len(items))
	for i := range items {
		item := items[i]
		var id int64
		err := tx.QueryRow(ctx,
			`INSERT INTO content_items (
			    source_id, external_id, canonical_key, url, title, content,
			    author, published_at, content_hash, simhash, kind
			 ) VALUES (
			    NULL, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
			 )
			 ON CONFLICT (canonical_key) DO NOTHING
			 RETURNING id`,
			item.ExternalID, item.CanonicalKey, item.URL, item.Title,
			item.Content, item.Author, item.PublishedAt, item.ContentHash,
			item.Simhash, item.Kind,
		).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			err = tx.QueryRow(ctx,
				`SELECT id FROM content_items WHERE canonical_key=$1`,
				item.CanonicalKey,
			).Scan(&id)
		}
		if err != nil {
			return nil, taskRunDatabaseError(
				"upsert Source-free Tool content", err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE content_items
			    SET content=$2, content_hash=$3, simhash=$4
			  WHERE id=$1 AND char_length(content)<char_length($2)`,
			id, item.Content, item.ContentHash, item.Simhash,
		); err != nil {
			return nil, taskRunDatabaseError(
				"update Source-free Tool content body", err)
		}
		item.ID = id
		item.SourceID = 0
		persisted[i] = item
		contentIDs[i] = id
	}
	_, payload, digest, err := runcontext.BuildToolObservationSetV1(
		ref.SnapshotID, invocationDigest, persisted)
	if err != nil {
		return nil, taskRunValidationError(
			"Tool observation result cannot be sealed")
	}
	tag, err := tx.Exec(ctx,
		`INSERT INTO task_run_content_provenance (
		    tenant_id,user_id,task_id,run_snapshot_id,invocation_digest,
		    content_item_ids,observation_payload,observation_digest
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		 ON CONFLICT (run_snapshot_id,invocation_digest) DO NOTHING`,
		expected.TenantID, expected.UserID, expected.TaskID,
		ref.SnapshotID, invocationDigest, contentIDs, payload, digest,
	)
	if err != nil {
		return nil, taskRunDatabaseError(
			"commit Source-free Tool observation", err)
	}
	if tag.RowsAffected() == 0 {
		recovered, found, err := loadContentObservationV2(
			ctx, tx, expected, ref, invocationDigest)
		if err != nil {
			return nil, err
		}
		if !found || !sameObservedContentV1(recovered, persisted) {
			return nil, types.NewAppError(types.CodeConflict,
				"Tool invocation already committed a different result", nil)
		}
		persisted = recovered
	}
	if err := commitCompiledRunWriteV1(
		ctx, tx, "commit Source-free Tool observation"); err != nil {
		return nil, err
	}
	return persisted, nil
}

func (s *Store) beginAuthorizedCompiledToolRunWriteV2(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRefV2,
) (pgx.Tx, runcontext.CompiledSnapshotV2, error) {
	tx, snapshot, err := s.beginCompiledToolRunReadV2(ctx, expected, ref)
	if err != nil {
		return nil, runcontext.CompiledSnapshotV2{}, err
	}
	if err := lockLiveCompiledRunWriteV1(ctx, tx, expected); err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, runcontext.CompiledSnapshotV2{}, err
	}
	return tx, snapshot, nil
}

func (s *Store) beginCompiledToolRunReadV2(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRefV2,
) (pgx.Tx, runcontext.CompiledSnapshotV2, error) {
	if validateTaskRunSnapshotReferenceForExpectedV2(ref, expected) != nil {
		return nil, runcontext.CompiledSnapshotV2{},
			taskRunValidationError("task run v2 snapshot reference is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, runcontext.CompiledSnapshotV2{},
			taskRunDatabaseError("begin Tool run transaction", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		fmt.Sprintf("%d", expected.TenantID)); err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, runcontext.CompiledSnapshotV2{},
			taskRunDatabaseError("set Tool run tenant context", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, runcontext.CompiledSnapshotV2{},
			taskRunDatabaseError("enter Tool run role", err)
	}
	lookup := taskRunLookupFromIdentity(expected)
	stored, found, err := loadTaskRunSnapshot(ctx, tx, lookup)
	if err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, runcontext.CompiledSnapshotV2{}, err
	}
	if !found {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, runcontext.CompiledSnapshotV2{}, taskRunNotFound()
	}
	storedRef, err := stored.safeRefV2()
	if err != nil || storedRef != ref {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, runcontext.CompiledSnapshotV2{}, taskRunIntegrityError()
	}
	decoded, err := readTaskRunSnapshotPayloadV2(stored.Payload)
	if err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, runcontext.CompiledSnapshotV2{}, taskRunIntegrityError()
	}
	payload := decoded.Payload
	snapshot := runcontext.CompiledSnapshotV2{
		Ref: storedRef, Mode: payload.Mode,
		DefinitionVersion:              payload.DefinitionVersion,
		AdaptiveVersion:                payload.AdaptiveVersion,
		AdaptiveDigest:                 payload.AdaptiveDigest,
		AdaptiveBasisDefinitionVersion: payload.AdaptiveBasisDefinitionVersion,
		AdaptiveBasisDefinitionDigest:  payload.AdaptiveBasisDefinitionDigest,
		ObservationRollout:             payload.ObservationRollout,
		Budget:                         types.PlannerBudget{},
		Definition:                     payload.Definition,
		Adaptive:                       payload.Adaptive,
		ToolBindings:                   payload.ToolBindings,
		Policy:                         decoded.Policy,
	}
	if snapshot.ValidateFor(expected) != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, runcontext.CompiledSnapshotV2{}, taskRunIntegrityError()
	}
	return tx, snapshot, nil
}

func loadContentObservationV2(
	ctx context.Context,
	tx pgx.Tx,
	expected types.RunIdentity,
	ref types.RunSnapshotRefV2,
	invocationDigest string,
) ([]types.ContentItem, bool, error) {
	var contentIDs []int64
	var payload []byte
	var storedDigest string
	err := tx.QueryRow(ctx,
		`SELECT content_item_ids,observation_payload,observation_digest
		   FROM task_run_content_provenance
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		    AND run_snapshot_id=$4 AND invocation_digest=$5`,
		expected.TenantID, expected.UserID, expected.TaskID,
		ref.SnapshotID, invocationDigest,
	).Scan(&contentIDs, &payload, &storedDigest)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, taskRunDatabaseError(
			"load Source-free Tool observation", err)
	}
	set, items, digest, err :=
		runcontext.DecodeToolObservationSetV1(payload)
	if err != nil || set.RunSnapshotID != ref.SnapshotID ||
		set.InvocationDigest != invocationDigest ||
		!constantTimeObservationDigestEqualV1(digest, storedDigest) ||
		len(contentIDs) != len(items) {
		return nil, false, taskRunIntegrityError()
	}
	itemIDs := make([]int64, len(items))
	for i := range items {
		itemIDs[i] = items[i].ID
	}
	if !slices.Equal(contentIDs, itemIDs) {
		return nil, false, taskRunIntegrityError()
	}
	return items, true, nil
}

func snapshotContainsInvocationV2(
	snapshot runcontext.CompiledSnapshotV2,
	invocationDigest string,
) bool {
	if len(invocationDigest) != 64 {
		return false
	}
	for _, call := range snapshot.Definition.ToolCalls {
		if call.Digest == invocationDigest {
			return true
		}
	}
	return false
}

func constantTimeObservationDigestEqualV1(left, right string) bool {
	return len(left) == 64 && len(right) == 64 &&
		subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func sameObservedContentV1(
	left []types.ContentItem,
	right []types.ContentItem,
) bool {
	const comparisonSnapshotID int64 = 1
	comparisonInvocationDigest := strings.Repeat("a", 64)
	_, leftRaw, _, leftErr := runcontext.BuildToolObservationSetV1(
		comparisonSnapshotID, comparisonInvocationDigest, left)
	_, rightRaw, _, rightErr := runcontext.BuildToolObservationSetV1(
		comparisonSnapshotID, comparisonInvocationDigest, right)
	return leftErr == nil && rightErr == nil &&
		slices.Equal(leftRaw, rightRaw)
}
