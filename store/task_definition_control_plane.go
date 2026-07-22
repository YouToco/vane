package store

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

// ApprovedDefinitionEditParams is the complete PostgreSQL-side input for one
// immutable definition edit. ApprovalRef is an opaque, durable confirmation
// identity and the idempotency key for the resulting version. This Store
// primitive cannot prove that a user confirmed the candidate: a future durable
// edit controller must bind the reference to its frozen proposal and confirmed
// action before this method acquires a production call point.
type ApprovedDefinitionEditParams struct {
	ExpectedHead ApprovedDefinitionFence
	Definition   taskstate.ApprovedDefinitionV1
	ApprovalRef  string
}

// CommitApprovedDefinitionEdit atomically appends one immutable compiled
// definition, updates every retained legacy projection, and advances the exact
// schedule head. It intentionally has zero production call points in C2b-2.
//
// PostgreSQL cannot atomically update a Temporal Schedule. In particular,
// SpecJSON, ScopeJSON, and NLDescription must not reach this primitive from a
// live entry point until a durable edit saga can pause, reconcile, and recover
// the external schedule. Generic pending_actions v0 claims before execution
// and is therefore not a valid caller.
func (s *Store) CommitApprovedDefinitionEdit(
	ctx context.Context,
	p ApprovedDefinitionEditParams,
) (ApprovedDefinitionVersionRecord, error) {
	if p.ExpectedHead.Version <= 0 || p.ExpectedHead.Version == math.MaxInt64 ||
		!validTaskStateDigest(p.ExpectedHead.Digest) {
		return ApprovedDefinitionVersionRecord{}, taskStateValidation(
			"approved definition edit base is invalid")
	}
	payload, digest, definition, err := encodeApprovedDefinitionForStore(p.Definition)
	if err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}
	if definition.Intent != definition.PlaybookContent {
		return ApprovedDefinitionVersionRecord{}, taskStateValidation(
			"approved definition intent is not representable by the legacy projection")
	}
	// legacy_subscriptions is a one-way compatibility marker for old/baseline
	// definitions, not a user-approved long-term source plan. A confirmed v2+
	// edit must materialize the exact sources it authorizes.
	if definition.SourceScope != taskstate.SourceScopeApprovedPlan {
		return ApprovedDefinitionVersionRecord{}, taskStateValidation(
			"approved definition edit requires an exact approved source plan")
	}
	if !validTaskStateReference(p.ApprovalRef, 1024) {
		return ApprovedDefinitionVersionRecord{}, taskStateValidation(
			"approved definition edit confirmation reference is invalid")
	}
	approvalRef := p.ApprovalRef
	nextVersion := p.ExpectedHead.Version + 1

	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ApprovedDefinitionVersionRecord{}, taskStateDatabaseError(
			"begin approved definition edit transaction", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)

	head, err := lockTaskDefinitionEditScope(ctx, tx, definition.TenantID,
		definition.UserID, definition.TaskID)
	if err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}
	existing, err := loadApprovedDefinitionByApprovalRefTx(ctx, tx,
		definition.TenantID, definition.UserID, definition.TaskID, approvalRef)
	if err == nil {
		return replayApprovedDefinitionEdit(ctx, tx, head, p.ExpectedHead,
			existing, definition, payload, digest)
	}
	if !errors.Is(err, types.ErrNotFound) {
		return ApprovedDefinitionVersionRecord{}, err
	}

	if head.Version == nil || head.Digest == nil {
		return ApprovedDefinitionVersionRecord{}, taskStateConflict(
			"approved definition edit requires an immutable head")
	}
	if *head.Version != p.ExpectedHead.Version ||
		!constantTimeTaskStateDigestEqual(*head.Digest, p.ExpectedHead.Digest) {
		return ApprovedDefinitionVersionRecord{}, taskStateConflict(
			"approved definition head changed before the edit")
	}
	if head.Mode != types.ExecutionModeCompiled || definition.ExecutionMode != head.Mode {
		return ApprovedDefinitionVersionRecord{}, taskStateConflict(
			"approved definition edit mode differs from the current head")
	}
	base, err := loadApprovedDefinitionVersionTx(ctx, tx, definition.TenantID,
		definition.UserID, definition.TaskID, p.ExpectedHead.Version)
	if err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}
	if !constantTimeTaskStateDigestEqual(base.Digest, p.ExpectedHead.Digest) ||
		base.Definition.ExecutionMode != head.Mode {
		return ApprovedDefinitionVersionRecord{}, taskStateIntegrity()
	}
	if err := validateApprovedDefinitionProjectionTx(
		ctx, tx, base.Definition, base.Payload); err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}
	if err := rejectAdaptiveStateForDefinitionEdit(ctx, tx, definition); err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}
	sourceIDs, err := lockApprovedDefinitionEditSources(ctx, tx, definition)
	if err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}

	record := ApprovedDefinitionVersionRecord{
		Definition: definition, Version: nextVersion, Digest: digest,
		Payload: bytes.Clone(payload), ApprovalRef: approvalRef,
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO task_approved_definition_versions (
			tenant_id, user_id, task_id, version, schema_version,
			execution_mode, definition_digest, payload, approval_ref
		 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 RETURNING created_at`,
		definition.TenantID, definition.UserID, definition.TaskID,
		record.Version, definition.SchemaVersion, definition.ExecutionMode,
		record.Digest, record.Payload, record.ApprovalRef,
	).Scan(&record.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return ApprovedDefinitionVersionRecord{}, taskStateConflict(
				"approved definition edit version or confirmation already exists")
		}
		return ApprovedDefinitionVersionRecord{}, taskStateDatabaseError(
			"append approved definition edit", err)
	}

	if err := updateApprovedDefinitionLegacyProjectionTx(
		ctx, tx, definition, sourceIDs); err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}
	tag, err := tx.Exec(ctx,
		`UPDATE schedules s
		    SET nl_description=$4, spec_json=$5, scope_json=$6,
		        push_strictness=$7, execution_mode=$8,
		        approved_definition_version=$9,
		        approved_definition_digest=$10,
		        updated_at=clock_timestamp()
		  WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3
		    AND s.approved_definition_version=$11
		    AND s.approved_definition_digest=$12
		    AND s.execution_mode=$13
		    AND `+matureSchedulePredicate,
		definition.TenantID, definition.UserID, definition.TaskID,
		definition.NLDescription, []byte(definition.SpecJSON),
		[]byte(definition.ScopeJSON), string(definition.Strictness),
		definition.ExecutionMode, record.Version, record.Digest,
		p.ExpectedHead.Version, p.ExpectedHead.Digest, head.Mode,
	)
	if err != nil {
		return ApprovedDefinitionVersionRecord{}, taskStateDatabaseError(
			"advance approved definition edit head", err)
	}
	if tag.RowsAffected() != 1 {
		return ApprovedDefinitionVersionRecord{}, taskStateConflict(
			"approved definition head changed while applying the edit")
	}
	if err := validateApprovedDefinitionProjectionTx(
		ctx, tx, definition, record.Payload); err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ApprovedDefinitionVersionRecord{}, taskStateDatabaseError(
			"commit approved definition edit", err)
	}
	return record, nil
}

func lockTaskDefinitionEditScope(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID int64,
	taskID string,
) (taskDefinitionHead, error) {
	if tenantID <= 0 || userID <= 0 || !validTaskStateReference(taskID, 255) {
		return taskDefinitionHead{}, taskStateValidation(
			"approved definition edit scope is invalid")
	}
	var head taskDefinitionHead
	var rawMode string
	err := tx.QueryRow(ctx,
		`SELECT s.execution_mode, s.approved_definition_version,
		        s.approved_definition_digest
		   FROM schedules s
		   JOIN tenants t ON t.id=s.tenant_id
		   JOIN memberships m ON m.tenant_id=s.tenant_id AND m.user_id=s.user_id
		  WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3
		    AND t.status='active' AND t.deleted_at IS NULL
		    AND `+matureSchedulePredicate+`
		  FOR UPDATE OF s FOR SHARE OF t, m`,
		tenantID, userID, taskID,
	).Scan(&rawMode, &head.Version, &head.Digest)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return taskDefinitionHead{}, taskStateNotFound()
		}
		return taskDefinitionHead{}, taskStateDatabaseError(
			"lock approved definition edit scope", err)
	}
	mode, err := types.ParseExecutionMode(rawMode)
	if err != nil || (head.Version == nil) != (head.Digest == nil) {
		return taskDefinitionHead{}, taskStateIntegrity()
	}
	head.Mode = mode
	return head, nil
}

func replayApprovedDefinitionEdit(
	ctx context.Context,
	tx pgx.Tx,
	head taskDefinitionHead,
	expected ApprovedDefinitionFence,
	existing ApprovedDefinitionVersionRecord,
	definition taskstate.ApprovedDefinitionV1,
	payload []byte,
	digest string,
) (ApprovedDefinitionVersionRecord, error) {
	if existing.Version != expected.Version+1 ||
		!constantTimeTaskStateDigestEqual(existing.Digest, digest) ||
		!bytes.Equal(existing.Payload, payload) ||
		existing.Definition.ExecutionMode != definition.ExecutionMode {
		return ApprovedDefinitionVersionRecord{}, taskStateConflict(
			"approved definition edit confirmation already has another result")
	}
	base, err := loadApprovedDefinitionVersionTx(ctx, tx, definition.TenantID,
		definition.UserID, definition.TaskID, expected.Version)
	if err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}
	if !constantTimeTaskStateDigestEqual(base.Digest, expected.Digest) {
		return ApprovedDefinitionVersionRecord{}, taskStateConflict(
			"approved definition edit confirmation has another base")
	}
	if head.Version == nil || head.Digest == nil || *head.Version < existing.Version {
		return ApprovedDefinitionVersionRecord{}, taskStateIntegrity()
	}
	if *head.Version == existing.Version {
		if !constantTimeTaskStateDigestEqual(*head.Digest, existing.Digest) ||
			head.Mode != existing.Definition.ExecutionMode {
			return ApprovedDefinitionVersionRecord{}, taskStateIntegrity()
		}
		if err := validateApprovedDefinitionProjectionTx(
			ctx, tx, existing.Definition, existing.Payload); err != nil {
			return ApprovedDefinitionVersionRecord{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ApprovedDefinitionVersionRecord{}, taskStateDatabaseError(
			"commit approved definition edit replay", err)
	}
	return existing, nil
}

func rejectAdaptiveStateForDefinitionEdit(
	ctx context.Context,
	tx pgx.Tx,
	definition taskstate.ApprovedDefinitionV1,
) error {
	var exists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM task_adaptive_states
			 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		)`,
		definition.TenantID, definition.UserID, definition.TaskID,
	).Scan(&exists); err != nil {
		return taskStateDatabaseError("check adaptive state before definition edit", err)
	}
	if exists {
		return taskStateConflict(
			"approved definition edit requires an explicit adaptive-state transition")
	}
	return nil
}

func lockApprovedDefinitionEditSources(
	ctx context.Context,
	tx pgx.Tx,
	definition taskstate.ApprovedDefinitionV1,
) ([]int64, error) {
	sourceIDs := make([]int64, 0, len(definition.Sources))
	wantURLs := make(map[int64]string, len(definition.Sources))
	for _, source := range definition.Sources {
		sourceIDs = append(sourceIDs, source.SourceID)
		wantURLs[source.SourceID] = source.URL
	}
	rows, err := tx.Query(ctx,
		`SELECT id, url
		   FROM sources
		  WHERE id=ANY($1::bigint[])
		  ORDER BY url, id
		  FOR KEY SHARE`, sourceIDs)
	if err != nil {
		return nil, taskStateDatabaseError(
			"lock approved definition edit sources", err)
	}
	defer rows.Close()
	seen := make(map[int64]struct{}, len(sourceIDs))
	for rows.Next() {
		var sourceID int64
		var sourceURL string
		if err := rows.Scan(&sourceID, &sourceURL); err != nil {
			return nil, taskStateDatabaseError(
				"scan approved definition edit source", err)
		}
		wantURL, ok := wantURLs[sourceID]
		if !ok || wantURL != sourceURL {
			return nil, taskStateConflict(
				"approved definition source identity changed")
		}
		seen[sourceID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, taskStateDatabaseError(
			"iterate approved definition edit sources", err)
	}
	if len(seen) != len(sourceIDs) {
		return nil, taskStateConflict(
			"approved definition source does not exist")
	}
	return sourceIDs, nil
}

func updateApprovedDefinitionLegacyProjectionTx(
	ctx context.Context,
	tx pgx.Tx,
	definition taskstate.ApprovedDefinitionV1,
	sourceIDs []int64,
) error {
	tag, err := tx.Exec(ctx,
		`UPDATE schedule_playbooks
		    SET content=$2, fetch_plan=$3, updated_at=clock_timestamp()
		  WHERE schedule_id=$1`,
		definition.TaskID, definition.PlaybookContent, []byte(definition.FetchPlan),
	)
	if err != nil {
		return taskStateDatabaseError(
			"update approved definition playbook projection", err)
	}
	if tag.RowsAffected() != 1 {
		return taskStateIntegrity()
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM schedule_sources WHERE schedule_id=$1`,
		definition.TaskID); err != nil {
		return taskStateDatabaseError(
			"clear approved definition source projection", err)
	}
	sort.Slice(sourceIDs, func(i, j int) bool { return sourceIDs[i] < sourceIDs[j] })
	for _, sourceID := range sourceIDs {
		if _, err := tx.Exec(ctx,
			`INSERT INTO schedule_sources (schedule_id, source_id)
			 VALUES ($1, $2)`, definition.TaskID, sourceID); err != nil {
			return taskStateDatabaseError(
				"insert approved definition source projection", err)
		}
	}
	var exact bool
	if err := tx.QueryRow(ctx,
		`SELECT
			(SELECT count(*) FROM schedule_sources WHERE schedule_id=$1)
			  = cardinality($2::bigint[])
			AND NOT EXISTS (
				(SELECT source_id FROM schedule_sources WHERE schedule_id=$1)
				EXCEPT
				(SELECT source_id FROM unnest($2::bigint[]) AS wanted(source_id))
			)
			AND NOT EXISTS (
				(SELECT source_id FROM unnest($2::bigint[]) AS wanted(source_id))
				EXCEPT
				(SELECT source_id FROM schedule_sources WHERE schedule_id=$1)
			)`, definition.TaskID, sourceIDs,
	).Scan(&exact); err != nil {
		return taskStateDatabaseError(
			"verify approved definition source projection", err)
	}
	if !exact {
		return taskStateIntegrity()
	}
	return nil
}
