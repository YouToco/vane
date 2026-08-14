package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/taskstate"
	"github.com/YouToco/vane/server/types"
)

// ApprovedDefinitionEditParams is the complete PostgreSQL-side input for one
// immutable definition edit. OperationRef is an opaque durable identity and the
// idempotency key for the resulting version. The controller binds it to the
// frozen operation before this method acquires a production call point.
type ApprovedDefinitionEditParams struct {
	ExpectedHead ApprovedDefinitionFence
	Definition   taskstate.ApprovedDefinitionV1
	OperationRef string
}

type approvedDefinitionEditCommand struct {
	expectedHead ApprovedDefinitionFence
	definition   taskstate.ApprovedDefinitionV1
	payload      []byte
	digest       string
	operationRef string
}

// CommitApprovedDefinitionEdit atomically appends one immutable compiled
// definition, updates every retained legacy projection, and advances the exact
// schedule head. It intentionally has zero production call points in C2b-2.
//
// PostgreSQL cannot atomically update a Temporal Schedule. In particular,
// SpecJSON, ScopeJSON, and NLDescription must not reach this primitive from a
// live entry point until a durable edit saga can pause, reconcile, and recover
// the external schedule. Generic task_creation_operations v0 claims before execution
// and is therefore not a valid caller.
func (s *Store) CommitApprovedDefinitionEdit(
	ctx context.Context,
	p ApprovedDefinitionEditParams,
) (ApprovedDefinitionVersionRecord, error) {
	command, err := prepareApprovedDefinitionEditCurrent(p)
	if err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}

	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ApprovedDefinitionVersionRecord{}, taskStateDatabaseError(
			"begin approved definition edit transaction", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)

	head, err := lockTaskDefinitionEditScope(ctx, tx, command.definition.TenantID,
		command.definition.UserID, command.definition.TaskID)
	if err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}
	existing, err := loadApprovedDefinitionByOperationRefTx(ctx, tx,
		command.definition.TenantID, command.definition.UserID,
		command.definition.TaskID, command.operationRef)
	if err == nil {
		record, replayErr := replayApprovedDefinitionEdit(
			ctx, tx, head, command.expectedHead, existing,
			command.definition, command.payload, command.digest,
		)
		if replayErr != nil {
			return ApprovedDefinitionVersionRecord{}, replayErr
		}
		if err := tx.Commit(ctx); err != nil {
			return ApprovedDefinitionVersionRecord{}, taskStateDatabaseError(
				"commit approved definition edit replay", err)
		}
		return record, nil
	}
	if !errors.Is(err, types.ErrNotFound) {
		return ApprovedDefinitionVersionRecord{}, err
	}
	if s.legacyAdmissionIsClosed() {
		return ApprovedDefinitionVersionRecord{},
			legacyAdmissionClosed("approved definition edit v1/v2")
	}

	record, err := appendApprovedDefinitionEditTx(ctx, tx, head, command)
	if err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ApprovedDefinitionVersionRecord{}, taskStateDatabaseError(
			"commit approved definition edit", err)
	}
	return record, nil
}

func prepareApprovedDefinitionEditCurrent(
	p ApprovedDefinitionEditParams,
) (approvedDefinitionEditCommand, error) {
	if p.ExpectedHead.Version <= 0 || p.ExpectedHead.Version == math.MaxInt64 ||
		!validTaskStateDigest(p.ExpectedHead.Digest) {
		return approvedDefinitionEditCommand{}, taskStateValidation(
			"approved definition edit base is invalid")
	}
	payload, digest, definition, err := encodeApprovedDefinitionForStore(p.Definition)
	if err != nil {
		return approvedDefinitionEditCommand{}, err
	}
	if definition.Intent != definition.PlaybookContent {
		return approvedDefinitionEditCommand{}, taskStateValidation(
			"approved definition intent is not representable by the legacy projection")
	}
	// legacy_subscriptions is a one-way compatibility marker for old/baseline
	// definitions, not a user-authorized long-term acquisition plan. A v2+
	// edit must materialize the exact sources it authorizes.
	if definition.SourceScope != taskstate.SourceScopeApprovedPlan {
		return approvedDefinitionEditCommand{}, taskStateValidation(
			"approved definition edit requires an exact approved source plan")
	}
	if !validTaskStateReference(p.OperationRef, 1024) {
		return approvedDefinitionEditCommand{}, taskStateValidation(
			"approved definition edit operation reference is invalid")
	}
	return approvedDefinitionEditCommand{
		expectedHead: p.ExpectedHead,
		definition:   definition,
		payload:      bytes.Clone(payload),
		digest:       digest,
		operationRef: p.OperationRef,
	}, nil
}

// appendApprovedDefinitionEditTx performs only the new append path. The
// caller must already hold the schedule row lock and owns commit/rollback.
// Historical approval-ref replay is deliberately excluded so a durable edit
// operation can never mistake an old successful version for current write
// authority.
func appendApprovedDefinitionEditTx(
	ctx context.Context,
	tx pgx.Tx,
	head taskDefinitionHead,
	command approvedDefinitionEditCommand,
) (ApprovedDefinitionVersionRecord, error) {
	definition := command.definition
	if head.Version == nil || head.Digest == nil {
		return ApprovedDefinitionVersionRecord{}, taskStateConflict(
			"approved definition edit requires an immutable head")
	}
	if *head.Version != command.expectedHead.Version ||
		!constantTimeTaskStateDigestEqual(*head.Digest, command.expectedHead.Digest) {
		return ApprovedDefinitionVersionRecord{}, taskStateConflict(
			"approved definition head changed before the edit")
	}
	if head.Mode != types.ExecutionModeCompiled || definition.ExecutionMode != head.Mode {
		return ApprovedDefinitionVersionRecord{}, taskStateConflict(
			"approved definition edit mode differs from the current head")
	}
	base, err := loadApprovedDefinitionVersionTx(ctx, tx, definition.TenantID,
		definition.UserID, definition.TaskID, command.expectedHead.Version)
	if err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}
	if !constantTimeTaskStateDigestEqual(base.Digest, command.expectedHead.Digest) ||
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
		Definition: definition, Version: command.expectedHead.Version + 1,
		Digest: command.digest, Payload: bytes.Clone(command.payload),
		OperationRef: command.operationRef,
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO task_approved_definition_versions (
			tenant_id, user_id, task_id, version, schema_version,
			execution_mode, definition_digest, payload, operation_ref
		 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 RETURNING created_at`,
		definition.TenantID, definition.UserID, definition.TaskID,
		record.Version, definition.SchemaVersion, definition.ExecutionMode,
		record.Digest, record.Payload, record.OperationRef,
	).Scan(&record.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return ApprovedDefinitionVersionRecord{}, taskStateConflict(
				"approved definition edit version or operation already exists")
		}
		return ApprovedDefinitionVersionRecord{}, taskStateDatabaseError(
			"append approved definition edit", err)
	}

	if err := updateApprovedDefinitionCurrentProjectionTx(
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
		command.expectedHead.Version, command.expectedHead.Digest, head.Mode,
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
			"approved definition edit operation already has another result")
	}
	base, err := loadApprovedDefinitionVersionTx(ctx, tx, definition.TenantID,
		definition.UserID, definition.TaskID, expected.Version)
	if err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}
	if !constantTimeTaskStateDigestEqual(base.Digest, expected.Digest) {
		return ApprovedDefinitionVersionRecord{}, taskStateConflict(
			"approved definition edit operation has another base")
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
	wantSources := make(map[int64]taskstate.ApprovedSourceV1, len(definition.Sources))
	for _, source := range definition.Sources {
		sourceIDs = append(sourceIDs, source.SourceID)
		wantSources[source.SourceID] = source
	}
	rows, err := tx.Query(ctx,
		`SELECT id, platform, capability, url, config
		   FROM fetch_targets
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
		var platform types.Platform
		var capability types.Capability
		var sourceURL string
		var config json.RawMessage
		if err := rows.Scan(
			&sourceID, &platform, &capability, &sourceURL, &config,
		); err != nil {
			return nil, taskStateDatabaseError(
				"scan approved definition edit source", err)
		}
		want, ok := wantSources[sourceID]
		if !ok || want.URL != sourceURL ||
			want.Platform != platform || want.Capability != capability ||
			!taskCreationJSONEqual(want.Config, config) {
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

func updateApprovedDefinitionCurrentProjectionTx(
	ctx context.Context,
	tx pgx.Tx,
	definition taskstate.ApprovedDefinitionV1,
	sourceIDs []int64,
) error {
	currentFetchPlan, err := currentFetchPlanFromApprovedDefinitionV1(definition)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx,
		`UPDATE schedule_playbooks
		    SET content=$2, fetch_plan=$3, updated_at=clock_timestamp()
		  WHERE schedule_id=$1`,
		definition.TaskID, definition.PlaybookContent, currentFetchPlan,
	)
	if err != nil {
		return taskStateDatabaseError(
			"update approved definition playbook projection", err)
	}
	if tag.RowsAffected() != 1 {
		return taskStateIntegrity()
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM task_fetch_targets WHERE schedule_id=$1`,
		definition.TaskID); err != nil {
		return taskStateDatabaseError(
			"clear approved definition source projection", err)
	}
	sort.Slice(sourceIDs, func(i, j int) bool { return sourceIDs[i] < sourceIDs[j] })
	for _, sourceID := range sourceIDs {
		if _, err := tx.Exec(ctx,
			`INSERT INTO task_fetch_targets (schedule_id, fetch_target_id)
			 VALUES ($1, $2)`, definition.TaskID, sourceID); err != nil {
			return taskStateDatabaseError(
				"insert approved definition source projection", err)
		}
	}
	var exact bool
	if err := tx.QueryRow(ctx,
		`SELECT
			(SELECT count(*) FROM task_fetch_targets WHERE schedule_id=$1)
			  = cardinality($2::bigint[])
			AND NOT EXISTS (
				(SELECT fetch_target_id FROM task_fetch_targets WHERE schedule_id=$1)
				EXCEPT
				(SELECT source_id FROM unnest($2::bigint[]) AS wanted(source_id))
			)
			AND NOT EXISTS (
				(SELECT source_id FROM unnest($2::bigint[]) AS wanted(source_id))
				EXCEPT
				(SELECT fetch_target_id FROM task_fetch_targets WHERE schedule_id=$1)
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

// currentFetchPlanFromApprovedDefinitionV1 is the only write-side adapter from
// the deployed immutable v1 wire to mutable current state. The v1 payload keeps
// its historical sources field; schedule_playbooks must store targets.
func currentFetchPlanFromApprovedDefinitionV1(
	definition taskstate.ApprovedDefinitionV1,
) ([]byte, error) {
	var frozen taskstate.FetchPlanV1
	if err := json.Unmarshal(definition.FetchPlan, &frozen); err != nil ||
		frozen.Sources == nil {
		return nil, taskStateIntegrity()
	}
	current := currentFetchPlanProjection{
		Targets: make([]currentFetchPlanTargetProjection, len(frozen.Sources)),
	}
	for index, source := range frozen.Sources {
		current.Targets[index] = currentFetchPlanTargetProjection{
			Platform: string(source.Platform), Capability: string(source.Capability),
			Title: source.Title, URL: source.URL,
			Config: bytes.Clone(source.Config),
		}
	}
	raw, err := json.Marshal(current)
	if err != nil {
		return nil, taskStateIntegrity()
	}
	return raw, nil
}
