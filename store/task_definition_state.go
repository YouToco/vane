package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

const initialApprovedDefinitionVersion int64 = 1

const legacyTaskProjectionDefaultStrictnessV1 types.PushStrictness = "loose"

// currentFetchPlanProjection is the mutable schedule_playbooks boundary. The
// baseline adapter converts it into the immutable task-approved-definition/v1
// wire, whose historical Sources field remains isolated in taskstate.
type currentFetchPlanProjection struct {
	Targets []currentFetchPlanTargetProjection `json:"targets"`
}

type currentFetchPlanTargetProjection struct {
	Platform   string          `json:"platform"`
	Capability string          `json:"capability"`
	Title      string          `json:"title,omitempty"`
	URL        string          `json:"url"`
	Config     json.RawMessage `json:"config,omitempty"`
}

// ApprovedDefinitionVersionRecord is one immutable, scope-bound definition.
// Payload remains available for byte-for-byte shadow comparisons during C2c;
// callers should otherwise consume the strictly decoded Definition.
type ApprovedDefinitionVersionRecord struct {
	Definition   taskstate.ApprovedDefinitionV1
	Version      int64
	Digest       string
	Payload      []byte
	OperationRef string
	CreatedAt    time.Time
}

// AdaptiveStateRecord is the current CAS-fenced low-risk state for one task.
type AdaptiveStateRecord struct {
	State                          taskstate.AdaptiveStateV1
	Version                        int64
	Digest                         string
	Payload                        []byte
	BasisDefinitionVersion         int64
	BasisDefinitionDigest          string
	LastKnownGoodDefinitionVersion *int64
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
}

// ApprovedDefinitionFence identifies the exact immutable definition bytes a
// caller consumed. Adaptive writes must carry the fence from their run
// snapshot so work started under an older authorized intent cannot update state
// after the user approves a new definition.
type ApprovedDefinitionFence struct {
	Version int64
	Digest  string
}

// InsertInitialApprovedDefinition installs only a baseline version-1 head.
// It is a C2a database primitive, not proof of owner authorization and not an
// edit API. C2b must add the authorized command/CAS writer that atomically
// updates every legacy projection. Until then an AST guard keeps this method
// at zero production call points.
func (s *Store) InsertInitialApprovedDefinition(
	ctx context.Context,
	definition taskstate.ApprovedDefinitionV1,
	operationRef string,
) (ApprovedDefinitionVersionRecord, error) {
	payload, digest, normalizedDefinition, err := encodeApprovedDefinitionForStore(definition)
	if err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}
	definition = normalizedDefinition
	if !validTaskStateReference(operationRef, 1024) {
		return ApprovedDefinitionVersionRecord{}, taskStateValidation(
			"approved definition approval reference is invalid")
	}

	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ApprovedDefinitionVersionRecord{}, taskStateDatabaseError(
			"begin approved definition transaction", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)

	head, err := lockTaskDefinitionScope(ctx, tx, definition.TenantID,
		definition.UserID, definition.TaskID)
	if err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}
	record, err := insertInitialApprovedDefinitionTx(
		ctx, tx, definition, payload, digest, operationRef, head)
	if err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ApprovedDefinitionVersionRecord{}, taskStateDatabaseError(
			"commit initial approved definition", err)
	}
	return record, nil
}

// insertInitialApprovedDefinitionTx is the shared exact version-1 append used
// by the explicit primitive above and the C2c one-way legacy baseline adapter.
// The caller must hold the schedule lock returned by lockTaskDefinitionScope
// and owns commit/rollback.
func insertInitialApprovedDefinitionTx(
	ctx context.Context,
	tx pgx.Tx,
	definition taskstate.ApprovedDefinitionV1,
	payload []byte,
	digest, operationRef string,
	head taskDefinitionHead,
) (ApprovedDefinitionVersionRecord, error) {
	if definition.ExecutionMode != types.ExecutionModeCompiled {
		return ApprovedDefinitionVersionRecord{}, taskStateValidation(
			"C2a only permits compiled approved definitions")
	}
	// Resolve the immutable approval identity before inspecting the movable
	// current head. A response-lost initialization may be retried after a later
	// authorized edit has already advanced the head; it must still return the
	// original version-1 result rather than manufacture a conflict or duplicate.
	existingByApproval, err := loadApprovedDefinitionByOperationRefTx(ctx, tx,
		definition.TenantID, definition.UserID, definition.TaskID, operationRef)
	if err == nil {
		if existingByApproval.Version != initialApprovedDefinitionVersion ||
			existingByApproval.Digest != digest ||
			!bytes.Equal(existingByApproval.Payload, payload) {
			return ApprovedDefinitionVersionRecord{}, taskStateConflict(
				"approved definition approval reference already has another result")
		}
		return existingByApproval, nil
	}
	if !errors.Is(err, types.ErrNotFound) {
		return ApprovedDefinitionVersionRecord{}, err
	}
	if head.Mode != types.ExecutionModeCompiled {
		return ApprovedDefinitionVersionRecord{}, taskStateValidation(
			"C2a only permits compiled approved definitions")
	}
	if head.Version != nil || head.Digest != nil {
		if head.Version == nil || head.Digest == nil ||
			*head.Version != initialApprovedDefinitionVersion {
			return ApprovedDefinitionVersionRecord{}, taskStateConflict(
				"approved definition head already exists")
		}
		existing, loadErr := loadApprovedDefinitionVersionTx(ctx, tx,
			definition.TenantID, definition.UserID, definition.TaskID,
			*head.Version)
		if loadErr != nil {
			return ApprovedDefinitionVersionRecord{}, loadErr
		}
		if existing.Digest != digest || existing.OperationRef != operationRef ||
			!bytes.Equal(existing.Payload, payload) {
			return ApprovedDefinitionVersionRecord{}, taskStateConflict(
				"approved definition initialization conflicts with the current head")
		}
		return existing, nil
	}
	if err := validateApprovedDefinitionProjectionTx(ctx, tx, definition, payload); err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}

	record := ApprovedDefinitionVersionRecord{
		Definition: definition, Version: initialApprovedDefinitionVersion,
		Digest: digest, Payload: bytes.Clone(payload), OperationRef: operationRef,
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO task_approved_definition_versions (
			tenant_id, user_id, task_id, version, schema_version,
			execution_mode, definition_digest, payload, operation_ref
		 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 RETURNING created_at`,
		definition.TenantID, definition.UserID, definition.TaskID,
		record.Version, definition.SchemaVersion, definition.ExecutionMode,
		digest, payload, operationRef,
	).Scan(&record.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return ApprovedDefinitionVersionRecord{}, taskStateConflict(
				"approved definition version or approval reference already exists")
		}
		return ApprovedDefinitionVersionRecord{}, taskStateDatabaseError(
			"insert initial approved definition", err)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE schedules
		    SET approved_definition_version = $4,
		        approved_definition_digest = $5,
		        execution_mode = $6
		  WHERE tenant_id = $1 AND user_id = $2 AND id = $3
		    AND approved_definition_version IS NULL
		    AND approved_definition_digest IS NULL
		    AND execution_mode = $6`,
		definition.TenantID, definition.UserID, definition.TaskID,
		record.Version, digest, definition.ExecutionMode)
	if err != nil {
		return ApprovedDefinitionVersionRecord{}, taskStateDatabaseError(
			"advance initial approved definition head", err)
	}
	if tag.RowsAffected() != 1 {
		return ApprovedDefinitionVersionRecord{}, taskStateConflict(
			"approved definition head changed concurrently")
	}
	return record, nil
}

// GetCurrentApprovedDefinition returns the exact immutable row referenced by
// the schedule head. Headless legacy schedules return NotFound; callers must
// not silently fall back to mutable projections after the C2c cutover.
func (s *Store) GetCurrentApprovedDefinition(
	ctx context.Context,
	tenantID, userID int64,
	taskID string,
) (ApprovedDefinitionVersionRecord, error) {
	if err := validateTaskStateScope(tenantID, userID, taskID); err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}
	return scanApprovedDefinitionVersion(s.pool.QueryRow(ctx,
		`SELECT d.version, d.schema_version, d.execution_mode,
		        d.definition_digest, d.payload, d.operation_ref, d.created_at
		   FROM schedules s
		   JOIN tenants t ON t.id = s.tenant_id
		   JOIN memberships m ON m.tenant_id = s.tenant_id AND m.user_id = s.user_id
		   JOIN task_approved_definition_versions d
		     ON d.tenant_id = s.tenant_id AND d.user_id = s.user_id AND d.task_id = s.id
		    AND d.version = s.approved_definition_version
		    AND d.definition_digest = s.approved_definition_digest
		    AND d.execution_mode = s.execution_mode
		  WHERE s.tenant_id = $1 AND s.user_id = $2 AND s.id = $3
		    AND t.status = 'active' AND t.deleted_at IS NULL`,
		tenantID, userID, taskID), tenantID, userID, taskID)
}

// GetApprovedDefinitionVersion reads one immutable historical version within
// an active Tenant/User/Task scope.
func (s *Store) GetApprovedDefinitionVersion(
	ctx context.Context,
	tenantID, userID int64,
	taskID string,
	version int64,
) (ApprovedDefinitionVersionRecord, error) {
	if err := validateTaskStateScope(tenantID, userID, taskID); err != nil {
		return ApprovedDefinitionVersionRecord{}, err
	}
	if version <= 0 {
		return ApprovedDefinitionVersionRecord{}, taskStateValidation(
			"approved definition version is invalid")
	}
	return scanApprovedDefinitionVersion(s.pool.QueryRow(ctx,
		`SELECT d.version, d.schema_version, d.execution_mode,
		        d.definition_digest, d.payload, d.operation_ref, d.created_at
		   FROM task_approved_definition_versions d
		   JOIN schedules s
		     ON s.tenant_id = d.tenant_id AND s.user_id = d.user_id AND s.id = d.task_id
		   JOIN tenants t ON t.id = s.tenant_id
		   JOIN memberships m ON m.tenant_id = s.tenant_id AND m.user_id = s.user_id
		  WHERE d.tenant_id = $1 AND d.user_id = $2 AND d.task_id = $3
		    AND d.version = $4 AND t.status = 'active' AND t.deleted_at IS NULL`,
		tenantID, userID, taskID, version), tenantID, userID, taskID)
}

// GetAdaptiveStateForDefinition returns found=false for the explicit version-0
// state (no persisted learning yet). The expected definition fence is checked
// while the schedule row is locked, so a caller can never read v1 adaptive
// state as if it belonged to a newly-authorized v2 definition. C2c must still
// build the final run snapshot in one database transaction rather than compose
// independent reads in memory.
func (s *Store) GetAdaptiveStateForDefinition(
	ctx context.Context,
	tenantID, userID int64,
	taskID string,
	basis ApprovedDefinitionFence,
) (AdaptiveStateRecord, bool, error) {
	if err := validateTaskStateScope(tenantID, userID, taskID); err != nil {
		return AdaptiveStateRecord{}, false, err
	}
	if basis.Version <= 0 || !validTaskStateDigest(basis.Digest) {
		return AdaptiveStateRecord{}, false, taskStateValidation(
			"adaptive approved-definition fence is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return AdaptiveStateRecord{}, false, taskStateDatabaseError(
			"begin adaptive state read transaction", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	head, err := lockTaskDefinitionScope(ctx, tx, tenantID, userID, taskID)
	if err != nil {
		return AdaptiveStateRecord{}, false, err
	}
	if head.Version == nil || head.Digest == nil || *head.Version != basis.Version ||
		!constantTimeTaskStateDigestEqual(*head.Digest, basis.Digest) {
		return AdaptiveStateRecord{}, false, taskStateConflict(
			"approved definition changed before adaptive state was read")
	}
	record, found, err := loadAdaptiveStateTx(ctx, tx, tenantID, userID, taskID, false)
	if err != nil {
		return AdaptiveStateRecord{}, false, err
	}
	if found && (record.BasisDefinitionVersion != basis.Version ||
		!constantTimeTaskStateDigestEqual(record.BasisDefinitionDigest, basis.Digest)) {
		return AdaptiveStateRecord{}, false, taskStateConflict(
			"adaptive state belongs to another approved definition")
	}
	if err := tx.Commit(ctx); err != nil {
		return AdaptiveStateRecord{}, false, taskStateDatabaseError(
			"commit adaptive state read", err)
	}
	return record, found, nil
}

// CompareAndSwapAdaptiveState advances structurally bounded state exactly once.
// An absent row is version 0; a successful first write creates version 1.
// Repeating the same expected version and bytes after a lost response is
// idempotent. This primitive cannot prove that query text remains within the
// user's intent; C3 must add that evidence/rules gate before any critic or
// runtime writer is connected. The C2a AST guard therefore keeps it dark.
func (s *Store) CompareAndSwapAdaptiveState(
	ctx context.Context,
	expectedVersion int64,
	basis ApprovedDefinitionFence,
	state taskstate.AdaptiveStateV1,
	lastKnownGoodDefinitionVersion *int64,
) (AdaptiveStateRecord, error) {
	if expectedVersion < 0 {
		return AdaptiveStateRecord{}, taskStateValidation(
			"adaptive expected version is invalid")
	}
	if basis.Version <= 0 || !validTaskStateDigest(basis.Digest) {
		return AdaptiveStateRecord{}, taskStateValidation(
			"adaptive approved-definition fence is invalid")
	}
	payload, err := taskstate.EncodeAdaptiveStateV1(state)
	if err != nil {
		return AdaptiveStateRecord{}, taskStateValidation("adaptive state is invalid")
	}
	normalizedState, err := taskstate.DecodeAdaptiveStateV1(payload)
	if err != nil {
		return AdaptiveStateRecord{}, taskStateValidation("adaptive state is invalid")
	}
	state = normalizedState
	digest := digestTaskStatePayload(payload)
	if lastKnownGoodDefinitionVersion != nil && *lastKnownGoodDefinitionVersion <= 0 {
		return AdaptiveStateRecord{}, taskStateValidation(
			"last-known-good definition version is invalid")
	}

	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return AdaptiveStateRecord{}, taskStateDatabaseError(
			"begin adaptive state transaction", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	head, err := lockTaskDefinitionScope(ctx, tx, state.TenantID, state.UserID, state.TaskID)
	if err != nil {
		return AdaptiveStateRecord{}, err
	}
	if head.Mode != types.ExecutionModeCompiled || head.Version == nil || head.Digest == nil {
		return AdaptiveStateRecord{}, taskStateConflict(
			"adaptive state requires a compiled approved definition head")
	}
	if *head.Version != basis.Version ||
		!constantTimeTaskStateDigestEqual(*head.Digest, basis.Digest) {
		return AdaptiveStateRecord{}, taskStateConflict(
			"approved definition changed after adaptive work started")
	}
	approved, err := loadApprovedDefinitionVersionTx(ctx, tx,
		state.TenantID, state.UserID, state.TaskID, basis.Version)
	if err != nil {
		return AdaptiveStateRecord{}, err
	}
	if !constantTimeTaskStateDigestEqual(approved.Digest, basis.Digest) ||
		approved.Definition.ExecutionMode != head.Mode {
		return AdaptiveStateRecord{}, taskStateIntegrity()
	}
	if err := validateAdaptiveStateAgainstApproved(state, approved.Definition); err != nil {
		return AdaptiveStateRecord{}, err
	}
	if lastKnownGoodDefinitionVersion != nil {
		// C2a has no fixed-code proof that an older version is the same intent
		// or an equivalent canonical-domain recovery. Until C3 introduces that
		// evidence, only the exact approved basis may be retained as LKG.
		if *lastKnownGoodDefinitionVersion != basis.Version {
			return AdaptiveStateRecord{}, taskStateConflict(
				"last-known-good definition is outside the current approved basis")
		}
	}

	current, found, err := loadAdaptiveStateTx(ctx, tx,
		state.TenantID, state.UserID, state.TaskID, true)
	if err != nil {
		return AdaptiveStateRecord{}, err
	}
	if found {
		if current.Version == expectedVersion+1 && current.Digest == digest &&
			bytes.Equal(current.Payload, payload) &&
			current.BasisDefinitionVersion == basis.Version &&
			constantTimeTaskStateDigestEqual(current.BasisDefinitionDigest, basis.Digest) &&
			equalOptionalVersion(current.LastKnownGoodDefinitionVersion,
				lastKnownGoodDefinitionVersion) {
			if err := tx.Commit(ctx); err != nil {
				return AdaptiveStateRecord{}, taskStateDatabaseError(
					"commit adaptive state replay", err)
			}
			return current, nil
		}
		if current.Version != expectedVersion {
			return AdaptiveStateRecord{}, taskStateConflict(
				"adaptive state version changed concurrently")
		}
		if !monotonicRunStats(current.State.RunStats, state.RunStats) {
			return AdaptiveStateRecord{}, taskStateValidation(
				"adaptive run counters cannot decrease")
		}
	} else if expectedVersion != 0 {
		return AdaptiveStateRecord{}, taskStateConflict(
			"adaptive state version does not exist")
	}

	next := AdaptiveStateRecord{
		State: state, Version: expectedVersion + 1, Digest: digest,
		Payload:                        bytes.Clone(payload),
		BasisDefinitionVersion:         basis.Version,
		BasisDefinitionDigest:          basis.Digest,
		LastKnownGoodDefinitionVersion: cloneOptionalVersion(lastKnownGoodDefinitionVersion),
	}
	if !found {
		err = tx.QueryRow(ctx,
			`INSERT INTO task_adaptive_states (
				tenant_id, user_id, task_id, version, schema_version,
				payload_digest, payload, basis_definition_version,
				basis_definition_digest, last_known_good_definition_version
			 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			 RETURNING created_at, updated_at`,
			state.TenantID, state.UserID, state.TaskID, next.Version,
			state.SchemaVersion, digest, payload, basis.Version, basis.Digest,
			lastKnownGoodDefinitionVersion,
		).Scan(&next.CreatedAt, &next.UpdatedAt)
	} else {
		err = tx.QueryRow(ctx,
			`UPDATE task_adaptive_states
			    SET version=$5, schema_version=$6, payload_digest=$7, payload=$8,
			        basis_definition_version=$9, basis_definition_digest=$10,
			        last_known_good_definition_version=$11, updated_at=clock_timestamp()
			  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND version=$4
			 RETURNING created_at, updated_at`,
			state.TenantID, state.UserID, state.TaskID, expectedVersion,
			next.Version, state.SchemaVersion, digest, payload, basis.Version, basis.Digest,
			lastKnownGoodDefinitionVersion,
		).Scan(&next.CreatedAt, &next.UpdatedAt)
	}
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || isUniqueViolation(err) {
			return AdaptiveStateRecord{}, taskStateConflict(
				"adaptive state version changed concurrently")
		}
		return AdaptiveStateRecord{}, taskStateDatabaseError(
			"persist adaptive state", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return AdaptiveStateRecord{}, taskStateDatabaseError(
			"commit adaptive state", err)
	}
	return next, nil
}

type taskDefinitionHead struct {
	Mode    types.ExecutionMode
	Version *int64
	Digest  *string
}

func lockTaskDefinitionScope(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID int64,
	taskID string,
) (taskDefinitionHead, error) {
	if tenantID <= 0 || userID <= 0 || !validTaskStateReference(taskID, 255) {
		return taskDefinitionHead{}, taskStateValidation("task definition scope is invalid")
	}
	var head taskDefinitionHead
	var rawMode string
	err := tx.QueryRow(ctx,
		`SELECT s.execution_mode, s.approved_definition_version,
		        s.approved_definition_digest
		   FROM schedules s
		   JOIN tenants t ON t.id = s.tenant_id
		   JOIN memberships m ON m.tenant_id = s.tenant_id AND m.user_id = s.user_id
		  WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3
		    AND t.status='active' AND t.deleted_at IS NULL
		  FOR UPDATE OF s FOR SHARE OF t, m`, tenantID, userID, taskID,
	).Scan(&rawMode, &head.Version, &head.Digest)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return taskDefinitionHead{}, taskStateNotFound()
		}
		return taskDefinitionHead{}, taskStateDatabaseError(
			"lock task definition scope", err)
	}
	mode, err := types.ParseExecutionMode(rawMode)
	if err != nil || (head.Version == nil) != (head.Digest == nil) {
		return taskDefinitionHead{}, taskStateIntegrity()
	}
	head.Mode = mode
	return head, nil
}

func loadApprovedDefinitionVersionTx(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID int64,
	taskID string,
	version int64,
) (ApprovedDefinitionVersionRecord, error) {
	return scanApprovedDefinitionVersion(tx.QueryRow(ctx,
		`SELECT version, schema_version, execution_mode, definition_digest,
		        payload, operation_ref, created_at
		   FROM task_approved_definition_versions
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND version=$4`,
		tenantID, userID, taskID, version), tenantID, userID, taskID)
}

func loadApprovedDefinitionByOperationRefTx(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID int64,
	taskID, operationRef string,
) (ApprovedDefinitionVersionRecord, error) {
	return scanApprovedDefinitionVersion(tx.QueryRow(ctx,
		`SELECT version, schema_version, execution_mode, definition_digest,
		        payload, operation_ref, created_at
		   FROM task_approved_definition_versions
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND operation_ref=$4`,
		tenantID, userID, taskID, operationRef), tenantID, userID, taskID)
}

func validateApprovedDefinitionProjectionTx(
	ctx context.Context,
	tx pgx.Tx,
	definition taskstate.ApprovedDefinitionV1,
	wantPayload []byte,
) error {
	var nlDescription string
	var specJSON, scopeJSON []byte
	var rawStrictness *string
	if err := tx.QueryRow(ctx,
		`SELECT s.nl_description, s.spec_json, s.scope_json, s.push_strictness
		   FROM schedules s
		  WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3
		    AND `+matureSchedulePredicate,
		definition.TenantID, definition.UserID, definition.TaskID,
	).Scan(&nlDescription, &specJSON, &scopeJSON, &rawStrictness); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return taskStateConflict("task projection is not mature")
		}
		return taskStateDatabaseError("load task definition projection", err)
	}

	playbookContent := ""
	fetchPlan := json.RawMessage("{}")
	err := tx.QueryRow(ctx,
		`SELECT content, fetch_plan
		   FROM schedule_playbooks
		  WHERE schedule_id=$1
		  FOR SHARE`, definition.TaskID,
	).Scan(&playbookContent, &fetchPlan)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return taskStateDatabaseError("load task definition playbook", err)
	}

	linkedIDs := make(map[string]int64)
	rows, err := tx.Query(ctx,
		`SELECT src.url, src.id
		   FROM task_fetch_targets ss
		   JOIN fetch_targets src ON src.id = ss.fetch_target_id
		  WHERE ss.schedule_id=$1
		  ORDER BY src.url, src.id
		  FOR SHARE OF ss, src`, definition.TaskID)
	if err != nil {
		return taskStateDatabaseError("load task definition source links", err)
	}
	for rows.Next() {
		var sourceURL string
		var sourceID int64
		if err := rows.Scan(&sourceURL, &sourceID); err != nil {
			rows.Close()
			return taskStateDatabaseError("scan task definition source link", err)
		}
		if sourceID <= 0 || !validTaskStateReference(sourceURL, 4096) {
			rows.Close()
			return taskStateIntegrity()
		}
		if _, duplicate := linkedIDs[sourceURL]; duplicate {
			rows.Close()
			return taskStateIntegrity()
		}
		linkedIDs[sourceURL] = sourceID
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return taskStateDatabaseError("iterate task definition source links", err)
	}
	rows.Close()

	var planObject map[string]json.RawMessage
	if err := strictjson.Decode(fetchPlan, &planObject); err != nil || planObject == nil {
		return taskStateIntegrity()
	}
	sourceScope := taskstate.SourceScopeApprovedPlan
	approvedSources := make([]taskstate.ApprovedSourceV1, 0, len(linkedIDs))
	if len(planObject) == 0 {
		return taskStateConflict("task definition requires an approved fetch plan")
	} else {
		// Existing compiled plans predate the V1 immutable wire and allowed
		// title/config to be omitted. Decode that compatibility shape, fill its
		// deterministic defaults, then build a new exact V1 payload. Never loosen
		// the retained V1 reader to make an old projection fit.
		var currentPlan currentFetchPlanProjection
		if err := strictjson.Decode(fetchPlan, &currentPlan); err != nil ||
			currentPlan.Targets == nil || len(currentPlan.Targets) != len(linkedIDs) {
			return taskStateConflict("fetch plan and task source links differ")
		}
		plan := taskstate.FetchPlanV1{
			Sources: make([]taskstate.PlanSourceV1, 0, len(currentPlan.Targets)),
		}
		for _, source := range currentPlan.Targets {
			config := source.Config
			if len(config) == 0 {
				config = json.RawMessage("{}")
			}
			planned := taskstate.PlanSourceV1{
				Platform:   types.Platform(source.Platform),
				Capability: types.Capability(source.Capability),
				Title:      source.Title, URL: source.URL, Config: config,
			}
			sourceID, ok := linkedIDs[source.URL]
			if !ok {
				return taskStateConflict("fetch plan and task source links differ")
			}
			plan.Sources = append(plan.Sources, planned)
			approvedSources = append(approvedSources, taskstate.ApprovedSourceV1{
				SourceID: sourceID, Platform: types.Platform(source.Platform),
				Capability: types.Capability(source.Capability),
				Title:      source.Title, URL: source.URL, Config: config,
			})
		}
		fetchPlan, err = json.Marshal(plan)
		if err != nil {
			return taskStateIntegrity()
		}
	}

	// NULL is historical data, so freeze the value that NULL meant when the
	// projection was written. A future product default must not reinterpret it.
	strictness := legacyTaskProjectionDefaultStrictnessV1
	if rawStrictness != nil {
		strictness = types.PushStrictness(*rawStrictness)
	}
	// The current aggregate has no separate intent column. Since A5 writes the
	// authorized intent into playbook.content and every later manual edit freezes
	// that whole content, it is the only lossless migration surrogate. Missing or
	// empty playbooks remain headless and are reported by C2c instead of inventing
	// an intent from a display label.
	// Projection verification is a retained-reader path. Construct the frozen V1
	// value directly instead of calling BuildApprovedDefinitionV1, whose current
	// writer registry may intentionally reject a capability/config which was
	// valid when an already-sealed operation was created.
	candidate := taskstate.ApprovedDefinitionV1{
		SchemaVersion: taskstate.ApprovedDefinitionSchemaVersionV1,
		TenantID:      definition.TenantID, UserID: definition.UserID,
		TaskID: definition.TaskID, Intent: playbookContent,
		NLDescription: nlDescription, SpecJSON: specJSON, ScopeJSON: scopeJSON,
		PlaybookContent: playbookContent, SourceScope: sourceScope,
		FetchPlan: fetchPlan, Strictness: strictness, Sources: approvedSources,
		ExecutionMode:  definition.ExecutionMode,
		DeliveryPolicy: taskstate.DeliveryPolicyOwnerFeishu,
		BudgetPolicy:   taskstate.BudgetPolicyInheritTenantQuota,
	}
	candidatePayload, err := taskstate.EncodeApprovedDefinitionV1(candidate)
	if err != nil || !bytes.Equal(candidatePayload, wantPayload) {
		return taskStateConflict("approved definition differs from the task projection")
	}
	return nil
}

func scanApprovedDefinitionVersion(
	row pgx.Row,
	tenantID, userID int64,
	taskID string,
) (ApprovedDefinitionVersionRecord, error) {
	var record ApprovedDefinitionVersionRecord
	var schemaVersion, rawMode string
	if err := row.Scan(&record.Version, &schemaVersion, &rawMode, &record.Digest,
		&record.Payload, &record.OperationRef, &record.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ApprovedDefinitionVersionRecord{}, taskStateNotFound()
		}
		return ApprovedDefinitionVersionRecord{}, taskStateDatabaseError(
			"load approved definition", err)
	}
	definition, err := taskstate.DecodeApprovedDefinitionV1(record.Payload)
	if err != nil {
		return ApprovedDefinitionVersionRecord{}, taskStateIntegrity()
	}
	canonical, err := taskstate.EncodeApprovedDefinitionV1(definition)
	if err != nil || !bytes.Equal(canonical, record.Payload) ||
		schemaVersion != taskstate.ApprovedDefinitionSchemaVersionV1 ||
		definition.SchemaVersion != schemaVersion || definition.TenantID != tenantID ||
		definition.UserID != userID || definition.TaskID != taskID ||
		record.Version <= 0 || !validTaskStateReference(record.OperationRef, 1024) ||
		!constantTimeDigestMatches(record.Digest, record.Payload) {
		return ApprovedDefinitionVersionRecord{}, taskStateIntegrity()
	}
	mode, err := types.ParseExecutionMode(rawMode)
	if err != nil || definition.ExecutionMode != mode {
		return ApprovedDefinitionVersionRecord{}, taskStateIntegrity()
	}
	record.Definition = definition
	record.Payload = bytes.Clone(record.Payload)
	return record, nil
}

func loadAdaptiveStateTx(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID int64,
	taskID string,
	lock bool,
) (AdaptiveStateRecord, bool, error) {
	query := `SELECT version, schema_version, payload_digest, payload,
	                 basis_definition_version, basis_definition_digest,
	                 last_known_good_definition_version, created_at, updated_at
	            FROM task_adaptive_states
	           WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`
	if lock {
		query += ` FOR UPDATE`
	}
	record, err := scanAdaptiveState(tx.QueryRow(ctx, query, tenantID, userID, taskID),
		tenantID, userID, taskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return AdaptiveStateRecord{}, false, nil
	}
	if err != nil {
		return AdaptiveStateRecord{}, false, err
	}
	return record, true, nil
}

func scanAdaptiveState(
	row pgx.Row,
	tenantID, userID int64,
	taskID string,
) (AdaptiveStateRecord, error) {
	var record AdaptiveStateRecord
	var schemaVersion string
	if err := row.Scan(&record.Version, &schemaVersion, &record.Digest,
		&record.Payload, &record.BasisDefinitionVersion, &record.BasisDefinitionDigest,
		&record.LastKnownGoodDefinitionVersion,
		&record.CreatedAt, &record.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return AdaptiveStateRecord{}, err
		}
		return AdaptiveStateRecord{}, taskStateDatabaseError("load adaptive state", err)
	}
	state, err := taskstate.DecodeAdaptiveStateV1(record.Payload)
	if err != nil {
		return AdaptiveStateRecord{}, taskStateIntegrity()
	}
	canonical, err := taskstate.EncodeAdaptiveStateV1(state)
	if err != nil || !bytes.Equal(canonical, record.Payload) ||
		schemaVersion != taskstate.AdaptiveStateSchemaVersionV1 ||
		state.SchemaVersion != schemaVersion || state.TenantID != tenantID ||
		state.UserID != userID || state.TaskID != taskID || record.Version <= 0 ||
		record.BasisDefinitionVersion <= 0 ||
		!validTaskStateDigest(record.BasisDefinitionDigest) ||
		(record.LastKnownGoodDefinitionVersion != nil &&
			*record.LastKnownGoodDefinitionVersion <= 0) ||
		!constantTimeDigestMatches(record.Digest, record.Payload) {
		return AdaptiveStateRecord{}, taskStateIntegrity()
	}
	record.State = state
	record.Payload = bytes.Clone(record.Payload)
	record.LastKnownGoodDefinitionVersion = cloneOptionalVersion(
		record.LastKnownGoodDefinitionVersion)
	return record, nil
}

func encodeApprovedDefinitionForStore(
	definition taskstate.ApprovedDefinitionV1,
) ([]byte, string, taskstate.ApprovedDefinitionV1, error) {
	if err := taskstate.ValidateApprovedDefinitionV1ForWrite(definition); err != nil {
		return nil, "", taskstate.ApprovedDefinitionV1{},
			taskStateValidation("approved definition is invalid")
	}
	payload, err := taskstate.EncodeApprovedDefinitionV1(definition)
	if err != nil {
		return nil, "", taskstate.ApprovedDefinitionV1{},
			taskStateValidation("approved definition is invalid")
	}
	normalized, err := taskstate.DecodeApprovedDefinitionV1(payload)
	if err != nil {
		return nil, "", taskstate.ApprovedDefinitionV1{},
			taskStateValidation("approved definition is invalid")
	}
	return payload, digestTaskStatePayload(payload), normalized, nil
}

func validateAdaptiveStateAgainstApproved(
	state taskstate.AdaptiveStateV1,
	definition taskstate.ApprovedDefinitionV1,
) error {
	if state.TenantID != definition.TenantID || state.UserID != definition.UserID ||
		state.TaskID != definition.TaskID {
		return taskStateValidation("adaptive state scope differs from approved definition")
	}
	// Historical subscription snapshots are reader-only and cannot seed
	// automatic learning or LKG.
	if definition.SourceScope != taskstate.SourceScopeApprovedPlan ||
		len(definition.Sources) == 0 {
		return taskStateValidation("adaptive state requires an exact approved source plan")
	}
	type capabilityKey struct {
		platform   types.Platform
		capability types.Capability
	}
	allowedSources := make(map[int64]taskstate.ApprovedSourceV1, len(definition.Sources))
	allowedCapabilities := make(map[capabilityKey]struct{}, len(definition.Sources))
	for _, source := range definition.Sources {
		allowedSources[source.SourceID] = source
		allowedCapabilities[capabilityKey{source.Platform, source.Capability}] = struct{}{}
	}
	for _, variant := range state.QueryVariants {
		source, ok := allowedSources[variant.SourceID]
		if !ok || source.Capability != types.CapSearch {
			return taskStateValidation(
				"adaptive query variant is outside approved searchable sources")
		}
	}
	if len(state.SourceOrder) != len(allowedSources) {
		return taskStateValidation("adaptive source order must cover approved sources exactly")
	}
	seenSources := make(map[int64]struct{}, len(state.SourceOrder))
	for _, sourceID := range state.SourceOrder {
		if _, ok := allowedSources[sourceID]; !ok {
			return taskStateValidation("adaptive source order contains an unapproved source")
		}
		seenSources[sourceID] = struct{}{}
	}
	if len(seenSources) != len(allowedSources) {
		return taskStateValidation("adaptive source order is not a permutation")
	}
	if len(state.CapabilityOrder) != len(allowedCapabilities) {
		return taskStateValidation(
			"adaptive capability order must cover approved capabilities exactly")
	}
	seenCapabilities := make(map[capabilityKey]struct{}, len(state.CapabilityOrder))
	for _, capability := range state.CapabilityOrder {
		key := capabilityKey{
			capability.Platform, capability.Capability,
		}
		if _, ok := allowedCapabilities[key]; !ok {
			return taskStateValidation(
				"adaptive capability order contains an unapproved capability")
		}
		seenCapabilities[key] = struct{}{}
	}
	if len(seenCapabilities) != len(allowedCapabilities) {
		return taskStateValidation("adaptive capability order is not a permutation")
	}
	return nil
}

func monotonicRunStats(old, next taskstate.RunStatsV1) bool {
	return next.AttemptedRuns >= old.AttemptedRuns &&
		next.SuccessfulRuns >= old.SuccessfulRuns &&
		next.EmptyRuns >= old.EmptyRuns && next.FailedRuns >= old.FailedRuns
}

func constantTimeDigestMatches(digest string, payload []byte) bool {
	if len(digest) != sha256.Size*2 {
		return false
	}
	expected := digestTaskStatePayload(payload)
	return subtle.ConstantTimeCompare([]byte(digest), []byte(expected)) == 1
}

func constantTimeTaskStateDigestEqual(left, right string) bool {
	if !validTaskStateDigest(left) || !validTaskStateDigest(right) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func validTaskStateDigest(digest string) bool {
	if len(digest) != sha256.Size*2 || strings.ToLower(digest) != digest {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil
}

func digestTaskStatePayload(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func validTaskStateReference(value string, maxBytes int) bool {
	if value == "" || strings.TrimSpace(value) != value || len(value) > maxBytes ||
		!utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf) {
			return false
		}
	}
	return true
}

func validateTaskStateScope(tenantID, userID int64, taskID string) error {
	if tenantID <= 0 || userID <= 0 || !validTaskStateReference(taskID, 255) {
		return taskStateValidation("task definition scope is invalid")
	}
	return nil
}

func equalOptionalVersion(a, b *int64) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}

func cloneOptionalVersion(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func taskStateValidation(message string) error {
	return types.NewAppError(types.CodeValidation, message, nil)
}

func taskStateNotFound() error {
	return types.NewAppError(types.CodeNotFound,
		"task definition state is unavailable", nil)
}

func taskStateConflict(message string) error {
	return types.NewAppError(types.CodeConflict, message, nil)
}

func taskStateIntegrity() error {
	return types.NewAppError(types.CodeInternal,
		"task definition state integrity check failed", nil)
}

// PostgreSQL constraint errors may include task payload bytes in Detail. Keep
// the cause useful for retry classification without leaking those bytes.
func taskStateDatabaseError(action string, cause error) error {
	var safeCause error
	switch {
	case cause == nil:
		safeCause = errors.New("database operation did not converge")
	case errors.Is(cause, context.Canceled), errors.Is(cause, context.DeadlineExceeded):
		safeCause = cause
	default:
		var pgErr *pgconn.PgError
		if errors.As(cause, &pgErr) {
			safeCause = fmt.Errorf("postgres sqlstate %s", pgErr.Code)
		} else {
			safeCause = errors.New("database operation failed")
		}
	}
	return types.NewAppError(types.CodeDatabase, action, safeCause)
}
