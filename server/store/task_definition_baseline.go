package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/internal/strictjson"
	"github.com/YouToco/vane/server/taskstate"
	"github.com/YouToco/vane/server/types"
)

const (
	maxTaskDefinitionBaselinePageSize = 1000
	taskDefinitionBaselineApprovalV1  = "c2c-baseline/v1"
)

// TaskDefinitionBaselineMode controls one page of the C2c legacy adapter.
// Dry-run and verify are read-only. Apply is the only mode which may install
// an exact immutable version-1 head.
type TaskDefinitionBaselineMode string

const (
	TaskDefinitionBaselineDryRun TaskDefinitionBaselineMode = "dry_run"
	TaskDefinitionBaselineApply  TaskDefinitionBaselineMode = "apply"
	TaskDefinitionBaselineVerify TaskDefinitionBaselineMode = "verify"
)

// TaskDefinitionBaselineStatus is an operator-facing, bounded result. It
// intentionally carries no database error text or legacy projection bytes.
type TaskDefinitionBaselineStatus string

const (
	TaskDefinitionBaselineWouldApply  TaskDefinitionBaselineStatus = "would_apply"
	TaskDefinitionBaselineApplied     TaskDefinitionBaselineStatus = "applied"
	TaskDefinitionBaselineVerified    TaskDefinitionBaselineStatus = "verified"
	TaskDefinitionBaselineNeedsApply  TaskDefinitionBaselineStatus = "needs_apply"
	TaskDefinitionBaselineUnsupported TaskDefinitionBaselineStatus = "unsupported"
	TaskDefinitionBaselineDeleted     TaskDefinitionBaselineStatus = "deleted"
)

const (
	TaskDefinitionBaselineReasonMissingPlaybook = "missing_playbook"
	TaskDefinitionBaselineReasonEmptyPlaybook   = "empty_playbook"
	TaskDefinitionBaselineReasonDescription     = "invalid_description"
	TaskDefinitionBaselineReasonProjection      = "projection_not_representable"
	TaskDefinitionBaselineReasonNotMature       = "task_not_mature"
	TaskDefinitionBaselineReasonDeleted         = "task_deleted"
)

// TaskDefinitionBaselineCursor is an exclusive stable keyset cursor.
type TaskDefinitionBaselineCursor struct {
	TenantID int64  `json:"tenant_id"`
	UserID   int64  `json:"user_id"`
	TaskID   string `json:"task_id"`
}

// TaskDefinitionBaselineResult reports the exact task inspected by one page.
// Version and Digest are populated only when an immutable head was either
// audited or deterministically constructed.
type TaskDefinitionBaselineResult struct {
	TenantID int64                        `json:"tenant_id"`
	UserID   int64                        `json:"user_id"`
	TaskID   string                       `json:"task_id"`
	Status   TaskDefinitionBaselineStatus `json:"status"`
	Reason   string                       `json:"reason,omitempty"`
	Version  int64                        `json:"version,omitempty"`
	Digest   string                       `json:"digest,omitempty"`
}

// TaskDefinitionBaselinePage is one bounded keyset page. Next is nil when the
// scan is exhausted.
type TaskDefinitionBaselinePage struct {
	Items []TaskDefinitionBaselineResult `json:"items"`
	Next  *TaskDefinitionBaselineCursor  `json:"next,omitempty"`
}

// ReconcileTaskDefinitionBaselines inspects one stable keyset page of mature
// tasks. Each item is processed in its own transaction, so one blocked legacy
// projection cannot make already-applied tasks roll back as a batch.
//
// The only write path is:
//
//	mature + headless + compiled + non-empty playbook -> exact retained V1.
//
// Existing heads are audit-only in every mode. This method does not change
// runtime reads, PrepareRun, or Temporal state.
func (s *Store) ReconcileTaskDefinitionBaselines(
	ctx context.Context,
	mode TaskDefinitionBaselineMode,
	after TaskDefinitionBaselineCursor,
	limit int,
) (TaskDefinitionBaselinePage, error) {
	if !validTaskDefinitionBaselineMode(mode) || limit <= 0 ||
		limit > maxTaskDefinitionBaselinePageSize ||
		!validTaskDefinitionBaselineCursor(after) {
		return TaskDefinitionBaselinePage{}, taskStateValidation(
			"task definition baseline page request is invalid")
	}

	scopes, hasMore, err := s.listTaskDefinitionBaselineScopes(
		ctx, after, limit)
	if err != nil {
		return TaskDefinitionBaselinePage{}, err
	}
	page := TaskDefinitionBaselinePage{
		Items: make([]TaskDefinitionBaselineResult, 0, len(scopes)),
	}
	for _, scope := range scopes {
		item, err := s.reconcileTaskDefinitionBaseline(ctx, mode, scope)
		if err != nil {
			return TaskDefinitionBaselinePage{}, err
		}
		page.Items = append(page.Items, item)
	}
	if hasMore {
		last := scopes[len(scopes)-1]
		page.Next = &TaskDefinitionBaselineCursor{
			TenantID: last.TenantID,
			UserID:   last.UserID,
			TaskID:   last.TaskID,
		}
	}
	return page, nil
}

func validTaskDefinitionBaselineMode(mode TaskDefinitionBaselineMode) bool {
	switch mode {
	case TaskDefinitionBaselineDryRun,
		TaskDefinitionBaselineApply,
		TaskDefinitionBaselineVerify:
		return true
	default:
		return false
	}
}

func validTaskDefinitionBaselineCursor(cursor TaskDefinitionBaselineCursor) bool {
	if cursor == (TaskDefinitionBaselineCursor{}) {
		return true
	}
	return cursor.TenantID > 0 && cursor.UserID > 0 &&
		validTaskStateReference(cursor.TaskID, 255)
}

func (s *Store) listTaskDefinitionBaselineScopes(
	ctx context.Context,
	after TaskDefinitionBaselineCursor,
	limit int,
) ([]TaskDefinitionBaselineCursor, bool, error) {
	// Deliberately include suspended/deleting tenants. A soft-deleted task must
	// not become an untracked headless task if the tenant is later restored;
	// concurrent hard deletion still wins at the per-task schedule lock.
	rows, err := s.pool.Query(ctx,
		`SELECT s.tenant_id, s.user_id, s.id
		   FROM schedules s
		  WHERE (s.tenant_id, s.user_id, s.id) > ($1, $2, $3)
		    AND `+matureSchedulePredicate+`
		    AND NOT EXISTS (
		        SELECT 1
		          FROM task_approved_definition_versions d
		         WHERE d.tenant_id = s.tenant_id
		           AND d.user_id = s.user_id
		           AND d.task_id = s.id
		           AND d.version = s.approved_definition_version
		           AND d.definition_digest = s.approved_definition_digest
		           AND d.execution_mode = s.execution_mode
		           AND d.schema_version = $4
		    )
		  ORDER BY s.tenant_id, s.user_id, s.id
		  LIMIT $5`,
		after.TenantID, after.UserID, after.TaskID,
		taskstate.ApprovedDefinitionSchemaVersionV2, limit+1)
	if err != nil {
		return nil, false, taskStateDatabaseError(
			"list task definition baseline page", err)
	}
	defer rows.Close()

	scopes := make([]TaskDefinitionBaselineCursor, 0, limit+1)
	for rows.Next() {
		var scope TaskDefinitionBaselineCursor
		if err := rows.Scan(&scope.TenantID, &scope.UserID, &scope.TaskID); err != nil {
			return nil, false, taskStateDatabaseError(
				"scan task definition baseline scope", err)
		}
		if !validTaskDefinitionBaselineCursor(scope) {
			return nil, false, taskStateIntegrity()
		}
		scopes = append(scopes, scope)
	}
	if err := rows.Err(); err != nil {
		return nil, false, taskStateDatabaseError(
			"iterate task definition baseline page", err)
	}
	hasMore := len(scopes) > limit
	if hasMore {
		scopes = scopes[:limit]
	}
	return scopes, hasMore, nil
}

func (s *Store) reconcileTaskDefinitionBaseline(
	ctx context.Context,
	mode TaskDefinitionBaselineMode,
	scope TaskDefinitionBaselineCursor,
) (TaskDefinitionBaselineResult, error) {
	result := TaskDefinitionBaselineResult{
		TenantID: scope.TenantID,
		UserID:   scope.UserID,
		TaskID:   scope.TaskID,
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return TaskDefinitionBaselineResult{}, taskStateDatabaseError(
			"begin task definition baseline transaction", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)

	head, err := lockTaskDefinitionBaselineScope(
		ctx, tx, scope.TenantID, scope.UserID, scope.TaskID)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			result.Status = TaskDefinitionBaselineDeleted
			result.Reason = TaskDefinitionBaselineReasonDeleted
			return result, nil
		}
		return TaskDefinitionBaselineResult{}, err
	}
	mature, err := taskDefinitionBaselineMatureTx(ctx, tx, scope)
	if err != nil {
		return TaskDefinitionBaselineResult{}, err
	}
	if !mature {
		result.Status = TaskDefinitionBaselineUnsupported
		result.Reason = TaskDefinitionBaselineReasonNotMature
		if err := tx.Commit(ctx); err != nil {
			return TaskDefinitionBaselineResult{}, taskStateDatabaseError(
				"commit task definition baseline maturity audit", err)
		}
		return result, nil
	}

	if head.Version != nil {
		record, err := loadApprovedDefinitionVersionTx(
			ctx, tx, scope.TenantID, scope.UserID, scope.TaskID, *head.Version)
		if err != nil {
			return TaskDefinitionBaselineResult{}, err
		}
		if head.Digest == nil ||
			!constantTimeTaskStateDigestEqual(*head.Digest, record.Digest) ||
			head.Mode != record.Definition.ExecutionMode {
			return TaskDefinitionBaselineResult{}, taskStateIntegrity()
		}
		if err := validateApprovedDefinitionProjectionTx(
			ctx, tx, record.Definition, record.Payload); err != nil {
			return TaskDefinitionBaselineResult{}, err
		}
		result.Status = TaskDefinitionBaselineVerified
		result.Version = record.Version
		result.Digest = record.Digest
		if err := tx.Commit(ctx); err != nil {
			return TaskDefinitionBaselineResult{}, taskStateDatabaseError(
				"commit task definition baseline head audit", err)
		}
		return result, nil
	}
	if head.Mode != types.ExecutionModeCompiled {
		return TaskDefinitionBaselineResult{}, taskStateIntegrity()
	}

	definition, unsupported, err := buildTaskDefinitionBaselineV1Tx(
		ctx, tx, scope)
	if err != nil {
		return TaskDefinitionBaselineResult{}, err
	}
	if unsupported != "" {
		result.Status = TaskDefinitionBaselineUnsupported
		result.Reason = unsupported
		if err := tx.Commit(ctx); err != nil {
			return TaskDefinitionBaselineResult{}, taskStateDatabaseError(
				"commit unsupported task definition baseline audit", err)
		}
		return result, nil
	}
	payload, err := taskstate.EncodeApprovedDefinitionV1(definition)
	if err != nil {
		result.Status = TaskDefinitionBaselineUnsupported
		result.Reason = taskDefinitionBaselineEncodeReason(definition)
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return TaskDefinitionBaselineResult{}, taskStateDatabaseError(
				"commit unrepresentable task definition baseline audit", commitErr)
		}
		return result, nil
	}
	normalized, err := taskstate.DecodeApprovedDefinitionV1(payload)
	if err != nil {
		return TaskDefinitionBaselineResult{}, taskStateIntegrity()
	}
	definition = normalized
	digest := digestTaskStatePayload(payload)
	result.Version = initialApprovedDefinitionVersion
	result.Digest = digest

	if mode != TaskDefinitionBaselineApply {
		if err := validateApprovedDefinitionProjectionTx(
			ctx, tx, definition, payload); err != nil {
			result.Status = TaskDefinitionBaselineUnsupported
			result.Reason = TaskDefinitionBaselineReasonProjection
			if commitErr := tx.Commit(ctx); commitErr != nil {
				return TaskDefinitionBaselineResult{}, taskStateDatabaseError(
					"commit task definition baseline projection audit", commitErr)
			}
			return result, nil
		}
		if mode == TaskDefinitionBaselineDryRun {
			result.Status = TaskDefinitionBaselineWouldApply
		} else {
			result.Status = TaskDefinitionBaselineNeedsApply
		}
		if err := tx.Commit(ctx); err != nil {
			return TaskDefinitionBaselineResult{}, taskStateDatabaseError(
				"commit task definition baseline read-only audit", err)
		}
		return result, nil
	}

	record, err := insertInitialApprovedDefinitionTx(
		ctx, tx, definition, payload, digest,
		taskDefinitionBaselineOperationRef(scope), head)
	if err != nil {
		return TaskDefinitionBaselineResult{}, err
	}
	result.Status = TaskDefinitionBaselineApplied
	result.Version = record.Version
	result.Digest = record.Digest
	if err := tx.Commit(ctx); err != nil {
		return TaskDefinitionBaselineResult{}, taskStateDatabaseError(
			"commit task definition baseline apply", err)
	}
	return result, nil
}

func lockTaskDefinitionBaselineScope(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID int64,
	taskID string,
) (taskDefinitionHead, error) {
	if err := validateTaskStateScope(tenantID, userID, taskID); err != nil {
		return taskDefinitionHead{}, err
	}
	var head taskDefinitionHead
	var rawMode string
	err := tx.QueryRow(ctx,
		`SELECT s.execution_mode, s.approved_definition_version,
		        s.approved_definition_digest
		   FROM schedules s
		  WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3
		  FOR UPDATE OF s`,
		tenantID, userID, taskID,
	).Scan(&rawMode, &head.Version, &head.Digest)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return taskDefinitionHead{}, taskStateNotFound()
		}
		return taskDefinitionHead{}, taskStateDatabaseError(
			"lock task definition baseline scope", err)
	}
	mode, err := types.ParseExecutionMode(rawMode)
	if err != nil || (head.Version == nil) != (head.Digest == nil) {
		return taskDefinitionHead{}, taskStateIntegrity()
	}
	head.Mode = mode
	return head, nil
}

func taskDefinitionBaselineEncodeReason(
	definition taskstate.ApprovedDefinitionV1,
) string {
	probe := definition
	probe.NLDescription = "retained baseline"
	if _, err := taskstate.EncodeApprovedDefinitionV1(probe); err == nil {
		return TaskDefinitionBaselineReasonDescription
	}
	return TaskDefinitionBaselineReasonProjection
}

func taskDefinitionBaselineMatureTx(
	ctx context.Context,
	tx pgx.Tx,
	scope TaskDefinitionBaselineCursor,
) (bool, error) {
	var mature bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1
			  FROM schedules s
			 WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3
			   AND `+matureSchedulePredicate+`
		)`,
		scope.TenantID, scope.UserID, scope.TaskID,
	).Scan(&mature); err != nil {
		return false, taskStateDatabaseError(
			"verify task definition baseline maturity", err)
	}
	return mature, nil
}

func taskDefinitionBaselineOperationRef(
	scope TaskDefinitionBaselineCursor,
) string {
	return fmt.Sprintf("%s:%d:%d:%s", taskDefinitionBaselineApprovalV1,
		scope.TenantID, scope.UserID, scope.TaskID)
}

func buildTaskDefinitionBaselineV1Tx(
	ctx context.Context,
	tx pgx.Tx,
	scope TaskDefinitionBaselineCursor,
) (taskstate.ApprovedDefinitionV1, string, error) {
	var nlDescription string
	var specJSON, scopeJSON []byte
	var rawStrictness *string
	if err := tx.QueryRow(ctx,
		`SELECT s.nl_description, s.spec_json, s.scope_json, s.push_strictness
		   FROM schedules s
		  WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3
		    AND s.execution_mode=$4
		    AND s.approved_definition_version IS NULL
		    AND s.approved_definition_digest IS NULL
		    AND `+matureSchedulePredicate,
		scope.TenantID, scope.UserID, scope.TaskID,
		types.ExecutionModeCompiled,
	).Scan(&nlDescription, &specJSON, &scopeJSON, &rawStrictness); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return taskstate.ApprovedDefinitionV1{},
				TaskDefinitionBaselineReasonProjection, nil
		}
		return taskstate.ApprovedDefinitionV1{}, "",
			taskStateDatabaseError("load task definition baseline projection", err)
	}

	var playbookContent string
	var fetchPlan json.RawMessage
	err := tx.QueryRow(ctx,
		`SELECT content, fetch_plan
		   FROM schedule_playbooks
		  WHERE schedule_id=$1
		  FOR SHARE`,
		scope.TaskID,
	).Scan(&playbookContent, &fetchPlan)
	if errors.Is(err, pgx.ErrNoRows) {
		return taskstate.ApprovedDefinitionV1{},
			TaskDefinitionBaselineReasonMissingPlaybook, nil
	}
	if err != nil {
		return taskstate.ApprovedDefinitionV1{}, "",
			taskStateDatabaseError("load task definition baseline playbook", err)
	}
	if strings.TrimSpace(playbookContent) == "" {
		return taskstate.ApprovedDefinitionV1{},
			TaskDefinitionBaselineReasonEmptyPlaybook, nil
	}

	linkedIDs := make(map[string]int64)
	rows, err := tx.Query(ctx,
		`SELECT src.url, src.id
		   FROM task_fetch_targets ss
		   JOIN fetch_targets src ON src.id=ss.fetch_target_id
		  WHERE ss.schedule_id=$1
		  ORDER BY src.url, src.id
		  FOR SHARE OF ss, src`,
		scope.TaskID)
	if err != nil {
		return taskstate.ApprovedDefinitionV1{}, "",
			taskStateDatabaseError("load task definition baseline source links", err)
	}
	defer rows.Close()
	for rows.Next() {
		var sourceURL string
		var sourceID int64
		if err := rows.Scan(&sourceURL, &sourceID); err != nil {
			return taskstate.ApprovedDefinitionV1{}, "",
				taskStateDatabaseError("scan task definition baseline source link", err)
		}
		if sourceID <= 0 || !validTaskStateReference(sourceURL, 4096) {
			return taskstate.ApprovedDefinitionV1{},
				TaskDefinitionBaselineReasonProjection, nil
		}
		if _, duplicate := linkedIDs[sourceURL]; duplicate {
			return taskstate.ApprovedDefinitionV1{},
				TaskDefinitionBaselineReasonProjection, nil
		}
		linkedIDs[sourceURL] = sourceID
	}
	if err := rows.Err(); err != nil {
		return taskstate.ApprovedDefinitionV1{}, "",
			taskStateDatabaseError("iterate task definition baseline source links", err)
	}
	rows.Close()

	var planObject map[string]json.RawMessage
	if err := strictjson.Decode(fetchPlan, &planObject); err != nil ||
		planObject == nil {
		return taskstate.ApprovedDefinitionV1{},
			TaskDefinitionBaselineReasonProjection, nil
	}
	sourceScope := taskstate.SourceScopeApprovedPlan
	approvedSources := make([]taskstate.ApprovedSourceV1, 0, len(linkedIDs))
	if len(planObject) == 0 {
		return taskstate.ApprovedDefinitionV1{},
			TaskDefinitionBaselineReasonProjection, nil
	} else {
		var currentPlan currentFetchPlanProjection
		if err := strictjson.Decode(fetchPlan, &currentPlan); err != nil ||
			currentPlan.Targets == nil ||
			len(currentPlan.Targets) != len(linkedIDs) {
			return taskstate.ApprovedDefinitionV1{},
				TaskDefinitionBaselineReasonProjection, nil
		}
		plan := taskstate.FetchPlanV1{
			Sources: make([]taskstate.PlanSourceV1, 0, len(currentPlan.Targets)),
		}
		for _, source := range currentPlan.Targets {
			config := source.Config
			if len(config) == 0 {
				config = json.RawMessage("{}")
			}
			sourceID, ok := linkedIDs[source.URL]
			if !ok {
				return taskstate.ApprovedDefinitionV1{},
					TaskDefinitionBaselineReasonProjection, nil
			}
			plan.Sources = append(plan.Sources, taskstate.PlanSourceV1{
				Platform: types.Platform(source.Platform), Capability: types.Capability(source.Capability),
				Title: source.Title, URL: source.URL, Config: config,
			})
			approvedSources = append(approvedSources, taskstate.ApprovedSourceV1{
				SourceID: sourceID, Platform: types.Platform(source.Platform),
				Capability: types.Capability(source.Capability), Title: source.Title,
				URL: source.URL, Config: config,
			})
		}
		fetchPlan, err = json.Marshal(plan)
		if err != nil {
			return taskstate.ApprovedDefinitionV1{}, "", taskStateIntegrity()
		}
	}

	strictness := legacyTaskProjectionDefaultStrictnessV1
	if rawStrictness != nil {
		strictness = types.PushStrictness(*rawStrictness)
	}
	return taskstate.ApprovedDefinitionV1{
		SchemaVersion: taskstate.ApprovedDefinitionSchemaVersionV1,
		TenantID:      scope.TenantID, UserID: scope.UserID, TaskID: scope.TaskID,
		Intent: playbookContent, NLDescription: nlDescription,
		SpecJSON: specJSON, ScopeJSON: scopeJSON,
		PlaybookContent: playbookContent, SourceScope: sourceScope,
		FetchPlan: fetchPlan, Strictness: strictness, Sources: approvedSources,
		ExecutionMode:  types.ExecutionModeCompiled,
		DeliveryPolicy: taskstate.DeliveryPolicyOwnerFeishu,
		BudgetPolicy:   taskstate.BudgetPolicyInheritTenantQuota,
	}, "", nil
}
