package store

import (
	"context"
	"fmt"
	"math"

	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/types"
)

// AuthorizeAndConsumeTaskRunLLMQuotaV1 is the compiled runtime's final paid
// effect gate. One UPDATE both proves that the sealed snapshot still names the
// exact live tenant/user/task and reserves the current tenant bucket. A task
// revocation and a depleted/missing bucket intentionally have the same public
// result: no upstream request may leave the process.
func (s *Store) AuthorizeAndConsumeTaskRunLLMQuotaV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	rule runtimepolicy.QuotaBucketV1,
	amount float64,
) error {
	pinned, err := validateTaskRunSnapshotReferenceForExpectedV1(ref, expected)
	if err != nil {
		return taskRunValidationError("task run snapshot reference is invalid")
	}
	if rule.Name != string(QuotaLLMTokens) || !rule.Financial ||
		rule.EnforcementVersion != runtimepolicy.QuotaEnforcementLLMPrechargeV1 ||
		amount <= 0 || math.IsNaN(amount) || math.IsInf(amount, 0) {
		return taskRunValidationError("compiled llm quota request is invalid")
	}

	tag, err := s.pool.Exec(ctx,
		`UPDATE tenant_quota q
		    SET tokens = LEAST(q.burst,
		                     q.tokens + q.rate * EXTRACT(EPOCH FROM (now() - q.updated_at))) - $4,
		        updated_at = now()
		   FROM task_run_snapshots r,
		        schedules s,
		        tenants t,
		        memberships m
		  WHERE q.tenant_id = $1
		    AND q.bucket = $2
		    AND r.id = $3
		    AND r.tenant_id = $1
		    AND r.user_id = $5
		    AND r.task_id = $6
		    AND r.temporal_workflow_id = $7
		    AND r.temporal_run_id = $8
		    AND r.reference_digest = $9
		    AND r.payload_digest = $10
		    AND r.definition_digest = $11
		    AND r.plan_digest = $12
		    AND r.capability_catalog_digest = $13
		    AND r.tool_policy_digest = $14
		    AND r.prompt_policy_digest = $15
		    AND r.model_policy_digest = $16
		    AND r.quota_policy_digest = $17
		    AND s.id = r.task_id
		    AND s.tenant_id = r.tenant_id
		    AND s.user_id = r.user_id
		    AND (
		      s.status = $18 OR (
		        s.status = $20 AND authorize_manual_task_run_v1(
		          r.tenant_id, r.user_id, r.task_id,
		          r.temporal_workflow_id
		        )
		      )
		    )
		    AND t.id = s.tenant_id
		    AND t.status = $19
		    AND t.deleted_at IS NULL
		    AND m.tenant_id = s.tenant_id
		    AND m.user_id = s.user_id
		    AND `+matureSchedulePredicate+`
		    AND LEAST(q.burst,
		              q.tokens + q.rate * EXTRACT(EPOCH FROM (now() - q.updated_at))) >= $4`,
		expected.TenantID, string(QuotaLLMTokens), pinned.SnapshotID, amount,
		expected.UserID, expected.TaskID, expected.TemporalWorkflowID,
		expected.TemporalRunID, pinned.ReferenceDigest, pinned.PayloadDigest,
		pinned.DefinitionDigest, pinned.PlanDigest,
		pinned.Policy.CapabilityCatalogDigest, pinned.Policy.ToolPolicyDigest,
		pinned.Policy.PromptPolicyDigest, pinned.Policy.ModelPolicyDigest,
		pinned.Policy.QuotaPolicyDigest, types.ScheduleStatusActive,
		types.TenantStatusActive,
		types.ScheduleStatusPaused,
	)
	if err != nil {
		return classifyQuotaErr(err, fmt.Sprintf(
			"reserve compiled task run llm quota (tenant=%d task=%s)",
			expected.TenantID, expected.TaskID))
	}
	if tag.RowsAffected() != 1 {
		return ErrQuotaExceeded
	}
	return nil
}

// AuthorizeAndConsumeTaskRunLLMQuotaV2 is the Source-free equivalent of the
// V1 paid-effect gate. The distinct ref type prevents a Tool run from entering
// a retained Source authorization path.
func (s *Store) AuthorizeAndConsumeTaskRunLLMQuotaV2(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRefV2,
	rule runtimepolicy.QuotaBucketV1,
	amount float64,
) error {
	if validateTaskRunSnapshotReferenceForExpectedV2(ref, expected) != nil {
		return taskRunValidationError(
			"task run v2 snapshot reference is invalid")
	}
	if rule.Name != string(QuotaLLMTokens) || !rule.Financial ||
		rule.EnforcementVersion !=
			runtimepolicy.QuotaEnforcementLLMPrechargeV1 ||
		amount <= 0 || math.IsNaN(amount) || math.IsInf(amount, 0) {
		return taskRunValidationError(
			"compiled Tool llm quota request is invalid")
	}

	tag, err := s.pool.Exec(ctx,
		`UPDATE tenant_quota q
		    SET tokens = LEAST(q.burst,
		                     q.tokens + q.rate * EXTRACT(EPOCH FROM (now() - q.updated_at))) - $4,
		        updated_at = now()
		   FROM task_run_snapshots r,
		        schedules s,
		        tenants t,
		        memberships m
		  WHERE q.tenant_id = $1
		    AND q.bucket = $2
		    AND r.id = $3
		    AND r.tenant_id = $1
		    AND r.user_id = $5
		    AND r.task_id = $6
		    AND r.temporal_workflow_id = $7
		    AND r.temporal_run_id = $8
		    AND r.reference_digest = $9
		    AND r.payload_digest = $10
		    AND r.definition_digest = $11
		    AND r.plan_digest = $12
		    AND r.capability_catalog_digest = $13
		    AND r.tool_policy_digest = $14
		    AND r.prompt_policy_digest = $15
		    AND r.model_policy_digest = $16
		    AND r.quota_policy_digest = $17
		    AND r.reference_schema_version = $18
		    AND s.id = r.task_id
		    AND s.tenant_id = r.tenant_id
		    AND s.user_id = r.user_id
		    AND (
		      s.status = $19 OR (
		        s.status = $21 AND authorize_manual_task_run_v1(
		          r.tenant_id, r.user_id, r.task_id,
		          r.temporal_workflow_id
		        )
		      )
		    )
		    AND t.id = s.tenant_id
		    AND t.status = $20
		    AND t.deleted_at IS NULL
		    AND m.tenant_id = s.tenant_id
		    AND m.user_id = s.user_id
		    AND `+matureSchedulePredicate+`
		    AND LEAST(q.burst,
		              q.tokens + q.rate * EXTRACT(EPOCH FROM (now() - q.updated_at))) >= $4`,
		expected.TenantID, string(QuotaLLMTokens), ref.SnapshotID, amount,
		expected.UserID, expected.TaskID, expected.TemporalWorkflowID,
		expected.TemporalRunID, ref.ReferenceDigest, ref.PayloadDigest,
		ref.DefinitionDigest, ref.PlanDigest,
		ref.Policy.CapabilityCatalogDigest, ref.Policy.ToolPolicyDigest,
		ref.Policy.PromptPolicyDigest, ref.Policy.ModelPolicyDigest,
		ref.Policy.QuotaPolicyDigest, types.RunSnapshotSchemaVersionV2,
		types.ScheduleStatusActive, types.TenantStatusActive,
		types.ScheduleStatusPaused,
	)
	if err != nil {
		return classifyQuotaErr(err, fmt.Sprintf(
			"reserve compiled Tool run llm quota (tenant=%d task=%s)",
			expected.TenantID, expected.TaskID))
	}
	if tag.RowsAffected() != 1 {
		return ErrQuotaExceeded
	}
	return nil
}
