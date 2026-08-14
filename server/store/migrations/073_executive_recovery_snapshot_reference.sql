-- 073: executive Brief recovery must receive the complete immutable
-- RunSnapshotRef, not only its digests. The v1 reader exposed a partial
-- reference that could never pass RunSnapshotRef.Validate once a real recovery
-- candidate appeared.

-- +goose Up

-- +goose StatementBegin
CREATE FUNCTION read_executive_synthesis_recovery_v2(
    after_candidate_at TIMESTAMPTZ,
    after_outcome_id BIGINT,
    requested_limit INTEGER
)
RETURNS TABLE (
    candidate_at TIMESTAMPTZ,
    recovery_kind TEXT,
    outcome_id BIGINT,
    outcome_schema_version TEXT,
    snapshot_reference JSONB,
    push_batch_id BIGINT,
    receipt_status TEXT,
    profile_epoch BIGINT,
    profile_version BIGINT,
    profile_digest TEXT,
    input_digest TEXT,
    finalized_at TIMESTAMPTZ
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
    SELECT
        c.candidate_at,c.recovery_kind,c.outcome_id,
        c.outcome_schema_version,
        jsonb_build_object(
            'schema_version',rs.reference_schema_version,
            'snapshot_id',rs.id,
            'temporal_workflow_id',rs.temporal_workflow_id,
            'temporal_run_id',rs.temporal_run_id,
            'run_kind',rs.run_kind,
            'tenant_id',rs.tenant_id,
            'user_id',rs.user_id,
            'task_id',rs.task_id,
            'mode',rs.execution_mode,
            'definition_digest',rs.definition_digest,
            'plan_digest',rs.plan_digest,
            'adaptive_version',rs.adaptive_version,
            'policy',jsonb_build_object(
                'capability_catalog_digest',rs.capability_catalog_digest,
                'tool_policy_digest',rs.tool_policy_digest,
                'prompt_policy_digest',rs.prompt_policy_digest,
                'model_policy_digest',rs.model_policy_digest,
                'quota_policy_digest',rs.quota_policy_digest
            ),
            'planner_budget',rs.budget,
            'payload_digest',rs.payload_digest,
            'reference_digest',rs.reference_digest
        ),
        c.push_batch_id,c.receipt_status,c.profile_epoch,
        c.profile_version,c.profile_digest,c.input_digest,c.finalized_at
      FROM public.read_executive_synthesis_recovery_v1(
               after_candidate_at,after_outcome_id,requested_limit
           ) c
      JOIN public.task_run_snapshots rs
        ON rs.id=c.run_snapshot_id
       AND rs.tenant_id=c.tenant_id
       AND rs.user_id=c.user_id
       AND rs.task_id=c.task_id
       AND rs.temporal_workflow_id=c.temporal_workflow_id
       AND rs.temporal_run_id=c.temporal_run_id
     ORDER BY c.candidate_at,c.outcome_id
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION
    read_executive_synthesis_recovery_v2(
        TIMESTAMPTZ,BIGINT,INTEGER
    )
    FROM PUBLIC,vane_app,vane_brief_synthesis_writer,
         vane_brief_synthesis_recovery;
GRANT EXECUTE ON FUNCTION
    read_executive_synthesis_recovery_v2(
        TIMESTAMPTZ,BIGINT,INTEGER
    )
    TO vane_brief_synthesis_recovery;

-- +goose Down

DROP FUNCTION read_executive_synthesis_recovery_v2(
    TIMESTAMPTZ,BIGINT,INTEGER
);
