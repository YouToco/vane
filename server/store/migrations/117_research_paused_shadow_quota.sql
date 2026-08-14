-- 117: let a delivery-dark Research V3 shadow read the tenant's rate/burst
-- policy while the exact production Schedule remains paused. A generic paused
-- task is still ineligible: the sidecar head must be bound to a prepare
-- operation that observed the same paused status.

-- +goose Up

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION resolve_research_quota_rule_v1(
    requested_tenant_id BIGINT,
    requested_user_id BIGINT,
    requested_task_id TEXT,
    requested_bucket TEXT
) RETURNS TABLE(out_rate DOUBLE PRECISION,out_burst DOUBLE PRECISION)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
    IF (session_user<>current_user AND session_user<>'vane_server_runtime') OR
       requested_tenant_id<=0 OR requested_user_id<=0 OR
       requested_task_id IS NULL OR btrim(requested_task_id)<>requested_task_id OR
       octet_length(requested_task_id) NOT BETWEEN 1 AND 255 OR
       requested_bucket NOT IN
           ('llm_tokens','exa_calls','tikhub_calls','push','fetch') THEN
        RAISE EXCEPTION '117: research quota resolver caller or input is forbidden'
            USING ERRCODE='42501';
    END IF;
    IF requested_tenant_id IS DISTINCT FROM
           NULLIF(current_setting('app.tenant_id',true),'')::bigint OR
       requested_user_id IS DISTINCT FROM
           NULLIF(current_setting('app.user_id',true),'')::bigint THEN
        RAISE EXCEPTION '117: research quota resolver scope is forbidden'
            USING ERRCODE='42501';
    END IF;

    RETURN QUERY
    SELECT quota.rate,quota.burst
      FROM public.schedules schedule
      JOIN public.tenants tenant ON tenant.id=schedule.tenant_id
      JOIN public.memberships membership
        ON membership.tenant_id=schedule.tenant_id
       AND membership.user_id=schedule.user_id
      JOIN public.tenant_quota quota ON quota.tenant_id=schedule.tenant_id
     WHERE schedule.tenant_id=requested_tenant_id
       AND schedule.user_id=requested_user_id
       AND schedule.id=requested_task_id
       AND (
           schedule.status='active' OR
           (schedule.status='paused' AND EXISTS (
               SELECT 1
                 FROM public.research_v3_prepared_definition_heads head
                 JOIN public.research_v3_definition_prepare_operations operation
                   ON operation.id=head.prepare_operation_id
                  AND operation.tenant_id=head.tenant_id
                  AND operation.user_id=head.user_id
                  AND operation.task_id=head.task_id
                  AND operation.target_definition_version=head.definition_version
                  AND operation.target_definition_digest=head.definition_digest
                  AND operation.source_baseline_digest=head.source_baseline_digest
                  AND operation.original_execution_mode=head.base_execution_mode
                  AND operation.original_definition_version IS NOT DISTINCT FROM
                      head.base_definition_version
                  AND operation.original_definition_digest IS NOT DISTINCT FROM
                      head.base_definition_digest
                 JOIN public.task_approved_definition_versions definition
                   ON definition.tenant_id=head.tenant_id
                  AND definition.user_id=head.user_id
                  AND definition.task_id=head.task_id
                  AND definition.version=head.definition_version
                  AND definition.definition_digest=head.definition_digest
                  AND definition.execution_mode=head.execution_mode
                WHERE head.tenant_id=schedule.tenant_id
                  AND head.user_id=schedule.user_id
                  AND head.task_id=schedule.id
                  AND head.execution_mode='discover_at_run'
                  AND head.prepared_schedule_status='paused'
                  AND head.prepared_schedule_status=schedule.status
                  AND operation.original_schedule_status='paused'
                  AND operation.original_schedule_status=head.prepared_schedule_status
                  AND operation.phase='prepared'
                  AND definition.schema_version='vane.task-approved-definition/v3'
                  AND (
                      (schedule.execution_mode=head.base_execution_mode AND
                       schedule.approved_definition_version IS NOT DISTINCT FROM
                           head.base_definition_version AND
                       schedule.approved_definition_digest IS NOT DISTINCT FROM
                           head.base_definition_digest) OR
                      (schedule.execution_mode='discover_at_run' AND
                       schedule.approved_definition_version=head.definition_version AND
                       schedule.approved_definition_digest=head.definition_digest)
                  )
           ))
       )
       AND tenant.status='active' AND tenant.deleted_at IS NULL
       AND membership.role='owner'
       AND quota.bucket=requested_bucket
     LIMIT 1;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION resolve_research_quota_rule_v1(
    BIGINT,BIGINT,TEXT,TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_research_quota_rule_v1(
    BIGINT,BIGINT,TEXT,TEXT) TO vane_app;

-- +goose Down

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION resolve_research_quota_rule_v1(
    requested_tenant_id BIGINT,
    requested_user_id BIGINT,
    requested_task_id TEXT,
    requested_bucket TEXT
) RETURNS TABLE(out_rate DOUBLE PRECISION,out_burst DOUBLE PRECISION)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
    IF (session_user<>current_user AND session_user<>'vane_server_runtime') OR
       requested_tenant_id<=0 OR requested_user_id<=0 OR
       requested_task_id IS NULL OR btrim(requested_task_id)<>requested_task_id OR
       octet_length(requested_task_id) NOT BETWEEN 1 AND 255 OR
       requested_bucket NOT IN
           ('llm_tokens','exa_calls','tikhub_calls','push','fetch') THEN
        RAISE EXCEPTION '103: research quota resolver caller or input is forbidden'
            USING ERRCODE='42501';
    END IF;
    IF requested_tenant_id IS DISTINCT FROM
           NULLIF(current_setting('app.tenant_id',true),'')::bigint OR
       requested_user_id IS DISTINCT FROM
           NULLIF(current_setting('app.user_id',true),'')::bigint THEN
        RAISE EXCEPTION '103: research quota resolver scope is forbidden'
            USING ERRCODE='42501';
    END IF;

    RETURN QUERY
    SELECT quota.rate,quota.burst
      FROM public.schedules schedule
      JOIN public.tenants tenant ON tenant.id=schedule.tenant_id
      JOIN public.memberships membership
        ON membership.tenant_id=schedule.tenant_id
       AND membership.user_id=schedule.user_id
      JOIN public.tenant_quota quota ON quota.tenant_id=schedule.tenant_id
     WHERE schedule.tenant_id=requested_tenant_id
       AND schedule.user_id=requested_user_id
       AND schedule.id=requested_task_id
       AND schedule.status='active'
       AND tenant.status='active' AND tenant.deleted_at IS NULL
       AND membership.role='owner'
       AND quota.bucket=requested_bucket
     LIMIT 1;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION resolve_research_quota_rule_v1(
    BIGINT,BIGINT,TEXT,TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_research_quota_rule_v1(
    BIGINT,BIGINT,TEXT,TEXT) TO vane_app;
