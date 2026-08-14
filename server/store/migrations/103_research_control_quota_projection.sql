-- 103: let the restricted research control Store read only the quota policy
-- for its exact active tenant/user/task. The mutable token balance remains
-- hidden and all quota consumption/settlement stays in the paid executor.

-- +goose Up

-- +goose StatementBegin
CREATE FUNCTION resolve_research_quota_rule_v1(
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

-- The projection is the only read capability. No server role receives table
-- or column privileges, especially not the tenant's current token balance.
REVOKE ALL ON tenant_quota FROM vane_app;

-- +goose Down

REVOKE ALL ON FUNCTION resolve_research_quota_rule_v1(
    BIGINT,BIGINT,TEXT,TEXT) FROM vane_app;
DROP FUNCTION resolve_research_quota_rule_v1(BIGINT,BIGINT,TEXT,TEXT);
