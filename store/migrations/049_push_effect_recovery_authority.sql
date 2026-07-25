-- 049: exact-task dark push-effect recovery authority.
--
-- This migration is additive and has no production call point. It gives the
-- existing tenant-scoped coordinator only the columns needed to rebuild one
-- sealed RunSnapshotRef and evaluate live task authority. Ambiguous block
-- transitions stay behind fixed-predicate SECURITY DEFINER primitives so the
-- coordinator never receives arbitrary blocked_at write authority.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);

GRANT SELECT (
    temporal_workflow_id, run_kind, execution_mode, adaptive_version,
    capability_catalog_digest,
    tool_policy_digest, prompt_policy_digest, model_policy_digest,
    quota_policy_digest, definition_digest, plan_digest, payload_digest,
    reference_digest, reference_schema_version, budget
) ON task_run_snapshots TO vane_push_effect_coordinator;
GRANT SELECT (
    id, tenant_id, user_id, status
) ON schedules TO vane_push_effect_coordinator;
GRANT SELECT (
    status, deleted_at
) ON tenants TO vane_push_effect_coordinator;
GRANT SELECT (
    tenant_id, user_id
) ON memberships TO vane_push_effect_coordinator;
GRANT SELECT (
    tenant_id, user_id, task_id, tool_name, execution_version, status, phase
) ON pending_actions TO vane_push_effect_coordinator;

-- schedules, pending_actions and task_run_snapshots already have restrictive
-- tenant policies applying to every role. tenants/memberships predate those
-- general policies, so add coordinator-specific restrictive boundaries.
CREATE POLICY push_effect_coordinator_tenant_isolation ON tenants
    AS RESTRICTIVE FOR SELECT TO vane_push_effect_coordinator
    USING (id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint);
CREATE POLICY push_effect_coordinator_tenant_isolation ON memberships
    AS RESTRICTIVE FOR SELECT TO vane_push_effect_coordinator
    USING (tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint);

-- +goose StatementBegin
CREATE FUNCTION defer_or_block_push_effect_reconciliation_v1(
    requested_effect_id TEXT,
    requested_tenant_id BIGINT,
    requested_user_id BIGINT,
    requested_fence BIGINT,
    requested_retry_after_microseconds BIGINT,
    requested_until_expiry BOOLEAN
)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public
AS $$
DECLARE
    tenant_context BIGINT;
    database_now TIMESTAMPTZ;
    resulting_status TEXT;
BEGIN
    tenant_context :=
        NULLIF(pg_catalog.current_setting('app.tenant_id',true),'')::bigint;
    database_now := pg_catalog.clock_timestamp();
    IF requested_effect_id IS NULL OR requested_effect_id='' OR
       requested_tenant_id<=0 OR requested_user_id<=0 OR
       requested_fence<=0 OR tenant_context IS DISTINCT FROM requested_tenant_id OR
       requested_retry_after_microseconds<=0 OR
       requested_retry_after_microseconds>2592000000000 THEN
        RETURN '';
    END IF;

    UPDATE public.push_effects
       SET status=CASE
               WHEN idempotency_expires_at<=database_now
               THEN 'blocked'
               ELSE 'ambiguous'
           END,
           failure_class=CASE
               WHEN idempotency_expires_at<=database_now
               THEN 'provider_window_expired'
               ELSE 'provider_history_inconclusive'
           END,
           next_attempt_at=CASE
               WHEN idempotency_expires_at<=database_now
               THEN next_attempt_at
               WHEN requested_until_expiry
               THEN idempotency_expires_at
               ELSE LEAST(
                   idempotency_expires_at,
                   database_now+
                       requested_retry_after_microseconds*
                       interval '1 microsecond'
               )
           END,
           blocked_at=CASE
               WHEN idempotency_expires_at<=database_now
               THEN database_now
               ELSE NULL
           END,
           updated_at=database_now
     WHERE id=requested_effect_id
       AND tenant_id=requested_tenant_id
       AND user_id=requested_user_id
       AND fence=requested_fence
       AND status='ambiguous'
       AND lease_owner='' AND lease_until IS NULL
       AND tenant_id IS NOT DISTINCT FROM tenant_context
     RETURNING CASE
                   WHEN status='blocked' THEN 'blocked'
                   ELSE 'deferred'
               END
          INTO resulting_status;

    IF resulting_status IS NULL THEN
        SELECT 'blocked' INTO resulting_status
          FROM public.push_effects
         WHERE id=requested_effect_id
           AND tenant_id=requested_tenant_id
           AND user_id=requested_user_id
           AND fence=requested_fence
           AND status='blocked'
           AND failure_class='provider_window_expired'
           AND blocked_at IS NOT NULL
           AND tenant_id IS NOT DISTINCT FROM tenant_context;
    END IF;
    RETURN COALESCE(resulting_status,'');
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION defer_or_block_push_effect_reconciliation_v1(
    TEXT,BIGINT,BIGINT,BIGINT,BIGINT,BOOLEAN
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION defer_or_block_push_effect_reconciliation_v1(
    TEXT,BIGINT,BIGINT,BIGINT,BIGINT,BOOLEAN
) TO vane_push_effect_coordinator;

-- +goose StatementBegin
CREATE FUNCTION block_conflicting_push_effect_history_v1(
    requested_effect_id TEXT,
    requested_tenant_id BIGINT,
    requested_user_id BIGINT,
    requested_fence BIGINT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public
AS $$
DECLARE
    tenant_context BIGINT;
    database_now TIMESTAMPTZ;
    changed BOOLEAN;
BEGIN
    tenant_context :=
        NULLIF(pg_catalog.current_setting('app.tenant_id',true),'')::bigint;
    database_now := pg_catalog.clock_timestamp();
    IF requested_effect_id IS NULL OR requested_effect_id='' OR
       requested_tenant_id<=0 OR requested_user_id<=0 OR
       requested_fence<=0 OR tenant_context IS DISTINCT FROM requested_tenant_id THEN
        RETURN false;
    END IF;

    UPDATE public.push_effects
       SET status='blocked',
           failure_class='provider_history_conflict',
           blocked_at=database_now,
           updated_at=database_now
     WHERE id=requested_effect_id
       AND tenant_id=requested_tenant_id
       AND user_id=requested_user_id
       AND fence=requested_fence
       AND status='ambiguous'
       AND lease_owner='' AND lease_until IS NULL
       AND tenant_id IS NOT DISTINCT FROM tenant_context
     RETURNING true INTO changed;

    IF changed IS NULL THEN
        SELECT true INTO changed
          FROM public.push_effects
         WHERE id=requested_effect_id
           AND tenant_id=requested_tenant_id
           AND user_id=requested_user_id
           AND fence=requested_fence
           AND status='blocked'
           AND failure_class='provider_history_conflict'
           AND blocked_at IS NOT NULL
           AND tenant_id IS NOT DISTINCT FROM tenant_context;
    END IF;
    RETURN COALESCE(changed,false);
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION block_conflicting_push_effect_history_v1(
    TEXT,BIGINT,BIGINT,BIGINT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION block_conflicting_push_effect_history_v1(
    TEXT,BIGINT,BIGINT,BIGINT
) TO vane_push_effect_coordinator;

-- +goose StatementBegin
CREATE FUNCTION block_exhausted_push_effect_attempts_v1(
    requested_effect_id TEXT,
    requested_tenant_id BIGINT,
    requested_user_id BIGINT,
    requested_fence BIGINT,
    requested_task_id TEXT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public
AS $$
DECLARE
    tenant_context BIGINT;
    database_now TIMESTAMPTZ;
    changed BOOLEAN;
BEGIN
    tenant_context :=
        NULLIF(pg_catalog.current_setting('app.tenant_id',true),'')::bigint;
    database_now := pg_catalog.clock_timestamp();
    IF requested_effect_id IS NULL OR requested_effect_id='' OR
       requested_tenant_id<=0 OR requested_user_id<=0 OR
       requested_fence<=0 OR tenant_context IS DISTINCT FROM requested_tenant_id OR
       requested_task_id IS NULL OR requested_task_id='' OR
       pg_catalog.btrim(requested_task_id)<>requested_task_id OR
       pg_catalog.octet_length(requested_task_id)>512 THEN
        RETURN false;
    END IF;

    UPDATE public.push_effects
       SET status='blocked',
           failure_class='attempt_budget_exhausted',
           blocked_at=database_now,
           updated_at=database_now
     WHERE id=requested_effect_id
       AND tenant_id=requested_tenant_id
       AND user_id=requested_user_id
       AND fence=requested_fence
       AND task_id=requested_task_id
       AND status IN ('prepared','definite_failed')
       AND attempt>=8
       AND lease_owner='' AND lease_until IS NULL
       AND tenant_id IS NOT DISTINCT FROM tenant_context
     RETURNING true INTO changed;

    IF changed IS NULL THEN
        SELECT true INTO changed
          FROM public.push_effects
         WHERE id=requested_effect_id
           AND tenant_id=requested_tenant_id
           AND user_id=requested_user_id
           AND fence=requested_fence
           AND task_id=requested_task_id
           AND attempt>=8
           AND status='blocked'
           AND failure_class='attempt_budget_exhausted'
           AND blocked_at IS NOT NULL
           AND tenant_id IS NOT DISTINCT FROM tenant_context;
    END IF;
    RETURN COALESCE(changed,false);
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION block_exhausted_push_effect_attempts_v1(
    TEXT,BIGINT,BIGINT,BIGINT,TEXT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION block_exhausted_push_effect_attempts_v1(
    TEXT,BIGINT,BIGINT,BIGINT,TEXT
) TO vane_push_effect_coordinator;

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);

REVOKE EXECUTE ON FUNCTION block_exhausted_push_effect_attempts_v1(
    TEXT,BIGINT,BIGINT,BIGINT,TEXT
) FROM vane_push_effect_coordinator;
DROP FUNCTION block_exhausted_push_effect_attempts_v1(
    TEXT,BIGINT,BIGINT,BIGINT,TEXT
);
REVOKE EXECUTE ON FUNCTION block_conflicting_push_effect_history_v1(
    TEXT,BIGINT,BIGINT,BIGINT
) FROM vane_push_effect_coordinator;
DROP FUNCTION block_conflicting_push_effect_history_v1(
    TEXT,BIGINT,BIGINT,BIGINT
);
REVOKE EXECUTE ON FUNCTION defer_or_block_push_effect_reconciliation_v1(
    TEXT,BIGINT,BIGINT,BIGINT,BIGINT,BOOLEAN
) FROM vane_push_effect_coordinator;
DROP FUNCTION defer_or_block_push_effect_reconciliation_v1(
    TEXT,BIGINT,BIGINT,BIGINT,BIGINT,BOOLEAN
);

DROP POLICY push_effect_coordinator_tenant_isolation ON memberships;
DROP POLICY push_effect_coordinator_tenant_isolation ON tenants;

REVOKE SELECT (
    tenant_id, user_id, task_id, tool_name, execution_version, status, phase
) ON pending_actions FROM vane_push_effect_coordinator;
REVOKE SELECT (
    tenant_id, user_id
) ON memberships FROM vane_push_effect_coordinator;
REVOKE SELECT (
    status, deleted_at
) ON tenants FROM vane_push_effect_coordinator;
REVOKE SELECT (
    id, tenant_id, user_id, status
) ON schedules FROM vane_push_effect_coordinator;
REVOKE SELECT (
    temporal_workflow_id, run_kind, execution_mode, adaptive_version,
    capability_catalog_digest,
    tool_policy_digest, prompt_policy_digest, model_policy_digest,
    quota_policy_digest, definition_digest, plan_digest, payload_digest,
    reference_digest, reference_schema_version, budget
) ON task_run_snapshots FROM vane_push_effect_coordinator;
