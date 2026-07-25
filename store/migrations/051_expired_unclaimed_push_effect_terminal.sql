-- 051: terminalize expired, unclaimed deterministic push effects.
--
-- A prepared or definitely-not-sent effect must never remain recoverable once
-- the remaining provider UUID window can no longer contain a complete send
-- lease. The coordinator receives only this fixed transition primitive; it
-- does not receive general blocked_at update authority.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);

-- +goose StatementBegin
CREATE FUNCTION block_expired_unclaimed_push_effect_v1(
    requested_effect_id TEXT,
    requested_tenant_id BIGINT,
    requested_user_id BIGINT,
    requested_fence BIGINT,
    requested_task_id TEXT,
    requested_required_window_microseconds BIGINT
)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public
AS $$
DECLARE
    tenant_context BIGINT;
    database_now TIMESTAMPTZ;
    resulting_decision TEXT;
BEGIN
    tenant_context :=
        NULLIF(pg_catalog.current_setting('app.tenant_id',true),'')::bigint;
    database_now := pg_catalog.clock_timestamp();
    IF requested_effect_id IS NULL OR requested_effect_id='' OR
       requested_tenant_id<=0 OR requested_user_id<=0 OR
       requested_fence<0 OR
       tenant_context IS DISTINCT FROM requested_tenant_id OR
       requested_task_id IS NULL OR requested_task_id='' OR
       pg_catalog.btrim(requested_task_id)<>requested_task_id OR
       pg_catalog.octet_length(requested_task_id)>512 OR
       requested_required_window_microseconds<=0 OR
       requested_required_window_microseconds>86400000000 THEN
        RETURN '';
    END IF;

    UPDATE public.push_effects
       SET status='blocked',
           -- A never-claimed prepared row has a zero fence/attempt, while the
           -- original state constraint requires a durable blocked fence to be
           -- positive. This transition is the first authority checkpoint; it
           -- never represents a provider request or receipt.
           fence=GREATEST(fence,1),
           attempt=GREATEST(attempt,1),
           failure_class='provider_window_expired_no_send',
           blocked_at=database_now,
           updated_at=database_now
     WHERE id=requested_effect_id
       AND tenant_id=requested_tenant_id
       AND user_id=requested_user_id
       AND fence=requested_fence
       AND task_id=requested_task_id
       AND status IN ('prepared','definite_failed')
       AND lease_owner='' AND lease_until IS NULL
       AND database_now+
           requested_required_window_microseconds*interval '1 microsecond'
           >idempotency_expires_at
       AND tenant_id IS NOT DISTINCT FROM tenant_context
     RETURNING 'blocked' INTO resulting_decision;

    IF resulting_decision IS NULL THEN
        SELECT 'blocked' INTO resulting_decision
          FROM public.push_effects
         WHERE id=requested_effect_id
           AND tenant_id=requested_tenant_id
           AND user_id=requested_user_id
           AND task_id=requested_task_id
           AND status='blocked'
           AND failure_class='provider_window_expired_no_send'
           AND blocked_at IS NOT NULL
           AND (
               fence=requested_fence OR
               (requested_fence=0 AND fence=1 AND attempt=1)
           )
           AND tenant_id IS NOT DISTINCT FROM tenant_context;
    END IF;

    IF resulting_decision IS NULL THEN
        SELECT 'open' INTO resulting_decision
          FROM public.push_effects
         WHERE id=requested_effect_id
           AND tenant_id=requested_tenant_id
           AND user_id=requested_user_id
           AND fence=requested_fence
           AND task_id=requested_task_id
           AND status IN ('prepared','definite_failed')
           AND lease_owner='' AND lease_until IS NULL
           AND database_now+
               requested_required_window_microseconds*interval '1 microsecond'
               <=idempotency_expires_at
           AND tenant_id IS NOT DISTINCT FROM tenant_context;
    END IF;
    RETURN COALESCE(resulting_decision,'');
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION block_expired_unclaimed_push_effect_v1(
    TEXT,BIGINT,BIGINT,BIGINT,TEXT,BIGINT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION block_expired_unclaimed_push_effect_v1(
    TEXT,BIGINT,BIGINT,BIGINT,TEXT,BIGINT
) TO vane_push_effect_coordinator;

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM push_effects
         WHERE status='blocked'
           AND failure_class='provider_window_expired_no_send'
    ) THEN
        RAISE EXCEPTION
            '051: refusing downgrade while expired push effect terminal state exists';
    END IF;
END
$$;
-- +goose StatementEnd

REVOKE EXECUTE ON FUNCTION block_expired_unclaimed_push_effect_v1(
    TEXT,BIGINT,BIGINT,BIGINT,TEXT,BIGINT
) FROM vane_push_effect_coordinator;
DROP FUNCTION block_expired_unclaimed_push_effect_v1(
    TEXT,BIGINT,BIGINT,BIGINT,TEXT,BIGINT
);
