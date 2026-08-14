-- 048: atomically settle one durable provider effect with its delivery,
-- observed-event, and complete batch projections.
--
-- Migration 047 owns delivery_authority and the exact batch row fence. This
-- migration grants only the additional receipt/aggregate capabilities.
--
-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);

GRANT SELECT (
    delivery_ids, card_payload, canonical_payload, chunk_index, chunk_count
) ON push_effects TO vane_push_effect_receipt;

GRANT SELECT (
    id, tenant_id, user_id, batch_id, status, feishu_message_id,
    card_json, sent_at
) ON deliveries TO vane_push_effect_receipt;
GRANT UPDATE (
    status, feishu_message_id, card_json, sent_at
) ON deliveries TO vane_push_effect_receipt;

GRANT SELECT (
    event_key, tenant_id, user_id, task_id, run_snapshot_id, temporal_run_id,
    delivery_id, status, delivered_at
) ON task_observed_events TO vane_push_effect_receipt;
GRANT UPDATE (
    status, delivered_at
) ON task_observed_events TO vane_push_effect_receipt;

GRANT SELECT (
    id, tenant_id, user_id, status, run_snapshot_id, delivery_authority
) ON push_batches TO vane_push_effect_receipt;
GRANT UPDATE (
    status
) ON push_batches TO vane_push_effect_receipt;

CREATE POLICY push_effect_receipt_select_visible ON task_observed_events
    FOR SELECT TO vane_push_effect_receipt USING (true);
CREATE POLICY push_effect_receipt_update_visible ON task_observed_events
    FOR UPDATE TO vane_push_effect_receipt
    USING (true) WITH CHECK (true);
CREATE POLICY push_effect_receipt_tenant_isolation ON task_observed_events
    AS RESTRICTIVE FOR ALL TO vane_push_effect_receipt
    USING (tenant_id IS NOT DISTINCT FROM
        (SELECT current_setting('app.tenant_id', true))::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
        (SELECT current_setting('app.tenant_id', true))::bigint);

-- Creation freezes delivery rows before proving the optional observed-event
-- binding. The coordinator receives no arbitrary UPDATE on either table.
-- +goose StatementBegin
CREATE FUNCTION lock_push_effect_aggregate_v1(
    requested_tenant_id BIGINT,
    requested_user_id BIGINT,
    requested_batch_id BIGINT,
    requested_delivery_ids BIGINT[]
)
RETURNS TABLE(
    locked_delivery_ids BIGINT[],
    locked_delivery_statuses TEXT[],
    locked_delivery_message_ids TEXT[],
    locked_delivery_has_sent_at BOOLEAN[],
    observed_delivery_ids BIGINT[],
    observed_event_keys TEXT[],
    observed_task_ids TEXT[],
    observed_run_snapshot_ids BIGINT[],
    observed_temporal_run_ids TEXT[],
    observed_statuses TEXT[],
    observed_has_delivered_at BOOLEAN[]
)
LANGUAGE sql
SECURITY DEFINER
SET search_path=pg_catalog,public
AS $$
    WITH locked_deliveries AS MATERIALIZED (
        SELECT d.id,d.status,d.feishu_message_id,d.sent_at
          FROM public.deliveries d
         WHERE d.tenant_id=requested_tenant_id
           AND d.user_id=requested_user_id
           AND d.batch_id=requested_batch_id
           AND d.id=ANY(requested_delivery_ids)
           AND d.tenant_id IS NOT DISTINCT FROM
               NULLIF(pg_catalog.current_setting('app.tenant_id',true),'')::bigint
         ORDER BY d.id
         FOR UPDATE OF d
    ),
    locked_events AS MATERIALIZED (
        SELECT e.delivery_id,e.event_key,e.task_id,e.run_snapshot_id,
               e.temporal_run_id,e.status,e.delivered_at
          FROM public.task_observed_events e
          JOIN locked_deliveries d ON d.id=e.delivery_id
         WHERE e.tenant_id=requested_tenant_id
           AND e.user_id=requested_user_id
         ORDER BY e.delivery_id,e.event_key
         FOR UPDATE OF e
    )
    SELECT
        ARRAY(SELECT id FROM locked_deliveries ORDER BY id),
        ARRAY(SELECT status FROM locked_deliveries ORDER BY id),
        ARRAY(SELECT feishu_message_id FROM locked_deliveries ORDER BY id),
        ARRAY(SELECT sent_at IS NOT NULL FROM locked_deliveries ORDER BY id),
        ARRAY(SELECT delivery_id FROM locked_events ORDER BY delivery_id,event_key),
        ARRAY(SELECT event_key FROM locked_events ORDER BY delivery_id,event_key),
        ARRAY(SELECT task_id FROM locked_events ORDER BY delivery_id,event_key),
        ARRAY(SELECT run_snapshot_id FROM locked_events ORDER BY delivery_id,event_key),
        ARRAY(SELECT temporal_run_id FROM locked_events ORDER BY delivery_id,event_key),
        ARRAY(SELECT status FROM locked_events ORDER BY delivery_id,event_key),
        ARRAY(SELECT delivered_at IS NOT NULL
                FROM locked_events ORDER BY delivery_id,event_key)
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION lock_push_effect_aggregate_v1(
    BIGINT,BIGINT,BIGINT,BIGINT[]
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lock_push_effect_aggregate_v1(
    BIGINT,BIGINT,BIGINT,BIGINT[]
) TO vane_push_effect_coordinator;

-- Observation admission runs as vane_app, which deliberately has no direct
-- push_effects privilege. Expose only exact batch IDs with durable effects.
-- +goose StatementBegin
CREATE FUNCTION lock_observed_event_push_effects_v1(
    requested_tenant_id BIGINT,
    requested_user_id BIGINT,
    requested_batch_ids BIGINT[]
)
RETURNS BIGINT[]
LANGUAGE sql
SECURITY DEFINER
SET search_path=pg_catalog,public
AS $$
    SELECT ARRAY(
        SELECT e.batch_id
          FROM public.push_effects e
         WHERE e.tenant_id=requested_tenant_id
           AND e.user_id=requested_user_id
           AND e.batch_id=ANY(requested_batch_ids)
           AND e.tenant_id IS NOT DISTINCT FROM
               NULLIF(pg_catalog.current_setting('app.tenant_id',true),'')::bigint
         ORDER BY e.batch_id,e.chunk_index,e.id
         FOR UPDATE OF e
    )
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION lock_observed_event_push_effects_v1(
    BIGINT,BIGINT,BIGINT[]
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lock_observed_event_push_effects_v1(
    BIGINT,BIGINT,BIGINT[]
) TO vane_app;

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);

LOCK TABLE push_batches, push_effects, deliveries, task_observed_events
    IN ACCESS EXCLUSIVE MODE /* migration 048 downgrade fence */;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM push_effects)
       OR EXISTS (
           SELECT 1 FROM push_batches
            WHERE delivery_authority='effect'
       ) THEN
        RAISE EXCEPTION
            '048: refusing downgrade while durable effect settlement exists';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE EXECUTE ON FUNCTION lock_observed_event_push_effects_v1(
    BIGINT,BIGINT,BIGINT[]
) FROM vane_app;
DROP FUNCTION lock_observed_event_push_effects_v1(
    BIGINT,BIGINT,BIGINT[]
);
REVOKE EXECUTE ON FUNCTION lock_push_effect_aggregate_v1(
    BIGINT,BIGINT,BIGINT,BIGINT[]
) FROM vane_push_effect_coordinator;
DROP FUNCTION lock_push_effect_aggregate_v1(
    BIGINT,BIGINT,BIGINT,BIGINT[]
);

DROP POLICY push_effect_receipt_tenant_isolation ON task_observed_events;
DROP POLICY push_effect_receipt_update_visible ON task_observed_events;
DROP POLICY push_effect_receipt_select_visible ON task_observed_events;

REVOKE UPDATE (
    status
) ON push_batches FROM vane_push_effect_receipt;
REVOKE SELECT (
    id, tenant_id, user_id, status, run_snapshot_id, delivery_authority
) ON push_batches FROM vane_push_effect_receipt;

REVOKE UPDATE (
    status, delivered_at
) ON task_observed_events FROM vane_push_effect_receipt;
REVOKE SELECT (
    event_key, tenant_id, user_id, task_id, run_snapshot_id, temporal_run_id,
    delivery_id, status, delivered_at
) ON task_observed_events FROM vane_push_effect_receipt;

REVOKE UPDATE (
    status, feishu_message_id, card_json, sent_at
) ON deliveries FROM vane_push_effect_receipt;
REVOKE SELECT (
    id, tenant_id, user_id, batch_id, status, feishu_message_id,
    card_json, sent_at
) ON deliveries FROM vane_push_effect_receipt;

REVOKE SELECT (
    delivery_ids, card_payload, canonical_payload, chunk_index, chunk_count
) ON push_effects FROM vane_push_effect_receipt;
