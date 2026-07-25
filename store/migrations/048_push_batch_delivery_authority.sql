-- 048: durable first-writer-wins authority for one push batch.
--
-- The nullable state is intentional. Rows created before this migration that
-- never entered the effect protocol remain unclaimed until a post-048 worker
-- reaches their Push activity. Any batch with durable push_effects is
-- irrevocably backfilled to effect before the new binary can observe it.
--
-- +goose Up

-- Stable ASCII "VANEPUSH". All post-048 push-effect writers take the matching
-- shared transaction lock before touching this schema.
SELECT pg_advisory_xact_lock(6215335020355474248);

ALTER TABLE push_batches
    ADD COLUMN delivery_authority TEXT,
    ADD CONSTRAINT push_batches_delivery_authority_valid
        CHECK (delivery_authority IN ('legacy', 'effect'));

UPDATE push_batches b
   SET delivery_authority='effect'
 WHERE EXISTS (
       SELECT 1 FROM push_effects e
        WHERE e.tenant_id=b.tenant_id
          AND e.user_id=b.user_id
          AND e.batch_id=b.id
   );

-- A pre-048 legacy retry may already contain exact sent receipts. It must
-- remain on the legacy protocol even when the task is now selected by the
-- effect canary; otherwise one batch could mix provider protocols.
UPDATE push_batches b
   SET delivery_authority='legacy'
 WHERE delivery_authority IS NULL
   AND EXISTS (
       SELECT 1 FROM deliveries d
        WHERE d.tenant_id=b.tenant_id
          AND d.user_id=b.user_id
          AND d.batch_id=b.id
          AND d.status='sent'
          AND d.feishu_message_id<>''
          AND d.sent_at IS NOT NULL
   );

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles
         WHERE rolname='vane_push_batch_authority'
    ) THEN
        BEGIN
            CREATE ROLE vane_push_batch_authority
                NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
                NOLOGIN NOINHERIT NOBYPASSRLS;
        EXCEPTION
            WHEN duplicate_object OR unique_violation THEN NULL;
        END;
    END IF;
END $$;
-- +goose StatementEnd

ALTER ROLE vane_push_batch_authority
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    NOLOGIN NOINHERIT NOBYPASSRLS;
GRANT vane_push_batch_authority TO CURRENT_USER;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles granted_role ON granted_role.oid=am.roleid
          JOIN pg_roles member_role ON member_role.oid=am.member
         WHERE granted_role.rolname='vane_push_batch_authority'
           AND member_role.rolname<>CURRENT_USER
    ) OR EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles member_role ON member_role.oid=am.member
         WHERE member_role.rolname='vane_push_batch_authority'
    ) THEN
        RAISE EXCEPTION
            '048: push batch authority role has an unsafe membership graph';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE ALL ON push_batches FROM vane_push_batch_authority;
GRANT USAGE ON SCHEMA public TO vane_push_batch_authority;
GRANT SELECT (
    id, tenant_id, user_id, delivery_authority
) ON push_batches TO vane_push_batch_authority;
GRANT UPDATE (
    delivery_authority
) ON push_batches TO vane_push_batch_authority;

-- Effect creation validates the batch winner without gaining authority to
-- change it.
GRANT SELECT (
    delivery_authority
) ON push_batches TO vane_push_effect_coordinator;

-- The durable sent receipt closes an optional observed event in the same
-- transaction as its frozen effect and delivery aggregate. It can inspect only
-- the exact event claim and mutate only the two terminal receipt fields.
GRANT SELECT (
    event_key, tenant_id, user_id, task_id, run_snapshot_id, temporal_run_id,
    delivery_id, status, delivered_at
) ON task_observed_events TO vane_push_effect_receipt;
GRANT SELECT (
    canonical_payload, payload_digest, chunk_index, chunk_count
) ON push_effects TO vane_push_effect_receipt;
GRANT SELECT (
    id, tenant_id, user_id, status, run_snapshot_id, delivery_authority
) ON push_batches TO vane_push_effect_receipt;
GRANT UPDATE (
    status
) ON push_batches TO vane_push_effect_receipt;
GRANT UPDATE (
    status, delivered_at
) ON task_observed_events TO vane_push_effect_receipt;

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

CREATE POLICY push_effect_receipt_batch_visible ON push_batches
    FOR ALL TO vane_push_effect_receipt
    USING (true) WITH CHECK (true);
CREATE POLICY push_effect_receipt_batch_tenant_isolation ON push_batches
    AS RESTRICTIVE FOR ALL TO vane_push_effect_receipt
    USING (tenant_id IS NOT DISTINCT FROM
        (SELECT current_setting('app.tenant_id', true))::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
        (SELECT current_setting('app.tenant_id', true))::bigint);

-- Restricted effect/receipt/application roles cannot be granted arbitrary
-- UPDATE on push_batches merely to take a row lock. This exact SECURITY
-- DEFINER fence validates the transaction-local tenant and immutable batch
-- scope, then returns the authority row while retaining FOR UPDATE until the
-- caller commits.
-- +goose StatementBegin
CREATE FUNCTION lock_push_effect_batch_v1(
    requested_batch_id BIGINT,
    requested_tenant_id BIGINT,
    requested_user_id BIGINT,
    requested_run_snapshot_id BIGINT,
    requested_authority TEXT
)
RETURNS TABLE(batch_status TEXT, batch_authority TEXT)
LANGUAGE sql
SECURITY DEFINER
SET search_path=pg_catalog,public
AS $$
    SELECT b.status,COALESCE(b.delivery_authority,'')
      FROM public.push_batches b
     WHERE b.id=requested_batch_id
       AND b.tenant_id=requested_tenant_id
       AND b.user_id=requested_user_id
       AND b.run_snapshot_id=requested_run_snapshot_id
       AND b.tenant_id IS NOT DISTINCT FROM
           NULLIF(pg_catalog.current_setting('app.tenant_id',true),'')::bigint
       AND (
           requested_authority IN ('','*')
           OR b.delivery_authority=requested_authority
       )
     FOR UPDATE OF b
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION lock_push_effect_batch_v1(
    BIGINT,BIGINT,BIGINT,BIGINT,TEXT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lock_push_effect_batch_v1(
    BIGINT,BIGINT,BIGINT,BIGINT,TEXT
) TO vane_app,vane_push_effect_coordinator,vane_push_effect_receipt;

-- Creation must freeze delivery rows before proving the optional observed
-- event binding, but the coordinator must not gain arbitrary UPDATE rights on
-- either table merely to use FOR UPDATE.
-- +goose StatementBegin
CREATE FUNCTION lock_push_effect_aggregate_v1(
    requested_tenant_id BIGINT,
    requested_user_id BIGINT,
    requested_batch_id BIGINT,
    requested_delivery_ids BIGINT[]
)
RETURNS TABLE(
    locked_delivery_ids BIGINT[],
    observed_delivery_ids BIGINT[],
    observed_event_keys TEXT[]
)
LANGUAGE sql
SECURITY DEFINER
SET search_path=pg_catalog,public
AS $$
    WITH locked_deliveries AS MATERIALIZED (
        SELECT d.id
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
        SELECT e.delivery_id,e.event_key
          FROM public.task_observed_events e
          JOIN locked_deliveries d ON d.id=e.delivery_id
         WHERE e.tenant_id=requested_tenant_id
           AND e.user_id=requested_user_id
         ORDER BY e.delivery_id,e.event_key
         FOR UPDATE OF e
    )
    SELECT
        ARRAY(SELECT id FROM locked_deliveries ORDER BY id),
        ARRAY(SELECT delivery_id FROM locked_events ORDER BY delivery_id,event_key),
        ARRAY(SELECT event_key FROM locked_events ORDER BY delivery_id,event_key)
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION lock_push_effect_aggregate_v1(
    BIGINT,BIGINT,BIGINT,BIGINT[]
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lock_push_effect_aggregate_v1(
    BIGINT,BIGINT,BIGINT,BIGINT[]
) TO vane_push_effect_coordinator;

-- Observation admission runs as vane_app, which deliberately has no direct
-- push_effects privilege. This exact fence exposes only the batch IDs needed
-- to reject stale event transfer once any durable provider effect exists.
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

LOCK TABLE push_effects, push_batches IN ACCESS EXCLUSIVE MODE
    /* migration 048 downgrade fence */;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM push_batches
         WHERE delivery_authority IS NOT NULL
    ) OR EXISTS (SELECT 1 FROM push_effects) THEN
        RAISE EXCEPTION
            '048: refusing downgrade while durable batch authority exists';
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
REVOKE EXECUTE ON FUNCTION lock_push_effect_batch_v1(
    BIGINT,BIGINT,BIGINT,BIGINT,TEXT
) FROM vane_app,vane_push_effect_coordinator,vane_push_effect_receipt;
DROP FUNCTION lock_push_effect_batch_v1(
    BIGINT,BIGINT,BIGINT,BIGINT,TEXT
);

DROP POLICY push_effect_receipt_batch_tenant_isolation ON push_batches;
DROP POLICY push_effect_receipt_batch_visible ON push_batches;
DROP POLICY push_effect_receipt_tenant_isolation ON task_observed_events;
DROP POLICY push_effect_receipt_update_visible ON task_observed_events;
DROP POLICY push_effect_receipt_select_visible ON task_observed_events;

REVOKE UPDATE (
    status, delivered_at
) ON task_observed_events FROM vane_push_effect_receipt;
REVOKE SELECT (
    event_key, tenant_id, user_id, task_id, run_snapshot_id, temporal_run_id,
    delivery_id, status, delivered_at
) ON task_observed_events FROM vane_push_effect_receipt;
REVOKE SELECT (
    canonical_payload, chunk_index, chunk_count
) ON push_effects FROM vane_push_effect_receipt;
REVOKE UPDATE (
    status
) ON push_batches FROM vane_push_effect_receipt;
REVOKE SELECT (
    id, tenant_id, user_id, status, run_snapshot_id, delivery_authority
) ON push_batches FROM vane_push_effect_receipt;
REVOKE SELECT (
    delivery_authority
) ON push_batches FROM vane_push_effect_coordinator;
REVOKE UPDATE (
    delivery_authority
) ON push_batches FROM vane_push_batch_authority;
REVOKE SELECT (
    id, tenant_id, user_id, delivery_authority
) ON push_batches FROM vane_push_batch_authority;
REVOKE USAGE ON SCHEMA public FROM vane_push_batch_authority;
REVOKE vane_push_batch_authority FROM CURRENT_USER;

ALTER TABLE push_batches
    DROP CONSTRAINT push_batches_delivery_authority_valid,
    DROP COLUMN delivery_authority;
