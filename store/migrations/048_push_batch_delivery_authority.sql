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
    canonical_payload, payload_digest
) ON push_effects TO vane_push_effect_receipt;
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
    canonical_payload, payload_digest
) ON push_effects FROM vane_push_effect_receipt;
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
