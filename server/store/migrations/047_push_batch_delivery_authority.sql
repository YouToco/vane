-- 047: durable first-writer-wins provider protocol per push batch.
--
-- Deploy only after every pre-047 worker has drained. Once this migration is
-- applied, rolling a worker back before the compatibility fence is unsafe.
--
-- +goose Up

-- Stable ASCII "VANEPUSH". Every post-047 writer touching push effect/batch
-- protocol state takes the matching shared transaction lock first.
SELECT pg_advisory_xact_lock(6215335020355474248);

ALTER TABLE push_batches
    ADD COLUMN delivery_authority TEXT,
    ADD CONSTRAINT push_batches_delivery_authority_valid
        CHECK (delivery_authority IN ('legacy', 'effect'));

-- Effect evidence wins even if a legacy delivery receipt also exists.
UPDATE push_batches b
   SET delivery_authority='effect'
 WHERE EXISTS (
       SELECT 1 FROM push_effects e
        WHERE e.tenant_id=b.tenant_id
          AND e.user_id=b.user_id
          AND e.batch_id=b.id
   );

-- Exact legacy sent evidence freezes batches that never entered the effect
-- protocol. All other historical rows deliberately remain unclaimed.
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
            '047: push batch authority role has an unsafe membership graph';
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

-- Receipt/operator writers need only the immutable coordinates required to
-- enter the exact batch fence; 039's payload_digest and receipt columns remain
-- untouched.
GRANT SELECT (
    batch_id, run_snapshot_id
) ON push_effects TO vane_push_effect_receipt,vane_push_effect_operator;

-- Restricted writer roles cannot receive arbitrary push_batches UPDATE merely
-- to retain the exact batch row. The definer validates tenant, owner, immutable
-- snapshot and durable winner while holding FOR UPDATE until commit.
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
) TO vane_app,vane_push_effect_coordinator,vane_push_effect_receipt,
    vane_push_effect_operator;

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);

LOCK TABLE push_effects, push_batches IN ACCESS EXCLUSIVE MODE
    /* migration 047 downgrade fence */;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM push_batches
         WHERE delivery_authority IS NOT NULL
    ) OR EXISTS (SELECT 1 FROM push_effects) THEN
        RAISE EXCEPTION
            '047: refusing downgrade while durable batch authority exists';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE EXECUTE ON FUNCTION lock_push_effect_batch_v1(
    BIGINT,BIGINT,BIGINT,BIGINT,TEXT
) FROM vane_app,vane_push_effect_coordinator,vane_push_effect_receipt,
    vane_push_effect_operator;
DROP FUNCTION lock_push_effect_batch_v1(
    BIGINT,BIGINT,BIGINT,BIGINT,TEXT
);

REVOKE SELECT (
    batch_id, run_snapshot_id
) ON push_effects FROM vane_push_effect_receipt,vane_push_effect_operator;
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
