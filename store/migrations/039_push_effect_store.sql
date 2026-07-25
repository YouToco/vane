-- 039: durable external push effect/checkpoint substrate.
--
-- This migration is deliberately dark: no workflow, provider, or server
-- package calls it. Immutable identity and card bytes are frozen before a
-- future sender crosses the Feishu boundary. A stale `sending` attempt can
-- only converge to `ambiguous`; it is never silently re-authorized to send.

-- +goose Up

-- Roles are cluster-wide. Reassert every security attribute and reject role
-- graph pivots; only the migration/session owner may SET LOCAL ROLE.
-- +goose StatementBegin
DO $$
DECLARE
    role_name TEXT;
BEGIN
    FOREACH role_name IN ARRAY ARRAY[
        'vane_push_effect_coordinator',
        'vane_push_effect_receipt',
        'vane_push_effect_operator'
    ] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            BEGIN
                EXECUTE format(
                    'CREATE ROLE %I NOSUPERUSER NOCREATEDB NOCREATEROLE ' ||
                    'NOREPLICATION NOLOGIN NOINHERIT NOBYPASSRLS',
                    role_name
                );
            EXCEPTION
                WHEN duplicate_object OR unique_violation THEN NULL;
            END;
        END IF;
    END LOOP;
END $$;
-- +goose StatementEnd

ALTER ROLE vane_push_effect_coordinator
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    NOLOGIN NOINHERIT NOBYPASSRLS;
ALTER ROLE vane_push_effect_receipt
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    NOLOGIN NOINHERIT NOBYPASSRLS;
ALTER ROLE vane_push_effect_operator
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    NOLOGIN NOINHERIT NOBYPASSRLS;

GRANT vane_push_effect_coordinator, vane_push_effect_receipt,
      vane_push_effect_operator TO CURRENT_USER;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles granted_role ON granted_role.oid = am.roleid
          JOIN pg_roles member_role ON member_role.oid = am.member
         WHERE granted_role.rolname IN (
                   'vane_push_effect_coordinator',
                   'vane_push_effect_receipt',
                   'vane_push_effect_operator'
               )
           AND member_role.rolname <> CURRENT_USER
    ) OR EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles member_role ON member_role.oid = am.member
         WHERE member_role.rolname IN (
                   'vane_push_effect_coordinator',
                   'vane_push_effect_receipt',
                   'vane_push_effect_operator'
               )
    ) THEN
        RAISE EXCEPTION '039: push effect roles have an unsafe membership graph';
    END IF;
END $$;
-- +goose StatementEnd

CREATE TABLE push_effects (
    id                    TEXT        PRIMARY KEY,
    tenant_id             BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    user_id               BIGINT      NOT NULL REFERENCES users (id),
    task_id               TEXT        NOT NULL,
    run_snapshot_id       BIGINT      NOT NULL REFERENCES task_run_snapshots (id),
    run_id                TEXT        NOT NULL,
    step_id               TEXT        NOT NULL,
    chunk_index           INTEGER     NOT NULL,
    chunk_count           INTEGER     NOT NULL,
    batch_id              BIGINT      NOT NULL REFERENCES push_batches (id),
    delivery_ids          BIGINT[]    NOT NULL,
    provider              TEXT        NOT NULL,
    app_identity          TEXT        NOT NULL,
    provider_chat_id      TEXT        NOT NULL,
    target                TEXT        NOT NULL,
    card_payload          BYTEA       NOT NULL,
    card_digest           TEXT        NOT NULL,
    provider_uuid         UUID        NOT NULL,
    idempotency_expires_at TIMESTAMPTZ NOT NULL,
    schema_version        TEXT        NOT NULL,
    canonical_payload     BYTEA       NOT NULL,
    payload_digest        TEXT        NOT NULL,

    status                TEXT        NOT NULL DEFAULT 'prepared',
    lease_owner           TEXT        NOT NULL DEFAULT '',
    lease_until           TIMESTAMPTZ,
    takeover_not_before   TIMESTAMPTZ,
    fence                 BIGINT      NOT NULL DEFAULT 0,
    attempt               INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    provider_message_id   TEXT        NOT NULL DEFAULT '',
    failure_class         TEXT        NOT NULL DEFAULT '',
    ambiguous_since       TIMESTAMPTZ,
    sent_at               TIMESTAMPTZ,
    blocked_at            TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_push_effect_provider_uuid UNIQUE (provider, app_identity, provider_uuid),
    CONSTRAINT uq_push_effect_run_step_chunk
        UNIQUE (tenant_id, user_id, task_id, run_snapshot_id, run_id, step_id, chunk_index),
    CONSTRAINT push_effect_identity_valid CHECK (
        id <> '' AND btrim(id) = id AND octet_length(id) <= 512 AND
        task_id <> '' AND btrim(task_id) = task_id AND octet_length(task_id) <= 512 AND
        run_id <> '' AND btrim(run_id) = run_id AND octet_length(run_id) <= 512 AND
        step_id <> '' AND btrim(step_id) = step_id AND octet_length(step_id) <= 512 AND
        provider <> '' AND btrim(provider) = provider AND octet_length(provider) <= 512 AND
        app_identity <> '' AND btrim(app_identity) = app_identity AND
        octet_length(app_identity) <= 512 AND
        provider_chat_id <> '' AND btrim(provider_chat_id) = provider_chat_id AND
        octet_length(provider_chat_id) <= 512 AND
        target <> '' AND btrim(target) = target AND octet_length(target) <= 1024
    ),
    CONSTRAINT push_effect_idempotency_window_valid CHECK (
        idempotency_expires_at > created_at AND
        idempotency_expires_at <= created_at + interval '1 hour'
    ),
    CONSTRAINT push_effect_chunk_valid
        CHECK (chunk_count > 0 AND chunk_index >= 0 AND chunk_index < chunk_count),
    CONSTRAINT push_effect_delivery_ids_valid CHECK (
        cardinality(delivery_ids) BETWEEN 1 AND 256 AND
        array_position(delivery_ids, NULL) IS NULL
    ),
    CONSTRAINT push_effect_card_valid CHECK (
        octet_length(card_payload) BETWEEN 1 AND 2097152 AND
        card_digest ~ '^[0-9a-f]{64}$' AND
        card_digest = encode(sha256(card_payload), 'hex')
    ),
    CONSTRAINT push_effect_payload_valid CHECK (
        schema_version = 'vane.push-effect/v1' AND
        octet_length(canonical_payload) BETWEEN 1 AND 3145728 AND
        payload_digest ~ '^[0-9a-f]{64}$' AND
        payload_digest = encode(sha256(canonical_payload), 'hex')
    ),
    CONSTRAINT push_effect_status_valid CHECK (
        status IN (
            'prepared', 'sending', 'ambiguous', 'sent',
            'definite_failed', 'blocked'
        )
    ),
    CONSTRAINT push_effect_fence_attempt_valid CHECK (fence >= 0 AND attempt >= 0),
    CONSTRAINT push_effect_lease_complete CHECK (
        (lease_owner = '' AND lease_until IS NULL AND takeover_not_before IS NULL) OR
        (lease_owner <> '' AND lease_until IS NOT NULL AND
         takeover_not_before IS NOT NULL AND takeover_not_before >= lease_until)
    ),
    CONSTRAINT push_effect_state_valid CHECK (
        (status = 'prepared' AND lease_owner = '' AND fence = 0 AND attempt = 0 AND
         failure_class = '' AND ambiguous_since IS NULL AND sent_at IS NULL AND
         blocked_at IS NULL AND provider_message_id = '') OR
        (status = 'sending' AND lease_owner <> '' AND fence > 0 AND attempt > 0 AND
         failure_class = '' AND ambiguous_since IS NULL AND sent_at IS NULL AND
         blocked_at IS NULL AND provider_message_id = '') OR
        (status = 'definite_failed' AND lease_owner = '' AND fence > 0 AND attempt > 0 AND
         failure_class <> '' AND ambiguous_since IS NULL AND sent_at IS NULL AND
         blocked_at IS NULL AND provider_message_id = '') OR
        (status = 'ambiguous' AND lease_owner = '' AND fence > 0 AND attempt > 0 AND
         failure_class <> '' AND ambiguous_since IS NOT NULL AND sent_at IS NULL AND
         blocked_at IS NULL AND provider_message_id = '') OR
        (status = 'sent' AND lease_owner = '' AND fence > 0 AND attempt > 0 AND
         failure_class = '' AND ambiguous_since IS NULL AND sent_at IS NOT NULL AND
         blocked_at IS NULL AND provider_message_id <> '') OR
        (status = 'blocked' AND lease_owner = '' AND fence > 0 AND attempt > 0 AND
         failure_class <> '' AND sent_at IS NULL AND blocked_at IS NOT NULL AND
         provider_message_id = '')
    )
);

CREATE INDEX idx_push_effects_due
    ON push_effects (tenant_id, next_attempt_at, id)
    WHERE status IN ('prepared', 'definite_failed');
CREATE INDEX idx_push_effects_stale_sending
    ON push_effects (tenant_id, takeover_not_before, id)
    WHERE status = 'sending';

ALTER TABLE push_effects ENABLE ROW LEVEL SECURITY;
CREATE POLICY push_effect_existing_visibility ON push_effects
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY push_effect_tenant_isolation ON push_effects AS RESTRICTIVE
    FOR ALL TO vane_push_effect_coordinator, vane_push_effect_receipt,
               vane_push_effect_operator
    USING (tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint);

REVOKE ALL ON push_effects FROM PUBLIC, vane_app;
GRANT USAGE ON SCHEMA public TO vane_push_effect_coordinator,
    vane_push_effect_receipt, vane_push_effect_operator;

-- Exact aggregate provenance is checked before insert. These read grants are
-- tenant-scoped by the existing restrictive RLS policies on all three tables.
GRANT SELECT (
    id, tenant_id, user_id, task_id, temporal_run_id
) ON task_run_snapshots TO vane_push_effect_coordinator;
GRANT SELECT (
    id, tenant_id, user_id, run_snapshot_id
) ON push_batches TO vane_push_effect_coordinator;
GRANT SELECT (
    id, tenant_id, user_id, batch_id
) ON deliveries TO vane_push_effect_coordinator;
GRANT SELECT (
    id
) ON tenants TO vane_push_effect_coordinator;

GRANT SELECT ON push_effects TO vane_push_effect_coordinator;
GRANT INSERT (
    id, tenant_id, user_id, task_id, run_snapshot_id, run_id, step_id,
    chunk_index, chunk_count, batch_id, delivery_ids, provider, app_identity,
    provider_chat_id, target, card_payload, card_digest, provider_uuid,
    idempotency_expires_at, schema_version,
    canonical_payload, payload_digest
) ON push_effects TO vane_push_effect_coordinator;
GRANT UPDATE (
    status, lease_owner, lease_until, takeover_not_before, fence, attempt,
    next_attempt_at, failure_class, ambiguous_since, updated_at
) ON push_effects TO vane_push_effect_coordinator;

GRANT SELECT (
    id, tenant_id, user_id, status, lease_owner, lease_until, fence,
    provider, app_identity, provider_chat_id, provider_uuid,
    idempotency_expires_at, payload_digest,
    provider_message_id, failure_class, ambiguous_since, sent_at
) ON push_effects TO vane_push_effect_receipt;
GRANT UPDATE (
    status, lease_owner, lease_until, takeover_not_before,
    provider_message_id, failure_class, ambiguous_since, sent_at, updated_at
) ON push_effects TO vane_push_effect_receipt;

GRANT SELECT (
    id, tenant_id, user_id, status, fence, provider, app_identity,
    provider_chat_id, provider_uuid, idempotency_expires_at,
    payload_digest, provider_message_id, failure_class,
    ambiguous_since, sent_at, blocked_at
) ON push_effects TO vane_push_effect_operator;
GRANT UPDATE (
    status, failure_class, ambiguous_since, blocked_at, updated_at
) ON push_effects TO vane_push_effect_operator;

-- +goose Down

LOCK TABLE push_effects IN ACCESS EXCLUSIVE MODE
    /* migration 039 downgrade fence */;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM push_effects) THEN
        RAISE EXCEPTION '039: refusing downgrade while durable push effects exist';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE UPDATE (
    status, failure_class, ambiguous_since, blocked_at, updated_at
) ON push_effects FROM vane_push_effect_operator;
REVOKE SELECT (
    id, tenant_id, user_id, status, fence, provider, app_identity,
    provider_chat_id, provider_uuid, idempotency_expires_at,
    payload_digest, provider_message_id, failure_class,
    ambiguous_since, sent_at, blocked_at
) ON push_effects FROM vane_push_effect_operator;

REVOKE UPDATE (
    status, lease_owner, lease_until, takeover_not_before,
    provider_message_id, failure_class, ambiguous_since, sent_at, updated_at
) ON push_effects FROM vane_push_effect_receipt;
REVOKE SELECT (
    id, tenant_id, user_id, status, lease_owner, lease_until, fence,
    provider, app_identity, provider_chat_id, provider_uuid,
    idempotency_expires_at, payload_digest,
    provider_message_id, failure_class, ambiguous_since, sent_at
) ON push_effects FROM vane_push_effect_receipt;

REVOKE UPDATE (
    status, lease_owner, lease_until, takeover_not_before, fence, attempt,
    next_attempt_at, failure_class, ambiguous_since, updated_at
) ON push_effects FROM vane_push_effect_coordinator;
REVOKE INSERT (
    id, tenant_id, user_id, task_id, run_snapshot_id, run_id, step_id,
    chunk_index, chunk_count, batch_id, delivery_ids, provider, app_identity,
    provider_chat_id, target, card_payload, card_digest, provider_uuid,
    idempotency_expires_at, schema_version,
    canonical_payload, payload_digest
) ON push_effects FROM vane_push_effect_coordinator;
REVOKE SELECT ON push_effects FROM vane_push_effect_coordinator;
REVOKE SELECT (
    id, tenant_id, user_id, batch_id
) ON deliveries FROM vane_push_effect_coordinator;
REVOKE SELECT (
    id
) ON tenants FROM vane_push_effect_coordinator;
REVOKE SELECT (
    id, tenant_id, user_id, run_snapshot_id
) ON push_batches FROM vane_push_effect_coordinator;
REVOKE SELECT (
    id, tenant_id, user_id, task_id, temporal_run_id
) ON task_run_snapshots FROM vane_push_effect_coordinator;
REVOKE USAGE ON SCHEMA public FROM vane_push_effect_coordinator,
    vane_push_effect_receipt, vane_push_effect_operator;

DROP POLICY IF EXISTS push_effect_tenant_isolation ON push_effects;
DROP POLICY IF EXISTS push_effect_existing_visibility ON push_effects;
ALTER TABLE push_effects DISABLE ROW LEVEL SECURITY;
DROP TABLE push_effects;
