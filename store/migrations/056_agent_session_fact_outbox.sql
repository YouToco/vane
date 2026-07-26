-- 056: Durable business-fact to exact Agent-session continuation outbox.
--
-- Producers freeze the session (or the absence of one) in the same
-- transaction as the feedback fact. Consumers never rediscover a live
-- session at runtime.

-- +goose Up

CREATE TABLE agent_session_fact_outbox (
    id                   BIGSERIAL   PRIMARY KEY,
    tenant_id            BIGINT      NOT NULL,
    user_id              BIGINT      NOT NULL,
    fact_type            TEXT        NOT NULL,
    fact_id              BIGINT      NOT NULL,
    source_identity      TEXT        NOT NULL,
    session_id           BIGINT,
    session_messages     BYTEA,
    payload_digest       TEXT,
    status               TEXT        NOT NULL,
    suppression_reason   TEXT,
    lease_owner          TEXT,
    lease_fence          BIGINT      NOT NULL DEFAULT 0,
    lease_expires_at     TIMESTAMPTZ,
    attempt_count        INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    session_recorded_at  TIMESTAMPTZ,
    blocked_reason       TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_agent_session_fact_outbox_fact
        UNIQUE (tenant_id, fact_type, fact_id),
    CONSTRAINT uq_agent_session_fact_outbox_identity
        UNIQUE (tenant_id, user_id, source_identity),
    CONSTRAINT fk_agent_session_fact_outbox_user
        FOREIGN KEY (user_id)
        REFERENCES users (id)
        ON DELETE CASCADE,
    CONSTRAINT fk_agent_session_fact_outbox_session
        FOREIGN KEY (session_id, tenant_id, user_id)
        REFERENCES agent_sessions (id, tenant_id, user_id),
    CONSTRAINT agent_session_fact_outbox_fact_valid
        CHECK (fact_type = 'feedback' AND fact_id > 0),
    CONSTRAINT agent_session_fact_outbox_identity_valid
        CHECK (
            source_identity =
                'feedback-click:' || fact_id::text
            AND octet_length(source_identity) <= 255
        ),
    CONSTRAINT agent_session_fact_outbox_status_valid
        CHECK (status IN ('pending', 'completed', 'suppressed', 'blocked')),
    CONSTRAINT agent_session_fact_outbox_fence_valid
        CHECK (lease_fence >= 0 AND attempt_count >= 0),
    CONSTRAINT agent_session_fact_outbox_payload_valid
        CHECK (
            (
                status IN ('pending', 'completed', 'blocked')
                AND session_id IS NOT NULL
                AND session_messages IS NOT NULL
                AND octet_length(session_messages) BETWEEN 2 AND 16384
                AND payload_digest ~ '^[0-9a-f]{64}$'
                AND suppression_reason IS NULL
            )
            OR
            (
                status = 'suppressed'
                AND session_id IS NULL
                AND session_messages IS NULL
                AND payload_digest IS NULL
                AND suppression_reason = 'no_active_session'
                AND session_recorded_at IS NULL
                AND blocked_reason IS NULL
            )
        ),
    CONSTRAINT agent_session_fact_outbox_terminal_valid
        CHECK (
            (status = 'completed' AND session_recorded_at IS NOT NULL
                AND blocked_reason IS NULL)
            OR
            (status = 'blocked' AND session_recorded_at IS NULL
                AND blocked_reason IS NOT NULL)
            OR
            (status IN ('pending', 'suppressed')
                AND session_recorded_at IS NULL
                AND blocked_reason IS NULL)
        ),
    CONSTRAINT agent_session_fact_outbox_lease_valid
        CHECK (
            (lease_owner IS NULL AND lease_expires_at IS NULL)
            OR
            (
                status = 'pending'
                AND lease_owner IS NOT NULL
                AND btrim(lease_owner) = lease_owner
                AND lease_owner <> ''
                AND octet_length(lease_owner) <= 255
                AND lease_expires_at IS NOT NULL
            )
        )
);

CREATE INDEX idx_agent_session_fact_outbox_due_tenant
    ON agent_session_fact_outbox
       (tenant_id, next_attempt_at, id)
    WHERE status = 'pending';

ALTER TABLE agent_session_fact_outbox ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible
    ON agent_session_fact_outbox
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation
    ON agent_session_fact_outbox AS RESTRICTIVE
    FOR ALL
    USING (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint
    )
    WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint
    );

-- Feedback producers may only create their immutable fact checkpoint.
GRANT INSERT (
    tenant_id, user_id, fact_type, fact_id, source_identity, session_id,
    session_messages, payload_digest, status, suppression_reason
) ON agent_session_fact_outbox TO vane_app;
GRANT USAGE ON SEQUENCE agent_session_fact_outbox_id_seq TO vane_app;

-- The projector identity cannot log in or inherit ambient privileges. Only
-- the migration owner may enter it from the exact Store transaction.
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles
         WHERE rolname = 'vane_agent_session_fact_projector'
    ) THEN
        BEGIN
            CREATE ROLE vane_agent_session_fact_projector
                NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
                NOLOGIN NOINHERIT NOBYPASSRLS;
        EXCEPTION
            WHEN duplicate_object OR unique_violation THEN NULL;
        END;
    END IF;
END $$;
-- +goose StatementEnd

ALTER ROLE vane_agent_session_fact_projector
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    NOLOGIN NOINHERIT NOBYPASSRLS;
ALTER ROLE vane_agent_session_fact_projector RESET ALL;
ALTER ROLE vane_agent_session_fact_projector
    SET search_path = pg_catalog, public;
GRANT vane_agent_session_fact_projector TO CURRENT_USER;

-- +goose StatementBegin
DO $$
BEGIN
    IF pg_has_role(
           'vane_agent_session_fact_projector', 'vane_app', 'MEMBER'
       ) OR
       pg_has_role(
           'vane_app', 'vane_agent_session_fact_projector', 'MEMBER'
       ) THEN
        RAISE EXCEPTION
            '056: vane_app and continuation projector must be unrelated';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles granted_role ON granted_role.oid = am.roleid
          JOIN pg_roles member_role ON member_role.oid = am.member
         WHERE granted_role.rolname =
                   'vane_agent_session_fact_projector'
           AND member_role.rolname <> CURRENT_USER
    ) THEN
        RAISE EXCEPTION
            '056: only migration owner may enter continuation projector';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles member_role ON member_role.oid = am.member
         WHERE member_role.rolname =
                   'vane_agent_session_fact_projector'
    ) THEN
        RAISE EXCEPTION
            '056: continuation projector must not enter another role';
    END IF;
END $$;
-- +goose StatementEnd

GRANT USAGE ON SCHEMA public TO vane_agent_session_fact_projector;
GRANT SELECT (
    id,tenant_id,user_id,messages,turn_count,activated_tools
) ON agent_sessions TO vane_agent_session_fact_projector;
GRANT UPDATE (messages)
    ON agent_sessions TO vane_agent_session_fact_projector;
GRANT SELECT ON agent_events TO vane_agent_session_fact_projector;
GRANT INSERT (
    tenant_id,user_id,session_id,sequence,batch_idempotency_key,
    batch_index,batch_size,kind,schema_version,payload,payload_digest,
    batch_digest
) ON agent_events TO vane_agent_session_fact_projector;
GRANT USAGE ON SEQUENCE agent_events_id_seq
    TO vane_agent_session_fact_projector;
GRANT SELECT ON agent_session_projection_authority_events
    TO vane_agent_session_fact_projector;
GRANT SELECT ON agent_session_fact_outbox
    TO vane_agent_session_fact_projector;
GRANT UPDATE (
    status, lease_owner, lease_fence, lease_expires_at, attempt_count,
    next_attempt_at, session_recorded_at, blocked_reason, updated_at
) ON agent_session_fact_outbox TO vane_agent_session_fact_projector;

-- +goose Down

LOCK TABLE agent_session_fact_outbox IN ACCESS EXCLUSIVE MODE
    /* migration 056 downgrade fence: projector/purge lock order first */;
LOCK TABLE agent_sessions IN ACCESS EXCLUSIVE MODE
    /* migration 056 downgrade fence: session root second */;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM agent_session_fact_outbox) THEN
        RAISE EXCEPTION
            '056: refusing downgrade while Agent continuation facts exist';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE UPDATE (
    status, lease_owner, lease_fence, lease_expires_at, attempt_count,
    next_attempt_at, session_recorded_at, blocked_reason, updated_at
) ON agent_session_fact_outbox FROM vane_agent_session_fact_projector;
REVOKE SELECT ON agent_session_fact_outbox
    FROM vane_agent_session_fact_projector;
REVOKE SELECT ON agent_session_projection_authority_events
    FROM vane_agent_session_fact_projector;
REVOKE USAGE ON SEQUENCE agent_events_id_seq
    FROM vane_agent_session_fact_projector;
REVOKE INSERT (
    tenant_id,user_id,session_id,sequence,batch_idempotency_key,
    batch_index,batch_size,kind,schema_version,payload,payload_digest,
    batch_digest
) ON agent_events FROM vane_agent_session_fact_projector;
REVOKE SELECT ON agent_events FROM vane_agent_session_fact_projector;
REVOKE UPDATE (messages)
    ON agent_sessions FROM vane_agent_session_fact_projector;
REVOKE SELECT (
    id,tenant_id,user_id,messages,turn_count,activated_tools
) ON agent_sessions FROM vane_agent_session_fact_projector;
REVOKE USAGE ON SCHEMA public FROM vane_agent_session_fact_projector;
REVOKE USAGE ON SEQUENCE agent_session_fact_outbox_id_seq FROM vane_app;
REVOKE INSERT (
    tenant_id, user_id, fact_type, fact_id, source_identity, session_id,
    session_messages, payload_digest, status, suppression_reason
) ON agent_session_fact_outbox FROM vane_app;

DROP POLICY IF EXISTS tenant_isolation ON agent_session_fact_outbox;
DROP POLICY IF EXISTS tenant_visible ON agent_session_fact_outbox;
ALTER TABLE agent_session_fact_outbox DISABLE ROW LEVEL SECURITY;
DROP TABLE agent_session_fact_outbox;
