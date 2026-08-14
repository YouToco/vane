-- 053: Append-only, exact-session authority for the Agent ledger projector.
--
-- No row means the retained agent_sessions JSONB projection remains
-- authoritative. Every activate/rollback transition records the exact,
-- matching legacy/ledger digest and the fully validated ledger head.

-- +goose Up

CREATE TABLE agent_session_projection_authority_events (
    id                   BIGSERIAL   PRIMARY KEY,
    tenant_id            BIGINT      NOT NULL,
    user_id              BIGINT      NOT NULL,
    session_id           BIGINT      NOT NULL,
    generation           BIGINT      NOT NULL,
    action               TEXT        NOT NULL,
    ledger_head_sequence BIGINT      NOT NULL,
    legacy_digest        TEXT        NOT NULL,
    ledger_digest        TEXT        NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT fk_agent_session_projection_authority_scope
        FOREIGN KEY (session_id, tenant_id, user_id)
        REFERENCES agent_sessions (id, tenant_id, user_id)
        ON DELETE CASCADE,
    CONSTRAINT uq_agent_session_projection_authority_generation
        UNIQUE (tenant_id, user_id, session_id, generation),
    CONSTRAINT agent_session_projection_authority_generation_positive
        CHECK (generation > 0),
    CONSTRAINT agent_session_projection_authority_action_valid
        CHECK (action IN ('activate', 'rollback')),
    CONSTRAINT agent_session_projection_authority_head_positive
        CHECK (ledger_head_sequence > 0),
    CONSTRAINT agent_session_projection_authority_legacy_digest_valid
        CHECK (legacy_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT agent_session_projection_authority_ledger_digest_valid
        CHECK (ledger_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT agent_session_projection_authority_digests_match
        CHECK (legacy_digest = ledger_digest)
);

CREATE INDEX idx_agent_session_projection_authority_current
    ON agent_session_projection_authority_events
       (tenant_id, user_id, session_id, generation DESC);

ALTER TABLE agent_session_projection_authority_events
    ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible
    ON agent_session_projection_authority_events
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation
    ON agent_session_projection_authority_events AS RESTRICTIVE
    FOR ALL
    USING (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint
    )
    WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint
    );

-- Runtime and the restricted definition-receipt writer may only resolve the
-- current route. They cannot create, rewrite, or remove authority history.
GRANT SELECT ON agent_session_projection_authority_events TO vane_app;
GRANT SELECT ON agent_session_projection_authority_events
    TO vane_edit_receipt;

-- The operator cannot log in or inherit ambient privileges. The migration
-- owner may SET ROLE only inside the exact Store control transaction.
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles
         WHERE rolname = 'vane_agent_session_projection_operator'
    ) THEN
        BEGIN
            CREATE ROLE vane_agent_session_projection_operator
                NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
                NOLOGIN NOINHERIT NOBYPASSRLS;
        EXCEPTION
            WHEN duplicate_object OR unique_violation THEN NULL;
        END;
    END IF;
END $$;
-- +goose StatementEnd

ALTER ROLE vane_agent_session_projection_operator
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    NOLOGIN NOINHERIT NOBYPASSRLS;
ALTER ROLE vane_agent_session_projection_operator RESET ALL;
ALTER ROLE vane_agent_session_projection_operator
    SET search_path = pg_catalog, public;
GRANT vane_agent_session_projection_operator TO CURRENT_USER;

-- Normalize the cluster-wide identity before granting any database-local
-- payload capability, then prove that no other principal can enter it and it
-- cannot enter any ambient role. A pre-existing unsafe membership aborts Up.
-- +goose StatementBegin
DO $$
BEGIN
    IF pg_has_role(
           'vane_agent_session_projection_operator', 'vane_app', 'MEMBER'
       ) OR
       pg_has_role(
           'vane_app', 'vane_agent_session_projection_operator', 'MEMBER'
       ) THEN
        RAISE EXCEPTION
            '053: vane_app and Agent projection operator must be unrelated';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles granted_role ON granted_role.oid = am.roleid
          JOIN pg_roles member_role ON member_role.oid = am.member
         WHERE granted_role.rolname =
                   'vane_agent_session_projection_operator'
           AND member_role.rolname <> CURRENT_USER
    ) THEN
        RAISE EXCEPTION
            '053: only migration owner may enter Agent projection operator';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles member_role ON member_role.oid = am.member
         WHERE member_role.rolname =
                   'vane_agent_session_projection_operator'
    ) THEN
        RAISE EXCEPTION
            '053: Agent projection operator must not enter another role';
    END IF;
END $$;
-- +goose StatementEnd

GRANT USAGE ON SCHEMA public
    TO vane_agent_session_projection_operator;
GRANT SELECT (
    id, tenant_id, user_id, messages, turn_count, activated_tools
) ON agent_sessions TO vane_agent_session_projection_operator;
GRANT SELECT ON agent_events
    TO vane_agent_session_projection_operator;
GRANT SELECT, INSERT (
    tenant_id, user_id, session_id, generation, action,
    ledger_head_sequence, legacy_digest, ledger_digest
) ON agent_session_projection_authority_events
    TO vane_agent_session_projection_operator;
GRANT USAGE
    ON SEQUENCE agent_session_projection_authority_events_id_seq
    TO vane_agent_session_projection_operator;

-- +goose Down

-- Authority transitions are non-regenerable operator audit history. Take the
-- root before the child in runtime order and refuse every non-empty downgrade.
LOCK TABLE agent_sessions IN ACCESS EXCLUSIVE MODE
    /* migration 053 downgrade fence: session root first */;
LOCK TABLE agent_session_projection_authority_events
    IN ACCESS EXCLUSIVE MODE
    /* migration 053 downgrade fence: authority child second */;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM agent_session_projection_authority_events
    ) THEN
        RAISE EXCEPTION
            '053: refusing downgrade while Agent projection authority history exists';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE USAGE
    ON SEQUENCE agent_session_projection_authority_events_id_seq
    FROM vane_agent_session_projection_operator;
REVOKE SELECT, INSERT (
    tenant_id, user_id, session_id, generation, action,
    ledger_head_sequence, legacy_digest, ledger_digest
) ON agent_session_projection_authority_events
    FROM vane_agent_session_projection_operator;
REVOKE SELECT ON agent_events
    FROM vane_agent_session_projection_operator;
REVOKE SELECT (
    id, tenant_id, user_id, messages, turn_count, activated_tools
) ON agent_sessions FROM vane_agent_session_projection_operator;
REVOKE USAGE ON SCHEMA public
    FROM vane_agent_session_projection_operator;

-- The NOLOGIN identity and owner membership are cluster-wide and may still
-- serve another database in the same PostgreSQL cluster. Down removes every
-- database-local capability but deliberately preserves that shared identity.

REVOKE SELECT ON agent_session_projection_authority_events
    FROM vane_edit_receipt;
REVOKE SELECT ON agent_session_projection_authority_events FROM vane_app;
DROP POLICY IF EXISTS tenant_isolation
    ON agent_session_projection_authority_events;
DROP POLICY IF EXISTS tenant_visible
    ON agent_session_projection_authority_events;
ALTER TABLE agent_session_projection_authority_events
    DISABLE ROW LEVEL SECURITY;
DROP TABLE agent_session_projection_authority_events;
