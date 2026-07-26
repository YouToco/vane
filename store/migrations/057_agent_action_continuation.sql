-- 057: exact-action durable terminal convergence for enable_source.
--
-- This is a dark, per-action lane. New durable candidates use
-- pending_actions.execution_version=2, so the historical v0 Claim can never
-- consume them. An append-only exact-action authority event must explicitly
-- activate one candidate before confirmation may admit it.

-- +goose Up

ALTER TABLE pending_actions
    ADD CONSTRAINT uq_pending_actions_exact_scope
    UNIQUE (id,tenant_id,user_id,session_id);

CREATE TABLE agent_action_continuations (
    action_id              TEXT        PRIMARY KEY,
    tenant_id              BIGINT      NOT NULL,
    user_id                BIGINT      NOT NULL,
    session_id             BIGINT      NOT NULL,
    tool_name              TEXT        NOT NULL,
    source_id              BIGINT      NOT NULL,
    canonical_args         BYTEA       NOT NULL,
    args_digest            TEXT        NOT NULL,
    tool_spec_version      TEXT        NOT NULL,
    tool_spec              BYTEA       NOT NULL,
    tool_spec_digest       TEXT        NOT NULL,
    tool_policy_version    TEXT        NOT NULL,
    tool_policy            BYTEA       NOT NULL,
    tool_policy_digest     TEXT        NOT NULL,
    adapter_version        TEXT        NOT NULL,
    success_messages       BYTEA       NOT NULL,
    success_digest         TEXT        NOT NULL,
    not_found_messages     BYTEA       NOT NULL,
    not_found_digest       TEXT        NOT NULL,
    status                 TEXT        NOT NULL DEFAULT 'pending',
    terminal_code          TEXT,
    lease_owner            TEXT,
    lease_fence            BIGINT      NOT NULL DEFAULT 0,
    lease_expires_at       TIMESTAMPTZ,
    attempt_count          INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    confirmed_at           TIMESTAMPTZ,
    completed_at           TIMESTAMPTZ,
    blocked_reason         TEXT,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_agent_action_continuation_scope
        UNIQUE (action_id,tenant_id,user_id,session_id),
    CONSTRAINT uq_agent_action_continuation_authority_scope
        UNIQUE (action_id,tenant_id,user_id),
    CONSTRAINT fk_agent_action_continuation_action
        FOREIGN KEY (action_id,tenant_id,user_id,session_id)
        REFERENCES pending_actions (id,tenant_id,user_id,session_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_agent_action_continuation_user
        FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
    CONSTRAINT fk_agent_action_continuation_session
        FOREIGN KEY (session_id, tenant_id, user_id)
        REFERENCES agent_sessions (id, tenant_id, user_id),
    CONSTRAINT agent_action_continuation_identity_valid CHECK (
        tool_name = 'enable_source'
        AND source_id > 0
        AND octet_length(action_id) BETWEEN 1 AND 255
    ),
    CONSTRAINT agent_action_continuation_versions_valid CHECK (
        tool_spec_version = 'vane.agent-tool-spec/v1'
        AND tool_policy_version = 'vane.agent-tool-policy/v1'
        AND adapter_version = 'vane.enable-source/postgres/v1'
    ),
    CONSTRAINT agent_action_continuation_payload_valid CHECK (
        octet_length(canonical_args) BETWEEN 2 AND 4096
        AND octet_length(tool_spec) BETWEEN 2 AND 16384
        AND octet_length(tool_policy) BETWEEN 2 AND 4096
        AND octet_length(success_messages) BETWEEN 2 AND 16384
        AND octet_length(not_found_messages) BETWEEN 2 AND 16384
        AND args_digest ~ '^[0-9a-f]{64}$'
        AND tool_spec_digest ~ '^[0-9a-f]{64}$'
        AND tool_policy_digest ~ '^[0-9a-f]{64}$'
        AND success_digest ~ '^[0-9a-f]{64}$'
        AND not_found_digest ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT agent_action_continuation_status_valid CHECK (
        status IN (
            'pending','confirmed','completed','cancelled','expired',
            'rolled_back','blocked'
        )
    ),
    CONSTRAINT agent_action_continuation_terminal_valid CHECK (
        (status = 'completed' AND terminal_code IN ('enabled','not_found')
            AND confirmed_at IS NOT NULL AND completed_at IS NOT NULL
            AND blocked_reason IS NULL)
        OR
        (status = 'confirmed' AND terminal_code IS NULL
            AND confirmed_at IS NOT NULL AND completed_at IS NULL
            AND blocked_reason IS NULL)
        OR
        (status = 'blocked' AND terminal_code IS NULL
            AND confirmed_at IS NOT NULL AND completed_at IS NULL
            AND blocked_reason IS NOT NULL)
        OR
        (status IN ('pending','cancelled','expired','rolled_back')
            AND terminal_code IS NULL AND completed_at IS NULL
            AND confirmed_at IS NULL AND blocked_reason IS NULL)
    ),
    CONSTRAINT agent_action_continuation_fence_valid CHECK (
        lease_fence >= 0 AND attempt_count >= 0
    ),
    CONSTRAINT agent_action_continuation_lease_valid CHECK (
        (lease_owner IS NULL AND lease_expires_at IS NULL)
        OR
        (status = 'confirmed' AND lease_owner IS NOT NULL
            AND btrim(lease_owner) = lease_owner AND lease_owner <> ''
            AND octet_length(lease_owner) <= 255
            AND lease_expires_at IS NOT NULL)
    )
);

CREATE INDEX idx_agent_action_continuation_due_tenant
    ON agent_action_continuations (tenant_id,next_attempt_at,action_id)
    WHERE status='confirmed';

CREATE TABLE agent_action_continuation_authority_events (
    id              BIGSERIAL   PRIMARY KEY,
    tenant_id       BIGINT      NOT NULL,
    user_id         BIGINT      NOT NULL,
    action_id       TEXT        NOT NULL,
    generation      BIGINT      NOT NULL,
    mode            TEXT        NOT NULL,
    evidence        TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT fk_agent_action_continuation_authority_action
        FOREIGN KEY (action_id,tenant_id,user_id)
        REFERENCES agent_action_continuations (action_id,tenant_id,user_id)
        ON DELETE CASCADE,
    CONSTRAINT uq_agent_action_continuation_authority_generation
        UNIQUE (action_id,generation),
    CONSTRAINT agent_action_continuation_authority_valid CHECK (
        generation > 0
        AND mode IN ('durable','legacy')
        AND btrim(evidence)=evidence
        AND octet_length(evidence) BETWEEN 1 AND 512
    )
);

CREATE INDEX idx_agent_action_continuation_authority_current
    ON agent_action_continuation_authority_events
       (action_id,generation DESC);

ALTER TABLE agent_action_continuations ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON agent_action_continuations FROM PUBLIC;
CREATE POLICY tenant_visible ON agent_action_continuations
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON agent_action_continuations AS RESTRICTIVE
    FOR ALL USING (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
    ) WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
    );

ALTER TABLE agent_action_continuation_authority_events ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON agent_action_continuation_authority_events FROM PUBLIC;
REVOKE ALL ON SEQUENCE agent_action_continuation_authority_events_id_seq
    FROM PUBLIC;
CREATE POLICY tenant_visible ON agent_action_continuation_authority_events
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON agent_action_continuation_authority_events AS RESTRICTIVE
    FOR ALL USING (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
    ) WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
    );

-- Control and effect identities are deliberately split: the continuator can
-- read exact authority but cannot promote or authorize itself.
-- +goose StatementBegin
DO $$
DECLARE role_name text;
BEGIN
    FOREACH role_name IN ARRAY ARRAY[
        'vane_agent_action_operator',
        'vane_agent_action_continuator'
    ] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=role_name) THEN
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
        EXECUTE format(
            'ALTER ROLE %I NOSUPERUSER NOCREATEDB NOCREATEROLE ' ||
            'NOREPLICATION NOLOGIN NOINHERIT NOBYPASSRLS',
            role_name
        );
        EXECUTE format('ALTER ROLE %I RESET ALL',role_name);
        EXECUTE format(
            'ALTER ROLE %I SET search_path=pg_catalog,public',
            role_name
        );
        EXECUTE format('GRANT %I TO %I',role_name,CURRENT_USER);
    END LOOP;
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
DO $$
DECLARE role_name text;
BEGIN
    FOREACH role_name IN ARRAY ARRAY[
        'vane_agent_action_operator',
        'vane_agent_action_continuator'
    ] LOOP
        IF pg_has_role(role_name,'vane_app','MEMBER')
           OR pg_has_role('vane_app',role_name,'MEMBER') THEN
            RAISE EXCEPTION '057: vane_app and % must be unrelated',role_name;
        END IF;
        IF EXISTS (
            SELECT 1 FROM pg_auth_members am
            JOIN pg_roles granted_role ON granted_role.oid=am.roleid
            JOIN pg_roles member_role ON member_role.oid=am.member
            WHERE granted_role.rolname=role_name
              AND member_role.rolname<>CURRENT_USER
        ) THEN
            RAISE EXCEPTION '057: only migration owner may enter %',role_name;
        END IF;
        IF EXISTS (
            SELECT 1 FROM pg_auth_members am
            JOIN pg_roles member_role ON member_role.oid=am.member
            WHERE member_role.rolname=role_name
        ) THEN
            RAISE EXCEPTION '057: % must not enter another role',role_name;
        END IF;
    END LOOP;
    IF pg_has_role(
           'vane_agent_action_operator',
           'vane_agent_action_continuator','MEMBER'
       ) OR pg_has_role(
           'vane_agent_action_continuator',
           'vane_agent_action_operator','MEMBER'
       ) THEN
        RAISE EXCEPTION '057: action control/effect roles must be unrelated';
    END IF;
END $$;
-- +goose StatementEnd

GRANT USAGE ON SCHEMA public
    TO vane_agent_action_operator,vane_agent_action_continuator;

GRANT SELECT (
    id,tenant_id,user_id,session_id,tool_name,args,summary,status,expires_at,
    executed_at,execution_version
) ON pending_actions
    TO vane_agent_action_operator,vane_agent_action_continuator;
GRANT UPDATE (execution_version)
    ON pending_actions TO vane_agent_action_operator;
GRANT UPDATE (status,executed_at,updated_at)
    ON pending_actions TO vane_agent_action_continuator;

GRANT INSERT (
    action_id,tenant_id,user_id,session_id,tool_name,source_id,
    canonical_args,args_digest,tool_spec_version,tool_spec,tool_spec_digest,
    tool_policy_version,tool_policy,tool_policy_digest,adapter_version,
    success_messages,success_digest,not_found_messages,not_found_digest
) ON agent_action_continuations TO vane_agent_action_operator;
GRANT SELECT (
    action_id,tenant_id,user_id,session_id,tool_name,source_id,
    canonical_args,args_digest,tool_spec_version,tool_spec,tool_spec_digest,
    tool_policy_version,tool_policy,tool_policy_digest,adapter_version,
    success_messages,success_digest,not_found_messages,not_found_digest,
    status,terminal_code,lease_owner,lease_fence,lease_expires_at,
    attempt_count,next_attempt_at,confirmed_at,completed_at,blocked_reason,
    created_at,updated_at
) ON agent_action_continuations
    TO vane_agent_action_operator,vane_agent_action_continuator;
GRANT UPDATE (
    status,terminal_code,lease_owner,lease_fence,lease_expires_at,
    attempt_count,next_attempt_at,confirmed_at,completed_at,
    blocked_reason,updated_at
) ON agent_action_continuations TO vane_agent_action_continuator;
GRANT UPDATE (status,updated_at)
    ON agent_action_continuations TO vane_agent_action_operator;

GRANT SELECT (
    id,tenant_id,user_id,action_id,generation,mode,evidence,created_at
) ON agent_action_continuation_authority_events
    TO vane_agent_action_operator,vane_agent_action_continuator;
GRANT INSERT (
    tenant_id,user_id,action_id,generation,mode,evidence
) ON agent_action_continuation_authority_events
    TO vane_agent_action_operator;
GRANT USAGE ON SEQUENCE agent_action_continuation_authority_events_id_seq
    TO vane_agent_action_operator;

GRANT SELECT (id,status,fail_count,next_fetch_at,updated_at)
    ON sources TO vane_agent_action_continuator;
GRANT UPDATE (status,fail_count,next_fetch_at,updated_at)
    ON sources TO vane_agent_action_continuator;
GRANT SELECT (source_id,user_id,status)
    ON subscriptions TO vane_agent_action_continuator;
GRANT SELECT (schedule_id,source_id)
    ON schedule_sources TO vane_agent_action_continuator;
GRANT SELECT (id,user_id) ON schedules TO vane_agent_action_continuator;
GRANT SELECT (id,tenant_id,user_id,messages,turn_count,activated_tools)
    ON agent_sessions TO vane_agent_action_continuator;
GRANT UPDATE (messages) ON agent_sessions TO vane_agent_action_continuator;
GRANT SELECT (
    id,tenant_id,user_id,session_id,sequence,batch_idempotency_key,
    batch_index,batch_size,kind,schema_version,payload,payload_digest,
    batch_digest,created_at
) ON agent_events TO vane_agent_action_continuator;
GRANT INSERT (
    tenant_id,user_id,session_id,sequence,batch_idempotency_key,
    batch_index,batch_size,kind,schema_version,payload,payload_digest,batch_digest
) ON agent_events TO vane_agent_action_continuator;
GRANT USAGE ON SEQUENCE agent_events_id_seq TO vane_agent_action_continuator;
GRANT SELECT (
    id,tenant_id,user_id,session_id,generation,action,
    ledger_head_sequence,legacy_digest,ledger_digest,created_at
) ON agent_session_projection_authority_events
    TO vane_agent_action_continuator;

-- +goose Down

SELECT pg_advisory_xact_lock(1447120453,1095976528)
    /* agent action producer/downgrade admission */;
LOCK TABLE pending_actions IN ACCESS EXCLUSIVE MODE
    /* migration 057 action root first */;
LOCK TABLE agent_action_continuations IN ACCESS EXCLUSIVE MODE
    /* migration 057 continuation second */;
LOCK TABLE sources IN ACCESS EXCLUSIVE MODE
    /* migration 057 effect target third */;
LOCK TABLE agent_sessions IN ACCESS EXCLUSIVE MODE
    /* migration 057 session target fourth */;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM agent_action_continuations) THEN
        RAISE EXCEPTION
            '057: refusing downgrade while durable Agent actions exist';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE USAGE ON SEQUENCE
    agent_action_continuation_authority_events_id_seq
    FROM vane_agent_action_operator;
REVOKE INSERT (
    tenant_id,user_id,action_id,generation,mode,evidence
) ON agent_action_continuation_authority_events
    FROM vane_agent_action_operator;
REVOKE SELECT (
    id,tenant_id,user_id,action_id,generation,mode,evidence,created_at
) ON agent_action_continuation_authority_events
    FROM vane_agent_action_operator,vane_agent_action_continuator;
REVOKE UPDATE (
    status,terminal_code,lease_owner,lease_fence,lease_expires_at,
    attempt_count,next_attempt_at,confirmed_at,completed_at,
    blocked_reason,updated_at
) ON agent_action_continuations FROM vane_agent_action_continuator;
REVOKE UPDATE (status,updated_at)
    ON agent_action_continuations FROM vane_agent_action_operator;
REVOKE INSERT (
    action_id,tenant_id,user_id,session_id,tool_name,source_id,
    canonical_args,args_digest,tool_spec_version,tool_spec,tool_spec_digest,
    tool_policy_version,tool_policy,tool_policy_digest,adapter_version,
    success_messages,success_digest,not_found_messages,not_found_digest
) ON agent_action_continuations FROM vane_agent_action_operator;
REVOKE SELECT (
    action_id,tenant_id,user_id,session_id,tool_name,source_id,
    canonical_args,args_digest,tool_spec_version,tool_spec,tool_spec_digest,
    tool_policy_version,tool_policy,tool_policy_digest,adapter_version,
    success_messages,success_digest,not_found_messages,not_found_digest,
    status,terminal_code,lease_owner,lease_fence,lease_expires_at,
    attempt_count,next_attempt_at,confirmed_at,completed_at,blocked_reason,
    created_at,updated_at
) ON agent_action_continuations
    FROM vane_agent_action_operator,vane_agent_action_continuator;
REVOKE SELECT (
    id,tenant_id,user_id,session_id,generation,action,
    ledger_head_sequence,legacy_digest,ledger_digest,created_at
) ON agent_session_projection_authority_events
    FROM vane_agent_action_continuator;
REVOKE USAGE ON SEQUENCE agent_events_id_seq
    FROM vane_agent_action_continuator;
REVOKE INSERT (
    tenant_id,user_id,session_id,sequence,batch_idempotency_key,
    batch_index,batch_size,kind,schema_version,payload,payload_digest,batch_digest
) ON agent_events FROM vane_agent_action_continuator;
REVOKE SELECT (
    id,tenant_id,user_id,session_id,sequence,batch_idempotency_key,
    batch_index,batch_size,kind,schema_version,payload,payload_digest,
    batch_digest,created_at
) ON agent_events FROM vane_agent_action_continuator;
REVOKE UPDATE (messages) ON agent_sessions
    FROM vane_agent_action_continuator;
REVOKE SELECT (id,tenant_id,user_id,messages,turn_count,activated_tools)
    ON agent_sessions FROM vane_agent_action_continuator;
REVOKE SELECT (id,user_id) ON schedules
    FROM vane_agent_action_continuator;
REVOKE SELECT (schedule_id,source_id) ON schedule_sources
    FROM vane_agent_action_continuator;
REVOKE SELECT (source_id,user_id,status) ON subscriptions
    FROM vane_agent_action_continuator;
REVOKE UPDATE (status,fail_count,next_fetch_at,updated_at)
    ON sources FROM vane_agent_action_continuator;
REVOKE SELECT (id,status,fail_count,next_fetch_at,updated_at)
    ON sources FROM vane_agent_action_continuator;
REVOKE UPDATE (status,executed_at,updated_at)
    ON pending_actions FROM vane_agent_action_continuator;
REVOKE UPDATE (execution_version)
    ON pending_actions FROM vane_agent_action_operator;
REVOKE SELECT (
    id,tenant_id,user_id,session_id,tool_name,args,summary,status,expires_at,
    executed_at,execution_version
) ON pending_actions
    FROM vane_agent_action_operator,vane_agent_action_continuator;
REVOKE USAGE ON SCHEMA public
    FROM vane_agent_action_operator,vane_agent_action_continuator;

DROP POLICY IF EXISTS tenant_isolation
    ON agent_action_continuation_authority_events;
DROP POLICY IF EXISTS tenant_visible
    ON agent_action_continuation_authority_events;
DROP TABLE agent_action_continuation_authority_events;
DROP POLICY IF EXISTS tenant_isolation ON agent_action_continuations;
DROP POLICY IF EXISTS tenant_visible ON agent_action_continuations;
DROP TABLE agent_action_continuations;
ALTER TABLE pending_actions
    DROP CONSTRAINT uq_pending_actions_exact_scope;
