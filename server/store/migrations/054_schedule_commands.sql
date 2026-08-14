-- 054: durable Web schedule command control plane.
--
-- A command intent is committed before Temporal I/O. Recovery can therefore
-- replay the exact request after response loss or process exit. The restricted
-- runtime role can mutate only owner-scoped command rows plus the two schedule
-- mirror columns needed by pause/resume, or delete the exact schedule.

-- +goose Up

CREATE TABLE schedule_commands (
    id                UUID        PRIMARY KEY,
    tenant_id         BIGINT      NOT NULL REFERENCES tenants (id),
    user_id           BIGINT      NOT NULL REFERENCES users (id),
    task_id           TEXT        NOT NULL,
    idempotency_key   TEXT        NOT NULL,
    kind              TEXT        NOT NULL,
    payload_digest    TEXT        NOT NULL,
    remote_request_id TEXT        NOT NULL,
    status            TEXT        NOT NULL DEFAULT 'pending',
    phase             TEXT        NOT NULL DEFAULT 'intent',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at      TIMESTAMPTZ,
    error_code        TEXT        NOT NULL DEFAULT '',
    error_message     TEXT        NOT NULL DEFAULT '',

    CONSTRAINT schedule_commands_task_id_valid CHECK (
        task_id <> '' AND btrim(task_id) = task_id AND
        octet_length(task_id) <= 255
    ),
    CONSTRAINT schedule_commands_idempotency_key_valid CHECK (
        idempotency_key <> '' AND btrim(idempotency_key) = idempotency_key AND
        octet_length(idempotency_key) <= 128 AND
        idempotency_key ~ '^[A-Za-z0-9][A-Za-z0-9._:-]*$'
    ),
    CONSTRAINT schedule_commands_kind_valid CHECK (
        kind IN ('run', 'pause', 'resume', 'delete')
    ),
    CONSTRAINT schedule_commands_payload_digest_valid CHECK (
        payload_digest ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT schedule_commands_remote_request_id_valid CHECK (
        remote_request_id ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT schedule_commands_status_valid CHECK (
        status IN ('pending', 'completed', 'blocked')
    ),
    CONSTRAINT schedule_commands_phase_valid CHECK (
        phase IN ('intent', 'completed', 'blocked')
    ),
    CONSTRAINT schedule_commands_terminal_valid CHECK (
        (status = 'pending' AND phase = 'intent' AND completed_at IS NULL AND
         error_code = '' AND error_message = '') OR
        (status = 'completed' AND phase = 'completed' AND
         completed_at IS NOT NULL AND error_code = '' AND error_message = '') OR
        (status = 'blocked' AND phase = 'blocked' AND
         completed_at IS NOT NULL AND error_code <> '' AND
         error_message <> '' AND octet_length(error_code) <= 64 AND
         octet_length(error_message) <= 1024)
    ),
    CONSTRAINT uq_schedule_commands_idempotency
        UNIQUE (tenant_id, user_id, idempotency_key)
);

-- One non-terminal mutation per exact task. Retained terminal rows do not
-- prevent later commands with new keys.
CREATE UNIQUE INDEX uq_schedule_commands_nonterminal_task
    ON schedule_commands (tenant_id, user_id, task_id)
    WHERE status = 'pending';

CREATE INDEX idx_schedule_commands_recovery
    ON schedule_commands (tenant_id, created_at, id)
    WHERE status = 'pending';

ALTER TABLE schedule_commands ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible
    ON schedule_commands
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation
    ON schedule_commands AS RESTRICTIVE
    FOR ALL
    USING (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint
    )
    WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint
    );

-- The role is NOLOGIN/NOINHERIT and can only be entered by the migration
-- owner through Store's tenant-scoped transaction helper.
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'vane_schedule_commander'
    ) THEN
        BEGIN
            CREATE ROLE vane_schedule_commander
                NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
                NOLOGIN NOINHERIT NOBYPASSRLS;
        EXCEPTION
            WHEN duplicate_object OR unique_violation THEN NULL;
        END;
    END IF;
END $$;
-- +goose StatementEnd

ALTER ROLE vane_schedule_commander
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    NOLOGIN NOINHERIT NOBYPASSRLS;
ALTER ROLE vane_schedule_commander RESET ALL;
ALTER ROLE vane_schedule_commander SET search_path = pg_catalog, public;
GRANT vane_schedule_commander TO CURRENT_USER;

-- Normalize the cluster-wide identity before granting database-local command
-- capabilities. A pre-existing membership could otherwise let vane_app or an
-- unrelated principal enter the commander, or let the commander inherit an
-- ambient role with broader access.
-- +goose StatementBegin
DO $$
BEGIN
    IF pg_has_role(
           'vane_schedule_commander', 'vane_app', 'MEMBER'
       ) OR
       pg_has_role(
           'vane_app', 'vane_schedule_commander', 'MEMBER'
       ) THEN
        RAISE EXCEPTION
            '054: vane_app and schedule commander must be unrelated';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles granted_role ON granted_role.oid = am.roleid
          JOIN pg_roles member_role ON member_role.oid = am.member
         WHERE granted_role.rolname = 'vane_schedule_commander'
           AND member_role.rolname <> CURRENT_USER
    ) THEN
        RAISE EXCEPTION
            '054: only migration owner may enter schedule commander';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles member_role ON member_role.oid = am.member
         WHERE member_role.rolname = 'vane_schedule_commander'
    ) THEN
        RAISE EXCEPTION
            '054: schedule commander must not enter another role';
    END IF;
END $$;
-- +goose StatementEnd

GRANT USAGE ON SCHEMA public TO vane_schedule_commander;
GRANT SELECT, INSERT (
    id, tenant_id, user_id, task_id, idempotency_key, kind,
    payload_digest, remote_request_id
), UPDATE (
    status, phase, updated_at, completed_at, error_code, error_message
) ON schedule_commands TO vane_schedule_commander;

GRANT SELECT (
    id, tenant_id, user_id, nl_description, spec_json, scope_json, status,
    execution_mode, created_at, updated_at,
    definition_edit_operation_id, definition_edit_fence
) ON schedules TO vane_schedule_commander;
GRANT UPDATE (status, updated_at), DELETE
    ON schedules TO vane_schedule_commander;

GRANT SELECT (
    target_tenant_id, target_user_id, task_id, status, tombstoned_at
) ON task_definition_edit_operations TO vane_schedule_commander;
GRANT SELECT (
    tenant_id, user_id, task_id, tool_name, execution_version, status, phase
) ON pending_actions TO vane_schedule_commander;

-- The general runtime role must not gain access to command payloads or the
-- ability to forge/complete command rows.
REVOKE ALL ON schedule_commands FROM PUBLIC, vane_app;

-- +goose Down

LOCK TABLE schedule_commands IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM schedule_commands) THEN
        RAISE EXCEPTION
            '054: refusing downgrade while durable schedule command audit exists';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE SELECT (
    tenant_id, user_id, task_id, tool_name, execution_version, status, phase
) ON pending_actions FROM vane_schedule_commander;
REVOKE SELECT (
    target_tenant_id, target_user_id, task_id, status, tombstoned_at
) ON task_definition_edit_operations FROM vane_schedule_commander;
REVOKE UPDATE (status, updated_at), DELETE
    ON schedules FROM vane_schedule_commander;
REVOKE SELECT (
    id, tenant_id, user_id, nl_description, spec_json, scope_json, status,
    execution_mode, created_at, updated_at,
    definition_edit_operation_id, definition_edit_fence
) ON schedules FROM vane_schedule_commander;
REVOKE SELECT, INSERT (
    id, tenant_id, user_id, task_id, idempotency_key, kind,
    payload_digest, remote_request_id
), UPDATE (
    status, phase, updated_at, completed_at, error_code, error_message
) ON schedule_commands FROM vane_schedule_commander;
REVOKE USAGE ON SCHEMA public FROM vane_schedule_commander;

DROP TABLE schedule_commands;

-- The NOLOGIN identity and owner membership are cluster-wide and may serve
-- another database in the same cluster. Down removes database-local
-- capabilities but preserves that shared identity.
