-- 140: principal-scoped A2A task and history storage.
--
-- The migration-013 table is retained as inert historical data. New runtime
-- code has no access path to it; silently rewriting old global rows into a
-- workspace would invent an authority that did not exist when they were made.

-- +goose Up

ALTER TABLE a2a_access_tokens
    ADD CONSTRAINT uq_a2a_access_token_task_scope_v140
    UNIQUE (tenant_id,id,principal_user_id,actor_type);

CREATE TABLE a2a_principal_tasks (
    tenant_id          BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    principal_user_id  BIGINT      NOT NULL REFERENCES users (id),
    actor_type         TEXT        NOT NULL,
    created_by_token_id UUID       NOT NULL,
    id                 TEXT        NOT NULL,
    context_id         TEXT        NOT NULL,
    status             TEXT        NOT NULL,
    task               JSONB       NOT NULL,
    version            BIGINT      NOT NULL DEFAULT 1,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id,principal_user_id,id),
    CONSTRAINT fk_a2a_principal_task_credential_v140
        FOREIGN KEY (tenant_id,created_by_token_id,principal_user_id,actor_type)
        REFERENCES a2a_access_tokens (tenant_id,id,principal_user_id,actor_type)
        ON DELETE RESTRICT,
    CONSTRAINT ck_a2a_principal_task_actor_v140
        CHECK (actor_type IN ('user','service_account')),
    CONSTRAINT ck_a2a_principal_task_identity_v140
        CHECK (octet_length(id) BETWEEN 1 AND 256 AND octet_length(context_id) BETWEEN 1 AND 256),
    CONSTRAINT ck_a2a_principal_task_version_v140 CHECK (version > 0),
    CONSTRAINT ck_a2a_principal_task_projection_v140 CHECK (
        jsonb_typeof(task)='object'
        AND task->>'id'=id
        AND task->>'contextId'=context_id
        AND task#>>'{status,state}'=status
    )
);
CREATE INDEX idx_a2a_principal_tasks_context_v140
    ON a2a_principal_tasks
       (tenant_id,principal_user_id,context_id,created_at DESC,id DESC);
CREATE INDEX idx_a2a_principal_tasks_status_v140
    ON a2a_principal_tasks
       (tenant_id,principal_user_id,status,updated_at DESC,id DESC);

ALTER TABLE a2a_principal_tasks ENABLE ROW LEVEL SECURITY;
ALTER TABLE a2a_principal_tasks FORCE ROW LEVEL SECURITY;
CREATE POLICY a2a_principal_tasks_select_v140 ON a2a_principal_tasks
    FOR SELECT TO vane_app
    USING (
        tenant_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.tenant_id',true),'')::bigint
        AND principal_user_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.user_id',true),'')::bigint
    );
CREATE POLICY a2a_principal_tasks_insert_v140 ON a2a_principal_tasks
    FOR INSERT TO vane_app
    WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.tenant_id',true),'')::bigint
        AND principal_user_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.user_id',true),'')::bigint
        AND created_by_token_id::text IS NOT DISTINCT FROM
            NULLIF(current_setting('app.a2a_token_id',true),'')
        AND actor_type IS NOT DISTINCT FROM
            NULLIF(current_setting('app.actor_type',true),'')
    );
CREATE POLICY a2a_principal_tasks_update_v140 ON a2a_principal_tasks
    FOR UPDATE TO vane_app
    USING (
        tenant_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.tenant_id',true),'')::bigint
        AND principal_user_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.user_id',true),'')::bigint
    )
    WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.tenant_id',true),'')::bigint
        AND principal_user_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.user_id',true),'')::bigint
    );

GRANT SELECT,INSERT ON a2a_principal_tasks TO vane_app;
GRANT UPDATE (status,task,version,updated_at) ON a2a_principal_tasks TO vane_app;

-- +goose Down

LOCK TABLE a2a_principal_tasks IN ACCESS EXCLUSIVE MODE;
ALTER TABLE a2a_principal_tasks NO FORCE ROW LEVEL SECURITY;
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM a2a_principal_tasks) THEN
        RAISE EXCEPTION 'refusing to drop retained principal-scoped A2A tasks'
            USING ERRCODE='55000';
    END IF;
END $$;
-- +goose StatementEnd
DROP TABLE a2a_principal_tasks;
ALTER TABLE a2a_access_tokens
    DROP CONSTRAINT uq_a2a_access_token_task_scope_v140;
