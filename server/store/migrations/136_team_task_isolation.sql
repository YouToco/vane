-- 136: shared-workspace task identity and append-only audit foundation.
--
-- schedules.user_id is intentionally NOT reassigned. It is part of immutable
-- definition/snapshot foreign keys and remains the frozen execution identity.
-- assignee_user_id is the product-visible, transferable responsible member.
-- Mixing the two would either rewrite history or make old snapshots replay as
-- a different user, so the database keeps both identities explicit.

-- +goose Up

-- Migration 132 deliberately seals the complete schedules catalog descriptor
-- so the retained legacy replayer can detect authority drift. Product-facing
-- creator/assignee/visibility therefore live in a separate projection instead
-- of mutating the frozen schedules relation or its ACLs.
CREATE TABLE task_workspace_access (
    tenant_id         BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    execution_user_id BIGINT NOT NULL REFERENCES users(id),
    schedule_id       TEXT NOT NULL,
    creator_user_id   BIGINT NOT NULL REFERENCES users(id),
    assignee_user_id  BIGINT NOT NULL REFERENCES users(id),
    task_visibility   TEXT NOT NULL CHECK (
        task_visibility IN ('personal','workspace')),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id,schedule_id),
    FOREIGN KEY (tenant_id,execution_user_id,schedule_id)
        REFERENCES schedules(tenant_id,user_id,id) ON DELETE CASCADE
);

INSERT INTO task_workspace_access(
    tenant_id,execution_user_id,schedule_id,creator_user_id,
    assignee_user_id,task_visibility,created_at,updated_at)
SELECT s.tenant_id,s.user_id,s.id,s.user_id,s.user_id,
       CASE WHEN t.workspace_kind='team' THEN 'workspace' ELSE 'personal' END,
       s.created_at,s.updated_at
  FROM schedules s JOIN tenants t ON t.id=s.tenant_id;

CREATE INDEX idx_task_workspace_access_visible
    ON task_workspace_access (tenant_id,task_visibility,created_at DESC,schedule_id);
CREATE INDEX idx_task_workspace_access_assignee
    ON task_workspace_access (tenant_id,assignee_user_id,created_at DESC,schedule_id);

ALTER TABLE task_workspace_access ENABLE ROW LEVEL SECURITY;
ALTER TABLE task_workspace_access FORCE ROW LEVEL SECURITY;
CREATE POLICY task_workspace_access_owner ON task_workspace_access
    TO PUBLIC
    USING (current_user=pg_catalog.pg_get_userbyid((
        SELECT relation.relowner FROM pg_catalog.pg_class relation
         WHERE relation.oid='task_workspace_access'::pg_catalog.regclass)))
    WITH CHECK (current_user=pg_catalog.pg_get_userbyid((
        SELECT relation.relowner FROM pg_catalog.pg_class relation
         WHERE relation.oid='task_workspace_access'::pg_catalog.regclass)));
CREATE POLICY task_workspace_access_scope ON task_workspace_access
    TO vane_app
    USING (tenant_id IS NOT DISTINCT FROM
        NULLIF(current_setting('app.tenant_id',true),'')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
        NULLIF(current_setting('app.tenant_id',true),'')::bigint);

-- Compatibility insert paths know nothing about the projection. The trigger
-- creates it from immutable schedule identity; later assignee transfers touch
-- only the projection and cannot rewrite frozen execution history.
-- +goose StatementBegin
CREATE FUNCTION schedule_workspace_access_defaults_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public
AS $$
DECLARE workspace_kind_value TEXT;
BEGIN
    SELECT t.workspace_kind INTO STRICT workspace_kind_value
      FROM public.tenants t WHERE t.id=NEW.tenant_id;
    INSERT INTO public.task_workspace_access(
        tenant_id,execution_user_id,schedule_id,creator_user_id,
        assignee_user_id,task_visibility,created_at,updated_at)
    VALUES(NEW.tenant_id,NEW.user_id,NEW.id,NEW.user_id,NEW.user_id,
        CASE WHEN workspace_kind_value='team' THEN 'workspace' ELSE 'personal' END,
        NEW.created_at,NEW.updated_at);
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- Trigger invocation does not require callers to execute the function
-- directly. Keep the SECURITY DEFINER entry point out of PUBLIC so adding the
-- compatibility trigger cannot expand any existing runtime capability role.
REVOKE ALL ON FUNCTION schedule_workspace_access_defaults_v1() FROM PUBLIC;

CREATE TRIGGER schedules_workspace_access_defaults_v1
AFTER INSERT ON schedules
FOR EACH ROW EXECUTE FUNCTION schedule_workspace_access_defaults_v1();

CREATE TABLE task_access_audit_events (
    id                 BIGSERIAL PRIMARY KEY,
    tenant_id          BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    task_id            TEXT        NOT NULL,
    actor_user_id      BIGINT      NOT NULL REFERENCES users (id),
    creator_user_id    BIGINT      NOT NULL REFERENCES users (id),
    execution_user_id  BIGINT      NOT NULL REFERENCES users (id),
    assignee_user_id   BIGINT      NOT NULL REFERENCES users (id),
    target_user_id     BIGINT      REFERENCES users (id),
    event_kind         TEXT        NOT NULL,
    details            JSONB       NOT NULL DEFAULT '{}',
    created_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_task_access_audit_event_kind CHECK (event_kind IN (
        'task.run_requested','task.pause_requested','task.resume_requested',
        'task.edit_requested','task.delete_requested','task.assignee_changed'
    )),
    CONSTRAINT ck_task_access_audit_details CHECK (
        jsonb_typeof(details)='object' AND octet_length(details::text)<=16384
    )
);
CREATE INDEX idx_task_access_audit_scope
    ON task_access_audit_events (tenant_id,task_id,created_at,id);

ALTER TABLE task_access_audit_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE task_access_audit_events FORCE ROW LEVEL SECURITY;
CREATE POLICY task_access_audit_exact_scope ON task_access_audit_events
    FOR SELECT TO vane_app
    USING (
        tenant_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.tenant_id',true),'')::bigint
    );
CREATE POLICY task_access_audit_append_scope ON task_access_audit_events
    FOR INSERT TO vane_app
    WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.tenant_id',true),'')::bigint
        AND actor_user_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.user_id',true),'')::bigint
    );

GRANT SELECT,INSERT ON task_access_audit_events TO vane_app;
GRANT USAGE,SELECT ON SEQUENCE task_access_audit_events_id_seq TO vane_app;

GRANT SELECT ON task_workspace_access TO vane_app;
GRANT UPDATE (assignee_user_id,updated_at) ON task_workspace_access TO vane_app;

-- +goose Down

REVOKE UPDATE (assignee_user_id,updated_at) ON task_workspace_access FROM vane_app;
REVOKE SELECT ON task_workspace_access FROM vane_app;
DROP TABLE task_access_audit_events;
DROP TRIGGER schedules_workspace_access_defaults_v1 ON schedules;
DROP FUNCTION schedule_workspace_access_defaults_v1();
DROP TABLE task_workspace_access;
