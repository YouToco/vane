-- 136: shared-workspace task identity and append-only audit foundation.
--
-- schedules.user_id is intentionally NOT reassigned. It is part of immutable
-- definition/snapshot foreign keys and remains the frozen execution identity.
-- assignee_user_id is the product-visible, transferable responsible member.
-- Mixing the two would either rewrite history or make old snapshots replay as
-- a different user, so the database keeps both identities explicit.

-- +goose Up

ALTER TABLE schedules
    ADD COLUMN creator_user_id BIGINT REFERENCES users (id),
    ADD COLUMN assignee_user_id BIGINT REFERENCES users (id),
    ADD COLUMN task_visibility TEXT;

UPDATE schedules s
SET creator_user_id=s.user_id,
    assignee_user_id=s.user_id,
    task_visibility=CASE
        WHEN t.workspace_kind='team' THEN 'workspace'
        ELSE 'personal'
    END
FROM tenants t
WHERE t.id=s.tenant_id;

-- Several retained task-definition foreign keys are DEFERRABLE. Flush their
-- pending trigger events before changing the schedules relation again in this
-- same transactional migration.
SET CONSTRAINTS ALL IMMEDIATE;

ALTER TABLE schedules
    ALTER COLUMN creator_user_id SET NOT NULL,
    ALTER COLUMN assignee_user_id SET NOT NULL,
    ALTER COLUMN task_visibility SET NOT NULL,
    ADD CONSTRAINT ck_schedules_task_visibility
        CHECK (task_visibility IN ('personal','workspace'));

CREATE INDEX idx_schedules_team_visible
    ON schedules (tenant_id,task_visibility,created_at DESC,id);
CREATE INDEX idx_schedules_assignee
    ON schedules (tenant_id,assignee_user_id,created_at DESC,id);

-- Compatibility insert paths predate these columns. The trigger fills only
-- absent identity on INSERT; later assignee transfers never touch user_id or
-- creator_user_id and therefore cannot rewrite frozen execution history.
-- +goose StatementBegin
CREATE FUNCTION schedule_team_identity_defaults_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public
AS $$
DECLARE workspace_kind_value TEXT;
BEGIN
    IF NEW.creator_user_id IS NULL THEN
        NEW.creator_user_id := NEW.user_id;
    END IF;
    IF NEW.assignee_user_id IS NULL THEN
        NEW.assignee_user_id := NEW.user_id;
    END IF;
    IF NEW.task_visibility IS NULL OR NEW.task_visibility='' THEN
        SELECT t.workspace_kind INTO STRICT workspace_kind_value
          FROM public.tenants t WHERE t.id=NEW.tenant_id;
        NEW.task_visibility := CASE
            WHEN workspace_kind_value='team' THEN 'workspace'
            ELSE 'personal'
        END;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER schedules_team_identity_defaults_v1
BEFORE INSERT ON schedules
FOR EACH ROW EXECUTE FUNCTION schedule_team_identity_defaults_v1();

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

-- Existing restricted task creation roles may keep omitting the new columns:
-- the compatibility trigger fills them. Explicit mutation authority is only
-- granted for the product assignee; frozen user_id remains outside vane_app's
-- update allowlist.
GRANT UPDATE (assignee_user_id,updated_at) ON schedules TO vane_app;

-- +goose Down

REVOKE UPDATE (assignee_user_id) ON schedules FROM vane_app;
DROP TABLE task_access_audit_events;
DROP TRIGGER schedules_team_identity_defaults_v1 ON schedules;
DROP FUNCTION schedule_team_identity_defaults_v1();
DROP INDEX idx_schedules_assignee;
DROP INDEX idx_schedules_team_visible;
ALTER TABLE schedules DROP CONSTRAINT ck_schedules_task_visibility;
ALTER TABLE schedules DROP COLUMN task_visibility;
ALTER TABLE schedules DROP COLUMN assignee_user_id;
ALTER TABLE schedules DROP COLUMN creator_user_id;
