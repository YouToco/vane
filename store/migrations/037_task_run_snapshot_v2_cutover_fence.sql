-- 037: durable per-run authority fence for the C2c v2 snapshot cutover.
--
-- This migration is deliberately dark. It persists enough state for every
-- new worker to understand an immutable per-run authority, but application
-- code still writes NULL markers until a later migration-safe activation
-- batch exposes the operator write path.

-- +goose Up

CREATE TABLE task_run_snapshot_v2_cutover_events (
    id                           BIGSERIAL   PRIMARY KEY,
    tenant_id                    BIGINT      NOT NULL
        REFERENCES tenants (id) ON DELETE CASCADE,
    user_id                      BIGINT      NOT NULL,
    task_id                      TEXT        NOT NULL,
    generation                   BIGINT      NOT NULL,
    action                       TEXT        NOT NULL,
    reverts_event_id             BIGINT,
    approved_definition_version  BIGINT      NOT NULL,
    approved_definition_digest   TEXT        NOT NULL,
    snapshot_high_watermark      BIGINT      NOT NULL,
    audit_from_snapshot_id       BIGINT      NOT NULL,
    audit_count                  BIGINT      NOT NULL,
    audit_through_id             BIGINT      NOT NULL,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_task_run_snapshot_v2_cutover_event_scope
        UNIQUE (id, tenant_id, user_id, task_id),
    CONSTRAINT uq_task_run_snapshot_v2_cutover_generation
        UNIQUE (tenant_id, user_id, task_id, generation),
    CONSTRAINT fk_task_run_snapshot_v2_cutover_reverts
        FOREIGN KEY (
            reverts_event_id, tenant_id, user_id, task_id
        ) REFERENCES task_run_snapshot_v2_cutover_events (
            id, tenant_id, user_id, task_id
        ),
    CONSTRAINT task_run_snapshot_v2_cutover_identity_valid CHECK (
        task_id <> '' AND btrim(task_id) = task_id AND
        octet_length(task_id) <= 255
    ),
    CONSTRAINT task_run_snapshot_v2_cutover_generation_positive
        CHECK (generation > 0),
    CONSTRAINT task_run_snapshot_v2_cutover_action_valid CHECK (
        (action = 'activate' AND reverts_event_id IS NULL) OR
        (action = 'rollback' AND reverts_event_id IS NOT NULL)
    ),
    CONSTRAINT task_run_snapshot_v2_cutover_definition_valid CHECK (
        approved_definition_version > 0 AND
        approved_definition_digest ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT task_run_snapshot_v2_cutover_audit_valid CHECK (
        snapshot_high_watermark > 0 AND
        audit_from_snapshot_id > 0 AND
        audit_from_snapshot_id <= audit_through_id AND
        audit_count > 0 AND
        audit_through_id = snapshot_high_watermark
    )
);

-- The audit range columns are a carrier only in this dark migration. No
-- production role can INSERT this table. The later activation primitive must
-- lock the schedule first, derive H/from/count inside that transaction, scan
-- the complete database-issued range, then append the event and CAS the
-- schedule pointer; caller-supplied evidence is never admissible.

CREATE INDEX idx_task_run_snapshot_v2_cutover_scope
    ON task_run_snapshot_v2_cutover_events (
        tenant_id, user_id, task_id, generation DESC
    );

ALTER TABLE schedules
    ADD COLUMN run_snapshot_cutover_event_id BIGINT,
    ADD CONSTRAINT fk_schedules_run_snapshot_cutover_event
        FOREIGN KEY (
            run_snapshot_cutover_event_id, tenant_id, user_id, id
        ) REFERENCES task_run_snapshot_v2_cutover_events (
            id, tenant_id, user_id, task_id
        );

ALTER TABLE task_run_snapshots
    ADD COLUMN v2_cutover_event_id BIGINT,
    ADD CONSTRAINT fk_task_run_snapshots_v2_cutover_event
        FOREIGN KEY (
            v2_cutover_event_id, tenant_id, user_id, task_id
        ) REFERENCES task_run_snapshot_v2_cutover_events (
            id, tenant_id, user_id, task_id
        );

-- Migration 022 predates the repository-wide NULLIF guard and may observe an
-- empty reset value (rather than SQL NULL) on a reused pooled connection.
-- Normalize it before the cast so an absent tenant context is simply
-- invisible, matching the newer snapshot/event policies and avoiding an
-- error-message oracle before the invoker admission trigger can fail closed.
DROP POLICY tenant_isolation ON schedules;
CREATE POLICY tenant_isolation ON schedules AS RESTRICTIVE
    FOR ALL
    USING (tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint);

-- Rollback is also immutable evidence. It must identify an earlier activation
-- under the same scope and repeat that activation's exact definition/audit
-- basis; a rollback cannot reinterpret or recursively roll back another
-- rollback event.
-- +goose StatementBegin
CREATE FUNCTION task_run_snapshot_v2_cutover_event_integrity()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    tenant_context TEXT;
    target_action TEXT;
    target_generation BIGINT;
    target_definition_version BIGINT;
    target_definition_digest TEXT;
    target_high_watermark BIGINT;
    target_audit_from BIGINT;
    target_audit_count BIGINT;
    target_audit_through BIGINT;
BEGIN
    tenant_context := current_setting('app.tenant_id', true);
    IF tenant_context IS NULL OR tenant_context !~ '^[1-9][0-9]*$' OR
       tenant_context::bigint IS DISTINCT FROM NEW.tenant_id THEN
        RAISE EXCEPTION 'task run snapshot v2 fence tenant context is invalid'
            USING ERRCODE = '42501';
    END IF;
    IF NEW.action = 'activate' THEN
        RETURN NEW;
    END IF;

    SELECT e.action, e.generation,
           e.approved_definition_version, e.approved_definition_digest,
           e.snapshot_high_watermark, e.audit_from_snapshot_id,
           e.audit_count, e.audit_through_id
      INTO target_action, target_generation,
           target_definition_version, target_definition_digest,
           target_high_watermark, target_audit_from,
           target_audit_count, target_audit_through
      FROM public.task_run_snapshot_v2_cutover_events e
     WHERE e.id = NEW.reverts_event_id
       AND e.tenant_id = NEW.tenant_id
       AND e.user_id = NEW.user_id
       AND e.task_id = NEW.task_id;

    IF target_action IS DISTINCT FROM 'activate' OR
       NEW.generation <= target_generation OR
       NEW.approved_definition_version IS DISTINCT FROM
           target_definition_version OR
       NEW.approved_definition_digest IS DISTINCT FROM
           target_definition_digest OR
       NEW.snapshot_high_watermark IS DISTINCT FROM target_high_watermark OR
       NEW.audit_from_snapshot_id IS DISTINCT FROM target_audit_from OR
       NEW.audit_count IS DISTINCT FROM target_audit_count OR
       NEW.audit_through_id IS DISTINCT FROM target_audit_through THEN
        RAISE EXCEPTION
            'task run snapshot v2 rollback event is not bound to an activation'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION task_run_snapshot_v2_cutover_event_integrity()
    FROM PUBLIC;

CREATE TRIGGER task_run_snapshot_v2_cutover_event_integrity
BEFORE INSERT ON task_run_snapshot_v2_cutover_events
FOR EACH ROW
EXECUTE FUNCTION task_run_snapshot_v2_cutover_event_integrity();

-- The current pointer is a state-machine head, not an arbitrary foreign key.
-- Keeping this transition rule in the database prevents a future role or owner
-- tool from pointing a task backwards to historical evidence.
-- +goose StatementBegin
CREATE FUNCTION task_run_snapshot_v2_cutover_pointer_transition()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    tenant_context TEXT;
    old_action TEXT;
    old_generation BIGINT;
    new_action TEXT;
    new_generation BIGINT;
    new_reverts_event_id BIGINT;
    new_definition_version BIGINT;
    new_definition_digest TEXT;
    prior_scope_generation BIGINT;
BEGIN
    tenant_context := current_setting('app.tenant_id', true);
    IF tenant_context IS NULL OR tenant_context !~ '^[1-9][0-9]*$' OR
       tenant_context::bigint IS DISTINCT FROM NEW.tenant_id THEN
        RAISE EXCEPTION 'task run snapshot v2 fence tenant context is invalid'
            USING ERRCODE = '42501';
    END IF;
    IF NEW.run_snapshot_cutover_event_id IS NULL THEN
        RAISE EXCEPTION 'task run snapshot v2 cutover pointer cannot be cleared'
            USING ERRCODE = '23514';
    END IF;
    IF OLD.run_snapshot_cutover_event_id IS NOT NULL THEN
        SELECT e.action, e.generation
          INTO old_action, old_generation
          FROM public.task_run_snapshot_v2_cutover_events e
         WHERE e.id = OLD.run_snapshot_cutover_event_id
           AND e.tenant_id = OLD.tenant_id
           AND e.user_id = OLD.user_id
           AND e.task_id = OLD.id;
    END IF;
    SELECT e.action, e.generation, e.reverts_event_id,
           e.approved_definition_version, e.approved_definition_digest
      INTO new_action, new_generation, new_reverts_event_id,
           new_definition_version, new_definition_digest
      FROM public.task_run_snapshot_v2_cutover_events e
     WHERE e.id = NEW.run_snapshot_cutover_event_id
       AND e.tenant_id = NEW.tenant_id
       AND e.user_id = NEW.user_id
       AND e.task_id = NEW.id;
    SELECT COALESCE(MAX(e.generation), 0)
      INTO prior_scope_generation
      FROM public.task_run_snapshot_v2_cutover_events e
     WHERE e.tenant_id = NEW.tenant_id
       AND e.user_id = NEW.user_id
       AND e.task_id = NEW.id
       AND e.id <> NEW.run_snapshot_cutover_event_id;

    IF new_action IS NULL OR
       (OLD.run_snapshot_cutover_event_id IS NULL AND
        (new_action <> 'activate' OR
         new_generation <> prior_scope_generation + 1)) OR
       (old_action = 'activate' AND
        (new_action <> 'rollback' OR new_generation <> old_generation + 1 OR
         new_reverts_event_id IS DISTINCT FROM
             OLD.run_snapshot_cutover_event_id)) OR
       (old_action = 'rollback' AND
        (new_action <> 'activate' OR new_generation <> old_generation + 1)) OR
       (OLD.run_snapshot_cutover_event_id IS NOT NULL AND
        old_action NOT IN ('activate', 'rollback')) THEN
        RAISE EXCEPTION 'task run snapshot v2 cutover pointer transition is invalid'
            USING ERRCODE = '23514';
    END IF;
    IF new_action = 'activate' AND
       (NEW.execution_mode <> 'compiled' OR
        NEW.approved_definition_version IS DISTINCT FROM
            new_definition_version OR
        NEW.approved_definition_digest IS DISTINCT FROM
            new_definition_digest) THEN
        RAISE EXCEPTION 'task run snapshot v2 activation pin is not current'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION task_run_snapshot_v2_cutover_pointer_transition()
    FROM PUBLIC;

CREATE TRIGGER task_run_snapshot_v2_cutover_pointer_transition
BEFORE UPDATE OF run_snapshot_cutover_event_id ON schedules
FOR EACH ROW
WHEN (
    OLD.run_snapshot_cutover_event_id IS DISTINCT FROM
        NEW.run_snapshot_cutover_event_id
)
EXECUTE FUNCTION task_run_snapshot_v2_cutover_pointer_transition();

-- A stale binary omits v2_cutover_event_id. Once a future operator transaction
-- points the exact schedule at an activation whose definition pin still
-- matches the current head, that omission must fail at the database boundary.
-- This trigger is intentionally SECURITY INVOKER: the pre-037 owner binary has
-- no tenant GUC and must remain compatible while the pointer is NULL, while a
-- future restricted vane_app caller must keep its existing RLS visibility.
-- +goose StatementBegin
CREATE FUNCTION task_run_snapshot_v2_admission_fence()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
DECLARE
    schedule_event_pointer BIGINT;
    current_event_id BIGINT;
    current_action TEXT;
    current_definition_version BIGINT;
    current_definition_digest TEXT;
    current_high_watermark BIGINT;
    schedule_definition_version BIGINT;
    schedule_definition_digest TEXT;
    schedule_execution_mode TEXT;
BEGIN
    SELECT s.run_snapshot_cutover_event_id, e.id, e.action,
           e.approved_definition_version, e.approved_definition_digest,
           e.snapshot_high_watermark,
           s.approved_definition_version, s.approved_definition_digest,
           s.execution_mode
      INTO schedule_event_pointer, current_event_id, current_action,
           current_definition_version, current_definition_digest,
           current_high_watermark,
           schedule_definition_version, schedule_definition_digest,
           schedule_execution_mode
      FROM public.schedules s
      LEFT JOIN public.task_run_snapshot_v2_cutover_events e
        ON e.id = s.run_snapshot_cutover_event_id
       AND e.tenant_id = s.tenant_id
       AND e.user_id = s.user_id
       AND e.task_id = s.id
     WHERE s.tenant_id = NEW.tenant_id
       AND s.user_id = NEW.user_id
       AND s.id = NEW.task_id
     FOR SHARE OF s;

    IF schedule_execution_mode IS NULL OR
       (schedule_event_pointer IS NOT NULL AND current_event_id IS NULL) THEN
        RAISE EXCEPTION
            'task run snapshot v2 admission state is invalid'
            USING ERRCODE = '23514';
    ELSIF schedule_event_pointer IS NULL THEN
        IF NEW.v2_cutover_event_id IS NOT NULL THEN
            RAISE EXCEPTION
                'task run snapshot v2 admission marker is not eligible'
                USING ERRCODE = '23514';
        END IF;
    ELSIF current_action = 'rollback' THEN
        IF NEW.v2_cutover_event_id IS NOT NULL THEN
            RAISE EXCEPTION
                'task run snapshot v2 admission marker is not eligible'
                USING ERRCODE = '23514';
        END IF;
    ELSIF current_action = 'activate' THEN
        IF schedule_execution_mode <> 'compiled' OR
           schedule_definition_version IS DISTINCT FROM
               current_definition_version OR
           schedule_definition_digest IS DISTINCT FROM
               current_definition_digest THEN
            RAISE EXCEPTION
                'task run snapshot v2 active definition pin drifted'
                USING ERRCODE = '23514';
        END IF;
        IF NEW.v2_cutover_event_id IS DISTINCT FROM current_event_id OR
           NEW.id <= current_high_watermark THEN
            RAISE EXCEPTION
                'task run snapshot v2 admission fence rejected stale writer'
                USING ERRCODE = '23514';
        END IF;
    ELSE
        RAISE EXCEPTION
            'task run snapshot v2 admission state is invalid'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION task_run_snapshot_v2_admission_fence() FROM PUBLIC;

CREATE TRIGGER task_run_snapshot_v2_admission_fence
BEFORE INSERT ON task_run_snapshots
FOR EACH ROW
EXECUTE FUNCTION task_run_snapshot_v2_admission_fence();

-- The parent is inserted before its sidecar. A deferred trigger checks the
-- complete transaction at commit, so a marked parent can never survive without
-- one exact match sidecar bound to the same activation pin and watermark.
-- +goose StatementBegin
CREATE FUNCTION task_run_snapshot_v2_marker_integrity()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
DECLARE
    event_action TEXT;
    event_definition_version BIGINT;
    event_definition_digest TEXT;
    event_high_watermark BIGINT;
    shadow_count BIGINT;
BEGIN
    IF NEW.v2_cutover_event_id IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT e.action, e.approved_definition_version,
           e.approved_definition_digest, e.snapshot_high_watermark
      INTO event_action, event_definition_version,
           event_definition_digest, event_high_watermark
      FROM public.task_run_snapshot_v2_cutover_events e
     WHERE e.id = NEW.v2_cutover_event_id
       AND e.tenant_id = NEW.tenant_id
       AND e.user_id = NEW.user_id
       AND e.task_id = NEW.task_id;

    IF event_action IS DISTINCT FROM 'activate' OR
       NEW.id <= event_high_watermark THEN
        RAISE EXCEPTION
            'task run snapshot v2 marker references an invalid activation'
            USING ERRCODE = '23514';
    END IF;

    SELECT count(*)
      INTO shadow_count
      FROM public.task_run_snapshot_v2_shadows sh
     WHERE sh.run_snapshot_id = NEW.id
       AND sh.tenant_id = NEW.tenant_id
       AND sh.user_id = NEW.user_id
       AND sh.task_id = NEW.task_id
       AND sh.temporal_workflow_id = NEW.temporal_workflow_id
       AND sh.temporal_run_id = NEW.temporal_run_id
       AND sh.status = 'match'
       AND sh.approved_definition_version = event_definition_version
       AND sh.approved_definition_digest = event_definition_digest
       AND convert_from(sh.payload, 'UTF8')::jsonb
            #>> '{legacy,payload_digest}' = NEW.payload_digest
       AND convert_from(sh.payload, 'UTF8')::jsonb
            #> '{legacy,payload}' = convert_from(NEW.payload, 'UTF8')::jsonb;

    IF shadow_count <> 1 THEN
        RAISE EXCEPTION
            'task run snapshot v2 marker has no exact match sidecar'
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION task_run_snapshot_v2_marker_integrity() FROM PUBLIC;

CREATE CONSTRAINT TRIGGER task_run_snapshot_v2_marker_integrity
AFTER INSERT ON task_run_snapshots
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION task_run_snapshot_v2_marker_integrity();

GRANT SELECT ON task_run_snapshot_v2_cutover_events TO vane_app;

ALTER TABLE task_run_snapshot_v2_cutover_events ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON task_run_snapshot_v2_cutover_events
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON task_run_snapshot_v2_cutover_events AS RESTRICTIVE
    FOR ALL
    USING (tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint);

-- +goose Down

LOCK TABLE schedules, task_run_snapshots,
    task_run_snapshot_v2_shadows, task_run_snapshot_v2_cutover_events
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM schedules
         WHERE run_snapshot_cutover_event_id IS NOT NULL
    ) OR EXISTS (
        SELECT 1 FROM task_run_snapshots
         WHERE v2_cutover_event_id IS NOT NULL
    ) OR EXISTS (
        SELECT 1 FROM task_run_snapshot_v2_cutover_events
    ) THEN
        RAISE EXCEPTION
            '037: refusing downgrade while snapshot v2 cutover state exists';
    END IF;
END $$;
-- +goose StatementEnd

DROP POLICY IF EXISTS tenant_isolation
    ON task_run_snapshot_v2_cutover_events;
DROP POLICY IF EXISTS tenant_visible
    ON task_run_snapshot_v2_cutover_events;
ALTER TABLE task_run_snapshot_v2_cutover_events DISABLE ROW LEVEL SECURITY;
REVOKE ALL ON task_run_snapshot_v2_cutover_events FROM vane_app;

DROP TRIGGER task_run_snapshot_v2_cutover_event_integrity
    ON task_run_snapshot_v2_cutover_events;
DROP FUNCTION task_run_snapshot_v2_cutover_event_integrity();
DROP TRIGGER task_run_snapshot_v2_cutover_pointer_transition ON schedules;
DROP FUNCTION task_run_snapshot_v2_cutover_pointer_transition();
DROP TRIGGER task_run_snapshot_v2_marker_integrity ON task_run_snapshots;
DROP FUNCTION task_run_snapshot_v2_marker_integrity();
DROP TRIGGER task_run_snapshot_v2_admission_fence ON task_run_snapshots;
DROP FUNCTION task_run_snapshot_v2_admission_fence();

ALTER TABLE task_run_snapshots
    DROP CONSTRAINT fk_task_run_snapshots_v2_cutover_event,
    DROP COLUMN v2_cutover_event_id;
ALTER TABLE schedules
    DROP CONSTRAINT fk_schedules_run_snapshot_cutover_event,
    DROP COLUMN run_snapshot_cutover_event_id;
DROP POLICY tenant_isolation ON schedules;
CREATE POLICY tenant_isolation ON schedules AS RESTRICTIVE
    FOR ALL
    USING (tenant_id IS NOT DISTINCT FROM
           (SELECT current_setting('app.tenant_id', true))::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
                (SELECT current_setting('app.tenant_id', true))::bigint);
DROP TABLE task_run_snapshot_v2_cutover_events;
