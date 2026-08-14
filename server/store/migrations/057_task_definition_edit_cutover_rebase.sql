-- 057: atomically rebase retained-v2 snapshot authority when an Approved
-- Definition edit advances the task head.

-- +goose Up

LOCK TABLE schedules, task_definition_edit_operations,
    task_run_snapshots, task_run_snapshot_v2_shadows,
    task_run_snapshot_v2_cutover_events IN ACCESS EXCLUSIVE MODE;

ALTER TABLE task_run_snapshot_v2_cutover_events
    ADD COLUMN definition_edit_operation_id TEXT,
    ADD CONSTRAINT task_run_snapshot_v2_cutover_definition_edit_valid CHECK (
        definition_edit_operation_id IS NULL OR (
            definition_edit_operation_id <> '' AND
            btrim(definition_edit_operation_id) =
                definition_edit_operation_id AND
            octet_length(definition_edit_operation_id) <= 512
        )
    );

CREATE UNIQUE INDEX uq_task_run_snapshot_v2_cutover_definition_edit_action
    ON task_run_snapshot_v2_cutover_events (
        definition_edit_operation_id, action
    )
    WHERE definition_edit_operation_id IS NOT NULL;

GRANT SELECT ON task_run_snapshots, task_run_snapshot_v2_shadows,
    task_run_snapshot_v2_cutover_events TO vane_edit_coordinator;

-- The pointer state machine also owns the edit/cutover exclusion boundary.
-- An edit marker accepts only events produced for that exact durable
-- operation. Outside an edit, operation-owned events cannot be installed.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION task_run_snapshot_v2_cutover_pointer_transition()
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
    new_definition_edit_operation_id TEXT;
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
           e.approved_definition_version, e.approved_definition_digest,
           e.definition_edit_operation_id
      INTO new_action, new_generation, new_reverts_event_id,
           new_definition_version, new_definition_digest,
           new_definition_edit_operation_id
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
    IF (NEW.definition_edit_operation_id IS NULL AND
        new_definition_edit_operation_id IS NOT NULL) OR
       (NEW.definition_edit_operation_id IS NOT NULL AND
        new_definition_edit_operation_id IS DISTINCT FROM
            NEW.definition_edit_operation_id) THEN
        RAISE EXCEPTION
            'task run snapshot v2 cutover pointer conflicts with definition edit'
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

-- Validate the transaction's final state, not a queued trigger invocation's
-- stale NEW value. This permits the edit transaction's deliberate temporary
-- head/pointer mismatch but makes it impossible to commit that mismatch.
-- +goose StatementBegin
CREATE FUNCTION task_run_snapshot_v2_active_pin_final_integrity()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    schedule_mode TEXT;
    schedule_version BIGINT;
    schedule_digest TEXT;
    pointer_action TEXT;
    pointer_version BIGINT;
    pointer_digest TEXT;
BEGIN
    SELECT s.execution_mode, s.approved_definition_version,
           s.approved_definition_digest, e.action,
           e.approved_definition_version, e.approved_definition_digest
      INTO schedule_mode, schedule_version, schedule_digest, pointer_action,
           pointer_version, pointer_digest
      FROM public.schedules s
      LEFT JOIN public.task_run_snapshot_v2_cutover_events e
        ON e.id = s.run_snapshot_cutover_event_id
       AND e.tenant_id = s.tenant_id
       AND e.user_id = s.user_id
       AND e.task_id = s.id
     WHERE s.tenant_id = NEW.tenant_id
       AND s.user_id = NEW.user_id
       AND s.id = NEW.id;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    IF NEW.run_snapshot_cutover_event_id IS NOT NULL AND
       pointer_action IS NULL THEN
        RAISE EXCEPTION 'task run snapshot v2 cutover pointer is corrupt'
            USING ERRCODE = '23514';
    END IF;
    IF pointer_action = 'activate' AND (
        schedule_mode IS DISTINCT FROM 'compiled' OR
        schedule_version IS DISTINCT FROM pointer_version OR
        schedule_digest IS DISTINCT FROM pointer_digest
    ) THEN
        RAISE EXCEPTION 'task run snapshot v2 active definition pin drifted'
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION task_run_snapshot_v2_active_pin_final_integrity()
    FROM PUBLIC;

CREATE CONSTRAINT TRIGGER task_run_snapshot_v2_active_pin_final_integrity
AFTER INSERT OR UPDATE ON schedules
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION task_run_snapshot_v2_active_pin_final_integrity();

-- All mutable proof is derived after locking the operation and schedule.
-- The caller supplies only its current lease capability. Exact replay returns
-- the already committed event pair and never appends another generation.
-- +goose StatementBegin
CREATE FUNCTION task_run_snapshot_v2_rebase_definition_edit(
    requested_operation_id TEXT,
    requested_fence BIGINT,
    requested_lease_owner TEXT
)
RETURNS TABLE (
    rebased BOOLEAN,
    rollback_event_id BIGINT,
    activate_event_id BIGINT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    tenant_context TEXT;
    op_tenant_id BIGINT;
    op_user_id BIGINT;
    op_task_id TEXT;
    op_status TEXT;
    op_phase TEXT;
    op_base_version BIGINT;
    op_base_digest TEXT;
    op_target_version BIGINT;
    op_target_digest TEXT;
    op_lease_owner TEXT;
    op_lease_until TIMESTAMPTZ;
    op_fence BIGINT;
    schedule_pointer BIGINT;
    schedule_status TEXT;
    schedule_mode TEXT;
    schedule_version BIGINT;
    schedule_digest TEXT;
    schedule_operation_id TEXT;
    schedule_fence BIGINT;
    current_action TEXT;
    current_generation BIGINT;
    current_operation_id TEXT;
    current_version BIGINT;
    current_digest TEXT;
    current_high_watermark BIGINT;
    current_audit_from BIGINT;
    current_audit_count BIGINT;
    current_audit_through BIGINT;
    existing_rollback BIGINT;
    existing_activate BIGINT;
    old_activate BIGINT;
    old_activate_generation BIGINT;
    rollback_generation BIGINT;
    rollback_reverts BIGINT;
    rollback_version BIGINT;
    rollback_digest TEXT;
    rollback_high_watermark BIGINT;
    rollback_audit_from BIGINT;
    rollback_audit_count BIGINT;
    rollback_audit_through BIGINT;
    activate_generation BIGINT;
    activate_version BIGINT;
    activate_digest TEXT;
    frozen_from BIGINT;
    frozen_high_watermark BIGINT;
    frozen_count BIGINT;
    frozen_all_exact BOOLEAN;
    next_generation BIGINT;
    changed BIGINT;
BEGIN
    IF current_setting('role', true) IS DISTINCT FROM
       'vane_edit_coordinator' THEN
        RAISE EXCEPTION
            'definition edit snapshot rebase requires coordinator role'
            USING ERRCODE = '42501';
    END IF;
    tenant_context := current_setting('app.tenant_id', true);
    IF tenant_context IS NULL OR tenant_context !~ '^[1-9][0-9]*$' OR
       requested_operation_id IS NULL OR requested_operation_id = '' OR
       btrim(requested_operation_id) <> requested_operation_id OR
       octet_length(requested_operation_id) > 512 OR
       requested_fence IS NULL OR requested_fence <= 0 OR
       requested_lease_owner IS NULL OR requested_lease_owner = '' OR
       btrim(requested_lease_owner) <> requested_lease_owner OR
       octet_length(requested_lease_owner) > 255 THEN
        RAISE EXCEPTION 'definition edit snapshot rebase request is invalid'
            USING ERRCODE = '22023';
    END IF;

    SELECT o.target_tenant_id, o.target_user_id, o.task_id, o.status, o.phase,
           o.base_definition_version, o.base_definition_digest,
           o.target_definition_version, o.target_definition_digest,
           o.lease_owner, o.lease_until, o.fence
      INTO op_tenant_id, op_user_id, op_task_id, op_status, op_phase,
           op_base_version, op_base_digest, op_target_version, op_target_digest,
           op_lease_owner, op_lease_until, op_fence
      FROM public.task_definition_edit_operations o
     WHERE o.id = requested_operation_id
       AND o.tenant_id = tenant_context::bigint
       AND o.tombstoned_at IS NULL
     FOR UPDATE;
    IF NOT FOUND OR op_tenant_id IS DISTINCT FROM tenant_context::bigint OR
       op_status IS DISTINCT FROM 'executing' OR
       op_phase NOT IN (
           'temporal_base_paused', 'definition_committed',
           'temporal_target_applied', 'temporal_target_restored'
       ) OR op_lease_owner IS DISTINCT FROM requested_lease_owner OR
       op_fence IS DISTINCT FROM requested_fence OR
       op_lease_until IS NULL OR op_lease_until <= clock_timestamp() THEN
        RAISE EXCEPTION 'definition edit snapshot rebase lease is invalid'
            USING ERRCODE = '40001';
    END IF;

    SELECT s.run_snapshot_cutover_event_id, s.status, s.execution_mode,
           s.approved_definition_version, s.approved_definition_digest,
           s.definition_edit_operation_id, s.definition_edit_fence
      INTO schedule_pointer, schedule_status, schedule_mode,
           schedule_version, schedule_digest,
           schedule_operation_id, schedule_fence
      FROM public.schedules s
     WHERE s.tenant_id = op_tenant_id
       AND s.user_id = op_user_id
       AND s.id = op_task_id
     FOR UPDATE;
    IF NOT FOUND OR schedule_status IS DISTINCT FROM 'paused' OR
       schedule_mode IS DISTINCT FROM 'compiled' OR
       schedule_version IS DISTINCT FROM op_target_version OR
       schedule_digest IS DISTINCT FROM op_target_digest OR
       schedule_operation_id IS DISTINCT FROM requested_operation_id OR
       schedule_fence IS DISTINCT FROM requested_fence THEN
        RAISE EXCEPTION 'definition edit snapshot rebase schedule is invalid'
            USING ERRCODE = '23514';
    END IF;

    IF schedule_pointer IS NOT NULL THEN
        SELECT e.action, e.generation, e.definition_edit_operation_id,
               e.approved_definition_version, e.approved_definition_digest,
               e.snapshot_high_watermark, e.audit_from_snapshot_id,
               e.audit_count, e.audit_through_id
          INTO current_action, current_generation, current_operation_id,
               current_version, current_digest, current_high_watermark,
               current_audit_from, current_audit_count, current_audit_through
          FROM public.task_run_snapshot_v2_cutover_events e
         WHERE e.id = schedule_pointer
           AND e.tenant_id = op_tenant_id
           AND e.user_id = op_user_id
           AND e.task_id = op_task_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'definition edit snapshot rebase pointer is corrupt'
                USING ERRCODE = '23514';
        END IF;
    END IF;

    SELECT
        max(e.id) FILTER (WHERE e.action = 'rollback'),
        max(e.id) FILTER (WHERE e.action = 'activate')
      INTO existing_rollback, existing_activate
      FROM public.task_run_snapshot_v2_cutover_events e
     WHERE e.definition_edit_operation_id = requested_operation_id;

    IF current_action = 'activate' AND
       current_operation_id IS NOT DISTINCT FROM requested_operation_id THEN
        IF existing_rollback IS NULL OR
           existing_activate IS DISTINCT FROM schedule_pointer THEN
            RAISE EXCEPTION
                'definition edit snapshot rebase replay pair is incomplete'
                USING ERRCODE = '23514';
        END IF;
        SELECT e.generation, e.reverts_event_id,
               e.approved_definition_version, e.approved_definition_digest,
               e.snapshot_high_watermark, e.audit_from_snapshot_id,
               e.audit_count, e.audit_through_id
          INTO rollback_generation, rollback_reverts,
               rollback_version, rollback_digest, rollback_high_watermark,
               rollback_audit_from, rollback_audit_count,
               rollback_audit_through
          FROM public.task_run_snapshot_v2_cutover_events e
         WHERE e.id = existing_rollback
           AND e.tenant_id = op_tenant_id
           AND e.user_id = op_user_id
           AND e.task_id = op_task_id
           AND e.action = 'rollback';
        SELECT e.generation, e.approved_definition_version,
               e.approved_definition_digest
          INTO activate_generation, activate_version, activate_digest
          FROM public.task_run_snapshot_v2_cutover_events e
         WHERE e.id = existing_activate
           AND e.tenant_id = op_tenant_id
           AND e.user_id = op_user_id
           AND e.task_id = op_task_id
           AND e.action = 'activate';
        SELECT e.id, e.generation
          INTO old_activate, old_activate_generation
          FROM public.task_run_snapshot_v2_cutover_events e
         WHERE e.id = rollback_reverts
           AND e.tenant_id = op_tenant_id
           AND e.user_id = op_user_id
           AND e.task_id = op_task_id
           AND e.action = 'activate'
           AND e.approved_definition_version = op_base_version
           AND e.approved_definition_digest = op_base_digest
           AND e.snapshot_high_watermark = rollback_high_watermark
           AND e.audit_from_snapshot_id = rollback_audit_from
           AND e.audit_count = rollback_audit_count
           AND e.audit_through_id = rollback_audit_through;
        IF old_activate IS NULL OR
           rollback_version IS DISTINCT FROM op_base_version OR
           rollback_digest IS DISTINCT FROM op_base_digest OR
           rollback_generation IS DISTINCT FROM old_activate_generation + 1 OR
           activate_generation IS DISTINCT FROM rollback_generation + 1 OR
           activate_version IS DISTINCT FROM op_target_version OR
           activate_digest IS DISTINCT FROM op_target_digest OR
           current_version IS DISTINCT FROM op_target_version OR
           current_digest IS DISTINCT FROM op_target_digest THEN
            RAISE EXCEPTION
                'definition edit snapshot rebase replay pair is invalid'
                USING ERRCODE = '23514';
        END IF;
        RETURN QUERY SELECT TRUE, existing_rollback, existing_activate;
        RETURN;
    END IF;

    IF existing_rollback IS NOT NULL OR existing_activate IS NOT NULL THEN
        RAISE EXCEPTION 'definition edit snapshot rebase has partial history'
            USING ERRCODE = '23514';
    END IF;
    -- A pre-057 worker may already have committed the target head, followed
    -- by the restricted runtime operator's ordinary rollback+activate repair.
    -- That chain has no operation provenance, but an exact target pin plus the
    -- complete Go typed audit performed by the caller is already converged.
    IF current_action = 'activate' AND
       current_version IS NOT DISTINCT FROM op_target_version AND
       current_digest IS NOT DISTINCT FROM op_target_digest THEN
        RETURN QUERY SELECT FALSE, NULL::BIGINT, NULL::BIGINT;
        RETURN;
    END IF;
    IF schedule_pointer IS NULL OR current_action = 'rollback' THEN
        RETURN QUERY SELECT FALSE, NULL::BIGINT, NULL::BIGINT;
        RETURN;
    END IF;
    IF current_action IS DISTINCT FROM 'activate' OR
       current_version IS DISTINCT FROM op_base_version OR
       current_digest IS DISTINCT FROM op_base_digest THEN
        RAISE EXCEPTION 'definition edit snapshot rebase base pin is invalid'
            USING ERRCODE = '23514';
    END IF;

    SELECT MIN(p.id), MAX(p.id), COUNT(*),
           COALESCE(bool_and(
               public.task_run_snapshot_v2_cutover_row_exact(p.id)
           ), FALSE)
      INTO frozen_from, frozen_high_watermark, frozen_count, frozen_all_exact
      FROM public.task_run_snapshots p
     WHERE p.tenant_id = op_tenant_id
       AND p.user_id = op_user_id
       AND p.task_id = op_task_id;
    IF frozen_count <= 0 OR frozen_from IS NULL OR
       frozen_high_watermark IS NULL OR NOT frozen_all_exact THEN
        RAISE EXCEPTION 'definition edit snapshot rebase full audit failed'
            USING ERRCODE = '23514';
    END IF;

    SELECT COALESCE(MAX(e.generation), 0) + 1
      INTO next_generation
      FROM public.task_run_snapshot_v2_cutover_events e
     WHERE e.tenant_id = op_tenant_id
       AND e.user_id = op_user_id
       AND e.task_id = op_task_id;

    INSERT INTO public.task_run_snapshot_v2_cutover_events (
        tenant_id, user_id, task_id, generation, action, reverts_event_id,
        approved_definition_version, approved_definition_digest,
        snapshot_high_watermark, audit_from_snapshot_id, audit_count,
        audit_through_id, definition_edit_operation_id
    ) VALUES (
        op_tenant_id, op_user_id, op_task_id, next_generation, 'rollback',
        schedule_pointer, current_version, current_digest,
        current_high_watermark, current_audit_from, current_audit_count,
        current_audit_through, requested_operation_id
    ) RETURNING id INTO existing_rollback;

    UPDATE public.schedules s
       SET run_snapshot_cutover_event_id = existing_rollback
     WHERE s.tenant_id = op_tenant_id
       AND s.user_id = op_user_id
       AND s.id = op_task_id
       AND s.run_snapshot_cutover_event_id = schedule_pointer
       AND s.definition_edit_operation_id = requested_operation_id
       AND s.definition_edit_fence = requested_fence;
    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> 1 THEN
        RAISE EXCEPTION 'definition edit snapshot rollback pointer CAS failed'
            USING ERRCODE = '40001';
    END IF;

    INSERT INTO public.task_run_snapshot_v2_cutover_events (
        tenant_id, user_id, task_id, generation, action,
        approved_definition_version, approved_definition_digest,
        snapshot_high_watermark, audit_from_snapshot_id, audit_count,
        audit_through_id, definition_edit_operation_id
    ) VALUES (
        op_tenant_id, op_user_id, op_task_id, next_generation + 1, 'activate',
        op_target_version, op_target_digest, frozen_high_watermark,
        frozen_from, frozen_count, frozen_high_watermark,
        requested_operation_id
    ) RETURNING id INTO existing_activate;

    UPDATE public.schedules s
       SET run_snapshot_cutover_event_id = existing_activate
     WHERE s.tenant_id = op_tenant_id
       AND s.user_id = op_user_id
       AND s.id = op_task_id
       AND s.run_snapshot_cutover_event_id = existing_rollback
       AND s.definition_edit_operation_id = requested_operation_id
       AND s.definition_edit_fence = requested_fence;
    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> 1 THEN
        RAISE EXCEPTION 'definition edit snapshot activation pointer CAS failed'
            USING ERRCODE = '40001';
    END IF;

    RETURN QUERY SELECT TRUE, existing_rollback, existing_activate;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION task_run_snapshot_v2_rebase_definition_edit(
    TEXT, BIGINT, TEXT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION task_run_snapshot_v2_rebase_definition_edit(
    TEXT, BIGINT, TEXT
) TO vane_edit_coordinator;

-- Existing active rows must already satisfy the new commit invariant.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM schedules s
          LEFT JOIN task_run_snapshot_v2_cutover_events e
            ON e.id = s.run_snapshot_cutover_event_id
           AND e.tenant_id = s.tenant_id
           AND e.user_id = s.user_id
           AND e.task_id = s.id
         WHERE (
               s.run_snapshot_cutover_event_id IS NOT NULL AND e.id IS NULL
           ) OR (
               e.action = 'activate' AND (
                   s.execution_mode IS DISTINCT FROM 'compiled' OR
                   s.approved_definition_version IS DISTINCT FROM
                       e.approved_definition_version OR
                   s.approved_definition_digest IS DISTINCT FROM
                       e.approved_definition_digest
               )
           )
    ) THEN
        RAISE EXCEPTION '057: refusing migration with an active pin drift';
    END IF;
END $$;
-- +goose StatementEnd

-- +goose Down

LOCK TABLE schedules, task_definition_edit_operations,
    task_run_snapshots, task_run_snapshot_v2_shadows,
    task_run_snapshot_v2_cutover_events IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM task_run_snapshot_v2_cutover_events
         WHERE definition_edit_operation_id IS NOT NULL
    ) THEN
        RAISE EXCEPTION
            '057: refusing downgrade while definition-edit cutover history exists';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE EXECUTE ON FUNCTION task_run_snapshot_v2_rebase_definition_edit(
    TEXT, BIGINT, TEXT
) FROM vane_edit_coordinator;
DROP FUNCTION task_run_snapshot_v2_rebase_definition_edit(
    TEXT, BIGINT, TEXT
);

DROP TRIGGER task_run_snapshot_v2_active_pin_final_integrity ON schedules;
DROP FUNCTION task_run_snapshot_v2_active_pin_final_integrity();

-- Restore migration 037's pointer transition function.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION task_run_snapshot_v2_cutover_pointer_transition()
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

REVOKE SELECT ON task_run_snapshots, task_run_snapshot_v2_shadows,
    task_run_snapshot_v2_cutover_events FROM vane_edit_coordinator;

DROP INDEX uq_task_run_snapshot_v2_cutover_definition_edit_action;
ALTER TABLE task_run_snapshot_v2_cutover_events
    DROP CONSTRAINT task_run_snapshot_v2_cutover_definition_edit_valid,
    DROP COLUMN definition_edit_operation_id;
