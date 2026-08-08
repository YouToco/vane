-- +goose Up

-- A Research V3 cutover deliberately keeps the preceding V2 activation
-- pointer immutable.  While discover_at_run is active the pointer is dormant:
-- the V2 admission trigger rejects legacy snapshots, and an exact V3 rollback
-- can restore the compiled head without manufacturing a new V2 generation.
-- The old deferred check treated that safe, reversible state as drift.
--
-- Only a retained compiled v1 pin may become dormant, and only behind a
-- canonical V3 production head.  Every other active-pointer mismatch remains
-- fail-closed.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION task_run_snapshot_v2_active_pin_final_integrity()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    schedule_mode TEXT;
    schedule_version BIGINT;
    schedule_digest TEXT;
    schedule_schema TEXT;
    schedule_definition_mode TEXT;
    pointer_action TEXT;
    pointer_version BIGINT;
    pointer_digest TEXT;
    pointer_schema TEXT;
    pointer_definition_mode TEXT;
BEGIN
    SELECT s.execution_mode, s.approved_definition_version,
           s.approved_definition_digest,
           current_definition.schema_version,
           current_definition.execution_mode,
           e.action, e.approved_definition_version,
           e.approved_definition_digest,
           pointer_definition.schema_version,
           pointer_definition.execution_mode
      INTO schedule_mode, schedule_version, schedule_digest,
           schedule_schema, schedule_definition_mode,
           pointer_action, pointer_version, pointer_digest,
           pointer_schema, pointer_definition_mode
      FROM public.schedules s
      LEFT JOIN public.task_run_snapshot_v2_cutover_events e
        ON e.id = s.run_snapshot_cutover_event_id
       AND e.tenant_id = s.tenant_id
       AND e.user_id = s.user_id
       AND e.task_id = s.id
      LEFT JOIN public.task_approved_definition_versions current_definition
        ON current_definition.tenant_id = s.tenant_id
       AND current_definition.user_id = s.user_id
       AND current_definition.task_id = s.id
       AND current_definition.version = s.approved_definition_version
       AND current_definition.definition_digest = s.approved_definition_digest
      LEFT JOIN public.task_approved_definition_versions pointer_definition
        ON pointer_definition.tenant_id = s.tenant_id
       AND pointer_definition.user_id = s.user_id
       AND pointer_definition.task_id = s.id
       AND pointer_definition.version = e.approved_definition_version
       AND pointer_definition.definition_digest = e.approved_definition_digest
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
    IF pointer_action = 'activate' THEN
        IF schedule_mode = 'compiled' THEN
            IF schedule_version IS DISTINCT FROM pointer_version OR
               schedule_digest IS DISTINCT FROM pointer_digest THEN
                RAISE EXCEPTION 'task run snapshot v2 active definition pin drifted'
                    USING ERRCODE = '23514';
            END IF;
        ELSIF schedule_mode = 'discover_at_run' THEN
            IF schedule_schema IS DISTINCT FROM
                   'vane.task-approved-definition/v3' OR
               schedule_definition_mode IS DISTINCT FROM
                   'discover_at_run' OR
               pointer_schema IS DISTINCT FROM
                   'vane.task-approved-definition/v1' OR
               pointer_definition_mode IS DISTINCT FROM 'compiled' THEN
                RAISE EXCEPTION 'task run snapshot v2 dormant definition pin is invalid'
                    USING ERRCODE = '23514';
            END IF;
        ELSE
            RAISE EXCEPTION 'task run snapshot v2 active definition pin drifted'
                USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NULL;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION task_run_snapshot_v2_active_pin_final_integrity()
    FROM PUBLIC;

-- +goose Down

-- Downgrade would make every dormant V2 pin behind an active V3 task
-- uncommittable on its next schedule update.  Refuse it until those tasks have
-- been rolled back through the durable V3 coordinator.
-- Serialize the check and function replacement against every schedule writer;
-- otherwise a cutover could create the first dormant pin after the check.
LOCK TABLE schedules IN SHARE ROW EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM schedules s
          JOIN task_run_snapshot_v2_cutover_events e
            ON e.id = s.run_snapshot_cutover_event_id
           AND e.tenant_id = s.tenant_id
           AND e.user_id = s.user_id
           AND e.task_id = s.id
         WHERE s.execution_mode = 'discover_at_run'
           AND e.action = 'activate'
    ) THEN
        RAISE EXCEPTION
            '121: rollback Research V3 tasks with dormant V2 pins before downgrade';
    END IF;
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION task_run_snapshot_v2_active_pin_final_integrity()
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
