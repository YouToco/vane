-- 079: expose only the exact manual-run authorization fact to runtime roles.
--
-- schedule_commands remains unreadable to vane_app. This function is the
-- narrow bridge: callers can ask whether one workflow ID is bound to one
-- owner-scoped run command, but cannot enumerate command history or payloads.

-- +goose Up

CREATE FUNCTION authorize_manual_task_run_v1(
    expected_tenant_id BIGINT,
    expected_user_id BIGINT,
    expected_task_id TEXT,
    expected_workflow_id TEXT
) RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
SET row_security = on
AS $$
    SELECT EXISTS (
        SELECT 1
          FROM public.schedule_commands c
         WHERE expected_workflow_id = 'wf-manual-' || c.id::text
           AND c.tenant_id = expected_tenant_id
           AND c.user_id = expected_user_id
           AND c.task_id = expected_task_id
           AND c.kind = 'run'
           AND c.status IN ('pending', 'completed')
    )
$$;

REVOKE ALL ON FUNCTION authorize_manual_task_run_v1(
    BIGINT, BIGINT, TEXT, TEXT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION authorize_manual_task_run_v1(
    BIGINT, BIGINT, TEXT, TEXT
) TO vane_app, vane_push_effect_coordinator;

-- The existing Tool snapshot trigger intentionally admits only active tasks.
-- Extend that same invariant with one exact exception: a paused task whose
-- workflow identity is backed by the owner-scoped command fact above.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION task_run_snapshot_v2_admission_fence()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
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
    schedule_status TEXT;
    approved_schema_version TEXT;
    adaptive_version BIGINT;
    adaptive_schema_version TEXT;
    adaptive_basis_version BIGINT;
    adaptive_basis_digest TEXT;
    adaptive_last_known_good_definition_version BIGINT;
BEGIN
    SELECT s.run_snapshot_cutover_event_id, e.id, e.action,
           e.approved_definition_version, e.approved_definition_digest,
           e.snapshot_high_watermark,
           s.approved_definition_version, s.approved_definition_digest,
           s.execution_mode, s.status
      INTO schedule_event_pointer, current_event_id, current_action,
           current_definition_version, current_definition_digest,
           current_high_watermark,
           schedule_definition_version, schedule_definition_digest,
           schedule_execution_mode, schedule_status
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
            'task run snapshot admission state is invalid'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.reference_schema_version =
       'vane.run-snapshot-ref/v2' THEN
        IF NEW.v2_cutover_event_id IS NOT NULL OR
           (
             schedule_status <> 'active' AND NOT (
               schedule_status = 'paused' AND
               public.authorize_manual_task_run_v1(
                   NEW.tenant_id,
                   NEW.user_id,
                   NEW.task_id,
                   NEW.temporal_workflow_id
               )
             )
           ) OR
           schedule_execution_mode <> 'compiled' OR
           NEW.execution_mode IS DISTINCT FROM schedule_execution_mode OR
           schedule_definition_version IS NULL OR
           schedule_definition_digest IS NULL OR
           NEW.definition_digest IS DISTINCT FROM
               schedule_definition_digest OR
           NEW.adaptive_version <= 0 THEN
            RAISE EXCEPTION
                'Tool run snapshot admission fence rejected task state'
                USING ERRCODE = '23514';
        END IF;

        SELECT d.schema_version, a.version, a.schema_version,
               a.basis_definition_version, a.basis_definition_digest,
               a.last_known_good_definition_version
          INTO approved_schema_version, adaptive_version,
               adaptive_schema_version, adaptive_basis_version,
               adaptive_basis_digest,
               adaptive_last_known_good_definition_version
          FROM public.task_approved_definition_versions d
          JOIN public.task_adaptive_states a
            ON a.tenant_id = d.tenant_id
           AND a.user_id = d.user_id
           AND a.task_id = d.task_id
           AND a.basis_definition_version = d.version
           AND a.basis_definition_digest = d.definition_digest
         WHERE d.tenant_id = NEW.tenant_id
           AND d.user_id = NEW.user_id
           AND d.task_id = NEW.task_id
           AND d.version = schedule_definition_version
           AND d.definition_digest = schedule_definition_digest
           AND d.execution_mode = schedule_execution_mode
         FOR SHARE OF d, a;

        IF approved_schema_version IS DISTINCT FROM
               'vane.task-approved-definition/v2' OR
           adaptive_version IS DISTINCT FROM NEW.adaptive_version OR
           adaptive_schema_version IS DISTINCT FROM
               'vane.task-adaptive-state/v2' OR
           adaptive_basis_version IS DISTINCT FROM
               schedule_definition_version OR
           adaptive_basis_digest IS DISTINCT FROM
               schedule_definition_digest OR
           adaptive_last_known_good_definition_version IS NOT NULL THEN
            RAISE EXCEPTION
                'Tool run snapshot admission fence rejected definition basis'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.reference_schema_version <>
       'vane.run-snapshot-ref/v1' THEN
        RAISE EXCEPTION
            'task run snapshot reference schema is unsupported'
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

REVOKE ALL ON FUNCTION task_run_snapshot_v2_admission_fence()
    FROM PUBLIC;

-- +goose Down

-- A manual snapshot cannot be represented by the preceding active-only
-- admission contract. Keep downgrade fail-closed instead of silently
-- weakening or orphaning its authority.
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION
        '079: manual task run authority migration is irreversible';
END
$$;
-- +goose StatementEnd
