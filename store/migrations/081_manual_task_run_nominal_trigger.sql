-- 081: bind a manual run's observation window to durable command time.
--
-- New manual workflow IDs append the command's exact UTC-second creation
-- time. The authorization fact remains exact and owner-scoped. The legacy
-- UUID-only identity stays authorized for in-flight recovery, but observation
-- policy deliberately rejects it because it has no trustworthy window.

-- +goose Up

CREATE OR REPLACE FUNCTION authorize_manual_task_run_v1(
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
         WHERE expected_workflow_id IN (
                   'wf-manual-' || c.id::text,
                   'wf-manual-' || c.id::text || '-' ||
                       pg_catalog.to_char(
                           c.created_at AT TIME ZONE 'UTC',
                           'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                       )
               )
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

-- +goose Down

-- A timestamped manual workflow may already own snapshots and effects. Keep
-- downgrade fail-closed rather than orphaning its exact authorization fact.
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION
        '081: timestamped manual task run authority migration is irreversible';
END
$$;
-- +goose StatementEnd
