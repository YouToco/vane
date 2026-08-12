-- 127: durable singleton cursor for fair schedule-command recovery.
--
-- The synchronous startup recovery gate may be killed after its fixed budget.
-- Persisting the last attempted (tenant,command) identity prevents every new
-- process from restarting at the lowest tenant and starving later tenants.

-- +goose Up

CREATE TABLE schedule_command_recovery_cursors (
    worker_key TEXT PRIMARY KEY,
    tenant_id  BIGINT NOT NULL,
    command_id UUID,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT schedule_command_recovery_cursor_singleton CHECK (
        worker_key = 'scheduler'
    ),
    CONSTRAINT schedule_command_recovery_cursor_shape CHECK (
        (tenant_id = 0 AND command_id IS NULL) OR tenant_id > 0
    )
);

REVOKE ALL ON schedule_command_recovery_cursors FROM PUBLIC,vane_app;
GRANT SELECT,INSERT,UPDATE ON schedule_command_recovery_cursors
    TO vane_schedule_commander;

-- V1 task creation now executes entirely as tenant-scoped vane_app. Capacity
-- remains a per-user cross-tenant invariant, so expose only the aggregate
-- integer after independently proving the exact tenant GUC and live
-- membership. No schedule or operation identity crosses this boundary.
-- +goose StatementBegin
CREATE FUNCTION count_task_creation_capacity_v1(
    requested_tenant_id BIGINT,
    requested_user_id BIGINT
) RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public
AS $$
DECLARE
    used_capacity BIGINT;
BEGIN
    IF requested_tenant_id <= 0 OR requested_user_id <= 0 OR
       requested_tenant_id IS DISTINCT FROM
           NULLIF(current_setting('app.tenant_id',true),'')::BIGINT OR
       NOT EXISTS (
           SELECT 1
             FROM public.memberships membership
             JOIN public.tenants tenant ON tenant.id=membership.tenant_id
            WHERE membership.tenant_id=requested_tenant_id
              AND membership.user_id=requested_user_id
              AND tenant.status='active' AND tenant.deleted_at IS NULL
       ) THEN
        RAISE EXCEPTION '127: task creation capacity scope is invalid'
            USING ERRCODE='23514';
    END IF;

    SELECT count(*) INTO used_capacity FROM (
        SELECT 0 AS reservation_kind,id AS reservation_id
          FROM public.schedules
         WHERE user_id=requested_user_id AND status='active'
        UNION
        SELECT 0,task_id FROM public.task_creation_operations
         WHERE user_id=requested_user_id AND status='executing'
           AND ((execution_version=1 AND tool_name='create_schedule') OR
                (execution_version=2 AND tool_name='manage_tasks'))
           AND tombstoned_at IS NULL AND task_id<>''
           AND phase IN ('definition_committed','activation_started','activated')
        UNION
        SELECT CASE WHEN result->>'task_id_known'='true' THEN 0 ELSE 1 END,
               CASE WHEN result->>'task_id_known'='true' THEN task_id ELSE id END
          FROM public.task_creation_operations
         WHERE user_id=requested_user_id AND execution_version=1
           AND tool_name='create_schedule' AND status='blocked' AND phase='blocked'
           AND tombstoned_at IS NOT NULL
           AND result->>'version'='vane.task-creation-quarantine/v1'
           AND result->>'reservation_retained'='true'
        UNION
        SELECT 0,task_id FROM public.task_creation_operations
         WHERE user_id=requested_user_id AND execution_version=2
           AND tool_name='manage_tasks' AND status='blocked' AND phase='blocked'
           AND tombstoned_at IS NOT NULL AND task_id<>''
    ) reserved;
    RETURN used_capacity;
END;
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION count_task_creation_capacity_v1(BIGINT,BIGINT)
    FROM PUBLIC;
GRANT EXECUTE ON FUNCTION count_task_creation_capacity_v1(BIGINT,BIGINT)
    TO vane_app;

-- +goose Down

DROP FUNCTION IF EXISTS count_task_creation_capacity_v1(BIGINT,BIGINT);
DROP TABLE IF EXISTS schedule_command_recovery_cursors;
