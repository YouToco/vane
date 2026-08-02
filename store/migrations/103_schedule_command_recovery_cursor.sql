-- 103: durable singleton cursor for fair schedule-command recovery.
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

-- +goose Down

DROP TABLE IF EXISTS schedule_command_recovery_cursors;
