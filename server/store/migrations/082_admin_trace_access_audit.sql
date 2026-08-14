-- 082: exact run attribution plus append-only audit for admin execution traces.
--
-- The target scope is explicit because a platform owner may inspect another
-- tenant. These rows are not runtime trace data; they prove who read which
-- immutable run. No application role receives UPDATE or DELETE.

-- +goose Up

ALTER TABLE task_run_snapshots
    ADD CONSTRAINT uq_task_run_snapshots_call_scope
    UNIQUE (id, tenant_id, user_id);

ALTER TABLE llm_calls ADD COLUMN run_snapshot_id BIGINT;
ALTER TABLE tool_calls ADD COLUMN run_snapshot_id BIGINT;

ALTER TABLE llm_calls
    ADD CONSTRAINT fk_llm_calls_run_snapshot
        FOREIGN KEY (run_snapshot_id, tenant_id, user_id)
        REFERENCES task_run_snapshots (id, tenant_id, user_id),
    ADD CONSTRAINT ck_llm_calls_run_snapshot_scope
        CHECK (
            run_snapshot_id IS NULL
            OR (tenant_id IS NOT NULL AND user_id IS NOT NULL)
        );
ALTER TABLE tool_calls
    ADD CONSTRAINT fk_tool_calls_run_snapshot
        FOREIGN KEY (run_snapshot_id, tenant_id, user_id)
        REFERENCES task_run_snapshots (id, tenant_id, user_id),
    ADD CONSTRAINT ck_tool_calls_run_snapshot_scope
        CHECK (
            run_snapshot_id IS NULL
            OR (tenant_id IS NOT NULL AND user_id IS NOT NULL)
        );

CREATE INDEX idx_llm_calls_run_snapshot
    ON llm_calls (run_snapshot_id, created_at, id)
    WHERE run_snapshot_id IS NOT NULL;
CREATE INDEX idx_tool_calls_run_snapshot
    ON tool_calls (run_snapshot_id, created_at, id)
    WHERE run_snapshot_id IS NOT NULL;

CREATE TABLE admin_trace_access_events (
    id                BIGSERIAL   PRIMARY KEY,
    actor_tenant_id   BIGINT      NOT NULL REFERENCES tenants (id),
    actor_user_id     BIGINT      NOT NULL REFERENCES users (id),
    target_tenant_id  BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    target_user_id    BIGINT      NOT NULL REFERENCES users (id),
    task_id           TEXT        NOT NULL,
    run_snapshot_id   BIGINT      NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT fk_admin_trace_access_snapshot
        FOREIGN KEY (target_tenant_id, target_user_id, task_id, run_snapshot_id)
        REFERENCES task_run_snapshots (tenant_id, user_id, task_id, id)
        ON DELETE CASCADE,
    CONSTRAINT ck_admin_trace_access_task_id
        CHECK (
            task_id <> '' AND btrim(task_id) = task_id
            AND octet_length(task_id) <= 255
        )
);

CREATE INDEX idx_admin_trace_access_target_created
    ON admin_trace_access_events
       (target_tenant_id, target_user_id, task_id, created_at DESC, id DESC);
CREATE INDEX idx_admin_trace_access_actor_created
    ON admin_trace_access_events
       (actor_tenant_id, actor_user_id, created_at DESC, id DESC);

GRANT SELECT, INSERT ON admin_trace_access_events TO vane_app;
GRANT USAGE, SELECT ON SEQUENCE admin_trace_access_events_id_seq TO vane_app;

-- +goose Down

-- An access ledger is non-regenerable security evidence. Development databases
-- can downgrade while empty; populated ledgers must be retained.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM admin_trace_access_events)
       OR EXISTS (SELECT 1 FROM llm_calls WHERE run_snapshot_id IS NOT NULL)
       OR EXISTS (SELECT 1 FROM tool_calls WHERE run_snapshot_id IS NOT NULL) THEN
        RAISE EXCEPTION
            '082: refusing downgrade while exact execution-trace evidence exists';
    END IF;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON SEQUENCE admin_trace_access_events_id_seq FROM vane_app;
REVOKE ALL ON admin_trace_access_events FROM vane_app;
DROP TABLE admin_trace_access_events;

ALTER TABLE tool_calls
    DROP CONSTRAINT fk_tool_calls_run_snapshot,
    DROP CONSTRAINT ck_tool_calls_run_snapshot_scope,
    DROP COLUMN run_snapshot_id;
ALTER TABLE llm_calls
    DROP CONSTRAINT fk_llm_calls_run_snapshot,
    DROP CONSTRAINT ck_llm_calls_run_snapshot_scope,
    DROP COLUMN run_snapshot_id;
ALTER TABLE task_run_snapshots
    DROP CONSTRAINT uq_task_run_snapshots_call_scope;
