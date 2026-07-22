-- 030: 每次定时任务运行的不可变执行快照（Agent Runtime C0）。
--
-- Workflow history 只保存安全引用；任务定义、手册、信源身份与受信策略版本在
-- Activity 开始时一次性冻结到本表。相同 Temporal RunID 的重试只能复用首次提交的
-- 行，不能因任务编辑、部署或策略切换而在运行途中改变语义。
--
-- task_id 刻意不建 FK：任务删除后运行审计仍须保留。tenant 删除则按数据生命周期
-- 级联清理；user 外键沿用现有 tenant-owned 表的 NO ACTION 习惯。

-- +goose Up

CREATE TABLE task_run_snapshots (
    id                         BIGSERIAL   PRIMARY KEY,
    tenant_id                  BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    user_id                    BIGINT      NOT NULL REFERENCES users (id),
    task_id                    TEXT        NOT NULL,
    temporal_workflow_id       TEXT        NOT NULL,
    temporal_run_id            TEXT        NOT NULL,
    run_kind                    TEXT        NOT NULL,
    execution_mode             TEXT        NOT NULL,
    adaptive_version           BIGINT      NOT NULL,
    capability_catalog_digest  TEXT        NOT NULL,
    tool_policy_digest         TEXT        NOT NULL,
    prompt_policy_digest       TEXT        NOT NULL,
    model_policy_digest        TEXT        NOT NULL,
    quota_policy_digest        TEXT        NOT NULL,
    definition_digest          TEXT        NOT NULL,
    plan_digest                TEXT        NOT NULL,
    payload_digest             TEXT        NOT NULL,
    reference_digest           TEXT        NOT NULL,
    reference_schema_version   TEXT        NOT NULL,
    payload                    BYTEA       NOT NULL,
    budget                     JSONB       NOT NULL,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_task_run_snapshots_scope
        UNIQUE (tenant_id, user_id, task_id, temporal_workflow_id, temporal_run_id),
    -- 当前单 Temporal namespace 中 RunID 是 execution 的全局 UUID。这个约束只作
    -- 防串号防线，不作为读取键：同一 execution 被错误路由到其他 scope/workflow 时
    -- 必须 Conflict，而不是静默生成第二份快照。
    CONSTRAINT uq_task_run_snapshots_temporal_run_id UNIQUE (temporal_run_id),
    CONSTRAINT task_run_snapshots_identity_valid CHECK (
        task_id <> '' AND btrim(task_id) = task_id AND octet_length(task_id) <= 255 AND
        temporal_workflow_id <> '' AND btrim(temporal_workflow_id) = temporal_workflow_id AND
        octet_length(temporal_workflow_id) <= 512 AND
        temporal_run_id <> '' AND btrim(temporal_run_id) = temporal_run_id AND
        octet_length(temporal_run_id) <= 512
    ),
    CONSTRAINT task_run_snapshots_mode_valid CHECK (
        execution_mode IN ('compiled', 'discover_at_run')
    ),
    CONSTRAINT task_run_snapshots_run_kind_valid CHECK (run_kind = 'scheduled'),
    CONSTRAINT task_run_snapshots_reference_schema_version_valid CHECK (
        reference_schema_version <> '' AND
        btrim(reference_schema_version) = reference_schema_version AND
        octet_length(reference_schema_version) <= 512
    ),
    CONSTRAINT task_run_snapshots_adaptive_version_nonnegative
        CHECK (adaptive_version >= 0),
    CONSTRAINT task_run_snapshots_capability_catalog_digest_valid
        CHECK (capability_catalog_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT task_run_snapshots_tool_policy_digest_valid
        CHECK (tool_policy_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT task_run_snapshots_prompt_policy_digest_valid
        CHECK (prompt_policy_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT task_run_snapshots_model_policy_digest_valid
        CHECK (model_policy_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT task_run_snapshots_quota_policy_digest_valid
        CHECK (quota_policy_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT task_run_snapshots_definition_digest_valid
        CHECK (definition_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT task_run_snapshots_plan_digest_valid CHECK (
        (execution_mode = 'compiled' AND plan_digest ~ '^[0-9a-f]{64}$') OR
        (execution_mode = 'discover_at_run' AND
         (plan_digest = '' OR plan_digest ~ '^[0-9a-f]{64}$'))
    ),
    CONSTRAINT task_run_snapshots_payload_digest_valid
        CHECK (payload_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT task_run_snapshots_reference_digest_valid
        CHECK (reference_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT task_run_snapshots_payload_size_valid CHECK (
        octet_length(payload) > 0 AND octet_length(payload) <= 2097152
    ),
    CONSTRAINT task_run_snapshots_budget_object
        CHECK (jsonb_typeof(budget) = 'object')
);

CREATE INDEX idx_task_run_snapshots_tenant_user_task_created
    ON task_run_snapshots (tenant_id, user_id, task_id, created_at DESC, id DESC);

-- 快照只增不改：受限业务角色没有 UPDATE/DELETE 权限。租户生命周期级联与
-- 运维清理由 owner 承担，不为应用暴露可篡改审计的 API。
GRANT SELECT, INSERT ON task_run_snapshots TO vane_app;
GRANT USAGE, SELECT ON SEQUENCE task_run_snapshots_id_seq TO vane_app;

ALTER TABLE task_run_snapshots ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON task_run_snapshots
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON task_run_snapshots AS RESTRICTIVE
    FOR ALL
    USING (tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint);

-- +goose Down

-- 一旦运行时接线，本表就是不可再生审计；禁止用普通 downgrade 静默销毁。
-- C0 保持零生产调用点时表为空，开发期回滚仍可正常执行。
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM task_run_snapshots) THEN
        RAISE EXCEPTION '030: refusing downgrade while task run snapshots exist';
    END IF;
END $$;
-- +goose StatementEnd

DROP POLICY IF EXISTS tenant_isolation ON task_run_snapshots;
DROP POLICY IF EXISTS tenant_visible ON task_run_snapshots;
ALTER TABLE task_run_snapshots DISABLE ROW LEVEL SECURITY;
REVOKE ALL ON SEQUENCE task_run_snapshots_id_seq FROM vane_app;
REVOKE ALL ON task_run_snapshots FROM vane_app;
DROP TABLE task_run_snapshots;
