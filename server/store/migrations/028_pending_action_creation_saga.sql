-- 028: create_schedule v1 的耐久创建 operation 底座（A4）。
--
-- 本迁移只增加「已确认动作如何安全跨进程恢复」所需的状态，不把任何现有入口
-- 接到新 saga。execution_version 默认 0 是刻意的：升级部署后，历史 pending_actions
-- 与旧 CreatePendingAction 写入仍走原有 ClaimPendingAction 语义；只有未来显式写成 v1
-- 的 create_schedule 动作才可被新领取/恢复路径看见。
--
-- 三个精确检查点用 BYTEA 而非 JSONB：JSONB 会重排键、归一化数字/空白，无法证明
-- 「重试复用了同一份冻结结果」。result 是终态展示数据，不承担 CAS 身份，因此仍用
-- JSONB。tombstoned_at 是永久终态标记；正常恢复器只扫描 executing 且无 tombstone 的行。

-- +goose Up

ALTER TABLE pending_actions
    ADD COLUMN execution_version  SMALLINT    NOT NULL DEFAULT 0,
    ADD COLUMN phase              TEXT        NOT NULL DEFAULT '',
    ADD COLUMN lease_owner        TEXT        NOT NULL DEFAULT '',
    ADD COLUMN lease_until        TIMESTAMPTZ,
    ADD COLUMN takeover_not_before TIMESTAMPTZ,
    ADD COLUMN fence              BIGINT      NOT NULL DEFAULT 0,
    ADD COLUMN attempt            INTEGER     NOT NULL DEFAULT 0,
    ADD COLUMN normalized_command BYTEA,
    ADD COLUMN compiled_definition BYTEA,
    ADD COLUMN compiled_digest    TEXT        NOT NULL DEFAULT '',
    ADD COLUMN prepared_schedule  BYTEA,
    ADD COLUMN ensure_receipt     BYTEA,
    ADD COLUMN task_id            TEXT        NOT NULL DEFAULT '',
    ADD COLUMN result             JSONB,
    ADD COLUMN error_code         TEXT        NOT NULL DEFAULT '',
    ADD COLUMN error_message      TEXT        NOT NULL DEFAULT '',
    ADD COLUMN updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN tombstoned_at      TIMESTAMPTZ;

ALTER TABLE pending_actions
    ADD CONSTRAINT pending_actions_execution_version_nonnegative CHECK (execution_version >= 0),
    ADD CONSTRAINT pending_actions_fence_nonnegative CHECK (fence >= 0),
    ADD CONSTRAINT pending_actions_attempt_nonnegative CHECK (attempt >= 0);

-- 契约要求租户表索引以 tenant_id 为首列。恢复器按租户分片扫描过期 lease；
-- partial predicate 同时把历史 v0、其他写工具与所有终态从索引中物理排除。
CREATE INDEX idx_pending_actions_creation_stale
    ON pending_actions (tenant_id, takeover_not_before, id)
    WHERE execution_version = 1
      AND tool_name = 'create_schedule'
      AND status = 'executing'
      AND tombstoned_at IS NULL;

-- +goose Down

-- 降级若已存在 v1 operation 会永久丢掉 fence、检查点和墓碑，使旧 worker 有机会
-- 重复付费或复活任务。A4 上线时没有生产调用点，正常回滚不会命中；A5 接线后必须
-- 先完成专门的数据退场方案，不能把带状态的列直接削掉。
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pending_actions WHERE execution_version <> 0) THEN
        RAISE EXCEPTION '028: refusing downgrade while versioned create_schedule operations exist';
    END IF;
END $$;
-- +goose StatementEnd

DROP INDEX idx_pending_actions_creation_stale;

ALTER TABLE pending_actions
    DROP CONSTRAINT pending_actions_attempt_nonnegative,
    DROP CONSTRAINT pending_actions_fence_nonnegative,
    DROP CONSTRAINT pending_actions_execution_version_nonnegative,
    DROP COLUMN tombstoned_at,
    DROP COLUMN updated_at,
    DROP COLUMN error_message,
    DROP COLUMN error_code,
    DROP COLUMN result,
    DROP COLUMN task_id,
    DROP COLUMN ensure_receipt,
    DROP COLUMN prepared_schedule,
    DROP COLUMN compiled_digest,
    DROP COLUMN compiled_definition,
    DROP COLUMN normalized_command,
    DROP COLUMN attempt,
    DROP COLUMN fence,
    DROP COLUMN takeover_not_before,
    DROP COLUMN lease_until,
    DROP COLUMN lease_owner,
    DROP COLUMN phase,
    DROP COLUMN execution_version;
