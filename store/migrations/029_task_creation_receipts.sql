-- 029: create_schedule v1 耐久用户回执 outbox（A6）。
--
-- pending_actions 的 provider/target 是用户在确认卡上实际操作的资源身份。
-- 终态事务把它复制进独立 outbox，之后即使进程退出也能重放同一张卡的 Patch。
-- payload 用 BYTEA + SHA-256：渲染结果一旦冻结，重试只能复用原字节。
-- lease/fence/attempt/next_attempt_at 全以数据库时钟裁决，避免进程时钟漂移造成早抢。

-- +goose Up

ALTER TABLE pending_actions
    ADD COLUMN receipt_provider TEXT NOT NULL DEFAULT '',
    ADD COLUMN receipt_target   TEXT NOT NULL DEFAULT '',
    ADD CONSTRAINT pending_actions_receipt_target_complete CHECK (
        (receipt_provider = '' AND receipt_target = '') OR
        (receipt_provider <> '' AND receipt_target <> '')
    ),
    ADD CONSTRAINT uq_pending_actions_receipt_scope
        UNIQUE (id, tenant_id, user_id);

CREATE TABLE task_creation_receipts (
    id                      BIGSERIAL   PRIMARY KEY,
    operation_id            TEXT        NOT NULL,
    tenant_id               BIGINT      NOT NULL REFERENCES tenants (id),
    user_id                 BIGINT      NOT NULL REFERENCES users (id),
    session_id              BIGINT      REFERENCES agent_sessions (id) ON DELETE SET NULL,
    provider                TEXT        NOT NULL DEFAULT '',
    target                  TEXT        NOT NULL DEFAULT '',
    provider_key            UUID        NOT NULL,
    status                  TEXT        NOT NULL DEFAULT 'pending',
    lease_owner             TEXT        NOT NULL DEFAULT '',
    lease_until             TIMESTAMPTZ,
    takeover_not_before     TIMESTAMPTZ,
    fence                   BIGINT      NOT NULL DEFAULT 0,
    attempt                 INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at         TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '4 seconds'),
    payload                 BYTEA,
    payload_digest          TEXT        NOT NULL DEFAULT '',
    session_recorded_at     TIMESTAMPTZ,
    session_messages_digest TEXT        NOT NULL DEFAULT '',
    provider_message_id     TEXT        NOT NULL DEFAULT '',
    failure_class           TEXT        NOT NULL DEFAULT '',
    ambiguous_since         TIMESTAMPTZ,
    sent_at                 TIMESTAMPTZ,
    blocked_at              TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_task_creation_receipts_operation UNIQUE (operation_id),
    CONSTRAINT uq_task_creation_receipts_provider_key UNIQUE (provider_key),
    CONSTRAINT fk_task_creation_receipts_operation_scope
        FOREIGN KEY (operation_id, tenant_id, user_id)
        REFERENCES pending_actions (id, tenant_id, user_id),
    CONSTRAINT task_creation_receipts_target_complete CHECK (
        (provider = '' AND target = '') OR
        (provider <> '' AND target <> '')
    ),
    CONSTRAINT task_creation_receipts_status_valid
        CHECK (status IN ('pending', 'sent', 'blocked', 'suppressed')),
    CONSTRAINT task_creation_receipts_fence_nonnegative CHECK (fence >= 0),
    CONSTRAINT task_creation_receipts_attempt_nonnegative CHECK (attempt >= 0),
    CONSTRAINT task_creation_receipts_payload_checkpoint_complete CHECK (
        (payload IS NULL AND payload_digest = '') OR
        (payload IS NOT NULL AND length(payload) > 0 AND
         payload_digest ~ '^[0-9a-f]{64}$')
    ),
    CONSTRAINT task_creation_receipts_session_checkpoint_complete CHECK (
        (session_recorded_at IS NULL AND session_messages_digest = '') OR
        (session_recorded_at IS NOT NULL AND session_messages_digest ~ '^[0-9a-f]{64}$')
    ),
    CONSTRAINT task_creation_receipts_lease_complete CHECK (
        (lease_owner = '' AND lease_until IS NULL AND takeover_not_before IS NULL) OR
        (lease_owner <> '' AND lease_until IS NOT NULL AND takeover_not_before IS NOT NULL)
    ),
    CONSTRAINT task_creation_receipts_terminal_markers CHECK (
        (status = 'pending' AND sent_at IS NULL AND blocked_at IS NULL) OR
        (status = 'sent' AND sent_at IS NOT NULL AND blocked_at IS NULL AND
         provider_message_id <> '') OR
        (status = 'suppressed' AND sent_at IS NOT NULL AND blocked_at IS NULL AND
         provider_message_id = 'legacy-suppressed') OR
        (status = 'blocked' AND sent_at IS NULL AND blocked_at IS NOT NULL AND
         failure_class <> '')
    )
);

CREATE INDEX idx_task_creation_receipts_due
    ON task_creation_receipts (tenant_id, next_attempt_at, id)
    WHERE status = 'pending';

-- 升级前已经终态的 v1 operation 代表 A5 已经给过同步回执。迁移只补一张
-- suppressed 审计行，绝不把历史取消/成功/失败再次推给用户。
INSERT INTO task_creation_receipts (
    operation_id, tenant_id, user_id, session_id, provider, target,
    provider_key, status, next_attempt_at, provider_message_id, sent_at
)
SELECT p.id, p.tenant_id, p.user_id, p.session_id,
       p.receipt_provider, p.receipt_target,
       md5('vane/task-creation-receipt/v1:' || p.id)::uuid,
       'suppressed', clock_timestamp(), 'legacy-suppressed', clock_timestamp()
  FROM pending_actions p
 WHERE p.tool_name = 'create_schedule'
   AND p.execution_version = 1
   AND p.tombstoned_at IS NOT NULL
   AND p.status IN ('executed', 'cancelled', 'expired', 'blocked', 'failed')
ON CONFLICT (operation_id) DO NOTHING;

GRANT SELECT, INSERT, UPDATE, DELETE ON task_creation_receipts TO vane_app;
GRANT USAGE, SELECT ON SEQUENCE task_creation_receipts_id_seq TO vane_app;

ALTER TABLE task_creation_receipts ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON task_creation_receipts
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON task_creation_receipts AS RESTRICTIVE
    FOR ALL
    USING (tenant_id IS NOT DISTINCT FROM
           (SELECT current_setting('app.tenant_id', true))::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
                (SELECT current_setting('app.tenant_id', true))::bigint);

-- +goose Down

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM task_creation_receipts
         WHERE status <> 'suppressed'
            OR provider_message_id <> 'legacy-suppressed'
    ) OR EXISTS (
        SELECT 1 FROM pending_actions
         WHERE receipt_provider <> '' OR receipt_target <> ''
    ) THEN
        RAISE EXCEPTION '029: refusing downgrade while durable receipt state exists';
    END IF;
END $$;
-- +goose StatementEnd

-- Rows still present here are exactly the legacy terminal audit markers
-- synthesized by Up. They were never delivery work and are safe to regenerate
-- if 029 is deployed again.
DELETE FROM task_creation_receipts
 WHERE status = 'suppressed' AND provider_message_id = 'legacy-suppressed';

DROP POLICY IF EXISTS tenant_isolation ON task_creation_receipts;
DROP POLICY IF EXISTS tenant_visible ON task_creation_receipts;
ALTER TABLE task_creation_receipts DISABLE ROW LEVEL SECURITY;
REVOKE ALL ON SEQUENCE task_creation_receipts_id_seq FROM vane_app;
REVOKE ALL ON task_creation_receipts FROM vane_app;
DROP TABLE task_creation_receipts;

ALTER TABLE pending_actions
    DROP CONSTRAINT uq_pending_actions_receipt_scope,
    DROP CONSTRAINT pending_actions_receipt_target_complete,
    DROP COLUMN receipt_target,
    DROP COLUMN receipt_provider;
