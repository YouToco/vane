-- 018: 租户地基——tenants / memberships / invites（企业级契约 §1.2，决议 D1/D4/D8/D9）
--
-- 本迁移只建表、不改任何既有表，运行中的系统对它无感（新表暂无人读写）。
-- 真正启用发生在认证落地（契约 §1.1 的 D2′）与租户隔离（接缝②）时。
--
-- 三张表的存在理由：
--   tenants     —— D1 真 SaaS 的隔离单元；D9 的软删除（deleted_at + purge_after）也挂这里
--   memberships —— D8 混合模型：现在每租户恒 1 人，但表结构提前按「多人」建，
--                  免得以后做团队时再经历一次改造性迁移
--   invites     —— D4 准入闸门。这不是产品功能而是**财务护栏**：D3 决定平台全垫付
--                  第三方 API 成本，邀请码是敞口的唯一上限（不变量 I-A2）

-- +goose Up

CREATE TABLE tenants (
    id          BIGSERIAL   PRIMARY KEY,
    status      TEXT        NOT NULL DEFAULT 'active',  -- active | suspended | deleting
    plan        TEXT        NOT NULL DEFAULT 'free',
    deleted_at  TIMESTAMPTZ,                            -- D9 软删除标记
    purge_after TIMESTAMPTZ,                            -- D9 硬删期限（deleted_at + 30d）
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- D9 的定时硬删任务按此扫描到期租户。
CREATE INDEX idx_tenants_purge ON tenants (purge_after) WHERE purge_after IS NOT NULL;

CREATE TABLE memberships (
    tenant_id  BIGINT      NOT NULL REFERENCES tenants (id),
    user_id    BIGINT      NOT NULL REFERENCES users (id),
    role       TEXT        NOT NULL DEFAULT 'owner',    -- owner | admin | member（后两者预留）
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, user_id)
);

-- 「这个用户属于哪些租户」——认证后按 user 查租户走这条。
CREATE INDEX idx_memberships_user ON memberships (user_id);

CREATE TABLE invites (
    code               TEXT        PRIMARY KEY,
    issued_by          BIGINT,                          -- 签发人 user_id；平台自签为 NULL
    issued_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at         TIMESTAMPTZ,                     -- NULL = 永不过期
    max_uses           INT         NOT NULL DEFAULT 1,
    used_count         INT         NOT NULL DEFAULT 0,
    -- 多次可用的邀请码上，下面两列记录的是**最近一次**消费（used_count 才是权威计数）。
    -- 完整消费流水不建表：当前场景（自用邀请制）不需要，真要审计再补 invite_uses 表。
    consumed_by_tenant BIGINT      REFERENCES tenants (id),
    consumed_at        TIMESTAMPTZ,
    -- 数据库层的第二道防线：即便应用层的 WHERE used_count < max_uses 被绕过或写错，
    -- 超额消费也会在这里失败。I-A2 是财务护栏，值得两道锁。
    CONSTRAINT ck_invites_uses CHECK (used_count >= 0 AND used_count <= max_uses),
    CONSTRAINT ck_invites_max_uses CHECK (max_uses >= 1)
);

-- 单租户存量数据的归属：types.SingleTenantID = 1 是过渡期所有代码写死的租户号，
-- 这里把它实体化，让后续给业务表加 tenant_id 外键时有行可指。
INSERT INTO tenants (id, status, plan) VALUES (1, 'active', 'free')
ON CONFLICT (id) DO NOTHING;

-- 序列跳过已占用的 1，否则下一个自助注册的租户会撞主键。
SELECT setval('tenants_id_seq', GREATEST((SELECT max(id) FROM tenants), 1));

-- 把当前 owner 补成租户 1 的 owner 成员。
-- **刻意只补 owner 一人**：users 表里还有给机器人发过消息的其他人（飞书 handler 会为
-- 每个发信人建 user 行），把他们全标成 owner 是错的。无 owner 记录时本语句插 0 行。
INSERT INTO memberships (tenant_id, user_id, role)
SELECT 1, u.id, 'owner'
FROM users u
WHERE u.feishu_open_id = (SELECT value ->> 'open_id' FROM settings WHERE key = 'feishu_owner')
ON CONFLICT DO NOTHING;

-- +goose Down

DROP TABLE invites;
DROP TABLE memberships;
DROP TABLE tenants;
