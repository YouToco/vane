-- 019: 邮箱+密码身份与会话表（企业级契约 §1.1 决议 D2′）
--
-- users 表原本是**飞书专属身份**（feishu_open_id NOT NULL UNIQUE）——那是单 owner
-- 时代的产物：唯一的用户就是给机器人发消息的人。真 SaaS 下用户从注册页来，
-- 没有飞书 open_id，故本迁移把身份维度放开为「飞书 或 邮箱，至少其一」。

-- +goose Up

-- 飞书身份变可选：邮箱注册的用户没有 open_id。
-- Postgres 的 UNIQUE 把多个 NULL 视为互不相同，故既有 uq_users_feishu_open_id
-- 无需改动，多行 NULL 不会互相冲突。
ALTER TABLE users ALTER COLUMN feishu_open_id DROP NOT NULL;

ALTER TABLE users ADD COLUMN email          TEXT;
ALTER TABLE users ADD COLUMN password_hash  TEXT;
-- D4 邀请制下首版不做邮箱验证（邀请码本身即把关），本列为将来接发信服务预留。
ALTER TABLE users ADD COLUMN email_verified BOOLEAN NOT NULL DEFAULT false;

-- 邮箱唯一性**按小写比较**：Alice@x.com 与 alice@x.com 是同一个人，
-- 不做归一会让同一邮箱注册出两个账号（且登录时不知该匹配哪个）。
CREATE UNIQUE INDEX uq_users_email_lower ON users (lower(email)) WHERE email IS NOT NULL;

-- 至少要有一种身份，否则是一行无法登录也无法关联的孤儿数据。
ALTER TABLE users ADD CONSTRAINT ck_users_identity
    CHECK (feishu_open_id IS NOT NULL OR email IS NOT NULL);

-- 有邮箱身份就必须有密码（当前唯一的邮箱认证方式）；纯飞书用户无密码。
-- 将来接社交登录时本约束要放宽（那时邮箱可以来自 IdP 而无本地密码）。
ALTER TABLE users ADD CONSTRAINT ck_users_email_has_password
    CHECK (email IS NULL OR password_hash IS NOT NULL);

-- 会话表。**存 token 的哈希而非明文**（见 auth/session.go 的说明）：
-- 会话 token 等价于密码，库里存明文意味着一次只读泄漏就能无声冒充所有在线用户。
CREATE TABLE user_sessions (
    token_hash   BYTEA       PRIMARY KEY,
    user_id      BIGINT      NOT NULL REFERENCES users (id),
    -- 会话直接钉住租户：登录那一刻确定身份，避免每次请求再查一次 memberships，
    -- 也让「同一用户属于多租户」时的切换语义明确（切租户 = 换会话）。
    tenant_id    BIGINT      NOT NULL REFERENCES tenants (id),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ
);

-- 「登出该用户的所有会话」「注销租户时清会话」都按这两列扫。
CREATE INDEX idx_user_sessions_user ON user_sessions (user_id);
CREATE INDEX idx_user_sessions_tenant ON user_sessions (tenant_id);
-- 过期会话清理任务按此扫描。
CREATE INDEX idx_user_sessions_expires ON user_sessions (expires_at);

-- +goose Down

DROP TABLE user_sessions;
ALTER TABLE users DROP CONSTRAINT ck_users_email_has_password;
ALTER TABLE users DROP CONSTRAINT ck_users_identity;
DROP INDEX uq_users_email_lower;
ALTER TABLE users DROP COLUMN email_verified;
ALTER TABLE users DROP COLUMN password_hash;
ALTER TABLE users DROP COLUMN email;
ALTER TABLE users ALTER COLUMN feishu_open_id SET NOT NULL;
