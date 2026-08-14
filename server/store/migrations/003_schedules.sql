-- 003_schedules.sql — Temporal Schedule 的 Postgres 镜像表（M3）
--
-- 设计要点：
--   1. 主键 id 用 TEXT 而非 BIGSERIAL：直接存 Temporal schedule_id
--      （push-{user_id}-{uuid}），与 Temporal 侧同一标识，省去二次映射。
--   2. 本表是镜像 / 对账用途：真源在 Temporal。scheduler.CreatePush 先在
--      Temporal Create 成功后再写本表（写失败仅告警，靠对账补偿）。
--   3. spec_json / scope_json 用 JSONB 透传结构（{cron,tz} 或 {every_seconds}、
--      PushScope），store 层只做序列化搬运不解析——与 002 settings 表同思路。
--   4. status 仅 active / paused 两态，由应用层校验（沿用 001 不建 CHECK 的约定）。
--   5. updated_at 由应用层负责（001 约定不建触发器）。

-- +goose Up
CREATE TABLE schedules (
    id              TEXT        PRIMARY KEY,               -- Temporal schedule_id: push-{user_id}-{uuid}
    user_id         BIGINT      NOT NULL REFERENCES users(id),
    nl_description  TEXT        NOT NULL DEFAULT '',       -- 用户原话/展示名："每天早8点推科技"
    spec_json       JSONB       NOT NULL DEFAULT '{}',     -- {cron:"0 8 * * *", tz:"Asia/Shanghai"} 或 {every_seconds:86400}
    scope_json      JSONB       NOT NULL DEFAULT '{}',     -- PushScope 序列化
    status          TEXT        NOT NULL DEFAULT 'active', -- active/paused
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_schedules_user ON schedules(user_id);

-- +goose Down
DROP TABLE IF EXISTS schedules;
