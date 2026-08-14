-- 017_schedule_playbooks.sql — 情报任务手册（Task Playbook）P0 存取层
--   （Task Playbook RFC「先 A 后 B」架构的 A：只做手册存取，不碰抓取/打分/出卡）
--
-- 设计要点：
--   1. 与 schedules 一对一：schedule_id 既是主键又是外键，ON DELETE CASCADE——
--      删定时任务（Temporal schedule 的 Postgres 镜像行）自动带走其手册，无孤儿表行。
--      「一个定时任务 = 一份自带手册的情报简报」由这条 PK=FK 关系在 schema 层钉死。
--   2. content 是 P0 的手册正文：create_schedule 建任务时用【用户 nl 意图原文】初始化
--      （不做 LLM 翻译，NL→结构化翻译是 P1）。NOT NULL DEFAULT '' 沿用 001/003 空串约定。
--   3. fetch_plan 为 P1 预留：NL→结构化抓取计划（按计划抓）的翻译产物。P0 只建列不写，
--      NOT NULL DEFAULT '{}' 使 P0 行天然持合法空对象，P1 落地无需再加迁移改列。
--   4. updated_at 由应用层 now() 维护（沿用 001/003 不建触发器的约定）。

-- +goose Up
CREATE TABLE schedule_playbooks (
    schedule_id TEXT        PRIMARY KEY REFERENCES schedules(id) ON DELETE CASCADE,
    content     TEXT        NOT NULL DEFAULT '',   -- P0 手册正文：初始化为用户 nl 意图原文
    fetch_plan  JSONB       NOT NULL DEFAULT '{}', -- P1 预留：NL→结构化抓取计划，P0 不写
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS schedule_playbooks;
