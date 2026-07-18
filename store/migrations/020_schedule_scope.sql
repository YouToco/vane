-- 020_schedule_scope.sql — 情报任务手册 P1b：候选按 schedule 归属的 schema 地基
--   （Task Playbook RFC「先 A 后 B」的 B：让手册真正决定"只按本任务的源抓/挑"）
--
-- 本迁移只建结构、不改任何运行时行为（P1b 分三步：b1=埋管[本迁移]、b2=填链接、b3=消费）：
--   1. schedule_sources：「任务 ↔ 源」软范围绑定，与 subscriptions（用户↔源）平行。
--      一个定时任务用哪几个源由它记录；手册编译时维护（b2）。软范围≠硬闸门：agent 的
--      配源/搜索工具面不受它约束，它只圈定某任务的取材范围。删任务连带删链接（CASCADE）。
--      source_id 也 CASCADE：源被硬删时其链接一并清，不留悬挂引用（源全局共享、按幂等键一份）。
--   2. push_batches.schedule_id：记录触发本批的定时任务，是"按任务的投递账本"的锚点（b3
--      的候选去重经 deliveries→push_batches→schedule_id 判"本任务是否投过"）。可空——
--      push_now / 老任务触发为 NULL（用户级语义，向后兼容，决策 #4）。

-- +goose Up
CREATE TABLE schedule_sources (
    schedule_id TEXT        NOT NULL REFERENCES schedules(id) ON DELETE CASCADE,
    source_id   BIGINT      NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (schedule_id, source_id)
);
-- 按源反查"哪些任务在用它"（b3 候选 join 走 schedule_id→source，此索引供 GC 孤儿源/分析反向查）
CREATE INDEX idx_schedule_sources_source ON schedule_sources (source_id);

ALTER TABLE push_batches ADD COLUMN schedule_id TEXT;
-- 按任务查其批次（b3 候选去重：deliveries JOIN push_batches ON schedule_id）。
-- 部分索引：绝大多数历史批次 schedule_id 为 NULL（push_now/老任务），只索引有值的行。
CREATE INDEX idx_push_batches_schedule ON push_batches (schedule_id) WHERE schedule_id IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_push_batches_schedule;
ALTER TABLE push_batches DROP COLUMN IF EXISTS schedule_id;
DROP TABLE IF EXISTS schedule_sources;
