-- 006_profile_feedback.sql — M5 画像 + 反馈闭环（契约 §1）
--
-- 设计要点：
--   1. 演化游标放 profiles 行内而非独立表：画像写入与游标推进同行 UPDATE 天然原子，
--      并作为 (updated_at, last_evolved_feedback_id) 双条件 CAS 的一半（审查 F6）。
--   2. 态度类反馈刻意无唯一索引：追加式事件日志、最新为准（主控裁决，见契约文档头）——
--      (delivery_id, action) 唯一索引会使第三次点击命中旧行、最新态度被错判。
--   3. 枚举与 updated_at 仍由应用层负责（沿用 001 约定：不建 CHECK、不建触发器）。

-- +goose Up

-- 演化游标：已消费到的最大 feedbacks.id（0=从未演化）。放 profiles 行内：
-- 画像写入与游标推进同行 UPDATE 天然原子。BIGSERIAL id 游标无 created_at
-- 窗口的边界歧义；id 顺序≈提交顺序仅在单语句自动提交下成立（当前反馈插入
-- 正是），多用户高并发时需换 (created_at,id) 复合游标。
ALTER TABLE profiles ADD COLUMN last_evolved_feedback_id BIGINT NOT NULL DEFAULT 0;

-- 解读正文 markdown（含"阅读原文"行）。按钮点击后重建整卡与追问上下文都需要；
-- 从 card_json 反解析太脆，独立成列。
ALTER TABLE deliveries ADD COLUMN body_md TEXT NOT NULL DEFAULT '';

-- 追问反查：回复消息的 ParentId/RootId → delivery。部分索引排除未发送行的 '' 默认值。
CREATE INDEX idx_deliveries_feishu_message_id
    ON deliveries (feishu_message_id) WHERE feishu_message_id <> '';

-- 深度解读幂等的数据库级保险（第一道是 in-flight 内存注册表，见契约 §10）。
-- 态度类刻意无唯一索引：追加式日志，最新为准（主控裁决，见文档头）。
CREATE UNIQUE INDEX uq_feedbacks_delivery_deep_dive
    ON feedbacks (delivery_id) WHERE action = 'deep_dive';

-- +goose Down
DROP INDEX IF EXISTS uq_feedbacks_delivery_deep_dive;
DROP INDEX IF EXISTS idx_deliveries_feishu_message_id;
ALTER TABLE deliveries DROP COLUMN IF EXISTS body_md;
ALTER TABLE profiles DROP COLUMN IF EXISTS last_evolved_feedback_id;
