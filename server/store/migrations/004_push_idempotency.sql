-- 004_push_idempotency.sql — 推送幂等地基（M3 对抗审查修复 FIX-A）
--
-- 背景：Temporal 会重试 Push Activity。若无幂等键，同一批次会被重复建、
-- 同一条内容会被重复发卡。这里给 push_batches 加幂等键（= workflow 确定性 traceID），
-- 给 deliveries 加批内内容唯一约束，使重试复用同一 batch、跳过已发条目。
--
-- +goose Up
-- push_batches 加幂等键：默认空串，兼容 001 里已有/历史行（无键的即时批次）。
ALTER TABLE push_batches ADD COLUMN idempotency_key TEXT NOT NULL DEFAULT '';
-- 部分唯一索引：只对非空 idempotency_key 生效。空串（历史/无键批次）不进约束，
-- 避免多条空键批次互相冲突；ON CONFLICT 推断此索引需带同样的 WHERE 谓词。
CREATE UNIQUE INDEX uq_push_batches_idem ON push_batches (idempotency_key) WHERE idempotency_key <> '';
-- content_item_id 可空（001 里 ON DELETE SET NULL），M3 推送恒有值；
-- NULL 不进唯一约束（部分索引排除），使同一 (batch_id, content_item_id) 只投递一次。
CREATE UNIQUE INDEX uq_deliveries_batch_content ON deliveries (batch_id, content_item_id) WHERE content_item_id IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS uq_deliveries_batch_content;
DROP INDEX IF EXISTS uq_push_batches_idem;
ALTER TABLE push_batches DROP COLUMN IF EXISTS idempotency_key;
