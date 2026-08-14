-- 023: tool_calls 增加 cost_usd 和 source_id 列——Exa/TikHub 信源抓取的花费记账。
--
-- cost_usd 可空：非计费调用（静态工具、search_endpoints 检索等）为 NULL；
-- source_id 可空：agent 面工具调用无信源归属，抓取面调用填 source_id 用于归因。

-- +goose Up

ALTER TABLE tool_calls ADD COLUMN cost_usd NUMERIC(12,6);
ALTER TABLE tool_calls ADD COLUMN source_id BIGINT;

-- +goose Down

ALTER TABLE tool_calls DROP COLUMN source_id;
ALTER TABLE tool_calls DROP COLUMN cost_usd;
