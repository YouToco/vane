-- 002_settings.sql — 运行时配置表（M2）
--
-- 设计要点：
--   1. 飞书 app_id/app_secret 等凭证由用户在 Dashboard 向导中填入，
--      存数据库而非 .env/config 文件——重配无需重启进程，且凭证不落仓库。
--   2. key 为业务约定字符串（'feishu' / 'feishu_owner'），value 为 JSONB，
--      结构由各消费方自行定义，store 层只做透传（json.RawMessage）。
--   3. updated_at 由应用层 UPSERT 时显式写 now()，与 001 "不建触发器"约定一致。

-- +goose Up
CREATE TABLE settings (
    key        TEXT        PRIMARY KEY,
    value      JSONB       NOT NULL DEFAULT '{}',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS settings;
