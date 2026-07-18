-- 015: 下线 page_watch —— 删除 page_snapshots 表
--
-- page_watch 页面监控能力已从代码移除（改由 Exa fetch 覆盖，见 CHANGELOG / M6 契约 §10）。
-- 011 建的 page_snapshots 表随之成为无人读写的孤儿表，本迁移把它删除。
-- 与 008 退役 type 列同理：功能下线后其专属 schema 一并回收，不留死表。
--
-- Down 重建表结构（与 011 逐字一致），使 schema 变更可逆；行数据不可逆（DROP 即丢），
-- 但该表在 page_watch 从未生产验证的前提下应为空，无资产可留。

-- +goose Up

DROP TABLE IF EXISTS page_snapshots;

-- +goose Down

CREATE TABLE page_snapshots (
    id             BIGSERIAL   PRIMARY KEY,
    source_id      BIGINT      NOT NULL,
    canonical_key  TEXT        NOT NULL,
    content_hash   TEXT        NOT NULL,
    extracted_text TEXT        NOT NULL,
    verdict        TEXT        NOT NULL DEFAULT 'pending'
                   CHECK (verdict IN ('pending','baseline','suppressed')),
    first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_page_snapshots_source FOREIGN KEY (source_id)
        REFERENCES sources (id) ON DELETE CASCADE,
    CONSTRAINT uq_page_snapshots UNIQUE (source_id, canonical_key)
);

CREATE INDEX idx_page_snapshots_source ON page_snapshots (source_id, id DESC);
