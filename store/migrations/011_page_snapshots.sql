-- 011: page_watch 页面快照表（M6 契约 §4 / §10）

-- +goose Up

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

-- +goose Down

DROP TABLE IF EXISTS page_snapshots;
