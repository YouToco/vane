-- 001_init.sql — 见微 Vane 初始 schema（9 张表，对应设计规格 Step 5）
--
-- 设计要点：
--   1. 主键全部 BIGSERIAL：单机部署无分布式需求，JOIN 性能优于 UUID。
--   2. sources.next_fetch_at 预计算列替代表达式索引（规格 Validator 修正 MAJOR-1+2）：
--      调度查询 WHERE status = 'active' AND next_fetch_at <= now() 命中普通 B-tree 复合索引。
--   3. deliveries.content_item_id FK 带 ON DELETE SET NULL（规格 Validator 修正 MAJOR-3）：
--      content_items 30 天 TTL 清理时投递历史保留，不连坐删除。
--   4. 文本/数值字段一律 NOT NULL + DEFAULT，配合 Go 零值语义，减少 pgx 扫描 NULL 的负担；
--      仅业务上确有"未发生/未知"含义的列允许 NULL：last_fetched_at / sent_at / published_at /
--      simhash / ref_id / content_item_id / llm_calls.user_id / prefix_cache_hit /
--      temperature / max_tokens（后三者为三态语义，审查裁决 2026-07-14）。
--   5. 枚举值（sources.type / *.status / feedbacks.action）由应用层校验，未建 CHECK：
--      规格中的 CHECK 取值集合与当前最新取值集合已不一致，等枚举稳定后再补迁移。
--   6. 不建触发器；updated_at 由应用层负责更新。
--   7. agent_sessions / tool_policies / agent_events 属 M4，放 003 及以后迁移，本文件不含
--      （002 已被 M2 的 settings 表占用）。

-- +goose Up

-- 用户表：以飞书 open_id 唯一标识用户
CREATE TABLE users (
    id             BIGSERIAL   PRIMARY KEY,
    feishu_open_id TEXT        NOT NULL,
    name           TEXT        NOT NULL DEFAULT '',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_users_feishu_open_id UNIQUE (feishu_open_id)
);

-- 信源表：RSS / tikhub 小红书等抓取源
CREATE TABLE sources (
    id                     BIGSERIAL   PRIMARY KEY,
    type                   TEXT        NOT NULL,              -- 'rss' | 'tikhub_xhs' 等，应用层校验
    url                    TEXT        NOT NULL,
    title                  TEXT        NOT NULL DEFAULT '',
    config                 JSONB       NOT NULL DEFAULT '{}',
    status                 TEXT        NOT NULL DEFAULT 'active',
    fetch_interval_seconds INT         NOT NULL DEFAULT 1800,
    -- 关键设计：预计算下次抓取时间；UpdateLastFetched 时同步写 next_fetch_at = now() + interval
    next_fetch_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_fetched_at        TIMESTAMPTZ,                       -- 从未抓取过为 NULL
    fail_count             INT         NOT NULL DEFAULT 0,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ListDueForFetch：WHERE status = 'active' AND next_fetch_at <= now()
CREATE INDEX idx_sources_status_next_fetch ON sources (status, next_fetch_at);

-- 订阅表：用户与信源多对多
CREATE TABLE subscriptions (
    id         BIGSERIAL   PRIMARY KEY,
    user_id    BIGINT      NOT NULL,
    source_id  BIGINT      NOT NULL,
    status     TEXT        NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_subscriptions_user_id     FOREIGN KEY (user_id)   REFERENCES users (id),
    CONSTRAINT fk_subscriptions_source_id   FOREIGN KEY (source_id) REFERENCES sources (id),
    CONSTRAINT uq_subscriptions_user_source UNIQUE (user_id, source_id)
);

-- 抓取后按信源反查订阅者做扇出
CREATE INDEX idx_subscriptions_source_id ON subscriptions (source_id);

-- 内容条目表：抓取到的原始内容
CREATE TABLE content_items (
    id           BIGSERIAL   PRIMARY KEY,
    source_id    BIGINT      NOT NULL,
    -- external_id 强制 NOT NULL 且无默认：源无自然 id 时应用层须以 url/hash 派生，
    -- 避免空串在 UNIQUE(source_id, external_id) 上静默冲突
    external_id  TEXT        NOT NULL,
    url          TEXT        NOT NULL DEFAULT '',
    title        TEXT        NOT NULL DEFAULT '',
    content      TEXT        NOT NULL DEFAULT '',
    author       TEXT        NOT NULL DEFAULT '',
    published_at TIMESTAMPTZ,                                 -- 部分 RSS 无发布时间
    content_hash TEXT        NOT NULL,
    simhash      BIGINT,                                      -- 近似去重指纹，未计算为 NULL
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_content_items_source_id       FOREIGN KEY (source_id) REFERENCES sources (id),
    CONSTRAINT uq_content_items_source_external UNIQUE (source_id, external_id)
);

-- FindByHash 精确去重
CREATE INDEX idx_content_items_content_hash ON content_items (content_hash);
-- ListBySource 按发布时间倒序分页
CREATE INDEX idx_content_items_source_published ON content_items (source_id, published_at DESC);
-- FindRecentSimhashes 近似去重：按抓取时间取最近窗口，INCLUDE 支持 index-only scan（规格索引设计）
CREATE INDEX idx_content_items_source_fetched_simhash
    ON content_items (source_id, fetched_at DESC) INCLUDE (simhash);

-- 推送批次表：一次决策周期
CREATE TABLE push_batches (
    id           BIGSERIAL   PRIMARY KEY,
    user_id      BIGINT      NOT NULL,
    scheduled_at TIMESTAMPTZ,
    status       TEXT        NOT NULL DEFAULT 'pending',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_push_batches_user_id FOREIGN KEY (user_id) REFERENCES users (id)
);

-- ListByUser 用户批次历史（规格索引设计）
CREATE INDEX idx_push_batches_user_created ON push_batches (user_id, created_at DESC);
-- 调度器捞待执行批次：WHERE status = 'pending' AND scheduled_at <= now()
CREATE INDEX idx_push_batches_status_scheduled ON push_batches (status, scheduled_at);

-- 投递表：批次内的单张推送卡片
CREATE TABLE deliveries (
    id                BIGSERIAL   PRIMARY KEY,
    batch_id          BIGINT      NOT NULL,
    user_id           BIGINT      NOT NULL,
    -- 关键设计：内容被 TTL 清理后置 NULL，投递历史保留（规格 Validator 修正 MAJOR-3）
    content_item_id   BIGINT,
    score             NUMERIC     NOT NULL DEFAULT 0,
    card_json         JSONB       NOT NULL DEFAULT '{}',
    feishu_message_id TEXT        NOT NULL DEFAULT '',
    status            TEXT        NOT NULL DEFAULT 'pending',
    sent_at           TIMESTAMPTZ,                            -- 未发送为 NULL
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_deliveries_batch_id        FOREIGN KEY (batch_id)        REFERENCES push_batches (id),
    CONSTRAINT fk_deliveries_user_id         FOREIGN KEY (user_id)         REFERENCES users (id),
    CONSTRAINT fk_deliveries_content_item_id FOREIGN KEY (content_item_id) REFERENCES content_items (id) ON DELETE SET NULL
);

-- ListByBatch
CREATE INDEX idx_deliveries_batch_id ON deliveries (batch_id);
-- ListByUser 推送历史
CREATE INDEX idx_deliveries_user_created ON deliveries (user_id, created_at DESC);
-- 原为支撑 content_items TTL 批量删除的子表反查而建。**TTL 已作废**（Boss 决策
-- 2026-07-15：数据是资产，一律不清理，留存做需求挖掘与信源质量评估，见 007）。
-- 索引保留，语义改为分析查询用：按内容反查其投递历史（这条被推给过谁、反馈如何）。
CREATE INDEX idx_deliveries_content_item_id ON deliveries (content_item_id);

-- 反馈表：用户对投递的按钮/文字反馈
CREATE TABLE feedbacks (
    id          BIGSERIAL   PRIMARY KEY,
    user_id     BIGINT      NOT NULL,
    delivery_id BIGINT      NOT NULL,
    -- interested | not_interested | misjudged | deep_dive | question，应用层校验
    action      TEXT        NOT NULL,
    detail      TEXT        NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_feedbacks_user_id     FOREIGN KEY (user_id)     REFERENCES users (id),
    CONSTRAINT fk_feedbacks_delivery_id FOREIGN KEY (delivery_id) REFERENCES deliveries (id)
);

-- GetByDelivery
CREATE INDEX idx_feedbacks_delivery_id ON feedbacks (delivery_id);
-- ListByUser 反馈历史
CREATE INDEX idx_feedbacks_user_created ON feedbacks (user_id, created_at DESC);

-- 用户画像表：与 users 一对一，含 token 预算控制
CREATE TABLE profiles (
    id                 BIGSERIAL   PRIMARY KEY,
    user_id            BIGINT      NOT NULL,
    industry           TEXT        NOT NULL DEFAULT '',
    occupation         TEXT        NOT NULL DEFAULT '',
    tags               TEXT[]      NOT NULL DEFAULT '{}',
    summary            TEXT        NOT NULL DEFAULT '',
    token_budget_daily INT         NOT NULL DEFAULT 100000,
    tokens_used_today  INT         NOT NULL DEFAULT 0,
    token_reset_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_profiles_user_id FOREIGN KEY (user_id) REFERENCES users (id),
    CONSTRAINT uq_profiles_user_id UNIQUE (user_id)
);

-- LLM 调用记录表（Step 6 可观测性核心表）：
-- ref_type + ref_id 多态关联（如 'push_batch' / 'feedback'），刻意不建 FK
CREATE TABLE llm_calls (
    id                BIGSERIAL     PRIMARY KEY,
    trace_id          TEXT          NOT NULL DEFAULT '',
    span_name         TEXT          NOT NULL DEFAULT '',      -- 调用环节，真实写入方恰好六个：score(scorer.go:131)/cardgen(cardgen.go:77)/profile_evolve(evolver.go:116)/deep_dive(deepdive.go:218)/chat_reply(feishu/handler.go:174)/agent(agent/loop.go:206)
    user_id           BIGINT,                                 -- 归属用户，系统级调用为 NULL；刻意不建 FK（与多态关联一致）
    ref_type          TEXT          NOT NULL DEFAULT '',
    ref_id            BIGINT,                                 -- 无关联对象时为 NULL
    provider          TEXT          NOT NULL DEFAULT '',
    model             TEXT          NOT NULL DEFAULT '',
    system_prompt     TEXT          NOT NULL DEFAULT '',
    user_prompt       TEXT          NOT NULL DEFAULT '',
    completion        TEXT          NOT NULL DEFAULT '',
    prompt_tokens     INT           NOT NULL DEFAULT 0,
    completion_tokens INT           NOT NULL DEFAULT 0,
    latency_ms        INT           NOT NULL DEFAULT 0,
    cost_usd          NUMERIC(10,6) NOT NULL DEFAULT 0,
    prefix_cache_hit  BOOLEAN,                                -- 三态：TRUE/FALSE/NULL(provider 未报告)，CacheHitRate 以 IS NOT NULL 为分母
    temperature       REAL,                                   -- NULL = 未显式设置（0 是合法显式取值，不能混同）
    max_tokens        INT,                                    -- NULL = 未显式设置
    error             TEXT          NOT NULL DEFAULT '',
    created_at        TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- ListByTraceID 链路查看（规格索引设计：trace_id + created_at 升序）
CREATE INDEX idx_llm_calls_trace_id ON llm_calls (trace_id, created_at);
-- 多态关联查询（规格索引设计：partial index 排除无关联记录）
CREATE INDEX idx_llm_calls_ref ON llm_calls (ref_type, ref_id) WHERE ref_id IS NOT NULL;
-- 原为 90 天 TTL 清理按时间扫描而建。**TTL 已作废**（Boss 决策 2026-07-15：数据是
-- 资产，一律不清理，见 007）。索引保留，语义改为分析查询用：按时间窗口统计 LLM
-- 调用量与成本。
CREATE INDEX idx_llm_calls_created_at ON llm_calls (created_at);

-- +goose Down

-- 按 FK 依赖逆序删除
DROP TABLE IF EXISTS llm_calls;
DROP TABLE IF EXISTS profiles;
DROP TABLE IF EXISTS feedbacks;
DROP TABLE IF EXISTS deliveries;
DROP TABLE IF EXISTS push_batches;
DROP TABLE IF EXISTS content_items;
DROP TABLE IF EXISTS subscriptions;
DROP TABLE IF EXISTS sources;
DROP TABLE IF EXISTS users;
