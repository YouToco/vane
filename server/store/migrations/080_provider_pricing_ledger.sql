-- 080: 可版本化 Provider 价格目录 + 精确用量计价回执。
--
-- 单价不能编译进 binary：模型、Exa、TikHub 都会改价，且同一供应商存在
-- token 三段价、按请求/页面计价等不同 meter。价格规则是平台全局配置，
-- 仅由 requirePlatformOwner 管理；调用行在写入时绑定 exact rule_id 与金额，
-- 后续改价不重写历史账单。

-- +goose Up

CREATE TABLE provider_price_rules (
    id                              BIGSERIAL PRIMARY KEY,
    provider                        TEXT NOT NULL,
    resource                        TEXT NOT NULL,
    meter                           TEXT NOT NULL,
    currency                        TEXT NOT NULL DEFAULT 'USD',
    input_cache_hit_per_million     NUMERIC(18,8),
    input_cache_miss_per_million    NUMERIC(18,8),
    output_per_million              NUMERIC(18,8),
    request_unit_price              NUMERIC(18,8),
    request_included_quantity       NUMERIC(18,6),
    request_additional_unit_price   NUMERIC(18,8),
    effective_from                  TIMESTAMPTZ NOT NULL,
    effective_to                    TIMESTAMPTZ,
    source_url                      TEXT NOT NULL,
    note                            TEXT NOT NULL DEFAULT '',
    created_by                      BIGINT,
    change_id                       TEXT NOT NULL DEFAULT '',
    request_hash                    TEXT NOT NULL DEFAULT '',
    created_at                      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_provider_price_rules_created_by
        FOREIGN KEY (created_by) REFERENCES users(id),
    CONSTRAINT ck_provider_price_rules_identity
        CHECK (provider = lower(btrim(provider)) AND provider <> ''
           AND resource = btrim(resource) AND resource <> ''),
    CONSTRAINT ck_provider_price_rules_meter
        CHECK (meter IN ('llm_tokens', 'request')),
    CONSTRAINT ck_provider_price_rules_currency
        CHECK (currency IN ('USD', 'CNY')),
    CONSTRAINT ck_provider_price_rules_window
        CHECK (effective_to IS NULL OR effective_to > effective_from),
    CONSTRAINT ck_provider_price_rules_nonnegative
        CHECK (
            COALESCE(input_cache_hit_per_million, 0) >= 0
            AND COALESCE(input_cache_miss_per_million, 0) >= 0
            AND COALESCE(output_per_million, 0) >= 0
            AND COALESCE(request_unit_price, 0) >= 0
            AND COALESCE(request_included_quantity, 0) >= 0
            AND COALESCE(request_additional_unit_price, 0) >= 0
        ),
    CONSTRAINT ck_provider_price_rules_shape
        CHECK (
            (meter = 'llm_tokens'
             AND input_cache_hit_per_million IS NOT NULL
             AND input_cache_miss_per_million IS NOT NULL
             AND output_per_million IS NOT NULL
             AND request_unit_price IS NULL
             AND request_included_quantity IS NULL
             AND request_additional_unit_price IS NULL)
            OR
            (meter = 'request'
             AND input_cache_hit_per_million IS NULL
             AND input_cache_miss_per_million IS NULL
             AND output_per_million IS NULL
             AND request_unit_price IS NOT NULL
             AND request_included_quantity IS NOT NULL
             AND request_additional_unit_price IS NOT NULL)
        )
);

CREATE INDEX idx_provider_price_rules_lookup
    ON provider_price_rules(provider, resource, meter, effective_from DESC);
CREATE UNIQUE INDEX uq_provider_price_rules_open
    ON provider_price_rules(provider, resource, meter)
    WHERE effective_to IS NULL;
CREATE UNIQUE INDEX uq_provider_price_rules_change
    ON provider_price_rules(change_id)
    WHERE change_id <> '';

-- +goose StatementBegin
CREATE FUNCTION protect_provider_price_rule_version()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.effective_to IS NOT NULL
       OR NEW.effective_to IS NULL
       OR NEW.effective_to <= OLD.effective_from
       OR NEW.id IS DISTINCT FROM OLD.id
       OR NEW.provider IS DISTINCT FROM OLD.provider
       OR NEW.resource IS DISTINCT FROM OLD.resource
       OR NEW.meter IS DISTINCT FROM OLD.meter
       OR NEW.currency IS DISTINCT FROM OLD.currency
       OR NEW.input_cache_hit_per_million IS DISTINCT FROM OLD.input_cache_hit_per_million
       OR NEW.input_cache_miss_per_million IS DISTINCT FROM OLD.input_cache_miss_per_million
       OR NEW.output_per_million IS DISTINCT FROM OLD.output_per_million
       OR NEW.request_unit_price IS DISTINCT FROM OLD.request_unit_price
       OR NEW.request_included_quantity IS DISTINCT FROM OLD.request_included_quantity
       OR NEW.request_additional_unit_price IS DISTINCT FROM OLD.request_additional_unit_price
       OR NEW.effective_from IS DISTINCT FROM OLD.effective_from
       OR NEW.source_url IS DISTINCT FROM OLD.source_url
       OR NEW.note IS DISTINCT FROM OLD.note
       OR NEW.created_by IS DISTINCT FROM OLD.created_by
       OR NEW.change_id IS DISTINCT FROM OLD.change_id
       OR NEW.request_hash IS DISTINCT FROM OLD.request_hash
       OR NEW.created_at IS DISTINCT FROM OLD.created_at
    THEN
        RAISE EXCEPTION 'provider price versions are immutable; only an open interval may be closed';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd

CREATE TRIGGER trg_provider_price_rules_immutable
BEFORE UPDATE ON provider_price_rules
FOR EACH ROW EXECUTE FUNCTION protect_provider_price_rule_version();

ALTER TABLE llm_calls
    ADD COLUMN prompt_cache_hit_tokens INT,
    ADD COLUMN prompt_cache_miss_tokens INT,
    ADD COLUMN reasoning_tokens INT,
    ADD COLUMN pricing_rule_id BIGINT,
    ADD COLUMN pricing_status TEXT NOT NULL DEFAULT 'legacy',
    ADD COLUMN cost_amount NUMERIC(18,8),
    ADD COLUMN cost_currency TEXT;

ALTER TABLE llm_calls
    ADD CONSTRAINT fk_llm_calls_pricing_rule
        FOREIGN KEY (pricing_rule_id) REFERENCES provider_price_rules(id),
    ADD CONSTRAINT ck_llm_calls_cache_tokens
        CHECK (
            (prompt_cache_hit_tokens IS NULL AND prompt_cache_miss_tokens IS NULL)
            OR
            (prompt_cache_hit_tokens >= 0 AND prompt_cache_miss_tokens >= 0
             AND prompt_cache_hit_tokens + prompt_cache_miss_tokens = prompt_tokens)
        ),
    ADD CONSTRAINT ck_llm_calls_reasoning_tokens
        CHECK (reasoning_tokens IS NULL OR
               (reasoning_tokens >= 0 AND reasoning_tokens <= completion_tokens)),
    ADD CONSTRAINT ck_llm_calls_pricing_status
        CHECK (pricing_status IN ('legacy', 'provider_reported', 'calculated', 'estimated', 'unpriced')),
    ADD CONSTRAINT ck_llm_calls_cost_currency
        CHECK (cost_currency IS NULL OR cost_currency IN ('USD', 'CNY'));

UPDATE llm_calls
   SET cost_amount = cost_usd,
       cost_currency = 'USD',
       pricing_status = 'legacy';

ALTER TABLE tool_calls
    ADD COLUMN provider TEXT NOT NULL DEFAULT '',
    ADD COLUMN usage_quantity NUMERIC(18,6) NOT NULL DEFAULT 1,
    ADD COLUMN pricing_rule_id BIGINT,
    ADD COLUMN pricing_status TEXT NOT NULL DEFAULT 'legacy',
    ADD COLUMN cost_amount NUMERIC(18,8),
    ADD COLUMN cost_currency TEXT;

ALTER TABLE tool_calls
    ADD CONSTRAINT fk_tool_calls_pricing_rule
        FOREIGN KEY (pricing_rule_id) REFERENCES provider_price_rules(id),
    ADD CONSTRAINT ck_tool_calls_usage_quantity
        CHECK (usage_quantity >= 0),
    ADD CONSTRAINT ck_tool_calls_pricing_status
        CHECK (pricing_status IN ('legacy', 'provider_reported', 'calculated', 'estimated', 'unpriced')),
    ADD CONSTRAINT ck_tool_calls_cost_currency
        CHECK (cost_currency IS NULL OR cost_currency IN ('USD', 'CNY'));

UPDATE tool_calls
   SET cost_amount = cost_usd,
       cost_currency = CASE WHEN cost_usd IS NULL THEN NULL ELSE 'USD' END,
       pricing_status = 'legacy';

-- 当前官方公开价仅作为数据库初始版本；运行时代码不含任何单价。
-- 管理员更新时会关闭旧版本并插入新版本，历史调用仍绑定旧 rule_id/金额。
INSERT INTO provider_price_rules (
    provider, resource, meter, currency,
    input_cache_hit_per_million, input_cache_miss_per_million, output_per_million,
    effective_from, source_url, note, created_by
)
SELECT provider, resource, 'llm_tokens', 'USD', hit_price, miss_price, output_price,
       TIMESTAMPTZ '2026-07-30 00:00:00+00', source_url, '080 初始官方价', NULL
  FROM (VALUES
    ('deepseek', 'deepseek-v4-flash', 0.0028::numeric, 0.14::numeric, 0.28::numeric,
     'https://api-docs.deepseek.com/quick_start/pricing'),
    ('deepseek', 'deepseek-v4-pro', 0.003625::numeric, 0.435::numeric, 0.87::numeric,
     'https://api-docs.deepseek.com/quick_start/pricing'),
    ('kimi', 'kimi-k2.6', 0.16::numeric, 0.95::numeric, 4.00::numeric,
     'https://platform.kimi.ai/docs/pricing/chat-k26')
  ) seed(provider, resource, hit_price, miss_price, output_price, source_url);

INSERT INTO provider_price_rules (
    provider, resource, meter, currency, request_unit_price,
    request_included_quantity, request_additional_unit_price,
    effective_from, source_url, note, created_by
)
SELECT provider, resource, 'request', 'USD', unit_price, included_quantity,
       additional_unit_price,
       TIMESTAMPTZ '2026-07-30 00:00:00+00', source_url, '080 初始官方价', NULL
  FROM (VALUES
    ('exa', '/search', 0.007::numeric, 10::numeric, 0.001::numeric, 'https://exa.ai/pricing'),
    ('exa', '/contents', 0.001::numeric, 1::numeric, 0.001::numeric, 'https://exa.ai/pricing'),
    -- TikHub 的通用页面只承诺“多数服务”基础价，不能据此把所有端点都算成
    -- $0.001。以下 exact 路径沿用已由 provider 使用日志核对的价格；未知路径
    -- 用保守 wildcard 估算并在调用回执上标 estimated，管理员可随时版本化修正。
    ('tikhub', '/api/v1/xiaohongshu/app_v2/search_notes',
     0.010::numeric, 1::numeric, 0.010::numeric, 'https://docs.tikhub.io/4592751m0'),
    ('tikhub', '/api/v1/xiaohongshu/app_v2/get_user_posted_notes',
     0.010::numeric, 1::numeric, 0.010::numeric, 'https://docs.tikhub.io/4592751m0'),
    ('tikhub', '/api/v1/xiaohongshu/app_v2/get_topic_feed',
     0.010::numeric, 1::numeric, 0.010::numeric, 'https://docs.tikhub.io/4592751m0'),
    ('tikhub', '/api/v1/xiaohongshu/app_v2/get_user_faved_notes',
     0.010::numeric, 1::numeric, 0.010::numeric, 'https://docs.tikhub.io/4592751m0'),
    ('tikhub', '/api/v1/xiaohongshu/web_v3/fetch_note_detail',
     0.010::numeric, 1::numeric, 0.010::numeric, 'https://docs.tikhub.io/4592751m0'),
    ('tikhub', '/api/v1/xiaohongshu/web_v3/fetch_hot_list',
     0.001::numeric, 1::numeric, 0.001::numeric, 'https://docs.tikhub.io/4592751m0'),
    ('tikhub', '/api/v1/twitter/web/fetch_user_post_tweet',
     0.001::numeric, 1::numeric, 0.001::numeric, 'https://docs.tikhub.io/4592751m0'),
    ('tikhub', '/api/v1/wechat_mp/v2/fetch_account_articles',
     0.010::numeric, 1::numeric, 0.010::numeric, 'https://docs.tikhub.io/4592751m0'),
    ('tikhub', '*',
     0.010::numeric, 1::numeric, 0.010::numeric, 'https://docs.tikhub.io/4592751m0')
  ) seed(provider, resource, unit_price, included_quantity, additional_unit_price, source_url);

GRANT SELECT, INSERT, UPDATE ON provider_price_rules TO vane_app;
GRANT USAGE, SELECT ON SEQUENCE provider_price_rules_id_seq TO vane_app;

-- +goose Down

-- 与三个业务 writer 共用 advisory fence；拿到 exclusive 后再取表锁，
-- 保证“检查为空 → DROP”之间不会插入一条刚发生的付费回执。
SELECT pg_advisory_xact_lock(
    hashtextextended('vane-provider-pricing-v1', 0)
);
LOCK TABLE provider_price_rules, llm_calls, tool_calls
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM llm_calls WHERE pricing_status <> 'legacy'
    ) OR EXISTS (
        SELECT 1 FROM tool_calls WHERE pricing_status <> 'legacy'
    ) OR EXISTS (
        SELECT 1
          FROM provider_price_rules
         WHERE created_by IS NOT NULL
            OR change_id <> ''
            OR effective_to IS NOT NULL
    ) THEN
        RAISE EXCEPTION
            'refusing Down while provider pricing ledger state exists';
    END IF;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON SEQUENCE provider_price_rules_id_seq FROM vane_app;
REVOKE ALL ON provider_price_rules FROM vane_app;

ALTER TABLE tool_calls
    DROP CONSTRAINT ck_tool_calls_cost_currency,
    DROP CONSTRAINT ck_tool_calls_pricing_status,
    DROP CONSTRAINT ck_tool_calls_usage_quantity,
    DROP CONSTRAINT fk_tool_calls_pricing_rule,
    DROP COLUMN cost_currency,
    DROP COLUMN cost_amount,
    DROP COLUMN pricing_status,
    DROP COLUMN pricing_rule_id,
    DROP COLUMN usage_quantity,
    DROP COLUMN provider;

ALTER TABLE llm_calls
    DROP CONSTRAINT ck_llm_calls_cost_currency,
    DROP CONSTRAINT ck_llm_calls_pricing_status,
    DROP CONSTRAINT ck_llm_calls_reasoning_tokens,
    DROP CONSTRAINT ck_llm_calls_cache_tokens,
    DROP CONSTRAINT fk_llm_calls_pricing_rule,
    DROP COLUMN cost_currency,
    DROP COLUMN cost_amount,
    DROP COLUMN pricing_status,
    DROP COLUMN pricing_rule_id,
    DROP COLUMN reasoning_tokens,
    DROP COLUMN prompt_cache_miss_tokens,
    DROP COLUMN prompt_cache_hit_tokens;

DROP TABLE provider_price_rules;
DROP FUNCTION protect_provider_price_rule_version();
