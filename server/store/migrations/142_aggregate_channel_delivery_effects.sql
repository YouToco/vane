-- 142: provider-specific child receipts for aggregate Brief delivery.
--
-- deliveries remain the channel-neutral content ledger.  Telegram message IDs
-- must never be written into deliveries.feishu_message_id; instead this table
-- freezes each provider child effect and its exact delivery set.

-- +goose Up

CREATE TABLE aggregate_channel_delivery_effects (
    channel_effect_id     UUID        PRIMARY KEY,
    plan_id               UUID        NOT NULL,
    tenant_id             BIGINT      NOT NULL,
    user_id               BIGINT      NOT NULL,
    task_id               TEXT        NOT NULL,
    batch_id              BIGINT      NOT NULL,
    provider              TEXT        NOT NULL,
    chunk_index           INTEGER     NOT NULL,
    chunk_count           INTEGER     NOT NULL,
    delivery_ids          BIGINT[]    NOT NULL,
    status                TEXT        NOT NULL DEFAULT 'prepared',
    provider_message_ids  JSONB,
    sent_at               TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT fk_aggregate_channel_effect_plan
        FOREIGN KEY (plan_id,tenant_id,user_id)
        REFERENCES artifact_delivery_plans (id,tenant_id,user_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_aggregate_channel_effect_batch
        FOREIGN KEY (batch_id,tenant_id,user_id)
        REFERENCES push_batches (id,tenant_id,user_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_aggregate_channel_effect_outbound
        FOREIGN KEY (channel_effect_id)
        REFERENCES channel_outbound_effects (effect_id)
        ON DELETE RESTRICT,
    CONSTRAINT uq_aggregate_channel_effect_chunk
        UNIQUE (plan_id,provider,chunk_index),
    CONSTRAINT ck_aggregate_channel_effect_shape CHECK (
        provider='telegram' AND
        btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255 AND
        chunk_index>=0 AND chunk_count>0 AND chunk_index<chunk_count AND
        cardinality(delivery_ids)>0 AND
        array_position(delivery_ids,NULL) IS NULL
    ),
    CONSTRAINT ck_aggregate_channel_effect_status CHECK (
        (status='prepared' AND provider_message_ids IS NULL AND sent_at IS NULL) OR
        (status='sent' AND provider_message_ids IS NOT NULL AND
         jsonb_typeof(provider_message_ids)='array' AND
         jsonb_array_length(provider_message_ids)>0 AND sent_at IS NOT NULL)
    )
);

CREATE INDEX idx_aggregate_channel_effect_batch
    ON aggregate_channel_delivery_effects
       (tenant_id,user_id,batch_id,chunk_index);

ALTER TABLE aggregate_channel_delivery_effects ENABLE ROW LEVEL SECURITY;
CREATE POLICY aggregate_channel_effect_exact_user
    ON aggregate_channel_delivery_effects
    FOR ALL
    USING (
        tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
        user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    )
    WITH CHECK (
        tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
        user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );

REVOKE ALL ON aggregate_channel_delivery_effects FROM PUBLIC,vane_app;
GRANT SELECT,INSERT,UPDATE ON aggregate_channel_delivery_effects TO vane_app;

-- +goose Down

LOCK TABLE aggregate_channel_delivery_effects IN ACCESS EXCLUSIVE MODE;
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM aggregate_channel_delivery_effects) THEN
        RAISE EXCEPTION
            '142: refusing downgrade while aggregate channel effects exist';
    END IF;
END $$;
-- +goose StatementEnd
DROP TABLE aggregate_channel_delivery_effects;
