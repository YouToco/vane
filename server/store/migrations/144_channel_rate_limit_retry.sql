-- 144: durable provider-declared rate-limit deferral for channel sends.
--
-- Telegram 429 is a definite non-send and may be retried after the provider's
-- retry_after delay. Transport errors, 5xx, and response loss remain
-- ambiguous and are never moved back before the provider boundary.

-- +goose Up

ALTER TABLE channel_ingress_receipts
    ADD COLUMN send_retry_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN next_send_at TIMESTAMPTZ;
ALTER TABLE channel_ingress_receipts
    ADD CONSTRAINT ck_channel_ingress_send_retry
        CHECK (send_retry_count BETWEEN 0 AND 100),
    ADD CONSTRAINT ck_channel_ingress_send_schedule
        CHECK (next_send_at IS NULL OR status='reply_ready');
CREATE INDEX idx_channel_ingress_send_schedule
    ON channel_ingress_receipts (next_send_at,provider,app_identity)
    WHERE status='reply_ready';

ALTER TABLE channel_outbound_effects
    ADD COLUMN send_retry_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN next_send_at TIMESTAMPTZ;
ALTER TABLE channel_outbound_effects
    ADD CONSTRAINT ck_channel_outbound_send_retry
        CHECK (send_retry_count BETWEEN 0 AND 100),
    ADD CONSTRAINT ck_channel_outbound_send_schedule
        CHECK (next_send_at IS NULL OR status='prepared');
CREATE INDEX idx_channel_outbound_send_schedule
    ON channel_outbound_effects (next_send_at,provider,app_identity)
    WHERE status='prepared';

-- +goose Down

LOCK TABLE channel_ingress_receipts,channel_outbound_effects
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM channel_ingress_receipts
         WHERE send_retry_count<>0 OR next_send_at IS NOT NULL
    ) OR EXISTS (
        SELECT 1 FROM channel_outbound_effects
         WHERE send_retry_count<>0 OR next_send_at IS NOT NULL
    ) THEN
        RAISE EXCEPTION
            'migration 144 down refused: channel rate-limit history exists';
    END IF;
END
$$;
-- +goose StatementEnd

DROP INDEX idx_channel_ingress_send_schedule;
ALTER TABLE channel_ingress_receipts
    DROP CONSTRAINT ck_channel_ingress_send_retry,
    DROP CONSTRAINT ck_channel_ingress_send_schedule,
    DROP COLUMN send_retry_count,
    DROP COLUMN next_send_at;

DROP INDEX idx_channel_outbound_send_schedule;
ALTER TABLE channel_outbound_effects
    DROP CONSTRAINT ck_channel_outbound_send_retry,
    DROP CONSTRAINT ck_channel_outbound_send_schedule,
    DROP COLUMN send_retry_count,
    DROP COLUMN next_send_at;
