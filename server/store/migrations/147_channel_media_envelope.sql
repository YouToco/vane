-- 147: durable provider-neutral media metadata for authenticated channel ingress.
--
-- Webhooks persist only a versioned envelope containing provider opaque file
-- references and untrusted metadata. No bot token, provider download URL,
-- temporary file path or media bytes enter this table. Existing text receipts
-- remain NULL and preserve their historical meaning.

-- +goose Up

ALTER TABLE channel_ingress_receipts
    ADD COLUMN media_envelope JSONB;

ALTER TABLE channel_ingress_receipts
    ADD CONSTRAINT ck_channel_ingress_media_envelope CHECK (
        media_envelope IS NULL OR (
            jsonb_typeof(media_envelope)='object' AND
            media_envelope->>'schema'='vane.channel-message/v1' AND
            jsonb_typeof(media_envelope->'items')='array' AND
            jsonb_array_length(media_envelope->'items') BETWEEN 1 AND 10 AND
            octet_length(media_envelope::text) <= 65536
        )
    );

-- +goose Down

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM channel_ingress_receipts WHERE media_envelope IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'refusing downgrade while channel media history exists';
    END IF;
END $$;
-- +goose StatementEnd

ALTER TABLE channel_ingress_receipts
    DROP CONSTRAINT ck_channel_ingress_media_envelope;

ALTER TABLE channel_ingress_receipts
    DROP COLUMN media_envelope;
