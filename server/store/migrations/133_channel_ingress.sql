-- 133: authenticated external-channel identities and durable ingress receipts.
--
-- Telegram is the first consumer. External actor/chat identifiers never become
-- Vane user IDs: a short-lived Web-authenticated link request binds them to an
-- existing tenant membership. Durable ingress receipts provide a stable Agent
-- turn identity and make webhook redelivery safe across process restarts.

-- +goose Up

CREATE TABLE channel_identities (
    id                BIGSERIAL   PRIMARY KEY,
    tenant_id         BIGINT      NOT NULL,
    user_id           BIGINT      NOT NULL,
    provider          TEXT        NOT NULL,
    app_identity      TEXT        NOT NULL,
    external_user_id  TEXT        NOT NULL,
    provider_chat_id  TEXT        NOT NULL,
    chat_type         TEXT        NOT NULL,
    status            TEXT        NOT NULL DEFAULT 'active',
    bound_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    revoked_at        TIMESTAMPTZ,
    CONSTRAINT fk_channel_identity_membership
        FOREIGN KEY (tenant_id,user_id)
        REFERENCES memberships (tenant_id,user_id) ON DELETE CASCADE,
    CONSTRAINT ck_channel_identity_provider
        CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    CONSTRAINT ck_channel_identity_app
        CHECK (octet_length(app_identity) BETWEEN 1 AND 128),
    CONSTRAINT ck_channel_identity_actor
        CHECK (octet_length(external_user_id) BETWEEN 1 AND 128),
    CONSTRAINT ck_channel_identity_chat
        CHECK (octet_length(provider_chat_id) BETWEEN 1 AND 128),
    CONSTRAINT ck_channel_identity_chat_type CHECK (chat_type='private'),
    CONSTRAINT ck_channel_identity_status CHECK (status IN ('active','revoked')),
    CONSTRAINT ck_channel_identity_revocation CHECK (
        (status='active' AND revoked_at IS NULL) OR
        (status='revoked' AND revoked_at IS NOT NULL)
    ),
    CONSTRAINT uq_channel_identity_scope UNIQUE (tenant_id,user_id,id)
);

CREATE UNIQUE INDEX uq_channel_identity_provider_actor_active
    ON channel_identities (provider,app_identity,external_user_id)
    WHERE status='active';
CREATE UNIQUE INDEX uq_channel_identity_provider_chat_active
    ON channel_identities (provider,app_identity,provider_chat_id)
    WHERE status='active';
CREATE UNIQUE INDEX uq_channel_identity_user_provider_active
    ON channel_identities (tenant_id,user_id,provider)
    WHERE status='active';
CREATE INDEX idx_channel_identity_scope
    ON channel_identities (tenant_id,user_id,provider,status);

CREATE TABLE channel_link_requests (
    token_hash            BYTEA       PRIMARY KEY,
    tenant_id             BIGINT      NOT NULL,
    user_id               BIGINT      NOT NULL,
    provider              TEXT        NOT NULL,
    app_identity          TEXT        NOT NULL,
    expires_at            TIMESTAMPTZ NOT NULL,
    consumed_at           TIMESTAMPTZ,
    consumed_identity_id  BIGINT,
    request_digest        TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT fk_channel_link_membership
        FOREIGN KEY (tenant_id,user_id)
        REFERENCES memberships (tenant_id,user_id) ON DELETE CASCADE,
    CONSTRAINT fk_channel_link_identity_scope
        FOREIGN KEY (tenant_id,user_id,consumed_identity_id)
        REFERENCES channel_identities (tenant_id,user_id,id),
    CONSTRAINT ck_channel_link_token CHECK (octet_length(token_hash)=32),
    CONSTRAINT ck_channel_link_provider
        CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    CONSTRAINT ck_channel_link_app
        CHECK (octet_length(app_identity) BETWEEN 1 AND 128),
    CONSTRAINT ck_channel_link_expiry CHECK (expires_at > created_at),
    CONSTRAINT ck_channel_link_consumption CHECK (
        (consumed_at IS NULL AND consumed_identity_id IS NULL AND request_digest IS NULL) OR
        (consumed_at IS NOT NULL AND consumed_identity_id IS NOT NULL AND
         request_digest ~ '^[0-9a-f]{64}$')
    )
);
CREATE INDEX idx_channel_link_scope
    ON channel_link_requests (tenant_id,user_id,provider,created_at DESC);

CREATE TABLE channel_ingress_receipts (
    provider             TEXT        NOT NULL,
    app_identity         TEXT        NOT NULL,
    provider_update_id   TEXT        NOT NULL,
    identity_id          BIGINT      NOT NULL,
    tenant_id            BIGINT      NOT NULL,
    user_id              BIGINT      NOT NULL,
    external_user_id     TEXT        NOT NULL,
    provider_chat_id     TEXT        NOT NULL,
    payload_digest       TEXT        NOT NULL,
    input_text           TEXT        NOT NULL,
    stable_turn_id       UUID        NOT NULL,
    status               TEXT        NOT NULL DEFAULT 'pending',
    attempt_count        INTEGER     NOT NULL DEFAULT 0,
    processing_lease     UUID,
    lease_expires_at     TIMESTAMPTZ,
    reply_text           TEXT,
    provider_message_ids JSONB,
    error_code           TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider,app_identity,provider_update_id),
    CONSTRAINT fk_channel_ingress_identity_scope
        FOREIGN KEY (tenant_id,user_id,identity_id)
        REFERENCES channel_identities (tenant_id,user_id,id) ON DELETE CASCADE,
    CONSTRAINT ck_channel_ingress_provider
        CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    CONSTRAINT ck_channel_ingress_identity
        CHECK (octet_length(app_identity) BETWEEN 1 AND 128 AND
               octet_length(provider_update_id) BETWEEN 1 AND 128 AND
               octet_length(external_user_id) BETWEEN 1 AND 128 AND
               octet_length(provider_chat_id) BETWEEN 1 AND 128),
    CONSTRAINT ck_channel_ingress_telegram_update_id CHECK (
        provider <> 'telegram' OR
        (provider_update_id ~ '^(0|[1-9][0-9]{0,18})$' AND
         provider_update_id::numeric <= 9223372036854775807)
    ),
    CONSTRAINT ck_channel_ingress_digest
        CHECK (payload_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_channel_ingress_input
        CHECK (octet_length(input_text) BETWEEN 1 AND 65536),
    CONSTRAINT ck_channel_ingress_status CHECK (
        status IN ('pending','processing','reply_ready','sending',
                   'completed','failed','ambiguous')
    ),
    CONSTRAINT ck_channel_ingress_attempt CHECK (attempt_count >= 0),
    CONSTRAINT ck_channel_ingress_lease CHECK (
        (status='processing' AND processing_lease IS NOT NULL AND
         lease_expires_at IS NOT NULL) OR
        (status<>'processing' AND processing_lease IS NULL AND
         lease_expires_at IS NULL)
    ),
    CONSTRAINT ck_channel_ingress_reply CHECK (
        (status IN ('reply_ready','sending','completed','ambiguous') AND
         reply_text IS NOT NULL AND octet_length(reply_text) BETWEEN 1 AND 262144) OR
        (status IN ('pending','processing','failed') AND reply_text IS NULL)
    ),
    CONSTRAINT ck_channel_ingress_messages CHECK (
        provider_message_ids IS NULL OR
        jsonb_typeof(provider_message_ids)='array'
    )
);
CREATE UNIQUE INDEX uq_channel_ingress_stable_turn
    ON channel_ingress_receipts (stable_turn_id);
CREATE INDEX idx_channel_ingress_recovery
    ON channel_ingress_receipts (status,lease_expires_at,created_at);
CREATE INDEX idx_channel_ingress_scope
    ON channel_ingress_receipts (tenant_id,user_id,created_at DESC);

REVOKE ALL ON channel_identities,channel_link_requests,channel_ingress_receipts
    FROM PUBLIC,vane_app;
REVOKE ALL ON SEQUENCE channel_identities_id_seq FROM PUBLIC,vane_app;

ALTER TABLE channel_identities ENABLE ROW LEVEL SECURITY;
ALTER TABLE channel_link_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE channel_ingress_receipts ENABLE ROW LEVEL SECURITY;

CREATE POLICY channel_identities_exact_user ON channel_identities
    USING (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    ) WITH CHECK (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );
CREATE POLICY channel_link_requests_exact_user ON channel_link_requests
    USING (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    ) WITH CHECK (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );
CREATE POLICY channel_ingress_receipts_exact_user ON channel_ingress_receipts
    USING (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    ) WITH CHECK (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );

-- +goose Down

LOCK TABLE channel_ingress_receipts,channel_link_requests,channel_identities
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM channel_ingress_receipts) OR
       EXISTS (SELECT 1 FROM channel_link_requests) OR
       EXISTS (SELECT 1 FROM channel_identities) THEN
        RAISE EXCEPTION '133: refusing downgrade while Telegram channel history exists';
    END IF;
END $$;
-- +goose StatementEnd

DROP TABLE channel_ingress_receipts;
DROP TABLE channel_link_requests;
DROP TABLE channel_identities;
