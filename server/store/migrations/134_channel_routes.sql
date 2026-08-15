-- 134: provider-neutral chat routes and Telegram group/topic metadata.
--
-- A channel identity authenticates an external human. A route authorizes one
-- destination (private chat, group, supergroup, or forum topic). Keeping those
-- concepts separate lets one Vane user retain one Telegram identity while
-- installing the Bot into multiple explicit destinations.

-- +goose Up

CREATE TABLE channel_routes (
    id                  BIGSERIAL   PRIMARY KEY,
    tenant_id           BIGINT      NOT NULL,
    user_id             BIGINT      NOT NULL,
    identity_id         BIGINT      NOT NULL,
    provider            TEXT        NOT NULL,
    app_identity        TEXT        NOT NULL,
    provider_chat_id    TEXT        NOT NULL,
    provider_thread_id  TEXT        NOT NULL DEFAULT '0',
    chat_type           TEXT        NOT NULL,
    route_kind          TEXT        NOT NULL,
    status              TEXT        NOT NULL DEFAULT 'active',
    bound_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    revoked_at          TIMESTAMPTZ,
    CONSTRAINT fk_channel_route_identity_scope
        FOREIGN KEY (tenant_id,user_id,identity_id)
        REFERENCES channel_identities (tenant_id,user_id,id) ON DELETE CASCADE,
    CONSTRAINT ck_channel_route_provider
        CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    CONSTRAINT ck_channel_route_identity
        CHECK (octet_length(app_identity) BETWEEN 1 AND 128 AND
               octet_length(provider_chat_id) BETWEEN 1 AND 128 AND
               provider_thread_id ~ '^(0|[1-9][0-9]{0,18})$'),
    CONSTRAINT ck_channel_route_chat_type
        CHECK (chat_type IN ('private','group','supergroup','channel')),
    CONSTRAINT ck_channel_route_kind
        CHECK ((route_kind='private' AND chat_type='private' AND provider_thread_id='0') OR
               (route_kind='group' AND chat_type IN ('group','supergroup') AND
                provider_thread_id='0') OR
               (route_kind='topic' AND chat_type='supergroup' AND
                provider_thread_id<>'0') OR
               (route_kind='channel' AND chat_type='channel' AND
                provider_thread_id='0')),
    CONSTRAINT ck_channel_route_status CHECK (status IN ('active','revoked')),
    CONSTRAINT ck_channel_route_revocation CHECK (
        (status='active' AND revoked_at IS NULL) OR
        (status='revoked' AND revoked_at IS NOT NULL)
    ),
    CONSTRAINT uq_channel_route_scope UNIQUE (tenant_id,user_id,id)
);
CREATE UNIQUE INDEX uq_channel_route_destination_active
    ON channel_routes (provider,app_identity,provider_chat_id,provider_thread_id)
    WHERE status='active';
CREATE INDEX idx_channel_route_user
    ON channel_routes (tenant_id,user_id,provider,status,bound_at DESC);

INSERT INTO channel_routes (
    tenant_id,user_id,identity_id,provider,app_identity,provider_chat_id,
    provider_thread_id,chat_type,route_kind,status,bound_at,revoked_at
)
SELECT tenant_id,user_id,id,provider,app_identity,provider_chat_id,'0',
       'private','private',status,bound_at,revoked_at
  FROM channel_identities;

CREATE TABLE channel_route_link_requests (
    token_hash          BYTEA       PRIMARY KEY,
    tenant_id           BIGINT      NOT NULL,
    user_id             BIGINT      NOT NULL,
    provider            TEXT        NOT NULL,
    app_identity        TEXT        NOT NULL,
    expires_at          TIMESTAMPTZ NOT NULL,
    consumed_at         TIMESTAMPTZ,
    consumed_route_id   BIGINT,
    request_digest      TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT fk_channel_route_link_membership
        FOREIGN KEY (tenant_id,user_id)
        REFERENCES memberships (tenant_id,user_id) ON DELETE CASCADE,
    CONSTRAINT fk_channel_route_link_scope
        FOREIGN KEY (tenant_id,user_id,consumed_route_id)
        REFERENCES channel_routes (tenant_id,user_id,id),
    CONSTRAINT ck_channel_route_link_token CHECK (octet_length(token_hash)=32),
    CONSTRAINT ck_channel_route_link_provider
        CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    CONSTRAINT ck_channel_route_link_app
        CHECK (octet_length(app_identity) BETWEEN 1 AND 128),
    CONSTRAINT ck_channel_route_link_expiry CHECK (expires_at > created_at),
    CONSTRAINT ck_channel_route_link_consumption CHECK (
        (consumed_at IS NULL AND consumed_route_id IS NULL AND request_digest IS NULL) OR
        (consumed_at IS NOT NULL AND consumed_route_id IS NOT NULL AND
         request_digest ~ '^[0-9a-f]{64}$')
    )
);
CREATE INDEX idx_channel_route_link_scope
    ON channel_route_link_requests
       (tenant_id,user_id,provider,created_at DESC);

ALTER TABLE channel_ingress_receipts
    ADD COLUMN route_id BIGINT,
    ADD COLUMN provider_thread_id TEXT NOT NULL DEFAULT '0',
    ADD COLUMN provider_message_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN ingress_kind TEXT NOT NULL DEFAULT 'message',
    ADD COLUMN callback_query_id TEXT NOT NULL DEFAULT '';
ALTER TABLE channel_ingress_receipts
    ADD CONSTRAINT fk_channel_ingress_route_scope
        FOREIGN KEY (tenant_id,user_id,route_id)
        REFERENCES channel_routes (tenant_id,user_id,id),
    ADD CONSTRAINT ck_channel_ingress_thread
        CHECK (provider_thread_id ~ '^(0|[1-9][0-9]{0,18})$'),
    ADD CONSTRAINT ck_channel_ingress_message
        CHECK (octet_length(provider_message_id) <= 128),
    ADD CONSTRAINT ck_channel_ingress_kind
        CHECK (ingress_kind IN ('message','command','callback')),
    ADD CONSTRAINT ck_channel_ingress_callback
        CHECK ((ingress_kind='callback' AND octet_length(callback_query_id) BETWEEN 1 AND 128) OR
               (ingress_kind<>'callback' AND callback_query_id=''));
UPDATE channel_ingress_receipts r
   SET route_id=cr.id
  FROM channel_routes cr
 WHERE cr.identity_id=r.identity_id AND cr.route_kind='private';
ALTER TABLE channel_ingress_receipts ALTER COLUMN route_id SET NOT NULL;
CREATE INDEX idx_channel_ingress_route_order
    ON channel_ingress_receipts
       (route_id,(provider_update_id::numeric))
    WHERE provider='telegram' AND
          status IN ('pending','processing','reply_ready');

CREATE TABLE channel_message_mappings (
    provider             TEXT        NOT NULL,
    app_identity         TEXT        NOT NULL,
    provider_chat_id     TEXT        NOT NULL,
    provider_thread_id   TEXT        NOT NULL DEFAULT '0',
    provider_message_id  TEXT        NOT NULL,
    tenant_id            BIGINT      NOT NULL,
    user_id              BIGINT      NOT NULL,
    route_id             BIGINT      NOT NULL,
    direction            TEXT        NOT NULL,
    message_kind         TEXT        NOT NULL,
    correlation_key      TEXT        NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (
        provider,app_identity,provider_chat_id,provider_thread_id,
        provider_message_id
    ),
    CONSTRAINT fk_channel_message_route_scope
        FOREIGN KEY (tenant_id,user_id,route_id)
        REFERENCES channel_routes (tenant_id,user_id,id) ON DELETE CASCADE,
    CONSTRAINT ck_channel_message_provider
        CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    CONSTRAINT ck_channel_message_identity
        CHECK (octet_length(app_identity) BETWEEN 1 AND 128 AND
               octet_length(provider_chat_id) BETWEEN 1 AND 128 AND
               provider_thread_id ~ '^(0|[1-9][0-9]{0,18})$' AND
               octet_length(provider_message_id) BETWEEN 1 AND 128),
    CONSTRAINT ck_channel_message_direction
        CHECK (direction IN ('inbound','outbound')),
    CONSTRAINT ck_channel_message_kind
        CHECK (message_kind IN ('message','agent','command','callback','notification','test')),
    CONSTRAINT ck_channel_message_correlation
        CHECK (octet_length(correlation_key) BETWEEN 1 AND 256)
);
CREATE INDEX idx_channel_message_scope
    ON channel_message_mappings
       (tenant_id,user_id,created_at DESC);

CREATE TABLE channel_outbound_effects (
    effect_id             UUID        PRIMARY KEY,
    tenant_id             BIGINT      NOT NULL,
    user_id               BIGINT      NOT NULL,
    route_id              BIGINT      NOT NULL,
    provider              TEXT        NOT NULL,
    app_identity          TEXT        NOT NULL,
    provider_chat_id      TEXT        NOT NULL,
    provider_thread_id    TEXT        NOT NULL DEFAULT '0',
    effect_kind           TEXT        NOT NULL,
    payload_text          TEXT        NOT NULL,
    payload_digest        TEXT        NOT NULL,
    status                TEXT        NOT NULL DEFAULT 'prepared',
    provider_message_ids  JSONB,
    error_code            TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT fk_channel_outbound_route_scope
        FOREIGN KEY (tenant_id,user_id,route_id)
        REFERENCES channel_routes (tenant_id,user_id,id) ON DELETE CASCADE,
    CONSTRAINT ck_channel_outbound_provider
        CHECK (provider ~ '^[a-z][a-z0-9_-]{0,31}$'),
    CONSTRAINT ck_channel_outbound_identity
        CHECK (octet_length(app_identity) BETWEEN 1 AND 128 AND
               octet_length(provider_chat_id) BETWEEN 1 AND 128 AND
               provider_thread_id ~ '^(0|[1-9][0-9]{0,18})$'),
    CONSTRAINT ck_channel_outbound_kind
        CHECK (effect_kind ~ '^[a-z][a-z0-9_-]{0,63}$'),
    CONSTRAINT ck_channel_outbound_payload
        CHECK (octet_length(payload_text) BETWEEN 1 AND 262144 AND
               payload_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_channel_outbound_status
        CHECK (status IN ('prepared','sending','sent','failed','ambiguous')),
    CONSTRAINT ck_channel_outbound_messages
        CHECK (provider_message_ids IS NULL OR
               jsonb_typeof(provider_message_ids)='array'),
    CONSTRAINT ck_channel_outbound_terminal CHECK (
        (status IN ('prepared','sending') AND provider_message_ids IS NULL AND
         error_code IS NULL) OR
        (status='sent' AND provider_message_ids IS NOT NULL AND error_code IS NULL) OR
        (status IN ('failed','ambiguous') AND error_code IS NOT NULL)
    )
);
CREATE INDEX idx_channel_outbound_scope
    ON channel_outbound_effects (tenant_id,user_id,updated_at DESC);
CREATE INDEX idx_channel_outbound_blocked
    ON channel_outbound_effects (provider,app_identity,status,updated_at)
    WHERE status IN ('sending','failed','ambiguous');

REVOKE ALL ON channel_routes,channel_route_link_requests,
    channel_message_mappings,channel_outbound_effects FROM PUBLIC,vane_app;
REVOKE ALL ON SEQUENCE channel_routes_id_seq FROM PUBLIC,vane_app;

ALTER TABLE channel_routes ENABLE ROW LEVEL SECURITY;
ALTER TABLE channel_route_link_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE channel_message_mappings ENABLE ROW LEVEL SECURITY;
ALTER TABLE channel_outbound_effects ENABLE ROW LEVEL SECURITY;

CREATE POLICY channel_routes_exact_user ON channel_routes
    USING (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    ) WITH CHECK (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );
CREATE POLICY channel_route_link_requests_exact_user ON channel_route_link_requests
    USING (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    ) WITH CHECK (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );
CREATE POLICY channel_message_mappings_exact_user ON channel_message_mappings
    USING (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    ) WITH CHECK (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );
CREATE POLICY channel_outbound_effects_exact_user ON channel_outbound_effects
    USING (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    ) WITH CHECK (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );

-- +goose Down

LOCK TABLE channel_outbound_effects,channel_message_mappings,
    channel_route_link_requests,channel_routes,
    channel_ingress_receipts IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM channel_outbound_effects) OR
       EXISTS (SELECT 1 FROM channel_message_mappings) OR
       EXISTS (SELECT 1 FROM channel_route_link_requests) OR
       EXISTS (SELECT 1 FROM channel_routes WHERE route_kind<>'private') OR
       EXISTS (SELECT 1 FROM channel_ingress_receipts
                WHERE provider_thread_id<>'0' OR provider_message_id<>'' OR
                      ingress_kind<>'message' OR callback_query_id<>'') THEN
        RAISE EXCEPTION '134: refusing downgrade while routed channel history exists';
    END IF;
END $$;
-- +goose StatementEnd

ALTER TABLE channel_ingress_receipts
    DROP CONSTRAINT fk_channel_ingress_route_scope,
    DROP CONSTRAINT ck_channel_ingress_thread,
    DROP CONSTRAINT ck_channel_ingress_message,
    DROP CONSTRAINT ck_channel_ingress_kind,
    DROP CONSTRAINT ck_channel_ingress_callback,
    DROP COLUMN route_id,
    DROP COLUMN provider_thread_id,
    DROP COLUMN provider_message_id,
    DROP COLUMN ingress_kind,
    DROP COLUMN callback_query_id;
DROP TABLE channel_message_mappings;
DROP TABLE channel_outbound_effects;
DROP TABLE channel_route_link_requests;
DROP TABLE channel_routes;
