-- 137: account email security lifecycle.
--
-- Verification, password-reset and recent-authentication credentials are
-- high-entropy bearer tokens. Only SHA-256 digests are persisted. Every row is
-- bound to an exact workspace+user pair and protected by FORCE RLS; public
-- verification/reset bootstrap is limited to the presented token digest.

-- +goose Up

CREATE TABLE account_security_tokens (
    id                 BIGSERIAL   PRIMARY KEY,
    tenant_id          BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    user_id            BIGINT      NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    token_hash         BYTEA       NOT NULL UNIQUE,
    token_kind         TEXT        NOT NULL,
    session_token_hash BYTEA,
    expires_at         TIMESTAMPTZ NOT NULL,
    consumed_at        TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_account_security_token_hash CHECK (octet_length(token_hash)=32),
    CONSTRAINT ck_account_security_session_hash CHECK (
        session_token_hash IS NULL OR octet_length(session_token_hash)=32
    ),
    CONSTRAINT ck_account_security_token_kind CHECK (
        token_kind IN ('email_verification','password_reset','reauth')
    ),
    CONSTRAINT ck_account_security_token_session_shape CHECK (
        (token_kind='reauth' AND session_token_hash IS NOT NULL) OR
        (token_kind IN ('email_verification','password_reset') AND session_token_hash IS NULL)
    ),
    CONSTRAINT ck_account_security_token_expiry CHECK (expires_at > created_at)
);

CREATE INDEX idx_account_security_tokens_scope
    ON account_security_tokens (tenant_id,user_id,token_kind,created_at DESC);
CREATE INDEX idx_account_security_tokens_expiry
    ON account_security_tokens (expires_at)
    WHERE consumed_at IS NULL;
CREATE UNIQUE INDEX uq_account_security_active_reauth
    ON account_security_tokens (tenant_id,user_id,session_token_hash)
    WHERE token_kind='reauth' AND consumed_at IS NULL;

CREATE TABLE account_security_audit_events (
    id          BIGSERIAL   PRIMARY KEY,
    tenant_id   BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    user_id     BIGINT      NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    token_id    BIGINT      REFERENCES account_security_tokens (id) ON DELETE SET NULL,
    event_type  TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_account_security_event_type CHECK (
        event_type IN (
            'email_verification_issued','email_verified',
            'password_reset_issued','password_reset_completed',
            'reauth_issued','logout_all_completed'
        )
    )
);
CREATE INDEX idx_account_security_audit_scope
    ON account_security_audit_events (tenant_id,user_id,created_at DESC,id DESC);

ALTER TABLE account_security_tokens ENABLE ROW LEVEL SECURITY;
ALTER TABLE account_security_tokens FORCE ROW LEVEL SECURITY;
ALTER TABLE account_security_audit_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE account_security_audit_events FORCE ROW LEVEL SECURITY;

CREATE POLICY account_security_tokens_exact_scope ON account_security_tokens
    FOR ALL TO PUBLIC
    USING (
        (tenant_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.tenant_id', true), '')::bigint
         AND user_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.user_id', true), '')::bigint)
        OR encode(token_hash,'hex') IS NOT DISTINCT FROM
            NULLIF(current_setting('app.account_security_token_hash', true), '')
    )
    WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.tenant_id', true), '')::bigint
        AND user_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.user_id', true), '')::bigint
    );

CREATE POLICY account_security_audit_exact_scope ON account_security_audit_events
    FOR ALL TO PUBLIC
    USING (
        tenant_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.tenant_id', true), '')::bigint
        AND user_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.user_id', true), '')::bigint
    )
    WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.tenant_id', true), '')::bigint
        AND user_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.user_id', true), '')::bigint
    );

GRANT SELECT,INSERT,UPDATE,DELETE ON account_security_tokens TO vane_app;
GRANT SELECT,INSERT ON account_security_audit_events TO vane_app;
GRANT USAGE,SELECT ON account_security_tokens_id_seq,account_security_audit_events_id_seq TO vane_app;

-- +goose Down

DROP TABLE account_security_audit_events;
DROP TABLE account_security_tokens;
