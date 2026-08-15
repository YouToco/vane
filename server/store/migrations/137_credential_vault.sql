-- 137: encrypted, versioned provider credential vault.
--
-- Bot credentials are tenant-owned; shared LLM credentials are platform-owned.
-- Secret bytes are encrypted by the application before INSERT. The database
-- retains only AES-GCM envelopes, a keyed fingerprint, lifecycle metadata and
-- actor audit fields. The deployment master key never crosses this boundary.

-- +goose Up

CREATE TABLE credential_vault_entries (
    id                 BIGSERIAL   PRIMARY KEY,
    scope_kind         TEXT        NOT NULL,
    tenant_id          BIGINT,
    provider           TEXT        NOT NULL,
    purpose            TEXT        NOT NULL,
    generation         BIGINT      NOT NULL,
    envelope_version   TEXT        NOT NULL,
    key_id             TEXT        NOT NULL,
    nonce              BYTEA       NOT NULL,
    ciphertext         BYTEA       NOT NULL,
    fingerprint        TEXT        NOT NULL,
    metadata           JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status             TEXT        NOT NULL DEFAULT 'active',
    created_by_user_id BIGINT      NOT NULL REFERENCES users(id),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    retired_at         TIMESTAMPTZ,
    revoked_at         TIMESTAMPTZ,
    CONSTRAINT fk_credential_vault_tenant
        FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT ck_credential_vault_scope CHECK (
        (scope_kind='platform' AND tenant_id IS NULL) OR
        (scope_kind='tenant' AND tenant_id IS NOT NULL)
    ),
    CONSTRAINT ck_credential_vault_provider
        CHECK (provider ~ '^[a-z][a-z0-9_-]{0,63}$'),
    CONSTRAINT ck_credential_vault_purpose
        CHECK (purpose ~ '^[a-z][a-z0-9_-]{0,63}$'),
    CONSTRAINT ck_credential_vault_generation CHECK (generation > 0),
    CONSTRAINT ck_credential_vault_envelope CHECK (
        envelope_version='v1' AND
        key_id ~ '^[a-z][a-z0-9_-]{0,63}$' AND
        octet_length(nonce)=12 AND
        octet_length(ciphertext) BETWEEN 17 AND 65552 AND
        fingerprint ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_credential_vault_metadata CHECK (
        jsonb_typeof(metadata)='object' AND octet_length(metadata::text) <= 8192
    ),
    CONSTRAINT ck_credential_vault_status
        CHECK (status IN ('active','retired','revoked')),
    CONSTRAINT ck_credential_vault_lifecycle CHECK (
        (status='active' AND retired_at IS NULL AND revoked_at IS NULL) OR
        (status='retired' AND retired_at IS NOT NULL AND revoked_at IS NULL) OR
        (status='revoked' AND revoked_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX uq_credential_vault_platform_generation
    ON credential_vault_entries (provider,purpose,generation)
    WHERE scope_kind='platform';
CREATE UNIQUE INDEX uq_credential_vault_tenant_generation
    ON credential_vault_entries (tenant_id,provider,purpose,generation)
    WHERE scope_kind='tenant';
CREATE UNIQUE INDEX uq_credential_vault_platform_active
    ON credential_vault_entries (provider,purpose)
    WHERE scope_kind='platform' AND status='active';
CREATE UNIQUE INDEX uq_credential_vault_tenant_active
    ON credential_vault_entries (tenant_id,provider,purpose)
    WHERE scope_kind='tenant' AND status='active';
CREATE INDEX idx_credential_vault_actor_audit
    ON credential_vault_entries (created_by_user_id,created_at DESC);

REVOKE ALL ON credential_vault_entries FROM PUBLIC,vane_app;
REVOKE ALL ON SEQUENCE credential_vault_entries_id_seq FROM PUBLIC,vane_app;

ALTER TABLE credential_vault_entries ENABLE ROW LEVEL SECURITY;
CREATE POLICY credential_vault_exact_tenant ON credential_vault_entries
    USING (
        scope_kind='tenant' AND
        tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
    ) WITH CHECK (
        scope_kind='tenant' AND
        tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
    );

-- +goose Down

LOCK TABLE credential_vault_entries IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM credential_vault_entries) THEN
        RAISE EXCEPTION
            'migration 137 down refused: encrypted credential history exists';
    END IF;
END
$$;
-- +goose StatementEnd

DROP TABLE credential_vault_entries;
