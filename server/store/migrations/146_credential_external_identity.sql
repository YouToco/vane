-- 146: user-owned channel scope and exact external application identity.
--
-- A Telegram bot id or Feishu app id may be active for only one Vane user.
-- This is a database boundary, not a manager-fleet convention: concurrent
-- users cannot configure the same public bot and race webhook authority.

-- +goose Up

ALTER TABLE credential_vault_entries
    ADD COLUMN user_id BIGINT,
    ADD COLUMN external_identity TEXT,
    ADD CONSTRAINT fk_credential_vault_membership
        FOREIGN KEY (tenant_id,user_id)
        REFERENCES memberships (tenant_id,user_id) ON DELETE CASCADE;

ALTER TABLE credential_vault_entries DROP CONSTRAINT ck_credential_vault_scope;
ALTER TABLE credential_vault_entries
    ADD CONSTRAINT ck_credential_vault_scope CHECK (
        (scope_kind='platform' AND tenant_id IS NULL AND user_id IS NULL) OR
        (scope_kind='tenant' AND tenant_id IS NOT NULL AND user_id IS NULL) OR
        (scope_kind='user' AND tenant_id IS NOT NULL AND user_id IS NOT NULL)
    );

UPDATE credential_vault_entries
SET external_identity = CASE
    WHEN scope_kind='user' AND provider='telegram' AND purpose='bot_api'
        THEN metadata->>'bot_id'
    WHEN scope_kind='user' AND provider='feishu' AND purpose='app_credentials'
        THEN metadata->>'app_id'
    ELSE NULL
END;

ALTER TABLE credential_vault_entries
    ADD CONSTRAINT ck_credential_vault_external_identity CHECK (
        (scope_kind='platform' AND external_identity IS NULL) OR
        (scope_kind='user' AND provider='telegram' AND purpose='bot_api' AND
            external_identity ~ '^[1-9][0-9]{0,19}$') OR
        (scope_kind='user' AND provider='feishu' AND purpose='app_credentials' AND
            external_identity ~ '^[A-Za-z0-9._-]{1,128}$') OR
        (scope_kind='tenant' AND external_identity IS NULL) OR
        (scope_kind='user' AND NOT (
            (provider='telegram' AND purpose='bot_api') OR
            (provider='feishu' AND purpose='app_credentials')
        ) AND external_identity IS NULL)
    );

CREATE UNIQUE INDEX uq_credential_vault_user_generation
    ON credential_vault_entries (tenant_id,user_id,provider,purpose,generation)
    WHERE scope_kind='user';
CREATE UNIQUE INDEX uq_credential_vault_user_active
    ON credential_vault_entries (tenant_id,user_id,provider,purpose)
    WHERE scope_kind='user' AND status='active';

CREATE UNIQUE INDEX uq_credential_vault_active_external_identity
    ON credential_vault_entries (provider,purpose,external_identity)
    WHERE scope_kind='user' AND status='active' AND external_identity IS NOT NULL;

CREATE POLICY credential_vault_exact_user ON credential_vault_entries
    USING (
        scope_kind='user' AND
        tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
        user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    ) WITH CHECK (
        scope_kind='user' AND
        tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
        user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );

-- +goose Down

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM credential_vault_entries WHERE scope_kind='user') THEN
        RAISE EXCEPTION
            'migration 146 down refused: user credential history exists';
    END IF;
END
$$;
-- +goose StatementEnd

DROP POLICY credential_vault_exact_user ON credential_vault_entries;
DROP INDEX uq_credential_vault_active_external_identity;
DROP INDEX uq_credential_vault_user_active;
DROP INDEX uq_credential_vault_user_generation;
ALTER TABLE credential_vault_entries
    DROP CONSTRAINT ck_credential_vault_external_identity;
ALTER TABLE credential_vault_entries DROP CONSTRAINT ck_credential_vault_scope;
ALTER TABLE credential_vault_entries
    ADD CONSTRAINT ck_credential_vault_scope CHECK (
        (scope_kind='platform' AND tenant_id IS NULL) OR
        (scope_kind='tenant' AND tenant_id IS NOT NULL)
    );
ALTER TABLE credential_vault_entries
    DROP CONSTRAINT fk_credential_vault_membership,
    DROP COLUMN external_identity,
    DROP COLUMN user_id;
