-- 135: tenant/user-isolated declarative Skill and remote MCP capability store.
--
-- This migration stores immutable, content-addressed non-secret versions. It
-- grants no execution authority: activation and task binding remain in the
-- trusted Vane harness. MCP server binaries and Skill scripts are never stored
-- as executable content.

-- +goose Up

CREATE TABLE user_capabilities (
    id                 UUID        PRIMARY KEY,
    tenant_id          BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    owner_user_id      BIGINT      NOT NULL REFERENCES users (id),
    kind               TEXT        NOT NULL,
    visibility         TEXT        NOT NULL,
    slug               TEXT        NOT NULL,
    display_name       TEXT        NOT NULL,
    status             TEXT        NOT NULL DEFAULT 'draft',
    current_version_id UUID,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_user_capability_scope UNIQUE (tenant_id,owner_user_id,id),
    CONSTRAINT uq_user_capability_visibility_scope UNIQUE (tenant_id,owner_user_id,id,visibility),
    CONSTRAINT ck_user_capability_kind CHECK (kind IN ('skill','mcp')),
    CONSTRAINT ck_user_capability_visibility CHECK (visibility IN ('personal','workspace')),
    CONSTRAINT ck_user_capability_slug CHECK (slug ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),
    CONSTRAINT ck_user_capability_display CHECK (octet_length(display_name) BETWEEN 1 AND 256),
    CONSTRAINT ck_user_capability_status CHECK (status IN ('draft','active','paused','incompatible'))
);
CREATE UNIQUE INDEX uq_user_capability_personal_slug
    ON user_capabilities (tenant_id,owner_user_id,kind,slug)
    WHERE visibility='personal';
CREATE UNIQUE INDEX uq_user_capability_workspace_slug
    ON user_capabilities (tenant_id,kind,slug)
    WHERE visibility='workspace';

CREATE TABLE user_capability_versions (
    id               UUID        PRIMARY KEY,
    capability_id    UUID        NOT NULL,
    tenant_id        BIGINT      NOT NULL,
    owner_user_id    BIGINT      NOT NULL,
    version          INT         NOT NULL,
    visibility       TEXT        NOT NULL,
    source_kind      TEXT        NOT NULL,
    source_ref       TEXT        NOT NULL DEFAULT '',
    payload_digest   TEXT        NOT NULL,
    manifest_payload BYTEA       NOT NULL,
    compatible       BOOLEAN     NOT NULL,
    created_by       BIGINT      NOT NULL REFERENCES users (id),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_user_capability_version_number UNIQUE (capability_id,version),
    CONSTRAINT uq_user_capability_version_scope UNIQUE (tenant_id,owner_user_id,id),
    CONSTRAINT uq_user_capability_version_parent_scope
        UNIQUE (tenant_id,owner_user_id,capability_id,visibility,id),
    CONSTRAINT fk_user_capability_version_parent
        FOREIGN KEY (tenant_id,owner_user_id,capability_id,visibility)
        REFERENCES user_capabilities (tenant_id,owner_user_id,id,visibility) ON DELETE CASCADE,
    CONSTRAINT ck_user_capability_version_positive CHECK (version > 0),
    CONSTRAINT ck_user_capability_version_visibility CHECK (visibility IN ('personal','workspace')),
    CONSTRAINT ck_user_capability_version_source CHECK (source_kind IN ('upload','public_catalog','remote_mcp')),
    CONSTRAINT ck_user_capability_version_source_ref CHECK (octet_length(source_ref) <= 2048),
    CONSTRAINT ck_user_capability_version_payload CHECK (
        octet_length(manifest_payload) BETWEEN 2 AND 2097152 AND
        payload_digest ~ '^[0-9a-f]{64}$' AND
        payload_digest=encode(sha256(manifest_payload),'hex')
    )
);

ALTER TABLE user_capabilities
    ADD CONSTRAINT fk_user_capability_current_version
    FOREIGN KEY (tenant_id,owner_user_id,id,visibility,current_version_id)
    REFERENCES user_capability_versions (tenant_id,owner_user_id,capability_id,visibility,id)
    ON DELETE SET NULL (current_version_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE skill_capability_versions (
    capability_version_id UUID        PRIMARY KEY,
    capability_id         UUID        NOT NULL,
    tenant_id             BIGINT      NOT NULL,
    owner_user_id         BIGINT      NOT NULL,
    visibility            TEXT        NOT NULL,
    name                  TEXT        NOT NULL,
    description           TEXT        NOT NULL DEFAULT '',
    skill_md_digest       TEXT        NOT NULL,
    archive_digest        TEXT        NOT NULL,
    file_manifest_payload BYTEA       NOT NULL,
    file_manifest_digest  TEXT        NOT NULL,
    contains_scripts      BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_skill_capability_version_scope
        UNIQUE (tenant_id,owner_user_id,capability_id,visibility,capability_version_id),
    CONSTRAINT fk_skill_capability_version_parent
        FOREIGN KEY (tenant_id,owner_user_id,capability_id,visibility,capability_version_id)
        REFERENCES user_capability_versions (tenant_id,owner_user_id,capability_id,visibility,id)
        ON DELETE CASCADE,
    CONSTRAINT ck_skill_capability_visibility CHECK (visibility IN ('personal','workspace')),
    CONSTRAINT ck_skill_capability_name CHECK (name ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),
    CONSTRAINT ck_skill_capability_description CHECK (octet_length(description) <= 4096),
    CONSTRAINT ck_skill_capability_digests CHECK (
        skill_md_digest ~ '^[0-9a-f]{64}$' AND archive_digest ~ '^[0-9a-f]{64}$' AND
        file_manifest_digest ~ '^[0-9a-f]{64}$' AND
        file_manifest_digest=encode(sha256(file_manifest_payload),'hex')
    ),
    CONSTRAINT ck_skill_capability_manifest_size
        CHECK (octet_length(file_manifest_payload) BETWEEN 2 AND 2097152)
);

-- Only parser-approved declarative files are retained. scripts/ entries stay
-- in the manifest as an incompatibility reason but their bytes cannot enter
-- this table.
CREATE TABLE skill_capability_files (
    capability_version_id UUID   NOT NULL,
    capability_id         UUID   NOT NULL,
    tenant_id             BIGINT NOT NULL,
    owner_user_id         BIGINT NOT NULL,
    visibility            TEXT   NOT NULL,
    file_path             TEXT   NOT NULL,
    file_kind             TEXT   NOT NULL,
    content_digest        TEXT   NOT NULL,
    content_payload       BYTEA  NOT NULL,
    PRIMARY KEY (capability_version_id,file_path),
    CONSTRAINT fk_skill_capability_file_parent
        FOREIGN KEY (tenant_id,owner_user_id,capability_id,visibility,capability_version_id)
        REFERENCES skill_capability_versions (
            tenant_id,owner_user_id,capability_id,visibility,capability_version_id)
        ON DELETE CASCADE,
    CONSTRAINT ck_skill_capability_file_visibility CHECK (visibility IN ('personal','workspace')),
    CONSTRAINT ck_skill_capability_file_kind CHECK (file_kind IN ('skill_md','reference','asset')),
    CONSTRAINT ck_skill_capability_file_path CHECK (
        octet_length(file_path) BETWEEN 1 AND 1024 AND
        file_path !~ '(^|/)\.\.?(/|$)' AND position(E'\\' IN file_path)=0
    ),
    CONSTRAINT ck_skill_capability_file_payload CHECK (
        octet_length(content_payload) <= 4194304 AND
        content_digest ~ '^[0-9a-f]{64}$' AND
        content_digest=encode(sha256(content_payload),'hex')
    )
);

CREATE TABLE mcp_connection_versions (
    capability_version_id UUID        PRIMARY KEY,
    capability_id         UUID        NOT NULL,
    tenant_id             BIGINT      NOT NULL,
    owner_user_id         BIGINT      NOT NULL,
    visibility            TEXT        NOT NULL,
    endpoint_url          TEXT        NOT NULL,
    protocol_version      TEXT        NOT NULL,
    authentication_kind  TEXT        NOT NULL,
    credential_ref       TEXT        NOT NULL DEFAULT '',
    tool_schema_payload  BYTEA       NOT NULL,
    tool_schema_digest   TEXT        NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT fk_mcp_connection_version_parent
        FOREIGN KEY (tenant_id,owner_user_id,capability_id,visibility,capability_version_id)
        REFERENCES user_capability_versions (tenant_id,owner_user_id,capability_id,visibility,id)
        ON DELETE CASCADE,
    CONSTRAINT ck_mcp_connection_visibility CHECK (visibility IN ('personal','workspace')),
    CONSTRAINT ck_mcp_connection_endpoint CHECK (
        endpoint_url LIKE 'https://%' AND endpoint_url !~ '[?#[:space:]]' AND
        octet_length(endpoint_url) <= 2048
    ),
    CONSTRAINT ck_mcp_connection_protocol CHECK (protocol_version IN ('2025-06-18','2025-11-25')),
    CONSTRAINT ck_mcp_connection_auth CHECK (
        authentication_kind IN ('none','api_key','bearer','oauth2') AND
        ((authentication_kind='none' AND credential_ref='') OR
         (authentication_kind<>'none' AND
          credential_ref ~ '^vault:[A-Za-z0-9][A-Za-z0-9._-]{0,239}$'))
    ),
    CONSTRAINT ck_mcp_connection_schema CHECK (
        octet_length(tool_schema_payload) BETWEEN 2 AND 2097152 AND
        tool_schema_digest ~ '^[0-9a-f]{64}$' AND
        tool_schema_digest=encode(sha256(tool_schema_payload),'hex')
    )
);

CREATE TABLE user_capability_events (
    id            BIGSERIAL   PRIMARY KEY,
    tenant_id     BIGINT      NOT NULL,
    capability_id UUID        NOT NULL,
    owner_user_id BIGINT      NOT NULL,
    visibility    TEXT        NOT NULL,
    actor_user_id BIGINT      NOT NULL REFERENCES users (id),
    event_kind    TEXT        NOT NULL,
    version_id    UUID,
    details       JSONB       NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT fk_user_capability_event_parent
        FOREIGN KEY (tenant_id,owner_user_id,capability_id,visibility)
        REFERENCES user_capabilities (tenant_id,owner_user_id,id,visibility) ON DELETE CASCADE,
    CONSTRAINT fk_user_capability_event_version
        FOREIGN KEY (tenant_id,owner_user_id,capability_id,visibility,version_id)
        REFERENCES user_capability_versions (tenant_id,owner_user_id,capability_id,visibility,id),
    CONSTRAINT ck_user_capability_event_visibility CHECK (visibility IN ('personal','workspace')),
    CONSTRAINT ck_user_capability_event_kind CHECK (
        event_kind IN ('installed','version_added','activated','paused','schema_drifted','upgrade_rejected')
    ),
    CONSTRAINT ck_user_capability_event_details CHECK (
        jsonb_typeof(details)='object' AND octet_length(details::text) <= 16384
    )
);
CREATE INDEX idx_user_capability_events_scope
    ON user_capability_events (tenant_id,capability_id,created_at,id);

GRANT SELECT,INSERT,UPDATE ON user_capabilities TO vane_app;
GRANT SELECT,INSERT ON user_capability_versions,skill_capability_versions,
    skill_capability_files,mcp_connection_versions,user_capability_events TO vane_app;
GRANT USAGE,SELECT ON SEQUENCE user_capability_events_id_seq TO vane_app;

ALTER TABLE user_capabilities ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_capabilities FORCE ROW LEVEL SECURITY;
ALTER TABLE user_capability_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_capability_versions FORCE ROW LEVEL SECURITY;
ALTER TABLE skill_capability_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE skill_capability_versions FORCE ROW LEVEL SECURITY;
ALTER TABLE skill_capability_files ENABLE ROW LEVEL SECURITY;
ALTER TABLE skill_capability_files FORCE ROW LEVEL SECURITY;
ALTER TABLE mcp_connection_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE mcp_connection_versions FORCE ROW LEVEL SECURITY;
ALTER TABLE user_capability_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_capability_events FORCE ROW LEVEL SECURITY;

-- +goose StatementBegin
DO $$
DECLARE table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'user_capabilities','user_capability_versions','skill_capability_versions',
        'skill_capability_files','mcp_connection_versions','user_capability_events'
    ] LOOP
        EXECUTE format(
            'CREATE POLICY capability_visible ON %I FOR SELECT USING (' ||
            'tenant_id=NULLIF((SELECT current_setting(''app.tenant_id'',true)),'''')::bigint AND ' ||
            '(visibility=''workspace'' OR owner_user_id=NULLIF((SELECT current_setting(''app.user_id'',true)),'''')::bigint))',
            table_name);
        EXECUTE format(
            'CREATE POLICY capability_insert ON %I FOR INSERT WITH CHECK (' ||
            'tenant_id=NULLIF((SELECT current_setting(''app.tenant_id'',true)),'''')::bigint AND ' ||
            '((visibility=''personal'' AND owner_user_id=NULLIF((SELECT current_setting(''app.user_id'',true)),'''')::bigint) OR ' ||
            '(visibility=''workspace'' AND NULLIF((SELECT current_setting(''app.membership_role'',true)),'''') IN (''owner'',''admin''))))',
            table_name);
    END LOOP;
END $$;
-- +goose StatementEnd

CREATE POLICY capability_head_update ON user_capabilities FOR UPDATE
    USING (
        tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
        ((visibility='personal' AND
          owner_user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint) OR
         (visibility='workspace' AND
          NULLIF((SELECT current_setting('app.membership_role',true)),'') IN ('owner','admin')))
    )
    WITH CHECK (
        tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
        ((visibility='personal' AND
          owner_user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint) OR
         (visibility='workspace' AND
          NULLIF((SELECT current_setting('app.membership_role',true)),'') IN ('owner','admin')))
    );

-- +goose Down

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM user_capabilities) THEN
        RAISE EXCEPTION '135: refusing downgrade while retained user capabilities exist';
    END IF;
END $$;
-- +goose StatementEnd

DROP TABLE user_capability_events;
DROP TABLE mcp_connection_versions;
DROP TABLE skill_capability_files;
DROP TABLE skill_capability_versions;
ALTER TABLE user_capabilities DROP CONSTRAINT fk_user_capability_current_version;
DROP TABLE user_capability_versions;
DROP TABLE user_capabilities;
