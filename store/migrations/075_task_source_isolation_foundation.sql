-- 075: task-owned Source runtime foundation.
--
-- This is deliberately a zero-call-point migration. Current writers remain on
-- the retained v1 tables until a later atomic writer/runtime cutover. No row is
-- backfilled here: a global fetch_target may be shared by many tasks, so
-- assigning ownership from that row would invent provenance.

-- +goose Up

-- Run Sources need an exact scope-bearing FK. task_run_snapshots.id is already
-- globally unique; the wider identity proves tenant/user/task ownership at the
-- database boundary.
ALTER TABLE task_run_snapshots
    ADD CONSTRAINT uq_task_run_snapshots_source_scope
    UNIQUE (tenant_id, user_id, task_id, id);

CREATE TABLE task_sources (
    id                  BIGSERIAL   PRIMARY KEY,
    tenant_id           BIGINT      NOT NULL,
    user_id             BIGINT      NOT NULL,
    task_id             TEXT        NOT NULL,
    revision            BIGINT      NOT NULL,
    schema_version      TEXT        NOT NULL,
    tool_name           TEXT        NOT NULL,
    tool_version        TEXT        NOT NULL,
    tool_arguments      JSONB       NOT NULL,
    platform            TEXT        NOT NULL,
    capability          TEXT        NOT NULL,
    title               TEXT        NOT NULL DEFAULT '',
    endpoint_url        TEXT        NOT NULL,
    runtime_config      JSONB       NOT NULL,
    identity_digest     TEXT        NOT NULL,
    supersedes_source_id BIGINT,
    retired_at          TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_task_sources_scope_id
        UNIQUE (tenant_id, user_id, task_id, id),
    CONSTRAINT uq_task_sources_revision
        UNIQUE (tenant_id, user_id, task_id, identity_digest, revision),
    CONSTRAINT fk_task_sources_schedule_scope
        FOREIGN KEY (tenant_id, user_id, task_id)
        REFERENCES schedules (tenant_id, user_id, id) ON DELETE CASCADE,
    CONSTRAINT fk_task_sources_supersedes_scope
        FOREIGN KEY (tenant_id, user_id, task_id, supersedes_source_id)
        REFERENCES task_sources (tenant_id, user_id, task_id, id),
    CONSTRAINT ck_task_sources_revision_positive CHECK (revision > 0),
    CONSTRAINT ck_task_sources_identity_digest
        CHECK (identity_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_task_sources_schema_version CHECK (
        schema_version <> '' AND btrim(schema_version) = schema_version AND
        octet_length(schema_version) <= 128
    ),
    CONSTRAINT ck_task_sources_tool_identity CHECK (
        tool_name <> '' AND btrim(tool_name) = tool_name AND
        octet_length(tool_name) <= 128 AND
        tool_version <> '' AND btrim(tool_version) = tool_version AND
        octet_length(tool_version) <= 128
    ),
    CONSTRAINT ck_task_sources_route CHECK (
        platform <> '' AND btrim(platform) = platform AND
        octet_length(platform) <= 128 AND
        capability <> '' AND btrim(capability) = capability AND
        octet_length(capability) <= 128 AND
        endpoint_url <> '' AND btrim(endpoint_url) = endpoint_url AND
        octet_length(endpoint_url) <= 4096 AND
        octet_length(title) <= 4096
    ),
    CONSTRAINT ck_task_sources_json CHECK (
        jsonb_typeof(tool_arguments) = 'object' AND
        jsonb_typeof(runtime_config) = 'object'
    )
);

CREATE INDEX idx_task_sources_scope_active
    ON task_sources (tenant_id, user_id, task_id, created_at DESC, id DESC)
    WHERE retired_at IS NULL;

CREATE TABLE task_source_states (
    tenant_id       BIGINT      NOT NULL,
    user_id         BIGINT      NOT NULL,
    task_id         TEXT        NOT NULL,
    source_id       BIGINT      NOT NULL,
    state_version   BIGINT      NOT NULL DEFAULT 1,
    status          TEXT        NOT NULL DEFAULT 'active',
    cursor          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    next_fetch_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_fetched_at TIMESTAMPTZ,
    fail_count      INT         NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT pk_task_source_states
        PRIMARY KEY (tenant_id, user_id, task_id, source_id),
    CONSTRAINT fk_task_source_states_source_scope
        FOREIGN KEY (tenant_id, user_id, task_id, source_id)
        REFERENCES task_sources (tenant_id, user_id, task_id, id)
        ON DELETE CASCADE,
    CONSTRAINT ck_task_source_states_version CHECK (state_version > 0),
    CONSTRAINT ck_task_source_states_status
        CHECK (status IN ('active', 'disabled')),
    CONSTRAINT ck_task_source_states_cursor
        CHECK (jsonb_typeof(cursor) = 'object'),
    CONSTRAINT ck_task_source_states_fail_count CHECK (fail_count >= 0)
);

CREATE INDEX idx_task_source_states_due
    ON task_source_states (
        tenant_id, user_id, task_id, status, next_fetch_at, source_id
    );

CREATE TABLE task_run_sources (
    id                  BIGSERIAL   PRIMARY KEY,
    tenant_id           BIGINT      NOT NULL,
    user_id             BIGINT      NOT NULL,
    task_id             TEXT        NOT NULL,
    run_snapshot_id     BIGINT      NOT NULL,
    ordinal             INT         NOT NULL,
    task_source_id      BIGINT,
    source_revision     BIGINT      NOT NULL,
    schema_version      TEXT        NOT NULL,
    tool_name           TEXT        NOT NULL,
    tool_version        TEXT        NOT NULL,
    tool_arguments      JSONB       NOT NULL,
    credential_ref      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    platform            TEXT        NOT NULL,
    capability          TEXT        NOT NULL,
    title               TEXT        NOT NULL DEFAULT '',
    endpoint_url        TEXT        NOT NULL,
    runtime_config      JSONB       NOT NULL,
    identity_digest     TEXT        NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_task_run_sources_scope_id
        UNIQUE (tenant_id, user_id, task_id, id),
    CONSTRAINT uq_task_run_sources_ordinal
        UNIQUE (tenant_id, user_id, task_id, run_snapshot_id, ordinal),
    CONSTRAINT uq_task_run_sources_identity
        UNIQUE (
            tenant_id, user_id, task_id, run_snapshot_id, identity_digest
        ),
    CONSTRAINT fk_task_run_sources_snapshot_scope
        FOREIGN KEY (tenant_id, user_id, task_id, run_snapshot_id)
        REFERENCES task_run_snapshots (tenant_id, user_id, task_id, id)
        ON DELETE CASCADE,
    CONSTRAINT fk_task_run_sources_task_source_scope
        FOREIGN KEY (tenant_id, user_id, task_id, task_source_id)
        REFERENCES task_sources (tenant_id, user_id, task_id, id)
        ON DELETE SET NULL (task_source_id),
    CONSTRAINT ck_task_run_sources_ordinal CHECK (ordinal >= 0),
    CONSTRAINT ck_task_run_sources_revision CHECK (source_revision > 0),
    CONSTRAINT ck_task_run_sources_identity_digest
        CHECK (identity_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_task_run_sources_strings CHECK (
        schema_version <> '' AND btrim(schema_version) = schema_version AND
        octet_length(schema_version) <= 128 AND
        tool_name <> '' AND btrim(tool_name) = tool_name AND
        octet_length(tool_name) <= 128 AND
        tool_version <> '' AND btrim(tool_version) = tool_version AND
        octet_length(tool_version) <= 128 AND
        platform <> '' AND btrim(platform) = platform AND
        octet_length(platform) <= 128 AND
        capability <> '' AND btrim(capability) = capability AND
        octet_length(capability) <= 128 AND
        endpoint_url <> '' AND btrim(endpoint_url) = endpoint_url AND
        octet_length(endpoint_url) <= 4096 AND
        octet_length(title) <= 4096
    ),
    CONSTRAINT ck_task_run_sources_json CHECK (
        jsonb_typeof(tool_arguments) = 'object' AND
        jsonb_typeof(credential_ref) = 'object' AND
        jsonb_typeof(runtime_config) = 'object'
    )
);

CREATE INDEX idx_task_run_sources_snapshot
    ON task_run_sources (
        tenant_id, user_id, task_id, run_snapshot_id, ordinal
    );

CREATE TABLE task_content_records (
    id            BIGSERIAL   PRIMARY KEY,
    tenant_id     BIGINT      NOT NULL,
    user_id       BIGINT      NOT NULL,
    canonical_key TEXT        NOT NULL,
    kind          TEXT        NOT NULL,
    url           TEXT        NOT NULL DEFAULT '',
    title         TEXT        NOT NULL DEFAULT '',
    content       TEXT        NOT NULL DEFAULT '',
    author        TEXT        NOT NULL DEFAULT '',
    published_at  TIMESTAMPTZ,
    content_hash  TEXT        NOT NULL,
    simhash       BIGINT,
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_task_content_records_scope_id
        UNIQUE (tenant_id, user_id, id),
    CONSTRAINT uq_task_content_records_canonical
        UNIQUE (tenant_id, user_id, canonical_key),
    CONSTRAINT fk_task_content_records_membership
        FOREIGN KEY (tenant_id, user_id)
        REFERENCES memberships (tenant_id, user_id) ON DELETE CASCADE,
    CONSTRAINT ck_task_content_records_identity CHECK (
        canonical_key <> '' AND btrim(canonical_key) = canonical_key AND
        octet_length(canonical_key) <= 8192 AND
        kind <> '' AND btrim(kind) = kind AND
        octet_length(kind) <= 128 AND
        content_hash <> '' AND btrim(content_hash) = content_hash AND
        octet_length(content_hash) <= 256
    )
);

CREATE INDEX idx_task_content_records_recent
    ON task_content_records (
        tenant_id, user_id, fetched_at DESC, id DESC
    );

CREATE TABLE task_content_appearances (
    id             BIGSERIAL   PRIMARY KEY,
    tenant_id      BIGINT      NOT NULL,
    user_id        BIGINT      NOT NULL,
    task_id        TEXT        NOT NULL,
    run_source_id  BIGINT      NOT NULL,
    content_id     BIGINT      NOT NULL,
    external_id    TEXT        NOT NULL,
    observed_url   TEXT        NOT NULL DEFAULT '',
    observed_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_task_content_appearances_scope_id
        UNIQUE (tenant_id, user_id, task_id, id),
    CONSTRAINT uq_task_content_appearance
        UNIQUE (
            tenant_id, user_id, task_id, run_source_id, content_id,
            external_id
        ),
    CONSTRAINT fk_task_content_appearance_run_source_scope
        FOREIGN KEY (tenant_id, user_id, task_id, run_source_id)
        REFERENCES task_run_sources (tenant_id, user_id, task_id, id)
        ON DELETE CASCADE,
    CONSTRAINT fk_task_content_appearance_content_scope
        FOREIGN KEY (tenant_id, user_id, content_id)
        REFERENCES task_content_records (tenant_id, user_id, id)
        ON DELETE CASCADE,
    CONSTRAINT ck_task_content_appearance_external_id CHECK (
        external_id <> '' AND octet_length(external_id) <= 8192
    )
);

CREATE INDEX idx_task_content_appearances_content
    ON task_content_appearances (
        tenant_id, user_id, task_id, content_id, observed_at DESC
    );

CREATE TABLE task_content_evidence (
    id             BIGSERIAL   PRIMARY KEY,
    tenant_id      BIGINT      NOT NULL,
    user_id        BIGINT      NOT NULL,
    task_id        TEXT        NOT NULL,
    run_source_id  BIGINT      NOT NULL,
    content_id     BIGINT      NOT NULL,
    evidence_kind  TEXT        NOT NULL,
    evidence_digest TEXT       NOT NULL,
    payload        JSONB       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_task_content_evidence_scope_id
        UNIQUE (tenant_id, user_id, task_id, id),
    CONSTRAINT uq_task_content_evidence_claim
        UNIQUE (
            tenant_id, user_id, task_id, run_source_id, content_id,
            evidence_kind, evidence_digest
        ),
    CONSTRAINT fk_task_content_evidence_run_source_scope
        FOREIGN KEY (tenant_id, user_id, task_id, run_source_id)
        REFERENCES task_run_sources (tenant_id, user_id, task_id, id)
        ON DELETE CASCADE,
    CONSTRAINT fk_task_content_evidence_content_scope
        FOREIGN KEY (tenant_id, user_id, content_id)
        REFERENCES task_content_records (tenant_id, user_id, id)
        ON DELETE CASCADE,
    CONSTRAINT ck_task_content_evidence_kind CHECK (
        evidence_kind <> '' AND btrim(evidence_kind) = evidence_kind AND
        octet_length(evidence_kind) <= 128
    ),
    CONSTRAINT ck_task_content_evidence_digest
        CHECK (evidence_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_task_content_evidence_payload
        CHECK (jsonb_typeof(payload) = 'object')
);

CREATE INDEX idx_task_content_evidence_content
    ON task_content_evidence (
        tenant_id, user_id, task_id, content_id, created_at DESC
    );

-- The application may retire a Source but may not rewrite its immutable
-- identity/configuration. Run Sources, appearances and evidence are append-only.
GRANT SELECT, INSERT ON task_sources TO vane_app;
GRANT UPDATE (retired_at) ON task_sources TO vane_app;
GRANT SELECT, INSERT, UPDATE ON task_source_states TO vane_app;
GRANT SELECT, INSERT ON task_run_sources TO vane_app;
GRANT SELECT, INSERT ON task_content_records TO vane_app;
GRANT UPDATE (content, content_hash, simhash) ON task_content_records TO vane_app;
GRANT SELECT, INSERT ON task_content_appearances TO vane_app;
GRANT SELECT, INSERT ON task_content_evidence TO vane_app;
GRANT USAGE, SELECT ON SEQUENCE
    task_sources_id_seq,
    task_run_sources_id_seq,
    task_content_records_id_seq,
    task_content_appearances_id_seq,
    task_content_evidence_id_seq
TO vane_app;

-- New-generation private data is visible only when both tenant and user
-- contexts match. Task ownership is additionally enforced by composite FKs.
-- +goose StatementBegin
DO $$
DECLARE t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'task_sources',
        'task_source_states',
        'task_run_sources',
        'task_content_records',
        'task_content_appearances',
        'task_content_evidence'
    ] LOOP
        EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
        EXECUTE format(
            'CREATE POLICY owner_visible ON %I FOR ALL USING (true) WITH CHECK (true)', t);
        EXECUTE format(
            'CREATE POLICY owner_isolation ON %I AS RESTRICTIVE FOR ALL '
            'USING ('
            'tenant_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.tenant_id'',true)),'''')::bigint '
            'AND user_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.user_id'',true)),'''')::bigint'
            ') WITH CHECK ('
            'tenant_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.tenant_id'',true)),'''')::bigint '
            'AND user_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.user_id'',true)),'''')::bigint'
            ')', t);
    END LOOP;
END $$;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM task_sources) OR
       EXISTS (SELECT 1 FROM task_run_sources) OR
       EXISTS (SELECT 1 FROM task_content_records) THEN
        RAISE EXCEPTION
            '075: refusing downgrade while task Source/content evidence exists';
    END IF;
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
DO $$
DECLARE t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'task_content_evidence',
        'task_content_appearances',
        'task_content_records',
        'task_run_sources',
        'task_source_states',
        'task_sources'
    ] LOOP
        EXECUTE format('DROP POLICY IF EXISTS owner_isolation ON %I', t);
        EXECUTE format('DROP POLICY IF EXISTS owner_visible ON %I', t);
    END LOOP;
END $$;
-- +goose StatementEnd

REVOKE USAGE, SELECT ON SEQUENCE
    task_sources_id_seq,
    task_run_sources_id_seq,
    task_content_records_id_seq,
    task_content_appearances_id_seq,
    task_content_evidence_id_seq
FROM vane_app;
REVOKE ALL ON task_content_evidence FROM vane_app;
REVOKE ALL ON task_content_appearances FROM vane_app;
REVOKE ALL ON task_content_records FROM vane_app;
REVOKE ALL ON task_run_sources FROM vane_app;
REVOKE ALL ON task_source_states FROM vane_app;
REVOKE ALL ON task_sources FROM vane_app;

DROP TABLE task_content_evidence;
DROP TABLE task_content_appearances;
DROP TABLE task_content_records;
DROP TABLE task_run_sources;
DROP TABLE task_source_states;
DROP TABLE task_sources;

ALTER TABLE task_run_snapshots
    DROP CONSTRAINT uq_task_run_snapshots_source_scope;
