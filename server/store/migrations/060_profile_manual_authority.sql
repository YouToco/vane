-- 060: Web 画像人工修正 authority。
--
-- revision 只保存可人工修改的结构化字段；summary、token 与演化游标不进入审计。
-- receipt 独立保存首次 HTTP 结果，保证响应丢失后的同键重试可精确重放。

-- +goose Up

CREATE TABLE profile_edit_revisions (
    id                 BIGSERIAL   PRIMARY KEY,
    tenant_id          BIGINT      NOT NULL,
    user_id            BIGINT      NOT NULL,
    actor_user_id      BIGINT      NOT NULL,
    kind               TEXT        NOT NULL,
    target_revision_id BIGINT,
    before_fields      JSONB       NOT NULL,
    after_fields       JSONB       NOT NULL,
    result_updated_at  TIMESTAMPTZ NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_profile_edit_revision_kind
        CHECK (kind IN ('edit', 'undo')),
    CONSTRAINT ck_profile_edit_revision_actor_self
        CHECK (actor_user_id=user_id),
    CONSTRAINT ck_profile_edit_revision_before_shape CHECK (
        jsonb_typeof(before_fields)='object' AND
        before_fields ?& ARRAY[
            'exists','industry','occupation','tags','removed_tags'
        ] AND
        (before_fields - ARRAY[
            'exists','industry','occupation','tags','removed_tags'
        ])='{}'::jsonb AND
        jsonb_typeof(before_fields->'exists')='boolean' AND
        jsonb_typeof(before_fields->'industry')='string' AND
        jsonb_typeof(before_fields->'occupation')='string' AND
        jsonb_typeof(before_fields->'tags')='array' AND
        jsonb_typeof(before_fields->'removed_tags')='array' AND
        NOT jsonb_path_exists(
            before_fields,'$.tags[*] ? (@.type() != "string")') AND
        NOT jsonb_path_exists(
            before_fields,'$.removed_tags[*] ? (@.type() != "string")')
    ),
    CONSTRAINT ck_profile_edit_revision_after_shape CHECK (
        jsonb_typeof(after_fields)='object' AND
        after_fields ?& ARRAY[
            'exists','industry','occupation','tags','removed_tags'
        ] AND
        (after_fields - ARRAY[
            'exists','industry','occupation','tags','removed_tags'
        ])='{}'::jsonb AND
        jsonb_typeof(after_fields->'exists')='boolean' AND
        jsonb_typeof(after_fields->'industry')='string' AND
        jsonb_typeof(after_fields->'occupation')='string' AND
        jsonb_typeof(after_fields->'tags')='array' AND
        jsonb_typeof(after_fields->'removed_tags')='array' AND
        NOT jsonb_path_exists(
            after_fields,'$.tags[*] ? (@.type() != "string")') AND
        NOT jsonb_path_exists(
            after_fields,'$.removed_tags[*] ? (@.type() != "string")')
    ),
    CONSTRAINT ck_profile_edit_revision_target
        CHECK ((kind='edit' AND target_revision_id IS NULL) OR
               (kind='undo' AND target_revision_id IS NOT NULL)),
    CONSTRAINT uq_profile_edit_revision_scope
        UNIQUE (tenant_id, user_id, id),
    CONSTRAINT fk_profile_edit_revision_membership
        FOREIGN KEY (tenant_id, user_id)
        REFERENCES memberships (tenant_id, user_id) ON DELETE CASCADE,
    CONSTRAINT fk_profile_edit_revision_target_scope
        FOREIGN KEY (tenant_id, user_id, target_revision_id)
        REFERENCES profile_edit_revisions (tenant_id, user_id, id)
);

CREATE INDEX idx_profile_edit_revisions_tenant_user_latest
    ON profile_edit_revisions (tenant_id, user_id, id DESC);

CREATE TABLE profile_edit_receipts (
    tenant_id         BIGINT      NOT NULL,
    user_id           BIGINT      NOT NULL,
    idempotency_key   TEXT        NOT NULL,
    request_digest    TEXT        NOT NULL,
    revision_id       BIGINT      NOT NULL,
    response_profile  JSONB       NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, user_id, idempotency_key),
    CONSTRAINT ck_profile_edit_receipt_key
        CHECK (length(idempotency_key) BETWEEN 1 AND 128),
    CONSTRAINT ck_profile_edit_receipt_digest
        CHECK (request_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_profile_edit_receipt_response_shape CHECK (
        jsonb_typeof(response_profile)='object' AND
        response_profile ?& ARRAY[
            'industry','occupation','tags','removed_tags','summary',
            'created_at','updated_at'
        ] AND
        (response_profile - ARRAY[
            'industry','occupation','tags','removed_tags','summary',
            'created_at','updated_at'
        ])='{}'::jsonb AND
        jsonb_typeof(response_profile->'industry')='string' AND
        jsonb_typeof(response_profile->'occupation')='string' AND
        jsonb_typeof(response_profile->'tags')='array' AND
        jsonb_typeof(response_profile->'removed_tags')='array' AND
        NOT jsonb_path_exists(
            response_profile,'$.tags[*] ? (@.type() != "string")') AND
        NOT jsonb_path_exists(
            response_profile,'$.removed_tags[*] ? (@.type() != "string")') AND
        jsonb_typeof(response_profile->'summary')='string' AND
        jsonb_typeof(response_profile->'created_at')='string' AND
        jsonb_typeof(response_profile->'updated_at')='string'
    ),
    CONSTRAINT fk_profile_edit_receipt_membership
        FOREIGN KEY (tenant_id, user_id)
        REFERENCES memberships (tenant_id, user_id) ON DELETE CASCADE,
    CONSTRAINT fk_profile_edit_receipt_revision_scope
        FOREIGN KEY (tenant_id, user_id, revision_id)
        REFERENCES profile_edit_revisions (tenant_id, user_id, id)
);

-- 专用运行角色只有本 authority 所需的最小列权限；无 DELETE、无 revision UPDATE。
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='vane_profile_editor') THEN
        BEGIN
            CREATE ROLE vane_profile_editor
                NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
                NOLOGIN NOINHERIT NOBYPASSRLS;
        EXCEPTION
            WHEN duplicate_object OR unique_violation THEN NULL;
        END;
    END IF;
END $$;
-- +goose StatementEnd

ALTER ROLE vane_profile_editor
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    NOLOGIN NOINHERIT NOBYPASSRLS;
ALTER ROLE vane_profile_editor RESET ALL;
ALTER ROLE vane_profile_editor SET search_path=pg_catalog,public,pg_temp;
GRANT vane_profile_editor TO CURRENT_USER;

-- Normalize database-local privileges before granting the exact capability.
REVOKE ALL ON profile_edit_revisions, profile_edit_receipts
    FROM PUBLIC, vane_app, vane_profile_editor;
REVOKE ALL ON SEQUENCE profile_edit_revisions_id_seq
    FROM PUBLIC, vane_app, vane_profile_editor;
REVOKE ALL ON profiles, memberships FROM vane_profile_editor;
REVOKE ALL ON SEQUENCE profiles_id_seq FROM vane_profile_editor;

-- A pre-existing cluster role must not have an ambient path to/from runtime
-- roles, and only this database's migration owner may SET ROLE into it.
-- +goose StatementBegin
DO $$
BEGIN
    IF pg_has_role('vane_profile_editor','vane_app','MEMBER') OR
       pg_has_role('vane_app','vane_profile_editor','MEMBER') THEN
        RAISE EXCEPTION '060: vane_app and profile editor must be unrelated';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles granted_role ON granted_role.oid=am.roleid
          JOIN pg_roles member_role ON member_role.oid=am.member
         WHERE granted_role.rolname='vane_profile_editor'
           AND member_role.rolname<>CURRENT_USER
    ) THEN
        RAISE EXCEPTION '060: only migration owner may enter profile editor';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles member_role ON member_role.oid=am.member
         WHERE member_role.rolname='vane_profile_editor'
    ) THEN
        RAISE EXCEPTION '060: profile editor must not enter another role';
    END IF;
END $$;
-- +goose StatementEnd

GRANT USAGE ON SCHEMA public TO vane_profile_editor;
GRANT SELECT (tenant_id,user_id) ON memberships TO vane_profile_editor;
GRANT SELECT (
    tenant_id,user_id,industry,occupation,tags,removed_tags,
    summary,created_at,updated_at
) ON profiles TO vane_profile_editor;
GRANT INSERT (
    tenant_id,user_id,industry,occupation,tags,updated_at
) ON profiles TO vane_profile_editor;
GRANT USAGE ON SEQUENCE profiles_id_seq TO vane_profile_editor;
GRANT UPDATE (industry,occupation,tags,removed_tags,updated_at)
    ON profiles TO vane_profile_editor;
GRANT SELECT (
    id,tenant_id,user_id,actor_user_id,kind,target_revision_id,
    before_fields,after_fields,result_updated_at,created_at
), INSERT (
    tenant_id,user_id,actor_user_id,kind,target_revision_id,
    before_fields,after_fields,result_updated_at
) ON profile_edit_revisions TO vane_profile_editor;
GRANT USAGE ON SEQUENCE profile_edit_revisions_id_seq
    TO vane_profile_editor;
GRANT SELECT (
    tenant_id,user_id,idempotency_key,request_digest,revision_id,
    response_profile,created_at
), INSERT (
    tenant_id,user_id,idempotency_key,request_digest,revision_id,
    response_profile
) ON profile_edit_receipts TO vane_profile_editor;

ALTER TABLE profile_edit_revisions ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON profile_edit_revisions
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON profile_edit_revisions AS RESTRICTIVE
    FOR ALL
    USING (tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint);
CREATE POLICY user_isolation ON profile_edit_revisions AS RESTRICTIVE
    FOR ALL
    USING (user_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.user_id', true)), '')::bigint)
    WITH CHECK (user_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.user_id', true)), '')::bigint);

ALTER TABLE profile_edit_receipts ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON profile_edit_receipts
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON profile_edit_receipts AS RESTRICTIVE
    FOR ALL
    USING (tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint);
CREATE POLICY user_isolation ON profile_edit_receipts AS RESTRICTIVE
    FOR ALL
    USING (user_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.user_id', true)), '')::bigint)
    WITH CHECK (user_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.user_id', true)), '')::bigint);

-- Existing tables have tenant RLS only. These RESTRICTIVE policies apply just
-- to the profile editor role and make its raw capability exact-user scoped.
CREATE POLICY profile_editor_identity ON profiles AS RESTRICTIVE
    FOR ALL TO vane_profile_editor
    USING (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint AND
        user_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.user_id', true)), '')::bigint
    )
    WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint AND
        user_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.user_id', true)), '')::bigint
    );
ALTER TABLE memberships ENABLE ROW LEVEL SECURITY;
CREATE POLICY profile_editor_identity ON memberships AS RESTRICTIVE
    FOR SELECT TO vane_profile_editor
    USING (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint AND
        user_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.user_id', true)), '')::bigint
    );

-- +goose Down

LOCK TABLE profile_edit_receipts, profile_edit_revisions
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM profile_edit_receipts) OR
       EXISTS (SELECT 1 FROM profile_edit_revisions) THEN
        RAISE EXCEPTION
            '060: refusing downgrade while profile edit audit evidence exists';
    END IF;
END $$;
-- +goose StatementEnd

DROP POLICY IF EXISTS tenant_isolation ON profile_edit_receipts;
DROP POLICY IF EXISTS tenant_visible ON profile_edit_receipts;
DROP POLICY IF EXISTS user_isolation ON profile_edit_receipts;
DROP POLICY IF EXISTS tenant_isolation ON profile_edit_revisions;
DROP POLICY IF EXISTS tenant_visible ON profile_edit_revisions;
DROP POLICY IF EXISTS user_isolation ON profile_edit_revisions;
DROP POLICY IF EXISTS profile_editor_identity ON profiles;
DROP POLICY IF EXISTS profile_editor_identity ON memberships;

REVOKE SELECT (
    tenant_id,user_id,idempotency_key,request_digest,revision_id,
    response_profile,created_at
), INSERT (
    tenant_id,user_id,idempotency_key,request_digest,revision_id,
    response_profile
) ON profile_edit_receipts FROM vane_profile_editor;
REVOKE USAGE ON SEQUENCE profile_edit_revisions_id_seq
    FROM vane_profile_editor;
REVOKE SELECT (
    id,tenant_id,user_id,actor_user_id,kind,target_revision_id,
    before_fields,after_fields,result_updated_at,created_at
), INSERT (
    tenant_id,user_id,actor_user_id,kind,target_revision_id,
    before_fields,after_fields,result_updated_at
) ON profile_edit_revisions FROM vane_profile_editor;
REVOKE UPDATE (industry,occupation,tags,removed_tags,updated_at)
    ON profiles FROM vane_profile_editor;
REVOKE USAGE ON SEQUENCE profiles_id_seq FROM vane_profile_editor;
REVOKE INSERT (
    tenant_id,user_id,industry,occupation,tags,updated_at
) ON profiles FROM vane_profile_editor;
REVOKE SELECT (
    tenant_id,user_id,industry,occupation,tags,removed_tags,
    summary,created_at,updated_at
) ON profiles FROM vane_profile_editor;
REVOKE SELECT (tenant_id,user_id) ON memberships FROM vane_profile_editor;
REVOKE USAGE ON SCHEMA public FROM vane_profile_editor;

DROP TABLE profile_edit_receipts;
DROP TABLE profile_edit_revisions;
