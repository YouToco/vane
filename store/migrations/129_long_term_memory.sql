-- 129: explicit, evidence-bound, per-user long-term memory ledger.
--
-- Records, events, and response-loss receipts are append-only to the runtime.
-- Recall is reconstructed only from active event results; correction and
-- forget never rewrite retained history. The only accepted source is a trusted
-- server-side owner-explicit Agent-turn UUID.

-- +goose Up

CREATE TABLE memory_authorizations (
    id                    UUID        PRIMARY KEY,
    tenant_id             BIGINT      NOT NULL REFERENCES tenants (id),
    user_id               BIGINT      NOT NULL REFERENCES users (id),
    session_id            BIGINT      NOT NULL,
    trace_id              UUID        NOT NULL,
    owner_request         TEXT        NOT NULL,
    authorization_digest  TEXT        NOT NULL,
    request_digest        TEXT        NOT NULL,
    consumed_event_id     BIGINT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_memory_authorization_scope UNIQUE (tenant_id,user_id,id),
    CONSTRAINT uq_memory_authorization_invocation
        UNIQUE (tenant_id,user_id,session_id,trace_id,request_digest),
    CONSTRAINT fk_memory_authorization_session
        FOREIGN KEY (session_id,tenant_id,user_id)
        REFERENCES agent_sessions (id,tenant_id,user_id),
    CONSTRAINT ck_memory_authorization_owner_request
        CHECK (octet_length(owner_request) BETWEEN 1 AND 65536),
    CONSTRAINT ck_memory_authorization_digest
        CHECK (authorization_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_memory_authorization_request_digest
        CHECK (request_digest ~ '^[0-9a-f]{64}$')
);

CREATE TABLE memory_records (
    id                    BIGSERIAL   PRIMARY KEY,
    tenant_id             BIGINT      NOT NULL,
    user_id               BIGINT      NOT NULL,
    memory_text           TEXT        NOT NULL,
    evidence_source_type  TEXT        NOT NULL,
    evidence_source_id    UUID        NOT NULL,
    authorization_id      UUID        NOT NULL,
    owner_request         TEXT        NOT NULL,
    authorization_digest  TEXT        NOT NULL,
    supersedes_memory_id  BIGINT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_memory_record_text
        CHECK (octet_length(memory_text) BETWEEN 1 AND 4096),
    CONSTRAINT ck_memory_record_explicit_source
        CHECK (evidence_source_type='owner_explicit_agent_turn'),
    CONSTRAINT ck_memory_record_owner_request
        CHECK (octet_length(owner_request) BETWEEN 1 AND 65536),
    CONSTRAINT ck_memory_record_authorization_digest
        CHECK (authorization_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT uq_memory_record_scope UNIQUE (tenant_id,user_id,id),
    CONSTRAINT fk_memory_record_tenant
        FOREIGN KEY (tenant_id) REFERENCES tenants (id),
    CONSTRAINT fk_memory_record_user
        FOREIGN KEY (user_id) REFERENCES users (id),
    CONSTRAINT fk_memory_record_authorization_scope
        FOREIGN KEY (tenant_id,user_id,authorization_id)
        REFERENCES memory_authorizations (tenant_id,user_id,id),
    CONSTRAINT fk_memory_record_supersedes_scope
        FOREIGN KEY (tenant_id,user_id,supersedes_memory_id)
        REFERENCES memory_records (tenant_id,user_id,id)
);

CREATE INDEX idx_memory_records_scope
    ON memory_records (tenant_id,user_id,id);

CREATE TABLE memory_events (
    id                    BIGSERIAL   PRIMARY KEY,
    tenant_id             BIGINT      NOT NULL,
    user_id               BIGINT      NOT NULL,
    actor_user_id         BIGINT      NOT NULL,
    event_kind            TEXT        NOT NULL,
    target_memory_id      BIGINT,
    result_memory_id      BIGINT,
    evidence_source_type  TEXT        NOT NULL,
    evidence_source_id    UUID        NOT NULL,
    authorization_id      UUID        NOT NULL,
    owner_request         TEXT        NOT NULL,
    authorization_digest  TEXT        NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_memory_event_actor_self CHECK (actor_user_id=user_id),
    CONSTRAINT ck_memory_event_kind
        CHECK (event_kind IN ('remember','correct','forget')),
    CONSTRAINT ck_memory_event_shape CHECK (
        (event_kind='remember' AND target_memory_id IS NULL
          AND result_memory_id IS NOT NULL) OR
        (event_kind='correct' AND target_memory_id IS NOT NULL
          AND result_memory_id IS NOT NULL) OR
        (event_kind='forget' AND target_memory_id IS NOT NULL
          AND result_memory_id IS NULL)
    ),
    CONSTRAINT ck_memory_event_explicit_source
        CHECK (evidence_source_type='owner_explicit_agent_turn'),
    CONSTRAINT ck_memory_event_owner_request
        CHECK (octet_length(owner_request) BETWEEN 1 AND 65536),
    CONSTRAINT ck_memory_event_authorization_digest
        CHECK (authorization_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT uq_memory_event_scope UNIQUE (tenant_id,user_id,id),
    CONSTRAINT fk_memory_event_tenant
        FOREIGN KEY (tenant_id) REFERENCES tenants (id),
    CONSTRAINT fk_memory_event_user
        FOREIGN KEY (user_id) REFERENCES users (id),
    CONSTRAINT fk_memory_event_authorization_scope
        FOREIGN KEY (tenant_id,user_id,authorization_id)
        REFERENCES memory_authorizations (tenant_id,user_id,id),
    CONSTRAINT fk_memory_event_target_scope
        FOREIGN KEY (tenant_id,user_id,target_memory_id)
        REFERENCES memory_records (tenant_id,user_id,id),
    CONSTRAINT fk_memory_event_result_scope
        FOREIGN KEY (tenant_id,user_id,result_memory_id)
        REFERENCES memory_records (tenant_id,user_id,id)
);

CREATE UNIQUE INDEX uq_memory_authorization_consumed_event
    ON memory_authorizations (tenant_id,user_id,consumed_event_id)
    WHERE consumed_event_id IS NOT NULL;

-- One record is introduced by exactly one event, and an active record can be
-- consumed by at most one correction/forget. The Store's membership row lock
-- gives friendly conflicts; these indexes remain the final concurrency fence.
CREATE UNIQUE INDEX uq_memory_event_result_once
    ON memory_events (tenant_id,user_id,result_memory_id)
    WHERE result_memory_id IS NOT NULL;
CREATE UNIQUE INDEX uq_memory_event_target_once
    ON memory_events (tenant_id,user_id,target_memory_id)
    WHERE target_memory_id IS NOT NULL;
CREATE INDEX idx_memory_events_scope
    ON memory_events (tenant_id,user_id,id);

CREATE TABLE memory_receipts (
    tenant_id          BIGINT      NOT NULL,
    user_id            BIGINT      NOT NULL,
    idempotency_key    TEXT        NOT NULL,
    request_digest     TEXT        NOT NULL,
    event_id           BIGINT      NOT NULL,
    response_payload   JSONB       NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id,user_id,idempotency_key),
    CONSTRAINT ck_memory_receipt_key
        CHECK (idempotency_key ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_memory_receipt_digest
        CHECK (request_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_memory_receipt_response
        CHECK (jsonb_typeof(response_payload)='object'),
    CONSTRAINT fk_memory_receipt_tenant
        FOREIGN KEY (tenant_id) REFERENCES tenants (id),
    CONSTRAINT fk_memory_receipt_user
        FOREIGN KEY (user_id) REFERENCES users (id),
    CONSTRAINT fk_memory_receipt_event_scope
        FOREIGN KEY (tenant_id,user_id,event_id)
        REFERENCES memory_events (tenant_id,user_id,id)
);

-- +goose StatementBegin
DO $$
DECLARE owner_name TEXT:=current_user;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles
                    WHERE rolname='vane_memory_editor') THEN
        BEGIN
            CREATE ROLE vane_memory_editor
                NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
                NOLOGIN NOINHERIT NOBYPASSRLS;
            ALTER ROLE vane_memory_editor
                SET search_path=pg_catalog,public,pg_temp;
            GRANT vane_memory_editor TO CURRENT_USER
                WITH ADMIN FALSE, SET TRUE, INHERIT FALSE;
        EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL;
        END;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles
         WHERE rolname='vane_memory_editor'
           AND NOT rolcanlogin AND NOT rolsuper AND NOT rolcreatedb
           AND NOT rolcreaterole AND NOT rolinherit AND NOT rolreplication
           AND NOT rolbypassrls
           AND rolconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::TEXT[]
    ) OR NOT pg_has_role(owner_name,'vane_memory_editor','SET') THEN
        RAISE EXCEPTION '129: memory editor role is absent or unsafe'
            USING ERRCODE='42501';
    END IF;
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
DO $$
BEGIN
    IF pg_has_role('vane_memory_editor','vane_app','MEMBER') OR
       pg_has_role('vane_app','vane_memory_editor','MEMBER') OR
       pg_has_role('vane_memory_editor','vane_profile_claim_editor','MEMBER') OR
       pg_has_role('vane_profile_claim_editor','vane_memory_editor','MEMBER') THEN
        RAISE EXCEPTION '129: memory editor must be unrelated to app/profile roles';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_auth_members edge
        JOIN pg_catalog.pg_roles granted ON granted.oid=edge.roleid
        JOIN pg_catalog.pg_roles member ON member.oid=edge.member
        WHERE granted.rolname='vane_memory_editor'
          AND member.rolname NOT IN (current_user,'vane_server_runtime')
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_auth_members edge
        JOIN pg_catalog.pg_roles member ON member.oid=edge.member
        WHERE member.rolname='vane_memory_editor'
    ) THEN
        RAISE EXCEPTION '129: memory editor membership drift';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE ALL ON memory_records,memory_events,memory_receipts
    FROM PUBLIC,vane_app,vane_profile_editor,vane_profile_claim_editor,
         vane_memory_editor;
REVOKE ALL ON memory_authorizations
    FROM PUBLIC,vane_app,vane_profile_editor,vane_profile_claim_editor,
         vane_memory_editor;
REVOKE ALL ON SEQUENCE memory_records_id_seq,memory_events_id_seq
    FROM PUBLIC,vane_app,vane_profile_editor,vane_profile_claim_editor,
         vane_memory_editor;

ALTER TABLE memory_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_records FORCE ROW LEVEL SECURITY;
ALTER TABLE memory_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_events FORCE ROW LEVEL SECURITY;
ALTER TABLE memory_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_receipts FORCE ROW LEVEL SECURITY;
ALTER TABLE memory_authorizations ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_authorizations FORCE ROW LEVEL SECURITY;

CREATE POLICY memory_authorizations_exact_user ON memory_authorizations
    USING (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    ) WITH CHECK (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );

CREATE POLICY memory_records_exact_user ON memory_records
    USING (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    ) WITH CHECK (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );
CREATE POLICY memory_events_exact_user ON memory_events
    USING (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    ) WITH CHECK (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );
CREATE POLICY memory_receipts_exact_user ON memory_receipts
    USING (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    ) WITH CHECK (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );
GRANT USAGE ON SCHEMA public TO vane_memory_editor;
GRANT SELECT,INSERT ON memory_records,memory_events,memory_receipts
    TO vane_memory_editor;
GRANT SELECT,INSERT,UPDATE (consumed_event_id) ON memory_authorizations
    TO vane_memory_editor;
GRANT USAGE,SELECT ON SEQUENCE memory_records_id_seq,memory_events_id_seq
    TO vane_memory_editor;

-- +goose StatementBegin
CREATE FUNCTION assert_vane_memory_editor_v129() RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE owner_name TEXT:=current_user; role_oid OID; unexpected BIGINT;
BEGIN
    IF session_user<>owner_name THEN
        RAISE EXCEPTION '129: only direct migration owner may validate memory editor'
            USING ERRCODE='42501';
    END IF;
    SELECT oid INTO role_oid FROM pg_catalog.pg_roles
     WHERE rolname='vane_memory_editor'
       AND NOT rolcanlogin AND NOT rolsuper AND NOT rolcreatedb
       AND NOT rolcreaterole AND NOT rolinherit AND NOT rolreplication
       AND NOT rolbypassrls
       AND rolconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::TEXT[];
    IF role_oid IS NULL THEN
        RAISE EXCEPTION '129: memory editor role is absent or unsafe'
            USING ERRCODE='42501';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_auth_members edge
        JOIN pg_catalog.pg_roles member ON member.oid=edge.member
        WHERE edge.roleid=role_oid AND (
            member.rolname NOT IN (owner_name,'vane_server_runtime') OR
            edge.admin_option OR edge.inherit_option OR NOT edge.set_option
        )
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_auth_members edge
        WHERE edge.member=role_oid
    ) OR NOT pg_has_role(owner_name,'vane_memory_editor','SET') THEN
        RAISE EXCEPTION '129: memory editor membership drift'
            USING ERRCODE='42501';
    END IF;

    SELECT
      (SELECT count(*) FROM pg_catalog.pg_namespace object
       CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.nspacl,
           pg_catalog.acldefault('n',object.nspowner))) acl
       WHERE acl.grantee=role_oid AND NOT (
           object.nspname='public' AND acl.privilege_type='USAGE')) +
      (SELECT count(*) FROM pg_catalog.pg_class object
       JOIN pg_catalog.pg_namespace namespace ON namespace.oid=object.relnamespace
       CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.relacl,
           pg_catalog.acldefault(CASE WHEN object.relkind='S' THEN 's'::"char"
                                     ELSE 'r'::"char" END,object.relowner))) acl
       WHERE acl.grantee=role_oid AND NOT (
         namespace.nspname='public' AND (
           (object.relname IN ('memory_authorizations','memory_records','memory_events','memory_receipts')
             AND acl.privilege_type IN ('SELECT','INSERT')) OR
           (object.relname IN ('memory_records_id_seq','memory_events_id_seq')
             AND acl.privilege_type IN ('USAGE','SELECT'))))) +
      (SELECT count(*) FROM pg_catalog.pg_attribute object
       JOIN pg_catalog.pg_class relation ON relation.oid=object.attrelid
       JOIN pg_catalog.pg_namespace namespace ON namespace.oid=relation.relnamespace
       CROSS JOIN LATERAL pg_catalog.aclexplode(object.attacl) acl
       WHERE acl.grantee=role_oid AND NOT (
         namespace.nspname='public' AND
         relation.relname='memory_authorizations' AND
         object.attname='consumed_event_id' AND
         acl.privilege_type='UPDATE')) +
      (SELECT count(*) FROM pg_catalog.pg_proc object
       CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.proacl,
           pg_catalog.acldefault('f',object.proowner))) acl
       WHERE acl.grantee=role_oid) +
      (SELECT count(*) FROM pg_catalog.pg_default_acl object
       CROSS JOIN LATERAL pg_catalog.aclexplode(object.defaclacl) acl
       WHERE acl.grantee=role_oid) +
      (SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspowner=role_oid) +
      (SELECT count(*) FROM pg_catalog.pg_class WHERE relowner=role_oid) +
      (SELECT count(*) FROM pg_catalog.pg_proc WHERE proowner=role_oid) +
      (SELECT count(*) FROM pg_catalog.pg_type WHERE typowner=role_oid)
      INTO unexpected;
    IF unexpected<>0 THEN
        RAISE EXCEPTION '129: memory editor retains % unexpected authorities',unexpected
            USING ERRCODE='42501';
    END IF;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION assert_vane_memory_editor_v129() FROM PUBLIC;
SELECT assert_vane_memory_editor_v129();

-- +goose StatementBegin
CREATE FUNCTION provision_vane_server_runtime_v129() RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
    IF session_user<>current_user THEN
        RAISE EXCEPTION '129: only direct migration owner may provision runtime'
            USING ERRCODE='42501';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles
         WHERE rolname='vane_memory_editor'
           AND NOT rolcanlogin AND NOT rolsuper AND NOT rolcreatedb
           AND NOT rolcreaterole AND NOT rolinherit AND NOT rolreplication
           AND NOT rolbypassrls
           AND rolconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::TEXT[]
    ) OR NOT pg_has_role(current_user,'vane_memory_editor','SET') THEN
        RAISE EXCEPTION '129: memory editor role is absent or unsafe'
            USING ERRCODE='42501';
    END IF;
    PERFORM public.assert_vane_memory_editor_v129();
    -- v128 ultimately invokes the immutable v098 exact-role validator. A
    -- repeated provision must temporarily remove the v129-only capability so
    -- that validator observes precisely its historical role set.
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles
               WHERE rolname='vane_server_runtime') THEN
        REVOKE vane_memory_editor FROM vane_server_runtime;
    END IF;
    PERFORM public.provision_vane_server_runtime_v128();
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles
               WHERE rolname='vane_server_runtime') THEN
        GRANT vane_memory_editor TO vane_server_runtime
            WITH ADMIN FALSE, SET TRUE, INHERIT FALSE;
    END IF;
    PERFORM public.assert_vane_memory_editor_v129();
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION provision_vane_server_runtime_v129() FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION deprovision_vane_server_runtime_v129() RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
    IF session_user<>current_user THEN
        RAISE EXCEPTION '129: only direct migration owner may deprovision runtime'
            USING ERRCODE='42501';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles
               WHERE rolname='vane_server_runtime') THEN
        REVOKE vane_memory_editor FROM vane_server_runtime;
    END IF;
    PERFORM public.deprovision_vane_server_runtime_v128();
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION deprovision_vane_server_runtime_v129() FROM PUBLIC;

-- +goose Down

LOCK TABLE memory_receipts,memory_authorizations,memory_events,memory_records
    IN ACCESS EXCLUSIVE MODE;

-- Cluster-global memberships change only through the explicit provisioner.
-- Deprovision while this exact wrapper still exists before rolling back.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles
                WHERE rolname='vane_server_runtime') THEN
        RAISE EXCEPTION '129: deprovision vane_server_runtime before schema downgrade';
    END IF;
END $$;
-- +goose StatementEnd

-- Retained owner evidence is never destroyed by a routine rollback.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM memory_authorizations) OR
       EXISTS (SELECT 1 FROM memory_records) OR
       EXISTS (SELECT 1 FROM memory_events) OR
       EXISTS (SELECT 1 FROM memory_receipts) THEN
        RAISE EXCEPTION '129: refusing downgrade while retained memory history exists';
    END IF;
END $$;
-- +goose StatementEnd

DROP FUNCTION IF EXISTS deprovision_vane_server_runtime_v129();
DROP FUNCTION IF EXISTS provision_vane_server_runtime_v129();
DROP FUNCTION IF EXISTS assert_vane_memory_editor_v129();
DROP TABLE memory_receipts;
DROP TABLE memory_events;
DROP TABLE memory_records;
DROP TABLE memory_authorizations;
REVOKE USAGE ON SCHEMA public FROM vane_memory_editor;
