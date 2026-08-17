-- 153: immutable remote MCP approval and credential-generation binding.
--
-- This is a dark substrate.  It grants the capability invocation coordinator
-- read-only access to exact approved bindings, but vane_server_runtime is not
-- a member of that role and receives no execution or binding authority here.

-- +goose Up

CREATE TABLE mcp_runtime_bindings (
    tenant_id                    BIGINT      NOT NULL,
    owner_user_id                BIGINT      NOT NULL,
    capability_id                UUID        NOT NULL,
    capability_version_id        UUID        NOT NULL,
    visibility                   TEXT        NOT NULL,
    capability_version_digest    TEXT        NOT NULL,
    endpoint_url                 TEXT        NOT NULL,
    protocol_version             TEXT        NOT NULL,
    authentication_kind          TEXT        NOT NULL,
    connection_schema_digest     TEXT        NOT NULL,
    approved_catalog_digest      TEXT        NOT NULL,
    approved_by_user_id          BIGINT      NOT NULL REFERENCES users(id),
    approved_at                  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    credential_opaque_ref        TEXT,
    credential_opaque_ref_digest TEXT,
    credential_entry_id          BIGINT      REFERENCES credential_vault_entries(id),
    credential_scope_kind        TEXT,
    credential_tenant_id         BIGINT,
    credential_user_id           BIGINT,
    credential_provider          TEXT,
    credential_purpose           TEXT,
    credential_generation        BIGINT,
    credential_fingerprint       TEXT,
    PRIMARY KEY (tenant_id,owner_user_id,capability_id,capability_version_id),
    CONSTRAINT fk_mcp_runtime_binding_version_scope
        FOREIGN KEY (tenant_id,owner_user_id,capability_id,visibility,capability_version_id)
        REFERENCES user_capability_versions(
            tenant_id,owner_user_id,capability_id,visibility,id) ON DELETE CASCADE,
    CONSTRAINT fk_mcp_runtime_binding_subtype
        FOREIGN KEY (capability_version_id)
        REFERENCES mcp_connection_versions(capability_version_id) ON DELETE CASCADE,
    CONSTRAINT ck_mcp_runtime_binding_visibility
        CHECK (visibility IN ('personal','workspace')),
    CONSTRAINT ck_mcp_runtime_binding_connection CHECK (
        capability_version_digest ~ '^[0-9a-f]{64}$' AND
        endpoint_url LIKE 'https://%' AND endpoint_url !~ '[?#[:space:]]' AND
        octet_length(endpoint_url) <= 2048 AND
        protocol_version IN ('2025-06-18','2025-11-25') AND
        authentication_kind IN ('none','api_key','bearer') AND
        connection_schema_digest ~ '^[0-9a-f]{64}$' AND
        approved_catalog_digest ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_mcp_runtime_binding_credential CHECK (
        (credential_opaque_ref IS NULL AND credential_opaque_ref_digest IS NULL AND
         credential_entry_id IS NULL AND credential_scope_kind IS NULL AND
         credential_tenant_id IS NULL AND credential_user_id IS NULL AND
         credential_provider IS NULL AND credential_purpose IS NULL AND
         credential_generation IS NULL AND credential_fingerprint IS NULL) OR
        (credential_opaque_ref ~ '^vault:[A-Za-z0-9][A-Za-z0-9._-]{0,239}$' AND
         credential_opaque_ref_digest ~ '^[0-9a-f]{64}$' AND
         credential_opaque_ref_digest=encode(sha256(convert_to(credential_opaque_ref,'UTF8')),'hex') AND
         credential_entry_id IS NOT NULL AND
         credential_scope_kind IN ('tenant','user') AND
         credential_tenant_id=tenant_id AND
         credential_provider ~ '^[a-z][a-z0-9_-]{0,63}$' AND
         credential_purpose ~ '^[a-z][a-z0-9_-]{0,63}$' AND
         credential_generation>0 AND credential_fingerprint ~ '^[0-9a-f]{64}$' AND
         ((visibility='personal' AND credential_scope_kind='user' AND
           credential_user_id=owner_user_id) OR
          (visibility='workspace' AND credential_scope_kind='tenant' AND
           credential_user_id IS NULL)))
    )
);

ALTER TABLE mcp_runtime_bindings ENABLE ROW LEVEL SECURITY;
ALTER TABLE mcp_runtime_bindings FORCE ROW LEVEL SECURITY;

-- Workspace readers do not receive SELECT on memberships or tenants. This
-- narrow definer helper re-proves one exact current user and live membership
-- for every workspace row evaluated by RLS.
-- +goose StatementBegin
CREATE FUNCTION mcp_runtime_reader_authorized_v153(p_tenant_id bigint,p_user_id bigint)
RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER PARALLEL SAFE
SET search_path=pg_catalog,public,pg_temp AS $$
  SELECT p_tenant_id>0 AND p_user_id>0 AND EXISTS(
    SELECT 1 FROM public.memberships membership
    JOIN public.tenants tenant ON tenant.id=membership.tenant_id
    WHERE membership.tenant_id=p_tenant_id AND membership.user_id=p_user_id AND
          tenant.status='active' AND tenant.deleted_at IS NULL
  )
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION mcp_runtime_reader_authorized_v153(bigint,bigint) FROM PUBLIC,vane_app;
GRANT EXECUTE ON FUNCTION mcp_runtime_reader_authorized_v153(bigint,bigint)
  TO vane_capability_invocation_coordinator;

CREATE POLICY mcp_runtime_binding_exact_principal ON mcp_runtime_bindings
    FOR SELECT TO vane_capability_invocation_coordinator USING (
        tenant_id=NULLIF(current_setting('app.tenant_id',true),'')::bigint AND
        ((visibility='personal' AND
          owner_user_id=NULLIF(current_setting('app.user_id',true),'')::bigint) OR
         (visibility='workspace' AND public.mcp_runtime_reader_authorized_v153(
           tenant_id,NULLIF(current_setting('app.user_id',true),'')::bigint)))
    );

REVOKE ALL ON mcp_runtime_bindings FROM PUBLIC,vane_app;
-- +goose StatementBegin
DO $$
BEGIN
  IF EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='vane_server_runtime') THEN
    REVOKE ALL ON public.mcp_runtime_bindings FROM vane_server_runtime;
  END IF;
END $$;
-- +goose StatementEnd
GRANT SELECT ON mcp_runtime_bindings TO vane_capability_invocation_coordinator;

-- Insert is deliberately migration/control-plane-owner only.  The trigger
-- re-proves the immutable connection, human approval authority, and exact
-- vault coordinates in the same transaction as the binding.
-- +goose StatementBegin
CREATE FUNCTION enforce_mcp_runtime_binding_v1()
RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE connection_row public.mcp_connection_versions%ROWTYPE;
DECLARE version_row public.user_capability_versions%ROWTYPE;
DECLARE credential_row public.credential_vault_entries%ROWTYPE;
DECLARE approver_allowed boolean;
BEGIN
  SELECT * INTO version_row FROM public.user_capability_versions
   WHERE tenant_id=NEW.tenant_id AND owner_user_id=NEW.owner_user_id AND
         capability_id=NEW.capability_id AND visibility=NEW.visibility AND
         id=NEW.capability_version_id
   FOR SHARE;
  IF NOT FOUND OR version_row.payload_digest<>NEW.capability_version_digest THEN
    RAISE EXCEPTION '153: binding differs from exact capability version' USING ERRCODE='23514';
  END IF;
  SELECT * INTO connection_row FROM public.mcp_connection_versions
   WHERE tenant_id=NEW.tenant_id AND owner_user_id=NEW.owner_user_id AND
         capability_id=NEW.capability_id AND visibility=NEW.visibility AND
         capability_version_id=NEW.capability_version_id
   FOR SHARE;
  IF NOT FOUND OR connection_row.endpoint_url<>NEW.endpoint_url OR
     connection_row.protocol_version<>NEW.protocol_version OR
     connection_row.authentication_kind<>NEW.authentication_kind OR
     connection_row.tool_schema_digest<>NEW.connection_schema_digest THEN
    RAISE EXCEPTION '153: approval differs from exact MCP version connection' USING ERRCODE='23514';
  END IF;
  SELECT EXISTS(SELECT 1 FROM public.memberships membership
    JOIN public.tenants tenant ON tenant.id=membership.tenant_id
    WHERE membership.tenant_id=NEW.tenant_id AND
          membership.user_id=NEW.approved_by_user_id AND
          tenant.status='active' AND tenant.deleted_at IS NULL AND
          ((NEW.visibility='personal' AND NEW.approved_by_user_id=NEW.owner_user_id) OR
           (NEW.visibility='workspace' AND membership.role IN ('owner','admin')))
    FOR SHARE OF membership,tenant) INTO approver_allowed;
  IF NOT approver_allowed THEN
    RAISE EXCEPTION '153: exact human approval authority is absent' USING ERRCODE='42501';
  END IF;
  IF connection_row.authentication_kind='none' THEN
    IF connection_row.credential_ref<>'' OR NEW.credential_entry_id IS NOT NULL THEN
      RAISE EXCEPTION '153: credentialless MCP binding contains credential authority' USING ERRCODE='23514';
    END IF;
  ELSIF connection_row.authentication_kind IN ('api_key','bearer') THEN
    IF NEW.credential_entry_id IS NULL OR
       connection_row.credential_ref<>NEW.credential_opaque_ref THEN
      RAISE EXCEPTION '153: MCP credential reference is not exactly bound' USING ERRCODE='23514';
    END IF;
    SELECT * INTO credential_row FROM public.credential_vault_entries
     WHERE id=NEW.credential_entry_id FOR SHARE;
    IF NOT FOUND OR credential_row.status<>'active' OR
       credential_row.scope_kind<>NEW.credential_scope_kind OR
       credential_row.tenant_id IS DISTINCT FROM NEW.credential_tenant_id OR
       credential_row.user_id IS DISTINCT FROM NEW.credential_user_id OR
       credential_row.provider<>NEW.credential_provider OR
       credential_row.purpose<>NEW.credential_purpose OR
       credential_row.generation<>NEW.credential_generation OR
       credential_row.fingerprint<>NEW.credential_fingerprint THEN
      RAISE EXCEPTION '153: MCP credential vault coordinates differ' USING ERRCODE='23514';
    END IF;
  ELSE
    RAISE EXCEPTION '153: OAuth and unsupported MCP authentication remain disabled' USING ERRCODE='0A000';
  END IF;
  RETURN NEW;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_mcp_runtime_binding_v1() FROM PUBLIC;
CREATE TRIGGER mcp_runtime_binding_insert_v1
BEFORE INSERT ON mcp_runtime_bindings
FOR EACH ROW EXECUTE FUNCTION enforce_mcp_runtime_binding_v1();

-- +goose StatementBegin
CREATE FUNCTION reject_mcp_runtime_binding_mutation_v1()
RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
  IF TG_OP='DELETE' AND current_setting('app.tenant_purge',true)='on' AND
     OLD.tenant_id=NULLIF(current_setting('app.tenant_id',true),'')::bigint THEN
    RETURN OLD;
  END IF;
  RAISE EXCEPTION '153: MCP runtime bindings are immutable' USING ERRCODE='55000';
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION reject_mcp_runtime_binding_mutation_v1() FROM PUBLIC;
CREATE TRIGGER mcp_runtime_binding_immutable_v1
BEFORE UPDATE OR DELETE ON mcp_runtime_bindings
FOR EACH ROW EXECUTE FUNCTION reject_mcp_runtime_binding_mutation_v1();

-- +goose StatementBegin
CREATE FUNCTION assert_vane_mcp_runtime_binding_v153()
RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE table_safe boolean; schema_safe boolean; grants_safe boolean; policy_safe boolean;
        triggers_safe boolean; functions_safe boolean; reader_function_safe boolean;
        schema_digest text;
BEGIN
  IF session_user<>current_user THEN
    RAISE EXCEPTION '153: only direct migration owner may assert MCP binding authority'
      USING ERRCODE='42501';
  END IF;
  SELECT relrowsecurity AND relforcerowsecurity AND
         relowner=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=current_user)
    FROM pg_catalog.pg_class WHERE oid='public.mcp_runtime_bindings'::regclass
    INTO table_safe;
  WITH entries(entry) AS (
    SELECT pg_catalog.format('column|%s|%s|%s|%s|%s|%s|%s',
      attribute.attnum,attribute.attname,
      pg_catalog.format_type(attribute.atttypid,attribute.atttypmod),
      attribute.attnotnull,COALESCE(pg_catalog.pg_get_expr(default_value.adbin,
        default_value.adrelid),''),attribute.attidentity,attribute.attgenerated)
      FROM pg_catalog.pg_attribute attribute
      LEFT JOIN pg_catalog.pg_attrdef default_value ON
        default_value.adrelid=attribute.attrelid AND default_value.adnum=attribute.attnum
     WHERE attribute.attrelid='public.mcp_runtime_bindings'::regclass AND
           attribute.attnum>0 AND NOT attribute.attisdropped
    UNION ALL
    SELECT pg_catalog.format('constraint|%s|%s|%s|%s|%s|%s',
      constraint_row.conname,constraint_row.contype,constraint_row.condeferrable,
      constraint_row.condeferred,constraint_row.convalidated,
      pg_catalog.pg_get_constraintdef(constraint_row.oid,false))
      FROM pg_catalog.pg_constraint constraint_row
     WHERE constraint_row.conrelid='public.mcp_runtime_bindings'::regclass
  )
  SELECT encode(sha256(convert_to(pg_catalog.string_agg(entry,E'\n' ORDER BY entry),
      'UTF8')),'hex') INTO schema_digest FROM entries;
  schema_safe := schema_digest='2dd79e429232df9c86a831dbda2e4101064589cbd33184ce0e2d47829f510a9e';
  SELECT has_table_privilege('vane_capability_invocation_coordinator',
           'public.mcp_runtime_bindings','SELECT') AND
         NOT has_table_privilege('vane_capability_invocation_coordinator',
           'public.mcp_runtime_bindings','INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN') AND
         COALESCE((SELECT NOT pg_catalog.has_table_privilege(role.oid,
           'public.mcp_runtime_bindings','SELECT') FROM pg_catalog.pg_roles role
           WHERE role.rolname='vane_server_runtime'),true) AND
         NOT has_table_privilege('vane_app','public.mcp_runtime_bindings','SELECT') AND
         NOT EXISTS(SELECT 1 FROM pg_catalog.pg_class relation
           CROSS JOIN LATERAL pg_catalog.aclexplode(relation.relacl) acl
           WHERE relation.oid='public.mcp_runtime_bindings'::regclass AND
                 acl.grantee=0 AND acl.privilege_type='SELECT') AND
         NOT EXISTS(SELECT 1 FROM pg_catalog.pg_class relation
           CROSS JOIN LATERAL pg_catalog.aclexplode(relation.relacl) acl
           WHERE relation.oid='public.mcp_runtime_bindings'::regclass AND NOT (
             (acl.grantee=relation.relowner AND acl.grantor=relation.relowner AND
              NOT acl.is_grantable) OR
             (acl.grantee=(SELECT oid FROM pg_catalog.pg_roles
                WHERE rolname='vane_capability_invocation_coordinator') AND
              acl.grantor=relation.relowner AND acl.privilege_type='SELECT' AND
              NOT acl.is_grantable))) AND
         NOT EXISTS(SELECT 1 FROM pg_catalog.pg_attribute attribute
           WHERE attribute.attrelid='public.mcp_runtime_bindings'::regclass AND
                 attribute.attnum>0 AND NOT attribute.attisdropped AND
                 attribute.attacl IS NOT NULL)
    INTO grants_safe;
  SELECT count(*)=1 AND bool_and(policy.polcmd='r' AND policy.polpermissive AND
      policy.polroles=ARRAY[(SELECT oid FROM pg_catalog.pg_roles
        WHERE rolname='vane_capability_invocation_coordinator')]::oid[] AND
      encode(sha256(convert_to(pg_catalog.pg_get_expr(policy.polqual,policy.polrelid),
        'UTF8')),'hex')='20f1154bf3214e71bf4d469a55e259524d22daa58758cd2af31ef9ecf3de702b')
    FROM pg_catalog.pg_policy policy
   WHERE policy.polrelid='public.mcp_runtime_bindings'::regclass AND
         policy.polname='mcp_runtime_binding_exact_principal'
    INTO policy_safe;
  WITH expected(trigger_name,function_oid,definition_digest) AS (VALUES
    ('mcp_runtime_binding_insert_v1',
     'public.enforce_mcp_runtime_binding_v1()'::regprocedure::oid,
     '285645e5b0ff70c13898d6a24f30d4b6d98373a771678517268ecf05f09eb3df'),
    ('mcp_runtime_binding_immutable_v1',
     'public.reject_mcp_runtime_binding_mutation_v1()'::regprocedure::oid,
     '20b557f5d17c8611b314ac3e40f91ceeb37546928227d7360117c015dc7ec964')
  ), actual AS (
    SELECT trigger.tgname trigger_name,trigger.tgfoid function_oid,trigger.tgenabled,
           encode(sha256(convert_to(pg_catalog.pg_get_triggerdef(trigger.oid,false),'UTF8')),'hex')
             definition_digest
      FROM pg_catalog.pg_trigger trigger
     WHERE trigger.tgrelid='public.mcp_runtime_bindings'::regclass AND NOT trigger.tgisinternal
  )
  SELECT count(*)=2 AND bool_and(expected.trigger_name IS NOT NULL AND
      actual.trigger_name IS NOT NULL AND actual.function_oid=expected.function_oid AND
      actual.tgenabled='O' AND actual.definition_digest=expected.definition_digest)
    FROM expected FULL JOIN actual USING(trigger_name)
    INTO triggers_safe;
  WITH expected(function_oid,definition_digest) AS (VALUES
    ('public.enforce_mcp_runtime_binding_v1()'::regprocedure::oid,
     '3a650b6e6f42c26ac90171a99166dc1e12fa57558e42788aa2691e356e856218'),
    ('public.reject_mcp_runtime_binding_mutation_v1()'::regprocedure::oid,
     '1f6cd7e4d2b9c322cda9d8342543d688e0c7499b08e5fb842bc9f05a8d6d8ae0')
  ), actual AS (
    SELECT procedure.oid function_oid,
           encode(sha256(convert_to(pg_catalog.pg_get_functiondef(procedure.oid),'UTF8')),'hex')
             definition_digest,
           procedure.proowner,procedure.prosecdef,procedure.proconfig,procedure.prolang,
           procedure.prorettype,procedure.prokind,procedure.pronargs,procedure.provolatile,
           procedure.proleakproof,procedure.proisstrict,procedure.proretset,procedure.proparallel,
           (SELECT count(*)=1 AND bool_and(acl.grantee=procedure.proowner AND
                    acl.grantor=procedure.proowner AND acl.privilege_type='EXECUTE' AND
                    NOT acl.is_grantable)
              FROM pg_catalog.aclexplode(procedure.proacl) acl) acl_safe
      FROM pg_catalog.pg_proc procedure
     WHERE procedure.pronamespace='public'::regnamespace AND procedure.proname IN(
       'enforce_mcp_runtime_binding_v1','reject_mcp_runtime_binding_mutation_v1')
  )
  SELECT count(*)=2 AND bool_and(expected.function_oid IS NOT NULL AND
      actual.function_oid IS NOT NULL AND actual.definition_digest=expected.definition_digest AND
      actual.proowner=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=current_user) AND
      NOT actual.prosecdef AND
      actual.proconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::text[] AND
      actual.prolang=(SELECT oid FROM pg_catalog.pg_language WHERE lanname='plpgsql') AND
      actual.prorettype='pg_catalog.trigger'::regtype AND actual.prokind='f' AND
      actual.pronargs=0 AND actual.provolatile='v' AND NOT actual.proleakproof AND
      NOT actual.proisstrict AND NOT actual.proretset AND actual.proparallel='u' AND actual.acl_safe)
    FROM expected FULL JOIN actual USING(function_oid)
    INTO functions_safe;
  SELECT encode(sha256(convert_to(pg_catalog.pg_get_functiondef(procedure.oid),'UTF8')),'hex')=
           'b90aa206ff9a58fc26985da2d86516d8251eeaf403a1da5c3c714cbbf02e51ba' AND
         procedure.proowner=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=current_user) AND
         procedure.prosecdef AND
         procedure.proconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::text[] AND
         procedure.prolang=(SELECT oid FROM pg_catalog.pg_language WHERE lanname='sql') AND
         procedure.prorettype='pg_catalog.bool'::regtype AND procedure.prokind='f' AND
         procedure.pronargs=2 AND procedure.provolatile='s' AND NOT procedure.proleakproof AND
         NOT procedure.proisstrict AND NOT procedure.proretset AND procedure.proparallel='s' AND
         (SELECT count(*)=2 AND bool_and(acl.grantor=procedure.proowner AND
             acl.privilege_type='EXECUTE' AND NOT acl.is_grantable AND
             acl.grantee IN(procedure.proowner,(SELECT oid FROM pg_catalog.pg_roles
               WHERE rolname='vane_capability_invocation_coordinator')))
            FROM pg_catalog.aclexplode(procedure.proacl) acl)
    FROM pg_catalog.pg_proc procedure
   WHERE procedure.oid='public.mcp_runtime_reader_authorized_v153(bigint,bigint)'::regprocedure
    INTO reader_function_safe;
  IF NOT table_safe OR NOT schema_safe OR NOT grants_safe OR NOT policy_safe OR
     NOT triggers_safe OR NOT functions_safe OR NOT reader_function_safe THEN
    RAISE EXCEPTION '153: MCP runtime binding contract unsafe table=% schema=% schema_digest=% grants=% policy=% triggers=% functions=% reader=%',
      table_safe,schema_safe,schema_digest,grants_safe,policy_safe,triggers_safe,
      functions_safe,reader_function_safe USING ERRCODE='42501';
  END IF;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION assert_vane_mcp_runtime_binding_v153() FROM PUBLIC;
SELECT public.assert_vane_mcp_runtime_binding_v153();

-- +goose Down

LOCK TABLE mcp_runtime_bindings IN ACCESS EXCLUSIVE MODE;
-- +goose StatementBegin
DO $$
BEGIN
  IF EXISTS(SELECT 1 FROM mcp_runtime_bindings) THEN
    RAISE EXCEPTION '153: refusing downgrade while retained MCP runtime bindings exist';
  END IF;
END $$;
-- +goose StatementEnd

DROP TABLE mcp_runtime_bindings;
DROP FUNCTION assert_vane_mcp_runtime_binding_v153();
DROP FUNCTION reject_mcp_runtime_binding_mutation_v1();
DROP FUNCTION enforce_mcp_runtime_binding_v1();
DROP FUNCTION mcp_runtime_reader_authorized_v153(bigint,bigint);
