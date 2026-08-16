-- +goose Up

-- The v138 schema created the isolated workspace-memory ledger and its role,
-- but its historical provision assertion intentionally predates the exact
-- production runtime closure.  Activation happens while the previous server
-- is still serving, so the grant and the complete catalog check must commit as
-- one transaction.  This migration also makes the frozen v129 bridge replay
-- tolerant of the optional v138 edge without weakening v098's exact-role gate.

-- +goose StatementBegin
CREATE FUNCTION assert_vane_workspace_memory_editor_v151(expect_runtime boolean)
RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE
  role_safe boolean;
  edges_safe boolean;
  rls_safe boolean;
  policies_safe boolean;
  required_acl_count integer;
  unexpected_authority_count integer;
BEGIN
  IF session_user<>current_user THEN
    RAISE EXCEPTION '151: only direct migration owner may assert workspace memory authority'
      USING ERRCODE='42501';
  END IF;

  WITH role_row AS (
    SELECT oid,rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolinherit,
           rolreplication,rolbypassrls,rolconfig
      FROM pg_catalog.pg_roles
     WHERE rolname='vane_workspace_memory_editor'
  ), owner_row AS (
    SELECT owner.oid,owner.rolname
      FROM pg_catalog.pg_class relation
      JOIN pg_catalog.pg_roles owner ON owner.oid=relation.relowner
     WHERE relation.oid='public.workspace_memory_authorizations'::pg_catalog.regclass
  )
  SELECT
    EXISTS(SELECT 1 FROM role_row WHERE NOT rolcanlogin AND NOT rolsuper
      AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolinherit
      AND NOT rolreplication AND NOT rolbypassrls
      AND rolconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::text[]),
    (SELECT count(*)=(CASE WHEN expect_runtime THEN 2 ELSE 1 END)
       FROM pg_catalog.pg_auth_members edge
       JOIN role_row role ON role.oid=edge.roleid
       JOIN pg_catalog.pg_roles member ON member.oid=edge.member
       CROSS JOIN owner_row owner
      WHERE member.rolname=owner.rolname
         OR (expect_runtime AND member.rolname='vane_server_runtime'))
    AND NOT EXISTS(
      SELECT 1 FROM pg_catalog.pg_auth_members edge
      JOIN role_row role ON role.oid=edge.roleid
      JOIN pg_catalog.pg_roles member ON member.oid=edge.member
      CROSS JOIN owner_row owner
      WHERE member.rolname<>owner.rolname
        AND (NOT expect_runtime OR member.rolname<>'vane_server_runtime')
         OR edge.roleid=role.oid AND
            (edge.admin_option OR edge.inherit_option OR NOT edge.set_option))
    AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_auth_members edge
      JOIN role_row role ON role.oid=edge.member),
    (SELECT count(*)=4 AND bool_and(relation.relrowsecurity AND relation.relforcerowsecurity)
       FROM pg_catalog.pg_class relation
      WHERE relation.oid IN(
        'public.workspace_memory_authorizations'::pg_catalog.regclass,
        'public.workspace_memory_records'::pg_catalog.regclass,
        'public.workspace_memory_events'::pg_catalog.regclass,
        'public.workspace_memory_receipts'::pg_catalog.regclass))
  INTO role_safe,edges_safe,rls_safe;

  IF NOT role_safe OR NOT edges_safe OR NOT rls_safe THEN
    RAISE EXCEPTION '151: workspace memory contract unsafe role=% edges=% rls=%',
      role_safe,edges_safe,rls_safe USING ERRCODE='42501';
  END IF;

  WITH role_row AS (SELECT oid FROM pg_catalog.pg_roles
    WHERE rolname='vane_workspace_memory_editor'), policies AS (
    SELECT relation.relname,policy.polname,policy.polpermissive,policy.polcmd,
           policy.polroles,
           pg_catalog.pg_get_expr(policy.polqual,policy.polrelid) AS using_expr,
           pg_catalog.pg_get_expr(policy.polwithcheck,policy.polrelid) AS check_expr
      FROM pg_catalog.pg_policy policy
      JOIN pg_catalog.pg_class relation ON relation.oid=policy.polrelid
      JOIN pg_catalog.pg_namespace namespace ON namespace.oid=relation.relnamespace
     WHERE namespace.nspname='public' AND relation.relname IN(
       'workspace_memory_authorizations','workspace_memory_records',
       'workspace_memory_events','workspace_memory_receipts'))
  SELECT count(*)=6
    AND array_agg(relname||':'||polname ORDER BY relname,polname)=ARRAY[
      'workspace_memory_authorizations:workspace_memory_authorization_insert',
      'workspace_memory_authorizations:workspace_memory_authorization_select',
      'workspace_memory_authorizations:workspace_memory_authorization_update',
      'workspace_memory_events:workspace_memory_event_tenant',
      'workspace_memory_receipts:workspace_memory_receipt_actor',
      'workspace_memory_records:workspace_memory_record_tenant']::text[]
    AND bool_and(polpermissive AND polroles=ARRAY[(SELECT oid FROM role_row)]::oid[])
    AND pg_catalog.md5(pg_catalog.string_agg(
      relname||'|'||polname||'|'||polpermissive::text||'|'||polcmd::text||'|'||
      COALESCE(using_expr,'<null>')||'|'||COALESCE(check_expr,'<null>'),E'\n'
      ORDER BY relname,polname))='6917d270023b8fb464af8bc03d56ba2f'
  INTO policies_safe FROM policies;
  IF NOT policies_safe THEN
    RAISE EXCEPTION '151: workspace memory RLS policy contract differs'
      USING ERRCODE='42501';
  END IF;

  WITH role_row AS (SELECT oid FROM pg_catalog.pg_roles
    WHERE rolname='vane_workspace_memory_editor')
  SELECT
    (SELECT count(*) FROM pg_catalog.pg_namespace object
      CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.nspacl,
        pg_catalog.acldefault('n',object.nspowner))) acl
      CROSS JOIN role_row WHERE acl.grantee=role_row.oid
        AND object.nspname='public' AND acl.privilege_type='USAGE'
        AND acl.grantor=object.nspowner AND NOT acl.is_grantable) +
    (SELECT count(*) FROM pg_catalog.pg_class object
      JOIN pg_catalog.pg_namespace namespace ON namespace.oid=object.relnamespace
      CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.relacl,
        pg_catalog.acldefault(CASE WHEN object.relkind='S' THEN 's'::"char"
          ELSE 'r'::"char" END,object.relowner))) acl
      CROSS JOIN role_row WHERE acl.grantee=role_row.oid
        AND namespace.nspname='public' AND acl.grantor=object.relowner
        AND NOT acl.is_grantable AND (
          (object.relname IN('workspace_memory_authorizations','workspace_memory_records',
            'workspace_memory_events','workspace_memory_receipts')
            AND acl.privilege_type IN('SELECT','INSERT')) OR
          (object.relname IN('workspace_memory_records_id_seq','workspace_memory_events_id_seq')
            AND acl.privilege_type IN('USAGE','SELECT')))) +
    (SELECT count(*) FROM pg_catalog.pg_attribute object
      JOIN pg_catalog.pg_class relation ON relation.oid=object.attrelid
      JOIN pg_catalog.pg_namespace namespace ON namespace.oid=relation.relnamespace
      CROSS JOIN LATERAL pg_catalog.aclexplode(object.attacl) acl
      CROSS JOIN role_row WHERE acl.grantee=role_row.oid
        AND namespace.nspname='public'
        AND relation.relname='workspace_memory_authorizations'
        AND object.attname='consumed_event_id' AND acl.privilege_type='UPDATE'
        AND acl.grantor=relation.relowner AND NOT acl.is_grantable),
    (SELECT count(*) FROM pg_catalog.pg_shdepend dependency CROSS JOIN role_row
      WHERE dependency.refclassid='pg_authid'::pg_catalog.regclass
        AND dependency.refobjid=role_row.oid AND dependency.deptype IN('a','o')
        AND (dependency.dbid=0 OR (dependency.dbid=(SELECT oid
          FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database()) AND NOT(
          (dependency.classid='pg_namespace'::pg_catalog.regclass
            AND dependency.objid='public'::pg_catalog.regnamespace AND dependency.objsubid=0) OR
          (dependency.classid='pg_class'::pg_catalog.regclass
            AND dependency.objid IN(
              'public.workspace_memory_authorizations'::pg_catalog.regclass,
              'public.workspace_memory_records'::pg_catalog.regclass,
              'public.workspace_memory_events'::pg_catalog.regclass,
              'public.workspace_memory_receipts'::pg_catalog.regclass,
              'public.workspace_memory_records_id_seq'::pg_catalog.regclass,
              'public.workspace_memory_events_id_seq'::pg_catalog.regclass)
            AND dependency.objsubid=0) OR
          (dependency.classid='pg_class'::pg_catalog.regclass
            AND dependency.objid='public.workspace_memory_authorizations'::pg_catalog.regclass
            AND dependency.objsubid=(SELECT attnum FROM pg_catalog.pg_attribute
              WHERE attrelid='public.workspace_memory_authorizations'::pg_catalog.regclass
                AND attname='consumed_event_id' AND NOT attisdropped)))))) +
    (SELECT count(*) FROM pg_catalog.pg_database object CROSS JOIN role_row
      CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.datacl,
        pg_catalog.acldefault('d',object.datdba))) acl WHERE acl.grantee=role_row.oid) +
    (SELECT count(*) FROM pg_catalog.pg_namespace object
      CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.nspacl,
        pg_catalog.acldefault('n',object.nspowner))) acl
      CROSS JOIN role_row WHERE acl.grantee=role_row.oid AND NOT(
        object.nspname='public' AND acl.privilege_type='USAGE'
        AND acl.grantor=object.nspowner AND NOT acl.is_grantable)) +
    (SELECT count(*) FROM pg_catalog.pg_class object
      JOIN pg_catalog.pg_namespace namespace ON namespace.oid=object.relnamespace
      CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.relacl,
        pg_catalog.acldefault(CASE WHEN object.relkind='S' THEN 's'::"char"
          ELSE 'r'::"char" END,object.relowner))) acl
      CROSS JOIN role_row WHERE acl.grantee=role_row.oid AND NOT(
        namespace.nspname='public' AND acl.grantor=object.relowner AND NOT acl.is_grantable AND (
          (object.relname IN('workspace_memory_authorizations','workspace_memory_records',
            'workspace_memory_events','workspace_memory_receipts')
            AND acl.privilege_type IN('SELECT','INSERT')) OR
          (object.relname IN('workspace_memory_records_id_seq','workspace_memory_events_id_seq')
            AND acl.privilege_type IN('USAGE','SELECT'))))) +
    (SELECT count(*) FROM pg_catalog.pg_attribute object
      JOIN pg_catalog.pg_class relation ON relation.oid=object.attrelid
      JOIN pg_catalog.pg_namespace namespace ON namespace.oid=relation.relnamespace
      CROSS JOIN LATERAL pg_catalog.aclexplode(object.attacl) acl
      CROSS JOIN role_row WHERE acl.grantee=role_row.oid AND NOT(
        namespace.nspname='public' AND relation.relname='workspace_memory_authorizations'
        AND object.attname='consumed_event_id' AND acl.privilege_type='UPDATE'
        AND acl.grantor=relation.relowner AND NOT acl.is_grantable)) +
    (SELECT count(*) FROM pg_catalog.pg_proc object CROSS JOIN role_row
      CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.proacl,
        pg_catalog.acldefault('f',object.proowner))) acl WHERE acl.grantee=role_row.oid) +
    (SELECT count(*) FROM pg_catalog.pg_default_acl object CROSS JOIN role_row
      CROSS JOIN LATERAL pg_catalog.aclexplode(object.defaclacl) acl
      WHERE acl.grantee=role_row.oid) +
    (SELECT count(*) FROM pg_catalog.pg_database object CROSS JOIN role_row
      WHERE object.datdba=role_row.oid) +
    (SELECT count(*) FROM pg_catalog.pg_namespace object CROSS JOIN role_row
      WHERE object.nspowner=role_row.oid) +
    (SELECT count(*) FROM pg_catalog.pg_class object CROSS JOIN role_row
      WHERE object.relowner=role_row.oid) +
    (SELECT count(*) FROM pg_catalog.pg_proc object CROSS JOIN role_row
      WHERE object.proowner=role_row.oid) +
    (SELECT count(*) FROM pg_catalog.pg_type object CROSS JOIN role_row
      WHERE object.typowner=role_row.oid)
  INTO required_acl_count,unexpected_authority_count;

  IF required_acl_count<>14 OR unexpected_authority_count<>0 THEN
    RAISE EXCEPTION '151: workspace memory ACL differs required=%/14 unexpected=%',
      required_acl_count,unexpected_authority_count USING ERRCODE='42501';
  END IF;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION assert_vane_workspace_memory_editor_v151(boolean) FROM PUBLIC;

-- Patch the rollback bridge by name. Old binaries legitimately replay v129;
-- they must temporarily remove the later edge before entering v098's frozen
-- exact-role validator, then restore precisely the edge they observed.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION provision_vane_server_runtime_v129() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE
  workspace_was_member boolean;
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
       AND rolconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::text[]
  ) OR NOT pg_has_role(current_user,'vane_memory_editor','SET') THEN
    RAISE EXCEPTION '129: memory editor role is absent or unsafe'
      USING ERRCODE='42501';
  END IF;
  PERFORM public.assert_vane_memory_editor_v129();
  SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_auth_members edge
    JOIN pg_catalog.pg_roles granted ON granted.oid=edge.roleid
    JOIN pg_catalog.pg_roles member ON member.oid=edge.member
    WHERE granted.rolname='vane_workspace_memory_editor'
      AND member.rolname='vane_server_runtime'
      AND NOT edge.admin_option AND NOT edge.inherit_option AND edge.set_option)
  INTO workspace_was_member;
  IF EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='vane_server_runtime') THEN
    PERFORM public.assert_vane_workspace_memory_editor_v151(workspace_was_member);
    REVOKE vane_workspace_memory_editor FROM vane_server_runtime;
    REVOKE vane_memory_editor FROM vane_server_runtime;
  ELSE
    PERFORM public.assert_vane_workspace_memory_editor_v151(false);
  END IF;
  PERFORM public.provision_vane_server_runtime_v128();
  IF EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='vane_server_runtime') THEN
    GRANT vane_memory_editor TO vane_server_runtime
      WITH ADMIN FALSE,SET TRUE,INHERIT FALSE;
    IF workspace_was_member THEN
      GRANT vane_workspace_memory_editor TO vane_server_runtime
        WITH ADMIN FALSE,SET TRUE,INHERIT FALSE;
    END IF;
  END IF;
  PERFORM public.assert_vane_memory_editor_v129();
  PERFORM public.assert_vane_workspace_memory_editor_v151(workspace_was_member);
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION provision_vane_server_runtime_v129() FROM PUBLIC;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION deprovision_vane_server_runtime_v129() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
  IF session_user<>current_user THEN
    RAISE EXCEPTION '129: only direct migration owner may deprovision runtime'
      USING ERRCODE='42501';
  END IF;
  IF EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='vane_server_runtime') THEN
    REVOKE vane_workspace_memory_editor FROM vane_server_runtime;
    REVOKE vane_memory_editor FROM vane_server_runtime;
  END IF;
  PERFORM public.deprovision_vane_server_runtime_v128();
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION deprovision_vane_server_runtime_v129() FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION provision_vane_server_runtime_v151() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
  IF session_user<>current_user THEN
    RAISE EXCEPTION '151: only direct migration owner may provision runtime'
      USING ERRCODE='42501';
  END IF;
  IF EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='vane_server_runtime') THEN
    REVOKE vane_workspace_memory_editor FROM vane_server_runtime;
  END IF;
  PERFORM public.assert_vane_workspace_memory_editor_v151(false);
  PERFORM public.provision_vane_server_runtime_v129();
  GRANT vane_workspace_memory_editor TO vane_server_runtime
    WITH ADMIN FALSE,SET TRUE,INHERIT FALSE;
  PERFORM public.assert_vane_workspace_memory_editor_v151(true);
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION provision_vane_server_runtime_v151() FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION deprovision_vane_server_runtime_v151() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
  IF session_user<>current_user THEN
    RAISE EXCEPTION '151: only direct migration owner may deprovision runtime'
      USING ERRCODE='42501';
  END IF;
  IF EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='vane_server_runtime') THEN
    REVOKE vane_workspace_memory_editor FROM vane_server_runtime;
  END IF;
  PERFORM public.deprovision_vane_server_runtime_v129();
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION deprovision_vane_server_runtime_v151() FROM PUBLIC;

-- Old v138 bridge binaries must receive the same strong transaction.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION provision_vane_server_runtime_v138() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
  IF session_user<>current_user THEN
    RAISE EXCEPTION '138: only direct migration owner may provision runtime'
      USING ERRCODE='42501';
  END IF;
  PERFORM public.provision_vane_server_runtime_v151();
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION provision_vane_server_runtime_v138() FROM PUBLIC;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION deprovision_vane_server_runtime_v138() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
  IF session_user<>current_user THEN
    RAISE EXCEPTION '138: only direct migration owner may deprovision runtime'
      USING ERRCODE='42501';
  END IF;
  PERFORM public.deprovision_vane_server_runtime_v151();
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION deprovision_vane_server_runtime_v138() FROM PUBLIC;

SELECT public.assert_vane_workspace_memory_editor_v151(false);

-- +goose Down

-- This migration changes cluster-global provision wrappers. Roll back only
-- after the runtime shell is explicitly deprovisioned.
-- +goose StatementBegin
DO $$
BEGIN
  IF EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='vane_server_runtime') THEN
    RAISE EXCEPTION '151: deprovision vane_server_runtime before schema downgrade';
  END IF;
END $$;
-- +goose StatementEnd

DROP FUNCTION deprovision_vane_server_runtime_v151();
DROP FUNCTION provision_vane_server_runtime_v151();
DROP FUNCTION assert_vane_workspace_memory_editor_v151(boolean);

-- Restore migration 138's historical wrappers for an exact schema-150 state.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION provision_vane_server_runtime_v138() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
  IF session_user<>current_user THEN
    RAISE EXCEPTION '138: only direct migration owner may provision runtime';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='vane_server_runtime') THEN
    REVOKE vane_workspace_memory_editor FROM vane_server_runtime;
  END IF;
  PERFORM public.provision_vane_server_runtime_v129();
  GRANT vane_workspace_memory_editor TO vane_server_runtime
    WITH ADMIN FALSE,SET TRUE,INHERIT FALSE;
  PERFORM public.assert_vane_workspace_memory_editor_v138();
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION provision_vane_server_runtime_v138() FROM PUBLIC;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION deprovision_vane_server_runtime_v138() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
  IF session_user<>current_user THEN
    RAISE EXCEPTION '138: only direct migration owner may deprovision runtime';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='vane_server_runtime') THEN
    REVOKE vane_workspace_memory_editor FROM vane_server_runtime;
  END IF;
  PERFORM public.deprovision_vane_server_runtime_v129();
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION deprovision_vane_server_runtime_v138() FROM PUBLIC;

-- Restore migration 129's historical wrappers as well.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION provision_vane_server_runtime_v129() RETURNS void
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
CREATE OR REPLACE FUNCTION deprovision_vane_server_runtime_v129() RETURNS void
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
