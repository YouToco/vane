-- 128: physically retire the feedback-to-session continuation outbox.
--
-- This migration deliberately does not freeze retained V1 task creation or
-- protocol-1 definition edits. Those lanes are coupled to Temporal retention
-- and require a separately persisted, deployment-bound attestation before a
-- later migration may make them read-only. The session-fact outbox has no
-- production consumer after migration 113 / PR #331; this migration keeps its
-- terminal rows for audit and tenant purge while removing every writer and
-- the obsolete projector capability from the server runtime.

-- +goose Up

LOCK TABLE agent_session_fact_outbox IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM agent_session_fact_outbox
         WHERE status='pending' OR lease_owner IS NOT NULL OR
               lease_expires_at IS NOT NULL
    ) THEN
        RAISE EXCEPTION '128: live Agent session fact prevents physical retirement';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles
         WHERE rolname='vane_agent_session_fact_projector'
           AND NOT rolcanlogin AND NOT rolsuper AND NOT rolcreatedb
           AND NOT rolcreaterole AND NOT rolinherit AND NOT rolreplication
           AND NOT rolbypassrls
           AND rolconfig=ARRAY['search_path=pg_catalog, public']::TEXT[]
    ) THEN
        RAISE EXCEPTION '128: retired projector role is absent or unsafe'
            USING ERRCODE='42501';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_auth_members edge
        JOIN pg_catalog.pg_roles granted ON granted.oid=edge.roleid
        JOIN pg_catalog.pg_roles member ON member.oid=edge.member
        WHERE (granted.rolname='vane_agent_session_fact_projector'
               AND member.rolname NOT IN (current_user,'vane_server_runtime'))
           OR member.rolname='vane_agent_session_fact_projector'
    ) THEN
        RAISE EXCEPTION '128: retired projector role membership drift'
            USING ERRCODE='42501';
    END IF;
END
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION reject_retired_session_fact_v128()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path=pg_catalog,public
AS $$
BEGIN
    RAISE EXCEPTION '128: Agent session fact continuation is physically retired'
        USING ERRCODE='23514';
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION reject_retired_session_fact_v128() FROM PUBLIC;

CREATE TRIGGER agent_session_fact_outbox_retired_v128
BEFORE INSERT OR UPDATE ON agent_session_fact_outbox
FOR EACH ROW EXECUTE FUNCTION reject_retired_session_fact_v128();

REVOKE USAGE ON SEQUENCE agent_session_fact_outbox_id_seq FROM vane_app;
REVOKE INSERT (
    tenant_id,user_id,fact_type,fact_id,source_identity,session_id,
    session_messages,payload_digest,status,suppression_reason
) ON agent_session_fact_outbox FROM vane_app;

REVOKE UPDATE (
    status,lease_owner,lease_fence,lease_expires_at,attempt_count,
    next_attempt_at,session_recorded_at,blocked_reason,updated_at
) ON agent_session_fact_outbox FROM vane_agent_session_fact_projector;
REVOKE SELECT ON agent_session_fact_outbox
    FROM vane_agent_session_fact_projector;
REVOKE SELECT ON agent_session_projection_authority_events
    FROM vane_agent_session_fact_projector;
REVOKE USAGE ON SEQUENCE agent_events_id_seq
    FROM vane_agent_session_fact_projector;
REVOKE INSERT (
    tenant_id,user_id,session_id,sequence,batch_idempotency_key,
    batch_index,batch_size,kind,schema_version,payload,payload_digest,batch_digest
) ON agent_events FROM vane_agent_session_fact_projector;
REVOKE SELECT ON agent_events FROM vane_agent_session_fact_projector;
REVOKE UPDATE (messages) ON agent_sessions
    FROM vane_agent_session_fact_projector;
REVOKE SELECT (id,tenant_id,user_id,messages,turn_count,activated_tools)
    ON agent_sessions FROM vane_agent_session_fact_projector;
REVOKE USAGE ON SCHEMA public FROM vane_agent_session_fact_projector;

-- Provisioning migration 098 is immutable and still grants the retired role.
-- Current binaries call this versioned wrapper so grant+retire occurs in one
-- owner transaction and the connection probe sees the new exact role set.
-- +goose StatementBegin
CREATE FUNCTION retire_agent_session_fact_projector_v128() RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE owner_name TEXT:=current_user; role_oid OID; direct_grants BIGINT;
BEGIN
    IF session_user<>owner_name THEN
        RAISE EXCEPTION '128: only the direct migration owner may retire projector'
            USING ERRCODE='42501';
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock(6215335020355474248);
    SELECT oid INTO role_oid FROM pg_catalog.pg_roles
     WHERE rolname='vane_agent_session_fact_projector'
       AND NOT rolcanlogin AND NOT rolsuper AND NOT rolcreatedb
       AND NOT rolcreaterole AND NOT rolinherit AND NOT rolreplication
       AND NOT rolbypassrls
       AND rolconfig=ARRAY['search_path=pg_catalog, public']::TEXT[];
    IF role_oid IS NULL THEN
        RAISE EXCEPTION '128: retired projector role is absent or unsafe'
            USING ERRCODE='42501';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members edge
        JOIN pg_catalog.pg_roles granted ON granted.oid=edge.roleid
        JOIN pg_catalog.pg_roles member ON member.oid=edge.member
        WHERE (granted.oid=role_oid AND member.rolname NOT IN
                   (owner_name,'vane_server_runtime')) OR member.oid=role_oid) THEN
        RAISE EXCEPTION '128: retired projector role membership drift'
            USING ERRCODE='42501';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='vane_server_runtime') THEN
        EXECUTE 'REVOKE vane_agent_session_fact_projector FROM vane_server_runtime';
    END IF;
    SELECT
      -- pg_shdepend is the authority index for this database plus shared
      -- cluster objects.  It catches ACL/ownership classes omitted by local
      -- relation scans (types, FDWs, parameters, and database CONNECT,
      -- including CONNECT on another database).  Dependencies owned by a
      -- different database are deliberately outside this per-database schema
      -- transition: an older database in the same cluster may still be at
      -- schema 127, and ordinary migration/provision tests exercise that
      -- coexistence concurrently.
      (SELECT count(*) FROM pg_catalog.pg_shdepend dependency
       WHERE dependency.refclassid='pg_authid'::pg_catalog.regclass
         AND dependency.refobjid=role_oid
         AND dependency.deptype IN ('a','o')
         AND (dependency.dbid=0 OR dependency.dbid=(
             SELECT oid FROM pg_catalog.pg_database
              WHERE datname=pg_catalog.current_database()
         ))) +
      (SELECT count(*) FROM pg_catalog.pg_auth_members edge
       JOIN pg_catalog.pg_roles member ON member.oid=edge.member
       WHERE (edge.roleid=role_oid AND member.rolname<>owner_name)
          OR edge.member=role_oid) +
      (SELECT count(*) FROM pg_catalog.pg_namespace object
       CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.nspacl,
           pg_catalog.acldefault('n',object.nspowner))) acl
       WHERE acl.grantee=role_oid) +
      (SELECT count(*) FROM pg_catalog.pg_class object
       CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.relacl,
           pg_catalog.acldefault(CASE WHEN object.relkind='S' THEN 's'::"char"
                                     ELSE 'r'::"char" END,object.relowner))) acl
       WHERE acl.grantee=role_oid) +
      (SELECT count(*) FROM pg_catalog.pg_attribute object
       CROSS JOIN LATERAL pg_catalog.aclexplode(object.attacl) acl
       WHERE acl.grantee=role_oid) +
      (SELECT count(*) FROM pg_catalog.pg_proc object
       CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.proacl,
           pg_catalog.acldefault('f',object.proowner))) acl
       WHERE acl.grantee=role_oid) +
      (SELECT count(*) FROM pg_catalog.pg_default_acl object
       CROSS JOIN LATERAL pg_catalog.aclexplode(object.defaclacl) acl
       WHERE acl.grantee=role_oid) +
      (SELECT count(*) FROM pg_catalog.pg_database WHERE datdba=role_oid) +
      (SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspowner=role_oid) +
      (SELECT count(*) FROM pg_catalog.pg_class WHERE relowner=role_oid) +
      (SELECT count(*) FROM pg_catalog.pg_proc WHERE proowner=role_oid) +
      (SELECT count(*) FROM pg_catalog.pg_type WHERE typowner=role_oid)
      INTO direct_grants;
    IF direct_grants<>0 THEN
        RAISE EXCEPTION '128: retired projector retains % direct authorities',direct_grants
            USING ERRCODE='42501';
    END IF;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION retire_agent_session_fact_projector_v128() FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION restore_agent_session_fact_projector_v128() RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE owner_name TEXT:=current_user;
BEGIN
    IF session_user<>owner_name THEN
        RAISE EXCEPTION '128: only the direct migration owner may restore projector'
            USING ERRCODE='42501';
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock(6215335020355474248);
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='vane_server_runtime') THEN
        EXECUTE 'GRANT vane_agent_session_fact_projector TO vane_server_runtime';
    END IF;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION restore_agent_session_fact_projector_v128() FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION provision_vane_server_runtime_v128() RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
    PERFORM public.provision_vane_server_runtime_v1();
    PERFORM public.provision_vane_server_runtime_research_binder_v1();
    PERFORM public.retire_agent_session_fact_projector_v128();
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION provision_vane_server_runtime_v128() FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION deprovision_vane_server_runtime_v128() RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
    PERFORM public.restore_agent_session_fact_projector_v128();
    PERFORM public.deprovision_vane_server_runtime_research_binder_v1();
    PERFORM public.deprovision_vane_server_runtime_v1();
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION deprovision_vane_server_runtime_v128() FROM PUBLIC;

-- +goose Down

LOCK TABLE agent_session_fact_outbox IN ACCESS EXCLUSIVE MODE;

-- Cluster-global memberships are changed only by the explicit provisioner.
-- A downgrade must first deprovision the runtime while this wrapper exists.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles
                WHERE rolname='vane_server_runtime') THEN
        RAISE EXCEPTION '128: deprovision vane_server_runtime before schema downgrade';
    END IF;
END
$$;
-- +goose StatementEnd

-- Reuse the same exact role/membership/ACL/ownership validator before
-- restoring legacy per-database grants. With no runtime role present this is
-- read-only and prevents a downgrade from blessing post-retirement drift.
SELECT retire_agent_session_fact_projector_v128();

DROP FUNCTION deprovision_vane_server_runtime_v128();
DROP FUNCTION provision_vane_server_runtime_v128();
DROP FUNCTION restore_agent_session_fact_projector_v128();
DROP FUNCTION retire_agent_session_fact_projector_v128();

DROP TRIGGER IF EXISTS agent_session_fact_outbox_retired_v128
    ON agent_session_fact_outbox;
DROP FUNCTION IF EXISTS reject_retired_session_fact_v128();

GRANT INSERT (
    tenant_id,user_id,fact_type,fact_id,source_identity,session_id,
    session_messages,payload_digest,status,suppression_reason
) ON agent_session_fact_outbox TO vane_app;
GRANT USAGE ON SEQUENCE agent_session_fact_outbox_id_seq TO vane_app;

GRANT USAGE ON SCHEMA public TO vane_agent_session_fact_projector;
GRANT SELECT (id,tenant_id,user_id,messages,turn_count,activated_tools)
    ON agent_sessions TO vane_agent_session_fact_projector;
GRANT UPDATE (messages) ON agent_sessions
    TO vane_agent_session_fact_projector;
GRANT SELECT ON agent_events TO vane_agent_session_fact_projector;
GRANT INSERT (
    tenant_id,user_id,session_id,sequence,batch_idempotency_key,
    batch_index,batch_size,kind,schema_version,payload,payload_digest,batch_digest
) ON agent_events TO vane_agent_session_fact_projector;
GRANT USAGE ON SEQUENCE agent_events_id_seq
    TO vane_agent_session_fact_projector;
GRANT SELECT ON agent_session_projection_authority_events
    TO vane_agent_session_fact_projector;
GRANT SELECT ON agent_session_fact_outbox
    TO vane_agent_session_fact_projector;
GRANT UPDATE (
    status,lease_owner,lease_fence,lease_expires_at,attempt_count,
    next_attempt_at,session_recorded_at,blocked_reason,updated_at
) ON agent_session_fact_outbox TO vane_agent_session_fact_projector;
