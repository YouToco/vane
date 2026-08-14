-- 132: physically close new retained V1 creation and protocol-1 edit writes.
--
-- This is an admission fence, not history deletion. Existing rows, receipts,
-- decoders and Temporal replay code remain intact until a later, independently
-- collected retention attestation proves they can be removed. Closing the DB
-- roots before the baseline clock prevents an old binary or recovery worker
-- from creating fresh legacy work during the retention interval.

-- +goose Up

-- Serialize before taking table locks. The v130 append path takes this same
-- advisory root before it reads any legacy table, so the order cannot invert.
SELECT pg_advisory_xact_lock(6215335020355474130);
LOCK TABLE agent_first_retention_attestation_events IN ACCESS EXCLUSIVE MODE;
LOCK TABLE task_creation_operations IN ACCESS EXCLUSIVE MODE;
LOCK TABLE task_creation_receipts IN ACCESS EXCLUSIVE MODE;
LOCK TABLE task_definition_edit_operations IN ACCESS EXCLUSIVE MODE;
LOCK TABLE task_definition_edit_receipts IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
DECLARE
    snapshot JSONB:=convert_from(
      public.agent_first_legacy_db_snapshot_v130(),'UTF8')::jsonb;
BEGIN
    IF COALESCE((snapshot#>>'{legacy_creation,active_count}')::bigint,-1)<>0 OR
       COALESCE((snapshot#>>'{legacy_creation,invalid_state_count}')::bigint,-1)<>0 OR
       COALESCE((snapshot#>>'{legacy_creation,lease_count}')::bigint,-1)<>0 OR
       COALESCE((snapshot#>>'{legacy_creation,pending_receipt_count}')::bigint,-1)<>0 OR
       COALESCE((snapshot#>>'{legacy_creation,receipt_lease_count}')::bigint,-1)<>0 OR
       COALESCE((snapshot#>>'{legacy_creation,receipt_gap_count}')::bigint,-1)<>0 OR
       COALESCE((snapshot#>>'{protocol1_definition_edit,active_count}')::bigint,-1)<>0 OR
       COALESCE((snapshot#>>'{protocol1_definition_edit,invalid_state_count}')::bigint,-1)<>0 OR
       COALESCE((snapshot#>>'{protocol1_definition_edit,lease_count}')::bigint,-1)<>0 OR
       COALESCE((snapshot#>>'{protocol1_definition_edit,pending_receipt_count}')::bigint,-1)<>0 OR
       COALESCE((snapshot#>>'{protocol1_definition_edit,receipt_lease_count}')::bigint,-1)<>0 OR
       COALESCE((snapshot#>>'{protocol1_definition_edit,receipt_gap_count}')::bigint,-1)<>0 THEN
        RAISE EXCEPTION '132: retained legacy protocol is not quiescent'
            USING ERRCODE='55000';
    END IF;
END
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION reject_legacy_creation_root_write_v132() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY INVOKER
SET search_path=pg_catalog,public AS $$
BEGIN
    IF (TG_OP='INSERT' AND NEW.execution_version=1 AND NEW.tool_name='create_schedule') OR
       (TG_OP='UPDATE' AND (
            (OLD.execution_version=1 AND OLD.tool_name='create_schedule') OR
            (NEW.execution_version=1 AND NEW.tool_name='create_schedule')
       )) THEN
        RAISE EXCEPTION '132: retained V1 creation writes are frozen'
            USING ERRCODE='55000';
    END IF;
    RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION reject_legacy_creation_root_write_v132() FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION reject_legacy_creation_receipt_write_v132() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY INVOKER
SET search_path=pg_catalog,public AS $$
DECLARE
    legacy_parent BOOLEAN:=false;
BEGIN
    IF TG_OP='UPDATE' THEN
      SELECT EXISTS (
        SELECT 1 FROM public.task_creation_operations operation
         WHERE operation.id=OLD.operation_id
           AND operation.execution_version=1 AND operation.tool_name='create_schedule'
      ) INTO legacy_parent;
    END IF;
    IF NOT legacy_parent THEN
      SELECT EXISTS (
        SELECT 1 FROM public.task_creation_operations operation
         WHERE operation.id=NEW.operation_id
           AND operation.execution_version=1 AND operation.tool_name='create_schedule'
      ) INTO legacy_parent;
    END IF;
    IF legacy_parent THEN
        RAISE EXCEPTION '132: retained V1 creation receipt writes are frozen'
            USING ERRCODE='55000';
    END IF;
    RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION reject_legacy_creation_receipt_write_v132() FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION reject_protocol1_edit_root_write_v132() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY INVOKER
SET search_path=pg_catalog,public AS $$
BEGIN
    IF (TG_OP='INSERT' AND NEW.operation_protocol=1) OR
       (TG_OP='UPDATE' AND (OLD.operation_protocol=1 OR NEW.operation_protocol=1)) THEN
        RAISE EXCEPTION '132: retained protocol-1 edit writes are frozen'
            USING ERRCODE='55000';
    END IF;
    RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION reject_protocol1_edit_root_write_v132() FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION reject_protocol1_edit_receipt_write_v132() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY INVOKER
SET search_path=pg_catalog,public AS $$
DECLARE
    legacy_parent BOOLEAN:=false;
BEGIN
    IF TG_OP='UPDATE' THEN
      SELECT EXISTS (
        SELECT 1 FROM public.task_definition_edit_operations operation
         WHERE operation.id=OLD.operation_id AND operation.operation_protocol=1
      ) INTO legacy_parent;
    END IF;
    IF NOT legacy_parent THEN
      SELECT EXISTS (
        SELECT 1 FROM public.task_definition_edit_operations operation
         WHERE operation.id=NEW.operation_id AND operation.operation_protocol=1
      ) INTO legacy_parent;
    END IF;
    IF legacy_parent THEN
        RAISE EXCEPTION '132: retained protocol-1 edit receipt writes are frozen'
            USING ERRCODE='55000';
    END IF;
    RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION reject_protocol1_edit_receipt_write_v132() FROM PUBLIC;

CREATE TRIGGER agent_first_legacy_creation_root_fence_v132
BEFORE INSERT OR UPDATE ON task_creation_operations
FOR EACH ROW EXECUTE FUNCTION reject_legacy_creation_root_write_v132();
ALTER TABLE task_creation_operations
    ENABLE ALWAYS TRIGGER agent_first_legacy_creation_root_fence_v132;

CREATE TRIGGER agent_first_legacy_creation_receipt_fence_v132
BEFORE INSERT OR UPDATE ON task_creation_receipts
FOR EACH ROW EXECUTE FUNCTION reject_legacy_creation_receipt_write_v132();
ALTER TABLE task_creation_receipts
    ENABLE ALWAYS TRIGGER agent_first_legacy_creation_receipt_fence_v132;

CREATE TRIGGER agent_first_protocol1_edit_root_fence_v132
BEFORE INSERT OR UPDATE ON task_definition_edit_operations
FOR EACH ROW EXECUTE FUNCTION reject_protocol1_edit_root_write_v132();
ALTER TABLE task_definition_edit_operations
    ENABLE ALWAYS TRIGGER agent_first_protocol1_edit_root_fence_v132;

CREATE TRIGGER agent_first_protocol1_edit_receipt_fence_v132
BEFORE INSERT OR UPDATE ON task_definition_edit_receipts
FOR EACH ROW EXECUTE FUNCTION reject_protocol1_edit_receipt_write_v132();
ALTER TABLE task_definition_edit_receipts
    ENABLE ALWAYS TRIGGER agent_first_protocol1_edit_receipt_fence_v132;

-- Normal runtime roles may advance V3 rows but cannot erase retained audit
-- roots. Tenant purge is an offline owner-only command and keeps owner DELETE.
REVOKE DELETE ON task_creation_operations FROM vane_app;
REVOKE DELETE ON task_creation_receipts FROM vane_app;
-- The restricted receipt dispatcher needs only this discriminator so its
-- SECURITY INVOKER trigger can classify V3 receipts without broadening reads.
GRANT SELECT (operation_protocol) ON task_definition_edit_operations
    TO vane_edit_receipt;

CREATE TABLE agent_first_legacy_protocol_write_fence_v132 (
    singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    installed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(installed_at)),
    preexisting_attestation_max_id BIGINT NOT NULL CHECK (
        preexisting_attestation_max_id>=0),
    descriptor_digest TEXT NOT NULL CHECK (
        descriptor_digest~'^[0-9a-f]{64}$')
);
REVOKE ALL ON agent_first_legacy_protocol_write_fence_v132 FROM PUBLIC,vane_app;

-- Once the physical fence epoch exists, every new retention event must pass
-- through the v132 snapshot-CAS wrapper below. This rejects an older collector
-- binary that still invokes the retained v130 append function directly.
-- +goose StatementBegin
CREATE FUNCTION require_fenced_retention_append_v132() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY INVOKER
SET search_path=pg_catalog,public AS $$
DECLARE
    expected_digest TEXT;
BEGIN
    SELECT descriptor_digest INTO expected_digest
      FROM public.agent_first_legacy_protocol_write_fence_v132
     WHERE singleton;
    IF expected_digest IS NULL OR
       current_setting('app.agent_first_retention_fence_v132',true)
          IS DISTINCT FROM expected_digest THEN
        RAISE EXCEPTION '132: retention evidence must use the fenced append path'
            USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION require_fenced_retention_append_v132() FROM PUBLIC;
CREATE TRIGGER agent_first_retention_append_fence_v132
BEFORE INSERT ON agent_first_retention_attestation_events
FOR EACH ROW EXECUTE FUNCTION require_fenced_retention_append_v132();
ALTER TABLE agent_first_retention_attestation_events
    ENABLE ALWAYS TRIGGER agent_first_retention_append_fence_v132;

-- +goose StatementBegin
CREATE FUNCTION reject_agent_first_legacy_write_fence_mutation_v132()
RETURNS TRIGGER LANGUAGE plpgsql SECURITY INVOKER
SET search_path=pg_catalog,public AS $$
BEGIN
    RAISE EXCEPTION '132: Agent-first legacy write fence epoch is immutable'
        USING ERRCODE='23514';
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION reject_agent_first_legacy_write_fence_mutation_v132() FROM PUBLIC;
CREATE TRIGGER agent_first_legacy_write_fence_immutable_v132
BEFORE UPDATE OR DELETE ON agent_first_legacy_protocol_write_fence_v132
FOR EACH ROW EXECUTE FUNCTION reject_agent_first_legacy_write_fence_mutation_v132();
CREATE TRIGGER agent_first_legacy_write_fence_no_truncate_v132
BEFORE TRUNCATE ON agent_first_legacy_protocol_write_fence_v132
FOR EACH STATEMENT EXECUTE FUNCTION reject_agent_first_legacy_write_fence_mutation_v132();
ALTER TABLE agent_first_legacy_protocol_write_fence_v132
    ENABLE ALWAYS TRIGGER agent_first_legacy_write_fence_immutable_v132;
ALTER TABLE agent_first_legacy_protocol_write_fence_v132
    ENABLE ALWAYS TRIGGER agent_first_legacy_write_fence_no_truncate_v132;

-- Canonical catalog authority for startup/collector drift detection. It binds
-- function bodies, owners, security/search-path/ACL metadata, trigger
-- definitions and the exact table/column grants used by the frozen protocol.
-- +goose StatementBegin
CREATE FUNCTION agent_first_legacy_write_fence_descriptor_v132()
RETURNS TEXT LANGUAGE sql STABLE SECURITY DEFINER
SET search_path=pg_catalog,public AS $$
WITH target_functions(name) AS (VALUES
  ('agent_first_legacy_db_snapshot_v130'),
  ('append_agent_first_retention_attestation_v130'),
  ('reject_agent_first_retention_history_mutation_v130'),
  ('reject_legacy_creation_root_write_v132'),
  ('reject_legacy_creation_receipt_write_v132'),
  ('reject_protocol1_edit_root_write_v132'),
  ('reject_protocol1_edit_receipt_write_v132'),
  ('require_fenced_retention_append_v132'),
  ('reject_agent_first_legacy_write_fence_mutation_v132'),
  ('agent_first_legacy_write_fence_descriptor_v132'),
  ('assert_agent_first_legacy_write_fence_v132'),
  ('append_agent_first_retention_attestation_v132')
), function_rows AS (
  SELECT function.proname,
         pg_get_userbyid(function.proowner) AS owner_name,
         function.prosecdef,
         COALESCE(array_to_string(function.proconfig,E'\x1f'),'') AS config,
         encode(sha256(convert_to(pg_get_functiondef(function.oid),'UTF8')),'hex') AS definition_digest,
         COALESCE((SELECT string_agg(
           (CASE WHEN acl.grantee=0 THEN 'PUBLIC'
                 ELSE acl.grantee::regrole::text END)||':'||
           acl.privilege_type||':'||acl.is_grantable::text,E'\x1f'
           ORDER BY acl.grantee,acl.privilege_type,acl.is_grantable)
           FROM aclexplode(COALESCE(function.proacl,
             acldefault('f',function.proowner))) acl),'') AS acl
    FROM target_functions expected
    JOIN pg_namespace namespace ON namespace.nspname='public'
    JOIN pg_proc function ON function.pronamespace=namespace.oid
                         AND function.proname=expected.name
), target_triggers(table_name,trigger_name) AS (VALUES
  ('agent_first_retention_attestation_events','agent_first_retention_history_immutable_v130'),
  ('agent_first_retention_attestation_events','agent_first_retention_history_no_truncate_v130'),
  ('task_creation_operations','agent_first_legacy_creation_root_fence_v132'),
  ('task_creation_receipts','agent_first_legacy_creation_receipt_fence_v132'),
  ('task_definition_edit_operations','agent_first_protocol1_edit_root_fence_v132'),
  ('task_definition_edit_receipts','agent_first_protocol1_edit_receipt_fence_v132'),
  ('agent_first_retention_attestation_events','agent_first_retention_append_fence_v132'),
  ('agent_first_legacy_protocol_write_fence_v132','agent_first_legacy_write_fence_immutable_v132'),
  ('agent_first_legacy_protocol_write_fence_v132','agent_first_legacy_write_fence_no_truncate_v132')
), trigger_rows AS (
  SELECT expected.table_name,expected.trigger_name,trigger.tgenabled,trigger.tgtype,
         function.proname,
         encode(sha256(convert_to(pg_get_triggerdef(trigger.oid,false),'UTF8')),'hex')
           AS definition_digest
    FROM target_triggers expected
    JOIN pg_namespace namespace ON namespace.nspname='public'
    JOIN pg_class relation ON relation.relnamespace=namespace.oid
                          AND relation.relname=expected.table_name
    JOIN pg_trigger trigger ON trigger.tgrelid=relation.oid
                           AND trigger.tgname=expected.trigger_name
                           AND NOT trigger.tgisinternal
    JOIN pg_proc function ON function.oid=trigger.tgfoid
), target_relations(name) AS (VALUES
  ('task_creation_operations'),('task_creation_receipts'),
  ('task_definition_edit_operations'),('task_definition_edit_receipts'),
  ('schedules'),('research_v3_delivery_authorities'),
  ('agent_first_retention_attestation_events'),
  ('agent_first_legacy_protocol_write_fence_v132')
), relation_rows AS (
  SELECT relation.relname,pg_get_userbyid(relation.relowner) AS owner_name,
         relation.relrowsecurity,relation.relforcerowsecurity,
         COALESCE((SELECT string_agg(
           (CASE WHEN acl.grantee=0 THEN 'PUBLIC'
                 ELSE acl.grantee::regrole::text END)||':'||
           acl.privilege_type||':'||acl.is_grantable::text,E'\x1f'
           ORDER BY acl.grantee,acl.privilege_type,acl.is_grantable)
           FROM aclexplode(COALESCE(relation.relacl,
             acldefault('r',relation.relowner))) acl),'') AS acl
    FROM target_relations expected
    JOIN pg_namespace namespace ON namespace.nspname='public'
    JOIN pg_class relation ON relation.relnamespace=namespace.oid
                          AND relation.relname=expected.name
), column_rows AS (
  SELECT relation.relname,attribute.attname,
         CASE WHEN attribute.attacl IS NULL THEN '' ELSE COALESCE((SELECT string_agg(
           (CASE WHEN acl.grantee=0 THEN 'PUBLIC'
                 ELSE acl.grantee::regrole::text END)||':'||
           acl.privilege_type||':'||acl.is_grantable::text,E'\x1f'
           ORDER BY acl.grantee,acl.privilege_type,acl.is_grantable)
           FROM aclexplode(attribute.attacl) acl),'') END AS acl
    FROM target_relations expected
    JOIN pg_namespace namespace ON namespace.nspname='public'
    JOIN pg_class relation ON relation.relnamespace=namespace.oid
                          AND relation.relname=expected.name
    JOIN pg_attribute attribute ON attribute.attrelid=relation.oid
                               AND attribute.attnum>0
                               AND NOT attribute.attisdropped
), policy_rows AS (
  SELECT relation.relname,policy.polname,policy.polcmd,
         policy.polpermissive,
         COALESCE(array_to_string(ARRAY(
           SELECT role::regrole::text FROM unnest(policy.polroles) role
            ORDER BY role::regrole::text),E'\x1f'),'') AS roles,
         COALESCE(pg_get_expr(policy.polqual,policy.polrelid,false),'') AS using_expr,
         COALESCE(pg_get_expr(policy.polwithcheck,policy.polrelid,false),'') AS check_expr
    FROM pg_namespace namespace
    JOIN pg_class relation ON relation.relnamespace=namespace.oid
                          AND relation.relname IN (
                            'task_creation_operations','task_creation_receipts',
                            'task_definition_edit_operations','task_definition_edit_receipts',
                            'schedules','research_v3_delivery_authorities')
    JOIN pg_policy policy ON policy.polrelid=relation.oid
   WHERE namespace.nspname='public'
), canonical(payload) AS (
  SELECT 'functions='||COALESCE((SELECT string_agg(
      proname||'|'||owner_name||'|'||prosecdef::text||'|'||config||'|'||
      definition_digest||'|'||acl,E'\x1e' ORDER BY proname) FROM function_rows),'')||
    E'\x1dtriggers='||COALESCE((SELECT string_agg(
      table_name||'|'||trigger_name||'|'||tgenabled::text||'|'||tgtype::text||'|'||
      proname||'|'||definition_digest,E'\x1e' ORDER BY table_name,trigger_name)
      FROM trigger_rows),'')||
    E'\x1drelations='||COALESCE((SELECT string_agg(
      relname||'|'||owner_name||'|'||relrowsecurity::text||'|'||
      relforcerowsecurity::text||'|'||acl,E'\x1e' ORDER BY relname)
      FROM relation_rows),'')||
    E'\x1dcolumns='||COALESCE((SELECT string_agg(
      relname||'|'||attname||'|'||acl,E'\x1e' ORDER BY relname,attname)
      FROM column_rows),'')||
    E'\x1dpolicies='||COALESCE((SELECT string_agg(
      relname||'|'||polname||'|'||polcmd::text||'|'||polpermissive::text||'|'||
      roles||'|'||using_expr||'|'||check_expr,E'\x1e' ORDER BY relname,polname)
      FROM policy_rows),'')
)
SELECT encode(sha256(convert_to(payload,'UTF8')),'hex') FROM canonical
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION agent_first_legacy_write_fence_descriptor_v132() FROM PUBLIC;

-- The offline collector calls this immediately before every evidence scan and
-- append. It catches a disabled/replaced trigger instead of silently treating
-- an unfenced retention interval as authoritative.
-- +goose StatementBegin
CREATE FUNCTION assert_agent_first_legacy_write_fence_v132()
RETURNS TABLE(installed_at TIMESTAMPTZ,
              preexisting_attestation_max_id BIGINT,
              descriptor_digest TEXT)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public AS $$
DECLARE
    matched BIGINT;
    actual_descriptor TEXT;
BEGIN
    IF encode(sha256(convert_to(pg_get_functiondef(
          'public.agent_first_legacy_write_fence_descriptor_v132()'::regprocedure),
          'UTF8')),'hex')<>'80c91011df44a29d84f4ed2760153921eb8addd7592dedb0f08e145eed403f4d' THEN
        RAISE EXCEPTION '132: Agent-first legacy write fence verifier drifted'
            USING ERRCODE='55000';
    END IF;
    SELECT count(*) INTO matched
      FROM (VALUES
        ('agent_first_retention_attestation_events','agent_first_retention_history_immutable_v130',
         'reject_agent_first_retention_history_mutation_v130',27),
        ('agent_first_retention_attestation_events','agent_first_retention_history_no_truncate_v130',
         'reject_agent_first_retention_history_mutation_v130',34),
        ('task_creation_operations','agent_first_legacy_creation_root_fence_v132',
         'reject_legacy_creation_root_write_v132',23),
        ('task_creation_receipts','agent_first_legacy_creation_receipt_fence_v132',
         'reject_legacy_creation_receipt_write_v132',23),
        ('task_definition_edit_operations','agent_first_protocol1_edit_root_fence_v132',
         'reject_protocol1_edit_root_write_v132',23),
        ('task_definition_edit_receipts','agent_first_protocol1_edit_receipt_fence_v132',
         'reject_protocol1_edit_receipt_write_v132',23),
        ('agent_first_retention_attestation_events','agent_first_retention_append_fence_v132',
         'require_fenced_retention_append_v132',7),
        ('agent_first_legacy_protocol_write_fence_v132','agent_first_legacy_write_fence_immutable_v132',
         'reject_agent_first_legacy_write_fence_mutation_v132',27),
        ('agent_first_legacy_protocol_write_fence_v132','agent_first_legacy_write_fence_no_truncate_v132',
         'reject_agent_first_legacy_write_fence_mutation_v132',34)
      ) expected(table_name,trigger_name,function_name,trigger_type)
      JOIN pg_namespace namespace ON namespace.nspname='public'
      JOIN pg_class relation ON relation.relnamespace=namespace.oid
                            AND relation.relname=expected.table_name
      JOIN pg_trigger trigger ON trigger.tgrelid=relation.oid
                             AND trigger.tgname=expected.trigger_name
                             AND NOT trigger.tgisinternal
                             AND trigger.tgenabled='A'
                             AND trigger.tgtype=expected.trigger_type
      JOIN pg_proc function ON function.oid=trigger.tgfoid
                           AND function.pronamespace=namespace.oid
                           AND function.proname=expected.function_name
                           AND NOT function.prosecdef;
    actual_descriptor:=public.agent_first_legacy_write_fence_descriptor_v132();
    IF matched<>9 OR (SELECT count(*) FROM
          public.agent_first_legacy_protocol_write_fence_v132)<>1 OR EXISTS (
        SELECT 1 FROM public.agent_first_legacy_protocol_write_fence_v132 fence
         WHERE fence.descriptor_digest<>actual_descriptor) THEN
        RAISE EXCEPTION '132: Agent-first legacy write fence is incomplete'
            USING ERRCODE='55000';
    END IF;
    RETURN QUERY SELECT fence.installed_at,fence.preexisting_attestation_max_id,
                        fence.descriptor_digest
      FROM public.agent_first_legacy_protocol_write_fence_v132 fence
     WHERE fence.singleton;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION assert_agent_first_legacy_write_fence_v132() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION assert_agent_first_legacy_write_fence_v132() TO vane_app;

-- CAS the exact DB snapshot under read locks before delegating canonical row
-- construction to the retained v130 function. The external Temporal side is
-- fenced by the VPS maintenance lock and stopped service; this closes the DB
-- half without changing v130 history bytes.
-- +goose StatementBegin
CREATE FUNCTION append_agent_first_retention_attestation_v132(
    requested_phase TEXT,requested_parent_digest TEXT,
    requested_temporal_cluster_id TEXT,requested_temporal_namespace TEXT,
    requested_temporal_namespace_id TEXT,requested_retention_seconds BIGINT,
    requested_history_archival_state TEXT,requested_history_archive_uri_digest TEXT,
    requested_visibility_archival_state TEXT,requested_visibility_archive_uri_digest TEXT,
    requested_temporal_server_witness TIMESTAMPTZ,
    requested_workflow_inventory_digest TEXT,requested_schedule_inventory_digest TEXT,
    requested_archive_inventory_digest TEXT,requested_temporal_evidence_digest TEXT,
    requested_source_revision TEXT,requested_deploy_digest TEXT,
    requested_expected_legacy_db_snapshot_digest TEXT
) RETURNS SETOF agent_first_retention_attestation_events
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE
    fence public.agent_first_legacy_protocol_write_fence_v132%ROWTYPE;
    actual_snapshot_digest TEXT;
    parent_id BIGINT;
BEGIN
    IF session_user<>current_user THEN
        RAISE EXCEPTION '132: only direct schema owner may append retention evidence'
            USING ERRCODE='42501';
    END IF;
    PERFORM pg_advisory_xact_lock(6215335020355474130);
    -- Schedule/action authority is the only live V3 data in this evidence.
    -- Production writers use both schedule->authority and authority->schedule
    -- orders. Never wait while holding one side: a busy live writer makes this
    -- observation fail closed and the whole transaction releases immediately.
    LOCK TABLE schedules,research_v3_delivery_authorities IN SHARE MODE NOWAIT;
    LOCK TABLE agent_first_retention_attestation_events,
      task_creation_operations,task_creation_receipts,
      task_definition_edit_operations,task_definition_edit_receipts
      IN ACCESS SHARE MODE;
    PERFORM * FROM public.assert_agent_first_legacy_write_fence_v132();
    SELECT * INTO fence FROM public.agent_first_legacy_protocol_write_fence_v132
     WHERE singleton FOR SHARE;
    actual_snapshot_digest:=encode(sha256(
      public.agent_first_legacy_db_snapshot_v130()),'hex');
    IF requested_expected_legacy_db_snapshot_digest !~ '^[0-9a-f]{64}$' OR
       actual_snapshot_digest<>requested_expected_legacy_db_snapshot_digest THEN
        RAISE EXCEPTION '132: expected Agent-first database snapshot drifted'
            USING ERRCODE='40001';
    END IF;
    IF requested_phase='prepared' THEN
        SELECT id INTO parent_id FROM agent_first_retention_attestation_events
         WHERE payload_digest=requested_parent_digest AND phase='baseline';
        IF parent_id IS NULL OR parent_id<=fence.preexisting_attestation_max_id THEN
            RAISE EXCEPTION '132: prepared parent predates legacy write fence'
                USING ERRCODE='23514';
        END IF;
    END IF;
    PERFORM set_config('app.agent_first_retention_fence_v132',
                       fence.descriptor_digest,true);
    RETURN QUERY SELECT * FROM public.append_agent_first_retention_attestation_v130(
      requested_phase,requested_parent_digest,requested_temporal_cluster_id,
      requested_temporal_namespace,requested_temporal_namespace_id,
      requested_retention_seconds,requested_history_archival_state,
      requested_history_archive_uri_digest,requested_visibility_archival_state,
      requested_visibility_archive_uri_digest,requested_temporal_server_witness,
      requested_workflow_inventory_digest,requested_schedule_inventory_digest,
      requested_archive_inventory_digest,requested_temporal_evidence_digest,
      requested_source_revision,requested_deploy_digest);
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION append_agent_first_retention_attestation_v132(
 text,text,text,text,text,bigint,text,text,text,text,timestamptz,text,text,text,
 text,text,text,text) FROM PUBLIC,vane_app;

INSERT INTO agent_first_legacy_protocol_write_fence_v132(
    singleton,preexisting_attestation_max_id,descriptor_digest)
SELECT true,COALESCE(max(id),0),
       public.agent_first_legacy_write_fence_descriptor_v132()
  FROM agent_first_retention_attestation_events;

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474130);
LOCK TABLE agent_first_retention_attestation_events IN ACCESS EXCLUSIVE MODE;
LOCK TABLE task_creation_operations IN ACCESS EXCLUSIVE MODE;
LOCK TABLE task_creation_receipts IN ACCESS EXCLUSIVE MODE;
LOCK TABLE task_definition_edit_operations IN ACCESS EXCLUSIVE MODE;
LOCK TABLE task_definition_edit_receipts IN ACCESS EXCLUSIVE MODE;
LOCK TABLE agent_first_legacy_protocol_write_fence_v132 IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
DECLARE
    cutoff BIGINT;
BEGIN
    SELECT preexisting_attestation_max_id INTO cutoff
      FROM agent_first_legacy_protocol_write_fence_v132 WHERE singleton;
    IF EXISTS (SELECT 1 FROM agent_first_retention_attestation_events
                WHERE id>cutoff) THEN
        RAISE EXCEPTION '132 down refused: retention evidence depends on the legacy write fence';
    END IF;
END
$$;
-- +goose StatementEnd

DROP TRIGGER agent_first_protocol1_edit_receipt_fence_v132
    ON task_definition_edit_receipts;
DROP TRIGGER agent_first_protocol1_edit_root_fence_v132
    ON task_definition_edit_operations;
DROP TRIGGER agent_first_legacy_creation_receipt_fence_v132
    ON task_creation_receipts;
DROP TRIGGER agent_first_legacy_creation_root_fence_v132
    ON task_creation_operations;
REVOKE SELECT (operation_protocol) ON task_definition_edit_operations
    FROM vane_edit_receipt;
GRANT DELETE ON task_creation_operations TO vane_app;
GRANT DELETE ON task_creation_receipts TO vane_app;
DROP FUNCTION append_agent_first_retention_attestation_v132(
 text,text,text,text,text,bigint,text,text,text,text,timestamptz,text,text,text,
 text,text,text,text);
DROP TRIGGER agent_first_retention_append_fence_v132
    ON agent_first_retention_attestation_events;
DROP FUNCTION require_fenced_retention_append_v132();
REVOKE EXECUTE ON FUNCTION assert_agent_first_legacy_write_fence_v132()
    FROM vane_app;
DROP FUNCTION assert_agent_first_legacy_write_fence_v132();
DROP FUNCTION agent_first_legacy_write_fence_descriptor_v132();
DROP TRIGGER agent_first_legacy_write_fence_no_truncate_v132
    ON agent_first_legacy_protocol_write_fence_v132;
DROP TRIGGER agent_first_legacy_write_fence_immutable_v132
    ON agent_first_legacy_protocol_write_fence_v132;
DROP FUNCTION reject_agent_first_legacy_write_fence_mutation_v132();
DROP TABLE agent_first_legacy_protocol_write_fence_v132;
DROP FUNCTION reject_protocol1_edit_receipt_write_v132();
DROP FUNCTION reject_protocol1_edit_root_write_v132();
DROP FUNCTION reject_legacy_creation_receipt_write_v132();
DROP FUNCTION reject_legacy_creation_root_write_v132();
