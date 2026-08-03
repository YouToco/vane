-- 109: native V3 task creation persistence checkpoints.
--
-- Version 1 remains the byte-for-byte retained compiled-task saga. Version 2
-- is admitted only for the new V3 protocol and receives narrowly scoped
-- SECURITY DEFINER commits so the restricted server role cannot construct an
-- enabled authority or partially publish a task with ad-hoc table writes.

-- +goose Up

ALTER TABLE task_creation_operations
    DROP CONSTRAINT task_creation_operations_execution_version_current;
ALTER TABLE task_creation_operations
    ADD CONSTRAINT task_creation_operations_execution_version_current
    CHECK (execution_version IN (1,2));
ALTER TABLE task_creation_operations
    ADD CONSTRAINT task_creation_operations_protocol_tool_binding
    CHECK ((execution_version=1 AND tool_name='create_schedule') OR
           (execution_version=2 AND tool_name='manage_tasks'));

DROP INDEX idx_task_creation_operations_stale;
CREATE INDEX idx_task_creation_operations_stale
    ON task_creation_operations (tenant_id,takeover_not_before,id)
    WHERE ((execution_version=1 AND tool_name='create_schedule') OR
           (execution_version=2 AND tool_name='manage_tasks'))
      AND status='executing'
      AND tombstoned_at IS NULL;

-- Only the migration/session owner may enter the native creation coordinator
-- during migration. vane_app and the long-lived server runtime never receive
-- membership or direct function execution. Native creation runs only through
-- an owner-compatible primary Store whose direct schema owner may SET ROLE for
-- the four exact atomic functions below.
-- +goose StatementBegin
DO $$
DECLARE membership RECORD;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles
                    WHERE rolname='vane_native_v3_creation_coordinator') THEN
        BEGIN
            CREATE ROLE vane_native_v3_creation_coordinator
                NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
                NOREPLICATION NOBYPASSRLS;
        EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL;
        END;
    END IF;
    ALTER ROLE vane_native_v3_creation_coordinator
        NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
        NOREPLICATION NOBYPASSRLS;
    FOR membership IN
        SELECT granted.rolname AS granted_name,member.rolname AS member_name
          FROM pg_auth_members edge
          JOIN pg_roles granted ON granted.oid=edge.roleid
          JOIN pg_roles member ON member.oid=edge.member
         WHERE (granted.rolname='vane_native_v3_creation_coordinator'
                AND member.rolname<>current_user)
            OR member.rolname='vane_native_v3_creation_coordinator'
    LOOP
        EXECUTE format('REVOKE %I FROM %I',
                       membership.granted_name,membership.member_name);
    END LOOP;
    EXECUTE format('GRANT vane_native_v3_creation_coordinator TO %I',current_user);
END $$;
-- +goose StatementEnd

GRANT USAGE ON SCHEMA public TO vane_native_v3_creation_coordinator;

-- Migration 098 owns the original exact server shell. This versioned
-- successor keeps that boundary byte-for-byte exact. Native V3 creation is
-- deliberately not a long-lived server-runtime capability.
-- +goose StatementBegin
CREATE FUNCTION provision_vane_server_runtime_v2() RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE owner_name TEXT:=current_user; direct_roles TEXT[]; expected_roles TEXT[];
        reverse_members TEXT[];
BEGIN
    IF session_user<>owner_name THEN
        RAISE EXCEPTION '109: only the direct migration owner may provision server runtime'
            USING ERRCODE='42501';
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock(6215335020355474248);
    PERFORM public.provision_vane_server_runtime_v1();

    SELECT pg_catalog.array_agg(granted.rolname ORDER BY granted.rolname)
      INTO direct_roles
      FROM pg_catalog.pg_auth_members edge
      JOIN pg_catalog.pg_roles granted ON granted.oid=edge.roleid
      JOIN pg_catalog.pg_roles member ON member.oid=edge.member
     WHERE member.rolname='vane_server_runtime';
    SELECT pg_catalog.array_agg(role_name ORDER BY role_name)
      INTO expected_roles
      FROM pg_catalog.unnest(ARRAY[
          'vane_app','vane_edit_coordinator','vane_edit_receipt',
          'vane_snapshot_cutover_operator','vane_push_effect_coordinator',
          'vane_push_effect_receipt','vane_push_effect_operator',
          'vane_push_batch_authority','vane_schedule_commander',
          'vane_agent_session_projection_operator',
          'vane_agent_session_fact_projector','vane_profile_editor',
          'vane_profile_claim_editor','vane_profile_epoch_editor',
          'vane_brief_writer','vane_brief_reader',
          'vane_brief_synthesis_writer','vane_brief_synthesis_recovery',
          'vane_periodic_brief_writer','vane_run_outcome_recovery',
          'vane_intelligence_reader'
      ]::TEXT[]) AS allowed(role_name);
    IF direct_roles IS DISTINCT FROM expected_roles THEN
        RAISE EXCEPTION '109: server runtime memberships differ: got %, expected %',
            direct_roles,expected_roles USING ERRCODE='42501';
    END IF;
    SELECT pg_catalog.array_agg(member.rolname ORDER BY member.rolname)
      INTO reverse_members
      FROM pg_catalog.pg_auth_members edge
      JOIN pg_catalog.pg_roles granted ON granted.oid=edge.roleid
      JOIN pg_catalog.pg_roles member ON member.oid=edge.member
     WHERE granted.rolname='vane_server_runtime';
    IF reverse_members IS NOT NULL THEN
        RAISE EXCEPTION '109: other roles can enter server runtime: %',reverse_members
            USING ERRCODE='42501';
    END IF;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION provision_vane_server_runtime_v2() FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION deprovision_vane_server_runtime_v2() RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE owner_name TEXT:=current_user; runtime_login BOOLEAN;
        direct_roles TEXT[]; expected_roles TEXT[];
BEGIN
    IF session_user<>owner_name THEN
        RAISE EXCEPTION '109: only the direct migration owner may deprovision server runtime'
            USING ERRCODE='42501';
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock(6215335020355474248);
    SELECT rolcanlogin INTO runtime_login FROM pg_catalog.pg_roles
     WHERE rolname='vane_server_runtime';
    IF runtime_login IS NULL THEN RETURN; END IF;
    IF runtime_login THEN
        RAISE EXCEPTION '109: refusing deprovision while vane_server_runtime can login'
            USING ERRCODE='55000';
    END IF;
    SELECT pg_catalog.array_agg(granted.rolname ORDER BY granted.rolname)
      INTO direct_roles
      FROM pg_catalog.pg_auth_members edge
      JOIN pg_catalog.pg_roles granted ON granted.oid=edge.roleid
      JOIN pg_catalog.pg_roles member ON member.oid=edge.member
     WHERE member.rolname='vane_server_runtime';
    SELECT pg_catalog.array_agg(role_name ORDER BY role_name)
      INTO expected_roles
      FROM pg_catalog.unnest(ARRAY[
          'vane_app','vane_edit_coordinator','vane_edit_receipt',
          'vane_snapshot_cutover_operator','vane_push_effect_coordinator',
          'vane_push_effect_receipt','vane_push_effect_operator',
          'vane_push_batch_authority','vane_schedule_commander',
          'vane_agent_session_projection_operator',
          'vane_agent_session_fact_projector','vane_profile_editor',
          'vane_profile_claim_editor','vane_profile_epoch_editor',
          'vane_brief_writer','vane_brief_reader',
          'vane_brief_synthesis_writer','vane_brief_synthesis_recovery',
          'vane_periodic_brief_writer','vane_run_outcome_recovery',
          'vane_intelligence_reader'
      ]::TEXT[]) AS allowed(role_name);
    IF direct_roles IS DISTINCT FROM expected_roles THEN
        RAISE EXCEPTION '109: refusing non-exact server runtime memberships: got %, expected %',
            direct_roles,expected_roles USING ERRCODE='42501';
    END IF;
    PERFORM public.deprovision_vane_server_runtime_v1();
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION deprovision_vane_server_runtime_v2() FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION commit_native_research_task_creation_v3_v1(
    operation_id TEXT, requested_tenant_id BIGINT, requested_user_id BIGINT,
    requested_lease_owner TEXT, requested_fence BIGINT, requested_task_id TEXT,
    requested_definition_digest TEXT, requested_definition BYTEA,
    requested_prepared_schedule BYTEA, requested_ensure_receipt BYTEA,
    requested_target_action BYTEA, requested_target_action_digest TEXT,
    requested_action_authorization_digest TEXT, requested_execution_version SMALLINT
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    operation_row public.task_creation_operations%ROWTYPE;
    definition_json JSONB;
    used_capacity BIGINT;
    existing_schema TEXT;
    existing_mode TEXT;
    existing_digest TEXT;
    existing_payload BYTEA;
    existing_status TEXT;
    authority_status TEXT;
    expected_task_id TEXT;
BEGIN
    expected_task_id:='task-v1-'||encode(sha256(convert_to(
        '{"version":"v1","tenant_id":'||requested_tenant_id::text||
        ',"user_id":'||requested_user_id::text||',"operation_id":'||
        to_json(operation_id)::text||'}','UTF8')),'hex');
    IF operation_id IS NULL OR requested_tenant_id IS NULL OR requested_user_id IS NULL OR
       requested_lease_owner IS NULL OR requested_fence IS NULL OR requested_task_id IS NULL OR
       requested_definition_digest IS NULL OR requested_definition IS NULL OR
       requested_prepared_schedule IS NULL OR requested_ensure_receipt IS NULL OR
       requested_target_action IS NULL OR requested_target_action_digest IS NULL OR
       requested_action_authorization_digest IS NULL OR requested_execution_version IS NULL OR
       requested_execution_version<>2 OR requested_tenant_id<=0 OR
       requested_user_id<=0 OR requested_fence<=0 OR
       btrim(operation_id) IS DISTINCT FROM operation_id OR operation_id='' OR
       btrim(requested_task_id) IS DISTINCT FROM requested_task_id OR requested_task_id='' OR
       requested_task_id<>expected_task_id OR
       btrim(requested_lease_owner) IS DISTINCT FROM requested_lease_owner OR requested_lease_owner='' OR
       requested_definition_digest !~ '^[0-9a-f]{64}$' OR
       requested_target_action_digest !~ '^[0-9a-f]{64}$' OR
       requested_action_authorization_digest !~ '^[0-9a-f]{64}$' OR
       encode(sha256(requested_definition),'hex')<>requested_definition_digest OR
       encode(sha256(requested_target_action),'hex')<>requested_target_action_digest OR
       octet_length(requested_definition) NOT BETWEEN 2 AND 2097152 OR
       octet_length(requested_prepared_schedule) NOT BETWEEN 2 AND 262144 OR
       octet_length(requested_ensure_receipt) NOT BETWEEN 2 AND 262144 OR
       octet_length(requested_target_action) NOT BETWEEN 2 AND 524288 THEN
        RAISE EXCEPTION '109: native V3 creation evidence is invalid' USING ERRCODE='23514';
    END IF;

    BEGIN
        definition_json:=convert_from(requested_definition,'UTF8')::jsonb;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION '109: native V3 definition is not JSON' USING ERRCODE='23514';
    END;
    IF jsonb_typeof(definition_json)<>'object' OR
       (SELECT count(*) FROM jsonb_object_keys(definition_json))<>13 OR
       EXISTS (
           SELECT 1 FROM jsonb_object_keys(definition_json) key
            WHERE key NOT IN (
                'schema_version','tenant_id','user_id','task_id','task_name','task_manual',
                'spec_json','execution_mode','notification','output','planner_budget',
                'delivery_policy','tenant_budget_policy')) OR
       definition_json->>'schema_version' IS DISTINCT FROM 'vane.task-approved-definition/v3' OR
       COALESCE((definition_json->>'tenant_id')::bigint,0)<>requested_tenant_id OR
       COALESCE((definition_json->>'user_id')::bigint,0)<>requested_user_id OR
       definition_json->>'task_id' IS DISTINCT FROM requested_task_id OR
       btrim(COALESCE(definition_json->>'task_name',''))='' OR
       btrim(COALESCE(definition_json->>'task_manual',''))='' OR
       jsonb_typeof(definition_json->'spec_json')<>'object' OR
       definition_json->>'execution_mode' IS DISTINCT FROM 'discover_at_run' OR
       definition_json->>'delivery_policy' IS DISTINCT FROM 'owner_feishu' OR
       definition_json->>'tenant_budget_policy' IS DISTINCT FROM 'inherit_tenant_quota' THEN
        RAISE EXCEPTION '109: native V3 definition shape is invalid' USING ERRCODE='23514';
    END IF;

    -- This is the same exact-task fence and lock order used by migrations
    -- 101/107: advisory authority first, then operation/schedule rows.
    PERFORM pg_advisory_xact_lock(hashtextextended(
        requested_tenant_id::text||'/'||requested_user_id::text||'/'||requested_task_id,101));

    SELECT * INTO operation_row
      FROM public.task_creation_operations
     WHERE id=operation_id AND tenant_id=requested_tenant_id
       AND user_id=requested_user_id AND tool_name='manage_tasks'
       AND execution_version=2
     FOR UPDATE;
    IF NOT FOUND OR operation_row.status<>'executing' OR
       operation_row.tombstoned_at IS NOT NULL OR
       operation_row.lease_owner<>requested_lease_owner OR
       operation_row.fence<>requested_fence OR
       operation_row.lease_until IS NULL OR operation_row.lease_until<=clock_timestamp() OR
       operation_row.compiled_definition IS DISTINCT FROM requested_definition OR
       operation_row.compiled_digest<>requested_definition_digest OR
       operation_row.prepared_schedule IS DISTINCT FROM requested_prepared_schedule OR
       operation_row.ensure_receipt IS DISTINCT FROM requested_ensure_receipt OR
       operation_row.task_id<>requested_task_id OR
       operation_row.args IS DISTINCT FROM definition_json THEN
        RAISE EXCEPTION '109: native V3 creation lease or checkpoints differ' USING ERRCODE='40001';
    END IF;

    PERFORM 1 FROM public.users WHERE id=requested_user_id FOR UPDATE;
    PERFORM 1
      FROM public.memberships membership
      JOIN public.tenants tenant ON tenant.id=membership.tenant_id
     WHERE membership.tenant_id=requested_tenant_id
       AND membership.user_id=requested_user_id
       AND membership.role='owner' AND tenant.status='active'
       AND tenant.deleted_at IS NULL
     FOR UPDATE OF membership FOR SHARE OF tenant;
    IF NOT FOUND THEN
        RAISE EXCEPTION '109: native V3 creation owner scope is inactive' USING ERRCODE='P1092';
    END IF;

    IF operation_row.phase IN ('definition_committed','activation_started','activated') THEN
        SELECT schedule.status,schedule.execution_mode,definition.schema_version,
               definition.definition_digest,definition.payload
          INTO existing_status,existing_mode,existing_schema,existing_digest,existing_payload
          FROM public.schedules schedule
          JOIN public.schedule_playbooks playbook ON playbook.schedule_id=schedule.id
          JOIN public.task_approved_definition_versions definition
            ON definition.tenant_id=schedule.tenant_id AND definition.user_id=schedule.user_id
           AND definition.task_id=schedule.id
           AND definition.version=schedule.approved_definition_version
           AND definition.definition_digest=schedule.approved_definition_digest
         WHERE schedule.id=requested_task_id AND schedule.tenant_id=requested_tenant_id
           AND schedule.user_id=requested_user_id
           AND schedule.spec_json=definition_json->'spec_json'
           AND schedule.nl_description=definition_json->>'task_name'
           AND schedule.scope_json='{}'::jsonb
           AND playbook.content=definition_json->>'task_manual'
           AND playbook.fetch_plan='{}'::jsonb
         FOR SHARE OF schedule,playbook,definition;
        SELECT status INTO authority_status
          FROM public.research_v3_delivery_authorities
         WHERE tenant_id=requested_tenant_id AND user_id=requested_user_id
           AND task_id=requested_task_id AND generation=1
           AND definition_version=1 AND definition_digest=requested_definition_digest
           AND target_action_digest=requested_target_action_digest
           AND action_authorization_digest=requested_action_authorization_digest
         FOR SHARE;
        IF existing_mode IS DISTINCT FROM 'discover_at_run' OR
           existing_schema IS DISTINCT FROM 'vane.task-approved-definition/v3' OR
           existing_digest IS DISTINCT FROM requested_definition_digest OR
           existing_payload IS DISTINCT FROM requested_definition OR
           (operation_row.phase IN ('definition_committed','activation_started') AND
                (existing_status IS DISTINCT FROM 'paused' OR
                 authority_status IS DISTINCT FROM 'staged')) OR
           (operation_row.phase='activated' AND
                (existing_status IS DISTINCT FROM 'active' OR
                 authority_status IS DISTINCT FROM 'enabled')) THEN
            RAISE EXCEPTION '109: native V3 creation replay aggregate differs' USING ERRCODE='40001';
        END IF;
        RETURN;
    END IF;
    IF operation_row.phase<>'schedule_ensured' THEN
        RAISE EXCEPTION '109: native V3 definition commit phase is invalid' USING ERRCODE='40001';
    END IF;

    -- This ledger is intentionally identical to the retained V1 Store
    -- boundary: one user lock protects mature active tasks, unfinished V1/V2
    -- aggregate reservations, and quarantines whose remote side effect may
    -- still exist.  The reservation kind keeps an unknown V1 quarantine from
    -- aliasing a known TaskID while UNION de-duplicates known aggregates.
    SELECT count(*) INTO used_capacity FROM (
        SELECT 0 AS reservation_kind,id AS reservation_id
          FROM public.schedules
         WHERE user_id=requested_user_id AND status='active'
        UNION
        SELECT 0,task_id FROM public.task_creation_operations
         WHERE user_id=requested_user_id AND status='executing'
           AND ((execution_version=1 AND tool_name='create_schedule') OR
                (execution_version=2 AND tool_name='manage_tasks'))
           AND tombstoned_at IS NULL AND task_id<>''
           AND phase IN ('definition_committed','activation_started','activated')
        UNION
        SELECT CASE WHEN result->>'task_id_known'='true' THEN 0 ELSE 1 END,
               CASE WHEN result->>'task_id_known'='true' THEN task_id ELSE id END
          FROM public.task_creation_operations
         WHERE user_id=requested_user_id AND execution_version=1
           AND tool_name='create_schedule' AND status='blocked' AND phase='blocked'
           AND tombstoned_at IS NOT NULL
           AND result->>'version'='vane.task-creation-quarantine/v1'
           AND result->>'reservation_retained'='true'
        UNION
        SELECT 0,task_id FROM public.task_creation_operations
         WHERE user_id=requested_user_id AND execution_version=2
           AND tool_name='manage_tasks' AND status='blocked' AND phase='blocked'
           AND tombstoned_at IS NOT NULL AND task_id<>''
    ) reserved;
    IF used_capacity>=20 THEN
        RAISE EXCEPTION '109: active task capacity reached' USING ERRCODE='P1091';
    END IF;

    INSERT INTO public.schedules
        (id,tenant_id,user_id,nl_description,spec_json,scope_json,status,
         execution_mode)
    VALUES
        (requested_task_id,requested_tenant_id,requested_user_id,
         definition_json->>'task_name',definition_json->'spec_json','{}'::jsonb,'paused',
         'compiled');
    INSERT INTO public.schedule_playbooks(schedule_id,content,fetch_plan)
    VALUES (requested_task_id,definition_json->>'task_manual','{}'::jsonb);
    INSERT INTO public.task_approved_definition_versions
        (tenant_id,user_id,task_id,version,schema_version,execution_mode,
         definition_digest,payload,operation_ref)
    VALUES
        (requested_tenant_id,requested_user_id,requested_task_id,1,
         'vane.task-approved-definition/v3','discover_at_run',
         requested_definition_digest,requested_definition,'task-create-v3/'||operation_id);
    UPDATE public.schedules
       SET execution_mode='discover_at_run',approved_definition_version=1,
           approved_definition_digest=requested_definition_digest,
           updated_at=clock_timestamp()
     WHERE id=requested_task_id AND tenant_id=requested_tenant_id
       AND user_id=requested_user_id AND status='paused'
       AND execution_mode='compiled' AND approved_definition_version IS NULL
       AND approved_definition_digest IS NULL;
    IF NOT FOUND THEN
        RAISE EXCEPTION '109: native V3 schedule promotion changed' USING ERRCODE='40001';
    END IF;
    INSERT INTO public.research_v3_delivery_authorities
        (tenant_id,user_id,task_id,generation,definition_version,definition_digest,
         target_action_digest,action_authorization_digest,status)
    VALUES
        (requested_tenant_id,requested_user_id,requested_task_id,1,1,
         requested_definition_digest,requested_target_action_digest,
         requested_action_authorization_digest,'staged');
    UPDATE public.task_creation_operations
       SET phase='definition_committed',updated_at=clock_timestamp()
     WHERE id=operation_id AND tenant_id=requested_tenant_id AND user_id=requested_user_id
       AND tool_name='manage_tasks' AND execution_version=2 AND phase='schedule_ensured'
       AND lease_owner=requested_lease_owner AND fence=requested_fence
       AND lease_until>clock_timestamp();
    IF NOT FOUND THEN
        RAISE EXCEPTION '109: native V3 definition checkpoint lost lease' USING ERRCODE='40001';
    END IF;
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION begin_native_research_task_activation_v3_v1(
    operation_id TEXT, requested_tenant_id BIGINT, requested_user_id BIGINT,
    requested_lease_owner TEXT, requested_fence BIGINT, requested_task_id TEXT,
    requested_definition_digest TEXT, requested_target_action_digest TEXT,
    requested_action_authorization_digest TEXT,
    requested_execution_version SMALLINT
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE operation_row public.task_creation_operations%ROWTYPE;
        schedule_status TEXT; authority_status TEXT;
BEGIN
    IF operation_id IS NULL OR requested_tenant_id IS NULL OR requested_user_id IS NULL OR
       requested_lease_owner IS NULL OR requested_fence IS NULL OR requested_task_id IS NULL OR
       requested_definition_digest IS NULL OR requested_target_action_digest IS NULL OR
       requested_action_authorization_digest IS NULL OR
       requested_definition_digest !~ '^[0-9a-f]{64}$' OR
       requested_target_action_digest !~ '^[0-9a-f]{64}$' OR
       requested_action_authorization_digest !~ '^[0-9a-f]{64}$' OR
       requested_execution_version IS NULL OR requested_execution_version<>2 THEN
        RAISE EXCEPTION '109: native V3 activation protocol differs' USING ERRCODE='23514';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        requested_tenant_id::text||'/'||requested_user_id::text||'/'||requested_task_id,101));
    SELECT * INTO operation_row FROM public.task_creation_operations
     WHERE id=operation_id AND tenant_id=requested_tenant_id AND user_id=requested_user_id
       AND tool_name='manage_tasks' AND execution_version=2 AND status='executing'
       AND tombstoned_at IS NULL AND lease_owner=requested_lease_owner
       AND fence=requested_fence AND lease_until>clock_timestamp()
       AND task_id=requested_task_id
       AND compiled_digest=requested_definition_digest FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION '109: native V3 activation lease differs' USING ERRCODE='40001';
    END IF;
    SELECT schedule.status INTO schedule_status FROM public.schedules schedule
      JOIN public.task_approved_definition_versions definition
        ON definition.tenant_id=schedule.tenant_id AND definition.user_id=schedule.user_id
       AND definition.task_id=schedule.id AND definition.version=1
     WHERE schedule.tenant_id=requested_tenant_id AND schedule.user_id=requested_user_id
       AND schedule.id=requested_task_id AND schedule.execution_mode='discover_at_run'
       AND schedule.approved_definition_version=1
       AND schedule.approved_definition_digest=requested_definition_digest
       AND definition.schema_version='vane.task-approved-definition/v3'
       AND definition.execution_mode='discover_at_run'
       AND definition.definition_digest=requested_definition_digest
       AND definition.payload=operation_row.compiled_definition
     FOR UPDATE OF schedule,definition;
    SELECT status INTO authority_status FROM public.research_v3_delivery_authorities
     WHERE tenant_id=requested_tenant_id AND user_id=requested_user_id
       AND task_id=requested_task_id AND generation=1 AND definition_version=1
       AND definition_digest=requested_definition_digest
       AND target_action_digest=requested_target_action_digest
       AND action_authorization_digest=requested_action_authorization_digest FOR UPDATE;
    IF operation_row.phase='activation_started' AND schedule_status='paused' AND authority_status='staged' THEN
        RETURN false;
    ELSIF operation_row.phase='activated' AND schedule_status='active' AND authority_status='enabled' THEN
        RETURN false;
    ELSIF operation_row.phase IS DISTINCT FROM 'definition_committed' OR
          schedule_status IS DISTINCT FROM 'paused' OR
          authority_status IS DISTINCT FROM 'staged' THEN
        RAISE EXCEPTION '109: native V3 activation aggregate differs' USING ERRCODE='40001';
    END IF;
    PERFORM 1
      FROM public.memberships membership
      JOIN public.tenants tenant ON tenant.id=membership.tenant_id
     WHERE membership.tenant_id=requested_tenant_id
       AND membership.user_id=requested_user_id
       AND membership.role='owner' AND tenant.status='active'
       AND tenant.deleted_at IS NULL
     FOR SHARE OF membership,tenant;
    IF NOT FOUND THEN
        RAISE EXCEPTION '109: native V3 activation owner scope is inactive' USING ERRCODE='P1092';
    END IF;
    UPDATE public.task_creation_operations SET phase='activation_started',updated_at=clock_timestamp()
     WHERE id=operation_id AND tenant_id=requested_tenant_id AND user_id=requested_user_id
       AND tool_name='manage_tasks' AND execution_version=2 AND phase='definition_committed'
       AND lease_owner=requested_lease_owner AND fence=requested_fence
       AND lease_until>clock_timestamp();
    IF NOT FOUND THEN
        RAISE EXCEPTION '109: native V3 activation authorization lost lease' USING ERRCODE='40001';
    END IF;
    RETURN true;
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION commit_native_research_task_activation_v3_v1(
    operation_id TEXT, requested_tenant_id BIGINT, requested_user_id BIGINT,
    requested_lease_owner TEXT, requested_fence BIGINT, requested_task_id TEXT,
    requested_definition_digest TEXT, requested_target_action_digest TEXT,
    requested_action_authorization_digest TEXT,
    requested_execution_version SMALLINT
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE operation_row public.task_creation_operations%ROWTYPE;
        schedule_status TEXT; authority_status TEXT;
BEGIN
    IF operation_id IS NULL OR requested_tenant_id IS NULL OR requested_user_id IS NULL OR
       requested_lease_owner IS NULL OR requested_fence IS NULL OR requested_task_id IS NULL OR
       requested_definition_digest IS NULL OR requested_target_action_digest IS NULL OR
       requested_action_authorization_digest IS NULL OR
       requested_definition_digest !~ '^[0-9a-f]{64}$' OR
       requested_target_action_digest !~ '^[0-9a-f]{64}$' OR
       requested_action_authorization_digest !~ '^[0-9a-f]{64}$' OR
       requested_execution_version IS NULL OR requested_execution_version<>2 THEN
        RAISE EXCEPTION '109: native V3 activation protocol differs' USING ERRCODE='23514';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        requested_tenant_id::text||'/'||requested_user_id::text||'/'||requested_task_id,101));
    SELECT * INTO operation_row FROM public.task_creation_operations
     WHERE id=operation_id AND tenant_id=requested_tenant_id AND user_id=requested_user_id
       AND tool_name='manage_tasks' AND execution_version=2 AND status='executing'
       AND tombstoned_at IS NULL AND lease_owner=requested_lease_owner
       AND fence=requested_fence AND lease_until>clock_timestamp()
       AND task_id=requested_task_id
       AND compiled_digest=requested_definition_digest FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION '109: native V3 activation commit lease differs' USING ERRCODE='40001';
    END IF;
    SELECT schedule.status INTO schedule_status FROM public.schedules schedule
      JOIN public.task_approved_definition_versions definition
        ON definition.tenant_id=schedule.tenant_id AND definition.user_id=schedule.user_id
       AND definition.task_id=schedule.id AND definition.version=1
     WHERE schedule.tenant_id=requested_tenant_id AND schedule.user_id=requested_user_id
       AND schedule.id=requested_task_id AND schedule.execution_mode='discover_at_run'
       AND schedule.approved_definition_version=1
       AND schedule.approved_definition_digest=requested_definition_digest
       AND definition.schema_version='vane.task-approved-definition/v3'
       AND definition.execution_mode='discover_at_run'
       AND definition.definition_digest=requested_definition_digest
       AND definition.payload=operation_row.compiled_definition
     FOR UPDATE OF schedule,definition;
    SELECT status INTO authority_status FROM public.research_v3_delivery_authorities
     WHERE tenant_id=requested_tenant_id AND user_id=requested_user_id
       AND task_id=requested_task_id AND generation=1 AND definition_version=1
       AND definition_digest=requested_definition_digest
       AND target_action_digest=requested_target_action_digest
       AND action_authorization_digest=requested_action_authorization_digest FOR UPDATE;
    IF operation_row.phase='activated' AND schedule_status='active' AND authority_status='enabled' THEN
        RETURN;
    ELSIF operation_row.phase IS DISTINCT FROM 'activation_started' OR
          schedule_status IS DISTINCT FROM 'paused' OR
          authority_status IS DISTINCT FROM 'staged' THEN
        RAISE EXCEPTION '109: native V3 activation commit aggregate differs' USING ERRCODE='40001';
    END IF;
    PERFORM 1
      FROM public.memberships membership
      JOIN public.tenants tenant ON tenant.id=membership.tenant_id
     WHERE membership.tenant_id=requested_tenant_id
       AND membership.user_id=requested_user_id
       AND membership.role='owner' AND tenant.status='active'
       AND tenant.deleted_at IS NULL
     FOR SHARE OF membership,tenant;
    IF NOT FOUND THEN
        RAISE EXCEPTION '109: native V3 activation commit owner scope is inactive' USING ERRCODE='P1092';
    END IF;
    UPDATE public.schedules SET status='active',updated_at=clock_timestamp()
     WHERE tenant_id=requested_tenant_id AND user_id=requested_user_id
       AND id=requested_task_id AND status='paused' AND execution_mode='discover_at_run';
    IF NOT FOUND THEN
        RAISE EXCEPTION '109: native V3 schedule activation changed' USING ERRCODE='40001';
    END IF;
    UPDATE public.research_v3_delivery_authorities
       SET status='enabled',enabled_at=clock_timestamp()
     WHERE tenant_id=requested_tenant_id AND user_id=requested_user_id
       AND task_id=requested_task_id AND generation=1 AND status='staged';
    IF NOT FOUND THEN
        RAISE EXCEPTION '109: native V3 authority activation changed' USING ERRCODE='40001';
    END IF;
    UPDATE public.task_creation_operations SET phase='activated',updated_at=clock_timestamp()
     WHERE id=operation_id AND tenant_id=requested_tenant_id AND user_id=requested_user_id
       AND tool_name='manage_tasks' AND execution_version=2 AND phase='activation_started'
       AND lease_owner=requested_lease_owner AND fence=requested_fence
       AND lease_until>clock_timestamp();
    IF NOT FOUND THEN
        RAISE EXCEPTION '109: native V3 activation checkpoint lost lease' USING ERRCODE='40001';
    END IF;
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION cleanup_native_research_task_creation_v3_v1(
    requested_operation_id TEXT, requested_tenant_id BIGINT, requested_user_id BIGINT,
    requested_lease_owner TEXT, requested_fence BIGINT, requested_task_id TEXT,
    requested_error_code TEXT, requested_error_message TEXT,
    requested_execution_version SMALLINT
) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE operation_row public.task_creation_operations%ROWTYPE;
BEGIN
    IF requested_operation_id IS NULL OR requested_tenant_id<=0 OR requested_user_id<=0 OR
       requested_lease_owner IS NULL OR btrim(requested_lease_owner)='' OR
       requested_fence<=0 OR requested_task_id IS NULL OR btrim(requested_task_id)='' OR
       requested_error_code IS NULL OR btrim(requested_error_code)='' OR
       octet_length(requested_error_code)>128 OR requested_error_message IS NULL OR
       btrim(requested_error_message)='' OR octet_length(requested_error_message)>4096 OR
       requested_execution_version<>2 THEN
        RAISE EXCEPTION '109: native V3 cleanup request is invalid' USING ERRCODE='23514';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        requested_tenant_id::text||'/'||requested_user_id::text||'/'||requested_task_id,101));
    SELECT * INTO operation_row FROM public.task_creation_operations
     WHERE id=requested_operation_id AND tenant_id=requested_tenant_id
       AND user_id=requested_user_id AND tool_name='manage_tasks'
       AND execution_version=2 FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION '109: native V3 cleanup operation is unavailable' USING ERRCODE='40001';
    END IF;
    IF operation_row.status='failed' AND operation_row.phase='failed' AND
       operation_row.tombstoned_at IS NOT NULL AND
       operation_row.task_id=requested_task_id AND
       operation_row.error_code=requested_error_code AND
       operation_row.error_message=requested_error_message THEN
        IF EXISTS (SELECT 1 FROM public.schedules
                    WHERE id=requested_task_id AND tenant_id=requested_tenant_id
                      AND user_id=requested_user_id) OR
           EXISTS (SELECT 1 FROM public.research_v3_delivery_authorities
                    WHERE tenant_id=requested_tenant_id AND user_id=requested_user_id
                      AND task_id=requested_task_id) OR
           NOT EXISTS (SELECT 1 FROM public.task_creation_receipts receipt
                        WHERE receipt.operation_id=requested_operation_id
                          AND receipt.tenant_id=requested_tenant_id
                          AND receipt.user_id=requested_user_id) THEN
            RAISE EXCEPTION '109: native V3 cleanup replay aggregate differs' USING ERRCODE='40001';
        END IF;
        RETURN;
    END IF;
    IF operation_row.status<>'executing' OR operation_row.tombstoned_at IS NOT NULL OR
       operation_row.lease_owner<>requested_lease_owner OR
       operation_row.fence<>requested_fence OR operation_row.lease_until IS NULL OR
       operation_row.lease_until<=clock_timestamp() OR
       operation_row.task_id<>requested_task_id OR
       operation_row.phase NOT IN ('schedule_prepared','schedule_ensured',
          'definition_committed','activation_started') THEN
        RAISE EXCEPTION '109: native V3 cleanup lease or phase differs' USING ERRCODE='40001';
    END IF;

    DELETE FROM public.schedules
     WHERE id=requested_task_id AND tenant_id=requested_tenant_id
       AND user_id=requested_user_id;
    DELETE FROM public.research_v3_delivery_authorities
     WHERE tenant_id=requested_tenant_id AND user_id=requested_user_id
       AND task_id=requested_task_id;
    UPDATE public.task_creation_operations
       SET status='failed',phase='failed',error_code=requested_error_code,
           error_message=requested_error_message,result=NULL,executed_at=NULL,
           tombstoned_at=clock_timestamp(),lease_owner='',lease_until=NULL,
           takeover_not_before=NULL,updated_at=clock_timestamp()
     WHERE id=requested_operation_id AND tenant_id=requested_tenant_id
       AND user_id=requested_user_id AND tool_name='manage_tasks'
       AND execution_version=2 AND status='executing'
       AND lease_owner=requested_lease_owner AND fence=requested_fence;
    IF NOT FOUND THEN
        RAISE EXCEPTION '109: native V3 cleanup checkpoint lost lease' USING ERRCODE='40001';
    END IF;
    INSERT INTO public.task_creation_receipts (
        operation_id,tenant_id,user_id,session_id,provider,target,provider_key,
        status,next_attempt_at,failure_class,blocked_at)
    SELECT operation.id,operation.tenant_id,operation.user_id,operation.session_id,
           operation.receipt_provider,operation.receipt_target,
           md5('vane/task-creation-receipt/v1:'||operation.id)::uuid,
           CASE WHEN operation.receipt_provider='' OR operation.receipt_target=''
                THEN 'blocked' ELSE 'pending' END,
           clock_timestamp()+interval '4 seconds',
           CASE WHEN operation.receipt_provider='' OR operation.receipt_target=''
                THEN 'target_unbound' ELSE '' END,
           CASE WHEN operation.receipt_provider='' OR operation.receipt_target=''
                THEN clock_timestamp() ELSE NULL END
      FROM public.task_creation_operations operation
     WHERE operation.id=requested_operation_id AND operation.tenant_id=requested_tenant_id
       AND operation.user_id=requested_user_id
    ON CONFLICT (operation_id) DO NOTHING;
    IF NOT FOUND THEN
        RAISE EXCEPTION '109: native V3 cleanup receipt differs' USING ERRCODE='40001';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION commit_native_research_task_creation_v3_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,BYTEA,BYTEA,BYTEA,BYTEA,TEXT,TEXT,SMALLINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION begin_native_research_task_activation_v3_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,TEXT,TEXT,SMALLINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION commit_native_research_task_activation_v3_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,TEXT,TEXT,SMALLINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION cleanup_native_research_task_creation_v3_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,TEXT,SMALLINT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION commit_native_research_task_creation_v3_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,BYTEA,BYTEA,BYTEA,BYTEA,TEXT,TEXT,SMALLINT)
    TO vane_native_v3_creation_coordinator;
GRANT EXECUTE ON FUNCTION begin_native_research_task_activation_v3_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,TEXT,TEXT,SMALLINT)
    TO vane_native_v3_creation_coordinator;
GRANT EXECUTE ON FUNCTION commit_native_research_task_activation_v3_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,TEXT,TEXT,SMALLINT)
    TO vane_native_v3_creation_coordinator;
GRANT EXECUTE ON FUNCTION cleanup_native_research_task_creation_v3_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,TEXT,SMALLINT)
    TO vane_native_v3_creation_coordinator;

-- +goose Down

-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM task_creation_operations WHERE execution_version=2) OR
       EXISTS (SELECT 1 FROM task_approved_definition_versions
                WHERE operation_ref LIKE 'task-create-v3/%') THEN
        RAISE EXCEPTION '109: refusing downgrade while native V3 creation state exists';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles
                WHERE rolname='vane_server_runtime') THEN
        RAISE EXCEPTION '109: deprovision vane_server_runtime before schema downgrade';
    END IF;
END $$;
-- +goose StatementEnd

DROP FUNCTION commit_native_research_task_activation_v3_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,TEXT,TEXT,SMALLINT);
DROP FUNCTION cleanup_native_research_task_creation_v3_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,TEXT,SMALLINT);
DROP FUNCTION begin_native_research_task_activation_v3_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,TEXT,TEXT,SMALLINT);
DROP FUNCTION commit_native_research_task_creation_v3_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,BYTEA,BYTEA,BYTEA,BYTEA,TEXT,TEXT,SMALLINT);
DROP FUNCTION deprovision_vane_server_runtime_v2();
DROP FUNCTION provision_vane_server_runtime_v2();
REVOKE USAGE ON SCHEMA public FROM vane_native_v3_creation_coordinator;
DROP INDEX idx_task_creation_operations_stale;
CREATE INDEX idx_task_creation_operations_stale
    ON task_creation_operations (tenant_id,takeover_not_before,id)
    WHERE execution_version=1 AND tool_name='create_schedule'
      AND status='executing' AND tombstoned_at IS NULL;
ALTER TABLE task_creation_operations
    DROP CONSTRAINT task_creation_operations_protocol_tool_binding;
ALTER TABLE task_creation_operations
    DROP CONSTRAINT task_creation_operations_execution_version_current;
ALTER TABLE task_creation_operations
    ADD CONSTRAINT task_creation_operations_execution_version_current
    CHECK (execution_version=1);
