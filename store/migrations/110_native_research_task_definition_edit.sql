-- 110: explicit decoder isolation for native Research V3 definition edits.
--
-- Historical proposal/prepared bytes remain protocol 1 and continue through
-- the frozen v1/v2 readers. Protocol 3 is a new writer/recovery lane using the
-- same per-task nonterminal exclusion and schedule marker.

-- +goose Up

ALTER TABLE task_definition_edit_operations
    ADD COLUMN operation_protocol SMALLINT NOT NULL DEFAULT 1;
ALTER TABLE task_definition_edit_operations
    ADD CONSTRAINT task_definition_edit_operation_protocol_valid
    CHECK (operation_protocol IN (1,3));

CREATE INDEX idx_task_definition_edit_operations_protocol_recovery
    ON task_definition_edit_operations
       (operation_protocol,tenant_id,takeover_not_before,id)
    WHERE status='executing' AND tombstoned_at IS NULL;

-- Retained compiled edits can never observe or mutate protocol 3 even though
-- the historical role still owns its original table privileges.
CREATE POLICY legacy_definition_edit_protocol_isolation
    ON task_definition_edit_operations AS RESTRICTIVE
    FOR ALL TO vane_edit_coordinator
    USING (operation_protocol=1) WITH CHECK (operation_protocol=1);

-- Native V3 transitions run only inside fixed SECURITY DEFINER functions.
-- Neither vane_app nor vane_server_runtime can enter this role or touch its
-- tables directly.  The migration owner is its sole cluster-wide member so a
-- later migration can safely maintain/drop the owned functions.
-- +goose StatementBegin
DO $$ BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_roles
	                WHERE rolname='vane_native_v3_edit_recovery') THEN
	    BEGIN
	        CREATE ROLE vane_native_v3_edit_recovery
	            NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
	            NOREPLICATION NOBYPASSRLS;
	    EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL;
	    END;
	END IF;
	ALTER ROLE vane_native_v3_edit_recovery
	    NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
	    NOREPLICATION NOBYPASSRLS;
	IF EXISTS (
	    SELECT 1 FROM pg_auth_members edge
	    JOIN pg_roles granted ON granted.oid=edge.roleid
	    JOIN pg_roles member ON member.oid=edge.member
	    WHERE (granted.rolname='vane_native_v3_edit_recovery'
	           AND member.rolname<>'vane_native_v3_edit_recovery_runtime')
	       OR member.rolname='vane_native_v3_edit_recovery'
	) THEN
	    RAISE EXCEPTION '110: unsafe native V3 edit recovery capability membership';
	END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles
                    WHERE rolname='vane_native_v3_edit_coordinator') THEN
        BEGIN
            CREATE ROLE vane_native_v3_edit_coordinator
                NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
                NOREPLICATION NOBYPASSRLS;
        EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL;
        END;
    END IF;
    ALTER ROLE vane_native_v3_edit_coordinator
        NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
        NOREPLICATION NOBYPASSRLS;
    IF EXISTS (
        SELECT 1 FROM pg_auth_members edge
        JOIN pg_roles granted ON granted.oid=edge.roleid
        JOIN pg_roles member ON member.oid=edge.member
        WHERE granted.rolname='vane_native_v3_edit_coordinator'
          AND member.rolname<>current_user
    ) OR EXISTS (
        SELECT 1 FROM pg_auth_members edge
        JOIN pg_roles member ON member.oid=edge.member
        WHERE member.rolname='vane_native_v3_edit_coordinator'
    ) THEN
        RAISE EXCEPTION '110: unsafe native V3 edit role membership';
    END IF;
    EXECUTE format('GRANT vane_native_v3_edit_coordinator TO %I',current_user);
END $$;
-- +goose StatementEnd

GRANT USAGE ON SCHEMA public TO vane_native_v3_edit_coordinator;
GRANT SELECT (id,status,deleted_at) ON tenants TO vane_native_v3_edit_coordinator;
GRANT SELECT (tenant_id,user_id,role) ON memberships TO vane_native_v3_edit_coordinator;
GRANT SELECT (id,tenant_id,user_id,status) ON agent_sessions TO vane_native_v3_edit_coordinator;
GRANT SELECT,UPDATE (status,nl_description,spec_json,approved_definition_version,
    approved_definition_digest,definition_edit_operation_id,
    definition_edit_fence,updated_at) ON schedules TO vane_native_v3_edit_coordinator;
GRANT SELECT,UPDATE (content) ON schedule_playbooks TO vane_native_v3_edit_coordinator;
GRANT SELECT,INSERT ON task_approved_definition_versions TO vane_native_v3_edit_coordinator;
GRANT SELECT,INSERT,UPDATE ON task_definition_edit_operations TO vane_native_v3_edit_coordinator;
GRANT SELECT,INSERT ON task_definition_edit_receipts TO vane_native_v3_edit_coordinator;
GRANT USAGE,SELECT ON SEQUENCE task_definition_edit_receipts_id_seq
    TO vane_native_v3_edit_coordinator;
GRANT SELECT (id,tenant_id,user_id,task_id,tool_name,prepared_schedule,
    compiled_digest,status,phase,execution_version,created_at,tombstoned_at)
    ON task_creation_operations TO vane_native_v3_edit_coordinator;
GRANT SELECT,INSERT,UPDATE (status,enabled_at,revoked_at)
    ON research_v3_delivery_authorities TO vane_native_v3_edit_coordinator;

CREATE POLICY native_v3_edit_protocol_isolation
    ON task_definition_edit_operations AS RESTRICTIVE
    FOR ALL TO vane_native_v3_edit_coordinator
    USING (operation_protocol=3) WITH CHECK (operation_protocol=3);

-- V3 authority access is user-scoped in addition to migration 101's tenant
-- policy.  The fixed functions below also compare both GUCs before touching a
-- row, so a future query regression fails closed twice.
CREATE POLICY native_v3_edit_authority_user_isolation
    ON research_v3_delivery_authorities AS RESTRICTIVE
    FOR ALL TO vane_native_v3_edit_coordinator
    USING (user_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint)
    WITH CHECK (user_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint);

-- Recovery is intentionally absent from vane_app. A separate non-login,
-- least-privilege definer may bypass tenant RLS only inside one fixed global
-- claim function. Only the separately provisioned recovery login can assume
-- the opaque capability role; neither server nor paid research runtime can.
-- +goose StatementBegin
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles
                    WHERE rolname='vane_native_v3_edit_recovery_coordinator') THEN
        BEGIN
            CREATE ROLE vane_native_v3_edit_recovery_coordinator
                NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
                NOREPLICATION BYPASSRLS;
        EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL;
        END;
    END IF;
    ALTER ROLE vane_native_v3_edit_recovery_coordinator
        NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
        NOREPLICATION BYPASSRLS;
    IF EXISTS (
        SELECT 1 FROM pg_auth_members edge
        JOIN pg_roles granted ON granted.oid=edge.roleid
        JOIN pg_roles member ON member.oid=edge.member
        WHERE granted.rolname='vane_native_v3_edit_recovery_coordinator'
          AND member.rolname<>current_user
    ) OR EXISTS (
        SELECT 1 FROM pg_auth_members edge
        JOIN pg_roles member ON member.oid=edge.member
        WHERE member.rolname='vane_native_v3_edit_recovery_coordinator'
    ) THEN
        RAISE EXCEPTION '110: unsafe native V3 recovery role membership';
    END IF;
    EXECUTE format('GRANT vane_native_v3_edit_recovery_coordinator TO %I',current_user);
END $$;
-- +goose StatementEnd

GRANT USAGE ON SCHEMA public TO vane_native_v3_edit_recovery_coordinator;
GRANT SELECT ON task_definition_edit_operations
    TO vane_native_v3_edit_recovery_coordinator;
GRANT UPDATE (lease_owner,lease_until,takeover_not_before,fence,attempt,updated_at)
    ON task_definition_edit_operations TO vane_native_v3_edit_recovery_coordinator;
GRANT SELECT (tenant_id,user_id,id,status,definition_edit_operation_id,
    definition_edit_fence) ON schedules TO vane_native_v3_edit_recovery_coordinator;
GRANT UPDATE (definition_edit_fence,updated_at) ON schedules
    TO vane_native_v3_edit_recovery_coordinator;

-- +goose StatementBegin
CREATE FUNCTION native_research_v3_edit_assert_scope_v1(
    requested_tenant_id BIGINT,requested_user_id BIGINT
) RETURNS void LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
    IF requested_tenant_id IS NULL OR requested_tenant_id<=0 OR
       requested_user_id IS NULL OR requested_user_id<=0 OR
       requested_tenant_id IS DISTINCT FROM
         NULLIF(current_setting('app.tenant_id',true),'')::bigint OR
       requested_user_id IS DISTINCT FROM
         NULLIF(current_setting('app.user_id',true),'')::bigint THEN
        RAISE EXCEPTION '110: native V3 edit scope differs'
            USING ERRCODE='42501';
    END IF;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION native_research_v3_edit_assert_scope_v1(bigint,bigint)
    FROM PUBLIC;
ALTER FUNCTION native_research_v3_edit_assert_scope_v1(bigint,bigint)
    OWNER TO vane_native_v3_edit_coordinator;

-- +goose StatementBegin
CREATE FUNCTION load_native_research_v3_edit_operation_v1(
    requested_id TEXT,requested_tenant_id BIGINT,requested_user_id BIGINT,
    requested_task_id TEXT
) RETURNS SETOF task_definition_edit_operations
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
    PERFORM public.native_research_v3_edit_assert_scope_v1(
        requested_tenant_id,requested_user_id);
    IF requested_id IS NULL OR btrim(requested_id)<>requested_id OR requested_id='' OR
       requested_task_id IS NULL OR btrim(requested_task_id)<>requested_task_id OR
       requested_task_id='' THEN
        RAISE EXCEPTION '110: native V3 edit identity is invalid'
            USING ERRCODE='23514';
    END IF;
    RETURN QUERY SELECT operation.*
      FROM public.task_definition_edit_operations operation
     WHERE operation.id=requested_id AND operation.operation_protocol=3
       AND operation.tenant_id=requested_tenant_id
       AND operation.user_id=requested_user_id
       AND operation.target_tenant_id=requested_tenant_id
       AND operation.target_user_id=requested_user_id
       AND operation.task_id=requested_task_id;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION load_native_research_v3_edit_operation_v1(text,bigint,bigint,text)
    FROM PUBLIC;
ALTER FUNCTION load_native_research_v3_edit_operation_v1(text,bigint,bigint,text)
    OWNER TO vane_native_v3_edit_coordinator;
GRANT EXECUTE ON FUNCTION load_native_research_v3_edit_operation_v1(text,bigint,bigint,text)
    TO vane_app;

-- +goose StatementBegin
CREATE FUNCTION load_native_research_v3_edit_basis_v1(
    requested_tenant_id BIGINT,requested_user_id BIGINT,requested_task_id TEXT
) RETURNS TABLE(
    schedule_status TEXT,definition_version BIGINT,definition_digest TEXT,
    definition_payload BYTEA,schedule_name TEXT,schedule_spec BYTEA,
    task_manual TEXT,authority_generation BIGINT,target_action_digest TEXT,
    action_authorization_digest TEXT,provenance_kind TEXT,provenance BYTEA
) LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
    PERFORM public.native_research_v3_edit_assert_scope_v1(
        requested_tenant_id,requested_user_id);
    IF requested_task_id IS NULL OR btrim(requested_task_id)<>requested_task_id OR
       requested_task_id='' THEN
        RAISE EXCEPTION '110: native V3 edit task is invalid' USING ERRCODE='23514';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        requested_tenant_id::text||'/'||requested_user_id::text||'/'||requested_task_id,101));
    RETURN QUERY
    SELECT schedule.status,schedule.approved_definition_version,
           schedule.approved_definition_digest,definition.payload,
           schedule.nl_description,schedule.spec_json::text::bytea,
           playbook.content,authority.generation,authority.target_action_digest,
           authority.action_authorization_digest,
           CASE WHEN completed.prepared_edit IS NOT NULL THEN 'edit' ELSE 'creation' END,
           COALESCE(completed.prepared_edit,creation.prepared_schedule)
      FROM public.schedules schedule
      JOIN public.schedule_playbooks playbook ON playbook.schedule_id=schedule.id
      JOIN public.task_approved_definition_versions definition
        ON definition.tenant_id=schedule.tenant_id AND definition.user_id=schedule.user_id
       AND definition.task_id=schedule.id
       AND definition.version=schedule.approved_definition_version
       AND definition.definition_digest=schedule.approved_definition_digest
      JOIN public.research_v3_delivery_authorities authority
        ON authority.tenant_id=schedule.tenant_id AND authority.user_id=schedule.user_id
       AND authority.task_id=schedule.id
       AND authority.definition_version=schedule.approved_definition_version
       AND authority.definition_digest=schedule.approved_definition_digest
       AND authority.status='enabled'
      LEFT JOIN LATERAL (
          SELECT operation.prepared_edit
            FROM public.task_definition_edit_operations operation
           WHERE operation.operation_protocol=3
             AND operation.tenant_id=schedule.tenant_id
             AND operation.user_id=schedule.user_id AND operation.task_id=schedule.id
             AND operation.target_definition_version=schedule.approved_definition_version
             AND operation.target_definition_digest=schedule.approved_definition_digest
             AND operation.status='completed'
             AND operation.phase='temporal_target_restored'
             AND operation.tombstoned_at IS NOT NULL
           ORDER BY operation.created_at DESC,operation.id DESC LIMIT 1
      ) completed ON true
      LEFT JOIN LATERAL (
          SELECT creation_operation.prepared_schedule
            FROM public.task_creation_operations creation_operation
           WHERE creation_operation.tenant_id=schedule.tenant_id
             AND creation_operation.user_id=schedule.user_id
             AND creation_operation.task_id=schedule.id
             AND creation_operation.execution_version=2
             AND creation_operation.tool_name='manage_tasks'
             AND creation_operation.status='executed'
             AND creation_operation.phase='completed'
             AND creation_operation.tombstoned_at IS NOT NULL
             AND creation_operation.compiled_digest=schedule.approved_definition_digest
           ORDER BY creation_operation.created_at DESC,creation_operation.id DESC LIMIT 1
      ) creation ON completed.prepared_edit IS NULL
     WHERE schedule.tenant_id=requested_tenant_id
       AND schedule.user_id=requested_user_id AND schedule.id=requested_task_id
       AND schedule.execution_mode='discover_at_run'
       AND schedule.status IN ('active','paused')
       AND schedule.scope_json='{}'::jsonb AND playbook.fetch_plan='{}'::jsonb
       AND schedule.definition_edit_operation_id IS NULL
       AND schedule.definition_edit_fence IS NULL
       AND definition.schema_version='vane.task-approved-definition/v3'
       AND COALESCE(completed.prepared_edit,creation.prepared_schedule) IS NOT NULL
       AND EXISTS (
           SELECT 1 FROM public.memberships membership
           JOIN public.tenants tenant ON tenant.id=membership.tenant_id
          WHERE membership.tenant_id=requested_tenant_id
            AND membership.user_id=requested_user_id AND membership.role='owner'
            AND tenant.status='active' AND tenant.deleted_at IS NULL)
     FOR UPDATE OF schedule FOR SHARE OF playbook,authority;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION load_native_research_v3_edit_basis_v1(bigint,bigint,text)
    FROM PUBLIC;
ALTER FUNCTION load_native_research_v3_edit_basis_v1(bigint,bigint,text)
    OWNER TO vane_native_v3_edit_coordinator;
GRANT EXECUTE ON FUNCTION load_native_research_v3_edit_basis_v1(bigint,bigint,text)
    TO vane_app;

-- +goose StatementBegin
CREATE FUNCTION claim_stale_native_research_v3_edit_v1(
    requested_before TIMESTAMPTZ,requested_lease_owner TEXT,
    requested_lease_us BIGINT
) RETURNS TABLE(operation_id TEXT,tenant_id BIGINT,user_id BIGINT,task_id TEXT,
                lease_owner TEXT,fence BIGINT)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE candidate RECORD; operation public.task_definition_edit_operations%ROWTYPE;
        now_at TIMESTAMPTZ; old_fence BIGINT;
BEGIN
    IF requested_before IS NULL OR requested_lease_owner IS NULL OR
       requested_lease_owner='' OR btrim(requested_lease_owner)<>requested_lease_owner OR
       requested_lease_us NOT BETWEEN 1 AND 86400000000 THEN
        RAISE EXCEPTION '110: native V3 recovery claim is invalid' USING ERRCODE='23514';
    END IF;
    now_at:=clock_timestamp();
    SELECT candidate_operation.id,candidate_operation.tenant_id,
           candidate_operation.user_id,candidate_operation.task_id
      INTO candidate
      FROM public.task_definition_edit_operations candidate_operation
     WHERE candidate_operation.operation_protocol=3
       AND candidate_operation.status='executing'
       AND candidate_operation.tombstoned_at IS NULL
       AND candidate_operation.lease_owner<>''
       AND candidate_operation.fence>0 AND candidate_operation.attempt>0
       AND candidate_operation.lease_until IS NOT NULL
       AND candidate_operation.takeover_not_before IS NOT NULL
       AND candidate_operation.lease_until<=now_at
       AND candidate_operation.takeover_not_before<=LEAST(requested_before,now_at)
     ORDER BY candidate_operation.takeover_not_before,candidate_operation.id
     LIMIT 1 FOR UPDATE OF candidate_operation SKIP LOCKED;
    IF NOT FOUND THEN RETURN; END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        candidate.tenant_id::text||'/'||candidate.user_id::text||'/'||candidate.task_id,101));
    SELECT * INTO operation
      FROM public.task_definition_edit_operations claimed
     WHERE claimed.id=candidate.id AND claimed.operation_protocol=3
       AND claimed.tenant_id=candidate.tenant_id AND claimed.user_id=candidate.user_id
       AND claimed.task_id=candidate.task_id AND claimed.status='executing'
       AND claimed.tombstoned_at IS NULL AND claimed.lease_until<=clock_timestamp()
       AND claimed.takeover_not_before<=LEAST(requested_before,clock_timestamp())
     FOR UPDATE;
    IF NOT FOUND THEN RETURN; END IF;
    old_fence:=operation.fence;
    UPDATE public.task_definition_edit_operations target SET
        lease_owner=requested_lease_owner,
        lease_until=clock_timestamp()+(requested_lease_us*interval '1 microsecond'),
        takeover_not_before=clock_timestamp()+((requested_lease_us+30000000)*interval '1 microsecond'),
        fence=target.fence+1,attempt=target.attempt+1,updated_at=clock_timestamp()
     WHERE target.id=operation.id AND target.operation_protocol=3
       AND target.status='executing' AND target.tombstoned_at IS NULL
       AND target.fence=old_fence AND target.takeover_not_before<=clock_timestamp()
     RETURNING target.* INTO operation;
    IF NOT FOUND THEN RETURN; END IF;
    IF operation.phase<>'proposal_sealed' THEN
        UPDATE public.schedules target SET definition_edit_fence=operation.fence,
            updated_at=clock_timestamp()
         WHERE target.tenant_id=operation.tenant_id
           AND target.user_id=operation.user_id AND target.id=operation.task_id
           AND target.status='paused'
           AND target.definition_edit_operation_id=operation.id
           AND target.definition_edit_fence=old_fence;
        IF NOT FOUND THEN
            RAISE EXCEPTION '110: native V3 recovery marker differs' USING ERRCODE='40001';
        END IF;
    END IF;
    RETURN QUERY SELECT operation.id,operation.tenant_id,operation.user_id,
        operation.task_id,operation.lease_owner,operation.fence;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION claim_stale_native_research_v3_edit_v1(timestamptz,text,bigint)
    FROM PUBLIC;
ALTER FUNCTION claim_stale_native_research_v3_edit_v1(timestamptz,text,bigint)
    OWNER TO vane_native_v3_edit_recovery_coordinator;
GRANT EXECUTE ON FUNCTION claim_stale_native_research_v3_edit_v1(timestamptz,text,bigint)
    TO vane_native_v3_edit_recovery;
GRANT USAGE ON SCHEMA public TO vane_native_v3_edit_recovery;

-- +goose StatementBegin
CREATE FUNCTION seal_native_research_v3_edit_v1(
    requested_id TEXT,requested_tenant_id BIGINT,requested_user_id BIGINT,
    requested_task_id TEXT,requested_session_id BIGINT,requested_expires_at TIMESTAMPTZ,
    requested_original_status TEXT,requested_base_version BIGINT,
    requested_base_digest TEXT,requested_base_definition BYTEA,
    requested_target_version BIGINT,requested_target_digest TEXT,
    requested_target_definition BYTEA,requested_proposal BYTEA,
    requested_proposal_digest TEXT,requested_prepared_edit BYTEA,
    requested_prepared_digest TEXT,requested_base_snapshot BYTEA,
    requested_base_snapshot_digest TEXT,requested_base_prepared BYTEA,
    requested_base_action_digest TEXT,requested_authorization_digest TEXT
) RETURNS SETOF task_definition_edit_operations
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE target_json JSONB;
BEGIN
    PERFORM public.native_research_v3_edit_assert_scope_v1(
        requested_tenant_id,requested_user_id);
    IF requested_id IS NULL OR btrim(requested_id)<>requested_id OR requested_id='' OR
       requested_task_id IS NULL OR btrim(requested_task_id)<>requested_task_id OR
       requested_task_id='' OR requested_session_id<=0 OR requested_expires_at IS NULL OR
       requested_original_status NOT IN ('active','paused') OR
       requested_base_version<=0 OR requested_target_version<>requested_base_version+1 OR
       requested_base_digest !~ '^[0-9a-f]{64}$' OR
       requested_target_digest !~ '^[0-9a-f]{64}$' OR
       requested_base_action_digest !~ '^[0-9a-f]{64}$' OR
       requested_authorization_digest !~ '^[0-9a-f]{64}$' OR
       requested_proposal_digest<>encode(sha256(requested_proposal),'hex') OR
       requested_prepared_digest<>encode(sha256(requested_prepared_edit),'hex') OR
       requested_base_snapshot_digest<>encode(sha256(requested_base_snapshot),'hex') OR
       requested_base_digest<>encode(sha256(requested_base_definition),'hex') OR
       requested_target_digest<>encode(sha256(requested_target_definition),'hex') THEN
        RAISE EXCEPTION '110: native V3 edit evidence is invalid' USING ERRCODE='23514';
    END IF;
    BEGIN target_json:=convert_from(requested_target_definition,'UTF8')::jsonb;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION '110: native V3 edit target is not JSON' USING ERRCODE='23514';
    END;
    IF jsonb_typeof(target_json)<>'object' OR
       target_json->>'schema_version' IS DISTINCT FROM 'vane.task-approved-definition/v3' OR
       COALESCE((target_json->>'tenant_id')::bigint,0)<>requested_tenant_id OR
       COALESCE((target_json->>'user_id')::bigint,0)<>requested_user_id OR
       target_json->>'task_id' IS DISTINCT FROM requested_task_id OR
       target_json->>'execution_mode' IS DISTINCT FROM 'discover_at_run' OR
       EXISTS (SELECT 1 FROM jsonb_object_keys(target_json) key
                WHERE key IN ('tool_calls','sources','fetch_targets','source_catalog')) THEN
        RAISE EXCEPTION '110: native V3 edit target shape differs' USING ERRCODE='23514';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        requested_tenant_id::text||'/'||requested_user_id::text||'/'||requested_task_id,101));
    PERFORM 1 FROM public.schedules schedule
      JOIN public.schedule_playbooks playbook ON playbook.schedule_id=schedule.id
      JOIN public.task_approved_definition_versions definition
        ON definition.tenant_id=schedule.tenant_id AND definition.user_id=schedule.user_id
       AND definition.task_id=schedule.id AND definition.version=requested_base_version
       AND definition.definition_digest=requested_base_digest
      JOIN public.research_v3_delivery_authorities authority
        ON authority.tenant_id=schedule.tenant_id AND authority.user_id=schedule.user_id
       AND authority.task_id=schedule.id AND authority.definition_version=requested_base_version
       AND authority.definition_digest=requested_base_digest
       AND authority.target_action_digest=requested_base_action_digest
       AND authority.action_authorization_digest=requested_authorization_digest
       AND authority.status='enabled'
     WHERE schedule.tenant_id=requested_tenant_id AND schedule.user_id=requested_user_id
       AND schedule.id=requested_task_id AND schedule.status=requested_original_status
       AND schedule.execution_mode='discover_at_run'
       AND schedule.approved_definition_version=requested_base_version
       AND schedule.approved_definition_digest=requested_base_digest
       AND schedule.definition_edit_operation_id IS NULL
       AND schedule.definition_edit_fence IS NULL
       AND schedule.scope_json='{}'::jsonb AND playbook.fetch_plan='{}'::jsonb
       AND definition.schema_version='vane.task-approved-definition/v3'
       AND definition.payload=requested_base_definition
       AND EXISTS (SELECT 1 FROM public.agent_sessions session
                    WHERE session.id=requested_session_id
                      AND session.tenant_id=requested_tenant_id
                      AND session.user_id=requested_user_id AND session.status='active')
       AND EXISTS (SELECT 1 FROM public.memberships membership
                    JOIN public.tenants tenant ON tenant.id=membership.tenant_id
                    WHERE membership.tenant_id=requested_tenant_id
                      AND membership.user_id=requested_user_id
                      AND membership.role='owner' AND tenant.status='active'
                      AND tenant.deleted_at IS NULL)
       AND (EXISTS (
              SELECT 1 FROM public.task_creation_operations creation
               WHERE creation.tenant_id=requested_tenant_id
                 AND creation.user_id=requested_user_id AND creation.task_id=requested_task_id
                 AND creation.execution_version=2 AND creation.tool_name='manage_tasks'
                 AND creation.status='executed' AND creation.phase='completed'
                 AND creation.tombstoned_at IS NOT NULL
                 AND creation.compiled_digest=requested_base_digest
                 AND creation.prepared_schedule=requested_base_prepared)
            OR EXISTS (
              SELECT 1 FROM public.task_definition_edit_operations prior
               WHERE prior.operation_protocol=3 AND prior.tenant_id=requested_tenant_id
                 AND prior.user_id=requested_user_id AND prior.task_id=requested_task_id
                 AND prior.target_definition_version=requested_base_version
                 AND prior.target_definition_digest=requested_base_digest
                 AND prior.status='completed' AND prior.phase='temporal_target_restored'
                 AND prior.tombstoned_at IS NOT NULL
                 AND decode(convert_from(prior.prepared_edit,'UTF8')::jsonb->>'target',
                            'base64')=requested_base_prepared))
     FOR UPDATE OF schedule FOR SHARE OF playbook,authority;
    IF NOT FOUND THEN
        RAISE EXCEPTION '110: native V3 edit base changed' USING ERRCODE='40001';
    END IF;
    RETURN QUERY INSERT INTO public.task_definition_edit_operations (
        id,operation_protocol,tenant_id,user_id,target_tenant_id,target_user_id,
        task_id,session_id,operation_ref,expires_at,original_status,
        base_definition_version,base_definition_digest,base_definition,
        target_definition_version,target_definition_digest,target_definition,
        canonical_proposal,proposal_digest,prepared_edit,prepared_edit_digest,
        base_snapshot,base_snapshot_digest)
    VALUES (requested_id,3,requested_tenant_id,requested_user_id,
        requested_tenant_id,requested_user_id,requested_task_id,requested_session_id,
        'agent_auto/v3:'||requested_id,requested_expires_at,requested_original_status,
        requested_base_version,requested_base_digest,requested_base_definition,
        requested_target_version,requested_target_digest,requested_target_definition,
        requested_proposal,requested_proposal_digest,requested_prepared_edit,
        requested_prepared_digest,requested_base_snapshot,requested_base_snapshot_digest)
    RETURNING *;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION seal_native_research_v3_edit_v1(text,bigint,bigint,text,bigint,timestamptz,text,bigint,text,bytea,bigint,text,bytea,bytea,text,bytea,text,bytea,text,bytea,text,text)
    FROM PUBLIC;
ALTER FUNCTION seal_native_research_v3_edit_v1(text,bigint,bigint,text,bigint,timestamptz,text,bigint,text,bytea,bigint,text,bytea,bytea,text,bytea,text,bytea,text,bytea,text,text)
    OWNER TO vane_native_v3_edit_coordinator;
GRANT EXECUTE ON FUNCTION seal_native_research_v3_edit_v1(text,bigint,bigint,text,bigint,timestamptz,text,bigint,text,bytea,bigint,text,bytea,bytea,text,bytea,text,bytea,text,bytea,text,text)
    TO vane_app;

-- +goose StatementBegin
CREATE FUNCTION acquire_native_research_v3_edit_v1(
    requested_id TEXT,requested_tenant_id BIGINT,requested_user_id BIGINT,
    requested_task_id TEXT,requested_lease_owner TEXT,requested_lease_us BIGINT,
    requested_receipt_provider TEXT,requested_receipt_target TEXT
) RETURNS SETOF task_definition_edit_operations
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE operation public.task_definition_edit_operations%ROWTYPE;
        schedule_row public.schedules%ROWTYPE; now_at TIMESTAMPTZ;
        receipt_status TEXT; receipt_message TEXT; receipt_failure TEXT;
BEGIN
    PERFORM public.native_research_v3_edit_assert_scope_v1(
        requested_tenant_id,requested_user_id);
    IF requested_lease_owner IS NULL OR btrim(requested_lease_owner)<>requested_lease_owner OR
       requested_lease_owner='' OR requested_lease_us NOT BETWEEN 1 AND 86400000000 OR
       ((requested_receipt_provider='')<>(requested_receipt_target='')) THEN
        RAISE EXCEPTION '110: native V3 edit acquisition is invalid' USING ERRCODE='23514';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        requested_tenant_id::text||'/'||requested_user_id::text||'/'||requested_task_id,101));
    SELECT * INTO schedule_row FROM public.schedules schedule
     WHERE schedule.tenant_id=requested_tenant_id AND schedule.user_id=requested_user_id
       AND schedule.id=requested_task_id FOR UPDATE;
    SELECT * INTO operation FROM public.task_definition_edit_operations candidate
     WHERE candidate.id=requested_id AND candidate.operation_protocol=3
       AND candidate.tenant_id=requested_tenant_id AND candidate.user_id=requested_user_id
       AND candidate.target_tenant_id=requested_tenant_id
       AND candidate.target_user_id=requested_user_id
       AND candidate.task_id=requested_task_id FOR UPDATE;
    IF NOT FOUND THEN RETURN; END IF;
    now_at:=clock_timestamp();
    IF operation.status NOT IN ('pending','executing') OR operation.tombstoned_at IS NOT NULL THEN
        RETURN NEXT operation; RETURN;
    END IF;
    IF schedule_row.id IS NULL OR schedule_row.execution_mode<>'discover_at_run' OR
       (operation.phase IN ('definition_committed','temporal_target_applied','temporal_target_restored') AND
          (schedule_row.approved_definition_version IS DISTINCT FROM operation.target_definition_version OR
           schedule_row.approved_definition_digest IS DISTINCT FROM operation.target_definition_digest)) OR
       (operation.phase NOT IN ('definition_committed','temporal_target_applied','temporal_target_restored') AND
          (schedule_row.approved_definition_version IS DISTINCT FROM operation.base_definition_version OR
           schedule_row.approved_definition_digest IS DISTINCT FROM operation.base_definition_digest)) OR
       (operation.phase='proposal_sealed' AND
          (schedule_row.status<>operation.original_status OR
           schedule_row.definition_edit_operation_id IS NOT NULL OR
           schedule_row.definition_edit_fence IS NOT NULL)) OR
       (operation.phase<>'proposal_sealed' AND
          (schedule_row.status<>'paused' OR
           schedule_row.definition_edit_operation_id IS DISTINCT FROM operation.id OR
           schedule_row.definition_edit_fence IS DISTINCT FROM operation.fence)) THEN
        RAISE EXCEPTION '110: native V3 edit durable state differs' USING ERRCODE='40001';
    END IF;
    IF operation.status='pending' THEN
        IF now_at>=operation.expires_at THEN
            UPDATE public.task_definition_edit_operations SET status='expired',
                receipt_provider=requested_receipt_provider,
                receipt_target=requested_receipt_target,tombstoned_at=now_at,updated_at=now_at
             WHERE id=operation.id RETURNING * INTO operation;
            receipt_status:=CASE WHEN requested_receipt_provider='' AND requested_receipt_target=''
                                 THEN 'suppressed' ELSE 'pending' END;
            receipt_message:=CASE WHEN receipt_status='suppressed'
                                  THEN 'target-unbound-suppressed' ELSE '' END;
            receipt_failure:=CASE WHEN receipt_status='suppressed'
                                  THEN 'target_unbound' ELSE '' END;
            INSERT INTO public.task_definition_edit_receipts
                (operation_id,tenant_id,user_id,session_id,provider,target,provider_key,
                 status,next_attempt_at,provider_message_id,failure_class,sent_at)
            VALUES (operation.id,operation.tenant_id,operation.user_id,operation.session_id,
                requested_receipt_provider,requested_receipt_target,
                substr(encode(sha256(convert_to(
                  'vane/task-definition-edit-receipt/v1:'||operation.id,'UTF8')),'hex'),1,32)::uuid,
                receipt_status,now_at+interval '4 seconds',receipt_message,receipt_failure,
                CASE WHEN receipt_status='suppressed' THEN now_at END)
            ON CONFLICT (operation_id) DO NOTHING;
            RETURN NEXT operation; RETURN;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM public.memberships membership
                        JOIN public.tenants tenant ON tenant.id=membership.tenant_id
                        WHERE membership.tenant_id=requested_tenant_id
                          AND membership.user_id=requested_user_id
                          AND membership.role='owner' AND tenant.status='active'
                          AND tenant.deleted_at IS NULL) THEN
            RAISE EXCEPTION '110: native V3 edit owner is inactive' USING ERRCODE='42501';
        END IF;
        UPDATE public.task_definition_edit_operations SET status='executing',
            execution_started_at=now_at,lease_owner=requested_lease_owner,
            lease_until=now_at+(requested_lease_us*interval '1 microsecond'),
            takeover_not_before=now_at+((requested_lease_us+30000000)*interval '1 microsecond'),
            fence=1,attempt=1,receipt_provider=requested_receipt_provider,
            receipt_target=requested_receipt_target,updated_at=now_at
         WHERE id=operation.id AND operation_protocol=3 AND status='pending'
         RETURNING * INTO operation;
        RETURN NEXT operation; RETURN;
    END IF;
    IF operation.receipt_provider<>requested_receipt_provider OR
       operation.receipt_target<>requested_receipt_target THEN
        RAISE EXCEPTION '110: native V3 edit receipt differs' USING ERRCODE='40001';
    END IF;
    IF operation.lease_until>now_at OR operation.takeover_not_before>now_at THEN
        RETURN NEXT operation; RETURN;
    END IF;
    UPDATE public.task_definition_edit_operations SET lease_owner=requested_lease_owner,
        lease_until=now_at+(requested_lease_us*interval '1 microsecond'),
        takeover_not_before=now_at+((requested_lease_us+30000000)*interval '1 microsecond'),
        fence=fence+1,attempt=attempt+1,updated_at=now_at
     WHERE id=operation.id AND operation_protocol=3 AND status='executing'
       AND fence=operation.fence AND takeover_not_before<=now_at
     RETURNING * INTO operation;
    IF operation.phase<>'proposal_sealed' THEN
        UPDATE public.schedules SET definition_edit_fence=operation.fence,updated_at=now_at
         WHERE tenant_id=requested_tenant_id AND user_id=requested_user_id
           AND id=requested_task_id AND status='paused'
           AND definition_edit_operation_id=operation.id
           AND definition_edit_fence=operation.fence-1;
        IF NOT FOUND THEN RAISE EXCEPTION '110: native V3 edit takeover marker differs'
            USING ERRCODE='40001'; END IF;
    END IF;
    RETURN NEXT operation;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION acquire_native_research_v3_edit_v1(text,bigint,bigint,text,text,bigint,text,text)
    FROM PUBLIC;
ALTER FUNCTION acquire_native_research_v3_edit_v1(text,bigint,bigint,text,text,bigint,text,text)
    OWNER TO vane_native_v3_edit_coordinator;
GRANT EXECUTE ON FUNCTION acquire_native_research_v3_edit_v1(text,bigint,bigint,text,text,bigint,text,text)
    TO vane_app;

-- +goose StatementBegin
CREATE FUNCTION lock_native_research_v3_edit_lease_v1(
    requested_id TEXT,requested_tenant_id BIGINT,requested_user_id BIGINT,
    requested_task_id TEXT,requested_lease_owner TEXT,requested_fence BIGINT,
    requested_phase TEXT
) RETURNS SETOF task_definition_edit_operations
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
    PERFORM public.native_research_v3_edit_assert_scope_v1(
        requested_tenant_id,requested_user_id);
    PERFORM pg_advisory_xact_lock(hashtextextended(
        requested_tenant_id::text||'/'||requested_user_id::text||'/'||requested_task_id,101));
    RETURN QUERY SELECT operation.*
      FROM public.task_definition_edit_operations operation
     WHERE operation.id=requested_id AND operation.operation_protocol=3
       AND operation.tenant_id=requested_tenant_id AND operation.user_id=requested_user_id
       AND operation.target_tenant_id=requested_tenant_id
       AND operation.target_user_id=requested_user_id AND operation.task_id=requested_task_id
       AND operation.status='executing' AND operation.tombstoned_at IS NULL
       AND operation.phase=requested_phase AND operation.lease_owner=requested_lease_owner
       AND operation.fence=requested_fence AND operation.lease_until>clock_timestamp()
     FOR UPDATE;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION lock_native_research_v3_edit_lease_v1(text,bigint,bigint,text,text,bigint,text)
    FROM PUBLIC;
ALTER FUNCTION lock_native_research_v3_edit_lease_v1(text,bigint,bigint,text,text,bigint,text)
    OWNER TO vane_native_v3_edit_coordinator;

-- +goose StatementBegin
CREATE FUNCTION quiesce_native_research_v3_edit_v1(
    requested_id TEXT,requested_tenant_id BIGINT,requested_user_id BIGINT,
    requested_task_id TEXT,requested_lease_owner TEXT,requested_fence BIGINT,
    requested_base_action_digest TEXT,requested_authorization_digest TEXT
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE operation public.task_definition_edit_operations%ROWTYPE;
BEGIN
    SELECT * INTO operation FROM public.lock_native_research_v3_edit_lease_v1(
        requested_id,requested_tenant_id,requested_user_id,requested_task_id,
        requested_lease_owner,requested_fence,'proposal_sealed');
    IF NOT FOUND THEN
        SELECT * INTO operation FROM public.task_definition_edit_operations candidate
         WHERE candidate.id=requested_id AND candidate.operation_protocol=3
           AND candidate.tenant_id=requested_tenant_id AND candidate.user_id=requested_user_id
           AND candidate.task_id=requested_task_id AND candidate.status='executing'
           AND candidate.phase='db_quiesced' AND candidate.lease_owner=requested_lease_owner
           AND candidate.fence=requested_fence AND candidate.lease_until>clock_timestamp();
        IF NOT FOUND OR NOT EXISTS (
            SELECT 1 FROM public.schedules schedule
             WHERE schedule.tenant_id=requested_tenant_id AND schedule.user_id=requested_user_id
               AND schedule.id=requested_task_id AND schedule.status='paused'
               AND schedule.definition_edit_operation_id=requested_id
               AND schedule.definition_edit_fence=requested_fence) OR NOT EXISTS (
            SELECT 1 FROM public.research_v3_delivery_authorities authority
             WHERE authority.tenant_id=requested_tenant_id AND authority.user_id=requested_user_id
               AND authority.task_id=requested_task_id
               AND authority.definition_version=operation.base_definition_version
               AND authority.definition_digest=operation.base_definition_digest
               AND authority.target_action_digest=requested_base_action_digest
               AND authority.action_authorization_digest=requested_authorization_digest
               AND authority.status='revoked') THEN
            RAISE EXCEPTION '110: native V3 quiesce lease/replay differs' USING ERRCODE='40001';
        END IF;
        RETURN;
    END IF;
    UPDATE public.research_v3_delivery_authorities SET status='revoked',
        revoked_at=clock_timestamp()
     WHERE tenant_id=requested_tenant_id AND user_id=requested_user_id
       AND task_id=requested_task_id AND definition_version=operation.base_definition_version
       AND definition_digest=operation.base_definition_digest
       AND target_action_digest=requested_base_action_digest
       AND action_authorization_digest=requested_authorization_digest
       AND status='enabled' AND enabled_at IS NOT NULL AND revoked_at IS NULL;
    IF NOT FOUND THEN RAISE EXCEPTION '110: native V3 base authority differs'
        USING ERRCODE='40001'; END IF;
    UPDATE public.schedules SET status='paused',definition_edit_operation_id=requested_id,
        definition_edit_fence=requested_fence,updated_at=clock_timestamp()
     WHERE tenant_id=requested_tenant_id AND user_id=requested_user_id
       AND id=requested_task_id AND status=operation.original_status
       AND execution_mode='discover_at_run'
       AND approved_definition_version=operation.base_definition_version
       AND approved_definition_digest=operation.base_definition_digest
       AND definition_edit_operation_id IS NULL AND definition_edit_fence IS NULL;
    IF NOT FOUND THEN RAISE EXCEPTION '110: native V3 base schedule differs'
        USING ERRCODE='40001'; END IF;
    UPDATE public.task_definition_edit_operations SET phase='db_quiesced',
        updated_at=clock_timestamp() WHERE id=requested_id AND operation_protocol=3
        AND phase='proposal_sealed' AND lease_owner=requested_lease_owner
        AND fence=requested_fence AND lease_until>clock_timestamp();
    IF NOT FOUND THEN RAISE EXCEPTION '110: native V3 quiesce checkpoint lost lease'
        USING ERRCODE='40001'; END IF;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION quiesce_native_research_v3_edit_v1(text,bigint,bigint,text,text,bigint,text,text)
    FROM PUBLIC;
ALTER FUNCTION quiesce_native_research_v3_edit_v1(text,bigint,bigint,text,text,bigint,text,text)
    OWNER TO vane_native_v3_edit_coordinator;
GRANT EXECUTE ON FUNCTION quiesce_native_research_v3_edit_v1(text,bigint,bigint,text,text,bigint,text,text)
    TO vane_app;

-- +goose StatementBegin
CREATE FUNCTION authorize_native_research_v3_edit_remote_v1(
    requested_id TEXT,requested_tenant_id BIGINT,requested_user_id BIGINT,
    requested_task_id TEXT,requested_lease_owner TEXT,requested_fence BIGINT,
    requested_phase TEXT,requested_target_action_digest TEXT,
    requested_authorization_digest TEXT
) RETURNS SETOF task_definition_edit_operations
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE operation public.task_definition_edit_operations%ROWTYPE;
BEGIN
    IF requested_phase NOT IN ('db_quiesced','definition_committed','temporal_target_applied') THEN
        RAISE EXCEPTION '110: native V3 remote phase is invalid' USING ERRCODE='23514';
    END IF;
    SELECT * INTO operation FROM public.lock_native_research_v3_edit_lease_v1(
        requested_id,requested_tenant_id,requested_user_id,requested_task_id,
        requested_lease_owner,requested_fence,requested_phase);
    IF NOT FOUND OR NOT EXISTS (
        SELECT 1 FROM public.schedules schedule
         WHERE schedule.tenant_id=requested_tenant_id AND schedule.user_id=requested_user_id
           AND schedule.id=requested_task_id AND schedule.status='paused'
           AND schedule.definition_edit_operation_id=requested_id
           AND schedule.definition_edit_fence=requested_fence
           AND schedule.execution_mode='discover_at_run'
           AND schedule.approved_definition_version=CASE WHEN requested_phase='db_quiesced'
               THEN operation.base_definition_version ELSE operation.target_definition_version END
           AND schedule.approved_definition_digest=CASE WHEN requested_phase='db_quiesced'
               THEN operation.base_definition_digest ELSE operation.target_definition_digest END) THEN
        RAISE EXCEPTION '110: native V3 remote authority differs' USING ERRCODE='40001';
    END IF;
    IF requested_phase<>'db_quiesced' AND NOT EXISTS (
        SELECT 1 FROM public.research_v3_delivery_authorities authority
         WHERE authority.tenant_id=requested_tenant_id AND authority.user_id=requested_user_id
           AND authority.task_id=requested_task_id
           AND authority.definition_version=operation.target_definition_version
           AND authority.definition_digest=operation.target_definition_digest
           AND authority.target_action_digest=requested_target_action_digest
           AND authority.action_authorization_digest=requested_authorization_digest
           AND authority.status='staged') THEN
        RAISE EXCEPTION '110: native V3 staged authority differs' USING ERRCODE='40001';
    END IF;
    RETURN NEXT operation;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION authorize_native_research_v3_edit_remote_v1(text,bigint,bigint,text,text,bigint,text,text,text)
    FROM PUBLIC;
ALTER FUNCTION authorize_native_research_v3_edit_remote_v1(text,bigint,bigint,text,text,bigint,text,text,text)
    OWNER TO vane_native_v3_edit_coordinator;
GRANT EXECUTE ON FUNCTION authorize_native_research_v3_edit_remote_v1(text,bigint,bigint,text,text,bigint,text,text,text)
    TO vane_app;

-- +goose StatementBegin
CREATE FUNCTION checkpoint_native_research_v3_edit_v1(
    requested_id TEXT,requested_tenant_id BIGINT,requested_user_id BIGINT,
    requested_task_id TEXT,requested_lease_owner TEXT,requested_fence BIGINT,
    requested_from_phase TEXT,requested_to_phase TEXT,requested_kind TEXT,
    requested_snapshot BYTEA,requested_snapshot_digest TEXT
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE operation public.task_definition_edit_operations%ROWTYPE;
BEGIN
    IF requested_snapshot_digest<>encode(sha256(requested_snapshot),'hex') OR
       (requested_kind,requested_from_phase,requested_to_phase) NOT IN (
        ('pause','db_quiesced','temporal_base_paused'),
        ('apply','definition_committed','temporal_target_applied'),
        ('restore','temporal_target_applied','temporal_target_restored')) THEN
        RAISE EXCEPTION '110: native V3 checkpoint identity is invalid' USING ERRCODE='23514';
    END IF;
    SELECT * INTO operation FROM public.lock_native_research_v3_edit_lease_v1(
        requested_id,requested_tenant_id,requested_user_id,requested_task_id,
        requested_lease_owner,requested_fence,requested_from_phase);
    IF NOT FOUND THEN
        SELECT * INTO operation FROM public.task_definition_edit_operations candidate
         WHERE candidate.id=requested_id AND candidate.operation_protocol=3
           AND candidate.tenant_id=requested_tenant_id AND candidate.user_id=requested_user_id
           AND candidate.task_id=requested_task_id AND candidate.status='executing'
           AND candidate.phase=requested_to_phase AND candidate.lease_owner=requested_lease_owner
           AND candidate.fence=requested_fence AND candidate.lease_until>clock_timestamp();
        IF NOT FOUND OR
           (requested_kind='pause' AND (operation.pause_snapshot IS DISTINCT FROM requested_snapshot OR operation.pause_snapshot_digest<>requested_snapshot_digest)) OR
           (requested_kind='apply' AND (operation.apply_snapshot IS DISTINCT FROM requested_snapshot OR operation.apply_snapshot_digest<>requested_snapshot_digest)) OR
           (requested_kind='restore' AND (operation.restore_snapshot IS DISTINCT FROM requested_snapshot OR operation.restore_snapshot_digest<>requested_snapshot_digest)) THEN
            RAISE EXCEPTION '110: native V3 checkpoint replay differs' USING ERRCODE='40001';
        END IF;
        RETURN;
    END IF;
    IF requested_kind='pause' THEN
        UPDATE public.task_definition_edit_operations SET pause_snapshot=requested_snapshot,
            pause_snapshot_digest=requested_snapshot_digest,phase=requested_to_phase,
            updated_at=clock_timestamp() WHERE id=requested_id AND operation_protocol=3
            AND phase=requested_from_phase AND pause_snapshot IS NULL
            AND lease_owner=requested_lease_owner AND fence=requested_fence
            AND lease_until>clock_timestamp();
    ELSIF requested_kind='apply' THEN
        UPDATE public.task_definition_edit_operations SET apply_snapshot=requested_snapshot,
            apply_snapshot_digest=requested_snapshot_digest,phase=requested_to_phase,
            updated_at=clock_timestamp() WHERE id=requested_id AND operation_protocol=3
            AND phase=requested_from_phase AND apply_snapshot IS NULL
            AND lease_owner=requested_lease_owner AND fence=requested_fence
            AND lease_until>clock_timestamp();
    ELSE
        UPDATE public.task_definition_edit_operations SET restore_snapshot=requested_snapshot,
            restore_snapshot_digest=requested_snapshot_digest,phase=requested_to_phase,
            updated_at=clock_timestamp() WHERE id=requested_id AND operation_protocol=3
            AND phase=requested_from_phase AND restore_snapshot IS NULL
            AND lease_owner=requested_lease_owner AND fence=requested_fence
            AND lease_until>clock_timestamp();
    END IF;
    IF NOT FOUND THEN RAISE EXCEPTION '110: native V3 checkpoint lost lease'
        USING ERRCODE='40001'; END IF;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION checkpoint_native_research_v3_edit_v1(text,bigint,bigint,text,text,bigint,text,text,text,bytea,text)
    FROM PUBLIC;
ALTER FUNCTION checkpoint_native_research_v3_edit_v1(text,bigint,bigint,text,text,bigint,text,text,text,bytea,text)
    OWNER TO vane_native_v3_edit_coordinator;
GRANT EXECUTE ON FUNCTION checkpoint_native_research_v3_edit_v1(text,bigint,bigint,text,text,bigint,text,text,text,bytea,text)
    TO vane_app;

-- +goose StatementBegin
CREATE FUNCTION commit_native_research_v3_edit_definition_v1(
    requested_id TEXT,requested_tenant_id BIGINT,requested_user_id BIGINT,
    requested_task_id TEXT,requested_lease_owner TEXT,requested_fence BIGINT,
    requested_base_action_digest TEXT,requested_target_action_digest TEXT,
    requested_authorization_digest TEXT
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE operation public.task_definition_edit_operations%ROWTYPE;
        definition_json JSONB; base_generation BIGINT;
BEGIN
    SELECT * INTO operation FROM public.lock_native_research_v3_edit_lease_v1(
        requested_id,requested_tenant_id,requested_user_id,requested_task_id,
        requested_lease_owner,requested_fence,'temporal_base_paused');
    IF NOT FOUND THEN
        SELECT * INTO operation FROM public.task_definition_edit_operations candidate
         WHERE candidate.id=requested_id AND candidate.operation_protocol=3
           AND candidate.tenant_id=requested_tenant_id AND candidate.user_id=requested_user_id
           AND candidate.task_id=requested_task_id AND candidate.status='executing'
           AND candidate.phase IN ('definition_committed','temporal_target_applied','temporal_target_restored')
           AND candidate.lease_owner=requested_lease_owner AND candidate.fence=requested_fence
           AND candidate.lease_until>clock_timestamp();
        IF NOT FOUND OR NOT EXISTS (
            SELECT 1 FROM public.task_approved_definition_versions definition
             WHERE definition.tenant_id=requested_tenant_id AND definition.user_id=requested_user_id
               AND definition.task_id=requested_task_id
               AND definition.version=operation.target_definition_version
               AND definition.definition_digest=operation.target_definition_digest
               AND definition.payload=operation.target_definition
               AND definition.operation_ref='definition-edit-v3/'||requested_id) OR NOT EXISTS (
            SELECT 1 FROM public.research_v3_delivery_authorities authority
             WHERE authority.tenant_id=requested_tenant_id AND authority.user_id=requested_user_id
               AND authority.task_id=requested_task_id
               AND authority.definition_version=operation.target_definition_version
               AND authority.definition_digest=operation.target_definition_digest
               AND authority.target_action_digest=requested_target_action_digest
               AND authority.action_authorization_digest=requested_authorization_digest
               AND authority.status='staged') THEN
            RAISE EXCEPTION '110: native V3 definition replay differs' USING ERRCODE='40001';
        END IF;
        RETURN;
    END IF;
    BEGIN definition_json:=convert_from(operation.target_definition,'UTF8')::jsonb;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION '110: native V3 target definition is invalid' USING ERRCODE='23514';
    END;
    IF definition_json->>'schema_version' IS DISTINCT FROM 'vane.task-approved-definition/v3' OR
       definition_json->>'execution_mode' IS DISTINCT FROM 'discover_at_run' OR
       COALESCE((definition_json->>'tenant_id')::bigint,0)<>requested_tenant_id OR
       COALESCE((definition_json->>'user_id')::bigint,0)<>requested_user_id OR
       definition_json->>'task_id' IS DISTINCT FROM requested_task_id OR
       encode(sha256(operation.target_definition),'hex')<>operation.target_definition_digest THEN
        RAISE EXCEPTION '110: native V3 target definition differs' USING ERRCODE='23514';
    END IF;
    SELECT authority.generation INTO base_generation
      FROM public.research_v3_delivery_authorities authority
     WHERE authority.tenant_id=requested_tenant_id AND authority.user_id=requested_user_id
       AND authority.task_id=requested_task_id
       AND authority.definition_version=operation.base_definition_version
       AND authority.definition_digest=operation.base_definition_digest
       AND authority.target_action_digest=requested_base_action_digest
       AND authority.action_authorization_digest=requested_authorization_digest
       AND authority.status='revoked' FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION '110: native V3 revoked base authority differs'
        USING ERRCODE='40001'; END IF;
    INSERT INTO public.task_approved_definition_versions
        (tenant_id,user_id,task_id,version,schema_version,execution_mode,
         definition_digest,payload,operation_ref)
    VALUES (requested_tenant_id,requested_user_id,requested_task_id,
        operation.target_definition_version,'vane.task-approved-definition/v3',
        'discover_at_run',operation.target_definition_digest,
        operation.target_definition,'definition-edit-v3/'||requested_id);
    UPDATE public.schedules SET nl_description=definition_json->>'task_name',
        spec_json=definition_json->'spec_json',
        approved_definition_version=operation.target_definition_version,
        approved_definition_digest=operation.target_definition_digest,
        updated_at=clock_timestamp()
     WHERE tenant_id=requested_tenant_id AND user_id=requested_user_id
       AND id=requested_task_id AND status='paused' AND execution_mode='discover_at_run'
       AND approved_definition_version=operation.base_definition_version
       AND approved_definition_digest=operation.base_definition_digest
       AND definition_edit_operation_id=requested_id
       AND definition_edit_fence=requested_fence AND scope_json='{}'::jsonb;
    IF NOT FOUND THEN RAISE EXCEPTION '110: native V3 definition head differs'
        USING ERRCODE='40001'; END IF;
    UPDATE public.schedule_playbooks SET content=definition_json->>'task_manual'
     WHERE schedule_id=requested_task_id AND fetch_plan='{}'::jsonb;
    IF NOT FOUND THEN RAISE EXCEPTION '110: native V3 task manual differs'
        USING ERRCODE='40001'; END IF;
    INSERT INTO public.research_v3_delivery_authorities
        (tenant_id,user_id,task_id,generation,definition_version,definition_digest,
         target_action_digest,action_authorization_digest,status)
    VALUES (requested_tenant_id,requested_user_id,requested_task_id,
        base_generation+1,operation.target_definition_version,
        operation.target_definition_digest,requested_target_action_digest,
        requested_authorization_digest,'staged');
    UPDATE public.task_definition_edit_operations SET phase='definition_committed',
        updated_at=clock_timestamp() WHERE id=requested_id AND operation_protocol=3
        AND phase='temporal_base_paused' AND lease_owner=requested_lease_owner
        AND fence=requested_fence AND lease_until>clock_timestamp();
    IF NOT FOUND THEN RAISE EXCEPTION '110: native V3 definition checkpoint lost lease'
        USING ERRCODE='40001'; END IF;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION commit_native_research_v3_edit_definition_v1(text,bigint,bigint,text,text,bigint,text,text,text)
    FROM PUBLIC;
ALTER FUNCTION commit_native_research_v3_edit_definition_v1(text,bigint,bigint,text,text,bigint,text,text,text)
    OWNER TO vane_native_v3_edit_coordinator;
GRANT EXECUTE ON FUNCTION commit_native_research_v3_edit_definition_v1(text,bigint,bigint,text,text,bigint,text,text,text)
    TO vane_app;

-- +goose StatementBegin
CREATE FUNCTION finish_native_research_v3_edit_v1(
    requested_action TEXT,requested_id TEXT,requested_tenant_id BIGINT,
    requested_user_id BIGINT,requested_task_id TEXT,requested_lease_owner TEXT,
    requested_fence BIGINT,requested_target_action_digest TEXT,
    requested_authorization_digest TEXT,requested_result JSONB,
    requested_error_code TEXT,requested_error_message TEXT
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE operation public.task_definition_edit_operations%ROWTYPE;
        receipt_status TEXT; receipt_message TEXT; receipt_failure TEXT;
BEGIN
    IF requested_action NOT IN ('complete','block') THEN
        RAISE EXCEPTION '110: native V3 finish action is invalid' USING ERRCODE='23514';
    END IF;
    PERFORM public.native_research_v3_edit_assert_scope_v1(
        requested_tenant_id,requested_user_id);
    PERFORM pg_advisory_xact_lock(hashtextextended(
        requested_tenant_id::text||'/'||requested_user_id::text||'/'||requested_task_id,101));
    SELECT * INTO operation FROM public.task_definition_edit_operations candidate
     WHERE candidate.id=requested_id AND candidate.operation_protocol=3
       AND candidate.tenant_id=requested_tenant_id AND candidate.user_id=requested_user_id
       AND candidate.task_id=requested_task_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION '110: native V3 finish operation is missing'
        USING ERRCODE='40001'; END IF;
    IF requested_action='complete' AND operation.status='completed' AND
       operation.tombstoned_at IS NOT NULL THEN
        IF operation.phase<>'temporal_target_restored' OR
           operation.fence<>requested_fence OR operation.lease_owner<>'' OR
           operation.lease_until IS NOT NULL OR operation.takeover_not_before IS NOT NULL OR
           operation.result IS DISTINCT FROM requested_result OR operation.error_code<>'' OR
           operation.error_message<>'' OR NOT EXISTS (
            SELECT 1 FROM public.schedules schedule
             WHERE schedule.tenant_id=requested_tenant_id AND schedule.user_id=requested_user_id
               AND schedule.id=requested_task_id AND schedule.status=operation.original_status
               AND schedule.execution_mode='discover_at_run'
               AND schedule.approved_definition_version=operation.target_definition_version
               AND schedule.approved_definition_digest=operation.target_definition_digest
               AND schedule.definition_edit_operation_id IS NULL
               AND schedule.definition_edit_fence IS NULL) OR NOT EXISTS (
            SELECT 1 FROM public.research_v3_delivery_authorities authority
             WHERE authority.tenant_id=requested_tenant_id AND authority.user_id=requested_user_id
               AND authority.task_id=requested_task_id
               AND authority.definition_version=operation.target_definition_version
               AND authority.definition_digest=operation.target_definition_digest
               AND authority.target_action_digest=requested_target_action_digest
               AND authority.action_authorization_digest=requested_authorization_digest
               AND authority.status='enabled') OR NOT EXISTS (
            SELECT 1 FROM public.task_definition_edit_receipts receipt
             WHERE receipt.operation_id=operation.id AND receipt.tenant_id=operation.tenant_id
               AND receipt.user_id=operation.user_id) THEN
            RAISE EXCEPTION '110: native V3 completion replay differs' USING ERRCODE='40001';
        END IF;
        RETURN;
    END IF;
    IF requested_action='block' AND operation.status='blocked' AND
       operation.tombstoned_at IS NOT NULL THEN
        IF operation.fence<>requested_fence OR operation.lease_owner<>'' OR
           operation.lease_until IS NOT NULL OR operation.takeover_not_before IS NOT NULL OR
           operation.result IS NOT NULL OR operation.error_code<>requested_error_code OR
           operation.error_message<>requested_error_message OR NOT EXISTS (
            SELECT 1 FROM public.task_definition_edit_receipts receipt
             WHERE receipt.operation_id=operation.id AND receipt.tenant_id=operation.tenant_id
               AND receipt.user_id=operation.user_id) THEN
            RAISE EXCEPTION '110: native V3 block replay differs' USING ERRCODE='40001';
        END IF;
        RETURN;
    END IF;
    IF operation.status<>'executing' OR operation.tombstoned_at IS NOT NULL OR
       operation.lease_owner<>requested_lease_owner OR operation.fence<>requested_fence OR
       operation.lease_until<=clock_timestamp() THEN
        RAISE EXCEPTION '110: native V3 finish lease differs' USING ERRCODE='40001';
    END IF;
    IF requested_action='complete' THEN
        IF operation.phase<>'temporal_target_restored' OR requested_result IS NULL OR
           requested_error_code<>'' OR requested_error_message<>'' OR NOT EXISTS (
            SELECT 1 FROM public.research_v3_delivery_authorities authority
             WHERE authority.tenant_id=requested_tenant_id AND authority.user_id=requested_user_id
               AND authority.task_id=requested_task_id
               AND authority.definition_version=operation.target_definition_version
               AND authority.definition_digest=operation.target_definition_digest
               AND authority.target_action_digest=requested_target_action_digest
               AND authority.action_authorization_digest=requested_authorization_digest
               AND authority.status='staged') THEN
            RAISE EXCEPTION '110: native V3 completion evidence differs' USING ERRCODE='40001';
        END IF;
        UPDATE public.schedules SET status=operation.original_status,
            definition_edit_operation_id=NULL,definition_edit_fence=NULL,
            updated_at=clock_timestamp()
         WHERE tenant_id=requested_tenant_id AND user_id=requested_user_id
           AND id=requested_task_id AND status='paused'
           AND definition_edit_operation_id=requested_id
           AND definition_edit_fence=requested_fence
           AND approved_definition_version=operation.target_definition_version
           AND approved_definition_digest=operation.target_definition_digest;
        IF NOT FOUND THEN RAISE EXCEPTION '110: native V3 completion schedule differs'
            USING ERRCODE='40001'; END IF;
        UPDATE public.research_v3_delivery_authorities SET status='enabled',
            enabled_at=clock_timestamp()
         WHERE tenant_id=requested_tenant_id AND user_id=requested_user_id
           AND task_id=requested_task_id
           AND definition_version=operation.target_definition_version
           AND definition_digest=operation.target_definition_digest
           AND target_action_digest=requested_target_action_digest
           AND action_authorization_digest=requested_authorization_digest
           AND status='staged' AND enabled_at IS NULL AND revoked_at IS NULL;
        IF NOT FOUND THEN RAISE EXCEPTION '110: native V3 completion authority differs'
            USING ERRCODE='40001'; END IF;
        UPDATE public.task_definition_edit_operations SET status='completed',
            result=requested_result,error_code='',error_message='',lease_owner='',
            lease_until=NULL,takeover_not_before=NULL,tombstoned_at=clock_timestamp(),
            updated_at=clock_timestamp() WHERE id=requested_id AND operation_protocol=3
            AND status='executing' AND phase='temporal_target_restored'
            AND lease_owner=requested_lease_owner AND fence=requested_fence
            AND lease_until>clock_timestamp();
    ELSE
        IF requested_error_code='' OR requested_error_message='' OR requested_result IS NOT NULL THEN
            RAISE EXCEPTION '110: native V3 block evidence differs' USING ERRCODE='23514';
        END IF;
        UPDATE public.task_definition_edit_operations SET status='blocked',result=NULL,
            error_code=requested_error_code,error_message=requested_error_message,
            lease_owner='',lease_until=NULL,takeover_not_before=NULL,
            tombstoned_at=clock_timestamp(),updated_at=clock_timestamp()
         WHERE id=requested_id AND operation_protocol=3 AND status='executing'
           AND lease_owner=requested_lease_owner AND fence=requested_fence
           AND lease_until>clock_timestamp();
    END IF;
    IF NOT FOUND THEN RAISE EXCEPTION '110: native V3 finish checkpoint lost lease'
        USING ERRCODE='40001'; END IF;
    receipt_status:=CASE WHEN operation.receipt_provider='' AND operation.receipt_target=''
                         THEN 'suppressed' ELSE 'pending' END;
    receipt_message:=CASE WHEN receipt_status='suppressed'
                          THEN 'target-unbound-suppressed' ELSE '' END;
    receipt_failure:=CASE WHEN receipt_status='suppressed'
                          THEN 'target_unbound' ELSE '' END;
    INSERT INTO public.task_definition_edit_receipts
        (operation_id,tenant_id,user_id,session_id,provider,target,provider_key,
         status,next_attempt_at,provider_message_id,failure_class,sent_at)
    VALUES (operation.id,operation.tenant_id,operation.user_id,operation.session_id,
        operation.receipt_provider,operation.receipt_target,
        substr(encode(sha256(convert_to(
          'vane/task-definition-edit-receipt/v1:'||operation.id,'UTF8')),'hex'),1,32)::uuid,
        receipt_status,clock_timestamp()+interval '4 seconds',receipt_message,
        receipt_failure,CASE WHEN receipt_status='suppressed' THEN clock_timestamp() END)
    ON CONFLICT (operation_id) DO NOTHING;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION finish_native_research_v3_edit_v1(text,text,bigint,bigint,text,text,bigint,text,text,jsonb,text,text)
    FROM PUBLIC;
ALTER FUNCTION finish_native_research_v3_edit_v1(text,text,bigint,bigint,text,text,bigint,text,text,jsonb,text,text)
    OWNER TO vane_native_v3_edit_coordinator;
GRANT EXECUTE ON FUNCTION finish_native_research_v3_edit_v1(text,text,bigint,bigint,text,text,bigint,text,text,jsonb,text,text)
    TO vane_app;

-- +goose Down

-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM task_definition_edit_operations
                WHERE operation_protocol=3) THEN
        RAISE EXCEPTION '110: refusing downgrade while native V3 edits exist';
    END IF;
END $$;
-- +goose StatementEnd

SET LOCAL ROLE vane_native_v3_edit_recovery_coordinator;
DROP FUNCTION claim_stale_native_research_v3_edit_v1(timestamptz,text,bigint);
RESET ROLE;

SET LOCAL ROLE vane_native_v3_edit_coordinator;
DROP FUNCTION finish_native_research_v3_edit_v1(text,text,bigint,bigint,text,text,bigint,text,text,jsonb,text,text);
DROP FUNCTION commit_native_research_v3_edit_definition_v1(text,bigint,bigint,text,text,bigint,text,text,text);
DROP FUNCTION checkpoint_native_research_v3_edit_v1(text,bigint,bigint,text,text,bigint,text,text,text,bytea,text);
DROP FUNCTION authorize_native_research_v3_edit_remote_v1(text,bigint,bigint,text,text,bigint,text,text,text);
DROP FUNCTION quiesce_native_research_v3_edit_v1(text,bigint,bigint,text,text,bigint,text,text);
DROP FUNCTION lock_native_research_v3_edit_lease_v1(text,bigint,bigint,text,text,bigint,text);
DROP FUNCTION acquire_native_research_v3_edit_v1(text,bigint,bigint,text,text,bigint,text,text);
DROP FUNCTION seal_native_research_v3_edit_v1(text,bigint,bigint,text,bigint,timestamptz,text,bigint,text,bytea,bigint,text,bytea,bytea,text,bytea,text,bytea,text,bytea,text,text);
DROP FUNCTION load_native_research_v3_edit_basis_v1(bigint,bigint,text);
DROP FUNCTION load_native_research_v3_edit_operation_v1(text,bigint,bigint,text);
DROP FUNCTION native_research_v3_edit_assert_scope_v1(bigint,bigint);
RESET ROLE;

DROP POLICY native_v3_edit_authority_user_isolation
    ON research_v3_delivery_authorities;
DROP POLICY native_v3_edit_protocol_isolation
    ON task_definition_edit_operations;
DROP POLICY legacy_definition_edit_protocol_isolation
    ON task_definition_edit_operations;

REVOKE ALL ON research_v3_delivery_authorities
    FROM vane_native_v3_edit_coordinator;
REVOKE SELECT ON task_creation_operations FROM vane_native_v3_edit_coordinator;
REVOKE ALL ON task_definition_edit_receipts FROM vane_native_v3_edit_coordinator;
REVOKE ALL ON SEQUENCE task_definition_edit_receipts_id_seq
    FROM vane_native_v3_edit_coordinator;
REVOKE ALL ON task_definition_edit_operations FROM vane_native_v3_edit_coordinator;
REVOKE ALL ON task_approved_definition_versions FROM vane_native_v3_edit_coordinator;
REVOKE ALL ON schedule_playbooks FROM vane_native_v3_edit_coordinator;
REVOKE ALL ON schedules FROM vane_native_v3_edit_coordinator;
REVOKE SELECT ON agent_sessions FROM vane_native_v3_edit_coordinator;
REVOKE SELECT ON memberships FROM vane_native_v3_edit_coordinator;
REVOKE SELECT ON tenants FROM vane_native_v3_edit_coordinator;
REVOKE USAGE ON SCHEMA public FROM vane_native_v3_edit_coordinator;

REVOKE ALL ON schedules FROM vane_native_v3_edit_recovery_coordinator;
REVOKE ALL ON task_definition_edit_operations
    FROM vane_native_v3_edit_recovery_coordinator;
REVOKE USAGE ON SCHEMA public FROM vane_native_v3_edit_recovery_coordinator;
REVOKE USAGE ON SCHEMA public FROM vane_native_v3_edit_recovery;

DROP INDEX idx_task_definition_edit_operations_protocol_recovery;
ALTER TABLE task_definition_edit_operations
    DROP CONSTRAINT task_definition_edit_operation_protocol_valid;
ALTER TABLE task_definition_edit_operations
    DROP COLUMN operation_protocol;
