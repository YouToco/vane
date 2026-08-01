-- 098: owner-only provisioning control plane for the long-lived Vane server.
--
-- PostgreSQL roles and memberships are cluster-global. Schema migration must
-- therefore not create or grant vane_server_runtime: doing so would make a
-- later fresh database in the same cluster fail older restricted-role guards.
-- The production migration command explicitly provisions only after every
-- schema migration in the target database has succeeded.

-- +goose Up

-- +goose StatementBegin
CREATE FUNCTION provision_vane_server_runtime_v1() RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,pg_temp AS $$
DECLARE
    owner_name TEXT := current_user;
    extra_roles TEXT[];
    reverse_members TEXT[];
BEGIN
    IF session_user <> owner_name THEN
        RAISE EXCEPTION '098: only the direct migration owner may provision server runtime'
            USING ERRCODE='42501';
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock(6215335020355474248);

    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='vane_server_runtime'
    ) THEN
        BEGIN
            EXECUTE 'CREATE ROLE vane_server_runtime NOLOGIN NOINHERIT NOSUPERUSER '
                 || 'NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS';
        EXCEPTION
            WHEN duplicate_object OR unique_violation THEN NULL;
        END;
    END IF;

    -- Preserve LOGIN/password on an already deployed runtime, but force every
    -- authority-bearing attribute back to the safe boundary.
    EXECUTE 'ALTER ROLE vane_server_runtime NOINHERIT NOSUPERUSER NOCREATEDB '
         || 'NOCREATEROLE NOREPLICATION NOBYPASSRLS';
    EXECUTE 'ALTER ROLE vane_server_runtime RESET ALL';

    EXECUTE 'REVOKE vane_research_runtime,vane_research_v3_executor,'
         || 'vane_research_llm_gateway_runtime,vane_research_llm_gateway '
         || 'FROM vane_server_runtime';
    EXECUTE 'REVOKE vane_server_runtime FROM vane_research_runtime,'
         || 'vane_research_v3_executor,vane_research_llm_gateway_runtime,'
         || 'vane_research_llm_gateway';

    EXECUTE 'GRANT vane_app,vane_edit_coordinator,vane_edit_receipt,'
         || 'vane_snapshot_cutover_operator,vane_push_effect_coordinator,'
         || 'vane_push_effect_receipt,vane_push_effect_operator,'
         || 'vane_push_batch_authority,vane_schedule_commander,'
         || 'vane_agent_session_projection_operator,'
         || 'vane_agent_session_fact_projector,vane_profile_editor,'
         || 'vane_profile_claim_editor,vane_profile_epoch_editor,'
         || 'vane_brief_writer,vane_brief_reader,'
         || 'vane_brief_synthesis_writer,vane_brief_synthesis_recovery,'
         || 'vane_periodic_brief_writer,vane_run_outcome_recovery,'
         || 'vane_intelligence_reader TO vane_server_runtime';

    SELECT pg_catalog.array_agg(candidate.rolname ORDER BY candidate.rolname)
      INTO extra_roles
      FROM pg_catalog.pg_roles candidate
     WHERE candidate.rolname <> 'vane_server_runtime'
       AND pg_catalog.pg_has_role(
               'vane_server_runtime',candidate.oid,'MEMBER')
       AND candidate.rolname <> ALL(ARRAY[
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
       ]::TEXT[]);
    IF extra_roles IS NOT NULL THEN
        RAISE EXCEPTION '098: server runtime has unexpected reachable roles: %',
            extra_roles USING ERRCODE='42501';
    END IF;

    SELECT pg_catalog.array_agg(member_role.rolname ORDER BY member_role.rolname)
      INTO reverse_members
      FROM pg_catalog.pg_auth_members membership
      JOIN pg_catalog.pg_roles granted_role
        ON granted_role.oid=membership.roleid
      JOIN pg_catalog.pg_roles member_role
        ON member_role.oid=membership.member
     WHERE granted_role.rolname='vane_server_runtime';
    IF reverse_members IS NOT NULL THEN
        RAISE EXCEPTION '098: other roles can enter server runtime: %',
            reverse_members USING ERRCODE='42501';
    END IF;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION provision_vane_server_runtime_v1() FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION deprovision_vane_server_runtime_v1() RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,pg_temp AS $$
DECLARE
    owner_name TEXT := current_user;
    runtime_login BOOLEAN;
    direct_roles TEXT[];
    expected_roles TEXT[];
    extra_roles TEXT[];
    reverse_members TEXT[];
BEGIN
    IF session_user <> owner_name THEN
        RAISE EXCEPTION '098: only the direct migration owner may deprovision server runtime'
            USING ERRCODE='42501';
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock(6215335020355474248);

    SELECT rolcanlogin INTO runtime_login
      FROM pg_catalog.pg_roles WHERE rolname='vane_server_runtime';
    IF runtime_login IS NULL THEN
        RETURN;
    END IF;
    IF runtime_login THEN
        RAISE EXCEPTION '098: refusing deprovision while vane_server_runtime can login'
            USING ERRCODE='55000';
    END IF;

    SELECT pg_catalog.array_agg(granted_role.rolname ORDER BY granted_role.rolname)
      INTO direct_roles
      FROM pg_catalog.pg_auth_members membership
      JOIN pg_catalog.pg_roles granted_role
        ON granted_role.oid=membership.roleid
      JOIN pg_catalog.pg_roles member_role
        ON member_role.oid=membership.member
     WHERE member_role.rolname='vane_server_runtime';
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
        RAISE EXCEPTION
            '098: refusing non-exact server runtime memberships: got %, expected %',
            direct_roles,expected_roles USING ERRCODE='42501';
    END IF;

    SELECT pg_catalog.array_agg(candidate.rolname ORDER BY candidate.rolname)
      INTO extra_roles
      FROM pg_catalog.pg_roles candidate
     WHERE candidate.rolname <> 'vane_server_runtime'
       AND pg_catalog.pg_has_role(
               'vane_server_runtime',candidate.oid,'MEMBER')
       AND candidate.rolname <> ALL(expected_roles);
    IF extra_roles IS NOT NULL THEN
        RAISE EXCEPTION '098: refusing unexpected reachable roles: %',
            extra_roles USING ERRCODE='42501';
    END IF;

    SELECT pg_catalog.array_agg(member_role.rolname ORDER BY member_role.rolname)
      INTO reverse_members
      FROM pg_catalog.pg_auth_members membership
      JOIN pg_catalog.pg_roles granted_role
        ON granted_role.oid=membership.roleid
      JOIN pg_catalog.pg_roles member_role
        ON member_role.oid=membership.member
     WHERE granted_role.rolname='vane_server_runtime';
    IF reverse_members IS NOT NULL THEN
        RAISE EXCEPTION '098: refusing reverse server runtime members: %',
            reverse_members USING ERRCODE='42501';
    END IF;

    -- Revoke only the exact authority installed by provision. DROP ROLE must
    -- then prove there is no ownership, direct grant, or unexpected membership
    -- dependency anywhere in the cluster; never hide drift with DROP OWNED.
    EXECUTE 'REVOKE vane_app,vane_edit_coordinator,vane_edit_receipt,'
         || 'vane_snapshot_cutover_operator,vane_push_effect_coordinator,'
         || 'vane_push_effect_receipt,vane_push_effect_operator,'
         || 'vane_push_batch_authority,vane_schedule_commander,'
         || 'vane_agent_session_projection_operator,'
         || 'vane_agent_session_fact_projector,vane_profile_editor,'
         || 'vane_profile_claim_editor,vane_profile_epoch_editor,'
         || 'vane_brief_writer,vane_brief_reader,'
         || 'vane_brief_synthesis_writer,vane_brief_synthesis_recovery,'
         || 'vane_periodic_brief_writer,vane_run_outcome_recovery,'
         || 'vane_intelligence_reader FROM vane_server_runtime';
    EXECUTE 'DROP ROLE vane_server_runtime';
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION deprovision_vane_server_runtime_v1() FROM PUBLIC;

-- +goose Down

-- A provisioned cluster identity must be explicitly deprovisioned while this
-- owner-only function still exists. Schema rollback never silently changes a
-- live cluster principal.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='vane_server_runtime'
    ) THEN
        RAISE EXCEPTION
            '098: deprovision vane_server_runtime before schema downgrade';
    END IF;
END
$$;
-- +goose StatementEnd

DROP FUNCTION deprovision_vane_server_runtime_v1();
DROP FUNCTION provision_vane_server_runtime_v1();
