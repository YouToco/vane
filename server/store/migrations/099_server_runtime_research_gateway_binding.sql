-- 099: exact-scope server-runtime binder for the process-isolated research
-- LLM gateway. The long-lived server receives no SELECT grant on research
-- capability, reservation, frozen-request or settlement tables. This one
-- owner-defined function returns only a request digest and capability hash;
-- the bearer is reconstructed and verified from the server's in-memory key.

-- +goose Up

-- +goose StatementBegin
CREATE FUNCTION bind_research_llm_process_gateway_v1(
    requested_tenant_id BIGINT,requested_user_id BIGINT,requested_task_id TEXT,
    requested_workflow_id TEXT,requested_temporal_run_id TEXT,
    requested_snapshot_id BIGINT,requested_reference_digest TEXT,
    requested_reservation_id BIGINT
) RETURNS TABLE(
    out_reservation_id BIGINT,out_request_digest TEXT,out_capability_key_id TEXT,
    out_capability_generation INTEGER,out_capability_hash BYTEA
)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,pg_temp AS $$
BEGIN
    IF session_user<>'vane_server_runtime' OR requested_tenant_id<=0 OR
       requested_user_id<=0 OR requested_task_id IS NULL OR
       requested_task_id='' OR length(requested_task_id)>255 OR
       requested_workflow_id IS NULL OR requested_workflow_id='' OR
       length(requested_workflow_id)>512 OR requested_temporal_run_id IS NULL OR
       requested_temporal_run_id='' OR length(requested_temporal_run_id)>512 OR
       requested_snapshot_id<=0 OR requested_reservation_id<=0 OR
       requested_reference_digest !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION '099: invalid process gateway binding scope'
            USING ERRCODE='22023';
    END IF;

    RETURN QUERY
    WITH latest_capability AS MATERIALIZED (
        SELECT capability.id,capability.run_snapshot_id,
               capability.tenant_id,capability.user_id,capability.task_id,
               capability.temporal_workflow_id,capability.temporal_run_id,
               capability.reference_digest,capability.key_id,
               capability.generation,capability.capability_hash,
               capability.not_after,capability.revoked_at
          FROM public.research_run_capabilities capability
         WHERE capability.run_snapshot_id=requested_snapshot_id
         ORDER BY capability.generation DESC
         LIMIT 1
    )
    SELECT reservation.id,reservation.request_digest,capability.key_id,
           capability.generation,capability.capability_hash
      FROM latest_capability capability
      JOIN public.task_run_snapshots snapshot
        ON snapshot.id=capability.run_snapshot_id
       AND snapshot.tenant_id=capability.tenant_id
       AND snapshot.user_id=capability.user_id
       AND snapshot.task_id=capability.task_id
       AND snapshot.temporal_workflow_id=capability.temporal_workflow_id
       AND snapshot.temporal_run_id=capability.temporal_run_id
       AND snapshot.reference_digest=capability.reference_digest
      JOIN public.research_run_llm_spend_reservations reservation
        ON reservation.run_snapshot_id=snapshot.id
       AND reservation.tenant_id=snapshot.tenant_id
       AND reservation.user_id=snapshot.user_id
       AND reservation.task_id=snapshot.task_id
      JOIN public.research_llm_gateway_frozen_requests frozen
        ON frozen.reservation_id=reservation.id
       AND frozen.request_digest=reservation.request_digest
     WHERE capability.tenant_id=requested_tenant_id
       AND capability.user_id=requested_user_id
       AND capability.task_id=requested_task_id
       AND capability.temporal_workflow_id=requested_workflow_id
       AND capability.temporal_run_id=requested_temporal_run_id
       AND capability.reference_digest=requested_reference_digest
       AND capability.revoked_at IS NULL
       AND capability.not_after>statement_timestamp()
       AND reservation.id=requested_reservation_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION '099: process gateway binding is unavailable'
            USING ERRCODE='42501';
    END IF;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION bind_research_llm_process_gateway_v1(
    BIGINT,BIGINT,TEXT,TEXT,TEXT,BIGINT,TEXT,BIGINT) FROM PUBLIC;

-- Cluster roles are intentionally not created or changed by ordinary schema
-- migration. The owner-only provision hook is called after all migrations.
-- +goose StatementBegin
CREATE FUNCTION provision_vane_server_runtime_research_binder_v1() RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,pg_temp AS $$
DECLARE owner_name TEXT := current_user; runtime_safe BOOLEAN;
BEGIN
    IF session_user<>owner_name THEN
        RAISE EXCEPTION '099: only the direct migration owner may provision binder'
            USING ERRCODE='42501';
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock(6215335020355474248);
    SELECT NOT rolsuper AND NOT rolbypassrls AND NOT rolcreatedb AND
           NOT rolcreaterole AND NOT rolreplication AND NOT rolinherit
      INTO runtime_safe
      FROM pg_catalog.pg_roles WHERE rolname='vane_server_runtime';
    IF runtime_safe IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION '099: server runtime is absent or unsafe'
            USING ERRCODE='42501';
    END IF;
    EXECUTE 'GRANT EXECUTE ON FUNCTION public.'
         || 'bind_research_llm_process_gateway_v1('
         || 'BIGINT,BIGINT,TEXT,TEXT,TEXT,BIGINT,TEXT,BIGINT) '
         || 'TO vane_server_runtime';
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION provision_vane_server_runtime_research_binder_v1()
    FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION deprovision_vane_server_runtime_research_binder_v1() RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,pg_temp AS $$
DECLARE owner_name TEXT := current_user; runtime_login BOOLEAN;
BEGIN
    IF session_user<>owner_name THEN
        RAISE EXCEPTION '099: only the direct migration owner may deprovision binder'
            USING ERRCODE='42501';
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock(6215335020355474248);
    SELECT rolcanlogin INTO runtime_login FROM pg_catalog.pg_roles
     WHERE rolname='vane_server_runtime';
    IF runtime_login THEN
        RAISE EXCEPTION '099: refusing binder deprovision while server runtime can login'
            USING ERRCODE='55000';
    END IF;
    IF runtime_login IS NOT NULL THEN
        EXECUTE 'REVOKE EXECUTE ON FUNCTION public.'
             || 'bind_research_llm_process_gateway_v1('
             || 'BIGINT,BIGINT,TEXT,TEXT,TEXT,BIGINT,TEXT,BIGINT) '
             || 'FROM vane_server_runtime';
    END IF;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION deprovision_vane_server_runtime_research_binder_v1()
    FROM PUBLIC;

-- +goose Down

-- A live server role may still hold the only direct EXECUTE grant. Require an
-- explicit owner-driven deprovision before removing its audited boundary.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles
                WHERE rolname='vane_server_runtime') THEN
        RAISE EXCEPTION
            '099: deprovision vane_server_runtime before schema downgrade';
    END IF;
END
$$;
-- +goose StatementEnd

DROP FUNCTION deprovision_vane_server_runtime_research_binder_v1();
DROP FUNCTION provision_vane_server_runtime_research_binder_v1();
DROP FUNCTION bind_research_llm_process_gateway_v1(
    BIGINT,BIGINT,TEXT,TEXT,TEXT,BIGINT,TEXT,BIGINT);
