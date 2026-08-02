-- 105: authorize first Tool effects for an exact delivery-dark V3 shadow.
--
-- Migration 102 admits a shadow snapshot from the prepared V3 sidecar while
-- the production Schedule deliberately remains on its compiled head.  The 095
-- Tool admission fence still required the live Schedule itself to be
-- discover_at_run, making pre-cutover shadow verification impossible.  Keep
-- the formal path byte-for-byte, and add one orthogonal shadow path bound to
-- the same prepared head and journal used by snapshot admission.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE research_run_steps,research_run_step_spend_reservations,
           research_run_step_spend_settlements,tenant_quota,
           research_run_llm_spend_reservations,
           research_run_llm_spend_settlements,
           research_v3_prepared_definition_heads,
           research_v3_definition_prepare_operations IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION admit_research_run_tool_step_cap_v1(
    requested_run_snapshot_id BIGINT,
    requested_plan_id BIGINT,
    requested_step_ordinal INTEGER
) RETURNS TABLE (
    out_started_step_id BIGINT,
    out_reservation_id BIGINT,
    out_first_writer BOOLEAN,
    out_plan_digest TEXT,
    out_invocation_id TEXT,
    out_tool_name TEXT,
    out_arguments BYTEA,
    out_request_digest TEXT,
    out_reserved_quota_units DOUBLE PRECISION,
    out_reserved_cost_micro_usd BIGINT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    capability_row RECORD;
    snapshot_row RECORD;
    plan_row RECORD;
    snapshot_json JSONB;
    plan_json JSONB;
    step_json JSONB;
    grant_json JSONB;
    derived_invocation_id TEXT;
    derived_tool_name TEXT;
    derived_request_digest TEXT;
    derived_quota_bucket TEXT;
    derived_max_cost BIGINT;
    max_tool_calls INTEGER;
    max_run_cost BIGINT;
    used_run_cost BIGINT;
    admitted BOOLEAN;
    is_shadow BOOLEAN;
BEGIN
    IF requested_run_snapshot_id<=0 OR requested_plan_id<=0 OR
       requested_step_ordinal NOT BETWEEN 0 AND 15 THEN
        RAISE EXCEPTION '095: Tool admission reference is invalid'
            USING ERRCODE='23514';
    END IF;

    SELECT capability.* INTO capability_row
      FROM public.current_research_run_capability_v1() capability
     WHERE capability.run_snapshot_id=requested_run_snapshot_id;
    IF capability_row.run_snapshot_id IS NULL THEN
        RAISE EXCEPTION '095: Tool admission capability is unavailable'
            USING ERRCODE='42501';
    END IF;

    -- Join the schema writer and tenant purge lock order before row locks.
    PERFORM pg_advisory_xact_lock_shared(6215335020355474248);
    PERFORM pg_advisory_xact_lock_shared(
        hashtextextended('vane/tenant-admission/v1/' ||
                         capability_row.tenant_id::text,1447120453));
    -- Prepare, snapshot creation and cutover serialize this exact task on the
    -- same key.  Hold a shared lock only for a syntactically exact shadow so a
    -- sidecar cannot roll back or advance during first-effect admission.
    IF capability_row.temporal_workflow_id ~
           '^research-v3-shadow-[0-9a-f]{64}$' THEN
        PERFORM pg_advisory_xact_lock_shared(hashtextextended(
            capability_row.tenant_id::text||'/'||
            capability_row.user_id::text||'/'||capability_row.task_id,101));
    END IF;
    PERFORM pg_advisory_xact_lock(
        hashtextextended('research-spend/v3:' ||
                         capability_row.temporal_run_id || ':budget',0));
    PERFORM pg_advisory_xact_lock(
        hashtextextended('research-step/v3:' ||
                         capability_row.temporal_run_id || ':' ||
                         requested_plan_id::text || ':' ||
                         requested_step_ordinal::text,0));

    SELECT snapshot.id,snapshot.tenant_id,snapshot.user_id,snapshot.task_id,
           snapshot.temporal_workflow_id,snapshot.temporal_run_id,
           snapshot.reference_digest,snapshot.definition_digest,
           snapshot.capability_catalog_digest,snapshot.tool_policy_digest,
           snapshot.payload_digest,snapshot.payload
      INTO snapshot_row
      FROM public.task_run_snapshots snapshot
     WHERE snapshot.id=requested_run_snapshot_id
       AND snapshot.tenant_id=capability_row.tenant_id
       AND snapshot.user_id=capability_row.user_id
       AND snapshot.task_id=capability_row.task_id
       AND snapshot.temporal_workflow_id=capability_row.temporal_workflow_id
       AND snapshot.temporal_run_id=capability_row.temporal_run_id
       AND snapshot.reference_digest=capability_row.reference_digest
       AND snapshot.reference_schema_version='vane.research-run-snapshot-ref/v3'
       AND snapshot.execution_mode='discover_at_run'
     FOR UPDATE;
    IF snapshot_row.id IS NULL OR
       encode(sha256(snapshot_row.payload),'hex')<>snapshot_row.payload_digest THEN
        RAISE EXCEPTION '095: Tool admission snapshot differs'
            USING ERRCODE='23514';
    END IF;

    SELECT plan.id,plan.plan_digest,plan.plan_payload,plan.run_snapshot_id,
           plan.definition_digest,plan.capability_catalog_digest,
           plan.temporal_run_id
      INTO plan_row
      FROM public.research_run_plans plan
     WHERE plan.id=requested_plan_id
       AND plan.run_snapshot_id=snapshot_row.id
       AND plan.tenant_id=snapshot_row.tenant_id
       AND plan.user_id=snapshot_row.user_id
       AND plan.task_id=snapshot_row.task_id
       AND plan.temporal_workflow_id=snapshot_row.temporal_workflow_id
       AND plan.temporal_run_id=snapshot_row.temporal_run_id
       AND plan.definition_digest=snapshot_row.definition_digest
       AND plan.capability_catalog_digest=snapshot_row.capability_catalog_digest
     FOR SHARE;
    IF plan_row.id IS NULL OR
       encode(sha256(plan_row.plan_payload),'hex')<>plan_row.plan_digest THEN
        RAISE EXCEPTION '095: Tool admission plan differs'
            USING ERRCODE='23514';
    END IF;

    BEGIN
        snapshot_json := convert_from(snapshot_row.payload,'UTF8')::jsonb;
        plan_json := convert_from(plan_row.plan_payload,'UTF8')::jsonb;
        max_tool_calls := (snapshot_json #>>
            '{planner_budget,max_tool_calls}')::integer;
        max_run_cost := (snapshot_json #>>
            '{planner_budget,max_cost_micro_usd}')::bigint;
        is_shadow := snapshot_row.temporal_workflow_id ~
            '^research-v3-shadow-[0-9a-f]{64}$';
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION '095: Tool admission immutable JSON is invalid'
            USING ERRCODE='23514';
    END;
    IF plan_json->>'schema_version'<>'vane.research-execution-plan/v3' OR
       plan_json->>'definition_digest'<>snapshot_row.definition_digest OR
       plan_json->>'capability_catalog_digest'<>
           snapshot_row.capability_catalog_digest OR
       plan_json->>'tool_policy_digest'<>snapshot_row.tool_policy_digest OR
       jsonb_typeof(plan_json->'steps')<>'array' OR
       jsonb_array_length(plan_json->'steps')>max_tool_calls OR
       requested_step_ordinal>=jsonb_array_length(plan_json->'steps') OR
       requested_step_ordinal>=max_tool_calls OR max_run_cost<=0 THEN
        RAISE EXCEPTION '095: Tool admission plan policy differs'
            USING ERRCODE='23514';
    END IF;

    step_json := plan_json->'steps'->requested_step_ordinal;
    derived_invocation_id := step_json->>'invocation_id';
    derived_tool_name := step_json->>'tool_name';
    IF jsonb_typeof(step_json)<>'object' OR
       jsonb_typeof(step_json->'arguments')<>'object' OR
       derived_invocation_id IS NULL OR derived_tool_name IS NULL THEN
        RAISE EXCEPTION '095: Tool admission step differs'
            USING ERRCODE='23514';
    END IF;

    SELECT tool_grant.value INTO grant_json
      FROM jsonb_array_elements(snapshot_json #> '{research_tools,allowed_tools}')
           AS tool_grant(value)
     WHERE tool_grant.value->>'name'=derived_tool_name;
    IF grant_json IS NULL THEN
        RAISE EXCEPTION '095: Tool admission grant is unavailable'
            USING ERRCODE='23514';
    END IF;
    derived_quota_bucket := grant_json->>'budget_bucket';
    BEGIN
        derived_max_cost := (grant_json->>'max_cost_micro_usd')::bigint;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION '095: Tool admission grant cost is invalid'
            USING ERRCODE='23514';
    END;
    IF derived_quota_bucket<>'exa_calls' OR
       derived_max_cost NOT BETWEEN 1 AND 1000000 THEN
        RAISE EXCEPTION '095: Tool admission grant policy differs'
            USING ERRCODE='23514';
    END IF;

    -- The plan digest content-addresses the complete canonical step including
    -- arguments. Binding it with the ordinal is deterministic in Go and SQL.
    derived_request_digest := encode(sha256(convert_to(
        'vane.research-step-request/v4:' || plan_row.plan_digest || ':' ||
        requested_step_ordinal::text,'UTF8')),'hex');

    SELECT started.id,reservation.id INTO out_started_step_id,out_reservation_id
      FROM public.research_run_steps started
      JOIN public.research_run_step_spend_reservations reservation
        ON reservation.started_step_id=started.id
       AND reservation.run_snapshot_id=snapshot_row.id
       AND reservation.plan_id=plan_row.id
       AND reservation.request_digest=derived_request_digest
       AND reservation.quota_bucket=derived_quota_bucket
       AND reservation.reserved_quota_units=1
       AND reservation.reserved_cost_micro_usd=derived_max_cost
     WHERE started.tenant_id=snapshot_row.tenant_id
       AND started.user_id=snapshot_row.user_id
       AND started.task_id=snapshot_row.task_id
       AND started.plan_id=plan_row.id
       AND started.temporal_run_id=snapshot_row.temporal_run_id
       AND started.plan_digest=plan_row.plan_digest
       AND started.step_ordinal=requested_step_ordinal
       AND started.phase='started'
       AND started.invocation_id=derived_invocation_id
       AND started.tool_name=derived_tool_name
       AND started.request_digest=derived_request_digest;
    IF out_started_step_id IS NOT NULL THEN
        out_first_writer := false;
        out_plan_digest := plan_row.plan_digest;
        out_invocation_id := derived_invocation_id;
        out_tool_name := derived_tool_name;
        out_arguments := convert_to((step_json->'arguments')::text,'UTF8');
        out_request_digest := derived_request_digest;
        out_reserved_quota_units := 1;
        out_reserved_cost_micro_usd := derived_max_cost;
        RETURN NEXT;
        RETURN;
    END IF;

    -- Authorization is checked only for the first external effect. An exact
    -- replay above stays recoverable after a later pause or revocation.
    admitted := false;
    IF is_shadow THEN
        SELECT true INTO admitted
          FROM public.schedules schedule
          JOIN public.tenants tenant ON tenant.id=schedule.tenant_id
            AND tenant.status='active' AND tenant.deleted_at IS NULL
          JOIN public.memberships membership
            ON membership.tenant_id=schedule.tenant_id
           AND membership.user_id=schedule.user_id
           AND membership.role='owner'
          JOIN public.research_v3_prepared_definition_heads head
            ON head.tenant_id=schedule.tenant_id
           AND head.user_id=schedule.user_id AND head.task_id=schedule.id
          JOIN public.research_v3_definition_prepare_operations operation
            ON operation.id=head.prepare_operation_id
           AND operation.tenant_id=head.tenant_id
           AND operation.user_id=head.user_id
           AND operation.task_id=head.task_id
           AND operation.target_definition_version=head.definition_version
           AND operation.target_definition_digest=head.definition_digest
           AND operation.original_execution_mode=head.base_execution_mode
           AND operation.original_definition_version IS NOT DISTINCT FROM
               head.base_definition_version
           AND operation.original_definition_digest IS NOT DISTINCT FROM
               head.base_definition_digest
           AND operation.source_baseline_digest=head.source_baseline_digest
           AND operation.phase='prepared'
          JOIN public.task_approved_definition_versions definition
            ON definition.tenant_id=head.tenant_id
           AND definition.user_id=head.user_id
           AND definition.task_id=head.task_id
           AND definition.version=head.definition_version
           AND definition.definition_digest=head.definition_digest
           AND definition.execution_mode=head.execution_mode
         WHERE schedule.id=snapshot_row.task_id
           AND schedule.tenant_id=snapshot_row.tenant_id
           AND schedule.user_id=snapshot_row.user_id
           AND schedule.status='active'
           AND head.execution_mode='discover_at_run'
           AND head.definition_digest=snapshot_row.definition_digest
           AND head.definition_version=
               (snapshot_json->>'definition_version')::bigint
           AND definition.schema_version='vane.task-approved-definition/v3'
           AND definition.execution_mode='discover_at_run'
           AND snapshot_json->>'schema_version'=
               'vane.research-run-snapshot-payload/v3'
           AND (snapshot_json->>'tenant_id')::bigint=snapshot_row.tenant_id
           AND (snapshot_json->>'user_id')::bigint=snapshot_row.user_id
           AND snapshot_json->>'task_id'=snapshot_row.task_id
           AND snapshot_json->>'temporal_workflow_id'=
               snapshot_row.temporal_workflow_id
           AND snapshot_json->>'temporal_run_id'=snapshot_row.temporal_run_id
           AND snapshot_json->>'run_kind'='scheduled'
           AND snapshot_json->>'mode'='discover_at_run'
           AND snapshot_json->>'reference_schema_version'=
               'vane.research-run-snapshot-ref/v3'
           AND COALESCE((snapshot_json->>'authority_generation')::bigint,0)=0
           AND COALESCE(snapshot_json->>'target_action_digest','')=''
           AND COALESCE(snapshot_json->>'action_authorization_digest','')=''
           AND (
               (schedule.execution_mode=head.base_execution_mode AND
                schedule.approved_definition_version IS NOT DISTINCT FROM
                    head.base_definition_version AND
                schedule.approved_definition_digest IS NOT DISTINCT FROM
                    head.base_definition_digest)
               OR
               (schedule.execution_mode='discover_at_run' AND
                schedule.approved_definition_version=head.definition_version AND
                schedule.approved_definition_digest=head.definition_digest)
           )
         FOR SHARE OF schedule,tenant,membership,head,operation,definition;
    ELSE
        -- The formal V3 path is migration 095's original owner-action fence.
        SELECT true INTO admitted
          FROM public.schedules schedule
          JOIN public.tenants tenant ON tenant.id=schedule.tenant_id
            AND tenant.status='active' AND tenant.deleted_at IS NULL
          JOIN public.memberships membership
            ON membership.tenant_id=schedule.tenant_id
           AND membership.user_id=schedule.user_id
         WHERE schedule.id=snapshot_row.task_id
           AND schedule.tenant_id=snapshot_row.tenant_id
           AND schedule.user_id=snapshot_row.user_id
           AND schedule.execution_mode='discover_at_run'
           AND (schedule.status='active' OR
                (schedule.status='paused' AND
                 public.authorize_research_manual_task_run_cap_v1(
                     schedule.tenant_id,schedule.user_id,schedule.id,
                     snapshot_row.temporal_workflow_id)))
         FOR SHARE OF schedule,tenant,membership;
    END IF;
    IF NOT COALESCE(admitted,false) THEN
        RAISE EXCEPTION '105: Tool admission owner action is not authorized'
            USING ERRCODE='42501';
    END IF;

    -- The planner artifact can exist only after 094 verified-gateway
    -- settlement, so its signed actual cost is authoritative. An unsettled or
    -- non-gateway row remains charged at the reservation ceiling.
    SELECT COALESCE(sum(CASE
               WHEN settlement.receipt_provenance='verified_gateway'
                 THEN settlement.actual_cost_micro_usd
               ELSE reservation.reserved_cost_micro_usd END),0)
      INTO used_run_cost
      FROM public.research_run_llm_spend_reservations reservation
      LEFT JOIN public.research_run_llm_spend_settlements settlement
        ON settlement.reservation_id=reservation.id
     WHERE reservation.run_snapshot_id=snapshot_row.id
       AND reservation.tenant_id=snapshot_row.tenant_id
       AND reservation.user_id=snapshot_row.user_id
       AND reservation.counts_against_planner_budget;
    -- Tool terminal facts remain executor-owned until the next security batch;
    -- never let a low/zero report mint additional run budget.
    SELECT used_run_cost+COALESCE(sum(GREATEST(
               reservation.reserved_cost_micro_usd,
               COALESCE(settlement.actual_cost_micro_usd,
                        reservation.reserved_cost_micro_usd))),0)
      INTO used_run_cost
      FROM public.research_run_step_spend_reservations reservation
      LEFT JOIN public.research_run_step_spend_settlements settlement
        ON settlement.reservation_id=reservation.id
     WHERE reservation.run_snapshot_id=snapshot_row.id
       AND reservation.tenant_id=snapshot_row.tenant_id
       AND reservation.user_id=snapshot_row.user_id;
    IF used_run_cost<0 OR derived_max_cost>max_run_cost-used_run_cost THEN
        RAISE EXCEPTION '095: Tool run budget is exhausted' USING ERRCODE='P0001';
    END IF;

    UPDATE public.tenant_quota quota
       SET tokens=LEAST(quota.burst,
                        quota.tokens+quota.rate*EXTRACT(EPOCH FROM(now()-quota.updated_at)))-1,
           updated_at=now()
     WHERE quota.tenant_id=snapshot_row.tenant_id
       AND quota.bucket=derived_quota_bucket
       AND LEAST(quota.burst,
                 quota.tokens+quota.rate*EXTRACT(EPOCH FROM(now()-quota.updated_at)))>=1;
    IF NOT FOUND THEN
        RAISE EXCEPTION '095: Tool quota admission denied' USING ERRCODE='P0001';
    END IF;

    INSERT INTO public.research_run_steps (
        tenant_id,user_id,task_id,plan_id,temporal_run_id,plan_digest,
        step_ordinal,phase,invocation_id,tool_name,request_digest,
        result_digest,cost_micro_usd,error_code,schema_version
    ) VALUES (
        snapshot_row.tenant_id,snapshot_row.user_id,snapshot_row.task_id,
        plan_row.id,snapshot_row.temporal_run_id,plan_row.plan_digest,
        requested_step_ordinal,'started',derived_invocation_id,derived_tool_name,
        derived_request_digest,NULL,0,NULL,'vane.research-run-step/v3'
    ) RETURNING id INTO out_started_step_id;

    INSERT INTO public.research_run_step_spend_reservations (
        tenant_id,user_id,task_id,run_snapshot_id,plan_id,started_step_id,
        temporal_run_id,plan_digest,step_ordinal,invocation_id,tool_name,
        request_digest,tool_policy_digest,quota_bucket,reserved_quota_units,
        reserved_cost_micro_usd,schema_version
    ) VALUES (
        snapshot_row.tenant_id,snapshot_row.user_id,snapshot_row.task_id,
        snapshot_row.id,plan_row.id,out_started_step_id,
        snapshot_row.temporal_run_id,plan_row.plan_digest,requested_step_ordinal,
        derived_invocation_id,derived_tool_name,derived_request_digest,
        snapshot_row.tool_policy_digest,derived_quota_bucket,1,
        derived_max_cost,'vane.research-run-step-spend-reservation/v1'
    ) RETURNING id INTO out_reservation_id;

    out_first_writer := true;
    out_plan_digest := plan_row.plan_digest;
    out_invocation_id := derived_invocation_id;
    out_tool_name := derived_tool_name;
    out_arguments := convert_to((step_json->'arguments')::text,'UTF8');
    out_request_digest := derived_request_digest;
    out_reserved_quota_units := 1;
    out_reserved_cost_micro_usd := derived_max_cost;
    RETURN NEXT;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION admit_research_run_tool_step_cap_v1(
    BIGINT,BIGINT,INTEGER
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION admit_research_run_tool_step_cap_v1(
    BIGINT,BIGINT,INTEGER
) TO vane_research_v3_executor;

-- +goose Down

-- A retained shadow first-writer may still need response-loss replay or a
-- later ordinal.  Restoring 095 would make that durable run unrecoverable.
-- Business rollback revokes the prepared sidecar and therefore blocks every
-- new first effect without rewriting the admission protocol.
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION
        '105: refusing downgrade after shadow Tool admission authority';
END
$$;
-- +goose StatementEnd
