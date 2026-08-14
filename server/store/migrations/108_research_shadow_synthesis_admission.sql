-- 108: authorize the synthesis claim for an exact delivery-dark V3 shadow.
--
-- Migration 105 admitted Tool effects for a prepared shadow while the live
-- schedule intentionally remained on its compiled baseline. The synthesis
-- claim still used the formal-only discover_at_run check, so a successful
-- partial shadow stopped after preparing its Brief. Keep the formal branch
-- unchanged and add the same prepared-binding proof used by Tool admission.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);

-- +goose StatementBegin
CREATE FUNCTION authorize_research_run_effect_cap_v1(
    requested_run_snapshot_id BIGINT
) RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    capability_row RECORD;
    snapshot_row RECORD;
    snapshot_json JSONB;
    admitted BOOLEAN := false;
    is_shadow BOOLEAN;
BEGIN
    IF requested_run_snapshot_id<=0 THEN
        RAISE EXCEPTION '108: research effect snapshot is invalid'
            USING ERRCODE='23514';
    END IF;

    SELECT capability.* INTO capability_row
      FROM public.current_research_run_capability_v1() capability
     WHERE capability.run_snapshot_id=requested_run_snapshot_id;
    IF capability_row.run_snapshot_id IS NULL THEN
        RAISE EXCEPTION '108: research effect capability is unavailable'
            USING ERRCODE='42501';
    END IF;

    PERFORM pg_advisory_xact_lock_shared(6215335020355474248);
    PERFORM pg_advisory_xact_lock_shared(
        hashtextextended('vane/tenant-admission/v1/' ||
                         capability_row.tenant_id::text,1447120453));

    is_shadow := capability_row.temporal_workflow_id ~
        '^research-v3-shadow-[0-9a-f]{64}$';
    IF is_shadow THEN
        -- Prepare, snapshot creation and cutover use this exact key. Match
        -- migration 105's advisory-then-MVCC order and never add row locks to
        -- the shadow branch, which would invert the prepare trigger order.
        PERFORM pg_advisory_xact_lock_shared(hashtextextended(
            capability_row.tenant_id::text||'/'||
            capability_row.user_id::text||'/'||capability_row.task_id,101));
    END IF;

    SELECT snapshot.id,snapshot.tenant_id,snapshot.user_id,snapshot.task_id,
           snapshot.temporal_workflow_id,snapshot.temporal_run_id,
           snapshot.reference_digest,snapshot.definition_digest,
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
       AND snapshot.execution_mode='discover_at_run';
    IF snapshot_row.id IS NULL OR
       encode(sha256(snapshot_row.payload),'hex')<>snapshot_row.payload_digest THEN
        RAISE EXCEPTION '108: research effect snapshot differs'
            USING ERRCODE='23514';
    END IF;

    BEGIN
        snapshot_json := convert_from(snapshot_row.payload,'UTF8')::jsonb;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION '108: research effect snapshot JSON is invalid'
            USING ERRCODE='23514';
    END;

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
           );
    ELSE
        -- This is migration 088's original first-effect owner-action fence.
        SELECT true INTO admitted
          FROM public.schedules schedule
          JOIN public.tenants tenant ON tenant.id=schedule.tenant_id
            AND tenant.status='active'
          JOIN public.memberships membership
            ON membership.tenant_id=schedule.tenant_id
           AND membership.user_id=schedule.user_id
         WHERE schedule.id=snapshot_row.task_id
           AND schedule.tenant_id=snapshot_row.tenant_id
           AND schedule.user_id=snapshot_row.user_id
           AND (schedule.status='active' OR (
               schedule.status='paused' AND
               public.authorize_research_manual_task_run_cap_v1(
                   schedule.tenant_id,schedule.user_id,schedule.id,
                   snapshot_row.temporal_workflow_id)
           )) AND schedule.execution_mode='discover_at_run'
         FOR SHARE OF schedule,tenant,membership;
    END IF;

    IF NOT COALESCE(admitted, false) THEN
        RAISE EXCEPTION '108: research effect owner action is not authorized'
            USING ERRCODE='42501';
    END IF;
    RETURN true;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION authorize_research_run_effect_cap_v1(BIGINT)
    FROM PUBLIC;
GRANT EXECUTE ON FUNCTION authorize_research_run_effect_cap_v1(BIGINT)
    TO vane_research_v3_executor;

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);

REVOKE ALL ON FUNCTION authorize_research_run_effect_cap_v1(BIGINT)
    FROM vane_research_v3_executor;
DROP FUNCTION authorize_research_run_effect_cap_v1(BIGINT);
