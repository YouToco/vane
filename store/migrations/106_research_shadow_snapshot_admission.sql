-- 106: remove the shadow snapshot advisory-to-row lock inversion.
--
-- CreateOrGetResearchRunSnapshotWithAuthorityV3 already owns the same
-- exact-task advisory lock used by prepare, cutover and every mutable owner
-- authorization writer.  Taking row SHARE locks inside the shadow trigger
-- inverted migration 102's writer order (row lock, then task advisory lock)
-- and could deadlock a real revocation.  Keep the formal branch unchanged;
-- the shadow branch uses an MVCC read under the exact-task fence.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE task_run_snapshots IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION task_run_snapshot_v3_admission_fence()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE
    schedule_status TEXT;
    schedule_execution_mode TEXT;
    selected_definition_digest TEXT;
    approved_schema_version TEXT;
    approved_execution_mode TEXT;
    is_shadow BOOLEAN := NEW.temporal_workflow_id ~ '^research-v3-shadow-[0-9a-f]{64}$';
BEGIN
    IF is_shadow THEN
        -- The production Store already owns this exact task exclusively.
        -- Reacquire it shared at the database boundary so future restricted
        -- INSERT callers cannot bypass revocation serialization.
        PERFORM pg_advisory_xact_lock_shared(hashtextextended(
            NEW.tenant_id::text||'/'||NEW.user_id::text||'/'||NEW.task_id,101));
        SELECT schedule.status,schedule.execution_mode,head.definition_digest,
               definition.schema_version,definition.execution_mode
          INTO schedule_status,schedule_execution_mode,selected_definition_digest,
               approved_schema_version,approved_execution_mode
          FROM public.schedules schedule
          JOIN public.tenants tenant ON tenant.id=schedule.tenant_id
          JOIN public.memberships membership
            ON membership.tenant_id=schedule.tenant_id
           AND membership.user_id=schedule.user_id
          JOIN public.research_v3_prepared_definition_heads head
            ON head.tenant_id=schedule.tenant_id AND head.user_id=schedule.user_id
           AND head.task_id=schedule.id
          JOIN public.task_approved_definition_versions definition
            ON definition.tenant_id=head.tenant_id AND definition.user_id=head.user_id
           AND definition.task_id=head.task_id AND definition.version=head.definition_version
           AND definition.definition_digest=head.definition_digest
           AND definition.execution_mode=head.execution_mode
         WHERE schedule.tenant_id=NEW.tenant_id AND schedule.user_id=NEW.user_id
           AND schedule.id=NEW.task_id
           AND tenant.status='active' AND tenant.deleted_at IS NULL
           AND membership.role='owner';
        -- The exact-task advisory fence makes a plain MVCC read linearize
        -- before a waiting revocation or observe
        -- its committed state. Row locks here would deadlock migration 102's
        -- row-then-advisory authorization triggers.
    ELSE
        -- Formal V3 admission remains byte-for-byte migration 102 behavior.
        SELECT schedule.status,schedule.execution_mode,schedule.approved_definition_digest,
               definition.schema_version,definition.execution_mode
          INTO schedule_status,schedule_execution_mode,selected_definition_digest,
               approved_schema_version,approved_execution_mode
          FROM public.schedules schedule
          JOIN public.tenants tenant ON tenant.id=schedule.tenant_id
          JOIN public.memberships membership
            ON membership.tenant_id=schedule.tenant_id
           AND membership.user_id=schedule.user_id
          JOIN public.task_approved_definition_versions definition
            ON definition.tenant_id=schedule.tenant_id
           AND definition.user_id=schedule.user_id AND definition.task_id=schedule.id
           AND definition.version=schedule.approved_definition_version
           AND definition.definition_digest=schedule.approved_definition_digest
         WHERE schedule.tenant_id=NEW.tenant_id AND schedule.user_id=NEW.user_id
           AND schedule.id=NEW.task_id
           AND tenant.status='active' AND tenant.deleted_at IS NULL
           AND membership.role='owner'
         FOR SHARE OF schedule,tenant,membership,definition;
    END IF;
    IF schedule_status IS NULL OR
       (is_shadow AND schedule_status<>'active') OR
       (NOT is_shadow AND schedule_status<>'active' AND NOT (
           schedule_status='paused' AND public.authorize_manual_task_run_v1(
               NEW.tenant_id,NEW.user_id,NEW.task_id,NEW.temporal_workflow_id))) OR
       NEW.execution_mode<>'discover_at_run' OR
       (NOT is_shadow AND schedule_execution_mode<>'discover_at_run') OR
       NEW.adaptive_version<>0 OR NEW.plan_digest<>'' OR
       NEW.v2_cutover_event_id IS NOT NULL OR
       NEW.definition_digest IS DISTINCT FROM selected_definition_digest OR
       approved_schema_version IS DISTINCT FROM 'vane.task-approved-definition/v3' OR
       approved_execution_mode IS DISTINCT FROM 'discover_at_run' THEN
        RAISE EXCEPTION '106: research snapshot admission fence rejected task state'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION task_run_snapshot_v3_admission_fence() FROM PUBLIC;

-- +goose Down

-- Restoring migration 102 would knowingly reintroduce a production-deadlock
-- path. Business rollback revokes the prepared sidecar; schema rollback is
-- intentionally unavailable.
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION
        '106: refusing downgrade to row-locking shadow snapshot admission';
END
$$;
-- +goose StatementEnd
