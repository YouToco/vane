-- 116: bind exact preflight/status evidence to the V3 cutover journal and
-- expose only enabled V3 task IDs to the long-lived recovery process.

-- +goose Up

ALTER TABLE research_v3_cutover_operations
    ADD COLUMN original_schedule_status TEXT NOT NULL DEFAULT 'active',
    ADD COLUMN preflight_digest TEXT;

ALTER TABLE research_v3_cutover_operations
    ADD CONSTRAINT ck_research_v3_cutover_original_status
        CHECK (original_schedule_status IN ('active','paused')),
    ADD CONSTRAINT ck_research_v3_cutover_preflight_digest
        CHECK (preflight_digest IS NULL OR preflight_digest ~ '^[0-9a-f]{64}$');

UPDATE research_v3_cutover_operations
   SET preflight_digest=encode(sha256(convert_to(
       'vane/research-v3-cutover/legacy/'||id::text,'UTF8')),'hex')
 WHERE preflight_digest IS NULL;

ALTER TABLE research_v3_cutover_operations
    ALTER COLUMN preflight_digest SET NOT NULL;

ALTER TABLE research_v3_cutover_operations
    ALTER COLUMN original_schedule_status DROP DEFAULT;

-- Bind a paused shadow to the status that the one-shot prepare operation
-- actually observed. Existing sidecars backfill to active conservatively, so
-- pausing an old active preparation never mints new shadow authority.
ALTER TABLE research_v3_definition_prepare_operations
    ADD COLUMN original_schedule_status TEXT NOT NULL DEFAULT 'active',
    ADD CONSTRAINT ck_research_v3_prepare_original_status
        CHECK (original_schedule_status IN ('active','paused'));
ALTER TABLE research_v3_prepared_definition_heads
    ADD COLUMN prepared_schedule_status TEXT NOT NULL DEFAULT 'active',
    ADD CONSTRAINT ck_research_v3_prepared_head_status
        CHECK (prepared_schedule_status IN ('active','paused'));

GRANT INSERT (original_schedule_status,preflight_digest)
    ON research_v3_cutover_operations TO vane_research_v3_cutover_operator;

-- The shadow workflow is an exact prepared-definition capability, not a
-- normal schedule fire. Migrations 105/108 originally required an active
-- mirror even in that isolated branch, which made a paused production task
-- impossible to shadow. Patch only the prepared-shadow predicate; the formal
-- V3 branch and its manual-run authorization remain unchanged.
-- +goose StatementBegin
DO $$
DECLARE
    function_oid REGPROCEDURE;
    definition TEXT;
    needle TEXT := E'AND schedule.status=''active''\n           AND head.execution_mode=''discover_at_run''';
    replacement TEXT := E'AND schedule.status IN (''active'',''paused'')\n           AND head.prepared_schedule_status=schedule.status\n           AND head.execution_mode=''discover_at_run''';
BEGIN
    FOREACH function_oid IN ARRAY ARRAY[
        'public.admit_research_run_tool_step_cap_v1(bigint,bigint,integer)'::regprocedure,
        'public.authorize_research_run_effect_cap_v1(bigint)'::regprocedure
    ] LOOP
        definition := pg_get_functiondef(function_oid);
        IF strpos(definition,needle)=0 OR
           strpos(replace(definition,needle,''),needle)>0 THEN
            RAISE EXCEPTION '116: paused shadow admission patch is ambiguous';
        END IF;
        EXECUTE replace(definition,needle,replacement);
    END LOOP;

    function_oid := 'public.task_run_snapshot_v3_admission_fence()'::regprocedure;
    definition := pg_get_functiondef(function_oid);
    needle := '(is_shadow AND schedule_status<>''active'')';
    replacement := '(is_shadow AND (schedule_status NOT IN (''active'',''paused'') OR NOT EXISTS (SELECT 1 FROM public.research_v3_prepared_definition_heads status_head WHERE status_head.tenant_id=NEW.tenant_id AND status_head.user_id=NEW.user_id AND status_head.task_id=NEW.task_id AND status_head.prepared_schedule_status=schedule_status)))';
    IF strpos(definition,needle)=0 OR
       strpos(replace(definition,needle,''),needle)>0 THEN
        RAISE EXCEPTION '116: paused shadow snapshot patch is ambiguous';
    END IF;
    EXECUTE replace(definition,needle,replacement);

    function_oid := 'public.enforce_research_v3_prepared_binding()'::regprocedure;
    definition := pg_get_functiondef(function_oid);
    needle := E'AND operation.source_baseline_digest=NEW.source_baseline_digest\n           AND operation.phase=''prepared''';
    replacement := E'AND operation.source_baseline_digest=NEW.source_baseline_digest\n           AND operation.original_schedule_status=NEW.prepared_schedule_status\n           AND operation.phase=''prepared''';
    IF strpos(definition,needle)=0 OR
       strpos(replace(definition,needle,''),needle)>0 THEN
        RAISE EXCEPTION '116: prepared status binding patch is ambiguous';
    END IF;
    EXECUTE replace(definition,needle,replacement);
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION protect_research_v3_cutover_scope_v2() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
    IF NEW.original_schedule_status IS DISTINCT FROM OLD.original_schedule_status OR
       NEW.preflight_digest IS DISTINCT FROM OLD.preflight_digest THEN
        RAISE EXCEPTION '116: immutable V3 cutover preflight changed';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION protect_research_v3_cutover_scope_v2() FROM PUBLIC;
CREATE TRIGGER protect_research_v3_cutover_scope_v2
BEFORE UPDATE ON research_v3_cutover_operations
FOR EACH ROW EXECUTE FUNCTION protect_research_v3_cutover_scope_v2();

-- The function returns task IDs only. Every effect is subsequently loaded in
-- its tenant scope and every provider claim rechecks the enabled authority.
-- +goose StatementBegin
CREATE FUNCTION list_enabled_research_v3_recovery_tasks_v1(
    requested_after_task_id TEXT, requested_limit INTEGER
) RETURNS TABLE(task_id TEXT)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
    IF current_setting('role',true)<>'vane_app' OR
       NOT pg_has_role(session_user,'vane_app','MEMBER') OR
       requested_after_task_id IS NULL OR requested_limit IS NULL OR
       requested_limit NOT BETWEEN 1 AND 1000 OR
       octet_length(requested_after_task_id)>255 THEN
        RAISE EXCEPTION '116: V3 recovery discovery caller is forbidden'
            USING ERRCODE='42501';
    END IF;
    RETURN QUERY
    SELECT authority.task_id
      FROM public.research_v3_delivery_authorities authority
      JOIN public.schedules schedule
        ON schedule.tenant_id=authority.tenant_id
       AND schedule.user_id=authority.user_id
       AND schedule.id=authority.task_id
      JOIN public.tenants tenant
        ON tenant.id=schedule.tenant_id
       AND tenant.status='active' AND tenant.deleted_at IS NULL
      JOIN public.memberships membership
        ON membership.tenant_id=schedule.tenant_id
       AND membership.user_id=schedule.user_id
       AND membership.role='owner'
     WHERE authority.status='enabled'
       AND authority.task_id>requested_after_task_id
       AND schedule.status IN ('active','paused')
       AND schedule.execution_mode='discover_at_run'
       AND schedule.approved_definition_version=authority.definition_version
       AND schedule.approved_definition_digest=authority.definition_digest
     ORDER BY authority.task_id
     LIMIT requested_limit;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION list_enabled_research_v3_recovery_tasks_v1(TEXT,INTEGER)
    FROM PUBLIC;
GRANT EXECUTE ON FUNCTION list_enabled_research_v3_recovery_tasks_v1(TEXT,INTEGER)
    TO vane_app;

-- +goose Down

-- Any enabled authority requires the dynamic selector introduced above; the
-- long-lived server intentionally keeps the legacy exact-task canary empty.
-- Cutover journals and paused sidecars also contain status/digest evidence
-- that cannot be represented by migration 115, so never erase it on Down.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM research_v3_delivery_authorities
         WHERE status='enabled'
    ) THEN
        RAISE EXCEPTION '116: cannot downgrade with enabled V3 authority';
    END IF;
    IF EXISTS (SELECT 1 FROM research_v3_cutover_operations) THEN
        RAISE EXCEPTION '116: cannot downgrade with V3 cutover audit';
    END IF;
    IF EXISTS (
        SELECT 1 FROM research_v3_definition_prepare_operations
         WHERE original_schedule_status='paused'
        UNION ALL
        SELECT 1 FROM research_v3_prepared_definition_heads
         WHERE prepared_schedule_status='paused'
    ) THEN
        RAISE EXCEPTION '116: cannot downgrade with paused V3 preparation audit';
    END IF;
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
DO $$
DECLARE
    function_oid REGPROCEDURE;
    definition TEXT;
    needle TEXT := E'AND schedule.status IN (''active'',''paused'')\n           AND head.prepared_schedule_status=schedule.status\n           AND head.execution_mode=''discover_at_run''';
    replacement TEXT := E'AND schedule.status=''active''\n           AND head.execution_mode=''discover_at_run''';
BEGIN
    FOREACH function_oid IN ARRAY ARRAY[
        'public.admit_research_run_tool_step_cap_v1(bigint,bigint,integer)'::regprocedure,
        'public.authorize_research_run_effect_cap_v1(bigint)'::regprocedure
    ] LOOP
        definition := pg_get_functiondef(function_oid);
        IF strpos(definition,needle)=0 OR
           strpos(replace(definition,needle,''),needle)>0 THEN
            RAISE EXCEPTION '116: paused shadow admission rollback is ambiguous';
        END IF;
        EXECUTE replace(definition,needle,replacement);
    END LOOP;

    function_oid := 'public.task_run_snapshot_v3_admission_fence()'::regprocedure;
    definition := pg_get_functiondef(function_oid);
    needle := '(is_shadow AND (schedule_status NOT IN (''active'',''paused'') OR NOT EXISTS (SELECT 1 FROM public.research_v3_prepared_definition_heads status_head WHERE status_head.tenant_id=NEW.tenant_id AND status_head.user_id=NEW.user_id AND status_head.task_id=NEW.task_id AND status_head.prepared_schedule_status=schedule_status)))';
    replacement := '(is_shadow AND schedule_status<>''active'')';
    IF strpos(definition,needle)=0 OR
       strpos(replace(definition,needle,''),needle)>0 THEN
        RAISE EXCEPTION '116: paused shadow snapshot rollback is ambiguous';
    END IF;
    EXECUTE replace(definition,needle,replacement);

    function_oid := 'public.enforce_research_v3_prepared_binding()'::regprocedure;
    definition := pg_get_functiondef(function_oid);
    needle := E'AND operation.source_baseline_digest=NEW.source_baseline_digest\n           AND operation.original_schedule_status=NEW.prepared_schedule_status\n           AND operation.phase=''prepared''';
    replacement := E'AND operation.source_baseline_digest=NEW.source_baseline_digest\n           AND operation.phase=''prepared''';
    IF strpos(definition,needle)=0 OR
       strpos(replace(definition,needle,''),needle)>0 THEN
        RAISE EXCEPTION '116: prepared status binding rollback is ambiguous';
    END IF;
    EXECUTE replace(definition,needle,replacement);
END $$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION list_enabled_research_v3_recovery_tasks_v1(TEXT,INTEGER)
    FROM vane_app;
DROP FUNCTION list_enabled_research_v3_recovery_tasks_v1(TEXT,INTEGER);
DROP TRIGGER protect_research_v3_cutover_scope_v2
    ON research_v3_cutover_operations;
DROP FUNCTION protect_research_v3_cutover_scope_v2();
REVOKE INSERT (original_schedule_status,preflight_digest)
    ON research_v3_cutover_operations FROM vane_research_v3_cutover_operator;
ALTER TABLE research_v3_cutover_operations
    DROP CONSTRAINT ck_research_v3_cutover_original_status,
    DROP CONSTRAINT ck_research_v3_cutover_preflight_digest,
    DROP COLUMN original_schedule_status,
    DROP COLUMN preflight_digest;
ALTER TABLE research_v3_prepared_definition_heads
    DROP CONSTRAINT ck_research_v3_prepared_head_status,
    DROP COLUMN prepared_schedule_status;
ALTER TABLE research_v3_definition_prepare_operations
    DROP CONSTRAINT ck_research_v3_prepare_original_status,
    DROP COLUMN original_schedule_status;
