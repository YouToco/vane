-- 038: restricted operator control for the retained-v2 snapshot cutover.
--
-- The runtime application cannot enter this role and receives no EXECUTE
-- grant. The only supported caller is the database-direct runtimeadmin
-- command, which enters the role transaction-locally. The definer primitive
-- derives every evidence field from locked database state; its arguments carry
-- identity and intent only.

-- +goose Up

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles
         WHERE rolname = 'vane_snapshot_cutover_operator'
    ) THEN
        BEGIN
            CREATE ROLE vane_snapshot_cutover_operator
                NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
                NOLOGIN NOINHERIT NOBYPASSRLS;
        EXCEPTION
            WHEN duplicate_object OR unique_violation THEN NULL;
        END;
    END IF;
END $$;
-- +goose StatementEnd

ALTER ROLE vane_snapshot_cutover_operator
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    NOLOGIN NOINHERIT NOBYPASSRLS;

GRANT vane_snapshot_cutover_operator TO CURRENT_USER;

-- The role is cluster-wide. Only the common migration/session owner may enter
-- it, it may enter no other role, and vane_app must remain unrelated in both
-- membership directions.
-- +goose StatementBegin
DO $$
BEGIN
    IF pg_has_role(
           'vane_snapshot_cutover_operator', 'vane_app', 'MEMBER'
       ) OR
       pg_has_role(
           'vane_app', 'vane_snapshot_cutover_operator', 'MEMBER'
       ) THEN
        RAISE EXCEPTION
            '038: vane_app and snapshot cutover operator must be unrelated';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles granted_role ON granted_role.oid = am.roleid
          JOIN pg_roles member_role ON member_role.oid = am.member
         WHERE granted_role.rolname = 'vane_snapshot_cutover_operator'
           AND member_role.rolname <> CURRENT_USER
    ) THEN
        RAISE EXCEPTION
            '038: only the migration/session owner may enter snapshot cutover operator';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles member_role ON member_role.oid = am.member
         WHERE member_role.rolname = 'vane_snapshot_cutover_operator'
    ) THEN
        RAISE EXCEPTION
            '038: snapshot cutover operator must not enter another role';
    END IF;
END $$;
-- +goose StatementEnd

GRANT USAGE ON SCHEMA public TO vane_snapshot_cutover_operator;

-- Database-verifiable half of strict typed equality. The Store controller
-- separately runs the frozen Go v1/v2 materializer over the same rows while
-- holding the schedule lock. This helper proves that the retained sidecar is
-- an exact match for its parent and current Approved payload, then compares
-- every field which materializes runcontext.DefinitionV1. It is not granted to
-- any runtime role.
-- +goose StatementBegin
CREATE FUNCTION task_run_snapshot_v2_cutover_row_exact(
    expected_snapshot_id BIGINT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    parent_payload BYTEA;
    parent_payload_digest TEXT;
    shadow_payload BYTEA;
    shadow_payload_digest TEXT;
    approved_payload BYTEA;
    approved_definition_version BIGINT;
    approved_definition_digest TEXT;
    parent_json JSON;
    shadow_json JSON;
    approved_json JSON;
    row_ok BOOLEAN;
BEGIN
    SELECT p.payload, p.payload_digest, sh.payload, sh.payload_digest,
           d.payload, d.version, d.definition_digest
      INTO parent_payload, parent_payload_digest,
           shadow_payload, shadow_payload_digest,
           approved_payload, approved_definition_version,
           approved_definition_digest
      FROM public.task_run_snapshots p
      JOIN public.task_run_snapshot_v2_shadows sh
        ON sh.run_snapshot_id = p.id
       AND sh.tenant_id = p.tenant_id
       AND sh.user_id = p.user_id
       AND sh.task_id = p.task_id
       AND sh.temporal_workflow_id = p.temporal_workflow_id
       AND sh.temporal_run_id = p.temporal_run_id
       AND sh.status = 'match'
       AND sh.adaptive_version = 0
       AND sh.adaptive_digest IS NULL
      JOIN public.task_approved_definition_versions d
        ON d.tenant_id = sh.tenant_id
       AND d.user_id = sh.user_id
       AND d.task_id = sh.task_id
       AND d.version = sh.approved_definition_version
       AND d.definition_digest = sh.approved_definition_digest
     WHERE p.id = expected_snapshot_id;

    IF parent_payload IS NULL OR shadow_payload IS NULL OR
       approved_payload IS NULL OR
       encode(sha256(parent_payload), 'hex') IS DISTINCT FROM
           parent_payload_digest OR
       encode(sha256(shadow_payload), 'hex') IS DISTINCT FROM
           shadow_payload_digest OR
       encode(sha256(approved_payload), 'hex')
           IS DISTINCT FROM approved_definition_digest THEN
        RETURN FALSE;
    END IF;

    parent_json := convert_from(parent_payload, 'UTF8')::json;
    shadow_json := convert_from(shadow_payload, 'UTF8')::json;
    approved_json := convert_from(approved_payload, 'UTF8')::json;

    row_ok :=
        shadow_json #>> '{schema_version}' =
            'vane.task-run-snapshot-shadow/v2' AND
        shadow_json #>> '{status}' = 'match' AND
        shadow_json #>> '{legacy,snapshot_id}' =
            expected_snapshot_id::text AND
        shadow_json #>> '{legacy,payload_digest}' =
            parent_payload_digest AND
        convert_to(
            (shadow_json #> '{legacy,payload}')::text, 'UTF8'
        ) = parent_payload AND
        shadow_json #>> '{approved,version}' =
            approved_definition_version::text AND
        shadow_json #>> '{approved,digest}' =
            approved_definition_digest AND
        convert_to(
            (shadow_json #> '{approved,payload}')::text, 'UTF8'
        ) = approved_payload AND
        (shadow_json #> '{adaptive}')::text = 'null' AND
        approved_json #>> '{schema_version}' =
            'vane.task-approved-definition/v1' AND
        approved_json #>> '{execution_mode}' = 'compiled' AND
        approved_json #>> '{intent}' =
            approved_json #>> '{playbook_content}' AND
        (parent_json #> '{definition,task_id}')::text =
            (approved_json #> '{task_id}')::text AND
        (parent_json #> '{definition,tenant_id}')::text =
            (approved_json #> '{tenant_id}')::text AND
        (parent_json #> '{definition,user_id}')::text =
            (approved_json #> '{user_id}')::text AND
        (parent_json #> '{definition,nl_description}')::text =
            (approved_json #> '{nl_description}')::text AND
        (parent_json #> '{definition,spec_json}')::text =
            (approved_json #> '{spec_json}')::text AND
        (parent_json #> '{definition,scope_json}')::text =
            (approved_json #> '{scope_json}')::text AND
        (parent_json #> '{definition,playbook_content}')::text =
            (approved_json #> '{playbook_content}')::text AND
        (parent_json #> '{definition,strictness}')::text =
            (approved_json #> '{strictness}')::text AND
        (parent_json #> '{definition,source_scope}')::text =
            (approved_json #> '{source_scope}')::text AND
        (parent_json #> '{definition,fetch_plan}')::text =
            (approved_json #> '{fetch_plan}')::text AND
        (parent_json #> '{definition,sources}')::text =
            (approved_json #> '{sources}')::text;
    RETURN COALESCE(row_ok, FALSE);
EXCEPTION
    WHEN OTHERS THEN
        RETURN FALSE;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION task_run_snapshot_v2_cutover_row_exact(
    BIGINT
) FROM PUBLIC;

-- Identity+intent are the complete public input. Activation freezes the exact
-- task's full retained population after taking the schedule lock; rollback is
-- bound to the currently pointed activation and copies its immutable proof.
-- No H/range/count/pin/digest supplied by a caller can enter the event.
-- +goose StatementBegin
CREATE FUNCTION task_run_snapshot_v2_cutover_control(
    requested_tenant_id BIGINT,
    requested_user_id BIGINT,
    requested_task_id TEXT,
    requested_action TEXT
)
RETURNS TABLE (
    event_id BIGINT,
    generation BIGINT,
    action TEXT,
    approved_definition_version BIGINT,
    approved_definition_digest TEXT,
    snapshot_high_watermark BIGINT,
    audit_from_snapshot_id BIGINT,
    audit_count BIGINT,
    audit_through_id BIGINT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    current_pointer BIGINT;
    current_action TEXT;
    current_generation BIGINT;
    current_reverts BIGINT;
    current_definition_version BIGINT;
    current_definition_digest TEXT;
    current_high_watermark BIGINT;
    current_audit_from BIGINT;
    current_audit_count BIGINT;
    current_audit_through BIGINT;
    schedule_mode TEXT;
    schedule_definition_version BIGINT;
    schedule_definition_digest TEXT;
    approved_payload BYTEA;
    frozen_high_watermark BIGINT;
    frozen_audit_from BIGINT;
    frozen_audit_count BIGINT;
    frozen_all_exact BOOLEAN;
    next_generation BIGINT;
    new_event_id BIGINT;
    changed BIGINT;
BEGIN
    IF requested_tenant_id <= 0 OR requested_user_id <= 0 OR
       requested_task_id IS NULL OR requested_task_id = '' OR
       btrim(requested_task_id) <> requested_task_id OR
       octet_length(requested_task_id) > 255 OR
       requested_action NOT IN ('activate', 'rollback') THEN
        RAISE EXCEPTION 'snapshot cutover control request is invalid'
            USING ERRCODE = '22023';
    END IF;

    PERFORM set_config(
        'app.tenant_id', requested_tenant_id::text, true);

    -- Global order shared with definition edit and tenant purge.
    SELECT s.run_snapshot_cutover_event_id, s.execution_mode,
           s.approved_definition_version, s.approved_definition_digest
      INTO current_pointer, schedule_mode,
           schedule_definition_version, schedule_definition_digest
      FROM public.schedules s
     WHERE s.tenant_id = requested_tenant_id
       AND s.user_id = requested_user_id
       AND s.id = requested_task_id
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'snapshot cutover task is not eligible'
            USING ERRCODE = 'P0002';
    END IF;

    IF current_pointer IS NOT NULL THEN
        SELECT e.action, e.generation, e.reverts_event_id,
               e.approved_definition_version,
               e.approved_definition_digest,
               e.snapshot_high_watermark, e.audit_from_snapshot_id,
               e.audit_count, e.audit_through_id
          INTO current_action, current_generation, current_reverts,
               current_definition_version, current_definition_digest,
               current_high_watermark, current_audit_from,
               current_audit_count, current_audit_through
          FROM public.task_run_snapshot_v2_cutover_events e
         WHERE e.id = current_pointer
           AND e.tenant_id = requested_tenant_id
           AND e.user_id = requested_user_id
           AND e.task_id = requested_task_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'snapshot cutover pointer is corrupt'
                USING ERRCODE = '23514';
        END IF;
        IF current_generation <= 0 OR
           current_action NOT IN ('activate', 'rollback') OR
           current_definition_version <= 0 OR
           current_definition_digest !~ '^[0-9a-f]{64}$' OR
           current_high_watermark <= 0 OR
           current_audit_from <= 0 OR
           current_audit_count <= 0 OR
           current_audit_through IS DISTINCT FROM
               current_high_watermark THEN
            RAISE EXCEPTION 'snapshot cutover pointer event is corrupt'
                USING ERRCODE = '23514';
        END IF;
        IF current_action = 'rollback' AND (
            current_reverts IS NULL OR NOT EXISTS (
                SELECT 1
                  FROM public.task_run_snapshot_v2_cutover_events target
                 WHERE target.id = current_reverts
                   AND target.tenant_id = requested_tenant_id
                   AND target.user_id = requested_user_id
                   AND target.task_id = requested_task_id
                   AND target.action = 'activate'
                   AND target.approved_definition_version =
                       current_definition_version
                   AND target.approved_definition_digest =
                       current_definition_digest
                   AND target.snapshot_high_watermark =
                       current_high_watermark
                   AND target.audit_from_snapshot_id =
                       current_audit_from
                   AND target.audit_count = current_audit_count
                   AND target.audit_through_id = current_audit_through
            )
        ) THEN
            RAISE EXCEPTION 'snapshot cutover rollback pointer is corrupt'
                USING ERRCODE = '23514';
        END IF;
    END IF;

    -- Desired-state replay is exact. A response-lost activate/rollback retry
    -- returns the event already at the pointer and never appends another row.
    IF current_pointer IS NOT NULL AND
       current_action IS NOT DISTINCT FROM requested_action THEN
        IF current_action = 'activate' AND (
            schedule_mode IS DISTINCT FROM 'compiled' OR
            schedule_definition_version IS DISTINCT FROM
                current_definition_version OR
            schedule_definition_digest IS DISTINCT FROM
                current_definition_digest
        ) THEN
            RAISE EXCEPTION 'snapshot cutover active pin drifted'
                USING ERRCODE = '23514';
        END IF;
        RETURN QUERY
        SELECT e.id, e.generation, e.action,
               e.approved_definition_version,
               e.approved_definition_digest,
               e.snapshot_high_watermark, e.audit_from_snapshot_id,
               e.audit_count, e.audit_through_id
          FROM public.task_run_snapshot_v2_cutover_events e
         WHERE e.id = current_pointer;
        RETURN;
    END IF;

    SELECT COALESCE(MAX(e.generation), 0) + 1
      INTO next_generation
      FROM public.task_run_snapshot_v2_cutover_events e
     WHERE e.tenant_id = requested_tenant_id
       AND e.user_id = requested_user_id
       AND e.task_id = requested_task_id;

    IF requested_action = 'activate' THEN
        IF current_pointer IS NOT NULL AND
           current_action IS DISTINCT FROM 'rollback' THEN
            RAISE EXCEPTION 'snapshot cutover task is already active'
                USING ERRCODE = '23514';
        END IF;
        IF schedule_mode IS DISTINCT FROM 'compiled' OR
           schedule_definition_version IS NULL OR
           schedule_definition_digest IS NULL THEN
            RAISE EXCEPTION 'snapshot cutover task has no exact Approved pin'
                USING ERRCODE = '23514';
        END IF;
        SELECT d.payload
          INTO approved_payload
          FROM public.task_approved_definition_versions d
         WHERE d.tenant_id = requested_tenant_id
           AND d.user_id = requested_user_id
           AND d.task_id = requested_task_id
           AND d.version = schedule_definition_version
           AND d.definition_digest = schedule_definition_digest;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'snapshot cutover Approved pin is missing'
                USING ERRCODE = '23514';
        END IF;

        SELECT MIN(p.id), MAX(p.id), COUNT(*),
               COALESCE(bool_and(
                   public.task_run_snapshot_v2_cutover_row_exact(
                       p.id
                   )
               ), FALSE)
          INTO frozen_audit_from, frozen_high_watermark,
               frozen_audit_count, frozen_all_exact
          FROM public.task_run_snapshots p
         WHERE p.tenant_id = requested_tenant_id
           AND p.user_id = requested_user_id
           AND p.task_id = requested_task_id;
        IF frozen_audit_count <= 0 OR
           frozen_audit_from IS NULL OR
           frozen_high_watermark IS NULL OR
           NOT frozen_all_exact THEN
            RAISE EXCEPTION
                'snapshot cutover full retained-v2 audit failed'
                USING ERRCODE = '23514';
        END IF;

        INSERT INTO public.task_run_snapshot_v2_cutover_events (
            tenant_id, user_id, task_id, generation, action,
            approved_definition_version, approved_definition_digest,
            snapshot_high_watermark, audit_from_snapshot_id,
            audit_count, audit_through_id
        ) VALUES (
            requested_tenant_id, requested_user_id, requested_task_id,
            next_generation, 'activate',
            schedule_definition_version, schedule_definition_digest,
            frozen_high_watermark, frozen_audit_from,
            frozen_audit_count, frozen_high_watermark
        ) RETURNING id INTO new_event_id;
    ELSE
        IF current_pointer IS NULL OR
           current_action IS DISTINCT FROM 'activate' THEN
            RAISE EXCEPTION 'snapshot cutover task is not active'
                USING ERRCODE = '23514';
        END IF;
        INSERT INTO public.task_run_snapshot_v2_cutover_events (
            tenant_id, user_id, task_id, generation, action,
            reverts_event_id,
            approved_definition_version, approved_definition_digest,
            snapshot_high_watermark, audit_from_snapshot_id,
            audit_count, audit_through_id
        ) VALUES (
            requested_tenant_id, requested_user_id, requested_task_id,
            next_generation, 'rollback', current_pointer,
            current_definition_version, current_definition_digest,
            current_high_watermark, current_audit_from,
            current_audit_count, current_audit_through
        ) RETURNING id INTO new_event_id;
    END IF;

    UPDATE public.schedules s
       SET run_snapshot_cutover_event_id = new_event_id
     WHERE s.tenant_id = requested_tenant_id
       AND s.user_id = requested_user_id
       AND s.id = requested_task_id
       AND s.run_snapshot_cutover_event_id IS NOT DISTINCT FROM
           current_pointer;
    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> 1 THEN
        RAISE EXCEPTION 'snapshot cutover pointer CAS failed'
            USING ERRCODE = '40001';
    END IF;

    RETURN QUERY
    SELECT e.id, e.generation, e.action,
           e.approved_definition_version, e.approved_definition_digest,
           e.snapshot_high_watermark, e.audit_from_snapshot_id,
           e.audit_count, e.audit_through_id
      FROM public.task_run_snapshot_v2_cutover_events e
     WHERE e.id = new_event_id;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION task_run_snapshot_v2_cutover_control(
    BIGINT, BIGINT, TEXT, TEXT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION task_run_snapshot_v2_cutover_control(
    BIGINT, BIGINT, TEXT, TEXT
) TO vane_snapshot_cutover_operator;

-- +goose Down

LOCK TABLE schedules, task_run_snapshots,
    task_run_snapshot_v2_shadows, task_run_snapshot_v2_cutover_events
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM task_run_snapshot_v2_cutover_events
    ) OR EXISTS (
        SELECT 1 FROM schedules
         WHERE run_snapshot_cutover_event_id IS NOT NULL
    ) OR EXISTS (
        SELECT 1 FROM task_run_snapshots
         WHERE v2_cutover_event_id IS NOT NULL
    ) THEN
        RAISE EXCEPTION
            '038: refusing downgrade while snapshot cutover state exists';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION task_run_snapshot_v2_cutover_control(
    BIGINT, BIGINT, TEXT, TEXT
) FROM vane_snapshot_cutover_operator;
DROP FUNCTION task_run_snapshot_v2_cutover_control(
    BIGINT, BIGINT, TEXT, TEXT
);
DROP FUNCTION task_run_snapshot_v2_cutover_row_exact(
    BIGINT
);
REVOKE USAGE ON SCHEMA public FROM vane_snapshot_cutover_operator;

-- The role and owner membership are intentionally cluster-wide and may serve
-- another database in the same cluster. Down removes every database-local
-- capability but does not revoke/drop that shared identity.
