-- 062: least-privilege recovery reader for stale pending RunOutcome rows.
--
-- Recovery receives only the exact marker/snapshot/Temporal identity required
-- to inspect an execution and submit a normal writer claim. It receives no
-- direct table read or write privilege.

-- +goose Up

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname='vane_run_outcome_recovery'
    ) THEN
        BEGIN
            CREATE ROLE vane_run_outcome_recovery
                NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
                NOLOGIN NOINHERIT NOBYPASSRLS;
        EXCEPTION
            WHEN duplicate_object OR unique_violation THEN NULL;
        END;
    END IF;
END $$;
-- +goose StatementEnd

ALTER ROLE vane_run_outcome_recovery
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    NOLOGIN NOINHERIT NOBYPASSRLS;
ALTER ROLE vane_run_outcome_recovery RESET ALL;
ALTER ROLE vane_run_outcome_recovery
    SET search_path=pg_catalog,public,pg_temp;
GRANT vane_run_outcome_recovery TO CURRENT_USER;

CREATE INDEX idx_task_run_outcomes_pending_recovery_v1
    ON task_run_outcomes (created_at,id)
    WHERE status='pending';

-- Reject a pre-owned or pre-authorized cluster role rather than silently
-- converting it into the recovery capability.
-- +goose StatementBegin
DO $$
BEGIN
    IF pg_has_role(
           'vane_run_outcome_recovery','vane_app','MEMBER'
       ) OR pg_has_role(
           'vane_app','vane_run_outcome_recovery','MEMBER'
       ) OR pg_has_role(
           'vane_run_outcome_recovery','vane_brief_writer','MEMBER'
       ) OR pg_has_role(
           'vane_brief_writer','vane_run_outcome_recovery','MEMBER'
       ) THEN
        RAISE EXCEPTION
            '062: recovery, application, and brief writer roles must be unrelated';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles granted_role ON granted_role.oid=am.roleid
          JOIN pg_roles member_role ON member_role.oid=am.member
         WHERE granted_role.rolname='vane_run_outcome_recovery'
           AND member_role.rolname<>CURRENT_USER
    ) THEN
        RAISE EXCEPTION
            '062: only migration owner may enter recovery role';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles member_role ON member_role.oid=am.member
         WHERE member_role.rolname='vane_run_outcome_recovery'
    ) THEN
        RAISE EXCEPTION '062: recovery role must not enter another role';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_shdepend dep
          JOIN pg_roles r ON r.oid=dep.refobjid
         WHERE r.rolname='vane_run_outcome_recovery'
           AND dep.refclassid='pg_authid'::regclass
           AND dep.deptype='o'
           AND (
               dep.dbid=0 OR dep.dbid=(
                   SELECT oid FROM pg_database
                    WHERE datname=current_database()
               )
           )
    ) THEN
        RAISE EXCEPTION
            '062: recovery role must not own database objects';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_shdepend dep
          JOIN pg_roles r ON r.oid=dep.refobjid
         WHERE r.rolname='vane_run_outcome_recovery'
           AND dep.refclassid='pg_authid'::regclass
           AND dep.deptype='a'
           AND (
               dep.dbid=(
                   SELECT oid FROM pg_database
                    WHERE datname=current_database()
               ) OR (
                   dep.dbid=0
                   AND dep.classid='pg_database'::regclass
                   AND dep.objid=(
                       SELECT oid FROM pg_database
                        WHERE datname=current_database()
                   )
               )
           )
    ) THEN
        RAISE EXCEPTION
            '062: recovery role has preexisting ACL in this database';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_parameter_acl parameter_acl
         WHERE has_parameter_privilege(
                   'vane_run_outcome_recovery',
                   parameter_acl.parname,'SET'
               )
            OR has_parameter_privilege(
                   'vane_run_outcome_recovery',
                   parameter_acl.parname,'ALTER SYSTEM'
               )
    ) THEN
        RAISE EXCEPTION
            '062: recovery role has unsafe cluster parameter ACL';
    END IF;
END $$;
-- +goose StatementEnd

-- Fixed two-minute staleness and a hard page ceiling keep this capability
-- narrower than a general table reader. A null cursor starts the first page.
-- +goose StatementBegin
CREATE FUNCTION read_stale_run_outcomes_v1(
    after_created_at TIMESTAMPTZ,
    after_id BIGINT,
    requested_limit INTEGER
)
RETURNS TABLE (
    outcome_id BIGINT,
    schema_version TEXT,
    run_snapshot_id BIGINT,
    tenant_id BIGINT,
    user_id BIGINT,
    task_id TEXT,
    temporal_workflow_id TEXT,
    temporal_run_id TEXT,
    created_at TIMESTAMPTZ
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
    SELECT o.id,o.schema_version,o.run_snapshot_id,
           o.tenant_id,o.user_id,o.task_id,
           rs.temporal_workflow_id,rs.temporal_run_id,
           o.created_at
      FROM public.task_run_outcomes o
      JOIN public.task_run_snapshots rs
        ON rs.id=o.run_snapshot_id
       AND rs.tenant_id=o.tenant_id
       AND rs.user_id=o.user_id
       AND rs.task_id=o.task_id
     WHERE o.status='pending'
       AND o.created_at<=clock_timestamp()-interval '2 minutes'
       AND (
           after_created_at IS NULL OR
           (o.created_at,o.id)>(after_created_at,after_id)
       )
     ORDER BY o.created_at,o.id
     LIMIT LEAST(GREATEST(COALESCE(requested_limit,0),0),100)
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION
    read_stale_run_outcomes_v1(TIMESTAMPTZ,BIGINT,INTEGER)
    FROM PUBLIC,vane_app,vane_brief_writer,vane_run_outcome_recovery;
REVOKE ALL ON task_run_outcomes,task_run_snapshots
    FROM vane_run_outcome_recovery;
GRANT USAGE ON SCHEMA public TO vane_run_outcome_recovery;
GRANT EXECUTE ON FUNCTION
    read_stale_run_outcomes_v1(TIMESTAMPTZ,BIGINT,INTEGER)
    TO vane_run_outcome_recovery;

-- +goose Down

-- Recovery must remain available until every pre-downgrade marker has reached
-- an immutable terminal state. Canonical outcome writers hold the matching
-- shared transaction fence, so this drains admitted writers and prevents a
-- fresh marker from committing between the check and recovery teardown.
SELECT pg_advisory_xact_lock(6215335020355474248);

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM task_run_outcomes WHERE status='pending') THEN
        RAISE EXCEPTION
            '062: refusing downgrade while pending run outcomes exist';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE EXECUTE ON FUNCTION
    read_stale_run_outcomes_v1(TIMESTAMPTZ,BIGINT,INTEGER)
    FROM vane_run_outcome_recovery;
REVOKE USAGE ON SCHEMA public FROM vane_run_outcome_recovery;
DROP FUNCTION
    read_stale_run_outcomes_v1(TIMESTAMPTZ,BIGINT,INTEGER);
DROP INDEX idx_task_run_outcomes_pending_recovery_v1;
