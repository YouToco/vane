-- 065: least-privilege canonical Brief read authority for the authenticated
-- task feed. It can read immutable Brief/outcome columns and current feedback,
-- but cannot write, delete, send, render, or read global content/source tables.

-- +goose Up

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='vane_brief_reader') THEN
        BEGIN
            CREATE ROLE vane_brief_reader
                NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
                NOLOGIN NOINHERIT NOBYPASSRLS;
        EXCEPTION
            WHEN duplicate_object OR unique_violation THEN NULL;
        END;
    END IF;
END $$;
-- +goose StatementEnd

ALTER ROLE vane_brief_reader
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    NOLOGIN NOINHERIT NOBYPASSRLS;
ALTER ROLE vane_brief_reader RESET ALL;
ALTER ROLE vane_brief_reader SET search_path=pg_catalog,public,pg_temp;
GRANT vane_brief_reader TO CURRENT_USER;

-- Reject a pre-owned, pre-authorized, or role-connected cluster identity.
-- +goose StatementBegin
DO $$
BEGIN
    IF pg_has_role('vane_brief_reader','vane_app','MEMBER') OR
       pg_has_role('vane_app','vane_brief_reader','MEMBER') OR
       pg_has_role('vane_brief_reader','vane_brief_writer','MEMBER') OR
       pg_has_role('vane_brief_writer','vane_brief_reader','MEMBER') OR
       pg_has_role('vane_brief_reader','vane_run_outcome_recovery','MEMBER') OR
       pg_has_role('vane_run_outcome_recovery','vane_brief_reader','MEMBER') THEN
        RAISE EXCEPTION
            '065: brief reader must be unrelated to runtime roles';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles granted_role ON granted_role.oid=am.roleid
          JOIN pg_roles member_role ON member_role.oid=am.member
         WHERE granted_role.rolname='vane_brief_reader'
           AND member_role.rolname<>CURRENT_USER
    ) THEN
        RAISE EXCEPTION '065: only migration owner may enter brief reader';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles member_role ON member_role.oid=am.member
         WHERE member_role.rolname='vane_brief_reader'
    ) THEN
        RAISE EXCEPTION '065: brief reader must not enter another role';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_shdepend dep
          JOIN pg_roles role_row ON role_row.oid=dep.refobjid
         WHERE role_row.rolname='vane_brief_reader'
           AND dep.refclassid='pg_authid'::regclass
           AND dep.deptype='o'
           AND (
               dep.dbid=0 OR dep.dbid=(
                   SELECT oid FROM pg_database
                    WHERE datname=current_database()
               )
           )
    ) THEN
        RAISE EXCEPTION '065: brief reader must not own database objects';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_shdepend dep
          JOIN pg_roles role_row ON role_row.oid=dep.refobjid
         WHERE role_row.rolname='vane_brief_reader'
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
            '065: brief reader has preexisting ACL in this database';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_parameter_acl parameter_acl
         WHERE has_parameter_privilege(
                   'vane_brief_reader',parameter_acl.parname,'SET'
               )
            OR has_parameter_privilege(
                   'vane_brief_reader',parameter_acl.parname,'ALTER SYSTEM'
               )
    ) THEN
        RAISE EXCEPTION '065: brief reader has unsafe parameter ACL';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE ALL ON task_run_outcomes,brief_snapshots,feedbacks
    FROM vane_brief_reader;
REVOKE ALL ON task_run_snapshots,push_batches,deliveries,content_items,
    sources,schedules,memberships,users,tenants
    FROM vane_brief_reader;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM vane_brief_reader;

GRANT USAGE ON SCHEMA public TO vane_brief_reader;
GRANT SELECT (
    id,tenant_id,user_id,task_id,status,result,source_coverage,processing,
    failure_code,finalized_at
) ON task_run_outcomes TO vane_brief_reader;
GRANT SELECT (
    id,tenant_id,user_id,task_id,run_outcome_id,push_batch_id,
    run_snapshot_id,schema_version,request_digest,payload_digest,payload,
    insight_count,generated_at
) ON brief_snapshots TO vane_brief_reader;
GRANT SELECT (
    id,tenant_id,user_id,delivery_id,action,created_at
) ON feedbacks TO vane_brief_reader;

CREATE POLICY brief_reader_identity ON feedbacks AS RESTRICTIVE
    FOR SELECT TO vane_brief_reader
    USING (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
        AND user_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );

-- +goose Down

DROP POLICY IF EXISTS brief_reader_identity ON feedbacks;
REVOKE ALL ON task_run_outcomes,brief_snapshots,feedbacks
    FROM vane_brief_reader;
REVOKE USAGE ON SCHEMA public FROM vane_brief_reader;

-- Role membership and role settings are cluster-wide. Keep them on Down, as
-- migrations 060/063 do, so downgrading one database cannot revoke another
-- database's still-live reader. With every local ACL removed, the retained
-- role has no capability in this database.
