-- 115: allow the fixed intelligence catalog to join native Research V3
-- evidence, terminal Briefs, and delivery receipts. No model-supplied SQL or
-- identity enters this capability; Store still injects tenant/user/task scope.

-- +goose Up

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles
         WHERE rolname='vane_intelligence_reader'
           AND NOT rolcanlogin AND NOT rolsuper AND NOT rolbypassrls
           AND NOT rolinherit AND NOT rolcreaterole AND NOT rolcreatedb
           AND NOT rolreplication
    ) THEN
        RAISE EXCEPTION '115: intelligence reader attributes are unsafe';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles granted_role ON granted_role.oid=am.roleid
          JOIN pg_roles member_role ON member_role.oid=am.member
         WHERE granted_role.rolname='vane_intelligence_reader'
           AND member_role.rolname NOT IN (CURRENT_USER,'vane_server_runtime')
    ) OR EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles member_role ON member_role.oid=am.member
         WHERE member_role.rolname='vane_intelligence_reader'
    ) THEN
        RAISE EXCEPTION '115: intelligence reader role graph is unsafe';
    END IF;
END $$;
-- +goose StatementEnd

GRANT SELECT (
    id,tenant_id,user_id,task_id,run_snapshot_id,temporal_run_id,plan_digest
) ON research_run_plans TO vane_intelligence_reader;
GRANT SELECT (reference_schema_version)
    ON task_run_snapshots TO vane_intelligence_reader;
GRANT SELECT (task_id)
    ON task_run_outcomes TO vane_intelligence_reader;
GRANT SELECT (payload_digest)
    ON brief_snapshots TO vane_intelligence_reader;
GRANT SELECT (
    id,tenant_id,user_id,task_id,run_snapshot_id,plan_id,status,
    significance,decision,delivery_required,brief_payload,brief_digest,
    failure_code,finalized_at,created_at
) ON research_brief_syntheses TO vane_intelligence_reader;
GRANT SELECT (
    tenant_id,user_id,task_id,run_snapshot_id,plan_id,brief_id,status,sent_at
) ON research_brief_deliveries TO vane_intelligence_reader;

-- The original legacy artifact tables predate user-scoped RLS. The semantic
-- reader is deliberately narrower than vane_app, so add role-specific
-- restrictive identity fences without changing historical application roles.
CREATE POLICY intelligence_reader_identity ON schedules AS RESTRICTIVE
    FOR SELECT TO vane_intelligence_reader
    USING (tenant_id IS NOT DISTINCT FROM
               NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
           AND user_id IS NOT DISTINCT FROM
               NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint);
CREATE POLICY intelligence_reader_identity ON schedule_playbooks AS RESTRICTIVE
    FOR SELECT TO vane_intelligence_reader
    USING (EXISTS (
        SELECT 1 FROM schedules s
         WHERE s.id=schedule_playbooks.schedule_id
           AND s.tenant_id IS NOT DISTINCT FROM
               NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
           AND s.user_id IS NOT DISTINCT FROM
               NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    ));
CREATE POLICY intelligence_reader_identity ON task_run_snapshots AS RESTRICTIVE
    FOR SELECT TO vane_intelligence_reader
    USING (tenant_id IS NOT DISTINCT FROM
               NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
           AND user_id IS NOT DISTINCT FROM
               NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint);
CREATE POLICY intelligence_reader_identity ON task_run_outcomes AS RESTRICTIVE
    FOR SELECT TO vane_intelligence_reader
    USING (tenant_id IS NOT DISTINCT FROM
               NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
           AND user_id IS NOT DISTINCT FROM
               NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint);
CREATE POLICY intelligence_reader_identity ON task_run_content_provenance AS RESTRICTIVE
    FOR SELECT TO vane_intelligence_reader
    USING (tenant_id IS NOT DISTINCT FROM
               NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
           AND user_id IS NOT DISTINCT FROM
               NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint);
CREATE POLICY intelligence_reader_identity ON brief_snapshots AS RESTRICTIVE
    FOR SELECT TO vane_intelligence_reader
    USING (tenant_id IS NOT DISTINCT FROM
               NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
           AND user_id IS NOT DISTINCT FROM
               NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint);
CREATE POLICY intelligence_reader_identity ON tool_calls AS RESTRICTIVE
    FOR SELECT TO vane_intelligence_reader
    USING (tenant_id IS NOT DISTINCT FROM
               NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
           AND user_id IS NOT DISTINCT FROM
               NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint);

-- +goose Down

DROP POLICY IF EXISTS intelligence_reader_identity ON tool_calls;
DROP POLICY IF EXISTS intelligence_reader_identity ON brief_snapshots;
DROP POLICY IF EXISTS intelligence_reader_identity ON task_run_content_provenance;
DROP POLICY IF EXISTS intelligence_reader_identity ON task_run_outcomes;
DROP POLICY IF EXISTS intelligence_reader_identity ON task_run_snapshots;
DROP POLICY IF EXISTS intelligence_reader_identity ON schedule_playbooks;
DROP POLICY IF EXISTS intelligence_reader_identity ON schedules;

REVOKE SELECT (
    tenant_id,user_id,task_id,run_snapshot_id,plan_id,brief_id,status,sent_at
) ON research_brief_deliveries FROM vane_intelligence_reader;
REVOKE SELECT (
    id,tenant_id,user_id,task_id,run_snapshot_id,plan_id,status,
    significance,decision,delivery_required,brief_payload,brief_digest,
    failure_code,finalized_at,created_at
) ON research_brief_syntheses FROM vane_intelligence_reader;
REVOKE SELECT (
    id,tenant_id,user_id,task_id,run_snapshot_id,temporal_run_id,plan_digest
) ON research_run_plans FROM vane_intelligence_reader;
REVOKE SELECT (reference_schema_version)
    ON task_run_snapshots FROM vane_intelligence_reader;
REVOKE SELECT (task_id)
    ON task_run_outcomes FROM vane_intelligence_reader;
REVOKE SELECT (payload_digest)
    ON brief_snapshots FROM vane_intelligence_reader;
