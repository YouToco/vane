-- 113: expose canonical delivery feedback through the generic intelligence
-- catalog. The existing feedbacks ledger remains authoritative; this migration
-- only grants a fixed, exact-subject read projection to the dedicated reader.

-- +goose Up

-- Revalidate the cluster role before expanding its database capability.
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles
         WHERE rolname='vane_intelligence_reader'
           AND NOT rolcanlogin AND NOT rolsuper AND NOT rolbypassrls
           AND NOT rolinherit AND NOT rolcreaterole AND NOT rolcreatedb
    ) THEN
        RAISE EXCEPTION '113: intelligence reader attributes are unsafe';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles granted_role ON granted_role.oid=am.roleid
          JOIN pg_roles member_role ON member_role.oid=am.member
         WHERE granted_role.rolname='vane_intelligence_reader'
           AND member_role.rolname<>CURRENT_USER
    ) OR EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles member_role ON member_role.oid=am.member
         WHERE member_role.rolname='vane_intelligence_reader'
    ) THEN
        RAISE EXCEPTION '113: intelligence reader role graph is unsafe';
    END IF;
END $$;
-- +goose StatementEnd

ALTER TABLE agent_intelligence_query_audits
    DROP CONSTRAINT ck_agent_intelligence_query_dataset;
ALTER TABLE agent_intelligence_query_audits
    ADD CONSTRAINT ck_agent_intelligence_query_dataset
    CHECK (dataset IN (
        'tasks','runs','observations','briefs',
        'agent_turns','tool_calls','profile','feedbacks','invalid'
    ));

-- Normalize these previously unneeded surfaces before installing exact column
-- grants. profiles retains its v1 catalog grant from migration 085.
REVOKE ALL ON feedbacks,deliveries,push_batches,profile_claim_states
    FROM vane_intelligence_reader;

GRANT SELECT (
    id,tenant_id,user_id,delivery_id,action,reason_code,detail,
    profile_epoch,created_at
) ON feedbacks TO vane_intelligence_reader;
GRANT SELECT (id,tenant_id,user_id,batch_id)
    ON deliveries TO vane_intelligence_reader;
GRANT SELECT (id,tenant_id,user_id,schedule_id,run_snapshot_id)
    ON push_batches TO vane_intelligence_reader;
GRANT SELECT (tenant_id,user_id,active_epoch)
    ON profile_claim_states TO vane_intelligence_reader;

-- Every joined table independently enforces the authenticated subject. The
-- fixed Store SQL also injects tenant/user predicates, so either layer alone
-- cannot turn a task reference or feedback id into a bearer capability.
CREATE POLICY intelligence_feedback_identity ON feedbacks AS RESTRICTIVE
    FOR SELECT TO vane_intelligence_reader
    USING (
        tenant_id IS NOT DISTINCT FROM
            NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
        AND user_id IS NOT DISTINCT FROM
            NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );
CREATE POLICY intelligence_feedback_identity ON deliveries AS RESTRICTIVE
    FOR SELECT TO vane_intelligence_reader
    USING (
        tenant_id IS NOT DISTINCT FROM
            NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
        AND user_id IS NOT DISTINCT FROM
            NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );
CREATE POLICY intelligence_feedback_identity ON push_batches AS RESTRICTIVE
    FOR SELECT TO vane_intelligence_reader
    USING (
        tenant_id IS NOT DISTINCT FROM
            NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
        AND user_id IS NOT DISTINCT FROM
            NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );
CREATE POLICY intelligence_feedback_identity ON profiles AS RESTRICTIVE
    FOR SELECT TO vane_intelligence_reader
    USING (
        tenant_id IS NOT DISTINCT FROM
            NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
        AND user_id IS NOT DISTINCT FROM
            NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );
CREATE POLICY intelligence_feedback_identity ON profile_claim_states AS RESTRICTIVE
    FOR SELECT TO vane_intelligence_reader
    USING (
        tenant_id IS NOT DISTINCT FROM
            NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
        AND user_id IS NOT DISTINCT FROM
            NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );

-- +goose Down

-- Audit rows are user business evidence. Never erase them merely to restore an
-- older catalog constraint.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM agent_intelligence_query_audits
         WHERE dataset='feedbacks'
    ) THEN
        RAISE EXCEPTION
            '113: refusing downgrade while feedback intelligence audits exist';
    END IF;
END $$;
-- +goose StatementEnd

DROP POLICY IF EXISTS intelligence_feedback_identity ON profile_claim_states;
DROP POLICY IF EXISTS intelligence_feedback_identity ON profiles;
DROP POLICY IF EXISTS intelligence_feedback_identity ON push_batches;
DROP POLICY IF EXISTS intelligence_feedback_identity ON deliveries;
DROP POLICY IF EXISTS intelligence_feedback_identity ON feedbacks;

REVOKE ALL ON feedbacks,deliveries,push_batches,profile_claim_states
    FROM vane_intelligence_reader;

ALTER TABLE agent_intelligence_query_audits
    DROP CONSTRAINT ck_agent_intelligence_query_dataset;
ALTER TABLE agent_intelligence_query_audits
    ADD CONSTRAINT ck_agent_intelligence_query_dataset
    CHECK (dataset IN (
        'tasks','runs','observations','briefs',
        'agent_turns','tool_calls','profile','invalid'
    ));
