-- 034: restricted transaction roles for durable Approved Definition edits
-- (Agent Runtime C2b3-2b).
--
-- The connection still belongs to the migration/session owner. Runtime code
-- enters one of these NOLOGIN roles with SET LOCAL ROLE after installing a
-- transaction-local tenant GUC. Neither role inherits vane_app: vane_app owns
-- DELETE on schedules, and inherited privileges cannot be subtracted with a
-- narrower REVOKE on the member role.

-- +goose Up

-- Roles are cluster-wide while grants below are database-local. Keep role
-- creation idempotent so scratch databases in the same PostgreSQL cluster can
-- migrate independently. The inner exception handlers close the first-use
-- race in which two databases both observe an absent shared role before one
-- commits CREATE ROLE. The two ALTER ROLE statements below then lock the same
-- shared-catalog rows in one fixed order until migration commit, serializing
-- GRANT and the membership guard as well. Every database in one cluster must
-- use the same migration owner; a foreign member is a fail-closed provisioning
-- error, not an identity to adopt.
--
-- Reassert the security attributes if a role survived a database downgrade;
-- in particular, neither role may inherit a future broad application grant.
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'vane_edit_coordinator'
    ) THEN
        BEGIN
            CREATE ROLE vane_edit_coordinator
                NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
                NOLOGIN NOINHERIT NOBYPASSRLS;
        EXCEPTION
            WHEN duplicate_object OR unique_violation THEN NULL;
        END;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'vane_edit_receipt'
    ) THEN
        BEGIN
            CREATE ROLE vane_edit_receipt
                NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
                NOLOGIN NOINHERIT NOBYPASSRLS;
        EXCEPTION
            WHEN duplicate_object OR unique_violation THEN NULL;
        END;
    END IF;
END $$;
-- +goose StatementEnd

ALTER ROLE vane_edit_coordinator
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    NOLOGIN NOINHERIT NOBYPASSRLS;
ALTER ROLE vane_edit_receipt
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    NOLOGIN NOINHERIT NOBYPASSRLS;

-- The owner opens the connection and is the only principal allowed to enter
-- these roles. Down deliberately keeps this membership because revoking it in
-- one database would break every other database in the shared cluster.
GRANT vane_edit_coordinator, vane_edit_receipt TO CURRENT_USER;

-- Role membership is cluster-wide too. NOINHERIT stops ambient privileges, but
-- does not close explicit SET ROLE pivots. Reject both directions involving the
-- broad application role, every non-owner member that could enter an edit role,
-- and every role that either restricted role could enter.
-- +goose StatementBegin
DO $$
BEGIN
    IF pg_has_role('vane_edit_coordinator', 'vane_app', 'MEMBER') OR
       pg_has_role('vane_edit_receipt', 'vane_app', 'MEMBER') OR
       pg_has_role('vane_app', 'vane_edit_coordinator', 'MEMBER') OR
       pg_has_role('vane_app', 'vane_edit_receipt', 'MEMBER') THEN
        RAISE EXCEPTION '034: vane_app and edit roles must not be members of each other';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles granted_role ON granted_role.oid = am.roleid
          JOIN pg_roles member_role ON member_role.oid = am.member
         WHERE granted_role.rolname IN (
                   'vane_edit_coordinator', 'vane_edit_receipt'
               )
           AND member_role.rolname <> CURRENT_USER
    ) THEN
        RAISE EXCEPTION '034: only the migration/session owner may enter edit roles';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles member_role ON member_role.oid = am.member
         WHERE member_role.rolname IN (
                   'vane_edit_coordinator', 'vane_edit_receipt'
               )
    ) THEN
        RAISE EXCEPTION '034: edit roles must not be members of any other role';
    END IF;
END $$;
-- +goose StatementEnd

GRANT USAGE ON SCHEMA public TO vane_edit_coordinator, vane_edit_receipt;

-- tenants/memberships predate tenant RLS because existing control-plane roles
-- legitimately enumerate them. Preserve that existing visibility for roles
-- which already hold SELECT, but add an edit-coordinator-only restrictive
-- policy. This makes the transaction-local tenant GUC a database-enforced
-- boundary even if a future coordinator query forgets its WHERE predicate.
ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
CREATE POLICY definition_edit_existing_visibility ON tenants
    FOR ALL TO PUBLIC USING (true) WITH CHECK (true);
CREATE POLICY definition_edit_tenant_isolation ON tenants AS RESTRICTIVE
    FOR ALL TO vane_edit_coordinator
    USING (id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint)
    WITH CHECK (id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint);

ALTER TABLE memberships ENABLE ROW LEVEL SECURITY;
CREATE POLICY definition_edit_existing_visibility ON memberships
    FOR ALL TO PUBLIC USING (true) WITH CHECK (true);
CREATE POLICY definition_edit_tenant_isolation ON memberships AS RESTRICTIVE
    FOR ALL TO vane_edit_coordinator
    USING (tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint);

ALTER TABLE schedules
    ADD CONSTRAINT schedules_definition_edit_marker_requires_paused CHECK (
        definition_edit_operation_id IS NULL OR status = 'paused'
    );

-- PostgreSQL requires UPDATE privilege on at least one column of every table
-- named by SELECT ... FOR SHARE/KEY SHARE. Giving the coordinator a real
-- business column would turn a row-lock requirement into a write channel.
-- PG18 VIRTUAL generated columns are metadata-only capabilities: they avoid a
-- stored-column rewrite, accept only SET ... = DEFAULT (which remains true),
-- and reject every supplied value before it can mutate business data.
ALTER TABLE tenants
    ADD COLUMN definition_edit_lock_capability BOOLEAN
        GENERATED ALWAYS AS (true) VIRTUAL;
ALTER TABLE memberships
    ADD COLUMN definition_edit_lock_capability BOOLEAN
        GENERATED ALWAYS AS (true) VIRTUAL;
ALTER TABLE agent_sessions
    ADD COLUMN definition_edit_lock_capability BOOLEAN
        GENERATED ALWAYS AS (true) VIRTUAL;
ALTER TABLE pending_actions
    ADD COLUMN definition_edit_lock_capability BOOLEAN
        GENERATED ALWAYS AS (true) VIRTUAL;
ALTER TABLE sources
    ADD COLUMN definition_edit_lock_capability BOOLEAN
        GENERATED ALWAYS AS (true) VIRTUAL;
ALTER TABLE schedule_sources
    ADD COLUMN definition_edit_lock_capability BOOLEAN
        GENERATED ALWAYS AS (true) VIRTUAL;

-- Coordinator read set. Shared/global tables are deliberately column-scoped;
-- tenant-owned edit/definition tables remain protected by their restrictive
-- RLS policies after SET LOCAL ROLE.
GRANT SELECT (id, status, deleted_at)
    ON tenants TO vane_edit_coordinator;
GRANT SELECT (tenant_id, user_id, role)
    ON memberships TO vane_edit_coordinator;
GRANT SELECT ON schedules TO vane_edit_coordinator;
GRANT SELECT (id, tenant_id, user_id, status)
    ON agent_sessions TO vane_edit_coordinator;
GRANT SELECT (
    id, tenant_id, user_id, tool_name, execution_version, status, phase,
    tombstoned_at, task_id, compiled_definition, compiled_digest,
    prepared_schedule, ensure_receipt
) ON pending_actions TO vane_edit_coordinator;
GRANT SELECT (id, url) ON sources TO vane_edit_coordinator;
GRANT SELECT ON schedule_playbooks TO vane_edit_coordinator;
GRANT SELECT ON schedule_sources TO vane_edit_coordinator;
GRANT SELECT ON task_approved_definition_versions TO vane_edit_coordinator;
GRANT SELECT ON task_adaptive_states TO vane_edit_coordinator;
GRANT SELECT ON task_definition_edit_operations TO vane_edit_coordinator;
GRANT SELECT (
    id, tenant_id, user_id, session_id, receipt_provider, receipt_target,
    status, phase, task_id, result, error_code, error_message, tombstoned_at
) ON task_definition_edit_operations TO vane_edit_receipt;
GRANT SELECT ON task_definition_edit_receipts
    TO vane_edit_coordinator, vane_edit_receipt;

GRANT UPDATE (definition_edit_lock_capability)
    ON tenants, memberships, agent_sessions, pending_actions, sources,
       schedule_sources
    TO vane_edit_coordinator;

-- The immutable proposal/base/target identity is insert-only. Every field not
-- listed in the UPDATE grant remains immutable to the coordinator even though
-- it can advance the durable protocol checkpoints.
GRANT INSERT (
    id, tenant_id, user_id, target_tenant_id, target_user_id,
    task_id, session_id, approval_ref, expires_at, original_status,
    base_definition_version, base_definition_digest, base_definition,
    target_definition_version, target_definition_digest, target_definition,
    canonical_proposal, proposal_digest, prepared_edit, prepared_edit_digest,
    base_snapshot, base_snapshot_digest
) ON task_definition_edit_operations TO vane_edit_coordinator;

GRANT UPDATE (
    status, phase, confirmed_at,
    pause_snapshot, pause_snapshot_digest,
    apply_snapshot, apply_snapshot_digest,
    restore_snapshot, restore_snapshot_digest,
    lease_owner, lease_until, takeover_not_before, fence, attempt,
    receipt_provider, receipt_target, result, error_code, error_message,
    updated_at, tombstoned_at
) ON task_definition_edit_operations TO vane_edit_coordinator;

-- Quiesce/marker and the Approved+legacy projection commit are one restricted
-- coordinator capability. DELETE on schedules is intentionally absent;
-- vane_app retains it as the independent delete-wins path.
GRANT UPDATE (
    status, definition_edit_operation_id, definition_edit_fence,
    nl_description, spec_json, scope_json, push_strictness, execution_mode,
    approved_definition_version, approved_definition_digest, updated_at
) ON schedules TO vane_edit_coordinator;

GRANT INSERT (
    tenant_id, user_id, task_id, version, schema_version, execution_mode,
    definition_digest, payload, approval_ref
) ON task_approved_definition_versions TO vane_edit_coordinator;

GRANT UPDATE (content, fetch_plan, updated_at)
    ON schedule_playbooks TO vane_edit_coordinator;
GRANT INSERT (schedule_id, source_id)
    ON schedule_sources TO vane_edit_coordinator;
GRANT DELETE ON schedule_sources TO vane_edit_coordinator;

-- The two retained legacy projection tables predate tenant_id columns. Their
-- schedule foreign key is nevertheless a complete tenant scope, so enforce it
-- at the database boundary before granting the coordinator write capability.
-- This closes an otherwise silent cross-tenant UPDATE/DELETE path.
ALTER TABLE schedule_playbooks ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON schedule_playbooks
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON schedule_playbooks AS RESTRICTIVE
    FOR ALL
    USING (EXISTS (
        SELECT 1 FROM schedules s
         WHERE s.id = schedule_playbooks.schedule_id
           AND s.tenant_id IS NOT DISTINCT FROM
               NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint
    ))
    WITH CHECK (EXISTS (
        SELECT 1 FROM schedules s
         WHERE s.id = schedule_playbooks.schedule_id
           AND s.tenant_id IS NOT DISTINCT FROM
               NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint
    ));

ALTER TABLE schedule_sources ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON schedule_sources
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON schedule_sources AS RESTRICTIVE
    FOR ALL
    USING (EXISTS (
        SELECT 1 FROM schedules s
         WHERE s.id = schedule_sources.schedule_id
           AND s.tenant_id IS NOT DISTINCT FROM
               NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint
    ))
    WITH CHECK (EXISTS (
        SELECT 1 FROM schedules s
         WHERE s.id = schedule_sources.schedule_id
           AND s.tenant_id IS NOT DISTINCT FROM
               NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint
    ));

-- The coordinator creates exactly one terminal receipt/outbox row. Payload,
-- lease and delivery checkpoints are reserved for the receipt role.
GRANT INSERT (
    operation_id, tenant_id, user_id, session_id, provider, target,
    provider_key, status, next_attempt_at, provider_message_id,
    failure_class, sent_at
) ON task_definition_edit_receipts TO vane_edit_coordinator;
GRANT USAGE, SELECT ON SEQUENCE task_definition_edit_receipts_id_seq
    TO vane_edit_coordinator;

-- Receipt dispatch can only mutate receipt delivery/checkpoint state plus the
-- retained Agent session message array. It has no schedule, operation-update,
-- Approved Definition, playbook, source-link, or sequence capability.
GRANT SELECT (id, tenant_id, user_id, messages)
    ON agent_sessions TO vane_edit_receipt;
GRANT UPDATE (messages) ON agent_sessions TO vane_edit_receipt;

GRANT UPDATE (
    status, lease_owner, lease_until, takeover_not_before, fence, attempt,
    next_attempt_at, payload, payload_digest,
    session_recorded_at, session_messages_digest,
    provider_message_id, failure_class, ambiguous_since,
    sent_at, blocked_at, updated_at
) ON task_definition_edit_receipts TO vane_edit_receipt;

-- +goose Down

-- Match the runtime lock order before inspecting durable state. ACCESS
-- EXCLUSIVE also closes the guard-to-REVOKE/constraint-drop window.
LOCK TABLE schedules IN ACCESS EXCLUSIVE MODE;
LOCK TABLE task_definition_edit_operations IN ACCESS EXCLUSIVE MODE;
LOCK TABLE task_definition_edit_receipts IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM task_definition_edit_operations) OR
       EXISTS (SELECT 1 FROM task_definition_edit_receipts) OR
       EXISTS (
           SELECT 1 FROM schedules
            WHERE definition_edit_operation_id IS NOT NULL
               OR definition_edit_fence IS NOT NULL
       ) THEN
        RAISE EXCEPTION '034: refusing downgrade while durable definition edit state exists';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE UPDATE (
    status, lease_owner, lease_until, takeover_not_before, fence, attempt,
    next_attempt_at, payload, payload_digest,
    session_recorded_at, session_messages_digest,
    provider_message_id, failure_class, ambiguous_since,
    sent_at, blocked_at, updated_at
) ON task_definition_edit_receipts FROM vane_edit_receipt;
REVOKE UPDATE (messages) ON agent_sessions FROM vane_edit_receipt;
REVOKE SELECT (id, tenant_id, user_id, messages)
    ON agent_sessions FROM vane_edit_receipt;

REVOKE USAGE, SELECT ON SEQUENCE task_definition_edit_receipts_id_seq
    FROM vane_edit_coordinator;
REVOKE INSERT (
    operation_id, tenant_id, user_id, session_id, provider, target,
    provider_key, status, next_attempt_at, provider_message_id,
    failure_class, sent_at
) ON task_definition_edit_receipts FROM vane_edit_coordinator;

REVOKE DELETE ON schedule_sources FROM vane_edit_coordinator;
REVOKE INSERT (schedule_id, source_id)
    ON schedule_sources FROM vane_edit_coordinator;
REVOKE UPDATE (content, fetch_plan, updated_at)
    ON schedule_playbooks FROM vane_edit_coordinator;
REVOKE INSERT (
    tenant_id, user_id, task_id, version, schema_version, execution_mode,
    definition_digest, payload, approval_ref
) ON task_approved_definition_versions FROM vane_edit_coordinator;
REVOKE UPDATE (
    status, definition_edit_operation_id, definition_edit_fence,
    nl_description, spec_json, scope_json, push_strictness, execution_mode,
    approved_definition_version, approved_definition_digest, updated_at
) ON schedules FROM vane_edit_coordinator;
REVOKE UPDATE (
    status, phase, confirmed_at,
    pause_snapshot, pause_snapshot_digest,
    apply_snapshot, apply_snapshot_digest,
    restore_snapshot, restore_snapshot_digest,
    lease_owner, lease_until, takeover_not_before, fence, attempt,
    receipt_provider, receipt_target, result, error_code, error_message,
    updated_at, tombstoned_at
) ON task_definition_edit_operations FROM vane_edit_coordinator;
REVOKE INSERT (
    id, tenant_id, user_id, target_tenant_id, target_user_id,
    task_id, session_id, approval_ref, expires_at, original_status,
    base_definition_version, base_definition_digest, base_definition,
    target_definition_version, target_definition_digest, target_definition,
    canonical_proposal, proposal_digest, prepared_edit, prepared_edit_digest,
    base_snapshot, base_snapshot_digest
) ON task_definition_edit_operations FROM vane_edit_coordinator;

REVOKE SELECT ON task_definition_edit_receipts
    FROM vane_edit_coordinator, vane_edit_receipt;
REVOKE SELECT (
    id, tenant_id, user_id, session_id, receipt_provider, receipt_target,
    status, phase, task_id, result, error_code, error_message, tombstoned_at
) ON task_definition_edit_operations FROM vane_edit_receipt;
REVOKE SELECT ON task_definition_edit_operations FROM vane_edit_coordinator;
REVOKE SELECT ON task_adaptive_states FROM vane_edit_coordinator;
REVOKE SELECT ON task_approved_definition_versions FROM vane_edit_coordinator;
REVOKE SELECT ON schedule_sources FROM vane_edit_coordinator;
REVOKE SELECT ON schedule_playbooks FROM vane_edit_coordinator;
REVOKE SELECT (id, url) ON sources FROM vane_edit_coordinator;
REVOKE SELECT (
    id, tenant_id, user_id, tool_name, execution_version, status, phase,
    tombstoned_at, task_id, compiled_definition, compiled_digest,
    prepared_schedule, ensure_receipt
) ON pending_actions FROM vane_edit_coordinator;
REVOKE SELECT (id, tenant_id, user_id, status)
    ON agent_sessions FROM vane_edit_coordinator;
REVOKE SELECT ON schedules FROM vane_edit_coordinator;
REVOKE SELECT (tenant_id, user_id, role)
    ON memberships FROM vane_edit_coordinator;
REVOKE SELECT (id, status, deleted_at)
    ON tenants FROM vane_edit_coordinator;

REVOKE UPDATE (definition_edit_lock_capability)
    ON tenants, memberships, agent_sessions, pending_actions, sources,
       schedule_sources
    FROM vane_edit_coordinator;

REVOKE USAGE ON SCHEMA public FROM vane_edit_coordinator, vane_edit_receipt;

DROP POLICY IF EXISTS definition_edit_tenant_isolation ON memberships;
DROP POLICY IF EXISTS definition_edit_existing_visibility ON memberships;
ALTER TABLE memberships DISABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS definition_edit_tenant_isolation ON tenants;
DROP POLICY IF EXISTS definition_edit_existing_visibility ON tenants;
ALTER TABLE tenants DISABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON schedule_sources;
DROP POLICY IF EXISTS tenant_visible ON schedule_sources;
ALTER TABLE schedule_sources DISABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON schedule_playbooks;
DROP POLICY IF EXISTS tenant_visible ON schedule_playbooks;
ALTER TABLE schedule_playbooks DISABLE ROW LEVEL SECURITY;

ALTER TABLE schedule_sources
    DROP COLUMN definition_edit_lock_capability;
ALTER TABLE sources
    DROP COLUMN definition_edit_lock_capability;
ALTER TABLE pending_actions
    DROP COLUMN definition_edit_lock_capability;
ALTER TABLE agent_sessions
    DROP COLUMN definition_edit_lock_capability;
ALTER TABLE memberships
    DROP COLUMN definition_edit_lock_capability;
ALTER TABLE tenants
    DROP COLUMN definition_edit_lock_capability;

ALTER TABLE schedules
    DROP CONSTRAINT schedules_definition_edit_marker_requires_paused;
