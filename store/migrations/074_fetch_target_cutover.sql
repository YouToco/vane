-- 074: remove the retired account-source product and give the remaining
-- task-scoped model names that describe their actual responsibilities.
--
-- Immutable migrations 001/007/020/058/059/070 keep their historical names;
-- this migration is the one-way production cutover.

-- +goose Up

-- Freeze every relation whose current state authorizes this irreversible
-- cutover. The gates below and the destructive DDL must observe one database
-- snapshot; an application writer must not be able to add a legacy task/action
-- after the proof and before its supporting relation is removed.
LOCK TABLE pending_actions, agent_action_continuations, schedules,
    schedule_playbooks, schedule_sources, task_approved_definition_versions,
    subscriptions
    IN ACCESS EXCLUSIVE MODE;

-- Every active task must already be on the immutable Approved Definition
-- control plane with one non-empty exact plan. A successful schema migration
-- must never be the event that silently turns a running task into an empty
-- task. Paused tasks may remain as retained audit/recovery state.
-- +goose StatementBegin
DO $$
DECLARE
    active_task RECORD;
    approved JSONB;
    approved_plan JSONB;
    approved_targets JSONB;
    current_targets JSONB;
    exact BOOLEAN;
BEGIN
    FOR active_task IN
        SELECT s.id, s.tenant_id, s.user_id, s.execution_mode,
               s.approved_definition_version, s.approved_definition_digest,
               d.schema_version AS definition_schema_version,
               d.execution_mode AS definition_execution_mode,
               d.payload AS definition_payload,
               pb.fetch_plan AS current_fetch_plan
          FROM schedules s
          LEFT JOIN task_approved_definition_versions d
            ON d.tenant_id=s.tenant_id AND d.user_id=s.user_id
           AND d.task_id=s.id AND d.version=s.approved_definition_version
           AND d.definition_digest=s.approved_definition_digest
           AND d.execution_mode=s.execution_mode
          LEFT JOIN schedule_playbooks pb ON pb.schedule_id=s.id
         WHERE s.status='active'
         ORDER BY s.tenant_id, s.user_id, s.id
    LOOP
        IF active_task.execution_mode IS DISTINCT FROM 'compiled'
           OR active_task.approved_definition_version IS NULL
           OR active_task.approved_definition_digest IS NULL
           OR active_task.definition_payload IS NULL
           OR active_task.definition_schema_version IS DISTINCT FROM
                'vane.task-approved-definition/v1'
           OR active_task.definition_execution_mode IS DISTINCT FROM 'compiled'
           OR active_task.current_fetch_plan IS NULL THEN
            RAISE EXCEPTION
                '074: active task % lacks an exact approved definition head',
                active_task.id;
        END IF;

        BEGIN
            approved :=
                convert_from(active_task.definition_payload, 'UTF8')::jsonb;
        EXCEPTION
            WHEN character_not_in_repertoire OR untranslatable_character
                OR invalid_text_representation THEN
                RAISE EXCEPTION
                    '074: active task % approved definition is not valid JSON',
                    active_task.id;
        END;

        approved_plan := approved #> '{fetch_plan,sources}';
        approved_targets := approved -> 'sources';
        current_targets := COALESCE(
            active_task.current_fetch_plan -> 'targets',
            active_task.current_fetch_plan -> 'sources'
        );

        IF approved ->> 'schema_version' IS DISTINCT FROM
                'vane.task-approved-definition/v1'
           OR approved ->> 'source_scope' IS DISTINCT FROM 'approved_plan'
           OR approved ->> 'execution_mode' IS DISTINCT FROM 'compiled'
           OR jsonb_typeof(approved_plan) IS DISTINCT FROM 'array'
           OR jsonb_array_length(approved_plan) = 0
           OR jsonb_typeof(approved_targets) IS DISTINCT FROM 'array'
           OR jsonb_array_length(approved_targets) = 0
           OR jsonb_typeof(current_targets) IS DISTINCT FROM 'array'
           OR jsonb_array_length(current_targets) = 0
           OR jsonb_array_length(approved_plan) <>
                jsonb_array_length(approved_targets)
           OR jsonb_array_length(approved_plan) <>
                jsonb_array_length(current_targets)
           OR EXISTS (
                SELECT 1
                  FROM jsonb_array_elements(approved_plan) item
                 WHERE jsonb_typeof(item) IS DISTINCT FROM 'object'
                    OR COALESCE(item ->> 'url', '') = ''
                    OR COALESCE(item ->> 'platform', '') = ''
                    OR COALESCE(item ->> 'capability', '') = ''
                    OR jsonb_typeof(item -> 'config') IS DISTINCT FROM 'object'
           )
           OR EXISTS (
                SELECT 1
                  FROM jsonb_array_elements(approved_targets) item
                 WHERE jsonb_typeof(item) IS DISTINCT FROM 'object'
                    OR COALESCE(item ->> 'url', '') = ''
                    OR COALESCE(item ->> 'platform', '') = ''
                    OR COALESCE(item ->> 'capability', '') = ''
                    OR jsonb_typeof(item -> 'config') IS DISTINCT FROM 'object'
                    OR COALESCE(item ->> 'source_id', '') !~ '^[1-9][0-9]*$'
           )
           OR EXISTS (
                SELECT 1
                  FROM jsonb_array_elements(current_targets) item
                 WHERE jsonb_typeof(item) IS DISTINCT FROM 'object'
                    OR COALESCE(item ->> 'url', '') = ''
                    OR COALESCE(item ->> 'platform', '') = ''
                    OR COALESCE(item ->> 'capability', '') = ''
                    OR jsonb_typeof(item -> 'config') IS DISTINCT FROM 'object'
           ) THEN
            RAISE EXCEPTION
                '074: active task % lacks a non-empty approved fetch plan',
                active_task.id;
        END IF;

        SELECT
            NOT EXISTS (
                (SELECT jsonb_build_object(
                            'platform', item -> 'platform',
                            'capability', item -> 'capability',
                            'url', item -> 'url',
                            'config', item -> 'config'
                        )
                   FROM jsonb_array_elements(approved_plan) item)
                EXCEPT
                (SELECT jsonb_build_object(
                            'platform', item -> 'platform',
                            'capability', item -> 'capability',
                            'url', item -> 'url',
                            'config', item -> 'config'
                        )
                   FROM jsonb_array_elements(approved_targets) item)
            )
            AND NOT EXISTS (
                (SELECT jsonb_build_object(
                            'platform', item -> 'platform',
                            'capability', item -> 'capability',
                            'url', item -> 'url',
                            'config', item -> 'config'
                        )
                   FROM jsonb_array_elements(approved_targets) item)
                EXCEPT
                (SELECT jsonb_build_object(
                            'platform', item -> 'platform',
                            'capability', item -> 'capability',
                            'url', item -> 'url',
                            'config', item -> 'config'
                        )
                   FROM jsonb_array_elements(approved_plan) item)
            )
            AND NOT EXISTS (
                (SELECT jsonb_build_object(
                            'platform', item -> 'platform',
                            'capability', item -> 'capability',
                            'url', item -> 'url',
                            'config', item -> 'config'
                        )
                   FROM jsonb_array_elements(approved_plan) item)
                EXCEPT
                (SELECT jsonb_build_object(
                            'platform', item -> 'platform',
                            'capability', item -> 'capability',
                            'url', item -> 'url',
                            'config', item -> 'config'
                        )
                   FROM jsonb_array_elements(current_targets) item)
            )
            AND NOT EXISTS (
                (SELECT jsonb_build_object(
                            'platform', item -> 'platform',
                            'capability', item -> 'capability',
                            'url', item -> 'url',
                            'config', item -> 'config'
                        )
                   FROM jsonb_array_elements(current_targets) item)
                EXCEPT
                (SELECT jsonb_build_object(
                            'platform', item -> 'platform',
                            'capability', item -> 'capability',
                            'url', item -> 'url',
                            'config', item -> 'config'
                        )
                   FROM jsonb_array_elements(approved_plan) item)
            )
            AND (
                SELECT count(*)
                  FROM schedule_sources ss
                 WHERE ss.schedule_id=active_task.id
            ) = jsonb_array_length(approved_targets)
            AND NOT EXISTS (
                (SELECT (item ->> 'source_id')::bigint
                   FROM jsonb_array_elements(approved_targets) item)
                EXCEPT
                (SELECT ss.source_id
                   FROM schedule_sources ss
                  WHERE ss.schedule_id=active_task.id)
            )
            AND NOT EXISTS (
                (SELECT ss.source_id
                   FROM schedule_sources ss
                  WHERE ss.schedule_id=active_task.id)
                EXCEPT
                (SELECT (item ->> 'source_id')::bigint
                   FROM jsonb_array_elements(approved_targets) item)
            )
            AND NOT EXISTS (
                SELECT 1
                  FROM jsonb_array_elements(approved_targets) item
                  LEFT JOIN sources src
                    ON src.id=(item ->> 'source_id')::bigint
                   AND src.url=item ->> 'url'
                   AND src.platform=item ->> 'platform'
                   AND src.capability=item ->> 'capability'
                   AND src.config=item -> 'config'
                 WHERE src.id IS NULL
            )
          INTO exact;

        IF NOT exact THEN
            RAISE EXCEPTION
                '074: active task % approved plan and target projection differ',
                active_task.id;
        END IF;
    END LOOP;
END
$$;
-- +goose StatementEnd

-- Retired generic agent actions were removed from every ingress before this
-- migration. Refuse to discard an action that could still execute.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM agent_action_continuations
         WHERE status IN ('pending','confirmed','blocked')
    ) THEN
        RAISE EXCEPTION
            '074: refusing fetch-target cutover while source actions are nonterminal';
    END IF;
END
$$;
-- +goose StatementEnd

-- Version 2 is the retired source-action root. Prove the parent/child protocol
-- is exact while both relations still exist. rolled_back is the one deliberate
-- exception: that terminal child restores its pristine root to version 0.
-- Unknown versions are never reinterpreted as task creation.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pending_actions
         WHERE execution_version NOT IN (0,1,2)
    ) THEN
        RAISE EXCEPTION
            '074: refusing cutover while an unknown action version exists';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM agent_action_continuations c
          JOIN pending_actions p
            ON p.id=c.action_id AND p.tenant_id=c.tenant_id
           AND p.user_id=c.user_id AND p.session_id=c.session_id
         WHERE NOT (
             (c.status='completed'
                AND p.execution_version=2 AND p.status='executed')
             OR
             (c.status='cancelled'
                AND p.execution_version=2 AND p.status='cancelled')
             OR
             (c.status='expired'
                AND p.execution_version=2 AND p.status='expired')
             OR
             (c.status='rolled_back'
                AND p.execution_version=0 AND p.status='pending')
         )
    ) OR EXISTS (
        SELECT 1
          FROM pending_actions p
          LEFT JOIN agent_action_continuations c
            ON c.action_id=p.id AND c.tenant_id=p.tenant_id
           AND c.user_id=p.user_id AND c.session_id=p.session_id
         WHERE p.execution_version=2
           AND (
               c.action_id IS NULL
               OR NOT (
                   (c.status='completed' AND p.status='executed')
                   OR (c.status='cancelled' AND p.status='cancelled')
                   OR (c.status='expired' AND p.status='expired')
               )
           )
    ) THEN
        RAISE EXCEPTION
            '074: refusing cutover while source action parent/child state differs';
    END IF;
END
$$;
-- +goose StatementEnd

-- execution_version=0 was the generic confirmation-card row shape. Current
-- durable task creation uses version 1 exclusively. Never discard a live old
-- action silently, but remove its terminal audit rows and retire the generic
-- table name in the same one-way cutover.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
         FROM pending_actions
         WHERE execution_version = 0
           AND status IN ('pending','executing')
           AND NOT EXISTS (
               SELECT 1
                 FROM agent_action_continuations
                WHERE action_id=pending_actions.id
                  AND tenant_id=pending_actions.tenant_id
                  AND user_id=pending_actions.user_id
                  AND session_id=pending_actions.session_id
                  AND status='rolled_back'
           )
    ) THEN
        RAISE EXCEPTION
            '074: refusing cutover while generic agent actions are nonterminal';
    END IF;
END
$$;
-- +goose StatementEnd

DELETE FROM pending_actions WHERE execution_version IN (0,2);

DROP TABLE agent_action_continuation_authority_events;
DROP TABLE agent_action_continuations;
ALTER TABLE pending_actions
    DROP CONSTRAINT IF EXISTS uq_pending_actions_exact_scope;

-- Direct execution has exactly one completion channel: append the terminal
-- fact to the Agent session. Refuse to orphan any live operation created by a
-- retired card/browser adapter. Agent-session receipts may safely discard a
-- not-yet-dispatched v1 presentation payload; the dispatcher deterministically
-- rebuilds the v2 session-only payload from the terminal operation.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM pending_actions
         WHERE execution_version = 1
           AND status IN ('pending','executing')
           AND receipt_provider <> 'agent_auto/v1'
    ) OR EXISTS (
        SELECT 1
          FROM task_definition_edit_operations
         WHERE status IN ('pending','executing')
           AND receipt_provider <> 'agent_auto/v1'
    ) OR EXISTS (
        SELECT 1
          FROM task_creation_receipts
         WHERE status = 'pending'
           AND provider <> 'agent_auto/v1'
    ) OR EXISTS (
        SELECT 1
          FROM task_definition_edit_receipts
         WHERE status = 'pending'
           AND provider <> 'agent_auto/v1'
    ) THEN
        RAISE EXCEPTION
            '074: refusing cutover while a retired receipt adapter is live';
    END IF;
END
$$;
-- +goose StatementEnd

UPDATE task_creation_receipts
   SET payload = NULL,
       payload_digest = ''
 WHERE status = 'pending'
   AND provider = 'agent_auto/v1';
UPDATE task_definition_edit_receipts
   SET payload = NULL,
       payload_digest = ''
 WHERE status = 'pending'
   AND provider = 'agent_auto/v1';

ALTER TABLE pending_actions RENAME TO task_creation_operations;

ALTER TABLE task_creation_operations
    RENAME CONSTRAINT pending_actions_pkey
    TO task_creation_operations_pkey;
ALTER TABLE task_creation_operations
    RENAME CONSTRAINT pending_actions_user_id_fkey
    TO task_creation_operations_user_id_fkey;
ALTER TABLE task_creation_operations
    RENAME CONSTRAINT pending_actions_session_id_fkey
    TO task_creation_operations_session_id_fkey;
ALTER TABLE task_creation_operations
    RENAME CONSTRAINT fk_pending_actions_tenant
    TO task_creation_operations_tenant_id_fkey;
ALTER TABLE task_creation_operations
    DROP CONSTRAINT pending_actions_execution_version_nonnegative;
ALTER TABLE task_creation_operations
    ADD CONSTRAINT task_creation_operations_execution_version_current
    CHECK (execution_version = 1);
ALTER TABLE task_creation_operations
    RENAME CONSTRAINT pending_actions_fence_nonnegative
    TO task_creation_operations_fence_nonnegative;
ALTER TABLE task_creation_operations
    RENAME CONSTRAINT pending_actions_attempt_nonnegative
    TO task_creation_operations_attempt_nonnegative;
ALTER TABLE task_creation_operations
    RENAME CONSTRAINT pending_actions_receipt_target_complete
    TO task_creation_operations_receipt_target_complete;
ALTER TABLE task_creation_operations
    RENAME CONSTRAINT uq_pending_actions_receipt_scope
    TO uq_task_creation_operations_receipt_scope;
ALTER TABLE task_creation_receipts
    RENAME CONSTRAINT fk_task_creation_receipts_operation_scope
    TO task_creation_receipts_operation_scope_fkey;
ALTER INDEX idx_pending_actions_user_status
    RENAME TO idx_task_creation_operations_user_status;
ALTER INDEX idx_pending_actions_tenant
    RENAME TO idx_task_creation_operations_tenant;
ALTER INDEX idx_pending_actions_creation_stale
    RENAME TO idx_task_creation_operations_stale;

-- Definition edits have always used these values as operation identity and
-- execution state, not as proof of a second user click. Rename the current
-- schema to match the direct-execution product.
ALTER TABLE task_approved_definition_versions
    RENAME COLUMN approval_ref TO operation_ref;
ALTER TABLE task_approved_definition_versions
    RENAME CONSTRAINT task_approved_definition_approval_ref_valid
    TO task_approved_definition_operation_ref_valid;
ALTER TABLE task_approved_definition_versions
    RENAME CONSTRAINT uq_task_approved_definition_approval_ref
    TO uq_task_approved_definition_operation_ref;

ALTER TABLE task_definition_edit_operations
    RENAME COLUMN approval_ref TO operation_ref;
ALTER TABLE task_definition_edit_operations
    RENAME COLUMN confirmed_at TO execution_started_at;
ALTER TABLE task_definition_edit_operations
    RENAME CONSTRAINT task_definition_edit_operation_approval_ref_valid
    TO task_definition_edit_operation_operation_ref_valid;
ALTER TABLE task_definition_edit_operations
    RENAME CONSTRAINT task_definition_edit_operation_confirmation_valid
    TO task_definition_edit_operation_execution_start_valid;
ALTER TABLE task_definition_edit_operations
    RENAME CONSTRAINT task_definition_edit_operation_confirmed_receipt_target
    TO task_definition_edit_operation_started_receipt_target;
ALTER TABLE task_definition_edit_operations
    RENAME CONSTRAINT uq_task_definition_edit_operation_approval_ref
    TO uq_task_definition_edit_operation_operation_ref;

-- The account-level source relationship is deliberately discarded. Fetch
-- targets themselves remain because task definitions share and de-duplicate
-- them.
DROP TABLE subscriptions;

ALTER TABLE sources RENAME TO fetch_targets;
ALTER TABLE schedule_sources RENAME TO task_fetch_targets;
ALTER TABLE task_fetch_targets
    RENAME COLUMN source_id TO fetch_target_id;

-- Migration 034 deliberately granted the edit coordinator only the two
-- columns needed by the old URL-only identity check. The current acquisition
-- identity is exact over platform/capability/url/config, so extend that
-- existing least-privilege grant after the table rename.
GRANT SELECT (platform, capability, config)
    ON fetch_targets TO vane_edit_coordinator;

-- Mutable playbooks now use current fetch-plan terminology. Immutable approved
-- definition/run snapshot v1 payloads retain their historical "sources" field
-- and are handled only by retained readers.
UPDATE schedule_playbooks
   SET fetch_plan = jsonb_build_object(
       'targets',
       COALESCE(fetch_plan->'targets', fetch_plan->'sources', '[]'::jsonb)
   ),
       updated_at = clock_timestamp();

ALTER INDEX idx_sources_status_next_fetch
    RENAME TO idx_fetch_targets_status_next_fetch;
ALTER TABLE fetch_targets
    RENAME CONSTRAINT sources_pkey TO fetch_targets_pkey;
ALTER TABLE fetch_targets
    RENAME CONSTRAINT uq_sources_url TO uq_fetch_targets_url;
ALTER INDEX idx_schedule_sources_source
    RENAME TO idx_task_fetch_targets_fetch_target;
ALTER TABLE task_fetch_targets
    RENAME CONSTRAINT schedule_sources_pkey TO task_fetch_targets_pkey;
ALTER TABLE task_fetch_targets
    RENAME CONSTRAINT schedule_sources_schedule_id_fkey
    TO task_fetch_targets_schedule_id_fkey;
ALTER TABLE task_fetch_targets
    RENAME CONSTRAINT schedule_sources_source_id_fkey
    TO task_fetch_targets_fetch_target_id_fkey;

-- PostgreSQL 18 represents NOT NULL as named constraints. Table/column RENAME
-- preserves those names, so clean every remaining old prefix instead of
-- leaking retired vocabulary through errors and catalog introspection.
-- +goose StatementBegin
DO $$
DECLARE
    renamed RECORD;
BEGIN
    FOR renamed IN
        SELECT c.conrelid::regclass AS relation_name,
               c.conname AS old_name,
               CASE
                   WHEN c.conrelid='task_creation_operations'::regclass
                       THEN regexp_replace(
                           c.conname, '^pending_actions_',
                           'task_creation_operations_')
                   WHEN c.conrelid='fetch_targets'::regclass
                       THEN regexp_replace(
                           c.conname, '^sources_', 'fetch_targets_')
                   WHEN c.conrelid='task_fetch_targets'::regclass
                       THEN regexp_replace(
                           c.conname, '^schedule_sources_',
                           'task_fetch_targets_')
               END AS new_name
          FROM pg_constraint c
         WHERE (
                 c.conrelid='task_creation_operations'::regclass
                 AND c.conname LIKE 'pending_actions\_%' ESCAPE '\'
               )
            OR (
                 c.conrelid='fetch_targets'::regclass
                 AND c.conname LIKE 'sources\_%' ESCAPE '\'
               )
            OR (
                 c.conrelid='task_fetch_targets'::regclass
                 AND c.conname LIKE 'schedule_sources\_%' ESCAPE '\'
               )
         ORDER BY c.conrelid, c.conname
    LOOP
        EXECUTE format(
            'ALTER TABLE %s RENAME CONSTRAINT %I TO %I',
            renamed.relation_name, renamed.old_name, renamed.new_name
        );
    END LOOP;
END
$$;
-- +goose StatementEnd

-- The batch-63 operator repair was a one-shot incident tool. Keep its immutable
-- audit table, but remove every executable capability after proving no effect
-- can still require that path.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM push_effects
         WHERE batch_id = 63
           AND status IN ('prepared','sending','ambiguous','definite_failed')
    ) THEN
        RAISE EXCEPTION
            '074: refusing cutover while legacy batch 63 has a live push effect';
    END IF;
END
$$;
-- +goose StatementEnd

DROP FUNCTION IF EXISTS abort_legacy_push_batch_63_v1(TEXT);
DROP FUNCTION IF EXISTS legacy_push_batch_63_fresh_claim_ready_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT
);
DROP FUNCTION IF EXISTS legacy_push_batch_63_claim_ready_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT
);
DROP FUNCTION IF EXISTS finalize_legacy_push_batch_63_v1(
    TEXT,TEXT,TEXT,TEXT,TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,BIGINT[],
    BYTEA,TEXT,BYTEA,TEXT,TEXT,TEXT,TEXT,TEXT,UUID,TIMESTAMPTZ
);
DROP POLICY IF EXISTS legacy_batch63_repair_role_visibility
    ON legacy_batch63_repair_events;
REVOKE vane_legacy_batch63_repair FROM CURRENT_USER;

-- Remove the four least-privilege roles whose only purpose was a retired
-- one-shot repair or the retired generic action lane.
-- +goose StatementBegin
DO $$
DECLARE
    role_name TEXT;
BEGIN
    FOREACH role_name IN ARRAY ARRAY[
        'vane_legacy_batch63_repair',
        'vane_agent_action_proposer',
        'vane_agent_action_continuator',
        'vane_agent_action_operator'
    ]
    LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format('DROP OWNED BY %I', role_name);
            EXECUTE format('DROP ROLE %I', role_name);
        END IF;
    END LOOP;
END
$$;
-- +goose StatementEnd

-- +goose Down

-- This cutover intentionally deletes account subscriptions and retired generic
-- action audit rows. Reconstructing either would invent data, so downgrade is
-- refused.
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION '074: fetch-target cutover is irreversible';
END
$$;
-- +goose StatementEnd
