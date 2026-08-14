-- 033: authenticated Approved Definition edit operation + durable receipt outbox
-- (Agent Runtime C2b3-2a).
--
-- This is deliberately a zero-production-call-point foundation. It freezes the
-- authenticated actor, exact target scope, immutable PostgreSQL/Temporal edit
-- proposal, DB-clock lease/fence state, phase receipts, and terminal user
-- receipt without registering an edit tool or calling the C2b3-1 raw APIs.
--
-- tenant_id/user_id are the authenticated actor. target_* is kept separate so
-- a later team-capable protocol cannot silently reinterpret an old operation;
-- v1 explicitly requires actor == target. BYTEA checkpoints are paired with a
-- database-recomputed SHA-256 so retry/recovery can prove exact byte identity.

-- +goose Up

-- A definition-edit proposal always originates in an authenticated Agent
-- session. The composite key lets both durable tables prove that the retained
-- session belongs to the same frozen tenant/user scope. Session rows are
-- retained for as long as edit audit rows reference them; tenant purge deletes
-- edit receipts/operations before agent_sessions.
ALTER TABLE agent_sessions
    ADD CONSTRAINT uq_agent_sessions_definition_edit_scope
        UNIQUE (id, tenant_id, user_id);

CREATE TABLE task_definition_edit_operations (
    id                       TEXT        PRIMARY KEY,
    tenant_id                BIGINT      NOT NULL REFERENCES tenants (id),
    user_id                  BIGINT      NOT NULL REFERENCES users (id),
    target_tenant_id         BIGINT      NOT NULL REFERENCES tenants (id),
    target_user_id           BIGINT      NOT NULL REFERENCES users (id),
    task_id                  TEXT        NOT NULL,
    session_id               BIGINT      NOT NULL,
    approval_ref             TEXT        NOT NULL,
    status                   TEXT        NOT NULL DEFAULT 'pending',
    phase                    TEXT        NOT NULL DEFAULT 'proposal_sealed',
    expires_at               TIMESTAMPTZ NOT NULL,
    confirmed_at             TIMESTAMPTZ,
    original_status          TEXT        NOT NULL,
    base_definition_version  BIGINT      NOT NULL,
    base_definition_digest   TEXT        NOT NULL,
    base_definition          BYTEA       NOT NULL,
    target_definition_version BIGINT     NOT NULL,
    target_definition_digest TEXT        NOT NULL,
    target_definition        BYTEA       NOT NULL,
    canonical_proposal       BYTEA       NOT NULL,
    proposal_digest          TEXT        NOT NULL,
    prepared_edit            BYTEA       NOT NULL,
    prepared_edit_digest     TEXT        NOT NULL,
    base_snapshot            BYTEA       NOT NULL,
    base_snapshot_digest     TEXT        NOT NULL,
    pause_snapshot           BYTEA,
    pause_snapshot_digest    TEXT        NOT NULL DEFAULT '',
    apply_snapshot           BYTEA,
    apply_snapshot_digest    TEXT        NOT NULL DEFAULT '',
    restore_snapshot         BYTEA,
    restore_snapshot_digest  TEXT        NOT NULL DEFAULT '',
    lease_owner              TEXT        NOT NULL DEFAULT '',
    lease_until              TIMESTAMPTZ,
    takeover_not_before      TIMESTAMPTZ,
    fence                    BIGINT      NOT NULL DEFAULT 0,
    attempt                  INTEGER     NOT NULL DEFAULT 0,
    receipt_provider         TEXT        NOT NULL DEFAULT '',
    receipt_target           TEXT        NOT NULL DEFAULT '',
    result                   JSONB,
    error_code               TEXT        NOT NULL DEFAULT '',
    error_message            TEXT        NOT NULL DEFAULT '',
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    tombstoned_at            TIMESTAMPTZ,

    CONSTRAINT task_definition_edit_operation_id_valid CHECK (
        id <> '' AND btrim(id) = id AND octet_length(id) <= 512
    ),
    CONSTRAINT task_definition_edit_operation_actor_is_target CHECK (
        tenant_id = target_tenant_id AND user_id = target_user_id
    ),
    CONSTRAINT fk_task_definition_edit_operation_session_scope
        FOREIGN KEY (session_id, tenant_id, user_id)
        REFERENCES agent_sessions (id, tenant_id, user_id),
    CONSTRAINT task_definition_edit_operation_task_id_valid CHECK (
        task_id <> '' AND btrim(task_id) = task_id AND octet_length(task_id) <= 255
    ),
    CONSTRAINT task_definition_edit_operation_approval_ref_valid CHECK (
        approval_ref <> '' AND btrim(approval_ref) = approval_ref AND
        octet_length(approval_ref) <= 1024
    ),
    CONSTRAINT task_definition_edit_operation_status_valid CHECK (
        status IN (
            'pending', 'executing', 'completed', 'cancelled',
            'expired', 'blocked', 'superseded'
        )
    ),
    CONSTRAINT task_definition_edit_operation_phase_valid CHECK (
        phase IN (
            'proposal_sealed', 'db_quiesced', 'temporal_base_paused',
            'definition_committed', 'temporal_target_applied',
            'temporal_target_restored'
        )
    ),
    CONSTRAINT task_definition_edit_operation_status_phase_valid CHECK (
        (status = 'pending' AND phase = 'proposal_sealed' AND tombstoned_at IS NULL) OR
        (status = 'executing' AND phase IN (
            'proposal_sealed', 'db_quiesced', 'temporal_base_paused',
            'definition_committed', 'temporal_target_applied',
            'temporal_target_restored'
        ) AND tombstoned_at IS NULL) OR
        (status = 'completed' AND phase = 'temporal_target_restored' AND
         tombstoned_at IS NOT NULL) OR
        (status IN ('cancelled', 'expired') AND phase = 'proposal_sealed' AND
         tombstoned_at IS NOT NULL) OR
        (status IN ('blocked', 'superseded') AND tombstoned_at IS NOT NULL)
    ),
    CONSTRAINT task_definition_edit_operation_confirmation_valid CHECK (
        (status IN ('pending', 'cancelled', 'expired') AND confirmed_at IS NULL) OR
        (status IN ('executing', 'completed', 'blocked', 'superseded') AND
         confirmed_at IS NOT NULL)
    ),
    CONSTRAINT task_definition_edit_operation_original_status_valid CHECK (
        original_status IN ('active', 'paused')
    ),
    CONSTRAINT task_definition_edit_operation_versions_valid CHECK (
        base_definition_version > 0 AND
        target_definition_version > base_definition_version AND
        target_definition_version - base_definition_version = 1
    ),
    CONSTRAINT task_definition_edit_operation_base_definition_valid CHECK (
        octet_length(base_definition) > 0 AND
        octet_length(base_definition) <= 2097152 AND
        base_definition_digest ~ '^[0-9a-f]{64}$' AND
        base_definition_digest = encode(sha256(base_definition), 'hex')
    ),
    CONSTRAINT task_definition_edit_operation_proposal_valid CHECK (
        octet_length(canonical_proposal) > 0 AND
        octet_length(canonical_proposal) <= 65536 AND
        proposal_digest ~ '^[0-9a-f]{64}$' AND
        proposal_digest = encode(sha256(canonical_proposal), 'hex')
    ),
    CONSTRAINT task_definition_edit_operation_target_definition_valid CHECK (
        octet_length(target_definition) > 0 AND
        octet_length(target_definition) <= 2097152 AND
        target_definition_digest ~ '^[0-9a-f]{64}$' AND
        target_definition_digest = encode(sha256(target_definition), 'hex')
    ),
    CONSTRAINT task_definition_edit_operation_prepared_valid CHECK (
        octet_length(prepared_edit) > 0 AND
        -- Must exactly match scheduler.EncodePreparedTaskDefinitionEdit.
        octet_length(prepared_edit) <= 4194304 AND
        prepared_edit_digest ~ '^[0-9a-f]{64}$' AND
        prepared_edit_digest = encode(sha256(prepared_edit), 'hex')
    ),
    CONSTRAINT task_definition_edit_operation_base_snapshot_valid CHECK (
        octet_length(base_snapshot) > 0 AND
        -- Must exactly match scheduler.EncodeTaskDefinitionEditBaseSnapshot.
        octet_length(base_snapshot) <= 16384 AND
        base_snapshot_digest ~ '^[0-9a-f]{64}$' AND
        base_snapshot_digest = encode(sha256(base_snapshot), 'hex')
    ),
    CONSTRAINT task_definition_edit_operation_pause_snapshot_complete CHECK (
        (pause_snapshot IS NULL AND pause_snapshot_digest = '') OR
        (pause_snapshot IS NOT NULL AND octet_length(pause_snapshot) > 0 AND
         octet_length(pause_snapshot) <= 16384 AND
         pause_snapshot_digest ~ '^[0-9a-f]{64}$' AND
         pause_snapshot_digest = encode(sha256(pause_snapshot), 'hex'))
    ),
    CONSTRAINT task_definition_edit_operation_apply_snapshot_complete CHECK (
        (apply_snapshot IS NULL AND apply_snapshot_digest = '') OR
        (apply_snapshot IS NOT NULL AND octet_length(apply_snapshot) > 0 AND
         octet_length(apply_snapshot) <= 16384 AND
         apply_snapshot_digest ~ '^[0-9a-f]{64}$' AND
         apply_snapshot_digest = encode(sha256(apply_snapshot), 'hex'))
    ),
    CONSTRAINT task_definition_edit_operation_restore_snapshot_complete CHECK (
        (restore_snapshot IS NULL AND restore_snapshot_digest = '') OR
        (restore_snapshot IS NOT NULL AND octet_length(restore_snapshot) > 0 AND
         octet_length(restore_snapshot) <= 16384 AND
         restore_snapshot_digest ~ '^[0-9a-f]{64}$' AND
         restore_snapshot_digest = encode(sha256(restore_snapshot), 'hex'))
    ),
    CONSTRAINT task_definition_edit_operation_snapshot_order CHECK (
        (apply_snapshot IS NULL OR pause_snapshot IS NOT NULL) AND
        (restore_snapshot IS NULL OR apply_snapshot IS NOT NULL)
    ),
    -- phase is the last durable progress checkpoint, even after status becomes
    -- terminal. It may never claim a remote transition without its exact
    -- canonical snapshot, nor retain a snapshot from a future phase.
    CONSTRAINT task_definition_edit_operation_phase_checkpoint_valid CHECK (
        (phase IN ('proposal_sealed', 'db_quiesced') AND
         pause_snapshot IS NULL AND apply_snapshot IS NULL AND
         restore_snapshot IS NULL) OR
        (phase IN ('temporal_base_paused', 'definition_committed') AND
         pause_snapshot IS NOT NULL AND apply_snapshot IS NULL AND
         restore_snapshot IS NULL) OR
        (phase = 'temporal_target_applied' AND
         pause_snapshot IS NOT NULL AND apply_snapshot IS NOT NULL AND
         restore_snapshot IS NULL) OR
        (phase = 'temporal_target_restored' AND
         pause_snapshot IS NOT NULL AND apply_snapshot IS NOT NULL AND
         restore_snapshot IS NOT NULL)
    ),
    CONSTRAINT task_definition_edit_operation_lease_complete CHECK (
        (lease_owner = '' AND lease_until IS NULL AND takeover_not_before IS NULL) OR
        (lease_owner <> '' AND lease_until IS NOT NULL AND
         takeover_not_before IS NOT NULL AND takeover_not_before >= lease_until)
    ),
    CONSTRAINT task_definition_edit_operation_lease_status CHECK (
        (status = 'pending' AND lease_owner = '' AND fence = 0 AND attempt = 0) OR
        (status = 'executing' AND lease_owner <> '' AND fence > 0 AND attempt > 0 AND
         confirmed_at IS NOT NULL) OR
        (status IN ('cancelled', 'expired') AND lease_owner = '' AND
         fence = 0 AND attempt = 0) OR
        (status IN ('completed', 'blocked', 'superseded') AND lease_owner = '' AND
         fence > 0 AND attempt > 0)
    ),
    CONSTRAINT task_definition_edit_operation_fence_nonnegative CHECK (fence >= 0),
    CONSTRAINT task_definition_edit_operation_attempt_nonnegative CHECK (attempt >= 0),
    CONSTRAINT task_definition_edit_operation_receipt_target_complete CHECK (
        (receipt_provider = '' AND receipt_target = '') OR
        (receipt_provider <> '' AND receipt_target <> '')
    ),
    CONSTRAINT task_definition_edit_operation_confirmed_receipt_target CHECK (
        status IN ('pending', 'cancelled', 'expired') OR
        (receipt_provider <> '' AND receipt_target <> '')
    ),
    CONSTRAINT uq_task_definition_edit_operation_scope
        UNIQUE (id, tenant_id, user_id),
    CONSTRAINT uq_task_definition_edit_operation_receipt_scope
        UNIQUE (
            id, tenant_id, user_id, session_id,
            receipt_provider, receipt_target
        ),
    CONSTRAINT uq_task_definition_edit_operation_marker
        UNIQUE (id, target_tenant_id, target_user_id, task_id, fence),
    CONSTRAINT uq_task_definition_edit_operation_approval_ref
        UNIQUE (target_tenant_id, target_user_id, task_id, approval_ref)
);

-- Only one proposal/execution may be live for a scoped task. Terminal audit
-- rows remain, and a later proposal can then advance from the new head.
CREATE UNIQUE INDEX uq_task_definition_edit_operations_nonterminal
    ON task_definition_edit_operations (target_tenant_id, target_user_id, task_id)
    WHERE status IN ('pending', 'executing');

-- Recovery is tenant-sharded and considers only executing, non-tombstoned
-- operations whose DB-clock takeover boundary has elapsed.
CREATE INDEX idx_task_definition_edit_operations_stale
    ON task_definition_edit_operations (tenant_id, takeover_not_before, id)
    WHERE status = 'executing' AND tombstoned_at IS NULL;

ALTER TABLE schedules
    ADD COLUMN definition_edit_operation_id TEXT,
    ADD COLUMN definition_edit_fence BIGINT,
    ADD CONSTRAINT schedules_definition_edit_marker_complete CHECK (
        (definition_edit_operation_id IS NULL AND definition_edit_fence IS NULL) OR
        (definition_edit_operation_id IS NOT NULL AND definition_edit_operation_id <> '' AND
         definition_edit_fence IS NOT NULL AND definition_edit_fence > 0)
    ),
    ADD CONSTRAINT fk_schedules_definition_edit_operation
        FOREIGN KEY (
            definition_edit_operation_id, tenant_id, user_id, id,
            definition_edit_fence
        ) REFERENCES task_definition_edit_operations (
            id, target_tenant_id, target_user_id, task_id, fence
        ) ON DELETE SET NULL (
            definition_edit_operation_id, definition_edit_fence
        ) DEFERRABLE INITIALLY DEFERRED;

-- Migration 022 granted vane_app table-level INSERT/UPDATE on schedules;
-- PostgreSQL extends those privileges to columns added later. Replace both
-- with the exact legacy writer allowlists so the general runtime role cannot
-- forge coordinator-private operation/fence markers on either create or
-- update. C2b3-2b must use a dedicated coordinator role/path for those two
-- columns.
REVOKE INSERT ON schedules FROM vane_app;
GRANT INSERT (
    id, tenant_id, user_id, nl_description, spec_json, scope_json, status,
    push_strictness, execution_mode
) ON schedules TO vane_app;

REVOKE UPDATE ON schedules FROM vane_app;
GRANT UPDATE (
    nl_description, spec_json, scope_json, status, updated_at,
    push_strictness, execution_mode,
    approved_definition_version, approved_definition_digest
) ON schedules TO vane_app;

CREATE TABLE task_definition_edit_receipts (
    id                      BIGSERIAL   PRIMARY KEY,
    operation_id            TEXT        NOT NULL,
    tenant_id               BIGINT      NOT NULL REFERENCES tenants (id),
    user_id                 BIGINT      NOT NULL REFERENCES users (id),
    session_id              BIGINT      NOT NULL,
    provider                TEXT        NOT NULL DEFAULT '',
    target                  TEXT        NOT NULL DEFAULT '',
    provider_key            UUID        NOT NULL,
    status                  TEXT        NOT NULL DEFAULT 'pending',
    lease_owner             TEXT        NOT NULL DEFAULT '',
    lease_until             TIMESTAMPTZ,
    takeover_not_before     TIMESTAMPTZ,
    fence                   BIGINT      NOT NULL DEFAULT 0,
    attempt                 INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at         TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '4 seconds'),
    payload                 BYTEA,
    payload_digest          TEXT        NOT NULL DEFAULT '',
    session_recorded_at     TIMESTAMPTZ,
    session_messages_digest TEXT        NOT NULL DEFAULT '',
    provider_message_id     TEXT        NOT NULL DEFAULT '',
    failure_class           TEXT        NOT NULL DEFAULT '',
    ambiguous_since         TIMESTAMPTZ,
    sent_at                 TIMESTAMPTZ,
    blocked_at              TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_task_definition_edit_receipts_operation UNIQUE (operation_id),
    CONSTRAINT uq_task_definition_edit_receipts_provider_key UNIQUE (provider_key),
    CONSTRAINT fk_task_definition_edit_receipts_session_scope
        FOREIGN KEY (session_id, tenant_id, user_id)
        REFERENCES agent_sessions (id, tenant_id, user_id),
    CONSTRAINT fk_task_definition_edit_receipts_operation_scope
        FOREIGN KEY (
            operation_id, tenant_id, user_id, session_id, provider, target
        ) REFERENCES task_definition_edit_operations (
            id, tenant_id, user_id, session_id,
            receipt_provider, receipt_target
        ),
    CONSTRAINT task_definition_edit_receipts_target_complete CHECK (
        (provider = '' AND target = '') OR
        (provider <> '' AND target <> '')
    ),
    CONSTRAINT task_definition_edit_receipts_status_valid CHECK (
        status IN ('pending', 'sent', 'blocked', 'suppressed')
    ),
    CONSTRAINT task_definition_edit_receipts_fence_nonnegative CHECK (fence >= 0),
    CONSTRAINT task_definition_edit_receipts_attempt_nonnegative CHECK (attempt >= 0),
    CONSTRAINT task_definition_edit_receipts_failure_class_valid CHECK (
        failure_class IN ('', 'retryable', 'ambiguous', 'permanent', 'target_unbound')
    ),
    CONSTRAINT task_definition_edit_receipts_failure_state_valid CHECK (
        (status = 'pending' AND (
            (failure_class = '' AND ambiguous_since IS NULL) OR
            (failure_class = 'retryable' AND ambiguous_since IS NULL AND
             fence > 0 AND attempt > 0) OR
            (failure_class = 'ambiguous' AND ambiguous_since IS NOT NULL AND
             fence > 0 AND attempt > 0)
        )) OR
        (status = 'sent' AND failure_class = '' AND ambiguous_since IS NULL) OR
        (status = 'blocked' AND failure_class = 'permanent' AND
         ambiguous_since IS NULL) OR
        (status = 'suppressed' AND failure_class = 'target_unbound' AND
         ambiguous_since IS NULL)
    ),
    CONSTRAINT task_definition_edit_receipts_payload_checkpoint_complete CHECK (
        (payload IS NULL AND payload_digest = '') OR
        (payload IS NOT NULL AND octet_length(payload) > 0 AND
         octet_length(payload) <= 2097152 AND
         payload_digest ~ '^[0-9a-f]{64}$' AND
         payload_digest = encode(sha256(payload), 'hex'))
    ),
    CONSTRAINT task_definition_edit_receipts_session_checkpoint_complete CHECK (
        (session_recorded_at IS NULL AND session_messages_digest = '') OR
        (session_recorded_at IS NOT NULL AND
         session_messages_digest ~ '^[0-9a-f]{64}$')
    ),
    CONSTRAINT task_definition_edit_receipts_lease_complete CHECK (
        (lease_owner = '' AND lease_until IS NULL AND takeover_not_before IS NULL) OR
        (lease_owner <> '' AND lease_until IS NOT NULL AND
         takeover_not_before IS NOT NULL AND takeover_not_before >= lease_until)
    ),
    CONSTRAINT task_definition_edit_receipts_lease_status CHECK (
        (status = 'pending' AND (
            (lease_owner = '' AND fence = 0 AND attempt = 0) OR
            (fence > 0 AND attempt > 0)
        )) OR
        (status IN ('sent', 'blocked') AND lease_owner = '' AND
         fence > 0 AND attempt > 0) OR
        (status = 'suppressed' AND lease_owner = '' AND
         fence = 0 AND attempt = 0)
    ),
    CONSTRAINT task_definition_edit_receipts_status_checkpoint_valid CHECK (
        (status = 'pending' AND provider <> '' AND target <> '') OR
        (status = 'sent' AND provider <> '' AND target <> '' AND
         payload IS NOT NULL AND session_recorded_at IS NOT NULL) OR
        (status = 'blocked' AND provider <> '' AND target <> '') OR
        (status = 'suppressed' AND provider = '' AND target = '' AND
         failure_class = 'target_unbound')
    ),
    CONSTRAINT task_definition_edit_receipts_terminal_markers CHECK (
        (status = 'pending' AND sent_at IS NULL AND blocked_at IS NULL) OR
        (status = 'sent' AND sent_at IS NOT NULL AND blocked_at IS NULL AND
         provider_message_id <> '') OR
        (status = 'suppressed' AND sent_at IS NOT NULL AND blocked_at IS NULL AND
         provider_message_id = 'target-unbound-suppressed') OR
        (status = 'blocked' AND sent_at IS NULL AND blocked_at IS NOT NULL AND
         failure_class <> '')
    )
);

CREATE INDEX idx_task_definition_edit_receipts_due
    ON task_definition_edit_receipts (tenant_id, next_attempt_at, id)
    WHERE status = 'pending';

-- C2b3-2a adds no explicit grant to the future restricted vane_app role.
-- The current deployment still connects as the migration/table owner, which
-- has implicit DML and bypasses RLS; zero production code references—not this
-- REVOKE posture—are the 2a dark guarantee. Runtime-role separation or scoped
-- SET LOCAL ROLE activation remains a hard gate before authenticated wiring.
-- C2b3-2b must add only the smallest column-level vane_app privileges it uses.

ALTER TABLE task_definition_edit_operations ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON task_definition_edit_operations
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON task_definition_edit_operations AS RESTRICTIVE
    FOR ALL
    USING (tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint);

ALTER TABLE task_definition_edit_receipts ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON task_definition_edit_receipts
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON task_definition_edit_receipts AS RESTRICTIVE
    FOR ALL
    USING (tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint);

-- +goose Down

-- Serialize with the production lock order (schedule -> operation -> receipt)
-- before checking state. Without these locks, an in-flight writer could insert
-- an operation after the guard and commit into columns/tables being dropped.
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
        RAISE EXCEPTION '033: refusing downgrade while durable definition edit state exists';
    END IF;
END $$;
-- +goose StatementEnd

ALTER TABLE schedules DROP CONSTRAINT fk_schedules_definition_edit_operation;

DROP POLICY IF EXISTS tenant_isolation ON task_definition_edit_receipts;
DROP POLICY IF EXISTS tenant_visible ON task_definition_edit_receipts;
ALTER TABLE task_definition_edit_receipts DISABLE ROW LEVEL SECURITY;
REVOKE ALL ON SEQUENCE task_definition_edit_receipts_id_seq FROM vane_app;
REVOKE ALL ON task_definition_edit_receipts FROM vane_app;
DROP TABLE task_definition_edit_receipts;

DROP POLICY IF EXISTS tenant_isolation ON task_definition_edit_operations;
DROP POLICY IF EXISTS tenant_visible ON task_definition_edit_operations;
ALTER TABLE task_definition_edit_operations DISABLE ROW LEVEL SECURITY;
REVOKE ALL ON task_definition_edit_operations FROM vane_app;
DROP TABLE task_definition_edit_operations;

ALTER TABLE agent_sessions
    DROP CONSTRAINT uq_agent_sessions_definition_edit_scope;

REVOKE INSERT (
    id, tenant_id, user_id, nl_description, spec_json, scope_json, status,
    push_strictness, execution_mode
) ON schedules FROM vane_app;
GRANT INSERT ON schedules TO vane_app;

REVOKE UPDATE (
    nl_description, spec_json, scope_json, status, updated_at,
    push_strictness, execution_mode,
    approved_definition_version, approved_definition_digest
) ON schedules FROM vane_app;
GRANT UPDATE ON schedules TO vane_app;

ALTER TABLE schedules
    DROP CONSTRAINT schedules_definition_edit_marker_complete,
    DROP COLUMN definition_edit_fence,
    DROP COLUMN definition_edit_operation_id;
