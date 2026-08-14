-- 032: Agent Runtime C2a — separate user-approved task definitions from
-- bounded, automatically learned adaptive state.
--
-- This migration is deliberately a zero-call-point foundation. Existing
-- schedules remain legacy projections with no approved-definition head, and
-- every existing/new row defaults to compiled execution. Later C2 batches
-- may populate a head only after the immutable definition bytes and their
-- user-approval evidence have been committed atomically.

-- +goose Up

-- Add the mode without a one-step DEFAULT rewrite so the legacy backfill is
-- explicit and auditable. Unknown is an in-process routing sentinel and must
-- never be persisted.
ALTER TABLE schedules ADD COLUMN execution_mode TEXT;
UPDATE schedules SET execution_mode = 'compiled';
ALTER TABLE schedules
    ALTER COLUMN execution_mode SET DEFAULT 'compiled',
    ALTER COLUMN execution_mode SET NOT NULL,
    ADD COLUMN approved_definition_version BIGINT,
    ADD COLUMN approved_definition_digest TEXT,
    ADD CONSTRAINT schedules_execution_mode_valid CHECK (
        execution_mode IN ('compiled', 'discover_at_run')
    ),
    ADD CONSTRAINT schedules_approved_definition_head_complete CHECK (
        (approved_definition_version IS NULL AND
         approved_definition_digest IS NULL AND
         execution_mode = 'compiled') OR
        (approved_definition_version IS NOT NULL AND
         approved_definition_digest IS NOT NULL AND
         approved_definition_version > 0 AND
         approved_definition_digest ~ '^[0-9a-f]{64}$')
    ),
    -- id is already globally unique. This wider key exists so every child
    -- relation proves the exact tenant/user/task ownership rather than merely
    -- relying on the global task id as an implicit scope check.
    ADD CONSTRAINT uq_schedules_runtime_scope UNIQUE (tenant_id, user_id, id);

CREATE TABLE task_approved_definition_versions (
    tenant_id      BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    user_id        BIGINT      NOT NULL REFERENCES users (id),
    task_id        TEXT        NOT NULL,
    version        BIGINT      NOT NULL,
    schema_version TEXT        NOT NULL,
    execution_mode TEXT        NOT NULL,
    definition_digest TEXT     NOT NULL,
    payload        BYTEA       NOT NULL,
    approval_ref   TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_task_approved_definition_versions
        PRIMARY KEY (tenant_id, user_id, task_id, version),
    CONSTRAINT fk_task_approved_definition_schedule_scope
        FOREIGN KEY (tenant_id, user_id, task_id)
        REFERENCES schedules (tenant_id, user_id, id) ON DELETE CASCADE,
    CONSTRAINT task_approved_definition_version_positive CHECK (version > 0),
    CONSTRAINT task_approved_definition_schema_valid CHECK (
        schema_version <> '' AND btrim(schema_version) = schema_version AND
        octet_length(schema_version) <= 128
    ),
    CONSTRAINT task_approved_definition_mode_valid CHECK (
        execution_mode IN ('compiled', 'discover_at_run')
    ),
    CONSTRAINT task_approved_definition_payload_valid CHECK (
        octet_length(payload) > 0 AND octet_length(payload) <= 2097152 AND
        definition_digest ~ '^[0-9a-f]{64}$' AND
        definition_digest = encode(sha256(payload), 'hex')
    ),
    CONSTRAINT task_approved_definition_approval_ref_valid CHECK (
        approval_ref <> '' AND btrim(approval_ref) = approval_ref AND
        octet_length(approval_ref) <= 1024
    ),
    -- This identity is the target of schedules' current-head FK. Including
    -- digest and mode makes a pointer unable to reinterpret the same version
    -- under different bytes or a different execution route.
    CONSTRAINT uq_task_approved_definition_head_identity UNIQUE (
        tenant_id, user_id, task_id, version, definition_digest, execution_mode
    ),
    -- Adaptive state is fenced by the exact definition bytes used to derive
    -- it. Keep a digest-bearing identity that does not duplicate mode in the
    -- child row; mode is already immutable on the referenced definition.
    CONSTRAINT uq_task_approved_definition_digest_identity UNIQUE (
        tenant_id, user_id, task_id, version, definition_digest
    ),
    -- A confirmed action/result may be replayed after response loss. The same
    -- approval evidence must resolve to the original version, never append a
    -- second definition under a fresh version number.
    CONSTRAINT uq_task_approved_definition_approval_ref UNIQUE (
        tenant_id, user_id, task_id, approval_ref
    )
);

ALTER TABLE schedules
    ADD CONSTRAINT fk_schedules_approved_definition_head
    FOREIGN KEY (
        tenant_id, user_id, id, approved_definition_version,
        approved_definition_digest, execution_mode
    ) REFERENCES task_approved_definition_versions (
        tenant_id, user_id, task_id, version, definition_digest, execution_mode
    ) ON DELETE SET DEFAULT (
        approved_definition_version, approved_definition_digest, execution_mode
    );

CREATE INDEX idx_task_approved_definition_versions_created
    ON task_approved_definition_versions (
        tenant_id, user_id, task_id, created_at DESC, version DESC
    );

-- One current adaptive state per task. version is the CAS fence: writers must
-- update with WHERE version = <observed>, then advance it exactly once. The
-- application role may update this table but cannot delete it.
CREATE TABLE task_adaptive_states (
    tenant_id                         BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    user_id                           BIGINT      NOT NULL REFERENCES users (id),
    task_id                           TEXT        NOT NULL,
    version                           BIGINT      NOT NULL,
    schema_version                    TEXT        NOT NULL,
    payload_digest                    TEXT        NOT NULL,
    payload                           BYTEA       NOT NULL,
    basis_definition_version          BIGINT      NOT NULL,
    basis_definition_digest           TEXT        NOT NULL,
    last_known_good_definition_version BIGINT,
    created_at                        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_task_adaptive_states
        PRIMARY KEY (tenant_id, user_id, task_id),
    CONSTRAINT fk_task_adaptive_state_schedule_scope
        FOREIGN KEY (tenant_id, user_id, task_id)
        REFERENCES schedules (tenant_id, user_id, id) ON DELETE CASCADE,
    CONSTRAINT fk_task_adaptive_state_definition_basis
        FOREIGN KEY (
            tenant_id, user_id, task_id,
            basis_definition_version, basis_definition_digest
        ) REFERENCES task_approved_definition_versions (
            tenant_id, user_id, task_id, version, definition_digest
        ),
    CONSTRAINT fk_task_adaptive_state_last_known_good
        FOREIGN KEY (
            tenant_id, user_id, task_id, last_known_good_definition_version
        ) REFERENCES task_approved_definition_versions (
            tenant_id, user_id, task_id, version
        ),
    CONSTRAINT task_adaptive_state_version_positive CHECK (version > 0),
    CONSTRAINT task_adaptive_state_definition_basis_valid CHECK (
        basis_definition_version > 0 AND
        basis_definition_digest ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT task_adaptive_state_schema_valid CHECK (
        schema_version <> '' AND btrim(schema_version) = schema_version AND
        octet_length(schema_version) <= 128
    ),
    CONSTRAINT task_adaptive_state_payload_valid CHECK (
        -- Keep the database boundary identical to taskstate V1's 256 KiB
        -- frozen reader; rows accepted here must never be undecodable solely
        -- because the store admitted a larger payload.
        octet_length(payload) > 0 AND octet_length(payload) <= 262144 AND
        payload_digest ~ '^[0-9a-f]{64}$' AND
        payload_digest = encode(sha256(payload), 'hex')
    ),
    CONSTRAINT task_adaptive_state_last_known_good_valid CHECK (
        last_known_good_definition_version IS NULL OR
        (last_known_good_definition_version > 0 AND
         last_known_good_definition_version = basis_definition_version)
    )
);

-- Approved definitions are append-only to the application. Tenant lifecycle
-- deletion is performed by the owner-only purge path and ON DELETE CASCADE.
GRANT SELECT, INSERT ON task_approved_definition_versions TO vane_app;
GRANT SELECT, INSERT, UPDATE ON task_adaptive_states TO vane_app;

ALTER TABLE task_approved_definition_versions ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON task_approved_definition_versions
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON task_approved_definition_versions AS RESTRICTIVE
    FOR ALL
    USING (tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint);

ALTER TABLE task_adaptive_states ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON task_adaptive_states
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON task_adaptive_states AS RESTRICTIVE
    FOR ALL
    USING (tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint);

-- +goose Down

-- Once any immutable definition, learned state, non-compiled mode, or current
-- head exists, downgrading would either erase durable meaning or silently
-- reinterpret a dynamic task as compiled. Only the untouched C2a foundation
-- may be rolled back.
--
-- Writers lock their schedule row before touching either C2a table. Take the
-- parent table first, with a mode that also blocks SELECT ... FOR UPDATE, so a
-- writer that started before this migration can finish before the guard reads
-- and a later writer cannot enter until this transaction commits. Locking the
-- children afterwards preserves that schedule-first order and closes the
-- guard-to-DDL window for owner/direct SQL as well.
LOCK TABLE schedules IN ACCESS EXCLUSIVE MODE;
LOCK TABLE task_approved_definition_versions IN ACCESS EXCLUSIVE MODE;
LOCK TABLE task_adaptive_states IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM task_adaptive_states) OR
       EXISTS (SELECT 1 FROM task_approved_definition_versions) OR
       EXISTS (
           SELECT 1 FROM schedules
            WHERE approved_definition_version IS NOT NULL
               OR approved_definition_digest IS NOT NULL
               OR execution_mode <> 'compiled'
       ) THEN
        RAISE EXCEPTION '032: refusing downgrade while approved/adaptive runtime state exists';
    END IF;
END $$;
-- +goose StatementEnd

ALTER TABLE schedules DROP CONSTRAINT fk_schedules_approved_definition_head;

DROP POLICY IF EXISTS tenant_isolation ON task_adaptive_states;
DROP POLICY IF EXISTS tenant_visible ON task_adaptive_states;
ALTER TABLE task_adaptive_states DISABLE ROW LEVEL SECURITY;
REVOKE ALL ON task_adaptive_states FROM vane_app;
DROP TABLE task_adaptive_states;

DROP POLICY IF EXISTS tenant_isolation ON task_approved_definition_versions;
DROP POLICY IF EXISTS tenant_visible ON task_approved_definition_versions;
ALTER TABLE task_approved_definition_versions DISABLE ROW LEVEL SECURITY;
REVOKE ALL ON task_approved_definition_versions FROM vane_app;
DROP TABLE task_approved_definition_versions;

ALTER TABLE schedules
    DROP CONSTRAINT uq_schedules_runtime_scope,
    DROP CONSTRAINT schedules_approved_definition_head_complete,
    DROP CONSTRAINT schedules_execution_mode_valid,
    DROP COLUMN approved_definition_digest,
    DROP COLUMN approved_definition_version,
    DROP COLUMN execution_mode;
