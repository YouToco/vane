-- 036: C2c-2 immutable run-snapshot v2 shadow sidecar.
--
-- The v1 row remains the only runtime source. A shadow row is append-only,
-- one-to-one with a newly-created v1 snapshot, and contains the exact
-- Approved/Adaptive bytes observed in the same transaction.

-- +goose Up

ALTER TABLE task_run_snapshots
    ADD CONSTRAINT uq_task_run_snapshots_shadow_parent
    UNIQUE (
        id, tenant_id, user_id, task_id,
        temporal_workflow_id, temporal_run_id
    );

CREATE TABLE task_run_snapshot_v2_shadows (
    id                           BIGSERIAL   PRIMARY KEY,
    run_snapshot_id              BIGINT      NOT NULL,
    tenant_id                    BIGINT      NOT NULL
        REFERENCES tenants (id) ON DELETE CASCADE,
    user_id                      BIGINT      NOT NULL,
    task_id                      TEXT        NOT NULL,
    temporal_workflow_id         TEXT        NOT NULL,
    temporal_run_id              TEXT        NOT NULL,
    status                       TEXT        NOT NULL,
    approved_definition_version  BIGINT,
    approved_definition_digest   TEXT,
    adaptive_version             BIGINT      NOT NULL,
    adaptive_digest              TEXT,
    payload                      BYTEA       NOT NULL,
    payload_digest               TEXT        NOT NULL,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_task_run_snapshot_v2_shadows_snapshot UNIQUE (run_snapshot_id),
    CONSTRAINT uq_task_run_snapshot_v2_shadows_run UNIQUE (temporal_run_id),
    CONSTRAINT fk_task_run_snapshot_v2_shadow_parent
        FOREIGN KEY (
            run_snapshot_id, tenant_id, user_id, task_id,
            temporal_workflow_id, temporal_run_id
        ) REFERENCES task_run_snapshots (
            id, tenant_id, user_id, task_id,
            temporal_workflow_id, temporal_run_id
        ) ON DELETE CASCADE,
    CONSTRAINT task_run_snapshot_v2_shadow_identity_valid CHECK (
        task_id <> '' AND btrim(task_id) = task_id AND octet_length(task_id) <= 255 AND
        temporal_workflow_id <> '' AND btrim(temporal_workflow_id) = temporal_workflow_id AND
        octet_length(temporal_workflow_id) <= 512 AND
        temporal_run_id <> '' AND btrim(temporal_run_id) = temporal_run_id AND
        octet_length(temporal_run_id) <= 512
    ),
    CONSTRAINT task_run_snapshot_v2_shadow_status_valid CHECK (
        status IN (
            'match',
            'legacy_compatible',
            'headless',
            'projection_mismatch',
            'adaptive_present',
            'adaptive_basis_mismatch',
            'adaptive_for_legacy'
        )
    ),
    CONSTRAINT task_run_snapshot_v2_shadow_approved_complete CHECK (
        (approved_definition_version IS NULL AND approved_definition_digest IS NULL AND
         status = 'headless') OR
        (approved_definition_version > 0 AND
         approved_definition_digest ~ '^[0-9a-f]{64}$' AND
         status <> 'headless')
    ),
    CONSTRAINT task_run_snapshot_v2_shadow_adaptive_valid CHECK (
        adaptive_version >= 0 AND
        ((adaptive_version = 0 AND adaptive_digest IS NULL) OR
         (adaptive_version > 0 AND adaptive_digest ~ '^[0-9a-f]{64}$'))
    ),
    CONSTRAINT task_run_snapshot_v2_shadow_status_complete CHECK (
        (status = 'headless' AND adaptive_version = 0) OR
        (status = 'match' AND adaptive_version = 0 AND
         convert_from(payload, 'UTF8')::jsonb #>> '{approved,payload,source_scope}'
            = 'approved_plan') OR
        (status = 'legacy_compatible' AND adaptive_version = 0 AND
         convert_from(payload, 'UTF8')::jsonb #>> '{approved,payload,source_scope}'
            = 'legacy_subscriptions') OR
        (status = 'projection_mismatch' AND adaptive_version = 0) OR
        (status = 'adaptive_present' AND adaptive_version > 0 AND
         convert_from(payload, 'UTF8')::jsonb #>> '{approved,payload,source_scope}'
            = 'approved_plan' AND
         (convert_from(payload, 'UTF8')::jsonb
            #>> '{adaptive,basis_definition_version}')::bigint
            = approved_definition_version AND
         convert_from(payload, 'UTF8')::jsonb
            #>> '{adaptive,basis_definition_digest}'
            = approved_definition_digest) OR
        (status = 'adaptive_basis_mismatch' AND adaptive_version > 0 AND (
         (convert_from(payload, 'UTF8')::jsonb
            #>> '{adaptive,basis_definition_version}')::bigint
            <> approved_definition_version OR
         convert_from(payload, 'UTF8')::jsonb
            #>> '{adaptive,basis_definition_digest}'
            <> approved_definition_digest
        )) OR
        (status = 'adaptive_for_legacy' AND adaptive_version > 0 AND
         convert_from(payload, 'UTF8')::jsonb #>> '{approved,payload,source_scope}'
            = 'legacy_subscriptions' AND
         (convert_from(payload, 'UTF8')::jsonb
            #>> '{adaptive,basis_definition_version}')::bigint
            = approved_definition_version AND
         convert_from(payload, 'UTF8')::jsonb
            #>> '{adaptive,basis_definition_digest}'
            = approved_definition_digest)
    ),
    CONSTRAINT task_run_snapshot_v2_shadow_payload_valid CHECK (
        octet_length(payload) > 0 AND octet_length(payload) <= 5242880 AND
        payload_digest ~ '^[0-9a-f]{64}$' AND
        payload_digest = encode(sha256(payload), 'hex')
    ),
    CONSTRAINT task_run_snapshot_v2_shadow_payload_complete CHECK (
        convert_from(payload, 'UTF8')::jsonb @> jsonb_build_object(
            'schema_version', 'vane.task-run-snapshot-shadow/v2',
            'status', status,
            'identity', jsonb_build_object(
                'tenant_id', tenant_id,
                'user_id', user_id,
                'task_id', task_id,
                'temporal_workflow_id', temporal_workflow_id,
                'temporal_run_id', temporal_run_id
            ),
            'legacy', jsonb_build_object('snapshot_id', run_snapshot_id)
        ) AND
        (convert_from(payload, 'UTF8')::jsonb #>> '{approved,version}')::bigint
            IS NOT DISTINCT FROM approved_definition_version AND
        convert_from(payload, 'UTF8')::jsonb #>> '{approved,digest}'
            IS NOT DISTINCT FROM approved_definition_digest AND
        COALESCE(
            (convert_from(payload, 'UTF8')::jsonb #>> '{adaptive,version}')::bigint,
            0
        ) = adaptive_version AND
        convert_from(payload, 'UTF8')::jsonb #>> '{adaptive,digest}'
            IS NOT DISTINCT FROM adaptive_digest
    )
);

CREATE INDEX idx_task_run_snapshot_v2_shadows_scope
    ON task_run_snapshot_v2_shadows (tenant_id, user_id, task_id, id);

GRANT SELECT ON task_run_snapshot_v2_shadows TO vane_app;
GRANT INSERT (
    run_snapshot_id, tenant_id, user_id, task_id, temporal_workflow_id,
    temporal_run_id, status, approved_definition_version,
    approved_definition_digest, adaptive_version, adaptive_digest,
    payload, payload_digest
) ON task_run_snapshot_v2_shadows TO vane_app;
GRANT USAGE ON SEQUENCE task_run_snapshot_v2_shadows_id_seq TO vane_app;

ALTER TABLE task_run_snapshot_v2_shadows ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON task_run_snapshot_v2_shadows
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON task_run_snapshot_v2_shadows AS RESTRICTIVE
    FOR ALL
    USING (tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint);

-- +goose Down

LOCK TABLE task_run_snapshots, task_run_snapshot_v2_shadows
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM task_run_snapshot_v2_shadows) THEN
        RAISE EXCEPTION '036: refusing downgrade while task run snapshot v2 shadows exist';
    END IF;
END $$;
-- +goose StatementEnd

DROP POLICY IF EXISTS tenant_isolation ON task_run_snapshot_v2_shadows;
DROP POLICY IF EXISTS tenant_visible ON task_run_snapshot_v2_shadows;
ALTER TABLE task_run_snapshot_v2_shadows DISABLE ROW LEVEL SECURITY;
REVOKE ALL ON SEQUENCE task_run_snapshot_v2_shadows_id_seq FROM vane_app;
REVOKE ALL ON task_run_snapshot_v2_shadows FROM vane_app;
DROP TABLE task_run_snapshot_v2_shadows;
ALTER TABLE task_run_snapshots
    DROP CONSTRAINT uq_task_run_snapshots_shadow_parent;
