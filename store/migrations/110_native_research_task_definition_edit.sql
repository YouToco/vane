-- 110: explicit decoder isolation for native Research V3 definition edits.
--
-- Historical proposal/prepared bytes remain protocol 1 and continue through
-- the frozen v1/v2 readers. Protocol 3 is a new writer/recovery lane using the
-- same per-task nonterminal exclusion and schedule marker.

-- +goose Up

ALTER TABLE task_definition_edit_operations
    ADD COLUMN operation_protocol SMALLINT NOT NULL DEFAULT 1;
ALTER TABLE task_definition_edit_operations
    ADD CONSTRAINT task_definition_edit_operation_protocol_valid
    CHECK (operation_protocol IN (1,3));

CREATE INDEX idx_task_definition_edit_operations_protocol_recovery
    ON task_definition_edit_operations
       (operation_protocol,tenant_id,takeover_not_before,id)
    WHERE status='executing' AND tombstoned_at IS NULL;

-- The existing edit coordinator is already the exact schedule/head writer.
-- Extend only its column-level protocol admission and current V3 evidence
-- surface; vane_app and receipt workers receive no new write capability.
GRANT INSERT (operation_protocol)
    ON task_definition_edit_operations TO vane_edit_coordinator;
GRANT SELECT (
    id,tenant_id,user_id,task_id,prepared_schedule,compiled_digest,
    status,phase,execution_version,tombstoned_at
) ON task_creation_operations TO vane_edit_coordinator;
GRANT SELECT ON research_v3_delivery_authorities
    TO vane_edit_coordinator;
GRANT INSERT (
    tenant_id,user_id,task_id,generation,definition_version,
    definition_digest,target_action_digest,action_authorization_digest,status
) ON research_v3_delivery_authorities TO vane_edit_coordinator;
GRANT UPDATE (status,enabled_at,revoked_at)
    ON research_v3_delivery_authorities TO vane_edit_coordinator;

-- +goose Down

-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM task_definition_edit_operations
                WHERE operation_protocol=3) THEN
        RAISE EXCEPTION '110: refusing downgrade while native V3 edits exist';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE UPDATE (status,enabled_at,revoked_at)
    ON research_v3_delivery_authorities FROM vane_edit_coordinator;
REVOKE INSERT (
    tenant_id,user_id,task_id,generation,definition_version,
    definition_digest,target_action_digest,action_authorization_digest,status
) ON research_v3_delivery_authorities FROM vane_edit_coordinator;
REVOKE SELECT ON research_v3_delivery_authorities
    FROM vane_edit_coordinator;
REVOKE SELECT (
    id,tenant_id,user_id,task_id,prepared_schedule,compiled_digest,
    status,phase,execution_version,tombstoned_at
) ON task_creation_operations FROM vane_edit_coordinator;
REVOKE INSERT (operation_protocol)
    ON task_definition_edit_operations FROM vane_edit_coordinator;

DROP INDEX idx_task_definition_edit_operations_protocol_recovery;
ALTER TABLE task_definition_edit_operations
    DROP CONSTRAINT task_definition_edit_operation_protocol_valid;
ALTER TABLE task_definition_edit_operations
    DROP COLUMN operation_protocol;

