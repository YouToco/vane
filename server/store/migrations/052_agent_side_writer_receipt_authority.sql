-- 052: Give the definition-edit receipt role only the retained projection and
-- immutable event-ledger capabilities needed for its atomic B2 checkpoint.
-- No business schema or mutable event capability is introduced.

-- +goose Up

GRANT SELECT (turn_count, activated_tools)
    ON agent_sessions TO vane_edit_receipt;

GRANT SELECT ON agent_events TO vane_edit_receipt;
GRANT INSERT (
    tenant_id, user_id, session_id, sequence,
    batch_idempotency_key, batch_index, batch_size,
    kind, schema_version, payload, payload_digest, batch_digest
) ON agent_events TO vane_edit_receipt;
GRANT USAGE ON SEQUENCE agent_events_id_seq TO vane_edit_receipt;

-- +goose Down

-- A side-writer generation is non-regenerable audit history. Downgrading the
-- authority after the first such commit would silently return definition-edit
-- receipts to legacy-only writes, so the downgrade is allowed only while the
-- B2 path is still dark. Lock in runtime order to close the check/revoke race.
LOCK TABLE agent_sessions IN ACCESS EXCLUSIVE MODE
    /* migration 052 downgrade fence: projection root first */;
LOCK TABLE agent_events IN ACCESS EXCLUSIVE MODE
    /* migration 052 downgrade fence: append-only child second */;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM agent_events
         WHERE batch_idempotency_key LIKE 'side.%'
    ) THEN
        RAISE EXCEPTION
            '052: refusing downgrade while side-writer event generations exist';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE USAGE ON SEQUENCE agent_events_id_seq FROM vane_edit_receipt;
REVOKE INSERT (
    tenant_id, user_id, session_id, sequence,
    batch_idempotency_key, batch_index, batch_size,
    kind, schema_version, payload, payload_digest, batch_digest
) ON agent_events FROM vane_edit_receipt;
REVOKE SELECT ON agent_events FROM vane_edit_receipt;
REVOKE SELECT (turn_count, activated_tools)
    ON agent_sessions FROM vane_edit_receipt;
