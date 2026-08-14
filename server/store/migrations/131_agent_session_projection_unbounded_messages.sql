-- 131: remove the application-level Agent session message-count ceiling.
--
-- Migration 035 bounded one atomic event batch to 64 rows because every turn
-- stored a complete session projection and the interactive Agent truncated the
-- projection to 60 messages. Interactive requests now preserve the complete
-- scrubbed session history and omit a model completion cap, so the ledger must
-- not reintroduce the retired message-count limit at its persistence boundary.
-- Per-event payload integrity and size checks, append-only ACL/RLS, exact replay,
-- tenant scope, and transaction atomicity remain unchanged.

-- +goose Up

ALTER TABLE agent_events
    DROP CONSTRAINT agent_events_batch_index_valid;
ALTER TABLE agent_events
    ADD CONSTRAINT agent_events_batch_index_valid CHECK (
        batch_size >= 1 AND batch_index BETWEEN 0 AND batch_size - 1
    );

-- +goose Down

-- Old binaries reject projection snapshots with more than 60 messages, which
-- means a normal start + messages + completion batch becomes incompatible at
-- 63 rows. Refuse downgrade once such immutable history exists instead of
-- restoring a schema that an old projector cannot read.
LOCK TABLE agent_events IN ACCESS EXCLUSIVE MODE
    /* migration 131 downgrade fence */;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM agent_events WHERE batch_size > 62) THEN
        RAISE EXCEPTION '131: refusing downgrade while wide agent event batches exist';
    END IF;
END $$;
-- +goose StatementEnd

ALTER TABLE agent_events
    DROP CONSTRAINT agent_events_batch_index_valid;
ALTER TABLE agent_events
    ADD CONSTRAINT agent_events_batch_index_valid CHECK (
        batch_size BETWEEN 1 AND 64
        AND batch_index BETWEEN 0 AND batch_size - 1
    );
