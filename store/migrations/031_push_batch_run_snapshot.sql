-- 031: bind compiled push batches to the immutable Temporal run snapshot.
--
-- Temporal Reset starts a new RunID but replays the original SideEffect trace
-- from history. A globally unique logical trace therefore cannot distinguish
-- the old and new run. The application stores a snapshot-namespaced physical
-- key for compiled batches while retaining run_snapshot_id as ownership proof.
--
-- IMPORTANT: uq_push_batches_idem must remain unchanged. A pre-031 binary uses
-- `ON CONFLICT (idempotency_key) WHERE idempotency_key <> ''`; dropping or
-- replacing that exact arbiter makes rollback writes fail before application
-- code has a chance to recover. Keeping it also lets old and new binaries run
-- during a rolling/forward-fix deployment.

-- +goose Up

ALTER TABLE push_batches
    ADD COLUMN run_snapshot_id BIGINT,
    ADD CONSTRAINT fk_push_batches_run_snapshot
        FOREIGN KEY (run_snapshot_id) REFERENCES task_run_snapshots (id);

CREATE INDEX idx_push_batches_tenant_run_snapshot
    ON push_batches (tenant_id, run_snapshot_id)
    WHERE run_snapshot_id IS NOT NULL;

-- +goose Down

-- Once a compiled batch exists, dropping its run binding would silently
-- destroy the ownership evidence. The namespaced physical key prevents a
-- collision but is deliberately not accepted as authorization by itself.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM push_batches WHERE run_snapshot_id IS NOT NULL) THEN
        RAISE EXCEPTION '031: refusing downgrade while compiled push batches exist';
    END IF;
END $$;
-- +goose StatementEnd

DROP INDEX idx_push_batches_tenant_run_snapshot;

ALTER TABLE push_batches
    DROP CONSTRAINT fk_push_batches_run_snapshot,
    DROP COLUMN run_snapshot_id;
