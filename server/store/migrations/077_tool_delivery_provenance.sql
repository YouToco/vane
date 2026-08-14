-- 077: bind every Source-free Tool delivery to the exact frozen invocation
-- that observed its content. Retained V1/legacy deliveries remain NULL.

-- +goose Up

ALTER TABLE deliveries
    ADD COLUMN invocation_digest TEXT;

ALTER TABLE deliveries
    ADD CONSTRAINT deliveries_invocation_digest_valid CHECK (
        invocation_digest IS NULL
        OR invocation_digest ~ '^[0-9a-f]{64}$'
    );

COMMENT ON COLUMN deliveries.invocation_digest IS
    'Exact Tool invocation for ref/v2 deliveries; NULL for retained V1 and legacy rows.';

-- Database admission closes both bypass directions:
-- * a V2 batch cannot create a delivery without Tool provenance;
-- * a legacy/V1 batch cannot smuggle Tool provenance into its row.
-- The content item must belong to that invocation's immutable observation set.
-- +goose StatementBegin
CREATE FUNCTION tool_delivery_provenance_admission()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    snapshot_schema TEXT;
    batch_task_id TEXT;
    batch_snapshot_id BIGINT;
BEGIN
    SELECT b.schedule_id, b.run_snapshot_id
      INTO batch_task_id, batch_snapshot_id
      FROM public.push_batches b
     WHERE b.id = NEW.batch_id
       AND b.tenant_id = NEW.tenant_id
       AND b.user_id = NEW.user_id
     FOR SHARE OF b;

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'delivery batch scope is invalid'
            USING ERRCODE = '23514';
    END IF;

    IF batch_snapshot_id IS NOT NULL THEN
        SELECT s.reference_schema_version
          INTO snapshot_schema
          FROM public.task_run_snapshots s
         WHERE s.id = batch_snapshot_id
           AND s.tenant_id = NEW.tenant_id
           AND s.user_id = NEW.user_id
           AND s.task_id = batch_task_id
         FOR SHARE OF s;
        IF NOT FOUND THEN
            RAISE EXCEPTION
                'delivery run snapshot scope is invalid'
                USING ERRCODE = '23514';
        END IF;
    END IF;

    IF snapshot_schema IS NOT DISTINCT FROM
           'vane.run-snapshot-ref/v2' THEN
        -- content_items keeps its historical ON DELETE SET NULL contract.
        -- Preserve immutable Tool provenance while allowing the one-way
        -- content tombstone; scope/digest rebinding remains forbidden.
        IF TG_OP = 'UPDATE'
           AND OLD.content_item_id IS NOT NULL
           AND NEW.content_item_id IS NULL
           AND NEW.tenant_id IS NOT DISTINCT FROM OLD.tenant_id
           AND NEW.batch_id IS NOT DISTINCT FROM OLD.batch_id
           AND NEW.user_id IS NOT DISTINCT FROM OLD.user_id
           AND NEW.invocation_digest IS NOT DISTINCT FROM
               OLD.invocation_digest THEN
            RETURN NEW;
        END IF;
        IF NEW.invocation_digest IS NULL
           OR NEW.content_item_id IS NULL
           OR NOT EXISTS (
                SELECT 1
                 FROM public.task_run_content_provenance p
                 WHERE p.tenant_id = NEW.tenant_id
                   AND p.user_id = NEW.user_id
                   AND p.task_id = batch_task_id
                   AND p.run_snapshot_id = batch_snapshot_id
                   AND p.invocation_digest =
                       NEW.invocation_digest
                   AND NEW.content_item_id =
                       ANY(p.content_item_ids)
           ) THEN
            RAISE EXCEPTION
                'Tool delivery is outside the frozen observation set'
                USING ERRCODE = '23514';
        END IF;
    ELSIF NEW.invocation_digest IS NOT NULL THEN
        RAISE EXCEPTION
            'legacy delivery cannot carry Tool invocation provenance'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION tool_delivery_provenance_admission()
    FROM PUBLIC;

CREATE TRIGGER trg_tool_delivery_provenance_admission
BEFORE INSERT OR UPDATE OF
    tenant_id, batch_id, user_id, content_item_id, invocation_digest
ON deliveries
FOR EACH ROW
EXECUTE FUNCTION tool_delivery_provenance_admission();

-- +goose Down

LOCK TABLE deliveries
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM deliveries
         WHERE invocation_digest IS NOT NULL
    ) THEN
        RAISE EXCEPTION
            '077: refusing downgrade while Tool delivery evidence exists';
    END IF;
END
$$;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS trg_tool_delivery_provenance_admission
    ON deliveries;
DROP FUNCTION IF EXISTS tool_delivery_provenance_admission();
ALTER TABLE deliveries
    DROP CONSTRAINT deliveries_invocation_digest_valid;
ALTER TABLE deliveries
    DROP COLUMN invocation_digest;
