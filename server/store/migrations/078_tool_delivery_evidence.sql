-- 078: persist the ordered evidence bundle used by a Tool V2 delivery.
-- The application stores presentation metadata, while this database fence
-- proves every content/invocation pair belongs to the exact run snapshot.

-- +goose Up

ALTER TABLE deliveries
    ADD COLUMN tool_evidence_required BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN tool_evidence JSONB;

ALTER TABLE deliveries
    ADD CONSTRAINT deliveries_tool_evidence_shape_valid CHECK (
        (NOT tool_evidence_required OR tool_evidence IS NOT NULL)
        AND (
            tool_evidence IS NULL OR (
                jsonb_typeof(tool_evidence) = 'array'
                AND jsonb_array_length(tool_evidence) BETWEEN 1 AND 8
            )
        )
    );

-- +goose StatementBegin
CREATE FUNCTION tool_delivery_evidence_admission()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    snapshot_schema TEXT;
    batch_task_id TEXT;
    batch_snapshot_id BIGINT;
    first_evidence JSONB;
BEGIN
    IF TG_OP = 'UPDATE'
       AND (
           NEW.tool_evidence_required IS DISTINCT FROM
               OLD.tool_evidence_required
           OR NEW.tool_evidence IS DISTINCT FROM OLD.tool_evidence
       ) THEN
        RAISE EXCEPTION 'Tool delivery evidence is immutable'
            USING ERRCODE = '23514';
    END IF;

    SELECT b.schedule_id, b.run_snapshot_id
      INTO batch_task_id, batch_snapshot_id
      FROM public.push_batches b
     WHERE b.id = NEW.batch_id
       AND b.tenant_id = NEW.tenant_id
       AND b.user_id = NEW.user_id
     FOR SHARE OF b;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'delivery evidence batch scope is invalid'
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
            RAISE EXCEPTION 'delivery evidence snapshot scope is invalid'
                USING ERRCODE = '23514';
        END IF;
    END IF;

    IF snapshot_schema IS NOT DISTINCT FROM
           'vane.run-snapshot-ref/v2' THEN
        IF NEW.tool_evidence_required
           AND NEW.tool_evidence IS NULL THEN
            RAISE EXCEPTION 'Tool delivery requires evidence'
                USING ERRCODE = '23514';
        END IF;
        IF NEW.tool_evidence IS NULL THEN
            RETURN NEW;
        END IF;
        first_evidence := NEW.tool_evidence -> 0;
        IF (
               NEW.content_item_id IS NOT NULL
               AND (
                   first_evidence ->> 'content_item_id'
                       IS DISTINCT FROM NEW.content_item_id::TEXT
                   OR first_evidence ->> 'invocation_digest'
                       IS DISTINCT FROM NEW.invocation_digest
               )
           )
           OR EXISTS (
                SELECT 1
                  FROM jsonb_array_elements(NEW.tool_evidence) AS e(value)
                 WHERE jsonb_typeof(e.value) <> 'object'
                    OR COALESCE(e.value ->> 'content_item_id', '')
                         !~ '^[1-9][0-9]*$'
                    OR COALESCE(e.value ->> 'invocation_digest', '')
                         !~ '^[0-9a-f]{64}$'
                    OR jsonb_typeof(e.value -> 'metadata')
                         IS DISTINCT FROM 'object'
                    OR NOT EXISTS (
                        SELECT 1
                          FROM public.task_run_content_provenance p
                         WHERE p.tenant_id = NEW.tenant_id
                           AND p.user_id = NEW.user_id
                           AND p.task_id = batch_task_id
                           AND p.run_snapshot_id = batch_snapshot_id
                           AND p.invocation_digest =
                               e.value ->> 'invocation_digest'
                           AND (e.value ->> 'content_item_id')::BIGINT =
                               ANY(p.content_item_ids)
                    )
           ) THEN
            RAISE EXCEPTION
                'Tool delivery evidence is outside the frozen observation set'
                USING ERRCODE = '23514';
        END IF;
    ELSIF NEW.tool_evidence_required
       OR NEW.tool_evidence IS NOT NULL THEN
        RAISE EXCEPTION
            'legacy delivery cannot carry Tool evidence'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION tool_delivery_evidence_admission()
    FROM PUBLIC;

CREATE TRIGGER trg_tool_delivery_evidence_admission
BEFORE INSERT OR UPDATE OF
    tenant_id, batch_id, user_id, content_item_id, invocation_digest,
    tool_evidence_required, tool_evidence
ON deliveries
FOR EACH ROW
EXECUTE FUNCTION tool_delivery_evidence_admission();

COMMENT ON COLUMN deliveries.tool_evidence IS
    'Ordered canonical Tool evidence manifest used to render this delivery.';

-- +goose Down

LOCK TABLE deliveries
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM deliveries
         WHERE tool_evidence_required OR tool_evidence IS NOT NULL
    ) THEN
        RAISE EXCEPTION
            '078: refusing downgrade while Tool delivery evidence exists';
    END IF;
END
$$;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS trg_tool_delivery_evidence_admission
    ON deliveries;
DROP FUNCTION IF EXISTS tool_delivery_evidence_admission();
ALTER TABLE deliveries
    DROP CONSTRAINT deliveries_tool_evidence_shape_valid;
ALTER TABLE deliveries
    DROP COLUMN tool_evidence,
    DROP COLUMN tool_evidence_required;
