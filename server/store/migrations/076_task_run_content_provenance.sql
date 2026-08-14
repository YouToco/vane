-- 076: Source-free Tool result provenance.
--
-- Content remains globally de-duplicated by canonical_key, but a V2 Tool run
-- no longer invents a fetch_target/source row merely to satisfy the historical
-- content_items.source_id column. The immutable relationship is the exact
-- (run snapshot, invocation digest, content item) observation instead.

-- +goose Up

ALTER TABLE task_run_snapshots
    ADD CONSTRAINT uq_task_run_snapshots_provenance_scope
    UNIQUE (tenant_id, user_id, task_id, id);

ALTER TABLE content_items
    ALTER COLUMN source_id DROP NOT NULL;

COMMENT ON COLUMN content_items.source_id IS
    'Nullable legacy first-discovery fetch target. Source-free Tool V2 writes NULL and uses task_run_content_provenance.';

CREATE TABLE task_run_content_provenance (
    tenant_id             BIGINT      NOT NULL,
    user_id               BIGINT      NOT NULL,
    task_id               TEXT        NOT NULL,
    run_snapshot_id       BIGINT      NOT NULL,
    invocation_digest     TEXT        NOT NULL,
    content_item_ids      BIGINT[]    NOT NULL,
    observation_payload   BYTEA       NOT NULL,
    observation_digest    TEXT        NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_task_run_content_provenance
        PRIMARY KEY (run_snapshot_id, invocation_digest),
    CONSTRAINT fk_task_run_content_provenance_snapshot
        FOREIGN KEY (tenant_id, user_id, task_id, run_snapshot_id)
        REFERENCES task_run_snapshots (tenant_id, user_id, task_id, id)
        ON DELETE CASCADE,
    CONSTRAINT task_run_content_provenance_task_id_valid CHECK (
        task_id <> '' AND btrim(task_id) = task_id
        AND octet_length(task_id) <= 255
    ),
    CONSTRAINT task_run_content_provenance_invocation_digest_valid
        CHECK (invocation_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT task_run_content_provenance_observation_digest_valid
        CHECK (observation_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT task_run_content_provenance_payload_size_valid CHECK (
        octet_length(observation_payload) > 0
        AND octet_length(observation_payload) <= 8388608
    ),
    CONSTRAINT task_run_content_provenance_content_ids_valid CHECK (
        array_position(content_item_ids, NULL) IS NULL
        AND cardinality(content_item_ids) <= 256
    )
);

CREATE INDEX idx_task_run_content_provenance_task_created
    ON task_run_content_provenance (
        tenant_id, user_id, task_id, created_at DESC, run_snapshot_id DESC
    );

-- The trigger is a database-level type fence: only ref/v2 snapshots may own
-- Source-free provenance, and the digest must name an invocation in that exact
-- immutable payload. Application validation remains stricter (full seal and
-- exact observation payload digest), but a future direct writer cannot attach
-- arbitrary content to a run.
-- +goose StatementBegin
CREATE FUNCTION task_run_content_provenance_admission()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    snapshot_schema TEXT;
    snapshot_payload JSONB;
    observed_payload JSONB;
    payload_item_count INTEGER;
    distinct_content_count INTEGER;
BEGIN
    SELECT s.reference_schema_version,
           convert_from(s.payload, 'UTF8')::jsonb
      INTO snapshot_schema, snapshot_payload
      FROM public.task_run_snapshots s
     WHERE s.tenant_id = NEW.tenant_id
       AND s.user_id = NEW.user_id
       AND s.task_id = NEW.task_id
       AND s.id = NEW.run_snapshot_id
     FOR SHARE OF s;

    IF snapshot_schema IS DISTINCT FROM 'vane.run-snapshot-ref/v2'
       OR jsonb_typeof(snapshot_payload #> '{definition,tool_calls}')
            IS DISTINCT FROM 'array'
       OR NOT EXISTS (
            SELECT 1
              FROM jsonb_array_elements(
                       snapshot_payload #> '{definition,tool_calls}') call
             WHERE call ->> 'invocation_digest' =
                   NEW.invocation_digest
       ) THEN
        RAISE EXCEPTION
            'task run content provenance is outside the frozen Tool plan'
            USING ERRCODE = '23514';
    END IF;

    BEGIN
        observed_payload :=
            convert_from(NEW.observation_payload, 'UTF8')::jsonb;
    EXCEPTION
        WHEN character_not_in_repertoire OR untranslatable_character
            OR invalid_text_representation THEN
        RAISE EXCEPTION
            'task run content provenance payload is not valid JSON'
            USING ERRCODE = '23514';
    END;

    IF observed_payload ->> 'schema_version' IS DISTINCT FROM
           'vane.task-run-content-observation-set/v1'
       OR COALESCE(observed_payload ->> 'run_snapshot_id', '') <>
            NEW.run_snapshot_id::text
       OR observed_payload ->> 'invocation_digest'
            IS DISTINCT FROM NEW.invocation_digest
       OR jsonb_typeof(observed_payload -> 'items')
            IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION
            'task run content provenance payload differs from content identity'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.observation_digest IS DISTINCT FROM
           encode(sha256(NEW.observation_payload), 'hex') THEN
        RAISE EXCEPTION
            'task run content provenance digest differs from payload'
            USING ERRCODE = '23514';
    END IF;

    payload_item_count :=
        jsonb_array_length(observed_payload -> 'items');
    SELECT count(DISTINCT content_id)
      INTO distinct_content_count
      FROM unnest(NEW.content_item_ids) content_id;

    IF payload_item_count <> cardinality(NEW.content_item_ids)
       OR distinct_content_count <> cardinality(NEW.content_item_ids)
       OR EXISTS (
            SELECT 1
              FROM unnest(NEW.content_item_ids) WITH ORDINALITY
                       content(content_id, ordinal)
              FULL JOIN jsonb_array_elements(
                            observed_payload -> 'items') WITH ORDINALITY
                       observed(item, ordinal)
                USING (ordinal)
             WHERE content.content_id IS NULL
                OR observed.item IS NULL
                OR observed.item ->> 'content_item_id'
                     IS DISTINCT FROM content.content_id::text
       )
       OR EXISTS (
            SELECT 1
              FROM jsonb_array_elements(observed_payload -> 'items') item
             WHERE COALESCE(item ->> 'content_item_id', '') !~ '^[1-9][0-9]*$'
                OR COALESCE(item ->> 'canonical_key', '') = ''
       )
       OR EXISTS (
            SELECT 1
              FROM jsonb_array_elements(observed_payload -> 'items') item
              LEFT JOIN public.content_items ci
                ON ci.id = (item ->> 'content_item_id')::bigint
               AND ci.canonical_key = item ->> 'canonical_key'
             WHERE ci.id IS NULL
                OR NOT (ci.id = ANY(NEW.content_item_ids))
       )
       OR EXISTS (
            SELECT 1
              FROM unnest(NEW.content_item_ids) content_id
             WHERE NOT EXISTS (
                    SELECT 1
                      FROM jsonb_array_elements(
                               observed_payload -> 'items') item
                     WHERE item ->> 'content_item_id' =
                           content_id::text
             )
       ) THEN
        RAISE EXCEPTION
            'task run content provenance result set is invalid'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION task_run_content_provenance_admission()
    FROM PUBLIC;

CREATE TRIGGER trg_task_run_content_provenance_admission
BEFORE INSERT ON task_run_content_provenance
FOR EACH ROW
EXECUTE FUNCTION task_run_content_provenance_admission();

GRANT SELECT, INSERT ON task_run_content_provenance TO vane_app;

ALTER TABLE task_run_content_provenance ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON task_run_content_provenance
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON task_run_content_provenance AS RESTRICTIVE
    FOR ALL
    USING (tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint);

-- +goose Down

-- Source-free rows and their provenance are durable evidence. Downgrade is
-- permitted only before this runtime has written either shape.
LOCK TABLE task_run_content_provenance, content_items
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM task_run_content_provenance)
       OR EXISTS (SELECT 1 FROM content_items WHERE source_id IS NULL) THEN
        RAISE EXCEPTION
            '076: refusing downgrade while Source-free content evidence exists';
    END IF;
END
$$;
-- +goose StatementEnd

DROP POLICY IF EXISTS tenant_isolation
    ON task_run_content_provenance;
DROP POLICY IF EXISTS tenant_visible
    ON task_run_content_provenance;
ALTER TABLE task_run_content_provenance DISABLE ROW LEVEL SECURITY;
REVOKE ALL ON task_run_content_provenance FROM vane_app;
DROP TRIGGER IF EXISTS trg_task_run_content_provenance_admission
    ON task_run_content_provenance;
DROP FUNCTION IF EXISTS task_run_content_provenance_admission();
DROP TABLE task_run_content_provenance;

COMMENT ON COLUMN content_items.source_id IS
    'Legacy first-discovery fetch target.';
ALTER TABLE content_items
    ALTER COLUMN source_id SET NOT NULL;

ALTER TABLE task_run_snapshots
    DROP CONSTRAINT uq_task_run_snapshots_provenance_scope;
