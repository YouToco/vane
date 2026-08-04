-- 112: admit a versioned, grounded-but-quiet partial-coverage Research Brief.
--
-- Migration 107 remains the retained v3.1 unknown-only reader. This migration
-- adds a separate v3.2 finalization route. A grounded result must cite a
-- completed official Evidence item and can never deliver while any Tool step
-- is failed or indeterminate.

-- +goose Up

LOCK TABLE research_brief_syntheses IN ACCESS EXCLUSIVE MODE;

DROP TRIGGER research_brief_synthesis_admission_v31
    ON research_brief_syntheses;
CREATE TRIGGER research_brief_synthesis_admission_v31
BEFORE INSERT OR UPDATE ON research_brief_syntheses
FOR EACH ROW
WHEN (
    (convert_from(NEW.context_payload,'UTF8')::jsonb->>'schema_version') =
        'vane.research-synthesis-context/v3.1'
    AND NOT (
        NEW.status='finalized' AND
        convert_from(NEW.brief_payload,'UTF8')::jsonb->>'schema_version'=
            'vane.research-brief/v3.2'
    )
)
EXECUTE FUNCTION enforce_research_brief_synthesis_admission_v31();

-- +goose StatementBegin
CREATE FUNCTION enforce_research_brief_grounded_partial_v32()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    evidence_json JSONB;
    brief_json JSONB;
BEGIN
    IF TG_OP IS DISTINCT FROM 'UPDATE' OR
       NEW.tenant_id IS DISTINCT FROM
           NULLIF(current_setting('app.tenant_id',true),'')::bigint OR
       NEW.user_id IS DISTINCT FROM
           NULLIF(current_setting('app.user_id',true),'')::bigint OR
       OLD.status IS DISTINCT FROM 'spending' OR
       NEW.status IS DISTINCT FROM 'finalized' THEN
        RAISE EXCEPTION '112: grounded partial Brief transition is invalid';
    END IF;

    IF (to_jsonb(NEW)-ARRAY[
            'status','significance','decision','delivery_required','brief_payload',
            'brief_digest','finalized_at','updated_at'
        ]) IS DISTINCT FROM
       (to_jsonb(OLD)-ARRAY[
            'status','significance','decision','delivery_required','brief_payload',
            'brief_digest','finalized_at','updated_at'
        ]) THEN
        RAISE EXCEPTION '112: grounded partial Brief changed frozen inputs';
    END IF;

    evidence_json := convert_from(OLD.evidence_manifest,'UTF8')::jsonb;
    brief_json := convert_from(NEW.brief_payload,'UTF8')::jsonb;
    IF convert_from(OLD.context_payload,'UTF8')::jsonb->>'schema_version'
           IS DISTINCT FROM 'vane.research-synthesis-context/v3.1' OR
       evidence_json->>'schema_version'
           IS DISTINCT FROM 'vane.research-evidence-manifest/v3.1' OR
       jsonb_typeof(evidence_json->'items') IS DISTINCT FROM 'array' OR
       jsonb_typeof(evidence_json->'tool_failures') IS DISTINCT FROM 'array' OR
       jsonb_array_length(evidence_json->'tool_failures')<1 OR
       jsonb_typeof(brief_json) IS DISTINCT FROM 'object' OR
       (SELECT count(*) FROM jsonb_object_keys(brief_json))<>6 OR
       (brief_json-ARRAY[
           'schema_version','assessment','headline','summary','significance','citations'
       ])<>'{}'::jsonb OR
       brief_json->>'schema_version' IS DISTINCT FROM 'vane.research-brief/v3.2' OR
       brief_json->>'assessment' IS DISTINCT FROM 'grounded' OR
       brief_json->>'significance' IS DISTINCT FROM 'none' OR
       NEW.significance IS DISTINCT FROM 'none' OR
       NEW.decision IS DISTINCT FROM 'quiet' OR
       NEW.delivery_required IS DISTINCT FROM false OR
       NEW.brief_digest IS DISTINCT FROM encode(sha256(NEW.brief_payload),'hex') OR
       jsonb_typeof(brief_json->'headline') IS DISTINCT FROM 'string' OR
       octet_length(brief_json->>'headline') NOT BETWEEN 1 AND 1024 OR
       btrim(brief_json->>'headline') IS DISTINCT FROM brief_json->>'headline' OR
       jsonb_typeof(brief_json->'summary') IS DISTINCT FROM 'string' OR
       octet_length(brief_json->>'summary') NOT BETWEEN 1 AND 65536 OR
       btrim(brief_json->>'summary') IS DISTINCT FROM brief_json->>'summary' OR
       jsonb_typeof(brief_json->'citations') IS DISTINCT FROM 'array' OR
       jsonb_array_length(brief_json->'citations') NOT BETWEEN 1 AND 64 OR
       EXISTS (
           SELECT 1 FROM jsonb_array_elements(brief_json->'citations') citation
            WHERE jsonb_typeof(citation) IS DISTINCT FROM 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(citation))<>2
               OR (citation-ARRAY['kind','ref'])<>'{}'::jsonb
               OR jsonb_typeof(citation->'kind') IS DISTINCT FROM 'string'
               OR jsonb_typeof(citation->'ref') IS DISTINCT FROM 'string'
               OR octet_length(citation->>'ref') NOT BETWEEN 1 AND 255
               OR btrim(citation->>'ref') IS DISTINCT FROM citation->>'ref'
               OR (citation->>'kind'='current_evidence' AND
                   (citation->>'ref' !~ '^[1-9][0-9]*$' OR NOT EXISTS (
                       SELECT 1 FROM jsonb_array_elements(evidence_json->'items') item
                        WHERE item->>'evidence_id'=citation->>'ref')))
               OR (citation->>'kind'='history' AND NOT EXISTS (
                       SELECT 1
                         FROM jsonb_array_elements(
                             convert_from(OLD.history_manifest,'UTF8')::jsonb->'items'
                         ) item
                        WHERE item->>'record_id'=citation->>'ref'))
               OR ((citation->>'kind') IS DISTINCT FROM 'current_evidence' AND
                   (citation->>'kind') IS DISTINCT FROM 'history')
       ) OR EXISTS (
           SELECT 1 FROM jsonb_array_elements(brief_json->'citations') citation
            GROUP BY citation->>'kind',citation->>'ref' HAVING count(*)>1
       ) OR NOT EXISTS (
           SELECT 1
             FROM jsonb_array_elements(brief_json->'citations') citation
             JOIN jsonb_array_elements(evidence_json->'items') item
               ON citation->>'kind'='current_evidence'
              AND citation->>'ref'=item->>'evidence_id'
            WHERE item->>'trust_type'='official'
       ) THEN
        RAISE EXCEPTION '112: grounded partial Brief must cite official Evidence and stay quiet';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_brief_grounded_partial_v32()
    FROM PUBLIC;

CREATE TRIGGER research_brief_synthesis_grounded_partial_v32
BEFORE INSERT OR UPDATE ON research_brief_syntheses
FOR EACH ROW
WHEN (
    (convert_from(NEW.context_payload,'UTF8')::jsonb->>'schema_version') =
        'vane.research-synthesis-context/v3.1' AND
    NEW.status='finalized' AND
    convert_from(NEW.brief_payload,'UTF8')::jsonb->>'schema_version'=
        'vane.research-brief/v3.2'
)
EXECUTE FUNCTION enforce_research_brief_grounded_partial_v32();

-- +goose Down

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM research_brief_syntheses
         WHERE status='finalized' AND
               convert_from(brief_payload,'UTF8')::jsonb->>'schema_version'=
                   'vane.research-brief/v3.2'
    ) THEN
        RAISE EXCEPTION
            '112: grounded partial-coverage Brief evidence exists; restore from backup';
    END IF;
END
$$;
-- +goose StatementEnd

DROP TRIGGER research_brief_synthesis_grounded_partial_v32
    ON research_brief_syntheses;
DROP FUNCTION enforce_research_brief_grounded_partial_v32();

DROP TRIGGER research_brief_synthesis_admission_v31
    ON research_brief_syntheses;
CREATE TRIGGER research_brief_synthesis_admission_v31
BEFORE INSERT OR UPDATE ON research_brief_syntheses
FOR EACH ROW
WHEN ((convert_from(NEW.context_payload,'UTF8')::jsonb->>'schema_version') =
      'vane.research-synthesis-context/v3.1')
EXECUTE FUNCTION enforce_research_brief_synthesis_admission_v31();
