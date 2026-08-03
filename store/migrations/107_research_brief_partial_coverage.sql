-- 107: admit an exact partial/failed Tool outcome manifest for Research V3.
--
-- Migration 088 remains the byte-for-byte legacy admission function for
-- complete Evidence runs. This migration routes only synthesis-context/v3.1
-- rows to a new fence; unknown schemas still fail closed.

-- +goose Up

LOCK TABLE research_brief_syntheses IN ACCESS EXCLUSIVE MODE;

DROP TRIGGER research_brief_synthesis_admission_v3
    ON research_brief_syntheses;
CREATE TRIGGER research_brief_synthesis_admission_v3
BEFORE INSERT OR UPDATE ON research_brief_syntheses
FOR EACH ROW
WHEN ((convert_from(NEW.context_payload,'UTF8')::jsonb->>'schema_version') =
      'vane.research-synthesis-context/v3')
EXECUTE FUNCTION enforce_research_brief_synthesis_admission_v3();

-- +goose StatementBegin
CREATE FUNCTION enforce_research_brief_synthesis_admission_v31()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    evidence_json JSONB;
    history_json JSONB;
    context_json JSONB;
    brief_json JSONB;
    snapshot_json JSONB;
    expected_evidence JSONB;
    expected_evidence_context JSONB;
    expected_failures JSONB;
    expected_history JSONB;
    expected_history_context JSONB;
    expected_history_manifest JSONB;
    expected_context JSONB;
    expected_definition_context JSONB;
    expected_steps INTEGER;
    history_candidate_count BIGINT;
    history_returned_count INTEGER;
    history_continuation JSONB;
    history_cutoff TEXT;
    expected_request_digest TEXT;
BEGIN
    IF NEW.tenant_id IS DISTINCT FROM
           NULLIF(current_setting('app.tenant_id',true),'')::bigint OR
       NEW.user_id IS DISTINCT FROM
           NULLIF(current_setting('app.user_id',true),'')::bigint THEN
        RAISE EXCEPTION '107: research Brief scope differs from session';
    END IF;
    SELECT jsonb_array_length(convert_from(plan.plan_payload,'UTF8')::jsonb->'steps')
      INTO expected_steps
      FROM public.research_run_plans plan
      JOIN public.task_run_snapshots snapshot
        ON snapshot.id=plan.run_snapshot_id
       AND snapshot.tenant_id=plan.tenant_id
       AND snapshot.user_id=plan.user_id
       AND snapshot.task_id=plan.task_id
       AND snapshot.temporal_workflow_id=plan.temporal_workflow_id
       AND snapshot.temporal_run_id=plan.temporal_run_id
     WHERE plan.id=NEW.plan_id
       AND plan.tenant_id=NEW.tenant_id AND plan.user_id=NEW.user_id
       AND plan.task_id=NEW.task_id AND plan.run_snapshot_id=NEW.run_snapshot_id
       AND plan.temporal_workflow_id=NEW.temporal_workflow_id
       AND plan.temporal_run_id=NEW.temporal_run_id
       AND plan.definition_digest=NEW.definition_digest
       AND plan.plan_digest=NEW.plan_digest
       AND snapshot.reference_schema_version='vane.research-run-snapshot-ref/v3'
       AND snapshot.definition_digest=NEW.definition_digest;
    IF expected_steps IS NULL OR expected_steps<=0 OR expected_steps>16 THEN
        RAISE EXCEPTION '107: research Brief parent scope differs';
    END IF;

    context_json := convert_from(NEW.context_payload,'UTF8')::jsonb;
    evidence_json := convert_from(NEW.evidence_manifest,'UTF8')::jsonb;
    history_json := convert_from(NEW.history_manifest,'UTF8')::jsonb;
    SELECT convert_from(snapshot.payload,'UTF8')::jsonb
      INTO snapshot_json
      FROM public.task_run_snapshots snapshot
     WHERE snapshot.id=NEW.run_snapshot_id
       AND snapshot.tenant_id=NEW.tenant_id AND snapshot.user_id=NEW.user_id
       AND snapshot.task_id=NEW.task_id;
    history_cutoff := snapshot_json->>'history_through_utc';
    expected_definition_context := jsonb_build_object(
        'task_name',snapshot_json#>>'{definition,task_name}',
        'task_manual',snapshot_json#>>'{definition,task_manual}',
        'output',snapshot_json#>'{definition,output}',
        'notification',snapshot_json#>'{definition,notification}'
    );
    expected_request_digest := encode(sha256(convert_to(concat_ws(E'\n',
        'vane.research-brief-synthesis/v3',NEW.run_snapshot_id::text,NEW.plan_id::text,
        NEW.definition_digest,NEW.plan_digest,NEW.notification_threshold,
        NEW.context_digest,NEW.evidence_digest,NEW.history_digest
    ),'UTF8')),'hex');
    IF jsonb_typeof(context_json)<>'object' OR
       context_json->>'schema_version'<>'vane.research-synthesis-context/v3.1' OR
       context_json->'definition' IS DISTINCT FROM expected_definition_context OR
       NEW.notification_threshold IS DISTINCT FROM
           snapshot_json#>>'{definition,notification,minimum_significance}' OR
       NEW.request_digest IS DISTINCT FROM expected_request_digest OR
       evidence_json->>'schema_version'<>'vane.research-evidence-manifest/v3.1' OR
       jsonb_typeof(evidence_json->'items')<>'array' OR
       jsonb_typeof(evidence_json->'tool_failures')<>'array' OR
       jsonb_array_length(evidence_json->'tool_failures')<1 OR
       jsonb_array_length(evidence_json->'items')+
           jsonb_array_length(evidence_json->'tool_failures')<>expected_steps OR
       history_json->>'schema_version'<>'vane.research-history-manifest/v3' OR
       history_json->>'history_through_utc' IS DISTINCT FROM history_cutoff OR
       jsonb_typeof(history_json->'items')<>'array' OR
       jsonb_array_length(history_json->'items')>20 THEN
        RAISE EXCEPTION '107: research Brief manifest shape is invalid';
    END IF;

    IF TG_OP='INSERT' THEN
    SELECT coalesce(jsonb_agg(jsonb_build_object(
               'evidence_id',evidence.id,'ordinal',evidence.step_ordinal,
               'invocation_id',evidence.invocation_id,'tool_name',evidence.tool_name,
               'request_digest',evidence.request_digest,'result_digest',evidence.result_digest,
               'original_size',evidence.original_size,'truncated',evidence.truncated,
               'trust_type',evidence.trust_type
           ) ORDER BY evidence.step_ordinal),'[]'::jsonb),
           coalesce(jsonb_agg(jsonb_build_object(
               'evidence_id',evidence.id,'ordinal',evidence.step_ordinal,
               'invocation_id',evidence.invocation_id,'tool_name',evidence.tool_name,
               'request_digest',evidence.request_digest,'result_digest',evidence.result_digest,
               'original_size',evidence.original_size,'truncated',evidence.truncated,
               'trust_type',evidence.trust_type,
               'synthesis_visible_text',convert_from(evidence.result_bytes,'UTF8'),
               'context_stored_size',octet_length(evidence.result_bytes),
               'context_visible_size',octet_length(evidence.result_bytes),
               'context_visible_digest',evidence.result_digest,
               'context_truncated',false
           ) ORDER BY evidence.step_ordinal),'[]'::jsonb)
      INTO expected_evidence,expected_evidence_context
      FROM public.research_run_evidence evidence
      JOIN public.research_run_steps terminal
        ON terminal.tenant_id=evidence.tenant_id AND terminal.user_id=evidence.user_id
       AND terminal.task_id=evidence.task_id AND terminal.plan_id=evidence.plan_id
       AND terminal.temporal_run_id=evidence.temporal_run_id
       AND terminal.plan_digest=evidence.plan_digest
       AND terminal.step_ordinal=evidence.step_ordinal AND terminal.phase='completed'
       AND terminal.invocation_id=evidence.invocation_id
       AND terminal.tool_name=evidence.tool_name
       AND terminal.request_digest=evidence.request_digest
       AND terminal.result_digest=evidence.result_digest
     WHERE evidence.tenant_id=NEW.tenant_id AND evidence.user_id=NEW.user_id
       AND evidence.task_id=NEW.task_id AND evidence.plan_id=NEW.plan_id
       AND evidence.temporal_run_id=NEW.temporal_run_id
       AND evidence.plan_digest=NEW.plan_digest;
    SELECT coalesce(jsonb_agg(jsonb_build_object(
               'ordinal',terminal.step_ordinal,
               'invocation_id',terminal.invocation_id,
               'tool_name',terminal.tool_name,
               'request_digest',terminal.request_digest,
               'phase',terminal.phase,
               'error_code',terminal.error_code,
               'cost_micro_usd',terminal.cost_micro_usd
           ) ORDER BY terminal.step_ordinal),'[]'::jsonb)
      INTO expected_failures
      FROM public.research_run_steps terminal
     WHERE terminal.tenant_id=NEW.tenant_id AND terminal.user_id=NEW.user_id
       AND terminal.task_id=NEW.task_id AND terminal.plan_id=NEW.plan_id
       AND terminal.temporal_run_id=NEW.temporal_run_id
       AND terminal.plan_digest=NEW.plan_digest
       AND terminal.phase IN ('failed','indeterminate');
    IF evidence_json IS DISTINCT FROM jsonb_build_object(
           'schema_version','vane.research-evidence-manifest/v3.1',
           'items',expected_evidence,'tool_failures',expected_failures) OR
       jsonb_array_length(expected_evidence)+jsonb_array_length(expected_failures)<>
           expected_steps OR
       context_json->'current_evidence' IS DISTINCT FROM expected_evidence_context OR
       context_json->'tool_failures' IS DISTINCT FROM expected_failures THEN
        RAISE EXCEPTION '107: research Brief manifest is not exact terminal Tool outcomes';
    END IF;

    WITH shaped AS (
        SELECT kind,record_id,generated_at,candidate_count,
               jsonb_build_object(
                   'kind',kind,'record_id',record_id,'run_snapshot_id',run_snapshot_id,
                   'generated_at',to_char(generated_at AT TIME ZONE 'UTC',
                       'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                   'digest',digest,'coverage',coverage
               ) AS manifest_item,
               jsonb_strip_nulls(jsonb_build_object(
                   'kind',kind,'record_id',record_id,'run_snapshot_id',run_snapshot_id,
                   'generated_at',to_char(generated_at AT TIME ZONE 'UTC',
                       'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                   'digest',digest,'coverage',coverage,
                   'payload_text',payload_text,'gap_reason',gap_reason,
                   'context_stored_size',context_stored_size,
                   'context_visible_size',context_visible_size,
                   'context_visible_digest',context_visible_digest,
                   'context_truncated',context_truncated
               )) AS context_item
          FROM public.read_research_history_v3(
              NEW.tenant_id,NEW.user_id,NEW.task_id,NEW.run_snapshot_id,NEW.plan_id)
    )
    SELECT coalesce(jsonb_agg(manifest_item ORDER BY generated_at DESC,kind,record_id DESC),'[]'::jsonb),
           coalesce(jsonb_agg(context_item ORDER BY generated_at DESC,kind,record_id DESC),'[]'::jsonb),
           coalesce(max(candidate_count),0),count(*)::integer
      INTO expected_history,expected_history_context,history_candidate_count,history_returned_count
      FROM shaped;
    history_continuation := CASE WHEN history_candidate_count>history_returned_count THEN
        jsonb_build_object(
            'generated_at',expected_history->-1->>'generated_at',
            'kind',expected_history->-1->>'kind',
            'record_id',expected_history->-1->>'record_id'
        ) ELSE NULL END;
    expected_history_manifest := jsonb_build_object(
        'schema_version','vane.research-history-manifest/v3',
        'history_through_utc',history_cutoff,
        'candidate_count',history_candidate_count,
        'returned_count',history_returned_count,
        'truncated',history_candidate_count>history_returned_count,
        'items',expected_history
    ) || CASE WHEN history_continuation IS NULL THEN '{}'::jsonb
              ELSE jsonb_build_object('continuation',history_continuation) END;
    expected_context := jsonb_build_object(
        'schema_version','vane.research-synthesis-context/v3.1',
        'definition',expected_definition_context,
        'current_evidence',expected_evidence_context,
        'tool_failures',expected_failures,
        'history',jsonb_build_object(
            'history_through_utc',history_cutoff,
            'candidate_count',history_candidate_count,
            'returned_count',history_returned_count,
            'truncated',history_candidate_count>history_returned_count,
            'items',expected_history_context
        ) || CASE WHEN history_continuation IS NULL THEN '{}'::jsonb
                  ELSE jsonb_build_object('continuation',history_continuation) END
    );
    IF history_json IS DISTINCT FROM expected_history_manifest OR
       context_json IS DISTINCT FROM expected_context THEN
        RAISE EXCEPTION '107: research Brief history is not exact same-owner history';
    END IF;
    ELSE
        expected_evidence := evidence_json->'items';
        expected_failures := evidence_json->'tool_failures';
        expected_history := history_json->'items';
    END IF;

    IF NEW.status='finalized' THEN
       brief_json := convert_from(NEW.brief_payload,'UTF8')::jsonb;
    END IF;
    IF NEW.status='finalized' AND (
       jsonb_typeof(brief_json) IS DISTINCT FROM 'object' OR
       (SELECT count(*) FROM jsonb_object_keys(brief_json))<>6 OR
       (brief_json-ARRAY['schema_version','assessment','headline','summary','significance','citations'])<>'{}'::jsonb OR
       brief_json->>'schema_version' IS DISTINCT FROM 'vane.research-brief/v3.1' OR
       brief_json->>'assessment' IS DISTINCT FROM 'unknown' OR
       brief_json->>'significance' IS DISTINCT FROM 'none' OR
       brief_json->>'significance' IS DISTINCT FROM NEW.significance OR
       NEW.significance IS DISTINCT FROM 'none' OR
       NEW.decision IS DISTINCT FROM 'quiet' OR
       NEW.delivery_required IS DISTINCT FROM false OR
       jsonb_typeof(brief_json->'headline') IS DISTINCT FROM 'string' OR
       octet_length(brief_json->>'headline') NOT BETWEEN 1 AND 1024 OR
       btrim(brief_json->>'headline') IS DISTINCT FROM brief_json->>'headline' OR
       jsonb_typeof(brief_json->'summary') IS DISTINCT FROM 'string' OR
       octet_length(brief_json->>'summary') NOT BETWEEN 1 AND 65536 OR
       btrim(brief_json->>'summary') IS DISTINCT FROM brief_json->>'summary' OR
       jsonb_typeof(brief_json->'citations') IS DISTINCT FROM 'array' OR
       jsonb_array_length(brief_json->'citations')>64 OR EXISTS (
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
                       SELECT 1 FROM jsonb_array_elements(expected_evidence) item
                        WHERE item->>'evidence_id'=citation->>'ref')))
               OR (citation->>'kind'='history' AND NOT EXISTS (
                       SELECT 1 FROM jsonb_array_elements(expected_history) item
                        WHERE item->>'record_id'=citation->>'ref'))
               OR ((citation->>'kind') IS DISTINCT FROM 'current_evidence' AND
                   (citation->>'kind') IS DISTINCT FROM 'history')
       ) OR EXISTS (
           SELECT 1 FROM jsonb_array_elements(brief_json->'citations') citation
            GROUP BY citation->>'kind',citation->>'ref' HAVING count(*)>1
       )
    ) THEN
        RAISE EXCEPTION '107: partial-coverage Brief must be grounded unknown and quiet';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_brief_synthesis_admission_v31()
    FROM PUBLIC;

CREATE TRIGGER research_brief_synthesis_admission_v31
BEFORE INSERT OR UPDATE ON research_brief_syntheses
FOR EACH ROW
WHEN ((convert_from(NEW.context_payload,'UTF8')::jsonb->>'schema_version') =
      'vane.research-synthesis-context/v3.1')
EXECUTE FUNCTION enforce_research_brief_synthesis_admission_v31();

-- +goose StatementBegin
CREATE FUNCTION reject_research_brief_synthesis_schema_v31()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    RAISE EXCEPTION '107: research Brief context schema is unavailable';
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION reject_research_brief_synthesis_schema_v31()
    FROM PUBLIC;

CREATE TRIGGER research_brief_synthesis_reject_unknown_v31
BEFORE INSERT OR UPDATE ON research_brief_syntheses
FOR EACH ROW
WHEN ((convert_from(NEW.context_payload,'UTF8')::jsonb->>'schema_version')
      IS NULL OR
      (convert_from(NEW.context_payload,'UTF8')::jsonb->>'schema_version')
      NOT IN ('vane.research-synthesis-context/v3',
              'vane.research-synthesis-context/v3.1'))
EXECUTE FUNCTION reject_research_brief_synthesis_schema_v31();

-- +goose Down

-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION
        '107: irreversible partial-coverage Brief evidence may exist; restore from backup';
END $$;
-- +goose StatementEnd
