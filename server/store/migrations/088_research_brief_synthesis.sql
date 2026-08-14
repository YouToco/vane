-- 088: V3 research Brief synthesis state and immutable terminal artifacts.
--
-- This is deliberately independent from V1/V2 run outcomes, canonical Briefs,
-- push batches and deliveries. A prepared row freezes the exact model-visible
-- context and evidence/history manifests before the paid synthesis claim.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE task_run_snapshots,research_run_plans,research_run_steps,
           research_run_evidence IN ACCESS EXCLUSIVE MODE;

CREATE TABLE research_brief_syntheses (
    id                       BIGSERIAL   PRIMARY KEY,
    tenant_id                BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    user_id                  BIGINT      NOT NULL REFERENCES users (id),
    task_id                  TEXT        NOT NULL,
    run_snapshot_id          BIGINT      NOT NULL REFERENCES task_run_snapshots (id),
    plan_id                  BIGINT      NOT NULL REFERENCES research_run_plans (id),
    temporal_workflow_id     TEXT        NOT NULL,
    temporal_run_id          TEXT        NOT NULL,
    definition_digest        TEXT        NOT NULL,
    plan_digest              TEXT        NOT NULL,
    notification_threshold   TEXT        NOT NULL,
    request_digest           TEXT        NOT NULL,
    context_payload          BYTEA       NOT NULL,
    context_digest           TEXT        NOT NULL,
    evidence_manifest        BYTEA       NOT NULL,
    evidence_digest          TEXT        NOT NULL,
    history_manifest         BYTEA       NOT NULL,
    history_digest           TEXT        NOT NULL,
    schema_version           TEXT        NOT NULL,
    status                   TEXT        NOT NULL DEFAULT 'prepared',
    significance             TEXT,
    decision                 TEXT,
    delivery_required        BOOLEAN,
    brief_payload            BYTEA,
    brief_digest             TEXT,
    failure_code             TEXT        NOT NULL DEFAULT '',
    spending_started_at      TIMESTAMPTZ,
    finalized_at             TIMESTAMPTZ,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_research_brief_synthesis_snapshot UNIQUE (run_snapshot_id),
    CONSTRAINT uq_research_brief_synthesis_plan UNIQUE (plan_id),
    CONSTRAINT uq_research_brief_synthesis_temporal_run UNIQUE (temporal_run_id),
    CONSTRAINT uq_research_brief_synthesis_scope
        UNIQUE (id,tenant_id,user_id,task_id,run_snapshot_id,plan_id),
    CONSTRAINT ck_research_brief_synthesis_identity CHECK (
        btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255 AND
        btrim(temporal_workflow_id)=temporal_workflow_id AND
        octet_length(temporal_workflow_id) BETWEEN 1 AND 512 AND
        btrim(temporal_run_id)=temporal_run_id AND
        octet_length(temporal_run_id) BETWEEN 1 AND 512
    ),
    CONSTRAINT ck_research_brief_synthesis_digests CHECK (
        definition_digest ~ '^[0-9a-f]{64}$' AND
        plan_digest ~ '^[0-9a-f]{64}$' AND
        request_digest ~ '^[0-9a-f]{64}$' AND
        context_digest ~ '^[0-9a-f]{64}$' AND
        evidence_digest ~ '^[0-9a-f]{64}$' AND
        history_digest ~ '^[0-9a-f]{64}$' AND
        (brief_digest IS NULL OR brief_digest ~ '^[0-9a-f]{64}$')
    ),
    CONSTRAINT ck_research_brief_synthesis_payloads CHECK (
        octet_length(context_payload) BETWEEN 2 AND 33554432 AND
        octet_length(evidence_manifest) BETWEEN 2 AND 65536 AND
        octet_length(history_manifest) BETWEEN 2 AND 65536 AND
        position(decode('00','hex') in context_payload)=0 AND
        position(decode('00','hex') in evidence_manifest)=0 AND
        position(decode('00','hex') in history_manifest)=0 AND
        context_digest=encode(sha256(context_payload),'hex') AND
        evidence_digest=encode(sha256(evidence_manifest),'hex') AND
        history_digest=encode(sha256(history_manifest),'hex') AND
        (brief_payload IS NULL OR (
            octet_length(brief_payload) BETWEEN 2 AND 262144 AND
            position(decode('00','hex') in brief_payload)=0 AND
            brief_digest=encode(sha256(brief_payload),'hex')
        ))
    ),
    CONSTRAINT ck_research_brief_synthesis_schema CHECK (
        schema_version='vane.research-brief-synthesis/v3'
    ),
    CONSTRAINT ck_research_brief_synthesis_threshold CHECK (
        notification_threshold IN ('major_updates_only','all_qualified_updates')
    ),
    CONSTRAINT ck_research_brief_synthesis_status CHECK (
        status IN ('prepared','spending','finalized','ambiguous','failed')
    ),
    CONSTRAINT ck_research_brief_synthesis_significance CHECK (
        significance IS NULL OR significance IN ('none','qualified','major')
    ),
    CONSTRAINT ck_research_brief_synthesis_decision CHECK (
        decision IS NULL OR decision IN ('quiet','deliver')
    ),
    CONSTRAINT ck_research_brief_synthesis_failure CHECK (
        octet_length(failure_code)<=128 AND btrim(failure_code)=failure_code
    ),
    CONSTRAINT ck_research_brief_synthesis_shape CHECK (
        (
            status='prepared' AND significance IS NULL AND decision IS NULL AND
            delivery_required IS NULL AND brief_payload IS NULL AND brief_digest IS NULL AND
            failure_code='' AND spending_started_at IS NULL AND finalized_at IS NULL
        ) OR (
            status='spending' AND significance IS NULL AND decision IS NULL AND
            delivery_required IS NULL AND brief_payload IS NULL AND brief_digest IS NULL AND
            failure_code='' AND spending_started_at IS NOT NULL AND finalized_at IS NULL
        ) OR (
            status='finalized' AND significance IS NOT NULL AND decision IS NOT NULL AND
            delivery_required IS NOT NULL AND brief_payload IS NOT NULL AND brief_digest IS NOT NULL AND
            failure_code='' AND spending_started_at IS NOT NULL AND finalized_at IS NOT NULL
        ) OR (
            status IN ('ambiguous','failed') AND significance IS NULL AND decision IS NULL AND
            delivery_required=false AND brief_payload IS NULL AND brief_digest IS NULL AND
            failure_code<>'' AND finalized_at IS NOT NULL
        )
    ),
    CONSTRAINT ck_research_brief_synthesis_notification_decision CHECK (
        status<>'finalized' OR (
            (
                decision='deliver' AND delivery_required=true AND
                (
                    (notification_threshold='major_updates_only' AND significance='major') OR
                    (notification_threshold='all_qualified_updates' AND significance IN ('qualified','major'))
                )
            ) OR (
                decision='quiet' AND delivery_required=false AND
                (
                    (notification_threshold='major_updates_only' AND significance IN ('none','qualified')) OR
                    (notification_threshold='all_qualified_updates' AND significance='none')
                )
            )
        )
    )
);

CREATE INDEX idx_research_brief_synthesis_scope_created
    ON research_brief_syntheses
       (tenant_id,user_id,task_id,created_at DESC,id DESC);

-- One narrow definer projection lets vane_app read retained same-owner
-- history without granting broad SELECT on the legacy artifact tables.
-- The cutoff is taken from the immutable current snapshot, never from input.
-- +goose StatementBegin
CREATE FUNCTION read_research_history_v3(
    target_tenant_id BIGINT,
    target_user_id BIGINT,
    target_task_id TEXT,
    current_snapshot_id BIGINT,
    current_plan_id BIGINT
)
RETURNS TABLE (
    kind TEXT,
    record_id TEXT,
    run_snapshot_id BIGINT,
    generated_at TIMESTAMPTZ,
    digest TEXT,
    coverage TEXT,
    payload_text TEXT,
    gap_reason TEXT,
    context_stored_size INTEGER,
    context_visible_size INTEGER,
    context_visible_digest TEXT,
    context_truncated BOOLEAN,
    candidate_count BIGINT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    history_cutoff TIMESTAMPTZ;
BEGIN
    IF target_tenant_id IS DISTINCT FROM
           NULLIF(current_setting('app.tenant_id',true),'')::bigint OR
       target_user_id IS DISTINCT FROM
           NULLIF(current_setting('app.user_id',true),'')::bigint THEN
        RAISE EXCEPTION '088: research history scope differs from session';
    END IF;
    SELECT (convert_from(snapshot.payload,'UTF8')::jsonb->>'history_through_utc')::timestamptz
      INTO history_cutoff
      FROM public.task_run_snapshots snapshot
      JOIN public.research_run_plans plan
        ON plan.id=current_plan_id AND plan.run_snapshot_id=snapshot.id
       AND plan.tenant_id=snapshot.tenant_id AND plan.user_id=snapshot.user_id
       AND plan.task_id=snapshot.task_id
     WHERE snapshot.id=current_snapshot_id
       AND snapshot.tenant_id=target_tenant_id
       AND snapshot.user_id=target_user_id
       AND snapshot.task_id=target_task_id
       AND snapshot.reference_schema_version='vane.research-run-snapshot-ref/v3';
    IF history_cutoff IS NULL THEN
        RAISE EXCEPTION '088: research history parent scope differs';
    END IF;
    RETURN QUERY
    WITH candidates AS (
        SELECT 'v3_brief'::text AS kind,brief_v3.id::text AS record_id,
               brief_v3.run_snapshot_id,brief_v3.finalized_at AS generated_at,
               brief_v3.brief_digest AS digest,'exact'::text AS coverage,
               convert_from(brief_v3.brief_payload,'UTF8') AS payload_text,
               NULL::text AS gap_reason
          FROM public.research_brief_syntheses brief_v3
         WHERE brief_v3.tenant_id=target_tenant_id
           AND brief_v3.user_id=target_user_id AND brief_v3.task_id=target_task_id
           AND brief_v3.plan_id<>current_plan_id AND brief_v3.status='finalized'
           AND brief_v3.finalized_at<=history_cutoff
        UNION ALL
        SELECT 'legacy_v1_brief','brief:'||brief.id::text,brief.run_snapshot_id,
               brief.generated_at,brief.payload_digest,'legacy',
               convert_from(brief.payload,'UTF8'),NULL::text
          FROM public.brief_snapshots brief
         WHERE brief.tenant_id=target_tenant_id AND brief.user_id=target_user_id
           AND brief.task_id=target_task_id AND brief.generated_at<=history_cutoff
           AND brief.run_snapshot_id<>current_snapshot_id
           AND NOT EXISTS (SELECT 1 FROM public.research_brief_syntheses v3
                WHERE v3.run_snapshot_id=brief.run_snapshot_id AND v3.status='finalized')
        UNION ALL
        SELECT 'legacy_v1_observation',
               'observation:'||observation.run_snapshot_id::text||':'||observation.invocation_digest,
               observation.run_snapshot_id,observation.created_at,
               observation.observation_digest,'legacy',
               convert_from(observation.observation_payload,'UTF8'),NULL::text
          FROM public.task_run_content_provenance observation
         WHERE observation.tenant_id=target_tenant_id AND observation.user_id=target_user_id
           AND observation.task_id=target_task_id AND observation.created_at<=history_cutoff
           AND observation.run_snapshot_id<>current_snapshot_id
           AND NOT EXISTS (SELECT 1 FROM public.research_brief_syntheses v3
                WHERE v3.run_snapshot_id=observation.run_snapshot_id AND v3.status='finalized')
        UNION ALL
        SELECT 'legacy_run_gap','run:'||outcome.id::text,outcome.run_snapshot_id,
               outcome.finalized_at,outcome.outcome_digest,'unavailable',NULL::text,
               CASE WHEN outcome.result IN ('failed','interrupted') THEN 'legacy_run_failed'
                    ELSE 'legacy_evidence_unavailable' END
          FROM public.task_run_outcomes outcome
         WHERE outcome.tenant_id=target_tenant_id AND outcome.user_id=target_user_id
           AND outcome.task_id=target_task_id AND outcome.status='finalized'
           AND outcome.finalized_at<=history_cutoff
           AND outcome.run_snapshot_id<>current_snapshot_id
           AND NOT EXISTS (SELECT 1 FROM public.brief_snapshots brief
                WHERE brief.run_snapshot_id=outcome.run_snapshot_id)
           AND NOT EXISTS (SELECT 1 FROM public.task_run_content_provenance observation
                WHERE observation.run_snapshot_id=outcome.run_snapshot_id)
           AND NOT EXISTS (SELECT 1 FROM public.research_brief_syntheses v3
                WHERE v3.run_snapshot_id=outcome.run_snapshot_id AND v3.status='finalized')
    ), ranked AS (
        SELECT candidate.*,count(*) OVER () AS candidate_count
          FROM candidates candidate
    )
    SELECT candidate.kind,candidate.record_id,candidate.run_snapshot_id,
           candidate.generated_at,candidate.digest,candidate.coverage,
           CASE WHEN candidate.payload_text IS NULL THEN NULL
                ELSE left(candidate.payload_text,4096) END,
           candidate.gap_reason,
           CASE WHEN candidate.payload_text IS NULL THEN 0
                ELSE octet_length(convert_to(candidate.payload_text,'UTF8')) END,
           CASE WHEN candidate.payload_text IS NULL THEN 0
                ELSE octet_length(convert_to(left(candidate.payload_text,4096),'UTF8')) END,
           CASE WHEN candidate.payload_text IS NULL THEN ''
                ELSE encode(sha256(convert_to(left(candidate.payload_text,4096),'UTF8')),'hex') END,
           CASE WHEN candidate.payload_text IS NULL THEN false
                ELSE char_length(candidate.payload_text)>4096 END,
           candidate.candidate_count
      FROM ranked candidate
     ORDER BY candidate.generated_at DESC,candidate.kind,candidate.record_id DESC
     LIMIT 20;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION read_research_history_v3(BIGINT,BIGINT,TEXT,BIGINT,BIGINT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION read_research_history_v3(BIGINT,BIGINT,TEXT,BIGINT,BIGINT) TO vane_app;

-- Oversized retained artifacts are exposed through an auditable, same-owner
-- character cursor. Synthesis coordinators can drain every chunk before making
-- a comparison; the preview in the frozen context never masquerades as full.
-- +goose StatementBegin
CREATE FUNCTION read_research_history_content_v3(
    target_tenant_id BIGINT,
    target_user_id BIGINT,
    target_task_id TEXT,
    target_synthesis_id BIGINT,
    target_request_digest TEXT,
    target_record_id TEXT,
    offset_chars INTEGER,
    limit_chars INTEGER
)
RETURNS TABLE (
    record_id TEXT,
    chunk_offset_chars INTEGER,
    next_offset_chars INTEGER,
    total_chars INTEGER,
    total_bytes INTEGER,
    chunk_text TEXT,
    chunk_digest TEXT,
    full_digest TEXT,
    complete BOOLEAN
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    frozen_history JSONB;
    expected_full_digest TEXT;
BEGIN
    IF target_tenant_id IS DISTINCT FROM
           NULLIF(current_setting('app.tenant_id',true),'')::bigint OR
       target_user_id IS DISTINCT FROM
           NULLIF(current_setting('app.user_id',true),'')::bigint OR
       offset_chars<0 OR limit_chars<1 OR limit_chars>4096 THEN
        RAISE EXCEPTION '088: research history content cursor is invalid';
    END IF;
    SELECT convert_from(synthesis.history_manifest,'UTF8')::jsonb
      INTO frozen_history
      FROM public.research_brief_syntheses synthesis
     WHERE synthesis.id=target_synthesis_id
       AND synthesis.tenant_id=target_tenant_id
       AND synthesis.user_id=target_user_id
       AND synthesis.task_id=target_task_id
       AND synthesis.request_digest=target_request_digest;
    SELECT item->>'digest'
      INTO expected_full_digest
      FROM jsonb_array_elements(frozen_history->'items') item
     WHERE item->>'record_id'=target_record_id
       AND item->>'coverage'<>'unavailable';
    IF expected_full_digest IS NULL OR expected_full_digest !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION '088: research history content cursor is invalid';
    END IF;
    RETURN QUERY
    WITH content AS (
        SELECT brief_v3.id::text AS record_id,
               convert_from(brief_v3.brief_payload,'UTF8') AS payload_text,
               brief_v3.brief_digest AS full_digest
          FROM public.research_brief_syntheses brief_v3
         WHERE brief_v3.tenant_id=target_tenant_id AND brief_v3.user_id=target_user_id
           AND brief_v3.task_id=target_task_id AND brief_v3.id::text=target_record_id
           AND brief_v3.status='finalized'
        UNION ALL
        SELECT 'brief:'||brief.id::text,convert_from(brief.payload,'UTF8'),brief.payload_digest
          FROM public.brief_snapshots brief
         WHERE brief.tenant_id=target_tenant_id AND brief.user_id=target_user_id
           AND brief.task_id=target_task_id AND 'brief:'||brief.id::text=target_record_id
        UNION ALL
        SELECT 'observation:'||observation.run_snapshot_id::text||':'||observation.invocation_digest,
               convert_from(observation.observation_payload,'UTF8'),observation.observation_digest
          FROM public.task_run_content_provenance observation
         WHERE observation.tenant_id=target_tenant_id AND observation.user_id=target_user_id
           AND observation.task_id=target_task_id
           AND 'observation:'||observation.run_snapshot_id::text||':'||observation.invocation_digest=target_record_id
    ), selected AS (
        SELECT content.record_id,content.payload_text,content.full_digest,
               char_length(content.payload_text)::integer AS total_chars,
               octet_length(convert_to(content.payload_text,'UTF8'))::integer AS total_bytes,
               substring(content.payload_text FROM offset_chars+1 FOR limit_chars) AS chunk_text
          FROM content
         WHERE content.full_digest=expected_full_digest
           AND offset_chars<=char_length(content.payload_text)
    )
    SELECT selected.record_id,offset_chars,
           (offset_chars+char_length(selected.chunk_text))::integer,
           selected.total_chars,selected.total_bytes,selected.chunk_text,
           encode(sha256(convert_to(selected.chunk_text,'UTF8')),'hex'),
           selected.full_digest,
           offset_chars+char_length(selected.chunk_text)>=selected.total_chars
      FROM selected;
    IF NOT FOUND THEN
        RAISE EXCEPTION '088: research history content is unavailable';
    END IF;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION read_research_history_content_v3(BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,INTEGER,INTEGER) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION read_research_history_content_v3(BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,INTEGER,INTEGER) TO vane_app;

-- Admission proves scope, immutable parent identity, exact completed Evidence,
-- and the typed manifest/cutoff shape. Cast failures are intentionally fatal.
-- +goose StatementBegin
CREATE FUNCTION enforce_research_brief_synthesis_admission_v3()
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
        RAISE EXCEPTION '088: research Brief scope differs from session';
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
        RAISE EXCEPTION '088: research Brief parent scope differs';
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
       context_json->>'schema_version'<>'vane.research-synthesis-context/v3' OR
       context_json->'definition' IS DISTINCT FROM expected_definition_context OR
       NEW.notification_threshold IS DISTINCT FROM
           snapshot_json#>>'{definition,notification,minimum_significance}' OR
       NEW.request_digest IS DISTINCT FROM expected_request_digest OR
       evidence_json->>'schema_version'<>'vane.research-evidence-manifest/v3' OR
       jsonb_typeof(evidence_json->'items')<>'array' OR
       jsonb_array_length(evidence_json->'items')<>expected_steps OR
       history_json->>'schema_version'<>'vane.research-history-manifest/v3' OR
       history_json->>'history_through_utc' IS DISTINCT FROM history_cutoff OR
       jsonb_typeof(history_json->'items')<>'array' OR
       jsonb_array_length(history_json->'items')>20 THEN
        RAISE EXCEPTION '088: research Brief manifest shape is invalid';
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
    IF evidence_json IS DISTINCT FROM jsonb_build_object(
           'schema_version','vane.research-evidence-manifest/v3','items',expected_evidence) OR
       jsonb_array_length(expected_evidence)<>expected_steps OR
       context_json->'current_evidence' IS DISTINCT FROM expected_evidence_context THEN
        RAISE EXCEPTION '088: research Brief manifest is not exact completed Evidence';
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
        'schema_version','vane.research-synthesis-context/v3',
        'definition',expected_definition_context,
        'current_evidence',expected_evidence_context,
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
        RAISE EXCEPTION '088: research Brief history is not exact same-owner history';
    END IF;
    ELSE
        -- Prepare is the single admission point that freezes Evidence and the
        -- retained top-20. Updates must never re-query mutable history: an old
        -- transaction can commit a cutoff-eligible row after the paid model
        -- call. The transition trigger separately makes these bytes immutable.
        expected_evidence := evidence_json->'items';
        expected_history := history_json->'items';
    END IF;

    IF NEW.status='finalized' THEN
       brief_json := convert_from(NEW.brief_payload,'UTF8')::jsonb;
    END IF;
    IF NEW.status='finalized' AND (
       jsonb_typeof(brief_json) IS DISTINCT FROM 'object' OR
       (SELECT count(*) FROM jsonb_object_keys(brief_json))<>5 OR
       (brief_json-ARRAY['schema_version','headline','summary','significance','citations'])<>'{}'::jsonb OR
       brief_json->>'schema_version' IS DISTINCT FROM 'vane.research-brief/v3' OR
       brief_json->>'significance' IS DISTINCT FROM NEW.significance OR
       jsonb_typeof(brief_json->'headline') IS DISTINCT FROM 'string' OR
       octet_length(brief_json->>'headline') NOT BETWEEN 1 AND 1024 OR
       btrim(brief_json->>'headline') IS DISTINCT FROM brief_json->>'headline' OR
       jsonb_typeof(brief_json->'summary') IS DISTINCT FROM 'string' OR
       octet_length(brief_json->>'summary') NOT BETWEEN 1 AND 65536 OR
       btrim(brief_json->>'summary') IS DISTINCT FROM brief_json->>'summary' OR
       jsonb_typeof(brief_json->'citations') IS DISTINCT FROM 'array' OR
       jsonb_array_length(brief_json->'citations') NOT BETWEEN 1 AND 64 OR
       NOT EXISTS (
           SELECT 1 FROM jsonb_array_elements(brief_json->'citations') citation
            WHERE citation->>'kind'='current_evidence'
       ) OR EXISTS (
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
        RAISE EXCEPTION '088: research Brief payload is not grounded in frozen Evidence';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_brief_synthesis_admission_v3() FROM PUBLIC;
CREATE TRIGGER research_brief_synthesis_admission_v3
BEFORE INSERT OR UPDATE ON research_brief_syntheses
FOR EACH ROW EXECUTE FUNCTION enforce_research_brief_synthesis_admission_v3();

-- Only prepared->spending and prepared/spending->terminal transitions exist.
-- The trigger owns timestamps; terminal artifacts and failures are immutable.
-- +goose StatementBegin
CREATE FUNCTION enforce_research_brief_synthesis_transition_v3()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF ROW(
        NEW.id,NEW.tenant_id,NEW.user_id,NEW.task_id,NEW.run_snapshot_id,
        NEW.plan_id,NEW.temporal_workflow_id,NEW.temporal_run_id,
        NEW.definition_digest,NEW.plan_digest,NEW.notification_threshold,
        NEW.request_digest,NEW.context_payload,NEW.context_digest,
        NEW.evidence_manifest,NEW.evidence_digest,
        NEW.history_manifest,NEW.history_digest,NEW.schema_version,NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.id,OLD.tenant_id,OLD.user_id,OLD.task_id,OLD.run_snapshot_id,
        OLD.plan_id,OLD.temporal_workflow_id,OLD.temporal_run_id,
        OLD.definition_digest,OLD.plan_digest,OLD.notification_threshold,
        OLD.request_digest,OLD.context_payload,OLD.context_digest,
        OLD.evidence_manifest,OLD.evidence_digest,
        OLD.history_manifest,OLD.history_digest,OLD.schema_version,OLD.created_at
    ) THEN
        RAISE EXCEPTION '088: research Brief synthesis identity is immutable';
    END IF;
    IF OLD.status IN ('finalized','ambiguous','failed') THEN
        RAISE EXCEPTION '088: terminal research Brief synthesis is immutable';
    END IF;
    IF OLD.status='prepared' AND NEW.status='spending' THEN
        NEW.spending_started_at := clock_timestamp();
        NEW.finalized_at := NULL;
    ELSIF OLD.status='prepared' AND NEW.status='failed' THEN
        NEW.spending_started_at := NULL;
        NEW.finalized_at := clock_timestamp();
    ELSIF OLD.status='spending' AND NEW.status IN ('finalized','ambiguous','failed') THEN
        NEW.spending_started_at := OLD.spending_started_at;
        NEW.finalized_at := clock_timestamp();
    ELSE
        RAISE EXCEPTION '088: research Brief synthesis transition is invalid';
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_brief_synthesis_transition_v3() FROM PUBLIC;
CREATE TRIGGER research_brief_synthesis_transition_v3
BEFORE UPDATE ON research_brief_syntheses
FOR EACH ROW EXECUTE FUNCTION enforce_research_brief_synthesis_transition_v3();

GRANT SELECT ON research_brief_syntheses TO vane_app;
GRANT INSERT (
    tenant_id,user_id,task_id,run_snapshot_id,plan_id,
    temporal_workflow_id,temporal_run_id,definition_digest,plan_digest,
    notification_threshold,request_digest,context_payload,context_digest,
    evidence_manifest,evidence_digest,history_manifest,history_digest,schema_version
) ON research_brief_syntheses TO vane_app;
GRANT UPDATE (
    status,significance,decision,delivery_required,
    brief_payload,brief_digest,failure_code
) ON research_brief_syntheses TO vane_app;
GRANT USAGE,SELECT ON SEQUENCE research_brief_syntheses_id_seq TO vane_app;

ALTER TABLE research_brief_syntheses ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON research_brief_syntheses
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON research_brief_syntheses AS RESTRICTIVE
    FOR ALL
    USING (tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint);
CREATE POLICY user_isolation ON research_brief_syntheses AS RESTRICTIVE
    FOR ALL
    USING (user_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint)
    WITH CHECK (user_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint);

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE research_brief_syntheses IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM research_brief_syntheses) THEN
        RAISE EXCEPTION
            '088: refusing downgrade while research Brief synthesis evidence exists';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE ALL ON SEQUENCE research_brief_syntheses_id_seq FROM vane_app;
REVOKE ALL ON research_brief_syntheses FROM vane_app;
REVOKE ALL ON FUNCTION read_research_history_content_v3(BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,INTEGER,INTEGER) FROM vane_app;
REVOKE ALL ON FUNCTION read_research_history_v3(BIGINT,BIGINT,TEXT,BIGINT,BIGINT) FROM vane_app;
DROP TRIGGER research_brief_synthesis_transition_v3 ON research_brief_syntheses;
DROP FUNCTION enforce_research_brief_synthesis_transition_v3();
DROP TRIGGER research_brief_synthesis_admission_v3 ON research_brief_syntheses;
DROP FUNCTION enforce_research_brief_synthesis_admission_v3();
DROP FUNCTION read_research_history_content_v3(BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,INTEGER,INTEGER);
DROP FUNCTION read_research_history_v3(BIGINT,BIGINT,TEXT,BIGINT,BIGINT);
DROP TABLE research_brief_syntheses;
