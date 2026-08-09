-- 124: owner-confirmed event-window synthesis projection and v3.5 admission.
-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE task_run_snapshots,research_run_evidence,research_brief_syntheses,
           research_run_llm_spend_reservations IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
CREATE FUNCTION research_scope_published_at_v1(value TEXT)
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
IMMUTABLE
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF value IS NULL OR value='' THEN
        RETURN NULL;
    END IF;
	-- Match Go time.RFC3339Nano admission. PostgreSQL otherwise accepts
	-- ambiguous local timestamps and many non-canonical date spellings.
	IF value !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,9})?(Z|[+-][0-9]{2}:[0-9]{2})$' THEN
		RETURN NULL;
	END IF;
    RETURN value::timestamptz;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION research_scope_published_at_v1(TEXT) FROM PUBLIC;

CREATE FUNCTION research_scope_json_string_v124(value TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE STRICT
SET search_path=pg_catalog,public,pg_temp
RETURN replace(replace(replace(replace(replace(
    to_json(value)::text,
    '<','\u003c'),'>','\u003e'),'&','\u0026'),U&'\2028','\u2028'),U&'\2029','\u2029');
REVOKE ALL ON FUNCTION research_scope_json_string_v124(TEXT) FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION filter_research_scope_evidence_v124(
    result_bytes BYTEA,
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ
) RETURNS TEXT
LANGUAGE plpgsql IMMUTABLE STRICT
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    payload JSONB;
    document JSONB;
    canonical_document TEXT;
    canonical_full TEXT := '[';
    canonical_filtered TEXT := '[';
    separator TEXT := '';
    filtered_separator TEXT := '';
    published TIMESTAMPTZ;
BEGIN
    payload := convert_from(result_bytes,'UTF8')::jsonb;
    IF jsonb_typeof(payload)<>'array' THEN
        RAISE EXCEPTION '124: scoped web Evidence is not a document array';
    END IF;
    FOR document IN SELECT value FROM jsonb_array_elements(payload) WITH ORDINALITY item(value,ordinal)
                    ORDER BY ordinal LOOP
        IF jsonb_typeof(document)<>'object' OR
           (SELECT count(*) FROM jsonb_object_keys(document)) NOT BETWEEN 3 AND 5 OR
           (document-ARRAY['title','url','published_at','author','text'])<>'{}'::jsonb OR
           jsonb_typeof(document->'title') IS DISTINCT FROM 'string' OR
           jsonb_typeof(document->'url') IS DISTINCT FROM 'string' OR
           jsonb_typeof(document->'text') IS DISTINCT FROM 'string' OR
           (document?'published_at' AND jsonb_typeof(document->'published_at') IS DISTINCT FROM 'string') OR
           (document?'author' AND jsonb_typeof(document->'author') IS DISTINCT FROM 'string') THEN
            RAISE EXCEPTION '124: scoped web Evidence document shape differs';
        END IF;
        canonical_document := '{"title":'||public.research_scope_json_string_v124(document->>'title')||
            ',"url":'||public.research_scope_json_string_v124(document->>'url')||
            CASE WHEN coalesce(document->>'published_at','')='' THEN ''
                 ELSE ',"published_at":'||public.research_scope_json_string_v124(document->>'published_at') END||
            CASE WHEN coalesce(document->>'author','')='' THEN ''
                 ELSE ',"author":'||public.research_scope_json_string_v124(document->>'author') END||
            ',"text":'||public.research_scope_json_string_v124(document->>'text')||'}';
        canonical_full := canonical_full||separator||canonical_document;
        separator := ',';
        published := public.research_scope_published_at_v1(document->>'published_at');
        IF published>window_start AND published<=window_end THEN
            canonical_filtered := canonical_filtered||filtered_separator||canonical_document;
            filtered_separator := ',';
        END IF;
    END LOOP;
    canonical_full := canonical_full||']';
    canonical_filtered := canonical_filtered||']';
    IF canonical_full IS DISTINCT FROM convert_from(result_bytes,'UTF8') THEN
        RAISE EXCEPTION '124: scoped web Evidence is not canonical';
    END IF;
    RETURN canonical_filtered;
EXCEPTION WHEN character_not_in_repertoire OR untranslatable_character OR invalid_text_representation THEN
    RAISE EXCEPTION '124: scoped web Evidence is malformed';
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION filter_research_scope_evidence_v124(BYTEA,TIMESTAMPTZ,TIMESTAMPTZ)
    FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION enforce_research_scope_window_v33()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    snapshot_json JSONB;
    context_json JSONB;
    cutoff TIMESTAMPTZ;
    window_start TIMESTAMPTZ;
    expected_ids JSONB;
    actual_ids JSONB;
	expected_evidence_context JSONB;
	eligible_count INTEGER;
BEGIN
    SELECT convert_from(snapshot.payload,'UTF8')::jsonb
      INTO snapshot_json
      FROM public.task_run_snapshots snapshot
     WHERE snapshot.id=NEW.run_snapshot_id
       AND snapshot.tenant_id=NEW.tenant_id
       AND snapshot.user_id=NEW.user_id
       AND snapshot.task_id=NEW.task_id;
    IF snapshot_json IS NULL THEN
        RAISE EXCEPTION '124: synthesis snapshot scope differs' USING ERRCODE='23514';
    END IF;
    IF snapshot_json #>> '{research_model,synthesis,renderer_version}' <>
       'research-synthesis.render/v3.5' THEN
        IF snapshot_json #> '{definition,research_scope}' IS NOT NULL OR
           convert_from(NEW.context_payload,'UTF8')::jsonb->>'schema_version' =
               'vane.research-synthesis-context/v3.3' THEN
            RAISE EXCEPTION '124: unscoped renderer carries event-window state'
                USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF snapshot_json #>> '{definition,research_scope,mode}' <> 'event_window' OR
       (snapshot_json #>> '{definition,research_scope,lookback_seconds}')::bigint <> 604800 THEN
        RAISE EXCEPTION '124: v3.5 snapshot lacks exact owner scope'
            USING ERRCODE='23514';
    END IF;
    cutoff := (snapshot_json->>'history_through_utc')::timestamptz;
    window_start := cutoff-make_interval(secs=>604800);
    context_json := convert_from(NEW.context_payload,'UTF8')::jsonb;
    IF context_json->>'schema_version' <> 'vane.research-synthesis-context/v3.3' OR
       context_json #>> '{research_scope_window,mode}' <> 'event_window' OR
       (context_json #>> '{research_scope_window,lookback_seconds}')::bigint <> 604800 OR
       context_json #>> '{research_scope_window,boundary}' <> '(start,end]' OR
       (context_json #>> '{research_scope_window,start_utc}')::timestamptz <> window_start OR
       (context_json #>> '{research_scope_window,end_utc}')::timestamptz <> cutoff OR
       context_json #>> '{history,history_through_utc}' <>
           snapshot_json->>'history_through_utc' THEN
        RAISE EXCEPTION '124: synthesis window differs from frozen owner scope'
            USING ERRCODE='23514';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM public.research_run_evidence evidence
         WHERE evidence.tenant_id=NEW.tenant_id AND evidence.user_id=NEW.user_id
           AND evidence.task_id=NEW.task_id AND evidence.plan_id=NEW.plan_id
           AND evidence.temporal_run_id=NEW.temporal_run_id
           AND evidence.tool_name IN ('web_search','web_contents')
           AND (evidence.truncated OR
                jsonb_typeof(convert_from(evidence.result_bytes,'UTF8')::jsonb)<>'array')
    ) THEN
        RAISE EXCEPTION '124: scoped web Evidence is truncated or malformed'
            USING ERRCODE='23514';
    END IF;
    WITH filtered AS (
        SELECT evidence.*,
               public.filter_research_scope_evidence_v124(
                   evidence.result_bytes,window_start,cutoff) AS filtered_text
          FROM public.research_run_evidence evidence
         WHERE evidence.tenant_id=NEW.tenant_id AND evidence.user_id=NEW.user_id
           AND evidence.task_id=NEW.task_id AND evidence.plan_id=NEW.plan_id
           AND evidence.temporal_run_id=NEW.temporal_run_id
           AND evidence.tool_name IN ('web_search','web_contents')
           AND NOT evidence.truncated
    ), eligible AS (
        SELECT * FROM filtered WHERE filtered_text<>'[]'
    )
    SELECT COALESCE(jsonb_agg(id ORDER BY step_ordinal),'[]'::jsonb),count(*)::integer
      INTO expected_ids,eligible_count FROM eligible;
    SELECT COALESCE(jsonb_agg((item->>'evidence_id')::bigint ORDER BY ordinal),'[]'::jsonb)
      INTO actual_ids
      FROM jsonb_array_elements(context_json->'current_evidence')
           WITH ORDINALITY AS current(item,ordinal);
    IF expected_ids='[]'::jsonb OR actual_ids IS DISTINCT FROM expected_ids THEN
        RAISE EXCEPTION '124: synthesis eligible Evidence ids differ'
            USING ERRCODE='23514';
    END IF;
    WITH filtered AS (
        SELECT evidence.*,
               public.filter_research_scope_evidence_v124(
                   evidence.result_bytes,window_start,cutoff) AS filtered_text
          FROM public.research_run_evidence evidence
         WHERE evidence.tenant_id=NEW.tenant_id AND evidence.user_id=NEW.user_id
           AND evidence.task_id=NEW.task_id AND evidence.plan_id=NEW.plan_id
           AND evidence.temporal_run_id=NEW.temporal_run_id
           AND evidence.tool_name IN ('web_search','web_contents')
           AND NOT evidence.truncated
    ), eligible AS (
        SELECT filtered.*,
               public.project_research_evidence_context_v118(
                   convert_to(filtered_text,'UTF8'),eligible_count) AS visible_text
          FROM filtered WHERE filtered_text<>'[]'
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'evidence_id',evidence.id,'ordinal',evidence.step_ordinal,
               'invocation_id',evidence.invocation_id,'tool_name',evidence.tool_name,
               'request_digest',evidence.request_digest,'result_digest',evidence.result_digest,
               'original_size',evidence.original_size,'truncated',evidence.truncated,
               'trust_type',evidence.trust_type,
               'synthesis_visible_text',evidence.visible_text,
               'context_stored_size',octet_length(convert_to(evidence.filtered_text,'UTF8')),
               'context_visible_size',octet_length(convert_to(evidence.visible_text,'UTF8')),
               'context_visible_digest',encode(sha256(convert_to(evidence.visible_text,'UTF8')),'hex'),
               'context_truncated',octet_length(convert_to(evidence.filtered_text,'UTF8'))>
                   octet_length(convert_to(evidence.visible_text,'UTF8'))
           ) ORDER BY evidence.step_ordinal),'[]'::jsonb)
      INTO expected_evidence_context FROM eligible evidence;
    IF context_json->'current_evidence' IS DISTINCT FROM expected_evidence_context THEN
        RAISE EXCEPTION '124: synthesis eligible Evidence projection differs'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_scope_window_v33() FROM PUBLIC;
CREATE TRIGGER research_scope_window_v33
BEFORE INSERT OR UPDATE ON research_brief_syntheses
FOR EACH ROW EXECUTE FUNCTION enforce_research_scope_window_v33();

DROP TRIGGER research_brief_synthesis_reject_unknown_v31
    ON research_brief_syntheses;
CREATE TRIGGER research_brief_synthesis_reject_unknown_v31
BEFORE INSERT OR UPDATE ON research_brief_syntheses
FOR EACH ROW
WHEN ((convert_from(NEW.context_payload,'UTF8')::jsonb->>'schema_version')
      IS NULL OR
      (convert_from(NEW.context_payload,'UTF8')::jsonb->>'schema_version')
      NOT IN ('vane.research-synthesis-context/v3',
              'vane.research-synthesis-context/v3.1',
              'vane.research-synthesis-context/v3.2',
              'vane.research-synthesis-context/v3.3'))
EXECUTE FUNCTION reject_research_brief_synthesis_schema_v31();

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION enforce_research_run_llm_spend_reservation_v2()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    snapshot_json JSONB;
    stage_key TEXT;
    stage_model TEXT;
    stage_system_prompt TEXT;
    stage_max_tokens BIGINT;
    stage_temperature REAL;
    stage_disable_thinking BOOLEAN;
    max_rounds INTEGER;
    max_tokens BIGINT;
    max_cost BIGINT;
    used_tokens BIGINT;
    used_cost BIGINT;
BEGIN
    SELECT convert_from(snapshot.payload,'UTF8')::jsonb
      INTO snapshot_json
      FROM public.task_run_snapshots snapshot
     WHERE snapshot.id=NEW.run_snapshot_id
       AND snapshot.tenant_id=NEW.tenant_id
       AND snapshot.user_id=NEW.user_id
       AND snapshot.task_id=NEW.task_id
       AND snapshot.reference_schema_version='vane.research-run-snapshot-ref/v3'
       AND snapshot.model_policy_digest=NEW.model_policy_digest
     FOR UPDATE;
    IF snapshot_json IS NULL THEN
        RAISE EXCEPTION '123: LLM reservation snapshot scope differs'
            USING ERRCODE='23514';
    END IF;
    stage_key := CASE
        WHEN NEW.stage='planner' THEN 'planner'
        WHEN NEW.stage='synthesis' AND NEW.round_ordinal=0 THEN 'synthesis'
        WHEN NEW.stage='synthesis' AND NEW.round_ordinal=1 AND
             snapshot_json #>> '{research_model,synthesis,renderer_version}' IN (
                 'research-synthesis.render/v3.3',
                 'research-synthesis.render/v3.4',
                 'research-synthesis.render/v3.5'
             )
            THEN 'grounding_verifier'
        ELSE NULL
    END;
    IF stage_key IS NULL THEN
        RAISE EXCEPTION '123: LLM reservation stage differs'
            USING ERRCODE='23514';
    END IF;
    stage_model := snapshot_json #>> ARRAY['research_model',stage_key,'model'];
    stage_max_tokens := (snapshot_json #>> ARRAY['research_model',stage_key,'max_tokens'])::bigint;
    stage_system_prompt := snapshot_json #>> ARRAY['research_model',stage_key,'system_prompt'];
    stage_temperature := (snapshot_json #>> ARRAY['research_model',stage_key,'temperature'])::real;
    stage_disable_thinking := (snapshot_json #>> ARRAY['research_model',stage_key,'disable_thinking'])::boolean;
    IF stage_model IS DISTINCT FROM NEW.model OR stage_max_tokens IS NULL OR
       NEW.reserved_completion_tokens<>stage_max_tokens OR
       NEW.system_prompt_digest IS DISTINCT FROM
           encode(sha256(convert_to(stage_system_prompt,'UTF8')),'hex') OR
       NEW.temperature IS DISTINCT FROM stage_temperature OR
       NEW.disable_thinking IS DISTINCT FROM stage_disable_thinking OR
       snapshot_json #>> '{research_model,quota_bucket}'<>NEW.quota_bucket THEN
        RAISE EXCEPTION '123: LLM reservation differs from frozen model policy'
            USING ERRCODE='23514';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM public.provider_price_rules pricing
         WHERE pricing.id=NEW.pricing_rule_id
           AND pricing.provider=(snapshot_json #>> '{research_model,provider}')
           AND pricing.resource=NEW.model AND pricing.meter='llm_tokens'
           AND pricing.currency=NEW.cost_currency
           AND pricing.effective_from<=NEW.created_at
           AND (pricing.effective_to IS NULL OR pricing.effective_to>NEW.created_at)
           AND NEW.reserved_cost_micro_usd=ceil(
                 (NEW.reserved_quota_tokens-NEW.reserved_completion_tokens)::numeric *
                    pricing.input_cache_miss_per_million +
                 NEW.reserved_completion_tokens::numeric * pricing.output_per_million
               )::bigint
    ) THEN
        RAISE EXCEPTION '123: LLM reservation differs from exact price ceiling'
            USING ERRCODE='23514';
    END IF;
    IF NEW.stage='synthesis' THEN
        IF NOT EXISTS (
            SELECT 1 FROM public.research_brief_syntheses brief
             WHERE brief.id=NEW.subject_id
               AND brief.tenant_id=NEW.tenant_id AND brief.user_id=NEW.user_id
               AND brief.task_id=NEW.task_id
               AND brief.run_snapshot_id=NEW.run_snapshot_id
               AND brief.status IN ('prepared','spending')
        ) OR (NEW.round_ordinal=1 AND NOT EXISTS (
            SELECT 1 FROM public.research_brief_grounding_verifications grounding
             WHERE grounding.synthesis_id=NEW.subject_id
               AND grounding.tenant_id=NEW.tenant_id
               AND grounding.user_id=NEW.user_id
               AND grounding.task_id=NEW.task_id
               AND grounding.run_snapshot_id=NEW.run_snapshot_id
               AND grounding.status='prepared'
        )) THEN
            RAISE EXCEPTION '123: synthesis reservation subject differs'
                USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    max_rounds := (snapshot_json #>> '{planner_budget,max_planner_rounds}')::integer;
    max_tokens := (snapshot_json #>> '{planner_budget,max_tokens}')::bigint;
    max_cost := (snapshot_json #>> '{planner_budget,max_cost_micro_usd}')::bigint;
    SELECT COALESCE(sum(GREATEST(
               reservation.reserved_planner_tokens,
               COALESCE(settlement.actual_prompt_tokens+
                        settlement.actual_completion_tokens,
                        reservation.reserved_planner_tokens)
           )),0),
           COALESCE(sum(GREATEST(
               reservation.reserved_cost_micro_usd,
               COALESCE(settlement.actual_cost_micro_usd,
                        reservation.reserved_cost_micro_usd)
           )),0)
      INTO used_tokens,used_cost
      FROM public.research_run_llm_spend_reservations reservation
      LEFT JOIN public.research_run_llm_spend_settlements settlement
        ON settlement.reservation_id=reservation.id
     WHERE reservation.run_snapshot_id=NEW.run_snapshot_id
       AND reservation.tenant_id=NEW.tenant_id
       AND reservation.user_id=NEW.user_id
       AND reservation.counts_against_planner_budget;
    SELECT used_cost+COALESCE(sum(GREATEST(
               tool.reserved_cost_micro_usd,
               COALESCE(settlement.actual_cost_micro_usd,
                        tool.reserved_cost_micro_usd)
           )),0)
      INTO used_cost
      FROM public.research_run_step_spend_reservations tool
      LEFT JOIN public.research_run_step_spend_settlements settlement
        ON settlement.reservation_id=tool.id
     WHERE tool.run_snapshot_id=NEW.run_snapshot_id
       AND tool.tenant_id=NEW.tenant_id AND tool.user_id=NEW.user_id;
    IF NEW.round_ordinal>=max_rounds OR
       used_tokens+NEW.reserved_planner_tokens>max_tokens OR
       used_cost+NEW.reserved_cost_micro_usd>max_cost THEN
        RAISE EXCEPTION '123: planner reservation exceeds frozen PlannerBudget'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION admit_research_run_llm_spend_v5(
    requested_tenant_id BIGINT,
    requested_user_id BIGINT,
    requested_task_id TEXT,
    requested_run_snapshot_id BIGINT,
    requested_stage TEXT,
    requested_round_ordinal INTEGER,
    requested_subject_id BIGINT,
    requested_attempt_key TEXT,
    requested_request_digest TEXT,
    requested_trace_id TEXT,
    requested_user_prompt TEXT
) RETURNS TABLE(out_reservation_id BIGINT,out_first_writer BOOLEAN)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    snapshot_json JSONB;
    snapshot_model_policy_digest TEXT;
    stage_model TEXT;
    stage_system_prompt TEXT;
    stage_max_tokens BIGINT;
    stage_temperature REAL;
    stage_disable_thinking BOOLEAN;
    derived_quota_tokens BIGINT;
    reserved_cost BIGINT;
    selected_pricing_rule_id BIGINT;
    admitted_id BIGINT;
BEGIN
    IF requested_stage<>'synthesis' OR requested_round_ordinal<>1 THEN
        RETURN QUERY SELECT * FROM public.admit_research_run_llm_spend_v3(
            requested_tenant_id,requested_user_id,requested_task_id,
            requested_run_snapshot_id,requested_stage,requested_round_ordinal,
            requested_subject_id,requested_attempt_key,requested_request_digest,
            requested_trace_id,requested_user_prompt);
        RETURN;
    END IF;
    IF requested_tenant_id IS NULL OR requested_user_id IS NULL OR
       requested_task_id IS NULL OR requested_run_snapshot_id IS NULL OR
       requested_subject_id IS NULL OR requested_attempt_key IS NULL OR
       requested_request_digest IS NULL OR requested_trace_id IS NULL OR
       requested_user_prompt IS NULL OR
       requested_tenant_id IS DISTINCT FROM
           NULLIF(current_setting('app.tenant_id',true),'')::bigint OR
       requested_user_id IS DISTINCT FROM
           NULLIF(current_setting('app.user_id',true),'')::bigint OR
       octet_length(requested_user_prompt) NOT BETWEEN 1 AND 2097152 OR
       NOT EXISTS (
           SELECT 1 FROM public.tenants tenant
           JOIN public.memberships membership ON membership.tenant_id=tenant.id
            AND membership.user_id=requested_user_id
           WHERE tenant.id=requested_tenant_id AND tenant.status='active'
             AND tenant.deleted_at IS NULL
       ) THEN
        RAISE EXCEPTION '123: verifier LLM admission scope is invalid'
            USING ERRCODE='42501';
    END IF;
    SELECT convert_from(snapshot.payload,'UTF8')::jsonb,
           snapshot.model_policy_digest
      INTO snapshot_json,snapshot_model_policy_digest
      FROM public.task_run_snapshots snapshot
     WHERE snapshot.id=requested_run_snapshot_id
       AND snapshot.tenant_id=requested_tenant_id
       AND snapshot.user_id=requested_user_id
       AND snapshot.task_id=requested_task_id
       AND snapshot.reference_schema_version='vane.research-run-snapshot-ref/v3'
     FOR UPDATE;
    IF snapshot_json IS NULL OR
       snapshot_json #>> '{research_model,synthesis,renderer_version}' NOT IN (
           'research-synthesis.render/v3.3',
           'research-synthesis.render/v3.4',
                 'research-synthesis.render/v3.5'
       ) OR NOT EXISTS (
        SELECT 1 FROM public.research_brief_grounding_verifications grounding
        JOIN public.research_brief_syntheses brief ON brief.id=grounding.synthesis_id
         WHERE grounding.synthesis_id=requested_subject_id
           AND grounding.tenant_id=requested_tenant_id
           AND grounding.user_id=requested_user_id
           AND grounding.task_id=requested_task_id
           AND grounding.run_snapshot_id=requested_run_snapshot_id
           AND grounding.status='prepared'
           AND brief.status='spending'
           AND grounding.verifier_prompt=convert_to(requested_user_prompt,'UTF8')
           AND grounding.verifier_prompt_digest=
               encode(sha256(convert_to(requested_user_prompt,'UTF8')),'hex')
    ) THEN
        RAISE EXCEPTION '123: verifier prompt differs from frozen candidate'
            USING ERRCODE='23514';
    END IF;
    stage_model := snapshot_json #>> '{research_model,grounding_verifier,model}';
    stage_system_prompt := snapshot_json #>> '{research_model,grounding_verifier,system_prompt}';
    stage_max_tokens := (snapshot_json #>> '{research_model,grounding_verifier,max_tokens}')::bigint;
    stage_temperature := (snapshot_json #>> '{research_model,grounding_verifier,temperature}')::real;
    stage_disable_thinking := (snapshot_json #>> '{research_model,grounding_verifier,disable_thinking}')::boolean;
    derived_quota_tokens := octet_length(stage_system_prompt)::bigint +
        octet_length(requested_user_prompt)::bigint + 64 + stage_max_tokens;
    IF stage_model IS NULL OR stage_system_prompt IS NULL OR
       derived_quota_tokens NOT BETWEEN 1 AND 1000000 THEN
        RAISE EXCEPTION '123: verifier frozen policy is invalid'
            USING ERRCODE='23514';
    END IF;
    SELECT reservation.id INTO admitted_id
      FROM public.research_run_llm_spend_reservations reservation
     WHERE reservation.run_snapshot_id=requested_run_snapshot_id
       AND reservation.stage='synthesis' AND reservation.round_ordinal=1;
    IF admitted_id IS NOT NULL THEN
        IF NOT EXISTS (
            SELECT 1 FROM public.research_run_llm_spend_reservations reservation
             WHERE reservation.id=admitted_id
               AND reservation.tenant_id=requested_tenant_id
               AND reservation.user_id=requested_user_id
               AND reservation.task_id=requested_task_id
               AND reservation.subject_id=requested_subject_id
               AND reservation.attempt_key=requested_attempt_key
               AND reservation.request_digest=requested_request_digest
               AND reservation.trace_id=requested_trace_id
               AND reservation.model=stage_model
               AND reservation.system_prompt_digest=
                   encode(sha256(convert_to(stage_system_prompt,'UTF8')),'hex')
               AND reservation.user_prompt_digest=
                   encode(sha256(convert_to(requested_user_prompt,'UTF8')),'hex')
               AND reservation.temperature IS NOT DISTINCT FROM stage_temperature
               AND reservation.disable_thinking IS NOT DISTINCT FROM stage_disable_thinking
               AND reservation.model_policy_digest=snapshot_model_policy_digest
               AND reservation.reserved_quota_tokens=derived_quota_tokens
               AND reservation.reserved_completion_tokens=stage_max_tokens
               AND reservation.reserved_planner_tokens=0
        ) THEN
            RAISE EXCEPTION '123: verifier LLM admission replay differs'
                USING ERRCODE='23514';
        END IF;
        RETURN QUERY SELECT admitted_id,false;
        RETURN;
    END IF;
    SELECT pricing.id,ceil(
             (derived_quota_tokens-stage_max_tokens)::numeric *
                pricing.input_cache_miss_per_million +
             stage_max_tokens::numeric * pricing.output_per_million
           )::bigint
      INTO selected_pricing_rule_id,reserved_cost
      FROM public.provider_price_rules pricing
     WHERE pricing.provider=(snapshot_json #>> '{research_model,provider}')
       AND pricing.resource=stage_model AND pricing.meter='llm_tokens'
       AND pricing.currency='USD' AND pricing.effective_from<=clock_timestamp()
       AND (pricing.effective_to IS NULL OR pricing.effective_to>clock_timestamp())
       AND derived_quota_tokens>=stage_max_tokens
     ORDER BY pricing.effective_from DESC,pricing.id DESC
     LIMIT 1;
    IF reserved_cost IS NULL OR reserved_cost<=0 THEN
        RAISE EXCEPTION '123: verifier price ceiling is unavailable'
            USING ERRCODE='23514';
    END IF;
    UPDATE public.tenant_quota quota
       SET tokens=LEAST(quota.burst,
                        quota.tokens+quota.rate*EXTRACT(EPOCH FROM(now()-quota.updated_at)))
                  - derived_quota_tokens,
           updated_at=now()
     WHERE quota.tenant_id=requested_tenant_id AND quota.bucket='llm_tokens'
       AND LEAST(quota.burst,
                 quota.tokens+quota.rate*EXTRACT(EPOCH FROM(now()-quota.updated_at)))
           >=derived_quota_tokens;
    IF NOT FOUND THEN
        RAISE EXCEPTION '123: verifier LLM quota admission denied'
            USING ERRCODE='P0001';
    END IF;
    INSERT INTO public.research_run_llm_spend_reservations (
        tenant_id,user_id,task_id,run_snapshot_id,stage,round_ordinal,subject_id,
        attempt_key,request_digest,trace_id,model,system_prompt_digest,
        user_prompt_digest,temperature,disable_thinking,model_policy_digest,
        quota_bucket,reserved_quota_tokens,reserved_completion_tokens,
        reserved_planner_tokens,reserved_cost_micro_usd,pricing_rule_id,
        cost_currency,counts_against_planner_budget,schema_version
    ) VALUES (
        requested_tenant_id,requested_user_id,requested_task_id,
        requested_run_snapshot_id,'synthesis',1,requested_subject_id,
        requested_attempt_key,requested_request_digest,requested_trace_id,
        stage_model,encode(sha256(convert_to(stage_system_prompt,'UTF8')),'hex'),
        encode(sha256(convert_to(requested_user_prompt,'UTF8')),'hex'),
        stage_temperature,stage_disable_thinking,snapshot_model_policy_digest,
        'llm_tokens',derived_quota_tokens,stage_max_tokens,0,reserved_cost,
        selected_pricing_rule_id,'USD',false,
        'vane.research-run-llm-spend-reservation/v1'
    ) RETURNING id INTO admitted_id;
    RETURN QUERY SELECT admitted_id,true;
END
$$;
-- +goose StatementEnd

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE task_run_snapshots,research_brief_syntheses,
           research_run_llm_spend_reservations IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM public.task_run_snapshots snapshot
         WHERE snapshot.reference_schema_version='vane.research-run-snapshot-ref/v3'
           AND convert_from(snapshot.payload,'UTF8')::jsonb
                 #>> '{research_model,synthesis,renderer_version}' =
               'research-synthesis.render/v3.5'
    ) OR EXISTS (
        SELECT 1 FROM public.research_brief_syntheses synthesis
         WHERE convert_from(synthesis.context_payload,'UTF8')::jsonb
                   ->>'schema_version'='vane.research-synthesis-context/v3.3'
    ) THEN
        RAISE EXCEPTION '124: v3.5 snapshot or v3.3 synthesis history exists'
            USING ERRCODE='55000';
    END IF;
END
$$;
-- +goose StatementEnd

DROP TRIGGER research_scope_window_v33 ON research_brief_syntheses;
DROP FUNCTION enforce_research_scope_window_v33();
DROP TRIGGER research_brief_synthesis_reject_unknown_v31
    ON research_brief_syntheses;
CREATE TRIGGER research_brief_synthesis_reject_unknown_v31
BEFORE INSERT OR UPDATE ON research_brief_syntheses
FOR EACH ROW
WHEN ((convert_from(NEW.context_payload,'UTF8')::jsonb->>'schema_version')
      IS NULL OR
      (convert_from(NEW.context_payload,'UTF8')::jsonb->>'schema_version')
      NOT IN ('vane.research-synthesis-context/v3',
              'vane.research-synthesis-context/v3.1',
              'vane.research-synthesis-context/v3.2'))
EXECUTE FUNCTION reject_research_brief_synthesis_schema_v31();
DROP FUNCTION filter_research_scope_evidence_v124(BYTEA,TIMESTAMPTZ,TIMESTAMPTZ);
DROP FUNCTION research_scope_json_string_v124(TEXT);
DROP FUNCTION research_scope_published_at_v1(TEXT);

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION enforce_research_run_llm_spend_reservation_v2()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    snapshot_json JSONB;
    stage_key TEXT;
    stage_model TEXT;
    stage_system_prompt TEXT;
    stage_max_tokens BIGINT;
    stage_temperature REAL;
    stage_disable_thinking BOOLEAN;
    max_rounds INTEGER;
    max_tokens BIGINT;
    max_cost BIGINT;
    used_tokens BIGINT;
    used_cost BIGINT;
BEGIN
    SELECT convert_from(snapshot.payload,'UTF8')::jsonb
      INTO snapshot_json
      FROM public.task_run_snapshots snapshot
     WHERE snapshot.id=NEW.run_snapshot_id
       AND snapshot.tenant_id=NEW.tenant_id
       AND snapshot.user_id=NEW.user_id
       AND snapshot.task_id=NEW.task_id
       AND snapshot.reference_schema_version='vane.research-run-snapshot-ref/v3'
       AND snapshot.model_policy_digest=NEW.model_policy_digest
     FOR UPDATE;
    IF snapshot_json IS NULL THEN
        RAISE EXCEPTION '123: LLM reservation snapshot scope differs'
            USING ERRCODE='23514';
    END IF;
    stage_key := CASE
        WHEN NEW.stage='planner' THEN 'planner'
        WHEN NEW.stage='synthesis' AND NEW.round_ordinal=0 THEN 'synthesis'
        WHEN NEW.stage='synthesis' AND NEW.round_ordinal=1 AND
             snapshot_json #>> '{research_model,synthesis,renderer_version}' IN (
                 'research-synthesis.render/v3.3',
                 'research-synthesis.render/v3.4'
             )
            THEN 'grounding_verifier'
        ELSE NULL
    END;
    IF stage_key IS NULL THEN
        RAISE EXCEPTION '123: LLM reservation stage differs'
            USING ERRCODE='23514';
    END IF;
    stage_model := snapshot_json #>> ARRAY['research_model',stage_key,'model'];
    stage_max_tokens := (snapshot_json #>> ARRAY['research_model',stage_key,'max_tokens'])::bigint;
    stage_system_prompt := snapshot_json #>> ARRAY['research_model',stage_key,'system_prompt'];
    stage_temperature := (snapshot_json #>> ARRAY['research_model',stage_key,'temperature'])::real;
    stage_disable_thinking := (snapshot_json #>> ARRAY['research_model',stage_key,'disable_thinking'])::boolean;
    IF stage_model IS DISTINCT FROM NEW.model OR stage_max_tokens IS NULL OR
       NEW.reserved_completion_tokens<>stage_max_tokens OR
       NEW.system_prompt_digest IS DISTINCT FROM
           encode(sha256(convert_to(stage_system_prompt,'UTF8')),'hex') OR
       NEW.temperature IS DISTINCT FROM stage_temperature OR
       NEW.disable_thinking IS DISTINCT FROM stage_disable_thinking OR
       snapshot_json #>> '{research_model,quota_bucket}'<>NEW.quota_bucket THEN
        RAISE EXCEPTION '123: LLM reservation differs from frozen model policy'
            USING ERRCODE='23514';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM public.provider_price_rules pricing
         WHERE pricing.id=NEW.pricing_rule_id
           AND pricing.provider=(snapshot_json #>> '{research_model,provider}')
           AND pricing.resource=NEW.model AND pricing.meter='llm_tokens'
           AND pricing.currency=NEW.cost_currency
           AND pricing.effective_from<=NEW.created_at
           AND (pricing.effective_to IS NULL OR pricing.effective_to>NEW.created_at)
           AND NEW.reserved_cost_micro_usd=ceil(
                 (NEW.reserved_quota_tokens-NEW.reserved_completion_tokens)::numeric *
                    pricing.input_cache_miss_per_million +
                 NEW.reserved_completion_tokens::numeric * pricing.output_per_million
               )::bigint
    ) THEN
        RAISE EXCEPTION '123: LLM reservation differs from exact price ceiling'
            USING ERRCODE='23514';
    END IF;
    IF NEW.stage='synthesis' THEN
        IF NOT EXISTS (
            SELECT 1 FROM public.research_brief_syntheses brief
             WHERE brief.id=NEW.subject_id
               AND brief.tenant_id=NEW.tenant_id AND brief.user_id=NEW.user_id
               AND brief.task_id=NEW.task_id
               AND brief.run_snapshot_id=NEW.run_snapshot_id
               AND brief.status IN ('prepared','spending')
        ) OR (NEW.round_ordinal=1 AND NOT EXISTS (
            SELECT 1 FROM public.research_brief_grounding_verifications grounding
             WHERE grounding.synthesis_id=NEW.subject_id
               AND grounding.tenant_id=NEW.tenant_id
               AND grounding.user_id=NEW.user_id
               AND grounding.task_id=NEW.task_id
               AND grounding.run_snapshot_id=NEW.run_snapshot_id
               AND grounding.status='prepared'
        )) THEN
            RAISE EXCEPTION '123: synthesis reservation subject differs'
                USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    max_rounds := (snapshot_json #>> '{planner_budget,max_planner_rounds}')::integer;
    max_tokens := (snapshot_json #>> '{planner_budget,max_tokens}')::bigint;
    max_cost := (snapshot_json #>> '{planner_budget,max_cost_micro_usd}')::bigint;
    SELECT COALESCE(sum(GREATEST(
               reservation.reserved_planner_tokens,
               COALESCE(settlement.actual_prompt_tokens+
                        settlement.actual_completion_tokens,
                        reservation.reserved_planner_tokens)
           )),0),
           COALESCE(sum(GREATEST(
               reservation.reserved_cost_micro_usd,
               COALESCE(settlement.actual_cost_micro_usd,
                        reservation.reserved_cost_micro_usd)
           )),0)
      INTO used_tokens,used_cost
      FROM public.research_run_llm_spend_reservations reservation
      LEFT JOIN public.research_run_llm_spend_settlements settlement
        ON settlement.reservation_id=reservation.id
     WHERE reservation.run_snapshot_id=NEW.run_snapshot_id
       AND reservation.tenant_id=NEW.tenant_id
       AND reservation.user_id=NEW.user_id
       AND reservation.counts_against_planner_budget;
    SELECT used_cost+COALESCE(sum(GREATEST(
               tool.reserved_cost_micro_usd,
               COALESCE(settlement.actual_cost_micro_usd,
                        tool.reserved_cost_micro_usd)
           )),0)
      INTO used_cost
      FROM public.research_run_step_spend_reservations tool
      LEFT JOIN public.research_run_step_spend_settlements settlement
        ON settlement.reservation_id=tool.id
     WHERE tool.run_snapshot_id=NEW.run_snapshot_id
       AND tool.tenant_id=NEW.tenant_id AND tool.user_id=NEW.user_id;
    IF NEW.round_ordinal>=max_rounds OR
       used_tokens+NEW.reserved_planner_tokens>max_tokens OR
       used_cost+NEW.reserved_cost_micro_usd>max_cost THEN
        RAISE EXCEPTION '123: planner reservation exceeds frozen PlannerBudget'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION admit_research_run_llm_spend_v5(
    requested_tenant_id BIGINT,
    requested_user_id BIGINT,
    requested_task_id TEXT,
    requested_run_snapshot_id BIGINT,
    requested_stage TEXT,
    requested_round_ordinal INTEGER,
    requested_subject_id BIGINT,
    requested_attempt_key TEXT,
    requested_request_digest TEXT,
    requested_trace_id TEXT,
    requested_user_prompt TEXT
) RETURNS TABLE(out_reservation_id BIGINT,out_first_writer BOOLEAN)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    snapshot_json JSONB;
    snapshot_model_policy_digest TEXT;
    stage_model TEXT;
    stage_system_prompt TEXT;
    stage_max_tokens BIGINT;
    stage_temperature REAL;
    stage_disable_thinking BOOLEAN;
    derived_quota_tokens BIGINT;
    reserved_cost BIGINT;
    selected_pricing_rule_id BIGINT;
    admitted_id BIGINT;
BEGIN
    IF requested_stage<>'synthesis' OR requested_round_ordinal<>1 THEN
        RETURN QUERY SELECT * FROM public.admit_research_run_llm_spend_v3(
            requested_tenant_id,requested_user_id,requested_task_id,
            requested_run_snapshot_id,requested_stage,requested_round_ordinal,
            requested_subject_id,requested_attempt_key,requested_request_digest,
            requested_trace_id,requested_user_prompt);
        RETURN;
    END IF;
    IF requested_tenant_id IS NULL OR requested_user_id IS NULL OR
       requested_task_id IS NULL OR requested_run_snapshot_id IS NULL OR
       requested_subject_id IS NULL OR requested_attempt_key IS NULL OR
       requested_request_digest IS NULL OR requested_trace_id IS NULL OR
       requested_user_prompt IS NULL OR
       requested_tenant_id IS DISTINCT FROM
           NULLIF(current_setting('app.tenant_id',true),'')::bigint OR
       requested_user_id IS DISTINCT FROM
           NULLIF(current_setting('app.user_id',true),'')::bigint OR
       octet_length(requested_user_prompt) NOT BETWEEN 1 AND 2097152 OR
       NOT EXISTS (
           SELECT 1 FROM public.tenants tenant
           JOIN public.memberships membership ON membership.tenant_id=tenant.id
            AND membership.user_id=requested_user_id
           WHERE tenant.id=requested_tenant_id AND tenant.status='active'
             AND tenant.deleted_at IS NULL
       ) THEN
        RAISE EXCEPTION '123: verifier LLM admission scope is invalid'
            USING ERRCODE='42501';
    END IF;
    SELECT convert_from(snapshot.payload,'UTF8')::jsonb,
           snapshot.model_policy_digest
      INTO snapshot_json,snapshot_model_policy_digest
      FROM public.task_run_snapshots snapshot
     WHERE snapshot.id=requested_run_snapshot_id
       AND snapshot.tenant_id=requested_tenant_id
       AND snapshot.user_id=requested_user_id
       AND snapshot.task_id=requested_task_id
       AND snapshot.reference_schema_version='vane.research-run-snapshot-ref/v3'
     FOR UPDATE;
    IF snapshot_json IS NULL OR
       snapshot_json #>> '{research_model,synthesis,renderer_version}' NOT IN (
           'research-synthesis.render/v3.3',
           'research-synthesis.render/v3.4'
       ) OR NOT EXISTS (
        SELECT 1 FROM public.research_brief_grounding_verifications grounding
        JOIN public.research_brief_syntheses brief ON brief.id=grounding.synthesis_id
         WHERE grounding.synthesis_id=requested_subject_id
           AND grounding.tenant_id=requested_tenant_id
           AND grounding.user_id=requested_user_id
           AND grounding.task_id=requested_task_id
           AND grounding.run_snapshot_id=requested_run_snapshot_id
           AND grounding.status='prepared'
           AND brief.status='spending'
           AND grounding.verifier_prompt=convert_to(requested_user_prompt,'UTF8')
           AND grounding.verifier_prompt_digest=
               encode(sha256(convert_to(requested_user_prompt,'UTF8')),'hex')
    ) THEN
        RAISE EXCEPTION '123: verifier prompt differs from frozen candidate'
            USING ERRCODE='23514';
    END IF;
    stage_model := snapshot_json #>> '{research_model,grounding_verifier,model}';
    stage_system_prompt := snapshot_json #>> '{research_model,grounding_verifier,system_prompt}';
    stage_max_tokens := (snapshot_json #>> '{research_model,grounding_verifier,max_tokens}')::bigint;
    stage_temperature := (snapshot_json #>> '{research_model,grounding_verifier,temperature}')::real;
    stage_disable_thinking := (snapshot_json #>> '{research_model,grounding_verifier,disable_thinking}')::boolean;
    derived_quota_tokens := octet_length(stage_system_prompt)::bigint +
        octet_length(requested_user_prompt)::bigint + 64 + stage_max_tokens;
    IF stage_model IS NULL OR stage_system_prompt IS NULL OR
       derived_quota_tokens NOT BETWEEN 1 AND 1000000 THEN
        RAISE EXCEPTION '123: verifier frozen policy is invalid'
            USING ERRCODE='23514';
    END IF;
    SELECT reservation.id INTO admitted_id
      FROM public.research_run_llm_spend_reservations reservation
     WHERE reservation.run_snapshot_id=requested_run_snapshot_id
       AND reservation.stage='synthesis' AND reservation.round_ordinal=1;
    IF admitted_id IS NOT NULL THEN
        IF NOT EXISTS (
            SELECT 1 FROM public.research_run_llm_spend_reservations reservation
             WHERE reservation.id=admitted_id
               AND reservation.tenant_id=requested_tenant_id
               AND reservation.user_id=requested_user_id
               AND reservation.task_id=requested_task_id
               AND reservation.subject_id=requested_subject_id
               AND reservation.attempt_key=requested_attempt_key
               AND reservation.request_digest=requested_request_digest
               AND reservation.trace_id=requested_trace_id
               AND reservation.model=stage_model
               AND reservation.system_prompt_digest=
                   encode(sha256(convert_to(stage_system_prompt,'UTF8')),'hex')
               AND reservation.user_prompt_digest=
                   encode(sha256(convert_to(requested_user_prompt,'UTF8')),'hex')
               AND reservation.temperature IS NOT DISTINCT FROM stage_temperature
               AND reservation.disable_thinking IS NOT DISTINCT FROM stage_disable_thinking
               AND reservation.model_policy_digest=snapshot_model_policy_digest
               AND reservation.reserved_quota_tokens=derived_quota_tokens
               AND reservation.reserved_completion_tokens=stage_max_tokens
               AND reservation.reserved_planner_tokens=0
        ) THEN
            RAISE EXCEPTION '123: verifier LLM admission replay differs'
                USING ERRCODE='23514';
        END IF;
        RETURN QUERY SELECT admitted_id,false;
        RETURN;
    END IF;
    SELECT pricing.id,ceil(
             (derived_quota_tokens-stage_max_tokens)::numeric *
                pricing.input_cache_miss_per_million +
             stage_max_tokens::numeric * pricing.output_per_million
           )::bigint
      INTO selected_pricing_rule_id,reserved_cost
      FROM public.provider_price_rules pricing
     WHERE pricing.provider=(snapshot_json #>> '{research_model,provider}')
       AND pricing.resource=stage_model AND pricing.meter='llm_tokens'
       AND pricing.currency='USD' AND pricing.effective_from<=clock_timestamp()
       AND (pricing.effective_to IS NULL OR pricing.effective_to>clock_timestamp())
       AND derived_quota_tokens>=stage_max_tokens
     ORDER BY pricing.effective_from DESC,pricing.id DESC
     LIMIT 1;
    IF reserved_cost IS NULL OR reserved_cost<=0 THEN
        RAISE EXCEPTION '123: verifier price ceiling is unavailable'
            USING ERRCODE='23514';
    END IF;
    UPDATE public.tenant_quota quota
       SET tokens=LEAST(quota.burst,
                        quota.tokens+quota.rate*EXTRACT(EPOCH FROM(now()-quota.updated_at)))
                  - derived_quota_tokens,
           updated_at=now()
     WHERE quota.tenant_id=requested_tenant_id AND quota.bucket='llm_tokens'
       AND LEAST(quota.burst,
                 quota.tokens+quota.rate*EXTRACT(EPOCH FROM(now()-quota.updated_at)))
           >=derived_quota_tokens;
    IF NOT FOUND THEN
        RAISE EXCEPTION '123: verifier LLM quota admission denied'
            USING ERRCODE='P0001';
    END IF;
    INSERT INTO public.research_run_llm_spend_reservations (
        tenant_id,user_id,task_id,run_snapshot_id,stage,round_ordinal,subject_id,
        attempt_key,request_digest,trace_id,model,system_prompt_digest,
        user_prompt_digest,temperature,disable_thinking,model_policy_digest,
        quota_bucket,reserved_quota_tokens,reserved_completion_tokens,
        reserved_planner_tokens,reserved_cost_micro_usd,pricing_rule_id,
        cost_currency,counts_against_planner_budget,schema_version
    ) VALUES (
        requested_tenant_id,requested_user_id,requested_task_id,
        requested_run_snapshot_id,'synthesis',1,requested_subject_id,
        requested_attempt_key,requested_request_digest,requested_trace_id,
        stage_model,encode(sha256(convert_to(stage_system_prompt,'UTF8')),'hex'),
        encode(sha256(convert_to(requested_user_prompt,'UTF8')),'hex'),
        stage_temperature,stage_disable_thinking,snapshot_model_policy_digest,
        'llm_tokens',derived_quota_tokens,stage_max_tokens,0,reserved_cost,
        selected_pricing_rule_id,'USD',false,
        'vane.research-run-llm-spend-reservation/v1'
    ) RETURNING id INTO admitted_id;
    RETURN QUERY SELECT admitted_id,true;
END
$$;
-- +goose StatementEnd
