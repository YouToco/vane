-- 123: admit frozen v3.4 synthesis candidates to the independent verifier.
--
-- Migration 122 remains byte-immutable for v3.3 replay. This versioned
-- admission path accepts both retained v3.3 and current v3.4 snapshots while
-- preserving the same capability, candidate-prompt, quota, and pricing fences.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);

LOCK TABLE task_run_snapshots,research_run_llm_spend_reservations
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
CREATE FUNCTION enforce_research_run_llm_spend_reservation_v2()
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
REVOKE ALL ON FUNCTION enforce_research_run_llm_spend_reservation_v2()
    FROM PUBLIC;
DROP TRIGGER research_run_llm_spend_reservation_v1
    ON research_run_llm_spend_reservations;
CREATE TRIGGER research_run_llm_spend_reservation_v1
BEFORE INSERT ON research_run_llm_spend_reservations
FOR EACH ROW EXECUTE FUNCTION enforce_research_run_llm_spend_reservation_v2();

-- +goose StatementBegin
CREATE FUNCTION admit_research_run_llm_spend_v5(
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
REVOKE ALL ON FUNCTION admit_research_run_llm_spend_v5(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
) FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION admit_research_run_llm_spend_cap_v5(
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
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.current_research_run_capability_v1() capability
         WHERE capability.run_snapshot_id=requested_run_snapshot_id
           AND capability.tenant_id=requested_tenant_id
           AND capability.user_id=requested_user_id
           AND capability.task_id=requested_task_id
    ) THEN
        RAISE EXCEPTION '123: LLM admission capability differs'
            USING ERRCODE='42501';
    END IF;
    RETURN QUERY SELECT * FROM public.admit_research_run_llm_spend_v5(
        requested_tenant_id,requested_user_id,requested_task_id,
        requested_run_snapshot_id,requested_stage,requested_round_ordinal,
        requested_subject_id,requested_attempt_key,requested_request_digest,
        requested_trace_id,requested_user_prompt);
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION admit_research_run_llm_spend_cap_v5(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION admit_research_run_llm_spend_cap_v5(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
) TO vane_research_v3_executor;

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE task_run_snapshots,research_run_llm_spend_reservations
    IN ACCESS EXCLUSIVE MODE;

-- A v3.4 snapshot is historical protocol state. Removing its only admission
-- decoder would make replay non-deterministic, so rollback fails closed.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM public.task_run_snapshots snapshot
         WHERE snapshot.reference_schema_version='vane.research-run-snapshot-ref/v3'
           AND convert_from(snapshot.payload,'UTF8')::jsonb
                 #>> '{research_model,synthesis,renderer_version}' =
               'research-synthesis.render/v3.4'
    ) THEN
        RAISE EXCEPTION '123: v3.4 snapshot history exists'
            USING ERRCODE='55000';
    END IF;
END
$$;
-- +goose StatementEnd

DROP TRIGGER research_run_llm_spend_reservation_v1
    ON research_run_llm_spend_reservations;
CREATE TRIGGER research_run_llm_spend_reservation_v1
BEFORE INSERT ON research_run_llm_spend_reservations
FOR EACH ROW EXECUTE FUNCTION enforce_research_run_llm_spend_reservation_v1();
DROP FUNCTION enforce_research_run_llm_spend_reservation_v2();

REVOKE ALL ON FUNCTION admit_research_run_llm_spend_cap_v5(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
) FROM vane_research_v3_executor;
DROP FUNCTION admit_research_run_llm_spend_cap_v5(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
);
DROP FUNCTION admit_research_run_llm_spend_v5(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
);
