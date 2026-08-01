-- 097: process-isolated research LLM gateway.
-- The main server transmits only reservation binding plus the opaque run
-- capability. Prompts, model policy, identity, provider usage and completion
-- never cross from main into the gateway.

-- +goose Up

CREATE TABLE research_llm_gateway_frozen_requests (
    reservation_id BIGINT PRIMARY KEY REFERENCES research_run_llm_spend_reservations(id) ON DELETE CASCADE,
    request_digest TEXT NOT NULL,
    system_prompt TEXT NOT NULL,
    user_prompt TEXT NOT NULL,
    provider TEXT NOT NULL,
    endpoint_id TEXT NOT NULL,
    endpoint_generation BIGINT NOT NULL,
    credential_id TEXT NOT NULL,
    credential_generation BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    schema_version TEXT NOT NULL DEFAULT 'vane.research-llm-gateway-frozen-request/v1',
    CONSTRAINT ck_research_llm_gateway_frozen_digest CHECK (request_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_research_llm_gateway_frozen_route CHECK (
        provider='deepseek' AND endpoint_id='deepseek-compatible-primary' AND
        endpoint_generation>0 AND credential_id='llm-primary' AND
        credential_generation>0)
);
REVOKE ALL ON research_llm_gateway_frozen_requests
    FROM PUBLIC,vane_app,vane_research_v3_executor,vane_research_llm_gateway;

CREATE TABLE research_llm_process_gateway_settlements (
    reservation_id BIGINT PRIMARY KEY REFERENCES research_run_llm_spend_reservations(id) ON DELETE CASCADE,
    settlement_id BIGINT NOT NULL UNIQUE REFERENCES research_run_llm_spend_settlements(id) ON DELETE CASCADE,
    process_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_research_llm_process_version CHECK (
        process_version='vane.research-llm-process-gateway/v1')
);
REVOKE ALL ON research_llm_process_gateway_settlements
    FROM PUBLIC,vane_app,vane_research_v3_executor,vane_research_llm_gateway;

-- +goose StatementBegin
CREATE FUNCTION freeze_research_llm_gateway_request_v2(
    requested_reservation_id BIGINT,requested_request_digest TEXT,
    requested_system_prompt TEXT,requested_user_prompt TEXT
) RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE reservation_row RECORD;
BEGIN
    SELECT reservation.run_snapshot_id,reservation.tenant_id,reservation.user_id,
           reservation.task_id,reservation.request_digest,
           reservation.system_prompt_digest,reservation.user_prompt_digest,
           snapshot.reference_digest,snapshot.temporal_workflow_id,snapshot.temporal_run_id,
           (convert_from(snapshot.payload,'UTF8')::jsonb
             #>> '{research_model,provider}') AS route_provider,
           (convert_from(snapshot.payload,'UTF8')::jsonb
             #>> '{research_model,endpoint,id}') AS endpoint_id,
           (convert_from(snapshot.payload,'UTF8')::jsonb
             #>> '{research_model,endpoint,generation}')::bigint
             AS endpoint_generation,
           (convert_from(snapshot.payload,'UTF8')::jsonb
             #>> '{research_model,credential_ref,id}') AS credential_id,
           (convert_from(snapshot.payload,'UTF8')::jsonb
             #>> '{research_model,credential_ref,generation}')::bigint
             AS credential_generation,
           pricing.provider AS pricing_provider
      INTO reservation_row
      FROM public.research_run_llm_spend_reservations reservation
      JOIN public.task_run_snapshots snapshot ON snapshot.id=reservation.run_snapshot_id
       AND snapshot.tenant_id=reservation.tenant_id
       AND snapshot.user_id=reservation.user_id AND snapshot.task_id=reservation.task_id
      JOIN public.provider_price_rules pricing ON pricing.id=reservation.pricing_rule_id
     WHERE reservation.id=requested_reservation_id;
    IF reservation_row.request_digest IS NULL OR
       reservation_row.request_digest IS DISTINCT FROM requested_request_digest OR
       reservation_row.system_prompt_digest IS DISTINCT FROM
         encode(sha256(convert_to(requested_system_prompt,'UTF8')),'hex') OR
       reservation_row.user_prompt_digest IS DISTINCT FROM
         encode(sha256(convert_to(requested_user_prompt,'UTF8')),'hex') OR
       reservation_row.route_provider IS DISTINCT FROM reservation_row.pricing_provider OR
       reservation_row.route_provider<>'deepseek' OR
       reservation_row.endpoint_id<>'deepseek-compatible-primary' OR
       reservation_row.endpoint_generation<=0 OR
       reservation_row.credential_id<>'llm-primary' OR
       reservation_row.credential_generation<=0 THEN
        RAISE EXCEPTION '097: frozen request differs from reservation' USING ERRCODE='23514';
    END IF;
    PERFORM public.require_research_run_capability_v1(
        reservation_row.run_snapshot_id,reservation_row.reference_digest,
        reservation_row.tenant_id,reservation_row.user_id,reservation_row.task_id,
        reservation_row.temporal_workflow_id,reservation_row.temporal_run_id);
    INSERT INTO public.research_llm_gateway_frozen_requests(
        reservation_id,request_digest,system_prompt,user_prompt,provider,
        endpoint_id,endpoint_generation,credential_id,credential_generation)
    VALUES(requested_reservation_id,requested_request_digest,
           requested_system_prompt,requested_user_prompt,
           reservation_row.route_provider,reservation_row.endpoint_id,
           reservation_row.endpoint_generation,reservation_row.credential_id,
           reservation_row.credential_generation)
    ON CONFLICT (reservation_id) DO NOTHING;
    IF NOT EXISTS (SELECT 1 FROM public.research_llm_gateway_frozen_requests frozen
      WHERE frozen.reservation_id=requested_reservation_id
        AND frozen.request_digest=requested_request_digest
        AND frozen.system_prompt=requested_system_prompt
        AND frozen.user_prompt=requested_user_prompt
        AND frozen.provider=reservation_row.route_provider
        AND frozen.endpoint_id=reservation_row.endpoint_id
        AND frozen.endpoint_generation=reservation_row.endpoint_generation
        AND frozen.credential_id=reservation_row.credential_id
        AND frozen.credential_generation=reservation_row.credential_generation) THEN
        RAISE EXCEPTION '097: frozen request replay differs' USING ERRCODE='23514';
    END IF;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION freeze_research_llm_gateway_request_v2(BIGINT,TEXT,TEXT,TEXT)
    FROM PUBLIC,vane_app,vane_research_llm_gateway;
GRANT EXECUTE ON FUNCTION freeze_research_llm_gateway_request_v2(BIGINT,TEXT,TEXT,TEXT)
    TO vane_research_v3_executor;

-- +goose StatementBegin
CREATE FUNCTION load_research_llm_gateway_frozen_request_v2(
    requested_reservation_id BIGINT,requested_request_digest TEXT,
    requested_capability_hex TEXT
) RETURNS TABLE(
    out_tenant_id BIGINT,out_user_id BIGINT,out_task_id TEXT,
    out_run_snapshot_id BIGINT,out_trace_id TEXT,out_stage TEXT,
    out_system_prompt TEXT,out_user_prompt TEXT,out_provider TEXT,out_model TEXT,
    out_endpoint_id TEXT,out_endpoint_generation BIGINT,
    out_credential_id TEXT,out_credential_generation BIGINT,
    out_temperature REAL,out_max_tokens INTEGER,out_disable_thinking BOOLEAN
)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF requested_capability_hex !~ '^[0-9a-f]{64}$' OR
       requested_request_digest !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION '097: invalid gateway binding' USING ERRCODE='22023';
    END IF;
    RETURN QUERY
    SELECT reservation.tenant_id,reservation.user_id,reservation.task_id,
           reservation.run_snapshot_id,reservation.trace_id,reservation.stage,
           frozen_request.system_prompt,frozen_request.user_prompt,
           frozen_request.provider,reservation.model,
           frozen_request.endpoint_id,frozen_request.endpoint_generation,
           frozen_request.credential_id,frozen_request.credential_generation,
           reservation.temperature,
           reservation.reserved_completion_tokens::integer,reservation.disable_thinking
      FROM public.research_run_llm_spend_reservations reservation
      JOIN public.task_run_snapshots snapshot ON snapshot.id=reservation.run_snapshot_id
       AND snapshot.tenant_id=reservation.tenant_id
       AND snapshot.user_id=reservation.user_id AND snapshot.task_id=reservation.task_id
      JOIN public.research_run_capabilities capability
        ON capability.run_snapshot_id=reservation.run_snapshot_id
       AND capability.tenant_id=reservation.tenant_id
       AND capability.user_id=reservation.user_id AND capability.task_id=reservation.task_id
       AND capability.temporal_workflow_id=snapshot.temporal_workflow_id
       AND capability.temporal_run_id=snapshot.temporal_run_id
       AND capability.reference_digest=snapshot.reference_digest
      JOIN public.research_llm_gateway_attempts attempt
        ON attempt.reservation_id=reservation.id
       AND attempt.request_digest=reservation.request_digest
      JOIN public.research_llm_gateway_frozen_requests frozen_request
        ON frozen_request.reservation_id=reservation.id
       AND frozen_request.request_digest=reservation.request_digest
     WHERE reservation.id=requested_reservation_id
       AND reservation.request_digest=requested_request_digest
       AND capability.capability_hash=sha256(decode(requested_capability_hex,'hex'))
       -- Terminalization/recovery is authorized by the already committed
       -- send_started marker. Expiry or later revocation prevents new claims,
       -- but must not strand a paid effect that already crossed that boundary.
       AND attempt.attempt_state='send_started';
    IF NOT FOUND THEN
        RAISE EXCEPTION '097: gateway binding is unavailable' USING ERRCODE='42501';
    END IF;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION load_research_llm_gateway_frozen_request_v2(BIGINT,TEXT,TEXT)
    FROM PUBLIC,vane_app,vane_research_v3_executor,vane_research_llm_gateway;

-- Claim validates the capability before the durable first-writer marker. The
-- request text used to create that marker is loaded from the reservation's
-- immutable digests; callers cannot supply any model/provider field.
-- +goose StatementBegin
CREATE FUNCTION claim_research_llm_gateway_request_v2(
    requested_reservation_id BIGINT,requested_request_digest TEXT,
    requested_capability_hex TEXT
) RETURNS TABLE(
    out_first_writer BOOLEAN,out_settled BOOLEAN,
    out_tenant_id BIGINT,out_user_id BIGINT,out_task_id TEXT,
    out_run_snapshot_id BIGINT,out_trace_id TEXT,out_stage TEXT,
    out_system_prompt TEXT,out_user_prompt TEXT,out_provider TEXT,out_model TEXT,
    out_endpoint_id TEXT,out_endpoint_generation BIGINT,
    out_credential_id TEXT,out_credential_generation BIGINT,
    out_temperature REAL,out_max_tokens INTEGER,out_disable_thinking BOOLEAN
)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE frozen RECORD; first_writer BOOLEAN; already_settled BOOLEAN;
BEGIN
    IF requested_capability_hex !~ '^[0-9a-f]{64}$' OR
       requested_request_digest !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION '097: invalid gateway binding' USING ERRCODE='22023';
    END IF;
    -- Freeze from reservation before the marker exists. Capability matching is
    -- repeated here because the internal loader intentionally requires marker.
    SELECT reservation.tenant_id,reservation.user_id,reservation.task_id,
           reservation.run_snapshot_id,reservation.trace_id,reservation.stage,
           frozen_request.system_prompt,frozen_request.user_prompt,
           frozen_request.provider,reservation.model,
           frozen_request.endpoint_id,frozen_request.endpoint_generation,
           frozen_request.credential_id,frozen_request.credential_generation,
           reservation.temperature,
           reservation.reserved_completion_tokens::integer AS max_tokens,
           reservation.disable_thinking,snapshot.reference_digest,
           snapshot.temporal_workflow_id,snapshot.temporal_run_id,
           (capability.revoked_at IS NULL AND
            capability.not_after>statement_timestamp()) AS capability_live,
           (SELECT existing_attempt.attempt_state
              FROM public.research_llm_gateway_attempts existing_attempt
             WHERE existing_attempt.reservation_id=reservation.id
               AND existing_attempt.request_digest=reservation.request_digest)
             AS existing_attempt_state
      INTO frozen
      FROM public.research_run_llm_spend_reservations reservation
      JOIN public.task_run_snapshots snapshot ON snapshot.id=reservation.run_snapshot_id
       AND snapshot.tenant_id=reservation.tenant_id
       AND snapshot.user_id=reservation.user_id AND snapshot.task_id=reservation.task_id
      JOIN public.research_run_capabilities capability
        ON capability.run_snapshot_id=reservation.run_snapshot_id
       AND capability.tenant_id=reservation.tenant_id
       AND capability.user_id=reservation.user_id AND capability.task_id=reservation.task_id
       AND capability.temporal_workflow_id=snapshot.temporal_workflow_id
       AND capability.temporal_run_id=snapshot.temporal_run_id
       AND capability.reference_digest=snapshot.reference_digest
      JOIN public.research_llm_gateway_frozen_requests frozen_request
        ON frozen_request.reservation_id=reservation.id
       AND frozen_request.request_digest=reservation.request_digest
     WHERE reservation.id=requested_reservation_id
       AND reservation.request_digest=requested_request_digest
       AND capability.capability_hash=sha256(decode(requested_capability_hex,'hex'))
       AND ((capability.revoked_at IS NULL AND capability.not_after>statement_timestamp())
            OR EXISTS (SELECT 1 FROM public.research_run_llm_spend_settlements settlement
                        WHERE settlement.reservation_id=requested_reservation_id)
            OR EXISTS (SELECT 1 FROM public.research_llm_gateway_attempts existing_attempt
                        WHERE existing_attempt.reservation_id=requested_reservation_id
                          AND existing_attempt.request_digest=requested_request_digest
                          AND existing_attempt.attempt_state='send_started'))
     FOR UPDATE OF reservation;
    IF frozen.run_snapshot_id IS NULL THEN
        RAISE EXCEPTION '097: gateway binding is unavailable' USING ERRCODE='42501';
    END IF;
    SELECT EXISTS(
        SELECT 1 FROM public.research_run_llm_spend_settlements settlement
         WHERE settlement.reservation_id=requested_reservation_id)
      INTO already_settled;
    IF already_settled THEN
        -- Response-loss replay after a pre-send rejection must not try to
        -- relabel its immutable attempt as send_started.
        RETURN QUERY SELECT false,true,
            frozen.tenant_id,frozen.user_id,frozen.task_id,frozen.run_snapshot_id,
            frozen.trace_id,frozen.stage,frozen.system_prompt,frozen.user_prompt,
            frozen.provider,frozen.model,frozen.endpoint_id,
            frozen.endpoint_generation,frozen.credential_id,
            frozen.credential_generation,frozen.temperature,frozen.max_tokens,
            frozen.disable_thinking;
        RETURN;
    END IF;
    IF frozen.existing_attempt_state='send_started' THEN
        -- A live capability was required when this marker was first written.
        -- Replays only recover/terminalize that exact effect and must not call
        -- the legacy record function, which would re-authorize it.
        RETURN QUERY SELECT false,false,
            frozen.tenant_id,frozen.user_id,frozen.task_id,frozen.run_snapshot_id,
            frozen.trace_id,frozen.stage,frozen.system_prompt,frozen.user_prompt,
            frozen.provider,frozen.model,frozen.endpoint_id,
            frozen.endpoint_generation,frozen.credential_id,
            frozen.credential_generation,frozen.temperature,frozen.max_tokens,
            frozen.disable_thinking;
        RETURN;
    ELSIF frozen.existing_attempt_state IS NOT NULL OR NOT frozen.capability_live THEN
        RAISE EXCEPTION '097: gateway claim cannot create provider authority'
            USING ERRCODE='42501';
    END IF;
    PERFORM set_config('app.research_run_capability_v1',requested_capability_hex,true);
    PERFORM set_config('app.tenant_id',frozen.tenant_id::text,true);
    PERFORM set_config('app.user_id',frozen.user_id::text,true);
    first_writer:=public.record_research_llm_gateway_attempt_v1(
        requested_reservation_id,requested_request_digest,frozen.system_prompt,
        frozen.user_prompt,frozen.provider,frozen.model,frozen.temperature,
        frozen.max_tokens,frozen.disable_thinking,'send_started');
    RETURN QUERY SELECT first_writer,false,
        frozen.tenant_id,frozen.user_id,frozen.task_id,frozen.run_snapshot_id,
        frozen.trace_id,frozen.stage,frozen.system_prompt,frozen.user_prompt,
        frozen.provider,frozen.model,frozen.endpoint_id,
        frozen.endpoint_generation,frozen.credential_id,
        frozen.credential_generation,frozen.temperature,frozen.max_tokens,
        frozen.disable_thinking;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION claim_research_llm_gateway_request_v2(BIGINT,TEXT,TEXT)
    FROM PUBLIC,vane_app,vane_research_v3_executor;
GRANT EXECUTE ON FUNCTION claim_research_llm_gateway_request_v2(BIGINT,TEXT,TEXT)
    TO vane_research_llm_gateway;

-- +goose StatementBegin
CREATE FUNCTION settle_research_llm_gateway_request_v2(
    requested_reservation_id BIGINT,requested_request_digest TEXT,
    requested_capability_hex TEXT,requested_completion TEXT,
    requested_provider_reported_model TEXT,requested_prompt_tokens INTEGER,
    requested_completion_tokens INTEGER,requested_prompt_cache_hit_tokens INTEGER,
    requested_prompt_cache_miss_tokens INTEGER,requested_reasoning_tokens INTEGER,
    requested_latency_ms INTEGER,requested_prefix_cache_hit BOOLEAN,
    requested_error TEXT,requested_attempted BOOLEAN,requested_usage_known BOOLEAN,
    requested_definitely_zero_usage BOOLEAN,requested_outcome TEXT,
    requested_error_code TEXT
) RETURNS TABLE(out_llm_call_id BIGINT,out_settlement_id BIGINT)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE frozen RECORD; key_id TEXT; signed_at BIGINT; payload TEXT; signature BYTEA;
        settled_call_id BIGINT; settled_id BIGINT;
BEGIN
    SELECT * INTO frozen FROM public.load_research_llm_gateway_frozen_request_v2(
        requested_reservation_id,requested_request_digest,requested_capability_hex);
    PERFORM set_config('app.research_run_capability_v1',requested_capability_hex,true);
    PERFORM set_config('app.tenant_id',frozen.out_tenant_id::text,true);
    PERFORM set_config('app.user_id',frozen.out_user_id::text,true);
    IF NOT requested_attempted THEN
        IF requested_usage_known OR NOT requested_definitely_zero_usage OR
           requested_outcome<>'failed' THEN
            RAISE EXCEPTION '097: unattempted settlement is not definitely zero'
                USING ERRCODE='23514';
        END IF;
        UPDATE public.research_llm_gateway_attempts attempt
           SET attempt_state='pre_send_rejected'
         WHERE attempt.reservation_id=requested_reservation_id
           AND attempt.request_digest=requested_request_digest
           AND attempt.attempt_state='send_started'
           AND NOT EXISTS (SELECT 1 FROM public.research_run_llm_spend_settlements settlement
                            WHERE settlement.reservation_id=requested_reservation_id);
    END IF;
    key_id:=public.active_research_llm_gateway_key_id_v1();
    signed_at:=floor(extract(epoch FROM clock_timestamp())*1000)::bigint;
    payload:=array_to_string(ARRAY[
        'vane.research-llm-gateway-receipt/v1',key_id,signed_at::text,
        requested_reservation_id::text,requested_request_digest,
        encode(sha256(convert_to(frozen.out_system_prompt,'UTF8')),'hex'),
        encode(sha256(convert_to(frozen.out_user_prompt,'UTF8')),'hex'),
        encode(sha256(convert_to(requested_completion,'UTF8')),'hex'),
        encode(sha256(convert_to(frozen.out_provider,'UTF8')),'hex'),
        encode(sha256(convert_to(requested_provider_reported_model,'UTF8')),'hex'),
        requested_prompt_tokens::text,requested_completion_tokens::text,
        COALESCE(requested_prompt_cache_hit_tokens::text,'null'),
        COALESCE(requested_prompt_cache_miss_tokens::text,'null'),
        COALESCE(requested_reasoning_tokens::text,'null'),requested_latency_ms::text,
        COALESCE(requested_prefix_cache_hit::text,'null'),
        frozen.out_disable_thinking::text,
        encode(sha256(convert_to(requested_error,'UTF8')),'hex'),
        requested_attempted::text,requested_usage_known::text,
        requested_definitely_zero_usage::text,requested_outcome,
        encode(sha256(convert_to(requested_error_code,'UTF8')),'hex')
    ],E'\n');
    signature:=public.sign_research_llm_gateway_payload_v1(
        key_id,convert_to(payload,'UTF8'),requested_reservation_id,
        requested_request_digest,requested_attempted);
    SELECT signed.out_llm_call_id,signed.out_settlement_id
      INTO settled_call_id,settled_id
      FROM public.settle_signed_research_run_llm_spend_v3(
        frozen.out_tenant_id,frozen.out_user_id,frozen.out_task_id,
        frozen.out_run_snapshot_id,requested_reservation_id,requested_request_digest,
        frozen.out_system_prompt,frozen.out_user_prompt,requested_completion,
        frozen.out_provider,requested_provider_reported_model,requested_prompt_tokens,
        requested_completion_tokens,requested_prompt_cache_hit_tokens,
        requested_prompt_cache_miss_tokens,requested_reasoning_tokens,
        requested_latency_ms,requested_prefix_cache_hit,frozen.out_temperature,
        frozen.out_max_tokens,frozen.out_disable_thinking,requested_error,
        requested_attempted,requested_usage_known,requested_definitely_zero_usage,
        requested_outcome,requested_error_code,key_id,signed_at,signature) signed;
    INSERT INTO public.research_llm_process_gateway_settlements(
        reservation_id,settlement_id,process_version)
    VALUES(requested_reservation_id,settled_id,'vane.research-llm-process-gateway/v1')
    ON CONFLICT (reservation_id) DO NOTHING;
    IF NOT EXISTS (SELECT 1 FROM public.research_llm_process_gateway_settlements marker
      WHERE marker.reservation_id=requested_reservation_id
        AND marker.settlement_id=settled_id
        AND marker.process_version='vane.research-llm-process-gateway/v1') THEN
        RAISE EXCEPTION '097: process gateway settlement replay differs' USING ERRCODE='23514';
    END IF;
    RETURN QUERY SELECT settled_call_id,settled_id;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION settle_research_llm_gateway_request_v2(
    BIGINT,TEXT,TEXT,TEXT,TEXT,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,
    BOOLEAN,TEXT,BOOLEAN,BOOLEAN,BOOLEAN,TEXT,TEXT
) FROM PUBLIC,vane_app,vane_research_v3_executor;
GRANT EXECUTE ON FUNCTION settle_research_llm_gateway_request_v2(
    BIGINT,TEXT,TEXT,TEXT,TEXT,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,
    BOOLEAN,TEXT,BOOLEAN,BOOLEAN,BOOLEAN,TEXT,TEXT
) TO vane_research_llm_gateway;

-- The process gateway can claim and settle, but cannot use the old receipt
-- signer as a generic completion/usage attestation oracle.
REVOKE EXECUTE ON FUNCTION sign_research_llm_gateway_payload_v1(TEXT,BYTEA,BIGINT,TEXT,BOOLEAN)
    FROM vane_research_llm_gateway;
REVOKE EXECUTE ON FUNCTION active_research_llm_gateway_key_id_v1()
    FROM vane_research_llm_gateway;
REVOKE EXECUTE ON FUNCTION mark_research_llm_gateway_send_started_v1(
    BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,REAL,INTEGER,BOOLEAN) FROM vane_research_llm_gateway;
REVOKE EXECUTE ON FUNCTION mark_research_llm_gateway_pre_send_rejected_v1(
    BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,REAL,INTEGER,BOOLEAN) FROM vane_research_llm_gateway;
REVOKE EXECUTE ON FUNCTION research_llm_gateway_attempt_started_v1(BIGINT,TEXT)
    FROM vane_research_llm_gateway;
REVOKE EXECUTE ON FUNCTION load_research_llm_gateway_recovery_intent_v1(BIGINT,TEXT,TEXT)
    FROM vane_research_llm_gateway;
REVOKE EXECUTE ON FUNCTION require_research_run_capability_v1(
    BIGINT,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT) FROM vane_research_llm_gateway;
REVOKE EXECUTE ON FUNCTION settle_signed_research_run_llm_spend_v3(
    BIGINT,BIGINT,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,
    INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,BOOLEAN,REAL,INTEGER,
    BOOLEAN,TEXT,BOOLEAN,BOOLEAN,BOOLEAN,TEXT,TEXT,TEXT,BIGINT,BYTEA
) FROM vane_research_llm_gateway;

-- +goose StatementBegin
CREATE FUNCTION recover_research_llm_gateway_request_v2(
    requested_reservation_id BIGINT,requested_request_digest TEXT,
    requested_capability_hex TEXT
) RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE frozen RECORD; started_at TIMESTAMPTZ; already_settled BOOLEAN;
        locked_temporal_run_id TEXT; verified_temporal_run_id TEXT;
BEGIN
    IF requested_capability_hex !~ '^[0-9a-f]{64}$' OR
       requested_request_digest !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION '097: invalid gateway recovery binding' USING ERRCODE='22023';
    END IF;
    -- Recovery must use the same run-wide lock order as normal settlement:
    -- budget first, then the immutable reservation row. Derive the key only
    -- from server-side snapshot/capability identity; the caller cannot choose
    -- a temporal run or use a bearer from another frozen run.
    SELECT snapshot.temporal_run_id INTO locked_temporal_run_id
      FROM public.research_run_llm_spend_reservations reservation
      JOIN public.task_run_snapshots snapshot ON snapshot.id=reservation.run_snapshot_id
       AND snapshot.tenant_id=reservation.tenant_id
       AND snapshot.user_id=reservation.user_id AND snapshot.task_id=reservation.task_id
      JOIN public.research_run_capabilities capability
        ON capability.run_snapshot_id=reservation.run_snapshot_id
       AND capability.tenant_id=reservation.tenant_id
       AND capability.user_id=reservation.user_id AND capability.task_id=reservation.task_id
       AND capability.temporal_workflow_id=snapshot.temporal_workflow_id
       AND capability.temporal_run_id=snapshot.temporal_run_id
       AND capability.reference_digest=snapshot.reference_digest
     WHERE reservation.id=requested_reservation_id
       AND reservation.request_digest=requested_request_digest
       AND capability.capability_hash=sha256(decode(requested_capability_hex,'hex'));
    IF locked_temporal_run_id IS NULL THEN
        RAISE EXCEPTION '097: gateway recovery binding is unavailable'
            USING ERRCODE='42501';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'research-spend/v3:' || locked_temporal_run_id || ':budget',0));
    SELECT attempt.send_started_at,EXISTS(
               SELECT 1 FROM public.research_run_llm_spend_settlements settlement
                WHERE settlement.reservation_id=requested_reservation_id),
           snapshot.temporal_run_id
      INTO started_at,already_settled,verified_temporal_run_id
      FROM public.research_run_llm_spend_reservations reservation
      JOIN public.task_run_snapshots snapshot ON snapshot.id=reservation.run_snapshot_id
       AND snapshot.tenant_id=reservation.tenant_id
       AND snapshot.user_id=reservation.user_id AND snapshot.task_id=reservation.task_id
      JOIN public.research_run_capabilities capability
        ON capability.run_snapshot_id=reservation.run_snapshot_id
       AND capability.tenant_id=reservation.tenant_id
       AND capability.user_id=reservation.user_id AND capability.task_id=reservation.task_id
       AND capability.temporal_workflow_id=snapshot.temporal_workflow_id
       AND capability.temporal_run_id=snapshot.temporal_run_id
       AND capability.reference_digest=snapshot.reference_digest
      JOIN public.research_llm_gateway_attempts attempt
        ON attempt.reservation_id=reservation.id
       AND attempt.request_digest=reservation.request_digest
       AND attempt.attempt_state='send_started'
      JOIN public.research_llm_gateway_frozen_requests frozen_request
        ON frozen_request.reservation_id=reservation.id
       AND frozen_request.request_digest=reservation.request_digest
     WHERE reservation.id=requested_reservation_id
       AND reservation.request_digest=requested_request_digest
       AND capability.capability_hash=sha256(decode(requested_capability_hex,'hex'))
     FOR UPDATE OF reservation;
    IF started_at IS NULL OR
       verified_temporal_run_id IS DISTINCT FROM locked_temporal_run_id THEN
        RAISE EXCEPTION '097: gateway recovery binding is unavailable'
            USING ERRCODE='42501';
    END IF;
    IF already_settled THEN
        RETURN;
    END IF;
    IF started_at>clock_timestamp()-interval '10 minutes' THEN
        RAISE EXCEPTION '097: gateway effect is not yet recoverable'
            USING ERRCODE='55000';
    END IF;
    SELECT * INTO frozen FROM public.load_research_llm_gateway_frozen_request_v2(
        requested_reservation_id,requested_request_digest,requested_capability_hex);
    PERFORM * FROM public.settle_research_llm_gateway_request_v2(
        requested_reservation_id,requested_request_digest,requested_capability_hex,
        '',frozen.out_model,0,0,NULL,NULL,NULL,0,NULL,
        'gateway recovery: provider outcome unavailable',true,false,false,
        'indeterminate','LLM_UNAVAILABLE');
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION recover_research_llm_gateway_request_v2(BIGINT,TEXT,TEXT)
    FROM PUBLIC,vane_app,vane_research_v3_executor;
GRANT EXECUTE ON FUNCTION recover_research_llm_gateway_request_v2(BIGINT,TEXT,TEXT)
    TO vane_research_llm_gateway;

-- +goose Down
LOCK TABLE research_llm_gateway_frozen_requests,
           research_llm_process_gateway_settlements IN ACCESS EXCLUSIVE MODE;
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM research_llm_gateway_frozen_requests) OR
       EXISTS (SELECT 1 FROM research_llm_process_gateway_settlements) THEN
        RAISE EXCEPTION '097: cannot remove frozen or issued process gateway effects'
            USING ERRCODE='55000';
    END IF;
END
$$;
-- +goose StatementEnd
DROP FUNCTION recover_research_llm_gateway_request_v2(BIGINT,TEXT,TEXT);
GRANT EXECUTE ON FUNCTION sign_research_llm_gateway_payload_v1(TEXT,BYTEA,BIGINT,TEXT,BOOLEAN)
    TO vane_research_llm_gateway;
GRANT EXECUTE ON FUNCTION active_research_llm_gateway_key_id_v1() TO vane_research_llm_gateway;
GRANT EXECUTE ON FUNCTION mark_research_llm_gateway_send_started_v1(
    BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,REAL,INTEGER,BOOLEAN) TO vane_research_llm_gateway;
GRANT EXECUTE ON FUNCTION mark_research_llm_gateway_pre_send_rejected_v1(
    BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,REAL,INTEGER,BOOLEAN) TO vane_research_llm_gateway;
GRANT EXECUTE ON FUNCTION research_llm_gateway_attempt_started_v1(BIGINT,TEXT)
    TO vane_research_llm_gateway;
GRANT EXECUTE ON FUNCTION load_research_llm_gateway_recovery_intent_v1(BIGINT,TEXT,TEXT)
    TO vane_research_llm_gateway;
GRANT EXECUTE ON FUNCTION require_research_run_capability_v1(
    BIGINT,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT) TO vane_research_llm_gateway;
GRANT EXECUTE ON FUNCTION settle_signed_research_run_llm_spend_v3(
    BIGINT,BIGINT,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,
    INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,BOOLEAN,REAL,INTEGER,
    BOOLEAN,TEXT,BOOLEAN,BOOLEAN,BOOLEAN,TEXT,TEXT,TEXT,BIGINT,BYTEA
) TO vane_research_llm_gateway;
DROP FUNCTION settle_research_llm_gateway_request_v2(
    BIGINT,TEXT,TEXT,TEXT,TEXT,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,
    BOOLEAN,TEXT,BOOLEAN,BOOLEAN,BOOLEAN,TEXT,TEXT);
DROP FUNCTION claim_research_llm_gateway_request_v2(BIGINT,TEXT,TEXT);
DROP FUNCTION load_research_llm_gateway_frozen_request_v2(BIGINT,TEXT,TEXT);
DROP FUNCTION freeze_research_llm_gateway_request_v2(BIGINT,TEXT,TEXT,TEXT);
DROP TABLE research_llm_process_gateway_settlements;
DROP TABLE research_llm_gateway_frozen_requests;
