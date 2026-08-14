-- 094: trusted V3 LLM gateway receipts.
--
-- Provider facts are no longer accepted from the general research executor.
-- A narrow gateway capability submits an HMAC-authenticated canonical receipt;
-- only the verifier (SECURITY DEFINER) can read the database verifier secret.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);
CREATE EXTENSION IF NOT EXISTS pgcrypto;
LOCK TABLE research_run_llm_spend_reservations,
           research_run_llm_spend_settlements,llm_calls,
           research_run_plans,research_brief_syntheses IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles
                    WHERE rolname='vane_research_llm_gateway') THEN
        CREATE ROLE vane_research_llm_gateway NOLOGIN NOINHERIT NOSUPERUSER
            NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles
                    WHERE rolname='vane_research_llm_gateway_runtime') THEN
        CREATE ROLE vane_research_llm_gateway_runtime NOLOGIN NOINHERIT NOSUPERUSER
            NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    END IF;
END $$;
-- +goose StatementEnd
ALTER ROLE vane_research_llm_gateway_runtime NOLOGIN NOINHERIT NOSUPERUSER
    NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
REVOKE vane_app,vane_research_v3_executor FROM vane_research_llm_gateway_runtime;
REVOKE vane_research_llm_gateway FROM vane_research_runtime,
    vane_research_v3_executor,vane_app;
GRANT vane_research_llm_gateway TO vane_research_llm_gateway_runtime;

CREATE TABLE research_llm_gateway_verifier_keys (
    key_id       TEXT        PRIMARY KEY,
    status       TEXT        NOT NULL,
    valid_from   TIMESTAMPTZ NOT NULL,
    valid_until  TIMESTAMPTZ,
    secret       BYTEA       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_research_llm_gateway_key_id CHECK (
        btrim(key_id)=key_id AND octet_length(key_id) BETWEEN 1 AND 128),
    CONSTRAINT ck_research_llm_gateway_key_status CHECK (
        status IN ('active','retired','revoked')),
    CONSTRAINT ck_research_llm_gateway_key_validity CHECK (
        octet_length(secret)>=32 AND
        (valid_until IS NULL OR valid_until>valid_from))
);
CREATE UNIQUE INDEX uq_research_llm_gateway_one_active_key
    ON research_llm_gateway_verifier_keys((status)) WHERE status='active';

INSERT INTO research_llm_gateway_verifier_keys(
    key_id,status,valid_from,secret
) VALUES (
    'gw-' || encode(gen_random_bytes(12),'hex'),
    'active',clock_timestamp(),gen_random_bytes(32)
);

REVOKE ALL ON research_llm_gateway_verifier_keys
    FROM PUBLIC,vane_app,vane_research_v3_executor,vane_research_llm_gateway;
GRANT EXECUTE ON FUNCTION require_research_run_capability_v1(
    BIGINT,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT
) TO vane_research_llm_gateway;

CREATE TABLE research_llm_gateway_attempts (
    reservation_id BIGINT PRIMARY KEY
        REFERENCES research_run_llm_spend_reservations(id) ON DELETE CASCADE,
    request_digest TEXT NOT NULL,
    system_prompt TEXT NOT NULL,
    user_prompt TEXT NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    temperature REAL NOT NULL,
    max_tokens INTEGER NOT NULL,
    disable_thinking BOOLEAN NOT NULL,
    attempt_state TEXT NOT NULL,
    send_started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    schema_version TEXT NOT NULL,
    CONSTRAINT ck_research_llm_gateway_attempt_digest CHECK (
        request_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_research_llm_gateway_attempt_schema CHECK (
        schema_version='vane.research-llm-gateway-attempt/v1'),
    CONSTRAINT ck_research_llm_gateway_attempt_state CHECK (
        attempt_state IN ('send_started','pre_send_rejected'))
);
REVOKE ALL ON research_llm_gateway_attempts
    FROM PUBLIC,vane_app,vane_research_v3_executor,vane_research_llm_gateway;

-- +goose StatementBegin
CREATE FUNCTION record_research_llm_gateway_attempt_v1(
    requested_reservation_id BIGINT,requested_request_digest TEXT,
    requested_system_prompt TEXT,requested_user_prompt TEXT,
    requested_provider TEXT,requested_model TEXT,requested_temperature REAL,
    requested_max_tokens INTEGER,requested_disable_thinking BOOLEAN,
    requested_attempt_state TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE reservation_row RECORD; inserted BOOLEAN;
BEGIN
    SELECT reservation.tenant_id,reservation.user_id,reservation.task_id,
           reservation.run_snapshot_id,reservation.request_digest,
           snapshot.reference_digest,snapshot.temporal_workflow_id,
           snapshot.temporal_run_id
      INTO reservation_row
      FROM public.research_run_llm_spend_reservations reservation
      JOIN public.task_run_snapshots snapshot ON snapshot.id=reservation.run_snapshot_id
       AND snapshot.tenant_id=reservation.tenant_id
       AND snapshot.user_id=reservation.user_id AND snapshot.task_id=reservation.task_id
     WHERE reservation.id=requested_reservation_id FOR UPDATE OF reservation;
    IF reservation_row.request_digest IS NULL OR
       reservation_row.request_digest IS DISTINCT FROM requested_request_digest THEN
        RAISE EXCEPTION '094: gateway send marker differs from reservation'
            USING ERRCODE='23514';
    END IF;
    PERFORM public.require_research_run_capability_v1(
        reservation_row.run_snapshot_id,reservation_row.reference_digest,
        reservation_row.tenant_id,reservation_row.user_id,reservation_row.task_id,
        reservation_row.temporal_workflow_id,reservation_row.temporal_run_id);
    IF NOT EXISTS (
        SELECT 1 FROM public.research_run_llm_spend_reservations reservation
        JOIN public.provider_price_rules pricing ON pricing.id=reservation.pricing_rule_id
        WHERE reservation.id=requested_reservation_id
          AND reservation.system_prompt_digest=encode(sha256(convert_to(requested_system_prompt,'UTF8')),'hex')
          AND reservation.user_prompt_digest=encode(sha256(convert_to(requested_user_prompt,'UTF8')),'hex')
          AND pricing.provider=requested_provider AND reservation.model=requested_model
          AND reservation.temperature=requested_temperature
          AND reservation.reserved_completion_tokens=requested_max_tokens
          AND reservation.disable_thinking=requested_disable_thinking
    ) THEN
        RAISE EXCEPTION '094: gateway send intent differs from reservation' USING ERRCODE='23514';
    END IF;
    INSERT INTO public.research_llm_gateway_attempts(
        reservation_id,request_digest,system_prompt,user_prompt,provider,model,
        temperature,max_tokens,disable_thinking,attempt_state,schema_version
    ) VALUES (requested_reservation_id,requested_request_digest,
              requested_system_prompt,requested_user_prompt,requested_provider,
              requested_model,requested_temperature,requested_max_tokens,
              requested_disable_thinking,requested_attempt_state,
              'vane.research-llm-gateway-attempt/v1')
    ON CONFLICT (reservation_id) DO NOTHING
    RETURNING true INTO inserted;
    IF inserted THEN RETURN true; END IF;
    IF NOT EXISTS (SELECT 1 FROM public.research_llm_gateway_attempts
      WHERE reservation_id=requested_reservation_id
        AND request_digest=requested_request_digest
        AND system_prompt=requested_system_prompt
        AND user_prompt=requested_user_prompt
        AND provider=requested_provider AND model=requested_model
        AND temperature=requested_temperature AND max_tokens=requested_max_tokens
        AND disable_thinking=requested_disable_thinking
        AND attempt_state=requested_attempt_state) THEN
        RAISE EXCEPTION '094: gateway send marker replay differs' USING ERRCODE='23514';
    END IF;
    RETURN false;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION record_research_llm_gateway_attempt_v1(
    BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,REAL,INTEGER,BOOLEAN,TEXT) FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION mark_research_llm_gateway_send_started_v1(
    BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,REAL,INTEGER,BOOLEAN
) RETURNS BOOLEAN LANGUAGE sql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
 SELECT public.record_research_llm_gateway_attempt_v1(
    $1,$2,$3,$4,$5,$6,$7,$8,$9,'send_started')
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION mark_research_llm_gateway_send_started_v1(
    BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,REAL,INTEGER,BOOLEAN) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION mark_research_llm_gateway_send_started_v1(
    BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,REAL,INTEGER,BOOLEAN) TO vane_research_llm_gateway;

-- +goose StatementBegin
CREATE FUNCTION mark_research_llm_gateway_pre_send_rejected_v1(
    BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,REAL,INTEGER,BOOLEAN
) RETURNS BOOLEAN LANGUAGE sql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
 SELECT public.record_research_llm_gateway_attempt_v1(
    $1,$2,$3,$4,$5,$6,$7,$8,$9,'pre_send_rejected')
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION mark_research_llm_gateway_pre_send_rejected_v1(
    BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,REAL,INTEGER,BOOLEAN) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION mark_research_llm_gateway_pre_send_rejected_v1(
    BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,REAL,INTEGER,BOOLEAN) TO vane_research_llm_gateway;

-- +goose StatementBegin
CREATE FUNCTION research_llm_gateway_attempt_started_v1(
    requested_reservation_id BIGINT,requested_request_digest TEXT
) RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE reservation_row RECORD;
BEGIN
    SELECT reservation.tenant_id,reservation.user_id,reservation.task_id,
           reservation.run_snapshot_id,reservation.request_digest,
           snapshot.reference_digest,snapshot.temporal_workflow_id,
           snapshot.temporal_run_id
      INTO reservation_row
      FROM public.research_run_llm_spend_reservations reservation
      JOIN public.task_run_snapshots snapshot ON snapshot.id=reservation.run_snapshot_id
       AND snapshot.tenant_id=reservation.tenant_id
       AND snapshot.user_id=reservation.user_id AND snapshot.task_id=reservation.task_id
     WHERE reservation.id=requested_reservation_id;
    IF reservation_row.request_digest IS NULL OR
       reservation_row.request_digest IS DISTINCT FROM requested_request_digest THEN
        RAISE EXCEPTION '094: gateway attempt query differs from reservation'
            USING ERRCODE='23514';
    END IF;
    PERFORM public.require_research_run_capability_v1(
        reservation_row.run_snapshot_id,reservation_row.reference_digest,
        reservation_row.tenant_id,reservation_row.user_id,reservation_row.task_id,
        reservation_row.temporal_workflow_id,reservation_row.temporal_run_id);
    RETURN COALESCE((
        SELECT attempt.attempt_state FROM public.research_llm_gateway_attempts attempt
         WHERE attempt.reservation_id=requested_reservation_id
           AND attempt.request_digest=requested_request_digest),'none');
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION research_llm_gateway_attempt_started_v1(BIGINT,TEXT)
    FROM PUBLIC,vane_app,vane_research_v3_executor;
GRANT EXECUTE ON FUNCTION research_llm_gateway_attempt_started_v1(BIGINT,TEXT)
    TO vane_research_llm_gateway;

-- +goose StatementBegin
CREATE FUNCTION load_research_llm_gateway_recovery_intent_v1(
    requested_reservation_id BIGINT,requested_request_digest TEXT,
    requested_attempt_state TEXT
) RETURNS TABLE(out_system_prompt TEXT,out_user_prompt TEXT,out_provider TEXT,
                out_model TEXT,out_temperature REAL,out_max_tokens INTEGER,
                out_disable_thinking BOOLEAN,out_trace_id TEXT,out_stage TEXT)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF requested_attempt_state NOT IN ('send_started','pre_send_rejected') OR
       public.research_llm_gateway_attempt_started_v1(
        requested_reservation_id,requested_request_digest)<>requested_attempt_state THEN
        RAISE EXCEPTION '094: gateway recovery requires durable send marker'
            USING ERRCODE='23514';
    END IF;
    IF requested_attempt_state='send_started' AND NOT EXISTS (
        SELECT 1 FROM public.research_llm_gateway_attempts attempt
         WHERE attempt.reservation_id=requested_reservation_id
           AND attempt.request_digest=requested_request_digest
           AND attempt.attempt_state='send_started'
           AND attempt.send_started_at<=clock_timestamp()-interval '10 minutes'
    ) THEN
        RAISE EXCEPTION '094: gateway send may still be in flight'
            USING ERRCODE='55000';
    END IF;
    RETURN QUERY
    SELECT attempt.system_prompt,attempt.user_prompt,attempt.provider,attempt.model,
           attempt.temperature,attempt.max_tokens,attempt.disable_thinking,
           reservation.trace_id,reservation.stage
      FROM public.research_llm_gateway_attempts attempt
      JOIN public.research_run_llm_spend_reservations reservation
        ON reservation.id=attempt.reservation_id
     WHERE attempt.reservation_id=requested_reservation_id
       AND attempt.request_digest=requested_request_digest;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION load_research_llm_gateway_recovery_intent_v1(BIGINT,TEXT,TEXT)
    FROM PUBLIC,vane_app,vane_research_v3_executor;
GRANT EXECUTE ON FUNCTION load_research_llm_gateway_recovery_intent_v1(BIGINT,TEXT,TEXT)
    TO vane_research_llm_gateway;

-- Executor replay code may inspect only the exact LLM call already bound to a
-- capability-scoped reservation. It never receives direct SELECT on llm_calls.
-- +goose StatementBegin
CREATE FUNCTION load_research_run_bound_llm_call_v1(
    requested_call_id BIGINT,requested_reservation_id BIGINT
) RETURNS SETOF public.llm_calls
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE capability_row RECORD;
BEGIN
    SELECT * INTO capability_row FROM public.current_research_run_capability_v1();
    IF capability_row.run_snapshot_id IS NULL THEN
        RAISE EXCEPTION '094: bound LLM call capability is unavailable'
            USING ERRCODE='23514';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM public.research_run_llm_spend_reservations reservation
        JOIN public.task_run_snapshots snapshot ON snapshot.id=reservation.run_snapshot_id
         AND snapshot.tenant_id=reservation.tenant_id
         AND snapshot.user_id=reservation.user_id AND snapshot.task_id=reservation.task_id
        WHERE reservation.id=requested_reservation_id
          AND reservation.run_snapshot_id=capability_row.run_snapshot_id
          AND reservation.tenant_id=capability_row.tenant_id
          AND reservation.user_id=capability_row.user_id
          AND reservation.task_id=capability_row.task_id
          AND snapshot.reference_digest=capability_row.reference_digest
          AND snapshot.temporal_workflow_id=capability_row.temporal_workflow_id
          AND snapshot.temporal_run_id=capability_row.temporal_run_id
    ) THEN
        RAISE EXCEPTION '094: bound LLM call reservation differs from capability'
            USING ERRCODE='42501';
    END IF;
    RETURN QUERY SELECT call_row.* FROM public.llm_calls call_row
      WHERE call_row.id=requested_call_id
        AND call_row.research_run_llm_spend_reservation_id=requested_reservation_id;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION load_research_run_bound_llm_call_v1(BIGINT,BIGINT)
    FROM PUBLIC,vane_app,vane_research_llm_gateway;
GRANT EXECUTE ON FUNCTION load_research_run_bound_llm_call_v1(BIGINT,BIGINT)
    TO vane_research_v3_executor;

ALTER TABLE research_run_llm_spend_settlements
    ADD COLUMN receipt_provenance TEXT NOT NULL DEFAULT 'legacy_runtime',
    ADD COLUMN gateway_key_id TEXT,
    ADD COLUMN gateway_signed_at_unix_ms BIGINT,
    ADD COLUMN gateway_payload_digest TEXT,
    ADD COLUMN gateway_signature BYTEA,
    ADD CONSTRAINT ck_research_llm_spend_gateway_provenance CHECK (
        (receipt_provenance='legacy_runtime' AND gateway_key_id IS NULL AND
         gateway_signed_at_unix_ms IS NULL AND gateway_payload_digest IS NULL AND
         gateway_signature IS NULL) OR
        (receipt_provenance='verified_gateway' AND
         btrim(gateway_key_id)=gateway_key_id AND
         octet_length(gateway_key_id) BETWEEN 1 AND 128 AND
         gateway_signed_at_unix_ms>0 AND
         gateway_payload_digest ~ '^[0-9a-f]{64}$' AND
         octet_length(gateway_signature)=32)
    );

-- Constant-work comparison for fixed-size HMAC-SHA256 values. Both operands
-- are scanned fully; no byte-position mismatch is returned early.
-- +goose StatementBegin
CREATE FUNCTION research_llm_gateway_constant_time_equal_v1(left_value BYTEA,right_value BYTEA)
RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE STRICT
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE difference INTEGER:=0; index_value INTEGER;
BEGIN
    IF octet_length(left_value)<>32 OR octet_length(right_value)<>32 THEN
        RETURN false;
    END IF;
    FOR index_value IN 0..31 LOOP
        difference := difference | (get_byte(left_value,index_value) #
                                    get_byte(right_value,index_value));
    END LOOP;
    RETURN difference=0;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION research_llm_gateway_constant_time_equal_v1(BYTEA,BYTEA)
    FROM PUBLIC,vane_app,vane_research_v3_executor,vane_research_llm_gateway;

-- Narrow signing API for the separately authenticated gateway process. It
-- reveals neither secret bytes nor verifier table rows. The active-key lookup
-- takes a row lock retained by the caller transaction so rotation cannot split
-- key selection from signing.
-- +goose StatementBegin
CREATE FUNCTION active_research_llm_gateway_key_id_v1()
RETURNS TEXT
LANGUAGE sql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
    SELECT key_id FROM public.research_llm_gateway_verifier_keys
     WHERE status='active' AND valid_from<=clock_timestamp()
       AND (valid_until IS NULL OR valid_until>clock_timestamp())
     FOR SHARE
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION active_research_llm_gateway_key_id_v1()
    FROM PUBLIC,vane_app,vane_research_v3_executor;
GRANT EXECUTE ON FUNCTION active_research_llm_gateway_key_id_v1()
    TO vane_research_llm_gateway;

-- +goose StatementBegin
CREATE FUNCTION sign_research_llm_gateway_payload_v1(
    requested_key_id TEXT,canonical_payload BYTEA,requested_reservation_id BIGINT,
    requested_request_digest TEXT,requested_attempted BOOLEAN)
RETURNS BYTEA
LANGUAGE sql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
    SELECT hmac(canonical_payload,key.secret,'sha256')
      FROM public.research_llm_gateway_verifier_keys key
     WHERE key.key_id=requested_key_id AND key.status='active'
       AND valid_from<=clock_timestamp()
       AND (valid_until IS NULL OR valid_until>clock_timestamp())
       AND EXISTS (
           SELECT 1 FROM public.research_llm_gateway_attempts attempt
            WHERE attempt.reservation_id=requested_reservation_id
              AND attempt.request_digest=requested_request_digest
              AND attempt.attempt_state=CASE WHEN requested_attempted
                  THEN 'send_started' ELSE 'pre_send_rejected' END)
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION sign_research_llm_gateway_payload_v1(TEXT,BYTEA,BIGINT,TEXT,BOOLEAN)
    FROM PUBLIC,vane_app,vane_research_v3_executor;
GRANT EXECUTE ON FUNCTION sign_research_llm_gateway_payload_v1(TEXT,BYTEA,BIGINT,TEXT,BOOLEAN)
    TO vane_research_llm_gateway;

-- The old terminal function remains for historical replay/rollback, but it is
-- no longer a runtime capability. Only the signed verifier below may enter it.
REVOKE EXECUTE ON FUNCTION settle_research_run_llm_spend_v3(
    BIGINT,BIGINT,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT,TEXT,INTEGER,INTEGER,
    INTEGER,INTEGER,INTEGER,INTEGER,BOOLEAN,REAL,INTEGER,BOOLEAN,TEXT,
    BOOLEAN,BOOLEAN,BOOLEAN,TEXT,TEXT
) FROM PUBLIC,vane_app,vane_research_v3_executor,vane_research_llm_gateway;
REVOKE SELECT ON llm_calls FROM vane_research_v3_executor;
REVOKE SELECT (
    id,trace_id,span_name,user_id,ref_type,ref_id,provider,model,system_prompt,
    user_prompt,completion,prompt_tokens,completion_tokens,latency_ms,cost_usd,
    prefix_cache_hit,temperature,max_tokens,disable_thinking,error,tenant_id,
    prompt_cache_hit_tokens,prompt_cache_miss_tokens,reasoning_tokens,
    pricing_rule_id,pricing_status,cost_amount,cost_currency,run_snapshot_id,
    research_run_llm_spend_reservation_id,created_at
) ON llm_calls FROM vane_research_v3_executor;

-- The legacy settlement INSERT is stamped only while executing inside the
-- verified wrapper. A caller-controlled custom GUC is harmless because the
-- executor no longer has the underlying settle capability.
-- +goose StatementBegin
CREATE FUNCTION stamp_verified_research_llm_gateway_receipt_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE marker TEXT;
BEGIN
    marker:=current_setting('app.research_llm_gateway_verified',true);
    IF marker='1' THEN
        NEW.receipt_provenance:='verified_gateway';
        NEW.gateway_key_id:=current_setting('app.research_llm_gateway_key_id',true);
        NEW.gateway_signed_at_unix_ms:=
            current_setting('app.research_llm_gateway_signed_at_ms',true)::bigint;
        NEW.gateway_payload_digest:=
            current_setting('app.research_llm_gateway_payload_digest',true);
        NEW.gateway_signature:=decode(
            current_setting('app.research_llm_gateway_signature_hex',true),'hex');
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION stamp_verified_research_llm_gateway_receipt_v1()
    FROM PUBLIC,vane_app,vane_research_v3_executor,vane_research_llm_gateway;
CREATE TRIGGER stamp_verified_research_llm_gateway_receipt_v1
BEFORE INSERT ON research_run_llm_spend_settlements
FOR EACH ROW EXECUTE FUNCTION stamp_verified_research_llm_gateway_receipt_v1();

-- +goose StatementBegin
CREATE FUNCTION settle_signed_research_run_llm_spend_v3(
    requested_tenant_id BIGINT,
    requested_user_id BIGINT,
    requested_task_id TEXT,
    requested_run_snapshot_id BIGINT,
    requested_reservation_id BIGINT,
    requested_request_digest TEXT,
    requested_system_prompt TEXT,
    requested_user_prompt TEXT,
    requested_completion TEXT,
    requested_provider TEXT,
    requested_provider_reported_model TEXT,
    requested_prompt_tokens INTEGER,
    requested_completion_tokens INTEGER,
    requested_prompt_cache_hit_tokens INTEGER,
    requested_prompt_cache_miss_tokens INTEGER,
    requested_reasoning_tokens INTEGER,
    requested_latency_ms INTEGER,
    requested_prefix_cache_hit BOOLEAN,
    requested_temperature REAL,
    requested_max_tokens INTEGER,
    requested_disable_thinking BOOLEAN,
    requested_error TEXT,
    requested_attempted BOOLEAN,
    requested_usage_known BOOLEAN,
    requested_definitely_zero_usage BOOLEAN,
    requested_outcome TEXT,
    requested_error_code TEXT,
    requested_key_id TEXT,
    requested_signed_at_unix_ms BIGINT,
    requested_signature BYTEA
) RETURNS TABLE(out_llm_call_id BIGINT,out_settlement_id BIGINT)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    reservation_row RECORD;
    locked_temporal_run_id TEXT;
    verifier_secret BYTEA;
    canonical_payload TEXT;
    expected_signature BYTEA;
    payload_digest TEXT;
BEGIN
    IF requested_signature IS NULL OR octet_length(requested_signature)<>32 OR
       requested_signed_at_unix_ms IS NULL OR requested_signed_at_unix_ms<=0 OR
       requested_signed_at_unix_ms>
           floor(extract(epoch FROM clock_timestamp())*1000)::bigint+300000 THEN
        RAISE EXCEPTION '094: gateway receipt envelope is invalid' USING ERRCODE='42501';
    END IF;
    -- Derive the run-wide budget lock from immutable server-side identity
    -- before taking the reservation row lock. Tool admission and all LLM
    -- admission/settlement paths use this same key, so an over-reservation
    -- Provider receipt cannot race a Tool admission against stale spend.
    -- The following locked read repeats and validates this pre-read; callers
    -- never provide temporal_run_id and therefore cannot choose the lock key.
    SELECT snapshot.temporal_run_id INTO locked_temporal_run_id
      FROM public.research_run_llm_spend_reservations reservation
      JOIN public.task_run_snapshots snapshot ON snapshot.id=reservation.run_snapshot_id
       AND snapshot.tenant_id=reservation.tenant_id
       AND snapshot.user_id=reservation.user_id
       AND snapshot.task_id=reservation.task_id
     WHERE reservation.id=requested_reservation_id
       AND reservation.tenant_id=requested_tenant_id
       AND reservation.user_id=requested_user_id
       AND reservation.task_id=requested_task_id
       AND reservation.run_snapshot_id=requested_run_snapshot_id
       AND reservation.request_digest=requested_request_digest;
    IF locked_temporal_run_id IS NULL THEN
        RAISE EXCEPTION '094: gateway receipt differs from reservation'
            USING ERRCODE='23514';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'research-spend/v3:' || locked_temporal_run_id || ':budget',0));
    SELECT reservation.request_digest,attempt.send_started_at AS attempt_started_at,
           pricing.provider,
           snapshot.reference_digest,snapshot.temporal_workflow_id,
           snapshot.temporal_run_id
      INTO reservation_row
      FROM public.research_run_llm_spend_reservations reservation
      JOIN public.provider_price_rules pricing ON pricing.id=reservation.pricing_rule_id
      JOIN public.task_run_snapshots snapshot ON snapshot.id=reservation.run_snapshot_id
       AND snapshot.tenant_id=reservation.tenant_id
       AND snapshot.user_id=reservation.user_id
       AND snapshot.task_id=reservation.task_id
      JOIN public.research_llm_gateway_attempts attempt
        ON attempt.reservation_id=reservation.id
       AND attempt.request_digest=reservation.request_digest
     WHERE reservation.id=requested_reservation_id
       AND reservation.tenant_id=requested_tenant_id
       AND reservation.user_id=requested_user_id
       AND reservation.task_id=requested_task_id
       AND reservation.run_snapshot_id=requested_run_snapshot_id
     FOR UPDATE OF reservation;
    IF reservation_row.request_digest IS NULL OR
       reservation_row.request_digest IS DISTINCT FROM requested_request_digest OR
       reservation_row.provider IS DISTINCT FROM requested_provider OR
       reservation_row.temporal_run_id IS DISTINCT FROM locked_temporal_run_id THEN
        RAISE EXCEPTION '094: gateway receipt differs from reservation' USING ERRCODE='23514';
    END IF;
    IF requested_outcome='indeterminate' AND requested_attempted AND
       NOT requested_usage_known AND NOT requested_definitely_zero_usage AND
       requested_prompt_tokens=0 AND requested_completion_tokens=0 AND
       requested_prompt_cache_hit_tokens IS NULL AND
       requested_prompt_cache_miss_tokens IS NULL AND
       requested_reasoning_tokens IS NULL AND requested_completion='' AND
       requested_error='gateway recovery: provider outcome unavailable' AND
       requested_error_code='LLM_UNAVAILABLE' THEN
        IF to_timestamp(requested_signed_at_unix_ms::numeric/1000) <
               clock_timestamp()-interval '5 minutes' THEN
            RAISE EXCEPTION '094: gateway recovery receipt is stale' USING ERRCODE='42501';
        END IF;
    ELSIF requested_outcome='failed' AND NOT requested_attempted AND
       NOT requested_usage_known AND requested_definitely_zero_usage AND
       requested_prompt_tokens=0 AND requested_completion_tokens=0 AND
       requested_completion='' AND
       requested_error='gateway recovery: pre-send rejection' AND
       requested_error_code='LLM_BAD_REQUEST' THEN
        IF to_timestamp(requested_signed_at_unix_ms::numeric/1000) <
               clock_timestamp()-interval '5 minutes' THEN
            RAISE EXCEPTION '094: gateway rejection recovery receipt is stale'
                USING ERRCODE='42501';
        END IF;
    ELSIF to_timestamp(requested_signed_at_unix_ms::numeric/1000) <
              reservation_row.attempt_started_at-interval '5 seconds' OR
          to_timestamp(requested_signed_at_unix_ms::numeric/1000) >
              reservation_row.attempt_started_at+interval '10 minutes' THEN
        RAISE EXCEPTION '094: gateway receipt timestamp is outside provider attempt window'
            USING ERRCODE='42501';
    END IF;
    -- New Provider authority is checked before the immutable gateway attempt
    -- marker. Once send_started/pre_send_rejected exists, settlement is
    -- terminalization of that already-authorized effect, not a new effect.
    -- Requiring a still-live capability here would strand paid calls if the
    -- capability expires or is revoked between send and receipt commit.
    IF NOT EXISTS (
        SELECT 1 FROM public.research_llm_gateway_attempts attempt
         WHERE attempt.reservation_id=requested_reservation_id
           AND attempt.request_digest=requested_request_digest
           AND attempt.attempt_state=CASE WHEN requested_attempted
               THEN 'send_started' ELSE 'pre_send_rejected' END
    ) THEN
        RAISE EXCEPTION '094: receipt lacks matching durable gateway marker'
            USING ERRCODE='23514';
    END IF;
    SELECT secret INTO verifier_secret
      FROM public.research_llm_gateway_verifier_keys
     WHERE key_id=requested_key_id AND status IN ('active','retired')
       AND valid_from<=to_timestamp(requested_signed_at_unix_ms::numeric/1000)
       AND (valid_until IS NULL OR
            valid_until>to_timestamp(requested_signed_at_unix_ms::numeric/1000));
    IF verifier_secret IS NULL THEN
        RAISE EXCEPTION '094: gateway verifier key is unavailable' USING ERRCODE='42501';
    END IF;

    canonical_payload:=array_to_string(ARRAY[
        'vane.research-llm-gateway-receipt/v1',requested_key_id,
        requested_signed_at_unix_ms::text,requested_reservation_id::text,
        requested_request_digest,
        encode(sha256(convert_to(requested_system_prompt,'UTF8')),'hex'),
        encode(sha256(convert_to(requested_user_prompt,'UTF8')),'hex'),
        encode(sha256(convert_to(requested_completion,'UTF8')),'hex'),
        encode(sha256(convert_to(requested_provider,'UTF8')),'hex'),
        encode(sha256(convert_to(requested_provider_reported_model,'UTF8')),'hex'),
        requested_prompt_tokens::text,requested_completion_tokens::text,
        COALESCE(requested_prompt_cache_hit_tokens::text,'null'),
        COALESCE(requested_prompt_cache_miss_tokens::text,'null'),
        COALESCE(requested_reasoning_tokens::text,'null'),
        requested_latency_ms::text,COALESCE(requested_prefix_cache_hit::text,'null'),
        requested_disable_thinking::text,
        encode(sha256(convert_to(requested_error,'UTF8')),'hex'),
        requested_attempted::text,requested_usage_known::text,
        requested_definitely_zero_usage::text,requested_outcome,
        encode(sha256(convert_to(requested_error_code,'UTF8')),'hex')
    ],E'\n');
    expected_signature:=hmac(convert_to(canonical_payload,'UTF8'),verifier_secret,'sha256');
    IF NOT public.research_llm_gateway_constant_time_equal_v1(
            expected_signature,requested_signature) THEN
        RAISE EXCEPTION '094: gateway receipt signature is invalid' USING ERRCODE='42501';
    END IF;
    payload_digest:=encode(sha256(convert_to(canonical_payload,'UTF8')),'hex');

    PERFORM set_config('app.research_llm_gateway_verified','1',true);
    PERFORM set_config('app.research_llm_gateway_key_id',requested_key_id,true);
    PERFORM set_config('app.research_llm_gateway_signed_at_ms',
                       requested_signed_at_unix_ms::text,true);
    PERFORM set_config('app.research_llm_gateway_payload_digest',payload_digest,true);
    PERFORM set_config('app.research_llm_gateway_signature_hex',
                       encode(requested_signature,'hex'),true);
    RETURN QUERY SELECT * FROM public.settle_research_run_llm_spend_v3(
        requested_tenant_id,requested_user_id,requested_task_id,
        requested_run_snapshot_id,requested_reservation_id,
        requested_system_prompt,requested_user_prompt,requested_completion,
        requested_provider_reported_model,requested_prompt_tokens,
        requested_completion_tokens,requested_prompt_cache_hit_tokens,
        requested_prompt_cache_miss_tokens,requested_reasoning_tokens,
        requested_latency_ms,requested_prefix_cache_hit,requested_temperature,
        requested_max_tokens,requested_disable_thinking,requested_error,
        requested_attempted,requested_usage_known,requested_definitely_zero_usage,
        requested_outcome,requested_error_code);
    PERFORM set_config('app.research_llm_gateway_verified','',true);
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION settle_signed_research_run_llm_spend_v3(
    BIGINT,BIGINT,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,
    INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,BOOLEAN,REAL,INTEGER,
    BOOLEAN,TEXT,BOOLEAN,BOOLEAN,BOOLEAN,TEXT,TEXT,TEXT,BIGINT,BYTEA
) FROM PUBLIC,vane_app,vane_research_v3_executor;
GRANT EXECUTE ON FUNCTION settle_signed_research_run_llm_spend_v3(
    BIGINT,BIGINT,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,
    INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,BOOLEAN,REAL,INTEGER,
    BOOLEAN,TEXT,BOOLEAN,BOOLEAN,BOOLEAN,TEXT,TEXT,TEXT,BIGINT,BYTEA
) TO vane_research_llm_gateway;

-- Additional artifact fence: 092's exact JSON equality remains in force and
-- 094 now also requires that the underlying settlement came from the gateway.
-- +goose StatementBegin
CREATE FUNCTION enforce_research_artifact_verified_gateway_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE bound_reservation_id BIGINT;
BEGIN
    IF TG_TABLE_NAME='research_run_plans' THEN
        bound_reservation_id:=NEW.planner_llm_spend_reservation_id;
    ELSIF NEW.status='finalized' THEN
        bound_reservation_id:=NEW.synthesis_llm_spend_reservation_id;
    ELSE
        RETURN NEW;
    END IF;
    IF bound_reservation_id IS NULL OR NOT EXISTS (
        SELECT 1 FROM public.research_run_llm_spend_settlements settlement
         WHERE settlement.reservation_id=bound_reservation_id
           AND settlement.receipt_provenance='verified_gateway'
    ) THEN
        RAISE EXCEPTION '094: research artifact requires verified gateway provenance'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_artifact_verified_gateway_v1()
    FROM PUBLIC,vane_app,vane_research_v3_executor,vane_research_llm_gateway;
CREATE TRIGGER research_run_plan_verified_gateway_v1
BEFORE INSERT OR UPDATE ON research_run_plans
FOR EACH ROW EXECUTE FUNCTION enforce_research_artifact_verified_gateway_v1();
CREATE TRIGGER research_brief_verified_gateway_v1
BEFORE INSERT OR UPDATE ON research_brief_syntheses
FOR EACH ROW EXECUTE FUNCTION enforce_research_artifact_verified_gateway_v1();

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE research_run_llm_spend_settlements,llm_calls,
           research_run_plans,research_brief_syntheses,
           research_llm_gateway_attempts,
           research_llm_gateway_verifier_keys IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM research_run_llm_spend_settlements
                WHERE receipt_provenance='verified_gateway') OR
       EXISTS (SELECT 1 FROM research_llm_gateway_attempts) THEN
        RAISE EXCEPTION '094: refusing downgrade while gateway attempts or receipts exist';
    END IF;
END $$;
-- +goose StatementEnd

DROP TRIGGER research_brief_verified_gateway_v1 ON research_brief_syntheses;
DROP TRIGGER research_run_plan_verified_gateway_v1 ON research_run_plans;
DROP FUNCTION enforce_research_artifact_verified_gateway_v1();
REVOKE ALL ON FUNCTION settle_signed_research_run_llm_spend_v3(
    BIGINT,BIGINT,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,
    INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,BOOLEAN,REAL,INTEGER,
    BOOLEAN,TEXT,BOOLEAN,BOOLEAN,BOOLEAN,TEXT,TEXT,TEXT,BIGINT,BYTEA
) FROM vane_research_llm_gateway;
DROP FUNCTION settle_signed_research_run_llm_spend_v3(
    BIGINT,BIGINT,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,
    INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,BOOLEAN,REAL,INTEGER,
    BOOLEAN,TEXT,BOOLEAN,BOOLEAN,BOOLEAN,TEXT,TEXT,TEXT,BIGINT,BYTEA);
DROP TRIGGER stamp_verified_research_llm_gateway_receipt_v1
    ON research_run_llm_spend_settlements;
DROP FUNCTION stamp_verified_research_llm_gateway_receipt_v1();
DROP FUNCTION research_llm_gateway_constant_time_equal_v1(BYTEA,BYTEA);
REVOKE ALL ON FUNCTION sign_research_llm_gateway_payload_v1(TEXT,BYTEA,BIGINT,TEXT,BOOLEAN)
    FROM vane_research_llm_gateway;
DROP FUNCTION sign_research_llm_gateway_payload_v1(TEXT,BYTEA,BIGINT,TEXT,BOOLEAN);
REVOKE ALL ON FUNCTION active_research_llm_gateway_key_id_v1()
    FROM vane_research_llm_gateway;
DROP FUNCTION active_research_llm_gateway_key_id_v1();
REVOKE ALL ON FUNCTION mark_research_llm_gateway_send_started_v1(
    BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,REAL,INTEGER,BOOLEAN)
    FROM vane_research_llm_gateway;
DROP FUNCTION mark_research_llm_gateway_send_started_v1(
    BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,REAL,INTEGER,BOOLEAN);
REVOKE ALL ON FUNCTION mark_research_llm_gateway_pre_send_rejected_v1(
    BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,REAL,INTEGER,BOOLEAN)
    FROM vane_research_llm_gateway;
DROP FUNCTION mark_research_llm_gateway_pre_send_rejected_v1(
    BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,REAL,INTEGER,BOOLEAN);
DROP FUNCTION record_research_llm_gateway_attempt_v1(
    BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,REAL,INTEGER,BOOLEAN,TEXT);
REVOKE ALL ON FUNCTION load_research_llm_gateway_recovery_intent_v1(BIGINT,TEXT,TEXT)
    FROM vane_research_llm_gateway;
DROP FUNCTION load_research_llm_gateway_recovery_intent_v1(BIGINT,TEXT,TEXT);
REVOKE ALL ON FUNCTION load_research_run_bound_llm_call_v1(BIGINT,BIGINT)
    FROM vane_research_v3_executor;
DROP FUNCTION load_research_run_bound_llm_call_v1(BIGINT,BIGINT);
REVOKE ALL ON FUNCTION research_llm_gateway_attempt_started_v1(BIGINT,TEXT)
    FROM vane_research_llm_gateway;
DROP FUNCTION research_llm_gateway_attempt_started_v1(BIGINT,TEXT);
DROP TABLE research_llm_gateway_attempts;
ALTER TABLE research_run_llm_spend_settlements
    DROP CONSTRAINT ck_research_llm_spend_gateway_provenance,
    DROP COLUMN gateway_signature,
    DROP COLUMN gateway_payload_digest,
    DROP COLUMN gateway_signed_at_unix_ms,
    DROP COLUMN gateway_key_id,
    DROP COLUMN receipt_provenance;
DROP TABLE research_llm_gateway_verifier_keys;
REVOKE EXECUTE ON FUNCTION require_research_run_capability_v1(
    BIGINT,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT
) FROM vane_research_llm_gateway;
GRANT EXECUTE ON FUNCTION settle_research_run_llm_spend_v3(
    BIGINT,BIGINT,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT,TEXT,INTEGER,INTEGER,
    INTEGER,INTEGER,INTEGER,INTEGER,BOOLEAN,REAL,INTEGER,BOOLEAN,TEXT,
    BOOLEAN,BOOLEAN,BOOLEAN,TEXT,TEXT
) TO vane_research_v3_executor;
GRANT SELECT (
    id,trace_id,span_name,user_id,ref_type,ref_id,provider,model,system_prompt,
    user_prompt,completion,prompt_tokens,completion_tokens,latency_ms,cost_usd,
    prefix_cache_hit,temperature,max_tokens,disable_thinking,error,tenant_id,
    prompt_cache_hit_tokens,prompt_cache_miss_tokens,reasoning_tokens,
    pricing_rule_id,pricing_status,cost_amount,cost_currency,run_snapshot_id,
    research_run_llm_spend_reservation_id,created_at
) ON llm_calls TO vane_research_v3_executor;
