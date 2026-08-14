-- 119: bind a canonical Research Brief to the exact representation-only
-- normalization accepted by research-synthesis.render/v3.1+.
--
-- llm_calls.completion remains the immutable provider response. A model may
-- wrap one otherwise-valid JSON object in a single ```json fence; the Go
-- decoder removes only that wrapper before producing the canonical Brief.
-- Migration 092 cast the raw provider response directly to jsonb, so a valid
-- fenced response raised 22P02 during finalization and was misreported as a
-- database failure. Keep the raw completion and compare through a strict,
-- fail-closed projection instead.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE research_brief_syntheses,research_run_llm_spend_reservations,
           research_run_llm_spend_settlements,llm_calls
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
CREATE FUNCTION research_brief_matches_synthesis_completion_v119(
    brief_payload BYTEA,
    synthesis_completion TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
STRICT
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    brief_text TEXT;
    normalized TEXT;
    remainder TEXT;
BEGIN
    brief_text := convert_from(brief_payload,'UTF8');
    normalized := btrim(synthesis_completion,E' \t\r\n\v\f');
    IF octet_length(normalized)<2 OR octet_length(normalized)>262144 THEN
        RETURN false;
    END IF;

    IF left(normalized,3)='```' THEN
        IF left(normalized,9)=E'```json\r\n' THEN
            remainder := substring(normalized FROM 10);
        ELSIF left(normalized,8)=E'```json\n' THEN
            remainder := substring(normalized FROM 9);
        ELSE
            RETURN false;
        END IF;
        IF right(remainder,3)<>'```' THEN
            RETURN false;
        END IF;
        remainder := left(remainder,length(remainder)-3);
        IF right(remainder,2)=E'\r\n' THEN
            remainder := left(remainder,length(remainder)-2);
        ELSIF right(remainder,1)=E'\n' THEN
            remainder := left(remainder,length(remainder)-1);
        ELSE
            RETURN false;
        END IF;
        normalized := btrim(remainder,E' \t\r\n\v\f');
        IF octet_length(normalized)<2 OR position('```' IN normalized)>0 THEN
            RETURN false;
        END IF;
    END IF;

    IF NOT (brief_text IS JSON OBJECT WITH UNIQUE KEYS) OR
       NOT (normalized IS JSON OBJECT WITH UNIQUE KEYS) THEN
        RETURN false;
    END IF;
    RETURN brief_text::jsonb=normalized::jsonb;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION research_brief_matches_synthesis_completion_v119(
    BYTEA,TEXT) FROM PUBLIC;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION enforce_research_brief_llm_receipt_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.synthesis_llm_spend_reservation_id IS NOT NULL THEN
            RAISE EXCEPTION '092: prepared research Brief cannot pre-bind model spend'
                USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.synthesis_llm_spend_reservation_id IS NULL AND
       NEW.synthesis_llm_spend_reservation_id IS NOT NULL THEN
        IF OLD.status<>'prepared' OR NEW.status<>'spending' OR NOT EXISTS (
            SELECT 1
              FROM public.research_run_llm_spend_reservations reservation
             WHERE reservation.id=NEW.synthesis_llm_spend_reservation_id
               AND reservation.tenant_id=NEW.tenant_id
               AND reservation.user_id=NEW.user_id
               AND reservation.task_id=NEW.task_id
               AND reservation.run_snapshot_id=NEW.run_snapshot_id
               AND reservation.stage='synthesis'
               AND reservation.round_ordinal=0
               AND reservation.subject_id=NEW.id
        ) THEN
            RAISE EXCEPTION '092: research Brief spend binding differs from synthesis subject'
                USING ERRCODE='23514';
        END IF;
    ELSIF NEW.synthesis_llm_spend_reservation_id IS DISTINCT FROM
          OLD.synthesis_llm_spend_reservation_id THEN
        RAISE EXCEPTION '092: research Brief model spend binding is immutable'
            USING ERRCODE='23514';
    END IF;

    IF NEW.status='finalized' AND NOT EXISTS (
        SELECT 1
          FROM public.research_run_llm_spend_reservations reservation
          JOIN public.research_run_llm_spend_settlements settlement
            ON settlement.reservation_id=reservation.id
           AND settlement.tenant_id=reservation.tenant_id
           AND settlement.user_id=reservation.user_id
           AND settlement.task_id=reservation.task_id
           AND settlement.run_snapshot_id=reservation.run_snapshot_id
           AND settlement.stage=reservation.stage
           AND settlement.round_ordinal=reservation.round_ordinal
          JOIN public.llm_calls call ON call.id=settlement.llm_call_id
         WHERE reservation.id=NEW.synthesis_llm_spend_reservation_id
           AND reservation.tenant_id=NEW.tenant_id
           AND reservation.user_id=NEW.user_id
           AND reservation.task_id=NEW.task_id
           AND reservation.run_snapshot_id=NEW.run_snapshot_id
           AND reservation.stage='synthesis' AND reservation.round_ordinal=0
           AND reservation.subject_id=NEW.id
           AND settlement.attempted AND settlement.usage_known
           AND NOT settlement.definitely_zero_usage
           AND settlement.outcome='completed' AND settlement.error_code=''
           AND call.research_run_llm_spend_reservation_id=reservation.id
           AND call.tenant_id=NEW.tenant_id AND call.user_id=NEW.user_id
           AND call.run_snapshot_id=NEW.run_snapshot_id
           AND call.span_name='research_synthesis' AND call.error=''
           AND public.research_brief_matches_synthesis_completion_v119(
                   NEW.brief_payload,call.completion)
    ) THEN
        RAISE EXCEPTION '119: final research Brief differs from its synthesis response projection'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_brief_llm_receipt_v1() FROM PUBLIC;

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE research_brief_syntheses,research_run_llm_spend_reservations,
           research_run_llm_spend_settlements,llm_calls
    IN ACCESS EXCLUSIVE MODE;

-- Restoring the direct jsonb cast would make retained fenced provider receipts
-- unverifiable. Preserve the immutable response instead of deleting evidence.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM llm_calls
         WHERE span_name='research_synthesis' AND error=''
           AND NOT (completion IS JSON OBJECT WITH UNIQUE KEYS)
    ) THEN
        RAISE EXCEPTION
            '119: normalized research Brief receipts exist; restore from backup';
    END IF;
END
$$;
-- +goose StatementEnd

-- Restore migration 092 byte-for-byte receipt semantics for a deliberate
-- downgrade before any normalized completion has been retained.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION enforce_research_brief_llm_receipt_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.synthesis_llm_spend_reservation_id IS NOT NULL THEN
            RAISE EXCEPTION '092: prepared research Brief cannot pre-bind model spend'
                USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.synthesis_llm_spend_reservation_id IS NULL AND
       NEW.synthesis_llm_spend_reservation_id IS NOT NULL THEN
        IF OLD.status<>'prepared' OR NEW.status<>'spending' OR NOT EXISTS (
            SELECT 1
              FROM public.research_run_llm_spend_reservations reservation
             WHERE reservation.id=NEW.synthesis_llm_spend_reservation_id
               AND reservation.tenant_id=NEW.tenant_id
               AND reservation.user_id=NEW.user_id
               AND reservation.task_id=NEW.task_id
               AND reservation.run_snapshot_id=NEW.run_snapshot_id
               AND reservation.stage='synthesis'
               AND reservation.round_ordinal=0
               AND reservation.subject_id=NEW.id
        ) THEN
            RAISE EXCEPTION '092: research Brief spend binding differs from synthesis subject'
                USING ERRCODE='23514';
        END IF;
    ELSIF NEW.synthesis_llm_spend_reservation_id IS DISTINCT FROM
          OLD.synthesis_llm_spend_reservation_id THEN
        RAISE EXCEPTION '092: research Brief model spend binding is immutable'
            USING ERRCODE='23514';
    END IF;

    IF NEW.status='finalized' AND NOT EXISTS (
        SELECT 1
          FROM public.research_run_llm_spend_reservations reservation
          JOIN public.research_run_llm_spend_settlements settlement
            ON settlement.reservation_id=reservation.id
           AND settlement.tenant_id=reservation.tenant_id
           AND settlement.user_id=reservation.user_id
           AND settlement.task_id=reservation.task_id
           AND settlement.run_snapshot_id=reservation.run_snapshot_id
           AND settlement.stage=reservation.stage
           AND settlement.round_ordinal=reservation.round_ordinal
          JOIN public.llm_calls call ON call.id=settlement.llm_call_id
         WHERE reservation.id=NEW.synthesis_llm_spend_reservation_id
           AND reservation.tenant_id=NEW.tenant_id
           AND reservation.user_id=NEW.user_id
           AND reservation.task_id=NEW.task_id
           AND reservation.run_snapshot_id=NEW.run_snapshot_id
           AND reservation.stage='synthesis' AND reservation.round_ordinal=0
           AND reservation.subject_id=NEW.id
           AND settlement.attempted AND settlement.usage_known
           AND NOT settlement.definitely_zero_usage
           AND settlement.outcome='completed' AND settlement.error_code=''
           AND call.research_run_llm_spend_reservation_id=reservation.id
           AND call.tenant_id=NEW.tenant_id AND call.user_id=NEW.user_id
           AND call.run_snapshot_id=NEW.run_snapshot_id
           AND call.span_name='research_synthesis' AND call.error=''
           AND convert_from(NEW.brief_payload,'UTF8')::jsonb=call.completion::jsonb
    ) THEN
        RAISE EXCEPTION '092: final research Brief differs from its completed synthesis receipt'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd

DROP FUNCTION research_brief_matches_synthesis_completion_v119(BYTEA,TEXT);
