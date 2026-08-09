-- 125: one immutable grounding correction and one final re-verification for
-- scoped v3.6 synthesis. Retained v3.3-v3.5 histories keep their exact
-- round-0/round-1 admission and finalization semantics.
-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE task_run_snapshots,research_brief_syntheses,
           research_brief_grounding_verifications,
           research_run_llm_spend_reservations,
           research_run_llm_spend_settlements,llm_calls
    IN ACCESS EXCLUSIVE MODE;

CREATE TABLE research_brief_grounding_corrections (
    id                                 BIGSERIAL PRIMARY KEY,
    tenant_id                          BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id                            BIGINT NOT NULL REFERENCES users(id),
    task_id                            TEXT NOT NULL,
    run_snapshot_id                    BIGINT NOT NULL REFERENCES task_run_snapshots(id),
    plan_id                            BIGINT NOT NULL REFERENCES research_run_plans(id),
    synthesis_id                       BIGINT NOT NULL REFERENCES research_brief_syntheses(id),
    grounding_verification_id          BIGINT NOT NULL REFERENCES research_brief_grounding_verifications(id),
    correction_prompt                  BYTEA NOT NULL,
    correction_prompt_digest           TEXT NOT NULL,
    corrector_llm_spend_reservation_id BIGINT REFERENCES research_run_llm_spend_reservations(id),
    corrected_brief_payload            BYTEA,
    corrected_brief_digest             TEXT,
    verifier_prompt                    BYTEA,
    verifier_prompt_digest             TEXT,
    verifier_llm_spend_reservation_id  BIGINT REFERENCES research_run_llm_spend_reservations(id),
    status                             TEXT NOT NULL DEFAULT 'prepared',
    verdict_payload                    BYTEA,
    verdict_digest                     TEXT,
    schema_version                     TEXT NOT NULL,
    created_at                         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    corrected_at                       TIMESTAMPTZ,
    finalized_at                       TIMESTAMPTZ,

    CONSTRAINT uq_research_grounding_correction_snapshot UNIQUE(run_snapshot_id),
    CONSTRAINT uq_research_grounding_correction_plan UNIQUE(plan_id),
    CONSTRAINT uq_research_grounding_correction_synthesis UNIQUE(synthesis_id),
    CONSTRAINT uq_research_grounding_correction_grounding UNIQUE(grounding_verification_id),
    CONSTRAINT uq_research_grounding_correction_corrector_reservation
        UNIQUE(corrector_llm_spend_reservation_id),
    CONSTRAINT uq_research_grounding_correction_verifier_reservation
        UNIQUE(verifier_llm_spend_reservation_id),
    CONSTRAINT ck_research_grounding_correction_identity CHECK (
        btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255
    ),
    CONSTRAINT ck_research_grounding_correction_digests CHECK (
        correction_prompt_digest ~ '^[0-9a-f]{64}$' AND
        (corrected_brief_digest IS NULL OR corrected_brief_digest ~ '^[0-9a-f]{64}$') AND
        (verifier_prompt_digest IS NULL OR verifier_prompt_digest ~ '^[0-9a-f]{64}$') AND
        (verdict_digest IS NULL OR verdict_digest ~ '^[0-9a-f]{64}$') AND
        correction_prompt_digest=encode(sha256(correction_prompt),'hex') AND
        (corrected_brief_payload IS NULL OR
         corrected_brief_digest=encode(sha256(corrected_brief_payload),'hex')) AND
        (verifier_prompt IS NULL OR
         verifier_prompt_digest=encode(sha256(verifier_prompt),'hex')) AND
        (verdict_payload IS NULL OR
         verdict_digest=encode(sha256(verdict_payload),'hex'))
    ),
    CONSTRAINT ck_research_grounding_correction_payloads CHECK (
        octet_length(correction_prompt) BETWEEN 2 AND 2097152 AND
        position(decode('00','hex') in correction_prompt)=0 AND
        (corrected_brief_payload IS NULL OR (
            octet_length(corrected_brief_payload) BETWEEN 2 AND 262144 AND
            position(decode('00','hex') in corrected_brief_payload)=0
        )) AND
        (verifier_prompt IS NULL OR (
            octet_length(verifier_prompt) BETWEEN 2 AND 2097152 AND
            position(decode('00','hex') in verifier_prompt)=0
        )) AND
        (verdict_payload IS NULL OR (
            octet_length(verdict_payload) BETWEEN 2 AND 65536 AND
            position(decode('00','hex') in verdict_payload)=0
        ))
    ),
    CONSTRAINT ck_research_grounding_correction_status CHECK (
        status IN ('prepared','corrected','grounded','rejected')
    ),
    CONSTRAINT ck_research_grounding_correction_shape CHECK (
        (status='prepared' AND corrector_llm_spend_reservation_id IS NULL AND
         corrected_brief_payload IS NULL AND corrected_brief_digest IS NULL AND
         verifier_prompt IS NULL AND verifier_prompt_digest IS NULL AND
         verifier_llm_spend_reservation_id IS NULL AND verdict_payload IS NULL AND
         verdict_digest IS NULL AND corrected_at IS NULL AND finalized_at IS NULL) OR
        (status='corrected' AND corrector_llm_spend_reservation_id IS NOT NULL AND
         corrected_brief_payload IS NOT NULL AND corrected_brief_digest IS NOT NULL AND
         verifier_prompt IS NOT NULL AND verifier_prompt_digest IS NOT NULL AND
         verifier_llm_spend_reservation_id IS NULL AND verdict_payload IS NULL AND
         verdict_digest IS NULL AND corrected_at IS NOT NULL AND finalized_at IS NULL) OR
        (status IN ('grounded','rejected') AND
         corrector_llm_spend_reservation_id IS NOT NULL AND
         corrected_brief_payload IS NOT NULL AND corrected_brief_digest IS NOT NULL AND
         verifier_prompt IS NOT NULL AND verifier_prompt_digest IS NOT NULL AND
         verifier_llm_spend_reservation_id IS NOT NULL AND verdict_payload IS NOT NULL AND
         verdict_digest IS NOT NULL AND corrected_at IS NOT NULL AND finalized_at IS NOT NULL)
    ),
    CONSTRAINT ck_research_grounding_correction_schema CHECK (
        schema_version='vane.research-grounding-correction/v1'
    )
);

CREATE INDEX idx_research_grounding_correction_scope
    ON research_brief_grounding_corrections
       (tenant_id,user_id,task_id,run_snapshot_id,id);

-- The corrected candidate may delete citations but can never create authority
-- that the original synthesis candidate did not cite.
-- +goose StatementBegin
CREATE FUNCTION research_grounding_correction_citations_subset_v125(
    original_payload BYTEA,
    corrected_payload BYTEA
) RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
STRICT
SET search_path=pg_catalog,public,pg_temp
AS $$
WITH original AS (
    SELECT convert_from(original_payload,'UTF8')::jsonb->'citations' AS citations
), corrected AS (
    SELECT convert_from(corrected_payload,'UTF8')::jsonb->'citations' AS citations
)
SELECT jsonb_typeof(original.citations)='array' AND
       jsonb_typeof(corrected.citations)='array' AND
       NOT EXISTS (
           SELECT 1
             FROM jsonb_array_elements(corrected.citations) citation
            WHERE NOT EXISTS (
                SELECT 1
                  FROM jsonb_array_elements(original.citations) allowed
                 WHERE allowed=citation
            )
       )
  FROM original,corrected
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION research_grounding_correction_citations_subset_v125(
    BYTEA,BYTEA) FROM PUBLIC;

-- v3.6 retains the frozen v3.2 representation repair for positive decimal
-- current_evidence refs emitted as JSON numbers. All other completion and
-- duplicate-key rules remain the v119 fail-closed projection.
-- +goose StatementBegin
CREATE FUNCTION research_brief_matches_completion_v125(
    brief_payload BYTEA,
    provider_completion TEXT
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
    completion_json JSONB;
    normalized_citations JSONB;
BEGIN
    IF public.research_brief_matches_synthesis_completion_v119(
            brief_payload,provider_completion) THEN
        RETURN true;
    END IF;
    brief_text := convert_from(brief_payload,'UTF8');
    normalized := btrim(provider_completion,E' \t\r\n\v\f');
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
        IF right(remainder,3)<>'```' THEN RETURN false; END IF;
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
    completion_json := normalized::jsonb;
    IF jsonb_typeof(completion_json->'citations') IS DISTINCT FROM 'array' THEN
        RETURN false;
    END IF;
    SELECT coalesce(jsonb_agg(CASE
        WHEN citation->>'kind'='current_evidence' AND
             jsonb_typeof(citation->'ref')='number' AND
             citation->>'ref' ~ '^[1-9][0-9]*$'
        THEN jsonb_set(citation,'{ref}',to_jsonb(citation->>'ref'))
        ELSE citation END ORDER BY ordinal),'[]'::jsonb)
      INTO normalized_citations
      FROM jsonb_array_elements(completion_json->'citations')
           WITH ORDINALITY item(citation,ordinal);
    completion_json := jsonb_set(completion_json,'{citations}',normalized_citations);
    RETURN brief_text::jsonb=completion_json;
EXCEPTION WHEN OTHERS THEN
    RETURN false;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION research_brief_matches_completion_v125(BYTEA,TEXT)
    FROM PUBLIC;

-- Mirror the frozen v3.6 Brief contract before any verifier round can spend.
-- The candidate must also cite only records present in the exact synthesis
-- context and follow the complete/partial coverage schema selected by Store.
-- +goose StatementBegin
CREATE FUNCTION research_brief_candidate_valid_v125(
    synthesis_id BIGINT,
    candidate_payload BYTEA
) RETURNS BOOLEAN
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    brief_json JSONB;
    context_json JSONB;
    failure_count INTEGER;
BEGIN
    IF NOT (convert_from(candidate_payload,'UTF8')
                IS JSON OBJECT WITH UNIQUE KEYS) THEN
        RETURN false;
    END IF;
    brief_json := convert_from(candidate_payload,'UTF8')::jsonb;
    SELECT convert_from(brief.context_payload,'UTF8')::jsonb
      INTO context_json
      FROM public.research_brief_syntheses brief
     WHERE brief.id=synthesis_id;
    IF context_json IS NULL OR jsonb_typeof(brief_json) IS DISTINCT FROM 'object' OR
       jsonb_typeof(brief_json->'headline') IS DISTINCT FROM 'string' OR
       octet_length(brief_json->>'headline') NOT BETWEEN 1 AND 1024 OR
       btrim(brief_json->>'headline') IS DISTINCT FROM brief_json->>'headline' OR
       jsonb_typeof(brief_json->'summary') IS DISTINCT FROM 'string' OR
       octet_length(brief_json->>'summary') NOT BETWEEN 1 AND 65536 OR
       btrim(brief_json->>'summary') IS DISTINCT FROM brief_json->>'summary' OR
       jsonb_typeof(brief_json->'significance') IS DISTINCT FROM 'string' OR
       brief_json->>'significance' NOT IN ('none','qualified','major') OR
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
               OR (citation->>'kind'='current_evidence' AND (
                   citation->>'ref' !~ '^[1-9][0-9]*$' OR NOT EXISTS (
                       SELECT 1 FROM jsonb_array_elements(
                           context_json->'current_evidence') item
                        WHERE item->>'evidence_id'=citation->>'ref'
                   )))
               OR (citation->>'kind'='history' AND NOT EXISTS (
                   SELECT 1 FROM jsonb_array_elements(
                       context_json#>'{history,items}') item
                    WHERE item->>'record_id'=citation->>'ref'
               ))
               OR citation->>'kind' NOT IN ('current_evidence','history')
       ) OR EXISTS (
           SELECT 1 FROM jsonb_array_elements(brief_json->'citations') citation
            GROUP BY citation->>'kind',citation->>'ref' HAVING count(*)>1
       ) THEN
        RETURN false;
    END IF;
    failure_count := CASE
        WHEN jsonb_typeof(context_json->'tool_failures')='array'
        THEN jsonb_array_length(context_json->'tool_failures') ELSE 0 END;
    IF failure_count=0 THEN
        RETURN (SELECT count(*) FROM jsonb_object_keys(brief_json))=5 AND
            (brief_json-ARRAY['schema_version','headline','summary',
                              'significance','citations'])='{}'::jsonb AND
            brief_json->>'schema_version'='vane.research-brief/v3' AND
            jsonb_array_length(brief_json->'citations') BETWEEN 1 AND 64 AND
            EXISTS (SELECT 1 FROM jsonb_array_elements(brief_json->'citations') citation
                     WHERE citation->>'kind'='current_evidence');
    END IF;
    IF brief_json->>'schema_version'='vane.research-brief/v3.1' THEN
        RETURN (SELECT count(*) FROM jsonb_object_keys(brief_json))=6 AND
            (brief_json-ARRAY['schema_version','assessment','headline','summary',
                              'significance','citations'])='{}'::jsonb AND
            brief_json->>'assessment'='unknown' AND
            brief_json->>'significance'='none';
    END IF;
    IF brief_json->>'schema_version'='vane.research-brief/v3.2' THEN
        RETURN (SELECT count(*) FROM jsonb_object_keys(brief_json))=6 AND
            (brief_json-ARRAY['schema_version','assessment','headline','summary',
                              'significance','citations'])='{}'::jsonb AND
            brief_json->>'assessment'='grounded' AND
            brief_json->>'significance'='none' AND
            jsonb_array_length(brief_json->'citations') BETWEEN 1 AND 64 AND
            EXISTS (
                SELECT 1
                  FROM jsonb_array_elements(brief_json->'citations') citation
                  JOIN jsonb_array_elements(context_json->'current_evidence') item
                    ON citation->>'kind'='current_evidence'
                   AND citation->>'ref'=item->>'evidence_id'
                 WHERE item->>'trust_type'='official'
                   AND item->>'tool_name'='web_product_status'
            );
    END IF;
    RETURN false;
EXCEPTION WHEN OTHERS THEN
    RETURN false;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION research_brief_candidate_valid_v125(BIGINT,BYTEA)
    FROM PUBLIC;

-- Mirror NormalizeResearchGroundingVerdictV1 plus the Store's citation-bound
-- issue check before a rejected verdict may authorize a paid correction.
-- +goose StatementBegin
CREATE FUNCTION research_grounding_verdict_valid_v125(
    verdict_payload BYTEA,
    candidate_payload BYTEA,
    expected_verdict TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
STRICT
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    verdict_json JSONB;
    candidate_json JSONB;
BEGIN
    IF expected_verdict NOT IN ('grounded','unsupported') OR
       NOT (convert_from(verdict_payload,'UTF8')
                IS JSON OBJECT WITH UNIQUE KEYS) THEN
        RETURN false;
    END IF;
    verdict_json := convert_from(verdict_payload,'UTF8')::jsonb;
    candidate_json := convert_from(candidate_payload,'UTF8')::jsonb;
    IF (SELECT count(*) FROM jsonb_object_keys(verdict_json))<>4 OR
       (verdict_json-ARRAY['schema_version','candidate_digest','verdict','issues'])<>
           '{}'::jsonb OR
       verdict_json->>'schema_version'<>'vane.research-grounding-verdict/v1' OR
       verdict_json->>'candidate_digest'<>
           encode(sha256(candidate_payload),'hex') OR
       verdict_json->>'verdict'<>expected_verdict OR
       jsonb_typeof(verdict_json->'issues') IS DISTINCT FROM 'array' OR
       jsonb_array_length(verdict_json->'issues')>16 OR
       ((expected_verdict='grounded') IS DISTINCT FROM
            (jsonb_array_length(verdict_json->'issues')=0)) OR EXISTS (
           SELECT 1 FROM jsonb_array_elements(verdict_json->'issues') issue
            WHERE jsonb_typeof(issue) IS DISTINCT FROM 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(issue))<>4
               OR (issue-ARRAY['field','claim','refs','reason'])<>'{}'::jsonb
               OR jsonb_typeof(issue->'field') IS DISTINCT FROM 'string'
               OR issue->>'field' NOT IN ('headline','summary','significance')
               OR jsonb_typeof(issue->'claim') IS DISTINCT FROM 'string'
               OR octet_length(issue->>'claim') NOT BETWEEN 1 AND 4096
               OR btrim(issue->>'claim') IS DISTINCT FROM issue->>'claim'
               OR jsonb_typeof(issue->'reason') IS DISTINCT FROM 'string'
               OR octet_length(issue->>'reason') NOT BETWEEN 1 AND 4096
               OR btrim(issue->>'reason') IS DISTINCT FROM issue->>'reason'
               OR jsonb_typeof(issue->'refs') IS DISTINCT FROM 'array'
               OR jsonb_array_length(issue->'refs')>64
               OR EXISTS (
                   SELECT 1 FROM jsonb_array_elements(issue->'refs') ref
                    WHERE jsonb_typeof(ref) IS DISTINCT FROM 'object'
                       OR (SELECT count(*) FROM jsonb_object_keys(ref))<>2
                       OR (ref-ARRAY['kind','ref'])<>'{}'::jsonb
                       OR jsonb_typeof(ref->'kind') IS DISTINCT FROM 'string'
                       OR ref->>'kind' NOT IN ('current_evidence','history')
                       OR jsonb_typeof(ref->'ref') IS DISTINCT FROM 'string'
                       OR octet_length(ref->>'ref') NOT BETWEEN 1 AND 255
                       OR btrim(ref->>'ref') IS DISTINCT FROM ref->>'ref'
                       OR NOT EXISTS (
                           SELECT 1 FROM jsonb_array_elements(
                               candidate_json->'citations') citation
                            WHERE citation=ref
                       )
               ) OR EXISTS (
                   SELECT 1 FROM jsonb_array_elements(issue->'refs') ref
                    GROUP BY ref->>'kind',ref->>'ref' HAVING count(*)>1
               )
       ) THEN
        RETURN false;
    END IF;
    RETURN true;
EXCEPTION WHEN OTHERS THEN
    RETURN false;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION research_grounding_verdict_valid_v125(
    BYTEA,BYTEA,TEXT) FROM PUBLIC;

-- Rebuild the complete v1.2 grounding-verifier input from the frozen
-- synthesis context and the exact candidate. JSONB equality intentionally
-- ignores representation-only key order/whitespace while rejecting every
-- semantic change, including evidence or response-contract substitutions.
-- +goose StatementBegin
CREATE FUNCTION research_expected_grounding_verifier_prompt_v125(
    synthesis_id BIGINT,
    candidate_payload BYTEA,
    candidate_digest TEXT
) RETURNS JSONB
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
WITH source AS (
    SELECT convert_from(brief.context_payload,'UTF8')::jsonb AS context_json,
           convert_from(candidate_payload,'UTF8')::jsonb AS candidate_json
      FROM public.research_brief_syntheses brief
     WHERE brief.id=synthesis_id
       AND public.research_brief_candidate_valid_v125(
               brief.id,candidate_payload)
), cited AS (
    SELECT coalesce(jsonb_agg(
        CASE citation->>'kind'
        WHEN 'current_evidence' THEN (
            SELECT jsonb_strip_nulls(jsonb_build_object(
                'kind','current_evidence','ref',citation->>'ref',
                'tool_name',nullif(item->>'tool_name',''),
                'trust_type',nullif(item->>'trust_type',''),
                'synthesis_visible_text',nullif(item->>'synthesis_visible_text',''),
                'context_truncated',item->'context_truncated'))
              FROM jsonb_array_elements(source.context_json->'current_evidence') item
             WHERE item->>'evidence_id'=citation->>'ref'
        )
        WHEN 'history' THEN (
            SELECT jsonb_strip_nulls(jsonb_build_object(
                'kind','history','ref',citation->>'ref',
                'generated_at',nullif(item->>'generated_at',''),
                'coverage',nullif(item->>'coverage',''),
                'payload_text',nullif(item->>'payload_text',''),
                'context_truncated',item->'context_truncated'))
              FROM jsonb_array_elements(source.context_json#>'{history,items}') item
             WHERE item->>'record_id'=citation->>'ref'
        )
        ELSE NULL END ORDER BY ordinal)
            FILTER (WHERE citation IS NOT NULL), '[]'::jsonb) AS items,
        source.context_json,source.candidate_json
      FROM source
      LEFT JOIN LATERAL jsonb_array_elements(source.candidate_json->'citations')
           WITH ORDINALITY AS candidate(citation,ordinal) ON true
     GROUP BY source.context_json,source.candidate_json
)
SELECT jsonb_build_object(
    'schema_version','vane.research-grounding-check-input/v1.1',
    'candidate_digest',candidate_digest,
    'task_manual',cited.context_json#>>'{definition,task_manual}',
    'history_through_utc',cited.context_json#>>'{history,history_through_utc}',
    'candidate_brief',cited.candidate_json,
    'cited_evidence',cited.items,
    'tool_failures',coalesce(cited.context_json->'tool_failures','null'::jsonb),
    'response_contract',jsonb_build_object(
        'schema_version_literal','vane.research-grounding-verdict/v1',
        'required_top_level_fields',
            '["schema_version","candidate_digest","verdict","issues"]'::jsonb,
        'verdict_values','["grounded","unsupported"]'::jsonb,
        'grounded_issues_rule','issues must be []',
        'unsupported_issues_rule','issues must contain every unsupported claim',
        'issue_fields','["field","claim","refs","reason"]'::jsonb,
        'issue_refs_item_fields','["kind","ref"]'::jsonb,
        'issue_refs_kind_values','["current_evidence","history"]'::jsonb,
        'issue_refs_rule','refs must be a JSON array of citation objects copied exactly from candidate_brief.citations; each object must contain only kind and ref, never a bare string; use [] when no candidate citation supports the claim',
        'single_canonical_json',true
    )
)
  FROM cited
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION research_expected_grounding_verifier_prompt_v125(
    BIGINT,BYTEA,TEXT) FROM PUBLIC;

-- v3.6 cannot buy or retain a grounding verdict for an arbitrary candidate or
-- verifier prompt. Bind INSERT to the completed round-0 synthesis receipt and
-- the exact semantic projection of the frozen synthesis context.
-- +goose StatementBegin
CREATE FUNCTION enforce_research_grounding_insert_v125()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    renderer TEXT;
BEGIN
    SELECT convert_from(snapshot.payload,'UTF8')::jsonb
               #>> '{research_model,synthesis,renderer_version}'
      INTO renderer
      FROM public.task_run_snapshots snapshot
     WHERE snapshot.id=NEW.run_snapshot_id
       AND snapshot.tenant_id=NEW.tenant_id
       AND snapshot.user_id=NEW.user_id
       AND snapshot.task_id=NEW.task_id;
    IF renderer IS DISTINCT FROM 'research-synthesis.render/v3.6' THEN
        RETURN NEW;
    END IF;
    IF NOT public.research_brief_candidate_valid_v125(
               NEW.synthesis_id,NEW.candidate_brief_payload) OR
       NOT (convert_from(NEW.verifier_prompt,'UTF8')
                IS JSON OBJECT WITH UNIQUE KEYS) OR
       convert_from(NEW.verifier_prompt,'UTF8')::jsonb IS DISTINCT FROM
           public.research_expected_grounding_verifier_prompt_v125(
               NEW.synthesis_id,NEW.candidate_brief_payload,
               NEW.candidate_digest) OR NOT EXISTS (
        SELECT 1
          FROM public.research_brief_syntheses brief
          JOIN public.research_run_llm_spend_reservations reservation
            ON reservation.id=brief.synthesis_llm_spend_reservation_id
           AND reservation.tenant_id=brief.tenant_id
           AND reservation.user_id=brief.user_id
           AND reservation.task_id=brief.task_id
           AND reservation.run_snapshot_id=brief.run_snapshot_id
          JOIN public.research_run_llm_spend_settlements settlement
            ON settlement.reservation_id=reservation.id
           AND settlement.tenant_id=reservation.tenant_id
           AND settlement.user_id=reservation.user_id
           AND settlement.task_id=reservation.task_id
           AND settlement.run_snapshot_id=reservation.run_snapshot_id
           AND settlement.stage=reservation.stage
           AND settlement.round_ordinal=reservation.round_ordinal
          JOIN public.llm_calls call ON call.id=settlement.llm_call_id
         WHERE brief.id=NEW.synthesis_id
           AND brief.tenant_id=NEW.tenant_id
           AND brief.user_id=NEW.user_id
           AND brief.task_id=NEW.task_id
           AND brief.run_snapshot_id=NEW.run_snapshot_id
           AND brief.plan_id=NEW.plan_id
           AND brief.status='spending'
           AND reservation.stage='synthesis' AND reservation.round_ordinal=0
           AND reservation.subject_id=brief.id
           AND settlement.attempted AND settlement.usage_known
           AND NOT settlement.definitely_zero_usage
           AND settlement.outcome='completed' AND settlement.error_code=''
           AND call.research_run_llm_spend_reservation_id=reservation.id
           AND call.tenant_id=brief.tenant_id AND call.user_id=brief.user_id
           AND call.run_snapshot_id=brief.run_snapshot_id
           AND call.span_name='research_synthesis' AND call.error=''
           AND public.research_brief_matches_completion_v125(
                   NEW.candidate_brief_payload,call.completion)
    ) THEN
        RAISE EXCEPTION '125: v3.6 grounding input differs from synthesis authority'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_grounding_insert_v125() FROM PUBLIC;
CREATE TRIGGER enforce_research_grounding_insert_v125
BEFORE INSERT ON research_brief_grounding_verifications
FOR EACH ROW EXECUTE FUNCTION enforce_research_grounding_insert_v125();

-- +goose StatementBegin
CREATE FUNCTION protect_research_brief_grounding_correction_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF ROW(
        NEW.id,NEW.tenant_id,NEW.user_id,NEW.task_id,NEW.run_snapshot_id,
        NEW.plan_id,NEW.synthesis_id,NEW.grounding_verification_id,
        NEW.correction_prompt,NEW.correction_prompt_digest,
        NEW.schema_version,NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.id,OLD.tenant_id,OLD.user_id,OLD.task_id,OLD.run_snapshot_id,
        OLD.plan_id,OLD.synthesis_id,OLD.grounding_verification_id,
        OLD.correction_prompt,OLD.correction_prompt_digest,
        OLD.schema_version,OLD.created_at
    ) THEN
        RAISE EXCEPTION '125: research grounding correction identity is immutable';
    END IF;
    IF OLD.status='prepared' AND NEW.status='corrected' THEN
        IF NOT public.research_grounding_correction_citations_subset_v125(
                (SELECT grounding.candidate_brief_payload
                   FROM public.research_brief_grounding_verifications grounding
                  WHERE grounding.id=NEW.grounding_verification_id),
                NEW.corrected_brief_payload) OR
           NOT (convert_from(NEW.verifier_prompt,'UTF8')
                    IS JSON OBJECT WITH UNIQUE KEYS) OR
           convert_from(NEW.verifier_prompt,'UTF8')::jsonb IS DISTINCT FROM
                public.research_expected_grounding_verifier_prompt_v125(
                    NEW.synthesis_id,NEW.corrected_brief_payload,
                    NEW.corrected_brief_digest) THEN
            RAISE EXCEPTION '125: corrected candidate authority differs';
        END IF;
        NEW.corrected_at := clock_timestamp();
        RETURN NEW;
    END IF;
    IF OLD.status='corrected' AND NEW.status IN ('grounded','rejected') AND
       ROW(NEW.corrector_llm_spend_reservation_id,NEW.corrected_brief_payload,
           NEW.corrected_brief_digest,NEW.verifier_prompt,NEW.verifier_prompt_digest,
           NEW.corrected_at) IS NOT DISTINCT FROM
       ROW(OLD.corrector_llm_spend_reservation_id,OLD.corrected_brief_payload,
           OLD.corrected_brief_digest,OLD.verifier_prompt,OLD.verifier_prompt_digest,
           OLD.corrected_at) THEN
        NEW.finalized_at := clock_timestamp();
        RETURN NEW;
    END IF;
    RAISE EXCEPTION '125: research grounding correction is immutable';
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION protect_research_brief_grounding_correction_v1() FROM PUBLIC;
CREATE TRIGGER protect_research_brief_grounding_correction_v1
BEFORE UPDATE ON research_brief_grounding_corrections
FOR EACH ROW EXECUTE FUNCTION protect_research_brief_grounding_correction_v1();

GRANT SELECT ON research_brief_grounding_corrections
    TO vane_app,vane_research_v3_executor;
GRANT INSERT (
    tenant_id,user_id,task_id,run_snapshot_id,plan_id,synthesis_id,
    grounding_verification_id,correction_prompt,correction_prompt_digest,
    schema_version
) ON research_brief_grounding_corrections TO vane_app,vane_research_v3_executor;
GRANT UPDATE (
    status,corrector_llm_spend_reservation_id,corrected_brief_payload,
    corrected_brief_digest,verifier_prompt,verifier_prompt_digest,
    verifier_llm_spend_reservation_id,verdict_payload,verdict_digest
) ON research_brief_grounding_corrections TO vane_app,vane_research_v3_executor;
GRANT USAGE,SELECT ON SEQUENCE research_brief_grounding_corrections_id_seq
    TO vane_app,vane_research_v3_executor;

ALTER TABLE research_brief_grounding_corrections ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON research_brief_grounding_corrections
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON research_brief_grounding_corrections AS RESTRICTIVE
    FOR ALL
    USING (tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint);
CREATE POLICY user_isolation ON research_brief_grounding_corrections AS RESTRICTIVE
    FOR ALL
    USING (user_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint)
    WITH CHECK (user_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint);
CREATE POLICY research_v3_scope ON research_brief_grounding_corrections AS RESTRICTIVE
    FOR ALL TO vane_research_v3_executor
    USING (tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
           user_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
                user_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint);
CREATE POLICY research_v3_capability_scope
    ON research_brief_grounding_corrections AS RESTRICTIVE
    FOR ALL TO vane_research_v3_executor
    USING (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,run_snapshot_id,NULL))
    WITH CHECK (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,run_snapshot_id,NULL));

ALTER TABLE research_run_llm_spend_reservations
    DROP CONSTRAINT ck_research_llm_spend_reservation_stage;
ALTER TABLE research_run_llm_spend_reservations
    ADD CONSTRAINT ck_research_llm_spend_reservation_stage CHECK (
        (stage='planner' AND subject_id=0 AND
         counts_against_planner_budget AND reserved_planner_tokens=reserved_quota_tokens) OR
        (stage='synthesis' AND subject_id>0 AND
         NOT counts_against_planner_budget AND reserved_planner_tokens=0 AND
         round_ordinal IN (0,1,2,3))
    );

-- Replace only the trigger binding. The v124 function remains byte-retained
-- for Down; this version accepts both retained v3.5 and current v3.6 snapshots.
-- +goose StatementBegin
CREATE FUNCTION enforce_research_scope_window_v36()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    snapshot_json JSONB;
    context_json JSONB;
    cutoff_ns NUMERIC;
    window_start_ns NUMERIC;
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
        RAISE EXCEPTION '125: synthesis snapshot scope differs' USING ERRCODE='23514';
    END IF;
    IF snapshot_json #>> '{research_model,synthesis,renderer_version}' IS NULL OR
       snapshot_json #>> '{research_model,synthesis,renderer_version}' NOT IN (
       'research-synthesis.render/v3.5','research-synthesis.render/v3.6') THEN
        IF snapshot_json #> '{definition,research_scope}' IS NOT NULL OR
           convert_from(NEW.context_payload,'UTF8')::jsonb->>'schema_version' =
               'vane.research-synthesis-context/v3.3' THEN
            RAISE EXCEPTION '125: unscoped renderer carries event-window state'
                USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF jsonb_typeof(snapshot_json#>'{definition,research_scope}') IS DISTINCT FROM 'object' OR
       (SELECT count(*) FROM jsonb_object_keys(snapshot_json#>'{definition,research_scope}'))<>3 OR
       ((snapshot_json#>'{definition,research_scope}')-
           ARRAY['mode','lookback_seconds','task_manual_digest'])<>'{}'::jsonb OR
       jsonb_typeof(snapshot_json#>'{definition,research_scope,mode}') IS DISTINCT FROM 'string' OR
       jsonb_typeof(snapshot_json#>'{definition,research_scope,lookback_seconds}') IS DISTINCT FROM 'number' OR
       jsonb_typeof(snapshot_json#>'{definition,research_scope,task_manual_digest}') IS DISTINCT FROM 'string' OR
       snapshot_json #>> '{definition,research_scope,mode}' IS DISTINCT FROM 'event_window' OR
       (snapshot_json #>> '{definition,research_scope,lookback_seconds}')::bigint IS DISTINCT FROM 604800 OR
       jsonb_typeof(snapshot_json#>'{definition,task_manual}') IS DISTINCT FROM 'string' OR
       octet_length(snapshot_json#>>'{definition,task_manual}')<1 OR
       snapshot_json #>> '{definition,research_scope,task_manual_digest}' !~ '^[0-9a-f]{64}$' OR
       snapshot_json #>> '{definition,research_scope,task_manual_digest}' IS DISTINCT FROM
           encode(sha256(convert_to(snapshot_json#>>'{definition,task_manual}','UTF8')),'hex') THEN
        RAISE EXCEPTION '125: scoped snapshot lacks exact owner scope'
            USING ERRCODE='23514';
    END IF;
    cutoff_ns := public.research_scope_timestamp_ns_v124(
        snapshot_json->>'history_through_utc');
    window_start_ns := cutoff_ns-604800000000000;
    context_json := convert_from(NEW.context_payload,'UTF8')::jsonb;
    IF context_json->>'schema_version' IS DISTINCT FROM
           'vane.research-synthesis-context/v3.3' OR
       cutoff_ns IS NULL OR
       jsonb_typeof(context_json->'research_scope_window') IS DISTINCT FROM 'object' OR
       (SELECT count(*) FROM jsonb_object_keys(context_json->'research_scope_window'))<>5 OR
       ((context_json->'research_scope_window')-
           ARRAY['mode','lookback_seconds','start_utc','end_utc','boundary'])<>'{}'::jsonb OR
       jsonb_typeof(context_json#>'{research_scope_window,mode}') IS DISTINCT FROM 'string' OR
       jsonb_typeof(context_json#>'{research_scope_window,lookback_seconds}') IS DISTINCT FROM 'number' OR
       jsonb_typeof(context_json#>'{research_scope_window,start_utc}') IS DISTINCT FROM 'string' OR
       jsonb_typeof(context_json#>'{research_scope_window,end_utc}') IS DISTINCT FROM 'string' OR
       jsonb_typeof(context_json#>'{research_scope_window,boundary}') IS DISTINCT FROM 'string' OR
       context_json #>> '{research_scope_window,mode}' IS DISTINCT FROM 'event_window' OR
       (context_json #>> '{research_scope_window,lookback_seconds}')::bigint IS DISTINCT FROM 604800 OR
       context_json #>> '{research_scope_window,boundary}' IS DISTINCT FROM '(start,end]' OR
       public.research_scope_timestamp_ns_v124(
           context_json #>> '{research_scope_window,start_utc}') IS DISTINCT FROM window_start_ns OR
       public.research_scope_timestamp_ns_v124(
           context_json #>> '{research_scope_window,end_utc}') IS DISTINCT FROM cutoff_ns OR
       context_json #>> '{research_scope_window,end_utc}' IS DISTINCT FROM
           snapshot_json->>'history_through_utc' OR
       context_json #>> '{research_scope_window,start_utc}' !~ 'Z$' OR
       context_json #>> '{history,history_through_utc}' IS DISTINCT FROM
           snapshot_json->>'history_through_utc' THEN
        RAISE EXCEPTION '125: synthesis window differs from frozen owner scope'
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
        RAISE EXCEPTION '125: scoped web Evidence is truncated or malformed'
            USING ERRCODE='23514';
    END IF;
    WITH filtered AS (
        SELECT evidence.*,
               public.filter_research_scope_evidence_v124(
                   evidence.result_bytes,window_start_ns,cutoff_ns) AS filtered_text
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
        RAISE EXCEPTION '125: synthesis eligible Evidence ids differ'
            USING ERRCODE='23514';
    END IF;
    WITH filtered AS (
        SELECT evidence.*,
               public.filter_research_scope_evidence_v124(
                   evidence.result_bytes,window_start_ns,cutoff_ns) AS filtered_text
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
        RAISE EXCEPTION '125: synthesis eligible Evidence projection differs'
            USING ERRCODE='23514';
    END IF;
    IF NEW.status='finalized' AND EXISTS (
        SELECT 1
          FROM jsonb_array_elements(
                 convert_from(NEW.brief_payload,'UTF8')::jsonb->'citations') citation
         WHERE citation->>'kind'='current_evidence'
           AND (citation->>'ref' !~ '^[1-9][0-9]*$' OR NOT EXISTS (
               SELECT 1 FROM jsonb_array_elements(expected_ids) eligible_id
                WHERE eligible_id::text=citation->>'ref'
           ))
    ) THEN
        RAISE EXCEPTION '125: final Brief cites ineligible scoped Evidence'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_scope_window_v36() FROM PUBLIC;
DROP TRIGGER research_scope_window_v33 ON research_brief_syntheses;
CREATE TRIGGER research_scope_window_v33
BEFORE INSERT OR UPDATE ON research_brief_syntheses
FOR EACH ROW EXECUTE FUNCTION enforce_research_scope_window_v36();

-- Recompute the immutable round-1 verifier authority from its exact admitted
-- reservation, completed settlement, provider receipt, frozen prompt, and
-- canonical verdict artifact. Both direct and corrected finalization call this
-- helper, so a forged grounding status can never authorize a Brief.
-- +goose StatementBegin
CREATE FUNCTION research_grounding_has_exact_receipt_v125(
    grounding_id BIGINT,
    expected_status TEXT
) RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
SELECT expected_status IN ('grounded','rejected') AND EXISTS (
    SELECT 1
      FROM public.research_brief_grounding_verifications grounding
      JOIN public.research_run_llm_spend_reservations reservation
        ON reservation.id=grounding.verifier_llm_spend_reservation_id
       AND reservation.tenant_id=grounding.tenant_id
       AND reservation.user_id=grounding.user_id
       AND reservation.task_id=grounding.task_id
       AND reservation.run_snapshot_id=grounding.run_snapshot_id
      JOIN public.research_run_llm_spend_settlements settlement
        ON settlement.reservation_id=reservation.id
       AND settlement.tenant_id=reservation.tenant_id
       AND settlement.user_id=reservation.user_id
       AND settlement.task_id=reservation.task_id
       AND settlement.run_snapshot_id=reservation.run_snapshot_id
       AND settlement.stage=reservation.stage
       AND settlement.round_ordinal=reservation.round_ordinal
      JOIN public.llm_calls call ON call.id=settlement.llm_call_id
     WHERE grounding.id=grounding_id
       AND grounding.status=expected_status
       AND grounding.verdict_digest=encode(sha256(grounding.verdict_payload),'hex')
       AND reservation.stage='synthesis' AND reservation.round_ordinal=1
       AND reservation.subject_id=grounding.synthesis_id
       AND reservation.user_prompt_digest=grounding.verifier_prompt_digest
       AND convert_from(grounding.verifier_prompt,'UTF8')
               IS JSON OBJECT WITH UNIQUE KEYS
       AND convert_from(grounding.verifier_prompt,'UTF8')::jsonb=
           public.research_expected_grounding_verifier_prompt_v125(
               grounding.synthesis_id,grounding.candidate_brief_payload,
               grounding.candidate_digest)
       AND settlement.attempted AND settlement.usage_known
       AND NOT settlement.definitely_zero_usage
       AND settlement.outcome='completed' AND settlement.error_code=''
       AND call.research_run_llm_spend_reservation_id=reservation.id
       AND call.tenant_id=grounding.tenant_id
       AND call.user_id=grounding.user_id
       AND call.run_snapshot_id=grounding.run_snapshot_id
       AND call.span_name='research_synthesis' AND call.error=''
       AND call.user_prompt=convert_from(grounding.verifier_prompt,'UTF8')
       AND public.research_brief_matches_synthesis_completion_v119(
               grounding.verdict_payload,call.completion)
       AND public.research_grounding_verdict_valid_v125(
               grounding.verdict_payload,grounding.candidate_brief_payload,
               CASE expected_status
                   WHEN 'grounded' THEN 'grounded' ELSE 'unsupported' END)
       AND jsonb_typeof(convert_from(grounding.verdict_payload,'UTF8')::jsonb)='object'
       AND convert_from(grounding.verdict_payload,'UTF8')::jsonb->>'schema_version'=
           'vane.research-grounding-verdict/v1'
       AND convert_from(grounding.verdict_payload,'UTF8')::jsonb->>'candidate_digest'=
           grounding.candidate_digest
       AND convert_from(grounding.verdict_payload,'UTF8')::jsonb->>'verdict'=
           CASE expected_status WHEN 'grounded' THEN 'grounded' ELSE 'unsupported' END
       AND jsonb_typeof(convert_from(grounding.verdict_payload,'UTF8')::jsonb->'issues')=
           'array'
       AND CASE expected_status
           WHEN 'grounded' THEN
               jsonb_array_length(
                   convert_from(grounding.verdict_payload,'UTF8')::jsonb->'issues')=0
           ELSE jsonb_array_length(
                   convert_from(grounding.verdict_payload,'UTF8')::jsonb->'issues')
                    BETWEEN 1 AND 16
           END
)
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION research_grounding_has_exact_receipt_v125(BIGINT,TEXT)
    FROM PUBLIC;

-- Rebuild the exact Go json.Marshal byte sequence from the immutable rejected
-- round-1 grounding record. The only interpolated scalar is a checked hex
-- digest; nested inputs are already canonical Store artifacts.
-- +goose StatementBegin
CREATE FUNCTION research_expected_grounding_correction_prompt_v125(
    grounding_id BIGINT
) RETURNS BYTEA
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
SELECT CASE
       WHEN public.research_grounding_has_exact_receipt_v125(grounding.id,'rejected')
       THEN convert_to(
           '{"schema_version":"vane.research-grounding-correction-input/v1",' ||
           '"original_candidate_digest":"' || grounding.candidate_digest || '",' ||
           '"initial_grounding_input":' ||
               convert_from(grounding.verifier_prompt,'UTF8') || ',' ||
           '"initial_verdict":' ||
               convert_from(grounding.verdict_payload,'UTF8') || ',' ||
           '"response_contract":{' ||
               '"output_schema":"one canonical vane.research-brief/v3, v3.1, or v3.2 JSON object as permitted by the frozen evidence coverage",' ||
               '"canonical_json_only":true,' ||
               '"citation_rule":"citations must be a subset of the original candidate citations; never add a ref",' ||
               '"correction_rule":"resolve every initial_verdict issue by deleting or narrowing unsupported claims; never introduce a new factual claim"}}',
           'UTF8')
       ELSE NULL
       END
  FROM public.research_brief_grounding_verifications grounding
 WHERE grounding.id=grounding_id
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION research_expected_grounding_correction_prompt_v125(BIGINT)
    FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION enforce_research_grounding_correction_insert_v125()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF NEW.correction_prompt IS DISTINCT FROM
           public.research_expected_grounding_correction_prompt_v125(
               NEW.grounding_verification_id) OR NOT EXISTS (
        SELECT 1
          FROM public.research_brief_grounding_verifications grounding
          JOIN public.research_brief_syntheses brief
            ON brief.id=grounding.synthesis_id
           AND brief.tenant_id=grounding.tenant_id
           AND brief.user_id=grounding.user_id
           AND brief.task_id=grounding.task_id
           AND brief.run_snapshot_id=grounding.run_snapshot_id
           AND brief.plan_id=grounding.plan_id
          JOIN public.task_run_snapshots snapshot
            ON snapshot.id=grounding.run_snapshot_id
           AND snapshot.tenant_id=grounding.tenant_id
           AND snapshot.user_id=grounding.user_id
           AND snapshot.task_id=grounding.task_id
         WHERE grounding.id=NEW.grounding_verification_id
           AND grounding.tenant_id=NEW.tenant_id
           AND grounding.user_id=NEW.user_id
           AND grounding.task_id=NEW.task_id
           AND grounding.run_snapshot_id=NEW.run_snapshot_id
           AND grounding.plan_id=NEW.plan_id
           AND grounding.synthesis_id=NEW.synthesis_id
           AND grounding.status='rejected'
           AND brief.status='spending'
           AND convert_from(snapshot.payload,'UTF8')::jsonb
                 #>> '{research_model,synthesis,renderer_version}'=
               'research-synthesis.render/v3.6'
    ) THEN
        RAISE EXCEPTION '125: grounding correction prompt differs from rejected authority'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_grounding_correction_insert_v125()
    FROM PUBLIC;
CREATE TRIGGER enforce_research_grounding_correction_insert_v125
BEFORE INSERT ON research_brief_grounding_corrections
FOR EACH ROW EXECUTE FUNCTION enforce_research_grounding_correction_insert_v125();

-- Round 3 is payable only after round 2 has a completed exact receipt whose
-- normalized Brief is the immutable corrected candidate. This helper also
-- rechecks the deterministic correction prompt and citation non-expansion.
-- +goose StatementBegin
CREATE FUNCTION research_correction_has_exact_candidate_receipt_v125(
    correction_id BIGINT
) RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
SELECT EXISTS (
    SELECT 1
      FROM public.research_brief_grounding_corrections correction
      JOIN public.research_brief_grounding_verifications grounding
        ON grounding.id=correction.grounding_verification_id
       AND grounding.tenant_id=correction.tenant_id
       AND grounding.user_id=correction.user_id
       AND grounding.task_id=correction.task_id
       AND grounding.run_snapshot_id=correction.run_snapshot_id
       AND grounding.plan_id=correction.plan_id
       AND grounding.synthesis_id=correction.synthesis_id
      JOIN public.research_run_llm_spend_reservations reservation
        ON reservation.id=correction.corrector_llm_spend_reservation_id
       AND reservation.tenant_id=correction.tenant_id
       AND reservation.user_id=correction.user_id
       AND reservation.task_id=correction.task_id
       AND reservation.run_snapshot_id=correction.run_snapshot_id
      JOIN public.research_run_llm_spend_settlements settlement
        ON settlement.reservation_id=reservation.id
       AND settlement.tenant_id=reservation.tenant_id
       AND settlement.user_id=reservation.user_id
       AND settlement.task_id=reservation.task_id
       AND settlement.run_snapshot_id=reservation.run_snapshot_id
       AND settlement.stage=reservation.stage
       AND settlement.round_ordinal=reservation.round_ordinal
      JOIN public.llm_calls call ON call.id=settlement.llm_call_id
     WHERE correction.id=correction_id
       AND correction.status IN ('corrected','grounded','rejected')
       AND correction.correction_prompt=
           public.research_expected_grounding_correction_prompt_v125(grounding.id)
       AND public.research_grounding_correction_citations_subset_v125(
               grounding.candidate_brief_payload,
               correction.corrected_brief_payload)
       AND convert_from(correction.verifier_prompt,'UTF8')
               IS JSON OBJECT WITH UNIQUE KEYS
       AND convert_from(correction.verifier_prompt,'UTF8')::jsonb=
           public.research_expected_grounding_verifier_prompt_v125(
               correction.synthesis_id,correction.corrected_brief_payload,
               correction.corrected_brief_digest)
       AND reservation.stage='synthesis' AND reservation.round_ordinal=2
       AND reservation.subject_id=correction.synthesis_id
       AND reservation.user_prompt_digest=correction.correction_prompt_digest
       AND settlement.attempted AND settlement.usage_known
       AND NOT settlement.definitely_zero_usage
       AND settlement.outcome='completed' AND settlement.error_code=''
       AND call.research_run_llm_spend_reservation_id=reservation.id
       AND call.tenant_id=correction.tenant_id
       AND call.user_id=correction.user_id
       AND call.run_snapshot_id=correction.run_snapshot_id
       AND call.span_name='research_synthesis' AND call.error=''
       AND call.user_prompt=convert_from(correction.correction_prompt,'UTF8')
       AND public.research_brief_matches_completion_v125(
               correction.corrected_brief_payload,call.completion)
)
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION research_correction_has_exact_candidate_receipt_v125(BIGINT)
    FROM PUBLIC;

-- Retain the v119 round-0 receipt fence byte-for-byte for every historical
-- path. A v3.6 correction may instead bind the final canonical Brief to the
-- exact completed round-2 receipt that produced the immutable corrected row.
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
           AND (
               public.research_brief_matches_synthesis_completion_v119(
                   NEW.brief_payload,call.completion) OR (
               public.research_brief_matches_completion_v125(
                   NEW.brief_payload,call.completion) AND EXISTS (
                   SELECT 1 FROM public.task_run_snapshots snapshot
                    WHERE snapshot.id=NEW.run_snapshot_id
                      AND snapshot.tenant_id=NEW.tenant_id
                      AND snapshot.user_id=NEW.user_id
                      AND snapshot.task_id=NEW.task_id
                      AND convert_from(snapshot.payload,'UTF8')::jsonb
                            #>> '{research_model,synthesis,renderer_version}'=
                          'research-synthesis.render/v3.6'
               )))
    ) AND NOT EXISTS (
        SELECT 1
          FROM public.task_run_snapshots snapshot
          JOIN public.research_brief_grounding_verifications grounding
            ON grounding.run_snapshot_id=snapshot.id
           AND grounding.tenant_id=snapshot.tenant_id
           AND grounding.user_id=snapshot.user_id
           AND grounding.task_id=snapshot.task_id
          JOIN public.research_brief_grounding_corrections correction
            ON correction.grounding_verification_id=grounding.id
           AND correction.tenant_id=grounding.tenant_id
           AND correction.user_id=grounding.user_id
           AND correction.task_id=grounding.task_id
           AND correction.run_snapshot_id=grounding.run_snapshot_id
           AND correction.plan_id=grounding.plan_id
           AND correction.synthesis_id=grounding.synthesis_id
          JOIN public.research_run_llm_spend_reservations reservation
            ON reservation.id=correction.corrector_llm_spend_reservation_id
           AND reservation.tenant_id=correction.tenant_id
           AND reservation.user_id=correction.user_id
           AND reservation.task_id=correction.task_id
           AND reservation.run_snapshot_id=correction.run_snapshot_id
          JOIN public.research_run_llm_spend_settlements settlement
            ON settlement.reservation_id=reservation.id
           AND settlement.tenant_id=reservation.tenant_id
           AND settlement.user_id=reservation.user_id
           AND settlement.task_id=reservation.task_id
           AND settlement.run_snapshot_id=reservation.run_snapshot_id
           AND settlement.stage=reservation.stage
           AND settlement.round_ordinal=reservation.round_ordinal
          JOIN public.llm_calls call ON call.id=settlement.llm_call_id
         WHERE snapshot.id=NEW.run_snapshot_id
           AND snapshot.tenant_id=NEW.tenant_id
           AND snapshot.user_id=NEW.user_id
           AND snapshot.task_id=NEW.task_id
           AND convert_from(snapshot.payload,'UTF8')::jsonb
                 #>> '{research_model,synthesis,renderer_version}'=
               'research-synthesis.render/v3.6'
           AND grounding.plan_id=NEW.plan_id
           AND grounding.synthesis_id=NEW.id
           AND grounding.status='rejected'
           AND public.research_grounding_has_exact_receipt_v125(
                   grounding.id,'rejected')
           AND correction.status='grounded'
           AND public.research_correction_has_exact_candidate_receipt_v125(
                   correction.id)
           AND correction.corrected_brief_payload=NEW.brief_payload
           AND correction.corrected_brief_digest=NEW.brief_digest
           AND public.research_grounding_correction_citations_subset_v125(
                   grounding.candidate_brief_payload,
                   correction.corrected_brief_payload)
           AND reservation.stage='synthesis' AND reservation.round_ordinal=2
           AND reservation.subject_id=NEW.id
           AND reservation.user_prompt_digest=correction.correction_prompt_digest
           AND settlement.attempted AND settlement.usage_known
           AND NOT settlement.definitely_zero_usage
           AND settlement.outcome='completed' AND settlement.error_code=''
           AND call.research_run_llm_spend_reservation_id=reservation.id
           AND call.tenant_id=NEW.tenant_id AND call.user_id=NEW.user_id
           AND call.run_snapshot_id=NEW.run_snapshot_id
           AND call.span_name='research_synthesis' AND call.error=''
           AND call.user_prompt=convert_from(correction.correction_prompt,'UTF8')
           AND public.research_brief_matches_completion_v125(
                   NEW.brief_payload,call.completion)
    ) THEN
        RAISE EXCEPTION '125: final corrected research Brief differs from its completed correction receipt'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_brief_llm_receipt_v1() FROM PUBLIC;

-- DB finalization is independently bound to either the first grounded verdict
-- or the one corrected candidate plus its final grounded verdict.
-- +goose StatementBegin
CREATE FUNCTION enforce_research_grounding_finalization_v36()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    renderer TEXT;
BEGIN
    SELECT convert_from(snapshot.payload,'UTF8')::jsonb
               #>> '{research_model,synthesis,renderer_version}'
      INTO renderer
      FROM public.task_run_snapshots snapshot
     WHERE snapshot.id=NEW.run_snapshot_id
       AND snapshot.tenant_id=NEW.tenant_id
       AND snapshot.user_id=NEW.user_id
       AND snapshot.task_id=NEW.task_id;
    IF renderer IS DISTINCT FROM 'research-synthesis.render/v3.6' OR
       NEW.status<>'finalized' THEN
        RETURN NEW;
    END IF;
    IF EXISTS (
        SELECT 1
          FROM public.research_brief_grounding_verifications grounding
         WHERE grounding.tenant_id=NEW.tenant_id
           AND grounding.user_id=NEW.user_id
           AND grounding.task_id=NEW.task_id
           AND grounding.run_snapshot_id=NEW.run_snapshot_id
           AND grounding.plan_id=NEW.plan_id
           AND grounding.synthesis_id=NEW.id
           AND grounding.status='grounded'
           AND public.research_grounding_has_exact_receipt_v125(
                   grounding.id,'grounded')
           AND grounding.candidate_brief_payload=NEW.brief_payload
           AND grounding.candidate_digest=NEW.brief_digest
           AND grounding.verdict_digest=encode(sha256(grounding.verdict_payload),'hex')
           AND convert_from(grounding.verdict_payload,'UTF8')=
               '{"schema_version":"vane.research-grounding-verdict/v1",' ||
               '"candidate_digest":"' || grounding.candidate_digest || '",' ||
               '"verdict":"grounded","issues":[]}'
           AND jsonb_typeof(convert_from(grounding.verdict_payload,'UTF8')::jsonb)='object'
           AND convert_from(grounding.verdict_payload,'UTF8')::jsonb->>'schema_version'=
               'vane.research-grounding-verdict/v1'
           AND convert_from(grounding.verdict_payload,'UTF8')::jsonb->>'candidate_digest'=
               grounding.candidate_digest
           AND convert_from(grounding.verdict_payload,'UTF8')::jsonb->>'verdict'='grounded'
           AND convert_from(grounding.verdict_payload,'UTF8')::jsonb->'issues'='[]'::jsonb
    ) THEN
        RETURN NEW;
    END IF;
    IF EXISTS (
        SELECT 1
          FROM public.research_brief_grounding_verifications grounding
          JOIN public.research_brief_grounding_corrections correction
            ON correction.grounding_verification_id=grounding.id
           AND correction.tenant_id=grounding.tenant_id
           AND correction.user_id=grounding.user_id
           AND correction.task_id=grounding.task_id
           AND correction.run_snapshot_id=grounding.run_snapshot_id
           AND correction.plan_id=grounding.plan_id
           AND correction.synthesis_id=grounding.synthesis_id
          JOIN public.research_run_llm_spend_reservations corrector_reservation
            ON corrector_reservation.id=correction.corrector_llm_spend_reservation_id
           AND corrector_reservation.tenant_id=correction.tenant_id
           AND corrector_reservation.user_id=correction.user_id
           AND corrector_reservation.task_id=correction.task_id
           AND corrector_reservation.run_snapshot_id=correction.run_snapshot_id
          JOIN public.research_run_llm_spend_settlements corrector_settlement
            ON corrector_settlement.reservation_id=corrector_reservation.id
           AND corrector_settlement.tenant_id=corrector_reservation.tenant_id
           AND corrector_settlement.user_id=corrector_reservation.user_id
           AND corrector_settlement.task_id=corrector_reservation.task_id
           AND corrector_settlement.run_snapshot_id=corrector_reservation.run_snapshot_id
           AND corrector_settlement.stage=corrector_reservation.stage
           AND corrector_settlement.round_ordinal=corrector_reservation.round_ordinal
          JOIN public.llm_calls corrector_call
            ON corrector_call.id=corrector_settlement.llm_call_id
          JOIN public.research_run_llm_spend_reservations verifier_reservation
            ON verifier_reservation.id=correction.verifier_llm_spend_reservation_id
           AND verifier_reservation.tenant_id=correction.tenant_id
           AND verifier_reservation.user_id=correction.user_id
           AND verifier_reservation.task_id=correction.task_id
           AND verifier_reservation.run_snapshot_id=correction.run_snapshot_id
          JOIN public.research_run_llm_spend_settlements verifier_settlement
            ON verifier_settlement.reservation_id=verifier_reservation.id
           AND verifier_settlement.tenant_id=verifier_reservation.tenant_id
           AND verifier_settlement.user_id=verifier_reservation.user_id
           AND verifier_settlement.task_id=verifier_reservation.task_id
           AND verifier_settlement.run_snapshot_id=verifier_reservation.run_snapshot_id
           AND verifier_settlement.stage=verifier_reservation.stage
           AND verifier_settlement.round_ordinal=verifier_reservation.round_ordinal
          JOIN public.llm_calls verifier_call
            ON verifier_call.id=verifier_settlement.llm_call_id
         WHERE grounding.tenant_id=NEW.tenant_id
           AND grounding.user_id=NEW.user_id
           AND grounding.task_id=NEW.task_id
           AND grounding.run_snapshot_id=NEW.run_snapshot_id
           AND grounding.plan_id=NEW.plan_id
           AND grounding.synthesis_id=NEW.id
           AND grounding.status='rejected'
           AND public.research_grounding_has_exact_receipt_v125(
                   grounding.id,'rejected')
           AND correction.status='grounded'
           AND public.research_correction_has_exact_candidate_receipt_v125(
                   correction.id)
           AND correction.corrected_brief_payload=NEW.brief_payload
           AND correction.corrected_brief_digest=NEW.brief_digest
           AND public.research_grounding_correction_citations_subset_v125(
                   grounding.candidate_brief_payload,
                   correction.corrected_brief_payload)
           AND corrector_reservation.stage='synthesis'
           AND corrector_reservation.round_ordinal=2
           AND corrector_reservation.subject_id=NEW.id
           AND corrector_reservation.user_prompt_digest=
               correction.correction_prompt_digest
           AND corrector_settlement.attempted AND corrector_settlement.usage_known
           AND NOT corrector_settlement.definitely_zero_usage
           AND corrector_settlement.outcome='completed'
           AND corrector_settlement.error_code=''
           AND corrector_call.research_run_llm_spend_reservation_id=
               corrector_reservation.id
           AND corrector_call.tenant_id=NEW.tenant_id
           AND corrector_call.user_id=NEW.user_id
           AND corrector_call.run_snapshot_id=NEW.run_snapshot_id
           AND corrector_call.span_name='research_synthesis'
           AND corrector_call.error=''
           AND corrector_call.user_prompt=
               convert_from(correction.correction_prompt,'UTF8')
           AND public.research_brief_matches_completion_v125(
                   correction.corrected_brief_payload,corrector_call.completion)
           AND verifier_reservation.stage='synthesis'
           AND verifier_reservation.round_ordinal=3
           AND verifier_reservation.subject_id=NEW.id
           AND verifier_reservation.user_prompt_digest=
               correction.verifier_prompt_digest
           AND verifier_settlement.attempted AND verifier_settlement.usage_known
           AND NOT verifier_settlement.definitely_zero_usage
           AND verifier_settlement.outcome='completed'
           AND verifier_settlement.error_code=''
           AND verifier_call.research_run_llm_spend_reservation_id=
               verifier_reservation.id
           AND verifier_call.tenant_id=NEW.tenant_id
           AND verifier_call.user_id=NEW.user_id
           AND verifier_call.run_snapshot_id=NEW.run_snapshot_id
           AND verifier_call.span_name='research_synthesis'
           AND verifier_call.error=''
           AND verifier_call.user_prompt=
               convert_from(correction.verifier_prompt,'UTF8')
           AND public.research_brief_matches_synthesis_completion_v119(
                   correction.verdict_payload,verifier_call.completion)
           AND correction.verdict_digest=encode(sha256(correction.verdict_payload),'hex')
           AND convert_from(correction.verdict_payload,'UTF8')=
               '{"schema_version":"vane.research-grounding-verdict/v1",' ||
               '"candidate_digest":"' || correction.corrected_brief_digest || '",' ||
               '"verdict":"grounded","issues":[]}'
           AND jsonb_typeof(convert_from(correction.verdict_payload,'UTF8')::jsonb)='object'
           AND convert_from(correction.verdict_payload,'UTF8')::jsonb->>'schema_version'=
               'vane.research-grounding-verdict/v1'
           AND convert_from(correction.verdict_payload,'UTF8')::jsonb->>'candidate_digest'=
               correction.corrected_brief_digest
           AND convert_from(correction.verdict_payload,'UTF8')::jsonb->>'verdict'='grounded'
           AND convert_from(correction.verdict_payload,'UTF8')::jsonb->'issues'='[]'::jsonb
    ) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION '125: v3.6 final Brief lacks exact grounded authority'
        USING ERRCODE='23514';
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_grounding_finalization_v36() FROM PUBLIC;
CREATE TRIGGER research_brief_grounding_finalization_v36
BEFORE INSERT OR UPDATE ON research_brief_syntheses
FOR EACH ROW EXECUTE FUNCTION enforce_research_grounding_finalization_v36();

-- +goose StatementBegin
CREATE FUNCTION enforce_research_run_llm_spend_reservation_v3()
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
        RAISE EXCEPTION '125: LLM reservation snapshot scope differs'
            USING ERRCODE='23514';
    END IF;
    stage_key := CASE
        WHEN NEW.stage='planner' THEN 'planner'
        WHEN NEW.stage='synthesis' AND NEW.round_ordinal=0 THEN 'synthesis'
        WHEN NEW.stage='synthesis' AND NEW.round_ordinal=1 AND
             snapshot_json #>> '{research_model,synthesis,renderer_version}' IN (
                 'research-synthesis.render/v3.3','research-synthesis.render/v3.4',
                 'research-synthesis.render/v3.5','research-synthesis.render/v3.6'
             ) THEN 'grounding_verifier'
        WHEN NEW.stage='synthesis' AND NEW.round_ordinal=2 AND
             snapshot_json #>> '{research_model,synthesis,renderer_version}'=
                 'research-synthesis.render/v3.6' THEN 'grounding_corrector'
        WHEN NEW.stage='synthesis' AND NEW.round_ordinal=3 AND
             snapshot_json #>> '{research_model,synthesis,renderer_version}'=
                 'research-synthesis.render/v3.6' THEN 'grounding_verifier'
        ELSE NULL
    END;
    IF stage_key IS NULL THEN
        RAISE EXCEPTION '125: LLM reservation stage differs'
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
        RAISE EXCEPTION '125: LLM reservation differs from frozen model policy'
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
        RAISE EXCEPTION '125: LLM reservation differs from exact price ceiling'
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
        )) OR (NEW.round_ordinal=2 AND NOT EXISTS (
            SELECT 1 FROM public.research_brief_grounding_corrections correction
            JOIN public.research_brief_grounding_verifications grounding
              ON grounding.id=correction.grounding_verification_id
             AND grounding.tenant_id=correction.tenant_id
             AND grounding.user_id=correction.user_id
             AND grounding.task_id=correction.task_id
             AND grounding.run_snapshot_id=correction.run_snapshot_id
             AND grounding.plan_id=correction.plan_id
             AND grounding.synthesis_id=correction.synthesis_id
             WHERE correction.synthesis_id=NEW.subject_id
               AND correction.tenant_id=NEW.tenant_id
               AND correction.user_id=NEW.user_id
               AND correction.task_id=NEW.task_id
               AND correction.run_snapshot_id=NEW.run_snapshot_id
               AND correction.status='prepared' AND grounding.status='rejected'
               AND public.research_grounding_has_exact_receipt_v125(
                       grounding.id,'rejected')
        )) OR (NEW.round_ordinal=3 AND NOT EXISTS (
            SELECT 1 FROM public.research_brief_grounding_corrections correction
             WHERE correction.synthesis_id=NEW.subject_id
               AND correction.tenant_id=NEW.tenant_id
               AND correction.user_id=NEW.user_id
               AND correction.task_id=NEW.task_id
               AND correction.run_snapshot_id=NEW.run_snapshot_id
               AND correction.status='corrected'
               AND public.research_correction_has_exact_candidate_receipt_v125(
                       correction.id)
        )) THEN
            RAISE EXCEPTION '125: synthesis reservation subject differs'
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
        RAISE EXCEPTION '125: planner reservation exceeds frozen PlannerBudget'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_run_llm_spend_reservation_v3() FROM PUBLIC;
DROP TRIGGER research_run_llm_spend_reservation_v1
    ON research_run_llm_spend_reservations;
CREATE TRIGGER research_run_llm_spend_reservation_v1
BEFORE INSERT ON research_run_llm_spend_reservations
FOR EACH ROW EXECUTE FUNCTION enforce_research_run_llm_spend_reservation_v3();

-- +goose StatementBegin
CREATE FUNCTION admit_research_run_llm_spend_v6(
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
    stage_key TEXT;
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
    IF requested_stage<>'synthesis' OR requested_round_ordinal NOT IN (1,2,3) THEN
        RETURN QUERY SELECT * FROM public.admit_research_run_llm_spend_v5(
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
        RAISE EXCEPTION '125: correction LLM admission scope is invalid'
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
	IF requested_round_ordinal=1 AND snapshot_json IS NOT NULL AND
	   snapshot_json #>> '{research_model,synthesis,renderer_version}' IS DISTINCT FROM
	       'research-synthesis.render/v3.6' THEN
		RETURN QUERY SELECT * FROM public.admit_research_run_llm_spend_v5(
			requested_tenant_id,requested_user_id,requested_task_id,
			requested_run_snapshot_id,requested_stage,requested_round_ordinal,
			requested_subject_id,requested_attempt_key,requested_request_digest,
			requested_trace_id,requested_user_prompt);
		RETURN;
	END IF;
    stage_key := CASE requested_round_ordinal
		WHEN 1 THEN 'grounding_verifier'
        WHEN 2 THEN 'grounding_corrector'
        WHEN 3 THEN 'grounding_verifier'
        ELSE NULL
    END;
    IF snapshot_json IS NULL OR stage_key IS NULL OR
       snapshot_json #>> '{research_model,synthesis,renderer_version}' IS DISTINCT FROM
           'research-synthesis.render/v3.6' OR
	   (requested_round_ordinal=1 AND NOT EXISTS (
		   SELECT 1
		     FROM public.research_brief_grounding_verifications grounding
		     JOIN public.research_brief_syntheses brief
		       ON brief.id=grounding.synthesis_id
		    WHERE grounding.synthesis_id=requested_subject_id
		      AND grounding.tenant_id=requested_tenant_id
		      AND grounding.user_id=requested_user_id
		      AND grounding.task_id=requested_task_id
		      AND grounding.run_snapshot_id=requested_run_snapshot_id
		      AND grounding.status='prepared' AND brief.status='spending'
		      AND grounding.verifier_prompt=convert_to(requested_user_prompt,'UTF8')
		      AND grounding.verifier_prompt_digest=
		          encode(sha256(convert_to(requested_user_prompt,'UTF8')),'hex')
	   )) OR
       (requested_round_ordinal=2 AND NOT EXISTS (
           SELECT 1
             FROM public.research_brief_grounding_corrections correction
             JOIN public.research_brief_grounding_verifications grounding
               ON grounding.id=correction.grounding_verification_id
              AND grounding.tenant_id=correction.tenant_id
              AND grounding.user_id=correction.user_id
              AND grounding.task_id=correction.task_id
              AND grounding.run_snapshot_id=correction.run_snapshot_id
              AND grounding.plan_id=correction.plan_id
              AND grounding.synthesis_id=correction.synthesis_id
             JOIN public.research_brief_syntheses brief
               ON brief.id=correction.synthesis_id
            WHERE correction.synthesis_id=requested_subject_id
              AND correction.tenant_id=requested_tenant_id
              AND correction.user_id=requested_user_id
              AND correction.task_id=requested_task_id
              AND correction.run_snapshot_id=requested_run_snapshot_id
              AND correction.status='prepared' AND grounding.status='rejected'
              AND public.research_grounding_has_exact_receipt_v125(
                      grounding.id,'rejected')
              AND brief.status='spending'
              AND correction.correction_prompt=convert_to(requested_user_prompt,'UTF8')
              AND correction.correction_prompt_digest=
                  encode(sha256(convert_to(requested_user_prompt,'UTF8')),'hex')
       )) OR (requested_round_ordinal=3 AND NOT EXISTS (
           SELECT 1
             FROM public.research_brief_grounding_corrections correction
             JOIN public.research_brief_syntheses brief
               ON brief.id=correction.synthesis_id
            WHERE correction.synthesis_id=requested_subject_id
              AND correction.tenant_id=requested_tenant_id
              AND correction.user_id=requested_user_id
              AND correction.task_id=requested_task_id
              AND correction.run_snapshot_id=requested_run_snapshot_id
              AND correction.status='corrected' AND brief.status='spending'
              AND public.research_correction_has_exact_candidate_receipt_v125(
                      correction.id)
              AND correction.verifier_prompt=convert_to(requested_user_prompt,'UTF8')
              AND correction.verifier_prompt_digest=
                  encode(sha256(convert_to(requested_user_prompt,'UTF8')),'hex')
       )) THEN
        RAISE EXCEPTION '125: correction prompt differs from frozen authority'
            USING ERRCODE='23514';
    END IF;
    stage_model := snapshot_json #>> ARRAY['research_model',stage_key,'model'];
    stage_system_prompt := snapshot_json #>> ARRAY['research_model',stage_key,'system_prompt'];
    stage_max_tokens := (snapshot_json #>> ARRAY['research_model',stage_key,'max_tokens'])::bigint;
    stage_temperature := (snapshot_json #>> ARRAY['research_model',stage_key,'temperature'])::real;
    stage_disable_thinking := (snapshot_json #>> ARRAY['research_model',stage_key,'disable_thinking'])::boolean;
    derived_quota_tokens := octet_length(stage_system_prompt)::bigint +
        octet_length(requested_user_prompt)::bigint + 64 + stage_max_tokens;
    IF stage_model IS NULL OR stage_system_prompt IS NULL OR
       derived_quota_tokens NOT BETWEEN 1 AND 1000000 THEN
        RAISE EXCEPTION '125: correction frozen policy is invalid'
            USING ERRCODE='23514';
    END IF;
    SELECT reservation.id INTO admitted_id
      FROM public.research_run_llm_spend_reservations reservation
     WHERE reservation.run_snapshot_id=requested_run_snapshot_id
       AND reservation.stage='synthesis'
       AND reservation.round_ordinal=requested_round_ordinal;
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
            RAISE EXCEPTION '125: correction LLM admission replay differs'
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
        RAISE EXCEPTION '125: correction price ceiling is unavailable'
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
        RAISE EXCEPTION '125: correction LLM quota admission denied'
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
        requested_run_snapshot_id,'synthesis',requested_round_ordinal,
        requested_subject_id,requested_attempt_key,requested_request_digest,
        requested_trace_id,stage_model,
        encode(sha256(convert_to(stage_system_prompt,'UTF8')),'hex'),
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
REVOKE ALL ON FUNCTION admit_research_run_llm_spend_v6(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
) FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION admit_research_run_llm_spend_cap_v6(
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
        RAISE EXCEPTION '125: LLM admission capability differs'
            USING ERRCODE='42501';
    END IF;
    RETURN QUERY SELECT * FROM public.admit_research_run_llm_spend_v6(
        requested_tenant_id,requested_user_id,requested_task_id,
        requested_run_snapshot_id,requested_stage,requested_round_ordinal,
        requested_subject_id,requested_attempt_key,requested_request_digest,
        requested_trace_id,requested_user_prompt);
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION admit_research_run_llm_spend_cap_v6(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION admit_research_run_llm_spend_cap_v6(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
) TO vane_research_v3_executor;
REVOKE ALL ON FUNCTION admit_research_run_llm_spend_cap_v5(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
) FROM vane_research_v3_executor;

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE task_run_snapshots,research_brief_syntheses,
           research_brief_grounding_verifications,
           research_brief_grounding_corrections,
           research_run_llm_spend_reservations IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.research_brief_grounding_corrections) OR
       EXISTS (
           SELECT 1 FROM public.research_run_llm_spend_reservations
            WHERE stage='synthesis' AND round_ordinal IN (2,3)
       ) OR EXISTS (
           SELECT 1 FROM public.task_run_snapshots snapshot
            WHERE snapshot.reference_schema_version='vane.research-run-snapshot-ref/v3'
              AND convert_from(snapshot.payload,'UTF8')::jsonb
                    #>> '{research_model,synthesis,renderer_version}'=
                  'research-synthesis.render/v3.6'
       ) THEN
        RAISE EXCEPTION '125 down refused: v3.6 correction history exists';
    END IF;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION admit_research_run_llm_spend_cap_v6(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
) FROM vane_research_v3_executor;
GRANT EXECUTE ON FUNCTION admit_research_run_llm_spend_cap_v5(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
) TO vane_research_v3_executor;
DROP FUNCTION admit_research_run_llm_spend_cap_v6(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
);
DROP FUNCTION admit_research_run_llm_spend_v6(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
);

DROP TRIGGER research_run_llm_spend_reservation_v1
    ON research_run_llm_spend_reservations;
CREATE TRIGGER research_run_llm_spend_reservation_v1
BEFORE INSERT ON research_run_llm_spend_reservations
FOR EACH ROW EXECUTE FUNCTION enforce_research_run_llm_spend_reservation_v2();
DROP FUNCTION enforce_research_run_llm_spend_reservation_v3();

ALTER TABLE research_run_llm_spend_reservations
    DROP CONSTRAINT ck_research_llm_spend_reservation_stage;
ALTER TABLE research_run_llm_spend_reservations
    ADD CONSTRAINT ck_research_llm_spend_reservation_stage CHECK (
        (stage='planner' AND subject_id=0 AND
         counts_against_planner_budget AND reserved_planner_tokens=reserved_quota_tokens) OR
        (stage='synthesis' AND subject_id>0 AND
         NOT counts_against_planner_budget AND reserved_planner_tokens=0 AND
         round_ordinal IN (0,1))
    );

DROP TRIGGER research_brief_grounding_finalization_v36 ON research_brief_syntheses;
DROP FUNCTION enforce_research_grounding_finalization_v36();

-- Restore migration 119's round-0-only receipt admission before removing the
-- correction relation referenced by the v125 replacement.
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

DROP FUNCTION research_correction_has_exact_candidate_receipt_v125(BIGINT);
DROP TRIGGER enforce_research_grounding_correction_insert_v125
    ON research_brief_grounding_corrections;
DROP FUNCTION enforce_research_grounding_correction_insert_v125();
DROP FUNCTION research_expected_grounding_correction_prompt_v125(BIGINT);
DROP FUNCTION research_grounding_has_exact_receipt_v125(BIGINT,TEXT);
DROP FUNCTION research_grounding_verdict_valid_v125(BYTEA,BYTEA,TEXT);
DROP TRIGGER enforce_research_grounding_insert_v125
    ON research_brief_grounding_verifications;
DROP FUNCTION enforce_research_grounding_insert_v125();
DROP TRIGGER protect_research_brief_grounding_correction_v1
    ON research_brief_grounding_corrections;
DROP FUNCTION protect_research_brief_grounding_correction_v1();
DROP FUNCTION research_expected_grounding_verifier_prompt_v125(
    BIGINT,BYTEA,TEXT);
DROP FUNCTION research_brief_candidate_valid_v125(BIGINT,BYTEA);
DROP FUNCTION research_brief_matches_completion_v125(BYTEA,TEXT);
DROP FUNCTION research_grounding_correction_citations_subset_v125(BYTEA,BYTEA);

DROP TRIGGER research_scope_window_v33 ON research_brief_syntheses;
CREATE TRIGGER research_scope_window_v33
BEFORE INSERT OR UPDATE ON research_brief_syntheses
FOR EACH ROW EXECUTE FUNCTION enforce_research_scope_window_v33();
DROP FUNCTION enforce_research_scope_window_v36();

DROP TABLE research_brief_grounding_corrections;
