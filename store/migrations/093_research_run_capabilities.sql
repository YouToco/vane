-- 093: per-run bearer capabilities for the restricted V3 research runtime.
--
-- app.tenant_id/app.user_id are caller-controlled PostgreSQL GUCs.  They
-- remain as compatibility filters, but are no longer an authority boundary:
-- every V3 row also has to match a control-plane-issued, exact-snapshot
-- capability.  Only SHA-256(bearer) is stored.  The bearer is derived again
-- from the immutable Temporal-safe snapshot reference inside an Activity.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);

CREATE TABLE research_run_capabilities (
    id                    BIGSERIAL   PRIMARY KEY,
    run_snapshot_id       BIGINT      NOT NULL
        REFERENCES task_run_snapshots(id) ON DELETE CASCADE,
    tenant_id             BIGINT      NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id               BIGINT      NOT NULL REFERENCES users(id),
    task_id               TEXT        NOT NULL,
    temporal_workflow_id  TEXT        NOT NULL,
    temporal_run_id       TEXT        NOT NULL,
    reference_digest      TEXT        NOT NULL,
    key_id                TEXT        NOT NULL,
    generation            INTEGER     NOT NULL,
    capability_hash       BYTEA       NOT NULL,
    issued_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    not_after             TIMESTAMPTZ NOT NULL,
    revoked_at            TIMESTAMPTZ,

    CONSTRAINT uq_research_run_capability_generation
        UNIQUE (run_snapshot_id,generation),
    CONSTRAINT uq_research_run_capability_hash UNIQUE (capability_hash),
    CONSTRAINT ck_research_run_capability_identity CHECK (
        btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255 AND
        btrim(temporal_workflow_id)=temporal_workflow_id AND
        octet_length(temporal_workflow_id) BETWEEN 1 AND 512 AND
        btrim(temporal_run_id)=temporal_run_id AND
        octet_length(temporal_run_id) BETWEEN 1 AND 512 AND
        btrim(key_id)=key_id AND octet_length(key_id) BETWEEN 1 AND 64 AND
        key_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$' AND generation>0
    ),
    CONSTRAINT ck_research_run_capability_digest CHECK (
        reference_digest ~ '^[0-9a-f]{64}$' AND
        octet_length(capability_hash)=32
    ),
    CONSTRAINT ck_research_run_capability_lifetime CHECK (
        not_after>issued_at AND (revoked_at IS NULL OR revoked_at>=issued_at)
    )
);

CREATE INDEX idx_research_run_capability_snapshot_active
    ON research_run_capabilities(run_snapshot_id,generation DESC)
    WHERE revoked_at IS NULL;

REVOKE ALL ON research_run_capabilities FROM PUBLIC,vane_research_runtime,
    vane_research_v3_executor;
REVOKE ALL ON SEQUENCE research_run_capabilities_id_seq
    FROM PUBLIC,vane_research_runtime,vane_research_v3_executor;

-- The runtime may inspect only the scope selected by possession of a valid
-- bearer.  Malformed or absent GUC values deliberately return zero rows.
-- +goose StatementBegin
CREATE FUNCTION current_research_run_capability_v1()
RETURNS TABLE (
    capability_id BIGINT,
    run_snapshot_id BIGINT,
    tenant_id BIGINT,
    user_id BIGINT,
    task_id TEXT,
    temporal_workflow_id TEXT,
    temporal_run_id TEXT,
    reference_digest TEXT
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    bearer_hex TEXT;
BEGIN
    bearer_hex := current_setting('app.research_run_capability_v1',true);
    IF bearer_hex IS NULL OR bearer_hex !~ '^[0-9a-f]{64}$' THEN
        RETURN;
    END IF;
    RETURN QUERY
    SELECT capability.id,capability.run_snapshot_id,capability.tenant_id,
           capability.user_id,capability.task_id,
           capability.temporal_workflow_id,capability.temporal_run_id,
           capability.reference_digest
      FROM public.research_run_capabilities capability
      JOIN public.task_run_snapshots snapshot
        ON snapshot.id=capability.run_snapshot_id
       AND snapshot.tenant_id=capability.tenant_id
       AND snapshot.user_id=capability.user_id
       AND snapshot.task_id=capability.task_id
       AND snapshot.temporal_workflow_id=capability.temporal_workflow_id
       AND snapshot.temporal_run_id=capability.temporal_run_id
       AND snapshot.reference_digest=capability.reference_digest
       AND snapshot.reference_schema_version='vane.research-run-snapshot-ref/v3'
       AND snapshot.execution_mode='discover_at_run'
      JOIN public.tenants tenant
        ON tenant.id=capability.tenant_id
       AND tenant.status='active' AND tenant.deleted_at IS NULL
      JOIN public.memberships membership
        ON membership.tenant_id=capability.tenant_id
       AND membership.user_id=capability.user_id
     WHERE capability.capability_hash=sha256(decode(bearer_hex,'hex'))
       AND capability.revoked_at IS NULL
       AND capability.not_after>statement_timestamp()
     LIMIT 1;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION current_research_run_capability_v1() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION current_research_run_capability_v1()
    TO vane_research_v3_executor;

-- One predicate serves the role-specific RESTRICTIVE policies. NULL means the
-- relation has no such dimension; the capability itself always remains bound
-- to one exact snapshot and run.
-- +goose StatementBegin
CREATE FUNCTION research_run_capability_allows_v1(
    row_tenant_id BIGINT,
    row_user_id BIGINT,
    row_task_id TEXT,
    row_snapshot_id BIGINT,
    row_temporal_run_id TEXT
) RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
    SELECT EXISTS (
        SELECT 1 FROM public.current_research_run_capability_v1() capability
         WHERE capability.tenant_id=row_tenant_id
           AND (row_user_id IS NULL OR capability.user_id=row_user_id)
           AND (row_task_id IS NULL OR capability.task_id=row_task_id)
           AND (row_snapshot_id IS NULL OR capability.run_snapshot_id=row_snapshot_id)
           AND (row_temporal_run_id IS NULL OR
                capability.temporal_run_id=row_temporal_run_id)
    )
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION research_run_capability_allows_v1(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION research_run_capability_allows_v1(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT
) TO vane_research_v3_executor;

-- SECURITY DEFINER entry points call this exact-scope assertion before doing
-- any work. It is intentionally separate from app.tenant_id/app.user_id.
-- +goose StatementBegin
CREATE FUNCTION require_research_run_capability_v1(
    expected_run_snapshot_id BIGINT,
    expected_reference_digest TEXT,
    expected_tenant_id BIGINT,
    expected_user_id BIGINT,
    expected_task_id TEXT,
    expected_temporal_workflow_id TEXT,
    expected_temporal_run_id TEXT
) RETURNS VOID
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.current_research_run_capability_v1() capability
         WHERE capability.run_snapshot_id=expected_run_snapshot_id
           AND capability.reference_digest=expected_reference_digest
           AND capability.tenant_id=expected_tenant_id
           AND capability.user_id=expected_user_id
           AND capability.task_id=expected_task_id
           AND capability.temporal_workflow_id=expected_temporal_workflow_id
           AND capability.temporal_run_id=expected_temporal_run_id
    ) THEN
        RAISE EXCEPTION '093: research run capability is unavailable'
            USING ERRCODE='42501';
    END IF;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION require_research_run_capability_v1(
    BIGINT,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION require_research_run_capability_v1(
    BIGINT,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT
) TO vane_research_v3_executor;

-- Capability-aware wrappers are the only SECURITY DEFINER calls exposed to
-- the executor. The retained originals continue serving legacy vane_app.
-- +goose StatementBegin
CREATE FUNCTION authorize_research_manual_task_run_cap_v1(
    expected_tenant_id BIGINT,
    expected_user_id BIGINT,
    expected_task_id TEXT,
    expected_workflow_id TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.current_research_run_capability_v1() capability
         WHERE capability.tenant_id=expected_tenant_id
           AND capability.user_id=expected_user_id
           AND capability.task_id=expected_task_id
           AND capability.temporal_workflow_id=expected_workflow_id
    ) THEN
        RAISE EXCEPTION '093: manual run capability differs' USING ERRCODE='42501';
    END IF;
    RETURN public.authorize_manual_task_run_v1(
        expected_tenant_id,expected_user_id,expected_task_id,expected_workflow_id);
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION authorize_research_manual_task_run_cap_v1(
    BIGINT,BIGINT,TEXT,TEXT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION authorize_research_manual_task_run_cap_v1(
    BIGINT,BIGINT,TEXT,TEXT
) TO vane_research_v3_executor;
REVOKE ALL ON FUNCTION authorize_manual_task_run_v1(
    BIGINT,BIGINT,TEXT,TEXT
) FROM vane_research_v3_executor;

-- +goose StatementBegin
CREATE FUNCTION admit_research_run_llm_spend_cap_v3(
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
        RAISE EXCEPTION '093: LLM admission capability differs'
            USING ERRCODE='42501';
    END IF;
    RETURN QUERY SELECT * FROM public.admit_research_run_llm_spend_v3(
        requested_tenant_id,requested_user_id,requested_task_id,
        requested_run_snapshot_id,requested_stage,requested_round_ordinal,
        requested_subject_id,requested_attempt_key,requested_request_digest,
        requested_trace_id,requested_user_prompt);
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION admit_research_run_llm_spend_cap_v3(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION admit_research_run_llm_spend_cap_v3(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
) TO vane_research_v3_executor;
REVOKE ALL ON FUNCTION admit_research_run_llm_spend_v3(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
) FROM vane_research_v3_executor;

-- +goose StatementBegin
CREATE FUNCTION read_research_history_cap_v3(
    target_tenant_id BIGINT,
    target_user_id BIGINT,
    target_task_id TEXT,
    current_snapshot_id BIGINT,
    current_plan_id BIGINT
)
RETURNS TABLE (
    kind TEXT, record_id TEXT, run_snapshot_id BIGINT,
    generated_at TIMESTAMPTZ, digest TEXT, coverage TEXT,
    payload_text TEXT, gap_reason TEXT, context_stored_size INTEGER,
    context_visible_size INTEGER, context_visible_digest TEXT,
    context_truncated BOOLEAN, candidate_count BIGINT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.current_research_run_capability_v1() capability
         WHERE capability.run_snapshot_id=current_snapshot_id
           AND capability.tenant_id=target_tenant_id
           AND capability.user_id=target_user_id
           AND capability.task_id=target_task_id
    ) THEN
        RAISE EXCEPTION '093: history capability differs' USING ERRCODE='42501';
    END IF;
    RETURN QUERY SELECT * FROM public.read_research_history_v3(
        target_tenant_id,target_user_id,target_task_id,
        current_snapshot_id,current_plan_id);
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION read_research_history_cap_v3(
    BIGINT,BIGINT,TEXT,BIGINT,BIGINT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION read_research_history_cap_v3(
    BIGINT,BIGINT,TEXT,BIGINT,BIGINT
) TO vane_research_v3_executor;
REVOKE ALL ON FUNCTION read_research_history_v3(
    BIGINT,BIGINT,TEXT,BIGINT,BIGINT
) FROM vane_research_v3_executor;

-- +goose StatementBegin
CREATE FUNCTION read_research_history_content_cap_v3(
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
    record_id TEXT, chunk_offset_chars INTEGER, next_offset_chars INTEGER,
    total_chars INTEGER, total_bytes INTEGER, chunk_text TEXT,
    chunk_digest TEXT, full_digest TEXT, complete BOOLEAN
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM public.current_research_run_capability_v1() capability
          JOIN public.research_brief_syntheses synthesis
            ON synthesis.id=target_synthesis_id
           AND synthesis.run_snapshot_id=capability.run_snapshot_id
           AND synthesis.tenant_id=capability.tenant_id
           AND synthesis.user_id=capability.user_id
           AND synthesis.task_id=capability.task_id
         WHERE capability.tenant_id=target_tenant_id
           AND capability.user_id=target_user_id
           AND capability.task_id=target_task_id
    ) THEN
        RAISE EXCEPTION '093: history cursor capability differs'
            USING ERRCODE='42501';
    END IF;
    RETURN QUERY SELECT * FROM public.read_research_history_content_v3(
        target_tenant_id,target_user_id,target_task_id,target_synthesis_id,
        target_request_digest,target_record_id,offset_chars,limit_chars);
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION read_research_history_content_cap_v3(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,INTEGER,INTEGER
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION read_research_history_content_cap_v3(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,INTEGER,INTEGER
) TO vane_research_v3_executor;
REVOKE ALL ON FUNCTION read_research_history_content_v3(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,INTEGER,INTEGER
) FROM vane_research_v3_executor;

-- Caller-selected tenant/user settings remain an additional filter, never the
-- sole authority. These policies cover every relation reachable by the V3
-- executor. Global provider prices remain read-only and intentionally global.
CREATE POLICY research_v3_capability_scope ON tenants AS RESTRICTIVE
    FOR ALL TO vane_research_v3_executor
    USING (research_run_capability_allows_v1(id,NULL,NULL,NULL,NULL))
    WITH CHECK (false);
CREATE POLICY research_v3_capability_scope ON memberships AS RESTRICTIVE
    FOR ALL TO vane_research_v3_executor
    USING (research_run_capability_allows_v1(tenant_id,user_id,NULL,NULL,NULL))
    WITH CHECK (false);
CREATE POLICY research_v3_capability_scope ON schedules AS RESTRICTIVE
    FOR ALL TO vane_research_v3_executor
    USING (research_run_capability_allows_v1(tenant_id,user_id,id,NULL,NULL))
    WITH CHECK (research_run_capability_allows_v1(tenant_id,user_id,id,NULL,NULL));
CREATE POLICY research_v3_capability_scope ON task_approved_definition_versions AS RESTRICTIVE
    FOR ALL TO vane_research_v3_executor
    USING (research_run_capability_allows_v1(tenant_id,user_id,task_id,NULL,NULL))
    WITH CHECK (research_run_capability_allows_v1(tenant_id,user_id,task_id,NULL,NULL));
CREATE POLICY research_v3_capability_scope ON task_run_snapshots AS RESTRICTIVE
    FOR ALL TO vane_research_v3_executor
    USING (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,id,temporal_run_id))
    WITH CHECK (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,id,temporal_run_id));
CREATE POLICY research_v3_capability_scope ON research_run_plans AS RESTRICTIVE
    FOR ALL TO vane_research_v3_executor
    USING (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,run_snapshot_id,temporal_run_id))
    WITH CHECK (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,run_snapshot_id,temporal_run_id));
CREATE POLICY research_v3_capability_scope ON research_run_steps AS RESTRICTIVE
    FOR ALL TO vane_research_v3_executor
    USING (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,NULL,temporal_run_id))
    WITH CHECK (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,NULL,temporal_run_id));
CREATE POLICY research_v3_capability_scope ON research_run_evidence AS RESTRICTIVE
    FOR ALL TO vane_research_v3_executor
    USING (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,NULL,temporal_run_id))
    WITH CHECK (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,NULL,temporal_run_id));
CREATE POLICY research_v3_capability_scope ON research_brief_syntheses AS RESTRICTIVE
    FOR ALL TO vane_research_v3_executor
    USING (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,run_snapshot_id,temporal_run_id))
    WITH CHECK (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,run_snapshot_id,temporal_run_id));
CREATE POLICY research_v3_capability_scope ON research_run_step_spend_reservations AS RESTRICTIVE
    FOR ALL TO vane_research_v3_executor
    USING (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,run_snapshot_id,temporal_run_id))
    WITH CHECK (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,run_snapshot_id,temporal_run_id));
CREATE POLICY research_v3_capability_scope ON research_run_step_spend_settlements AS RESTRICTIVE
    FOR ALL TO vane_research_v3_executor
    USING (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,run_snapshot_id,temporal_run_id))
    WITH CHECK (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,run_snapshot_id,temporal_run_id));
CREATE POLICY research_v3_capability_scope ON research_run_llm_spend_reservations AS RESTRICTIVE
    FOR ALL TO vane_research_v3_executor
    USING (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,run_snapshot_id,NULL))
    WITH CHECK (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,run_snapshot_id,NULL));
CREATE POLICY research_v3_capability_scope ON research_run_llm_spend_settlements AS RESTRICTIVE
    FOR ALL TO vane_research_v3_executor
    USING (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,run_snapshot_id,NULL))
    WITH CHECK (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,run_snapshot_id,NULL));
CREATE POLICY research_v3_capability_scope ON tool_calls AS RESTRICTIVE
    FOR ALL TO vane_research_v3_executor
    USING (research_run_capability_allows_v1(
        tenant_id,user_id,NULL,run_snapshot_id,NULL))
    WITH CHECK (research_run_capability_allows_v1(
        tenant_id,user_id,NULL,run_snapshot_id,NULL));
CREATE POLICY research_v3_capability_scope ON llm_calls AS RESTRICTIVE
    FOR ALL TO vane_research_v3_executor
    USING (research_run_capability_allows_v1(
        tenant_id,user_id,NULL,run_snapshot_id,NULL))
    WITH CHECK (research_run_capability_allows_v1(
        tenant_id,user_id,NULL,run_snapshot_id,NULL));

-- Snapshot creation is a control-plane operation from this migration onward.
REVOKE INSERT ON task_run_snapshots FROM vane_research_v3_executor;
REVOKE USAGE,SELECT ON SEQUENCE task_run_snapshots_id_seq
    FROM vane_research_v3_executor;

-- The 090 standalone primitive can be called repeatedly without producing a
-- step reservation. Keep Tool execution dark until a later migration replaces
-- it with one atomic capability-bound step admission function.
REVOKE ALL ON FUNCTION reserve_research_run_quota_v3(
    BIGINT,TEXT,DOUBLE PRECISION
) FROM vane_research_v3_executor;

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);

-- Restoring the old GUC-only authority while any capability has been issued is
-- an unsafe downgrade and therefore refused.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM research_run_capabilities) THEN
        RAISE EXCEPTION '093: refusing downgrade while run capabilities exist';
    END IF;
END
$$;
-- +goose StatementEnd

GRANT EXECUTE ON FUNCTION reserve_research_run_quota_v3(
    BIGINT,TEXT,DOUBLE PRECISION
) TO vane_research_v3_executor;
GRANT USAGE,SELECT ON SEQUENCE task_run_snapshots_id_seq
    TO vane_research_v3_executor;
GRANT INSERT (
    id,tenant_id,user_id,task_id,temporal_workflow_id,temporal_run_id,
    run_kind,execution_mode,adaptive_version,capability_catalog_digest,
    tool_policy_digest,prompt_policy_digest,model_policy_digest,
    quota_policy_digest,definition_digest,plan_digest,payload_digest,
    reference_digest,reference_schema_version,payload,budget,created_at
) ON task_run_snapshots TO vane_research_v3_executor;

DROP POLICY research_v3_capability_scope ON llm_calls;
DROP POLICY research_v3_capability_scope ON tool_calls;
DROP POLICY research_v3_capability_scope ON research_run_llm_spend_settlements;
DROP POLICY research_v3_capability_scope ON research_run_llm_spend_reservations;
DROP POLICY research_v3_capability_scope ON research_run_step_spend_settlements;
DROP POLICY research_v3_capability_scope ON research_run_step_spend_reservations;
DROP POLICY research_v3_capability_scope ON research_brief_syntheses;
DROP POLICY research_v3_capability_scope ON research_run_evidence;
DROP POLICY research_v3_capability_scope ON research_run_steps;
DROP POLICY research_v3_capability_scope ON research_run_plans;
DROP POLICY research_v3_capability_scope ON task_run_snapshots;
DROP POLICY research_v3_capability_scope ON task_approved_definition_versions;
DROP POLICY research_v3_capability_scope ON schedules;
DROP POLICY research_v3_capability_scope ON memberships;
DROP POLICY research_v3_capability_scope ON tenants;

GRANT EXECUTE ON FUNCTION authorize_manual_task_run_v1(
    BIGINT,BIGINT,TEXT,TEXT
) TO vane_research_v3_executor;
GRANT EXECUTE ON FUNCTION admit_research_run_llm_spend_v3(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
) TO vane_research_v3_executor;
GRANT EXECUTE ON FUNCTION read_research_history_v3(
    BIGINT,BIGINT,TEXT,BIGINT,BIGINT
) TO vane_research_v3_executor;
GRANT EXECUTE ON FUNCTION read_research_history_content_v3(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,INTEGER,INTEGER
) TO vane_research_v3_executor;

DROP FUNCTION read_research_history_content_cap_v3(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,INTEGER,INTEGER
);
DROP FUNCTION read_research_history_cap_v3(
    BIGINT,BIGINT,TEXT,BIGINT,BIGINT
);
DROP FUNCTION admit_research_run_llm_spend_cap_v3(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
);
DROP FUNCTION authorize_research_manual_task_run_cap_v1(
    BIGINT,BIGINT,TEXT,TEXT
);

REVOKE ALL ON FUNCTION require_research_run_capability_v1(
    BIGINT,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT
) FROM vane_research_v3_executor;
DROP FUNCTION require_research_run_capability_v1(
    BIGINT,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT
);
REVOKE ALL ON FUNCTION research_run_capability_allows_v1(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT
) FROM vane_research_v3_executor;
DROP FUNCTION research_run_capability_allows_v1(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT
);
REVOKE ALL ON FUNCTION current_research_run_capability_v1()
    FROM vane_research_v3_executor;
DROP FUNCTION current_research_run_capability_v1();

DROP TABLE research_run_capabilities;
