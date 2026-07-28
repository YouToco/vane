-- 072: task-scoped periodic Brief settings, durable intents and immutable reports.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE schedules,brief_snapshots,task_run_outcomes IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname='vane_periodic_brief_writer'
    ) THEN
        CREATE ROLE vane_periodic_brief_writer
            NOLOGIN NOINHERIT NOCREATEDB NOCREATEROLE
            NOSUPERUSER NOREPLICATION NOBYPASSRLS;
    END IF;
END $$;
-- +goose StatementEnd

ALTER ROLE vane_periodic_brief_writer
    NOLOGIN NOINHERIT NOCREATEDB NOCREATEROLE
    NOSUPERUSER NOREPLICATION NOBYPASSRLS;
ALTER ROLE vane_periodic_brief_writer RESET ALL;
ALTER ROLE vane_periodic_brief_writer
    SET search_path=pg_catalog,public,pg_temp;
GRANT vane_periodic_brief_writer TO CURRENT_USER;

CREATE TABLE brief_report_settings (
    tenant_id              BIGINT      NOT NULL,
    user_id                BIGINT      NOT NULL,
    task_id                TEXT        NOT NULL,
    mode                   TEXT        NOT NULL DEFAULT 'auto',
    cadence                TEXT        NOT NULL DEFAULT 'weekly',
    delivery               TEXT        NOT NULL DEFAULT 'important',
    auto_candidate         TEXT,
    auto_candidate_streak  INTEGER     NOT NULL DEFAULT 0,
    auto_evaluated_at      TIMESTAMPTZ,
    cadence_changed_at     TIMESTAMPTZ,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id,user_id,task_id),
    CONSTRAINT fk_brief_report_settings_task
        FOREIGN KEY (tenant_id,user_id,task_id)
        REFERENCES schedules (tenant_id,user_id,id) ON DELETE CASCADE,
    CONSTRAINT ck_brief_report_settings_mode
        CHECK (mode IN ('auto','manual')),
    CONSTRAINT ck_brief_report_settings_cadence
        CHECK (cadence IN ('daily','weekly','monthly')),
    CONSTRAINT ck_brief_report_settings_delivery
        CHECK (delivery IN ('important','always','web_only')),
    CONSTRAINT ck_brief_report_settings_candidate
        CHECK (auto_candidate IS NULL OR
               auto_candidate IN ('daily','weekly','monthly')),
    CONSTRAINT ck_brief_report_settings_streak
        CHECK (auto_candidate_streak BETWEEN 0 AND 2)
);

CREATE TABLE periodic_brief_intents (
    id                    BIGSERIAL   PRIMARY KEY,
    tenant_id             BIGINT      NOT NULL,
    user_id               BIGINT      NOT NULL,
    task_id               TEXT        NOT NULL,
    cadence               TEXT        NOT NULL,
    timezone              TEXT        NOT NULL,
    period_start          TIMESTAMPTZ NOT NULL,
    period_end            TIMESTAMPTZ NOT NULL,
    workflow_id           TEXT        NOT NULL,
    temporal_run_id       TEXT,
    input_brief_ids       BIGINT[]    NOT NULL DEFAULT '{}',
    input_digest          TEXT        NOT NULL,
    run_outcome_ids       BIGINT[]    NOT NULL DEFAULT '{}',
    outcome_digest        TEXT        NOT NULL,
    source_coverage       TEXT        NOT NULL,
    processing            TEXT        NOT NULL,
    partial_reason        TEXT,
    status                TEXT        NOT NULL DEFAULT 'prepared',
    created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    started_at            TIMESTAMPTZ,
    finalized_at          TIMESTAMPTZ,
    CONSTRAINT uq_periodic_brief_period
        UNIQUE (tenant_id,user_id,task_id,cadence,period_start,period_end),
    CONSTRAINT uq_periodic_brief_workflow UNIQUE (workflow_id),
    CONSTRAINT fk_periodic_brief_task
        FOREIGN KEY (tenant_id,user_id,task_id)
        REFERENCES schedules (tenant_id,user_id,id) ON DELETE RESTRICT,
    CONSTRAINT ck_periodic_brief_cadence
        CHECK (cadence IN ('daily','weekly','monthly')),
    CONSTRAINT ck_periodic_brief_timezone
        CHECK (btrim(timezone)=timezone AND octet_length(timezone) BETWEEN 1 AND 255),
    CONSTRAINT ck_periodic_brief_period CHECK (period_start<period_end),
    CONSTRAINT ck_periodic_brief_digests CHECK (
        input_digest ~ '^[0-9a-f]{64}$' AND
        outcome_digest ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_periodic_brief_limits CHECK (
        cardinality(input_brief_ids)<=20 AND
        cardinality(run_outcome_ids)<=2048
    ),
    CONSTRAINT ck_periodic_brief_coverage
        CHECK (source_coverage IN ('complete','partial')),
    CONSTRAINT ck_periodic_brief_processing
        CHECK (processing IN ('complete','partial')),
    CONSTRAINT ck_periodic_brief_intent_status
        CHECK (status IN ('prepared','running','finalized','fallback')),
    CONSTRAINT ck_periodic_brief_intent_shape CHECK (
        (status='prepared' AND started_at IS NULL AND finalized_at IS NULL) OR
        (status='running' AND started_at IS NOT NULL AND finalized_at IS NULL) OR
        (status IN ('finalized','fallback') AND finalized_at IS NOT NULL)
    )
);

CREATE INDEX idx_periodic_brief_intents_keyset
    ON periodic_brief_intents (
        tenant_id,user_id,task_id,period_end DESC,id DESC
    );

ALTER TABLE periodic_brief_intents
    ADD CONSTRAINT uq_periodic_brief_intent_scope
    UNIQUE (id,tenant_id,user_id,task_id);

CREATE TABLE periodic_synthesis_receipts (
    intent_id            BIGINT      PRIMARY KEY
        REFERENCES periodic_brief_intents(id) ON DELETE RESTRICT,
    tenant_id            BIGINT      NOT NULL,
    user_id              BIGINT      NOT NULL,
    task_id              TEXT        NOT NULL,
    request_digest       TEXT        NOT NULL,
    profile_epoch        BIGINT      NOT NULL,
    profile_version      BIGINT      NOT NULL,
    profile_digest       TEXT        NOT NULL,
    input_digest         TEXT        NOT NULL,
    status               TEXT        NOT NULL DEFAULT 'prepared',
    generation_mode      TEXT,
    content_payload      BYTEA,
    content_digest       TEXT,
    spending_started_at  TIMESTAMPTZ,
    finalized_at         TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_periodic_synthesis_request
        UNIQUE (tenant_id,user_id,request_digest),
    CONSTRAINT fk_periodic_synthesis_scope
        FOREIGN KEY (intent_id,tenant_id,user_id,task_id)
        REFERENCES periodic_brief_intents (id,tenant_id,user_id,task_id),
    CONSTRAINT ck_periodic_synthesis_digest
        CHECK (request_digest ~ '^[0-9a-f]{64}$' AND
               profile_digest ~ '^[0-9a-f]{64}$' AND
               input_digest ~ '^[0-9a-f]{64}$' AND
               (content_digest IS NULL OR content_digest ~ '^[0-9a-f]{64}$')),
    CONSTRAINT ck_periodic_synthesis_profile
        CHECK (profile_epoch>=0 AND profile_version>=0),
    CONSTRAINT ck_periodic_synthesis_status
        CHECK (status IN ('prepared','spending','finalized','fallback')),
    CONSTRAINT ck_periodic_synthesis_generation
        CHECK (generation_mode IS NULL OR
               generation_mode IN ('model','deterministic_fallback')),
    CONSTRAINT ck_periodic_synthesis_payload CHECK (
        content_payload IS NULL OR
        octet_length(content_payload) BETWEEN 2 AND 262144
    ),
    CONSTRAINT ck_periodic_synthesis_payload_digest CHECK (
        content_payload IS NULL OR
        content_digest=encode(sha256(content_payload),'hex')
    ),
    CONSTRAINT ck_periodic_synthesis_shape CHECK (
        (status='prepared' AND generation_mode IS NULL AND
         content_payload IS NULL AND spending_started_at IS NULL AND
         finalized_at IS NULL) OR
        (status='spending' AND generation_mode IS NULL AND
         content_payload IS NULL AND spending_started_at IS NOT NULL AND
         finalized_at IS NULL) OR
        (status='finalized' AND generation_mode='model' AND
         content_payload IS NOT NULL AND spending_started_at IS NOT NULL AND
         finalized_at IS NOT NULL) OR
        (status='fallback' AND generation_mode='deterministic_fallback' AND
         content_payload IS NOT NULL AND finalized_at IS NOT NULL)
    )
);

CREATE TABLE periodic_brief_reports (
    id               BIGSERIAL   PRIMARY KEY,
    intent_id        BIGINT      NOT NULL UNIQUE,
    tenant_id        BIGINT      NOT NULL,
    user_id          BIGINT      NOT NULL,
    task_id          TEXT        NOT NULL,
    cadence          TEXT        NOT NULL,
    period_start     TIMESTAMPTZ NOT NULL,
    period_end       TIMESTAMPTZ NOT NULL,
    schema_version   TEXT        NOT NULL,
    request_digest   TEXT        NOT NULL,
    payload_digest   TEXT        NOT NULL,
    payload          BYTEA       NOT NULL,
    generated_at     TIMESTAMPTZ NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT fk_periodic_report_intent
        FOREIGN KEY (intent_id,tenant_id,user_id,task_id)
        REFERENCES periodic_brief_intents (id,tenant_id,user_id,task_id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_periodic_report_schema
        CHECK (schema_version='vane.periodic-brief/v1'),
    CONSTRAINT ck_periodic_report_cadence
        CHECK (cadence IN ('daily','weekly','monthly')),
    CONSTRAINT ck_periodic_report_period CHECK (period_start<period_end),
    CONSTRAINT ck_periodic_report_request
        CHECK (request_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_periodic_report_payload CHECK (
        octet_length(payload) BETWEEN 2 AND 524288 AND
        payload_digest ~ '^[0-9a-f]{64}$' AND
        payload_digest=(
            convert_from(payload,'UTF8')::jsonb->>'digest'
        )
    ),
    CONSTRAINT uq_periodic_report_period
        UNIQUE (tenant_id,user_id,task_id,cadence,period_start,period_end)
);

CREATE INDEX idx_periodic_brief_reports_keyset
    ON periodic_brief_reports (
        tenant_id,user_id,task_id,cadence,period_end DESC,id DESC
    );

ALTER TABLE periodic_brief_reports
    ADD CONSTRAINT uq_periodic_report_delivery_scope
    UNIQUE (id,tenant_id,user_id,task_id);

CREATE TABLE periodic_report_deliveries (
    report_id             BIGINT      PRIMARY KEY
        REFERENCES periodic_brief_reports(id) ON DELETE RESTRICT,
    tenant_id             BIGINT      NOT NULL,
    user_id               BIGINT      NOT NULL,
    task_id               TEXT        NOT NULL,
    delivery_mode         TEXT        NOT NULL,
    decision_state        TEXT        NOT NULL,
    card_payload          BYTEA       NOT NULL,
    card_digest           TEXT        NOT NULL,
    provider_uuid         UUID        NOT NULL UNIQUE,
    app_identity          TEXT        NOT NULL,
    target_open_id        TEXT        NOT NULL,
    provider_chat_id      TEXT        NOT NULL DEFAULT '',
    status                TEXT        NOT NULL,
    attempt               INTEGER     NOT NULL DEFAULT 0,
    attempt_started_at    TIMESTAMPTZ,
    provider_message_id   TEXT,
    finalized_at          TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT fk_periodic_delivery_scope
        FOREIGN KEY (report_id,tenant_id,user_id,task_id)
        REFERENCES periodic_brief_reports (id,tenant_id,user_id,task_id),
    CONSTRAINT ck_periodic_delivery_mode
        CHECK (delivery_mode IN ('important','always','web_only')),
    CONSTRAINT ck_periodic_delivery_decision
        CHECK (decision_state IN (
            'act','watch','no_action','insufficient_evidence'
        )),
    CONSTRAINT ck_periodic_delivery_card CHECK (
        octet_length(card_payload) BETWEEN 2 AND 6144 AND
        card_digest=encode(sha256(card_payload),'hex')
    ),
    CONSTRAINT ck_periodic_delivery_identity CHECK (
        btrim(app_identity)=app_identity AND octet_length(app_identity) BETWEEN 1 AND 512 AND
        btrim(target_open_id)=target_open_id AND octet_length(target_open_id) BETWEEN 1 AND 512 AND
        octet_length(provider_chat_id)<=512
    ),
    CONSTRAINT ck_periodic_delivery_status
        CHECK (status IN ('prepared','sending','sent','ambiguous','skipped')),
    CONSTRAINT ck_periodic_delivery_shape CHECK (
        (status IN ('prepared','skipped') AND attempt_started_at IS NULL AND
         provider_message_id IS NULL AND
         ((status='prepared' AND finalized_at IS NULL) OR
          (status='skipped' AND finalized_at IS NOT NULL))) OR
        (status='sending' AND attempt_started_at IS NOT NULL AND
         provider_message_id IS NULL AND finalized_at IS NULL) OR
        (status='sent' AND attempt_started_at IS NOT NULL AND
         provider_message_id IS NOT NULL AND finalized_at IS NOT NULL) OR
        (status='ambiguous' AND attempt_started_at IS NOT NULL AND
         provider_message_id IS NULL AND finalized_at IS NOT NULL)
    )
);

CREATE INDEX idx_periodic_report_delivery_recovery
    ON periodic_report_deliveries (status,updated_at,report_id)
    WHERE status IN ('prepared','sending','ambiguous');

-- +goose StatementBegin
CREATE FUNCTION enforce_periodic_brief_intent_transition_v1()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF ROW(
        NEW.id,NEW.tenant_id,NEW.user_id,NEW.task_id,NEW.cadence,
        NEW.timezone,NEW.period_start,NEW.period_end,NEW.workflow_id,
        NEW.input_brief_ids,NEW.input_digest,NEW.run_outcome_ids,
        NEW.outcome_digest,NEW.source_coverage,NEW.processing,
        NEW.partial_reason,NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.id,OLD.tenant_id,OLD.user_id,OLD.task_id,OLD.cadence,
        OLD.timezone,OLD.period_start,OLD.period_end,OLD.workflow_id,
        OLD.input_brief_ids,OLD.input_digest,OLD.run_outcome_ids,
        OLD.outcome_digest,OLD.source_coverage,OLD.processing,
        OLD.partial_reason,OLD.created_at
    ) THEN
        RAISE EXCEPTION '072: periodic Brief intent identity is immutable';
    END IF;
    IF OLD.temporal_run_id IS NOT NULL AND
       NEW.temporal_run_id IS DISTINCT FROM OLD.temporal_run_id THEN
        RAISE EXCEPTION '072: periodic Brief Temporal run is immutable';
    END IF;
    IF NOT (
        (OLD.status='prepared' AND NEW.status IN ('prepared','running')) OR
        (OLD.status='running' AND NEW.status IN ('running','finalized','fallback')) OR
        (OLD.status IN ('finalized','fallback') AND NEW IS NOT DISTINCT FROM OLD)
    ) THEN
        RAISE EXCEPTION '072: periodic Brief intent transition denied';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd

CREATE TRIGGER periodic_brief_intent_transition_v1
BEFORE UPDATE ON periodic_brief_intents
FOR EACH ROW EXECUTE FUNCTION enforce_periodic_brief_intent_transition_v1();

-- +goose StatementBegin
CREATE FUNCTION enforce_periodic_synthesis_transition_v1()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF ROW(
        NEW.intent_id,NEW.tenant_id,NEW.user_id,NEW.task_id,
        NEW.request_digest,NEW.profile_epoch,NEW.profile_version,
        NEW.profile_digest,NEW.input_digest,NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.intent_id,OLD.tenant_id,OLD.user_id,OLD.task_id,
        OLD.request_digest,OLD.profile_epoch,OLD.profile_version,
        OLD.profile_digest,OLD.input_digest,OLD.created_at
    ) THEN
        RAISE EXCEPTION '072: periodic synthesis identity is immutable';
    END IF;
    IF NOT (
        (OLD.status='prepared' AND NEW.status='spending') OR
        (OLD.status='spending' AND NEW.status IN ('finalized','fallback')) OR
        (OLD.status IN ('finalized','fallback') AND NEW IS NOT DISTINCT FROM OLD)
    ) THEN
        RAISE EXCEPTION '072: periodic synthesis transition denied';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd

CREATE TRIGGER periodic_synthesis_transition_v1
BEFORE UPDATE ON periodic_synthesis_receipts
FOR EACH ROW EXECUTE FUNCTION enforce_periodic_synthesis_transition_v1();

-- +goose StatementBegin
CREATE FUNCTION deny_periodic_brief_report_mutation_v1()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        IF current_setting('app.tenant_purge',true)='on' AND
           EXISTS (
               SELECT 1 FROM public.tenants t
                WHERE t.id=OLD.tenant_id
                  AND t.status='deleting'
                  AND t.purge_after IS NOT NULL
                  AND t.purge_after<=clock_timestamp()
           ) THEN
            RETURN OLD;
        END IF;
        RAISE EXCEPTION '072: periodic Brief reports are immutable';
    END IF;
    RAISE EXCEPTION '072: periodic Brief reports are immutable';
END
$$;
-- +goose StatementEnd

CREATE TRIGGER periodic_brief_report_immutable_v1
BEFORE UPDATE OR DELETE ON periodic_brief_reports
FOR EACH ROW EXECUTE FUNCTION deny_periodic_brief_report_mutation_v1();

-- +goose StatementBegin
CREATE FUNCTION enforce_periodic_report_delivery_transition_v1()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF ROW(
        NEW.report_id,NEW.tenant_id,NEW.user_id,NEW.task_id,
        NEW.delivery_mode,NEW.decision_state,NEW.card_payload,
        NEW.card_digest,NEW.provider_uuid,NEW.app_identity,
        NEW.target_open_id,NEW.provider_chat_id,NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.report_id,OLD.tenant_id,OLD.user_id,OLD.task_id,
        OLD.delivery_mode,OLD.decision_state,OLD.card_payload,
        OLD.card_digest,OLD.provider_uuid,OLD.app_identity,
        OLD.target_open_id,OLD.provider_chat_id,OLD.created_at
    ) THEN
        RAISE EXCEPTION '072: periodic report delivery identity is immutable';
    END IF;
    IF NOT (
        (OLD.status='prepared' AND NEW.status IN ('prepared','sending')) OR
        (OLD.status='sending' AND NEW.status IN ('prepared','sent','ambiguous')) OR
        (OLD.status IN ('sent','ambiguous','skipped') AND NEW IS NOT DISTINCT FROM OLD)
    ) THEN
        RAISE EXCEPTION '072: periodic report delivery transition denied';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
CREATE TRIGGER periodic_report_delivery_transition_v1
BEFORE UPDATE ON periodic_report_deliveries
FOR EACH ROW EXECUTE FUNCTION enforce_periodic_report_delivery_transition_v1();

REVOKE ALL ON brief_report_settings,periodic_brief_intents,
    periodic_synthesis_receipts,periodic_brief_reports,
    periodic_report_deliveries
    FROM PUBLIC,vane_app,vane_brief_reader,vane_periodic_brief_writer,
         vane_brief_synthesis_recovery;
REVOKE ALL ON SEQUENCE periodic_brief_intents_id_seq,
    periodic_brief_reports_id_seq
    FROM PUBLIC,vane_app,vane_brief_reader,vane_periodic_brief_writer,
         vane_brief_synthesis_recovery;

GRANT USAGE ON SCHEMA public TO vane_periodic_brief_writer;
GRANT SELECT,INSERT,UPDATE ON brief_report_settings,
    periodic_brief_intents,periodic_synthesis_receipts,
    periodic_report_deliveries
    TO vane_periodic_brief_writer;
GRANT SELECT,INSERT ON periodic_brief_reports
    TO vane_periodic_brief_writer;
GRANT SELECT (
    id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id,
    push_batch_id,schema_version,request_digest,payload,payload_digest,
    insight_count,generated_at
) ON brief_snapshots TO vane_periodic_brief_writer;
GRANT SELECT (
    id,tenant_id,user_id,task_id,run_snapshot_id,status,result,
    source_coverage,processing,finalized_at
) ON task_run_outcomes TO vane_periodic_brief_writer;
GRANT USAGE,SELECT ON SEQUENCE periodic_brief_intents_id_seq,
    periodic_brief_reports_id_seq TO vane_periodic_brief_writer;
GRANT SELECT ON brief_report_settings,periodic_brief_reports
    TO vane_brief_reader;
GRANT SELECT (
    intent_id,tenant_id,user_id,task_id,request_digest,
    profile_epoch,profile_version,profile_digest,input_digest,
    status,generation_mode,content_payload,content_digest,
    spending_started_at,finalized_at,created_at
), UPDATE (
    status,generation_mode,content_payload,content_digest,finalized_at
) ON periodic_synthesis_receipts TO vane_brief_synthesis_recovery;
GRANT SELECT (
    id,tenant_id,user_id,task_id,cadence,timezone,period_start,period_end,
    workflow_id,temporal_run_id,input_brief_ids,input_digest,
    run_outcome_ids,outcome_digest,source_coverage,processing,
    partial_reason,status,created_at,started_at,finalized_at
) ON periodic_brief_intents TO vane_brief_synthesis_recovery;
GRANT UPDATE (status,finalized_at)
    ON periodic_brief_intents TO vane_brief_synthesis_recovery;
GRANT SELECT (intent_id,payload), INSERT (
    id,intent_id,tenant_id,user_id,task_id,cadence,period_start,period_end,
    schema_version,request_digest,payload_digest,payload,generated_at
) ON periodic_brief_reports TO vane_brief_synthesis_recovery;
GRANT USAGE,SELECT ON SEQUENCE periodic_brief_reports_id_seq
    TO vane_brief_synthesis_recovery;
GRANT SELECT (
    id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id,
    push_batch_id,schema_version,request_digest,payload,payload_digest,
    insight_count,generated_at
) ON brief_snapshots TO vane_brief_synthesis_recovery;

ALTER TABLE brief_report_settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE periodic_brief_intents ENABLE ROW LEVEL SECURITY;
ALTER TABLE periodic_synthesis_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE periodic_brief_reports ENABLE ROW LEVEL SECURITY;
ALTER TABLE periodic_report_deliveries ENABLE ROW LEVEL SECURITY;

-- +goose StatementBegin
DO $$
DECLARE table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'brief_report_settings','periodic_brief_intents',
        'periodic_synthesis_receipts','periodic_brief_reports',
        'periodic_report_deliveries'
    ] LOOP
        EXECUTE format(
            'CREATE POLICY tenant_visible ON %I FOR ALL USING (true) WITH CHECK (true)',
            table_name
        );
        EXECUTE format(
            'CREATE POLICY tenant_isolation ON %I AS RESTRICTIVE FOR ALL USING
             (tenant_id IS NOT DISTINCT FROM NULLIF(current_setting(''app.tenant_id'',true),'''')::bigint)
             WITH CHECK
             (tenant_id IS NOT DISTINCT FROM NULLIF(current_setting(''app.tenant_id'',true),'''')::bigint)',
            table_name
        );
        EXECUTE format(
            'CREATE POLICY user_isolation ON %I AS RESTRICTIVE FOR ALL USING
             (user_id IS NOT DISTINCT FROM NULLIF(current_setting(''app.user_id'',true),'''')::bigint)
             WITH CHECK
             (user_id IS NOT DISTINCT FROM NULLIF(current_setting(''app.user_id'',true),'''')::bigint)',
            table_name
        );
    END LOOP;
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION read_periodic_synthesis_recovery_v1(
    after_spending_at TIMESTAMPTZ,
    after_intent_id BIGINT,
    requested_limit INTEGER
)
RETURNS TABLE (
    candidate_at TIMESTAMPTZ,
    recovery_kind TEXT,
    intent_id BIGINT,
    tenant_id BIGINT,
    user_id BIGINT,
    workflow_id TEXT,
    temporal_run_id TEXT,
    request_digest TEXT,
    profile_epoch BIGINT,
    profile_version BIGINT,
    profile_digest TEXT,
    input_digest TEXT
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
    WITH candidates AS (
        SELECT r.spending_started_at AS candidate_at,
               'spending'::text AS recovery_kind,
               r.intent_id,r.tenant_id,r.user_id,i.workflow_id,
               COALESCE(i.temporal_run_id,'') AS temporal_run_id,
               r.request_digest,r.profile_epoch,r.profile_version,
               r.profile_digest,r.input_digest
          FROM public.periodic_synthesis_receipts r
          JOIN public.periodic_brief_intents i ON i.id=r.intent_id
          JOIN public.memberships m
            ON m.tenant_id=i.tenant_id AND m.user_id=i.user_id
          JOIN public.schedules s
            ON s.tenant_id=i.tenant_id AND s.user_id=i.user_id
           AND s.id=i.task_id
          LEFT JOIN public.periodic_brief_reports p ON p.intent_id=r.intent_id
         WHERE r.status='spending'
           AND r.spending_started_at<=clock_timestamp()-interval '2 minutes'
           AND i.status='running'
           AND p.id IS NULL
        UNION ALL
        SELECT i.created_at,'prepared'::text,i.id,i.tenant_id,i.user_id,
               i.workflow_id,COALESCE(i.temporal_run_id,''),
               repeat('0',64),0,0,repeat('0',64),i.input_digest
          FROM public.periodic_brief_intents i
          JOIN public.memberships m
            ON m.tenant_id=i.tenant_id AND m.user_id=i.user_id
          JOIN public.schedules s
            ON s.tenant_id=i.tenant_id AND s.user_id=i.user_id
           AND s.id=i.task_id
          LEFT JOIN public.periodic_synthesis_receipts r
            ON r.intent_id=i.id
          LEFT JOIN public.periodic_brief_reports p ON p.intent_id=i.id
         WHERE i.status='prepared'
           AND i.created_at<=clock_timestamp()-interval '2 minutes'
           AND r.intent_id IS NULL
           AND p.id IS NULL
    )
    SELECT c.candidate_at,c.recovery_kind,c.intent_id,c.tenant_id,c.user_id,
           c.workflow_id,c.temporal_run_id,c.request_digest,c.profile_epoch,
           c.profile_version,c.profile_digest,c.input_digest
      FROM candidates c
     WHERE after_spending_at IS NULL OR
           (c.candidate_at,c.intent_id)>
               (after_spending_at,after_intent_id)
     ORDER BY c.candidate_at,c.intent_id
     LIMIT LEAST(GREATEST(COALESCE(requested_limit,0),0),100)
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION read_periodic_synthesis_recovery_v1(
    TIMESTAMPTZ,BIGINT,INTEGER
) FROM PUBLIC,vane_app,vane_periodic_brief_writer,
       vane_brief_synthesis_recovery;
GRANT EXECUTE ON FUNCTION read_periodic_synthesis_recovery_v1(
    TIMESTAMPTZ,BIGINT,INTEGER
) TO vane_brief_synthesis_recovery;

-- A report commit may succeed even when the Activity completion is lost
-- before Temporal can schedule delivery. Recovery gets only immutable report
-- payloads that still lack a delivery receipt; it receives no table-wide
-- privilege.
-- +goose StatementBegin
CREATE FUNCTION read_periodic_missing_delivery_recovery_v1(
    after_report_id BIGINT,
    requested_limit INTEGER
)
RETURNS TABLE (
    report_id BIGINT,
    payload BYTEA
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
    SELECT p.id,p.payload
      FROM public.periodic_brief_reports p
      JOIN public.memberships m
        ON m.tenant_id=p.tenant_id AND m.user_id=p.user_id
      JOIN public.schedules s
        ON s.tenant_id=p.tenant_id AND s.user_id=p.user_id
       AND s.id=p.task_id
      LEFT JOIN public.periodic_report_deliveries d
        ON d.report_id=p.id
     WHERE d.report_id IS NULL
       AND p.created_at<=clock_timestamp()-interval '2 minutes'
       AND p.id>COALESCE(after_report_id,0)
     ORDER BY p.id
     LIMIT LEAST(GREATEST(COALESCE(requested_limit,0),0),100)
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION read_periodic_missing_delivery_recovery_v1(
    BIGINT,INTEGER
) FROM PUBLIC,vane_app,vane_periodic_brief_writer,
       vane_brief_synthesis_recovery;
GRANT EXECUTE ON FUNCTION read_periodic_missing_delivery_recovery_v1(
    BIGINT,INTEGER
) TO vane_brief_synthesis_recovery;

-- +goose StatementBegin
CREATE FUNCTION read_periodic_delivery_recovery_v1(
    after_updated_at TIMESTAMPTZ,
    after_report_id BIGINT,
    requested_limit INTEGER
)
RETURNS TABLE (
    updated_at TIMESTAMPTZ,
    report_id BIGINT,
    tenant_id BIGINT,
    user_id BIGINT,
    task_id TEXT,
    delivery_mode TEXT,
    decision_state TEXT,
    card_payload BYTEA,
    card_digest TEXT,
    provider_uuid UUID,
    app_identity TEXT,
    target_open_id TEXT,
    provider_chat_id TEXT,
    status TEXT,
    attempt INTEGER,
    attempt_started_at TIMESTAMPTZ,
    provider_message_id TEXT,
    finalized_at TIMESTAMPTZ
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
    SELECT d.updated_at,d.report_id,d.tenant_id,d.user_id,d.task_id,
           d.delivery_mode,d.decision_state,d.card_payload,d.card_digest,
           d.provider_uuid,d.app_identity,d.target_open_id,
           d.provider_chat_id,d.status,d.attempt,d.attempt_started_at,
           d.provider_message_id,d.finalized_at
      FROM public.periodic_report_deliveries d
      JOIN public.memberships m
        ON m.tenant_id=d.tenant_id AND m.user_id=d.user_id
      JOIN public.schedules s
        ON s.tenant_id=d.tenant_id AND s.user_id=d.user_id
       AND s.id=d.task_id
     WHERE d.status IN ('prepared','sending')
       AND d.updated_at<=clock_timestamp()-interval '2 minutes'
       AND (
           after_updated_at IS NULL OR
           (d.updated_at,d.report_id)>(after_updated_at,after_report_id)
       )
     ORDER BY d.updated_at,d.report_id
     LIMIT LEAST(GREATEST(COALESCE(requested_limit,0),0),100)
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION read_periodic_delivery_recovery_v1(
    TIMESTAMPTZ,BIGINT,INTEGER
) FROM PUBLIC,vane_app,vane_periodic_brief_writer,
       vane_brief_synthesis_recovery;
GRANT EXECUTE ON FUNCTION read_periodic_delivery_recovery_v1(
    TIMESTAMPTZ,BIGINT,INTEGER
) TO vane_brief_synthesis_recovery;

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE brief_report_settings,periodic_brief_intents,
    periodic_synthesis_receipts,periodic_brief_reports,
    periodic_report_deliveries
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM brief_report_settings) OR
       EXISTS (SELECT 1 FROM periodic_brief_intents) OR
       EXISTS (SELECT 1 FROM periodic_synthesis_receipts) OR
       EXISTS (SELECT 1 FROM periodic_brief_reports) OR
       EXISTS (SELECT 1 FROM periodic_report_deliveries) THEN
        RAISE EXCEPTION '072: refusing Down while periodic Brief state exists';
    END IF;
END $$;
-- +goose StatementEnd

DROP FUNCTION read_periodic_synthesis_recovery_v1(
    TIMESTAMPTZ,BIGINT,INTEGER
);
DROP FUNCTION read_periodic_missing_delivery_recovery_v1(
    BIGINT,INTEGER
);
DROP FUNCTION read_periodic_delivery_recovery_v1(
    TIMESTAMPTZ,BIGINT,INTEGER
);
DROP TABLE periodic_report_deliveries;
ALTER TABLE periodic_brief_reports
    DROP CONSTRAINT uq_periodic_report_delivery_scope;
DROP TABLE periodic_brief_reports;
DROP TABLE periodic_synthesis_receipts;
DROP TABLE periodic_brief_intents;
DROP TABLE brief_report_settings;
DROP FUNCTION deny_periodic_brief_report_mutation_v1();
DROP FUNCTION enforce_periodic_report_delivery_transition_v1();
DROP FUNCTION enforce_periodic_synthesis_transition_v1();
DROP FUNCTION enforce_periodic_brief_intent_transition_v1();
REVOKE SELECT (
    id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id,
    push_batch_id,schema_version,request_digest,payload,payload_digest,
    insight_count,generated_at
) ON brief_snapshots FROM vane_periodic_brief_writer;
REVOKE SELECT (
    id,tenant_id,user_id,task_id,run_snapshot_id,status,result,
    source_coverage,processing,finalized_at
) ON task_run_outcomes FROM vane_periodic_brief_writer;
REVOKE SELECT (
    id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id,
    push_batch_id,schema_version,request_digest,payload,payload_digest,
    insight_count,generated_at
) ON brief_snapshots FROM vane_brief_synthesis_recovery;
-- The role is cluster-scoped while this Down is database-scoped. Revoke all
-- privileges owned in this database but keep the hardened NOLOGIN role and
-- owner membership for other databases that may still be on migration 072.
DROP OWNED BY vane_periodic_brief_writer;
