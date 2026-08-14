-- 100: immutable, receipt-backed delivery anchor for V3 research Briefs.
--
-- This migration is dark by default.  It adds no scheduler or Workflow
-- authority; it only allows an already-authorized V3 delivery coordinator to
-- bind one finalized Brief to one durable push effect and to atomically seal
-- the provider receipt with the existing effect/delivery/batch settlement.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);

CREATE TABLE research_brief_deliveries (
    id                       BIGSERIAL   PRIMARY KEY,
    tenant_id                BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    user_id                  BIGINT      NOT NULL REFERENCES users (id),
    task_id                  TEXT        NOT NULL,
    run_snapshot_id          BIGINT      NOT NULL REFERENCES task_run_snapshots (id),
    plan_id                  BIGINT      NOT NULL REFERENCES research_run_plans (id),
    brief_id                 BIGINT      NOT NULL REFERENCES research_brief_syntheses (id),
    temporal_workflow_id     TEXT        NOT NULL,
    temporal_run_id          TEXT        NOT NULL,
    brief_reference_digest   TEXT        NOT NULL,
    brief_digest             TEXT        NOT NULL,
    card_digest              TEXT        NOT NULL,
    batch_id                 BIGINT      NOT NULL REFERENCES push_batches (id),
    delivery_id              BIGINT      NOT NULL REFERENCES deliveries (id),
    effect_id                TEXT        NOT NULL REFERENCES push_effects (id),
    schema_version           TEXT        NOT NULL,
    status                   TEXT        NOT NULL DEFAULT 'prepared',
    provider_message_id      TEXT        NOT NULL DEFAULT '',
    receipt_digest           TEXT        NOT NULL DEFAULT '',
    sent_at                  TIMESTAMPTZ,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_research_brief_delivery_brief UNIQUE (brief_id),
    CONSTRAINT uq_research_brief_delivery_effect UNIQUE (effect_id),
    CONSTRAINT uq_research_brief_delivery_projection UNIQUE (batch_id,delivery_id),
    CONSTRAINT uq_research_brief_delivery_scope
        UNIQUE (id,tenant_id,user_id,task_id,run_snapshot_id,plan_id,brief_id),
    CONSTRAINT ck_research_brief_delivery_identity CHECK (
        btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255 AND
        btrim(temporal_workflow_id)=temporal_workflow_id AND
        octet_length(temporal_workflow_id) BETWEEN 1 AND 512 AND
        btrim(temporal_run_id)=temporal_run_id AND
        octet_length(temporal_run_id) BETWEEN 1 AND 512 AND
        btrim(effect_id)=effect_id AND octet_length(effect_id) BETWEEN 1 AND 512
    ),
    CONSTRAINT ck_research_brief_delivery_digests CHECK (
        brief_reference_digest ~ '^[0-9a-f]{64}$' AND
        brief_digest ~ '^[0-9a-f]{64}$' AND
        card_digest ~ '^[0-9a-f]{64}$' AND
        (receipt_digest='' OR receipt_digest ~ '^[0-9a-f]{64}$')
    ),
    CONSTRAINT ck_research_brief_delivery_schema CHECK (
        schema_version='vane.research-brief-delivery/v3'
    ),
    CONSTRAINT ck_research_brief_delivery_status CHECK (
        status IN ('prepared','sent')
    ),
    CONSTRAINT ck_research_brief_delivery_state CHECK (
        (status='prepared' AND provider_message_id='' AND receipt_digest='' AND sent_at IS NULL) OR
        (status='sent' AND octet_length(provider_message_id) BETWEEN 1 AND 512 AND
         receipt_digest<>'' AND sent_at IS NOT NULL)
    )
);

CREATE INDEX idx_research_brief_deliveries_owner_created
    ON research_brief_deliveries (tenant_id,user_id,created_at DESC,id DESC);

ALTER TABLE research_brief_deliveries ENABLE ROW LEVEL SECURITY;
CREATE POLICY research_brief_delivery_visible ON research_brief_deliveries
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY research_brief_delivery_tenant_isolation
    ON research_brief_deliveries AS RESTRICTIVE FOR ALL
    USING (tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint);
CREATE POLICY research_brief_delivery_user_isolation
    ON research_brief_deliveries AS RESTRICTIVE FOR ALL
    USING (user_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint)
    WITH CHECK (user_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint);

REVOKE ALL ON research_brief_deliveries FROM PUBLIC;
GRANT SELECT ON research_brief_deliveries TO vane_app;
GRANT INSERT (
    tenant_id,user_id,task_id,run_snapshot_id,plan_id,brief_id,
    temporal_workflow_id,temporal_run_id,brief_reference_digest,brief_digest,
    card_digest,batch_id,delivery_id,effect_id,schema_version
) ON research_brief_deliveries TO vane_app;
GRANT USAGE,SELECT ON SEQUENCE research_brief_deliveries_id_seq TO vane_app;

GRANT SELECT ON research_brief_deliveries TO vane_push_effect_receipt;
GRANT SELECT ON research_brief_deliveries TO vane_push_effect_coordinator;
GRANT SELECT (payload,created_at,v2_cutover_event_id)
    ON task_run_snapshots TO vane_push_effect_coordinator;
GRANT SELECT (
    id,tenant_id,user_id,task_id,run_snapshot_id,plan_id,
    temporal_workflow_id,temporal_run_id,definition_digest,plan_digest,
    notification_threshold,request_digest,evidence_digest,history_digest,
    status,significance,decision,delivery_required,brief_digest,finalized_at
) ON research_brief_syntheses TO vane_push_effect_coordinator;
GRANT UPDATE (status,provider_message_id,receipt_digest,sent_at,updated_at)
    ON research_brief_deliveries TO vane_push_effect_receipt;

-- All immutable coordinates are frozen before the provider boundary.  The
-- only legal transition is the receipt role's prepared -> sent settlement.
-- +goose StatementBegin
CREATE FUNCTION enforce_research_brief_delivery_transition_v3()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF ROW(
        NEW.id,NEW.tenant_id,NEW.user_id,NEW.task_id,NEW.run_snapshot_id,
        NEW.plan_id,NEW.brief_id,NEW.temporal_workflow_id,NEW.temporal_run_id,
        NEW.brief_reference_digest,NEW.brief_digest,NEW.card_digest,
        NEW.batch_id,NEW.delivery_id,NEW.effect_id,NEW.schema_version,NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.id,OLD.tenant_id,OLD.user_id,OLD.task_id,OLD.run_snapshot_id,
        OLD.plan_id,OLD.brief_id,OLD.temporal_workflow_id,OLD.temporal_run_id,
        OLD.brief_reference_digest,OLD.brief_digest,OLD.card_digest,
        OLD.batch_id,OLD.delivery_id,OLD.effect_id,OLD.schema_version,OLD.created_at
    ) THEN
        RAISE EXCEPTION '100: research Brief delivery identity is immutable';
    END IF;
    IF OLD.status='prepared' AND NEW.status='sent' THEN
        NEW.sent_at := clock_timestamp();
        NEW.updated_at := NEW.sent_at;
        RETURN NEW;
    END IF;
    RAISE EXCEPTION '100: research Brief delivery transition is invalid';
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_brief_delivery_transition_v3() FROM PUBLIC;
CREATE TRIGGER research_brief_delivery_transition_v3
BEFORE UPDATE ON research_brief_deliveries
FOR EACH ROW EXECUTE FUNCTION enforce_research_brief_delivery_transition_v3();

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE research_brief_deliveries IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM research_brief_deliveries) THEN
        RAISE EXCEPTION
            '100: refusing downgrade while research Brief delivery receipts exist';
    END IF;
END $$;
-- +goose StatementEnd

DROP TRIGGER research_brief_delivery_transition_v3 ON research_brief_deliveries;
DROP FUNCTION enforce_research_brief_delivery_transition_v3();
REVOKE ALL ON SEQUENCE research_brief_deliveries_id_seq FROM vane_app;
REVOKE SELECT (
    id,tenant_id,user_id,task_id,run_snapshot_id,plan_id,
    temporal_workflow_id,temporal_run_id,definition_digest,plan_digest,
    notification_threshold,request_digest,evidence_digest,history_digest,
    status,significance,decision,delivery_required,brief_digest,finalized_at
) ON research_brief_syntheses FROM vane_push_effect_coordinator;
REVOKE SELECT (payload,created_at,v2_cutover_event_id)
    ON task_run_snapshots FROM vane_push_effect_coordinator;
REVOKE ALL ON research_brief_deliveries
    FROM vane_app,vane_push_effect_receipt,vane_push_effect_coordinator;
DROP TABLE research_brief_deliveries;
