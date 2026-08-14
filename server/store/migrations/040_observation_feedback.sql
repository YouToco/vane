-- 040: typed feedback causes and one-record bad-feedback semantics.
-- Depends on 039_push_effect_store.sql and must be merged after it.

-- +goose Up

ALTER TABLE feedbacks
    ADD COLUMN reason_code TEXT;

ALTER TABLE feedbacks
    ADD CONSTRAINT feedbacks_reason_code_valid CHECK (
        reason_code IS NULL OR (
            action = 'misjudged' AND reason_code IN (
                'outdated_or_out_of_window',
                'not_relevant',
                'duplicate',
                'factually_wrong',
                'poor_source_or_evidence',
                'other'
            )
        )
    );

-- A bad-feedback panel produces at most one submitted event. Attitudes remain
-- append-only and are deliberately not covered by this index.
CREATE UNIQUE INDEX uq_feedbacks_delivery_typed_misjudged
    ON feedbacks (delivery_id)
    WHERE action = 'misjudged' AND reason_code IS NOT NULL;

CREATE TABLE task_event_qualification_steps (
    id                  BIGSERIAL PRIMARY KEY,
    tenant_id           BIGINT NOT NULL REFERENCES tenants(id),
    user_id             BIGINT NOT NULL REFERENCES users(id),
    -- No schedules FK: task deletion must not destroy or be blocked by the
    -- paid-call checkpoint audit, matching task_run_snapshots.task_id.
    task_id             TEXT NOT NULL,
    run_snapshot_id     BIGINT NOT NULL REFERENCES task_run_snapshots(id),
    temporal_run_id     TEXT NOT NULL,
    step_id             TEXT NOT NULL,
    request_digest      TEXT NOT NULL,
    status              TEXT NOT NULL,
    response_json       JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT task_event_qualification_digest_valid
        CHECK (request_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT task_event_qualification_status_valid
        CHECK (status IN ('prepared','sending','completed','uncertain')),
    CONSTRAINT task_event_qualification_response_valid CHECK (
        (status = 'completed' AND response_json IS NOT NULL) OR
        (status <> 'completed' AND response_json IS NULL)
    ),
    CONSTRAINT uq_task_event_qualification_step
        UNIQUE (tenant_id, task_id, run_snapshot_id, temporal_run_id, step_id)
);

CREATE TABLE task_observed_events (
    id                  BIGSERIAL PRIMARY KEY,
    tenant_id           BIGINT NOT NULL REFERENCES tenants(id),
    user_id             BIGINT NOT NULL REFERENCES users(id),
    -- Durable event evidence survives task deletion; exact ownership remains
    -- bound by tenant/user plus the immutable run snapshot.
    task_id             TEXT NOT NULL,
    policy_digest       TEXT NOT NULL,
    event_key           TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    subject             TEXT NOT NULL,
    occurred_at         TIMESTAMPTZ NOT NULL,
    evidence_json       JSONB NOT NULL,
    run_snapshot_id     BIGINT NOT NULL REFERENCES task_run_snapshots(id),
    temporal_run_id     TEXT NOT NULL,
    delivery_id         BIGINT REFERENCES deliveries(id),
    status              TEXT NOT NULL DEFAULT 'qualified',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    delivered_at        TIMESTAMPTZ,
    CONSTRAINT task_observed_event_digests_valid CHECK (
        policy_digest ~ '^[0-9a-f]{64}$' AND event_key ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT task_observed_event_status_valid
        CHECK (status IN ('qualified','delivered')),
    CONSTRAINT task_observed_event_delivery_valid CHECK (
        (status = 'qualified' AND delivered_at IS NULL) OR
        (status = 'delivered' AND delivery_id IS NOT NULL AND delivered_at IS NOT NULL)
    ),
    CONSTRAINT uq_task_observed_event
        UNIQUE (tenant_id, task_id, policy_digest, event_key)
);

CREATE INDEX idx_task_observed_events_delivery
    ON task_observed_events (delivery_id) WHERE delivery_id IS NOT NULL;

GRANT SELECT, INSERT, UPDATE, DELETE ON
    task_event_qualification_steps, task_observed_events TO vane_app;
GRANT USAGE, SELECT ON SEQUENCE
    task_event_qualification_steps_id_seq, task_observed_events_id_seq TO vane_app;

ALTER TABLE task_event_qualification_steps ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON task_event_qualification_steps
    FOR ALL TO vane_app USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON task_event_qualification_steps AS RESTRICTIVE
    FOR ALL TO vane_app
    USING (tenant_id IS NOT DISTINCT FROM
        (SELECT current_setting('app.tenant_id', true))::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
        (SELECT current_setting('app.tenant_id', true))::bigint);

ALTER TABLE task_observed_events ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON task_observed_events
    FOR ALL TO vane_app USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON task_observed_events AS RESTRICTIVE
    FOR ALL TO vane_app
    USING (tenant_id IS NOT DISTINCT FROM
        (SELECT current_setting('app.tenant_id', true))::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
        (SELECT current_setting('app.tenant_id', true))::bigint);

-- +goose Down

-- Retain both locks through the emptiness check and DROP. Without the locks, a
-- concurrent append could commit after EXISTS and be silently destroyed.
LOCK TABLE task_observed_events, task_event_qualification_steps, feedbacks
    IN ACCESS EXCLUSIVE MODE /* migration 040 downgrade fence */;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM task_observed_events) OR
       EXISTS (SELECT 1 FROM task_event_qualification_steps) OR
       EXISTS (SELECT 1 FROM feedbacks WHERE reason_code IS NOT NULL) THEN
        RAISE EXCEPTION '040: refusing downgrade while durable observation state exists';
    END IF;
END $$;
-- +goose StatementEnd

DROP INDEX IF EXISTS uq_feedbacks_delivery_typed_misjudged;
DROP TABLE IF EXISTS task_observed_events;
DROP TABLE IF EXISTS task_event_qualification_steps;
ALTER TABLE feedbacks DROP CONSTRAINT IF EXISTS feedbacks_reason_code_valid;
ALTER TABLE feedbacks DROP COLUMN IF EXISTS reason_code;
