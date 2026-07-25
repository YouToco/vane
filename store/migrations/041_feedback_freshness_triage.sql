-- 041: deterministic audit lane for "outdated" problem feedback.
-- The audit is diagnostic only; it never edits a task or profile.

-- +goose Up

CREATE TABLE feedback_freshness_triage (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       BIGINT NOT NULL REFERENCES tenants(id),
    user_id         BIGINT NOT NULL REFERENCES users(id),
    feedback_id     BIGINT NOT NULL UNIQUE REFERENCES feedbacks(id),
    delivery_id     BIGINT NOT NULL REFERENCES deliveries(id),
    task_id         TEXT,
    outcome         TEXT NOT NULL,
    audit_json      JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT feedback_freshness_triage_outcome_valid CHECK (
        outcome IN (
            'system_defect',
            'task_policy_suggestion',
            'policy_satisfied',
            'unverifiable'
        )
    )
);

GRANT SELECT, INSERT, UPDATE, DELETE ON feedback_freshness_triage TO vane_app;
GRANT USAGE, SELECT ON SEQUENCE feedback_freshness_triage_id_seq TO vane_app;

ALTER TABLE feedback_freshness_triage ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON feedback_freshness_triage
    FOR ALL TO vane_app USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON feedback_freshness_triage AS RESTRICTIVE
    FOR ALL TO vane_app
    USING (tenant_id IS NOT DISTINCT FROM
        (SELECT current_setting('app.tenant_id', true))::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
        (SELECT current_setting('app.tenant_id', true))::bigint);

-- +goose Down

LOCK TABLE feedback_freshness_triage
    IN ACCESS EXCLUSIVE MODE /* migration 041 downgrade fence */;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM feedback_freshness_triage) THEN
        RAISE EXCEPTION '041: refusing downgrade while feedback freshness audit exists';
    END IF;
END $$;
-- +goose StatementEnd

DROP TABLE feedback_freshness_triage;
