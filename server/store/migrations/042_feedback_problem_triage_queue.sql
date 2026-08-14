-- 042: make feedback triage a durable all-reason queue.
-- Outdated reports remain pending until deterministic window reconstruction;
-- other fixed reasons are routed immediately to their non-interest channels.

-- +goose Up

ALTER TABLE feedback_freshness_triage
    ALTER COLUMN outcome DROP NOT NULL,
    ADD COLUMN reason_code TEXT,
    ADD COLUMN detail TEXT NOT NULL DEFAULT '',
    ADD COLUMN status TEXT NOT NULL DEFAULT 'pending',
    ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp();

UPDATE feedback_freshness_triage
   SET reason_code='outdated_or_out_of_window',
       status='classified',
       updated_at=created_at
 WHERE reason_code IS NULL;

ALTER TABLE feedback_freshness_triage
    ALTER COLUMN reason_code SET NOT NULL,
    DROP CONSTRAINT feedback_freshness_triage_outcome_valid,
    ADD CONSTRAINT feedback_freshness_triage_reason_valid CHECK (
        reason_code IN (
            'outdated_or_out_of_window',
            'not_relevant',
            'duplicate',
            'factually_wrong',
            'poor_source_or_evidence',
            'other'
        )
    ),
    ADD CONSTRAINT feedback_freshness_triage_status_valid CHECK (
        status IN ('pending','routed','classified')
    ),
    ADD CONSTRAINT feedback_freshness_triage_outcome_valid CHECK (
        outcome IS NULL OR outcome IN (
            'system_defect',
            'task_policy_suggestion',
            'policy_satisfied',
            'unverifiable',
            'interest_signal',
            'duplicate_diagnostic',
            'factual_diagnostic',
            'evidence_diagnostic',
            'manual_review'
        )
    ),
    ADD CONSTRAINT feedback_freshness_triage_state_valid CHECK (
        (status='pending' AND outcome IS NULL) OR
        (status<>'pending' AND outcome IS NOT NULL)
    );

CREATE INDEX idx_feedback_freshness_triage_pending
    ON feedback_freshness_triage (tenant_id, user_id, created_at, id)
    WHERE status='pending';

-- +goose Down

LOCK TABLE feedback_freshness_triage
    IN ACCESS EXCLUSIVE MODE /* migration 042 downgrade fence */;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM feedback_freshness_triage
         WHERE reason_code <> 'outdated_or_out_of_window'
            OR status <> 'classified'
            OR attempts <> 0
    ) THEN
        RAISE EXCEPTION '042: refusing downgrade while generalized problem triage exists';
    END IF;
END $$;
-- +goose StatementEnd

DROP INDEX idx_feedback_freshness_triage_pending;

ALTER TABLE feedback_freshness_triage
    DROP CONSTRAINT feedback_freshness_triage_state_valid,
    DROP CONSTRAINT feedback_freshness_triage_status_valid,
    DROP CONSTRAINT feedback_freshness_triage_reason_valid,
    DROP CONSTRAINT feedback_freshness_triage_outcome_valid,
    DROP COLUMN updated_at,
    DROP COLUMN attempts,
    DROP COLUMN status,
    DROP COLUMN detail,
    DROP COLUMN reason_code,
    ALTER COLUMN outcome SET NOT NULL,
    ADD CONSTRAINT feedback_freshness_triage_outcome_valid CHECK (
        outcome IN (
            'system_defect',
            'task_policy_suggestion',
            'policy_satisfied',
            'unverifiable'
        )
    );
