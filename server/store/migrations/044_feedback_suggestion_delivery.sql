-- 044: durable user-visible delivery state for task-policy suggestions.

-- +goose Up

ALTER TABLE feedback_freshness_triage
    ADD COLUMN notification_status TEXT NOT NULL DEFAULT 'not_required',
    ADD COLUMN notification_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN notified_at TIMESTAMPTZ,
    ADD CONSTRAINT feedback_freshness_triage_notification_valid CHECK (
        notification_status IN ('not_required','pending','sent') AND
        (
            (notification_status='sent' AND notified_at IS NOT NULL) OR
            (notification_status<>'sent' AND notified_at IS NULL)
        )
    );

UPDATE feedback_freshness_triage
   SET notification_status='pending'
 WHERE outcome='task_policy_suggestion';

CREATE INDEX idx_feedback_freshness_triage_notification_pending
    ON feedback_freshness_triage (tenant_id,user_id,updated_at,id)
    WHERE notification_status='pending';

-- +goose Down

LOCK TABLE feedback_freshness_triage
    IN ACCESS EXCLUSIVE MODE /* migration 044 downgrade fence */;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM feedback_freshness_triage
         WHERE notification_status <> 'not_required'
            OR notification_attempts <> 0
    ) THEN
        RAISE EXCEPTION '044: refusing downgrade while suggestion delivery state exists';
    END IF;
END $$;
-- +goose StatementEnd

DROP INDEX idx_feedback_freshness_triage_notification_pending;
ALTER TABLE feedback_freshness_triage
    DROP CONSTRAINT feedback_freshness_triage_notification_valid,
    DROP COLUMN notified_at,
    DROP COLUMN notification_attempts,
    DROP COLUMN notification_status;
