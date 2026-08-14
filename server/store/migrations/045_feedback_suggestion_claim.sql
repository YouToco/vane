-- 045: fenced delivery claim and fail-closed ambiguity for task-policy suggestions.

-- +goose Up

ALTER TABLE feedback_freshness_triage
    DROP CONSTRAINT feedback_freshness_triage_notification_valid,
    ADD COLUMN notification_claim_token TEXT,
    ADD COLUMN notification_lease_until TIMESTAMPTZ,
    ADD COLUMN notification_message_id TEXT,
    ADD COLUMN notification_last_error TEXT,
    ADD CONSTRAINT feedback_freshness_triage_notification_valid CHECK (
        notification_status IN (
            'not_required','pending','sending','sent','uncertain'
        ) AND
        (
            (
                notification_status='sending' AND
                notification_claim_token IS NOT NULL AND
                notification_lease_until IS NOT NULL AND
                notified_at IS NULL
            ) OR
            (
                notification_status='sent' AND
                notification_claim_token IS NULL AND
                notification_lease_until IS NULL AND
                notified_at IS NOT NULL
            ) OR
            (
                notification_status IN (
                    'not_required','pending','uncertain'
                ) AND
                notification_claim_token IS NULL AND
                notification_lease_until IS NULL AND
                notified_at IS NULL
            )
        )
    );

DROP INDEX idx_feedback_freshness_triage_notification_pending;
CREATE INDEX idx_feedback_freshness_triage_notification_pending
    ON feedback_freshness_triage (tenant_id,user_id,updated_at,id)
    WHERE notification_status IN ('pending','sending');

-- +goose Down

LOCK TABLE feedback_freshness_triage
    IN ACCESS EXCLUSIVE MODE /* migration 045 downgrade fence */;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM feedback_freshness_triage
         WHERE notification_status IN ('sending','uncertain')
            OR notification_claim_token IS NOT NULL
            OR notification_lease_until IS NOT NULL
            OR notification_message_id IS NOT NULL
            OR notification_last_error IS NOT NULL
    ) THEN
        RAISE EXCEPTION '045: refusing downgrade while fenced suggestion delivery state exists';
    END IF;
END $$;
-- +goose StatementEnd

DROP INDEX idx_feedback_freshness_triage_notification_pending;

ALTER TABLE feedback_freshness_triage
    DROP CONSTRAINT feedback_freshness_triage_notification_valid,
    DROP COLUMN notification_last_error,
    DROP COLUMN notification_message_id,
    DROP COLUMN notification_lease_until,
    DROP COLUMN notification_claim_token,
    ADD CONSTRAINT feedback_freshness_triage_notification_valid CHECK (
        notification_status IN ('not_required','pending','sent') AND
        (
            (notification_status='sent' AND notified_at IS NOT NULL) OR
            (notification_status<>'sent' AND notified_at IS NULL)
        )
    );

CREATE INDEX idx_feedback_freshness_triage_notification_pending
    ON feedback_freshness_triage (tenant_id,user_id,updated_at,id)
    WHERE notification_status='pending';
