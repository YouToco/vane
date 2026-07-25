-- 046: split pre-dispatch claim from ambiguous external send.

-- +goose Up

ALTER TABLE feedback_freshness_triage
    DROP CONSTRAINT feedback_freshness_triage_notification_valid,
    ADD CONSTRAINT feedback_freshness_triage_notification_valid CHECK (
        notification_status IN (
            'not_required','pending','claimed','sending','sent','uncertain'
        ) AND
        (
            (
                notification_status IN ('claimed','sending') AND
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
    WHERE notification_status IN ('pending','claimed','sending');

-- +goose Down

LOCK TABLE feedback_freshness_triage
    IN ACCESS EXCLUSIVE MODE /* migration 046 downgrade fence */;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM feedback_freshness_triage
         WHERE notification_status='claimed'
    ) THEN
        RAISE EXCEPTION '046: refusing downgrade while pre-dispatch claims exist';
    END IF;
END $$;
-- +goose StatementEnd

DROP INDEX idx_feedback_freshness_triage_notification_pending;

ALTER TABLE feedback_freshness_triage
    DROP CONSTRAINT feedback_freshness_triage_notification_valid,
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

CREATE INDEX idx_feedback_freshness_triage_notification_pending
    ON feedback_freshness_triage (tenant_id,user_id,updated_at,id)
    WHERE notification_status IN ('pending','sending');
