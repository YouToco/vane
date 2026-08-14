-- 043: keep triage and its feedback fact in one deletion lifecycle.

-- +goose Up

ALTER TABLE feedback_freshness_triage
    DROP CONSTRAINT feedback_freshness_triage_feedback_id_fkey,
    ADD CONSTRAINT feedback_freshness_triage_feedback_id_fkey
        FOREIGN KEY (feedback_id) REFERENCES feedbacks(id) ON DELETE CASCADE;

-- +goose Down

ALTER TABLE feedback_freshness_triage
    DROP CONSTRAINT feedback_freshness_triage_feedback_id_fkey,
    ADD CONSTRAINT feedback_freshness_triage_feedback_id_fkey
        FOREIGN KEY (feedback_id) REFERENCES feedbacks(id);
