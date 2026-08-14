-- 083: make task deletion remove its periodic-brief descendants atomically.
--
-- Migration 072 made periodic brief artifacts immutable by using RESTRICT on
-- every parent edge. That also made DELETE /api/schedules/{id} impossible as
-- soon as a task had produced a periodic brief intent. These artifacts are
-- owned by exactly one task, so task deletion must cascade through the whole
-- private subtree while all tenant/user scope constraints remain intact.

-- +goose Up

ALTER TABLE periodic_report_deliveries
    DROP CONSTRAINT periodic_report_deliveries_report_id_fkey,
    DROP CONSTRAINT fk_periodic_delivery_scope,
    ADD CONSTRAINT periodic_report_deliveries_report_id_fkey
        FOREIGN KEY (report_id)
        REFERENCES periodic_brief_reports(id) ON DELETE CASCADE,
    ADD CONSTRAINT fk_periodic_delivery_scope
        FOREIGN KEY (report_id,tenant_id,user_id,task_id)
        REFERENCES periodic_brief_reports (id,tenant_id,user_id,task_id)
        ON DELETE CASCADE;

ALTER TABLE periodic_brief_reports
    DROP CONSTRAINT fk_periodic_report_intent,
    ADD CONSTRAINT fk_periodic_report_intent
        FOREIGN KEY (intent_id,tenant_id,user_id,task_id)
        REFERENCES periodic_brief_intents (id,tenant_id,user_id,task_id)
        ON DELETE CASCADE;

ALTER TABLE periodic_synthesis_receipts
    DROP CONSTRAINT periodic_synthesis_receipts_intent_id_fkey,
    DROP CONSTRAINT fk_periodic_synthesis_scope,
    ADD CONSTRAINT periodic_synthesis_receipts_intent_id_fkey
        FOREIGN KEY (intent_id)
        REFERENCES periodic_brief_intents(id) ON DELETE CASCADE,
    ADD CONSTRAINT fk_periodic_synthesis_scope
        FOREIGN KEY (intent_id,tenant_id,user_id,task_id)
        REFERENCES periodic_brief_intents (id,tenant_id,user_id,task_id)
        ON DELETE CASCADE;

ALTER TABLE periodic_brief_intents
    DROP CONSTRAINT fk_periodic_brief_task,
    ADD CONSTRAINT fk_periodic_brief_task
        FOREIGN KEY (tenant_id,user_id,task_id)
        REFERENCES schedules (tenant_id,user_id,id) ON DELETE CASCADE;

-- +goose Down

ALTER TABLE periodic_report_deliveries
    DROP CONSTRAINT periodic_report_deliveries_report_id_fkey,
    DROP CONSTRAINT fk_periodic_delivery_scope,
    ADD CONSTRAINT periodic_report_deliveries_report_id_fkey
        FOREIGN KEY (report_id)
        REFERENCES periodic_brief_reports(id) ON DELETE RESTRICT,
    ADD CONSTRAINT fk_periodic_delivery_scope
        FOREIGN KEY (report_id,tenant_id,user_id,task_id)
        REFERENCES periodic_brief_reports (id,tenant_id,user_id,task_id);

ALTER TABLE periodic_brief_reports
    DROP CONSTRAINT fk_periodic_report_intent,
    ADD CONSTRAINT fk_periodic_report_intent
        FOREIGN KEY (intent_id,tenant_id,user_id,task_id)
        REFERENCES periodic_brief_intents (id,tenant_id,user_id,task_id)
        ON DELETE RESTRICT;

ALTER TABLE periodic_synthesis_receipts
    DROP CONSTRAINT periodic_synthesis_receipts_intent_id_fkey,
    DROP CONSTRAINT fk_periodic_synthesis_scope,
    ADD CONSTRAINT periodic_synthesis_receipts_intent_id_fkey
        FOREIGN KEY (intent_id)
        REFERENCES periodic_brief_intents(id) ON DELETE RESTRICT,
    ADD CONSTRAINT fk_periodic_synthesis_scope
        FOREIGN KEY (intent_id,tenant_id,user_id,task_id)
        REFERENCES periodic_brief_intents (id,tenant_id,user_id,task_id);

ALTER TABLE periodic_brief_intents
    DROP CONSTRAINT fk_periodic_brief_task,
    ADD CONSTRAINT fk_periodic_brief_task
        FOREIGN KEY (tenant_id,user_id,task_id)
        REFERENCES schedules (tenant_id,user_id,id) ON DELETE RESTRICT;
