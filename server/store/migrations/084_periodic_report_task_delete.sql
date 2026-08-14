-- 084: preserve report immutability while allowing task-owned FK cascades.
--
-- PostgreSQL executes ON DELETE CASCADE through nested trigger commands. The
-- report immutability trigger from 072 therefore also sees legitimate task
-- deletion. Permit only a nested delete whose owning schedule is already gone;
-- direct report deletion and cascades from any still-live task remain denied.

-- +goose Up

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION deny_periodic_brief_report_mutation_v1()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        IF pg_trigger_depth()>1 AND NOT EXISTS (
            SELECT 1
              FROM public.schedules s
             WHERE s.tenant_id=OLD.tenant_id
               AND s.user_id=OLD.user_id
               AND s.id=OLD.task_id
        ) THEN
            RETURN OLD;
        END IF;
        IF EXISTS (
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

-- +goose Down

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION deny_periodic_brief_report_mutation_v1()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        IF EXISTS (
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
