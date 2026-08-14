-- 068: least-privilege exact event-evidence reader for the canonical Brief
-- writer. The function returns only one delivery's first-writer observed event
-- and the ordered content inventory admitted by that event and frozen run.

-- +goose Up

-- +goose StatementBegin
CREATE FUNCTION read_canonical_brief_event_evidence_v1(
    target_batch_id BIGINT,
    target_run_snapshot_id BIGINT,
    target_delivery_id BIGINT,
    target_observed_event_id BIGINT
)
RETURNS TABLE (
    observed_event_id BIGINT,
    policy_digest TEXT,
    event_key TEXT,
    event_type TEXT,
    subject TEXT,
    occurred_at TIMESTAMPTZ,
    evidence_json JSONB,
    evidence_ordinal BIGINT,
    delivery_content_item_id BIGINT,
    content_item_id BIGINT,
    content_title TEXT,
    canonical_url TEXT,
    published_at TIMESTAMPTZ,
    discovered_at TIMESTAMPTZ,
    content_body TEXT,
    frozen_source_id BIGINT,
    frozen_source_title TEXT,
    frozen_source_platform TEXT
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
    WITH scoped AS (
        SELECT e.id,e.policy_digest,e.event_key,e.event_type,e.subject,
               e.occurred_at,e.evidence_json,d.content_item_id AS delivery_item,
               d.created_at AS delivery_created_at,rs.payload
          FROM public.push_batches b
          JOIN public.deliveries d
            ON d.id=target_delivery_id
           AND d.batch_id=b.id
           AND d.tenant_id=b.tenant_id
           AND d.user_id=b.user_id
          JOIN public.task_run_snapshots rs
            ON rs.id=b.run_snapshot_id
           AND rs.tenant_id=b.tenant_id
           AND rs.user_id=b.user_id
           AND rs.task_id=b.schedule_id
          JOIN public.task_observed_events e
            ON e.id=target_observed_event_id
           AND e.tenant_id=b.tenant_id
           AND e.user_id=b.user_id
           AND e.task_id=b.schedule_id
           AND e.run_snapshot_id=rs.id
           AND e.temporal_run_id=rs.temporal_run_id
           AND e.delivery_id=d.id
           AND e.status IN ('qualified','delivered')
         WHERE b.id=target_batch_id
           AND rs.id=target_run_snapshot_id
           AND b.tenant_id IS NOT DISTINCT FROM
               NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
           AND b.user_id IS NOT DISTINCT FROM
               NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
           AND jsonb_typeof(e.evidence_json->'evidence_content_ids')='array'
    ),
    raw_ids AS (
        SELECT scoped.*,raw_id.value,raw_id.ordinality
          FROM scoped
          CROSS JOIN LATERAL jsonb_array_elements(
              scoped.evidence_json->'evidence_content_ids'
          ) WITH ORDINALITY AS raw_id(value,ordinality)
    ),
    parsed_ids AS (
        SELECT raw_ids.*,
               CASE
                 WHEN jsonb_typeof(value)='number'
                  AND (value#>>'{}') ~ '^[1-9][0-9]{0,18}$'
                 THEN (value#>>'{}')::bigint
               END AS parsed_content_item_id
          FROM raw_ids
    ),
    valid_ids AS (
        SELECT parsed_ids.*
          FROM parsed_ids
         WHERE parsed_content_item_id IS NOT NULL
           AND (SELECT count(*) FROM parsed_ids) BETWEEN 1 AND 8
           AND (SELECT count(*) FROM parsed_ids)=
               (SELECT count(DISTINCT parsed_content_item_id) FROM parsed_ids)
           AND (SELECT parsed_content_item_id
                  FROM parsed_ids
                 WHERE ordinality=1)=delivery_item
    )
    SELECT ids.id,ids.policy_digest,ids.event_key,ids.event_type,ids.subject,
           ids.occurred_at,ids.evidence_json,ids.ordinality,
           ids.delivery_item,ci.id,ci.title,ci.url,ci.published_at,
           ci.created_at,ci.content,matched.source_id,matched.source_title,
           matched.source_platform
      FROM valid_ids ids
      JOIN public.content_items ci ON ci.id=ids.parsed_content_item_id
      JOIN LATERAL (
          SELECT cs.source_id,
                 frozen_source.value->>'title' AS source_title,
                 frozen_source.value->>'platform' AS source_platform
            FROM public.content_sources cs
            JOIN LATERAL jsonb_array_elements(
                convert_from(ids.payload,'UTF8')::jsonb
                    #> '{definition,sources}'
            ) AS frozen_source(value)
              ON (frozen_source.value->>'source_id')::bigint=cs.source_id
           WHERE cs.content_item_id=ci.id
             AND cs.first_seen_at<=ids.delivery_created_at
           ORDER BY cs.source_id
           LIMIT 1
      ) matched ON true
     ORDER BY ids.ordinality
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION read_canonical_brief_event_evidence_v1(
    BIGINT,BIGINT,BIGINT,BIGINT
) FROM PUBLIC,vane_app,vane_brief_writer;
GRANT EXECUTE ON FUNCTION read_canonical_brief_event_evidence_v1(
    BIGINT,BIGINT,BIGINT,BIGINT
) TO vane_brief_writer;

-- +goose Down

REVOKE ALL ON FUNCTION read_canonical_brief_event_evidence_v1(
    BIGINT,BIGINT,BIGINT,BIGINT
) FROM vane_brief_writer;
DROP FUNCTION read_canonical_brief_event_evidence_v1(
    BIGINT,BIGINT,BIGINT,BIGINT
);
