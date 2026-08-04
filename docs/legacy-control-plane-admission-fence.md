# Legacy control-plane admission fence

Status: active migration boundary
Owner decision: 2026-08-03

## Closed admission

The production composition root must inject `store.LegacyAdmissionFencedStore`
into every retained V1/V2 creation or definition-edit coordinator. Constructing
the facade closes an atomic admission gate inside the underlying Store, so even
a retained interface that promotes the original Store methods cannot bypass it.
The boundary has two deliberately different behaviors:

- a missing V1 creation/edit operation is rejected with
  `ErrLegacyControlPlaneAdmissionClosed`;
- an already durable operation is passed to the frozen Store implementation,
  which accepts only an exact response-loss replay;
- direct creation of a compiled V1 definition, `fetch_targets`, or
  `task_fetch_targets` product state is rejected.

Native Research V3 methods are inherited unchanged. V3 stores only the task
manual, schedule, notification/output policy and budget; it does not create a
Source, subscription, fetch target, or frozen Tool plan.

Migration 113 terminally resolves pristine expired V1 creation operations as
`expired/expired`, keeps the operation row, and inserts a `suppressed` terminal
receipt with `failure_class=legacy_admission_fence_expired`. It never deletes
an operation and never creates a delivery attempt. A still-unexpired or
executing operation remains available to the retained recovery path.

Migration 074 normalized historical automatic receipts to exactly
`receipt_provider='agent_auto/v1'` and `receipt_target=operation.id`. Migration
113 therefore recognizes only two pristine receipt shapes: the pre-074 empty
pair, or that exact normalized pair. Any mismatched provider/target remains
untouched for investigation. A row carrying a historical result or execution
timestamp is also non-pristine and remains byte-for-byte untouched. A read-only
production audit is:

```sql
SELECT id,receipt_provider,receipt_target
FROM task_creation_operations
WHERE execution_version=1 AND status='pending' AND expires_at<=clock_timestamp();
```

## Retained compatibility code

The following is historical/recovery state, not a current product surface:

- V1/V2 Approved Definition encoders and decoders;
- V1/V2 run snapshot readers and Temporal workflow/activity registrations;
- the compiled-task creation/edit commit code that may finish an operation
  admitted before the fence;
- `fetch_targets`, `task_fetch_targets`, and `content_sources` rows referenced
  by retained run/evidence provenance;
- old confirmation event enum decoders and migration files.

Migration 074 already dropped the account `subscriptions` table and renamed
`sources`/`schedule_sources` to the compatibility
`fetch_targets`/`task_fetch_targets` tables. No Source CRUD is allowed back
into Agent tools or Web APIs. `agent_session_fact_outbox` and its dispatcher
are business-fact projection, not confirmation-card infrastructure.

## Physical removal gate

Compatibility code or tables may be deleted only after all of the following
are proven in production:

1. every current task head is V3 and every formal schedule runs V3;
2. no non-terminal V1 creation or legacy definition-edit operation remains;
3. no V1/V2 Temporal execution remains inside namespace retention or archive;
4. all retained evidence references have an exact replacement or an explicit
   immutable legacy reader;
5. a static production-wiring inventory proves that only V3 admission is
   reachable.

Executed migrations and historical audit rows are never deleted as cleanup.
