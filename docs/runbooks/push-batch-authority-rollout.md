# Push batch authority compatibility rollout

Migration 047 is a binary-compatibility fence, not a live effect rollout.
Production push-effect and recovery call points remain dark.

## Deploy

1. Stop every pre-047 worker and wait until its in-flight Push activities have
   drained.
2. Apply migration 047.
3. Start only binaries that take the shared `VANEPUSH` schema fence and claim a
   durable batch delivery authority before any provider side effect.
4. Verify new batches select `legacy`, batches with historical `push_effects`
   select `effect`, and no batch authority changes after its first claim.

## Rollback boundary

After migration 047 is applied, never roll a worker back to a pre-fence binary.
Such a binary does not participate in the schema/batch locks and can bypass the
durable winner. Roll forward with another fenced binary instead.

Migration 047 Down is intentionally fail-closed while any authority or durable
effect exists. It does not revoke permissions created by migration 039.
