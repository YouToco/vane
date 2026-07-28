# Structured Event Evidence Contract

## Status

- Phase: 2-B0 (dark Store foundation)
- Risk: S (event identity, first-writer replay, future immutable Brief payload)
- Production call points: zero
- Migration: 068, adding only the restricted canonical-Brief event-evidence
  reader described below
- Rollout: none in 2-B0; the existing Phase 2-A exact canary is unchanged

Phase 2-A has engineering and quiet-path production proof, but no natural
non-empty structured Brief yet. This batch therefore does not advance that
canary or change CardGen, Temporal, Brief, API, Feishu, Web, LLM, quota, push,
observation admission, or recovery behavior.

## Purpose

The next structured-analysis step needs one durable identity for the qualified
event that owns an Insight. `task_observed_events` already enforces event
deduplication and binds a qualified event to one exact delivery, but its current
reservation API returns only a boolean. A later versioned runtime would
otherwise have to rediscover or guess the event row when freezing multi-source
evidence into a Brief.

2-B0 adds a zero-call-point Store primitive that returns a sealed
`ObservedEventProvenanceV1` while reusing the exact existing reservation,
replay, stale takeover, push-effect and tenant/user/task/run fences.

## Provenance

`ObservedEventProvenanceV1` contains:

- observed-event row ID;
- schema version;
- policy digest and event key;
- event type and subject;
- canonical UTC/microsecond occurrence time;
- a digest of the canonical `evidence_json` value.

The Store obtains these fields from the authoritative
`task_observed_events` row inside the reservation transaction and exposes them
only after commit. It never seals caller bytes after an exact replay: if a
response-lost caller retries with different event fields, the original
first-writer row remains authoritative and the returned provenance still
matches that stored row.

The evidence digest canonicalizes JSON object key order and insignificant
whitespace. It changes when the semantic evidence value changes. Raw evidence
JSON and content bodies are not copied into the provenance object. The
provenance boundary rejects evidence larger than 256 KiB before reservation
and canonical decoding.

## Invariants

1. `ReserveObservedEventV1` behavior and signature do not change.
2. The new primitive has zero non-test callers in 2-B0.
3. First reservation, exact replay and frozen push-effect replay return the
   same observed-event row identity.
4. Cross-run duplicates remain rejected unless the existing bounded stale
   takeover rules admit the new exact run.
5. The returned provenance is loaded under exact tenant, user, task, snapshot
   and Temporal RunID scope; a mismatched persisted user is a conflict even
   when an abnormal legacy row has no delivery.
6. Driver error chains remain behind the existing controlled database error.
7. No Brief schema, API DTO, renderer or Temporal payload changes in 2-B0.

## Required proof

- pure type tests for canonical JSON, time normalization and tamper rejection;
- PostgreSQL 18 reservation/replay test proving caller drift cannot replace
  the stored first writer;
- existing observed-event concurrency, stale takeover and push-effect suites;
- mutation proof: sealing caller evidence instead of the committed row must
  fail the first-writer replay test;
- AST guard proving zero production callers;
- affected race, full Store race, full repository race, vet and build.

## Later wiring boundary

A separate 2-B1 batch may introduce a new durable runtime label and CardGen
Activity for multi-source event evidence. It must keep one paid CardGen call,
load only content IDs admitted by the frozen qualified event and run snapshot,
freeze inventory-owned source metadata, and preserve the Phase 2-A exact
canary until a natural non-empty run proves claims/evidence end to end.

2-B0 does not authorize 2-B1, batch summaries, extra LLM calls, search,
bookmarks, trends, reports, default experience changes or a wider user scope.

## Phase 2-B1 contract: default-off event evidence runtime

### Status and rollout

- Risk: S (snapshot authority, paid-call replay, immutable Brief payload)
- Production activation: off in the first deployment
- Migration: none
- User-visible behavior: none while the rollout is off

2-B1 adds a separate durable runtime label and a separate CardGen Activity
name. Its rollout is independently default-off and must be nested inside the
existing Phase 2-A generation and renderer authority and the exact observation
authority canary. Because observation authority is currently exact-task only,
2-B1 `allow_all` is rejected until a later separately approved rollout design.
The Phase 2-A exact-task canary remains selected in production until a natural
non-empty run proves its current claims/evidence path. Deploying 2-B1 does not
move that task to the new runtime and does not change any existing Temporal
command sequence.

### Evidence inventory

For one qualified event, the new Activity may use only the ordered,
duplicate-free `evidence_content_ids` frozen in that event's canonical
`evidence_json`. The list is bounded to eight sources. Every content ID must:

1. exist in the exact run snapshot's candidate inventory;
2. have an appearance in one of the exact frozen source IDs;
3. be loaded under the exact tenant, user, task, snapshot, WorkflowID and
   RunID authority;
4. retain the qualifier's order, with opaque request references
   `source-1` through `source-N`.

The model receives bounded, sanitized body text plus inventory-owned titles.
It never receives database IDs or mutable source configuration. The Activity
freezes, in its Temporal result, the exact bounded evidence corpus used for
claim validation and these inventory-owned fields:

- opaque source reference;
- title and canonical source URL;
- frozen source title and platform;
- publication and discovery time.

The CardGen result may cite any supplied opaque source reference. Every claim
excerpt must occur in every cited source. The existing evidence digest covers
the complete ordered ref/body corpus; a later Store step independently
revalidates the frozen Activity result before staging the Brief.

### Durable Brief binding

The canonical Brief freezes an optional event-evidence extension containing:

- the first-writer `ObservedEventProvenanceV1` returned by the reservation
  transaction;
- the ordered inventory-owned evidence source metadata;
- the structured evidence digest already validated against the exact Activity
  corpus.

Raw evidence bodies and database content IDs are not copied into the Brief or
its API projection. The Brief request digest covers the complete extension.
Response-lost replay must return the byte-identical first writer; a different
provenance, source order, source metadata or evidence digest conflicts.

The reservation still occurs in the existing pre-Brief delivery-planning
transactional seam. Rejected cross-run duplicates do not acquire a Brief row.
Stale takeover and frozen push-effect replay keep the same first-writer event
identity guaranteed by 2-B0.

Migration 068 adds one bounded `SECURITY DEFINER` reader executable only by
`vane_brief_writer`. Inside the Brief staging transaction it returns exactly
one delivery-bound first-writer observed event plus the ordered content rows
and frozen snapshot source attribution named by that event. The writer retains
no table-level access to observed events, content, sources, or run snapshots.
The Store compares the complete Activity-owned IDs, bounded bodies, metadata,
corpus refs and provenance before stripping bodies/IDs from the Brief.

### Temporal and paid-call invariants

1. Pre-2-B1 histories keep `CardGenOutcomeV2` and their existing result shape.
2. The new runtime records a `GetVersion` marker before scheduling the new
   Activity name and result shape.
3. Each selected item still causes at most one paid CardGen call. The new
   Activity keeps one Temporal attempt; ambiguous completion fails partial and
   does not retry the paid call.
4. Evidence loading and authority validation happen before the paid call.
   Missing, duplicated, out-of-snapshot or over-limit evidence fails closed
   with zero CardGen spend.
5. Rollout disablement stops new 2-B1 histories but readers and renderers keep
   serving already-frozen optional event evidence.
6. No extra LLM, observation, quota, push, feedback, API route or renderer
   authority is introduced.

### Required proof

- pure validation tests for evidence order, bounds, opaque refs, canonical
  times and tamper rejection;
- PostgreSQL 18 tests for exact snapshot membership, cross-tenant/user/run
  denial, source drift, missing/duplicate IDs and deterministic source
  attribution;
- CardGen tests proving multi-source citations, excerpt validation, zero spend
  before invalid inventory rejection and exactly one model call;
- Brief first seal, response-lost replay, provenance/source conflict and raw
  body/content-ID absence tests;
- Temporal replay for pre-2-B1 structured histories and new
  success/partial/failure histories;
- mutation proof: replacing the first-writer provenance or allowing an
  out-of-snapshot evidence ID must make the corresponding test fail;
- affected and full repository race, vet, build, two independent reviews,
  complete CI and a production zero-behavior probe with the rollout off.

2-B1 still does not authorize batch summaries, extra model calls, search,
bookmarks, trends, reports, default experience changes, wider user scope, or
activation before the Phase 2-A natural non-empty Gate.
